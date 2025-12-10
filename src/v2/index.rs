#![allow(dead_code)]

use crate::util::{core_begin_temp_file, core_sync_and_commit, retry_eintr};
use crate::v2::error::{CoreError, CoreResult};
use nix::unistd::write;
use std::collections::HashMap;
use std::ffi::CStr;
use std::io::{Read, Write};
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;
use zstd::stream::{read::Decoder as ZstdDecoder, write::Encoder as ZstdEncoder};

const MAGIC: &[u8; 4] = b"LN2I";
const MAGIC_ZSTD: &[u8; 4] = b"LN2Z";
const VERSION: u32 = 1;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024; // 上限（压缩后）防止损坏文件拖垮内存

pub const INDEX_NAME: &str = ".ln2_index";
#[allow(clippy::manual_c_str_literals)]
const INDEX_NAME_CSTR: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b".ln2_index\0") };

type SharedBytes = Arc<[u8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirIndexEntry {
    pub backend_name: SharedBytes,
    pub raw_name: SharedBytes,
}

#[derive(Debug, Default, Clone)]
pub struct DirIndex {
    entries: HashMap<SharedBytes, DirIndexEntry>,
    raw_to_backend: HashMap<SharedBytes, SharedBytes>,
    dirty: bool,
}

impl DirIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            raw_to_backend: HashMap::new(),
            dirty: false,
        }
    }

    pub fn upsert(&mut self, backend_name: Vec<u8>, raw_name: Vec<u8>) {
        let backend_name: SharedBytes = Arc::from(backend_name);
        let raw_name: SharedBytes = Arc::from(raw_name);
        let entry = DirIndexEntry {
            backend_name: backend_name.clone(),
            raw_name: raw_name.clone(),
        };
        if let Some(prev) = self.entries.insert(backend_name.clone(), entry) {
            self.raw_to_backend.remove(&prev.raw_name);
        }
        self.raw_to_backend.insert(raw_name, backend_name);
        self.dirty = true;
    }

    pub fn remove(&mut self, backend_name: &[u8]) -> Option<DirIndexEntry> {
        let removed = self.entries.remove(backend_name);
        if let Some(entry) = &removed {
            self.raw_to_backend.remove(&entry.raw_name);
        }
        if removed.is_some() {
            self.dirty = true;
        }
        removed
    }

    pub fn get(&self, backend_name: &[u8]) -> Option<&DirIndexEntry> {
        self.entries.get(backend_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&SharedBytes, &DirIndexEntry)> {
        self.entries.iter()
    }

    pub fn contains_key(&self, backend_name: &[u8]) -> bool {
        self.entries.contains_key(backend_name)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub fn contains_raw_name(&self, raw: &[u8]) -> bool {
        self.raw_to_backend.contains_key(raw)
    }

    pub fn backend_for_raw(&self, raw: &[u8]) -> Option<SharedBytes> {
        self.raw_to_backend.get(raw).cloned()
    }
}

fn read_index_bytes(dir_fd: BorrowedFd<'_>) -> CoreResult<Option<Vec<u8>>> {
    let fd = match nix::fcntl::openat(
        dir_fd,
        INDEX_NAME_CSTR,
        nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(err) => return Err(CoreError::from(err)),
    };

    let mut file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(CoreError::from)?;
    if buf.len() > MAX_INDEX_BYTES {
        return Ok(None);
    }
    Ok(Some(buf))
}

fn decode_plain_index(bytes: &[u8]) -> Option<DirIndex> {
    if bytes.len() < MAGIC.len() + 8 {
        return None;
    }
    if &bytes[..MAGIC.len()] != MAGIC {
        return None;
    }
    let mut offset = MAGIC.len();

    let read_u32 = |buf: &[u8], off: &mut usize| -> Option<u32> {
        if buf.len() < *off + 4 {
            return None;
        }
        let val = u32::from_le_bytes(buf[*off..*off + 4].try_into().ok()?);
        *off += 4;
        Some(val)
    };

    let version = read_u32(bytes, &mut offset)?;
    if version != VERSION {
        return None;
    }
    let count = read_u32(bytes, &mut offset)? as usize;
    let mut index = DirIndex::new();

    for _ in 0..count {
        let name_len = read_u32(bytes, &mut offset)? as usize;
        if bytes.len() < offset + name_len {
            return None;
        }
        let name = Arc::<[u8]>::from(&bytes[offset..offset + name_len]);
        offset += name_len;

        let raw_len = read_u32(bytes, &mut offset)? as usize;
        if bytes.len() < offset + raw_len {
            return None;
        }
        let raw_name = Arc::<[u8]>::from(&bytes[offset..offset + raw_len]);
        offset += raw_len;

        index.entries.insert(
            name.clone(),
            DirIndexEntry {
                backend_name: name,
                raw_name,
            },
        );
    }

    index.dirty = false;
    for entry in index.entries.values() {
        index
            .raw_to_backend
            .insert(entry.raw_name.clone(), entry.backend_name.clone());
    }
    Some(index)
}

fn decode_index_bytes(bytes: &[u8]) -> Option<DirIndex> {
    if bytes.len() < MAGIC.len() {
        return None;
    }
    match &bytes[..MAGIC.len()] {
        magic if magic == MAGIC => decode_plain_index(bytes),
        magic if magic == MAGIC_ZSTD => {
            let mut decoder = ZstdDecoder::new(&bytes[MAGIC_ZSTD.len()..]).ok()?;
            let mut decoded = Vec::new();
            decoder.read_to_end(&mut decoded).ok()?;
            decode_index_bytes(&decoded)
        }
        _ => None,
    }
}

pub fn read_dir_index(dir_fd: BorrowedFd<'_>) -> CoreResult<Option<DirIndex>> {
    match read_index_bytes(dir_fd)? {
        None => Ok(None),
        Some(bytes) => Ok(decode_index_bytes(&bytes)),
    }
}

fn encode_index(index: &DirIndex) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&(index.entries.len() as u32).to_le_bytes());

    for entry in index.entries.values() {
        buf.extend_from_slice(&(entry.backend_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(entry.backend_name.as_ref());

        buf.extend_from_slice(&(entry.raw_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(entry.raw_name.as_ref());
    }

    buf
}

fn encode_index_compressed(raw: &[u8]) -> CoreResult<Vec<u8>> {
    let mut encoder = ZstdEncoder::new(Vec::new(), 3).map_err(CoreError::from)?;
    encoder.write_all(raw).map_err(CoreError::from)?;
    let mut compressed = encoder.finish().map_err(CoreError::from)?;
    let mut out = Vec::with_capacity(MAGIC_ZSTD.len() + compressed.len());
    out.extend_from_slice(MAGIC_ZSTD);
    out.append(&mut compressed);
    Ok(out)
}

pub fn write_dir_index(dir_fd: BorrowedFd<'_>, index: &DirIndex) -> CoreResult<()> {
    let raw = encode_index(index);
    let compressed = encode_index_compressed(&raw)?;
    let data = if compressed.len() <= MAX_INDEX_BYTES {
        compressed
    } else if raw.len() <= MAX_INDEX_BYTES {
        raw
    } else {
        eprintln!(
            "Index too large to write: raw {} bytes, compressed {} bytes",
            raw.len(),
            compressed.len()
        );
        return Ok(());
    };
    let tmp = core_begin_temp_file(dir_fd, INDEX_NAME_CSTR, "idx").map_err(CoreError::from)?;
    let mut written = 0;
    while written < data.len() {
        let n = retry_eintr(|| write(tmp.fd.as_fd(), &data[written..])).map_err(CoreError::from)?;
        written += n;
    }
    core_sync_and_commit(dir_fd, tmp, INDEX_NAME_CSTR).map_err(CoreError::from)
}
