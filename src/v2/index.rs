#![allow(dead_code)]

use crate::util::{begin_temp_file, retry_eintr, string_to_cstring, sync_and_commit};
use crate::v2::error::{CoreError, CoreResult, core_error_from_fuse};
use nix::unistd::write;
use std::collections::HashMap;
use std::io::Read;
use std::os::fd::{AsFd, BorrowedFd};

const MAGIC: &[u8; 4] = b"LN2I";
const VERSION: u32 = 1;
const MAX_INDEX_BYTES: usize = 16 * 1024 * 1024; // 简单上限防止损坏文件拖垮内存

pub const INDEX_NAME: &str = ".ln2_index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirIndexEntry {
    pub backend_name: String,
    pub raw_name: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
pub struct DirIndex {
    entries: HashMap<String, DirIndexEntry>,
    raw_to_backend: HashMap<Vec<u8>, String>,
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

    pub fn upsert(&mut self, backend_name: String, raw_name: Vec<u8>) {
        let entry = DirIndexEntry {
            backend_name: backend_name.clone(),
            raw_name,
        };
        if let Some(prev) = self.entries.insert(backend_name.clone(), entry) {
            self.raw_to_backend.remove(&prev.raw_name);
        }
        self.raw_to_backend
            .insert(self.entries[&backend_name].raw_name.clone(), backend_name);
        self.dirty = true;
    }

    pub fn remove(&mut self, backend_name: &str) -> Option<DirIndexEntry> {
        let removed = self.entries.remove(backend_name);
        if let Some(entry) = &removed {
            self.raw_to_backend.remove(&entry.raw_name);
        }
        if removed.is_some() {
            self.dirty = true;
        }
        removed
    }

    pub fn get(&self, backend_name: &str) -> Option<&DirIndexEntry> {
        self.entries.get(backend_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirIndexEntry)> {
        self.entries.iter()
    }

    pub fn contains_key(&self, backend_name: &str) -> bool {
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

    pub fn backend_for_raw(&self, raw: &[u8]) -> Option<&String> {
        self.raw_to_backend.get(raw)
    }
}

fn read_index_bytes(dir_fd: BorrowedFd<'_>) -> CoreResult<Option<Vec<u8>>> {
    let name = string_to_cstring(INDEX_NAME).map_err(core_error_from_fuse)?;
    let fd = match nix::fcntl::openat(
        dir_fd,
        name.as_c_str(),
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

fn decode_index_bytes(bytes: &[u8]) -> Option<DirIndex> {
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
        let name = std::str::from_utf8(&bytes[offset..offset + name_len])
            .ok()?
            .to_owned();
        offset += name_len;

        let raw_len = read_u32(bytes, &mut offset)? as usize;
        if bytes.len() < offset + raw_len {
            return None;
        }
        let raw_name = bytes[offset..offset + raw_len].to_vec();
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
        let name_bytes = entry.backend_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(name_bytes);

        buf.extend_from_slice(&(entry.raw_name.len() as u32).to_le_bytes());
        buf.extend_from_slice(&entry.raw_name);
    }

    buf
}

pub fn write_dir_index(dir_fd: BorrowedFd<'_>, index: &DirIndex) -> CoreResult<()> {
    let data = encode_index(index);
    let final_name = string_to_cstring(INDEX_NAME).map_err(core_error_from_fuse)?;
    let tmp =
        begin_temp_file(dir_fd, final_name.as_c_str(), "idx").map_err(core_error_from_fuse)?;
    let mut written = 0;
    while written < data.len() {
        let n = retry_eintr(|| write(tmp.fd.as_fd(), &data[written..])).map_err(CoreError::from)?;
        written += n;
    }
    sync_and_commit(dir_fd, tmp, final_name.as_c_str()).map_err(core_error_from_fuse)
}
