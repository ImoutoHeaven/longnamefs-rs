#![allow(dead_code)]

use crate::util::{core_begin_temp_file, core_sync_and_commit, retry_eintr};
use crate::v2::error::{CoreError, CoreResult};
use nix::sys::stat::fstat;
use nix::unistd::ftruncate;
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
pub const JOURNAL_NAME: &str = ".ln2_journal";
#[allow(clippy::manual_c_str_literals)]
const JOURNAL_NAME_CSTR: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b".ln2_journal\0") };
pub const JOURNAL_MAX_BYTES: u64 = 8 * 1024 * 1024;
pub const JOURNAL_MAX_OPS: u64 = 4096;

pub const INDEX_NAME: &str = ".ln2_index";
#[allow(clippy::manual_c_str_literals)]
const INDEX_NAME_CSTR: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b".ln2_index\0") };

type SharedBytes = Arc<[u8]>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JournalOp {
    Upsert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

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
    pending_ops: Vec<JournalOp>,
}

impl DirIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            raw_to_backend: HashMap::new(),
            dirty: false,
            pending_ops: Vec::new(),
        }
    }

    pub fn upsert(&mut self, backend_name: Vec<u8>, raw_name: Vec<u8>) {
        let backend_name: SharedBytes = Arc::from(backend_name);
        let raw_name: SharedBytes = Arc::from(raw_name);
        let entry = DirIndexEntry {
            backend_name: backend_name.clone(),
            raw_name: raw_name.clone(),
        };
        self.pending_ops.push(JournalOp::Upsert(
            entry.backend_name.as_ref().to_vec(),
            entry.raw_name.as_ref().to_vec(),
        ));
        if let Some(prev) = self.entries.insert(backend_name.clone(), entry) {
            self.raw_to_backend.remove(&prev.raw_name);
        }
        self.raw_to_backend.insert(raw_name, backend_name);
        self.dirty = true;
    }

    pub fn remove(&mut self, backend_name: &[u8]) -> Option<DirIndexEntry> {
        if self.entries.contains_key(backend_name) {
            self.pending_ops
                .push(JournalOp::Remove(backend_name.to_vec()));
        }
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

    pub fn take_pending_ops(&mut self) -> Vec<JournalOp> {
        std::mem::take(&mut self.pending_ops)
    }

    pub fn clear_pending_ops(&mut self) {
        self.pending_ops.clear();
    }

    pub fn has_pending_ops(&self) -> bool {
        !self.pending_ops.is_empty()
    }

    pub fn extend_pending_ops(&mut self, ops: Vec<JournalOp>) {
        if ops.is_empty() {
            return;
        }
        self.pending_ops.extend(ops);
        self.dirty = true;
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

    let size = fstat(fd.as_fd())
        .map(|st| st.st_size as u64)
        .unwrap_or(MAX_INDEX_BYTES as u64 + 1);
    if size > MAX_INDEX_BYTES as u64 {
        return Ok(None);
    }

    let file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    let mut limited = file.take((MAX_INDEX_BYTES + 1) as u64);
    limited.read_to_end(&mut buf).map_err(CoreError::from)?;
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
            let decoder = ZstdDecoder::new(&bytes[MAGIC_ZSTD.len()..]).ok()?;
            let mut decoded = Vec::new();
            let mut limited = decoder.take((MAX_INDEX_BYTES + 1) as u64);
            limited.read_to_end(&mut decoded).ok()?;
            if decoded.len() > MAX_INDEX_BYTES {
                return None;
            }
            // Only decode the decompressed payload as a plain index to avoid recursive
            // LN2Z nesting from untrusted/corrupt files.
            decode_plain_index(&decoded)
        }
        _ => None,
    }
}

fn read_journal_bytes(dir_fd: BorrowedFd<'_>) -> CoreResult<Option<(Vec<u8>, u64)>> {
    let fd = match nix::fcntl::openat(
        dir_fd,
        JOURNAL_NAME_CSTR,
        nix::fcntl::OFlag::O_RDONLY | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(err) => return Err(CoreError::from(err)),
    };
    let size = fstat(fd.as_fd()).map(|st| st.st_size as u64).unwrap_or(0);
    if size > JOURNAL_MAX_BYTES {
        return Ok(Some((Vec::new(), size)));
    }
    let file = std::fs::File::from(fd);
    let mut buf = Vec::new();
    let mut limited = file.take(JOURNAL_MAX_BYTES + 1);
    limited.read_to_end(&mut buf).map_err(CoreError::from)?;
    if buf.len() as u64 > JOURNAL_MAX_BYTES {
        return Ok(Some((Vec::new(), size)));
    }
    Ok(Some((buf, size)))
}

#[derive(Debug, Default)]
struct JournalDecodeResult {
    ops: Vec<JournalOp>,
    truncate_to: Option<usize>,
    corrupted: bool,
}

fn decode_journal_tolerant(bytes: &[u8]) -> JournalDecodeResult {
    let mut out = JournalDecodeResult::default();
    let mut offset = 0usize;

    let read_u32 = |buf: &[u8], off: &mut usize| -> Option<u32> {
        if buf.len() < *off + 4 {
            return None;
        }
        let val = u32::from_le_bytes(buf[*off..*off + 4].try_into().ok()?);
        *off += 4;
        Some(val)
    };

    while offset < bytes.len() {
        let record_start = offset;
        let Some(op_type) = bytes.get(offset).copied() else {
            out.truncate_to = Some(record_start);
            break;
        };
        offset += 1;

        let Some(key_len) = read_u32(bytes, &mut offset).map(|v| v as usize) else {
            out.truncate_to = Some(record_start);
            break;
        };
        if bytes.len() < offset + key_len {
            out.truncate_to = Some(record_start);
            break;
        }
        let key = bytes[offset..offset + key_len].to_vec();
        offset += key_len;

        let Some(val_len) = read_u32(bytes, &mut offset).map(|v| v as usize) else {
            out.truncate_to = Some(record_start);
            break;
        };

        match op_type {
            1 => {
                if bytes.len() < offset + val_len {
                    out.truncate_to = Some(record_start);
                    break;
                }
                let val = bytes[offset..offset + val_len].to_vec();
                offset += val_len;
                out.ops.push(JournalOp::Upsert(key, val));
            }
            2 => {
                if val_len != 0 {
                    out.truncate_to = Some(record_start);
                    out.corrupted = true;
                    break;
                }
                out.ops.push(JournalOp::Remove(key));
            }
            _ => {
                out.truncate_to = Some(record_start);
                out.corrupted = true;
                break;
            }
        }
    }

    out
}

fn truncate_journal(dir_fd: BorrowedFd<'_>, truncate_to: usize) -> CoreResult<()> {
    let fd = nix::fcntl::openat(
        dir_fd,
        JOURNAL_NAME_CSTR,
        nix::fcntl::OFlag::O_WRONLY | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )
    .map_err(CoreError::from)?;
    ftruncate(fd.as_fd(), truncate_to as i64).map_err(CoreError::from)
}

pub fn read_dir_index(dir_fd: BorrowedFd<'_>) -> CoreResult<Option<IndexLoadResult>> {
    let base = match read_index_bytes(dir_fd)? {
        None => None,
        Some(bytes) => decode_index_bytes(&bytes),
    };
    let has_base_index = base.is_some();
    let mut index = match base {
        Some(idx) => idx,
        None => DirIndex::new(),
    };

    let (journal, journal_size) = match read_journal_bytes(dir_fd)? {
        Some((buf, size)) => {
            if size > JOURNAL_MAX_BYTES {
                let _ = reset_journal(dir_fd);
                if has_base_index {
                    return Ok(Some(IndexLoadResult {
                        index,
                        journal_size: 0,
                        journal_ops_since_compact: 0,
                    }));
                }
                return Ok(None);
            }
            (buf, size)
        }
        None if has_base_index => {
            return Ok(Some(IndexLoadResult {
                index,
                journal_size: 0,
                journal_ops_since_compact: 0,
            }));
        }
        None => return Ok(None),
    };

    let decoded = decode_journal_tolerant(&journal);
    if decoded.corrupted {
        let _ = reset_journal(dir_fd);
    } else if let Some(truncate_to) = decoded.truncate_to {
        let _ = truncate_journal(dir_fd, truncate_to);
    }

    if !decoded.corrupted {
        for op in &decoded.ops {
            match op {
                JournalOp::Upsert(k, v) => index.upsert(k.clone(), v.clone()),
                JournalOp::Remove(k) => {
                    index.remove(k);
                }
            }
        }
    }
    index.clear_pending_ops();
    index.clear_dirty();

    Ok(Some(IndexLoadResult {
        index,
        journal_size,
        journal_ops_since_compact: decoded.ops.len() as u64,
    }))
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
        return Err(CoreError::NoSpace);
    };
    let tmp = core_begin_temp_file(dir_fd, INDEX_NAME_CSTR, "idx").map_err(CoreError::from)?;
    let mut written = 0;
    while written < data.len() {
        let n = retry_eintr(|| write(tmp.fd.as_fd(), &data[written..])).map_err(CoreError::from)?;
        written += n;
    }
    core_sync_and_commit(dir_fd, tmp, INDEX_NAME_CSTR).map_err(CoreError::from)
}

pub fn append_to_journal_file(
    file: &mut std::fs::File,
    ops: &[JournalOp],
    sync: bool,
) -> CoreResult<(u64, u64, u64)> {
    if ops.is_empty() {
        return Ok((0, 0, 0));
    }

    let estimated: usize = ops
        .iter()
        .map(|op| match op {
            JournalOp::Upsert(k, v) => 1 + 4 + k.len() + 4 + v.len(),
            JournalOp::Remove(k) => 1 + 4 + k.len() + 4,
        })
        .sum();
    let mut buf = Vec::with_capacity(estimated);

    for op in ops {
        match op {
            JournalOp::Upsert(k, v) => {
                buf.push(1u8);
                buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            JournalOp::Remove(k) => {
                buf.push(2u8);
                buf.extend_from_slice(&(k.len() as u32).to_le_bytes());
                buf.extend_from_slice(k);
                buf.extend_from_slice(&0u32.to_le_bytes());
            }
        }
    }

    file.write_all(&buf).map_err(CoreError::from)?;
    if sync {
        file.sync_all().map_err(CoreError::from)?;
    }

    let size_after = fstat(file.as_fd())
        .map(|st| st.st_size as u64)
        .unwrap_or(buf.len() as u64);
    Ok((buf.len() as u64, ops.len() as u64, size_after))
}

pub fn append_to_journal(
    dir_fd: BorrowedFd<'_>,
    ops: &[JournalOp],
    sync: bool,
) -> CoreResult<(u64, u64, u64)> {
    if ops.is_empty() {
        return Ok((0, 0, 0));
    }
    let fd = nix::fcntl::openat(
        dir_fd,
        JOURNAL_NAME_CSTR,
        nix::fcntl::OFlag::O_WRONLY
            | nix::fcntl::OFlag::O_CREAT
            | nix::fcntl::OFlag::O_APPEND
            | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::from_bits_truncate(0o600),
    )
    .map_err(CoreError::from)?;
    let mut file = std::fs::File::from(fd);
    append_to_journal_file(&mut file, ops, sync)
}

pub fn reset_journal(dir_fd: BorrowedFd<'_>) -> CoreResult<()> {
    match nix::unistd::unlinkat(
        dir_fd,
        JOURNAL_NAME_CSTR,
        nix::unistd::UnlinkatFlags::NoRemoveDir,
    ) {
        Ok(_) => Ok(()),
        Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(err) => Err(CoreError::from(err)),
    }
}

#[derive(Debug, Clone)]
pub struct IndexLoadResult {
    pub index: DirIndex,
    pub journal_size: u64,
    pub journal_ops_since_compact: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_index() -> DirIndex {
        let mut index = DirIndex::new();
        index.upsert(b"backend".to_vec(), b"raw".to_vec());
        index
    }

    #[test]
    fn decode_index_bytes_handles_single_compression_layer() {
        let raw = encode_index(&sample_index());
        let compressed = encode_index_compressed(&raw).unwrap();
        let decoded = decode_index_bytes(&compressed).expect("compressed index should decode");
        assert!(decoded.get(b"backend").is_some());
    }

    #[test]
    fn decode_index_bytes_rejects_nested_compression() {
        let raw = encode_index(&sample_index());
        let compressed = encode_index_compressed(&raw).unwrap();

        let nested_payload = {
            let mut encoder = ZstdEncoder::new(Vec::new(), 3).unwrap();
            encoder.write_all(&compressed).unwrap();
            encoder.finish().unwrap()
        };

        let mut nested = Vec::with_capacity(MAGIC_ZSTD.len() + nested_payload.len());
        nested.extend_from_slice(MAGIC_ZSTD);
        nested.extend_from_slice(&nested_payload);

        assert!(decode_index_bytes(&nested).is_none());
    }
}
