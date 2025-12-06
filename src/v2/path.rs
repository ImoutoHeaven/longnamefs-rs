#![allow(dead_code)]

use sha2::{Digest, Sha256};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

pub const INTERNAL_PREFIX: &str = ".__ln2_";
pub const MAX_SEGMENT_ON_DISK: usize = 255;
pub const HASH_BYTES: usize = 16; // SHA-256 前 16 字节 → 32 hex
pub const MAX_COLLISION_SUFFIX: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Short,
    Long,
}

pub fn is_reserved_prefix(raw: &[u8]) -> bool {
    raw.starts_with(INTERNAL_PREFIX.as_bytes())
}

pub fn classify_segment(raw: &[u8], max_name_len: usize) -> Result<SegmentKind, fuse3::Errno> {
    if raw.len() > max_name_len {
        return Err(fuse3::Errno::from(libc::ENAMETOOLONG));
    }
    if is_reserved_prefix(raw) {
        return Err(fuse3::Errno::from(libc::EINVAL));
    }
    if raw.len() <= MAX_SEGMENT_ON_DISK {
        return Ok(SegmentKind::Short);
    }
    Ok(SegmentKind::Long)
}

pub fn encode_long_name(raw: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw);
    let digest = hasher.finalize();
    hex::encode(&digest[..HASH_BYTES])
}

pub fn backend_basename_from_hash(hash_hex: &str, suffix: Option<u32>) -> String {
    match suffix {
        None => format!("{INTERNAL_PREFIX}{hash_hex}"),
        Some(k) => format!("{INTERNAL_PREFIX}{hash_hex}~{k}"),
    }
}

pub fn normalize_osstr(value: &OsStr) -> Vec<u8> {
    value.as_bytes().to_vec()
}

pub fn make_child_path(parent: &OsStr, name: &OsStr) -> OsString {
    if parent == OsStr::new("/") {
        let mut composed = OsString::from("/");
        composed.push(name);
        composed
    } else {
        let mut composed = OsString::from(parent);
        composed.push(OsStr::new("/"));
        composed.push(name);
        composed
    }
}
