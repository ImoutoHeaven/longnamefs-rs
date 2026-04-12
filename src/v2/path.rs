#![allow(dead_code)]

use crate::v2::error::{CoreError, CoreResult};
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::OsStrExt;

pub const INTERNAL_PREFIX: &str = ".__ln2_";
pub const MAX_SEGMENT_ON_DISK: usize = 255;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentKind {
    Short,
    Long,
}

pub fn is_reserved_prefix(raw: &[u8]) -> bool {
    raw.starts_with(INTERNAL_PREFIX.as_bytes())
}

pub fn is_stable_long_object_backend_name(raw: &[u8]) -> bool {
    crate::v2::object_id::is_stable_long_object_name(raw)
}

pub fn classify_committed_segment(raw: &[u8]) -> CoreResult<SegmentKind> {
    if is_reserved_prefix(raw) {
        return Err(CoreError::ReservedPrefix);
    }
    if raw.len() <= MAX_SEGMENT_ON_DISK {
        return Ok(SegmentKind::Short);
    }
    Ok(SegmentKind::Long)
}

pub fn classify_segment(raw: &[u8], max_name_len: usize) -> CoreResult<SegmentKind> {
    if raw.len() > max_name_len {
        return Err(CoreError::NameTooLong);
    }
    classify_committed_segment(raw)
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
