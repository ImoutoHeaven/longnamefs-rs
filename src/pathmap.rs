use crate::config::Config;
use crate::util::{errno_from_nix, string_to_cstring};
use libc;
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use sha2::{Digest, Sha256};
use std::ffi::{OsStr, OsString};
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::ops::Range;

pub const MAX_NAME_LENGTH: usize = 4096;
pub const BACKEND_HASH_OCTET_COUNT: usize = 16;
pub const BACKEND_HASH_STRING_LENGTH: usize = BACKEND_HASH_OCTET_COUNT * 2;

#[derive(Debug)]
pub struct LnfsPath {
    pub dir_fd: OwnedFd,
    pub fname: String,
    pub raw_name: OsString,
}

#[derive(Debug)]
struct PathSegments {
    buf: Vec<u8>,
    parts: Vec<Range<usize>>,
}

impl PathSegments {
    fn new(path: &OsStr) -> Self {
        let buf = path.as_bytes().to_vec();
        let mut parts = Vec::new();
        let mut start = 0usize;
        for (idx, b) in buf.iter().enumerate() {
            if *b == b'/' {
                parts.push(start..idx);
                start = idx + 1;
            }
        }
        parts.push(start..buf.len());
        if let Some(last) = parts.last() {
            if last.start == buf.len() {
                let _ = parts.pop();
            }
        }

        Self { buf, parts }
    }

    fn len(&self) -> usize {
        self.parts.len()
    }

    fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    fn last(&self) -> Option<&[u8]> {
        self.parts
            .last()
            .map(|range| &self.buf[range.clone()])
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.parts.iter().map(|range| &self.buf[range.clone()])
    }
}

pub fn encode_name(raw: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(raw);
    let digest = hasher.finalize();
    hex::encode(&digest[..BACKEND_HASH_OCTET_COUNT])
}

pub fn open_path(config: &Config, path: &OsStr) -> Result<LnfsPath, fuse3::Errno> {
    if path == OsStr::new("/") {
        return Err(fuse3::Errno::from(libc::EFAULT));
    }

    let parts = PathSegments::new(path);
    if parts.is_empty() {
        return Err(fuse3::Errno::new_not_exist());
    }

    let last_part = parts.last().unwrap();
    if last_part.len() >= MAX_NAME_LENGTH {
        return Err(fuse3::Errno::from(libc::ENAMETOOLONG));
    }

    let mut dir_fd = openat(
        config.backend_fd(),
        ".",
        OFlag::O_PATH | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_from_nix)?;

    if parts.len() > 2 {
        for seg in parts.iter().skip(1).take(parts.len() - 2) {
            let encoded = encode_name(seg);
            let c_name = string_to_cstring(&encoded)?;
            let next_fd = openat(
                dir_fd.as_fd(),
                c_name.as_c_str(),
                OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            drop(dir_fd);
            dir_fd = next_fd;
        }
    }

    let fname = encode_name(last_part);
    let raw_name = OsString::from_vec(last_part.to_vec());

    Ok(LnfsPath {
        dir_fd,
        fname,
        raw_name,
    })
}

pub fn open_paths(
    config: &Config,
    one: &OsStr,
    two: &OsStr,
) -> Result<(LnfsPath, LnfsPath), fuse3::Errno> {
    let p1 = open_path(config, one)?;
    match open_path(config, two) {
        Ok(p2) => Ok((p1, p2)),
        Err(err) => Err(err),
    }
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
