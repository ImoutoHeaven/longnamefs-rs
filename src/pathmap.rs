use crate::config::Config;
use crate::util::{errno_from_nix, string_to_cstring};
use libc;
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use nix::unistd::dup;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::ops::Range;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::{Mutex, OnceLock};

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
struct DirFdCache {
    entries: Mutex<HashMap<String, OwnedFd>>,
    capacity: usize,
}

impl DirFdCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    fn get(&self, key: &str) -> Option<OwnedFd> {
        let guard = self.entries.lock().ok()?;
        let fd = guard.get(key)?;
        dup(fd.as_fd()).ok()
    }

    fn insert(&self, key: String, fd: OwnedFd) {
        let mut guard = match self.entries.lock() {
            Ok(g) => g,
            Err(_) => return,
        };

        if guard.len() >= self.capacity {
            if let Some(k) = guard.keys().next().cloned() {
                guard.remove(&k);
            }
        }

        guard.insert(key, fd);
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.clear();
        }
    }
}

const DIR_FD_CACHE_CAPACITY: usize = 64;

fn dir_fd_cache() -> &'static DirFdCache {
    static CACHE: OnceLock<DirFdCache> = OnceLock::new();
    CACHE.get_or_init(|| DirFdCache::new(DIR_FD_CACHE_CAPACITY))
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
        self.parts.last().map(|range| &self.buf[range.clone()])
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

fn open_path_with_cache(
    config: &Config,
    path: &OsStr,
    use_cache: bool,
) -> Result<LnfsPath, fuse3::Errno> {
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

    let mut prefix = String::new();

    if parts.len() > 2 {
        for seg in parts.iter().skip(1).take(parts.len() - 2) {
            let encoded = encode_name(seg);

            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(&encoded);

            if use_cache {
                if let Some(fd) = dir_fd_cache().get(&prefix) {
                    dir_fd = fd;
                    continue;
                }
            }

            let c_name = string_to_cstring(&encoded)?;
            let next_fd = openat(
                dir_fd.as_fd(),
                c_name.as_c_str(),
                OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;

            if use_cache {
                if let Ok(dup_fd) = dup(next_fd.as_fd()) {
                    dir_fd_cache().insert(prefix.clone(), dup_fd);
                }
            }
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

pub fn open_path(config: &Config, path: &OsStr) -> Result<LnfsPath, fuse3::Errno> {
    match open_path_with_cache(config, path, true) {
        Ok(v) => Ok(v),
        Err(err)
            if (err == fuse3::Errno::from(libc::ENOENT)
                || err == fuse3::Errno::from(libc::ENOTDIR)) =>
        {
            dir_fd_cache().clear();
            open_path_with_cache(config, path, false)
        }
        Err(err) => Err(err),
    }
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
