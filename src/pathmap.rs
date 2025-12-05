use crate::config::Config;
use crate::util::{errno_from_nix, retry_eintr, string_to_cstring};
use nix::fcntl::{AtFlags, OFlag, openat};
use nix::sys::stat::Mode;
use nix::unistd::dup;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::ops::Range;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::{Mutex, OnceLock};

pub const MAX_NAME_LENGTH: usize = 4096;
pub const BACKEND_HASH_OCTET_COUNT: usize = 16;
pub const BACKEND_HASH_STRING_LENGTH: usize = BACKEND_HASH_OCTET_COUNT * 2;
const MAX_COLLISION_SUFFIX: usize = 64;
const MAX_COLLISION_PROBE: usize = MAX_COLLISION_SUFFIX + 1;

#[derive(Debug)]
pub struct LnfsPath {
    pub dir_fd: OwnedFd,
    pub fname: String,
    pub raw_name: OsString,
}

#[derive(Debug)]
struct DirFdCache {
    entries: Mutex<DirFdCacheInner>,
    capacity: usize,
}

impl DirFdCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(DirFdCacheInner::new(capacity)),
            capacity,
        }
    }

    fn get(&self, key: &str) -> Option<OwnedFd> {
        let mut guard = self.entries.lock().ok()?;
        guard.get(key)
    }

    fn insert(&self, key: String, fd: OwnedFd) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.insert(key, fd, self.capacity);
        }
    }

    fn clear(&self) {
        if let Ok(mut guard) = self.entries.lock() {
            guard.clear();
        }
    }
}

#[derive(Debug)]
struct DirFdCacheInner {
    map: HashMap<String, OwnedFd>,
    order: VecDeque<String>,
}

impl DirFdCacheInner {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            order: VecDeque::with_capacity(capacity),
        }
    }

    fn get(&mut self, key: &str) -> Option<OwnedFd> {
        let fd = self.map.get(key)?;
        let dup_fd = dup(fd.as_fd()).ok();
        self.touch(key);
        dup_fd
    }

    fn touch(&mut self, key: &str) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
            self.order.push_back(key.to_owned());
        }
    }

    fn insert(&mut self, key: String, fd: OwnedFd, capacity: usize) {
        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), fd);
            self.touch(&key);
            return;
        }

        if self.map.len() >= capacity
            && let Some(old) = self.order.pop_front()
        {
            self.map.remove(&old);
        }

        self.order.push_back(key.clone());
        self.map.insert(key, fd);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.order.clear();
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
        if let Some(last) = parts.last()
            && last.start == buf.len()
        {
            let _ = parts.pop();
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

fn read_namefile_match(
    dir_fd: std::os::fd::BorrowedFd<'_>,
    encoded: &str,
    raw: &[u8],
    buf: &mut Vec<u8>,
) -> Result<Option<bool>, fuse3::Errno> {
    let mut fname = encoded.as_bytes().to_vec();
    fname.push(b'n');
    let fname = CString::new(fname).map_err(|_| fuse3::Errno::from(libc::EINVAL))?;
    let fd = match openat(
        dir_fd,
        fname.as_c_str(),
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(err) => return Err(errno_from_nix(err)),
    };

    if buf.len() < MAX_NAME_LENGTH {
        buf.resize(MAX_NAME_LENGTH, 0);
    }
    let read_len = match retry_eintr(|| nix::unistd::read(&fd, buf)) {
        Ok(len) => len,
        Err(err) => return Err(errno_from_nix(err)),
    };
    Ok(Some(read_len == raw.len() && &buf[..read_len] == raw))
}

fn candidate_encoded_names(base: &str) -> impl Iterator<Item = String> + '_ {
    (0..MAX_COLLISION_PROBE).map(move |idx| {
        if idx == 0 {
            base.to_owned()
        } else {
            format!("{}.{}", base, idx)
        }
    })
}

fn resolve_component(
    dir_fd: std::os::fd::BorrowedFd<'_>,
    raw: &[u8],
    want_dir: bool,
    allow_new: bool,
    collision_protect: bool,
) -> Result<(String, Option<OwnedFd>), fuse3::Errno> {
    let base = encode_name(raw);

    if !collision_protect {
        return Ok((base, None));
    }

    let mut name_buf = Vec::new();
    let mut first_free: Option<String> = None;

    for encoded in candidate_encoded_names(&base) {
        match read_namefile_match(dir_fd, &encoded, raw, &mut name_buf)? {
            Some(true) => {
                let mut flags = OFlag::O_PATH | OFlag::O_CLOEXEC;
                if want_dir {
                    flags |= OFlag::O_DIRECTORY;
                } else {
                    flags |= OFlag::O_NOFOLLOW;
                }
                let c_name =
                    CString::new(encoded.clone()).map_err(|_| fuse3::Errno::from(libc::EINVAL))?;
                let fd = openat(dir_fd, c_name.as_c_str(), flags, Mode::empty())
                    .map_err(errno_from_nix)?;
                return Ok((encoded, Some(fd)));
            }
            Some(false) => {
                // namefile exists but does not match, try next suffix
            }
            None => {
                // Missing namefile: consider as free slot if data file also absent
                if first_free.is_none() {
                    let c_name = CString::new(encoded.as_bytes().to_vec())
                        .map_err(|_| fuse3::Errno::from(libc::EINVAL))?;
                    let data_exists = match nix::sys::stat::fstatat(
                        dir_fd,
                        c_name.as_c_str(),
                        AtFlags::AT_SYMLINK_NOFOLLOW,
                    ) {
                        Ok(_) => true,
                        Err(nix::errno::Errno::ENOENT) => false,
                        Err(err) => return Err(errno_from_nix(err)),
                    };
                    if !data_exists {
                        first_free = Some(encoded.clone());
                    }
                }
            }
        }
    }

    if allow_new {
        if let Some(encoded) = first_free {
            return Ok((encoded, None));
        }
        return Err(fuse3::Errno::from(libc::ENOSPC));
    }

    Err(fuse3::Errno::from(libc::ENOENT))
}

fn open_path_with_cache(
    config: &Config,
    path: &OsStr,
    use_cache: bool,
    collision_protect: bool,
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

    let mut prefix = String::from(config.cache_namespace());

    if parts.len() > 2 {
        for seg in parts.iter().skip(1).take(parts.len() - 2) {
            let (encoded, opened) =
                resolve_component(dir_fd.as_fd(), seg, true, false, collision_protect)?;

            prefix.push('/');
            prefix.push_str(&encoded);

            if use_cache && let Some(fd) = dir_fd_cache().get(&prefix) {
                dir_fd = fd;
                continue;
            }

            let c_name = string_to_cstring(&encoded)?;
            let next_fd = if let Some(fd) = opened {
                fd
            } else {
                openat(
                    dir_fd.as_fd(),
                    c_name.as_c_str(),
                    OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                    Mode::empty(),
                )
                .map_err(errno_from_nix)?
            };

            if use_cache && let Ok(dup_fd) = dup(next_fd.as_fd()) {
                dir_fd_cache().insert(prefix.clone(), dup_fd);
            }
            drop(dir_fd);
            dir_fd = next_fd;
        }
    }

    let raw_name = OsString::from_vec(last_part.to_vec());
    let (fname, _) = resolve_component(dir_fd.as_fd(), last_part, false, true, collision_protect)?;

    Ok(LnfsPath {
        dir_fd,
        fname,
        raw_name,
    })
}

pub fn open_path(config: &Config, path: &OsStr) -> Result<LnfsPath, fuse3::Errno> {
    match open_path_with_cache(config, path, true, config.collision_protect()) {
        Ok(v) => Ok(v),
        Err(err)
            if (err == fuse3::Errno::from(libc::ENOENT)
                || err == fuse3::Errno::from(libc::ENOTDIR)) =>
        {
            dir_fd_cache().clear();
            open_path_with_cache(config, path, false, config.collision_protect())
        }
        Err(err) => Err(err),
    }
}

pub fn clear_dir_fd_cache() {
    dir_fd_cache().clear();
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
