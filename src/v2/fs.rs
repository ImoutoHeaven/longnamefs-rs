use crate::config::Config;
use crate::util::{
    access_mask_from_bits, errno_from_nix, file_attr_from_stat, file_type_from_mode,
    oflag_from_bits, retry_eintr, string_to_cstring,
};
use crate::v2::index::{DirIndex, INDEX_NAME, read_dir_index, write_dir_index};
use crate::v2::path::{
    INTERNAL_PREFIX, MAX_COLLISION_SUFFIX, SegmentKind, backend_basename_from_hash,
    classify_segment, encode_long_name, is_reserved_prefix, normalize_osstr,
};
use bytes::Bytes;
use fuse3::notify::Notify;
use fuse3::path::prelude::*;
use fuse3::path::reply::{DirectoryEntryPlus, ReplyPoll, ReplyXAttr};
use fuse3::{FileType, SetAttr};
use nix::dir::Dir;
use nix::fcntl::{AtFlags, OFlag, readlinkat, renameat};
use nix::sys::stat::{
    FchmodatFlags, Mode, UtimensatFlags, fchmodat, fstat, fstatat, mkdirat, mknodat, utimensat,
};
use nix::sys::statvfs::fstatvfs;
use nix::sys::time::TimeSpec;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{
    Gid, LinkatFlags, Uid, UnlinkatFlags, faccessat, fchownat, fdatasync, fsync, linkat, symlinkat,
    unlinkat,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::io;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::mpsc;
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant};

const ATTR_TTL: Duration = Duration::from_secs(1);
const RAWNAME_XATTR: &str = "user.ln2.rawname";
const JOURNAL_NAME: &str = ".ln2_journal";
const PARALLEL_REBUILD_THRESHOLD: usize = 64;
const PARALLEL_REBUILD_WORKERS: usize = 4;

fn is_internal_meta(raw: &[u8]) -> bool {
    raw == INDEX_NAME.as_bytes() || raw == JOURNAL_NAME.as_bytes()
}

#[derive(Copy, Clone, Debug)]
pub enum IndexSync {
    Always,
    Batch {
        max_pending: usize,
        max_age: Duration,
    },
    Off,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct DirCacheKey {
    dev: u64,
    ino: u64,
}

#[derive(Debug)]
struct DirCacheEntry {
    expires_at: Instant,
    entries: Arc<Vec<DirEntryInfo>>,
}

const DIR_CACHE_MAX_DIRS: usize = 1024;

#[derive(Debug)]
struct DirCache {
    ttl: Duration,
    enabled: bool,
    entries: RwLock<HashMap<DirCacheKey, DirCacheEntry>>,
}

impl DirCache {
    fn new(ttl: Option<Duration>) -> Self {
        let (enabled, ttl) = match ttl {
            Some(t) => (true, t),
            None => (false, Duration::ZERO),
        };
        Self {
            ttl,
            enabled,
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, key: DirCacheKey) -> Option<Arc<Vec<DirEntryInfo>>> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        {
            let guard = self.entries.read().ok()?;
            if let Some(entry) = guard.get(&key)
                && entry.expires_at > now
            {
                return Some(entry.entries.clone());
            }
        }

        let mut guard = self.entries.write().ok()?;
        if let Some(entry) = guard.get(&key)
            && entry.expires_at > now
        {
            return Some(entry.entries.clone());
        }
        guard.remove(&key);
        None
    }

    fn insert(&self, key: DirCacheKey, items: Vec<DirEntryInfo>) -> Arc<Vec<DirEntryInfo>> {
        if !self.enabled {
            return Arc::new(items);
        }
        let expires_at = Instant::now() + self.ttl;
        let entries = Arc::new(items);
        let mut guard = self.entries.write().unwrap();
        if guard.len() >= DIR_CACHE_MAX_DIRS {
            guard.clear();
        }
        guard.insert(
            key,
            DirCacheEntry {
                expires_at,
                entries: entries.clone(),
            },
        );
        entries
    }

    fn invalidate(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        if let Ok(mut guard) = self.entries.write() {
            guard.remove(&key);
        }
    }
}

fn dir_cache_key(fd: BorrowedFd<'_>) -> Option<DirCacheKey> {
    fstat(fd).ok().map(|stat| DirCacheKey {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

#[derive(Debug, Clone)]
struct DirEntryInfo {
    name: OsString,
    kind: FileType,
    attr: Option<fuse3::path::reply::FileAttr>,
    backend_name: Vec<u8>,
}

fn map_dirent_type(entry: &nix::dir::Entry) -> Option<FileType> {
    entry.file_type().map(|dt| match dt {
        nix::dir::Type::Directory => FileType::Directory,
        nix::dir::Type::Symlink => FileType::Symlink,
        nix::dir::Type::File => FileType::RegularFile,
        nix::dir::Type::BlockDevice => FileType::BlockDevice,
        nix::dir::Type::CharacterDevice => FileType::CharDevice,
        nix::dir::Type::Fifo => FileType::NamedPipe,
        nix::dir::Type::Socket => FileType::Socket,
    })
}

#[derive(Debug)]
struct IndexState {
    index: DirIndex,
    pending: usize,
    last_flush: Instant,
}

#[derive(Debug)]
struct IndexCacheEntry {
    value: Arc<RwLock<IndexState>>,
}

const INDEX_CACHE_MAX_DIRS: usize = 1024;

#[derive(Debug, Default)]
struct IndexCache {
    entries: Mutex<HashMap<DirCacheKey, IndexCacheEntry>>,
    lru: Mutex<VecDeque<DirCacheKey>>,
}

impl IndexCache {
    fn new() -> Self {
        Self::default()
    }

    fn touch_lru(&self, key: DirCacheKey) {
        if let Ok(mut lru) = self.lru.lock() {
            if let Some(pos) = lru.iter().position(|k| *k == key) {
                lru.remove(pos);
            }
            lru.push_back(key);
            while lru.len() > INDEX_CACHE_MAX_DIRS {
                lru.pop_front();
            }
        }
    }

    fn evict_if_needed(&self) {
        let mut lru = match self.lru.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        let mut entries = match self.entries.lock() {
            Ok(v) => v,
            Err(_) => return,
        };
        while entries.len() > INDEX_CACHE_MAX_DIRS {
            if let Some(old) = lru.pop_front() {
                entries.remove(&old);
            } else {
                break;
            }
        }
    }

    fn get_or_load(&self, dir_fd: BorrowedFd<'_>) -> Result<Arc<RwLock<IndexState>>, fuse3::Errno> {
        let key = dir_cache_key(dir_fd).ok_or_else(fuse3::Errno::new_not_exist)?;
        if let Ok(entries) = self.entries.lock()
            && let Some(entry) = entries.get(&key)
        {
            let value = entry.value.clone();
            drop(entries);
            self.touch_lru(key);
            return Ok(value);
        }

        let index = match read_dir_index(dir_fd)? {
            Some(idx) => idx,
            None => rebuild_dir_index_from_backend(dir_fd)?,
        };
        let state = Arc::new(RwLock::new(IndexState {
            index,
            pending: 0,
            last_flush: Instant::now(),
        }));

        if let Ok(mut entries) = self.entries.lock() {
            let existing = entries.entry(key).or_insert_with(|| IndexCacheEntry {
                value: state.clone(),
            });
            let value = existing.value.clone();
            drop(entries);
            self.touch_lru(key);
            self.evict_if_needed();
            return Ok(value);
        }

        Ok(state)
    }
}

#[derive(Debug)]
struct DirState {
    index: Arc<RwLock<IndexState>>,
    attr_cache: HashMap<Vec<u8>, fuse3::path::reply::FileAttr>,
}

#[derive(Debug)]
struct DirHandle {
    fd: OwnedFd,
    state: RwLock<DirState>,
    cache_key: Option<DirCacheKey>,
}

impl DirHandle {
    fn new(fd: OwnedFd, state: DirState) -> Self {
        let cache_key = dir_cache_key(fd.as_fd());
        Self {
            fd,
            state: RwLock::new(state),
            cache_key,
        }
    }

    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    fn clear_cached_attrs(&self) {
        if let Ok(mut state) = self.state.write() {
            state.attr_cache.clear();
        }
    }
}

#[derive(Debug, Clone)]
enum Handle {
    File(Arc<OwnedFd>),
    Dir(Arc<DirHandle>),
}

impl Handle {
    fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Handle::File(fd) => fd.as_fd(),
            Handle::Dir(dir) => dir.as_fd(),
        }
    }
}

#[derive(Debug, Default)]
struct V2HandleTable {
    next_id: AtomicU64,
    entries: RwLock<HashMap<u64, Handle>>,
}

impl V2HandleTable {
    fn new() -> Self {
        Self::default()
    }

    fn insert_file(&self, fd: OwnedFd) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries
            .write()
            .unwrap()
            .insert(id, Handle::File(Arc::new(fd)));
        id
    }

    fn insert_dir(&self, handle: DirHandle) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries
            .write()
            .unwrap()
            .insert(id, Handle::Dir(Arc::new(handle)));
        id
    }

    fn get_file(&self, id: u64) -> Option<Arc<OwnedFd>> {
        let guard = self.entries.read().unwrap();
        match guard.get(&id)? {
            Handle::File(fd) => Some(fd.clone()),
            _ => None,
        }
    }

    fn get_dir(&self, id: u64) -> Option<Arc<DirHandle>> {
        let guard = self.entries.read().unwrap();
        match guard.get(&id)? {
            Handle::Dir(dir) => Some(dir.clone()),
            _ => None,
        }
    }

    fn remove(&self, id: u64) -> Option<Handle> {
        self.entries.write().unwrap().remove(&id)
    }

    fn clear_dir_attr_cache(&self, key: DirCacheKey) {
        if let Ok(guard) = self.entries.read() {
            for handle in guard.values() {
                if let Handle::Dir(dir) = handle
                    && dir.cache_key == Some(key)
                {
                    dir.clear_cached_attrs();
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum BackendName {
    Short(Vec<u8>),
    Internal(String),
}

impl BackendName {
    fn as_cstring(&self) -> Result<CString, fuse3::Errno> {
        match self {
            BackendName::Short(raw) => {
                CString::new(raw.clone()).map_err(|_| fuse3::Errno::from(libc::EINVAL))
            }
            BackendName::Internal(name) => string_to_cstring(name),
        }
    }

    fn display_bytes(&self) -> Vec<u8> {
        match self {
            BackendName::Short(raw) => raw.clone(),
            BackendName::Internal(name) => name.as_bytes().to_vec(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, BackendName::Internal(_))
    }
}

#[derive(Debug)]
struct Ln2Path {
    dir_fd: OwnedFd,
    backend_name: BackendName,
    raw_name: Vec<u8>,
    kind: SegmentKind,
}

#[derive(Debug)]
struct ParentCtx {
    dir_fd: OwnedFd,
    state: DirState,
}

fn cstring_from_bytes(bytes: &[u8]) -> Result<CString, fuse3::Errno> {
    CString::new(bytes.to_vec()).map_err(|_| fuse3::Errno::from(libc::EINVAL))
}

fn set_internal_rawname(fd: BorrowedFd<'_>, raw: &[u8]) -> Result<(), fuse3::Errno> {
    let name = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let res = unsafe {
        libc::fsetxattr(
            fd.as_raw_fd(),
            name.as_ptr(),
            raw.as_ptr() as *const libc::c_void,
            raw.len(),
            0,
        )
    };
    if res < 0 {
        let err = io::Error::last_os_error();
        let raw_err = err.raw_os_error().unwrap_or(libc::EIO);
        if raw_err == libc::ENOSPC || raw_err == libc::E2BIG {
            return Err(fuse3::Errno::from(libc::ENAMETOOLONG));
        }
        return Err(fuse3::Errno::from(raw_err));
    }
    Ok(())
}

fn get_internal_rawname(fd: BorrowedFd<'_>) -> Result<Vec<u8>, fuse3::Errno> {
    let name = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let res = unsafe { libc::fgetxattr(fd.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if res < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut buf = vec![0u8; res as usize];
    let res = unsafe {
        libc::fgetxattr(
            fd.as_raw_fd(),
            name.as_ptr(),
            buf.as_mut_ptr() as *mut libc::c_void,
            buf.len(),
        )
    };
    if res < 0 {
        return Err(io::Error::last_os_error().into());
    }
    buf.truncate(res as usize);
    Ok(buf)
}

fn verify_backend_supports_xattr(dir_fd: BorrowedFd<'_>) -> Result<(), fuse3::Errno> {
    let fname = string_to_cstring(".ln2_xattr_check.tmp")?;
    let fd = nix::fcntl::openat(
        dir_fd,
        fname.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(errno_from_nix)?;
    let res = set_internal_rawname(fd.as_fd(), b"probe");
    let _ = unlinkat(dir_fd, fname.as_c_str(), UnlinkatFlags::NoRemoveDir);
    res
}

fn rebuild_dir_index_from_backend(dir_fd: BorrowedFd<'_>) -> Result<DirIndex, fuse3::Errno> {
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_from_nix)?;

    let mut internal = Vec::new();
    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name_bytes = entry.file_name().to_bytes().to_vec();
        if name_bytes.is_empty() {
            continue;
        }
        if name_bytes.starts_with(INDEX_NAME.as_bytes())
            || name_bytes.starts_with(JOURNAL_NAME.as_bytes())
        {
            continue;
        }
        if name_bytes.starts_with(INTERNAL_PREFIX.as_bytes()) {
            internal.push(name_bytes);
        }
    }

    let mut index = DirIndex::new();
    if internal.len() <= PARALLEL_REBUILD_THRESHOLD {
        for name_bytes in internal {
            let c_name = match cstring_from_bytes(&name_bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let fd = match nix::fcntl::openat(
                dir_fd,
                c_name.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            let raw_name = match get_internal_rawname(fd.as_fd()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let backend_name = match std::str::from_utf8(&name_bytes) {
                Ok(s) => s.to_owned(),
                Err(_) => continue,
            };
            index.upsert(backend_name, raw_name);
        }
        return Ok(index);
    }

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let (res_tx, res_rx) = mpsc::channel::<(String, Vec<u8>)>();
    let workers = PARALLEL_REBUILD_WORKERS.max(1);

    thread::scope(|scope| {
        let shared_rx = Arc::new(Mutex::new(rx));
        for _ in 0..workers {
            let rx = Arc::clone(&shared_rx);
            let res_tx = res_tx.clone();
            let dup_fd = match nix::unistd::dup(dir_fd) {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            scope.spawn(move || {
                loop {
                    let name_bytes = {
                        let guard = rx.lock().unwrap();
                        match guard.recv() {
                            Ok(v) => v,
                            Err(_) => break,
                        }
                    };
                    let c_name = match cstring_from_bytes(&name_bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let fd = match nix::fcntl::openat(
                        dup_fd.as_fd(),
                        c_name.as_c_str(),
                        OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                        Mode::empty(),
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };
                    let raw_name = match get_internal_rawname(fd.as_fd()) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let backend_name = match std::str::from_utf8(&name_bytes) {
                        Ok(s) => s.to_owned(),
                        Err(_) => continue,
                    };
                    let _ = res_tx.send((backend_name, raw_name));
                }
            });
        }

        for name in internal {
            let _ = tx.send(name);
        }
        drop(tx);
    });
    drop(res_tx);

    for (backend_name, raw_name) in res_rx {
        index.upsert(backend_name, raw_name);
    }
    Ok(index)
}

fn load_dir_state(cache: &IndexCache, dir_fd: BorrowedFd<'_>) -> Result<DirState, fuse3::Errno> {
    let index = cache.get_or_load(dir_fd)?;
    Ok(DirState {
        index,
        attr_cache: HashMap::new(),
    })
}

fn mark_dirty(state: &mut DirState) {
    state.attr_cache.clear();
    if let Ok(mut guard) = state.index.write() {
        guard.index.mark_dirty();
        guard.pending = guard.pending.saturating_add(1);
    }
}

fn maybe_flush_index(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    strategy: IndexSync,
) -> Result<(), fuse3::Errno> {
    let mut guard = state.index.write().unwrap();
    let should_flush = match strategy {
        IndexSync::Always => guard.index.is_dirty(),
        IndexSync::Batch {
            max_pending,
            max_age,
        } => {
            guard.index.is_dirty()
                && (guard.pending >= max_pending || guard.last_flush.elapsed() >= max_age)
        }
        IndexSync::Off => false,
    };

    if should_flush {
        write_dir_index(dir_fd, &guard.index)?;
        guard.index.clear_dirty();
        guard.pending = 0;
        guard.last_flush = Instant::now();
    }
    Ok(())
}

fn list_logical_entries(
    handle: &DirHandle,
    max_name_len: usize,
    index_sync: IndexSync,
) -> Result<Vec<DirEntryInfo>, fuse3::Errno> {
    let dir_fd = handle.as_fd();
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_from_nix)?;

    let mut state = handle.state.write().unwrap();
    let mut entries = Vec::new();
    let mut seen_backend = HashSet::new();

    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name_bytes = entry.file_name().to_bytes().to_vec();
        if name_bytes.is_empty() {
            continue;
        }
        if name_bytes.starts_with(INDEX_NAME.as_bytes())
            || name_bytes.starts_with(JOURNAL_NAME.as_bytes())
        {
            continue;
        }

        let mut kind = map_dirent_type(&entry);
        let mut attr = state.attr_cache.get(&name_bytes).cloned();
        if kind.is_none()
            && let Some(cached) = attr.as_ref()
        {
            kind = Some(cached.kind);
        }

        if name_bytes.starts_with(INTERNAL_PREFIX.as_bytes()) {
            let backend_name = match std::str::from_utf8(&name_bytes) {
                Ok(s) => s.to_owned(),
                Err(_) => continue,
            };
            let has_entry = {
                let guard = state.index.read().unwrap();
                guard.index.contains_key(&backend_name)
            };
            if !has_entry {
                // 孤儿条目，尝试修复索引
                let c_name = match cstring_from_bytes(&name_bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let fd = match nix::fcntl::openat(
                    dir_fd,
                    c_name.as_c_str(),
                    OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                    Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };
                if let Ok(raw_name) = get_internal_rawname(fd.as_fd())
                    && raw_name.len() <= max_name_len
                {
                    {
                        let mut guard = state.index.write().unwrap();
                        guard.index.upsert(backend_name.clone(), raw_name);
                        guard.pending = guard.pending.saturating_add(1);
                    }
                    state.attr_cache.clear();
                }
            }
            let raw_name = {
                let guard = state.index.read().unwrap();
                match guard.index.get(&backend_name) {
                    Some(entry) => entry.raw_name.clone(),
                    None => continue,
                }
            };
            if attr.is_none() || kind.is_none() {
                let c_name = match cstring_from_bytes(&name_bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let stat = match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
                    Ok(st) => st,
                    Err(_) => continue,
                };
                let file_attr = file_attr_from_stat(&stat);
                state.attr_cache.insert(name_bytes.clone(), file_attr);
                if kind.is_none() {
                    kind = Some(file_type_from_mode(stat.st_mode));
                }
                attr = Some(file_attr);
            }
            entries.push(DirEntryInfo {
                name: OsString::from_vec(raw_name),
                kind: kind.unwrap_or(FileType::RegularFile),
                attr,
                backend_name: name_bytes.clone(),
            });
            seen_backend.insert(name_bytes);
        } else {
            if attr.is_none() || kind.is_none() {
                let c_name = match cstring_from_bytes(&name_bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let stat = match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
                    Ok(st) => st,
                    Err(_) => continue,
                };
                let file_attr = file_attr_from_stat(&stat);
                state.attr_cache.insert(name_bytes.clone(), file_attr);
                if kind.is_none() {
                    kind = Some(file_type_from_mode(stat.st_mode));
                }
                attr = Some(file_attr);
            }
            entries.push(DirEntryInfo {
                name: OsString::from_vec(name_bytes.clone()),
                kind: kind.unwrap_or(FileType::RegularFile),
                attr,
                backend_name: name_bytes.clone(),
            });
            seen_backend.insert(name_bytes);
        }
    }

    state.attr_cache.retain(|k, _| seen_backend.contains(k));
    maybe_flush_index(dir_fd, &mut state, index_sync)?;
    Ok(entries)
}

fn map_long_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
) -> Result<String, fuse3::Errno> {
    if let Some(entry) = {
        let guard = state.index.read().unwrap();
        guard.index.backend_for_raw(raw).cloned()
    } {
        let c_name = string_to_cstring(&entry)?;
        match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => return Ok(entry),
            Err(nix::errno::Errno::ENOENT) => {
                {
                    let mut guard = state.index.write().unwrap();
                    guard.index.remove(&entry);
                    guard.pending = guard.pending.saturating_add(1);
                }
                state.attr_cache.clear();
            }
            Err(err) => return Err(errno_from_nix(err)),
        }
    }

    let hash = encode_long_name(raw);
    for suffix in 0..=MAX_COLLISION_SUFFIX {
        let candidate = backend_basename_from_hash(&hash, (suffix != 0).then_some(suffix));
        let c_name = string_to_cstring(&candidate)?;
        match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => {
                let fd = nix::fcntl::openat(
                    dir_fd,
                    c_name.as_c_str(),
                    OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                    Mode::empty(),
                )
                .map_err(errno_from_nix)?;
                if let Ok(raw_name) = get_internal_rawname(fd.as_fd())
                    && raw_name == raw
                {
                    {
                        let mut guard = state.index.write().unwrap();
                        guard.index.upsert(candidate.clone(), raw_name);
                        guard.pending = guard.pending.saturating_add(1);
                    }
                    state.attr_cache.clear();
                    return Ok(candidate);
                }
            }
            Err(nix::errno::Errno::ENOENT) => continue,
            Err(err) => return Err(errno_from_nix(err)),
        }
    }

    Err(fuse3::Errno::from(libc::ENOENT))
}

fn map_segment_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
    max_name_len: usize,
) -> Result<(BackendName, SegmentKind), fuse3::Errno> {
    if is_internal_meta(raw) {
        return Err(fuse3::Errno::from(libc::EPERM));
    }
    if is_reserved_prefix(raw) {
        return Err(fuse3::Errno::from(libc::EPERM));
    }
    let kind = classify_segment(raw, max_name_len)?;
    match kind {
        SegmentKind::Short => Ok((BackendName::Short(raw.to_vec()), kind)),
        SegmentKind::Long => {
            let backend = map_long_for_lookup(dir_fd, state, raw)?;
            Ok((BackendName::Internal(backend), kind))
        }
    }
}

fn map_segment_for_create(
    state: &DirState,
    raw: &[u8],
    max_name_len: usize,
) -> Result<(BackendName, SegmentKind), fuse3::Errno> {
    if is_internal_meta(raw) {
        return Err(fuse3::Errno::from(libc::EPERM));
    }
    let kind = classify_segment(raw, max_name_len)?;
    match kind {
        SegmentKind::Short => Ok((BackendName::Short(raw.to_vec()), kind)),
        SegmentKind::Long => {
            let hash = encode_long_name(raw);
            let base = backend_basename_from_hash(&hash, None);
            if let Some(entry_raw) = {
                let guard = state.index.read().unwrap();
                guard.index.get(&base).map(|e| e.raw_name.clone())
            } && entry_raw == raw
            {
                return Err(fuse3::Errno::from(libc::EEXIST));
            }
            let has_base = {
                let guard = state.index.read().unwrap();
                guard.index.contains_key(&base)
            };
            if !has_base {
                return Ok((BackendName::Internal(base), kind));
            }
            for suffix in 1..=MAX_COLLISION_SUFFIX {
                let candidate = backend_basename_from_hash(&hash, Some(suffix));
                let entry_raw = {
                    let guard = state.index.read().unwrap();
                    guard.index.get(&candidate).map(|e| e.raw_name.clone())
                };
                match entry_raw {
                    None => return Ok((BackendName::Internal(candidate), kind)),
                    Some(existing) if existing == raw => {
                        return Err(fuse3::Errno::from(libc::EEXIST));
                    }
                    Some(_) => continue,
                }
            }
            Err(fuse3::Errno::from(libc::ENOSPC))
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum CreateDecision {
    AlreadyExistsSameName,
    NeedNewSuffix,
}

fn handle_backend_eexist_index_missing(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    backend_name: &str,
    desired_raw: &[u8],
) -> Result<CreateDecision, fuse3::Errno> {
    let c_name = string_to_cstring(backend_name)?;
    let fd = nix::fcntl::openat(
        dir_fd,
        c_name.as_c_str(),
        OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(errno_from_nix)?;
    let existing_raw = get_internal_rawname(fd.as_fd())?;
    {
        let mut guard = state.index.write().unwrap();
        guard
            .index
            .upsert(backend_name.to_owned(), existing_raw.clone());
        guard.pending = guard.pending.saturating_add(1);
    }
    state.attr_cache.clear();
    if existing_raw == desired_raw {
        Ok(CreateDecision::AlreadyExistsSameName)
    } else {
        Ok(CreateDecision::NeedNewSuffix)
    }
}

fn path_segments(path: &OsStr) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let bytes = path.as_bytes();
    let mut start = 0usize;
    for (idx, b) in bytes.iter().enumerate() {
        if *b == b'/' {
            if idx > start {
                out.push(bytes[start..idx].to_vec());
            }
            start = idx + 1;
        }
    }
    if start < bytes.len() {
        out.push(bytes[start..].to_vec());
    }
    out
}

fn open_backend_root(config: &Config) -> Result<OwnedFd, fuse3::Errno> {
    nix::fcntl::openat(
        config.backend_fd(),
        ".",
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_from_nix)
}

pub struct LongNameFsV2 {
    pub config: Arc<Config>,
    handles: V2HandleTable,
    dir_cache: DirCache,
    index_cache: IndexCache,
    max_write: NonZeroU32,
    max_name_len: usize,
    index_sync: IndexSync,
}

impl LongNameFsV2 {
    pub fn new(
        config: Config,
        max_name_len: usize,
        dir_cache_ttl: Option<Duration>,
        max_write_kb: u32,
        index_sync: IndexSync,
    ) -> Result<Self, fuse3::Errno> {
        verify_backend_supports_xattr(config.backend_fd())?;
        let bytes = max_write_kb.saturating_mul(1024).max(4096);
        let max_write = NonZeroU32::new(bytes).unwrap_or_else(|| NonZeroU32::new(4096).unwrap());
        Ok(Self {
            config: Arc::new(config),
            handles: V2HandleTable::new(),
            dir_cache: DirCache::new(dir_cache_ttl),
            index_cache: IndexCache::new(),
            max_write,
            max_name_len,
            index_sync,
        })
    }

    fn invalidate_dir(&self, dir_fd: BorrowedFd<'_>) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.dir_cache.invalidate(key);
            self.handles.clear_dir_attr_cache(key);
        }
    }

    fn load_dir_entries(&self, handle: &Arc<DirHandle>) -> Arc<Vec<DirEntryInfo>> {
        let key = dir_cache_key(handle.as_fd());
        if let Some(cache_key) = key
            && let Some(entries) = self.dir_cache.get(cache_key)
        {
            return entries;
        }

        let logical =
            list_logical_entries(handle, self.max_name_len, self.index_sync).unwrap_or_default();
        if let Some(cache_key) = key {
            return self.dir_cache.insert(cache_key, logical);
        }
        Arc::new(logical)
    }

    fn stat_path(&self, path: &OsStr) -> Result<fuse3::path::reply::FileAttr, fuse3::Errno> {
        if path == OsStr::new("/") {
            let stat = fstatat(
                self.config.backend_fd(),
                "",
                AtFlags::AT_EMPTY_PATH | AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(errno_from_nix)?;
            return Ok(file_attr_from_stat(&stat));
        }
        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let stat = fstatat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(errno_from_nix)?;
        Ok(file_attr_from_stat(&stat))
    }

    fn resolve_dir(&self, path: &OsStr) -> Result<ParentCtx, fuse3::Errno> {
        if path == OsStr::new("/") {
            let dir_fd = open_backend_root(&self.config)?;
            let state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            return Ok(ParentCtx { dir_fd, state });
        }
        let mut segs = path_segments(path);
        if segs.is_empty() {
            return Err(fuse3::Errno::new_not_exist());
        }

        let mut dir_fd = open_backend_root(&self.config)?;
        for seg in segs.drain(..) {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            let (backend, _kind) =
                map_segment_for_lookup(dir_fd.as_fd(), &mut state, &seg, self.max_name_len)?;
            maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync)?;
            let c_name = backend.as_cstring()?;
            let next_fd = nix::fcntl::openat(
                dir_fd.as_fd(),
                c_name.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            drop(dir_fd);
            dir_fd = next_fd;
        }
        let state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
        Ok(ParentCtx { dir_fd, state })
    }

    fn resolve_path(&self, path: &OsStr) -> Result<Ln2Path, fuse3::Errno> {
        if path == OsStr::new("/") {
            return Err(fuse3::Errno::from(libc::EFAULT));
        }

        let segments = path_segments(path);
        if segments.is_empty() {
            return Err(fuse3::Errno::new_not_exist());
        }
        let mut dir_fd = open_backend_root(&self.config)?;
        for seg in segments[..segments.len() - 1].iter() {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            let (backend, _) =
                map_segment_for_lookup(dir_fd.as_fd(), &mut state, seg, self.max_name_len)?;
            maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync)?;
            let c_name = backend.as_cstring()?;
            let next_fd = nix::fcntl::openat(
                dir_fd.as_fd(),
                c_name.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            drop(dir_fd);
            dir_fd = next_fd;
        }

        let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
        let raw_last = segments.last().unwrap().clone();
        let (backend_name, kind) =
            map_segment_for_lookup(dir_fd.as_fd(), &mut state, &raw_last, self.max_name_len)?;
        maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync)?;

        Ok(Ln2Path {
            dir_fd,
            backend_name,
            raw_name: raw_last,
            kind,
        })
    }
}

impl PathFilesystem for LongNameFsV2 {
    async fn init(&self, _req: Request) -> Result<ReplyInit, fuse3::Errno> {
        Ok(ReplyInit {
            max_write: self.max_write,
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, _) =
            map_segment_for_lookup(ctx.dir_fd.as_fd(), &mut ctx.state, &raw, self.max_name_len)?;
        maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        let attr = {
            let fname = backend.as_cstring()?;
            let stat = fstatat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(errno_from_nix)?;
            file_attr_from_stat(&stat)
        };
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr, fuse3::Errno> {
        if let Some(handle) = fh.and_then(|id| self.handles.get_file(id)) {
            let stat = fstat(handle.as_fd()).map_err(errno_from_nix)?;
            let attr = file_attr_from_stat(&stat);
            return Ok(ReplyAttr {
                ttl: ATTR_TTL,
                attr,
            });
        }

        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        let attr = self.stat_path(path)?;
        Ok(ReplyAttr {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr, fuse3::Errno> {
        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        if path == OsStr::new("/") {
            if let Some(mode) = set_attr.mode {
                nix::sys::stat::fchmod(self.config.backend_fd(), Mode::from_bits_truncate(mode))
                    .map_err(errno_from_nix)?;
            }
            if set_attr.uid.is_some() || set_attr.gid.is_some() {
                let uid = set_attr.uid.map(Uid::from_raw);
                let gid = set_attr.gid.map(Gid::from_raw);
                nix::unistd::fchown(self.config.backend_fd(), uid, gid).map_err(errno_from_nix)?;
            }
            if set_attr.size.is_some() {
                return Err(fuse3::Errno::from(libc::EFAULT));
            }
            if set_attr.atime.is_some() || set_attr.mtime.is_some() {
                let atime = set_attr
                    .atime
                    .map(|t| TimeSpec::new(t.sec, t.nsec as _))
                    .unwrap_or(TimeSpec::UTIME_OMIT);
                let mtime = set_attr
                    .mtime
                    .map(|t| TimeSpec::new(t.sec, t.nsec as _))
                    .unwrap_or(TimeSpec::UTIME_OMIT);
                let times = [*atime.as_ref(), *mtime.as_ref()];
                let res =
                    unsafe { libc::futimens(self.config.backend_fd().as_raw_fd(), times.as_ptr()) };
                if res < 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            return self.getattr(_req, Some(path), None, 0).await;
        }

        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        if let Some(mode) = set_attr.mode {
            fchmodat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                Mode::from_bits_truncate(mode),
                FchmodatFlags::FollowSymlink,
            )
            .map_err(errno_from_nix)?;
        }
        if set_attr.uid.is_some() || set_attr.gid.is_some() {
            fchownat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                set_attr.uid.map(Uid::from_raw),
                set_attr.gid.map(Gid::from_raw),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(errno_from_nix)?;
        }
        if let Some(size) = set_attr.size {
            let file = nix::fcntl::openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_WRONLY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            nix::unistd::ftruncate(&file, size as i64).map_err(errno_from_nix)?;
        }
        if set_attr.atime.is_some() || set_attr.mtime.is_some() {
            let atime = set_attr
                .atime
                .map(|t| TimeSpec::new(t.sec, t.nsec as _))
                .unwrap_or(TimeSpec::UTIME_OMIT);
            let mtime = set_attr
                .mtime
                .map(|t| TimeSpec::new(t.sec, t.nsec as _))
                .unwrap_or(TimeSpec::UTIME_OMIT);
            utimensat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                &atime,
                &mtime,
                UtimensatFlags::NoFollowSymlink,
            )
            .map_err(errno_from_nix)?;
        }
        self.invalidate_dir(mapped.dir_fd.as_fd());
        let attr = self.stat_path(path)?;
        Ok(ReplyAttr {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn readlink(&self, _req: Request, path: &OsStr) -> Result<ReplyData, fuse3::Errno> {
        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let target = readlinkat(mapped.dir_fd.as_fd(), fname.as_c_str()).map_err(errno_from_nix)?;
        let bytes = target.into_vec();
        Ok(Bytes::from(bytes).into())
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, kind) = map_segment_for_create(&ctx.state, &raw, self.max_name_len)?;
        let fname = backend.as_cstring()?;
        symlinkat(link_path, ctx.dir_fd.as_fd(), fname.as_c_str()).map_err(errno_from_nix)?;
        if matches!(kind, SegmentKind::Long) {
            let fd = nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            set_internal_rawname(fd.as_fd(), &raw)?;
            {
                let mut guard = ctx.state.index.write().unwrap();
                guard
                    .index
                    .upsert(String::from_utf8(backend.display_bytes()).unwrap(), raw);
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        }
        self.invalidate_dir(ctx.dir_fd.as_fd());
        let attr = self.stat_path(&crate::v2::path::make_child_path(parent, name))?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn mknod(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, kind) = map_segment_for_create(&ctx.state, &raw, self.max_name_len)?;
        let fname = backend.as_cstring()?;
        let sflag = nix::sys::stat::SFlag::from_bits_truncate(mode);
        let perm = Mode::from_bits_truncate(mode);
        let dev = rdev as u64;
        mknodat(ctx.dir_fd.as_fd(), fname.as_c_str(), sflag, perm, dev).map_err(errno_from_nix)?;
        if matches!(kind, SegmentKind::Long) {
            let fd = nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            set_internal_rawname(fd.as_fd(), &raw)?;
            {
                let mut guard = ctx.state.index.write().unwrap();
                guard
                    .index
                    .upsert(String::from_utf8(backend.display_bytes()).unwrap(), raw);
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        }
        self.invalidate_dir(ctx.dir_fd.as_fd());
        let attr = self.stat_path(&crate::v2::path::make_child_path(parent, name))?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, kind) = map_segment_for_create(&ctx.state, &raw, self.max_name_len)?;
        let fname = backend.as_cstring()?;
        mkdirat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            Mode::from_bits_truncate(mode),
        )
        .map_err(errno_from_nix)?;
        if matches!(kind, SegmentKind::Long) {
            let fd = nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            set_internal_rawname(fd.as_fd(), &raw)?;
            {
                let mut guard = ctx.state.index.write().unwrap();
                guard
                    .index
                    .upsert(String::from_utf8(backend.display_bytes()).unwrap(), raw);
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        }
        self.invalidate_dir(ctx.dir_fd.as_fd());
        let attr = self.stat_path(&crate::v2::path::make_child_path(parent, name))?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn unlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, kind) =
            map_segment_for_lookup(ctx.dir_fd.as_fd(), &mut ctx.state, &raw, self.max_name_len)?;
        let fname = backend.as_cstring()?;
        unlinkat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::NoRemoveDir,
        )
        .map_err(errno_from_nix)?;
        if matches!(kind, SegmentKind::Long) {
            {
                let mut guard = ctx.state.index.write().unwrap();
                if guard
                    .index
                    .remove(&String::from_utf8(backend.display_bytes()).unwrap())
                    .is_some()
                {
                    guard.pending = guard.pending.saturating_add(1);
                }
            }
            ctx.state.attr_cache.clear();
        }
        maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        self.invalidate_dir(ctx.dir_fd.as_fd());
        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<(), fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let (backend, kind) =
            map_segment_for_lookup(ctx.dir_fd.as_fd(), &mut ctx.state, &raw, self.max_name_len)?;
        let fname = backend.as_cstring()?;
        unlinkat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::RemoveDir,
        )
        .map_err(errno_from_nix)?;
        if matches!(kind, SegmentKind::Long) {
            {
                let mut guard = ctx.state.index.write().unwrap();
                if guard
                    .index
                    .remove(&String::from_utf8(backend.display_bytes()).unwrap())
                    .is_some()
                {
                    guard.pending = guard.pending.saturating_add(1);
                }
            }
            ctx.state.attr_cache.clear();
        }
        maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
        self.invalidate_dir(ctx.dir_fd.as_fd());
        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        let mut from_ctx = self.resolve_dir(origin_parent)?;
        let mut to_ctx = self.resolve_dir(parent)?;
        let raw_from = normalize_osstr(origin_name);
        let raw_to = normalize_osstr(name);
        let (from_backend, from_kind) = map_segment_for_lookup(
            from_ctx.dir_fd.as_fd(),
            &mut from_ctx.state,
            &raw_from,
            self.max_name_len,
        )?;
        maybe_flush_index(
            from_ctx.dir_fd.as_fd(),
            &mut from_ctx.state,
            self.index_sync,
        )?;
        let to_kind = classify_segment(&raw_to, self.max_name_len)?;
        if from_kind != to_kind {
            return Err(fuse3::Errno::from(libc::EOPNOTSUPP));
        }

        let dest_backend = if matches!(to_kind, SegmentKind::Short) {
            BackendName::Short(raw_to.clone())
        } else {
            // 可能已有同名长名
            if let Some(entry) = {
                let guard = to_ctx.state.index.read().unwrap();
                guard.index.backend_for_raw(&raw_to).cloned()
            } {
                BackendName::Internal(entry)
            } else {
                let (candidate, _) =
                    map_segment_for_create(&to_ctx.state, &raw_to, self.max_name_len)?;
                candidate
            }
        };

        let from_c = from_backend.as_cstring()?;
        let to_c = dest_backend.as_cstring()?;
        renameat(
            from_ctx.dir_fd.as_fd(),
            from_c.as_c_str(),
            to_ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
        )
        .map_err(errno_from_nix)?;

        if matches!(from_kind, SegmentKind::Long) {
            let to_fd = nix::fcntl::openat(
                to_ctx.dir_fd.as_fd(),
                to_c.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            set_internal_rawname(to_fd.as_fd(), &raw_to)?;
            {
                let mut guard = from_ctx.state.index.write().unwrap();
                guard
                    .index
                    .remove(&String::from_utf8(from_backend.display_bytes()).unwrap());
                guard.pending = guard.pending.saturating_add(1);
            }
            from_ctx.state.attr_cache.clear();
            {
                let mut guard = to_ctx.state.index.write().unwrap();
                guard.index.upsert(
                    String::from_utf8(dest_backend.display_bytes()).unwrap(),
                    raw_to,
                );
                guard.pending = guard.pending.saturating_add(1);
            }
            to_ctx.state.attr_cache.clear();
            maybe_flush_index(
                from_ctx.dir_fd.as_fd(),
                &mut from_ctx.state,
                self.index_sync,
            )?;
            maybe_flush_index(to_ctx.dir_fd.as_fd(), &mut to_ctx.state, self.index_sync)?;
        }

        self.invalidate_dir(from_ctx.dir_fd.as_fd());
        self.invalidate_dir(to_ctx.dir_fd.as_fd());
        Ok(())
    }

    async fn link(
        &self,
        _req: Request,
        path: &OsStr,
        new_parent: &OsStr,
        new_name: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let target = self.resolve_path(path)?;
        if target.backend_name.is_internal() {
            return Err(fuse3::Errno::from(libc::EOPNOTSUPP));
        }
        let ctx = self.resolve_dir(new_parent)?;
        let raw_new = normalize_osstr(new_name);
        let (dest_backend, dest_kind) =
            map_segment_for_create(&ctx.state, &raw_new, self.max_name_len)?;
        if matches!(dest_kind, SegmentKind::Long) {
            return Err(fuse3::Errno::from(libc::EOPNOTSUPP));
        }
        let from_c = target.backend_name.as_cstring()?;
        let to_c = dest_backend.as_cstring()?;
        linkat(
            target.dir_fd.as_fd(),
            from_c.as_c_str(),
            ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
            LinkatFlags::empty(),
        )
        .map_err(errno_from_nix)?;
        self.invalidate_dir(ctx.dir_fd.as_fd());
        let attr = self.stat_path(&crate::v2::path::make_child_path(new_parent, new_name))?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn open(
        &self,
        _req: Request,
        path: &OsStr,
        flags: u32,
    ) -> Result<ReplyOpen, fuse3::Errno> {
        let mapped = self.resolve_path(path)?;
        let oflag = oflag_from_bits(flags) | OFlag::O_CLOEXEC;
        let fname = mapped.backend_name.as_cstring()?;
        let fd = nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            oflag,
            Mode::from_bits_truncate(0o666),
        )
        .map_err(errno_from_nix)?;
        let handle = self.handles.insert_file(fd);
        Ok(ReplyOpen {
            fh: handle,
            flags: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData, fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        let mut buf = vec![0u8; size as usize];
        let read_len = retry_eintr(|| pread(handle.as_fd(), &mut buf, offset as i64))
            .map_err(errno_from_nix)?;
        buf.truncate(read_len);
        Ok(Bytes::from(buf).into())
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite, fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        let written =
            retry_eintr(|| pwrite(handle.as_fd(), data, offset as i64)).map_err(errno_from_nix)?;

        if self.config.sync_data() {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        }

        if let Some(path) = path
            && path != "/"
            && let Ok(mapped) = self.resolve_path(path)
        {
            self.invalidate_dir(mapped.dir_fd.as_fd());
        }

        Ok(ReplyWrite {
            written: written as u32,
        })
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<(), fuse3::Errno> {
        self.handles.remove(fh);
        Ok(())
    }

    async fn fsync(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        datasync: bool,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        if datasync {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        } else {
            fsync(handle.as_fd()).map_err(errno_from_nix)?;
        }
        Ok(())
    }

    async fn setxattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> Result<(), fuse3::Errno> {
        if position != 0 {
            return Err(fuse3::Errno::from(libc::EINVAL));
        }
        if name.as_bytes().starts_with(b"user.ln2.") {
            return Err(fuse3::Errno::from(libc::EPERM));
        }
        if path == OsStr::new("/") {
            let cname = cstring_from_bytes(name.as_bytes())?;
            let res = unsafe {
                libc::fsetxattr(
                    self.config.backend_fd().as_raw_fd(),
                    cname.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    flags as libc::c_int,
                )
            };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            return Ok(());
        }
        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let fd = nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_from_nix)?;
        let cname = cstring_from_bytes(name.as_bytes())?;
        let res = unsafe {
            libc::fsetxattr(
                fd.as_raw_fd(),
                cname.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags as libc::c_int,
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    async fn getxattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr, fuse3::Errno> {
        if name.as_bytes().starts_with(b"user.ln2.") {
            return Err(fuse3::Errno::from(libc::EPERM));
        }
        let cname = cstring_from_bytes(name.as_bytes())?;
        let fd_raw = if path == OsStr::new("/") {
            self.config.backend_fd().as_raw_fd()
        } else {
            let mapped = self.resolve_path(path)?;
            let fname = mapped.backend_name.as_cstring()?;
            let fd = nix::fcntl::openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            fd.as_raw_fd()
        };
        if size == 0 {
            let res = unsafe { libc::fgetxattr(fd_raw, cname.as_ptr(), std::ptr::null_mut(), 0) };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            return Ok(ReplyXAttr::Size(res as u32));
        }
        let mut buf = vec![0u8; size as usize];
        let res = unsafe {
            libc::fgetxattr(
                fd_raw,
                cname.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                size as usize,
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let read_len = res as usize;
        if read_len > size as usize {
            return Err(fuse3::Errno::from(libc::ERANGE));
        }
        buf.truncate(read_len);
        Ok(ReplyXAttr::Data(Bytes::from(buf)))
    }

    async fn listxattr(
        &self,
        _req: Request,
        path: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr, fuse3::Errno> {
        let fd_raw = if path == OsStr::new("/") {
            self.config.backend_fd().as_raw_fd()
        } else {
            let mapped = self.resolve_path(path)?;
            let fname = mapped.backend_name.as_cstring()?;
            let fd = nix::fcntl::openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            fd.as_raw_fd()
        };
        let initial = unsafe { libc::flistxattr(fd_raw, std::ptr::null_mut(), 0) };
        if initial < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let mut buf = vec![0u8; initial as usize];
        let res = unsafe {
            libc::flistxattr(
                fd_raw,
                buf.as_mut_ptr() as *mut libc::c_char,
                buf.len() as libc::size_t,
            )
        };
        if res < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let list_len = res as usize;
        buf.truncate(list_len);
        // 过滤 user.ln2.*
        let mut filtered = Vec::new();
        let mut start = 0usize;
        for i in 0..buf.len() {
            if buf[i] == 0 {
                let key = &buf[start..i];
                if !key.starts_with(b"user.ln2.") {
                    filtered.extend_from_slice(key);
                    filtered.push(0);
                }
                start = i + 1;
            }
        }
        if size == 0 {
            Ok(ReplyXAttr::Size(filtered.len() as u32))
        } else if filtered.len() > size as usize {
            Err(fuse3::Errno::from(libc::ERANGE))
        } else {
            Ok(ReplyXAttr::Data(Bytes::from(filtered)))
        }
    }

    async fn removexattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        if name.as_bytes().starts_with(b"user.ln2.") {
            return Err(fuse3::Errno::from(libc::EPERM));
        }
        let cname = cstring_from_bytes(name.as_bytes())?;
        let fd_raw = if path == OsStr::new("/") {
            self.config.backend_fd().as_raw_fd()
        } else {
            let mapped = self.resolve_path(path)?;
            let fname = mapped.backend_name.as_cstring()?;
            let fd = nix::fcntl::openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            fd.as_raw_fd()
        };
        let res = unsafe { libc::fremovexattr(fd_raw, cname.as_ptr()) };
        if res < 0 {
            return Err(io::Error::last_os_error().into());
        }
        Ok(())
    }

    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _lock_owner: u64,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        fsync(handle.as_fd()).map_err(errno_from_nix)
    }

    async fn access(&self, _req: Request, path: &OsStr, mask: u32) -> Result<(), fuse3::Errno> {
        let flags = access_mask_from_bits(mask);
        if path == OsStr::new("/") {
            faccessat(self.config.backend_fd(), ".", flags, AtFlags::empty())
                .map_err(errno_from_nix)?;
            return Ok(());
        }

        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        faccessat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            flags,
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(errno_from_nix)?;
        Ok(())
    }

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated, fuse3::Errno> {
        let mut ctx = self.resolve_dir(parent)?;
        let raw = normalize_osstr(name);
        let mut backend = map_segment_for_create(&ctx.state, &raw, self.max_name_len)?;

        let mut attempt = 0;
        loop {
            let fname = backend.0.as_cstring()?;
            match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                oflag_from_bits(flags) | OFlag::O_CLOEXEC | OFlag::O_CREAT | OFlag::O_EXCL,
                Mode::from_bits_truncate(mode & 0o777),
            ) {
                Ok(fd) => {
                    if matches!(backend.1, SegmentKind::Long) {
                        set_internal_rawname(fd.as_fd(), &raw)?;
                        {
                            let mut guard = ctx.state.index.write().unwrap();
                            guard.index.upsert(
                                String::from_utf8(backend.0.display_bytes()).unwrap(),
                                raw.clone(),
                            );
                            guard.pending = guard.pending.saturating_add(1);
                        }
                        ctx.state.attr_cache.clear();
                        maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
                    }
                    self.invalidate_dir(ctx.dir_fd.as_fd());
                    let fh = self.handles.insert_file(fd);
                    let attr = self.stat_path(&crate::v2::path::make_child_path(parent, name))?;
                    return Ok(ReplyCreated {
                        ttl: ATTR_TTL,
                        attr,
                        generation: 0,
                        fh,
                        flags: 0,
                    });
                }
                Err(nix::errno::Errno::EEXIST) => {
                    if matches!(backend.1, SegmentKind::Short) || attempt > MAX_COLLISION_SUFFIX {
                        return Err(fuse3::Errno::from(libc::EEXIST));
                    }
                    let decision = handle_backend_eexist_index_missing(
                        ctx.dir_fd.as_fd(),
                        &mut ctx.state,
                        &String::from_utf8(backend.0.display_bytes()).unwrap(),
                        &raw,
                    )?;
                    maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync)?;
                    match decision {
                        CreateDecision::AlreadyExistsSameName => {
                            return Err(fuse3::Errno::from(libc::EEXIST));
                        }
                        CreateDecision::NeedNewSuffix => {
                            backend = map_segment_for_create(&ctx.state, &raw, self.max_name_len)?;
                            attempt += 1;
                            continue;
                        }
                    }
                }
                Err(err) => return Err(errno_from_nix(err)),
            }
        }
    }

    async fn opendir(
        &self,
        _req: Request,
        path: &OsStr,
        flags: u32,
    ) -> Result<ReplyOpen, fuse3::Errno> {
        if path == OsStr::new("/") {
            let fd = open_backend_root(&self.config)?;
            let index = load_dir_state(&self.index_cache, fd.as_fd())?;
            let handle = self.handles.insert_dir(DirHandle::new(fd, index));
            return Ok(ReplyOpen { fh: handle, flags });
        }
        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let fd = nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(errno_from_nix)?;
        let index = load_dir_state(&self.index_cache, fd.as_fd())?;
        let handle = self.handles.insert_dir(DirHandle::new(fd, index));
        Ok(ReplyOpen { fh: handle, flags })
    }

    type DirEntryStream<'a>
        = futures_util::stream::Iter<std::vec::IntoIter<fuse3::Result<DirectoryEntry>>>
    where
        Self: 'a;
    type DirEntryPlusStream<'a>
        = futures_util::stream::Iter<std::vec::IntoIter<fuse3::Result<DirectoryEntryPlus>>>
    where
        Self: 'a;

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        _path: &'a OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>, fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        let logical = self.load_dir_entries(&handle);
        let mut entries: Vec<fuse3::Result<DirectoryEntry>> = Vec::with_capacity(logical.len() + 2);

        let mut idx: i64 = 0;
        entries.push(Ok(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: idx + 1,
        }));
        idx += 1;
        entries.push(Ok(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: idx + 1,
        }));
        idx += 1;

        for entry in logical.iter() {
            idx += 1;
            entries.push(Ok(DirectoryEntry {
                kind: entry.kind,
                name: entry.name.clone(),
                offset: idx,
            }));
        }

        let skip = offset.max(0) as usize;
        let entries: Vec<_> = entries.into_iter().skip(skip).collect();
        let stream = futures_util::stream::iter(entries);
        Ok(ReplyDirectory { entries: stream })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        _parent: &'a OsStr,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>, fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        let logical = self.load_dir_entries(&handle);
        let mut entries: Vec<fuse3::Result<DirectoryEntryPlus>> =
            Vec::with_capacity(logical.len() + 2);
        let dir_attr = fstat(handle.as_fd())
            .map_err(errno_from_nix)
            .map(|stat| file_attr_from_stat(&stat))?;

        let mut idx: i64 = 0;
        entries.push(Ok(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: idx + 1,
            attr: dir_attr,
            entry_ttl: ATTR_TTL,
            attr_ttl: ATTR_TTL,
        }));
        idx += 1;
        entries.push(Ok(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: idx + 1,
            attr: dir_attr,
            entry_ttl: ATTR_TTL,
            attr_ttl: ATTR_TTL,
        }));
        idx += 1;

        for entry in logical.iter() {
            idx += 1;
            let attr = if let Some(attr) = entry.attr {
                attr
            } else {
                let c_name = match cstring_from_bytes(&entry.backend_name) {
                    Ok(v) => v,
                    Err(err) => {
                        entries.push(Err(err));
                        continue;
                    }
                };
                match fstatat(
                    handle.as_fd(),
                    c_name.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                ) {
                    Ok(st) => file_attr_from_stat(&st),
                    Err(err) => {
                        entries.push(Err(errno_from_nix(err)));
                        continue;
                    }
                }
            };
            entries.push(Ok(DirectoryEntryPlus {
                kind: entry.kind,
                name: entry.name.clone(),
                offset: idx,
                attr,
                entry_ttl: ATTR_TTL,
                attr_ttl: ATTR_TTL,
            }));
        }

        let skip = offset as usize;
        let entries: Vec<_> = entries.into_iter().skip(skip).collect();
        let stream = futures_util::stream::iter(entries);
        Ok(ReplyDirectoryPlus { entries: stream })
    }

    async fn releasedir(
        &self,
        _req: Request,
        _path: &OsStr,
        fh: u64,
        _flags: u32,
    ) -> Result<(), fuse3::Errno> {
        self.handles.remove(fh);
        Ok(())
    }

    async fn fsyncdir(
        &self,
        _req: Request,
        _path: &OsStr,
        fh: u64,
        datasync: bool,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        if datasync {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        } else {
            fsync(handle.as_fd()).map_err(errno_from_nix)?;
        }
        Ok(())
    }

    async fn statfs(&self, _req: Request, _path: &OsStr) -> Result<ReplyStatFs, fuse3::Errno> {
        let stats = fstatvfs(self.config.backend_fd()).map_err(errno_from_nix)?;
        Ok(ReplyStatFs {
            blocks: stats.blocks(),
            bfree: stats.blocks_free(),
            bavail: stats.blocks_available(),
            files: stats.files(),
            ffree: stats.files_free(),
            bsize: stats.block_size() as u32,
            namelen: stats.name_max() as u32,
            frsize: stats.fragment_size() as u32,
        })
    }

    async fn poll(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _kn: Option<u64>,
        _flags: u32,
        _events: u32,
        _notify: &Notify,
    ) -> Result<ReplyPoll, fuse3::Errno> {
        if self.handles.get_file(fh).is_none() {
            return Err(fuse3::Errno::from(libc::EBADF));
        }
        Ok(ReplyPoll { revents: 0 })
    }
}
