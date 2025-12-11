use crate::config::Config;
use crate::util::{
    access_mask_from_bits, core_begin_temp_file, oflag_from_bits, retry_eintr, string_to_cstring,
};
use crate::v2::error::{CoreError, CoreResult, core_err_to_errno};
use crate::v2::index::{
    DirIndex, INDEX_NAME, IndexLoadResult, JOURNAL_MAX_BYTES, JOURNAL_MAX_OPS, JOURNAL_NAME,
    append_to_journal_file, read_dir_index, reset_journal, write_dir_index,
};
use crate::v2::inode_store::{
    BackendKey, InodeEntry, InodeId, InodeKind, InodeStore, ParentName, ROOT_INODE,
};
use crate::v2::path::{
    INTERNAL_PREFIX, MAX_COLLISION_SUFFIX, SegmentKind, backend_basename_from_hash,
    classify_segment, encode_long_name, is_reserved_prefix, normalize_osstr,
};
use fuser::{
    FileAttr as FuserFileAttr, FileType as FuserFileType, Filesystem as FuserFilesystem,
    KernelConfig, Notifier as FuserNotifier, PollHandle as FuserPollHandle,
    ReplyAttr as FuserReplyAttr, ReplyCreate as FuserReplyCreate, ReplyData as FuserReplyData,
    ReplyDirectory as FuserReplyDirectory, ReplyDirectoryPlus as FuserReplyDirectoryPlus,
    ReplyEmpty as FuserReplyEmpty, ReplyEntry as FuserReplyEntry, ReplyOpen as FuserReplyOpen,
    ReplyPoll as FuserReplyPoll, ReplyStatfs as FuserReplyStatfs, ReplyWrite as FuserReplyWrite,
    ReplyXattr as FuserReplyXattr, Request as FuserRequest, TimeOrNow,
};
use nix::dir::Dir;
use nix::fcntl::{AtFlags, OFlag, RenameFlags, readlinkat, renameat, renameat2};
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
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::fs::File;
use std::io;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::sync::mpsc;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const RAWNAME_XATTR: &str = "user.ln2.rawname";
const PARALLEL_REBUILD_THRESHOLD: usize = 64;
const PARALLEL_REBUILD_WORKERS: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CoreFileType {
    Directory,
    Symlink,
    RegularFile,
    BlockDevice,
    CharDevice,
    NamedPipe,
    Socket,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CoreFileAttr {
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub kind: CoreFileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
}

fn core_errno_from_nix(err: nix::Error) -> CoreError {
    CoreError::from(err)
}

fn core_string_to_cstring(value: &str) -> CoreResult<CString> {
    CString::new(value.as_bytes()).map_err(|_| CoreError::from_errno(libc::EINVAL))
}

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
pub(crate) struct DirCacheKey {
    dev: u64,
    ino: u64,
}

// Lock ordering (v2):
// - InodeStore backend_map -> inode shard
// - IndexCache shard mutex -> per-dir IndexState lock (multiple dirs in (dev, ino) order)
// - DirCache shard locks are independent; keep I/O outside
// - Handle table shards are independent; iterate shards one at a time

#[derive(Debug)]
struct DirCacheEntry {
    expires_at: Instant,
    entries: Arc<Vec<DirEntryInfo>>,
    has_attrs: bool,
}

#[derive(Debug, Clone)]
struct DirCacheHit {
    entries: Arc<Vec<DirEntryInfo>>,
    has_attrs: bool,
}

#[derive(Debug, Default)]
struct DirCacheShard {
    entries: HashMap<DirCacheKey, DirCacheEntry>,
    lru: VecDeque<DirCacheKey>,
}

const DIR_CACHE_MAX_DIRS: usize = 1024;
const DIR_CACHE_SHARD_COUNT: usize = 64;
const DIR_CACHE_SHARD_MASK: usize = DIR_CACHE_SHARD_COUNT - 1;
// Keep per-shard capacity generous to avoid aggressive eviction on busy shards.
const DIR_CACHE_MAX_DIRS_PER_SHARD: usize =
    if DIR_CACHE_MAX_DIRS.div_ceil(DIR_CACHE_SHARD_COUNT) > 64 {
        DIR_CACHE_MAX_DIRS.div_ceil(DIR_CACHE_SHARD_COUNT)
    } else {
        64
    };
const DIR_FD_CACHE_MAX_DIRS: usize = 1024;
const _: () = assert!(DIR_CACHE_SHARD_COUNT.is_power_of_two());

#[derive(Debug)]
struct DirCache {
    ttl: Duration,
    enabled: bool,
    shards: Vec<RwLock<DirCacheShard>>,
}

#[derive(Debug, Clone)]
pub(crate) enum CacheOp {
    Add(DirEntryInfo),
    Remove(Vec<u8>),
    UpdateAttr(Vec<u8>, CoreFileAttr),
}

impl DirCache {
    fn new(ttl: Option<Duration>) -> Self {
        let (enabled, ttl) = match ttl {
            Some(t) => (true, t),
            None => (false, Duration::ZERO),
        };
        let shards = (0..DIR_CACHE_SHARD_COUNT)
            .map(|_| RwLock::new(DirCacheShard::default()))
            .collect();
        Self {
            ttl,
            enabled,
            shards,
        }
    }

    fn shard(&self, key: DirCacheKey) -> &RwLock<DirCacheShard> {
        let idx = ((key.dev ^ key.ino) as usize) & DIR_CACHE_SHARD_MASK;
        &self.shards[idx]
    }

    fn touch_lru(shard: &mut DirCacheShard, key: DirCacheKey) {
        if let Some(pos) = shard.lru.iter().position(|k| *k == key) {
            shard.lru.remove(pos);
        }
        shard.lru.push_back(key);
        while shard.lru.len() > DIR_CACHE_MAX_DIRS_PER_SHARD {
            shard.lru.pop_front();
        }
    }

    fn evict_if_needed(shard: &mut DirCacheShard) {
        while shard.entries.len() > DIR_CACHE_MAX_DIRS_PER_SHARD {
            if let Some(old) = shard.lru.pop_front() {
                shard.entries.remove(&old);
                continue;
            }
            break;
        }
    }

    fn drop_from_lru(shard: &mut DirCacheShard, key: &DirCacheKey) {
        if let Some(pos) = shard.lru.iter().position(|k| k == key) {
            shard.lru.remove(pos);
        }
    }

    fn get(&self, key: DirCacheKey) -> Option<DirCacheHit> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        let shard = self.shard(key);
        let mut guard = shard.write();
        let hit = if let Some(entry) = guard.entries.get_mut(&key) {
            if entry.expires_at > now {
                entry.expires_at = now + self.ttl;
                Some(DirCacheHit {
                    entries: entry.entries.clone(),
                    has_attrs: entry.has_attrs,
                })
            } else {
                guard.entries.remove(&key);
                Self::drop_from_lru(&mut guard, &key);
                None
            }
        } else {
            None
        };
        if hit.is_some() {
            Self::touch_lru(&mut guard, key);
        }
        hit
    }

    fn insert(
        &self,
        key: DirCacheKey,
        items: Vec<DirEntryInfo>,
        has_attrs: bool,
    ) -> Arc<Vec<DirEntryInfo>> {
        if !self.enabled {
            return Arc::new(items);
        }
        let expires_at = Instant::now() + self.ttl;
        let entries = Arc::new(items);
        let shard = self.shard(key);
        let mut guard = shard.write();
        Self::touch_lru(&mut guard, key);
        Self::evict_if_needed(&mut guard);
        guard.entries.insert(
            key,
            DirCacheEntry {
                expires_at,
                entries: entries.clone(),
                has_attrs,
            },
        );
        entries
    }

    fn invalidate(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        let shard = self.shard(key);
        let mut guard = shard.write();
        guard.entries.remove(&key);
        Self::drop_from_lru(&mut guard, &key);
    }

    fn patch(&self, key: DirCacheKey, op: CacheOp) {
        if !self.enabled {
            return;
        }
        let shard = self.shard(key);
        let mut guard = shard.write();
        let Some(entry) = guard.entries.get_mut(&key) else {
            return;
        };
        let mut vec = (*entry.entries).clone();
        match op {
            CacheOp::Add(info) => {
                if !vec.iter().any(|e| e.backend_name == info.backend_name) {
                    vec.push(info);
                }
            }
            CacheOp::Remove(backend) => {
                vec.retain(|e| e.backend_name != backend);
            }
            CacheOp::UpdateAttr(backend, new_attr) => {
                if let Some(item) = vec.iter_mut().find(|e| e.backend_name == backend) {
                    item.attr = Some(new_attr);
                }
            }
        }
        entry.entries = Arc::new(vec);
        entry.has_attrs = entry.entries.iter().all(|e| e.attr.is_some());
        entry.expires_at = Instant::now() + self.ttl;
        Self::touch_lru(&mut guard, key);
        Self::evict_if_needed(&mut guard);
    }
}

fn dir_cache_key(fd: BorrowedFd<'_>) -> Option<DirCacheKey> {
    fstat(fd).ok().map(|stat| DirCacheKey {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

#[derive(Debug, Clone)]
pub(crate) struct DirEntryInfo {
    name: OsString,
    kind: CoreFileType,
    attr: Option<CoreFileAttr>,
    backend_name: Vec<u8>,
    backend_key: Option<BackendKey>,
}

#[derive(Debug)]
struct DirFdCacheEntry {
    expires_at: Instant,
    fd: Arc<OwnedFd>,
}

#[derive(Debug)]
struct DirFdCache {
    ttl: Duration,
    enabled: bool,
    entries: Mutex<HashMap<DirCacheKey, DirFdCacheEntry>>,
    lru: Mutex<VecDeque<DirCacheKey>>,
}

impl DirFdCache {
    fn new(ttl: Option<Duration>) -> Self {
        let (enabled, ttl) = match ttl {
            Some(t) => (true, t),
            None => (false, Duration::ZERO),
        };
        Self {
            ttl,
            enabled,
            entries: Mutex::new(HashMap::new()),
            lru: Mutex::new(VecDeque::new()),
        }
    }

    fn touch_lru(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        let mut lru = self.lru.lock();
        if let Some(pos) = lru.iter().position(|k| *k == key) {
            lru.remove(pos);
        }
        lru.push_back(key);
        while lru.len() > DIR_FD_CACHE_MAX_DIRS {
            lru.pop_front();
        }
    }

    fn evict_if_needed(&self) {
        if !self.enabled {
            return;
        }
        let mut entries = self.entries.lock();
        let mut lru = self.lru.lock();
        while entries.len() > DIR_FD_CACHE_MAX_DIRS {
            if let Some(old) = lru.pop_front() {
                entries.remove(&old);
            } else {
                break;
            }
        }
    }

    fn get(&self, key: DirCacheKey) -> Option<Arc<OwnedFd>> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        let mut entries = self.entries.lock();
        if let Some(entry) = entries.get(&key)
            && entry.expires_at > now
        {
            let fd = entry.fd.clone();
            drop(entries);
            self.touch_lru(key);
            return Some(fd);
        }
        entries.remove(&key);
        None
    }

    fn insert(&self, key: DirCacheKey, fd: OwnedFd) -> Arc<OwnedFd> {
        let fd = Arc::new(fd);
        if !self.enabled {
            return fd;
        }
        let expires_at = Instant::now() + self.ttl;
        {
            let mut entries = self.entries.lock();
            entries.insert(
                key,
                DirFdCacheEntry {
                    expires_at,
                    fd: fd.clone(),
                },
            );
        }
        self.touch_lru(key);
        self.evict_if_needed();
        fd
    }

    fn invalidate(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        let mut entries = self.entries.lock();
        entries.remove(&key);
        let mut lru = self.lru.lock();
        if let Some(pos) = lru.iter().position(|k| *k == key) {
            lru.remove(pos);
        }
    }
}

fn core_file_type_from_mode(mode: libc::mode_t) -> CoreFileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => CoreFileType::Directory,
        libc::S_IFLNK => CoreFileType::Symlink,
        libc::S_IFCHR => CoreFileType::CharDevice,
        libc::S_IFBLK => CoreFileType::BlockDevice,
        libc::S_IFIFO => CoreFileType::NamedPipe,
        libc::S_IFSOCK => CoreFileType::Socket,
        _ => CoreFileType::RegularFile,
    }
}

fn system_time_from_raw(sec: i64, nsec: i64) -> SystemTime {
    if sec < 0 {
        return UNIX_EPOCH;
    }
    let nanos = if nsec < 0 { 0 } else { nsec as u32 };
    UNIX_EPOCH + Duration::new(sec as u64, nanos)
}

fn core_attr_from_stat(stat: &nix::sys::stat::FileStat) -> CoreFileAttr {
    let kind = core_file_type_from_mode(stat.st_mode);
    CoreFileAttr {
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: system_time_from_raw(stat.st_atime, stat.st_atime_nsec),
        mtime: system_time_from_raw(stat.st_mtime, stat.st_mtime_nsec),
        ctime: system_time_from_raw(stat.st_ctime, stat.st_ctime_nsec),
        kind,
        perm: (stat.st_mode & 0o7777) as u16,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        blksize: stat.st_blksize as u32,
    }
}

impl From<CoreFileType> for InodeKind {
    fn from(value: CoreFileType) -> Self {
        match value {
            CoreFileType::Directory => InodeKind::Directory,
            CoreFileType::Symlink => InodeKind::Symlink,
            CoreFileType::RegularFile => InodeKind::File,
            CoreFileType::BlockDevice => InodeKind::BlockDevice,
            CoreFileType::CharDevice => InodeKind::CharDevice,
            CoreFileType::NamedPipe => InodeKind::NamedPipe,
            CoreFileType::Socket => InodeKind::Socket,
        }
    }
}

fn fuser_file_type(kind: CoreFileType) -> FuserFileType {
    match kind {
        CoreFileType::Directory => FuserFileType::Directory,
        CoreFileType::Symlink => FuserFileType::Symlink,
        CoreFileType::RegularFile => FuserFileType::RegularFile,
        CoreFileType::BlockDevice => FuserFileType::BlockDevice,
        CoreFileType::CharDevice => FuserFileType::CharDevice,
        CoreFileType::NamedPipe => FuserFileType::NamedPipe,
        CoreFileType::Socket => FuserFileType::Socket,
    }
}

impl From<InodeKind> for FuserFileType {
    fn from(value: InodeKind) -> Self {
        match value {
            InodeKind::Directory => FuserFileType::Directory,
            InodeKind::Symlink => FuserFileType::Symlink,
            InodeKind::File => FuserFileType::RegularFile,
            InodeKind::BlockDevice => FuserFileType::BlockDevice,
            InodeKind::CharDevice => FuserFileType::CharDevice,
            InodeKind::NamedPipe => FuserFileType::NamedPipe,
            InodeKind::Socket => FuserFileType::Socket,
        }
    }
}

fn fuser_attr_from_core(attr: CoreFileAttr, ino: InodeId) -> FuserFileAttr {
    FuserFileAttr {
        ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        crtime: UNIX_EPOCH,
        kind: fuser_file_type(attr.kind),
        perm: attr.perm,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        flags: 0,
        blksize: attr.blksize,
    }
}

fn backend_key_from_stat(stat: &nix::sys::stat::FileStat) -> BackendKey {
    BackendKey {
        dev: stat.st_dev,
        ino: stat.st_ino,
    }
}

fn timespec_from_time_or_now(value: Option<TimeOrNow>) -> TimeSpec {
    match value {
        Some(TimeOrNow::Now) => TimeSpec::UTIME_NOW,
        Some(TimeOrNow::SpecificTime(t)) => match t.duration_since(UNIX_EPOCH) {
            Ok(dur) => TimeSpec::from_duration(dur),
            Err(err) => {
                let dur = err.duration();
                -TimeSpec::from_duration(dur)
            }
        },
        None => TimeSpec::UTIME_OMIT,
    }
}

fn map_dirent_type(entry: &nix::dir::Entry) -> Option<CoreFileType> {
    entry.file_type().map(|dt| match dt {
        nix::dir::Type::Directory => CoreFileType::Directory,
        nix::dir::Type::Symlink => CoreFileType::Symlink,
        nix::dir::Type::File => CoreFileType::RegularFile,
        nix::dir::Type::BlockDevice => CoreFileType::BlockDevice,
        nix::dir::Type::CharacterDevice => CoreFileType::CharDevice,
        nix::dir::Type::Fifo => CoreFileType::NamedPipe,
        nix::dir::Type::Socket => CoreFileType::Socket,
    })
}

#[derive(Debug)]
struct IndexState {
    index: DirIndex,
    journal_file: Option<File>,
    pending: usize,
    last_flush: Instant,
    journal_size_bytes: u64,
    journal_ops_since_compact: u64,
}

#[derive(Debug)]
struct IndexCacheEntry {
    value: Arc<RwLock<IndexState>>,
}

const INDEX_CACHE_MAX_DIRS: usize = 1024;
const INDEX_CACHE_SHARD_COUNT: usize = 64;
const INDEX_CACHE_SHARD_MASK: usize = INDEX_CACHE_SHARD_COUNT - 1;
const INDEX_CACHE_MAX_DIRS_PER_SHARD: usize =
    INDEX_CACHE_MAX_DIRS.div_ceil(INDEX_CACHE_SHARD_COUNT);
const _: () = assert!(INDEX_CACHE_SHARD_COUNT.is_power_of_two());

#[derive(Debug, Default)]
struct IndexCacheShard {
    entries: HashMap<DirCacheKey, IndexCacheEntry>,
    lru: VecDeque<DirCacheKey>,
}

#[derive(Debug)]
struct IndexCache {
    shards: Vec<Mutex<IndexCacheShard>>,
}

impl IndexCache {
    fn new() -> Self {
        let shards = (0..INDEX_CACHE_SHARD_COUNT)
            .map(|_| Mutex::new(IndexCacheShard::default()))
            .collect();
        Self { shards }
    }

    fn shard(&self, key: DirCacheKey) -> &Mutex<IndexCacheShard> {
        let idx = ((key.dev ^ key.ino) as usize) & INDEX_CACHE_SHARD_MASK;
        &self.shards[idx]
    }

    fn touch_lru(shard: &mut IndexCacheShard, key: DirCacheKey) {
        if let Some(pos) = shard.lru.iter().position(|k| *k == key) {
            shard.lru.remove(pos);
        }
        shard.lru.push_back(key);
        while shard.lru.len() > INDEX_CACHE_MAX_DIRS_PER_SHARD {
            shard.lru.pop_front();
        }
    }

    fn evict_if_needed(shard: &mut IndexCacheShard) {
        while shard.entries.len() > INDEX_CACHE_MAX_DIRS_PER_SHARD {
            if let Some(old) = shard.lru.pop_front() {
                let can_drop = shard
                    .entries
                    .get(&old)
                    .map(|entry| Arc::strong_count(&entry.value) == 1)
                    .unwrap_or(true);
                if can_drop {
                    shard.entries.remove(&old);
                    continue;
                }
                shard.lru.push_back(old);
                break;
            } else {
                break;
            }
        }
    }

    fn get_or_load(&self, dir_fd: BorrowedFd<'_>) -> CoreResult<Arc<RwLock<IndexState>>> {
        let key = dir_cache_key(dir_fd).ok_or(CoreError::NotFound)?;
        let shard = self.shard(key);
        {
            let mut guard = shard.lock();
            if let Some(entry) = guard.entries.get(&key) {
                let value = entry.value.clone();
                Self::touch_lru(&mut guard, key);
                return Ok(value);
            }
        }

        let (index, journal_size_bytes, journal_ops_since_compact) = match read_dir_index(dir_fd)? {
            Some(IndexLoadResult {
                index,
                journal_size,
                journal_ops_since_compact,
            }) => (index, journal_size, journal_ops_since_compact),
            None => (rebuild_dir_index_from_backend(dir_fd)?, 0, 0),
        };
        let state = Arc::new(RwLock::new(IndexState {
            index,
            journal_file: None,
            pending: 0,
            last_flush: Instant::now(),
            journal_size_bytes,
            journal_ops_since_compact,
        }));

        let mut guard = shard.lock();
        let value = guard
            .entries
            .entry(key)
            .or_insert_with(|| IndexCacheEntry {
                value: state.clone(),
            })
            .value
            .clone();
        Self::touch_lru(&mut guard, key);
        Self::evict_if_needed(&mut guard);
        Ok(value)
    }
}

impl Default for IndexCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct DirState {
    index: Arc<RwLock<IndexState>>,
    attr_cache: HashMap<Vec<u8>, CachedAttr>,
}

#[derive(Debug, Clone, Copy)]
struct CachedAttr {
    attr: CoreFileAttr,
    backend: BackendKey,
}

#[derive(Debug, Clone)]
struct DirSnapshot {
    entries: Arc<Vec<DirEntryInfo>>,
    has_attrs: bool,
}

#[derive(Debug)]
pub(crate) struct DirHandle {
    fd: OwnedFd,
    state: RwLock<DirState>,
    cache_key: Option<DirCacheKey>,
    snapshot: Mutex<Option<DirSnapshot>>,
}

impl DirHandle {
    fn new(fd: OwnedFd, state: DirState) -> Self {
        let cache_key = dir_cache_key(fd.as_fd());
        Self {
            fd,
            state: RwLock::new(state),
            cache_key,
            snapshot: Mutex::new(None),
        }
    }

    fn as_fd(&self) -> BorrowedFd<'_> {
        self.fd.as_fd()
    }

    fn clear_cached_attrs(&self) {
        self.state.write().attr_cache.clear();
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
struct HandleShard {
    entries: HashMap<u64, Handle>,
}

const HANDLE_SHARD_COUNT: usize = 64;
const HANDLE_SHARD_MASK: usize = HANDLE_SHARD_COUNT - 1;
const _: () = assert!(HANDLE_SHARD_COUNT.is_power_of_two());

#[derive(Debug)]
struct V2HandleTable {
    next_id: AtomicU64,
    shards: Vec<RwLock<HandleShard>>,
}

impl V2HandleTable {
    fn new() -> Self {
        let shards = (0..HANDLE_SHARD_COUNT)
            .map(|_| RwLock::new(HandleShard::default()))
            .collect();
        Self {
            next_id: AtomicU64::new(0),
            shards,
        }
    }

    fn shard(&self, id: u64) -> &RwLock<HandleShard> {
        let idx = (id as usize) & HANDLE_SHARD_MASK;
        &self.shards[idx]
    }

    fn insert_file(&self, fd: OwnedFd) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shard = self.shard(id);
        shard.write().entries.insert(id, Handle::File(Arc::new(fd)));
        id
    }

    fn insert_dir(&self, handle: DirHandle) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let shard = self.shard(id);
        shard
            .write()
            .entries
            .insert(id, Handle::Dir(Arc::new(handle)));
        id
    }

    fn get_file(&self, id: u64) -> Option<Arc<OwnedFd>> {
        let shard = self.shard(id);
        let guard = shard.read();
        match guard.entries.get(&id)? {
            Handle::File(fd) => Some(fd.clone()),
            _ => None,
        }
    }

    fn get_dir(&self, id: u64) -> Option<Arc<DirHandle>> {
        let shard = self.shard(id);
        let guard = shard.read();
        match guard.entries.get(&id)? {
            Handle::Dir(dir) => Some(dir.clone()),
            _ => None,
        }
    }

    fn remove(&self, id: u64) -> Option<Handle> {
        let shard = self.shard(id);
        shard.write().entries.remove(&id)
    }

    fn clear_dir_attr_cache(&self, key: DirCacheKey) {
        for shard in &self.shards {
            let guard = shard.read();
            for handle in guard.entries.values() {
                if let Handle::Dir(dir) = handle
                    && dir.cache_key == Some(key)
                {
                    dir.clear_cached_attrs();
                }
            }
        }
    }

    fn clear_all_dir_attr_cache(&self) {
        for shard in &self.shards {
            let guard = shard.read();
            for handle in guard.entries.values() {
                if let Handle::Dir(dir) = handle {
                    dir.clear_cached_attrs();
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum BackendName {
    Short(Vec<u8>),
    Internal(Vec<u8>),
}

impl BackendName {
    fn as_cstring(&self) -> CoreResult<CString> {
        match self {
            BackendName::Short(raw) => {
                CString::new(raw.clone()).map_err(|_| CoreError::from_errno(libc::EINVAL))
            }
            BackendName::Internal(name) => {
                cstring_from_bytes(name).map_err(|_| CoreError::from_errno(libc::EINVAL))
            }
        }
    }

    fn display_bytes(&self) -> Vec<u8> {
        match self {
            BackendName::Short(raw) => raw.clone(),
            BackendName::Internal(name) => name.clone(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, BackendName::Internal(_))
    }
}

#[derive(Debug)]
pub(crate) struct Ln2Path {
    dir_fd: Arc<OwnedFd>,
    backend_name: BackendName,
    raw_name: Vec<u8>,
    kind: SegmentKind,
}

#[derive(Debug)]
pub(crate) struct ParentCtx {
    dir_fd: Arc<OwnedFd>,
    state: DirState,
}

#[derive(Debug)]
struct ResolvedPath {
    parent_fd: Arc<OwnedFd>,
    parent_key: DirCacheKey,
    backend_name: Option<BackendName>,
    logical_name: Vec<u8>,
    kind: SegmentKind,
    exists: bool,
}

#[derive(Debug)]
pub(crate) struct RenameTarget {
    ctx: ParentCtx,
    path: ResolvedPath,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirInvalidation {
    pub primary: DirCacheKey,
    pub secondary: Option<DirCacheKey>,
}

impl DirInvalidation {
    fn new(primary: DirCacheKey, secondary: Option<DirCacheKey>) -> Self {
        let secondary = secondary.filter(|s| *s != primary);
        Self { primary, secondary }
    }

    fn for_move(src: DirCacheKey, dst: DirCacheKey) -> Self {
        Self::new(src, Some(dst))
    }

    fn apply<F: Fn(DirCacheKey)>(&self, f: F) {
        f(self.primary);
        if let Some(key) = self.secondary {
            f(key);
        }
    }
}

#[derive(Debug)]
enum NotifyEvent {
    InvalEntry(InodeId, OsString),
    InvalInode(InodeId),
    Delete(InodeId, InodeId, OsString),
}

#[derive(Default, Debug)]
struct NotifyInner {
    sender: Mutex<Option<mpsc::Sender<NotifyEvent>>>,
}

#[derive(Clone, Default, Debug)]
pub struct FsNotifier {
    inner: Arc<NotifyInner>,
}

impl FsNotifier {
    pub fn set(&self, notifier: FuserNotifier) {
        let mut guard = self.inner.sender.lock();
        if guard.is_some() {
            return;
        }
        let (tx, rx) = mpsc::channel::<NotifyEvent>();
        *guard = Some(tx);

        let _ = thread::Builder::new()
            .name("ln2-fs-notifier".to_string())
            .spawn(move || {
                while let Ok(event) = rx.recv() {
                    match event {
                        NotifyEvent::InvalEntry(parent, name) => {
                            let _ = notifier.inval_entry(parent, &name);
                        }
                        NotifyEvent::InvalInode(ino) => {
                            let _ = notifier.inval_inode(ino, 0, 0);
                        }
                        NotifyEvent::Delete(parent, child, name) => {
                            let _ = notifier.delete(parent, child, &name);
                        }
                    }
                }
            });
    }

    fn send(&self, event: NotifyEvent) {
        if let Some(sender) = self.inner.sender.lock().as_ref() {
            let _ = sender.send(event);
        }
    }

    fn inval_entry(&self, parent: InodeId, name: &OsStr) {
        self.send(NotifyEvent::InvalEntry(parent, name.to_os_string()));
    }

    fn inval_inode(&self, ino: InodeId) {
        self.send(NotifyEvent::InvalInode(ino));
    }

    fn delete(&self, parent: InodeId, child: InodeId, name: &OsStr) {
        self.send(NotifyEvent::Delete(parent, child, name.to_os_string()));
    }
}

fn cstring_from_bytes(bytes: &[u8]) -> CoreResult<CString> {
    CString::new(bytes.to_vec()).map_err(|_| CoreError::from_errno(libc::EINVAL))
}

fn set_internal_rawname(fd: BorrowedFd<'_>, raw: &[u8]) -> CoreResult<()> {
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
            return Err(CoreError::NameTooLong);
        }
        return Err(CoreError::from_errno(raw_err));
    }
    Ok(())
}

fn get_internal_rawname(fd: BorrowedFd<'_>) -> CoreResult<Vec<u8>> {
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

fn verify_backend_supports_xattr(dir_fd: BorrowedFd<'_>) -> CoreResult<()> {
    let fname = core_string_to_cstring(".ln2_xattr_check.tmp")?;
    let fd = nix::fcntl::openat(
        dir_fd,
        fname.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(core_errno_from_nix)?;
    let res = set_internal_rawname(fd.as_fd(), b"probe");
    let _ = unlinkat(dir_fd, fname.as_c_str(), UnlinkatFlags::NoRemoveDir);
    res
}

fn probe_renameat2(dir_fd: BorrowedFd<'_>) -> CoreResult<bool> {
    let final_name = core_string_to_cstring(".__ln2_renameat2_probe")?;
    let temp = match core_begin_temp_file(dir_fd, final_name.as_c_str(), "rn2") {
        Ok(v) => v,
        Err(err) if err.raw_os_error() == Some(libc::EROFS) => return Ok(false),
        Err(err) => return Err(CoreError::from(err)),
    };
    let mut dst_bytes = temp.name.as_bytes().to_vec();
    dst_bytes.extend_from_slice(b".dst");
    let dst_name = CString::new(dst_bytes).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
    let res = renameat2(
        dir_fd,
        temp.name.as_c_str(),
        dir_fd,
        dst_name.as_c_str(),
        RenameFlags::RENAME_NOREPLACE,
    );
    let _ = unlinkat(dir_fd, temp.name.as_c_str(), UnlinkatFlags::NoRemoveDir);
    let _ = unlinkat(dir_fd, dst_name.as_c_str(), UnlinkatFlags::NoRemoveDir);
    match res {
        Ok(_) => Ok(true),
        Err(
            nix::errno::Errno::ENOSYS | nix::errno::Errno::EINVAL | nix::errno::Errno::EOPNOTSUPP,
        ) => Ok(false),
        Err(err) => Err(core_errno_from_nix(err)),
    }
}

fn rebuild_dir_index_from_backend(dir_fd: BorrowedFd<'_>) -> CoreResult<DirIndex> {
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;

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
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            let raw_name = match get_internal_rawname(fd.as_fd()) {
                Ok(v) => v,
                Err(_) => continue,
            };
            index.upsert(name_bytes, raw_name);
        }
        return Ok(index);
    }

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let (res_tx, res_rx) = mpsc::channel::<(Vec<u8>, Vec<u8>)>();
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
                        let guard = rx.lock();
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
                        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                        Mode::empty(),
                    ) {
                        Ok(fd) => fd,
                        Err(_) => continue,
                    };
                    let raw_name = match get_internal_rawname(fd.as_fd()) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let _ = res_tx.send((name_bytes, raw_name));
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
    index.clear_pending_ops();
    index.clear_dirty();
    Ok(index)
}

fn load_dir_state(cache: &IndexCache, dir_fd: BorrowedFd<'_>) -> CoreResult<DirState> {
    let index = cache.get_or_load(dir_fd)?;
    Ok(DirState {
        index,
        attr_cache: HashMap::new(),
    })
}

fn mark_dirty(state: &mut DirState) {
    state.attr_cache.clear();
    let mut guard = state.index.write();
    guard.index.mark_dirty();
    guard.pending = guard.pending.saturating_add(1);
}

fn maybe_flush_index(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    strategy: IndexSync,
    force_sync: bool,
) -> CoreResult<()> {
    let plan = {
        let mut guard = state.index.write();
        let should_flush = match strategy {
            IndexSync::Always => guard.index.is_dirty() || guard.index.has_pending_ops(),
            IndexSync::Batch {
                max_pending,
                max_age,
            } => {
                guard.index.is_dirty()
                    && (guard.pending >= max_pending || guard.last_flush.elapsed() >= max_age)
            }
            IndexSync::Off => false,
        };
        let should_compact = guard.journal_size_bytes > JOURNAL_MAX_BYTES
            || guard.journal_ops_since_compact > JOURNAL_MAX_OPS;
        let need_force_sync_only =
            force_sync && !should_flush && !should_compact && guard.journal_size_bytes > 0;

        if !(should_flush || should_compact || need_force_sync_only) {
            return Ok(());
        }

        let pending_ops = guard.index.take_pending_ops();
        let snapshot = guard.index.clone();
        let pending_before = guard.pending;
        let journal_size = guard.journal_size_bytes;
        let journal_ops_since_compact = guard.journal_ops_since_compact;
        (
            pending_ops,
            snapshot,
            pending_before,
            journal_size,
            journal_ops_since_compact,
            should_compact,
            need_force_sync_only,
            guard.journal_file.take(),
        )
    };

    let (
        pending_ops,
        snapshot,
        pending_before,
        mut journal_size_bytes,
        mut journal_ops_since_compact,
        mut should_compact,
        need_force_sync_only,
        mut journal_file,
    ) = plan;

    let restore_ops = pending_ops.clone();

    let flush_res: CoreResult<(u64, u64, Option<File>)> = (|| {
        if !pending_ops.is_empty() {
            if journal_file.is_none() {
                let name = core_string_to_cstring(JOURNAL_NAME)?;
                let fd = nix::fcntl::openat(
                    dir_fd,
                    name.as_c_str(),
                    OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_CLOEXEC,
                    nix::sys::stat::Mode::from_bits_truncate(0o600),
                )
                .map_err(core_errno_from_nix)?;
                journal_file = Some(File::from(fd));
            }
            if let Some(file) = journal_file.as_mut() {
                let (_added_bytes, added_ops, size_after) =
                    append_to_journal_file(file, &pending_ops, force_sync)?;
                journal_size_bytes = size_after;
                journal_ops_since_compact = journal_ops_since_compact.saturating_add(added_ops);
                if journal_size_bytes > JOURNAL_MAX_BYTES {
                    should_compact = true;
                }
            }
        }
        if need_force_sync_only && journal_size_bytes > 0 {
            if journal_file.is_none() {
                let name = core_string_to_cstring(JOURNAL_NAME)?;
                let fd = nix::fcntl::openat(
                    dir_fd,
                    name.as_c_str(),
                    OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_APPEND | OFlag::O_CLOEXEC,
                    nix::sys::stat::Mode::from_bits_truncate(0o600),
                )
                .map_err(core_errno_from_nix)?;
                journal_file = Some(File::from(fd));
            }
            if let Some(file) = journal_file.as_mut() {
                file.sync_all().map_err(CoreError::from)?;
            }
        }

        if should_compact || journal_ops_since_compact > JOURNAL_MAX_OPS {
            journal_file = None;
            write_dir_index(dir_fd, &snapshot)?;
            reset_journal(dir_fd)?;
            journal_size_bytes = 0;
            journal_ops_since_compact = 0;
        }
        Ok((journal_size_bytes, journal_ops_since_compact, journal_file))
    })();

    if let Err(err) = flush_res {
        let mut guard = state.index.write();
        guard.index.extend_pending_ops(restore_ops);
        guard.pending = pending_before;
        guard.journal_file = None;
        return Err(err);
    }
    let (journal_size_bytes, journal_ops_since_compact, journal_file) = flush_res.unwrap();

    let mut guard = state.index.write();
    if guard.pending == pending_before {
        guard.index.clear_dirty();
        guard.index.clear_pending_ops();
        guard.pending = 0;
        guard.last_flush = Instant::now();
    } else {
        guard.pending = guard.pending.saturating_sub(pending_before);
    }
    guard.journal_size_bytes = journal_size_bytes;
    guard.journal_ops_since_compact = journal_ops_since_compact;
    guard.journal_file = journal_file;
    Ok(())
}

fn list_logical_entries(
    handle: &DirHandle,
    max_name_len: usize,
    index_sync: IndexSync,
    need_attr: bool,
) -> CoreResult<Vec<DirEntryInfo>> {
    let dir_fd = handle.as_fd();
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;

    #[derive(Debug)]
    struct ScanEntry {
        backend_name: Vec<u8>,
        kind_hint: Option<CoreFileType>,
        is_internal: bool,
        ino: u64,
    }

    #[derive(Debug)]
    struct PendingEntry {
        backend_name: Vec<u8>,
        internal_backend_name: Option<Vec<u8>>,
        is_internal: bool,
        kind: Option<CoreFileType>,
        attr: Option<CoreFileAttr>,
        backend_key: Option<BackendKey>,
        raw_name: Option<Vec<u8>>,
    }

    let dir_dev = fstat(dir_fd).ok().map(|stat| stat.st_dev);
    let mut scanned = Vec::new();
    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name_bytes = entry.file_name().to_bytes().to_vec();
        if name_bytes.is_empty() {
            continue;
        }
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        if name_bytes.starts_with(INDEX_NAME.as_bytes())
            || name_bytes.starts_with(JOURNAL_NAME.as_bytes())
        {
            continue;
        }
        let is_internal = name_bytes.starts_with(INTERNAL_PREFIX.as_bytes());
        let kind_hint = map_dirent_type(&entry);
        scanned.push(ScanEntry {
            backend_name: name_bytes,
            kind_hint,
            is_internal,
            ino: entry.ino(),
        });
    }

    let mut pending = Vec::new();
    let mut attr_miss = Vec::new();
    let mut repair_candidates = Vec::new();
    {
        let state = handle.state.read();
        let index = state.index.read();
        for entry in &scanned {
            let cached = state.attr_cache.get(&entry.backend_name).cloned();
            let attr = cached.as_ref().map(|c| c.attr);
            let mut backend_key = cached.as_ref().map(|c| c.backend);
            if backend_key.is_none() {
                backend_key = dir_dev.and_then(|dev| {
                    (entry.ino != 0).then_some(BackendKey {
                        dev,
                        ino: entry.ino,
                    })
                });
            }
            let kind = entry.kind_hint.or_else(|| attr.map(|a| a.kind));

            let internal_backend_name = entry.is_internal.then(|| entry.backend_name.clone());

            let raw_name = if entry.is_internal {
                let existing = index
                    .index
                    .get(&entry.backend_name)
                    .map(|e| e.raw_name.as_ref().to_vec());
                if existing.is_none() {
                    repair_candidates.push(entry.backend_name.clone());
                }
                existing
            } else {
                Some(entry.backend_name.clone())
            };

            let needs_attr = if need_attr {
                attr.is_none() || backend_key.is_none() || kind.is_none()
            } else {
                kind.is_none()
            };
            if needs_attr {
                attr_miss.push(entry.backend_name.clone());
            }

            pending.push(PendingEntry {
                backend_name: entry.backend_name.clone(),
                internal_backend_name,
                is_internal: entry.is_internal,
                kind,
                attr,
                backend_key,
                raw_name,
            });
        }
    }

    let mut repairs: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for backend_name in &repair_candidates {
        let c_name = match cstring_from_bytes(backend_name) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let fd = match nix::fcntl::openat(
            dir_fd,
            c_name.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(_) => continue,
        };
        if let Ok(raw_name) = get_internal_rawname(fd.as_fd())
            && raw_name.len() <= max_name_len
        {
            repairs.insert(backend_name.clone(), raw_name);
        }
    }

    let mut fetched_stats: HashMap<Vec<u8>, nix::sys::stat::FileStat> = HashMap::new();
    for name_bytes in &attr_miss {
        let c_name = match cstring_from_bytes(name_bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stat = match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(st) => st,
            Err(_) => continue,
        };
        fetched_stats.insert(name_bytes.clone(), stat);
    }

    for entry in &mut pending {
        if entry.raw_name.is_none()
            && entry.is_internal
            && let Some(name) = entry.internal_backend_name.as_ref()
            && let Some(raw) = repairs.get(name)
        {
            entry.raw_name = Some(raw.clone());
        }
        if (entry.attr.is_none() || entry.backend_key.is_none())
            && let Some(stat) = fetched_stats.get(&entry.backend_name)
        {
            let core_attr = core_attr_from_stat(stat);
            entry.attr.get_or_insert(core_attr);
            entry
                .backend_key
                .get_or_insert_with(|| backend_key_from_stat(stat));
            if entry.kind.is_none() {
                entry.kind = Some(core_attr.kind);
            }
        }
    }

    let mut entries = Vec::new();
    for entry in pending {
        let raw_name = match entry.raw_name {
            Some(v) => v,
            None => continue,
        };
        entries.push(DirEntryInfo {
            name: OsString::from_vec(raw_name),
            kind: entry.kind.unwrap_or(CoreFileType::RegularFile),
            attr: entry.attr,
            backend_name: entry.backend_name.clone(),
            backend_key: entry.backend_key,
        });
    }

    let seen_backend: HashSet<Vec<u8>> = entries.iter().map(|e| e.backend_name.clone()).collect();

    {
        let mut state = handle.state.write();
        if !repairs.is_empty() {
            {
                let mut guard = state.index.write();
                for (backend, raw_name) in &repairs {
                    guard.index.upsert(backend.clone(), raw_name.clone());
                    guard.pending = guard.pending.saturating_add(1);
                }
            }
            state.attr_cache.clear();
        }
        for (backend_name, stat) in fetched_stats {
            if seen_backend.contains(&backend_name) {
                state.attr_cache.insert(
                    backend_name,
                    CachedAttr {
                        attr: core_attr_from_stat(&stat),
                        backend: backend_key_from_stat(&stat),
                    },
                );
            }
        }
        state.attr_cache.retain(|k, _| seen_backend.contains(k));
        maybe_flush_index(dir_fd, &mut state, index_sync, false)?;
    }

    Ok(entries)
}

fn map_long_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
) -> CoreResult<Vec<u8>> {
    if let Some(entry) = {
        let guard = state.index.read();
        guard.index.backend_for_raw(raw)
    } {
        let c_name =
            cstring_from_bytes(entry.as_ref()).map_err(|_| CoreError::from_errno(libc::EILSEQ))?;
        match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => return Ok(entry.as_ref().to_vec()),
            Err(nix::errno::Errno::ENOENT) => {
                {
                    let mut guard = state.index.write();
                    guard.index.remove(entry.as_ref());
                    guard.pending = guard.pending.saturating_add(1);
                }
                state.attr_cache.clear();
            }
            Err(err) => return Err(core_errno_from_nix(err)),
        }
    }

    let hash = encode_long_name(raw);
    for suffix in 0..=MAX_COLLISION_SUFFIX {
        let candidate_bytes =
            backend_basename_from_hash(&hash, (suffix != 0).then_some(suffix)).into_bytes();
        let c_name = cstring_from_bytes(&candidate_bytes)
            .map_err(|_| CoreError::from_errno(libc::EINVAL))?;
        match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => {
                let fd = nix::fcntl::openat(
                    dir_fd,
                    c_name.as_c_str(),
                    OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                    Mode::empty(),
                )
                .map_err(core_errno_from_nix)?;
                if let Ok(raw_name) = get_internal_rawname(fd.as_fd())
                    && raw_name == raw
                {
                    {
                        let mut guard = state.index.write();
                        guard.index.upsert(candidate_bytes.clone(), raw_name);
                        guard.pending = guard.pending.saturating_add(1);
                    }
                    state.attr_cache.clear();
                    return Ok(candidate_bytes);
                }
            }
            Err(nix::errno::Errno::ENOENT) => continue,
            Err(err) => return Err(core_errno_from_nix(err)),
        }
    }

    Err(CoreError::NotFound)
}

fn map_segment_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
    max_name_len: usize,
) -> CoreResult<(BackendName, SegmentKind)> {
    if is_internal_meta(raw) {
        return Err(CoreError::InternalMeta);
    }
    if is_reserved_prefix(raw) {
        return Err(CoreError::ReservedPrefix);
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
) -> CoreResult<(BackendName, SegmentKind)> {
    if is_internal_meta(raw) {
        return Err(CoreError::InternalMeta);
    }
    let kind = classify_segment(raw, max_name_len)?;
    match kind {
        SegmentKind::Short => Ok((BackendName::Short(raw.to_vec()), kind)),
        SegmentKind::Long => {
            let hash = encode_long_name(raw);
            let base = backend_basename_from_hash(&hash, None);
            let base_bytes = base.as_bytes();
            if let Some(entry_raw) = {
                let guard = state.index.read();
                guard.index.get(base_bytes).map(|e| e.raw_name.clone())
            } && entry_raw.as_ref() == raw
            {
                return Err(CoreError::AlreadyExists);
            }
            let has_base = {
                let guard = state.index.read();
                guard.index.contains_key(base_bytes)
            };
            if !has_base {
                return Ok((BackendName::Internal(base.into_bytes()), kind));
            }
            for suffix in 1..=MAX_COLLISION_SUFFIX {
                let candidate = backend_basename_from_hash(&hash, Some(suffix));
                let candidate_bytes = candidate.as_bytes();
                let entry_raw = {
                    let guard = state.index.read();
                    guard.index.get(candidate_bytes).map(|e| e.raw_name.clone())
                };
                match entry_raw {
                    None => return Ok((BackendName::Internal(candidate.into_bytes()), kind)),
                    Some(existing) if existing.as_ref() == raw => {
                        return Err(CoreError::AlreadyExists);
                    }
                    Some(_) => continue,
                }
            }
            Err(CoreError::NoSpace)
        }
    }
}

fn select_backend_for_long_name(
    dir_index: &mut DirIndex,
    logical_raw: &[u8],
) -> CoreResult<Vec<u8>> {
    let hash = encode_long_name(logical_raw);
    if let Some(existing) = dir_index.backend_for_raw(logical_raw) {
        return Ok(existing.as_ref().to_vec());
    }
    let base = backend_basename_from_hash(&hash, None);
    if !dir_index.contains_key(base.as_bytes()) {
        return Ok(base.into_bytes());
    }
    for suffix in 1..=MAX_COLLISION_SUFFIX {
        let candidate = backend_basename_from_hash(&hash, Some(suffix));
        if !dir_index.contains_key(candidate.as_bytes()) {
            return Ok(candidate.into_bytes());
        }
    }
    Err(CoreError::NoSpace)
}

#[derive(Debug, Clone, Copy)]
enum CreateDecision {
    AlreadyExistsSameName,
    NeedNewSuffix,
}

fn handle_backend_eexist_index_missing(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    backend_name: &[u8],
    desired_raw: &[u8],
) -> CoreResult<CreateDecision> {
    let c_name = cstring_from_bytes(backend_name)?;
    let fd = nix::fcntl::openat(
        dir_fd,
        c_name.as_c_str(),
        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    let existing_raw = get_internal_rawname(fd.as_fd())?;
    {
        let mut guard = state.index.write();
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

fn refresh_dir_index_from_backend(
    dir_fd: BorrowedFd<'_>,
    backend_name: &[u8],
) -> CoreResult<Vec<u8>> {
    let c_name = cstring_from_bytes(backend_name)?;
    let fd = nix::fcntl::openat(
        dir_fd,
        c_name.as_c_str(),
        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    let raw = get_internal_rawname(fd.as_fd())?;
    Ok(raw)
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

fn open_backend_root(config: &Config) -> CoreResult<OwnedFd> {
    nix::fcntl::openat(
        config.backend_fd(),
        ".",
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)
}

fn raw_fd_for_xattr(
    core: &LongNameFsCore,
    path: &OsStr,
    write_intent: bool,
) -> CoreResult<(RawFd, Option<OwnedFd>)> {
    let access_mode = if write_intent {
        OFlag::O_RDWR
    } else {
        OFlag::O_RDONLY
    };
    if path == OsStr::new("/") {
        if !write_intent {
            return Ok((core.config.backend_fd().as_raw_fd(), None));
        }
        let fd = nix::fcntl::openat(
            core.config.backend_fd(),
            ".",
            access_mode | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        let raw = fd.as_raw_fd();
        return Ok((raw, Some(fd)));
    }
    let mapped = core.resolve_path(path)?;
    let fname = mapped.backend_name.as_cstring()?;
    let fd = nix::fcntl::openat(
        mapped.dir_fd.as_fd(),
        fname.as_c_str(),
        access_mode | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    let raw = fd.as_raw_fd();
    Ok((raw, Some(fd)))
}

pub struct LongNameFsCore {
    pub config: Arc<Config>,
    dir_cache: DirCache,
    dir_fd_cache: DirFdCache,
    index_cache: IndexCache,
    max_name_len: usize,
    index_sync: IndexSync,
    supports_renameat2: bool,
}

impl LongNameFsCore {
    pub fn new(
        config: Config,
        max_name_len: usize,
        dir_cache_ttl: Option<Duration>,
        index_sync: IndexSync,
    ) -> CoreResult<Self> {
        verify_backend_supports_xattr(config.backend_fd())?;
        let supports_renameat2 = probe_renameat2(config.backend_fd())?;
        Ok(Self {
            config: Arc::new(config),
            dir_cache: DirCache::new(dir_cache_ttl),
            dir_fd_cache: DirFdCache::new(dir_cache_ttl),
            index_cache: IndexCache::new(),
            max_name_len,
            index_sync,
            supports_renameat2,
        })
    }

    pub(crate) fn invalidate_dir(&self, dir_fd: BorrowedFd<'_>) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.invalidate_dir_by_key(key);
        }
    }

    pub(crate) fn invalidate_dir_by_key(&self, key: DirCacheKey) {
        self.dir_cache.invalidate(key);
        self.dir_fd_cache.invalidate(key);
    }

    pub(crate) fn patch_dir_cache(&self, dir_fd: BorrowedFd<'_>, op: CacheOp) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.dir_cache.patch(key, op);
        }
    }

    pub(crate) fn cached_root_fd(&self) -> CoreResult<Arc<OwnedFd>> {
        let key = dir_cache_key(self.config.backend_fd()).ok_or(CoreError::NotFound)?;
        if let Some(fd) = self.dir_fd_cache.get(key) {
            return Ok(fd);
        }
        let fd = open_backend_root(&self.config)?;
        Ok(self.dir_fd_cache.insert(key, fd))
    }

    pub(crate) fn open_dir_cached(
        &self,
        parent_fd: BorrowedFd<'_>,
        backend: &BackendName,
    ) -> CoreResult<Arc<OwnedFd>> {
        let c_name = backend.as_cstring()?;
        let stat = fstatat(parent_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)
            .map_err(core_errno_from_nix)?;
        if (stat.st_mode & libc::S_IFMT) != libc::S_IFDIR {
            return Err(CoreError::NotDir);
        }
        let key = DirCacheKey {
            dev: stat.st_dev,
            ino: stat.st_ino,
        };
        if let Some(fd) = self.dir_fd_cache.get(key) {
            return Ok(fd);
        }
        let fd = nix::fcntl::openat(
            parent_fd,
            c_name.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        Ok(self.dir_fd_cache.insert(key, fd))
    }

    pub(crate) fn load_dir_entries(
        &self,
        handle: &Arc<DirHandle>,
        need_attr: bool,
    ) -> Arc<Vec<DirEntryInfo>> {
        let key = dir_cache_key(handle.as_fd());
        if let Some(cache_key) = key
            && let Some(hit) = self.dir_cache.get(cache_key)
            && (!need_attr || hit.has_attrs)
        {
            return hit.entries;
        }

        let logical = list_logical_entries(handle, self.max_name_len, self.index_sync, need_attr)
            .unwrap_or_default();
        let has_attrs = logical.iter().all(|e| e.attr.is_some());
        if let Some(cache_key) = key {
            return self.dir_cache.insert(cache_key, logical, has_attrs);
        }
        Arc::new(logical)
    }

    pub(crate) fn load_dir_entries_snapshot(
        &self,
        handle: &Arc<DirHandle>,
        need_attr: bool,
    ) -> Arc<Vec<DirEntryInfo>> {
        {
            let guard = handle.snapshot.lock();
            if let Some(snapshot) = guard.as_ref()
                && (!need_attr || snapshot.has_attrs)
            {
                return snapshot.entries.clone();
            }
        }

        let entries = self.load_dir_entries(handle, true);
        let snapshot = DirSnapshot {
            entries: entries.clone(),
            has_attrs: entries.iter().all(|e| e.attr.is_some()),
        };
        let mut guard = handle.snapshot.lock();
        *guard = Some(snapshot);
        entries
    }

    pub(crate) fn stat_path(&self, path: &OsStr) -> CoreResult<CoreFileAttr> {
        if path == OsStr::new("/") {
            let stat = fstatat(
                self.config.backend_fd(),
                "",
                AtFlags::AT_EMPTY_PATH | AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)?;
            return Ok(core_attr_from_stat(&stat));
        }
        let mapped = self.resolve_path(path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let stat = fstatat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)?;
        Ok(core_attr_from_stat(&stat))
    }

    pub(crate) fn resolve_dir(&self, path: &OsStr) -> CoreResult<ParentCtx> {
        if path == OsStr::new("/") {
            let dir_fd = self.cached_root_fd()?;
            let state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            return Ok(ParentCtx { dir_fd, state });
        }
        let mut segs = path_segments(path);
        if segs.is_empty() {
            return Err(CoreError::NotFound);
        }

        let mut dir_fd = self.cached_root_fd()?;
        for seg in segs.drain(..) {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            let (backend, _kind) =
                map_segment_for_lookup(dir_fd.as_fd(), &mut state, &seg, self.max_name_len)?;
            maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
            let next_fd = self.open_dir_cached(dir_fd.as_fd(), &backend)?;
            dir_fd = next_fd;
        }
        let state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
        Ok(ParentCtx { dir_fd, state })
    }

    pub(crate) fn resolve_path(&self, path: &OsStr) -> CoreResult<Ln2Path> {
        if path == OsStr::new("/") {
            return Err(CoreError::from_errno(libc::EFAULT));
        }

        let segments = path_segments(path);
        if segments.is_empty() {
            return Err(CoreError::NotFound);
        }
        let mut dir_fd = self.cached_root_fd()?;
        for seg in segments[..segments.len() - 1].iter() {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
            let (backend, _) =
                map_segment_for_lookup(dir_fd.as_fd(), &mut state, seg, self.max_name_len)?;
            maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
            let next_fd = self.open_dir_cached(dir_fd.as_fd(), &backend)?;
            dir_fd = next_fd;
        }

        let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd())?;
        let raw_last = segments.last().unwrap().clone();
        let (backend_name, kind) =
            map_segment_for_lookup(dir_fd.as_fd(), &mut state, &raw_last, self.max_name_len)?;
        maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;

        Ok(Ln2Path {
            dir_fd,
            backend_name,
            raw_name: raw_last,
            kind,
        })
    }

    pub(crate) fn resolve_path_for_rename(
        &self,
        parent: &OsStr,
        name: &OsStr,
    ) -> CoreResult<RenameTarget> {
        let mut ctx = self.resolve_dir(parent)?;
        let logical_name = normalize_osstr(name);
        if is_internal_meta(&logical_name) {
            return Err(CoreError::InternalMeta);
        }
        let kind = classify_segment(&logical_name, self.max_name_len)?;
        let parent_key = dir_cache_key(ctx.dir_fd.as_fd()).ok_or(CoreError::NotFound)?;
        let map_res = map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &logical_name,
            self.max_name_len,
        );
        maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync, false)?;
        let (backend_name, exists) = match map_res {
            Ok((backend, _)) => (Some(backend), true),
            Err(CoreError::NotFound) => (None, false),
            Err(err) => return Err(err),
        };

        let parent_fd = ctx.dir_fd.clone();
        Ok(RenameTarget {
            ctx,
            path: ResolvedPath {
                parent_fd,
                parent_key,
                backend_name,
                logical_name,
                kind,
                exists,
            },
        })
    }

    pub(crate) fn do_backend_rename(
        &self,
        src_dir: BorrowedFd<'_>,
        src_backend: &BackendName,
        dst_dir: BorrowedFd<'_>,
        dst_backend: &BackendName,
        flags: u32,
    ) -> CoreResult<()> {
        let src_c = src_backend.as_cstring()?;
        let dst_c = dst_backend.as_cstring()?;
        if flags == 0 {
            return renameat(src_dir, src_c.as_c_str(), dst_dir, dst_c.as_c_str())
                .map_err(core_errno_from_nix);
        }
        if !self.supports_renameat2 {
            return Err(CoreError::Unsupported);
        }
        let rename_flags =
            RenameFlags::from_bits(flags).ok_or_else(|| CoreError::from_errno(libc::EINVAL))?;
        renameat2(
            src_dir,
            src_c.as_c_str(),
            dst_dir,
            dst_c.as_c_str(),
            rename_flags,
        )
        .map_err(core_errno_from_nix)
    }

    fn invalidate_dirs(&self, inv: &DirInvalidation) {
        inv.apply(|key| self.invalidate_dir_by_key(key));
    }

    fn rename_short_to_short(
        &self,
        src: &RenameTarget,
        dst: &RenameTarget,
        flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        let dst_backend = BackendName::Short(dst.path.logical_name.clone());
        self.do_backend_rename(
            src.ctx.dir_fd.as_fd(),
            src_backend,
            dst.ctx.dir_fd.as_fd(),
            &dst_backend,
            flags,
        )?;
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_upgrade(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        let mut dst_internal = {
            let mut guard = dst.ctx.state.index.write();
            select_backend_for_long_name(&mut guard.index, &dst.path.logical_name)?
        };

        let src_c = src_backend.as_cstring()?;
        let src_fd = match nix::fcntl::openat(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            OFlag::O_RDWR | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(nix::errno::Errno::EISDIR) => nix::fcntl::openat(
                src.ctx.dir_fd.as_fd(),
                src_c.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW | OFlag::O_DIRECTORY,
                Mode::empty(),
            )
            .map_err(core_errno_from_nix)?,
            Err(nix::errno::Errno::ELOOP) => return Err(CoreError::from_errno(libc::ELOOP)),
            Err(err) => return Err(core_errno_from_nix(err)),
        };
        set_internal_rawname(src_fd.as_fd(), &dst.path.logical_name)?;

        let mut attempt = 0;
        loop {
            let rename_res = self.do_backend_rename(
                src.ctx.dir_fd.as_fd(),
                src_backend,
                dst.ctx.dir_fd.as_fd(),
                &BackendName::Internal(dst_internal.clone()),
                flags,
            );
            match rename_res {
                Ok(()) => break,
                Err(err @ CoreError::AlreadyExists) => {
                    if attempt > 0 {
                        return Err(err);
                    }
                    let raw =
                        refresh_dir_index_from_backend(dst.ctx.dir_fd.as_fd(), &dst_internal)?;
                    {
                        let mut guard = dst.ctx.state.index.write();
                        guard.index.upsert(dst_internal.clone(), raw);
                        guard.pending = guard.pending.saturating_add(1);
                        dst_internal =
                            select_backend_for_long_name(&mut guard.index, &dst.path.logical_name)?;
                    }
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }

        {
            let mut guard = dst.ctx.state.index.write();
            guard
                .index
                .upsert(dst_internal.clone(), dst.path.logical_name.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        dst.ctx.state.attr_cache.clear();
        maybe_flush_index(
            dst.ctx.dir_fd.as_fd(),
            &mut dst.ctx.state,
            self.index_sync,
            false,
        )?;
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_downgrade(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        let dst_backend = BackendName::Short(dst.path.logical_name.clone());
        self.do_backend_rename(
            src.ctx.dir_fd.as_fd(),
            src_backend,
            dst.ctx.dir_fd.as_fd(),
            &dst_backend,
            flags,
        )?;
        let backend_name = src_backend.display_bytes();
        {
            let mut src_guard = src.ctx.state.index.write();
            if src_guard.index.remove(&backend_name).is_some() {
                src_guard.pending = src_guard.pending.saturating_add(1);
            }
        }
        src.ctx.state.attr_cache.clear();
        maybe_flush_index(
            src.ctx.dir_fd.as_fd(),
            &mut src.ctx.state,
            self.index_sync,
            false,
        )?;
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_long_to_long(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        let same_dir = src.path.parent_key == dst.path.parent_key;
        let mut dst_internal = {
            let mut guard = dst.ctx.state.index.write();
            select_backend_for_long_name(&mut guard.index, &dst.path.logical_name)?
        };

        let mut attempt = 0;
        loop {
            let res = self.do_backend_rename(
                src.ctx.dir_fd.as_fd(),
                src_backend,
                dst.ctx.dir_fd.as_fd(),
                &BackendName::Internal(dst_internal.clone()),
                flags,
            );
            match res {
                Ok(()) => break,
                Err(err @ CoreError::AlreadyExists) => {
                    if attempt > 0 {
                        return Err(err);
                    }
                    let raw =
                        refresh_dir_index_from_backend(dst.ctx.dir_fd.as_fd(), &dst_internal)?;
                    {
                        let mut guard = dst.ctx.state.index.write();
                        guard.index.upsert(dst_internal.clone(), raw);
                        guard.pending = guard.pending.saturating_add(1);
                        dst_internal =
                            select_backend_for_long_name(&mut guard.index, &dst.path.logical_name)?;
                    }
                    attempt += 1;
                }
                Err(err) => return Err(err),
            }
        }

        let dst_c = BackendName::Internal(dst_internal.clone()).as_cstring()?;
        let dst_fd = nix::fcntl::openat(
            dst.ctx.dir_fd.as_fd(),
            dst_c.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        set_internal_rawname(dst_fd.as_fd(), &dst.path.logical_name)?;

        let src_backend_name = src_backend.display_bytes();
        if same_dir {
            let mut guard = dst.ctx.state.index.write();
            if guard.index.remove(&src_backend_name).is_some() {
                guard.pending = guard.pending.saturating_add(1);
            }
            guard
                .index
                .upsert(dst_internal.clone(), dst.path.logical_name.clone());
            guard.pending = guard.pending.saturating_add(1);
        } else {
            let src_key = (src.path.parent_key.dev, src.path.parent_key.ino);
            let dst_key = (dst.path.parent_key.dev, dst.path.parent_key.ino);
            if src_key < dst_key {
                let mut src_guard = src.ctx.state.index.write();
                let mut dst_guard = dst.ctx.state.index.write();
                if src_guard.index.remove(&src_backend_name).is_some() {
                    src_guard.pending = src_guard.pending.saturating_add(1);
                }
                dst_guard
                    .index
                    .upsert(dst_internal.clone(), dst.path.logical_name.clone());
                dst_guard.pending = dst_guard.pending.saturating_add(1);
            } else {
                let mut dst_guard = dst.ctx.state.index.write();
                let mut src_guard = src.ctx.state.index.write();
                if src_guard.index.remove(&src_backend_name).is_some() {
                    src_guard.pending = src_guard.pending.saturating_add(1);
                }
                dst_guard
                    .index
                    .upsert(dst_internal.clone(), dst.path.logical_name.clone());
                dst_guard.pending = dst_guard.pending.saturating_add(1);
            }
        }
        src.ctx.state.attr_cache.clear();
        dst.ctx.state.attr_cache.clear();
        maybe_flush_index(
            src.ctx.dir_fd.as_fd(),
            &mut src.ctx.state,
            self.index_sync,
            false,
        )?;
        if src.path.parent_key != dst.path.parent_key {
            maybe_flush_index(
                dst.ctx.dir_fd.as_fd(),
                &mut dst.ctx.state,
                self.index_sync,
                false,
            )?;
        }
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    pub(crate) fn rename_with_flags(
        &self,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
        flags: u32,
    ) -> CoreResult<DirInvalidation> {
        if flags != 0 && flags != libc::RENAME_NOREPLACE {
            return Err(CoreError::Unsupported);
        }
        if flags != 0 && !self.supports_renameat2 {
            return Err(CoreError::Unsupported);
        }

        let mut src = self.resolve_path_for_rename(origin_parent, origin_name)?;
        if !src.path.exists {
            return Err(CoreError::NotFound);
        }
        let mut dst = self.resolve_path_for_rename(parent, name)?;

        match (src.path.kind, dst.path.kind) {
            (SegmentKind::Short, SegmentKind::Short) => {
                self.rename_short_to_short(&src, &dst, flags)
            }
            (SegmentKind::Long, SegmentKind::Long) => {
                self.rename_long_to_long(&mut src, &mut dst, flags)
            }
            (SegmentKind::Short, SegmentKind::Long) => {
                self.rename_upgrade(&mut src, &mut dst, flags)
            }
            (SegmentKind::Long, SegmentKind::Short) => {
                self.rename_downgrade(&mut src, &mut dst, flags)
            }
        }
    }
}

pub struct LongNameFsV2Fuser {
    core: Arc<LongNameFsCore>,
    inode_store: InodeStore,
    handles: V2HandleTable,
    max_write: NonZeroU32,
    attr_ttl: Duration,
    entry_ttl: Duration,
    notifier: FsNotifier,
}

impl LongNameFsV2Fuser {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        max_name_len: usize,
        dir_cache_ttl: Option<Duration>,
        max_write_kb: u32,
        index_sync: IndexSync,
        attr_ttl: Duration,
    ) -> CoreResult<Self> {
        let core = Arc::new(LongNameFsCore::new(
            config,
            max_name_len,
            dir_cache_ttl,
            index_sync,
        )?);
        let bytes = max_write_kb.saturating_mul(1024).max(4096);
        let max_write = NonZeroU32::new(bytes).unwrap_or_else(|| NonZeroU32::new(4096).unwrap());
        let notifier = FsNotifier::default();
        let inode_store = InodeStore::new();
        let root_fd = core.cached_root_fd()?;
        let root_stat = fstat(root_fd.as_fd()).map_err(core_errno_from_nix)?;
        let root_backend = backend_key_from_stat(&root_stat);
        inode_store.init_root(root_backend);

        Ok(Self {
            core,
            inode_store,
            handles: V2HandleTable::new(),
            max_write,
            attr_ttl,
            entry_ttl: attr_ttl,
            notifier,
        })
    }

    fn invalidate_dir(&self, dir_fd: BorrowedFd<'_>) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.invalidate_dir_by_key(key);
        } else {
            self.core.invalidate_dir(dir_fd);
            self.handles.clear_all_dir_attr_cache();
        }
    }

    fn invalidate_dir_by_key(&self, key: DirCacheKey) {
        self.core.invalidate_dir_by_key(key);
        self.handles.clear_dir_attr_cache(key);
    }

    fn apply_invalidation(&self, inv: DirInvalidation) {
        inv.apply(|key| self.invalidate_dir_by_key(key));
    }

    fn patch_dir_cache(&self, dir_fd: BorrowedFd<'_>, op: CacheOp) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.core.dir_cache.patch(key, op);
            self.handles.clear_dir_attr_cache(key);
        }
    }

    fn entry_path(&self, entry: &InodeEntry) -> CoreResult<OsString> {
        self.inode_store.get_path(entry.ino)
    }

    fn attr_for_entry(&self, entry: &InodeEntry) -> CoreResult<FuserFileAttr> {
        let path = self.entry_path(entry)?;
        let attr = self.core.stat_path(&path)?;
        Ok(fuser_attr_from_core(attr, entry.ino))
    }

    fn parent_ino_for(&self, entry: &InodeEntry) -> InodeId {
        if entry.ino == ROOT_INODE {
            ROOT_INODE
        } else {
            entry.parent
        }
    }

    fn open_dir_handle(&self, entry: &InodeEntry) -> CoreResult<DirHandle> {
        let path = self.entry_path(entry)?;
        if entry.ino == ROOT_INODE || path == OsStr::new("/") {
            let fd = open_backend_root(&self.core.config)?;
            let index = load_dir_state(&self.core.index_cache, fd.as_fd())?;
            return Ok(DirHandle::new(fd, index));
        }
        let mapped = self.core.resolve_path(&path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let fd = nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        let index = load_dir_state(&self.core.index_cache, fd.as_fd())?;
        Ok(DirHandle::new(fd, index))
    }

    fn ensure_child_entry(
        &self,
        parent: InodeId,
        name: &OsStr,
        stat: nix::sys::stat::FileStat,
        lookup_inc: u64,
    ) -> InodeEntry {
        let backend = backend_key_from_stat(&stat);
        let attr = core_attr_from_stat(&stat);
        let kind = InodeKind::from(attr.kind);
        self.inode_store.get_or_insert(
            backend,
            kind,
            ParentName {
                parent,
                name: name.to_os_string(),
            },
            lookup_inc,
        )
    }

    fn open_backend_file(&self, entry: &InodeEntry, flags: u32) -> CoreResult<OwnedFd> {
        let path = self.entry_path(entry)?;
        let mapped = self.core.resolve_path(&path)?;
        let oflag = oflag_from_bits(flags) | OFlag::O_CLOEXEC;
        let fname = mapped.backend_name.as_cstring()?;
        nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            oflag,
            Mode::from_bits_truncate(0o666),
        )
        .map_err(core_errno_from_nix)
    }

    pub fn notifier_handle(&self) -> FsNotifier {
        self.notifier.clone()
    }

    fn notify_entry_change(&self, parent: InodeId, name: &OsStr) {
        self.notifier.inval_entry(parent, name);
        self.notifier.inval_inode(parent);
    }

    fn notify_delete(&self, parent: InodeId, child: InodeId, name: &OsStr) {
        self.notifier.delete(parent, child, name);
        self.notifier.inval_inode(parent);
        self.notifier.inval_inode(child);
    }

    fn notify_inode(&self, ino: InodeId) {
        self.notifier.inval_inode(ino);
    }
}

impl FuserFilesystem for LongNameFsV2Fuser {
    fn init(
        &mut self,
        _req: &FuserRequest<'_>,
        config: &mut KernelConfig,
    ) -> Result<(), libc::c_int> {
        let _ = config.set_max_write(self.max_write.get());
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        reply: FuserReplyEntry,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(path) => path,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let child_path = crate::v2::path::make_child_path(&parent_path, name);
        let mapped = match self.core.resolve_path(&child_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match mapped.backend_name.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let stat = match fstatat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let child_entry = self.ensure_child_entry(parent, name, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child_entry.ino);
        reply.entry(&self.entry_ttl, &attr, 0);
    }

    fn getattr(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: Option<u64>,
        reply: FuserReplyAttr,
    ) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let result = if let Some(fh) = fh {
            self.handles
                .get_file(fh)
                .and_then(|fd| fstat(fd.as_fd()).ok())
                .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                .ok_or(CoreError::NotFound)
        } else {
            self.attr_for_entry(&entry)
        };
        match result {
            Ok(attr) => reply.attr(&self.attr_ttl, &attr),
            Err(err) => reply.error(core_err_to_errno(&err)),
        }
    }

    fn setattr(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: FuserReplyAttr,
    ) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if flags.unwrap_or(0) != 0 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        if path == OsStr::new("/") {
            if let Some(mode) = mode
                && let Err(err) = nix::sys::stat::fchmod(
                    self.core.config.backend_fd(),
                    Mode::from_bits_truncate(mode),
                )
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            if (uid.is_some() || gid.is_some())
                && let Err(err) = nix::unistd::fchown(
                    self.core.config.backend_fd(),
                    uid.map(Uid::from_raw),
                    gid.map(Gid::from_raw),
                )
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            if size.is_some() {
                reply.error(libc::EFAULT);
                return;
            }
            if atime.is_some() || mtime.is_some() {
                let at = timespec_from_time_or_now(atime);
                let mt = timespec_from_time_or_now(mtime);
                let times = [*at.as_ref(), *mt.as_ref()];
                let res = unsafe {
                    libc::futimens(self.core.config.backend_fd().as_raw_fd(), times.as_ptr())
                };
                if res < 0 {
                    reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
                    return;
                }
            }
            self.notify_inode(ino);
            match self.attr_for_entry(&entry) {
                Ok(attr) => reply.attr(&self.attr_ttl, &attr),
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }

        let mapped = match self.core.resolve_path(&path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match mapped.backend_name.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Some(mode) = mode
            && let Err(err) = fchmodat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                Mode::from_bits_truncate(mode),
                FchmodatFlags::FollowSymlink,
            )
        {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if (uid.is_some() || gid.is_some())
            && let Err(err) = fchownat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                uid.map(Uid::from_raw),
                gid.map(Gid::from_raw),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
        {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if let Some(size) = size {
            if let Some(fh) = fh {
                if let Some(fd) = self.handles.get_file(fh)
                    && let Err(err) = nix::unistd::ftruncate(fd.as_fd(), size as i64)
                {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            } else {
                let file = match nix::fcntl::openat(
                    mapped.dir_fd.as_fd(),
                    fname.as_c_str(),
                    OFlag::O_WRONLY | OFlag::O_CLOEXEC,
                    Mode::empty(),
                ) {
                    Ok(fd) => fd,
                    Err(err) => {
                        reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                        return;
                    }
                };
                if let Err(err) = nix::unistd::ftruncate(&file, size as i64) {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            }
        }
        if atime.is_some() || mtime.is_some() {
            let at = timespec_from_time_or_now(atime);
            let mt = timespec_from_time_or_now(mtime);
            if let Err(err) = utimensat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                &at,
                &mt,
                UtimensatFlags::NoFollowSymlink,
            ) {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        }

        self.invalidate_dir(mapped.dir_fd.as_fd());
        self.notify_inode(ino);
        match self.attr_for_entry(&entry) {
            Ok(attr) => reply.attr(&self.attr_ttl, &attr),
            Err(err) => reply.error(core_err_to_errno(&err)),
        }
    }

    fn readlink(&mut self, _req: &FuserRequest<'_>, ino: u64, reply: FuserReplyData) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mapped = match self.core.resolve_path(&path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match mapped.backend_name.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        match readlinkat(mapped.dir_fd.as_fd(), fname.as_c_str()) {
            Ok(target) => reply.data(&target.into_vec()),
            Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
        }
    }

    fn mknod(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: FuserReplyEntry,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(name);
        let (backend, kind) = match map_segment_for_create(&ctx.state, &raw, self.core.max_name_len)
        {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let backend_bytes = backend.display_bytes();
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let sflag = nix::sys::stat::SFlag::from_bits_truncate(mode);
        let perm = Mode::from_bits_truncate(mode);
        if let Err(err) = mknodat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            sflag,
            perm,
            rdev as u64,
        ) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if matches!(kind, SegmentKind::Long) {
            let fd = match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(err) => {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            };
            if let Err(err) = set_internal_rawname(fd.as_fd(), &raw) {
                reply.error(core_err_to_errno(&err));
                return;
            }
            {
                let mut guard = ctx.state.index.write();
                guard.index.upsert(backend_bytes.clone(), raw.clone());
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            let _ = maybe_flush_index(
                ctx.dir_fd.as_fd(),
                &mut ctx.state,
                self.core.index_sync,
                false,
            );
        }
        let stat = match fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(st) => st,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_bytes.clone(),
            CachedAttr {
                attr: core_attr_from_stat(&stat),
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: name.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_bytes.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(parent, name);
        self.notify_inode(child.ino);
        reply.entry(&self.entry_ttl, &attr, 0);
    }

    fn create(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: FuserReplyCreate,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(name);
        let mut backend = match map_segment_for_create(&ctx.state, &raw, self.core.max_name_len) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };

        let mut attempt = 0;
        loop {
            let fname = match backend.0.as_cstring() {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            };
            let backend_bytes = backend.0.display_bytes();
            match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                oflag_from_bits(flags as u32) | OFlag::O_CLOEXEC | OFlag::O_CREAT | OFlag::O_EXCL,
                Mode::from_bits_truncate(mode & 0o777),
            ) {
                Ok(fd) => {
                    if matches!(backend.1, SegmentKind::Long) {
                        if let Err(err) = set_internal_rawname(fd.as_fd(), &raw) {
                            reply.error(core_err_to_errno(&err));
                            return;
                        }
                        {
                            let mut guard = ctx.state.index.write();
                            guard.index.upsert(backend_bytes.clone(), raw.clone());
                            guard.pending = guard.pending.saturating_add(1);
                        }
                        ctx.state.attr_cache.clear();
                        let _ = maybe_flush_index(
                            ctx.dir_fd.as_fd(),
                            &mut ctx.state,
                            self.core.index_sync,
                            false,
                        );
                    }
                    let stat = match fstat(fd.as_fd()) {
                        Ok(st) => st,
                        Err(err) => {
                            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                            return;
                        }
                    };
                    let core_attr = core_attr_from_stat(&stat);
                    let backend_key = backend_key_from_stat(&stat);
                    ctx.state.attr_cache.insert(
                        backend_bytes.clone(),
                        CachedAttr {
                            attr: core_attr_from_stat(&stat),
                            backend: backend_key,
                        },
                    );
                    self.patch_dir_cache(
                        ctx.dir_fd.as_fd(),
                        CacheOp::Add(DirEntryInfo {
                            name: name.to_os_string(),
                            kind: core_attr.kind,
                            attr: Some(core_attr),
                            backend_name: backend_bytes.clone(),
                            backend_key: Some(backend_key),
                        }),
                    );
                    let child = self.ensure_child_entry(parent, name, stat, 1);
                    let fh = self.handles.insert_file(fd);
                    let _ = self.inode_store.inc_open(child.ino);
                    let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
                    self.notify_entry_change(parent, name);
                    self.notify_inode(child.ino);
                    reply.created(&self.entry_ttl, &attr, 0, fh, 0);
                    return;
                }
                Err(nix::errno::Errno::EEXIST) => {
                    if matches!(backend.1, SegmentKind::Short) || attempt > MAX_COLLISION_SUFFIX {
                        reply.error(libc::EEXIST);
                        return;
                    }
                    let decision = match handle_backend_eexist_index_missing(
                        ctx.dir_fd.as_fd(),
                        &mut ctx.state,
                        &backend_bytes,
                        &raw,
                    ) {
                        Ok(v) => v,
                        Err(err) => {
                            reply.error(core_err_to_errno(&err));
                            return;
                        }
                    };
                    let _ = maybe_flush_index(
                        ctx.dir_fd.as_fd(),
                        &mut ctx.state,
                        self.core.index_sync,
                        false,
                    );
                    match decision {
                        CreateDecision::AlreadyExistsSameName => {
                            reply.error(libc::EEXIST);
                            return;
                        }
                        CreateDecision::NeedNewSuffix => {
                            backend = match map_segment_for_create(
                                &ctx.state,
                                &raw,
                                self.core.max_name_len,
                            ) {
                                Ok(v) => v,
                                Err(err) => {
                                    reply.error(core_err_to_errno(&err));
                                    return;
                                }
                            };
                            attempt += 1;
                            continue;
                        }
                    }
                }
                Err(err) => {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            }
        }
    }

    fn rename(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: FuserReplyEmpty,
    ) {
        let Some(src_parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let Some(dst_parent_entry) = self.inode_store.get(newparent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let src_parent_path = match self.entry_path(&src_parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let dst_parent_path = match self.entry_path(&dst_parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut renamed_child: Option<InodeId> = None;
        let inv = match self.core.rename_with_flags(
            &src_parent_path,
            name,
            &dst_parent_path,
            newname,
            flags,
        ) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        self.apply_invalidation(inv);

        let mut dst_ctx = match self.core.resolve_dir(&dst_parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw_new = normalize_osstr(newname);
        let (backend, _) = match map_segment_for_lookup(
            dst_ctx.dir_fd.as_fd(),
            &mut dst_ctx.state,
            &raw_new,
            self.core.max_name_len,
        ) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let _ = maybe_flush_index(
            dst_ctx.dir_fd.as_fd(),
            &mut dst_ctx.state,
            self.core.index_sync,
            false,
        );
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Ok(stat) = fstatat(
            dst_ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            let child = self.ensure_child_entry(newparent, newname, stat, 0);
            renamed_child = Some(child.ino);
            let _ = self.inode_store.remove_parent_name(
                child.ino,
                &ParentName {
                    parent,
                    name: name.to_os_string(),
                },
            );
            let new_parent_name = ParentName {
                parent: newparent,
                name: newname.to_os_string(),
            };
            let _ = self
                .inode_store
                .add_parent_name(child.ino, new_parent_name.clone());
            let _ = self.inode_store.move_entry(child.ino, new_parent_name);
        }
        self.notify_entry_change(parent, name);
        self.notify_entry_change(newparent, newname);
        if let Some(child) = renamed_child {
            self.notify_inode(child);
        }
        reply.ok();
    }

    fn link(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        newparent: u64,
        newname: &OsStr,
        reply: FuserReplyEntry,
    ) {
        let Some(target_entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let target_path = match self.entry_path(&target_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let target = match self.core.resolve_path(&target_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if target.backend_name.is_internal() {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let Some(parent_entry) = self.inode_store.get(newparent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw_new = normalize_osstr(newname);
        let (dest_backend, dest_kind) =
            match map_segment_for_create(&ctx.state, &raw_new, self.core.max_name_len) {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            };
        if matches!(dest_kind, SegmentKind::Long) {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        let from_c = match target.backend_name.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let to_c = match dest_backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = linkat(
            target.dir_fd.as_fd(),
            from_c.as_c_str(),
            ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
            LinkatFlags::empty(),
        ) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        let stat = match fstatat(
            ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(st) => st,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        let backend_bytes = dest_backend.display_bytes();
        ctx.state.attr_cache.insert(
            backend_bytes.clone(),
            CachedAttr {
                attr: core_attr_from_stat(&stat),
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: newname.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_bytes.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(newparent, newname, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(newparent, newname);
        self.notify_inode(ino);
        self.notify_inode(child.ino);
        reply.entry(&self.entry_ttl, &attr, 0);
    }

    fn unlink(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(name);
        let (backend, kind) = match map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        ) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let backend_bytes = backend.display_bytes();
        let existing_stat = match fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(st) => st,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        if let Err(err) = unlinkat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::NoRemoveDir,
        ) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if matches!(kind, SegmentKind::Long) {
            {
                let mut guard = ctx.state.index.write();
                if guard.index.remove(&backend_bytes).is_some() {
                    guard.pending = guard.pending.saturating_add(1);
                }
            }
            ctx.state.attr_cache.clear();
        }
        ctx.state.attr_cache.remove(&backend_bytes);
        let _ = maybe_flush_index(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            self.core.index_sync,
            false,
        );
        let child = self.ensure_child_entry(parent, name, existing_stat, 0);
        self.patch_dir_cache(ctx.dir_fd.as_fd(), CacheOp::Remove(backend_bytes));
        let _ = self.inode_store.remove_parent_name(
            child.ino,
            &ParentName {
                parent,
                name: name.to_os_string(),
            },
        );
        self.notify_delete(parent, child.ino, name);
        reply.ok();
    }

    fn rmdir(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(name);
        let (backend, kind) = match map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        ) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let backend_bytes = backend.display_bytes();
        let existing_stat = fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .ok();
        let removal = match unlinkat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::RemoveDir,
        ) {
            Ok(()) => Ok(()),
            Err(nix::errno::Errno::ENOTEMPTY) => {
                if let Ok(target_dir_fd) = nix::fcntl::openat(
                    ctx.dir_fd.as_fd(),
                    fname.as_c_str(),
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                    Mode::empty(),
                ) {
                    if let Ok(index_cstr) = string_to_cstring(INDEX_NAME) {
                        let _ = unlinkat(
                            target_dir_fd.as_fd(),
                            index_cstr.as_c_str(),
                            UnlinkatFlags::NoRemoveDir,
                        );
                    }
                    if let Ok(journal_cstr) = string_to_cstring(JOURNAL_NAME) {
                        let _ = unlinkat(
                            target_dir_fd.as_fd(),
                            journal_cstr.as_c_str(),
                            UnlinkatFlags::NoRemoveDir,
                        );
                    }
                }
                unlinkat(
                    ctx.dir_fd.as_fd(),
                    fname.as_c_str(),
                    UnlinkatFlags::RemoveDir,
                )
                .map_err(core_errno_from_nix)
            }
            Err(err) => Err(core_errno_from_nix(err)),
        };
        if let Err(err) = removal {
            reply.error(core_err_to_errno(&err));
            return;
        }
        {
            let mut guard = ctx.state.index.write();
            if matches!(kind, SegmentKind::Long) && guard.index.remove(&backend_bytes).is_some() {
                guard.pending = guard.pending.saturating_add(1);
            }
        }
        ctx.state.attr_cache.clear();
        if let Err(err) =
            maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, IndexSync::Always, false)
        {
            reply.error(core_err_to_errno(&err));
            return;
        }
        self.patch_dir_cache(ctx.dir_fd.as_fd(), CacheOp::Remove(backend_bytes));
        if let Some(stat) = existing_stat {
            let child = self.ensure_child_entry(parent, name, stat, 0);
            let _ = self.inode_store.remove_parent_name(
                child.ino,
                &ParentName {
                    parent,
                    name: name.to_os_string(),
                },
            );
            self.notify_delete(parent, child.ino, name);
        } else {
            self.notify_entry_change(parent, name);
        }
        reply.ok();
    }

    fn symlink(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        reply: FuserReplyEntry,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(link_name);
        let (backend, kind) = match map_segment_for_create(&ctx.state, &raw, self.core.max_name_len)
        {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let backend_bytes = backend.display_bytes();
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = symlinkat(target.as_os_str(), ctx.dir_fd.as_fd(), fname.as_c_str()) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if matches!(kind, SegmentKind::Long) {
            let fd = match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(err) => {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            };
            if let Err(err) = set_internal_rawname(fd.as_fd(), &raw) {
                reply.error(core_err_to_errno(&err));
                return;
            }
            {
                let mut guard = ctx.state.index.write();
                guard.index.upsert(backend.display_bytes(), raw.clone());
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            let _ = maybe_flush_index(
                ctx.dir_fd.as_fd(),
                &mut ctx.state,
                self.core.index_sync,
                false,
            );
        }
        let stat = match fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(st) => st,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_bytes.clone(),
            CachedAttr {
                attr: core_attr_from_stat(&stat),
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: link_name.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_bytes.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, link_name, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(parent, link_name);
        self.notify_inode(child.ino);
        reply.entry(&self.entry_ttl, &attr, 0);
    }

    fn mkdir(
        &mut self,
        _req: &FuserRequest<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: FuserReplyEntry,
    ) {
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let parent_path = match self.entry_path(&parent_entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut ctx = match self.core.resolve_dir(&parent_path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let raw = normalize_osstr(name);
        let (backend, kind) = match map_segment_for_create(&ctx.state, &raw, self.core.max_name_len)
        {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let backend_bytes = backend.display_bytes();
        let fname = match backend.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = mkdirat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            Mode::from_bits_truncate(mode),
        ) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if matches!(kind, SegmentKind::Long) {
            let fd = match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(err) => {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            };
            if let Err(err) = set_internal_rawname(fd.as_fd(), &raw) {
                reply.error(core_err_to_errno(&err));
                return;
            }
            {
                let mut guard = ctx.state.index.write();
                guard.index.upsert(backend_bytes.clone(), raw.clone());
                guard.pending = guard.pending.saturating_add(1);
            }
            ctx.state.attr_cache.clear();
            let _ = maybe_flush_index(
                ctx.dir_fd.as_fd(),
                &mut ctx.state,
                self.core.index_sync,
                false,
            );
        }
        let stat = match fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Ok(st) => st,
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_bytes.clone(),
            CachedAttr {
                attr: core_attr_from_stat(&stat),
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: name.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_bytes.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        reply.entry(&self.entry_ttl, &attr, 0);
    }

    fn opendir(&mut self, _req: &FuserRequest<'_>, ino: u64, flags: i32, reply: FuserReplyOpen) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        if entry.kind != InodeKind::Directory {
            reply.error(libc::ENOTDIR);
            return;
        }
        match self.open_dir_handle(&entry) {
            Ok(handle) => {
                let fh = self.handles.insert_dir(handle);
                let _ = self.inode_store.inc_open(ino);
                reply.opened(fh, flags as u32);
            }
            Err(err) => {
                reply.error(core_err_to_errno(&err));
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: FuserReplyDirectory,
    ) {
        let Some(dir_entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let Some(handle) = self.handles.get_dir(fh) else {
            reply.error(libc::EBADF);
            return;
        };

        let mut entries: Vec<(InodeId, FuserFileType, OsString)> = Vec::new();
        entries.push((ino, FuserFileType::Directory, OsString::from(".")));
        let parent_ino = self.parent_ino_for(&dir_entry);
        let parent_kind = self
            .inode_store
            .get(parent_ino)
            .map(|p| p.kind.into())
            .unwrap_or(FuserFileType::Directory);
        entries.push((parent_ino, parent_kind, OsString::from("..")));

        let dir_listing = self.core.load_dir_entries_snapshot(&handle, false);
        for info in dir_listing.iter() {
            let mut backend_key = info.backend_key;
            let mut kind = Some(info.kind);
            let needs_stat = backend_key.map(|k| k.ino == 0).unwrap_or(true) || kind.is_none();
            if needs_stat {
                let c_name = match cstring_from_bytes(&info.backend_name) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Ok(stat) = fstatat(
                    handle.as_fd(),
                    c_name.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                ) {
                    kind.get_or_insert_with(|| core_file_type_from_mode(stat.st_mode));
                    backend_key.get_or_insert_with(|| backend_key_from_stat(&stat));
                }
            }
            let Some(kind) = kind else { continue };
            let Some(backend_key) = backend_key else {
                continue;
            };
            let child_entry = self.inode_store.get_or_insert(
                backend_key,
                InodeKind::from(kind),
                ParentName {
                    parent: ino,
                    name: info.name.clone(),
                },
                0,
            );
            let file_type = FuserFileType::from(child_entry.kind);
            entries.push((child_entry.ino, file_type, info.name.clone()));
        }

        let mut index = offset.max(0) as usize;
        while index < entries.len() {
            let (child_ino, kind, name) = &entries[index];
            let next = (index + 1) as i64;
            if reply.add(*child_ino, next, *kind, name) {
                break;
            }
            index += 1;
        }
        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: FuserReplyDirectoryPlus,
    ) {
        let Some(dir_entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let Some(handle) = self.handles.get_dir(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let dir_attr = match fstat(handle.as_fd()) {
            Ok(stat) => fuser_attr_from_core(core_attr_from_stat(&stat), ino),
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };

        let mut entries: Vec<(InodeId, OsString, FuserFileAttr)> = Vec::new();
        entries.push((ino, OsString::from("."), dir_attr));
        let parent_attr = self
            .inode_store
            .get(self.parent_ino_for(&dir_entry))
            .and_then(|p| self.attr_for_entry(&p).ok())
            .unwrap_or(dir_attr);
        entries.push((
            self.parent_ino_for(&dir_entry),
            OsString::from(".."),
            parent_attr,
        ));

        let dir_listing = self.core.load_dir_entries_snapshot(&handle, true);
        for info in dir_listing.iter() {
            let (attr, backend_key) = match (info.attr, info.backend_key) {
                (Some(attr), Some(backend)) => (attr, backend),
                _ => {
                    let c_name = match cstring_from_bytes(&info.backend_name) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    let stat = match fstatat(
                        handle.as_fd(),
                        c_name.as_c_str(),
                        AtFlags::AT_SYMLINK_NOFOLLOW,
                    ) {
                        Ok(st) => st,
                        Err(_) => continue,
                    };
                    (core_attr_from_stat(&stat), backend_key_from_stat(&stat))
                }
            };
            let child_entry = self.inode_store.get_or_insert(
                backend_key,
                InodeKind::from(attr.kind),
                ParentName {
                    parent: ino,
                    name: info.name.clone(),
                },
                0,
            );
            let attr = fuser_attr_from_core(attr, child_entry.ino);
            entries.push((child_entry.ino, info.name.clone(), attr));
        }

        let mut index = offset.max(0) as usize;
        while index < entries.len() {
            let (child_ino, name, attr) = &entries[index];
            let next = (index + 1) as i64;
            if reply.add(*child_ino, next, name, &self.entry_ttl, attr, 0) {
                break;
            }
            index += 1;
        }
        reply.ok();
    }

    fn open(&mut self, _req: &FuserRequest<'_>, ino: u64, flags: i32, reply: FuserReplyOpen) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        match self.open_backend_file(&entry, flags as u32) {
            Ok(fd) => {
                let fh = self.handles.insert_file(fd);
                let _ = self.inode_store.inc_open(ino);
                reply.opened(fh, 0);
            }
            Err(err) => reply.error(core_err_to_errno(&err)),
        }
    }

    fn read(
        &mut self,
        _req: &FuserRequest<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: FuserReplyData,
    ) {
        let Some(handle) = self.handles.get_file(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let mut buf = vec![0u8; size as usize];
        match retry_eintr(|| pread(handle.as_fd(), &mut buf, offset)) {
            Ok(read_len) => {
                buf.truncate(read_len);
                reply.data(&buf);
            }
            Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
        }
    }

    fn write(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: FuserReplyWrite,
    ) {
        let Some(handle) = self.handles.get_file(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        match retry_eintr(|| pwrite(handle.as_fd(), data, offset)) {
            Ok(written) => {
                if self.core.config.sync_data() {
                    let _ = fdatasync(handle.as_fd());
                }
                if let Some(entry) = self.inode_store.get(ino)
                    && let Ok(path) = self.entry_path(&entry)
                    && path != OsStr::new("/")
                    && let Ok(mapped) = self.core.resolve_path(&path)
                {
                    self.invalidate_dir(mapped.dir_fd.as_fd());
                }
                self.notify_inode(ino);
                reply.written(written as u32);
            }
            Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
        }
    }

    fn fsync(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: FuserReplyEmpty,
    ) {
        let Some(handle) = self.handles.get_file(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let sync_res = if datasync {
            fdatasync(handle.as_fd())
        } else {
            fsync(handle.as_fd())
        };
        if let Err(err) = sync_res {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if let Some(entry) = self.inode_store.get(ino)
            && let Ok(path) = self.entry_path(&entry)
            && path != OsStr::new("/")
            && let Ok(mapped) = self.core.resolve_path(&path)
        {
            self.invalidate_dir(mapped.dir_fd.as_fd());
        }
        self.notify_inode(ino);
        reply.ok();
    }

    fn flush(
        &mut self,
        _req: &FuserRequest<'_>,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: FuserReplyEmpty,
    ) {
        if self.handles.get_file(fh).is_none() {
            reply.error(libc::EBADF);
            return;
        }
        reply.ok();
    }

    fn access(&mut self, _req: &FuserRequest<'_>, ino: u64, mask: i32, reply: FuserReplyEmpty) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let flags = access_mask_from_bits(mask as u32);
        if path == OsStr::new("/") {
            if let Err(err) = faccessat(self.core.config.backend_fd(), ".", flags, AtFlags::empty())
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            reply.ok();
            return;
        }
        let mapped = match self.core.resolve_path(&path) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let fname = match mapped.backend_name.as_cstring() {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = faccessat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            flags,
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        reply.ok();
    }

    fn setxattr(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: FuserReplyEmpty,
    ) {
        if position != 0 {
            reply.error(libc::EINVAL);
            return;
        }
        if name.as_bytes().starts_with(b"user.ln2.") {
            reply.error(libc::EPERM);
            return;
        }
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let cname = match cstring_from_bytes(name.as_bytes()) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let (fd_raw, fd_guard) = match raw_fd_for_xattr(self.core.as_ref(), &path, true) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let _fd_guard = fd_guard;
        let res = unsafe {
            libc::fsetxattr(
                fd_raw,
                cname.as_ptr(),
                value.as_ptr() as *const libc::c_void,
                value.len(),
                flags as libc::c_int,
            )
        };
        if res < 0 {
            reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
            return;
        }
        reply.ok();
    }

    fn getxattr(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        name: &OsStr,
        size: u32,
        reply: FuserReplyXattr,
    ) {
        if name.as_bytes().starts_with(b"user.ln2.") {
            reply.error(libc::EPERM);
            return;
        }
        let cname = match cstring_from_bytes(name.as_bytes()) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let (fd_raw, fd_guard) = match raw_fd_for_xattr(self.core.as_ref(), &path, false) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let _fd_guard = fd_guard;
        if size == 0 {
            let res = unsafe { libc::fgetxattr(fd_raw, cname.as_ptr(), std::ptr::null_mut(), 0) };
            if res < 0 {
                reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
                return;
            }
            reply.size(res as u32);
            return;
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
            reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
            return;
        }
        let read_len = res as usize;
        if read_len > size as usize {
            reply.error(libc::ERANGE);
            return;
        }
        buf.truncate(read_len);
        reply.data(&buf);
    }

    fn listxattr(&mut self, _req: &FuserRequest<'_>, ino: u64, size: u32, reply: FuserReplyXattr) {
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let (fd_raw, fd_guard) = match raw_fd_for_xattr(self.core.as_ref(), &path, false) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let _fd_guard = fd_guard;
        let initial = unsafe { libc::flistxattr(fd_raw, std::ptr::null_mut(), 0) };
        if initial < 0 {
            reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
            return;
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
            reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
            return;
        }
        let list_len = res as usize;
        buf.truncate(list_len);
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
            reply.size(filtered.len() as u32);
        } else if filtered.len() > size as usize {
            reply.error(libc::ERANGE);
        } else {
            reply.data(&filtered);
        }
    }

    fn removexattr(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        if name.as_bytes().starts_with(b"user.ln2.") {
            reply.error(libc::EPERM);
            return;
        }
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let cname = match cstring_from_bytes(name.as_bytes()) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let path = match self.entry_path(&entry) {
            Ok(p) => p,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let (fd_raw, fd_guard) = match raw_fd_for_xattr(self.core.as_ref(), &path, true) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let _fd_guard = fd_guard;
        let res = unsafe { libc::fremovexattr(fd_raw, cname.as_ptr()) };
        if res < 0 {
            reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
            return;
        }
        reply.ok();
    }

    fn poll(
        &mut self,
        _req: &FuserRequest<'_>,
        _ino: u64,
        fh: u64,
        _ph: FuserPollHandle,
        _events: u32,
        _flags: u32,
        reply: FuserReplyPoll,
    ) {
        if self.handles.get_file(fh).is_none() {
            reply.error(libc::EBADF);
            return;
        }
        reply.poll(0);
    }

    fn release(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: FuserReplyEmpty,
    ) {
        self.handles.remove(fh);
        let _ = self.inode_store.dec_open(ino);
        reply.ok();
    }

    fn releasedir(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        _flags: i32,
        reply: FuserReplyEmpty,
    ) {
        self.handles.remove(fh);
        let _ = self.inode_store.dec_open(ino);
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &FuserRequest<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: FuserReplyEmpty,
    ) {
        let Some(handle) = self.handles.get_dir(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        {
            let mut state = handle.state.write();
            if let Err(err) = maybe_flush_index(handle.as_fd(), &mut state, IndexSync::Always, true)
            {
                reply.error(core_err_to_errno(&err));
                return;
            }
        }
        let sync_res = if datasync {
            fdatasync(handle.as_fd())
        } else {
            fsync(handle.as_fd())
        };
        if let Err(err) = sync_res {
            reply.error(core_err_to_errno(&core_errno_from_nix(err)));
            return;
        }
        if let Some(dir_entry) = self.inode_store.get(ino)
            && let Ok(path) = self.entry_path(&dir_entry)
        {
            if path == OsStr::new("/") {
                self.invalidate_dir(handle.as_fd());
            } else if let Ok(ctx) = self.core.resolve_dir(&path) {
                self.invalidate_dir(ctx.dir_fd.as_fd());
            }
        }
        self.notify_inode(ino);
        reply.ok();
    }

    fn forget(&mut self, _req: &FuserRequest<'_>, ino: u64, nlookup: u64) {
        let _ = self.inode_store.dec_lookup(ino, nlookup);
    }

    fn statfs(&mut self, _req: &FuserRequest<'_>, _ino: u64, reply: FuserReplyStatfs) {
        match fstatvfs(self.core.config.backend_fd()) {
            Ok(stats) => reply.statfs(
                stats.blocks(),
                stats.blocks_free(),
                stats.blocks_available(),
                stats.files(),
                stats.files_free(),
                stats.block_size() as u32,
                stats.name_max() as u32,
                stats.fragment_size() as u32,
            ),
            Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::path::MAX_SEGMENT_ON_DISK;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{Instant, SystemTime, UNIX_EPOCH};

    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            let mut path = std::env::temp_dir();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            path.push(format!("ln2_core_test_{}_{}", std::process::id(), nanos));
            fs::create_dir(&path).expect("create temp dir");
            TempDir(path)
        }

        fn path(&self) -> &PathBuf {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn core_rename_short_to_short_moves_entry() {
        let tmp = TempDir::new();
        let file_a = tmp.path().join("a");
        fs::write(&file_a, b"hello").unwrap();
        let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
        let core = LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off).unwrap();

        let inv = core
            .rename_with_flags(
                OsStr::new("/"),
                OsStr::new("a"),
                OsStr::new("/"),
                OsStr::new("b"),
                0,
            )
            .unwrap();
        assert!(tmp.path().join("b").exists());
        assert!(!file_a.exists());
        assert!(inv.secondary.is_none());
    }

    #[test]
    fn map_segment_for_create_detects_existing_long_name() {
        let state = DirState {
            index: Arc::new(RwLock::new(IndexState {
                index: DirIndex::new(),
                journal_file: None,
                pending: 0,
                last_flush: Instant::now(),
                journal_size_bytes: 0,
                journal_ops_since_compact: 0,
            })),
            attr_cache: HashMap::new(),
        };
        let raw = vec![b'x'; MAX_SEGMENT_ON_DISK + 8];
        let first = map_segment_for_create(&state, &raw, raw.len() + 4).unwrap();
        let backend_name = match first.0 {
            BackendName::Internal(ref name) => name.clone(),
            BackendName::Short(_) => panic!("expected internal backend name for long segment"),
        };
        {
            let mut guard = state.index.write();
            guard.index.upsert(backend_name.clone(), raw.clone());
        }
        let err = map_segment_for_create(&state, &raw, raw.len() + 4).unwrap_err();
        assert!(matches!(err, CoreError::AlreadyExists));
    }
}
