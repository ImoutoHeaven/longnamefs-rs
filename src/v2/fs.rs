use crate::config::Config;
use crate::util::{
    access_mask_from_bits, core_begin_temp_file, core_fsync_dir, oflag_from_bits, procfs_path_for,
    retry_eintr,
};
use crate::v2::error::{CoreError, CoreResult, core_err_to_errno};
use crate::v2::index::{
    DirIndex, FS_INTERNAL_PREFIX, IndexLoadResult, JOURNAL_MAX_BYTES, JOURNAL_MAX_OPS,
    JOURNAL_NAME, append_to_journal_file, read_dir_index, reset_journal, write_dir_index,
};
use crate::v2::inode_store::{
    BackendKey, InodeEntry, InodeId, InodeKind, InodeStore, ParentName, ROOT_INODE,
};
use crate::v2::lock::open_and_lock_backend;
use crate::v2::object_id::{
    allocate_long_object_id, bootstrap_id_allocator_if_missing, format_long_object_name,
};
use crate::v2::path::{
    INTERNAL_PREFIX, MAX_SEGMENT_ON_DISK, SegmentKind, classify_committed_segment,
    classify_segment, is_reserved_prefix, is_stable_long_object_backend_name, normalize_osstr,
};
use crate::v2::txn::{
    RollbackMode, TxnRecord, clear_txn_record, read_txn_record, rollback_inflight_txn,
    write_txn_record,
};
#[cfg(feature = "abi-7-40")]
use fuser::BackingId;
use fuser::{
    AccessFlags as FuserAccessFlags, BsdFileFlags as FuserBsdFileFlags, Errno as FuserErrno,
    FileAttr as FuserFileAttr, FileHandle as FuserFileHandle, FileType as FuserFileType,
    Filesystem as FuserFilesystem, FopenFlags, Generation as FuserGeneration,
    INodeNo as FuserInodeNo, InitFlags as FuserInitFlags, KernelConfig,
    LockOwner as FuserLockOwner, Notifier as FuserNotifier, OpenFlags as FuserOpenFlags,
    PollEvents as FuserPollEvents, PollFlags as FuserPollFlags, PollNotifier as FuserPollHandle,
    RenameFlags as FuserRenameFlags, ReplyAttr as FuserReplyAttr, ReplyCreate as FuserReplyCreate,
    ReplyData as FuserReplyData, ReplyDirectory as FuserReplyDirectory,
    ReplyDirectoryPlus as FuserReplyDirectoryPlus, ReplyEmpty as FuserReplyEmpty,
    ReplyEntry as FuserReplyEntry, ReplyOpen as FuserReplyOpen, ReplyPoll as FuserReplyPoll,
    ReplyStatfs as FuserReplyStatfs, ReplyWrite as FuserReplyWrite, ReplyXattr as FuserReplyXattr,
    TimeOrNow, WriteFlags as FuserWriteFlags,
};
use nix::dir::Dir;
use nix::fcntl::{AtFlags, OFlag, RenameFlags as NixRenameFlags, readlinkat, renameat, renameat2};
#[cfg(target_os = "linux")]
use nix::fcntl::{FallocateFlags, fallocate as nix_fallocate};
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
use parking_lot::{Condvar, Mutex, RwLock};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::cell::Cell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{CStr, CString, OsStr, OsString};
use std::fs::File;
use std::io::{self, Read};
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::AtomicI32;
use std::sync::mpsc;
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type FuserRequest<'a> = fuser::Request;

const RAWNAME_XATTR: &str = "user.ln2.rawname";
const PARALLEL_REBUILD_THRESHOLD: usize = 64;
const PARALLEL_REBUILD_WORKERS: usize = 4;
const XATTR_CHECK_NAME: &str = ".ln2_fs_xattr_check.tmp";
const RENAMEAT2_PROBE_NAME: &str = ".ln2_fs_renameat2_probe";
const CREATE_TMP_INTERNAL_PREFIX: &str = ".ln2_fs_ctmp_";
static TMP_INTERNAL_COUNTER: AtomicU64 = AtomicU64::new(0);
static OPATH_XATTR_WARNED: AtomicBool = AtomicBool::new(false);
static PROCFS_XATTR_WARNED: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static PROCFS_SYMLINK_FALLBACK_USED: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static PARALLEL_REBUILD_DUP_HELPER_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_FORCE_POST_COMMIT_FLUSH_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_FSYNC_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_PARENT_DIR_FSYNC_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_FDATASYNC_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_INTERNAL_RAWNAME_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_RENAME_BOOKKEEPING_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_POST_CLEAR_DELETE_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_LIST_ITER_SKIP_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_PROCFS_PATH_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_PROCFS_UNAVAILABLE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static TEST_FSYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_FDATASYNC_CALLS: AtomicUsize = AtomicUsize::new(0);
#[cfg(test)]
static TEST_GLOBAL_REPAIR_ANOMALIES: OnceLock<Mutex<Vec<TestRepairAnomalyRecord>>> =
    OnceLock::new();
#[cfg(test)]
static TEST_FORCE_PASSTHROUGH_RELEASE_AFTER_CHECK: OnceLock<Mutex<Option<u64>>> = OnceLock::new();
#[cfg(test)]
static TEST_PAUSE_NEXT_POST_COMMIT_FLUSH: OnceLock<Mutex<Option<TestPostCommitFlushPause>>> =
    OnceLock::new();
#[cfg(test)]
static TEST_PAUSE_NEXT_RENAME_POST_COMMIT: OnceLock<Mutex<Option<TestRenamePostCommitPause>>> =
    OnceLock::new();
#[cfg(test)]
// Fault-injection hooks stay thread-local so parallel tests cannot bleed state;
// tests that need cross-thread coordination use explicit synchronization.
thread_local! {
    static TEST_FORCE_POST_COMMIT_FLUSH_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_FSYNC_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_PARENT_DIR_FSYNC_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_FDATASYNC_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_RENAME_BOOKKEEPING_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_POST_CLEAR_DELETE_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_LIST_ITER_SKIP_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_PROCFS_PATH_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_PROCFS_UNAVAILABLE_LOCAL: Cell<bool> = const { Cell::new(false) };
    static PARALLEL_REBUILD_DUP_FORCE_FAIL_LOCAL: Cell<bool> = const { Cell::new(false) };
}

#[cfg(test)]
struct TestPostCommitFlushPause {
    target: DirCacheKey,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
}

#[cfg(test)]
struct TestRenamePostCommitPause {
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
}
#[cfg(feature = "abi-7-40")]
const PASSTHROUGH_BACKING_CACHE_MAX_ENTRIES: usize = 4096;

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughBackingCacheCaps {
    read: bool,
    write: bool,
}

#[cfg(feature = "abi-7-40")]
impl PassthroughBackingCacheCaps {
    fn for_open_flags(flags: u32) -> Self {
        let accmode = flags & (libc::O_ACCMODE as u32);
        let read = accmode != libc::O_WRONLY as u32;
        let write = accmode != libc::O_RDONLY as u32;
        Self { read, write }
    }

    fn allows_read(&self) -> bool {
        self.read
    }

    fn allows_write(&self) -> bool {
        self.write
    }

    fn satisfies(&self, needed: Self) -> bool {
        (!needed.read || self.read) && (!needed.write || self.write)
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughBackingCacheEntry {
    backing: Option<PassthroughHandleBackingWeak>,
    caps: PassthroughBackingCacheCaps,
}

#[cfg(feature = "abi-7-40")]
fn empty_passthrough_backing_cache_entry() -> PassthroughBackingCacheEntry {
    PassthroughBackingCacheEntry {
        backing: None,
        caps: PassthroughBackingCacheCaps {
            read: false,
            write: false,
        },
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
enum PassthroughBackingSlotState {
    Ready(PassthroughBackingCacheEntry),
    Creating,
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughBackingSlot {
    state: Mutex<PassthroughBackingSlotState>,
    cv: Condvar,
}

#[cfg(feature = "abi-7-40")]
impl PassthroughBackingSlot {
    fn new_empty() -> Self {
        Self {
            state: Mutex::new(PassthroughBackingSlotState::Ready(
                empty_passthrough_backing_cache_entry(),
            )),
            cv: Condvar::new(),
        }
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughBackingCache {
    max_entries: usize,
    entries: HashMap<BackendKey, Arc<PassthroughBackingSlot>>,
    lru: VecDeque<BackendKey>,
}

#[cfg(feature = "abi-7-40")]
impl PassthroughBackingCache {
    fn new(max_entries: usize) -> Self {
        Self {
            max_entries,
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    fn touch(&mut self, key: BackendKey) {
        if let Some(pos) = self.lru.iter().position(|v| *v == key) {
            self.lru.remove(pos);
        }
        self.lru.push_back(key);
    }

    fn remove(&mut self, key: BackendKey) {
        self.entries.remove(&key);
        if let Some(pos) = self.lru.iter().position(|v| *v == key) {
            self.lru.remove(pos);
        }
    }

    fn evict_if_needed(&mut self) {
        if self.entries.len() <= self.max_entries {
            return;
        }
        let mut scanned = 0usize;
        while self.entries.len() > self.max_entries && scanned < self.lru.len().max(1) {
            let Some(oldest) = self.lru.pop_front() else {
                break;
            };
            scanned += 1;
            let Some(slot) = self.entries.get(&oldest) else {
                continue;
            };
            // Avoid blocking on per-inode slot locks while holding the global cache lock.
            let evictable = match slot.state.try_lock() {
                None => false,
                Some(state) => match &*state {
                    PassthroughBackingSlotState::Creating => false,
                    PassthroughBackingSlotState::Ready(entry) => entry
                        .backing
                        .as_ref()
                        .and_then(PassthroughHandleBackingWeak::upgrade)
                        .is_none(),
                },
            };
            if evictable {
                self.entries.remove(&oldest);
            } else {
                // Keep it but rotate, so we don't get stuck scanning the same live entry.
                self.lru.push_back(oldest);
            }
        }
    }

    fn slot(&mut self, key: BackendKey) -> Arc<PassthroughBackingSlot> {
        let slot = self
            .entries
            .entry(key)
            .or_insert_with(|| Arc::new(PassthroughBackingSlot::new_empty()))
            .clone();
        self.touch(key);
        self.evict_if_needed();
        slot
    }
}

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

fn is_fs_internal_name(raw: &[u8]) -> bool {
    raw.starts_with(FS_INTERNAL_PREFIX.as_bytes())
}

const MAX_COLLISION_SUFFIX: u32 = 64;

fn walk_backend_tree<F>(root: BorrowedFd<'_>, mut visit: F) -> CoreResult<()>
where
    F: FnMut(BorrowedFd<'_>, &[u8], libc::mode_t) -> CoreResult<bool>,
{
    let mut stack = vec![dup_cloexec(root)?];
    while let Some(dir_fd) = stack.pop() {
        let mut dir = Dir::openat(
            dir_fd.as_fd(),
            ".",
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        for entry in dir.iter() {
            let entry = entry.map_err(core_errno_from_nix)?;
            let name = entry.file_name().to_bytes().to_vec();
            if name.is_empty() || name == b"." || name == b".." {
                continue;
            }
            let c_name =
                CString::new(name.clone()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
            let stat = fstatat(
                dir_fd.as_fd(),
                c_name.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)?;
            let should_descend = visit(dir_fd.as_fd(), &name, stat.st_mode)?;
            if should_descend && (stat.st_mode & libc::S_IFMT) == libc::S_IFDIR {
                let child = nix::fcntl::openat(
                    dir_fd.as_fd(),
                    c_name.as_c_str(),
                    OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                    Mode::empty(),
                )
                .map_err(core_errno_from_nix)?;
                stack.push(child);
            }
        }
    }
    Ok(())
}

fn looks_like_legacy_hash_long_name(name: &[u8]) -> bool {
    name.starts_with(b".__ln2_")
        && !name.starts_with(crate::v2::object_id::LONG_OBJECT_PREFIX)
        && name.len() > b".__ln2_".len()
}

fn stable_long_object_has_valid_committed_rawname(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
) -> CoreResult<bool> {
    match get_internal_rawname_at(dir_fd, name) {
        Ok(raw) => Ok(matches!(classify_committed_segment(&raw), Ok(SegmentKind::Long))),
        Err(err) if is_missing_rawname_xattr_error(&err) => Ok(false),
        Err(CoreError::NotFound) => Ok(false),
        Err(err) => Err(err),
    }
}

fn validate_v2_backend_format(root: BorrowedFd<'_>) -> CoreResult<bool> {
    let mut saw_committed_stable_long = false;
    walk_backend_tree(root, |dir_fd, name, _mode| {
        if name.starts_with(b".ln2_fs_rtmp_") {
            return Err(CoreError::from_errno(libc::EINVAL));
        }
        if looks_like_legacy_hash_long_name(name) {
            return Err(CoreError::from_errno(libc::EINVAL));
        }
        if is_stable_long_object_backend_name(name) {
            let c_name =
                CString::new(name.to_vec()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
            if stable_long_object_has_valid_committed_rawname(dir_fd, c_name.as_c_str())? {
                let stat = fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW)
                    .map_err(core_errno_from_nix)?;
                if (stat.st_mode & libc::S_IFMT) != libc::S_IFDIR && stat.st_nlink != 1 {
                    return Err(CoreError::from_errno(libc::EINVAL));
                }
                saw_committed_stable_long = true;
            }
        }
        Ok(true)
    })?;
    Ok(saw_committed_stable_long)
}

fn read_exact_u64_le(fd: BorrowedFd<'_>) -> CoreResult<u64> {
    let file = File::from(dup_cloexec(fd)?);
    let mut limited = file.take(9);
    let mut buf = Vec::new();
    limited.read_to_end(&mut buf).map_err(CoreError::from)?;
    if buf.len() != 8 {
        return Err(CoreError::BadFormat);
    }
    Ok(u64::from_le_bytes(buf.try_into().unwrap()))
}

pub fn validate_id_allocator_file(root: BorrowedFd<'_>) -> CoreResult<u64> {
    let fd = nix::fcntl::openat(
        root,
        c".ln2_fs_idalloc",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    let value = read_exact_u64_le(fd.as_fd())?;
    if value == 0 {
        return Err(CoreError::BadFormat);
    }
    Ok(value)
}

fn cleanup_create_tmp_residue(root: BorrowedFd<'_>) -> CoreResult<()> {
    walk_backend_tree(root, |dir_fd, name, mode| {
        if name.starts_with(b".ln2_fs_ctmp_") {
            let c_name =
                CString::new(name.to_vec()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
            let flags = if (mode & libc::S_IFMT) == libc::S_IFDIR {
                UnlinkatFlags::RemoveDir
            } else {
                UnlinkatFlags::NoRemoveDir
            };
            unlinkat(dir_fd, c_name.as_c_str(), flags).map_err(core_errno_from_nix)?;
            return Ok(false);
        }
        Ok(true)
    })?;
    Ok(())
}

fn dir_is_only_fs_internal_files(dir_fd: BorrowedFd<'_>) -> CoreResult<bool> {
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(err) => return Err(core_errno_from_nix(err)),
        };
        let name_bytes = entry.file_name().to_bytes();
        if name_bytes.is_empty() || name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        if is_fs_internal_name(name_bytes) {
            continue;
        }
        return Ok(false);
    }
    Ok(true)
}

fn best_effort_unlink_fs_internal_files(dir_fd: BorrowedFd<'_>) {
    let mut dir = match Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(v) => v,
        Err(_) => return,
    };
    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name_bytes = entry.file_name().to_bytes();
        if name_bytes.is_empty() || name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        if !is_fs_internal_name(name_bytes) {
            continue;
        }
        let c_name = match cstring_from_bytes(name_bytes) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if let Err(err) = unlinkat(dir_fd, c_name.as_c_str(), UnlinkatFlags::NoRemoveDir)
            && err == nix::errno::Errno::EISDIR
        {
            let _ = unlinkat(dir_fd, c_name.as_c_str(), UnlinkatFlags::RemoveDir);
        }
    }
}

fn cleanup_create_staging_entry(dir_fd: BorrowedFd<'_>, tmp_backend: &BackendName) {
    let Ok(tmp_c) = tmp_backend.as_cstring() else {
        return;
    };
    best_effort_unlinkat_file(dir_fd, tmp_c.as_c_str());
    best_effort_unlinkat_dir(dir_fd, tmp_c.as_c_str());
}

fn is_delete_quarantine_name(name: &[u8]) -> bool {
    name.starts_with(b".ln2_fs_delobj_")
}

fn is_dir_delete_quarantine_name(name: &[u8]) -> bool {
    name.starts_with(b".ln2_fs_deldir_")
}

fn is_malformed_long_object_basename(name: &[u8]) -> bool {
    name.starts_with(b".__ln2_obj_") && !is_stable_long_object_backend_name(name)
}

fn rawname_missing_or_malformed(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
    _max_name_len: usize,
) -> CoreResult<bool> {
    match openat_nofollow_for_xattr(dir_fd, name).and_then(|fd| get_internal_rawname(fd.as_fd())) {
        Ok(raw) => Ok(!matches!(classify_committed_segment(&raw), Ok(SegmentKind::Long))),
        Err(err) if is_missing_rawname_xattr_error(&err) || matches!(err, CoreError::NotFound) => {
            Ok(true)
        }
        Err(err) => Err(err),
    }
}

fn remove_rmdir_residue_shallow(
    parent_dir: BorrowedFd<'_>,
    dir_name: &CStr,
    max_name_len: usize,
) -> CoreResult<()> {
    let dir_fd = nix::fcntl::openat(
        parent_dir,
        dir_name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;

    let mut dir = Dir::openat(
        dir_fd.as_fd(),
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    for entry in dir.iter() {
        let entry = entry.map_err(core_errno_from_nix)?;
        let name = entry.file_name().to_bytes().to_vec();
        if name.is_empty() || name == b"." || name == b".." {
            continue;
        }

        let c_name = CString::new(name.clone()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
        let removable = is_fs_internal_name(&name)
            || is_delete_quarantine_name(&name)
            || is_dir_delete_quarantine_name(&name)
            || is_malformed_long_object_basename(&name)
            || (is_stable_long_object_backend_name(&name)
                && rawname_missing_or_malformed(dir_fd.as_fd(), c_name.as_c_str(), max_name_len)?);
        if !removable {
            continue;
        }

        let unlink_res = unlinkat(
            dir_fd.as_fd(),
            c_name.as_c_str(),
            UnlinkatFlags::NoRemoveDir,
        );
        match unlink_res {
            Ok(()) => {}
            Err(nix::errno::Errno::EISDIR) => {
                unlinkat(dir_fd.as_fd(), c_name.as_c_str(), UnlinkatFlags::RemoveDir)
                    .map_err(core_errno_from_nix)?;
            }
            Err(nix::errno::Errno::ENOENT) => {}
            Err(err) => return Err(core_errno_from_nix(err)),
        }
    }
    core_fsync_dir(dir_fd.as_fd()).map_err(CoreError::from)
}

fn ensure_dir_empty_after_residue_cleanup(
    parent_dir: BorrowedFd<'_>,
    dir_name: &CStr,
    max_name_len: usize,
) -> CoreResult<()> {
    let dir_fd = nix::fcntl::openat(
        parent_dir,
        dir_name,
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;

    let mut dir = Dir::openat(
        dir_fd.as_fd(),
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    for entry in dir.iter() {
        let entry = entry.map_err(core_errno_from_nix)?;
        let name = entry.file_name().to_bytes().to_vec();
        if name.is_empty() || name == b"." || name == b".." {
            continue;
        }

        let c_name = CString::new(name.clone()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
        let removable = is_fs_internal_name(&name)
            || is_delete_quarantine_name(&name)
            || is_dir_delete_quarantine_name(&name)
            || is_malformed_long_object_basename(&name)
            || (is_stable_long_object_backend_name(&name)
                && rawname_missing_or_malformed(dir_fd.as_fd(), c_name.as_c_str(), max_name_len)?);
        if !removable {
            return Err(CoreError::from_errno(libc::ENOTEMPTY));
        }
    }

    Ok(())
}

fn sync_long_create_staging_entry(
    parent_dir: BorrowedFd<'_>,
    tmp_backend: &BackendName,
) -> CoreResult<()> {
    let tmp_c = tmp_backend.as_cstring()?;
    let fd = openat_nofollow_for_xattr(parent_dir, tmp_c.as_c_str())?;
    sync_fd(fd.as_fd(), false).map_err(core_errno_from_nix)
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

#[derive(Clone, Debug)]
pub struct PassthroughMetaFdConfig {
    pub enabled: bool,
    pub max_meta_fds: usize,
    pub min_open_count: u32,
    pub min_lifetime: Duration,
    pub min_meta_ops: u32,
    pub cooldown: Duration,
}

impl PassthroughMetaFdConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            max_meta_fds: 0,
            min_open_count: 0,
            min_lifetime: Duration::ZERO,
            min_meta_ops: 0,
            cooldown: Duration::ZERO,
        }
    }
}

impl Default for PassthroughMetaFdConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DirCacheKey {
    dev: u64,
    ino: u64,
}

#[derive(Debug, Default)]
struct DirVisibilityState {
    live_readers: usize,
    pending_writers: usize,
    active_writer: bool,
    committed_snapshot: Option<DirSnapshot>,
}

#[derive(Debug, Default)]
struct DirVisibilityLockEntry {
    state: Mutex<DirVisibilityState>,
    cv: Condvar,
}

#[derive(Debug, Default)]
struct DirVisibilityLockTable {
    entries: RwLock<HashMap<DirCacheKey, Arc<DirVisibilityLockEntry>>>,
}

#[derive(Debug)]
struct DirVisibilityReadGuard {
    entry: Arc<DirVisibilityLockEntry>,
}

#[cfg(test)]
struct TestHeldDirReadGuard {
    _guard: DirVisibilityReadGuard,
}

#[cfg(test)]
impl Drop for TestHeldDirReadGuard {
    fn drop(&mut self) {}
}

#[derive(Debug)]
struct DirVisibilityWriteGuard {
    entry: Arc<DirVisibilityLockEntry>,
}

impl DirVisibilityLockTable {
    fn entry(&self, key: DirCacheKey) -> Arc<DirVisibilityLockEntry> {
        if let Some(entry) = self.entries.read().get(&key).cloned() {
            return entry;
        }
        let mut entries = self.entries.write();
        entries
            .entry(key)
            .or_insert_with(|| Arc::new(DirVisibilityLockEntry::default()))
            .clone()
    }

    fn read_guard(&self, key: DirCacheKey) -> DirVisibilityReadGuard {
        let entry = self.entry(key);
        {
            let mut state = entry.state.lock();
            while (state.pending_writers != 0 && !state.active_writer)
                || (state.active_writer && state.committed_snapshot.is_none())
            {
                entry.cv.wait(&mut state);
            }
            state.live_readers = state.live_readers.saturating_add(1);
        }
        DirVisibilityReadGuard { entry }
    }

    fn write_guard(&self, key: DirCacheKey) -> DirVisibilityWriteGuard {
        let entry = self.entry(key);
        {
            let mut state = entry.state.lock();
            state.pending_writers = state.pending_writers.saturating_add(1);
            while state.active_writer || state.live_readers != 0 {
                entry.cv.wait(&mut state);
            }
            state.pending_writers = state.pending_writers.saturating_sub(1);
            state.active_writer = true;
            state.committed_snapshot = None;
        }
        DirVisibilityWriteGuard { entry }
    }

    fn write_guards_ordered(&self, keys: &[DirCacheKey]) -> Vec<DirVisibilityWriteGuard> {
        let mut ordered = keys.to_vec();
        ordered.sort_by_key(|key| (key.dev, key.ino));
        ordered.dedup();
        ordered
            .into_iter()
            .map(|key| self.write_guard(key))
            .collect()
    }
}

impl Drop for DirVisibilityReadGuard {
    fn drop(&mut self) {
        let mut state = self.entry.state.lock();
        state.live_readers = state.live_readers.saturating_sub(1);
        if state.live_readers == 0 {
            self.entry.cv.notify_all();
        }
    }
}

impl Drop for DirVisibilityWriteGuard {
    fn drop(&mut self) {
        let mut state = self.entry.state.lock();
        state.active_writer = false;
        state.committed_snapshot = None;
        self.entry.cv.notify_all();
    }
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
        let vec = Arc::make_mut(&mut entry.entries);
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
struct DirFdNameIndexDir {
    expires_at: Instant,
    entries: HashMap<Vec<u8>, DirCacheKey>,
    lru: VecDeque<Vec<u8>>,
}

impl Default for DirFdNameIndexDir {
    fn default() -> Self {
        Self {
            expires_at: Instant::now(),
            entries: HashMap::new(),
            lru: VecDeque::new(),
        }
    }
}

#[derive(Debug, Default)]
struct DirFdNameIndex {
    dirs: HashMap<DirCacheKey, DirFdNameIndexDir>,
    lru: VecDeque<DirCacheKey>,
}

#[derive(Debug)]
struct DirFdCache {
    ttl: Duration,
    enabled: bool,
    entries: Mutex<HashMap<DirCacheKey, DirFdCacheEntry>>,
    lru: Mutex<VecDeque<DirCacheKey>>,
    name_index: Mutex<DirFdNameIndex>,
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
            name_index: Mutex::new(DirFdNameIndex::default()),
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

    fn invalidate_name_index_dir(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        let mut index = self.name_index.lock();
        index.dirs.remove(&key);
        if let Some(pos) = index.lru.iter().position(|k| *k == key) {
            index.lru.remove(pos);
        }
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

    fn name_index_touch_dir_lru(lru: &mut VecDeque<DirCacheKey>, key: DirCacheKey) {
        if let Some(pos) = lru.iter().position(|k| *k == key) {
            lru.remove(pos);
        }
        lru.push_back(key);
    }

    fn name_index_get(&self, parent: DirCacheKey, name: &[u8]) -> Option<DirCacheKey> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        let mut index = self.name_index.lock();
        let expired = index
            .dirs
            .get(&parent)
            .is_none_or(|dir| dir.expires_at <= now);
        if expired {
            index.dirs.remove(&parent);
            if let Some(pos) = index.lru.iter().position(|k| *k == parent) {
                index.lru.remove(pos);
            }
            return None;
        }

        let key = {
            let dir = index
                .dirs
                .get_mut(&parent)
                .expect("dir exists and is not expired");
            let key = dir.entries.get(name).copied();
            if key.is_some() {
                dir.expires_at = now + self.ttl;
                if let Some(pos) = dir.lru.iter().position(|k| k.as_slice() == name) {
                    let val = dir.lru.remove(pos).expect("pos checked");
                    dir.lru.push_back(val);
                }
            }
            key
        };
        if key.is_some() {
            Self::name_index_touch_dir_lru(&mut index.lru, parent);
        }
        key
    }

    fn name_index_insert(&self, parent: DirCacheKey, name: &[u8], child: DirCacheKey) {
        if !self.enabled {
            return;
        }
        let now = Instant::now();
        let mut index = self.name_index.lock();
        let dir = index.dirs.entry(parent).or_default();
        dir.expires_at = now + self.ttl;

        const MAX_NAMES_PER_DIR: usize = 256;
        if !dir.entries.contains_key(name) {
            dir.lru.push_back(name.to_vec());
        }
        dir.entries.insert(name.to_vec(), child);
        while dir.entries.len() > MAX_NAMES_PER_DIR {
            if let Some(old) = dir.lru.pop_front() {
                dir.entries.remove(&old);
            } else {
                break;
            }
        }

        Self::name_index_touch_dir_lru(&mut index.lru, parent);
        while index.lru.len() > DIR_FD_CACHE_MAX_DIRS {
            if let Some(old_parent) = index.lru.pop_front() {
                index.dirs.remove(&old_parent);
            }
        }
    }

    fn name_index_remove(&self, parent: DirCacheKey, name: &[u8]) {
        if !self.enabled {
            return;
        }
        let mut index = self.name_index.lock();
        let Some(dir) = index.dirs.get_mut(&parent) else {
            return;
        };
        dir.entries.remove(name);
        if let Some(pos) = dir.lru.iter().position(|k| k.as_slice() == name) {
            dir.lru.remove(pos);
        }
        if dir.entries.is_empty() {
            index.dirs.remove(&parent);
            if let Some(pos) = index.lru.iter().position(|k| *k == parent) {
                index.lru.remove(pos);
            }
        }
    }

    fn patch_name_index(&self, parent: DirCacheKey, op: CacheOp) {
        match op {
            CacheOp::Add(info) => {
                if info.kind != CoreFileType::Directory {
                    return;
                }
                let Some(backend_key) = info.backend_key else {
                    return;
                };
                self.name_index_insert(
                    parent,
                    &info.backend_name,
                    DirCacheKey {
                        dev: backend_key.dev,
                        ino: backend_key.ino,
                    },
                );
            }
            CacheOp::Remove(backend) => self.name_index_remove(parent, &backend),
            CacheOp::UpdateAttr(..) => {}
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
        ino: FuserInodeNo(ino),
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

fn fuser_inode(ino: InodeId) -> FuserInodeNo {
    FuserInodeNo(ino)
}

fn inode_id_from_fuser(ino: FuserInodeNo) -> InodeId {
    ino.0
}

fn fuser_fh(fh: u64) -> FuserFileHandle {
    FuserFileHandle(fh)
}

fn fh_from_fuser(fh: FuserFileHandle) -> u64 {
    fh.0
}

fn fuser_generation_zero() -> FuserGeneration {
    FuserGeneration(0)
}

fn fuser_errno(errno: i32) -> FuserErrno {
    FuserErrno::from_i32(errno)
}

fn fuser_errno_from_core(err: &CoreError) -> FuserErrno {
    fuser_errno(core_err_to_errno(err))
}

fn fuser_errno_from_nix(err: nix::errno::Errno) -> FuserErrno {
    fuser_errno_from_core(&core_errno_from_nix(err))
}

fn fuser_fopen_flags(bits: u32) -> FopenFlags {
    FopenFlags::from_bits_retain(bits)
}

fn open_flags_bits(flags: FuserOpenFlags) -> u32 {
    flags.0 as u32
}

fn lock_owner_from_fuser(owner: FuserLockOwner) -> u64 {
    owner.0
}

fn lock_owner_from_fuser_opt(owner: Option<FuserLockOwner>) -> Option<u64> {
    owner.map(lock_owner_from_fuser)
}

fn access_flags_bits(flags: FuserAccessFlags) -> u32 {
    flags.bits() as u32
}

fn rename_flags_bits(flags: FuserRenameFlags) -> u32 {
    flags.bits()
}

fn write_flags_bits(flags: FuserWriteFlags) -> u32 {
    flags.bits()
}

fn poll_events_bits(events: FuserPollEvents) -> u32 {
    events.bits()
}

fn poll_flags_bits(flags: FuserPollFlags) -> u32 {
    flags.bits()
}

fn bsd_file_flags_bits(flags: FuserBsdFileFlags) -> u32 {
    flags.bits()
}

struct ReplyEntryCompat(FuserReplyEntry);

impl ReplyEntryCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn entry(self, ttl: &Duration, attr: &FuserFileAttr, generation: u64) {
        self.0.entry(ttl, attr, FuserGeneration(generation));
    }
}

struct ReplyAttrCompat(FuserReplyAttr);

impl ReplyAttrCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn attr(self, ttl: &Duration, attr: &FuserFileAttr) {
        self.0.attr(ttl, attr);
    }
}

struct ReplyDataCompat(FuserReplyData);

impl ReplyDataCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn data(self, data: &[u8]) {
        self.0.data(data);
    }
}

struct ReplyEmptyCompat(FuserReplyEmpty);

impl ReplyEmptyCompat {
    fn ok(self) {
        self.0.ok();
    }

    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }
}

struct ReplyOpenCompat(FuserReplyOpen);

impl ReplyOpenCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn opened(self, fh: u64, flags: u32) {
        self.0.opened(fuser_fh(fh), fuser_fopen_flags(flags));
    }

    #[cfg(feature = "abi-7-40")]
    fn opened_passthrough(self, fh: u64, flags: u32, backing: &BackingId) {
        self.0
            .opened_passthrough(fuser_fh(fh), fuser_fopen_flags(flags), backing);
    }

    fn open_backing(&self, fd: impl AsFd) -> io::Result<BackingId> {
        self.0.open_backing(fd)
    }
}

struct ReplyWriteCompat(FuserReplyWrite);

impl ReplyWriteCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn written(self, size: u32) {
        self.0.written(size);
    }
}

struct ReplyStatfsCompat(FuserReplyStatfs);

impl ReplyStatfsCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    #[allow(clippy::too_many_arguments)]
    fn statfs(
        self,
        blocks: u64,
        bfree: u64,
        bavail: u64,
        files: u64,
        ffree: u64,
        bsize: u32,
        namelen: u32,
        frsize: u32,
    ) {
        self.0
            .statfs(blocks, bfree, bavail, files, ffree, bsize, namelen, frsize);
    }
}

struct ReplyCreateCompat {
    reply: Option<FuserReplyCreate>,
    #[cfg(test)]
    test_trace: Option<Arc<Mutex<TestCreateReplyTrace>>>,
    #[cfg(test)]
    test_open_backing_errno: Option<i32>,
}

impl ReplyCreateCompat {
    fn new(reply: FuserReplyCreate) -> Self {
        Self {
            reply: Some(reply),
            #[cfg(test)]
            test_trace: None,
            #[cfg(test)]
            test_open_backing_errno: None,
        }
    }

    #[cfg(test)]
    fn for_test(open_backing_errno: Option<i32>) -> (Self, Arc<Mutex<TestCreateReplyTrace>>) {
        let test_trace = Arc::new(Mutex::new(TestCreateReplyTrace::default()));
        (
            Self {
                reply: None,
                test_trace: Some(test_trace.clone()),
                test_open_backing_errno: open_backing_errno,
            },
            test_trace,
        )
    }

    #[cfg(test)]
    fn record_test_trace(&self, update: impl FnOnce(&mut TestCreateReplyTrace)) {
        if let Some(trace) = self.test_trace.as_ref() {
            update(&mut trace.lock());
        }
    }

    fn error(self, err: impl IntoFuserErrno) {
        if let Some(reply) = self.reply {
            reply.error(err.into_fuser_errno());
        }
    }

    fn created(self, ttl: &Duration, attr: &FuserFileAttr, generation: u64, fh: u64, flags: u32) {
        #[cfg(test)]
        self.record_test_trace(|trace| trace.created_called = true);

        if let Some(reply) = self.reply {
            reply.created(
                ttl,
                attr,
                FuserGeneration(generation),
                fuser_fh(fh),
                fuser_fopen_flags(flags),
            );
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn created_with_optional_passthrough(
        self,
        ttl: &Duration,
        attr: &FuserFileAttr,
        generation: u64,
        fh: u64,
        flags: u32,
        backing: Option<&PassthroughHandleBacking>,
    ) {
        if let Some(backing) = backing {
            #[cfg(test)]
            self.record_test_trace(|trace| trace.created_passthrough_called = true);

            if let Some(reply) = self.reply
                && let Some(real_backing) = backing.real_backing()
            {
                reply.created_passthrough(
                    ttl,
                    attr,
                    FuserGeneration(generation),
                    fuser_fh(fh),
                    fuser_fopen_flags(flags),
                    real_backing.as_ref(),
                );
            }
            return;
        }

        self.created(ttl, attr, generation, fh, flags);
    }

    #[cfg(feature = "abi-7-40")]
    fn open_backing_passthrough(&self, fd: impl AsFd) -> io::Result<PassthroughHandleBacking> {
        #[cfg(test)]
        self.record_test_trace(|trace| trace.open_backing_called = true);

        #[cfg(test)]
        if let Some(errno) = self.test_open_backing_errno {
            return Err(io::Error::from_raw_os_error(errno));
        }

        if let Some(reply) = self.reply.as_ref() {
            return reply
                .open_backing(fd)
                .map(|backing| PassthroughHandleBacking::Real(Arc::new(backing)));
        }

        #[cfg(test)]
        {
            Ok(PassthroughHandleBacking::Test(Arc::new(())))
        }

        #[cfg(not(test))]
        unreachable!("test-only passthrough backing path must not reach production");
    }
}

struct ReplyPollCompat(FuserReplyPoll);

impl ReplyPollCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn poll(self, revents: u32) {
        self.0.poll(FuserPollEvents::from_bits_retain(revents));
    }
}

struct ReplyDirectoryCompat(FuserReplyDirectory);

impl ReplyDirectoryCompat {
    fn add<T: AsRef<OsStr>>(
        &mut self,
        ino: InodeId,
        offset: i64,
        kind: FuserFileType,
        name: T,
    ) -> bool {
        self.0
            .add(fuser_inode(ino), offset.max(0) as u64, kind, name)
    }

    fn ok(self) {
        self.0.ok();
    }

    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }
}

struct ReplyDirectoryPlusCompat(FuserReplyDirectoryPlus);

impl ReplyDirectoryPlusCompat {
    fn add<T: AsRef<OsStr>>(
        &mut self,
        ino: InodeId,
        offset: i64,
        name: T,
        ttl: &Duration,
        attr: &FuserFileAttr,
        generation: u64,
    ) -> bool {
        self.0.add(
            fuser_inode(ino),
            offset.max(0) as u64,
            name,
            ttl,
            attr,
            FuserGeneration(generation),
        )
    }

    fn ok(self) {
        self.0.ok();
    }

    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }
}

struct ReplyXattrCompat(FuserReplyXattr);

impl ReplyXattrCompat {
    fn error(self, err: impl IntoFuserErrno) {
        self.0.error(err.into_fuser_errno());
    }

    fn size(self, size: u32) {
        self.0.size(size);
    }

    fn data(self, data: &[u8]) {
        self.0.data(data);
    }
}

trait IntoFuserErrno {
    fn into_fuser_errno(self) -> FuserErrno;
}

impl IntoFuserErrno for FuserErrno {
    fn into_fuser_errno(self) -> FuserErrno {
        self
    }
}

impl IntoFuserErrno for i32 {
    fn into_fuser_errno(self) -> FuserErrno {
        fuser_errno(self)
    }
}

impl IntoFuserErrno for CoreError {
    fn into_fuser_errno(self) -> FuserErrno {
        fuser_errno_from_core(&self)
    }
}

impl IntoFuserErrno for &CoreError {
    fn into_fuser_errno(self) -> FuserErrno {
        fuser_errno_from_core(self)
    }
}

impl IntoFuserErrno for nix::errno::Errno {
    fn into_fuser_errno(self) -> FuserErrno {
        fuser_errno_from_nix(self)
    }
}

fn reply_entry_zero(reply: FuserReplyEntry, ttl: &Duration, attr: &FuserFileAttr) {
    reply.entry(ttl, attr, fuser_generation_zero());
}

fn reply_created_zero(
    reply: FuserReplyCreate,
    ttl: &Duration,
    attr: &FuserFileAttr,
    fh: u64,
    flags: u32,
) {
    reply.created(
        ttl,
        attr,
        fuser_generation_zero(),
        fuser_fh(fh),
        fuser_fopen_flags(flags),
    );
}

#[cfg(feature = "abi-7-40")]
fn reply_created_passthrough_zero(
    reply: FuserReplyCreate,
    ttl: &Duration,
    attr: &FuserFileAttr,
    fh: u64,
    flags: u32,
    backing: &BackingId,
) {
    reply.created_passthrough(
        ttl,
        attr,
        fuser_generation_zero(),
        fuser_fh(fh),
        fuser_fopen_flags(flags),
        backing,
    );
}

fn reply_opened_flags(reply: FuserReplyOpen, fh: u64, flags: u32) {
    reply.opened(fuser_fh(fh), fuser_fopen_flags(flags));
}

#[cfg(feature = "abi-7-40")]
fn reply_opened_passthrough_zero(reply: FuserReplyOpen, fh: u64, backing: &BackingId) {
    reply.opened_passthrough(fuser_fh(fh), FopenFlags::empty(), backing);
}

fn reply_poll_empty(reply: FuserReplyPoll) {
    reply.poll(FuserPollEvents::empty());
}

fn reply_dir_add<T: AsRef<OsStr>>(
    reply: &mut FuserReplyDirectory,
    ino: InodeId,
    offset: u64,
    kind: FuserFileType,
    name: T,
) -> bool {
    reply.add(fuser_inode(ino), offset, kind, name)
}

fn reply_dirplus_add<T: AsRef<OsStr>>(
    reply: &mut FuserReplyDirectoryPlus,
    ino: InodeId,
    offset: u64,
    name: T,
    ttl: &Duration,
    attr: &FuserFileAttr,
) -> bool {
    reply.add(
        fuser_inode(ino),
        offset,
        name,
        ttl,
        attr,
        fuser_generation_zero(),
    )
}

fn backend_key_from_stat(stat: &nix::sys::stat::FileStat) -> BackendKey {
    BackendKey {
        dev: stat.st_dev,
        ino: stat.st_ino,
    }
}

fn dir_cache_key_from_backend(backend: BackendKey) -> DirCacheKey {
    DirCacheKey {
        dev: backend.dev,
        ino: backend.ino,
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

fn root_setattr_size_errno(size: Option<u64>) -> Option<i32> {
    if size.is_some() {
        return Some(libc::EISDIR);
    }
    None
}

fn validate_rename_flags_v2(flags: u32) -> CoreResult<()> {
    if flags == 0 {
        return Ok(());
    }
    let rename_flags =
        NixRenameFlags::from_bits(flags).ok_or_else(|| CoreError::from_errno(libc::EINVAL))?;
    if rename_flags.contains(NixRenameFlags::RENAME_NOREPLACE)
        && rename_flags.contains(NixRenameFlags::RENAME_EXCHANGE)
    {
        return Err(CoreError::from_errno(libc::EINVAL));
    }
    if rename_flags == NixRenameFlags::RENAME_NOREPLACE {
        return Ok(());
    }
    Err(CoreError::Unsupported)
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

#[derive(Default)]
struct FlushWait {
    lock: Mutex<()>,
    cv: Condvar,
}

impl std::fmt::Debug for FlushWait {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlushWait").finish()
    }
}

#[derive(Debug)]
struct IndexState {
    index: DirIndex,
    journal_file: Option<File>,
    pending: usize,
    last_flush: Instant,
    flushing: bool,
    journal_size_bytes: u64,
    journal_ops_since_compact: u64,
    flush_wait: Arc<FlushWait>,
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
        let mut scanned = 0usize;
        const MAX_SCAN: usize = 64;
        while shard.entries.len() > INDEX_CACHE_MAX_DIRS_PER_SHARD && scanned < MAX_SCAN {
            let Some(old) = shard.lru.pop_front() else {
                break;
            };
            scanned += 1;
            let can_drop = shard
                .entries
                .get(&old)
                .map(|entry| Arc::strong_count(&entry.value) == 1)
                .unwrap_or(true);
            if can_drop {
                shard.entries.remove(&old);
            } else {
                shard.lru.push_back(old);
            }
        }
    }

    fn get_or_load(
        &self,
        dir_fd: BorrowedFd<'_>,
        max_name_len: usize,
    ) -> CoreResult<Arc<RwLock<IndexState>>> {
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
                has_base_index,
                journal_size,
                journal_ops_since_compact,
            }) => {
                if !has_base_index {
                    write_dir_index(dir_fd, &index)?;
                    reset_journal(dir_fd)?;
                    (index, 0, 0)
                } else {
                    (index, journal_size, journal_ops_since_compact)
                }
            }
            None => {
                let index = rebuild_dir_index_from_backend(dir_fd, max_name_len)?;
                write_dir_index(dir_fd, &index)?;
                reset_journal(dir_fd)?;
                (index, 0, 0)
            }
        };
        let state = Arc::new(RwLock::new(IndexState {
            index,
            journal_file: None,
            pending: 0,
            last_flush: Instant::now(),
            flushing: false,
            journal_size_bytes,
            journal_ops_since_compact,
            flush_wait: Arc::new(FlushWait::default()),
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
        *self.snapshot.lock() = None;
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

    fn allocate_fh(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    fn shard(&self, id: u64) -> &RwLock<HandleShard> {
        let idx = (id as usize) & HANDLE_SHARD_MASK;
        &self.shards[idx]
    }

    fn insert_file(&self, fd: OwnedFd) -> u64 {
        let id = self.allocate_fh();
        let shard = self.shard(id);
        shard.write().entries.insert(id, Handle::File(Arc::new(fd)));
        id
    }

    fn insert_dir(&self, handle: DirHandle) -> u64 {
        let id = self.allocate_fh();
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
            let dirs: Vec<Arc<DirHandle>> = {
                let guard = shard.read();
                guard
                    .entries
                    .values()
                    .filter_map(|handle| match handle {
                        Handle::Dir(dir) if dir.cache_key == Some(key) => Some(dir.clone()),
                        _ => None,
                    })
                    .collect()
            };
            for dir in dirs {
                dir.clear_cached_attrs();
            }
        }
    }

    fn clear_all_dir_attr_cache(&self) {
        for shard in &self.shards {
            let dirs: Vec<Arc<DirHandle>> = {
                let guard = shard.read();
                guard
                    .entries
                    .values()
                    .filter_map(|handle| match handle {
                        Handle::Dir(dir) => Some(dir.clone()),
                        _ => None,
                    })
                    .collect()
            };
            for dir in dirs {
                dir.clear_cached_attrs();
            }
        }
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug, Clone)]
enum PassthroughHandleBacking {
    Real(Arc<BackingId>),
    #[cfg(test)]
    Test(Arc<()>),
}

#[cfg(feature = "abi-7-40")]
impl PassthroughHandleBacking {
    fn real_backing(&self) -> Option<&Arc<BackingId>> {
        match self {
            PassthroughHandleBacking::Real(backing) => Some(backing),
            #[cfg(test)]
            PassthroughHandleBacking::Test(_) => None,
        }
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug, Clone)]
enum PassthroughHandleBackingWeak {
    Real(Weak<BackingId>),
    #[cfg(test)]
    Test(Weak<()>),
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughHandleInner {
    backing: PassthroughHandleBacking,
    data_fd: Arc<OwnedFd>,
    open_flags: u32,
    opened_at: Instant,
    meta_ops: AtomicU32,
    promotion_inflight: AtomicBool,
    meta_fd: RwLock<Option<Arc<OwnedFd>>>,
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug, Default, Clone, Copy)]
struct PassthroughSetattrUpdate {
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    size: Option<u64>,
    atime: Option<TimeOrNow>,
    mtime: Option<TimeOrNow>,
}

#[cfg(feature = "abi-7-40")]
impl PassthroughHandleInner {
    fn downgrade(backing: &PassthroughHandleBacking) -> PassthroughHandleBackingWeak {
        match backing {
            PassthroughHandleBacking::Real(backing) => {
                PassthroughHandleBackingWeak::Real(Arc::downgrade(backing))
            }
            #[cfg(test)]
            PassthroughHandleBacking::Test(backing) => {
                PassthroughHandleBackingWeak::Test(Arc::downgrade(backing))
            }
        }
    }

    fn meta_fd(&self) -> Option<Arc<OwnedFd>> {
        self.meta_fd.read().clone()
    }

    fn set_meta_fd(&self, fd: Arc<OwnedFd>) {
        *self.meta_fd.write() = Some(fd);
    }

    fn take_meta_fd(&self) -> Option<Arc<OwnedFd>> {
        self.meta_fd.write().take()
    }
}

#[cfg(feature = "abi-7-40")]
impl PassthroughHandleBackingWeak {
    fn upgrade(&self) -> Option<PassthroughHandleBacking> {
        match self {
            PassthroughHandleBackingWeak::Real(backing) => {
                backing.upgrade().map(PassthroughHandleBacking::Real)
            }
            #[cfg(test)]
            PassthroughHandleBackingWeak::Test(backing) => {
                backing.upgrade().map(PassthroughHandleBacking::Test)
            }
        }
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughHandleTable {
    shards: Vec<RwLock<HashMap<u64, Arc<PassthroughHandleInner>>>>,
}

#[cfg(feature = "abi-7-40")]
impl Default for PassthroughHandleTable {
    fn default() -> Self {
        const PASSTHROUGH_SHARD_COUNT: usize = 64;
        Self {
            shards: (0..PASSTHROUGH_SHARD_COUNT)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
        }
    }
}

#[cfg(feature = "abi-7-40")]
impl PassthroughHandleTable {
    const PASSTHROUGH_SHARD_MASK: usize = 64 - 1;

    #[inline]
    fn shard_index(fh: u64) -> usize {
        (fh as usize) & Self::PASSTHROUGH_SHARD_MASK
    }

    #[inline]
    fn shard(&self, fh: u64) -> &RwLock<HashMap<u64, Arc<PassthroughHandleInner>>> {
        &self.shards[Self::shard_index(fh)]
    }

    fn insert_registered(
        &self,
        fh: u64,
        backing: PassthroughHandleBacking,
        data_fd: Arc<OwnedFd>,
        open_flags: u32,
        meta_fd: Option<Arc<OwnedFd>>,
    ) {
        let handle = Arc::new(PassthroughHandleInner {
            backing,
            data_fd,
            open_flags,
            opened_at: Instant::now(),
            meta_ops: AtomicU32::new(0),
            promotion_inflight: AtomicBool::new(false),
            meta_fd: RwLock::new(meta_fd),
        });
        self.shard(fh).write().insert(fh, handle);
    }

    fn contains(&self, fh: u64) -> bool {
        self.shard(fh).read().contains_key(&fh)
    }

    fn get(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        self.shard(fh).read().get(&fh).cloned()
    }

    fn remove(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        self.shard(fh).write().remove(&fh)
    }
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug)]
struct PassthroughMetaFdPolicy {
    cfg: PassthroughMetaFdConfig,
    meta_fd_count: AtomicUsize,
    cooldown_until_ms: AtomicU64,
}

#[cfg(feature = "abi-7-40")]
impl PassthroughMetaFdPolicy {
    fn new(cfg: PassthroughMetaFdConfig) -> Self {
        Self {
            cfg,
            meta_fd_count: AtomicUsize::new(0),
            cooldown_until_ms: AtomicU64::new(0),
        }
    }

    fn enabled(&self) -> bool {
        self.cfg.enabled && self.cfg.max_meta_fds > 0
    }

    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    fn in_cooldown(&self) -> bool {
        if !self.enabled() {
            return true;
        }
        Self::now_ms() < self.cooldown_until_ms.load(Ordering::Relaxed)
    }

    fn enter_cooldown(&self) {
        if !self.enabled() {
            return;
        }
        let until = Self::now_ms().saturating_add(self.cfg.cooldown.as_millis() as u64);
        self.cooldown_until_ms.store(until, Ordering::Relaxed);
    }

    fn try_acquire_slot(&self) -> bool {
        if !self.enabled() || self.in_cooldown() {
            return false;
        }
        loop {
            let current = self.meta_fd_count.load(Ordering::Relaxed);
            if current >= self.cfg.max_meta_fds {
                return false;
            }
            if self
                .meta_fd_count
                .compare_exchange(current, current + 1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    fn release_slot(&self) {
        if !self.enabled() {
            return;
        }
        let _ = self
            .meta_fd_count
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    fn should_keep_on_open(&self, open_count: u32) -> bool {
        self.enabled() && !self.in_cooldown() && open_count >= self.cfg.min_open_count
    }

    fn should_promote(&self, open_count: u32, opened_at: Instant, meta_ops: u32) -> bool {
        if !self.enabled() || self.in_cooldown() {
            return false;
        }
        if open_count < self.cfg.min_open_count {
            return false;
        }
        if meta_ops < self.cfg.min_meta_ops {
            return false;
        }
        if Instant::now().duration_since(opened_at) < self.cfg.min_lifetime {
            return false;
        }
        true
    }
}

#[cfg(feature = "abi-7-40")]
fn install_promoted_meta_fd(
    policy: &PassthroughMetaFdPolicy,
    meta_fd_slot: &RwLock<Option<Arc<OwnedFd>>>,
    fd: OwnedFd,
    acquired_slot: bool,
) {
    let mut slot = meta_fd_slot.write();
    if slot.is_none() {
        *slot = Some(Arc::new(fd));
        return;
    }
    if acquired_slot {
        policy.release_slot();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

    fn as_bytes(&self) -> &[u8] {
        match self {
            BackendName::Short(raw) => raw.as_slice(),
            BackendName::Internal(name) => name.as_slice(),
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
    parent_segments: Vec<Vec<u8>>,
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
                            let _ = notifier.inval_entry(fuser_inode(parent), &name);
                        }
                        NotifyEvent::InvalInode(ino) => {
                            let _ = notifier.inval_inode(fuser_inode(ino), 0, 0);
                        }
                        NotifyEvent::Delete(parent, child, name) => {
                            let _ = notifier.delete(fuser_inode(parent), fuser_inode(child), &name);
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

    #[cfg(test)]
    fn test_record(&self, event: NotifyEvent) {
        self.send(event);
    }
}

#[cfg(test)]
fn test_atomic_errno_load(
    _atom: &AtomicI32,
    local: &'static std::thread::LocalKey<Cell<i32>>,
) -> Option<i32> {
    let errno = local.with(Cell::get);
    (errno != 0).then_some(errno)
}

#[cfg(test)]
fn test_atomic_errno_store(
    atom: &AtomicI32,
    local: &'static std::thread::LocalKey<Cell<i32>>,
    errno: Option<i32>,
) {
    let errno = errno.unwrap_or(0);
    local.with(|slot| slot.set(errno));
    atom.store(errno, Ordering::Relaxed);
}

#[cfg(test)]
fn set_test_force_post_commit_flush_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_POST_COMMIT_FLUSH_ERRNO,
        &TEST_FORCE_POST_COMMIT_FLUSH_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_fsync_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_FSYNC_ERRNO,
        &TEST_FORCE_FSYNC_ERRNO_LOCAL,
        errno,
    );
    TEST_FSYNC_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
fn set_test_force_parent_dir_fsync_errno_inner(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_PARENT_DIR_FSYNC_ERRNO,
        &TEST_FORCE_PARENT_DIR_FSYNC_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_fdatasync_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_FDATASYNC_ERRNO,
        &TEST_FORCE_FDATASYNC_ERRNO_LOCAL,
        errno,
    );
    TEST_FDATASYNC_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
fn set_test_force_internal_rawname_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO,
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_rename_bookkeeping_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_RENAME_BOOKKEEPING_ERRNO,
        &TEST_FORCE_RENAME_BOOKKEEPING_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_post_clear_delete_errno_inner(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO,
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_list_iter_skip_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_LIST_ITER_SKIP_ERRNO,
        &TEST_FORCE_LIST_ITER_SKIP_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_procfs_path_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_PROCFS_PATH_ERRNO,
        &TEST_FORCE_PROCFS_PATH_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
fn set_test_force_procfs_unavailable(unavailable: bool) {
    TEST_FORCE_PROCFS_UNAVAILABLE.store(unavailable, Ordering::Relaxed);
    TEST_FORCE_PROCFS_UNAVAILABLE_LOCAL.with(|slot| slot.set(unavailable));
}

#[cfg(test)]
fn test_force_passthrough_release_after_check_slot() -> &'static Mutex<Option<u64>> {
    TEST_FORCE_PASSTHROUGH_RELEASE_AFTER_CHECK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_pause_next_post_commit_flush_slot() -> &'static Mutex<Option<TestPostCommitFlushPause>> {
    TEST_PAUSE_NEXT_POST_COMMIT_FLUSH.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_pause_next_rename_post_commit_slot(
) -> &'static Mutex<Option<TestRenamePostCommitPause>> {
    TEST_PAUSE_NEXT_RENAME_POST_COMMIT.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn install_test_pause_post_commit_flush(
    target: DirCacheKey,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
) {
    *test_pause_next_post_commit_flush_slot().lock() = Some(TestPostCommitFlushPause {
        target,
        ready_tx,
        release_rx,
    });
}

#[cfg(test)]
fn clear_test_pause_post_commit_flush() {
    test_pause_next_post_commit_flush_slot().lock().take();
}

#[cfg(test)]
fn install_test_pause_next_rename_post_commit(
    _fs: &LongNameFsV2Fuser,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
) {
    *test_pause_next_rename_post_commit_slot().lock() = Some(TestRenamePostCommitPause {
        ready_tx,
        release_rx,
    });
}

#[cfg(test)]
fn clear_test_pause_next_rename_post_commit() {
    test_pause_next_rename_post_commit_slot().lock().take();
}

#[cfg(test)]
fn maybe_pause_after_rename_commit_before_bookkeeping() {
    let pause = test_pause_next_rename_post_commit_slot().lock().take();
    if let Some(pause) = pause {
        let _ = pause.ready_tx.send(());
        let _ = pause.release_rx.recv();
    }
}

#[cfg(test)]
fn install_test_pause_next_txn_before_clear(
    fs: &LongNameFsV2Fuser,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
) {
    crate::v2::txn::install_test_pause_next_txn_before_clear(
        fs.core.config.backend_fd(),
        ready_tx,
        release_rx,
    )
    .expect("txn-clear pause hook target should be installable")
}

#[cfg(test)]
fn clear_test_pause_next_txn_before_clear() {
    crate::v2::txn::clear_test_pause_next_txn_before_clear()
}

#[cfg(test)]
fn set_test_force_txn_write_errno(errno: Option<i32>) {
    crate::v2::txn::set_test_force_txn_write_errno(errno)
}

#[cfg(test)]
fn set_test_force_txn_clear_errno(errno: Option<i32>) {
    crate::v2::txn::set_test_force_txn_clear_errno(errno)
}

#[cfg(test)]
fn set_test_force_txn_recovery_errno(errno: Option<i32>) {
    crate::v2::txn::set_test_force_txn_recovery_errno(errno)
}

#[cfg(test)]
fn set_test_force_parent_dir_fsync_errno(errno: Option<i32>) {
    set_test_force_parent_dir_fsync_errno_inner(errno)
}

#[cfg(test)]
fn set_test_force_post_clear_delete_errno(errno: Option<i32>) {
    set_test_force_post_clear_delete_errno_inner(errno)
}

#[cfg(test)]
fn set_test_force_rename_bookkeeping_errno_for_tests(errno: Option<i32>) {
    set_test_force_rename_bookkeeping_errno(errno)
}

#[cfg(test)]
fn wait_if_test_pause_post_commit_flush(dir_key: Option<DirCacheKey>) {
    let mut slot = test_pause_next_post_commit_flush_slot().lock();
    let should_pause = slot
        .as_ref()
        .zip(dir_key)
        .is_some_and(|(pause, key)| pause.target == key);
    if should_pause && let Some(pause) = slot.take() {
        drop(slot);
        let _ = pause.ready_tx.send(());
        let _ = pause.release_rx.recv();
    }
}

#[cfg(test)]
fn set_test_force_passthrough_release_after_check(fh: Option<u64>) {
    *test_force_passthrough_release_after_check_slot().lock() = fh;
}

#[cfg(test)]
fn consume_test_force_passthrough_release_after_check(fh: u64) -> bool {
    let mut guard = test_force_passthrough_release_after_check_slot().lock();
    if *guard == Some(fh) {
        *guard = None;
        true
    } else {
        false
    }
}

#[cfg(test)]
fn test_fsync_call_count() -> usize {
    TEST_FSYNC_CALLS.load(Ordering::Relaxed)
}

#[cfg(test)]
fn test_fdatasync_call_count() -> usize {
    TEST_FDATASYNC_CALLS.load(Ordering::Relaxed)
}

fn sync_fd(fd: BorrowedFd<'_>, datasync: bool) -> nix::Result<()> {
    #[cfg(test)]
    {
        if datasync {
            TEST_FDATASYNC_CALLS.fetch_add(1, Ordering::Relaxed);
            if let Some(errno) = test_atomic_errno_load(
                &TEST_FORCE_FDATASYNC_ERRNO,
                &TEST_FORCE_FDATASYNC_ERRNO_LOCAL,
            ) {
                return Err(nix::errno::Errno::from_raw(errno));
            }
        } else {
            TEST_FSYNC_CALLS.fetch_add(1, Ordering::Relaxed);
            if let Some(errno) =
                test_atomic_errno_load(&TEST_FORCE_FSYNC_ERRNO, &TEST_FORCE_FSYNC_ERRNO_LOCAL)
            {
                return Err(nix::errno::Errno::from_raw(errno));
            }
        }
    }

    if datasync { fdatasync(fd) } else { fsync(fd) }
}

fn sync_parent_dir_for_live_txn(dir_fd: BorrowedFd<'_>) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_PARENT_DIR_FSYNC_ERRNO,
        &TEST_FORCE_PARENT_DIR_FSYNC_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    core_fsync_dir(dir_fd).map_err(CoreError::from)
}

fn cstring_from_bytes(bytes: &[u8]) -> CoreResult<CString> {
    CString::new(bytes.to_vec()).map_err(|_| CoreError::from_errno(libc::EINVAL))
}

fn dup_cloexec(fd: BorrowedFd<'_>) -> CoreResult<OwnedFd> {
    // Use atomic cloexec duplication to avoid races between dup and setting FD_CLOEXEC.
    //
    // Note: dup semantics share the open file description (including offsets). Today v2 treats
    // these as "dirfd bases" (openat/fstatat), not as iterated `getdents` streams; if we
    // later start iterating directly on these fds, keep this in mind.
    let raw_fd = nix::fcntl::fcntl(fd, nix::fcntl::FcntlArg::F_DUPFD_CLOEXEC(0))
        .map_err(core_errno_from_nix)?;
    // SAFETY: fcntl(F_DUPFD_CLOEXEC) returns a new owned file descriptor on success.
    Ok(unsafe { OwnedFd::from_raw_fd(raw_fd) })
}

fn dup_rebuild_worker_fd(fd: BorrowedFd<'_>) -> CoreResult<OwnedFd> {
    #[cfg(test)]
    PARALLEL_REBUILD_DUP_HELPER_CALLS.fetch_add(1, Ordering::Relaxed);
    #[cfg(test)]
    if PARALLEL_REBUILD_DUP_FORCE_FAIL_LOCAL.with(Cell::get) {
        return Err(CoreError::from_errno(libc::EMFILE));
    }
    dup_cloexec(fd)
}

#[cfg(test)]
fn reset_parallel_rebuild_dup_helper_calls() {
    PARALLEL_REBUILD_DUP_HELPER_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
fn parallel_rebuild_dup_helper_calls() -> usize {
    PARALLEL_REBUILD_DUP_HELPER_CALLS.load(Ordering::Relaxed)
}

#[cfg(test)]
fn set_parallel_rebuild_dup_force_fail(enabled: bool) {
    PARALLEL_REBUILD_DUP_FORCE_FAIL_LOCAL.with(|slot| slot.set(enabled));
}

#[cfg(test)]
struct ParallelRebuildDupForceFailGuard;

#[cfg(test)]
impl Drop for ParallelRebuildDupForceFailGuard {
    fn drop(&mut self) {
        set_parallel_rebuild_dup_force_fail(false);
    }
}

#[cfg(test)]
fn force_parallel_rebuild_dup_fail() -> ParallelRebuildDupForceFailGuard {
    set_parallel_rebuild_dup_force_fail(true);
    ParallelRebuildDupForceFailGuard
}

fn openat_nofollow_for_xattr_with_errno(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
) -> CoreResult<(OwnedFd, Option<i32>)> {
    // Prefer a normal O_RDONLY fd for xattr operations: some kernels return EBADF for
    // fgetxattr/fsetxattr on O_PATH. Use O_NONBLOCK so fifo metadata probes do not block, and
    // fall back to O_PATH for entries that reject readable opens while we still need a
    // no-follow handle for path-based xattr fallback.
    match nix::fcntl::openat(
        dir_fd,
        name,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => Ok((fd, None)),
        Err(nix::errno::Errno::EISDIR) => nix::fcntl::openat(
            dir_fd,
            name,
            OFlag::O_RDONLY
                | OFlag::O_NONBLOCK
                | OFlag::O_DIRECTORY
                | OFlag::O_CLOEXEC
                | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map(|fd| (fd, None))
        .map_err(core_errno_from_nix),
        Err(nix::errno::Errno::ELOOP) => nix::fcntl::openat(
            dir_fd,
            name,
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map(|fd| (fd, Some(libc::ELOOP)))
        .map_err(core_errno_from_nix),
        Err(nix::errno::Errno::ENXIO) => nix::fcntl::openat(
            dir_fd,
            name,
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map(|fd| (fd, Some(libc::ENXIO)))
        .map_err(core_errno_from_nix),
        Err(err) => Err(core_errno_from_nix(err)),
    }
}

fn openat_nofollow_for_xattr(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<OwnedFd> {
    openat_nofollow_for_xattr_with_errno(dir_fd, name).map(|(fd, _)| fd)
}

fn set_internal_rawname_at(dir_fd: BorrowedFd<'_>, name: &CStr, raw: &[u8]) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO,
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }
    let (fd, procfs_original_errno) = openat_nofollow_for_xattr_with_errno(dir_fd, name)?;
    let procfs_original_errno = procfs_original_errno.unwrap_or(libc::EBADF);
    match set_internal_rawname(fd.as_fd(), raw) {
        Ok(()) => Ok(()),
        Err(CoreError::Io(ref ioe)) if ioe.raw_os_error() == Some(libc::EBADF) => {
            if let Some(proc_path) = procfs_path_for(dir_fd, name) {
                mark_procfs_symlink_fallback();
                return match set_internal_rawname_via_procfs_path(proc_path.as_c_str(), name, raw) {
                    Ok(()) => Ok(()),
                    Err(err) if is_procfs_unavailable(&err) => {
                        Err(CoreError::from_errno(procfs_original_errno))
                    }
                    Err(err) => Err(err),
                };
            }
            // Some kernels reject xattr operations on metadata fds; reopen with a normal
            // access mode and retry (similar to v1's fsync workaround).
            let reopened = match nix::fcntl::openat(
                dir_fd,
                name,
                OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            ) {
                Ok(fd) => fd,
                Err(nix::errno::Errno::ELOOP) => {
                    if let Some(proc_path) = procfs_path_for(dir_fd, name) {
                        match set_internal_rawname_via_procfs_path(proc_path.as_c_str(), name, raw)
                        {
                            Ok(()) => return Ok(()),
                            Err(err) if is_procfs_unavailable(&err) => {
                                return Err(CoreError::from_errno(procfs_original_errno));
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    return Err(CoreError::from_errno(procfs_original_errno));
                }
                Err(nix::errno::Errno::EISDIR) => nix::fcntl::openat(
                    dir_fd,
                    name,
                    OFlag::O_RDONLY
                        | OFlag::O_NONBLOCK
                        | OFlag::O_DIRECTORY
                        | OFlag::O_CLOEXEC
                        | OFlag::O_NOFOLLOW,
                    Mode::empty(),
                )
                .map_err(core_errno_from_nix)?,
                Err(err) => return Err(core_errno_from_nix(err)),
            };
            set_internal_rawname(reopened.as_fd(), raw)
        }
        Err(err) => Err(err),
    }
}

fn select_create_tmp_internal_name(dir_fd: BorrowedFd<'_>) -> CoreResult<Vec<u8>> {
    for _ in 0..256 {
        let n = TMP_INTERNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = format!("{CREATE_TMP_INTERNAL_PREFIX}{n:x}");
        let bytes = candidate.into_bytes();
        let c = cstring_from_bytes(&bytes)?;
        match fstatat(dir_fd, c.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => continue,
            Err(nix::errno::Errno::ENOENT) => return Ok(bytes),
            Err(err) => return Err(core_errno_from_nix(err)),
        }
    }
    Err(CoreError::NoSpace)
}

fn rename_noreplace_same_dir(
    core: &LongNameFsCore,
    dir_fd: BorrowedFd<'_>,
    src: &BackendName,
    dst: &BackendName,
) -> CoreResult<()> {
    if core.supports_renameat2 {
        return core.do_backend_rename(dir_fd, src, dir_fd, dst, libc::RENAME_NOREPLACE);
    }
    let dst_c = dst.as_cstring()?;
    match fstatat(dir_fd, dst_c.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(_) => return Err(CoreError::AlreadyExists),
        Err(nix::errno::Errno::ENOENT) => {}
        Err(err) => return Err(core_errno_from_nix(err)),
    }
    core.do_backend_rename(dir_fd, src, dir_fd, dst, 0)
}

fn rename_noreplace(
    core: &LongNameFsCore,
    src_dir: BorrowedFd<'_>,
    src: &BackendName,
    dst_dir: BorrowedFd<'_>,
    dst: &BackendName,
) -> CoreResult<()> {
    if core.supports_renameat2 {
        return core.do_backend_rename(src_dir, src, dst_dir, dst, libc::RENAME_NOREPLACE);
    }
    if backend_entry_exists(dst_dir, dst)? {
        return Err(CoreError::AlreadyExists);
    }
    core.do_backend_rename(src_dir, src, dst_dir, dst, 0)
}

fn backend_entry_exists(dir_fd: BorrowedFd<'_>, backend: &BackendName) -> CoreResult<bool> {
    let backend_c = backend.as_cstring()?;
    match fstatat(dir_fd, backend_c.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(nix::errno::Errno::ENOENT) => Ok(false),
        Err(err) => Err(core_errno_from_nix(err)),
    }
}

fn openat_nofollow_for_sync(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<OwnedFd> {
    match nix::fcntl::openat(
        dir_fd,
        name,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => Ok(fd),
        Err(nix::errno::Errno::EISDIR) => nix::fcntl::openat(
            dir_fd,
            name,
            OFlag::O_RDONLY
                | OFlag::O_NONBLOCK
                | OFlag::O_DIRECTORY
                | OFlag::O_CLOEXEC
                | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix),
        Err(err) => Err(core_errno_from_nix(err)),
    }
}

fn sync_mutated_backend_entry(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<()> {
    let fd = openat_nofollow_for_sync(dir_fd, name)?;
    sync_fd(fd.as_fd(), false).map_err(core_errno_from_nix)
}

fn sync_mutated_mknod_entry(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
    _mode: libc::mode_t,
) -> CoreResult<()> {
    sync_mutated_backend_entry(dir_fd, name)
}

fn set_internal_rawname(fd: BorrowedFd<'_>, raw: &[u8]) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO,
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }
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

fn warn_procfs_fallback(name: &CStr) {
    if !PROCFS_XATTR_WARNED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "longnamefs-rs v2: WARNING: using /proc/self/fd fallback for rawname xattr on {:?}",
            name.to_bytes()
        );
    }
}

fn is_procfs_unavailable(err: &CoreError) -> bool {
    matches!(core_err_to_errno(err), libc::ENOENT | libc::ENOTDIR) && !procfs_available()
}

fn procfs_available() -> bool {
    #[cfg(test)]
    if TEST_FORCE_PROCFS_UNAVAILABLE_LOCAL.with(Cell::get) {
        return false;
    }
    Path::new("/proc/self/fd").exists()
}

fn normalize_procfs_fallback_errno(
    raw_errno: i32,
    original_errno: i32,
    procfs_available: bool,
) -> i32 {
    if !procfs_available && matches!(raw_errno, libc::ENOENT | libc::ENOTDIR) {
        original_errno
    } else {
        raw_errno
    }
}

#[cfg(test)]
fn mark_procfs_symlink_fallback() {
    PROCFS_SYMLINK_FALLBACK_USED.store(true, Ordering::Relaxed);
}

#[cfg(not(test))]
fn mark_procfs_symlink_fallback() {}

fn set_internal_rawname_via_path(path: &CStr, raw: &[u8]) -> CoreResult<()> {
    let name = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let res = unsafe {
        libc::lsetxattr(
            path.as_ptr(),
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

fn set_internal_rawname_via_procfs_path(
    proc_path: &CStr,
    name: &CStr,
    raw: &[u8],
) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_PROCFS_PATH_ERRNO,
        &TEST_FORCE_PROCFS_PATH_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }
    warn_procfs_fallback(name);
    set_internal_rawname_via_path(proc_path, raw)
}

fn get_internal_rawname(fd: BorrowedFd<'_>) -> CoreResult<Vec<u8>> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO,
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }
    let name = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let res = unsafe { libc::fgetxattr(fd.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if res < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut buf = vec![0u8; res as usize];
    let mut did_retry = false;
    loop {
        let res = unsafe {
            libc::fgetxattr(
                fd.as_raw_fd(),
                name.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        };
        if res >= 0 {
            buf.truncate(res as usize);
            return Ok(buf);
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ERANGE) && !did_retry {
            did_retry = true;
            let size =
                unsafe { libc::fgetxattr(fd.as_raw_fd(), name.as_ptr(), std::ptr::null_mut(), 0) };
            if size < 0 {
                return Err(io::Error::last_os_error().into());
            }
            buf.resize(size as usize, 0u8);
            continue;
        }
        return Err(err.into());
    }
}

fn get_internal_rawname_via_path(path: &CStr) -> CoreResult<Vec<u8>> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO,
        &TEST_FORCE_INTERNAL_RAWNAME_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }
    let name = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let res = unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
    if res < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut buf = vec![0u8; res as usize];
    let mut did_retry = false;
    loop {
        let res = unsafe {
            libc::lgetxattr(
                path.as_ptr(),
                name.as_ptr(),
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        };
        if res >= 0 {
            buf.truncate(res as usize);
            return Ok(buf);
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ERANGE) && !did_retry {
            did_retry = true;
            let size =
                unsafe { libc::lgetxattr(path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) };
            if size < 0 {
                return Err(io::Error::last_os_error().into());
            }
            buf.resize(size as usize, 0u8);
            continue;
        }
        return Err(err.into());
    }
}

fn get_internal_rawname_via_procfs_path(proc_path: &CStr, name: &CStr) -> CoreResult<Vec<u8>> {
    warn_procfs_fallback(name);
    get_internal_rawname_via_path(proc_path)
}

fn get_internal_rawname_at(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<Vec<u8>> {
    let proc_path = procfs_path_for(dir_fd, name);
    let mut procfs_raw = None;
    let fd = nix::fcntl::openat(
        dir_fd,
        name,
        OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    );
    match fd {
        Ok(fd) => match get_internal_rawname(fd.as_fd()) {
            Ok(raw) => return Ok(raw),
            Err(CoreError::Io(ref ioe)) if ioe.raw_os_error() == Some(libc::EBADF) => {
                if let Some(proc_path) = proc_path.as_ref()
                    && let Ok(raw) =
                        get_internal_rawname_via_procfs_path(proc_path.as_c_str(), name)
                {
                    procfs_raw = Some(raw);
                }
            }
            Err(err) => return Err(err),
        },
        Err(nix::errno::Errno::ELOOP) => {
            if let Some(proc_path) = proc_path.as_ref() {
                return match get_internal_rawname_via_procfs_path(proc_path.as_c_str(), name) {
                    Ok(raw) => Ok(raw),
                    Err(err) if is_procfs_unavailable(&err) => {
                        Err(CoreError::from_errno(libc::ELOOP))
                    }
                    Err(err) => Err(err),
                };
            }
            return Err(CoreError::from_errno(libc::ELOOP));
        }
        Err(err) => return Err(core_errno_from_nix(err)),
    }
    if !OPATH_XATTR_WARNED.swap(true, Ordering::Relaxed) {
        eprintln!(
            "longnamefs-rs v2: WARNING: O_PATH fgetxattr EBADF for backend entry {:?}; retrying with readable fd",
            name.to_bytes()
        );
    }
    if let Some(raw) = procfs_raw {
        return Ok(raw);
    }
    let fd = openat_nofollow_for_xattr(dir_fd, name)?;
    get_internal_rawname(fd.as_fd())
}

fn is_missing_rawname_xattr_error(err: &CoreError) -> bool {
    match err {
        CoreError::Io(ioe) => ioe.raw_os_error() == Some(libc::ENODATA),
        _ => false,
    }
}

fn rawname_value_is_malformed(raw: &[u8]) -> bool {
    classify_committed_segment(raw).is_err()
}

fn committed_long_rawname(raw: &[u8]) -> Option<Vec<u8>> {
    matches!(classify_committed_segment(raw), Ok(SegmentKind::Long)).then(|| raw.to_vec())
}

fn read_committed_long_rawname_at(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
) -> CoreResult<Option<Vec<u8>>> {
    match get_internal_rawname_at(dir_fd, name) {
        Ok(raw_name) => Ok(committed_long_rawname(&raw_name)),
        Err(err) => match classify_rawname_read_error(&err) {
            ReadSideRepairDisposition::Recoverable(_) => Ok(None),
            ReadSideRepairDisposition::Fatal => Err(err),
        },
    }
}

fn find_committed_long_backend_by_raw(
    dir_fd: BorrowedFd<'_>,
    raw: &[u8],
) -> CoreResult<Option<(Vec<u8>, Vec<u8>)>> {
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(core_errno_from_nix)?;
    for entry in dir.iter() {
        let entry = entry.map_err(core_errno_from_nix)?;
        let backend_name = entry.file_name().to_bytes().to_vec();
        if !is_stable_long_object_backend_name(&backend_name) {
            continue;
        }
        let c_name = cstring_from_bytes(&backend_name)?;
        let Some(raw_name) = read_committed_long_rawname_at(dir_fd, c_name.as_c_str())? else {
            continue;
        };
        if raw_name == raw {
            return Ok(Some((backend_name, raw_name)));
        }
    }
    Ok(None)
}

fn committed_long_backend_matches_raw(
    dir_fd: BorrowedFd<'_>,
    backend_name: &[u8],
    raw: &[u8],
) -> CoreResult<bool> {
    if !is_stable_long_object_backend_name(backend_name) {
        return Ok(false);
    }
    let c_name = cstring_from_bytes(backend_name)?;
    Ok(
        read_committed_long_rawname_at(dir_fd, c_name.as_c_str())?
            .is_some_and(|existing_raw| existing_raw == raw),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadSideRepairAnomaly {
    MalformedRawname,
    MissingRawnameXattr,
    StaleIndex,
    ConcurrentDisappearance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadSideRepairDisposition {
    Recoverable(ReadSideRepairAnomaly),
    Fatal,
}

fn classify_rawname_value_anomaly(
    raw_name: &[u8],
) -> Option<ReadSideRepairAnomaly> {
    rawname_value_is_malformed(raw_name).then_some(ReadSideRepairAnomaly::MalformedRawname)
}

fn stable_long_name_lookup_allowed(raw: &[u8], max_name_len: usize) -> bool {
    let _ = max_name_len;
    raw.len() > MAX_SEGMENT_ON_DISK && !is_reserved_prefix(raw)
}

fn classify_cached_index_revalidation_error(err: &CoreError) -> ReadSideRepairDisposition {
    match core_err_to_errno(err) {
        libc::ENOENT => ReadSideRepairDisposition::Recoverable(ReadSideRepairAnomaly::StaleIndex),
        _ => ReadSideRepairDisposition::Fatal,
    }
}

fn classify_rawname_read_error(err: &CoreError) -> ReadSideRepairDisposition {
    match core_err_to_errno(err) {
        libc::ENODATA => {
            ReadSideRepairDisposition::Recoverable(ReadSideRepairAnomaly::MissingRawnameXattr)
        }
        libc::ENOENT => {
            ReadSideRepairDisposition::Recoverable(ReadSideRepairAnomaly::ConcurrentDisappearance)
        }
        _ => ReadSideRepairDisposition::Fatal,
    }
}

fn classify_enumerated_entry_stat_error(err: &CoreError) -> ReadSideRepairDisposition {
    match core_err_to_errno(err) {
        libc::ENOENT => {
            ReadSideRepairDisposition::Recoverable(ReadSideRepairAnomaly::ConcurrentDisappearance)
        }
        _ => ReadSideRepairDisposition::Fatal,
    }
}

#[cfg(test)]
fn test_repair_anomaly_kind(anomaly: ReadSideRepairAnomaly) -> TestRepairAnomalyKind {
    match anomaly {
        ReadSideRepairAnomaly::MalformedRawname => TestRepairAnomalyKind::MalformedRawname,
        ReadSideRepairAnomaly::MissingRawnameXattr => TestRepairAnomalyKind::MissingRawnameXattr,
        ReadSideRepairAnomaly::StaleIndex => TestRepairAnomalyKind::StaleIndex,
        ReadSideRepairAnomaly::ConcurrentDisappearance => {
            TestRepairAnomalyKind::ConcurrentDisappearance
        }
    }
}

fn record_global_repair_anomaly(backend_name: &[u8], anomaly: ReadSideRepairAnomaly) {
    #[cfg(test)]
    record_test_repair_anomaly_global(backend_name, test_repair_anomaly_kind(anomaly));
    #[cfg(not(test))]
    let _ = (backend_name, anomaly);
}

fn record_core_repair_anomaly(
    core: &LongNameFsCore,
    backend_name: &[u8],
    anomaly: ReadSideRepairAnomaly,
) {
    #[cfg(test)]
    core.record_test_repair_anomaly(backend_name, test_repair_anomaly_kind(anomaly));
    #[cfg(not(test))]
    let _ = (core, backend_name, anomaly);
}

fn store_first_fatal_read_error(slot: &Mutex<Option<CoreError>>, err: CoreError) {
    let mut guard = slot.lock();
    if guard.is_none() {
        *guard = Some(err);
    }
}

fn verify_backend_supports_xattr(dir_fd: BorrowedFd<'_>) -> CoreResult<()> {
    let fname = core_string_to_cstring(XATTR_CHECK_NAME)?;
    let _ = unlinkat(dir_fd, fname.as_c_str(), UnlinkatFlags::NoRemoveDir);
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
    let final_name = core_string_to_cstring(RENAMEAT2_PROBE_NAME)?;
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
        NixRenameFlags::RENAME_NOREPLACE,
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

fn rebuild_dir_index_from_backend(
    dir_fd: BorrowedFd<'_>,
    _max_name_len: usize,
) -> CoreResult<DirIndex> {
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
            Err(err) => return Err(core_errno_from_nix(err)),
        };
        let name_bytes = entry.file_name().to_bytes().to_vec();
        if name_bytes.is_empty() {
            continue;
        }
        if is_fs_internal_name(&name_bytes) {
            continue;
        }
        if is_stable_long_object_backend_name(&name_bytes) {
            internal.push(name_bytes);
        }
    }

    let mut index = DirIndex::new();
    if internal.len() <= PARALLEL_REBUILD_THRESHOLD {
        for name_bytes in internal {
            let c_name = cstring_from_bytes(&name_bytes)?;
            let raw_name = match get_internal_rawname_at(dir_fd, c_name.as_c_str()) {
                Ok(v) => match committed_long_rawname(&v) {
                    Some(raw_name) => raw_name,
                    None => {
                        record_global_repair_anomaly(
                            &name_bytes,
                            ReadSideRepairAnomaly::MalformedRawname,
                        );
                        continue;
                    }
                },
                Err(err) => match classify_rawname_read_error(&err) {
                    ReadSideRepairDisposition::Recoverable(anomaly) => {
                        record_global_repair_anomaly(&name_bytes, anomaly);
                        continue;
                    }
                    ReadSideRepairDisposition::Fatal => return Err(err),
                },
            };
            index.upsert(name_bytes, raw_name);
        }
        return Ok(index);
    }

    let workers = PARALLEL_REBUILD_WORKERS.max(1);
    let next = AtomicUsize::new(0);
    let results: Mutex<Vec<(Vec<u8>, Vec<u8>)>> = Mutex::new(Vec::new());
    let fatal_error: Mutex<Option<CoreError>> = Mutex::new(None);
    let spawned = AtomicUsize::new(0);

    thread::scope(|scope| {
        for _ in 0..workers {
            let dup_fd = match dup_rebuild_worker_fd(dir_fd) {
                Ok(fd) => fd,
                Err(_) => continue,
            };
            spawned.fetch_add(1, Ordering::Relaxed);
            let internal = &internal;
            let next = &next;
            let results = &results;
            let fatal_error = &fatal_error;
            scope.spawn(move || {
                loop {
                    if fatal_error.lock().is_some() {
                        break;
                    }
                    let idx = next.fetch_add(1, Ordering::Relaxed);
                    if idx >= internal.len() {
                        break;
                    }
                    let name_bytes = internal[idx].clone();
                    let c_name = match cstring_from_bytes(&name_bytes) {
                        Ok(v) => v,
                        Err(err) => {
                            store_first_fatal_read_error(fatal_error, err);
                            break;
                        }
                    };
                    let raw_name = match get_internal_rawname_at(dup_fd.as_fd(), c_name.as_c_str())
                    {
                        Ok(v) => match committed_long_rawname(&v) {
                            Some(raw_name) => raw_name,
                            None => {
                                record_global_repair_anomaly(
                                    &name_bytes,
                                    ReadSideRepairAnomaly::MalformedRawname,
                                );
                                continue;
                            }
                        },
                        Err(err) => match classify_rawname_read_error(&err) {
                            ReadSideRepairDisposition::Recoverable(anomaly) => {
                                record_global_repair_anomaly(&name_bytes, anomaly);
                                continue;
                            }
                            ReadSideRepairDisposition::Fatal => {
                                store_first_fatal_read_error(fatal_error, err);
                                break;
                            }
                        },
                    };
                    results.lock().push((name_bytes, raw_name));
                }
            });
        }
    });

    if spawned.load(Ordering::Relaxed) == 0 {
        // If worker fd duplication fails for every worker, do a safe sequential fallback
        // instead of returning an empty/partial index as success.
        for name_bytes in internal {
            let c_name = cstring_from_bytes(&name_bytes)?;
            let raw_name = match get_internal_rawname_at(dir_fd, c_name.as_c_str()) {
                Ok(v) => match committed_long_rawname(&v) {
                    Some(raw_name) => raw_name,
                    None => {
                        record_global_repair_anomaly(
                            &name_bytes,
                            ReadSideRepairAnomaly::MalformedRawname,
                        );
                        continue;
                    }
                },
                Err(err) => match classify_rawname_read_error(&err) {
                    ReadSideRepairDisposition::Recoverable(anomaly) => {
                        record_global_repair_anomaly(&name_bytes, anomaly);
                        continue;
                    }
                    ReadSideRepairDisposition::Fatal => return Err(err),
                },
            };
            index.upsert(name_bytes, raw_name);
        }
        index.clear_pending_ops();
        index.clear_dirty();
        return Ok(index);
    }

    if let Some(err) = fatal_error.into_inner() {
        return Err(err);
    }

    for (backend_name, raw_name) in results.into_inner() {
        index.upsert(backend_name, raw_name);
    }
    index.clear_pending_ops();
    index.clear_dirty();
    Ok(index)
}

fn load_dir_state(
    cache: &IndexCache,
    dir_fd: BorrowedFd<'_>,
    max_name_len: usize,
) -> CoreResult<DirState> {
    let index = cache.get_or_load(dir_fd, max_name_len)?;
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

fn finalize_post_commit_index_state(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    strategy: IndexSync,
) {
    #[cfg(test)]
    wait_if_test_pause_post_commit_flush(dir_cache_key(dir_fd));
    if maybe_flush_index(dir_fd, state, strategy, false).is_err() {
        mark_dirty(state);
    }
}

fn rollback_dir_index_entry(state: &mut DirState, backend_name: &[u8]) {
    state.attr_cache.remove(backend_name);
    let mut guard = state.index.write();
    if guard.index.remove(backend_name).is_some() {
        guard.pending = guard.pending.saturating_add(1);
    }
}

fn best_effort_unlinkat_file(dir_fd: BorrowedFd<'_>, name: &CStr) {
    let _ = unlinkat(dir_fd, name, UnlinkatFlags::NoRemoveDir);
}

fn best_effort_unlinkat_dir(dir_fd: BorrowedFd<'_>, name: &CStr) {
    let _ = unlinkat(dir_fd, name, UnlinkatFlags::RemoveDir);
}

fn finalize_delete_quarantine_entry(
    parent_dir: BorrowedFd<'_>,
    quarantine_name: &CStr,
) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO,
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    unlinkat(parent_dir, quarantine_name, UnlinkatFlags::NoRemoveDir)
        .map_err(core_errno_from_nix)?;
    core_fsync_dir(parent_dir).map_err(CoreError::from)
}

fn finalize_dir_delete_quarantine_entry(
    parent_dir: BorrowedFd<'_>,
    quarantine_name: &CStr,
) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO,
        &TEST_FORCE_POST_CLEAR_DELETE_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    unlinkat(parent_dir, quarantine_name, UnlinkatFlags::RemoveDir).map_err(core_errno_from_nix)?;
    core_fsync_dir(parent_dir).map_err(CoreError::from)
}

fn next_delete_quarantine_backend_name(
    parent_dir: BorrowedFd<'_>,
    prefix: &[u8],
    seed: &[u8],
) -> CoreResult<BackendName> {
    let seed_hash = hex::encode(Sha256::digest(seed));
    let prefix_text =
        std::str::from_utf8(prefix).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
    for counter in 0u64.. {
        let candidate = format!("{prefix_text}{seed_hash}_{counter}");
        let candidate_c =
            CString::new(candidate.clone()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
        match fstatat(
            parent_dir,
            candidate_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        ) {
            Err(nix::errno::Errno::ENOENT) => {
                return Ok(BackendName::Internal(candidate.into_bytes()));
            }
            Ok(_) => continue,
            Err(err) => return Err(core_errno_from_nix(err)),
        }
    }
    Err(CoreError::NoSpace)
}

fn maybe_flush_index(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    strategy: IndexSync,
    force_sync: bool,
) -> CoreResult<()> {
    let flush_wait = { state.index.read().flush_wait.clone() };
    loop {
        let plan = {
            let mut guard = state.index.write();
            if guard.flushing {
                if force_sync || matches!(strategy, IndexSync::Always) {
                    None
                } else {
                    return Ok(());
                }
            } else {
                let should_flush = match strategy {
                    IndexSync::Always => guard.index.is_dirty() || guard.index.has_pending_ops(),
                    IndexSync::Batch {
                        max_pending,
                        max_age,
                    } => {
                        guard.index.is_dirty()
                            && (guard.pending >= max_pending
                                || guard.last_flush.elapsed() >= max_age)
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

                guard.flushing = true;
                let pending_ops = guard.index.take_pending_ops();
                let snapshot = guard.index.clone();
                let pending_before = guard.pending;
                let journal_size = guard.journal_size_bytes;
                let journal_ops_since_compact = guard.journal_ops_since_compact;
                Some((
                    pending_ops,
                    snapshot,
                    pending_before,
                    journal_size,
                    journal_ops_since_compact,
                    should_compact,
                    need_force_sync_only,
                    guard.journal_file.take(),
                ))
            }
        };

        let Some(plan) = plan else {
            let mut wait_guard = flush_wait.lock.lock();
            while state.index.read().flushing {
                flush_wait.cv.wait(&mut wait_guard);
            }
            continue;
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
            #[cfg(test)]
            if let Some(errno) = test_atomic_errno_load(
                &TEST_FORCE_POST_COMMIT_FLUSH_ERRNO,
                &TEST_FORCE_POST_COMMIT_FLUSH_ERRNO_LOCAL,
            ) {
                return Err(CoreError::from_errno(errno));
            }
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
            let journal_size_on_err = core_string_to_cstring(JOURNAL_NAME)
                .ok()
                .and_then(|name| {
                    nix::fcntl::openat(
                        dir_fd,
                        name.as_c_str(),
                        OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                        Mode::empty(),
                    )
                    .ok()
                })
                .and_then(|fd| fstat(fd.as_fd()).ok())
                .map(|st| st.st_size as u64);

            let wait_guard = flush_wait.lock.lock();
            let mut guard = state.index.write();
            guard.index.extend_pending_ops(restore_ops);
            if let Some(size) = journal_size_on_err {
                guard.journal_size_bytes = size;
            }
            guard.journal_file = None;
            guard.flushing = false;
            drop(guard);
            flush_wait.cv.notify_all();
            drop(wait_guard);
            return Err(err);
        }
        let (journal_size_bytes, journal_ops_since_compact, journal_file) = flush_res.unwrap();

        let wait_guard = flush_wait.lock.lock();
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
        guard.flushing = false;
        drop(guard);
        flush_wait.cv.notify_all();
        drop(wait_guard);
        return Ok(());
    }
}

fn list_logical_entries(
    core: &LongNameFsCore,
    handle: &DirHandle,
    _max_name_len: usize,
    index_sync: IndexSync,
    need_attr: bool,
    visibility_active: bool,
    prefer_backend_rawname_truth: bool,
    allow_index_flush: bool,
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

    let dir_dev = fstat(dir_fd).map_err(core_errno_from_nix)?.st_dev;
    let mut scanned = Vec::new();
    for entry in dir.iter() {
        #[cfg(test)]
        let entry = if let Some(errno) = test_atomic_errno_load(
            &TEST_FORCE_LIST_ITER_SKIP_ERRNO,
            &TEST_FORCE_LIST_ITER_SKIP_ERRNO_LOCAL,
        ) {
            Err(nix::errno::Errno::from_raw(errno))
        } else {
            entry
        };
        let entry = match entry {
            Ok(v) => v,
            Err(err) => return Err(core_errno_from_nix(err)),
        };
        let name_bytes = entry.file_name().to_bytes().to_vec();
        if name_bytes.is_empty() {
            continue;
        }
        if name_bytes == b"." || name_bytes == b".." {
            continue;
        }
        if is_fs_internal_name(&name_bytes) {
            continue;
        }
        if name_bytes.starts_with(INTERNAL_PREFIX.as_bytes())
            && !is_stable_long_object_backend_name(&name_bytes)
        {
            continue;
        }
        let is_internal = is_stable_long_object_backend_name(&name_bytes);
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
                backend_key = (entry.ino != 0).then_some(BackendKey {
                    dev: dir_dev,
                    ino: entry.ino,
                });
            }
            let kind = entry.kind_hint.or_else(|| attr.map(|a| a.kind));

            let internal_backend_name = entry.is_internal.then(|| entry.backend_name.clone());

            let raw_name = if entry.is_internal {
                if !prefer_backend_rawname_truth {
                    if let Some(existing) = index.index.get(&entry.backend_name) {
                        let existing_raw = existing.raw_name.as_ref().to_vec();
                        repair_candidates.push(entry.backend_name.clone());
                        Some(existing_raw)
                    } else {
                        repair_candidates.push(entry.backend_name.clone());
                        None
                    }
                } else {
                    repair_candidates.push(entry.backend_name.clone());
                    None
                }
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
    let mut anomaly_found = false;
    let mut skipped_backends: HashSet<Vec<u8>> = HashSet::new();
    for backend_name in &repair_candidates {
        let c_name = cstring_from_bytes(backend_name)?;
        match get_internal_rawname_at(dir_fd, c_name.as_c_str()) {
            Ok(raw_name) => match committed_long_rawname(&raw_name) {
                Some(raw_name) => {
                    repairs.insert(backend_name.clone(), raw_name);
                }
                None => {
                    record_core_repair_anomaly(
                        core,
                        backend_name,
                        ReadSideRepairAnomaly::MalformedRawname,
                    );
                    anomaly_found = true;
                    skipped_backends.insert(backend_name.clone());
                    continue;
                }
            },
            Err(err) => match classify_rawname_read_error(&err) {
                ReadSideRepairDisposition::Recoverable(anomaly) => {
                    record_core_repair_anomaly(core, backend_name, anomaly);
                    anomaly_found = true;
                    skipped_backends.insert(backend_name.clone());
                }
                ReadSideRepairDisposition::Fatal => return Err(err),
            },
        }
    }

    let mut fetched_stats: HashMap<Vec<u8>, nix::sys::stat::FileStat> = HashMap::new();
    if !visibility_active {
        for name_bytes in &attr_miss {
            let c_name = cstring_from_bytes(name_bytes)?;
            let stat = match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
                Ok(st) => st,
                Err(err) => {
                    let err = core_errno_from_nix(err);
                    match classify_enumerated_entry_stat_error(&err) {
                        ReadSideRepairDisposition::Recoverable(anomaly) => {
                            record_core_repair_anomaly(core, name_bytes, anomaly);
                            anomaly_found = true;
                            skipped_backends.insert(name_bytes.clone());
                            continue;
                        }
                        ReadSideRepairDisposition::Fatal => return Err(err),
                    }
                }
            };
            fetched_stats.insert(name_bytes.clone(), stat);
        }
    }

    for entry in &mut pending {
        if skipped_backends.contains(&entry.backend_name) {
            continue;
        }
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

    {
        let mut state = handle.state.write();
        let mut state_repaired = false;
        {
            let mut guard = state.index.write();
            for backend_name in &skipped_backends {
                if guard.index.remove(backend_name).is_some() {
                    guard.pending = guard.pending.saturating_add(1);
                    state_repaired = true;
                }
            }
            for entry in &mut pending {
                if !entry.is_internal || skipped_backends.contains(&entry.backend_name) {
                    continue;
                }
                let Some(raw_name) = repairs.get(&entry.backend_name).cloned() else {
                    continue;
                };
                entry.raw_name = Some(raw_name.clone());
                let needs_repair = match guard.index.get(&entry.backend_name) {
                    Some(existing) => existing.raw_name.as_ref() != raw_name.as_slice(),
                    None => true,
                };
                if needs_repair {
                    guard.index.upsert(entry.backend_name.clone(), raw_name);
                    guard.pending = guard.pending.saturating_add(1);
                    state_repaired = true;
                }
            }
        }
        if state_repaired {
            state.attr_cache.clear();
        }
        if anomaly_found && !state_repaired {
            mark_dirty(&mut state);
        }

        let mut entries = Vec::new();
        for entry in &pending {
            if skipped_backends.contains(&entry.backend_name) {
                continue;
            }
            let raw_name = match entry.raw_name.as_ref() {
                Some(v) => v,
                None => continue,
            };
            entries.push(DirEntryInfo {
                name: OsString::from_vec(raw_name.clone()),
                kind: entry.kind.unwrap_or(CoreFileType::RegularFile),
                attr: entry.attr,
                backend_name: entry.backend_name.clone(),
                backend_key: entry.backend_key,
            });
        }

        let seen_backend: HashSet<Vec<u8>> = entries
            .iter()
            .map(|entry| entry.backend_name.clone())
            .collect();
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
        if allow_index_flush && !visibility_active {
            maybe_flush_index(dir_fd, &mut state, index_sync, false)?;
        }
        return Ok(entries);
    }
}

fn list_logical_entries_from_index(handle: &DirHandle) -> Vec<DirEntryInfo> {
    let state = handle.state.read();
    let index = state.index.read();
    index
        .index
        .iter()
        .map(|(_backend, entry)| DirEntryInfo {
            name: OsString::from_vec(entry.raw_name.as_ref().to_vec()),
            kind: CoreFileType::RegularFile,
            attr: state
                .attr_cache
                .get(entry.backend_name.as_ref())
                .map(|cached| cached.attr),
            backend_name: entry.backend_name.as_ref().to_vec(),
            backend_key: state
                .attr_cache
                .get(entry.backend_name.as_ref())
                .map(|cached| cached.backend),
        })
        .collect()
}

fn snapshot_lookup_backend(snapshot: &DirSnapshot, raw: &[u8]) -> Option<Vec<u8>> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.name.as_os_str().as_bytes() == raw)
        .map(|entry| entry.backend_name.clone())
}

fn visibility_snapshot_for_dir(
    core: &LongNameFsCore,
    key: DirCacheKey,
    handle: &Arc<DirHandle>,
    need_attr: bool,
) -> CoreResult<Arc<Vec<DirEntryInfo>>> {
    if let Some(snapshot) = core.dir_visibility_snapshot(key)
        && (!need_attr || snapshot.has_attrs)
    {
        let entries = snapshot.entries.clone();
        *handle.snapshot.lock() = Some(snapshot);
        return Ok(entries);
    }

    let logical = list_logical_entries(
        core,
        handle,
        core.max_name_len,
        core.index_sync,
        need_attr,
        false,
        true,
        false,
    )?;
    let snapshot = DirSnapshot {
        entries: Arc::new(logical),
        has_attrs: need_attr,
    };
    let entries = snapshot.entries.clone();
    core.set_dir_visibility_snapshot(key, snapshot.clone());
    *handle.snapshot.lock() = Some(snapshot);
    Ok(entries)
}

fn map_long_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
    max_name_len: usize,
    visibility_active: bool,
    visibility_snapshot: Option<&DirSnapshot>,
) -> CoreResult<Vec<u8>> {
    if !stable_long_name_lookup_allowed(raw, max_name_len) {
        return Err(CoreError::NotFound);
    }

    if let Some(snapshot) = visibility_snapshot {
        return snapshot_lookup_backend(snapshot, raw).ok_or(CoreError::NotFound);
    }

    if let Some(entry) = {
        let guard = state.index.read();
        guard.index.backend_for_raw(raw)
    } {
        if visibility_active {
            return Ok(entry.as_ref().to_vec());
        }
        if committed_long_backend_matches_raw(dir_fd, entry.as_ref(), raw)? {
            return Ok(entry.as_ref().to_vec());
        }
        let c_name =
            cstring_from_bytes(entry.as_ref()).map_err(|_| CoreError::from_errno(libc::EILSEQ))?;
        match fstatat(dir_fd, c_name.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(_) => {
                {
                    let mut guard = state.index.write();
                    guard.index.remove(entry.as_ref());
                    guard.pending = guard.pending.saturating_add(1);
                }
                state.attr_cache.clear();
            }
            Err(err) => {
                let err = core_errno_from_nix(err);
                match classify_cached_index_revalidation_error(&err) {
                    ReadSideRepairDisposition::Recoverable(ReadSideRepairAnomaly::StaleIndex) => {
                        {
                            let mut guard = state.index.write();
                            guard.index.remove(entry.as_ref());
                            guard.pending = guard.pending.saturating_add(1);
                        }
                        state.attr_cache.clear();
                    }
                    ReadSideRepairDisposition::Recoverable(_) => unreachable!(),
                    ReadSideRepairDisposition::Fatal => return Err(err),
                }
            }
        }
    }

    if visibility_active {
        return Err(CoreError::NotFound);
    }

    if let Some((candidate_bytes, raw_name)) = find_committed_long_backend_by_raw(dir_fd, raw)? {
        {
            let mut guard = state.index.write();
            guard.index.upsert(candidate_bytes.clone(), raw_name);
            guard.pending = guard.pending.saturating_add(1);
        }
        state.attr_cache.clear();
        return Ok(candidate_bytes);
    }

    Err(CoreError::NotFound)
}

fn map_segment_for_lookup(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
    max_name_len: usize,
    visibility_active: bool,
    visibility_snapshot: Option<&DirSnapshot>,
) -> CoreResult<(BackendName, SegmentKind)> {
    if is_fs_internal_name(raw) {
        return Err(CoreError::InternalMeta);
    }
    if is_reserved_prefix(raw) {
        return Err(CoreError::ReservedPrefix);
    }
    let kind = classify_committed_segment(raw)?;
    match kind {
        SegmentKind::Short => Ok((BackendName::Short(raw.to_vec()), kind)),
        SegmentKind::Long => {
            let backend = map_long_for_lookup(
                dir_fd,
                state,
                raw,
                max_name_len,
                visibility_active,
                visibility_snapshot,
            )?;
            Ok((BackendName::Internal(backend), kind))
        }
    }
}

fn map_segment_for_create(
    dir_fd: BorrowedFd<'_>,
    state: &mut DirState,
    raw: &[u8],
    max_name_len: usize,
) -> CoreResult<(BackendName, SegmentKind)> {
    if is_fs_internal_name(raw) {
        return Err(CoreError::InternalMeta);
    }
    if is_reserved_prefix(raw) {
        return Err(CoreError::ReservedPrefix);
    }
    let kind = classify_segment(raw, max_name_len)?;
    match kind {
        SegmentKind::Short => Ok((BackendName::Short(raw.to_vec()), kind)),
        SegmentKind::Long => {
            if let Some(existing) = {
                let guard = state.index.read();
                guard.index.backend_for_raw(raw)
            } {
                if committed_long_backend_matches_raw(dir_fd, existing.as_ref(), raw)? {
                    return Err(CoreError::AlreadyExists);
                }
                let mut guard = state.index.write();
                guard.index.remove(existing.as_ref());
                guard.pending = guard.pending.saturating_add(1);
                state.attr_cache.clear();
            }
            if let Some((backend_name, existing_raw)) = find_committed_long_backend_by_raw(dir_fd, raw)? {
                {
                    let mut guard = state.index.write();
                    guard.index.upsert(backend_name, existing_raw);
                    guard.pending = guard.pending.saturating_add(1);
                }
                state.attr_cache.clear();
                return Err(CoreError::AlreadyExists);
            }
            Ok((BackendName::Internal(Vec::new()), kind))
        }
    }
}

fn stable_backend_for_existing_long_name(
    dir_index: &DirIndex,
    logical_raw: &[u8],
) -> CoreResult<Vec<u8>> {
    dir_index
        .backend_for_raw(logical_raw)
        .map(|existing| existing.as_ref().to_vec())
        .ok_or(CoreError::NotFound)
}

fn refresh_dir_index_from_backend(
    dir_fd: BorrowedFd<'_>,
    backend_name: &[u8],
) -> CoreResult<Vec<u8>> {
    let c_name = cstring_from_bytes(backend_name)?;
    let raw = get_internal_rawname_at(dir_fd, c_name.as_c_str())?;
    Ok(raw)
}

fn path_segments(path: &OsStr) -> CoreResult<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let bytes = path.as_bytes();
    let mut start = 0usize;
    for (idx, b) in bytes.iter().enumerate() {
        if *b == b'/' {
            if idx > start {
                let seg = &bytes[start..idx];
                if seg == b"." || seg == b".." {
                    return Err(CoreError::InternalMeta);
                }
                out.push(seg.to_vec());
            }
            start = idx + 1;
        }
    }
    if start < bytes.len() {
        let seg = &bytes[start..];
        if seg == b"." || seg == b".." {
            return Err(CoreError::InternalMeta);
        }
        out.push(seg.to_vec());
    }
    Ok(out)
}

fn backend_path_segments_for_inode(
    inode_store: &InodeStore,
    parent_ino: InodeId,
) -> CoreResult<Vec<Vec<u8>>> {
    inode_store.get_backend_path_segments(parent_ino)
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

#[derive(Debug)]
struct ProcfsXattrPath {
    path: CString,
    _parent_dir_guard: Option<Arc<OwnedFd>>,
}

#[derive(Debug)]
enum XattrTarget {
    Fd {
        raw_fd: RawFd,
        _guard: Option<OwnedFd>,
        proc_path: Option<ProcfsXattrPath>,
    },
    ProcPath(ProcfsXattrPath),
}

fn xattr_target_for_path(
    core: &LongNameFsCore,
    path: &OsStr,
    write_intent: bool,
) -> CoreResult<XattrTarget> {
    let access_mode = OFlag::O_RDONLY;
    if path == OsStr::new("/") {
        if !write_intent {
            return Ok(XattrTarget::Fd {
                raw_fd: core.config.backend_fd().as_raw_fd(),
                _guard: None,
                proc_path: None,
            });
        }
        let fd = nix::fcntl::openat(
            core.config.backend_fd(),
            ".",
            access_mode | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        return Ok(XattrTarget::Fd {
            raw_fd: fd.as_raw_fd(),
            _guard: Some(fd),
            proc_path: None,
        });
    }
    let mapped = core.resolve_path(path)?;
    let fname = mapped.backend_name.as_cstring()?;
    let proc_path =
        procfs_path_for(mapped.dir_fd.as_fd(), fname.as_c_str()).map(|path| ProcfsXattrPath {
            path,
            _parent_dir_guard: Some(mapped.dir_fd.clone()),
        });
    let fd = match nix::fcntl::openat(
        mapped.dir_fd.as_fd(),
        fname.as_c_str(),
        access_mode | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(nix::errno::Errno::EISDIR) => nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            access_mode | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?,
        Err(nix::errno::Errno::ELOOP) => {
            if let Some(proc_path) = proc_path {
                return Ok(XattrTarget::ProcPath(proc_path));
            }
            return Err(CoreError::from_errno(libc::ELOOP));
        }
        Err(err) => return Err(core_errno_from_nix(err)),
    };
    Ok(XattrTarget::Fd {
        raw_fd: fd.as_raw_fd(),
        _guard: Some(fd),
        proc_path,
    })
}

fn xattr_set(target: &XattrTarget, name: &CStr, value: &[u8], flags: i32) -> CoreResult<()> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe {
                libc::fsetxattr(
                    *raw_fd,
                    name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    flags as libc::c_int,
                )
            };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe {
                        libc::lsetxattr(
                            proc_path.path.as_ptr(),
                            name.as_ptr(),
                            value.as_ptr() as *const libc::c_void,
                            value.len(),
                            flags as libc::c_int,
                        )
                    },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe {
                libc::lsetxattr(
                    path.path.as_ptr(),
                    name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    flags as libc::c_int,
                )
            },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(())
}

fn xattr_get_size(target: &XattrTarget, name: &CStr) -> CoreResult<usize> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe { libc::fgetxattr(*raw_fd, name.as_ptr(), std::ptr::null_mut(), 0) };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe {
                        libc::lgetxattr(
                            proc_path.path.as_ptr(),
                            name.as_ptr(),
                            std::ptr::null_mut(),
                            0,
                        )
                    },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe { libc::lgetxattr(path.path.as_ptr(), name.as_ptr(), std::ptr::null_mut(), 0) },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(res as usize)
}

fn xattr_get_into(target: &XattrTarget, name: &CStr, buf: &mut [u8]) -> CoreResult<usize> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe {
                libc::fgetxattr(
                    *raw_fd,
                    name.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe {
                        libc::lgetxattr(
                            proc_path.path.as_ptr(),
                            name.as_ptr(),
                            buf.as_mut_ptr() as *mut libc::c_void,
                            buf.len(),
                        )
                    },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe {
                libc::lgetxattr(
                    path.path.as_ptr(),
                    name.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(res as usize)
}

fn xattr_list_size(target: &XattrTarget) -> CoreResult<usize> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe { libc::flistxattr(*raw_fd, std::ptr::null_mut(), 0) };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe { libc::llistxattr(proc_path.path.as_ptr(), std::ptr::null_mut(), 0) },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe { libc::llistxattr(path.path.as_ptr(), std::ptr::null_mut(), 0) },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(res as usize)
}

fn xattr_list_into(target: &XattrTarget, buf: &mut [u8]) -> CoreResult<usize> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe {
                libc::flistxattr(
                    *raw_fd,
                    buf.as_mut_ptr() as *mut libc::c_char,
                    buf.len() as libc::size_t,
                )
            };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe {
                        libc::llistxattr(
                            proc_path.path.as_ptr(),
                            buf.as_mut_ptr() as *mut libc::c_char,
                            buf.len() as libc::size_t,
                        )
                    },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe {
                libc::llistxattr(
                    path.path.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_char,
                    buf.len() as libc::size_t,
                )
            },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(res as usize)
}

fn xattr_remove(target: &XattrTarget, name: &CStr) -> CoreResult<()> {
    let (res, original_errno) = match target {
        XattrTarget::Fd {
            raw_fd, proc_path, ..
        } => {
            let res = unsafe { libc::fremovexattr(*raw_fd, name.as_ptr()) };
            if res < 0
                && io::Error::last_os_error().raw_os_error() == Some(libc::EBADF)
                && let Some(proc_path) = proc_path
            {
                (
                    unsafe { libc::lremovexattr(proc_path.path.as_ptr(), name.as_ptr()) },
                    Some(libc::EBADF),
                )
            } else {
                (res, None)
            }
        }
        XattrTarget::ProcPath(path) => (
            unsafe { libc::lremovexattr(path.path.as_ptr(), name.as_ptr()) },
            Some(libc::ELOOP),
        ),
    };
    if res < 0 {
        let raw_errno = io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        let errno = original_errno.map_or(raw_errno, |orig| {
            normalize_procfs_fallback_errno(raw_errno, orig, Path::new("/proc/self/fd").exists())
        });
        return Err(CoreError::from_errno(errno));
    }
    Ok(())
}

fn clear_internal_rawname_at(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<()> {
    let proc_path = procfs_path_for(dir_fd, name).map(|path| ProcfsXattrPath {
        path,
        _parent_dir_guard: None,
    });
    let target = match nix::fcntl::openat(
        dir_fd,
        name,
        OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => XattrTarget::Fd {
            raw_fd: fd.as_raw_fd(),
            _guard: Some(fd),
            proc_path,
        },
        Err(nix::errno::Errno::ELOOP) => {
            let Some(proc_path) = proc_path else {
                return Err(CoreError::from_errno(libc::ELOOP));
            };
            XattrTarget::ProcPath(proc_path)
        }
        Err(nix::errno::Errno::EISDIR) => {
            let fd = nix::fcntl::openat(
                dir_fd,
                name,
                OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
                Mode::empty(),
            )
            .map_err(core_errno_from_nix)?;
            XattrTarget::Fd {
                raw_fd: fd.as_raw_fd(),
                _guard: Some(fd),
                proc_path,
            }
        }
        Err(err) => return Err(core_errno_from_nix(err)),
    };
    let rawname = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    xattr_remove(&target, rawname.as_c_str())
}

pub struct LongNameFsCore {
    pub config: Arc<Config>,
    backend_lock_fd: OwnedFd,
    poisoned: AtomicBool,
    mutation_txn_lock: Mutex<()>,
    dir_visibility_locks: DirVisibilityLockTable,
    dir_cache: DirCache,
    dir_fd_cache: DirFdCache,
    index_cache: IndexCache,
    max_name_len: usize,
    index_sync: IndexSync,
    supports_renameat2: bool,
    #[cfg(test)]
    repair_anomalies: Mutex<Vec<TestRepairAnomalyRecord>>,
}

impl LongNameFsCore {
    pub fn new(
        config: Config,
        max_name_len: usize,
        dir_cache_ttl: Option<Duration>,
        index_sync: IndexSync,
    ) -> CoreResult<Self> {
        let backend_lock_fd = open_and_lock_backend(config.backend_fd())?;
        match read_txn_record(config.backend_fd()) {
            Ok(None) => {}
            Ok(Some(record)) => {
                if let Err(err) = rollback_inflight_txn(
                    config.backend_fd(),
                    &record,
                    RollbackMode::StartupRecovery,
                ) {
                    emit_startup_recovery_failure(&err);
                    return Err(err);
                }
                if let Err(err) = clear_txn_record(config.backend_fd()) {
                    emit_startup_recovery_failure(&err);
                    return Err(err);
                }
            }
            Err(CoreError::BadFormat) => return Err(CoreError::BadFormat),
            Err(err) => return Err(err),
        }
        let has_any_committed_long_object = validate_v2_backend_format(config.backend_fd())?;
        bootstrap_id_allocator_if_missing(config.backend_fd(), has_any_committed_long_object)?;
        validate_id_allocator_file(config.backend_fd())?;
        cleanup_create_tmp_residue(config.backend_fd())?;
        verify_backend_supports_xattr(config.backend_fd())?;
        let supports_renameat2 = probe_renameat2(config.backend_fd())?;
        Ok(Self {
            config: Arc::new(config),
            backend_lock_fd,
            poisoned: AtomicBool::new(false),
            mutation_txn_lock: Mutex::new(()),
            dir_visibility_locks: DirVisibilityLockTable::default(),
            dir_cache: DirCache::new(dir_cache_ttl),
            dir_fd_cache: DirFdCache::new(dir_cache_ttl),
            index_cache: IndexCache::new(),
            max_name_len,
            index_sync,
            supports_renameat2,
            #[cfg(test)]
            repair_anomalies: Mutex::new(Vec::new()),
        })
    }

    fn ensure_not_poisoned(&self) -> CoreResult<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(CoreError::Poisoned);
        }
        Ok(())
    }

    fn dir_visibility_is_active(&self, key: DirCacheKey) -> bool {
        let Some(entry) = self.dir_visibility_locks.entries.read().get(&key).cloned() else {
            return false;
        };
        entry.state.lock().active_writer
    }

    fn dir_visibility_snapshot(&self, key: DirCacheKey) -> Option<DirSnapshot> {
        let entry = self
            .dir_visibility_locks
            .entries
            .read()
            .get(&key)
            .cloned()?;
        entry.state.lock().committed_snapshot.clone()
    }

    fn set_dir_visibility_snapshot(&self, key: DirCacheKey, snapshot: DirSnapshot) {
        let entry = self.dir_visibility_locks.entry(key);
        entry.state.lock().committed_snapshot = Some(snapshot);
        entry.cv.notify_all();
    }

    fn handle_live_txn_failure(&self, txn: &TxnRecord, err: CoreError) -> CoreError {
        if let Err(rollback_err) =
            rollback_inflight_txn(self.config.backend_fd(), txn, RollbackMode::LiveFailure)
        {
            eprintln!(
                "longnamefs-rs v2: ERROR: live rollback failed; poisoning mount and leaving surviving .ln2_fs_txn in place (original_errno={} rollback_errno={} original_err={err:?} rollback_err={rollback_err:?})",
                core_err_to_errno(&err),
                core_err_to_errno(&rollback_err),
            );
            self.poisoned.store(true, Ordering::Release);
            return CoreError::Poisoned;
        }
        if let Err(clear_err) = clear_txn_record(self.config.backend_fd()) {
            eprintln!(
                "longnamefs-rs v2: ERROR: live rollback completed but txn clear failed; poisoning mount and leaving surviving .ln2_fs_txn in place (original_errno={} clear_errno={} original_err={err:?} clear_err={clear_err:?})",
                core_err_to_errno(&err),
                core_err_to_errno(&clear_err),
            );
            self.poisoned.store(true, Ordering::Release);
            return CoreError::Poisoned;
        }
        err
    }

    fn handle_live_pre_mutation_failure(&self, err: CoreError) -> CoreError {
        if let Err(clear_err) = clear_txn_record(self.config.backend_fd()) {
            eprintln!(
                "longnamefs-rs v2: ERROR: live pre-mutation failure could not clear txn; poisoning mount and leaving surviving .ln2_fs_txn in place (original_errno={} clear_errno={} original_err={err:?} clear_err={clear_err:?})",
                core_err_to_errno(&err),
                core_err_to_errno(&clear_err),
            );
            self.poisoned.store(true, Ordering::Release);
            return CoreError::Poisoned;
        }
        err
    }

    fn commit_long_create_namespace(
        &self,
        ctx: &mut ParentCtx,
        raw: &[u8],
        parent_segments: &[Vec<u8>],
        backend: &mut BackendName,
        tmp_backend: &BackendName,
        object_kind: libc::mode_t,
    ) -> CoreResult<Vec<u8>> {
        let backend_bytes = backend.display_bytes();
        let txn = TxnRecord::create_long(
            crate::v2::object_id::parse_long_object_id(&backend_bytes)?,
            backend_bytes.clone(),
            parent_segments.to_vec(),
            raw.to_vec(),
            tmp_backend.display_bytes(),
            object_kind,
        );
        write_txn_record(self.config.backend_fd(), &txn)?;

        if let Err(err) = rename_noreplace_same_dir(self, ctx.dir_fd.as_fd(), tmp_backend, backend)
        {
            return Err(self.handle_live_pre_mutation_failure(err));
        }

        if let Err(err) = core_fsync_dir(ctx.dir_fd.as_fd()).map_err(CoreError::from) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = clear_txn_record(self.config.backend_fd()) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        Ok(backend_bytes)
    }

    #[cfg(test)]
    fn record_test_repair_anomaly(&self, backend_name: &[u8], kind: TestRepairAnomalyKind) {
        self.repair_anomalies.lock().push(TestRepairAnomalyRecord {
            backend_name: backend_name.to_vec(),
            kind,
        });
    }

    #[cfg(test)]
    fn test_take_repair_anomalies(&self) -> Vec<TestRepairAnomalyRecord> {
        std::mem::take(&mut *self.repair_anomalies.lock())
    }

    pub(crate) fn invalidate_dir(&self, dir_fd: BorrowedFd<'_>) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.invalidate_dir_by_key(key);
        }
    }

    pub(crate) fn invalidate_dir_by_key(&self, key: DirCacheKey) {
        self.dir_cache.invalidate(key);
        self.dir_fd_cache.invalidate(key);
        self.dir_fd_cache.invalidate_name_index_dir(key);
    }

    pub(crate) fn patch_dir_cache(&self, dir_fd: BorrowedFd<'_>, op: CacheOp) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.dir_cache.patch(key, op.clone());
            self.dir_fd_cache.patch_name_index(key, op);
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

    pub(crate) fn try_dir_fd_by_backend_key(&self, backend: BackendKey) -> Option<Arc<OwnedFd>> {
        self.dir_fd_cache.get(DirCacheKey {
            dev: backend.dev,
            ino: backend.ino,
        })
    }

    pub(crate) fn open_dir_cached(
        &self,
        parent_fd: BorrowedFd<'_>,
        backend: &BackendName,
    ) -> CoreResult<Arc<OwnedFd>> {
        let parent_key = dir_cache_key(parent_fd);
        if let Some(parent_key) = parent_key
            && let Some(child_key) = self
                .dir_fd_cache
                .name_index_get(parent_key, backend.as_bytes())
            && let Some(cached) = self.dir_fd_cache.get(child_key)
        {
            return Ok(cached);
        }

        let c_name = backend.as_cstring()?;
        let fd = nix::fcntl::openat(
            parent_fd,
            c_name.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)?;
        let stat = fstat(fd.as_fd()).map_err(core_errno_from_nix)?;
        let key = DirCacheKey {
            dev: stat.st_dev,
            ino: stat.st_ino,
        };
        if let Some(parent_key) = parent_key {
            self.dir_fd_cache
                .name_index_insert(parent_key, backend.as_bytes(), key);
        }
        if let Some(cached) = self.dir_fd_cache.get(key) {
            return Ok(cached);
        }
        Ok(self.dir_fd_cache.insert(key, fd))
    }

    pub(crate) fn load_dir_entries(
        &self,
        handle: &Arc<DirHandle>,
        need_attr: bool,
    ) -> CoreResult<Arc<Vec<DirEntryInfo>>> {
        let key = dir_cache_key(handle.as_fd());
        let visibility_active =
            key.is_some_and(|cache_key| self.dir_visibility_is_active(cache_key));
        if let Some(cache_key) = key
            && let Some(hit) = self.dir_cache.get(cache_key)
            && (!need_attr || hit.has_attrs)
        {
            return Ok(hit.entries);
        }

        if visibility_active {
            if let Some(cache_key) = key {
                return visibility_snapshot_for_dir(self, cache_key, handle, need_attr);
            }
        }

        let logical = list_logical_entries(
            self,
            handle,
            self.max_name_len,
            self.index_sync,
            need_attr,
            visibility_active,
            false,
            true,
        )?;
        let has_attrs = logical.iter().all(|e| e.attr.is_some());
        if let Some(cache_key) = key {
            return Ok(self.dir_cache.insert(cache_key, logical, has_attrs));
        }
        Ok(Arc::new(logical))
    }

    pub(crate) fn load_dir_entries_snapshot(
        &self,
        handle: &Arc<DirHandle>,
        need_attr: bool,
        offset: i64,
    ) -> CoreResult<Arc<Vec<DirEntryInfo>>> {
        if offset <= 0 {
            *handle.snapshot.lock() = None;
        } else {
            let guard = handle.snapshot.lock();
            if let Some(snapshot) = guard.as_ref()
                && (!need_attr || snapshot.has_attrs)
            {
                return Ok(snapshot.entries.clone());
            }
        }

        let entries = self.load_dir_entries(handle, need_attr)?;
        let snapshot = DirSnapshot {
            entries: entries.clone(),
            has_attrs: entries.iter().all(|e| e.attr.is_some()),
        };
        let mut guard = handle.snapshot.lock();
        *guard = Some(snapshot);
        Ok(entries)
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
            let state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
            return Ok(ParentCtx { dir_fd, state });
        }
        let mut segs = path_segments(path)?;
        if segs.is_empty() {
            return Err(CoreError::NotFound);
        }

        let mut dir_fd = self.cached_root_fd()?;
        for seg in segs.drain(..) {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
            let visibility_key = dir_cache_key(dir_fd.as_fd());
            let active = visibility_key.is_some_and(|key| self.dir_visibility_is_active(key));
            let visibility_snapshot =
                visibility_key.and_then(|key| self.dir_visibility_snapshot(key));
            let (backend, _kind) = map_segment_for_lookup(
                dir_fd.as_fd(),
                &mut state,
                &seg,
                self.max_name_len,
                active,
                visibility_snapshot.as_ref(),
            )?;
            if !active {
                maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
            }
            let next_fd = self.open_dir_cached(dir_fd.as_fd(), &backend)?;
            dir_fd = next_fd;
        }
        let state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
        Ok(ParentCtx { dir_fd, state })
    }

    fn backend_path_segments(&self, path: &OsStr) -> CoreResult<Vec<Vec<u8>>> {
        if path == OsStr::new("/") {
            return Ok(Vec::new());
        }

        let mut segs = path_segments(path)?;
        if segs.is_empty() {
            return Err(CoreError::NotFound);
        }

        let mut dir_fd = self.cached_root_fd()?;
        let mut backend_segments = Vec::with_capacity(segs.len());
        for seg in segs.drain(..) {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
            let visibility_key = dir_cache_key(dir_fd.as_fd());
            let active = visibility_key.is_some_and(|key| self.dir_visibility_is_active(key));
            let visibility_snapshot =
                visibility_key.and_then(|key| self.dir_visibility_snapshot(key));
            let (backend, _kind) = map_segment_for_lookup(
                dir_fd.as_fd(),
                &mut state,
                &seg,
                self.max_name_len,
                active,
                visibility_snapshot.as_ref(),
            )?;
            if !active {
                maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
            }
            backend_segments.push(backend.display_bytes());
            dir_fd = self.open_dir_cached(dir_fd.as_fd(), &backend)?;
        }

        Ok(backend_segments)
    }

    pub(crate) fn resolve_path(&self, path: &OsStr) -> CoreResult<Ln2Path> {
        if path == OsStr::new("/") {
            return Err(CoreError::from_errno(libc::EFAULT));
        }

        let segments = path_segments(path)?;
        if segments.is_empty() {
            return Err(CoreError::NotFound);
        }
        let mut dir_fd = self.cached_root_fd()?;
        for seg in segments[..segments.len() - 1].iter() {
            let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
            let visibility_key = dir_cache_key(dir_fd.as_fd());
            let active = visibility_key.is_some_and(|key| self.dir_visibility_is_active(key));
            let visibility_snapshot =
                visibility_key.and_then(|key| self.dir_visibility_snapshot(key));
            let (backend, _) = map_segment_for_lookup(
                dir_fd.as_fd(),
                &mut state,
                seg,
                self.max_name_len,
                active,
                visibility_snapshot.as_ref(),
            )?;
            if !active {
                maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
            }
            let next_fd = self.open_dir_cached(dir_fd.as_fd(), &backend)?;
            dir_fd = next_fd;
        }

        let mut state = load_dir_state(&self.index_cache, dir_fd.as_fd(), self.max_name_len)?;
        let visibility_key = dir_cache_key(dir_fd.as_fd());
        let active = visibility_key.is_some_and(|key| self.dir_visibility_is_active(key));
        let visibility_snapshot = visibility_key.and_then(|key| self.dir_visibility_snapshot(key));
        let raw_last = segments.last().unwrap().clone();
        let (backend_name, kind) = map_segment_for_lookup(
            dir_fd.as_fd(),
            &mut state,
            &raw_last,
            self.max_name_len,
            active,
            visibility_snapshot.as_ref(),
        )?;
        if !active {
            maybe_flush_index(dir_fd.as_fd(), &mut state, self.index_sync, false)?;
        }

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
        let parent_segments = self.backend_path_segments(parent)?;
        let mut ctx = self.resolve_dir(parent)?;
        let logical_name = normalize_osstr(name);
        if is_fs_internal_name(&logical_name) {
            return Err(CoreError::InternalMeta);
        }
        let kind = classify_committed_segment(&logical_name)?;
        let parent_key = dir_cache_key(ctx.dir_fd.as_fd()).ok_or(CoreError::NotFound)?;
        let visibility_snapshot = self.dir_visibility_snapshot(parent_key);
        let map_res = map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &logical_name,
            self.max_name_len,
            visibility_snapshot.is_some(),
            visibility_snapshot.as_ref(),
        );
        if visibility_snapshot.is_none() {
            maybe_flush_index(ctx.dir_fd.as_fd(), &mut ctx.state, self.index_sync, false)?;
        }
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
                parent_segments,
                backend_name,
                logical_name,
                kind,
                exists,
            },
        })
    }

    fn validate_new_logical_name(&self, raw: &[u8]) -> CoreResult<SegmentKind> {
        classify_segment(raw, self.max_name_len)
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
            NixRenameFlags::from_bits(flags).ok_or_else(|| CoreError::from_errno(libc::EINVAL))?;
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
        if flags == libc::RENAME_NOREPLACE {
            if !self.supports_renameat2 {
                return Err(CoreError::Unsupported);
            }
            if src.path.parent_key == dst.path.parent_key && src_backend == &dst_backend {
                return Err(CoreError::AlreadyExists);
            }
        } else if src.path.parent_key == dst.path.parent_key && src_backend == &dst_backend {
            return Ok(DirInvalidation::for_move(
                src.path.parent_key,
                dst.path.parent_key,
            ));
        }

        let src_c = src_backend.as_cstring()?;
        let src_stat = fstatat(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)?;

        let dst_exists = backend_entry_exists(dst.ctx.dir_fd.as_fd(), &dst_backend)?;
        if dst_exists {
            let dst_c = dst_backend.as_cstring()?;
            let dst_stat = fstatat(
                dst.ctx.dir_fd.as_fd(),
                dst_c.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)?;
            if backend_key_from_stat(&src_stat) == backend_key_from_stat(&dst_stat) {
                return if flags == libc::RENAME_NOREPLACE {
                    Err(CoreError::AlreadyExists)
                } else {
                    Ok(DirInvalidation::for_move(
                        src.path.parent_key,
                        dst.path.parent_key,
                    ))
                };
            }
        }
        if flags == libc::RENAME_NOREPLACE && dst_exists {
            return Err(CoreError::AlreadyExists);
        }
        let quarantine_backend = if dst_exists {
            Some(next_delete_quarantine_backend_name(
                dst.ctx.dir_fd.as_fd(),
                b".ln2_fs_delobj_",
                dst_backend.display_bytes().as_slice(),
            )?)
        } else {
            None
        };
        let quarantine_c = match quarantine_backend.as_ref() {
            Some(name) => Some(name.as_cstring()?),
            None => None,
        };
        let txn = TxnRecord::rename_short_to_short(
            src.path.parent_segments.clone(),
            dst.path.parent_segments.clone(),
            src_backend.display_bytes(),
            dst_backend.display_bytes(),
            quarantine_backend.as_ref().map(|name| name.display_bytes()),
        );
        write_txn_record(self.config.backend_fd(), &txn)?;

        if let Some(quarantine_c) = quarantine_c.as_ref()
            && let Err(err) = renameat(
                dst.ctx.dir_fd.as_fd(),
                dst_backend.as_cstring()?.as_c_str(),
                dst.ctx.dir_fd.as_fd(),
                quarantine_c.as_c_str(),
            )
            .map_err(core_errno_from_nix)
        {
            return Err(self.handle_live_pre_mutation_failure(err));
        }
        if let Err(err) = self.do_backend_rename(
            src.ctx.dir_fd.as_fd(),
            src_backend,
            dst.ctx.dir_fd.as_fd(),
            &dst_backend,
            0,
        ) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = sync_parent_dir_for_live_txn(src.ctx.dir_fd.as_fd()) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if src.path.parent_key != dst.path.parent_key
            && let Err(err) = sync_parent_dir_for_live_txn(dst.ctx.dir_fd.as_fd())
        {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = clear_txn_record(self.config.backend_fd()) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Some(quarantine_c) = quarantine_c.as_ref() {
            let _ =
                finalize_delete_quarantine_entry(dst.ctx.dir_fd.as_fd(), quarantine_c.as_c_str());
        }
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_upgrade(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        _flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        if dst.path.exists {
            return Err(CoreError::AlreadyExists);
        }
        let src_c = src_backend.as_cstring()?;
        let src_stat = fstatat(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)?;
        if (src_stat.st_mode & libc::S_IFMT) != libc::S_IFDIR && src_stat.st_nlink != 1 {
            return Err(CoreError::from_errno(libc::EPERM));
        }
        let root_fd = self.config.backend_fd();
        let object_id = allocate_long_object_id(root_fd)?;
        let dst_internal = format_long_object_name(object_id);
        let txn = TxnRecord::rename_short_to_long(
            object_id,
            src.path.parent_segments.clone(),
            dst.path.parent_segments.clone(),
            src_backend.display_bytes(),
            dst_internal.clone(),
            dst.path.logical_name.clone(),
            src_stat.st_mode,
        );
        write_txn_record(root_fd, &txn)?;

        if let Err(err) = set_internal_rawname_at(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            &dst.path.logical_name,
        ) {
            return Err(self.handle_live_pre_mutation_failure(err));
        }
        if let Err(err) = sync_mutated_backend_entry(src.ctx.dir_fd.as_fd(), src_c.as_c_str()) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = rename_noreplace(
            self,
            src.ctx.dir_fd.as_fd(),
            src_backend,
            dst.ctx.dir_fd.as_fd(),
            &BackendName::Internal(dst_internal.clone()),
        ) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = core_fsync_dir(src.ctx.dir_fd.as_fd()).map_err(CoreError::from) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if src.path.parent_key != dst.path.parent_key
            && let Err(err) = core_fsync_dir(dst.ctx.dir_fd.as_fd()).map_err(CoreError::from)
        {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = clear_txn_record(root_fd) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }

        {
            let mut guard = dst.ctx.state.index.write();
            guard
                .index
                .upsert(dst_internal.clone(), dst.path.logical_name.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        dst.ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(
            dst.ctx.dir_fd.as_fd(),
            &mut dst.ctx.state,
            self.index_sync,
        );
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_downgrade(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        _flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        let dst_backend = BackendName::Short(dst.path.logical_name.clone());
        if backend_entry_exists(dst.ctx.dir_fd.as_fd(), &dst_backend)? {
            return Err(CoreError::AlreadyExists);
        }

        let src_c = src_backend.as_cstring()?;
        let src_stat = fstatat(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)?;
        let old_raw = get_internal_rawname_at(src.ctx.dir_fd.as_fd(), src_c.as_c_str())
            .unwrap_or_else(|_| src.path.logical_name.clone());
        let root_fd = self.config.backend_fd();
        let txn = TxnRecord::rename_long_to_short(
            src.path.parent_segments.clone(),
            dst.path.parent_segments.clone(),
            src_backend.display_bytes(),
            old_raw,
            dst_backend.display_bytes(),
            src_stat.st_mode,
        );
        write_txn_record(root_fd, &txn)?;

        if let Err(err) = self.do_backend_rename(
            src.ctx.dir_fd.as_fd(),
            src_backend,
            dst.ctx.dir_fd.as_fd(),
            &dst_backend,
            0,
        ) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = core_fsync_dir(src.ctx.dir_fd.as_fd()).map_err(CoreError::from) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if src.path.parent_key != dst.path.parent_key
            && let Err(err) = core_fsync_dir(dst.ctx.dir_fd.as_fd()).map_err(CoreError::from)
        {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = clear_txn_record(root_fd) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }

        let backend_name = src_backend.display_bytes();
        {
            let mut src_guard = src.ctx.state.index.write();
            if src_guard.index.remove(&backend_name).is_some() {
                src_guard.pending = src_guard.pending.saturating_add(1);
            }
        }
        src.ctx.state.attr_cache.clear();
        dst.ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(
            src.ctx.dir_fd.as_fd(),
            &mut src.ctx.state,
            self.index_sync,
        );
        let inv = DirInvalidation::for_move(src.path.parent_key, dst.path.parent_key);
        self.invalidate_dirs(&inv);
        Ok(inv)
    }

    fn rename_long_to_long(
        &self,
        src: &mut RenameTarget,
        dst: &mut RenameTarget,
        _flags: u32,
    ) -> CoreResult<DirInvalidation> {
        let src_backend = src.path.backend_name.as_ref().ok_or(CoreError::NotFound)?;
        if dst.path.exists
            && !(src.path.parent_key == dst.path.parent_key
                && dst.path.backend_name.as_ref() == Some(src_backend))
        {
            return Err(CoreError::AlreadyExists);
        }
        let same_dir = src.path.parent_key == dst.path.parent_key;
        let dst_internal = src_backend.display_bytes();

        let src_c = src_backend.as_cstring()?;
        let old_raw = get_internal_rawname_at(src.ctx.dir_fd.as_fd(), src_c.as_c_str())
            .unwrap_or_else(|_| src.path.logical_name.clone());

        let root_fd = self.config.backend_fd();
        let txn = if same_dir {
            TxnRecord::rename_long_to_long_same_dir(
                src.path.parent_segments.clone(),
                dst_internal.clone(),
                old_raw.clone(),
                dst.path.logical_name.clone(),
                fstatat(
                    src.ctx.dir_fd.as_fd(),
                    src_c.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                )
                .map_err(core_errno_from_nix)?
                .st_mode,
            )
        } else {
            TxnRecord::rename_long_to_long_cross_dir(
                src.path.parent_segments.clone(),
                dst.path.parent_segments.clone(),
                dst_internal.clone(),
                old_raw.clone(),
                dst.path.logical_name.clone(),
                fstatat(
                    src.ctx.dir_fd.as_fd(),
                    src_c.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                )
                .map_err(core_errno_from_nix)?
                .st_mode,
            )
        };
        write_txn_record(root_fd, &txn)?;
        if let Err(err) = set_internal_rawname_at(
            src.ctx.dir_fd.as_fd(),
            src_c.as_c_str(),
            &dst.path.logical_name,
        ) {
            return Err(self.handle_live_pre_mutation_failure(err));
        }
        if let Err(err) = sync_mutated_backend_entry(src.ctx.dir_fd.as_fd(), src_c.as_c_str()) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if !same_dir
            && let Err(err) = self.do_backend_rename(
                src.ctx.dir_fd.as_fd(),
                src_backend,
                dst.ctx.dir_fd.as_fd(),
                &BackendName::Internal(dst_internal.clone()),
                0,
            )
        {
            return Err(self.handle_live_txn_failure(&txn, err));
        }
        if !same_dir {
            if let Err(err) = sync_parent_dir_for_live_txn(src.ctx.dir_fd.as_fd()) {
                return Err(self.handle_live_txn_failure(&txn, err));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(dst.ctx.dir_fd.as_fd()) {
                return Err(self.handle_live_txn_failure(&txn, err));
            }
        }
        if let Err(err) = clear_txn_record(root_fd) {
            return Err(self.handle_live_txn_failure(&txn, err));
        }

        let src_backend_name = src_backend.display_bytes();
        if same_dir {
            let mut guard = dst.ctx.state.index.write();
            guard
                .index
                .upsert(src_backend_name.clone(), dst.path.logical_name.clone());
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
        finalize_post_commit_index_state(
            src.ctx.dir_fd.as_fd(),
            &mut src.ctx.state,
            self.index_sync,
        );
        if src.path.parent_key != dst.path.parent_key {
            finalize_post_commit_index_state(
                dst.ctx.dir_fd.as_fd(),
                &mut dst.ctx.state,
                self.index_sync,
            );
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
        validate_rename_flags_v2(flags)?;

        let mut src = self.resolve_path_for_rename(origin_parent, origin_name)?;
        if !src.path.exists {
            return Err(CoreError::NotFound);
        }
        let mut dst = self.resolve_path_for_rename(parent, name)?;
        if !dst.path.exists {
            let dst_requested_kind = self.validate_new_logical_name(&dst.path.logical_name)?;
            debug_assert_eq!(dst_requested_kind, dst.path.kind);
        }

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

fn emit_startup_recovery_failure(err: &CoreError) {
    eprintln!(
        "longnamefs-rs v2: ERROR: recovery did not complete; leaving surviving .ln2_fs_txn for next startup attempt (errno={} err={err:?})",
        core_err_to_errno(err)
    );
}

pub struct LongNameFsV2Fuser {
    core: Arc<LongNameFsCore>,
    inode_store: InodeStore,
    handles: V2HandleTable,
    passthrough_cfg: bool,
    passthrough_runtime: AtomicBool,
    #[cfg(feature = "abi-7-40")]
    passthrough_handles: PassthroughHandleTable,
    #[cfg(feature = "abi-7-40")]
    passthrough_backing_cache: Mutex<PassthroughBackingCache>,
    #[cfg(feature = "abi-7-40")]
    passthrough_meta_policy: PassthroughMetaFdPolicy,
    writeback_cache_cfg: bool,
    max_write: NonZeroU32,
    attr_ttl: Duration,
    entry_ttl: Duration,
    open_attr_ttl: Duration,
    open_entry_ttl: Duration,
    notifier: FsNotifier,
}

#[derive(Clone, Copy, Debug)]
struct ReplacedChildSnapshot {
    ino: InodeId,
    backend: BackendKey,
}

#[derive(Clone, Debug, Default)]
struct RenameBookkeepingSnapshot {
    replaced_child: Option<ReplacedChildSnapshot>,
    replaced_backend: Option<BackendKey>,
    renamed_backend: Option<BackendKey>,
    renamed_backend_name: Option<Vec<u8>>,
}

impl LongNameFsV2Fuser {
    fn lock_mutation_txn_guard(&self) -> Result<parking_lot::MutexGuard<'_, ()>, i32> {
        let guard = self.core.mutation_txn_lock.lock();
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        Ok(guard)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: Config,
        max_name_len: usize,
        dir_cache_ttl: Option<Duration>,
        max_write_kb: u32,
        index_sync: IndexSync,
        attr_ttl: Duration,
        open_ttl: Duration,
        enable_passthrough: bool,
        enable_writeback_cache: bool,
        passthrough_meta_fd: PassthroughMetaFdConfig,
    ) -> CoreResult<Self> {
        #[cfg(not(feature = "abi-7-40"))]
        let _ = passthrough_meta_fd;

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
            passthrough_cfg: enable_passthrough,
            passthrough_runtime: AtomicBool::new(false),
            #[cfg(feature = "abi-7-40")]
            passthrough_handles: PassthroughHandleTable::default(),
            #[cfg(feature = "abi-7-40")]
            passthrough_backing_cache: Mutex::new(PassthroughBackingCache::new(
                PASSTHROUGH_BACKING_CACHE_MAX_ENTRIES,
            )),
            #[cfg(feature = "abi-7-40")]
            passthrough_meta_policy: PassthroughMetaFdPolicy::new(passthrough_meta_fd),
            writeback_cache_cfg: enable_writeback_cache,
            max_write,
            attr_ttl,
            entry_ttl: attr_ttl,
            open_attr_ttl: open_ttl,
            open_entry_ttl: open_ttl,
            notifier,
        })
    }

    fn ttl_for_open_count(
        base: Duration,
        open: Duration,
        is_file: bool,
        open_count: u32,
    ) -> Duration {
        if is_file && open_count > 0 {
            open
        } else {
            base
        }
    }

    fn ttl_for_entry(&self, entry: &InodeEntry) -> (Duration, Duration) {
        let is_file = entry.kind == InodeKind::File;
        let entry_ttl = Self::ttl_for_open_count(
            self.entry_ttl,
            self.open_entry_ttl,
            is_file,
            entry.open_count,
        );
        let attr_ttl =
            Self::ttl_for_open_count(self.attr_ttl, self.open_attr_ttl, is_file, entry.open_count);
        (entry_ttl, attr_ttl)
    }

    fn ttl_for_ino(&self, ino: InodeId) -> (Duration, Duration) {
        self.inode_store
            .get(ino)
            .map(|e| self.ttl_for_entry(&e))
            .unwrap_or((self.entry_ttl, self.attr_ttl))
    }

    fn getattr_via_parent_dirfd(&self, entry: &InodeEntry) -> Option<FuserFileAttr> {
        if entry.ino == ROOT_INODE || entry.backend_name.is_empty() {
            return None;
        }
        let parent_entry = self.inode_store.get(entry.parent)?;
        let parent_dirfd = self.core.try_dir_fd_by_backend_key(parent_entry.backend)?;
        let fname = cstring_from_bytes(&entry.backend_name).ok()?;
        fstatat(
            parent_dirfd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .ok()
        .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
    }

    #[cfg(feature = "abi-7-40")]
    fn clear_passthrough_meta_fd(&self, handle: &PassthroughHandleInner) {
        if handle.take_meta_fd().is_some() {
            self.passthrough_meta_policy.release_slot();
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn prepare_passthrough_meta_open(&self, entry: &InodeEntry) -> Option<(Arc<OwnedFd>, CString)> {
        if entry.ino == ROOT_INODE || entry.backend_name.is_empty() {
            return None;
        }
        let parent_entry = self.inode_store.get(entry.parent)?;
        let parent_dirfd = self.core.try_dir_fd_by_backend_key(parent_entry.backend)?;
        let fname = cstring_from_bytes(&entry.backend_name).ok()?;
        Some((parent_dirfd, fname))
    }

    #[cfg(feature = "abi-7-40")]
    fn open_passthrough_meta_fd_prepared(
        &self,
        parent_dirfd: &Arc<OwnedFd>,
        fname: &CString,
    ) -> CoreResult<OwnedFd> {
        nix::fcntl::openat(
            parent_dirfd.as_fd(),
            fname.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(core_errno_from_nix)
    }

    #[cfg(feature = "abi-7-40")]
    fn open_passthrough_meta_fd(&self, entry: &InodeEntry) -> CoreResult<OwnedFd> {
        let Some((parent_dirfd, fname)) = self.prepare_passthrough_meta_open(entry) else {
            return Err(CoreError::NotFound);
        };
        self.open_passthrough_meta_fd_prepared(&parent_dirfd, &fname)
    }

    #[cfg(feature = "abi-7-40")]
    fn maybe_promote_passthrough_meta_fd(
        &self,
        entry: &InodeEntry,
        handle: &PassthroughHandleInner,
        meta_ops: u32,
    ) {
        if !self.passthrough_meta_policy.should_promote(
            entry.open_count,
            handle.opened_at,
            meta_ops,
        ) {
            return;
        }
        if handle
            .promotion_inflight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let Some((parent_dirfd, fname)) = self.prepare_passthrough_meta_open(entry) else {
            handle.promotion_inflight.store(false, Ordering::Release);
            return;
        };

        let mut acquired_slot = false;
        let opened = if self.passthrough_meta_policy.try_acquire_slot() {
            acquired_slot = true;
            self.open_passthrough_meta_fd_prepared(&parent_dirfd, &fname)
        } else {
            Err(CoreError::NotFound)
        };
        handle.promotion_inflight.store(false, Ordering::Release);

        match opened {
            Ok(fd) => {
                install_promoted_meta_fd(
                    &self.passthrough_meta_policy,
                    &handle.meta_fd,
                    fd,
                    acquired_slot,
                );
            }
            Err(CoreError::Io(ioe))
                if matches!(ioe.raw_os_error(), Some(libc::EMFILE) | Some(libc::ENFILE)) =>
            {
                self.passthrough_meta_policy.enter_cooldown();
                if acquired_slot {
                    self.passthrough_meta_policy.release_slot();
                }
            }
            Err(_) => {
                if acquired_slot {
                    self.passthrough_meta_policy.release_slot();
                }
            }
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn passthrough_attr_via_meta_fd(
        &self,
        handle: &PassthroughHandleInner,
        entry: &InodeEntry,
    ) -> Option<FuserFileAttr> {
        let meta_ops = handle
            .meta_ops
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);

        if let Some(meta_fd) = handle.meta_fd() {
            if let Ok(stat) = fstat(meta_fd.as_fd()) {
                return Some(fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino));
            }
            self.clear_passthrough_meta_fd(handle);
        }

        self.maybe_promote_passthrough_meta_fd(entry, handle, meta_ops);

        if let Some(meta_fd) = handle.meta_fd() {
            if let Ok(stat) = fstat(meta_fd.as_fd()) {
                return Some(fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino));
            }
            self.clear_passthrough_meta_fd(handle);
        }
        None
    }

    fn passthrough_active(&self) -> bool {
        #[cfg(feature = "abi-7-40")]
        {
            self.passthrough_cfg && self.passthrough_runtime.load(Ordering::Relaxed)
        }
        #[cfg(not(feature = "abi-7-40"))]
        {
            false
        }
    }

    fn set_passthrough_runtime(&self, enabled: bool) {
        self.passthrough_runtime.store(enabled, Ordering::Relaxed);
    }

    #[cfg(feature = "abi-7-40")]
    fn warn_passthrough_userspace_fallback(
        &self,
        reason: &str,
        ino: u64,
        backend: BackendKey,
        flags: u32,
    ) {
        eprintln!(
            "longnamefs-rs v2: WARNING: passthrough userspace fallback ({reason}) ino={ino} backend.dev={} backend.ino={} flags=0x{flags:x}",
            backend.dev, backend.ino
        );
    }

    #[cfg(not(feature = "abi-7-40"))]
    fn warn_passthrough_userspace_fallback(
        &self,
        _reason: &str,
        _ino: u64,
        _backend: BackendKey,
        _flags: u32,
    ) {
    }

    #[cfg(feature = "abi-7-40")]
    fn is_passthrough_fh(&self, fh: u64) -> bool {
        self.passthrough_handles.contains(fh)
    }

    #[cfg(not(feature = "abi-7-40"))]
    fn is_passthrough_fh(&self, _fh: u64) -> bool {
        false
    }

    #[cfg(feature = "abi-7-40")]
    fn get_passthrough_handle(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        self.passthrough_handles.get(fh)
    }

    #[cfg(feature = "abi-7-40")]
    fn pin_passthrough_handle(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        let handle = self.get_passthrough_handle(fh);
        #[cfg(test)]
        if handle.is_some()
            && consume_test_force_passthrough_release_after_check(fh)
            && let Some(removed) = self.remove_passthrough_handle(fh)
        {
            self.clear_passthrough_meta_fd(removed.as_ref());
        }
        handle
    }

    #[cfg(feature = "abi-7-40")]
    fn remove_passthrough_handle(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        self.passthrough_handles.remove(fh)
    }

    #[cfg(not(feature = "abi-7-40"))]
    fn get_passthrough_handle(&self, _fh: u64) -> Option<()> {
        None
    }

    #[cfg(not(feature = "abi-7-40"))]
    fn remove_passthrough_handle(&self, _fh: u64) -> Option<()> {
        None
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
            self.core.dir_cache.patch(key, op.clone());
            self.core.dir_fd_cache.patch_name_index(key, op);
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
        if entry.ino == ROOT_INODE {
            let cached = self.core.cached_root_fd()?;
            let fd = dup_cloexec(cached.as_fd())?;
            let index = load_dir_state(&self.core.index_cache, fd.as_fd(), self.core.max_name_len)?;
            return Ok(DirHandle::new(fd, index));
        }

        if !entry.backend_name.is_empty()
            && let Some(parent_entry) = self.inode_store.get(entry.parent)
            && let Some(parent_dirfd) = self.core.try_dir_fd_by_backend_key(parent_entry.backend)
        {
            let backend = if entry.backend_name.starts_with(INTERNAL_PREFIX.as_bytes()) {
                BackendName::Internal(entry.backend_name.clone())
            } else {
                BackendName::Short(entry.backend_name.clone())
            };
            if let Ok(cached) = self.core.open_dir_cached(parent_dirfd.as_fd(), &backend)
                && let Ok(fd) = dup_cloexec(cached.as_fd())
            {
                let index =
                    load_dir_state(&self.core.index_cache, fd.as_fd(), self.core.max_name_len)?;
                return Ok(DirHandle::new(fd, index));
            }
        }

        let path = self.entry_path(entry)?;
        let mapped = self.core.resolve_path(&path)?;
        let cached = self
            .core
            .open_dir_cached(mapped.dir_fd.as_fd(), &mapped.backend_name)?;
        let fd = dup_cloexec(cached.as_fd())?;
        let index = load_dir_state(&self.core.index_cache, fd.as_fd(), self.core.max_name_len)?;
        Ok(DirHandle::new(fd, index))
    }

    fn seed_committed_dir_snapshot(&self, entry: &InodeEntry) -> Result<(), i32> {
        let key = dir_cache_key_from_backend(entry.backend);
        let handle = Arc::new(
            self.open_dir_handle(entry)
                .map_err(|err| core_err_to_errno(&err))?,
        );
        visibility_snapshot_for_dir(self.core.as_ref(), key, &handle, true)
            .map_err(|err| core_err_to_errno(&err))?;
        Ok(())
    }

    fn ensure_child_entry(
        &self,
        parent: InodeId,
        name: &OsStr,
        backend_name: Vec<u8>,
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
                backend_name,
            },
            lookup_inc,
        )
    }

    fn materialize_readdirplus_child(
        &self,
        parent: InodeId,
        info: &DirEntryInfo,
        attr: CoreFileAttr,
        backend_key: BackendKey,
    ) -> InodeEntry {
        self.inode_store.get_or_insert(
            backend_key,
            InodeKind::from(attr.kind),
            ParentName {
                parent,
                name: info.name.clone(),
                backend_name: info.backend_name.clone(),
            },
            1,
        )
    }

    fn rollback_readdirplus_child_lookup(&self, ino: InodeId) {
        let _ = self.inode_store.dec_lookup(ino, 1);
    }

    fn emit_readdirplus_child_with_lookup<F>(
        &self,
        parent: InodeId,
        info: &DirEntryInfo,
        attr: CoreFileAttr,
        backend_key: BackendKey,
        next: i64,
        mut add_entry: F,
    ) -> (InodeId, bool)
    where
        F: FnMut(InodeId, i64, &OsStr, &Duration, &FuserFileAttr) -> bool,
    {
        let child_entry = self.materialize_readdirplus_child(parent, info, attr, backend_key);
        let attr = fuser_attr_from_core(attr, child_entry.ino);
        let (entry_ttl, _) = self.ttl_for_entry(&child_entry);
        let full = add_entry(child_entry.ino, next, &info.name, &entry_ttl, &attr);
        if full {
            self.rollback_readdirplus_child_lookup(child_entry.ino);
        }
        (child_entry.ino, full)
    }

    fn lookup_from_visibility_snapshot(
        &self,
        parent: InodeId,
        name: &OsStr,
        snapshot: &DirSnapshot,
        lookup_inc: u64,
    ) -> CoreResult<(InodeEntry, CoreFileAttr)> {
        let raw = normalize_osstr(name);
        let info = snapshot
            .entries
            .iter()
            .find(|entry| entry.name.as_os_str().as_bytes() == raw)
            .ok_or(CoreError::NotFound)?;
        let attr = info.attr.ok_or(CoreError::NotFound)?;
        let backend_key = info.backend_key.ok_or(CoreError::NotFound)?;
        let child = self.inode_store.get_or_insert(
            backend_key,
            InodeKind::from(attr.kind),
            ParentName {
                parent,
                name: info.name.clone(),
                backend_name: info.backend_name.clone(),
            },
            lookup_inc,
        );
        Ok((child, attr))
    }

    fn lookup_existing_child_snapshot(
        &self,
        _parent: InodeId,
        parent_path: &OsStr,
        name: &OsStr,
    ) -> Option<ReplacedChildSnapshot> {
        let mut ctx = self.core.resolve_dir(parent_path).ok()?;
        let raw = normalize_osstr(name);
        let (backend, _) = map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
            false,
            None,
        )
        .ok()?;
        let fname = backend.as_cstring().ok()?;
        let stat = fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .ok()?;
        let backend_key = backend_key_from_stat(&stat);
        self.inode_store
            .get_by_backend(backend_key)
            .map(|entry| ReplacedChildSnapshot {
                ino: entry.ino,
                backend: backend_key,
            })
    }

    fn child_backend_snapshot(
        &self,
        parent_path: &OsStr,
        name: &OsStr,
    ) -> Option<(BackendKey, Vec<u8>)> {
        let mut dst_ctx = self.core.resolve_dir(parent_path).ok()?;
        let raw_new = normalize_osstr(name);
        let (backend, _) = map_segment_for_lookup(
            dst_ctx.dir_fd.as_fd(),
            &mut dst_ctx.state,
            &raw_new,
            self.core.max_name_len,
            false,
            None,
        )
        .ok()?;
        let fname = backend.as_cstring().ok()?;
        let stat = fstatat(
            dst_ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .ok()?;
        Some((backend_key_from_stat(&stat), backend.display_bytes()))
    }

    fn apply_rename_inode_bookkeeping(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        snapshot: RenameBookkeepingSnapshot,
    ) -> CoreResult<Option<InodeId>> {
        if let (Some(replaced_backend), Some(renamed_backend)) =
            (snapshot.replaced_backend, snapshot.renamed_backend)
            && replaced_backend == renamed_backend
        {
            return Ok(self
                .inode_store
                .get_by_backend(renamed_backend)
                .map(|entry| entry.ino));
        }

        #[cfg(test)]
        if let Some(errno) = test_atomic_errno_load(
            &TEST_FORCE_RENAME_BOOKKEEPING_ERRNO,
            &TEST_FORCE_RENAME_BOOKKEEPING_ERRNO_LOCAL,
        ) {
            return Err(CoreError::from_errno(errno));
        }

        let renamed_backend_name = snapshot.renamed_backend_name.unwrap_or_default();

        let mut renamed_ino = None;
        if let Some(backend_key) = snapshot.renamed_backend
            && let Some(child) = self.inode_store.get_by_backend(backend_key)
        {
            renamed_ino = Some(child.ino);
            let _ = self.inode_store.remove_parent_name(
                child.ino,
                &ParentName {
                    parent,
                    name: name.to_os_string(),
                    backend_name: Vec::new(),
                },
            );
            let new_parent_name = ParentName {
                parent: newparent,
                name: newname.to_os_string(),
                backend_name: renamed_backend_name,
            };
            let _ = self
                .inode_store
                .add_parent_name(child.ino, new_parent_name.clone());
            let _ = self.inode_store.move_entry(child.ino, new_parent_name);
        }

        if let Some(replaced_snapshot) = snapshot.replaced_child
            && Some(replaced_snapshot.ino) != renamed_ino
            && let Some(entry) = self.inode_store.get(replaced_snapshot.ino)
            && entry.backend == replaced_snapshot.backend
            && entry
                .parents
                .iter()
                .any(|p| p.parent == newparent && p.name == newname)
        {
            let _ = self.inode_store.remove_parent_name(
                replaced_snapshot.ino,
                &ParentName {
                    parent: newparent,
                    name: newname.to_os_string(),
                    backend_name: Vec::new(),
                },
            );
        }
        Ok(renamed_ino)
    }

    fn repair_rename_inode_identity_after_bookkeeping_failure(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        renamed_backend: Option<BackendKey>,
        renamed_backend_name: Option<Vec<u8>>,
    ) -> Option<InodeId> {
        let backend_key = renamed_backend?;
        let entry = self.inode_store.get_by_backend(backend_key)?;
        let new_parent_name = ParentName {
            parent: newparent,
            name: newname.to_os_string(),
            backend_name: renamed_backend_name.unwrap_or_default(),
        };
        let _ = self.inode_store.move_entry(entry.ino, new_parent_name.clone());
        let _ = self.inode_store.add_parent_name(entry.ino, new_parent_name);
        let _ = self.inode_store.remove_parent_name(
            entry.ino,
            &ParentName {
                parent,
                name: name.to_os_string(),
                backend_name: Vec::new(),
            },
        );
        Some(entry.ino)
    }

    fn apply_unlink_inode_bookkeeping(
        &self,
        parent: InodeId,
        name: &OsStr,
        backend_bytes: &[u8],
        existing_stat: nix::sys::stat::FileStat,
    ) -> Option<InodeId> {
        let backend_key = backend_key_from_stat(&existing_stat);
        let child = self.inode_store.get_by_backend(backend_key)?;
        let _ = self.inode_store.remove_parent_name(
            child.ino,
            &ParentName {
                parent,
                name: name.to_os_string(),
                backend_name: backend_bytes.to_vec(),
            },
        );
        Some(child.ino)
    }

    fn apply_rmdir_inode_bookkeeping(
        &self,
        parent: InodeId,
        name: &OsStr,
        backend_bytes: &[u8],
        existing_stat: nix::sys::stat::FileStat,
    ) -> Option<InodeId> {
        let backend_key = backend_key_from_stat(&existing_stat);
        let child = self.inode_store.get_by_backend(backend_key)?;
        let _ = self.inode_store.remove_parent_name(
            child.ino,
            &ParentName {
                parent,
                name: name.to_os_string(),
                backend_name: backend_bytes.to_vec(),
            },
        );
        Some(child.ino)
    }

    fn open_backend_file(&self, entry: &InodeEntry, flags: u32) -> CoreResult<OwnedFd> {
        if entry.ino != ROOT_INODE
            && !entry.backend_name.is_empty()
            && let Some(parent_entry) = self.inode_store.get(entry.parent)
            && let Some(parent_dirfd) = self.core.try_dir_fd_by_backend_key(parent_entry.backend)
            && let Ok(fname) = cstring_from_bytes(&entry.backend_name)
        {
            let oflag = oflag_from_bits(flags) | OFlag::O_CLOEXEC;
            if let Ok(fd) = nix::fcntl::openat(
                parent_dirfd.as_fd(),
                fname.as_c_str(),
                oflag,
                Mode::from_bits_truncate(0o666),
            ) {
                return Ok(fd);
            }
        }

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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum NotifyEventRecord {
    ParentInvalidation {
        parent: InodeId,
        name: OsString,
    },
    InodeInvalidation {
        ino: InodeId,
    },
    Delete {
        parent: InodeId,
        child: InodeId,
        name: OsString,
    },
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestRepairAnomalyKind {
    MalformedRawname,
    MissingRawnameXattr,
    StaleIndex,
    ConcurrentDisappearance,
}

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
struct TestRepairAnomalyRecord {
    backend_name: Vec<u8>,
    kind: TestRepairAnomalyKind,
}

#[cfg(test)]
fn test_global_repair_anomalies() -> &'static Mutex<Vec<TestRepairAnomalyRecord>> {
    TEST_GLOBAL_REPAIR_ANOMALIES.get_or_init(|| Mutex::new(Vec::new()))
}

#[cfg(test)]
fn record_test_repair_anomaly_global(backend_name: &[u8], kind: TestRepairAnomalyKind) {
    test_global_repair_anomalies()
        .lock()
        .push(TestRepairAnomalyRecord {
            backend_name: backend_name.to_vec(),
            kind,
        });
}

#[cfg(test)]
fn take_global_test_repair_anomalies() -> Vec<TestRepairAnomalyRecord> {
    std::mem::take(&mut *test_global_repair_anomalies().lock())
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct TestDirStateView {
    dirty: bool,
    pending: usize,
    attr_cache_keys: HashSet<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct TestEntrySuccess {
    ino: InodeId,
    attr: FuserFileAttr,
    entry_ttl: Duration,
    #[cfg(test)]
    backend_name: Vec<u8>,
    #[cfg(test)]
    state: TestDirStateView,
}

#[derive(Debug, Clone)]
struct TestCreateSuccess {
    ino: InodeId,
    attr: FuserFileAttr,
    entry_ttl: Duration,
    fh: u64,
    passthrough: bool,
    #[cfg(test)]
    used_passthrough_create_reply: bool,
    #[cfg(test)]
    reply_open_backing_called: bool,
    #[cfg(test)]
    reply_created_passthrough_called: bool,
    #[cfg(test)]
    backend_name: Vec<u8>,
    #[cfg(test)]
    state: TestDirStateView,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
struct TestCreateReplyTrace {
    open_backing_called: bool,
    created_called: bool,
    created_passthrough_called: bool,
}

#[derive(Debug, Clone)]
struct TestOpenSuccess {
    fh: u64,
    passthrough: bool,
}

#[derive(Debug, Clone)]
struct TestEmptySuccess {
    #[cfg(test)]
    used_callback_path: bool,
}

#[derive(Debug, Clone)]
struct TestRenameSuccess {
    renamed_ino: Option<InodeId>,
    #[cfg(test)]
    used_callback_path: bool,
}

#[cfg(feature = "abi-7-40")]
#[derive(Debug, Clone)]
struct OpenInternalSuccess {
    open: TestOpenSuccess,
    backing: Option<PassthroughHandleBacking>,
}

#[derive(Debug, Clone)]
struct CreateOpenOutcome {
    fh: u64,
    passthrough: bool,
    #[cfg(feature = "abi-7-40")]
    backing: Option<PassthroughHandleBacking>,
    #[cfg(test)]
    used_passthrough_create_reply: bool,
}

#[derive(Debug, Clone)]
struct CreateInternalSuccess {
    ino: InodeId,
    attr: FuserFileAttr,
    entry_ttl: Duration,
    open: CreateOpenOutcome,
    #[cfg(test)]
    backend_name: Vec<u8>,
    #[cfg(test)]
    state: TestDirStateView,
}

#[derive(Debug, Clone)]
struct TestDataSuccess {
    data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct TestWriteSuccess {
    size: u32,
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct TestStateSnapshot {
    dirty: bool,
    pending: usize,
}

#[cfg(test)]
fn capture_test_dir_state(state: &DirState) -> TestDirStateView {
    let guard = state.index.read();
    TestDirStateView {
        dirty: guard.index.is_dirty(),
        pending: guard.pending,
        attr_cache_keys: state.attr_cache.keys().cloned().collect(),
    }
}

impl LongNameFsV2Fuser {
    fn finish_open_file_result(
        &self,
        ino: InodeId,
        fd: OwnedFd,
        passthrough: bool,
    ) -> TestOpenSuccess {
        let fh = self.handles.insert_file(fd);
        let _ = self.inode_store.inc_open(ino);
        TestOpenSuccess { fh, passthrough }
    }

    #[cfg(feature = "abi-7-40")]
    fn finish_passthrough_open_result(
        &self,
        ino: InodeId,
        open_flags: u32,
        data_fd: OwnedFd,
        backing: PassthroughHandleBacking,
        keep_meta_fd: bool,
    ) -> OpenInternalSuccess {
        let data_fd = Arc::new(data_fd);
        let meta_fd = keep_meta_fd.then(|| data_fd.clone());
        let fh = self.handles.allocate_fh();
        self.passthrough_handles.insert_registered(
            fh,
            backing.clone(),
            data_fd,
            open_flags,
            meta_fd,
        );
        let _ = self.inode_store.inc_open(ino);
        OpenInternalSuccess {
            open: TestOpenSuccess {
                fh,
                passthrough: true,
            },
            backing: Some(backing),
        }
    }

    fn finish_create_open_fallback_result(&self, ino: InodeId, fd: OwnedFd) -> CreateOpenOutcome {
        let open = self.finish_open_file_result(ino, fd, false);
        CreateOpenOutcome {
            fh: open.fh,
            passthrough: open.passthrough,
            #[cfg(feature = "abi-7-40")]
            backing: None,
            #[cfg(test)]
            used_passthrough_create_reply: false,
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn finish_create_open_passthrough_result<F>(
        &self,
        child: &InodeEntry,
        open_flags: u32,
        data_fd: OwnedFd,
        mut register_backing: F,
    ) -> CreateOpenOutcome
    where
        F: FnMut(&OwnedFd) -> io::Result<PassthroughHandleBacking>,
    {
        match register_backing(&data_fd) {
            Ok(backing) => {
                let success = self
                    .finish_passthrough_open_result(child.ino, open_flags, data_fd, backing, false);
                CreateOpenOutcome {
                    fh: success.open.fh,
                    passthrough: success.open.passthrough,
                    backing: success.backing,
                    #[cfg(test)]
                    used_passthrough_create_reply: true,
                }
            }
            Err(err) => {
                let errno = err.raw_os_error().unwrap_or(libc::EIO);
                self.warn_passthrough_userspace_fallback(
                    "open_backing failed",
                    child.ino,
                    child.backend,
                    open_flags,
                );
                if errno == libc::EPERM || errno == libc::EOPNOTSUPP || errno == libc::ENOTTY {
                    self.set_passthrough_runtime(false);
                }
                self.finish_create_open_fallback_result(child.ino, data_fd)
            }
        }
    }

    fn emit_create_reply(&self, reply: ReplyCreateCompat, success: &CreateInternalSuccess) {
        #[cfg(feature = "abi-7-40")]
        {
            reply.created_with_optional_passthrough(
                &success.entry_ttl,
                &success.attr,
                0,
                success.open.fh,
                0,
                success.open.backing.as_ref(),
            );
        }

        #[cfg(not(feature = "abi-7-40"))]
        {
            reply.created(&success.entry_ttl, &success.attr, 0, success.open.fh, 0);
        }
    }

    fn prepare_create_reply_result(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        flags: i32,
        reply: &ReplyCreateCompat,
    ) -> Result<CreateInternalSuccess, i32> {
        self.create_result_internal(parent, name, mode, flags, |child, open_flags, fd| {
            #[cfg(feature = "abi-7-40")]
            if self.passthrough_active() && child.kind == InodeKind::File {
                return Ok(self.finish_create_open_passthrough_result(
                    child,
                    open_flags,
                    fd,
                    |fd| reply.open_backing_passthrough(fd.as_fd()),
                ));
            }

            #[cfg(not(feature = "abi-7-40"))]
            let _ = open_flags;

            Ok(self.finish_create_open_fallback_result(child.ino, fd))
        })
    }

    fn finalize_long_create_success<F>(
        &self,
        parent: InodeId,
        name: &OsStr,
        ctx: &mut ParentCtx,
        backend_name: Vec<u8>,
        stat: nix::sys::stat::FileStat,
        complete: F,
    ) -> Result<CreateInternalSuccess, i32>
    where
        F: FnOnce(&InodeEntry) -> Result<CreateOpenOutcome, i32>,
    {
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_name.clone(),
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
                backend_name: backend_name.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, backend_name, stat, 1);
        let open = complete(&child)?;
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(parent, name);
        self.notify_inode(child.ino);
        let entry_ttl = Self::ttl_for_open_count(
            self.entry_ttl,
            self.open_entry_ttl,
            child.kind == InodeKind::File,
            child.open_count.saturating_add(1),
        );
        Ok(CreateInternalSuccess {
            ino: child.ino,
            attr,
            entry_ttl,
            open,
            #[cfg(test)]
            backend_name: child.backend_name.clone(),
            #[cfg(test)]
            state: capture_test_dir_state(&ctx.state),
        })
    }

    fn finalize_long_test_entry_success(
        &self,
        parent: InodeId,
        name: &OsStr,
        ctx: &mut ParentCtx,
        backend_name: Vec<u8>,
        stat: nix::sys::stat::FileStat,
    ) -> TestEntrySuccess {
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_name.clone(),
            CachedAttr {
                attr: core_attr,
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: name.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_name.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, backend_name, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(parent, name);
        self.notify_inode(child.ino);
        let (entry_ttl, _) = self.ttl_for_entry(&child);
        TestEntrySuccess {
            ino: child.ino,
            attr,
            entry_ttl,
            #[cfg(test)]
            backend_name: child.backend_name.clone(),
            #[cfg(test)]
            state: capture_test_dir_state(&ctx.state),
        }
    }

    fn finalize_short_create_success<F>(
        &self,
        parent: InodeId,
        name: &OsStr,
        ctx: &mut ParentCtx,
        backend_name: Vec<u8>,
        stat: nix::sys::stat::FileStat,
        complete: F,
    ) -> Result<CreateInternalSuccess, i32>
    where
        F: FnOnce(&InodeEntry) -> Result<CreateOpenOutcome, i32>,
    {
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_name.clone(),
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
                backend_name: backend_name.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, backend_name, stat, 1);
        let open = complete(&child)?;
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        self.notify_entry_change(parent, name);
        self.notify_inode(child.ino);
        let entry_ttl = Self::ttl_for_open_count(
            self.entry_ttl,
            self.open_entry_ttl,
            child.kind == InodeKind::File,
            child.open_count.saturating_add(1),
        );
        Ok(CreateInternalSuccess {
            ino: child.ino,
            attr,
            entry_ttl,
            open,
            #[cfg(test)]
            backend_name: child.backend_name.clone(),
            #[cfg(test)]
            state: capture_test_dir_state(&ctx.state),
        })
    }

    fn finalize_short_test_entry_success(
        &self,
        parent: InodeId,
        name: &OsStr,
        ctx: &mut ParentCtx,
        backend_name: Vec<u8>,
        stat: nix::sys::stat::FileStat,
    ) -> TestEntrySuccess {
        let core_attr = core_attr_from_stat(&stat);
        let backend_key = backend_key_from_stat(&stat);
        ctx.state.attr_cache.insert(
            backend_name.clone(),
            CachedAttr {
                attr: core_attr,
                backend: backend_key,
            },
        );
        self.patch_dir_cache(
            ctx.dir_fd.as_fd(),
            CacheOp::Add(DirEntryInfo {
                name: name.to_os_string(),
                kind: core_attr.kind,
                attr: Some(core_attr),
                backend_name: backend_name.clone(),
                backend_key: Some(backend_key),
            }),
        );
        let child = self.ensure_child_entry(parent, name, backend_name, stat, 1);
        let attr = fuser_attr_from_core(core_attr, child.ino);
        self.notify_entry_change(parent, name);
        self.notify_inode(child.ino);
        let (entry_ttl, _) = self.ttl_for_entry(&child);
        TestEntrySuccess {
            ino: child.ino,
            attr,
            entry_ttl,
            #[cfg(test)]
            backend_name: child.backend_name.clone(),
            #[cfg(test)]
            state: capture_test_dir_state(&ctx.state),
        }
    }

    fn commit_short_create_namespace(
        &self,
        ctx: &ParentCtx,
        parent_segments: &[Vec<u8>],
        backend: &BackendName,
        object_kind: libc::mode_t,
    ) -> CoreResult<Vec<u8>> {
        let backend_bytes = backend.display_bytes();
        let txn =
            TxnRecord::create_short(parent_segments.to_vec(), backend_bytes.clone(), object_kind);
        write_txn_record(self.core.config.backend_fd(), &txn)?;
        if let Err(err) =
            sync_mutated_backend_entry(ctx.dir_fd.as_fd(), backend.as_cstring()?.as_c_str())
        {
            return Err(self.core.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
            return Err(self.core.handle_live_txn_failure(&txn, err));
        }
        if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
            return Err(self.core.handle_live_txn_failure(&txn, err));
        }
        Ok(backend_bytes)
    }

    #[cfg(feature = "abi-7-40")]
    fn open_result_internal<F>(
        &self,
        ino: InodeId,
        flags: u32,
        mut register_backing: F,
    ) -> Result<OpenInternalSuccess, i32>
    where
        F: FnMut(&OwnedFd) -> io::Result<PassthroughHandleBacking>,
    {
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        if self.passthrough_active() && entry.kind == InodeKind::File {
            let needed_caps = PassthroughBackingCacheCaps::for_open_flags(flags);
            let open_count = entry.open_count.saturating_add(1);
            let acquired_slot = self.passthrough_meta_policy.should_keep_on_open(open_count)
                && self.passthrough_meta_policy.try_acquire_slot();
            let data_fd = match self.open_backend_file(&entry, flags) {
                Ok(fd) => fd,
                Err(err) => {
                    if acquired_slot {
                        self.passthrough_meta_policy.release_slot();
                    }
                    return Err(core_err_to_errno(&err));
                }
            };

            let slot = {
                let mut cache = self.passthrough_backing_cache.lock();
                cache.slot(entry.backend)
            };

            let handle_backing = loop {
                let mut guard = slot.state.lock();
                match &*guard {
                    PassthroughBackingSlotState::Creating => {
                        slot.cv.wait(&mut guard);
                        continue;
                    }
                    PassthroughBackingSlotState::Ready(entry0) => {
                        if let Some(backing) = entry0
                            .backing
                            .as_ref()
                            .and_then(PassthroughHandleBackingWeak::upgrade)
                        {
                            if entry0.caps.satisfies(needed_caps) {
                                break backing;
                            }
                            drop(guard);
                            self.warn_passthrough_userspace_fallback(
                                "cached backing lacks required access",
                                ino,
                                entry.backend,
                                flags,
                            );
                            if acquired_slot {
                                self.passthrough_meta_policy.release_slot();
                            }
                            let open = self.finish_open_file_result(ino, data_fd, false);
                            return Ok(OpenInternalSuccess {
                                open,
                                backing: None,
                            });
                        }

                        *guard = PassthroughBackingSlotState::Creating;
                    }
                }
                drop(guard);

                let (backing_fd, caps) = match self.open_backend_file(&entry, libc::O_RDWR as u32) {
                    Ok(backing_fd) => (
                        backing_fd,
                        PassthroughBackingCacheCaps {
                            read: true,
                            write: true,
                        },
                    ),
                    Err(_) => match self.open_backend_file(&entry, libc::O_RDONLY as u32) {
                        Ok(backing_fd) => (
                            backing_fd,
                            PassthroughBackingCacheCaps {
                                read: true,
                                write: false,
                            },
                        ),
                        Err(_) => match self.open_backend_file(&entry, libc::O_WRONLY as u32) {
                            Ok(backing_fd) => (
                                backing_fd,
                                PassthroughBackingCacheCaps {
                                    read: false,
                                    write: true,
                                },
                            ),
                            Err(err) => {
                                let mut guard = slot.state.lock();
                                *guard = PassthroughBackingSlotState::Ready(
                                    empty_passthrough_backing_cache_entry(),
                                );
                                slot.cv.notify_all();
                                drop(guard);

                                eprintln!(
                                    "longnamefs-rs v2: WARNING: passthrough userspace fallback (open backing fd failed: {err:?}) ino={ino} backend.dev={} backend.ino={} flags=0x{flags:x}",
                                    entry.backend.dev, entry.backend.ino
                                );
                                if acquired_slot {
                                    self.passthrough_meta_policy.release_slot();
                                }
                                let open = self.finish_open_file_result(ino, data_fd, false);
                                return Ok(OpenInternalSuccess {
                                    open,
                                    backing: None,
                                });
                            }
                        },
                    },
                };

                if !caps.satisfies(needed_caps) {
                    let mut guard = slot.state.lock();
                    *guard =
                        PassthroughBackingSlotState::Ready(empty_passthrough_backing_cache_entry());
                    slot.cv.notify_all();
                    drop(guard);

                    self.warn_passthrough_userspace_fallback(
                        "no backing fd satisfies requested access",
                        ino,
                        entry.backend,
                        flags,
                    );
                    if acquired_slot {
                        self.passthrough_meta_policy.release_slot();
                    }
                    let open = self.finish_open_file_result(ino, data_fd, false);
                    return Ok(OpenInternalSuccess {
                        open,
                        backing: None,
                    });
                }

                let backing_res = register_backing(&backing_fd);
                let mut guard = slot.state.lock();
                match backing_res {
                    Ok(backing) => {
                        *guard = PassthroughBackingSlotState::Ready(PassthroughBackingCacheEntry {
                            backing: Some(PassthroughHandleInner::downgrade(&backing)),
                            caps,
                        });
                        slot.cv.notify_all();
                        break backing;
                    }
                    Err(err) => {
                        *guard = PassthroughBackingSlotState::Ready(
                            empty_passthrough_backing_cache_entry(),
                        );
                        slot.cv.notify_all();
                        drop(guard);

                        let errno = err.raw_os_error().unwrap_or(libc::EIO);
                        self.warn_passthrough_userspace_fallback(
                            "open_backing failed",
                            ino,
                            entry.backend,
                            flags,
                        );
                        if errno == libc::EPERM
                            || errno == libc::EOPNOTSUPP
                            || errno == libc::ENOTTY
                        {
                            self.set_passthrough_runtime(false);
                        }
                        if acquired_slot {
                            self.passthrough_meta_policy.release_slot();
                        }
                        let open = self.finish_open_file_result(ino, data_fd, false);
                        return Ok(OpenInternalSuccess {
                            open,
                            backing: None,
                        });
                    }
                }
            };

            return Ok(self.finish_passthrough_open_result(
                ino,
                flags,
                data_fd,
                handle_backing,
                acquired_slot,
            ));
        }

        let fd = self
            .open_backend_file(&entry, flags)
            .map_err(|err| core_err_to_errno(&err))?;
        let open = self.finish_open_file_result(ino, fd, false);
        Ok(OpenInternalSuccess {
            open,
            backing: None,
        })
    }

    fn invalidate_dir_for_ino(&self, ino: InodeId) {
        if let Some(entry) = self.inode_store.get(ino)
            && entry.ino != ROOT_INODE
            && let Some(parent_entry) = self.inode_store.get(entry.parent)
            && let Some(parent_dirfd) = self.core.try_dir_fd_by_backend_key(parent_entry.backend)
        {
            self.invalidate_dir(parent_dirfd.as_fd());
        } else if let Some(entry) = self.inode_store.get(ino)
            && let Ok(path) = self.entry_path(&entry)
            && path != OsStr::new("/")
            && let Ok(mapped) = self.core.resolve_path(&path)
        {
            self.invalidate_dir(mapped.dir_fd.as_fd());
        }
    }

    #[cfg(feature = "abi-7-40")]
    fn setattr_passthrough_handle_result(
        &self,
        ino: InodeId,
        handle: &PassthroughHandleInner,
        update: PassthroughSetattrUpdate,
    ) -> Result<FuserFileAttr, i32> {
        if update.size.is_some()
            && !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_write()
        {
            return Err(libc::EBADF);
        }

        if update.mode.is_some()
            || update.uid.is_some()
            || update.gid.is_some()
            || update.atime.is_some()
            || update.mtime.is_some()
        {
            let _ = handle.meta_ops.fetch_add(1, Ordering::Relaxed);
        }

        if let Some(mode) = update.mode {
            nix::sys::stat::fchmod(handle.data_fd.as_fd(), Mode::from_bits_truncate(mode))
                .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;
        }
        if update.uid.is_some() || update.gid.is_some() {
            nix::unistd::fchown(
                handle.data_fd.as_fd(),
                update.uid.map(Uid::from_raw),
                update.gid.map(Gid::from_raw),
            )
            .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;
        }
        if let Some(size) = update.size {
            nix::unistd::ftruncate(handle.data_fd.as_fd(), size as i64)
                .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;
        }
        if update.atime.is_some() || update.mtime.is_some() {
            let at = timespec_from_time_or_now(update.atime);
            let mt = timespec_from_time_or_now(update.mtime);
            let times = [*at.as_ref(), *mt.as_ref()];
            let res = unsafe { libc::futimens(handle.data_fd.as_raw_fd(), times.as_ptr()) };
            if res < 0 {
                return Err(core_err_to_errno(&io::Error::last_os_error().into()));
            }
        }

        self.invalidate_dir_for_ino(ino);
        self.notify_inode(ino);

        fstat(handle.data_fd.as_fd())
            .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), ino))
            .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))
    }

    fn write_result_internal(
        &self,
        ino: InodeId,
        fh: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<TestWriteSuccess, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let offset = offset as i64;
        let file_handle = self.handles.get_file(fh);
        #[cfg(feature = "abi-7-40")]
        let passthrough_handle = if file_handle.is_none() {
            self.pin_passthrough_handle(fh)
        } else {
            None
        };

        if let Some(handle) = file_handle.as_ref() {
            let written = retry_eintr(|| pwrite(handle.as_fd(), data, offset))
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            self.invalidate_dir_for_ino(ino);
            self.notify_inode(ino);
            if self.core.config.sync_data() {
                sync_fd(handle.as_fd(), true)
                    .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            }
            return Ok(TestWriteSuccess {
                size: written as u32,
            });
        }

        #[cfg(feature = "abi-7-40")]
        if let Some(handle) = passthrough_handle.as_ref() {
            if !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_write() {
                return Err(libc::EBADF);
            }
            let written = retry_eintr(|| pwrite(handle.data_fd.as_fd(), data, offset))
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            self.invalidate_dir_for_ino(ino);
            self.notify_inode(ino);
            if self.core.config.sync_data() {
                sync_fd(handle.data_fd.as_fd(), true)
                    .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            }
            return Ok(TestWriteSuccess {
                size: written as u32,
            });
        }

        Err(libc::EBADF)
    }

    fn fsync_result_internal(&self, ino: InodeId, fh: u64, datasync: bool) -> Result<(), i32> {
        let file_handle = self.handles.get_file(fh);
        #[cfg(feature = "abi-7-40")]
        let passthrough_handle = if file_handle.is_none() {
            self.pin_passthrough_handle(fh)
        } else {
            None
        };

        let mut synced = false;
        if let Some(handle) = file_handle.as_ref() {
            sync_fd(handle.as_fd(), datasync)
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            synced = true;
        }
        #[cfg(feature = "abi-7-40")]
        if !synced && let Some(handle) = passthrough_handle.as_ref() {
            sync_fd(handle.data_fd.as_fd(), datasync)
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            synced = true;
        }
        if !synced {
            return Err(libc::EBADF);
        }
        self.invalidate_dir_for_ino(ino);
        self.notify_inode(ino);
        Ok(())
    }

    fn create_result_internal<F>(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        flags: i32,
        mut finish_open: F,
    ) -> Result<CreateInternalSuccess, i32>
    where
        F: FnMut(&InodeEntry, u32, OwnedFd) -> Result<CreateOpenOutcome, i32>,
    {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(name);
        let mut backend = map_segment_for_create(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        )
        .map_err(|e| core_err_to_errno(&e))?;

        if matches!(backend.1, SegmentKind::Short) {
            let fname = backend.0.as_cstring().map_err(|e| core_err_to_errno(&e))?;
            let backend_bytes = backend.0.display_bytes();
            let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
                .map_err(|e| core_err_to_errno(&e))?;
            let txn =
                TxnRecord::create_short(parent_segments, backend_bytes.clone(), libc::S_IFREG);
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            let fd = match nix::fcntl::openat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                oflag_from_bits(flags as u32) | OFlag::O_CLOEXEC | OFlag::O_CREAT | OFlag::O_EXCL,
                Mode::from_bits_truncate(mode & 0o777),
            ) {
                Ok(fd) => fd,
                Err(err) => {
                    let err = match err {
                        nix::errno::Errno::EEXIST => CoreError::AlreadyExists,
                        other => core_errno_from_nix(other),
                    };
                    return Err(core_err_to_errno(
                        &self.core.handle_live_pre_mutation_failure(err),
                    ));
                }
            };
            let stat = match fstat(fd.as_fd()).map_err(core_errno_from_nix) {
                Ok(stat) => stat,
                Err(err) => {
                    return Err(core_err_to_errno(
                        &self.core.handle_live_txn_failure(&txn, err),
                    ));
                }
            };
            if let Err(err) = sync_mutated_backend_entry(ctx.dir_fd.as_fd(), fname.as_c_str()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            return self.finalize_short_create_success(
                parent,
                name,
                &mut ctx,
                backend_bytes,
                stat,
                |child| finish_open(child, flags as u32, fd),
            );
        }

        backend.0 = BackendName::Internal(format_long_object_name(
            allocate_long_object_id(self.core.config.backend_fd())
                .map_err(|e| core_err_to_errno(&e))?,
        ));
        let tmp_bytes = select_create_tmp_internal_name(ctx.dir_fd.as_fd())
            .map_err(|e| core_err_to_errno(&e))?;
        let tmp_backend = BackendName::Internal(tmp_bytes);
        let tmp_c = tmp_backend
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        let fd = nix::fcntl::openat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            oflag_from_bits(flags as u32) | OFlag::O_CLOEXEC | OFlag::O_CREAT | OFlag::O_EXCL,
            Mode::from_bits_truncate(mode & 0o777),
        )
        .map_err(|err| match err {
            nix::errno::Errno::EEXIST => libc::EEXIST,
            other => core_err_to_errno(&core_errno_from_nix(other)),
        })?;
        if let Err(err) = set_internal_rawname(fd.as_fd(), &raw) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        if let Err(err) = sync_long_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        let committed_stat = fstat(fd.as_fd()).map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;

        let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
            .map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = match self.core.commit_long_create_namespace(
            &mut ctx,
            &raw,
            &parent_segments,
            &mut backend.0,
            &tmp_backend,
            libc::S_IFREG,
        ) {
            Ok(backend_bytes) => backend_bytes,
            Err(err) => {
                cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
                return Err(core_err_to_errno(&err));
            }
        };
        {
            let mut guard = ctx.state.index.write();
            guard.index.upsert(backend_bytes.clone(), raw.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        self.finalize_long_create_success(parent, name, &mut ctx, backend_bytes, committed_stat, |child| {
            finish_open(child, flags as u32, fd)
        })
    }

    fn mkdir_result_internal(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
    ) -> Result<TestEntrySuccess, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(name);
        let mut backend = map_segment_for_create(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        )
        .map_err(|e| core_err_to_errno(&e))?;
        if matches!(backend.1, SegmentKind::Short) {
            let backend_bytes = backend.0.display_bytes();
            let fname = backend.0.as_cstring().map_err(|e| core_err_to_errno(&e))?;
            let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
                .map_err(|e| core_err_to_errno(&e))?;
            let txn =
                TxnRecord::create_short(parent_segments, backend_bytes.clone(), libc::S_IFDIR);
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            if let Err(err) = mkdirat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                Mode::from_bits_truncate(mode),
            ) {
                return Err(core_err_to_errno(
                    &self
                        .core
                        .handle_live_pre_mutation_failure(core_errno_from_nix(err)),
                ));
            }
            let stat = match fstatat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)
            {
                Ok(stat) => stat,
                Err(err) => {
                    return Err(core_err_to_errno(
                        &self.core.handle_live_txn_failure(&txn, err),
                    ));
                }
            };
            if let Err(err) = sync_mutated_backend_entry(ctx.dir_fd.as_fd(), fname.as_c_str()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            return Ok(self.finalize_short_test_entry_success(
                parent,
                name,
                &mut ctx,
                backend_bytes,
                stat,
            ));
        }

        backend.0 = BackendName::Internal(format_long_object_name(
            allocate_long_object_id(self.core.config.backend_fd())
                .map_err(|e| core_err_to_errno(&e))?,
        ));
        let tmp_bytes = select_create_tmp_internal_name(ctx.dir_fd.as_fd())
            .map_err(|e| core_err_to_errno(&e))?;
        let tmp_backend = BackendName::Internal(tmp_bytes);
        let tmp_c = tmp_backend
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        mkdirat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            Mode::from_bits_truncate(mode),
        )
        .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        if let Err(err) = set_internal_rawname_at(ctx.dir_fd.as_fd(), tmp_c.as_c_str(), &raw) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        if let Err(err) = sync_long_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        let committed_stat = fstatat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;

        let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
            .map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = match self.core.commit_long_create_namespace(
            &mut ctx,
            &raw,
            &parent_segments,
            &mut backend.0,
            &tmp_backend,
            libc::S_IFDIR,
        ) {
            Ok(backend_bytes) => backend_bytes,
            Err(err) => {
                cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
                return Err(core_err_to_errno(&err));
            }
        };
        {
            let mut guard = ctx.state.index.write();
            guard.index.upsert(backend_bytes.clone(), raw.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        Ok(self.finalize_long_test_entry_success(
            parent,
            name,
            &mut ctx,
            backend_bytes,
            committed_stat,
        ))
    }

    fn symlink_result_internal(
        &self,
        parent: InodeId,
        link_name: &OsStr,
        target: &Path,
    ) -> Result<TestEntrySuccess, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(link_name);
        let mut backend = map_segment_for_create(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        )
        .map_err(|e| core_err_to_errno(&e))?;
        if matches!(backend.1, SegmentKind::Short) {
            let backend_bytes = backend.0.display_bytes();
            let fname = backend.0.as_cstring().map_err(|e| core_err_to_errno(&e))?;
            let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
                .map_err(|e| core_err_to_errno(&e))?;
            let txn =
                TxnRecord::create_short(parent_segments, backend_bytes.clone(), libc::S_IFLNK);
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            if let Err(err) = symlinkat(target.as_os_str(), ctx.dir_fd.as_fd(), fname.as_c_str()) {
                return Err(core_err_to_errno(
                    &self
                        .core
                        .handle_live_pre_mutation_failure(core_errno_from_nix(err)),
                ));
            }
            let stat = match fstatat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)
            {
                Ok(stat) => stat,
                Err(err) => {
                    return Err(core_err_to_errno(
                        &self.core.handle_live_txn_failure(&txn, err),
                    ));
                }
            };
            if let Err(err) = sync_long_create_staging_entry(ctx.dir_fd.as_fd(), &backend.0) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            return Ok(self.finalize_short_test_entry_success(
                parent,
                link_name,
                &mut ctx,
                backend_bytes,
                stat,
            ));
        }

        backend.0 = BackendName::Internal(format_long_object_name(
            allocate_long_object_id(self.core.config.backend_fd())
                .map_err(|e| core_err_to_errno(&e))?,
        ));
        let tmp_bytes = select_create_tmp_internal_name(ctx.dir_fd.as_fd())
            .map_err(|e| core_err_to_errno(&e))?;
        let tmp_backend = BackendName::Internal(tmp_bytes);
        let tmp_c = tmp_backend
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        symlinkat(target.as_os_str(), ctx.dir_fd.as_fd(), tmp_c.as_c_str())
            .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        if let Err(err) = set_internal_rawname_at(ctx.dir_fd.as_fd(), tmp_c.as_c_str(), &raw) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        if let Err(err) = sync_long_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        let committed_stat = fstatat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;

        let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
            .map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = match self.core.commit_long_create_namespace(
            &mut ctx,
            &raw,
            &parent_segments,
            &mut backend.0,
            &tmp_backend,
            libc::S_IFLNK,
        ) {
            Ok(backend_bytes) => backend_bytes,
            Err(err) => {
                cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
                return Err(core_err_to_errno(&err));
            }
        };
        {
            let mut guard = ctx.state.index.write();
            guard.index.upsert(backend_bytes.clone(), raw.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        Ok(self.finalize_long_test_entry_success(
            parent,
            link_name,
            &mut ctx,
            backend_bytes,
            committed_stat,
        ))
    }

    fn mknod_result_internal(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<TestEntrySuccess, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(name);
        let mut backend = map_segment_for_create(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
        )
        .map_err(|e| core_err_to_errno(&e))?;
        let sflag = nix::sys::stat::SFlag::from_bits_truncate(mode);
        let perm = Mode::from_bits_truncate(mode);
        if matches!(backend.1, SegmentKind::Short) {
            let fname = backend.0.as_cstring().map_err(|e| core_err_to_errno(&e))?;
            let backend_bytes = backend.0.display_bytes();
            let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
                .map_err(|e| core_err_to_errno(&e))?;
            let txn = TxnRecord::create_short(
                parent_segments,
                backend_bytes.clone(),
                sflag.bits() as libc::mode_t,
            );
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            if let Err(err) = mknodat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                sflag,
                perm,
                rdev as u64,
            ) {
                let err = match err {
                    nix::errno::Errno::EEXIST => CoreError::AlreadyExists,
                    other => core_errno_from_nix(other),
                };
                return Err(core_err_to_errno(
                    &self.core.handle_live_pre_mutation_failure(err),
                ));
            }
            let stat = match fstatat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(core_errno_from_nix)
            {
                Ok(stat) => stat,
                Err(err) => {
                    return Err(core_err_to_errno(
                        &self.core.handle_live_txn_failure(&txn, err),
                    ));
                }
            };
            if let Err(err) =
                sync_mutated_mknod_entry(ctx.dir_fd.as_fd(), fname.as_c_str(), sflag.bits())
            {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            return Ok(self.finalize_short_test_entry_success(
                parent,
                name,
                &mut ctx,
                backend_bytes,
                stat,
            ));
        }

        backend.0 = BackendName::Internal(format_long_object_name(
            allocate_long_object_id(self.core.config.backend_fd())
                .map_err(|e| core_err_to_errno(&e))?,
        ));
        let tmp_bytes = select_create_tmp_internal_name(ctx.dir_fd.as_fd())
            .map_err(|e| core_err_to_errno(&e))?;
        let tmp_backend = BackendName::Internal(tmp_bytes);
        let tmp_c = tmp_backend
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        mknodat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            sflag,
            perm,
            rdev as u64,
        )
        .map_err(|err| match err {
            nix::errno::Errno::EEXIST => libc::EEXIST,
            other => core_err_to_errno(&core_errno_from_nix(other)),
        })?;
        if let Err(err) = set_internal_rawname_at(ctx.dir_fd.as_fd(), tmp_c.as_c_str(), &raw) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        if let Err(err) = sync_long_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend) {
            cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
            return Err(core_err_to_errno(&err));
        }
        let committed_stat = fstatat(
            ctx.dir_fd.as_fd(),
            tmp_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|err| core_err_to_errno(&core_errno_from_nix(err)))?;

        let parent_segments = backend_path_segments_for_inode(&self.inode_store, parent)
            .map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = match self.core.commit_long_create_namespace(
            &mut ctx,
            &raw,
            &parent_segments,
            &mut backend.0,
            &tmp_backend,
            sflag.bits() as libc::mode_t,
        ) {
            Ok(backend_bytes) => backend_bytes,
            Err(err) => {
                cleanup_create_staging_entry(ctx.dir_fd.as_fd(), &tmp_backend);
                return Err(core_err_to_errno(&err));
            }
        };
        {
            let mut guard = ctx.state.index.write();
            guard.index.upsert(backend_bytes.clone(), raw.clone());
            guard.pending = guard.pending.saturating_add(1);
        }
        ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        Ok(self.finalize_long_test_entry_success(
            parent,
            name,
            &mut ctx,
            backend_bytes,
            committed_stat,
        ))
    }

    fn link_result_internal(
        &self,
        ino: InodeId,
        newparent: InodeId,
        newname: &OsStr,
    ) -> Result<TestEntrySuccess, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let target_entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        let target_path = self
            .entry_path(&target_entry)
            .map_err(|err| core_err_to_errno(&err))?;
        let target = self
            .core
            .resolve_path(&target_path)
            .map_err(|err| core_err_to_errno(&err))?;
        if target.backend_name.is_internal() {
            return Err(libc::EPERM);
        }

        let parent_entry = self.inode_store.get(newparent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|err| core_err_to_errno(&err))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|err| core_err_to_errno(&err))?;
        let raw_new = normalize_osstr(newname);
        let (dest_backend, dest_kind) = map_segment_for_create(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw_new,
            self.core.max_name_len,
        )
        .map_err(|err| core_err_to_errno(&err))?;
        if matches!(dest_kind, SegmentKind::Long) {
            return Err(libc::EPERM);
        }

        let txn = TxnRecord::link_short(
            backend_path_segments_for_inode(&self.inode_store, target_entry.parent)
                .map_err(|err| core_err_to_errno(&err))?,
            backend_path_segments_for_inode(&self.inode_store, newparent)
                .map_err(|err| core_err_to_errno(&err))?,
            target.backend_name.display_bytes(),
            dest_backend.display_bytes(),
        );
        if let Err(err) = write_txn_record(self.core.config.backend_fd(), &txn) {
            return Err(core_err_to_errno(&err));
        }

        let from_c = target
            .backend_name
            .as_cstring()
            .map_err(|err| core_err_to_errno(&err))?;
        let to_c = dest_backend
            .as_cstring()
            .map_err(|err| core_err_to_errno(&err))?;
        linkat(
            target.dir_fd.as_fd(),
            from_c.as_c_str(),
            ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
            LinkatFlags::empty(),
        )
        .map_err(core_errno_from_nix)
        .map_err(|err| core_err_to_errno(&self.core.handle_live_pre_mutation_failure(err)))?;
        let stat = fstatat(
            ctx.dir_fd.as_fd(),
            to_c.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)
        .map_err(|err| core_err_to_errno(&self.core.handle_live_txn_failure(&txn, err)))?;
        if let Err(err) = sync_mutated_backend_entry(target.dir_fd.as_fd(), from_c.as_c_str()) {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
        if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
        if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
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
        let child =
            self.ensure_child_entry(newparent, newname, dest_backend.display_bytes(), stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child.ino);
        let (entry_ttl, _) = self.ttl_for_entry(&child);
        Ok(TestEntrySuccess {
            ino: child.ino,
            attr,
            entry_ttl,
            #[cfg(test)]
            backend_name: child.backend_name.clone(),
            #[cfg(test)]
            state: capture_test_dir_state(&ctx.state),
        })
    }

    fn unlink_callback_result_internal(&self, parent: InodeId, name: &OsStr) -> Result<(), i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(name);
        let visibility_snapshot = self.core.dir_visibility_snapshot(dir_key);
        let (backend, kind) = map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
            visibility_snapshot.is_some(),
            visibility_snapshot.as_ref(),
        )
        .map_err(|e| core_err_to_errno(&e))?;
        let fname = backend.as_cstring().map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = backend.display_bytes();
        let existing_stat = fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        let rawname = if matches!(kind, SegmentKind::Long) {
            get_internal_rawname_at(ctx.dir_fd.as_fd(), fname.as_c_str())
                .unwrap_or_else(|_| raw.clone())
        } else {
            raw.clone()
        };
        let quarantine_backend = if matches!(kind, SegmentKind::Long) {
            next_delete_quarantine_backend_name(
                ctx.dir_fd.as_fd(),
                b".ln2_fs_delobj_",
                backend_bytes.as_slice(),
            )
            .map_err(|e| core_err_to_errno(&e))?
        } else {
            next_delete_quarantine_backend_name(
                ctx.dir_fd.as_fd(),
                b".ln2_fs_delobj_",
                backend_bytes.as_slice(),
            )
            .map_err(|e| core_err_to_errno(&e))?
        };
        let quarantine_c = Some(
            quarantine_backend
                .as_cstring()
                .map_err(|e| core_err_to_errno(&e))?,
        );
        if matches!(kind, SegmentKind::Long) {
            let txn = TxnRecord::unlink_long(
                backend_path_segments_for_inode(&self.inode_store, parent)
                    .map_err(|e| core_err_to_errno(&e))?,
                backend_bytes.clone(),
                quarantine_backend.display_bytes(),
                rawname,
                existing_stat.st_mode,
            );
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            if let Err(err) = renameat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                ctx.dir_fd.as_fd(),
                quarantine_c.as_ref().unwrap().as_c_str(),
            )
            .map_err(core_errno_from_nix)
            {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = core_fsync_dir(ctx.dir_fd.as_fd()).map_err(CoreError::from) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            let _ = finalize_delete_quarantine_entry(
                ctx.dir_fd.as_fd(),
                quarantine_c.as_ref().unwrap().as_c_str(),
            );
        } else {
            let txn = TxnRecord::unlink_short(
                backend_path_segments_for_inode(&self.inode_store, parent)
                    .map_err(|e| core_err_to_errno(&e))?,
                backend_bytes.clone(),
                quarantine_backend.display_bytes(),
            );
            write_txn_record(self.core.config.backend_fd(), &txn)
                .map_err(|e| core_err_to_errno(&e))?;
            if let Err(err) = renameat(
                ctx.dir_fd.as_fd(),
                fname.as_c_str(),
                ctx.dir_fd.as_fd(),
                quarantine_c.as_ref().unwrap().as_c_str(),
            )
            .map_err(core_errno_from_nix)
            {
                return Err(core_err_to_errno(
                    &self.core.handle_live_pre_mutation_failure(err),
                ));
            }
            if let Err(err) = sync_parent_dir_for_live_txn(ctx.dir_fd.as_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
                return Err(core_err_to_errno(
                    &self.core.handle_live_txn_failure(&txn, err),
                ));
            }
            let _ = finalize_delete_quarantine_entry(
                ctx.dir_fd.as_fd(),
                quarantine_c.as_ref().unwrap().as_c_str(),
            );
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
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        self.patch_dir_cache(ctx.dir_fd.as_fd(), CacheOp::Remove(backend_bytes.clone()));
        if let Some(child_ino) =
            self.apply_unlink_inode_bookkeeping(parent, name, &backend_bytes, existing_stat)
        {
            self.notify_delete(parent, child_ino, name);
        } else {
            self.notify_entry_change(parent, name);
        }
        Ok(())
    }

    fn rename_callback_result_internal(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        flags: u32,
    ) -> Result<(DirInvalidation, Option<InodeId>), i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let src_parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let dst_parent_entry = self.inode_store.get(newparent).ok_or(libc::ESTALE)?;
        let src_parent_path = self
            .entry_path(&src_parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dst_parent_path = self
            .entry_path(&dst_parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let src_key = dir_cache_key_from_backend(src_parent_entry.backend);
        let dst_key = dir_cache_key_from_backend(dst_parent_entry.backend);
        let rename_is_same_name = parent == newparent && name == newname;
        let (snapshot, inv) = {
            let _dir_write_guard =
                (src_key == dst_key).then(|| self.core.dir_visibility_locks.write_guard(src_key));
            let _dir_write_guards = if src_key == dst_key {
                Vec::new()
            } else {
                self.core
                    .dir_visibility_locks
                    .write_guards_ordered(&[src_key, dst_key])
            };
            self.seed_committed_dir_snapshot(&src_parent_entry)?;
            if src_key != dst_key {
                self.seed_committed_dir_snapshot(&dst_parent_entry)?;
            }
            let replaced_child = self.lookup_existing_child_snapshot(newparent, &dst_parent_path, newname);
            let replaced_backend = self.child_backend_snapshot(&dst_parent_path, newname);
            let inv = self
                .core
                .rename_with_flags(
                    &src_parent_path,
                    name,
                    &dst_parent_path,
                    newname,
                    flags,
                )
                .map_err(|e| core_err_to_errno(&e))?;
            let renamed_backend = self.child_backend_snapshot(&dst_parent_path, newname);
            (
                RenameBookkeepingSnapshot {
                    replaced_child,
                    replaced_backend: replaced_backend.map(|(backend, _)| backend),
                    renamed_backend: renamed_backend.as_ref().map(|(backend, _)| *backend),
                    renamed_backend_name: renamed_backend.map(|(_, backend_name)| backend_name),
                },
                inv,
            )
        };
        if rename_is_same_name {
            return Ok((inv, None));
        }
        let _dir_write_guard =
            (src_key == dst_key).then(|| self.core.dir_visibility_locks.write_guard(src_key));
        let _dir_write_guards = if src_key == dst_key {
            Vec::new()
        } else {
            self.core
                .dir_visibility_locks
                .write_guards_ordered(&[src_key, dst_key])
        };
        #[cfg(test)]
        maybe_pause_after_rename_commit_before_bookkeeping();
        let renamed_backend_for_fallback = snapshot.renamed_backend;
        let renamed_backend_name_for_fallback = snapshot.renamed_backend_name.clone();
        let renamed_child = self
            .apply_rename_inode_bookkeeping(
                parent,
                name,
                newparent,
                newname,
                snapshot,
            )
            .unwrap_or_else(|_| {
                self.repair_rename_inode_identity_after_bookkeeping_failure(
                    parent,
                    name,
                    newparent,
                    newname,
                    renamed_backend_for_fallback,
                    renamed_backend_name_for_fallback,
                )
            });
        Ok((inv, renamed_child))
    }

    fn rmdir_result_internal(&self, parent: InodeId, name: &OsStr) -> Result<(), i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let _txn_guard = self.lock_mutation_txn_guard()?;
        let parent_entry = self.inode_store.get(parent).ok_or(libc::ESTALE)?;
        let parent_path = self
            .entry_path(&parent_entry)
            .map_err(|e| core_err_to_errno(&e))?;
        let dir_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_write_guard = self.core.dir_visibility_locks.write_guard(dir_key);
        self.seed_committed_dir_snapshot(&parent_entry)?;
        let mut ctx = self
            .core
            .resolve_dir(&parent_path)
            .map_err(|e| core_err_to_errno(&e))?;
        let raw = normalize_osstr(name);
        let visibility_snapshot = self.core.dir_visibility_snapshot(dir_key);
        let (backend, kind) = map_segment_for_lookup(
            ctx.dir_fd.as_fd(),
            &mut ctx.state,
            &raw,
            self.core.max_name_len,
            visibility_snapshot.is_some(),
            visibility_snapshot.as_ref(),
        )
        .map_err(|e| core_err_to_errno(&e))?;
        let fname = backend.as_cstring().map_err(|e| core_err_to_errno(&e))?;
        let backend_bytes = backend.display_bytes();
        let existing_stat = fstatat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        remove_rmdir_residue_shallow(ctx.dir_fd.as_fd(), fname.as_c_str(), self.core.max_name_len)
            .map_err(|e| core_err_to_errno(&e))?;
        ensure_dir_empty_after_residue_cleanup(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            self.core.max_name_len,
        )
        .map_err(|e| core_err_to_errno(&e))?;
        let quarantine_backend = next_delete_quarantine_backend_name(
            ctx.dir_fd.as_fd(),
            b".ln2_fs_deldir_",
            backend_bytes.as_slice(),
        )
        .map_err(|e| core_err_to_errno(&e))?;
        let quarantine_c = quarantine_backend
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        let txn = TxnRecord::remove_dir(
            backend_path_segments_for_inode(&self.inode_store, parent)
                .map_err(|e| core_err_to_errno(&e))?,
            backend_bytes.clone(),
            quarantine_backend.display_bytes(),
        );
        write_txn_record(self.core.config.backend_fd(), &txn).map_err(|e| core_err_to_errno(&e))?;
        if let Err(err) = renameat(
            ctx.dir_fd.as_fd(),
            fname.as_c_str(),
            ctx.dir_fd.as_fd(),
            quarantine_c.as_c_str(),
        )
        .map_err(core_errno_from_nix)
        {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
        if let Err(err) = core_fsync_dir(ctx.dir_fd.as_fd()).map_err(CoreError::from) {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
        if let Err(err) = clear_txn_record(self.core.config.backend_fd()) {
            return Err(core_err_to_errno(
                &self.core.handle_live_txn_failure(&txn, err),
            ));
        }
        {
            let mut guard = ctx.state.index.write();
            if matches!(kind, SegmentKind::Long) && guard.index.remove(&backend_bytes).is_some() {
                guard.pending = guard.pending.saturating_add(1);
            }
        }
        ctx.state.attr_cache.clear();
        finalize_post_commit_index_state(ctx.dir_fd.as_fd(), &mut ctx.state, self.core.index_sync);
        self.patch_dir_cache(ctx.dir_fd.as_fd(), CacheOp::Remove(backend_bytes.clone()));
        let _ = finalize_dir_delete_quarantine_entry(ctx.dir_fd.as_fd(), quarantine_c.as_c_str());
        let child_key = DirCacheKey {
            dev: existing_stat.st_dev,
            ino: existing_stat.st_ino,
        };
        self.core.invalidate_dir_by_key(child_key);
        self.handles.clear_dir_attr_cache(child_key);
        if let Some(child_ino) =
            self.apply_rmdir_inode_bookkeeping(parent, name, &backend_bytes, existing_stat)
        {
            self.notify_delete(parent, child_ino, name);
        } else {
            self.notify_entry_change(parent, name);
        }
        Ok(())
    }

}

#[cfg(test)]
impl LongNameFsV2Fuser {
    fn test_notify_parent_invalidation(&self, parent: InodeId, name: &OsStr) {
        self.notifier
            .test_record(NotifyEvent::InvalEntry(parent, name.to_os_string()));
    }

    fn test_notify_inode_invalidation(&self, ino: InodeId) {
        self.notifier.test_record(NotifyEvent::InvalInode(ino));
    }

    fn test_notify_delete(&self, parent: InodeId, child: InodeId, name: &OsStr) {
        self.notifier
            .test_record(NotifyEvent::Delete(parent, child, name.to_os_string()));
    }

    fn test_subscribe_notifier(&self) -> mpsc::Receiver<NotifyEventRecord> {
        let (tx, rx) = mpsc::channel();
        let notifier = self.notifier.clone();
        notifier
            .inner
            .sender
            .lock()
            .replace(mpsc::Sender::clone(&tx));
        drop(tx);
        let (record_tx, record_rx) = mpsc::channel();
        let _ = thread::Builder::new()
            .name("ln2-test-notifier-recorder".to_string())
            .spawn(move || {
                while let Ok(event) = rx.recv() {
                    let record = match event {
                        NotifyEvent::InvalEntry(parent, name) => {
                            NotifyEventRecord::ParentInvalidation { parent, name }
                        }
                        NotifyEvent::InvalInode(ino) => {
                            NotifyEventRecord::InodeInvalidation { ino }
                        }
                        NotifyEvent::Delete(parent, child, name) => NotifyEventRecord::Delete {
                            parent,
                            child,
                            name,
                        },
                    };
                    let _ = record_tx.send(record);
                }
            });
        record_rx
    }

    fn test_set_passthrough_runtime(&self, enabled: bool) {
        self.set_passthrough_runtime(enabled);
    }

    #[cfg(feature = "abi-7-40")]
    fn test_get_passthrough_handle(&self, fh: u64) -> Option<Arc<PassthroughHandleInner>> {
        self.get_passthrough_handle(fh)
    }

    fn test_passthrough_runtime_enabled(&self) -> bool {
        self.passthrough_active()
    }

    fn test_root_dir_cache_key(&self) -> DirCacheKey {
        dir_cache_key(self.core.config.backend_fd())
            .expect("backend root dir cache key should exist")
    }

    fn test_take_repair_anomalies(&self) -> Vec<TestRepairAnomalyRecord> {
        let mut anomalies = self.core.test_take_repair_anomalies();
        anomalies.extend(take_global_test_repair_anomalies());
        anomalies
    }

    fn test_handle_fh(&self, fd: OwnedFd) -> u64 {
        self.handles.insert_file(fd)
    }

    fn test_state_snapshot_for_path(&self, path: &OsStr) -> CoreResult<TestStateSnapshot> {
        let ctx = self.core.resolve_dir(path)?;
        let guard = ctx.state.index.read();
        Ok(TestStateSnapshot {
            dirty: guard.index.is_dirty(),
            pending: guard.pending,
        })
    }

    fn test_attr_cache_contains_entry(
        &self,
        path: &OsStr,
        backend_name: &[u8],
    ) -> CoreResult<bool> {
        let ctx = self.core.resolve_dir(path)?;
        Ok(ctx.state.attr_cache.contains_key(backend_name))
    }

    fn test_dir_cache_contains_logical_child(
        &self,
        path: &OsStr,
        child: &OsStr,
    ) -> CoreResult<bool> {
        let ctx = self.core.resolve_dir(path)?;
        let Some(key) = dir_cache_key(ctx.dir_fd.as_fd()) else {
            return Ok(false);
        };
        Ok(self
            .core
            .dir_cache
            .get(key)
            .is_some_and(|hit| hit.entries.iter().any(|entry| entry.name == child)))
    }

    fn test_backend_name_for_ino(&self, ino: InodeId) -> Result<Vec<u8>, i32> {
        self.inode_store
            .get(ino)
            .map(|entry| entry.backend_name)
            .ok_or(libc::ESTALE)
    }

    fn test_inode_entry(&self, ino: InodeId) -> Result<InodeEntry, i32> {
        self.inode_store.get(ino).ok_or(libc::ESTALE)
    }

    fn test_lookup_entry(&self, parent: InodeId, name: &OsStr) -> CoreResult<InodeEntry> {
        let parent_entry = self.inode_store.get(parent).ok_or(CoreError::StaleInode)?;
        let visibility_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_read_guard = self.core.dir_visibility_locks.read_guard(visibility_key);
        if let Some(snapshot) = self.core.dir_visibility_snapshot(visibility_key) {
            return self
                .lookup_from_visibility_snapshot(parent, name, &snapshot, 0)
                .map(|(entry, _attr)| entry);
        }
        let parent_path = self.entry_path(&parent_entry)?;
        let child_path = crate::v2::path::make_child_path(&parent_path, name);
        let mapped = self.core.resolve_path(&child_path)?;
        let fname = mapped.backend_name.as_cstring()?;
        let stat = fstatat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(core_errno_from_nix)?;
        Ok(self.ensure_child_entry(parent, name, mapped.backend_name.display_bytes(), stat, 0))
    }

    fn test_hold_dir_read_guard(&self, ino: InodeId) -> Result<TestHeldDirReadGuard, i32> {
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        if entry.kind != InodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        Ok(TestHeldDirReadGuard {
            _guard: self
                .core
                .dir_visibility_locks
                .read_guard(dir_cache_key_from_backend(entry.backend)),
        })
    }

    fn test_create(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        flags: i32,
    ) -> Result<TestCreateSuccess, i32> {
        self.test_create_with_open_backing_errno(parent, name, mode, flags, 0)
    }

    #[cfg(feature = "abi-7-40")]
    fn test_create_with_open_backing_errno(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        flags: i32,
        open_backing_errno: i32,
    ) -> Result<TestCreateSuccess, i32> {
        let (reply, trace) =
            ReplyCreateCompat::for_test((open_backing_errno != 0).then_some(open_backing_errno));
        let success = self.prepare_create_reply_result(parent, name, mode, flags, &reply)?;
        self.emit_create_reply(reply, &success);
        let trace_snapshot = trace.lock().clone();

        Ok(TestCreateSuccess {
            ino: success.ino,
            attr: success.attr,
            entry_ttl: success.entry_ttl,
            fh: success.open.fh,
            passthrough: success.open.passthrough,
            #[cfg(test)]
            used_passthrough_create_reply: success.open.used_passthrough_create_reply,
            #[cfg(test)]
            reply_open_backing_called: trace_snapshot.open_backing_called,
            #[cfg(test)]
            reply_created_passthrough_called: trace_snapshot.created_passthrough_called,
            #[cfg(test)]
            backend_name: success.backend_name,
            #[cfg(test)]
            state: success.state,
        })
    }

    #[cfg(not(feature = "abi-7-40"))]
    fn test_create_with_open_backing_errno(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        flags: i32,
        _open_backing_errno: i32,
    ) -> Result<TestCreateSuccess, i32> {
        let success =
            self.create_result_internal(parent, name, mode, flags, |_child, _open_flags, fd| {
                Ok(self.finish_create_open_fallback_result(parent, fd))
            })?;

        Ok(TestCreateSuccess {
            ino: success.ino,
            attr: success.attr,
            entry_ttl: success.entry_ttl,
            fh: success.open.fh,
            passthrough: success.open.passthrough,
            #[cfg(test)]
            used_passthrough_create_reply: success.open.used_passthrough_create_reply,
            #[cfg(test)]
            reply_open_backing_called: false,
            #[cfg(test)]
            reply_created_passthrough_called: false,
            #[cfg(test)]
            backend_name: success.backend_name,
            #[cfg(test)]
            state: success.state,
        })
    }

    fn test_mkdir(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
    ) -> Result<TestEntrySuccess, i32> {
        self.mkdir_result_internal(parent, name, mode)
    }

    fn test_symlink(
        &self,
        parent: InodeId,
        link_name: &OsStr,
        target: &Path,
    ) -> Result<TestEntrySuccess, i32> {
        self.symlink_result_internal(parent, link_name, target)
    }

    fn test_mknod(
        &self,
        parent: InodeId,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<TestEntrySuccess, i32> {
        self.mknod_result_internal(parent, name, mode, rdev)
    }

    fn test_link(
        &self,
        ino: InodeId,
        newparent: InodeId,
        newname: &OsStr,
    ) -> Result<TestEntrySuccess, i32> {
        self.link_result_internal(ino, newparent, newname)
    }

    fn test_unlink(&self, parent: InodeId, name: &OsStr) -> Result<TestEmptySuccess, i32> {
        self.unlink_callback_result_internal(parent, name)?;
        Ok(TestEmptySuccess {
            #[cfg(test)]
            used_callback_path: true,
        })
    }

    fn test_rmdir(&self, parent: InodeId, name: &OsStr) -> Result<(), i32> {
        self.rmdir_result_internal(parent, name)
    }

    fn test_rename(
        &self,
        parent: InodeId,
        name: &OsStr,
        newparent: InodeId,
        newname: &OsStr,
        flags: u32,
    ) -> Result<TestRenameSuccess, i32> {
        let (_inv, renamed_ino) =
            self.rename_callback_result_internal(parent, name, newparent, newname, flags)?;
        Ok(TestRenameSuccess {
            renamed_ino,
            #[cfg(test)]
            used_callback_path: true,
        })
    }

    fn test_open(&self, ino: InodeId, flags: u32) -> Result<TestOpenSuccess, i32> {
        #[cfg(feature = "abi-7-40")]
        {
            self.open_result_internal(ino, flags, |_fd| {
                Ok(PassthroughHandleBacking::Test(Arc::new(())))
            })
            .map(|success| success.open)
        }
        #[cfg(not(feature = "abi-7-40"))]
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        #[cfg(not(feature = "abi-7-40"))]
        let fd = self
            .open_backend_file(&entry, flags)
            .map_err(|e| core_err_to_errno(&e))?;
        #[cfg(not(feature = "abi-7-40"))]
        Ok(self.finish_open_file_result(ino, fd, false))
    }

    fn test_release(&self, ino: InodeId, fh: u64) -> Result<(), i32> {
        #[cfg(feature = "abi-7-40")]
        if let Some(handle) = self.remove_passthrough_handle(fh) {
            self.clear_passthrough_meta_fd(handle.as_ref());
            let _ = self.inode_store.dec_open(ino);
            return Ok(());
        }
        if self.handles.remove(fh).is_none() {
            return Err(libc::EBADF);
        }
        let _ = self.inode_store.dec_open(ino);
        Ok(())
    }

    fn test_read(
        &self,
        _ino: InodeId,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<TestDataSuccess, i32> {
        let offset = offset as i64;
        #[cfg(feature = "abi-7-40")]
        if let Some(handle) = self.pin_passthrough_handle(fh) {
            if !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_read() {
                return Err(libc::EBADF);
            }
            let mut buf = vec![0u8; size as usize];
            return retry_eintr(|| pread(handle.data_fd.as_fd(), &mut buf, offset))
                .map(|read_len| {
                    buf.truncate(read_len);
                    TestDataSuccess { data: buf }
                })
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)));
        }

        let handle = self.handles.get_file(fh).ok_or(libc::EBADF)?;
        let mut buf = vec![0u8; size as usize];
        retry_eintr(|| pread(handle.as_fd(), &mut buf, offset))
            .map(|read_len| {
                buf.truncate(read_len);
                TestDataSuccess { data: buf }
            })
            .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))
    }

    fn test_write(
        &self,
        ino: InodeId,
        fh: u64,
        offset: u64,
        data: &[u8],
    ) -> Result<TestWriteSuccess, i32> {
        self.write_result_internal(ino, fh, offset, data)
    }

    fn test_getattr(&self, ino: InodeId, fh: Option<u64>) -> Result<FuserFileAttr, i32> {
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        let result = if let Some(fh) = fh {
            #[cfg(feature = "abi-7-40")]
            if let Some(handle) = self.pin_passthrough_handle(fh) {
                fstat(handle.data_fd.as_fd())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .map_err(core_errno_from_nix)
            } else {
                self.handles
                    .get_file(fh)
                    .and_then(|fd| fstat(fd.as_fd()).ok())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .ok_or(CoreError::NotFound)
            }
            #[cfg(not(feature = "abi-7-40"))]
            {
                self.handles
                    .get_file(fh)
                    .and_then(|fd| fstat(fd.as_fd()).ok())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .ok_or(CoreError::NotFound)
            }
        } else {
            self.getattr_via_parent_dirfd(&entry)
                .ok_or(CoreError::NotFound)
                .or_else(|_| self.attr_for_entry(&entry))
        };
        result.map_err(|e| core_err_to_errno(&e))
    }

    fn test_setattr_size(
        &self,
        ino: InodeId,
        fh: Option<u64>,
        size: u64,
    ) -> Result<FuserFileAttr, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        if let Some(fh) = fh
            && let Some(fd) = self.handles.get_file(fh)
        {
            nix::unistd::ftruncate(fd.as_fd(), size as i64)
                .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            let stat = fstat(fd.as_fd()).map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
            return Ok(fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino));
        }

        if let Some(fh) = fh
            && let Some(handle) = self.pin_passthrough_handle(fh)
        {
            return self.setattr_passthrough_handle_result(
                entry.ino,
                handle.as_ref(),
                PassthroughSetattrUpdate {
                    size: Some(size),
                    ..PassthroughSetattrUpdate::default()
                },
            );
        }

        let path = self.entry_path(&entry).map_err(|e| core_err_to_errno(&e))?;
        let mapped = self
            .core
            .resolve_path(&path)
            .map_err(|e| core_err_to_errno(&e))?;
        let fname = mapped
            .backend_name
            .as_cstring()
            .map_err(|e| core_err_to_errno(&e))?;
        let file = nix::fcntl::openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        nix::unistd::ftruncate(file.as_fd(), size as i64)
            .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)))?;
        self.attr_for_entry(&entry)
            .map_err(|e| core_err_to_errno(&e))
    }

    #[cfg(feature = "abi-7-40")]
    fn test_setattr_size_and_mode(
        &self,
        ino: InodeId,
        fh: u64,
        size: u64,
        mode: u32,
    ) -> Result<FuserFileAttr, i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        let handle = self.pin_passthrough_handle(fh).ok_or(libc::EBADF)?;
        self.setattr_passthrough_handle_result(
            entry.ino,
            handle.as_ref(),
            PassthroughSetattrUpdate {
                mode: Some(mode),
                size: Some(size),
                ..PassthroughSetattrUpdate::default()
            },
        )
    }

    fn test_fallocate(
        &self,
        _ino: InodeId,
        fh: u64,
        offset: u64,
        length: u64,
        mode: i32,
    ) -> Result<(), i32> {
        self.core
            .ensure_not_poisoned()
            .map_err(|e| core_err_to_errno(&e))?;
        #[cfg(target_os = "linux")]
        {
            let mode = FallocateFlags::from_bits_truncate(mode);
            let offset = offset as libc::off_t;
            let length = length as libc::off_t;
            if let Some(handle) = self.handles.get_file(fh) {
                return nix_fallocate(handle.as_fd(), mode, offset, length)
                    .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)));
            }
            if let Some(handle) = self.pin_passthrough_handle(fh) {
                if !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_write() {
                    return Err(libc::EBADF);
                }
                return nix_fallocate(handle.data_fd.as_fd(), mode, offset, length)
                    .map_err(|e| core_err_to_errno(&core_errno_from_nix(e)));
            }
            Err(libc::EBADF)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (ino, fh, offset, length, mode);
            Err(libc::EOPNOTSUPP)
        }
    }

    fn test_fsync(&self, ino: InodeId, fh: u64, datasync: bool) -> Result<(), i32> {
        self.fsync_result_internal(ino, fh, datasync)
    }

    fn test_readdir_names(&self, ino: InodeId, offset: i64) -> Result<Vec<OsString>, i32> {
        let entry = self.inode_store.get(ino).ok_or(libc::ESTALE)?;
        if entry.kind != InodeKind::Directory {
            return Err(libc::ENOTDIR);
        }
        let _dir_read_guard = self
            .core
            .dir_visibility_locks
            .read_guard(dir_cache_key_from_backend(entry.backend));
        let handle = Arc::new(
            self.open_dir_handle(&entry)
                .map_err(|e| core_err_to_errno(&e))?,
        );
        let dir_listing = self
            .core
            .load_dir_entries_snapshot(&handle, false, offset)
            .map_err(|e| core_err_to_errno(&e))?;
        Ok(dir_listing.iter().map(|info| info.name.clone()).collect())
    }
}

#[cfg(all(test, feature = "abi-7-40"))]
impl PassthroughHandleInner {
    fn test_data_fd_raw(&self) -> RawFd {
        self.data_fd.as_raw_fd()
    }

    fn test_open_flags(&self) -> u32 {
        self.open_flags
    }

    fn test_backing_identity(&self) -> usize {
        match &self.backing {
            PassthroughHandleBacking::Real(backing) => Arc::as_ptr(backing) as usize,
            PassthroughHandleBacking::Test(backing) => Arc::as_ptr(backing) as usize,
        }
    }
}

impl FuserFilesystem for LongNameFsV2Fuser {
    fn init(
        &mut self,
        _req: &FuserRequest<'_>,
        config: &mut KernelConfig,
    ) -> Result<(), std::io::Error> {
        let _ = config.set_max_write(self.max_write.get());
        if self.writeback_cache_cfg {
            match config.add_capabilities(FuserInitFlags::FUSE_WRITEBACK_CACHE) {
                Ok(()) => {
                    eprintln!("longnamefs-rs v2: writeback_cache requested and accepted");
                }
                Err(missing) => {
                    eprintln!(
                        "longnamefs-rs v2: writeback_cache not accepted (missing {missing:#x}), continuing without"
                    );
                }
            }
        } else {
            eprintln!("longnamefs-rs v2: writeback_cache disabled by CLI");
        }
        #[cfg(feature = "abi-7-40")]
        {
            if self.passthrough_cfg {
                // Enable passthrough by advertising a non-zero max_stack_depth.
                // A depth of 1 is sufficient when the backend is not itself a stacked filesystem.
                let _ = config.set_max_stack_depth(1);
                match config.add_capabilities(FuserInitFlags::FUSE_PASSTHROUGH) {
                    Ok(()) => {
                        self.set_passthrough_runtime(true);
                        eprintln!(
                            "longnamefs-rs v2: passthrough requested and FUSE_PASSTHROUGH accepted"
                        );
                    }
                    Err(err) => {
                        self.set_passthrough_runtime(false);
                        eprintln!(
                            "longnamefs-rs v2: passthrough requested but FUSE_PASSTHROUGH not accepted ({err:?}), disabling"
                        );
                    }
                }
            } else {
                self.set_passthrough_runtime(false);
                eprintln!("longnamefs-rs v2: passthrough disabled by CLI");
            }
        }
        #[cfg(not(feature = "abi-7-40"))]
        {
            if self.passthrough_cfg {
                eprintln!(
                    "longnamefs-rs v2: passthrough requested but fuser abi-7-40 is not compiled, disabling"
                );
            }
            self.set_passthrough_runtime(false);
        }
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        reply: FuserReplyEntry,
    ) {
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEntryCompat(reply);
        let Some(parent_entry) = self.inode_store.get(parent) else {
            reply.error(libc::ESTALE);
            return;
        };
        let visibility_key = dir_cache_key_from_backend(parent_entry.backend);
        let _dir_read_guard = self.core.dir_visibility_locks.read_guard(visibility_key);
        let raw = normalize_osstr(name);

        if let Some(snapshot) = self.core.dir_visibility_snapshot(visibility_key) {
            match self.lookup_from_visibility_snapshot(parent, name, &snapshot, 1) {
                Ok((child_entry, core_attr)) => {
                    let attr = fuser_attr_from_core(core_attr, child_entry.ino);
                    let (entry_ttl, _) = self.ttl_for_entry(&child_entry);
                    reply.entry(&entry_ttl, &attr, 0);
                }
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }

        if let Some(parent_dirfd) = self.core.try_dir_fd_by_backend_key(parent_entry.backend) {
            let mut state = match load_dir_state(
                &self.core.index_cache,
                parent_dirfd.as_fd(),
                self.core.max_name_len,
            ) {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            };
            let (backend, _kind) = match map_segment_for_lookup(
                parent_dirfd.as_fd(),
                &mut state,
                &raw,
                self.core.max_name_len,
                self.core.dir_visibility_is_active(visibility_key),
                self.core.dir_visibility_snapshot(visibility_key).as_ref(),
            ) {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            };
            if !self.core.dir_visibility_is_active(visibility_key)
                && let Err(err) = maybe_flush_index(
                    parent_dirfd.as_fd(),
                    &mut state,
                    self.core.index_sync,
                    false,
                )
            {
                reply.error(core_err_to_errno(&err));
                return;
            }
            let fname = match backend.as_cstring() {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            };
            let stat = match fstatat(
                parent_dirfd.as_fd(),
                fname.as_c_str(),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            ) {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            };
            let child_entry =
                self.ensure_child_entry(parent, name, backend.display_bytes(), stat, 1);
            let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child_entry.ino);
            let (entry_ttl, _) = self.ttl_for_entry(&child_entry);
            reply.entry(&entry_ttl, &attr, 0);
            return;
        }

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
        let backend_bytes = mapped.backend_name.display_bytes();
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
        let child_entry = self.ensure_child_entry(parent, name, backend_bytes, stat, 1);
        let attr = fuser_attr_from_core(core_attr_from_stat(&stat), child_entry.ino);
        let (entry_ttl, _) = self.ttl_for_entry(&child_entry);
        reply.entry(&entry_ttl, &attr, 0);
    }

    fn getattr(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: Option<FuserFileHandle>,
        reply: FuserReplyAttr,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh.map(fh_from_fuser);
        let reply = ReplyAttrCompat(reply);
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let result = if let Some(fh) = fh {
            #[cfg(feature = "abi-7-40")]
            if let Some(handle) = self.pin_passthrough_handle(fh) {
                fstat(handle.data_fd.as_fd())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .map_err(core_errno_from_nix)
            } else {
                self.handles
                    .get_file(fh)
                    .and_then(|fd| fstat(fd.as_fd()).ok())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .ok_or(CoreError::NotFound)
            }

            #[cfg(not(feature = "abi-7-40"))]
            {
                self.handles
                    .get_file(fh)
                    .and_then(|fd| fstat(fd.as_fd()).ok())
                    .map(|stat| fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino))
                    .ok_or(CoreError::NotFound)
            }
        } else {
            self.getattr_via_parent_dirfd(&entry)
                .ok_or(CoreError::NotFound)
                .or_else(|_| self.attr_for_entry(&entry))
        };
        match result {
            Ok(attr) => {
                let (_, attr_ttl) = self.ttl_for_entry(&entry);
                reply.attr(&attr_ttl, &attr)
            }
            Err(err) => reply.error(core_err_to_errno(&err)),
        }
    }

    fn setattr(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<FuserFileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<FuserBsdFileFlags>,
        reply: FuserReplyAttr,
    ) {
        let is_mutation = mode.is_some()
            || uid.is_some()
            || gid.is_some()
            || size.is_some()
            || atime.is_some()
            || mtime.is_some();
        if is_mutation {
            if let Err(err) = self.core.ensure_not_poisoned() {
                ReplyAttrCompat(reply).error(core_err_to_errno(&err));
                return;
            }
        }
        let ino = inode_id_from_fuser(ino);
        let fh = fh.map(fh_from_fuser);
        let flags = flags.map(bsd_file_flags_bits);
        let reply = ReplyAttrCompat(reply);
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        if flags.unwrap_or(0) != 0 {
            reply.error(libc::EOPNOTSUPP);
            return;
        }
        if entry.ino == ROOT_INODE {
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
            if let Some(errno) = root_setattr_size_errno(size) {
                reply.error(errno);
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
                Ok(attr) => {
                    let (_, attr_ttl) = self.ttl_for_entry(&entry);
                    reply.attr(&attr_ttl, &attr)
                }
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }

        if let Some(fh) = fh
            && let Some(fd) = self.handles.get_file(fh)
        {
            if let Some(mode) = mode
                && let Err(err) = nix::sys::stat::fchmod(fd.as_fd(), Mode::from_bits_truncate(mode))
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            if (uid.is_some() || gid.is_some())
                && let Err(err) =
                    nix::unistd::fchown(fd.as_fd(), uid.map(Uid::from_raw), gid.map(Gid::from_raw))
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            if let Some(size) = size
                && let Err(err) = nix::unistd::ftruncate(fd.as_fd(), size as i64)
            {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
            if atime.is_some() || mtime.is_some() {
                let at = timespec_from_time_or_now(atime);
                let mt = timespec_from_time_or_now(mtime);
                let times = [*at.as_ref(), *mt.as_ref()];
                let res = unsafe { libc::futimens(fd.as_raw_fd(), times.as_ptr()) };
                if res < 0 {
                    reply.error(core_err_to_errno(&io::Error::last_os_error().into()));
                    return;
                }
            }

            if let Some(parent_entry) = self.inode_store.get(entry.parent)
                && let Some(parent_dirfd) =
                    self.core.try_dir_fd_by_backend_key(parent_entry.backend)
            {
                self.invalidate_dir(parent_dirfd.as_fd());
            }
            self.notify_inode(ino);

            match fstat(fd.as_fd()).map_err(core_errno_from_nix) {
                Ok(stat) => {
                    let attr = fuser_attr_from_core(core_attr_from_stat(&stat), entry.ino);
                    let (_, attr_ttl) = self.ttl_for_entry(&entry);
                    reply.attr(&attr_ttl, &attr);
                }
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }

        #[cfg(feature = "abi-7-40")]
        if let Some(fh) = fh
            && let Some(handle) = self.pin_passthrough_handle(fh)
            && (size.is_some()
                || mode.is_some()
                || uid.is_some()
                || gid.is_some()
                || atime.is_some()
                || mtime.is_some())
        {
            match self.setattr_passthrough_handle_result(
                entry.ino,
                handle.as_ref(),
                PassthroughSetattrUpdate {
                    mode,
                    uid,
                    gid,
                    size,
                    atime,
                    mtime,
                },
            ) {
                Ok(attr) => {
                    let (_, attr_ttl) = self.ttl_for_entry(&entry);
                    reply.attr(&attr_ttl, &attr);
                }
                Err(err) => reply.error(err),
            }
            return;
        }

        if entry.ino != ROOT_INODE
            && !entry.backend_name.is_empty()
            && let Some(parent_entry) = self.inode_store.get(entry.parent)
            && let Some(parent_dirfd) = self.core.try_dir_fd_by_backend_key(parent_entry.backend)
            && let Ok(fname) = cstring_from_bytes(&entry.backend_name)
        {
            if let Some(mode) = mode
                && let Err(err) = fchmodat(
                    parent_dirfd.as_fd(),
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
                    parent_dirfd.as_fd(),
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
                let file = match nix::fcntl::openat(
                    parent_dirfd.as_fd(),
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
            if atime.is_some() || mtime.is_some() {
                let at = timespec_from_time_or_now(atime);
                let mt = timespec_from_time_or_now(mtime);
                if let Err(err) = utimensat(
                    parent_dirfd.as_fd(),
                    fname.as_c_str(),
                    &at,
                    &mt,
                    UtimensatFlags::NoFollowSymlink,
                ) {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
            }

            self.invalidate_dir(parent_dirfd.as_fd());
            self.notify_inode(ino);
            match self
                .getattr_via_parent_dirfd(&entry)
                .ok_or(CoreError::NotFound)
                .or_else(|_| self.attr_for_entry(&entry))
            {
                Ok(attr) => {
                    let (_, attr_ttl) = self.ttl_for_entry(&entry);
                    reply.attr(&attr_ttl, &attr);
                }
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }

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
            let mut truncated = false;
            if let Some(fh) = fh
                && let Some(fd) = self.handles.get_file(fh)
            {
                if let Err(err) = nix::unistd::ftruncate(fd.as_fd(), size as i64) {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
                truncated = true;
            }
            if !truncated {
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
            Ok(attr) => {
                let (_, attr_ttl) = self.ttl_for_entry(&entry);
                reply.attr(&attr_ttl, &attr)
            }
            Err(err) => reply.error(core_err_to_errno(&err)),
        }
    }

    fn readlink(&self, _req: &FuserRequest<'_>, ino: FuserInodeNo, reply: FuserReplyData) {
        let ino = inode_id_from_fuser(ino);
        let reply = ReplyDataCompat(reply);
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
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        rdev: u32,
        reply: FuserReplyEntry,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEntryCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEntryCompat(reply);
        match self.mknod_result_internal(parent, name, mode, rdev) {
            Ok(success) => reply.entry(&success.entry_ttl, &success.attr, 0),
            Err(err) => reply.error(err),
        }
    }

    fn create(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        flags: i32,
        reply: FuserReplyCreate,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyCreateCompat::new(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyCreateCompat::new(reply);
        match self.prepare_create_reply_result(parent, name, mode, flags, &reply) {
            Ok(success) => self.emit_create_reply(reply, &success),
            Err(err) => reply.error(err),
        }
    }

    fn rename(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        newparent: FuserInodeNo,
        newname: &OsStr,
        flags: FuserRenameFlags,
        reply: FuserReplyEmpty,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEmptyCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let newparent = inode_id_from_fuser(newparent);
        let flags = rename_flags_bits(flags);
        let reply = ReplyEmptyCompat(reply);
        let (inv, renamed_child) =
            match self.rename_callback_result_internal(parent, name, newparent, newname, flags) {
                Ok(v) => v,
                Err(err) => {
                    reply.error(err);
                    return;
                }
            };
        self.apply_invalidation(inv);
        self.notify_entry_change(parent, name);
        self.notify_entry_change(newparent, newname);
        if let Some(child) = renamed_child {
            self.notify_inode(child);
        }
        reply.ok();
    }

    fn link(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        newparent: FuserInodeNo,
        newname: &OsStr,
        reply: FuserReplyEntry,
    ) {
        let ino = inode_id_from_fuser(ino);
        let newparent = inode_id_from_fuser(newparent);
        let reply = ReplyEntryCompat(reply);
        let success = match self.link_result_internal(ino, newparent, newname) {
            Ok(success) => success,
            Err(err) => {
                reply.error(err);
                return;
            }
        };
        self.notify_entry_change(newparent, newname);
        self.notify_inode(ino);
        self.notify_inode(success.ino);
        reply.entry(&success.entry_ttl, &success.attr, 0);
    }

    fn unlink(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEmptyCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEmptyCompat(reply);
        match self.unlink_callback_result_internal(parent, name) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn rmdir(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEmptyCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEmptyCompat(reply);
        match self.rmdir_result_internal(parent, name) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn symlink(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        link_name: &OsStr,
        target: &Path,
        reply: FuserReplyEntry,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEntryCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEntryCompat(reply);
        match self.symlink_result_internal(parent, link_name, target) {
            Ok(success) => reply.entry(&success.entry_ttl, &success.attr, 0),
            Err(err) => reply.error(err),
        }
    }

    fn mkdir(
        &self,
        _req: &FuserRequest<'_>,
        parent: FuserInodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: FuserReplyEntry,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEntryCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let parent = inode_id_from_fuser(parent);
        let reply = ReplyEntryCompat(reply);
        match self.mkdir_result_internal(parent, name, mode) {
            Ok(success) => reply.entry(&success.entry_ttl, &success.attr, 0),
            Err(err) => reply.error(err),
        }
    }

    fn opendir(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        flags: FuserOpenFlags,
        reply: FuserReplyOpen,
    ) {
        let ino = inode_id_from_fuser(ino);
        let flags = open_flags_bits(flags);
        let reply = ReplyOpenCompat(reply);
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
                reply.opened(fh, flags);
            }
            Err(err) => {
                reply.error(core_err_to_errno(&err));
            }
        }
    }

    fn readdir(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        offset: u64,
        reply: FuserReplyDirectory,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let offset = offset as i64;
        let mut reply = ReplyDirectoryCompat(reply);
        let Some(dir_entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let Some(handle) = self.handles.get_dir(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let _dir_read_guard = self
            .core
            .dir_visibility_locks
            .read_guard(dir_cache_key_from_backend(dir_entry.backend));

        let mut entries: Vec<(InodeId, FuserFileType, OsString)> = Vec::new();
        entries.push((ino, FuserFileType::Directory, OsString::from(".")));
        let parent_ino = self.parent_ino_for(&dir_entry);
        let parent_kind = self
            .inode_store
            .get(parent_ino)
            .map(|p| p.kind.into())
            .unwrap_or(FuserFileType::Directory);
        entries.push((parent_ino, parent_kind, OsString::from("..")));

        let dir_listing = match self.core.load_dir_entries_snapshot(&handle, false, offset) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut fatal_errno = None;
        for info in dir_listing.iter() {
            let mut backend_key = info.backend_key;
            let mut kind = Some(info.kind);
            let needs_stat = backend_key.map(|k| k.ino == 0).unwrap_or(true) || kind.is_none();
            if needs_stat {
                let c_name = match cstring_from_bytes(&info.backend_name) {
                    Ok(v) => v,
                    Err(err) => {
                        fatal_errno = Some(core_err_to_errno(&err));
                        break;
                    }
                };
                match fstatat(
                    handle.as_fd(),
                    c_name.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                ) {
                    Ok(stat) => {
                        kind.get_or_insert_with(|| core_file_type_from_mode(stat.st_mode));
                        backend_key.get_or_insert_with(|| backend_key_from_stat(&stat));
                    }
                    Err(err) => {
                        let err = core_errno_from_nix(err);
                        match classify_enumerated_entry_stat_error(&err) {
                            ReadSideRepairDisposition::Recoverable(_) => continue,
                            ReadSideRepairDisposition::Fatal => {
                                fatal_errno = Some(core_err_to_errno(&err));
                                break;
                            }
                        }
                    }
                }
            }
            let Some(kind) = kind else {
                fatal_errno = Some(libc::EIO);
                break;
            };
            let Some(backend_key) = backend_key else {
                fatal_errno = Some(libc::EIO);
                break;
            };
            let child_entry = self.inode_store.get_or_insert(
                backend_key,
                InodeKind::from(kind),
                ParentName {
                    parent: ino,
                    name: info.name.clone(),
                    backend_name: info.backend_name.clone(),
                },
                0,
            );
            let file_type = FuserFileType::from(child_entry.kind);
            entries.push((child_entry.ino, file_type, info.name.clone()));
        }
        if let Some(errno) = fatal_errno {
            reply.error(errno);
            return;
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
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        offset: u64,
        reply: FuserReplyDirectoryPlus,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let offset = offset as i64;
        let mut reply = ReplyDirectoryPlusCompat(reply);
        let Some(dir_entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        let Some(handle) = self.handles.get_dir(fh) else {
            reply.error(libc::EBADF);
            return;
        };
        let _dir_read_guard = self
            .core
            .dir_visibility_locks
            .read_guard(dir_cache_key_from_backend(dir_entry.backend));
        let dir_attr = match fstat(handle.as_fd()) {
            Ok(stat) => fuser_attr_from_core(core_attr_from_stat(&stat), ino),
            Err(err) => {
                reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                return;
            }
        };
        let (dot_ttl, _) = self.ttl_for_ino(ino);
        let parent_attr = self
            .inode_store
            .get(self.parent_ino_for(&dir_entry))
            .and_then(|p| self.attr_for_entry(&p).ok())
            .unwrap_or(dir_attr);
        let parent_ino = self.parent_ino_for(&dir_entry);
        let (dotdot_ttl, _) = self.ttl_for_ino(parent_ino);

        let start = offset.max(0) as usize;
        let preloaded_listing = if start <= 1 {
            self.core
                .load_dir_entries_snapshot(&handle, true, start as i64)
                .ok()
        } else {
            None
        };
        if start == 0 && reply.add(ino, 1, OsStr::new("."), &dot_ttl, &dir_attr, 0) {
            reply.ok();
            return;
        }
        if start <= 1
            && reply.add(
                parent_ino,
                2,
                OsStr::new(".."),
                &dotdot_ttl,
                &parent_attr,
                0,
            )
        {
            reply.ok();
            return;
        }

        let dir_listing = if let Some(entries) = preloaded_listing {
            entries
        } else {
            match self
                .core
                .load_dir_entries_snapshot(&handle, true, start as i64)
            {
                Ok(v) => v,
                Err(err) => {
                    reply.error(core_err_to_errno(&err));
                    return;
                }
            }
        };
        let child_start = start.saturating_sub(2);
        let mut fatal_errno = None;
        for (child_index, info) in dir_listing.iter().enumerate().skip(child_start) {
            let (attr, backend_key) = match (info.attr, info.backend_key) {
                (Some(attr), Some(backend)) => (attr, backend),
                _ => {
                    let c_name = match cstring_from_bytes(&info.backend_name) {
                        Ok(v) => v,
                        Err(err) => {
                            fatal_errno = Some(core_err_to_errno(&err));
                            break;
                        }
                    };
                    let stat = match fstatat(
                        handle.as_fd(),
                        c_name.as_c_str(),
                        AtFlags::AT_SYMLINK_NOFOLLOW,
                    ) {
                        Ok(st) => st,
                        Err(err) => {
                            let err = core_errno_from_nix(err);
                            match classify_enumerated_entry_stat_error(&err) {
                                ReadSideRepairDisposition::Recoverable(_) => continue,
                                ReadSideRepairDisposition::Fatal => {
                                    fatal_errno = Some(core_err_to_errno(&err));
                                    break;
                                }
                            }
                        }
                    };
                    (core_attr_from_stat(&stat), backend_key_from_stat(&stat))
                }
            };
            let next = (child_index + 3) as i64;
            let (_child_ino, full) = self.emit_readdirplus_child_with_lookup(
                ino,
                info,
                attr,
                backend_key,
                next,
                |child_ino, next, name, entry_ttl, attr| {
                    reply.add(child_ino, next, name, entry_ttl, attr, 0)
                },
            );
            if full {
                break;
            }
        }
        if let Some(errno) = fatal_errno {
            reply.error(errno);
            return;
        }
        reply.ok();
    }

    fn open(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        flags: FuserOpenFlags,
        reply: FuserReplyOpen,
    ) {
        let ino = inode_id_from_fuser(ino);
        let flags = open_flags_bits(flags);
        let reply = ReplyOpenCompat(reply);
        #[cfg(feature = "abi-7-40")]
        {
            match self.open_result_internal(ino, flags, |fd| {
                reply
                    .open_backing(fd.as_fd())
                    .map(|backing| PassthroughHandleBacking::Real(Arc::new(backing)))
            }) {
                Ok(success) => {
                    if let Some(backing) = success.backing.as_ref() {
                        reply.opened_passthrough(
                            success.open.fh,
                            0,
                            backing
                                .real_backing()
                                .expect(
                                    "production open must only reply with real passthrough backing",
                                )
                                .as_ref(),
                        );
                    } else {
                        reply.opened(success.open.fh, 0);
                    }
                }
                Err(err) => reply.error(err),
            }
        }

        #[cfg(not(feature = "abi-7-40"))]
        let Some(entry) = self.inode_store.get(ino) else {
            reply.error(libc::ESTALE);
            return;
        };
        #[cfg(not(feature = "abi-7-40"))]
        let fd = match self.open_backend_file(&entry, flags as u32) {
            Ok(fd) => fd,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        #[cfg(not(feature = "abi-7-40"))]
        let success = self.finish_open_file_result(ino, fd, false);
        #[cfg(not(feature = "abi-7-40"))]
        reply.opened(success.fh, 0);
    }

    fn read(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        offset: u64,
        size: u32,
        _flags: FuserOpenFlags,
        _lock_owner: Option<FuserLockOwner>,
        reply: FuserReplyData,
    ) {
        let _ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let offset = offset as i64;
        let reply = ReplyDataCompat(reply);
        #[cfg(feature = "abi-7-40")]
        if let Some(handle) = self.pin_passthrough_handle(fh) {
            eprintln!(
                "longnamefs-rs v2: WARNING: read for passthrough fh {fh} unexpectedly reached userspace; falling back to userspace IO"
            );
            if !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_read() {
                reply.error(libc::EBADF);
                return;
            }

            let mut buf = vec![0u8; size as usize];
            match retry_eintr(|| pread(handle.data_fd.as_fd(), &mut buf, offset)) {
                Ok(read_len) => {
                    buf.truncate(read_len);
                    reply.data(&buf);
                }
                Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
            }
            return;
        }
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
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: FuserWriteFlags,
        _flags: FuserOpenFlags,
        _lock_owner: Option<FuserLockOwner>,
        reply: FuserReplyWrite,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyWriteCompat(reply);
        match self.write_result_internal(ino, fh, offset, data) {
            Ok(success) => reply.written(success.size),
            Err(err) => reply.error(err),
        }
    }

    fn fallocate(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        offset: u64,
        length: u64,
        mode: i32,
        reply: FuserReplyEmpty,
    ) {
        let _ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);

        if let Err(err) = self.core.ensure_not_poisoned() {
            reply.error(core_err_to_errno(&err));
            return;
        }

        #[cfg(target_os = "linux")]
        {
            let mode = FallocateFlags::from_bits_truncate(mode);
            let offset = offset as libc::off_t;
            let length = length as libc::off_t;

            if let Some(handle) = self.handles.get_file(fh) {
                if let Err(err) = nix_fallocate(handle.as_fd(), mode, offset, length) {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
                reply.ok();
                return;
            }

            #[cfg(feature = "abi-7-40")]
            if let Some(handle) = self.pin_passthrough_handle(fh) {
                if !PassthroughBackingCacheCaps::for_open_flags(handle.open_flags).allows_write() {
                    reply.error(libc::EBADF);
                    return;
                }

                if let Err(err) = nix_fallocate(handle.data_fd.as_fd(), mode, offset, length) {
                    reply.error(core_err_to_errno(&core_errno_from_nix(err)));
                    return;
                }
                reply.ok();
                return;
            }

            reply.error(libc::EBADF);
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (ino, fh, offset, length, mode);
            reply.error(libc::EOPNOTSUPP);
        }
    }

    fn fsync(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        datasync: bool,
        reply: FuserReplyEmpty,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);
        match self.fsync_result_internal(ino, fh, datasync) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(err),
        }
    }

    fn flush(
        &self,
        _req: &FuserRequest<'_>,
        _ino: FuserInodeNo,
        fh: FuserFileHandle,
        _lock_owner: FuserLockOwner,
        reply: FuserReplyEmpty,
    ) {
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);
        if self.handles.get_file(fh).is_some() {
            reply.ok();
            return;
        }
        #[cfg(feature = "abi-7-40")]
        if self.pin_passthrough_handle(fh).is_some() {
            reply.ok();
            return;
        }
        reply.error(libc::EBADF);
    }

    fn access(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        mask: FuserAccessFlags,
        reply: FuserReplyEmpty,
    ) {
        let ino = inode_id_from_fuser(ino);
        let mask = access_flags_bits(mask) as i32;
        let reply = ReplyEmptyCompat(reply);
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
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        name: &OsStr,
        value: &[u8],
        flags: i32,
        position: u32,
        reply: FuserReplyEmpty,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEmptyCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let ino = inode_id_from_fuser(ino);
        let reply = ReplyEmptyCompat(reply);
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
        let target = match xattr_target_for_path(self.core.as_ref(), &path, true) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = xattr_set(&target, cname.as_c_str(), value, flags) {
            reply.error(core_err_to_errno(&err));
            return;
        }
        reply.ok();
    }

    fn getxattr(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        name: &OsStr,
        size: u32,
        reply: FuserReplyXattr,
    ) {
        let ino = inode_id_from_fuser(ino);
        let reply = ReplyXattrCompat(reply);
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
        let target = match xattr_target_for_path(self.core.as_ref(), &path, false) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if size == 0 {
            match xattr_get_size(&target, cname.as_c_str()) {
                Ok(v) => reply.size(v as u32),
                Err(err) => reply.error(core_err_to_errno(&err)),
            }
            return;
        }
        let mut buf = vec![0u8; size as usize];
        let read_len = match xattr_get_into(&target, cname.as_c_str(), &mut buf) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if read_len > size as usize {
            reply.error(libc::ERANGE);
            return;
        }
        buf.truncate(read_len);
        reply.data(&buf);
    }

    fn listxattr(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        size: u32,
        reply: FuserReplyXattr,
    ) {
        let ino = inode_id_from_fuser(ino);
        let reply = ReplyXattrCompat(reply);
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
        let target = match xattr_target_for_path(self.core.as_ref(), &path, false) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let initial = match xattr_list_size(&target) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        let mut buf = vec![0u8; initial];
        let list_len = match xattr_list_into(&target, &mut buf) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
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
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        name: &OsStr,
        reply: FuserReplyEmpty,
    ) {
        if let Err(err) = self.core.ensure_not_poisoned() {
            ReplyEmptyCompat(reply).error(core_err_to_errno(&err));
            return;
        }
        let ino = inode_id_from_fuser(ino);
        let reply = ReplyEmptyCompat(reply);
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
        let target = match xattr_target_for_path(self.core.as_ref(), &path, true) {
            Ok(v) => v,
            Err(err) => {
                reply.error(core_err_to_errno(&err));
                return;
            }
        };
        if let Err(err) = xattr_remove(&target, cname.as_c_str()) {
            reply.error(core_err_to_errno(&err));
            return;
        }
        reply.ok();
    }

    fn poll(
        &self,
        _req: &FuserRequest<'_>,
        _ino: FuserInodeNo,
        fh: FuserFileHandle,
        _ph: FuserPollHandle,
        _events: FuserPollEvents,
        _flags: FuserPollFlags,
        reply: FuserReplyPoll,
    ) {
        let fh = fh_from_fuser(fh);
        let reply = ReplyPollCompat(reply);
        if self.handles.get_file(fh).is_some() {
            reply.poll(0);
            return;
        }
        #[cfg(feature = "abi-7-40")]
        if self.pin_passthrough_handle(fh).is_some() {
            reply.poll(0);
            return;
        }
        reply.error(libc::EBADF);
    }

    fn release(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        _flags: FuserOpenFlags,
        _lock_owner: Option<FuserLockOwner>,
        _flush: bool,
        reply: FuserReplyEmpty,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);
        #[cfg(feature = "abi-7-40")]
        if let Some(handle) = self.remove_passthrough_handle(fh) {
            self.clear_passthrough_meta_fd(handle.as_ref());
            let _ = self.inode_store.dec_open(ino);
            if let Some(entry) = self.inode_store.get(ino)
                && entry.ino != ROOT_INODE
                && let Some(parent_entry) = self.inode_store.get(entry.parent)
                && let Some(parent_dirfd) =
                    self.core.try_dir_fd_by_backend_key(parent_entry.backend)
            {
                self.invalidate_dir(parent_dirfd.as_fd());
            } else if let Some(entry) = self.inode_store.get(ino)
                && let Ok(path) = self.entry_path(&entry)
                && path != OsStr::new("/")
                && let Ok(mapped) = self.core.resolve_path(&path)
            {
                self.invalidate_dir(mapped.dir_fd.as_fd());
            }
            self.notify_inode(ino);
            reply.ok();
            return;
        }
        self.handles.remove(fh);
        let _ = self.inode_store.dec_open(ino);
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        _flags: FuserOpenFlags,
        reply: FuserReplyEmpty,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);
        self.handles.remove(fh);
        let _ = self.inode_store.dec_open(ino);
        reply.ok();
    }

    fn fsyncdir(
        &self,
        _req: &FuserRequest<'_>,
        ino: FuserInodeNo,
        fh: FuserFileHandle,
        datasync: bool,
        reply: FuserReplyEmpty,
    ) {
        let ino = inode_id_from_fuser(ino);
        let fh = fh_from_fuser(fh);
        let reply = ReplyEmptyCompat(reply);
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
        let sync_res = sync_fd(handle.as_fd(), datasync);
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

    fn forget(&self, _req: &FuserRequest<'_>, ino: FuserInodeNo, nlookup: u64) {
        let ino = inode_id_from_fuser(ino);
        let _ = self.inode_store.dec_lookup(ino, nlookup);
    }

    fn statfs(&self, _req: &FuserRequest<'_>, _ino: FuserInodeNo, reply: FuserReplyStatfs) {
        let reply = ReplyStatfsCompat(reply);
        match fstatvfs(self.core.config.backend_fd()) {
            Ok(stats) => {
                let name_max = (self.core.max_name_len.min(u32::MAX as usize)) as u32;
                reply.statfs(
                    stats.blocks(),
                    stats.blocks_free(),
                    stats.blocks_available(),
                    stats.files(),
                    stats.files_free(),
                    stats.block_size() as u32,
                    name_max,
                    stats.fragment_size() as u32,
                )
            }
            Err(err) => reply.error(core_err_to_errno(&core_errno_from_nix(err))),
        }
    }
}

#[cfg(test)]
#[path = "../../tests/v2/mod.rs"]
mod tests;
