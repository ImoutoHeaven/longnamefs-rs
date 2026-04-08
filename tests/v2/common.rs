use super::*;
use std::fs;
use std::io::Write;
use std::os::fd::OwnedFd;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) struct TempDir(PathBuf);

impl TempDir {
    pub(super) fn new() -> Self {
        let mut path = std::env::temp_dir();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        path.push(format!("ln2_core_test_{}_{}", std::process::id(), nanos));
        fs::create_dir(&path).expect("create temp dir");
        TempDir(path)
    }

    pub(super) fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(super) fn opath_rawname_ebadf(dir_fd: BorrowedFd<'_>, name: &CStr) -> bool {
    let fd = match nix::fcntl::openat(
        dir_fd,
        name,
        OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(_) => return false,
    };
    match get_internal_rawname(fd.as_fd()) {
        Ok(_) => false,
        Err(CoreError::Io(ref ioe)) if ioe.raw_os_error() == Some(libc::EBADF) => true,
        Err(_) => false,
    }
}

pub(super) fn capture_stderr<F, T>(func: F) -> (T, Vec<u8>)
where
    F: FnOnce() -> T,
{
    unsafe {
        let mut fds = [0; 2];
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            panic!("pipe failed");
        }
        let read_fd = fds[0];
        let write_fd = fds[1];
        let saved = libc::dup(libc::STDERR_FILENO);
        if saved < 0 {
            libc::close(read_fd);
            libc::close(write_fd);
            panic!("dup stderr failed");
        }
        if libc::dup2(write_fd, libc::STDERR_FILENO) < 0 {
            libc::close(read_fd);
            libc::close(write_fd);
            libc::close(saved);
            panic!("dup2 stderr failed");
        }
        libc::close(write_fd);

        let result = func();
        let _ = std::io::stderr().flush();

        if libc::dup2(saved, libc::STDERR_FILENO) < 0 {
            libc::close(read_fd);
            libc::close(saved);
            panic!("restore stderr failed");
        }
        libc::close(saved);

        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = libc::read(read_fd, tmp.as_mut_ptr() as *mut libc::c_void, tmp.len());
            if n <= 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n as usize]);
        }
        libc::close(read_fd);
        (result, buf)
    }
}

pub(super) struct TestNotifierRecorder {
    rx: mpsc::Receiver<NotifyEventRecord>,
    seen: Mutex<Vec<NotifyEventRecord>>,
}

impl TestNotifierRecorder {
    pub(super) fn attach(fs: &LongNameFsV2Fuser) -> Self {
        Self {
            rx: fs.test_subscribe_notifier(),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn drain_into_seen(&self) {
        let mut seen = self.seen.lock().unwrap();
        if let Ok(event) = self.rx.recv_timeout(Duration::from_millis(50)) {
            seen.push(event);
        }
        while let Ok(event) = self.rx.try_recv() {
            seen.push(event);
        }
    }

    pub(super) fn recorded_parent_invalidation(&self, parent: u64, name: &OsStr) -> bool {
        self.drain_into_seen();
        self.seen.lock().unwrap().iter().any(|event| {
            matches!(
                event,
                &NotifyEventRecord::ParentInvalidation { parent: p, name: ref n }
                    if p == parent && n == name
            )
        })
    }

    pub(super) fn recorded_inode_invalidation(&self, ino: u64) -> bool {
        self.drain_into_seen();
        self.seen.lock().unwrap().iter().any(|event| {
            matches!(event, &NotifyEventRecord::InodeInvalidation { ino: seen } if seen == ino)
        })
    }

    pub(super) fn recorded_delete(&self, parent: u64, child: u64, name: &OsStr) -> bool {
        self.drain_into_seen();
        self.seen.lock().unwrap().iter().any(|event| {
            matches!(
                event,
                &NotifyEventRecord::Delete { parent: p, child: c, name: ref n }
                    if p == parent && c == child && n == name
            )
        })
    }
}

static TEST_HOOK_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(super) fn lock_test_hooks() -> MutexGuard<'static, ()> {
    TEST_HOOK_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) struct TestErrnoHookGuard {
    reset: fn(Option<i32>),
}

impl Drop for TestErrnoHookGuard {
    fn drop(&mut self) {
        (self.reset)(None);
    }
}

fn install_test_errno_hook(reset: fn(Option<i32>), errno: i32) -> TestErrnoHookGuard {
    reset(Some(errno));
    TestErrnoHookGuard { reset }
}

pub(super) fn force_post_commit_flush_errno(errno: i32) -> TestErrnoHookGuard {
    install_test_errno_hook(set_test_force_post_commit_flush_errno, errno)
}

pub(super) fn force_fsync_errno(errno: i32) -> TestErrnoHookGuard {
    install_test_errno_hook(set_test_force_fsync_errno, errno)
}

pub(super) fn force_fdatasync_errno(errno: i32) -> TestErrnoHookGuard {
    install_test_errno_hook(set_test_force_fdatasync_errno, errno)
}

pub(super) fn force_internal_rawname_errno(errno: i32) -> TestErrnoHookGuard {
    install_test_errno_hook(set_test_force_internal_rawname_errno, errno)
}

pub(super) fn force_list_iter_skip_errno(errno: i32) -> TestErrnoHookGuard {
    install_test_errno_hook(set_test_force_list_iter_skip_errno, errno)
}

pub(super) struct TestPostCommitFlushPauseGuard {
    ready_rx: mpsc::Receiver<()>,
    release_tx: Option<mpsc::Sender<()>>,
}

impl TestPostCommitFlushPauseGuard {
    pub(super) fn wait_until_blocked(&self) {
        self.ready_rx
            .recv()
            .expect("flush pause hook should report when the flush is in flight");
    }

    pub(super) fn release(&mut self) {
        if let Some(tx) = self.release_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for TestPostCommitFlushPauseGuard {
    fn drop(&mut self) {
        super::clear_test_pause_post_commit_flush();
        self.release();
    }
}

pub(super) fn pause_next_post_commit_flush() -> TestPostCommitFlushPauseGuard {
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    super::install_test_pause_post_commit_flush(ready_tx, release_rx);
    TestPostCommitFlushPauseGuard {
        ready_rx,
        release_tx: Some(release_tx),
    }
}

pub(super) struct TestPassthroughReleaseAfterCheckGuard;

impl Drop for TestPassthroughReleaseAfterCheckGuard {
    fn drop(&mut self) {
        set_test_force_passthrough_release_after_check(None);
    }
}

pub(super) fn force_passthrough_release_after_check(
    fh: u64,
) -> TestPassthroughReleaseAfterCheckGuard {
    set_test_force_passthrough_release_after_check(Some(fh));
    TestPassthroughReleaseAfterCheckGuard
}

pub(super) fn set_test_force_post_commit_flush_errno(errno: Option<i32>) {
    super::set_test_force_post_commit_flush_errno(errno);
}

pub(super) fn set_test_force_fsync_errno(errno: Option<i32>) {
    super::set_test_force_fsync_errno(errno);
}

pub(super) fn set_test_force_fdatasync_errno(errno: Option<i32>) {
    super::set_test_force_fdatasync_errno(errno);
}

pub(super) fn set_test_force_internal_rawname_errno(errno: Option<i32>) {
    super::set_test_force_internal_rawname_errno(errno);
}

pub(super) fn set_test_force_list_iter_skip_errno(errno: Option<i32>) {
    super::set_test_force_list_iter_skip_errno(errno);
}

pub(super) fn set_test_force_passthrough_release_after_check(fh: Option<u64>) {
    super::set_test_force_passthrough_release_after_check(fh);
}

pub(super) fn test_fsync_call_count() -> usize {
    super::test_fsync_call_count()
}

pub(super) fn test_fdatasync_call_count() -> usize {
    super::test_fdatasync_call_count()
}

pub(super) fn create_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    mode: u32,
    flags: i32,
) -> Result<TestCreateSuccess, i32> {
    fs.test_create(parent, name, mode, flags)
}

#[cfg(feature = "abi-7-40")]
pub(super) fn create_result_with_open_backing_errno(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    mode: u32,
    flags: i32,
    errno: i32,
) -> Result<TestCreateSuccess, i32> {
    fs.test_create_with_open_backing_errno(parent, name, mode, flags, errno)
}

pub(super) fn mkdir_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    mode: u32,
) -> Result<TestEntrySuccess, i32> {
    fs.test_mkdir(parent, name, mode)
}

pub(super) fn symlink_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    target: &std::path::Path,
) -> Result<TestEntrySuccess, i32> {
    fs.test_symlink(parent, name, target)
}

pub(super) fn mknod_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    mode: u32,
    rdev: u32,
) -> Result<TestEntrySuccess, i32> {
    fs.test_mknod(parent, name, mode, rdev)
}

pub(super) fn unlink_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
) -> Result<TestEmptySuccess, i32> {
    fs.test_unlink(parent, name)
}

pub(super) fn rmdir_result(fs: &LongNameFsV2Fuser, parent: u64, name: &OsStr) -> Result<(), i32> {
    fs.test_rmdir(parent, name)
}

pub(super) fn rename_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
    newparent: u64,
    newname: &OsStr,
    flags: u32,
) -> Result<TestRenameSuccess, i32> {
    fs.test_rename(parent, name, newparent, newname, flags)
}

pub(super) fn fsync_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: u64,
    datasync: bool,
) -> Result<(), i32> {
    fs.test_fsync(ino, fh, datasync)
}

pub(super) fn open_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    flags: u32,
) -> Result<TestOpenSuccess, i32> {
    fs.test_open(ino, flags)
}

pub(super) fn release_result(fs: &LongNameFsV2Fuser, ino: u64, fh: u64) -> Result<(), i32> {
    fs.test_release(ino, fh)
}

pub(super) fn read_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: u64,
    offset: u64,
    size: u32,
) -> Result<TestDataSuccess, i32> {
    fs.test_read(ino, fh, offset, size)
}

pub(super) fn write_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: u64,
    offset: u64,
    data: &[u8],
) -> Result<TestWriteSuccess, i32> {
    fs.test_write(ino, fh, offset, data)
}

pub(super) fn getattr_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: Option<u64>,
) -> Result<FuserFileAttr, i32> {
    fs.test_getattr(ino, fh)
}

pub(super) fn setattr_size_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: Option<u64>,
    size: u64,
) -> Result<FuserFileAttr, i32> {
    fs.test_setattr_size(ino, fh, size)
}

pub(super) fn fallocate_result(
    fs: &LongNameFsV2Fuser,
    ino: u64,
    fh: u64,
    offset: u64,
    length: u64,
    mode: i32,
) -> Result<(), i32> {
    fs.test_fallocate(ino, fh, offset, length, mode)
}

pub(super) fn readdir_names_result(fs: &LongNameFsV2Fuser, ino: u64) -> Result<Vec<OsString>, i32> {
    fs.test_readdir_names(ino, 0)
}

pub(super) fn state_snapshot_result(
    fs: &LongNameFsV2Fuser,
    path: &OsStr,
) -> Result<TestStateSnapshot, i32> {
    fs.test_state_snapshot_for_path(path)
        .map_err(|err| core_err_to_errno(&err))
}

pub(super) fn take_repair_anomalies_result(fs: &LongNameFsV2Fuser) -> Vec<TestRepairAnomalyRecord> {
    fs.test_take_repair_anomalies()
}

pub(super) fn attr_cache_contains_result(
    fs: &LongNameFsV2Fuser,
    path: &OsStr,
    backend_name: &[u8],
) -> Result<bool, i32> {
    fs.test_attr_cache_contains_entry(path, backend_name)
        .map_err(|err| core_err_to_errno(&err))
}

pub(super) fn op_state_is_dirty(state: &TestDirStateView) -> bool {
    state.dirty
}

pub(super) fn op_state_pending(state: &TestDirStateView) -> usize {
    state.pending
}

pub(super) fn op_state_has_attr_cache_backend(
    state: &TestDirStateView,
    backend_name: &[u8],
) -> bool {
    state.attr_cache_keys.contains(backend_name)
}

pub(super) fn dir_cache_contains_result(
    fs: &LongNameFsV2Fuser,
    path: &OsStr,
    child: &OsStr,
) -> Result<bool, i32> {
    fs.test_dir_cache_contains_logical_child(path, child)
        .map_err(|err| core_err_to_errno(&err))
}

pub(super) fn lookup_entry_result(
    fs: &LongNameFsV2Fuser,
    parent: u64,
    name: &OsStr,
) -> Result<InodeEntry, i32> {
    fs.test_lookup_entry(parent, name)
        .map_err(|err| core_err_to_errno(&err))
}

#[cfg(feature = "abi-7-40")]
pub(super) fn passthrough_handle_result(
    fs: &LongNameFsV2Fuser,
    fh: u64,
) -> Option<Arc<PassthroughHandleInner>> {
    fs.test_get_passthrough_handle(fh)
}

pub(super) fn insert_file_handle(fs: &LongNameFsV2Fuser, fd: OwnedFd) -> u64 {
    fs.test_handle_fh(fd)
}

pub(super) fn set_passthrough_runtime(fs: &LongNameFsV2Fuser, enabled: bool) {
    fs.test_set_passthrough_runtime(enabled);
}

pub(super) fn passthrough_runtime_enabled(fs: &LongNameFsV2Fuser) -> bool {
    fs.test_passthrough_runtime_enabled()
}

pub(super) fn new_test_fs(
    backend: &TempDir,
    sync_data: bool,
    enable_passthrough: bool,
) -> LongNameFsV2Fuser {
    let config = Config::open_backend(backend.path().clone(), sync_data, false).unwrap();
    LongNameFsV2Fuser::new(
        config,
        crate::v2::path::MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        enable_passthrough,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap()
}

pub(super) fn new_test_core(backend: &TempDir, sync_data: bool) -> LongNameFsCore {
    let config = Config::open_backend(backend.path().clone(), sync_data, false).unwrap();
    LongNameFsCore::new(
        config,
        crate::v2::path::MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(60)),
        IndexSync::Off,
    )
    .unwrap()
}

pub(super) fn new_longname_test_fs(
    backend: &TempDir,
    sync_data: bool,
    enable_passthrough: bool,
) -> LongNameFsV2Fuser {
    let config = Config::open_backend(backend.path().clone(), sync_data, false).unwrap();
    LongNameFsV2Fuser::new(
        config,
        1024,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        enable_passthrough,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap()
}

pub(super) fn new_longname_test_core(backend: &TempDir, sync_data: bool) -> LongNameFsCore {
    let config = Config::open_backend(backend.path().clone(), sync_data, false).unwrap();
    LongNameFsCore::new(config, 1024, Some(Duration::from_secs(60)), IndexSync::Off).unwrap()
}
