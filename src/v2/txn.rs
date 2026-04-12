use crate::util::{core_begin_temp_file, core_fsync_dir, retry_eintr};
use crate::v2::error::{CoreError, CoreResult, core_err_to_errno};
use crate::v2::object_id::parse_long_object_id;
use crate::v2::path::{
    MAX_SEGMENT_ON_DISK, SegmentKind, classify_committed_segment, classify_segment,
    is_reserved_prefix,
};
use nix::fcntl::{AtFlags, OFlag, openat, renameat};
use nix::sys::stat::{Mode, fstatat};
use nix::unistd::{UnlinkatFlags, fdatasync, fsync, unlinkat, write};
#[cfg(test)]
use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::Read;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;
#[cfg(test)]
use std::sync::OnceLock;
#[cfg(test)]
use std::sync::atomic::{AtomicI32, Ordering};
#[cfg(test)]
use std::sync::mpsc;

use crate::util::procfs_path_for;
#[cfg(test)]
use nix::sys::stat::fstat;
#[cfg(test)]
use parking_lot::Mutex;

pub const TXN_FILE_NAME: &str = ".ln2_fs_txn";

const TXN_MAGIC: &[u8; 4] = b"LN2T";
const TXN_VERSION: u32 = 1;

#[cfg(test)]
struct TestTxnPauseBeforeClear {
    target_dev: u64,
    target_ino: u64,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
}

#[cfg(test)]
static TEST_PAUSE_NEXT_TXN_BEFORE_CLEAR: OnceLock<Mutex<Option<TestTxnPauseBeforeClear>>> =
    OnceLock::new();

#[cfg(test)]
static TEST_FORCE_TXN_WRITE_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_TXN_CLEAR_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_FORCE_TXN_RECOVERY_ERRNO: AtomicI32 = AtomicI32::new(0);
#[cfg(test)]
static TEST_ROLLBACK_INFLIGHT_TXN_CALLS: AtomicI32 = AtomicI32::new(0);

#[cfg(test)]
thread_local! {
    static TEST_FORCE_TXN_WRITE_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_TXN_CLEAR_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
    static TEST_FORCE_TXN_RECOVERY_ERRNO_LOCAL: Cell<i32> = const { Cell::new(0) };
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
pub(crate) fn set_test_force_txn_write_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_TXN_WRITE_ERRNO,
        &TEST_FORCE_TXN_WRITE_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
pub(crate) fn set_test_force_txn_clear_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_TXN_CLEAR_ERRNO,
        &TEST_FORCE_TXN_CLEAR_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
pub(crate) fn set_test_force_txn_recovery_errno(errno: Option<i32>) {
    test_atomic_errno_store(
        &TEST_FORCE_TXN_RECOVERY_ERRNO,
        &TEST_FORCE_TXN_RECOVERY_ERRNO_LOCAL,
        errno,
    );
}

#[cfg(test)]
pub(crate) fn reset_test_rollback_inflight_txn_calls() {
    TEST_ROLLBACK_INFLIGHT_TXN_CALLS.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn test_rollback_inflight_txn_calls() -> i32 {
    TEST_ROLLBACK_INFLIGHT_TXN_CALLS.load(Ordering::Relaxed)
}

#[cfg(test)]
fn test_pause_next_txn_before_clear_slot() -> &'static Mutex<Option<TestTxnPauseBeforeClear>> {
    TEST_PAUSE_NEXT_TXN_BEFORE_CLEAR.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
pub(crate) fn install_test_pause_next_txn_before_clear(
    root: BorrowedFd<'_>,
    ready_tx: mpsc::Sender<()>,
    release_rx: mpsc::Receiver<()>,
) -> CoreResult<()> {
    let stat = fstat(root).map_err(CoreError::from)?;
    *test_pause_next_txn_before_clear_slot().lock() = Some(TestTxnPauseBeforeClear {
        target_dev: stat.st_dev as u64,
        target_ino: stat.st_ino as u64,
        ready_tx,
        release_rx,
    });
    Ok(())
}

#[cfg(test)]
pub(crate) fn clear_test_pause_next_txn_before_clear() {
    test_pause_next_txn_before_clear_slot().lock().take();
}

#[cfg(test)]
fn maybe_pause_before_clear_txn_record(root: BorrowedFd<'_>) {
    let root_stat = match fstat(root) {
        Ok(stat) => stat,
        Err(_) => return,
    };

    let mut slot = test_pause_next_txn_before_clear_slot().lock();
    let should_pause = slot.as_ref().is_some_and(|pause| {
        pause.target_dev == root_stat.st_dev as u64 && pause.target_ino == root_stat.st_ino as u64
    });
    if should_pause && let Some(pause) = slot.take() {
        drop(slot);
        let _ = pause.ready_tx.send(());
        let _ = pause.release_rx.recv();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnKind {
    CreateLong,
    CreateShort,
    LinkShort,
    RenameShortToShort,
    RenameShortToLong,
    RenameLongToShort,
    RenameLongToLongSameDir,
    RenameLongToLongCrossDir,
    UnlinkLong,
    UnlinkShort,
    RemoveDir,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RollbackMode {
    StartupRecovery,
    LiveFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnRecord {
    pub version: u32,
    pub kind: TxnKind,
    pub object_id: Option<u64>,
    pub backend_name: Option<Vec<u8>>,
    pub old_parent_segments: Vec<Vec<u8>>,
    pub new_parent_segments: Vec<Vec<u8>>,
    pub old_rawname: Option<Vec<u8>>,
    pub new_rawname: Option<Vec<u8>>,
    pub temp_backend_name: Option<Vec<u8>>,
    pub object_kind: Option<libc::mode_t>,
}

impl TxnRecord {
    pub fn create_long(
        object_id: u64,
        final_backend_name: Vec<u8>,
        parent_segments: Vec<Vec<u8>>,
        rawname: Vec<u8>,
        temp_backend_name: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::CreateLong,
            object_id: Some(object_id),
            backend_name: Some(final_backend_name),
            old_parent_segments: Vec::new(),
            new_parent_segments: parent_segments,
            old_rawname: None,
            new_rawname: Some(rawname),
            temp_backend_name: Some(temp_backend_name),
            object_kind: Some(object_kind),
        }
    }

    pub fn create_short(
        parent_segments: Vec<Vec<u8>>,
        backend_name: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::CreateShort,
            object_id: None,
            backend_name: Some(backend_name),
            old_parent_segments: Vec::new(),
            new_parent_segments: parent_segments,
            old_rawname: None,
            new_rawname: None,
            temp_backend_name: None,
            object_kind: Some(object_kind),
        }
    }

    pub fn link_short(
        old_parent_segments: Vec<Vec<u8>>,
        new_parent_segments: Vec<Vec<u8>>,
        old_backend_name: Vec<u8>,
        new_backend_name: Vec<u8>,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::LinkShort,
            object_id: None,
            backend_name: Some(old_backend_name),
            old_parent_segments,
            new_parent_segments,
            old_rawname: None,
            new_rawname: None,
            temp_backend_name: Some(new_backend_name),
            object_kind: None,
        }
    }

    pub fn rename_short_to_short(
        old_parent_segments: Vec<Vec<u8>>,
        new_parent_segments: Vec<Vec<u8>>,
        old_backend_name: Vec<u8>,
        new_backend_name: Vec<u8>,
        displaced_backend_name: Option<Vec<u8>>,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RenameShortToShort,
            object_id: None,
            backend_name: Some(old_backend_name),
            old_parent_segments,
            new_parent_segments,
            old_rawname: None,
            new_rawname: Some(new_backend_name),
            temp_backend_name: displaced_backend_name,
            object_kind: None,
        }
    }

    pub fn rename_short_to_long(
        object_id: u64,
        old_parent_segments: Vec<Vec<u8>>,
        new_parent_segments: Vec<Vec<u8>>,
        old_backend_name: Vec<u8>,
        new_backend_name: Vec<u8>,
        new_rawname: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RenameShortToLong,
            object_id: Some(object_id),
            backend_name: Some(new_backend_name),
            old_parent_segments,
            new_parent_segments,
            old_rawname: None,
            new_rawname: Some(new_rawname),
            temp_backend_name: Some(old_backend_name),
            object_kind: Some(object_kind),
        }
    }

    pub fn rename_long_to_short(
        old_parent_segments: Vec<Vec<u8>>,
        new_parent_segments: Vec<Vec<u8>>,
        stable_backend_name: Vec<u8>,
        old_rawname: Vec<u8>,
        new_backend_name: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RenameLongToShort,
            object_id: parse_long_object_id(&stable_backend_name).ok(),
            backend_name: Some(stable_backend_name),
            old_parent_segments,
            new_parent_segments,
            old_rawname: Some(old_rawname),
            new_rawname: None,
            temp_backend_name: Some(new_backend_name),
            object_kind: Some(object_kind),
        }
    }

    pub fn rename_long_to_long_same_dir(
        parent_segments: Vec<Vec<u8>>,
        stable_backend_name: Vec<u8>,
        old_rawname: Vec<u8>,
        new_rawname: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RenameLongToLongSameDir,
            object_id: parse_long_object_id(&stable_backend_name).ok(),
            backend_name: Some(stable_backend_name),
            old_parent_segments: parent_segments.clone(),
            new_parent_segments: parent_segments,
            old_rawname: Some(old_rawname),
            new_rawname: Some(new_rawname),
            temp_backend_name: None,
            object_kind: Some(object_kind),
        }
    }

    pub fn rename_long_to_long_cross_dir(
        old_parent_segments: Vec<Vec<u8>>,
        new_parent_segments: Vec<Vec<u8>>,
        stable_backend_name: Vec<u8>,
        old_rawname: Vec<u8>,
        new_rawname: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RenameLongToLongCrossDir,
            object_id: parse_long_object_id(&stable_backend_name).ok(),
            backend_name: Some(stable_backend_name),
            old_parent_segments,
            new_parent_segments,
            old_rawname: Some(old_rawname),
            new_rawname: Some(new_rawname),
            temp_backend_name: None,
            object_kind: Some(object_kind),
        }
    }

    pub fn unlink_long(
        parent_segments: Vec<Vec<u8>>,
        stable_backend_name: Vec<u8>,
        quarantine_backend_name: Vec<u8>,
        old_rawname: Vec<u8>,
        object_kind: libc::mode_t,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::UnlinkLong,
            object_id: parse_long_object_id(&stable_backend_name).ok(),
            backend_name: Some(stable_backend_name),
            old_parent_segments: parent_segments.clone(),
            new_parent_segments: parent_segments,
            old_rawname: Some(old_rawname),
            new_rawname: None,
            temp_backend_name: Some(quarantine_backend_name),
            object_kind: Some(object_kind),
        }
    }

    pub fn unlink_short(
        parent_segments: Vec<Vec<u8>>,
        backend_name: Vec<u8>,
        quarantine_backend_name: Vec<u8>,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::UnlinkShort,
            object_id: None,
            backend_name: Some(backend_name),
            old_parent_segments: parent_segments.clone(),
            new_parent_segments: parent_segments,
            old_rawname: None,
            new_rawname: None,
            temp_backend_name: Some(quarantine_backend_name),
            object_kind: None,
        }
    }

    pub fn remove_dir(
        parent_segments: Vec<Vec<u8>>,
        old_backend_name: Vec<u8>,
        quarantine_backend_name: Vec<u8>,
    ) -> Self {
        Self {
            version: TXN_VERSION,
            kind: TxnKind::RemoveDir,
            object_id: None,
            backend_name: Some(old_backend_name),
            old_parent_segments: parent_segments.clone(),
            new_parent_segments: parent_segments,
            old_rawname: None,
            new_rawname: None,
            temp_backend_name: Some(quarantine_backend_name),
            object_kind: None,
        }
    }
}

pub fn write_txn_record(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_TXN_WRITE_ERRNO,
        &TEST_FORCE_TXN_WRITE_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    let final_name = c".ln2_fs_txn";
    let tmp = core_begin_temp_file(root, final_name, "txn").map_err(CoreError::from)?;
    let encoded = encode_record(record)?;
    write_all(tmp.fd.as_fd(), &encoded)?;
    retry_eintr(|| fdatasync(tmp.fd.as_fd())).map_err(CoreError::from)?;
    renameat(root, tmp.name.as_c_str(), root, final_name).map_err(CoreError::from)?;
    core_fsync_dir(root).map_err(CoreError::from)
}

pub fn read_txn_record(root: BorrowedFd<'_>) -> CoreResult<Option<TxnRecord>> {
    let fd = match openat(
        root,
        c".ln2_fs_txn",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => fd,
        Err(nix::errno::Errno::ENOENT) => return Ok(None),
        Err(err) => return Err(CoreError::from(err)),
    };

    let mut file = File::from(fd);
    let mut buf = Vec::new();
    file.read_to_end(&mut buf).map_err(CoreError::from)?;
    let record = decode_record(&buf)?;
    validate_txn_record(&record)?;
    Ok(Some(record))
}

pub fn clear_txn_record(root: BorrowedFd<'_>) -> CoreResult<()> {
    #[cfg(test)]
    maybe_pause_before_clear_txn_record(root);

    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_TXN_CLEAR_ERRNO,
        &TEST_FORCE_TXN_CLEAR_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    match unlinkat(root, c".ln2_fs_txn", UnlinkatFlags::NoRemoveDir) {
        Ok(()) => core_fsync_dir(root).map_err(CoreError::from),
        Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(err) => Err(CoreError::from(err)),
    }
}

pub fn rollback_inflight_txn(
    root: BorrowedFd<'_>,
    record: &TxnRecord,
    mode: RollbackMode,
) -> CoreResult<()> {
    #[cfg(test)]
    TEST_ROLLBACK_INFLIGHT_TXN_CALLS.fetch_add(1, Ordering::Relaxed);

    #[cfg(test)]
    if let Some(errno) = test_atomic_errno_load(
        &TEST_FORCE_TXN_RECOVERY_ERRNO,
        &TEST_FORCE_TXN_RECOVERY_ERRNO_LOCAL,
    ) {
        return Err(CoreError::from_errno(errno));
    }

    match record.kind {
        TxnKind::CreateLong => rollback_create_long(root, record),
        TxnKind::CreateShort => rollback_create_short(root, record),
        TxnKind::LinkShort => rollback_link_short(root, record),
        TxnKind::RenameShortToShort => rollback_rename_short_to_short(root, record),
        TxnKind::RenameShortToLong => rollback_rename_short_to_long(root, record, mode),
        TxnKind::RenameLongToShort => rollback_rename_long_to_short(root, record),
        TxnKind::RenameLongToLongSameDir => rollback_rename_long_to_long_same_dir(root, record),
        TxnKind::RenameLongToLongCrossDir => rollback_rename_long_to_long_cross_dir(root, record),
        TxnKind::UnlinkLong => rollback_unlink_long(root, record),
        TxnKind::UnlinkShort => rollback_unlink_short(root, record),
        TxnKind::RemoveDir => rollback_remove_dir(root, record),
    }
}

fn rollback_create_long(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let final_name = record_backend_name(record)?;
    let temp_name = record_temp_backend_name(record)?;
    let flags = removal_flags_for_kind(record_object_kind(record)?);
    let final_name = cstring_from_record_bytes(final_name)?;
    let temp_name = cstring_from_record_bytes(temp_name)?;

    let mut changed = false;
    changed |= remove_entry_if_present(parent_dir.as_fd(), final_name.as_c_str(), flags)?;
    changed |= remove_entry_if_present(parent_dir.as_fd(), temp_name.as_c_str(), flags)?;
    if changed {
        core_fsync_dir(parent_dir.as_fd()).map_err(CoreError::from)?;
    }
    Ok(())
}

fn rollback_create_short(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let flags = removal_flags_for_kind(record_object_kind(record)?);
    if remove_entry_if_present(parent_dir.as_fd(), name.as_c_str(), flags)? {
        core_fsync_dir(parent_dir.as_fd()).map_err(CoreError::from)?;
    }
    Ok(())
}

fn rollback_link_short(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let old_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let new_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let old_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let new_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;

    let removed = remove_entry_if_present(
        new_dir.as_fd(),
        new_name.as_c_str(),
        UnlinkatFlags::NoRemoveDir,
    )?;
    if removed {
        sync_parent_dirs(
            old_dir.as_fd(),
            &record.old_parent_segments,
            new_dir.as_fd(),
            &record.new_parent_segments,
        )?;
        sync_entry(old_dir.as_fd(), old_name.as_c_str())?;
    }
    Ok(())
}

fn rollback_rename_short_to_short(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let old_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let new_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let old_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let new_name =
        cstring_from_record_bytes(record.new_rawname.as_deref().ok_or(CoreError::BadFormat)?)?;
    let displaced_name = match record.temp_backend_name.as_deref() {
        Some(bytes) => Some(cstring_from_record_bytes(bytes)?),
        None => None,
    };

    let old_exists = entry_exists(old_dir.as_fd(), old_name.as_c_str())?;
    let new_exists = entry_exists(new_dir.as_fd(), new_name.as_c_str())?;
    let displaced_exists = match displaced_name.as_ref() {
        Some(name) => entry_exists(new_dir.as_fd(), name.as_c_str())?,
        None => false,
    };

    let mut restored = false;
    if !old_exists && new_exists {
        renameat(
            new_dir.as_fd(),
            new_name.as_c_str(),
            old_dir.as_fd(),
            old_name.as_c_str(),
        )
        .map_err(CoreError::from)?;
        restored = true;
    } else if !old_exists && !new_exists && !displaced_exists {
        return Err(recovery_poisoned());
    }

    if let Some(displaced_name) = displaced_name.as_ref() {
        let old_exists = entry_exists(old_dir.as_fd(), old_name.as_c_str())?;
        let new_exists = entry_exists(new_dir.as_fd(), new_name.as_c_str())?;
        let displaced_exists = entry_exists(new_dir.as_fd(), displaced_name.as_c_str())?;
        if displaced_exists && !new_exists {
            renameat(
                new_dir.as_fd(),
                displaced_name.as_c_str(),
                new_dir.as_fd(),
                new_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            restored = true;
        } else if !old_exists && !new_exists && !displaced_exists {
            return Err(recovery_poisoned());
        }
    }

    if restored {
        sync_parent_dirs(
            old_dir.as_fd(),
            &record.old_parent_segments,
            new_dir.as_fd(),
            &record.new_parent_segments,
        )?;
    }
    Ok(())
}

fn rollback_rename_short_to_long(
    root: BorrowedFd<'_>,
    record: &TxnRecord,
    mode: RollbackMode,
) -> CoreResult<()> {
    let old_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let new_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let old_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;
    let new_name = cstring_from_record_bytes(record_backend_name(record)?)?;

    let new_exists = entry_exists(new_dir.as_fd(), new_name.as_c_str())?;
    let old_exists = entry_exists(old_dir.as_fd(), old_name.as_c_str())?;

    match (new_exists, old_exists) {
        (true, false) => {
            renameat(
                new_dir.as_fd(),
                new_name.as_c_str(),
                old_dir.as_fd(),
                old_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            sync_parent_dirs(
                old_dir.as_fd(),
                &record.old_parent_segments,
                new_dir.as_fd(),
                &record.new_parent_segments,
            )?;
        }
        (false, true) => {}
        (true, true) if mode == RollbackMode::LiveFailure => {}
        (true, true) => return Err(recovery_poisoned()),
        (false, false) => return Err(recovery_poisoned()),
    }

    if clear_rawname_if_present(old_dir.as_fd(), old_name.as_c_str())? {
        sync_entry(old_dir.as_fd(), old_name.as_c_str())?;
    }
    Ok(())
}

fn rollback_rename_long_to_short(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let old_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let new_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let stable_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let short_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;

    let stable_exists = entry_exists(old_dir.as_fd(), stable_name.as_c_str())?;
    let short_exists = entry_exists(new_dir.as_fd(), short_name.as_c_str())?;

    match (stable_exists, short_exists) {
        (false, true) => {
            renameat(
                new_dir.as_fd(),
                short_name.as_c_str(),
                old_dir.as_fd(),
                stable_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            sync_parent_dirs(
                old_dir.as_fd(),
                &record.old_parent_segments,
                new_dir.as_fd(),
                &record.new_parent_segments,
            )?;
        }
        (true, false) => {}
        (false, false) | (true, true) => return Err(recovery_poisoned()),
    }

    set_rawname_at(
        old_dir.as_fd(),
        stable_name.as_c_str(),
        record_old_rawname(record)?,
    )?;
    sync_entry(old_dir.as_fd(), stable_name.as_c_str())?;
    Ok(())
}

fn rollback_rename_long_to_long_same_dir(
    root: BorrowedFd<'_>,
    record: &TxnRecord,
) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let stable_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    if !entry_exists(parent_dir.as_fd(), stable_name.as_c_str())? {
        return Err(recovery_poisoned());
    }
    set_rawname_at(
        parent_dir.as_fd(),
        stable_name.as_c_str(),
        record_old_rawname(record)?,
    )?;
    sync_entry(parent_dir.as_fd(), stable_name.as_c_str())?;
    Ok(())
}

fn rollback_rename_long_to_long_cross_dir(
    root: BorrowedFd<'_>,
    record: &TxnRecord,
) -> CoreResult<()> {
    let old_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let new_dir = open_relative_dir(root, &record.new_parent_segments)?;
    let stable_name = cstring_from_record_bytes(record_backend_name(record)?)?;

    let stable_in_old = entry_exists(old_dir.as_fd(), stable_name.as_c_str())?;
    let stable_in_new = entry_exists(new_dir.as_fd(), stable_name.as_c_str())?;

    match (stable_in_old, stable_in_new) {
        (false, true) => {
            renameat(
                new_dir.as_fd(),
                stable_name.as_c_str(),
                old_dir.as_fd(),
                stable_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            sync_parent_dirs(
                old_dir.as_fd(),
                &record.old_parent_segments,
                new_dir.as_fd(),
                &record.new_parent_segments,
            )?;
        }
        (true, false) => {}
        (false, false) | (true, true) => return Err(recovery_poisoned()),
    }

    set_rawname_at(
        old_dir.as_fd(),
        stable_name.as_c_str(),
        record_old_rawname(record)?,
    )?;
    sync_entry(old_dir.as_fd(), stable_name.as_c_str())?;
    Ok(())
}

fn rollback_unlink_long(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let stable_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let quarantine_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;

    let stable_exists = entry_exists(parent_dir.as_fd(), stable_name.as_c_str())?;
    let quarantine_exists = entry_exists(parent_dir.as_fd(), quarantine_name.as_c_str())?;

    match (stable_exists, quarantine_exists) {
        (false, true) => {
            renameat(
                parent_dir.as_fd(),
                quarantine_name.as_c_str(),
                parent_dir.as_fd(),
                stable_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            core_fsync_dir(parent_dir.as_fd()).map_err(CoreError::from)?;
        }
        (true, false) => {}
        (false, false) | (true, true) => return Err(recovery_poisoned()),
    }

    Ok(())
}

fn rollback_unlink_short(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let old_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let quarantine_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;

    let old_exists = entry_exists(parent_dir.as_fd(), old_name.as_c_str())?;
    let quarantine_exists = entry_exists(parent_dir.as_fd(), quarantine_name.as_c_str())?;

    match (old_exists, quarantine_exists) {
        (false, true) => {
            renameat(
                parent_dir.as_fd(),
                quarantine_name.as_c_str(),
                parent_dir.as_fd(),
                old_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            core_fsync_dir(parent_dir.as_fd()).map_err(CoreError::from)?;
        }
        (true, false) => {}
        (false, false) => return Err(recovery_poisoned()),
        (true, true) => return Err(recovery_poisoned()),
    }

    Ok(())
}

fn rollback_remove_dir(root: BorrowedFd<'_>, record: &TxnRecord) -> CoreResult<()> {
    let parent_dir = open_relative_dir(root, &record.old_parent_segments)?;
    let old_name = cstring_from_record_bytes(record_backend_name(record)?)?;
    let quarantine_name = cstring_from_record_bytes(record_temp_backend_name(record)?)?;

    let old_exists = entry_exists(parent_dir.as_fd(), old_name.as_c_str())?;
    let quarantine_exists = entry_exists(parent_dir.as_fd(), quarantine_name.as_c_str())?;

    match (old_exists, quarantine_exists) {
        (false, true) => {
            renameat(
                parent_dir.as_fd(),
                quarantine_name.as_c_str(),
                parent_dir.as_fd(),
                old_name.as_c_str(),
            )
            .map_err(CoreError::from)?;
            core_fsync_dir(parent_dir.as_fd()).map_err(CoreError::from)?;
        }
        (true, false) => {}
        (false, false) | (true, true) => return Err(recovery_poisoned()),
    }

    Ok(())
}

fn open_relative_dir(root: BorrowedFd<'_>, segments: &[Vec<u8>]) -> CoreResult<OwnedFd> {
    let mut current = openat(
        root,
        c".",
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(CoreError::from)?;

    for segment in segments {
        validate_backend_path_segment(segment)?;
        let name = CString::new(segment.as_slice()).map_err(|_| CoreError::BadFormat)?;
        current = openat(
            current.as_fd(),
            name.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(map_open_relative_dir_error)?;
    }

    Ok(current)
}

fn validate_relative_segment(segment: &[u8]) -> CoreResult<()> {
    if segment.is_empty() || segment == b"." || segment == b".." || segment.contains(&b'/') {
        return Err(CoreError::BadFormat);
    }
    Ok(())
}

fn map_open_relative_dir_error(err: nix::errno::Errno) -> CoreError {
    match err {
        nix::errno::Errno::ENOENT | nix::errno::Errno::ENOTDIR => recovery_poisoned(),
        other => CoreError::from(other),
    }
}

fn recovery_poisoned() -> CoreError {
    CoreError::Poisoned
}

fn record_backend_name(record: &TxnRecord) -> CoreResult<&[u8]> {
    record.backend_name.as_deref().ok_or(CoreError::BadFormat)
}

fn record_temp_backend_name(record: &TxnRecord) -> CoreResult<&[u8]> {
    record
        .temp_backend_name
        .as_deref()
        .ok_or(CoreError::BadFormat)
}

fn record_old_rawname(record: &TxnRecord) -> CoreResult<&[u8]> {
    record.old_rawname.as_deref().ok_or(CoreError::BadFormat)
}

fn record_new_rawname(record: &TxnRecord) -> CoreResult<&[u8]> {
    record.new_rawname.as_deref().ok_or(CoreError::BadFormat)
}

fn record_object_id(record: &TxnRecord) -> CoreResult<u64> {
    record.object_id.ok_or(CoreError::BadFormat)
}

fn record_object_kind(record: &TxnRecord) -> CoreResult<libc::mode_t> {
    record.object_kind.ok_or(CoreError::BadFormat)
}

fn cstring_from_record_bytes(bytes: &[u8]) -> CoreResult<CString> {
    CString::new(bytes).map_err(|_| CoreError::BadFormat)
}

fn removal_flags_for_kind(kind: libc::mode_t) -> UnlinkatFlags {
    if (kind & libc::S_IFMT) == libc::S_IFDIR {
        UnlinkatFlags::RemoveDir
    } else {
        UnlinkatFlags::NoRemoveDir
    }
}

fn entry_exists(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<bool> {
    match fstatat(dir_fd, name, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(_) => Ok(true),
        Err(nix::errno::Errno::ENOENT) => Ok(false),
        Err(err) => Err(CoreError::from(err)),
    }
}

fn remove_entry_if_present(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
    flags: UnlinkatFlags,
) -> CoreResult<bool> {
    if !entry_exists(dir_fd, name)? {
        return Ok(false);
    }
    unlinkat(dir_fd, name, flags).map_err(CoreError::from)?;
    Ok(true)
}

fn sync_parent_dirs(
    old_dir: BorrowedFd<'_>,
    old_segments: &[Vec<u8>],
    new_dir: BorrowedFd<'_>,
    new_segments: &[Vec<u8>],
) -> CoreResult<()> {
    core_fsync_dir(old_dir).map_err(CoreError::from)?;
    if old_segments != new_segments {
        core_fsync_dir(new_dir).map_err(CoreError::from)?;
    }
    Ok(())
}

fn open_entry_for_sync(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<OwnedFd> {
    match openat(
        dir_fd,
        name,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => Ok(fd),
        Err(nix::errno::Errno::EISDIR) => openat(
            dir_fd,
            name,
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(CoreError::from),
        Err(err) => Err(CoreError::from(err)),
    }
}

fn sync_entry(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<()> {
    let fd = open_entry_for_sync(dir_fd, name)?;
    retry_eintr(|| fsync(fd.as_fd())).map_err(CoreError::from)
}

fn openat_nofollow_for_xattr_with_errno(
    dir_fd: BorrowedFd<'_>,
    name: &CStr,
) -> CoreResult<(OwnedFd, Option<i32>)> {
    match openat(
        dir_fd,
        name,
        OFlag::O_RDONLY | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
        Mode::empty(),
    ) {
        Ok(fd) => Ok((fd, None)),
        Err(nix::errno::Errno::EISDIR) => openat(
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
        .map_err(CoreError::from),
        Err(nix::errno::Errno::ELOOP) => openat(
            dir_fd,
            name,
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map(|fd| (fd, Some(libc::ELOOP)))
        .map_err(CoreError::from),
        Err(nix::errno::Errno::ENXIO) => openat(
            dir_fd,
            name,
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map(|fd| (fd, Some(libc::ENXIO)))
        .map_err(CoreError::from),
        Err(err) => Err(CoreError::from(err)),
    }
}

fn rawname_xattr_name() -> CString {
    CString::new("user.ln2.rawname").unwrap()
}

fn set_rawname(fd: BorrowedFd<'_>, raw: &[u8]) -> CoreResult<()> {
    let name = rawname_xattr_name();
    let rc = unsafe {
        libc::fsetxattr(
            fd.as_raw_fd(),
            name.as_ptr(),
            raw.as_ptr() as *const libc::c_void,
            raw.len(),
            0,
        )
    };
    if rc < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn set_rawname_via_path(path: &CStr, raw: &[u8]) -> CoreResult<()> {
    let name = rawname_xattr_name();
    let rc = unsafe {
        libc::lsetxattr(
            path.as_ptr(),
            name.as_ptr(),
            raw.as_ptr() as *const libc::c_void,
            raw.len(),
            0,
        )
    };
    if rc < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn clear_rawname(fd: BorrowedFd<'_>) -> CoreResult<()> {
    let name = rawname_xattr_name();
    let rc = unsafe { libc::fremovexattr(fd.as_raw_fd(), name.as_ptr()) };
    if rc < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn clear_rawname_via_path(path: &CStr) -> CoreResult<()> {
    let name = rawname_xattr_name();
    let rc = unsafe { libc::lremovexattr(path.as_ptr(), name.as_ptr()) };
    if rc < 0 {
        return Err(CoreError::from(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn set_rawname_at(dir_fd: BorrowedFd<'_>, name: &CStr, raw: &[u8]) -> CoreResult<()> {
    let (fd, procfs_original_errno) = openat_nofollow_for_xattr_with_errno(dir_fd, name)?;
    match set_rawname(fd.as_fd(), raw) {
        Ok(()) => Ok(()),
        Err(err) if core_err_to_errno(&err) == libc::EBADF => {
            let original_errno = procfs_original_errno.unwrap_or(libc::EBADF);
            let Some(proc_path) = procfs_path_for(dir_fd, name) else {
                return Err(CoreError::from_errno(original_errno));
            };
            match set_rawname_via_path(proc_path.as_c_str(), raw) {
                Ok(()) => Ok(()),
                Err(path_err)
                    if matches!(core_err_to_errno(&path_err), libc::ENOENT | libc::ENOTDIR)
                        && !Path::new("/proc/self/fd").exists() =>
                {
                    Err(CoreError::from_errno(original_errno))
                }
                Err(path_err) => Err(path_err),
            }
        }
        Err(err) => Err(err),
    }
}

fn clear_rawname_at(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<()> {
    let (fd, procfs_original_errno) = openat_nofollow_for_xattr_with_errno(dir_fd, name)?;
    match clear_rawname(fd.as_fd()) {
        Ok(()) => Ok(()),
        Err(err) if core_err_to_errno(&err) == libc::EBADF => {
            let original_errno = procfs_original_errno.unwrap_or(libc::EBADF);
            let Some(proc_path) = procfs_path_for(dir_fd, name) else {
                return Err(CoreError::from_errno(original_errno));
            };
            match clear_rawname_via_path(proc_path.as_c_str()) {
                Ok(()) => Ok(()),
                Err(path_err)
                    if matches!(core_err_to_errno(&path_err), libc::ENOENT | libc::ENOTDIR)
                        && !Path::new("/proc/self/fd").exists() =>
                {
                    Err(CoreError::from_errno(original_errno))
                }
                Err(path_err) => Err(path_err),
            }
        }
        Err(err) => Err(err),
    }
}

fn clear_rawname_if_present(dir_fd: BorrowedFd<'_>, name: &CStr) -> CoreResult<bool> {
    match clear_rawname_at(dir_fd, name) {
        Ok(()) => Ok(true),
        Err(err)
            if matches!(
                core_err_to_errno(&err),
                libc::ENODATA | libc::ENOENT | libc::EOPNOTSUPP | libc::ENOSYS | libc::EPERM
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
    }
}

fn encode_record(record: &TxnRecord) -> CoreResult<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(TXN_MAGIC);
    out.extend_from_slice(&record.version.to_le_bytes());
    out.push(encode_kind(record.kind));
    encode_option_u64(&mut out, record.object_id);
    encode_option_bytes(&mut out, record.backend_name.as_deref())?;
    encode_segments(&mut out, &record.old_parent_segments)?;
    encode_segments(&mut out, &record.new_parent_segments)?;
    encode_option_bytes(&mut out, record.old_rawname.as_deref())?;
    encode_option_bytes(&mut out, record.new_rawname.as_deref())?;
    encode_option_bytes(&mut out, record.temp_backend_name.as_deref())?;
    encode_option_mode(&mut out, record.object_kind);
    Ok(out)
}

fn decode_record(buf: &[u8]) -> CoreResult<TxnRecord> {
    let mut cursor = Cursor::new(buf);
    if cursor.take(4)? != TXN_MAGIC {
        return Err(CoreError::BadFormat);
    }

    let version = cursor.take_u32()?;
    if version != TXN_VERSION {
        return Err(CoreError::BadFormat);
    }

    let kind = decode_kind(cursor.take_u8()?)?;
    let record = TxnRecord {
        version,
        kind,
        object_id: cursor.take_option_u64()?,
        backend_name: cursor.take_option_bytes()?,
        old_parent_segments: cursor.take_segments()?,
        new_parent_segments: cursor.take_segments()?,
        old_rawname: cursor.take_option_bytes()?,
        new_rawname: cursor.take_option_bytes()?,
        temp_backend_name: cursor.take_option_bytes()?,
        object_kind: cursor.take_option_mode()?,
    };

    if !cursor.is_empty() {
        return Err(CoreError::BadFormat);
    }

    Ok(record)
}

fn validate_txn_record(record: &TxnRecord) -> CoreResult<()> {
    validate_backend_segment_list(&record.old_parent_segments)?;
    validate_backend_segment_list(&record.new_parent_segments)?;

    match record.kind {
        TxnKind::CreateLong => {
            validate_long_backend_identity(record)?;
            validate_long_rawname(record_new_rawname(record)?)?;
            validate_prefixed_internal_name(record_temp_backend_name(record)?, b".ln2_fs_ctmp_")?;
            record_object_kind(record)?;
        }
        TxnKind::CreateShort => {
            validate_short_backend_name(record_backend_name(record)?)?;
            record_object_kind(record)?;
        }
        TxnKind::LinkShort => {
            validate_short_backend_name(record_backend_name(record)?)?;
            validate_short_backend_name(record_temp_backend_name(record)?)?;
        }
        TxnKind::RenameShortToShort => {
            validate_short_backend_name(record_backend_name(record)?)?;
            validate_short_backend_name(record_new_rawname(record)?)?;
            if let Some(displaced) = record.temp_backend_name.as_deref() {
                validate_short_backend_name(displaced)?;
            }
        }
        TxnKind::RenameShortToLong => {
            validate_long_backend_identity(record)?;
            validate_short_backend_name(record_temp_backend_name(record)?)?;
            validate_long_rawname(record_new_rawname(record)?)?;
            record_object_kind(record)?;
        }
        TxnKind::RenameLongToShort => {
            validate_long_backend_identity(record)?;
            validate_long_rawname(record_old_rawname(record)?)?;
            validate_short_backend_name(record_temp_backend_name(record)?)?;
            record_object_kind(record)?;
        }
        TxnKind::RenameLongToLongSameDir | TxnKind::RenameLongToLongCrossDir => {
            validate_long_backend_identity(record)?;
            validate_long_rawname(record_old_rawname(record)?)?;
            validate_long_rawname(record_new_rawname(record)?)?;
            record_object_kind(record)?;
        }
        TxnKind::UnlinkLong => {
            validate_long_backend_identity(record)?;
            validate_long_rawname(record_old_rawname(record)?)?;
            validate_prefixed_internal_name(record_temp_backend_name(record)?, b".ln2_fs_delobj_")?;
            record_object_kind(record)?;
        }
        TxnKind::UnlinkShort => {
            validate_short_backend_name(record_backend_name(record)?)?;
            validate_prefixed_internal_name(record_temp_backend_name(record)?, b".ln2_fs_delobj_")?;
        }
        TxnKind::RemoveDir => {
            validate_backend_dir_name(record_backend_name(record)?)?;
            validate_prefixed_internal_name(record_temp_backend_name(record)?, b".ln2_fs_deldir_")?;
        }
    }

    Ok(())
}

fn validate_backend_segment_list(segments: &[Vec<u8>]) -> CoreResult<()> {
    for segment in segments {
        validate_backend_path_segment(segment)?;
    }
    Ok(())
}

fn validate_backend_path_segment(name: &[u8]) -> CoreResult<()> {
    validate_record_name(name)?;

    if parse_long_object_id(name).is_ok() {
        return Ok(());
    }

    validate_short_backend_name(name)
}

fn validate_backend_dir_name(name: &[u8]) -> CoreResult<()> {
    validate_backend_path_segment(name)
}

fn validate_record_name(name: &[u8]) -> CoreResult<()> {
    validate_relative_segment(name)?;
    CString::new(name).map_err(|_| CoreError::BadFormat)?;
    Ok(())
}

fn validate_user_visible_name(name: &[u8]) -> CoreResult<()> {
    validate_record_name(name)?;
    if is_reserved_prefix(name) || name.starts_with(crate::v2::index::FS_INTERNAL_PREFIX.as_bytes())
    {
        return Err(CoreError::BadFormat);
    }
    Ok(())
}

fn validate_short_backend_name(name: &[u8]) -> CoreResult<()> {
    validate_user_visible_name(name)?;
    match classify_segment(name, MAX_SEGMENT_ON_DISK) {
        Ok(SegmentKind::Short) => Ok(()),
        _ => Err(CoreError::BadFormat),
    }
}

fn validate_long_rawname(raw: &[u8]) -> CoreResult<()> {
    validate_user_visible_name(raw)?;
    match classify_committed_segment(raw) {
        Ok(SegmentKind::Long) => Ok(()),
        _ => Err(CoreError::BadFormat),
    }
}

fn validate_prefixed_internal_name(name: &[u8], prefix: &[u8]) -> CoreResult<()> {
    validate_record_name(name)?;
    if !name.starts_with(prefix) {
        return Err(CoreError::BadFormat);
    }
    Ok(())
}

fn validate_long_backend_identity(record: &TxnRecord) -> CoreResult<()> {
    let backend_name = record_backend_name(record)?;
    validate_record_name(backend_name)?;
    let object_id = parse_long_object_id(backend_name).map_err(|_| CoreError::BadFormat)?;
    if record_object_id(record)? != object_id {
        return Err(CoreError::BadFormat);
    }
    Ok(())
}

fn encode_kind(kind: TxnKind) -> u8 {
    match kind {
        TxnKind::CreateLong => 1,
        TxnKind::CreateShort => 2,
        TxnKind::LinkShort => 3,
        TxnKind::RenameShortToShort => 4,
        TxnKind::RenameShortToLong => 5,
        TxnKind::RenameLongToShort => 6,
        TxnKind::RenameLongToLongSameDir => 7,
        TxnKind::RenameLongToLongCrossDir => 8,
        TxnKind::UnlinkLong => 9,
        TxnKind::UnlinkShort => 10,
        TxnKind::RemoveDir => 11,
    }
}

fn decode_kind(tag: u8) -> CoreResult<TxnKind> {
    match tag {
        1 => Ok(TxnKind::CreateLong),
        2 => Ok(TxnKind::CreateShort),
        3 => Ok(TxnKind::LinkShort),
        4 => Ok(TxnKind::RenameShortToShort),
        5 => Ok(TxnKind::RenameShortToLong),
        6 => Ok(TxnKind::RenameLongToShort),
        7 => Ok(TxnKind::RenameLongToLongSameDir),
        8 => Ok(TxnKind::RenameLongToLongCrossDir),
        9 => Ok(TxnKind::UnlinkLong),
        10 => Ok(TxnKind::UnlinkShort),
        11 => Ok(TxnKind::RemoveDir),
        _ => Err(CoreError::BadFormat),
    }
}

fn encode_option_u64(out: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_le_bytes());
        }
        None => out.push(0),
    }
}

fn encode_option_mode(out: &mut Vec<u8>, value: Option<libc::mode_t>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        None => out.push(0),
    }
}

fn encode_option_bytes(out: &mut Vec<u8>, value: Option<&[u8]>) -> CoreResult<()> {
    match value {
        Some(bytes) => {
            out.push(1);
            encode_bytes(out, bytes)?;
        }
        None => out.push(0),
    }
    Ok(())
}

fn encode_segments(out: &mut Vec<u8>, segments: &[Vec<u8>]) -> CoreResult<()> {
    let count = u32::try_from(segments.len()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
    out.extend_from_slice(&count.to_le_bytes());
    for segment in segments {
        encode_bytes(out, segment)?;
    }
    Ok(())
}

fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) -> CoreResult<()> {
    let len = u32::try_from(bytes.len()).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(bytes);
    Ok(())
}

fn write_all(fd: BorrowedFd<'_>, buf: &[u8]) -> CoreResult<()> {
    let mut written = 0;
    while written < buf.len() {
        let step = retry_eintr(|| write(fd, &buf[written..])).map_err(CoreError::from)?;
        if step == 0 {
            return Err(CoreError::from_errno(libc::EIO));
        }
        written += step;
    }
    Ok(())
}

struct Cursor<'a> {
    buf: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.buf.len()
    }

    fn take(&mut self, len: usize) -> CoreResult<&'a [u8]> {
        let end = self.offset.checked_add(len).ok_or(CoreError::BadFormat)?;
        if end > self.buf.len() {
            return Err(CoreError::BadFormat);
        }
        let chunk = &self.buf[self.offset..end];
        self.offset = end;
        Ok(chunk)
    }

    fn take_u8(&mut self) -> CoreResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> CoreResult<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn take_u64(&mut self) -> CoreResult<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn take_len_prefixed_bytes(&mut self) -> CoreResult<Vec<u8>> {
        let len = usize::try_from(self.take_u32()?).map_err(|_| CoreError::BadFormat)?;
        Ok(self.take(len)?.to_vec())
    }

    fn take_option_bytes(&mut self) -> CoreResult<Option<Vec<u8>>> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.take_len_prefixed_bytes()?)),
            _ => Err(CoreError::BadFormat),
        }
    }

    fn take_option_u64(&mut self) -> CoreResult<Option<u64>> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.take_u64()?)),
            _ => Err(CoreError::BadFormat),
        }
    }

    fn take_option_mode(&mut self) -> CoreResult<Option<libc::mode_t>> {
        match self.take_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.take_u32()? as libc::mode_t)),
            _ => Err(CoreError::BadFormat),
        }
    }

    fn take_segments(&mut self) -> CoreResult<Vec<Vec<u8>>> {
        let count = usize::try_from(self.take_u32()?).map_err(|_| CoreError::BadFormat)?;
        let mut segments = Vec::with_capacity(count);
        for _ in 0..count {
            segments.push(self.take_len_prefixed_bytes()?);
        }
        Ok(segments)
    }
}
