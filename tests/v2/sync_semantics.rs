use super::common::*;
use super::*;
use std::fs;

#[test]
fn normal_handle_fsync_reports_backend_sync_errors() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, false);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(!opened.passthrough);

    let _hook = force_fsync_errno(libc::EIO);
    let before = common::test_fsync_call_count();
    let err = fsync_result(&fs, entry.ino, opened.fh, false).unwrap_err();

    assert_eq!(err, libc::EIO);
    assert_eq!(common::test_fsync_call_count(), before + 1);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_fsync_reports_backend_sync_errors() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);

    let _hook = force_fsync_errno(libc::EIO);
    let before = common::test_fsync_call_count();
    let err = fsync_result(&fs, entry.ino, opened.fh, false).unwrap_err();

    assert_eq!(err, libc::EIO);
    assert_eq!(common::test_fsync_call_count(), before + 1);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_fsync_success_requires_successful_sync() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);

    let before = common::test_fsync_call_count();
    let result = fsync_result(&fs, entry.ino, opened.fh, false);

    assert!(
        result.is_ok(),
        "passthrough fsync reaches success after syncing stored data_fd"
    );
    assert_eq!(
        common::test_fsync_call_count(),
        before + 1,
        "passthrough fsync must perform a real sync on the stored data_fd"
    );
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_fsync_pins_handle_across_release_race() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);

    let _race = force_passthrough_release_after_check(opened.fh);
    let _hook = force_fsync_errno(libc::EIO);
    let before = common::test_fsync_call_count();
    let err = fsync_result(&fs, entry.ino, opened.fh, false).unwrap_err();

    assert_eq!(err, libc::EIO);
    assert_eq!(common::test_fsync_call_count(), before + 1);
}

#[test]
fn sync_data_true_surfaces_fdatasync_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, true, false);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    assert!(!opened.passthrough);

    let _hook = force_fdatasync_errno(libc::EIO);
    let before = common::test_fdatasync_call_count();
    let err = write_result(&fs, entry.ino, opened.fh, 0, b"!").unwrap_err();

    assert_eq!(
        err,
        libc::EIO,
        "sync_data writes must surface fdatasync failure"
    );
    assert_eq!(common::test_fdatasync_call_count(), before + 1);
}

#[test]
fn sync_data_false_does_not_force_fdatasync() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, false);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    assert!(!opened.passthrough);

    let _hook = force_fdatasync_errno(libc::EIO);
    let before = common::test_fdatasync_call_count();
    let write = write_result(&fs, entry.ino, opened.fh, 0, b"!").unwrap();

    assert_eq!(write.size, 1);
    assert_eq!(common::test_fdatasync_call_count(), before);
}

#[test]
fn sync_data_true_sync_failure_still_invalidates_dir_cache_and_notifies_inode() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, true, false);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();
    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    readdir_names_result(&fs, ROOT_INODE).unwrap();
    assert!(dir_cache_contains_result(&fs, OsStr::new("/"), OsStr::new("file")).unwrap());

    let recorder = TestNotifierRecorder::attach(&fs);
    let _hook = force_fdatasync_errno(libc::EIO);
    let err = write_result(&fs, entry.ino, opened.fh, 0, b"!").unwrap_err();

    assert_eq!(err, libc::EIO);
    assert!(
        !dir_cache_contains_result(&fs, OsStr::new("/"), OsStr::new("file")).unwrap(),
        "dir cache should still be invalidated once backend write has committed"
    );
    assert!(
        recorder.recorded_inode_invalidation(entry.ino),
        "inode invalidation should still be emitted once backend write has committed"
    );
}
