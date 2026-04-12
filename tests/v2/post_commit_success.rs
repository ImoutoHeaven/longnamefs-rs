use super::common::*;
use super::*;
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileTypeExt;
use std::path::PathBuf;
use std::time::Duration;

fn new_fs_with_index_sync(backend: &TempDir, index_sync: IndexSync) -> LongNameFsV2Fuser {
    let config = Config::open_backend(backend.path().clone(), false, false).unwrap();
    LongNameFsV2Fuser::new(
        config,
        1024,
        Some(Duration::from_secs(60)),
        1024,
        index_sync,
        Duration::from_secs(1),
        Duration::from_secs(1),
        false,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap()
}

fn new_always_sync_fs(backend: &TempDir) -> LongNameFsV2Fuser {
    new_fs_with_index_sync(backend, IndexSync::Always)
}

fn long_name(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        "x".repeat(crate::v2::path::MAX_SEGMENT_ON_DISK + 8)
    )
}

fn backend_path(tmp: &TempDir, backend_name: &[u8]) -> PathBuf {
    tmp.path().join(OsStr::from_bytes(backend_name))
}

fn assert_dirty_pending(snapshot: &TestStateSnapshot) {
    assert!(
        snapshot.dirty,
        "directory state should remain recoverably dirty"
    );
    assert!(
        snapshot.pending > 0,
        "directory state should retain pending work"
    );
}

fn assert_clean(snapshot: &TestStateSnapshot) {
    assert!(
        !snapshot.dirty,
        "directory state should be clean after an `IndexSync::Always` flush completes"
    );
    assert_eq!(
        snapshot.pending, 0,
        "directory state should not retain pending work after an `IndexSync::Always` flush completes"
    );
}

fn assert_dirty_pending_view(state: &TestDirStateView) {
    assert!(
        op_state_is_dirty(state),
        "directory state should remain recoverably dirty"
    );
    assert!(
        op_state_pending(state) > 0,
        "directory state should retain pending work"
    );
}

fn long_symlink_rawname_supported() -> bool {
    let probe = TempDir::new();
    let dir_fd = nix::fcntl::open(
        probe.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new(".__ln2_symlink_probe").unwrap();
    symlinkat("target", dir_fd.as_fd(), name.as_c_str()).unwrap();

    match set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), b"rawname-symlink-probe") {
        Ok(()) => true,
        Err(CoreError::Io(ref ioe))
            if matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM) | Some(libc::ELOOP)
            ) =>
        {
            false
        }
        Err(err) => panic!("unexpected symlink rawname probe failure: {err:?}"),
    }
}

fn rawname_write_is_unsupported_errno(errno: i32) -> bool {
    matches!(errno, libc::EOPNOTSUPP | libc::ENOSYS | libc::EPERM)
}

fn special_node_sync_is_unavailable_errno(errno: i32) -> bool {
    matches!(errno, libc::EINVAL | libc::ENXIO | libc::EBADF)
}

fn assert_special_node_absent_after_failed_mknod(tmp: &TempDir, fs: &LongNameFsV2Fuser, name: &str) {
    assert_eq!(
        lookup_entry_result(fs, ROOT_INODE, OsStr::new(name)).unwrap_err(),
        libc::ENOENT,
        "failed special-node mknod must not expose the logical name"
    );
    assert!(
        !tmp.path().join(name).exists(),
        "failed special-node mknod must roll back the backend entry"
    );
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "failed special-node mknod must clear the live txn record"
    );
}

#[test]
fn rename_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("rename-src");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new("renamed"),
            0,
        )
    };

    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("renamed")).is_ok(),
        "backend rename should already be committed at the destination name"
    );
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_err(),
        "source name should be gone once backend rename commits"
    );
    assert_dirty_pending(&snapshot);
    let rename = result
        .expect("rename should report success after backend commit even if index flush fails");
    assert!(
        rename.used_callback_path,
        "rename regression must execute the shipped callback path"
    );
}

#[test]
fn rename_upgrade_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let dst = long_name("rename-upgrade-dst");
    let created = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new("rename-upgrade-src"),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("rename-upgrade-src"),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
    };

    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_ok(),
        "backend rename should already be committed at the destination name"
    );
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("rename-upgrade-src")).is_err(),
        "source name should be gone once backend rename commits"
    );
    assert_dirty_pending(&snapshot);
    let rename = result.expect(
        "rename upgrade should report success after backend commit even if index flush fails",
    );
    assert!(
        rename.used_callback_path,
        "rename upgrade regression must execute the shipped callback path"
    );
}

#[test]
fn rename_upgrade_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let dst = long_name("rename-upgrade-clear-dst");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new("rename-upgrade-clear-src"),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let dst_thread = dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-upgrade-clear-src"),
            ROOT_INODE,
            OsStr::new(&dst_thread),
            0,
        )
    });

    assert!(pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(
        lookup_entry_result(
            fs.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-upgrade-clear-src")
        )
        .is_ok()
    );
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).is_ok());
    assert!(
        lookup_entry_result(
            fs.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-upgrade-clear-src")
        )
        .is_err()
    );
}

#[test]
fn rename_upgrade_pause_hook_is_not_stolen_by_unrelated_txn() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let dst = long_name("rename-upgrade-scope-dst");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new("rename-upgrade-scope-src"),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let unrelated_tmp = TempDir::new();
    let unrelated_fs = std::sync::Arc::new(new_always_sync_fs(&unrelated_tmp));
    let unrelated_name = long_name("rename-upgrade-scope-unrelated");

    let reader = hold_dir_read_guard(fs.as_ref(), ROOT_INODE).unwrap();
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let dst_thread = dst.clone();
    let rename_handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-upgrade-scope-src"),
            ROOT_INODE,
            OsStr::new(&dst_thread),
            0,
        )
    });

    let unrelated_fs_thread = std::sync::Arc::clone(&unrelated_fs);
    let unrelated_name_thread = unrelated_name.clone();
    let unrelated_handle = std::thread::spawn(move || {
        create_result(
            unrelated_fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&unrelated_name_thread),
            0o644,
            libc::O_RDWR,
        )
    });

    let unrelated_stole_pause = pause.wait_until_blocked_timeout(Duration::from_millis(100));
    if unrelated_stole_pause {
        pause.release();
    }
    let unrelated_created = unrelated_handle.join().unwrap().unwrap();
    release_result(
        unrelated_fs.as_ref(),
        unrelated_created.ino,
        unrelated_created.fh,
    )
    .unwrap();

    drop(reader);

    let rename_blocked_after_reader_release = if unrelated_stole_pause {
        false
    } else {
        let blocked = pause.wait_until_blocked_timeout(Duration::from_secs(1));
        if blocked {
            assert!(tmp.path().join(".ln2_fs_txn").exists());
            pause.release();
        }
        blocked
    };

    rename_handle.join().unwrap().unwrap();

    assert!(
        !unrelated_stole_pause,
        "unrelated txn must not consume rename-upgrade's txn-clear pause hook"
    );
    assert!(
        rename_blocked_after_reader_release,
        "rename-upgrade should block on its own txn-clear hook once the directory reader is released"
    );
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).is_ok());
    assert!(
        lookup_entry_result(
            fs.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-upgrade-scope-src")
        )
        .is_err()
    );
}

#[test]
fn rename_long_to_short_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src = long_name("rename-downgrade-clear-src");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new(&src),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let src_thread = src.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&src_thread),
            ROOT_INODE,
            OsStr::new("rename-downgrade-clear-dst"),
            0,
        )
    });

    assert!(pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(
            fs.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-downgrade-clear-dst")
        )
        .unwrap_err(),
        libc::ENOENT
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(
        lookup_entry_result(
            fs.as_ref(),
            ROOT_INODE,
            OsStr::new("rename-downgrade-clear-dst")
        )
        .is_ok()
    );
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_err());
}

#[test]
fn rename_callback_nested_destination_survives_post_commit_bookkeeping_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("srcdir"), 0o755).unwrap();
    let nested_child = mkdir_result(&fs, src_dir.ino, OsStr::new("dst-leaf"), 0o755).unwrap();
    let src = long_name("nested-src");
    let dst = long_name("nested-dst");
    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        rename_result(
            &fs,
            src_dir.ino,
            OsStr::new(&src),
            nested_child.ino,
            OsStr::new(&dst),
            0,
        )
    };

    let src_snapshot = state_snapshot_result(&fs, OsStr::new("/srcdir")).unwrap();
    assert!(
        lookup_entry_result(&fs, nested_child.ino, OsStr::new(&dst)).is_ok(),
        "backend rename should already be committed at the nested destination name"
    );
    assert!(
        lookup_entry_result(&fs, src_dir.ino, OsStr::new(&src)).is_err(),
        "source name should be gone once backend rename commits"
    );
    assert_dirty_pending(&src_snapshot);
    let rename = result
        .expect("rename callback should not leak bookkeeping failure after backend rename commits");
    assert!(
        rename.used_callback_path,
        "nested rename regression must execute the shipped callback path"
    );
}

#[test]
fn rename_short_to_long_survives_apply_rename_inode_bookkeeping_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let dst = long_name("rename-bookkeeping-dst");
    let created =
        create_result(&fs, ROOT_INODE, OsStr::new("rename-bookkeeping-src"), 0o644, libc::O_RDWR)
            .unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let result = {
        let _hook = force_rename_bookkeeping_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("rename-bookkeeping-src"),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
    };

    let rename = result.expect(
        "rename should still report success once durable txn clear completed even if post-commit bookkeeping fails",
    );
    assert!(
        rename.used_callback_path,
        "rename regression must execute the shipped callback path"
    );
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("rename-bookkeeping-src")).unwrap_err(),
        libc::ENOENT
    );
    assert!(
        getattr_result(&fs, created.ino, None).is_ok(),
        "committed rename must keep the original inode path-addressable in-mount even if post-commit bookkeeping fails"
    );
    let reopened = open_result(&fs, created.ino, libc::O_RDONLY as u32).expect(
        "committed rename must keep the original inode reopenable even if post-commit bookkeeping fails",
    );
    release_result(&fs, created.ino, reopened.fh).unwrap();
    let renamed = lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).unwrap();
    assert_eq!(
        renamed.ino, created.ino,
        "fresh destination lookup must reuse the already-issued inode instead of allocating a duplicate"
    );
    let cached = inode_entry_result(&fs, created.ino).unwrap();
    assert_eq!(cached.name.as_os_str(), OsStr::new(&dst));
    assert_eq!(cached.parent, ROOT_INODE);
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "post-commit bookkeeping failure must not resurrect the txn record"
    );

    let followup = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new("rename-bookkeeping-followup"),
        0o644,
        libc::O_RDWR,
    )
    .expect("post-commit bookkeeping failure must not poison later mutations");
    release_result(&fs, followup.ino, followup.fh).unwrap();
}

#[test]
fn unlink_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("unlink-target");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    let recorder = TestNotifierRecorder::attach(&fs);

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        unlink_result(&fs, ROOT_INODE, OsStr::new(&name))
    };

    assert!(
        result.is_ok(),
        "unlink should report success after backend commit even if index flush fails"
    );
    let unlink = result.expect("unlink callback path should succeed after backend commit");
    assert!(
        unlink.used_callback_path,
        "unlink regression must execute the shipped callback path, not helper-only logic"
    );
    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        !backend_path(&tmp, &created.backend_name).exists(),
        "backend entry should already be gone after unlink commit"
    );
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_err());
    assert!(
        recorder.recorded_delete(ROOT_INODE, created.ino, OsStr::new(&name)),
        "unlink should preserve delete notifications after post-commit flush failure"
    );
    assert_dirty_pending(&snapshot);
}

#[test]
fn rmdir_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("rmdir-target");
    let created = mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap();
    let recorder = TestNotifierRecorder::attach(&fs);

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        rmdir_result(&fs, ROOT_INODE, OsStr::new(&name))
    };

    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        !backend_path(&tmp, &created.backend_name).exists(),
        "backend directory should already be gone after rmdir commit"
    );
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_err());
    assert_dirty_pending(&snapshot);
    assert!(
        result.is_ok(),
        "rmdir should report success after backend commit even if index flush fails"
    );
    assert!(
        recorder.recorded_delete(ROOT_INODE, created.ino, OsStr::new(&name)),
        "rmdir should preserve delete notifications after post-commit flush failure"
    );
    assert!(
        recorder.recorded_inode_invalidation(ROOT_INODE),
        "rmdir should invalidate the parent inode after commit"
    );
    assert!(
        recorder.recorded_inode_invalidation(created.ino),
        "rmdir should invalidate the removed child inode after commit"
    );
}

#[test]
fn post_clear_delete_errno_hook_leaves_quarantine_residue_but_preserves_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("hook-delobj");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let _hook = force_post_clear_delete_errno(libc::EIO);
    unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();

    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_err());
    assert!(
        fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .any(|name| name.as_os_str().as_bytes().starts_with(b".ln2_fs_delobj_"))
    );
}

#[test]
fn unlink_long_uses_delete_quarantine_and_clears_txn_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let name = long_name("unlink-quarantine");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new(&name),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let name_thread = name.clone();
    let handle = std::thread::spawn(move || {
        unlink_result(fs_thread.as_ref(), ROOT_INODE, OsStr::new(&name_thread))
    });

    assert!(pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).is_ok());
    assert!(fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .as_os_str()
            .as_bytes()
            .starts_with(b".ln2_fs_delobj_")
    }));

    pause.release();
    let unlink = handle.join().unwrap().unwrap();

    assert!(unlink.used_callback_path);
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn unlink_success_survives_restart_with_stranded_quarantine_residue() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("restart-unlink");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    {
        let _hook = force_post_clear_delete_errno(libc::EIO);
        unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();
    }
    drop(fs);

    let reopened = new_always_sync_fs(&tmp);
    assert_eq!(
        lookup_entry_result(&reopened, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT
    );
    assert!(fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .as_os_str()
            .as_bytes()
            .starts_with(b".ln2_fs_delobj_")
    }));
}

#[test]
fn rmdir_post_clear_delete_errno_hook_leaves_quarantine_residue_but_preserves_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("hook-deldir");
    mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap();

    let _hook = force_post_clear_delete_errno(libc::EIO);
    rmdir_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();

    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_err());
    assert!(
        fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .any(|name| name.as_os_str().as_bytes().starts_with(b".ln2_fs_deldir_"))
    );
}

#[test]
fn rmdir_success_survives_restart_with_stranded_quarantine_residue() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("restart-rmdir");
    mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap();

    {
        let _hook = force_post_clear_delete_errno(libc::EIO);
        rmdir_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();
    }
    drop(fs);

    let reopened = new_always_sync_fs(&tmp);
    assert_eq!(
        lookup_entry_result(&reopened, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT
    );
    assert!(fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .as_os_str()
            .as_bytes()
            .starts_with(b".ln2_fs_deldir_")
    }));
}

#[test]
fn rmdir_respects_configured_index_sync_policy() {
    let tmp = TempDir::new();
    let fs = new_fs_with_index_sync(&tmp, IndexSync::Off);
    let name = long_name("rmdir-off");
    let created = mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap();

    let result = rmdir_result(&fs, ROOT_INODE, OsStr::new(&name));

    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        result.is_ok(),
        "rmdir should still succeed with index sync disabled"
    );
    assert!(
        !backend_path(&tmp, &created.backend_name).exists(),
        "backend directory should already be gone after rmdir commit"
    );
    assert!(
        snapshot.dirty,
        "rmdir should preserve dirty state when index sync policy is off"
    );
    assert!(
        snapshot.pending > 0,
        "rmdir should retain pending index work when index sync policy is off"
    );
}

#[test]
fn create_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("create-target");

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR)
    };
    assert!(
        result.is_ok(),
        "create should report success after backend commit"
    );
    let created = result.expect("create should succeed after backend commit");

    assert!(backend_path(&tmp, &created.backend_name).exists());
    assert_dirty_pending_view(&created.state);
}

#[test]
fn long_create_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let name = long_name("create-txn-clear");
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let name_thread = name.clone();
    let handle = std::thread::spawn(move || {
        create_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&name_thread),
            0o644,
            libc::O_RDWR,
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "long create should block at txn clear before reporting success"
    );
    assert!(
        tmp.path().join(".ln2_fs_txn").exists(),
        "txn file should remain present until the paused clear is released"
    );
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    let created = handle.join().unwrap().unwrap();

    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "txn file must be cleared before create returns success"
    );
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).is_ok());
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();
}

#[test]
fn short_create_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        create_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-create-clear"),
            0o644,
            libc::O_RDWR,
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short create should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-create-clear")).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    let created = handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-create-clear")).is_ok());
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();
}

#[test]
fn short_mkdir_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        mkdir_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-mkdir-clear"),
            0o755,
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short mkdir should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-mkdir-clear")).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-mkdir-clear")).is_ok());
}

#[test]
fn short_symlink_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    if !long_symlink_rawname_supported() {
        return;
    }
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        symlink_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-symlink-clear"),
            std::path::Path::new("target"),
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short symlink should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-symlink-clear"))
            .unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-symlink-clear")).is_ok()
    );
}

#[test]
fn short_mknod_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        mknod_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-mknod-clear"),
            libc::S_IFREG | 0o644,
            0,
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short mknod should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-mknod-clear")).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-mknod-clear")).is_ok());
}

#[test]
fn long_mkdir_create_fails_when_staging_object_sync_is_unavailable() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("mkdir-sync-unavailable");

    let err = {
        let _hook = force_fsync_errno(libc::EBADF);
        mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap_err()
    };

    assert_eq!(err, libc::EBADF);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT,
        "failed create-like sync must not expose the logical name"
    );
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "pre-namespace durability failure must not leave a txn record behind"
    );

    let backend_entries = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_vec())
        .collect::<Vec<_>>();
    assert!(
        !backend_entries
            .iter()
            .any(|name| name.starts_with(b".__ln2_obj_")),
        "failed create-like sync must not leave a committed stable backend object"
    );
    assert!(
        !backend_entries
            .iter()
            .any(|name| name.starts_with(b".ln2_fs_ctmp_")),
        "failed create-like sync must clean up the staging entry"
    );
}

#[test]
fn long_fifo_mknod_create_fails_when_staging_object_sync_is_unavailable() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("fifo-sync-unavailable");

    let err = {
        let _hook = force_fsync_errno(libc::EBADF);
        mknod_result(&fs, ROOT_INODE, OsStr::new(&name), libc::S_IFIFO | 0o644, 0).unwrap_err()
    };

    assert!(
        rawname_write_is_unsupported_errno(err) || special_node_sync_is_unavailable_errno(err),
        "long special-node create should fail with an unsupported primitive errno, got {err}"
    );
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT,
        "failed long special-node sync must not expose the logical name"
    );
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "pre-namespace durability failure must not leave a txn record behind"
    );

    let backend_entries = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_vec())
        .collect::<Vec<_>>();
    assert!(
        !backend_entries
            .iter()
            .any(|backend| backend.starts_with(b".__ln2_obj_")),
        "failed long special-node sync must not leave a committed stable backend object"
    );
    assert!(
        !backend_entries
            .iter()
            .any(|backend| backend.starts_with(b".ln2_fs_ctmp_")),
        "failed long special-node sync must clean up the staging entry"
    );
}

#[test]
fn long_create_post_txn_backend_collision_returns_eexist_without_retrying_new_id() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let occupied_backend = crate::v2::object_id::format_long_object_name(1);
    let occupied_path = tmp.path().join(OsStr::from_bytes(&occupied_backend));
    fs::write(&occupied_path, b"occupied").unwrap();
    set_rawname_xattr(&occupied_path, long_name("occupied-existing").as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (1u64).to_le_bytes()).unwrap();

    let fs = new_always_sync_fs(&tmp);
    let name = long_name("create-collision");
    let err = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap_err();

    assert_eq!(err, libc::EEXIST);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT,
        "failed long create collision must not expose the logical name"
    );
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "post-txn collision failure must clear the txn record"
    );
    let next_id = u64::from_le_bytes(
        fs::read(tmp.path().join(".ln2_fs_idalloc")).unwrap()[..8]
            .try_into()
            .unwrap(),
    );
    assert_eq!(
        next_id, 2,
        "collision failure should burn the allocated id once without silently allocating another"
    );

    let stable_entries = fs::read_dir(tmp.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_vec())
        .filter(|name| name.starts_with(b".__ln2_obj_"))
        .collect::<Vec<_>>();
    assert_eq!(stable_entries, vec![occupied_backend]);
}

#[test]
fn restart_after_long_create_post_commit_flush_failure_rejects_duplicate_with_eexist() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let name = long_name("restart-create-eexist");

    {
        let fs = new_always_sync_fs(&tmp);
        let created = {
            let _hook = force_post_commit_flush_errno(libc::EIO);
            create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR)
                .expect("initial create should still succeed after the namespace commits")
        };
        assert!(backend_path(&tmp, &created.backend_name).exists());
        assert_dirty_pending_view(&created.state);
        release_result(&fs, created.ino, created.fh).unwrap();
    }

    let reopened = new_always_sync_fs(&tmp);
    let err = create_result(
        &reopened,
        ROOT_INODE,
        OsStr::new(&name),
        0o644,
        libc::O_RDWR,
    )
    .unwrap_err();
    assert_eq!(err, libc::EEXIST);
}

#[test]
fn unlink_success_survives_post_clear_quarantine_cleanup_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("unlink-postclear");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let _hook = force_post_clear_delete_errno(libc::EIO);
    unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();

    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_err());
}

#[test]
fn rmdir_success_survives_post_clear_quarantine_cleanup_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    mkdir_result(&fs, ROOT_INODE, OsStr::new("rmdir-postclear"), 0o755).unwrap();

    let _hook = force_post_clear_delete_errno(libc::EIO);
    rmdir_result(&fs, ROOT_INODE, OsStr::new("rmdir-postclear")).unwrap();

    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("rmdir-postclear")).is_err());
}

#[test]
fn post_success_crash_survival_is_covered_for_every_supported_operation_kind() {
    let _serial = lock_test_hooks();

    for case in [
        "create_long",
        "create_short",
        "link_short",
        "rename_short_to_short",
        "rename_short_to_long",
        "rename_long_to_short",
        "rename_long_to_long_same_dir",
        "rename_long_to_long_cross_dir",
        "unlink_long",
        "unlink_short",
        "remove_dir",
    ] {
        let tmp = TempDir::new();
        let fs = new_always_sync_fs(&tmp);

        match case {
            "create_long" => {
                let name = long_name("restart-create");
                let created =
                    create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(lookup_entry_result(&restarted, ROOT_INODE, OsStr::new(&name)).is_ok());
            }
            "create_short" => {
                let created =
                    create_result(&fs, ROOT_INODE, OsStr::new("restart-short-create"), 0o644, libc::O_RDWR)
                        .unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-short-create"))
                        .is_ok()
                );
            }
            "link_short" => {
                fs::write(tmp.path().join("restart-link-src"), b"payload").unwrap();
                let source = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("restart-link-src")).unwrap();
                link_result(&fs, source.ino, ROOT_INODE, OsStr::new("restart-link-dst")).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-link-src"))
                        .is_ok()
                );
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-link-dst"))
                        .is_ok()
                );
            }
            "rename_short_to_short" => {
                fs::write(tmp.path().join("restart-short-rename-src"), b"payload").unwrap();
                rename_result(
                    &fs,
                    ROOT_INODE,
                    OsStr::new("restart-short-rename-src"),
                    ROOT_INODE,
                    OsStr::new("restart-short-rename-dst"),
                    0,
                )
                .unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(
                        &restarted,
                        ROOT_INODE,
                        OsStr::new("restart-short-rename-dst")
                    )
                    .is_ok()
                );
                assert!(
                    lookup_entry_result(
                        &restarted,
                        ROOT_INODE,
                        OsStr::new("restart-short-rename-src")
                    )
                    .is_err()
                );
            }
            "rename_short_to_long" => {
                fs::write(tmp.path().join("short-src"), b"payload").unwrap();
                let dst = long_name("restart-upgrade");
                rename_result(
                    &fs,
                    ROOT_INODE,
                    OsStr::new("short-src"),
                    ROOT_INODE,
                    OsStr::new(&dst),
                    0,
                )
                .unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(lookup_entry_result(&restarted, ROOT_INODE, OsStr::new(&dst)).is_ok());
            }
            "rename_long_to_short" => {
                let src = long_name("restart-downgrade-src");
                let created =
                    create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                rename_result(
                    &fs,
                    ROOT_INODE,
                    OsStr::new(&src),
                    ROOT_INODE,
                    OsStr::new("restart-short-dst"),
                    0,
                )
                .unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-short-dst"))
                        .is_ok()
                );
            }
            "rename_long_to_long_same_dir" => {
                let src = long_name("restart-same-src");
                let dst = long_name("restart-same-dst");
                let created =
                    create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                rename_result(
                    &fs,
                    ROOT_INODE,
                    OsStr::new(&src),
                    ROOT_INODE,
                    OsStr::new(&dst),
                    0,
                )
                .unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(lookup_entry_result(&restarted, ROOT_INODE, OsStr::new(&dst)).is_ok());
                assert!(lookup_entry_result(&restarted, ROOT_INODE, OsStr::new(&src)).is_err());
            }
            "rename_long_to_long_cross_dir" => {
                let src_dir =
                    mkdir_result(&fs, ROOT_INODE, OsStr::new("restart-src"), 0o755).unwrap();
                let dst_dir =
                    mkdir_result(&fs, ROOT_INODE, OsStr::new("restart-dst"), 0o755).unwrap();
                let src = long_name("restart-cross-src");
                let dst = long_name("restart-cross-dst");
                let created =
                    create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                rename_result(
                    &fs,
                    src_dir.ino,
                    OsStr::new(&src),
                    dst_dir.ino,
                    OsStr::new(&dst),
                    0,
                )
                .unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                let restarted_src_dir =
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-src")).unwrap();
                let restarted_dst_dir =
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-dst")).unwrap();
                assert!(
                    lookup_entry_result(&restarted, restarted_dst_dir.ino, OsStr::new(&dst))
                        .is_ok()
                );
                assert!(
                    lookup_entry_result(&restarted, restarted_src_dir.ino, OsStr::new(&src))
                        .is_err()
                );
            }
            "unlink_long" => {
                let name = long_name("restart-unlink");
                let created =
                    create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
                release_result(&fs, created.ino, created.fh).unwrap();
                unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(lookup_entry_result(&restarted, ROOT_INODE, OsStr::new(&name)).is_err());
            }
            "unlink_short" => {
                fs::write(tmp.path().join("restart-short-unlink"), b"payload").unwrap();
                unlink_result(&fs, ROOT_INODE, OsStr::new("restart-short-unlink")).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-short-unlink"))
                        .is_err()
                );
            }
            "remove_dir" => {
                mkdir_result(&fs, ROOT_INODE, OsStr::new("restart-rmdir"), 0o755).unwrap();
                rmdir_result(&fs, ROOT_INODE, OsStr::new("restart-rmdir")).unwrap();
                drop(fs);
                let restarted = new_always_sync_fs(&tmp);
                assert!(
                    lookup_entry_result(&restarted, ROOT_INODE, OsStr::new("restart-rmdir"))
                        .is_err()
                );
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn always_sync_overlapping_same_directory_mutations_wait_for_follow_up_flush() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let first_name = long_name("overlap-first");
    let second_name = long_name("overlap-second");
    let mut pause = pause_next_post_commit_flush(&fs);

    std::thread::scope(|scope| {
        let fs_ref = &fs;
        let first_name_ref = &first_name;
        let first = scope.spawn(move || {
            create_result(
                fs_ref,
                ROOT_INODE,
                OsStr::new(first_name_ref),
                0o644,
                libc::O_RDWR,
            )
        });
        pause.wait_until_blocked();

        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let fs_ref = &fs;
        let second_name_ref = &second_name;
        scope.spawn(move || {
            started_tx.send(()).unwrap();
            let result = create_result(
                fs_ref,
                ROOT_INODE,
                OsStr::new(second_name_ref),
                0o644,
                libc::O_RDWR,
            );
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();

        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        let mut early_result = None;
        while std::time::Instant::now() < deadline {
            match done_rx.try_recv() {
                Ok(result) => {
                    early_result = Some(result);
                    break;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => std::thread::yield_now(),
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    panic!("second create worker disconnected before reporting a result")
                }
            }
        }
        let second_completed_early = early_result.is_some();
        pause.release();

        let first_created = first
            .join()
            .unwrap()
            .expect("first create should succeed once the paused flush resumes");
        let second_result = match early_result {
            Some(result) => result,
            None => done_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("second create should complete after the paused flush resumes"),
        };
        assert!(
            !second_completed_early,
            "second same-directory mutation should not report success before the in-flight `IndexSync::Always` flush finishes"
        );
        let second_created = second_result
            .expect("second create should succeed once follow-up flush work is handled");

        let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
        assert_clean(&snapshot);

        release_result(&fs, first_created.ino, first_created.fh).unwrap();
        release_result(&fs, second_created.ino, second_created.fh).unwrap();
    });
}

#[test]
fn mkdir_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("mkdir-target");

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755)
    };
    assert!(
        result.is_ok(),
        "mkdir should report success after backend commit"
    );
    let created = result.expect("mkdir should succeed after backend commit");

    let meta = fs::symlink_metadata(backend_path(&tmp, &created.backend_name)).unwrap();
    assert!(meta.is_dir());
    assert_dirty_pending_view(&created.state);
}

#[test]
fn symlink_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    if !long_symlink_rawname_supported() {
        return;
    }
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("symlink-target");

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        symlink_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&name),
            std::path::Path::new("target"),
        )
    };
    assert!(
        result.is_ok(),
        "symlink should report success after backend commit"
    );
    let created = result.expect("symlink should succeed after backend commit");

    let meta = fs::symlink_metadata(backend_path(&tmp, &created.backend_name)).unwrap();
    assert!(meta.file_type().is_symlink());
    assert_dirty_pending_view(&created.state);
}

#[test]
fn mknod_success_survives_post_commit_index_flush_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("mknod-target");

    let created = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        mknod_result(&fs, ROOT_INODE, OsStr::new(&name), libc::S_IFREG | 0o644, 0)
            .expect("mknod should succeed after backend commit")
    };

    let meta = fs::symlink_metadata(backend_path(&tmp, &created.backend_name)).unwrap();
    assert!(meta.file_type().is_file());
    assert_dirty_pending_view(&created.state);
}

#[test]
fn long_to_long_rename_does_not_run_invalid_post_commit_rollback() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("long-src");
    let dst = long_name("long-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let result = {
        let _hook = force_post_commit_flush_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
    };

    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_ok(),
        "post-commit rename should leave the destination name live"
    );
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_err(),
        "post-commit rename should not roll back to the source name"
    );
    assert_dirty_pending(&snapshot);
    let rename =
        result.expect("long-to-long rename should not fail after the backend rename has committed");
    assert!(
        rename.used_callback_path,
        "long-to-long rename regression must execute the shipped callback path"
    );
}

#[test]
fn rename_long_to_long_same_dir_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src = long_name("rename-same-clear-src");
    let dst = long_name("rename-same-clear-dst");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new(&src),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let src_thread = src.clone();
    let dst_thread = dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&src_thread),
            ROOT_INODE,
            OsStr::new(&dst_thread),
            0,
        )
    });

    assert!(pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).is_ok());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_err());
}

#[test]
fn rename_long_to_long_cross_dir_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("txn-src"), 0o755).unwrap();
    let dst_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("txn-dst"), 0o755).unwrap();
    let src = long_name("rename-cross-clear-src");
    let dst = long_name("rename-cross-clear-dst");
    let created = create_result(
        fs.as_ref(),
        src_dir.ino,
        OsStr::new(&src),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let src_thread = src.clone();
    let dst_thread = dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            src_dir.ino,
            OsStr::new(&src_thread),
            dst_dir.ino,
            OsStr::new(&dst_thread),
            0,
        )
    });

    assert!(pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), src_dir.ino, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), dst_dir.ino, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), dst_dir.ino, OsStr::new(&dst)).is_ok());
    assert!(lookup_entry_result(fs.as_ref(), src_dir.ino, OsStr::new(&src)).is_err());
}

#[test]
fn short_rename_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-rename-src"), b"payload").unwrap();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-rename-src"),
            ROOT_INODE,
            OsStr::new("short-rename-dst"),
            0,
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short rename should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-rename-src")).is_ok());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-rename-dst")).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-rename-dst")).is_ok());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-rename-src")).is_err());
}

#[test]
fn short_link_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-link-src"), b"payload").unwrap();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let source =
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-link-src")).unwrap();
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        link_result(
            fs_thread.as_ref(),
            source.ino,
            ROOT_INODE,
            OsStr::new("short-link-dst"),
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short hardlink creation should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-link-dst")).unwrap_err(),
        libc::ENOENT,
        "committed lookup must stay absent until txn clear completes"
    );

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-link-dst")).is_ok());
}

#[test]
fn short_unlink_txn_record_is_cleared_before_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-unlink-src"), b"payload").unwrap();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let handle = std::thread::spawn(move || {
        unlink_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new("short-unlink-src"),
        )
    });

    assert!(
        pause.wait_until_blocked_timeout(Duration::from_secs(1)),
        "short unlink should block at txn clear before reporting success"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-unlink-src")).is_ok());

    pause.release();
    handle.join().unwrap().unwrap();

    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-unlink-src")).is_err());
}

#[test]
fn mkdir_updates_attr_cache_and_emits_parent_and_child_notifications() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("mkdir-notify");

    readdir_names_result(&fs, ROOT_INODE).unwrap();
    assert!(
        !dir_cache_contains_result(&fs, OsStr::new("/"), OsStr::new(&name)).unwrap(),
        "prewarmed dir cache should not already contain the new child"
    );

    let recorder = TestNotifierRecorder::attach(&fs);

    let created = mkdir_result(&fs, ROOT_INODE, OsStr::new(&name), 0o755).unwrap();

    assert!(op_state_has_attr_cache_backend(
        &created.state,
        &created.backend_name
    ));
    assert!(dir_cache_contains_result(&fs, OsStr::new("/"), OsStr::new(&name)).unwrap());
    assert!(
        recorder.recorded_parent_invalidation(ROOT_INODE, OsStr::new(&name)),
        "mkdir should invalidate the parent entry"
    );
    assert!(
        recorder.recorded_inode_invalidation(created.ino),
        "mkdir should invalidate the child inode"
    );
}

fn assert_special_mknod_sync_unavailable_fails(mode: u32, name_prefix: &str) {
    for errno in [libc::EINVAL, libc::ENXIO, libc::EBADF] {
        let tmp = TempDir::new();
        let fs = new_always_sync_fs(&tmp);
        let name = format!("{name_prefix}-sync-fail-{errno}");

        let err = {
            let _hook = force_fsync_errno(errno);
            mknod_result(&fs, ROOT_INODE, OsStr::new(&name), mode, 0).unwrap_err()
        };

        assert!(
            special_node_sync_is_unavailable_errno(err),
            "special-node mknod should fail with an unsupported sync errno, got {err}"
        );
        assert_special_node_absent_after_failed_mknod(&tmp, &fs, &name);
    }
}

#[test]
fn fifo_mknod_fails_before_commit_when_object_sync_is_unavailable() {
    let _serial = lock_test_hooks();
    assert_special_mknod_sync_unavailable_fails(libc::S_IFIFO | 0o644, "fifo");
}

#[test]
fn unix_socket_mknod_fails_before_commit_when_object_sync_is_unavailable() {
    let _serial = lock_test_hooks();
    assert_special_mknod_sync_unavailable_fails(libc::S_IFSOCK | 0o644, "sock");
}

#[test]
fn fifo_mknod_respects_backend_durability_capability() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let recorder = TestNotifierRecorder::attach(&fs);

    match mknod_result(
        &fs,
        ROOT_INODE,
        OsStr::new("fifo"),
        libc::S_IFIFO | 0o644,
        0,
    ) {
        Ok(created) => {
            assert_eq!(created.attr.kind, FuserFileType::NamedPipe);
            assert!(
                tmp.path()
                    .join("fifo")
                    .symlink_metadata()
                    .unwrap()
                    .file_type()
                    .is_fifo()
            );
            assert!(
                recorder.recorded_parent_invalidation(ROOT_INODE, OsStr::new("fifo")),
                "fifo mknod should keep create-like parent invalidation behavior"
            );
            assert!(
                recorder.recorded_inode_invalidation(created.ino),
                "fifo mknod should keep create-like child invalidation behavior"
            );
        }
        Err(err) => {
            assert!(
                special_node_sync_is_unavailable_errno(err),
                "unsupported fifo durability should surface a backend sync errno, got {err}"
            );
            assert_special_node_absent_after_failed_mknod(&tmp, &fs, "fifo");
        }
    }
}

#[test]
fn unix_socket_mknod_respects_backend_durability_capability() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let recorder = TestNotifierRecorder::attach(&fs);

    match mknod_result(
        &fs,
        ROOT_INODE,
        OsStr::new("sock"),
        libc::S_IFSOCK | 0o644,
        0,
    ) {
        Ok(created) => {
            assert_eq!(created.attr.kind, FuserFileType::Socket);
            assert!(
                tmp.path()
                    .join("sock")
                    .symlink_metadata()
                    .unwrap()
                    .file_type()
                    .is_socket()
            );
            assert!(
                recorder.recorded_parent_invalidation(ROOT_INODE, OsStr::new("sock")),
                "unix socket mknod should keep create-like parent invalidation behavior"
            );
            assert!(
                recorder.recorded_inode_invalidation(created.ino),
                "unix socket mknod should keep create-like child invalidation behavior"
            );
        }
        Err(err) => {
            assert!(
                special_node_sync_is_unavailable_errno(err),
                "unsupported socket durability should surface a backend sync errno, got {err}"
            );
            assert_special_node_absent_after_failed_mknod(&tmp, &fs, "sock");
        }
    }
}
