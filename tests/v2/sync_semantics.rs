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

#[test]
fn poisoned_state_rejects_followup_write_and_fallocate_after_rollback_failure() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("poisoned-followup-src");
    let dst = long_name("poisoned-followup-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let opened = open_result(&fs, created.ino, libc::O_RDWR as u32).unwrap();

    {
        let _recovery = force_txn_recovery_errno(libc::EIO);
        let _sync = force_fsync_errno(libc::EIO);
        let err = rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
        .unwrap_err();
        assert_eq!(err, libc::EIO);
    }

    assert_eq!(
        write_result(&fs, created.ino, opened.fh, 0, b"blocked").unwrap_err(),
        libc::EIO
    );
    assert_eq!(
        fallocate_result(&fs, created.ino, opened.fh, 0, 4096, 0).unwrap_err(),
        libc::EIO
    );
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("poisoned-followup-create")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn rollback_failure_forces_eio_for_live_request() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("rollback-live-src");
    let dst = long_name("rollback-live-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = {
        let _recovery = force_txn_recovery_errno(libc::ENOSPC);
        let _sync = force_fsync_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("rollback-live-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn txn_clear_failure_after_live_rollback_forces_eio_and_poison() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("rollback-clear-src");
    let dst = long_name("rollback-clear-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = {
        let _sync = force_fsync_errno(libc::ENOSPC);
        let _clear = force_txn_clear_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_ok());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_err());
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("rollback-clear-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn poisoned_state_rejects_new_mutations_with_eio() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("poison-seed");
    mkdir_result(&fs, ROOT_INODE, OsStr::new("poison-dir"), 0o755).unwrap();
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = {
        let _recovery = force_txn_recovery_errno(libc::EIO);
        let _sync = force_fsync_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&name),
            ROOT_INODE,
            OsStr::new(&long_name("poison-dst")),
            0,
        )
        .unwrap_err()
    };
    assert_eq!(err, libc::EIO);

    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("poison-create")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
    assert_eq!(
        unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::EIO
    );
    assert_eq!(
        rmdir_result(&fs, ROOT_INODE, OsStr::new("poison-dir")).unwrap_err(),
        libc::EIO
    );
}

#[test]
fn queued_mutator_rechecks_poison_after_waiting_on_txn_lock() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src = long_name("queued-poison-src");
    let dst = long_name("queued-poison-dst");
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
    let fs_rename = std::sync::Arc::clone(&fs);
    let src_thread = src.clone();
    let dst_thread = dst.clone();
    let rename_handle = std::thread::spawn(move || {
        let _clear = force_txn_clear_errno(libc::EIO);
        rename_result(
            fs_rename.as_ref(),
            ROOT_INODE,
            OsStr::new(&src_thread),
            ROOT_INODE,
            OsStr::new(&dst_thread),
            0,
        )
    });

    assert!(pause.wait_until_blocked_timeout(std::time::Duration::from_secs(1)));

    let (queued_tx, queued_rx) = std::sync::mpsc::channel();
    let fs_create = std::sync::Arc::clone(&fs);
    let queued_name = long_name("queued-poison-create");
    let queued_name_thread = queued_name.clone();
    let queued_handle = std::thread::spawn(move || {
        let result = create_result(
            fs_create.as_ref(),
            ROOT_INODE,
            OsStr::new(&queued_name_thread),
            0o644,
            libc::O_RDWR,
        );
        let _ = queued_tx.send(result);
    });

    assert!(
        queued_rx
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_err(),
        "queued mutator should still be waiting on mutation_txn_lock while poisoning request is in flight"
    );

    pause.release();

    assert_eq!(rename_handle.join().unwrap().unwrap_err(), libc::EIO);
    let queued_err = queued_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .expect("queued mutator should finish once poisoning request releases the txn lock")
        .unwrap_err();
    queued_handle.join().unwrap();

    assert_eq!(queued_err, libc::EIO);
    assert!(
        tmp.path().join(".ln2_fs_txn").exists(),
        "poisoned mount should retain the surviving txn record"
    );
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&queued_name)).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn txn_creation_sync_failure_prevents_committed_state_mutation() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("txn-create-sync-failure");

    let err = {
        let _hook = force_txn_write_errno(libc::EIO);
        create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT,
        "logical name must remain absent when txn creation fails"
    );
    assert!(
        !readdir_names_result(&fs, ROOT_INODE)
            .unwrap()
            .iter()
            .any(|entry| entry == OsStr::new(&name)),
        "directory listing must not expose an uncommitted create after txn creation failure"
    );
    assert!(
        !tmp.path().join(".ln2_fs_txn").exists(),
        "failed txn creation must not leave a committed txn record behind"
    );
}

#[test]
fn short_create_txn_creation_sync_failure_prevents_committed_state_mutation() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);

    let err = {
        let _hook = force_txn_write_errno(libc::EIO);
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-create-sync-failure"),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-create-sync-failure")).unwrap_err(),
        libc::ENOENT,
        "logical name must remain absent when txn creation fails"
    );
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn short_mkdir_txn_creation_sync_failure_prevents_committed_state_mutation() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);

    let err = {
        let _hook = force_txn_write_errno(libc::EIO);
        mkdir_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-mkdir-sync-failure"),
            0o755,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-mkdir-sync-failure")).unwrap_err(),
        libc::ENOENT,
        "logical name must remain absent when txn creation fails"
    );
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn short_unlink_txn_clear_failure_after_live_rollback_forces_eio_and_poison() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-unlink-clear-fail"), b"payload").unwrap();
    let fs = new_always_sync_fs(&tmp);
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_txn_clear_errno(libc::EIO);
        unlink_result(&fs, ROOT_INODE, OsStr::new("short-unlink-clear-fail")).unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 1);
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-unlink-clear-fail")).is_ok());
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("short-unlink-clear-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn short_link_txn_clear_failure_after_live_rollback_forces_eio_and_poison() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-link-src"), b"payload").unwrap();
    let fs = new_always_sync_fs(&tmp);
    let source = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-link-src")).unwrap();
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_txn_clear_errno(libc::EIO);
        link_result(&fs, source.ino, ROOT_INODE, OsStr::new("short-link-dst")).unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 1);
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-link-dst")).unwrap_err(),
        libc::ENOENT
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("short-link-clear-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn short_rename_txn_clear_failure_after_live_rollback_forces_eio_and_poison() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-rename-src"), b"src").unwrap();
    let fs = new_always_sync_fs(&tmp);
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_txn_clear_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-rename-src"),
            ROOT_INODE,
            OsStr::new("short-rename-dst"),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 1);
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-rename-src")).is_ok());
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-rename-dst")).unwrap_err(),
        libc::ENOENT
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("short-rename-clear-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}

#[test]
fn short_rename_overwrite_txn_clear_failure_restores_both_entries() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-overwrite-src"), b"src").unwrap();
    fs::write(tmp.path().join("short-overwrite-dst"), b"dst").unwrap();
    let fs = new_always_sync_fs(&tmp);
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_txn_clear_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-overwrite-src"),
            ROOT_INODE,
            OsStr::new("short-overwrite-dst"),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 1);
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-overwrite-src")).is_ok());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-overwrite-dst")).is_ok());
    assert_eq!(
        fs::read(tmp.path().join("short-overwrite-src")).unwrap(),
        b"src"
    );
    assert_eq!(
        fs::read(tmp.path().join("short-overwrite-dst")).unwrap(),
        b"dst"
    );
    assert!(tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn object_sync_failure_rolls_back_same_dir_long_rename() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("sync-fail-src");
    let dst = long_name("sync-fail-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    let backend_path = tmp.path().join(OsStr::from_bytes(&created.backend_name));
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = {
        let _hook = force_fsync_errno(libc::EIO);
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&src),
            ROOT_INODE,
            OsStr::new(&dst),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_ok());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_err());
    assert_eq!(read_rawname_xattr(&backend_path), src.as_bytes());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn directory_sync_failure_rolls_back_cross_dir_long_rename() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("src"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("dst"), 0o755).unwrap();
    let src = long_name("dirsync-src");
    let dst = long_name("dirsync-dst");
    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    let backend_path = tmp
        .path()
        .join("src")
        .join(OsStr::from_bytes(&created.backend_name));
    release_result(&fs, created.ino, created.fh).unwrap();
    let before = common::test_fsync_call_count();

    let err = {
        let _hook = force_parent_dir_fsync_errno(libc::EIO);
        rename_result(
            &fs,
            src_dir.ino,
            OsStr::new(&src),
            dst_dir.ino,
            OsStr::new(&dst),
            0,
        )
        .unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(
        common::test_fsync_call_count(),
        before + 1,
        "directory-sync rollback coverage must complete the object-sync phase before parent-dir sync fails"
    );
    assert!(lookup_entry_result(&fs, src_dir.ino, OsStr::new(&src)).is_ok());
    assert!(lookup_entry_result(&fs, dst_dir.ino, OsStr::new(&dst)).is_err());
    assert!(backend_path.exists());
    assert_eq!(read_rawname_xattr(&backend_path), src.as_bytes());
    assert!(
        !tmp.path()
            .join("dst")
            .join(OsStr::from_bytes(&created.backend_name))
            .exists(),
        "rollback must restore the backend object out of the destination directory after the move"
    );
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn unlink_txn_clear_failure_after_quarantine_rename_forces_eio_and_poison() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("unlink-clear-fail");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_txn_clear_errno(libc::EIO);
        unlink_result(&fs, ROOT_INODE, OsStr::new(&name)).unwrap_err()
    };

    assert_eq!(err, libc::EIO);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 1);
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&name)).is_ok());
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(!fs::read_dir(tmp.path()).unwrap().any(|entry| {
        entry
            .unwrap()
            .file_name()
            .as_os_str()
            .as_bytes()
            .starts_with(b".ln2_fs_delobj_")
    }));
    assert_eq!(
        create_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_name("unlink-clear-poisoned")),
            0o644,
            libc::O_RDWR,
        )
        .unwrap_err(),
        libc::EIO
    );
}
