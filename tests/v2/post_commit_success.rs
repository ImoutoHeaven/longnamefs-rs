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
fn always_sync_overlapping_same_directory_mutations_wait_for_follow_up_flush() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let first_name = long_name("overlap-first");
    let second_name = long_name("overlap-second");
    let mut pause = pause_next_post_commit_flush();

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

#[test]
fn fifo_mknod_remains_implemented_and_tested() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let recorder = TestNotifierRecorder::attach(&fs);

    let created = mknod_result(
        &fs,
        ROOT_INODE,
        OsStr::new("fifo"),
        libc::S_IFIFO | 0o644,
        0,
    )
    .expect("fifo mknod should remain implemented");

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

#[test]
fn unix_socket_mknod_remains_implemented_and_tested() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let recorder = TestNotifierRecorder::attach(&fs);

    let created = mknod_result(
        &fs,
        ROOT_INODE,
        OsStr::new("sock"),
        libc::S_IFSOCK | 0o644,
        0,
    )
    .expect("unix socket mknod should remain implemented");

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
