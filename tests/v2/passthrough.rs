use super::common::*;
use super::*;
use std::fs;
use std::os::unix::fs::PermissionsExt;

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_meta_policy_respects_budget_and_cooldown() {
    let policy = PassthroughMetaFdPolicy::new(PassthroughMetaFdConfig {
        enabled: true,
        max_meta_fds: 1,
        min_open_count: 0,
        min_lifetime: Duration::ZERO,
        min_meta_ops: 0,
        cooldown: Duration::from_secs(60),
    });

    assert!(policy.try_acquire_slot());
    assert!(!policy.try_acquire_slot());
    policy.release_slot();
    assert!(policy.try_acquire_slot());
    policy.enter_cooldown();
    assert!(!policy.try_acquire_slot());
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_meta_promotion_releases_slot_when_meta_fd_already_installed() {
    let policy = PassthroughMetaFdPolicy::new(PassthroughMetaFdConfig {
        enabled: true,
        max_meta_fds: 2,
        min_open_count: 0,
        min_lifetime: Duration::ZERO,
        min_meta_ops: 0,
        cooldown: Duration::ZERO,
    });
    let existing_fd = Arc::new(
        nix::fcntl::open(
            c"/dev/null",
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .unwrap(),
    );
    let meta_fd_slot = RwLock::new(Some(existing_fd.clone()));

    assert!(policy.try_acquire_slot());
    let promoted_fd = nix::fcntl::open(
        c"/dev/null",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();

    install_promoted_meta_fd(&policy, &meta_fd_slot, promoted_fd, true);

    assert!(Arc::ptr_eq(
        meta_fd_slot.read().as_ref().unwrap(),
        &existing_fd,
    ));
    assert_eq!(policy.meta_fd_count.load(Ordering::Relaxed), 0);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_create_uses_direct_replycreate_path() {
    let tmp = TempDir::new();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);

    let created =
        create_result(&fs, ROOT_INODE, OsStr::new("created"), 0o644, libc::O_RDWR).unwrap();
    let handle = passthrough_handle_result(&fs, created.fh).unwrap();

    assert!(created.passthrough);
    assert!(
        created.used_passthrough_create_reply,
        "passthrough create must use the direct ReplyCreate passthrough branch"
    );
    assert!(
        created.reply_open_backing_called,
        "production-level create proof must observe reply.open_backing(...)"
    );
    assert!(
        created.reply_created_passthrough_called,
        "production-level create proof must observe reply.created_passthrough(...)"
    );
    assert!(handle.test_data_fd_raw() >= 0);
    assert_eq!(handle.test_open_flags(), libc::O_RDWR as u32);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_release_closes_only_released_handle_data_fd() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened1 = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();
    let opened2 = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();
    let handle1 = passthrough_handle_result(&fs, opened1.fh).unwrap();
    let handle2 = passthrough_handle_result(&fs, opened2.fh).unwrap();

    assert!(opened1.passthrough);
    assert!(opened2.passthrough);
    assert_eq!(
        handle1.test_backing_identity(),
        handle2.test_backing_identity(),
        "second open should reuse the shared backing registration path"
    );
    assert_ne!(handle1.test_data_fd_raw(), handle2.test_data_fd_raw());

    release_result(&fs, entry.ino, opened1.fh).unwrap();
    fs::rename(tmp.path().join("file"), tmp.path().join("renamed")).unwrap();
    let attr = getattr_result(&fs, entry.ino, Some(opened2.fh)).unwrap();

    assert_eq!(attr.size, 5);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_open_creates_handle_with_stable_data_fd() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();
    let handle = passthrough_handle_result(&fs, opened.fh).unwrap();

    assert!(opened.passthrough);
    assert!(handle.test_data_fd_raw() >= 0);
    assert_eq!(handle.test_open_flags(), libc::O_RDONLY as u32);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_create_creates_handle_with_stable_data_fd() {
    let tmp = TempDir::new();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);

    let created =
        create_result(&fs, ROOT_INODE, OsStr::new("created"), 0o644, libc::O_RDWR).unwrap();
    let handle = passthrough_handle_result(&fs, created.fh).unwrap();

    assert!(created.passthrough);
    assert!(created.used_passthrough_create_reply);
    assert!(created.reply_open_backing_called);
    assert!(created.reply_created_passthrough_called);
    assert!(handle.test_data_fd_raw() >= 0);
    assert_eq!(handle.test_open_flags(), libc::O_RDWR as u32);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_create_open_backing_eperm_falls_back_without_partial_state() {
    let tmp = TempDir::new();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);

    let created = create_result_with_open_backing_errno(
        &fs,
        ROOT_INODE,
        OsStr::new("created"),
        0o644,
        libc::O_RDWR,
        libc::EPERM,
    )
    .unwrap();

    assert!(!created.passthrough);
    assert!(!created.used_passthrough_create_reply);
    assert!(created.reply_open_backing_called);
    assert!(!created.reply_created_passthrough_called);
    assert!(passthrough_handle_result(&fs, created.fh).is_none());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("created")).is_ok());
    assert!(
        !passthrough_runtime_enabled(&fs),
        "EPERM open_backing failure must disable passthrough runtime"
    );
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_create_open_backing_eopnotsupp_falls_back_without_partial_state() {
    let tmp = TempDir::new();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);

    let created = create_result_with_open_backing_errno(
        &fs,
        ROOT_INODE,
        OsStr::new("created"),
        0o644,
        libc::O_RDWR,
        libc::EOPNOTSUPP,
    )
    .unwrap();

    assert!(!created.passthrough);
    assert!(!created.used_passthrough_create_reply);
    assert!(created.reply_open_backing_called);
    assert!(!created.reply_created_passthrough_called);
    assert!(passthrough_handle_result(&fs, created.fh).is_none());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("created")).is_ok());
    assert!(
        !passthrough_runtime_enabled(&fs),
        "EOPNOTSUPP open_backing failure must disable passthrough runtime"
    );
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_create_open_backing_enotty_falls_back_without_partial_state() {
    let tmp = TempDir::new();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);

    let created = create_result_with_open_backing_errno(
        &fs,
        ROOT_INODE,
        OsStr::new("created"),
        0o644,
        libc::O_RDWR,
        libc::ENOTTY,
    )
    .unwrap();

    assert!(!created.passthrough);
    assert!(!created.used_passthrough_create_reply);
    assert!(created.reply_open_backing_called);
    assert!(!created.reply_created_passthrough_called);
    assert!(passthrough_handle_result(&fs, created.fh).is_none());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new("created")).is_ok());
    assert!(
        !passthrough_runtime_enabled(&fs),
        "ENOTTY open_backing failure must disable passthrough runtime"
    );
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_follow_up_ops_do_not_widen_access_mode() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();

    assert!(opened.passthrough);

    let err = write_result(&fs, entry.ino, opened.fh, 0, b"x").unwrap_err();

    assert_eq!(err, libc::EBADF);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_fallback_read_and_write_use_stored_data_fd() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);
    fs::rename(tmp.path().join("file"), tmp.path().join("renamed")).unwrap();
    fs::remove_file(tmp.path().join("renamed")).unwrap();

    let read = read_result(&fs, entry.ino, opened.fh, 0, 5).unwrap();
    let write = write_result(&fs, entry.ino, opened.fh, 0, b"abc").unwrap();

    assert_eq!(read.data, b"hello");
    assert_eq!(write.size, 3);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_read_pins_handle_across_release_race() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();

    assert!(opened.passthrough);

    let _race = force_passthrough_release_after_check(opened.fh);
    let read = read_result(&fs, entry.ino, opened.fh, 0, 5).unwrap();

    assert_eq!(read.data, b"hello");
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_write_pins_handle_across_release_race() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);

    let _race = force_passthrough_release_after_check(opened.fh);
    let write = write_result(&fs, entry.ino, opened.fh, 0, b"abc").unwrap();

    assert_eq!(write.size, 3);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_getattr_with_fh_survives_rename_or_unlink() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDONLY as u32).unwrap();

    assert!(opened.passthrough);
    fs::rename(tmp.path().join("file"), tmp.path().join("renamed")).unwrap();
    fs::remove_file(tmp.path().join("renamed")).unwrap();

    let attr = getattr_result(&fs, entry.ino, Some(opened.fh)).unwrap();
    assert_eq!(attr.size, 5);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_setattr_size_with_fh_survives_rename_or_unlink() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    assert!(opened.passthrough);
    fs::rename(tmp.path().join("file"), tmp.path().join("renamed")).unwrap();
    fs::remove_file(tmp.path().join("renamed")).unwrap();

    let attr = setattr_size_result(&fs, entry.ino, Some(opened.fh), 1).unwrap();
    let after = getattr_result(&fs, entry.ino, Some(opened.fh)).unwrap();

    assert_eq!(attr.size, 1);
    assert_eq!(after.size, 1);
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_fallocate_with_fh_survives_rename_or_unlink() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    assert!(opened.passthrough);
    fs::rename(tmp.path().join("file"), tmp.path().join("renamed")).unwrap();
    fs::remove_file(tmp.path().join("renamed")).unwrap();

    fallocate_result(&fs, entry.ino, opened.fh, 0, 16, 0).unwrap();
    let attr = getattr_result(&fs, entry.ino, Some(opened.fh)).unwrap();

    assert!(attr.size >= 16);
}

#[cfg(all(feature = "abi-7-40", target_os = "linux"))]
#[test]
fn passthrough_fallocate_pins_handle_across_release_race() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_RDWR as u32).unwrap();

    assert!(opened.passthrough);

    let _race = force_passthrough_release_after_check(opened.fh);
    fallocate_result(
        &fs,
        entry.ino,
        opened.fh,
        0,
        4096,
        libc::FALLOC_FL_KEEP_SIZE,
    )
    .unwrap();
}

#[cfg(feature = "abi-7-40")]
#[test]
fn passthrough_setattr_size_with_fh_preserves_metadata_updates() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, true);
    set_passthrough_runtime(&fs, true);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("file")).unwrap();

    let opened = open_result(&fs, entry.ino, libc::O_WRONLY as u32).unwrap();

    assert!(opened.passthrough);

    let attr = fs
        .test_setattr_size_and_mode(entry.ino, opened.fh, 1, 0o600)
        .unwrap();
    let mode = fs::metadata(tmp.path().join("file"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;

    assert_eq!(attr.size, 1);
    assert_eq!(mode, 0o600);
}
