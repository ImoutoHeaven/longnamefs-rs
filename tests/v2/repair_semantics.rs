use super::common::*;
use super::*;
use crate::v2::index::{INDEX_NAME, JOURNAL_NAME, JournalOp, append_to_journal, read_dir_index};
use crate::v2::path::{MAX_SEGMENT_ON_DISK, backend_basename_from_hash};
use std::fs;

#[test]
fn dup_cloexec_preserves_cloexec_semantics() {
    let tmp = TempDir::new();
    let file = tmp.path().join("f");
    fs::write(&file, b"x").unwrap();
    let fd = nix::fcntl::open(&file, OFlag::O_RDONLY | OFlag::O_CLOEXEC, Mode::empty()).unwrap();

    let duped = dup_cloexec(fd.as_fd()).unwrap();
    let flags = nix::fcntl::fcntl(duped.as_fd(), nix::fcntl::FcntlArg::F_GETFD).unwrap();
    assert_ne!(flags & libc::FD_CLOEXEC, 0);
}

#[test]
fn set_internal_rawname_at_sets_xattr_on_regular_file() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new("xattr-probe").unwrap();
    let _file_fd = nix::fcntl::openat(
        dir_fd.as_fd(),
        name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();

    let raw = b"hello-xattr".to_vec();
    if let Err(err) = set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw) {
        if let CoreError::Io(ref ioe) = err
            && matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
            )
        {
            return;
        }
        panic!("set_internal_rawname_at failed: {err:?}");
    }

    let fd = openat_nofollow_for_xattr(dir_fd.as_fd(), name.as_c_str()).unwrap();
    let got = get_internal_rawname(fd.as_fd()).unwrap();
    assert_eq!(got, raw);
}

#[test]
fn raw_fd_for_xattr_write_intent_allows_directory() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().to_path_buf(), false, false).unwrap();
    let core = LongNameFsCore::new(
        config,
        MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(1)),
        IndexSync::Off,
    )
    .unwrap();

    let res = xattr_target_for_path(&core, OsStr::new("/"), true);
    assert!(
        res.is_ok(),
        "expected write_intent dir open to succeed: {res:?}"
    );
}

#[test]
fn symlink_xattr_uses_procfs_lxattr_fallback() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().to_path_buf(), false, false).unwrap();
    let core = match LongNameFsCore::new(
        config,
        MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(1)),
        IndexSync::Off,
    ) {
        Ok(core) => core,
        Err(CoreError::Io(ioe))
            if matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
            ) =>
        {
            return;
        }
        Err(err) => panic!("LongNameFsCore::new failed: {err:?}"),
    };
    let root_fd = core.cached_root_fd().unwrap();
    symlinkat("target", root_fd.as_fd(), c"link").unwrap();

    let target = xattr_target_for_path(&core, OsStr::new("/link"), true).unwrap();
    assert!(
        matches!(target, XattrTarget::ProcPath(_)),
        "expected symlink no-follow to use procfs fallback"
    );

    let xname = CString::new("user.task3.symlink").unwrap();
    let value = b"task3-value";

    if let Err(CoreError::Io(ioe)) = xattr_set(&target, xname.as_c_str(), value, 0) {
        if matches!(
            ioe.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
        ) {
            return;
        }
        panic!("xattr_set failed: {ioe:?}");
    }

    let sz = xattr_get_size(&target, xname.as_c_str()).unwrap();
    let mut got = vec![0u8; sz];
    let read_len = xattr_get_into(&target, xname.as_c_str(), &mut got).unwrap();
    got.truncate(read_len);
    assert_eq!(got, value);

    let list_len = xattr_list_size(&target).unwrap();
    let mut list_buf = vec![0u8; list_len];
    let list_read = xattr_list_into(&target, &mut list_buf).unwrap();
    list_buf.truncate(list_read);
    assert!(
        list_buf
            .windows(xname.as_bytes_with_nul().len())
            .any(|w| w == xname.as_bytes_with_nul()),
        "expected listxattr to include key"
    );

    xattr_remove(&target, xname.as_c_str()).unwrap();
    let err = xattr_get_size(&target, xname.as_c_str()).unwrap_err();
    if let CoreError::Io(ioe) = err {
        assert_eq!(
            ioe.raw_os_error(),
            Some(libc::ENODATA),
            "unexpected errno after removexattr"
        );
    } else {
        panic!("unexpected error type after removexattr: {err:?}");
    }
}

#[test]
fn symlink_xattr_procfs_fallback_stable_without_dir_fd_cache() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("sub")).unwrap();
    let config = Config::open_backend(tmp.path().to_path_buf(), false, false).unwrap();
    let core = match LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off) {
        Ok(core) => core,
        Err(CoreError::Io(ioe))
            if matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
            ) =>
        {
            return;
        }
        Err(err) => panic!("LongNameFsCore::new failed: {err:?}"),
    };

    let sub_fd = nix::fcntl::open(
        tmp.path().join("sub").as_path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    symlinkat("target", sub_fd.as_fd(), c"link").unwrap();

    let target = xattr_target_for_path(&core, OsStr::new("/sub/link"), true).unwrap();
    assert!(
        matches!(target, XattrTarget::ProcPath(_)),
        "expected symlink no-follow to use procfs fallback"
    );

    let xname = CString::new("user.task3.symlink.no_cache").unwrap();
    let value = b"task3-no-cache";

    if let Err(CoreError::Io(ioe)) = xattr_set(&target, xname.as_c_str(), value, 0) {
        if matches!(
            ioe.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
        ) {
            return;
        }
        panic!("xattr_set failed: {ioe:?}");
    }

    let sz = xattr_get_size(&target, xname.as_c_str()).unwrap();
    let mut got = vec![0u8; sz];
    let read_len = xattr_get_into(&target, xname.as_c_str(), &mut got).unwrap();
    got.truncate(read_len);
    assert_eq!(got, value);

    let list_len = xattr_list_size(&target).unwrap();
    let mut list_buf = vec![0u8; list_len];
    let list_read = xattr_list_into(&target, &mut list_buf).unwrap();
    list_buf.truncate(list_read);
    assert!(
        list_buf
            .windows(xname.as_bytes_with_nul().len())
            .any(|w| w == xname.as_bytes_with_nul()),
        "expected listxattr to include key"
    );

    xattr_remove(&target, xname.as_c_str()).unwrap();
    let err = xattr_get_size(&target, xname.as_c_str()).unwrap_err();
    if let CoreError::Io(ioe) = err {
        assert_eq!(
            ioe.raw_os_error(),
            Some(libc::ENODATA),
            "unexpected errno after removexattr"
        );
    } else {
        panic!("unexpected error type after removexattr: {err:?}");
    }
}

#[test]
fn set_internal_rawname_at_supports_symlink_via_procfs() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new(".__ln2_symlink").unwrap();
    symlinkat("target", dir_fd.as_fd(), name.as_c_str()).unwrap();

    let raw = b"rawname-symlink".to_vec();
    let res = set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw);
    if let Err(CoreError::Io(ref ioe)) = res
        && matches!(
            ioe.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM) | Some(libc::ELOOP)
        )
    {
        return;
    }
    res.unwrap();

    let got = get_internal_rawname_at(dir_fd.as_fd(), name.as_c_str()).unwrap();
    assert_eq!(got, raw);
}

#[test]
fn set_internal_rawname_at_uses_procfs_when_openat_eloop() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new(".__ln2_symlink_eloop").unwrap();
    symlinkat("target", dir_fd.as_fd(), name.as_c_str()).unwrap();

    PROCFS_SYMLINK_FALLBACK_USED.store(false, Ordering::Relaxed);
    let raw = b"rawname-eloop".to_vec();
    let res = set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw);
    if let Err(CoreError::Io(ref ioe)) = res
        && matches!(
            ioe.raw_os_error(),
            Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM) | Some(libc::ELOOP)
        )
    {
        assert!(
            PROCFS_SYMLINK_FALLBACK_USED.load(Ordering::Relaxed),
            "expected procfs fallback on openat ELOOP"
        );
        return;
    }

    assert!(
        PROCFS_SYMLINK_FALLBACK_USED.load(Ordering::Relaxed),
        "expected procfs fallback on openat ELOOP"
    );
}

#[test]
fn procfs_unavailable_error_detection() {
    let missing = CoreError::Io(io::Error::from_raw_os_error(libc::ENOENT));
    let denied = CoreError::Io(io::Error::from_raw_os_error(libc::EACCES));
    let forbidden = CoreError::Io(io::Error::from_raw_os_error(libc::EPERM));
    let other = CoreError::Io(io::Error::from_raw_os_error(libc::EIO));

    let procfs_exists = Path::new("/proc/self/fd").exists();
    assert_eq!(is_procfs_unavailable(&missing), !procfs_exists);
    assert!(!is_procfs_unavailable(&denied));
    assert!(!is_procfs_unavailable(&forbidden));
    assert!(!is_procfs_unavailable(&other));
}

#[test]
fn procfs_fallback_maps_unavailable_to_original_errno() {
    assert_eq!(
        normalize_procfs_fallback_errno(libc::ENOENT, libc::ELOOP, false),
        libc::ELOOP
    );
    assert_eq!(
        normalize_procfs_fallback_errno(libc::ENOTDIR, libc::EBADF, false),
        libc::EBADF
    );
}

#[test]
fn procfs_fallback_keeps_real_errno_when_procfs_available() {
    assert_eq!(
        normalize_procfs_fallback_errno(libc::ENOENT, libc::ELOOP, true),
        libc::ENOENT
    );
    assert_eq!(
        normalize_procfs_fallback_errno(libc::ENOTDIR, libc::EBADF, true),
        libc::ENOTDIR
    );
    assert_eq!(
        normalize_procfs_fallback_errno(libc::EACCES, libc::ELOOP, false),
        libc::EACCES
    );
}

#[test]
fn ttl_for_open_count_prefers_open_ttl_only_for_open_files() {
    let base = Duration::from_secs(1);
    let open = Duration::from_millis(50);
    assert_eq!(
        LongNameFsV2Fuser::ttl_for_open_count(base, open, true, 0),
        base
    );
    assert_eq!(
        LongNameFsV2Fuser::ttl_for_open_count(base, open, true, 1),
        open
    );
    assert_eq!(
        LongNameFsV2Fuser::ttl_for_open_count(base, open, false, 1),
        base
    );
}

#[test]
fn getattr_via_parent_dirfd_uses_cached_parent_dir() {
    let tmp = TempDir::new();
    let file = tmp.path().join("a");
    fs::write(&file, b"hello").unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let fs = LongNameFsV2Fuser::new(
        config,
        MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        false,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap();

    let root_fd = fs.core.cached_root_fd().unwrap();
    let stat = fstatat(root_fd.as_fd(), c"a", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let child = fs.ensure_child_entry(ROOT_INODE, OsStr::new("a"), b"a".to_vec(), stat, 1);
    let attr = fs.getattr_via_parent_dirfd(&child).unwrap();
    assert_eq!(attr.ino, FuserInodeNo(child.ino));
    assert_eq!(attr.kind, FuserFileType::RegularFile);
}

#[test]
fn readdirplus_materialization_increments_lookup_count() {
    let tmp = TempDir::new();
    let file = tmp.path().join("a");
    fs::write(&file, b"hello").unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let fs = LongNameFsV2Fuser::new(
        config,
        MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        false,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap();

    let root_fd = fs.core.cached_root_fd().unwrap();
    let stat = fstatat(root_fd.as_fd(), c"a", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let info = DirEntryInfo {
        name: OsString::from("a"),
        kind: core_file_type_from_mode(stat.st_mode),
        attr: Some(core_attr_from_stat(&stat)),
        backend_name: b"a".to_vec(),
        backend_key: Some(backend_key_from_stat(&stat)),
    };

    let first = fs.materialize_readdirplus_child(
        ROOT_INODE,
        &info,
        core_attr_from_stat(&stat),
        backend_key_from_stat(&stat),
    );
    assert_eq!(first.lookup_count, 1);

    let second = fs.materialize_readdirplus_child(
        ROOT_INODE,
        &info,
        core_attr_from_stat(&stat),
        backend_key_from_stat(&stat),
    );
    assert_eq!(second.ino, first.ino);
    assert_eq!(second.lookup_count, 2);
}

#[test]
fn readdirplus_non_emitted_child_rolls_back_lookup_count_on_full_add() {
    let tmp = TempDir::new();
    let file = tmp.path().join("a");
    fs::write(&file, b"hello").unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let fs = LongNameFsV2Fuser::new(
        config,
        MAX_SEGMENT_ON_DISK,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        false,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap();

    let root_fd = fs.core.cached_root_fd().unwrap();
    let stat = fstatat(root_fd.as_fd(), c"a", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let backend = backend_key_from_stat(&stat);
    let info = DirEntryInfo {
        name: OsString::from("a"),
        kind: core_file_type_from_mode(stat.st_mode),
        attr: Some(core_attr_from_stat(&stat)),
        backend_name: b"a".to_vec(),
        backend_key: Some(backend),
    };

    let (emitted_ino, full) = fs.emit_readdirplus_child_with_lookup(
        ROOT_INODE,
        &info,
        core_attr_from_stat(&stat),
        backend,
        3,
        |_child_ino, _next, _name, _entry_ttl, _attr| true,
    );
    assert!(full);

    assert!(
        fs.inode_store.get(emitted_ino).is_none(),
        "rolled-back non-emitted child must not keep lookup refs"
    );
}

#[test]
fn root_setattr_size_errno_is_eisdir() {
    assert_eq!(root_setattr_size_errno(Some(1)), Some(libc::EISDIR));
    assert_eq!(root_setattr_size_errno(None), None);
}

#[test]
fn rmdir_meta_cleanup_allows_only_ln2_meta() {
    let tmp = TempDir::new();
    let child = tmp.path().join("d");
    fs::create_dir(&child).unwrap();
    fs::write(child.join(INDEX_NAME), b"idx").unwrap();
    fs::write(child.join(JOURNAL_NAME), b"jnl").unwrap();
    fs::write(child.join(format!("{INDEX_NAME}.tmp.1.2.idx")), b"tmp").unwrap();
    fs::write(child.join(".ln2_fs_renameat2_probe.tmp.1.2.rn2"), b"rn2").unwrap();
    fs::write(
        child.join(".ln2_fs_renameat2_probe.tmp.1.2.rn2.dst"),
        b"rn2dst",
    )
    .unwrap();
    fs::write(child.join(XATTR_CHECK_NAME), b"xattr").unwrap();
    fs::create_dir(child.join(".ln2_fs_ctmp_deadbeef")).unwrap();
    fs::write(child.join(".ln2_fs_rtmp_deadbeef"), b"x").unwrap();

    let dfd =
        nix::fcntl::open(&child, OFlag::O_RDONLY | OFlag::O_DIRECTORY, Mode::empty()).unwrap();
    assert!(dir_is_only_fs_internal_files(dfd.as_fd()).unwrap());
    best_effort_unlink_fs_internal_files(dfd.as_fd());

    let entries: Vec<_> = fs::read_dir(&child).unwrap().collect();
    assert!(entries.is_empty());
}

#[test]
fn dir_is_only_fs_internal_files_rejects_other_hidden_files() {
    let tmp = TempDir::new();
    let child = tmp.path().join("d");
    fs::create_dir(&child).unwrap();
    fs::write(child.join(".keep"), b"x").unwrap();

    let dfd =
        nix::fcntl::open(&child, OFlag::O_RDONLY | OFlag::O_DIRECTORY, Mode::empty()).unwrap();
    assert!(!dir_is_only_fs_internal_files(dfd.as_fd()).unwrap());
}

#[test]
fn journal_only_load_persists_base_index_once() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();

    let backend_name = b".__ln2_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec();
    let raw_name = b"hello-world".to_vec();
    append_to_journal(
        config.backend_fd(),
        &[JournalOp::Upsert(backend_name.clone(), raw_name.clone())],
        true,
    )
    .unwrap();
    assert!(tmp.path().join(JOURNAL_NAME).exists());
    assert!(!tmp.path().join(INDEX_NAME).exists());

    let core = LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off).unwrap();
    let _ = core.resolve_dir(OsStr::new("/")).unwrap();

    assert!(tmp.path().join(INDEX_NAME).exists());
    assert!(!tmp.path().join(JOURNAL_NAME).exists());

    let loaded = read_dir_index(core.config.backend_fd())
        .unwrap()
        .expect("index should load after persist");
    assert!(loaded.has_base_index);
    assert!(loaded.index.contains_key(&backend_name));
}

#[test]
fn rebuild_dir_index_recovers_rawname_when_opath_xattr_fails() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new(".__ln2_opath_probe").unwrap();
    let _file_fd = nix::fcntl::openat(
        dir_fd.as_fd(),
        name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();

    let raw = b"opath-probe-rawname".to_vec();
    set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw).unwrap();

    if !opath_rawname_ebadf(dir_fd.as_fd(), name.as_c_str()) {
        return;
    }

    OPATH_XATTR_WARNED.store(false, Ordering::Relaxed);
    let (index, stderr) =
        capture_stderr(|| rebuild_dir_index_from_backend(dir_fd.as_fd()).unwrap());
    let entry = index
        .get(name.as_bytes())
        .expect("index entry should exist");
    assert_eq!(entry.raw_name.as_ref(), raw.as_slice());
    assert!(OPATH_XATTR_WARNED.load(Ordering::Relaxed));
    if !stderr.is_empty() {
        let stderr_text = String::from_utf8_lossy(&stderr);
        assert!(stderr_text.contains("O_PATH fgetxattr EBADF"));
    }
}

#[test]
fn parallel_rebuild_worker_dup_fd_sets_cloexec() {
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();

    let dup_fd = dup_rebuild_worker_fd(dir_fd.as_fd()).unwrap();
    let fd_flags = nix::fcntl::fcntl(dup_fd.as_fd(), nix::fcntl::FcntlArg::F_GETFD).unwrap();
    assert_ne!(fd_flags & libc::FD_CLOEXEC, 0);
}

fn assert_waits_for_internal_rawname_hook_lock(test_fn: fn()) {
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _serial = lock_test_hooks();
        let _hook = force_internal_rawname_errno(libc::EIO);
        ready_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    ready_rx.recv().unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        test_fn();
    });
    started_rx.recv().unwrap();

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while std::time::Instant::now() < deadline && !worker.is_finished() {
        std::thread::yield_now();
    }
    assert!(
        !worker.is_finished(),
        "test must wait for the rawname hook lock while the hook is active"
    );

    release_tx.send(()).unwrap();
    blocker.join().unwrap();
    worker.join().unwrap();
}

#[test]
fn rebuild_dir_index_parallel_path_waits_for_rawname_hook_lock() {
    assert_waits_for_internal_rawname_hook_lock(
        rebuild_dir_index_parallel_path_uses_worker_dup_helper,
    );
}

#[test]
fn rebuild_dir_index_sequential_fallback_waits_for_rawname_hook_lock() {
    assert_waits_for_internal_rawname_hook_lock(
        rebuild_dir_index_parallel_path_falls_back_when_all_worker_dups_fail,
    );
}

#[test]
fn rebuild_dir_index_parallel_path_uses_worker_dup_helper() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();

    let mut expected = HashMap::new();
    for i in 0..(PARALLEL_REBUILD_THRESHOLD + 1) {
        let name = CString::new(format!(".__ln2_parallel_dup_{i:03}")).unwrap();
        let _fd = nix::fcntl::openat(
            dir_fd.as_fd(),
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap();
        let raw = format!("raw-parallel-{i}").into_bytes();
        if let Err(CoreError::Io(ioe)) =
            set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw)
        {
            if matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
            ) {
                return;
            }
            panic!("set_internal_rawname_at failed: {ioe:?}");
        }
        expected.insert(name.as_bytes().to_vec(), raw);
    }

    reset_parallel_rebuild_dup_helper_calls();
    let index = rebuild_dir_index_from_backend(dir_fd.as_fd()).unwrap();
    assert!(
        parallel_rebuild_dup_helper_calls() > 0,
        "parallel rebuild must call dup helper at worker call-site"
    );

    let probe = b".__ln2_parallel_dup_000".to_vec();
    let entry = index
        .get(&probe)
        .expect("parallel rebuild should index probe entry");
    assert_eq!(
        entry.raw_name.as_ref(),
        expected.get(&probe).unwrap().as_slice()
    );
}

#[test]
fn rebuild_dir_index_parallel_path_falls_back_when_all_worker_dups_fail() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();

    let mut expected = HashMap::new();
    for i in 0..(PARALLEL_REBUILD_THRESHOLD + 1) {
        let name = CString::new(format!(".__ln2_parallel_fallback_{i:03}")).unwrap();
        let _fd = nix::fcntl::openat(
            dir_fd.as_fd(),
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap();
        let raw = format!("raw-fallback-{i}").into_bytes();
        if let Err(CoreError::Io(ioe)) =
            set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw)
        {
            if matches!(
                ioe.raw_os_error(),
                Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EPERM)
            ) {
                return;
            }
            panic!("set_internal_rawname_at failed: {ioe:?}");
        }
        expected.insert(name.as_bytes().to_vec(), raw);
    }

    reset_parallel_rebuild_dup_helper_calls();
    let _fail_guard = force_parallel_rebuild_dup_fail();
    let index = rebuild_dir_index_from_backend(dir_fd.as_fd()).unwrap();

    assert!(
        parallel_rebuild_dup_helper_calls() > 0,
        "test setup should exercise dup helper attempts"
    );
    let probe = b".__ln2_parallel_fallback_000".to_vec();
    let entry = index
        .get(&probe)
        .expect("sequential fallback should still index probe entry");
    assert_eq!(
        entry.raw_name.as_ref(),
        expected.get(&probe).unwrap().as_slice()
    );
}

#[test]
fn malformed_rawname_is_counted_as_recoverable_entry_anomaly() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let core = new_longname_test_core(&tmp, false);
    let root_fd = core.cached_root_fd().unwrap();
    let logical = b"malformed";
    let malformed_raw = b".__ln2_reserved-logical".to_vec();
    let long_name =
        CString::new(backend_basename_from_hash(&encode_long_name(logical), None)).unwrap();
    let _fd = nix::fcntl::openat(
        root_fd.as_fd(),
        long_name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    set_internal_rawname_at(root_fd.as_fd(), long_name.as_c_str(), &malformed_raw).unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let names = readdir_names_result(&fs, ROOT_INODE).unwrap();
    let anomalies = take_repair_anomalies_result(&fs);
    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();

    assert!(
        anomalies.iter().any(|record| {
            record.backend_name == long_name.as_bytes()
                && record.kind == TestRepairAnomalyKind::MalformedRawname
        }),
        "recoverable malformed entry should be recorded as an anomaly"
    );
    assert!(
        !names
            .iter()
            .any(|name| name.as_os_str().as_bytes() == malformed_raw.as_slice()),
        "recoverable malformed entry should be skipped, not materialized"
    );
    assert!(
        snapshot.dirty,
        "recoverable anomaly should leave directory state dirty"
    );
    assert!(
        snapshot.pending > 0,
        "recoverable anomaly should leave pending repair work"
    );
}

#[test]
fn missing_rawname_xattr_is_counted_as_recoverable_entry_anomaly() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let core = new_longname_test_core(&tmp, false);
    let root_fd = core.cached_root_fd().unwrap();
    let logical = b"missing-xattr";
    let long_name =
        CString::new(backend_basename_from_hash(&encode_long_name(logical), None)).unwrap();
    let _fd = nix::fcntl::openat(
        root_fd.as_fd(),
        long_name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let names = readdir_names_result(&fs, ROOT_INODE).unwrap();
    let anomalies = take_repair_anomalies_result(&fs);
    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();

    assert!(
        anomalies.iter().any(|record| {
            record.backend_name == long_name.as_bytes()
                && record.kind == TestRepairAnomalyKind::MissingRawnameXattr
        }),
        "missing rawname xattr should be recorded as a recoverable anomaly"
    );
    assert!(
        !names
            .iter()
            .any(|name| name.as_os_str().as_bytes() == logical),
        "missing-xattr anomaly should be skipped, not materialized"
    );
    assert!(
        snapshot.dirty,
        "recoverable anomaly should leave directory state dirty"
    );
    assert!(
        snapshot.pending > 0,
        "recoverable anomaly should leave pending repair work"
    );
}

#[test]
fn missing_rawname_xattr_readdir_waits_for_rawname_hook_lock() {
    assert_waits_for_internal_rawname_hook_lock(
        missing_rawname_xattr_is_counted_as_recoverable_entry_anomaly,
    );
}

#[test]
fn iterator_failure_is_request_fatal() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, false);

    let _hook = force_list_iter_skip_errno(libc::EIO);
    let result = readdir_names_result(&fs, ROOT_INODE);

    assert!(result.is_err(), "request should fail on iterator error");
}

#[test]
fn ebadf_without_fallback_is_request_fatal() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, false);

    let _hook = force_list_iter_skip_errno(libc::EBADF);
    let result = readdir_names_result(&fs, ROOT_INODE);

    assert!(result.is_err(), "bad dirfd should be request-fatal");
}

#[test]
fn eio_is_request_fatal_and_errno_is_preserved() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let fs = new_test_fs(&tmp, false, false);

    let _hook = force_list_iter_skip_errno(libc::EIO);
    let err = readdir_names_result(&fs, ROOT_INODE).unwrap_err();

    assert_eq!(err, libc::EIO);
}

#[test]
fn lookup_long_name_fallback_rawname_read_failure_preserves_errno() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let _snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();

    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let logical = vec![b'l'; MAX_SEGMENT_ON_DISK + 1];
    let backend = CString::new(backend_basename_from_hash(
        &encode_long_name(&logical),
        None,
    ))
    .unwrap();
    let _fd = nix::fcntl::openat(
        dir_fd.as_fd(),
        backend.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    set_internal_rawname_at(dir_fd.as_fd(), backend.as_c_str(), &logical).unwrap();

    let _hook = force_internal_rawname_errno(libc::EIO);
    let err = lookup_entry_result(&fs, ROOT_INODE, OsStr::from_bytes(&logical)).unwrap_err();

    assert_eq!(err, libc::EIO);
}

#[test]
fn parallel_rebuild_falls_back_sequentially_when_all_worker_dups_fail() {
    rebuild_dir_index_parallel_path_falls_back_when_all_worker_dups_fail();
}

#[test]
fn sequential_fallback_failure_returns_the_backend_errno() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let core = new_test_core(&tmp, false);
    let root_fd = core.cached_root_fd().unwrap();
    let mut names = Vec::new();
    for i in 0..(PARALLEL_REBUILD_THRESHOLD + 1) {
        let name = CString::new(format!(".__ln2_parallel_fail_{i:03}")).unwrap();
        let _fd = nix::fcntl::openat(
            root_fd.as_fd(),
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap();
        set_internal_rawname_at(
            root_fd.as_fd(),
            name.as_c_str(),
            format!("raw-{i}").as_bytes(),
        )
        .unwrap();
        names.push(name);
    }

    let _guard = force_parallel_rebuild_dup_fail();
    let _hook = force_internal_rawname_errno(libc::EIO);
    let err = rebuild_dir_index_from_backend(root_fd.as_fd()).unwrap_err();

    assert_eq!(core_err_to_errno(&err), libc::EIO);
}
