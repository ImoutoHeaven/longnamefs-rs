use super::common::*;
use super::*;
use crate::v2::path::MAX_SEGMENT_ON_DISK;
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::symlink;

struct ProcfsTestGuard;

impl Drop for ProcfsTestGuard {
    fn drop(&mut self) {
        set_test_force_procfs_path_errno(None);
        set_test_force_procfs_unavailable(false);
    }
}

fn force_procfs_unavailable_errno(errno: i32) -> ProcfsTestGuard {
    super::set_test_force_procfs_path_errno(Some(errno));
    super::set_test_force_procfs_unavailable(true);
    ProcfsTestGuard
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
fn rename_overwrite_clears_replaced_inode_parent_mapping() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("a"), b"a").unwrap();
    fs::write(tmp.path().join("b"), b"b").unwrap();
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
    let stat_a = fstatat(root_fd.as_fd(), c"a", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let stat_b = fstatat(root_fd.as_fd(), c"b", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let a = fs.ensure_child_entry(ROOT_INODE, OsStr::new("a"), b"a".to_vec(), stat_a, 1);
    let b = fs.ensure_child_entry(ROOT_INODE, OsStr::new("b"), b"b".to_vec(), stat_b, 1);

    let replaced = fs.lookup_existing_child_snapshot(ROOT_INODE, OsStr::new("/"), OsStr::new("b"));
    fs.core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("a"),
            OsStr::new("/"),
            OsStr::new("b"),
            0,
        )
        .unwrap();

    let renamed = fs
        .apply_rename_inode_bookkeeping(
            ROOT_INODE,
            OsStr::new("a"),
            ROOT_INODE,
            OsStr::new("b"),
            OsStr::new("/"),
            replaced,
        )
        .unwrap()
        .expect("renamed inode should resolve");
    assert_eq!(renamed, a.ino);

    let moved = fs.inode_store.get(a.ino).unwrap();
    assert_eq!(moved.parent, ROOT_INODE);
    assert_eq!(moved.name, OsStr::new("b"));
    assert!(
        moved
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("b"))
    );

    let replaced = fs.inode_store.get(b.ino).unwrap();
    assert!(
        !replaced
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("b"))
    );
    assert!(matches!(
        fs.inode_store.get_path(b.ino),
        Err(CoreError::StaleInode)
    ));
}

#[test]
fn rename_bookkeeping_ignores_stale_replaced_snapshot() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("a"), b"a").unwrap();
    fs::write(tmp.path().join("b"), b"b").unwrap();
    fs::write(tmp.path().join("c"), b"c").unwrap();
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
    let stat_a = fstatat(root_fd.as_fd(), c"a", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let stat_b = fstatat(root_fd.as_fd(), c"b", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let stat_c = fstatat(root_fd.as_fd(), c"c", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let a = fs.ensure_child_entry(ROOT_INODE, OsStr::new("a"), b"a".to_vec(), stat_a, 1);
    let b = fs.ensure_child_entry(ROOT_INODE, OsStr::new("b"), b"b".to_vec(), stat_b, 1);
    let c = fs.ensure_child_entry(ROOT_INODE, OsStr::new("c"), b"c".to_vec(), stat_c, 1);

    let _ = fs.inode_store.add_parent_name(
        c.ino,
        ParentName {
            parent: ROOT_INODE,
            name: OsString::from("b"),
            backend_name: b"c".to_vec(),
        },
    );
    assert!(
        fs.inode_store
            .get(c.ino)
            .unwrap()
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("b"))
    );

    fs.core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("a"),
            OsStr::new("/"),
            OsStr::new("b"),
            0,
        )
        .unwrap();

    let renamed = fs
        .apply_rename_inode_bookkeeping(
            ROOT_INODE,
            OsStr::new("a"),
            ROOT_INODE,
            OsStr::new("b"),
            OsStr::new("/"),
            Some(ReplacedChildSnapshot {
                ino: c.ino,
                backend: b.backend,
            }),
        )
        .unwrap()
        .expect("renamed inode should resolve");
    assert_eq!(renamed, a.ino);

    assert!(
        fs.inode_store
            .get(c.ino)
            .unwrap()
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("b")),
        "stale replaced snapshot must not remove unrelated inode parent mapping"
    );
}

#[test]
fn post_mutation_bookkeeping_does_not_create_zero_lookup_inode() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("a"), b"a").unwrap();
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

    fs.core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("a"),
            OsStr::new("/"),
            OsStr::new("b"),
            0,
        )
        .unwrap();

    let renamed = fs
        .apply_rename_inode_bookkeeping(
            ROOT_INODE,
            OsStr::new("a"),
            ROOT_INODE,
            OsStr::new("b"),
            OsStr::new("/"),
            None,
        )
        .unwrap();
    assert!(
        renamed.is_none(),
        "rename bookkeeping should not create inode for uncached backend"
    );
}

#[test]
fn unlink_bookkeeping_miss_does_not_create_zero_lookup_inode() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("u"), b"u").unwrap();
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
    let stat = fstatat(root_fd.as_fd(), c"u", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let backend_key = backend_key_from_stat(&stat);
    assert!(fs.inode_store.get_by_backend(backend_key).is_none());

    let removed = fs.apply_unlink_inode_bookkeeping(ROOT_INODE, OsStr::new("u"), b"u", stat);
    assert!(removed.is_none());
    assert!(fs.inode_store.get_by_backend(backend_key).is_none());
}

#[test]
fn rmdir_bookkeeping_miss_does_not_create_zero_lookup_inode() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("d")).unwrap();
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
    let stat = fstatat(root_fd.as_fd(), c"d", AtFlags::AT_SYMLINK_NOFOLLOW).unwrap();
    let backend_key = backend_key_from_stat(&stat);
    assert!(fs.inode_store.get_by_backend(backend_key).is_none());

    let removed = fs.apply_rmdir_inode_bookkeeping(ROOT_INODE, OsStr::new("d"), b"d", stat);
    assert!(removed.is_none());
    assert!(fs.inode_store.get_by_backend(backend_key).is_none());
}

#[test]
fn map_segment_for_create_detects_existing_long_name() {
    let state = DirState {
        index: Arc::new(RwLock::new(IndexState {
            index: DirIndex::new(),
            journal_file: None,
            pending: 0,
            last_flush: Instant::now(),
            flushing: false,
            journal_size_bytes: 0,
            journal_ops_since_compact: 0,
            flush_wait: Arc::new(FlushWait::default()),
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

#[test]
fn rename_invalid_flags_return_einval() {
    let err = validate_rename_flags_v2(1 << 31).unwrap_err();
    assert_eq!(core_err_to_errno(&err), libc::EINVAL);
}

#[test]
fn rename_supported_but_unimplemented_flags_stay_unsupported() {
    let err = validate_rename_flags_v2(libc::RENAME_EXCHANGE).unwrap_err();
    assert_eq!(core_err_to_errno(&err), libc::EOPNOTSUPP);
}

#[test]
fn rename_noreplace_exchange_combination_returns_einval() {
    let err = validate_rename_flags_v2(libc::RENAME_NOREPLACE | libc::RENAME_EXCHANGE).unwrap_err();
    assert_eq!(core_err_to_errno(&err), libc::EINVAL);
}

#[test]
fn dir_fd_name_index_tracks_parent_name_to_child_key() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("a")).unwrap();
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
    let child_fd = fs
        .core
        .open_dir_cached(root_fd.as_fd(), &BackendName::Short(b"a".to_vec()))
        .unwrap();

    let parent_key = dir_cache_key(root_fd.as_fd()).unwrap();
    let child_stat = fstat(child_fd.as_fd()).unwrap();
    let child_key = DirCacheKey {
        dev: child_stat.st_dev,
        ino: child_stat.st_ino,
    };

    assert_eq!(
        fs.core.dir_fd_cache.name_index_get(parent_key, b"a"),
        Some(child_key)
    );

    fs.core.invalidate_dir_by_key(parent_key);
    assert_eq!(fs.core.dir_fd_cache.name_index_get(parent_key, b"a"), None);

    let _ = fs
        .core
        .open_dir_cached(root_fd.as_fd(), &BackendName::Short(b"a".to_vec()))
        .unwrap();
    assert_eq!(
        fs.core.dir_fd_cache.name_index_get(parent_key, b"a"),
        Some(child_key)
    );

    fs.patch_dir_cache(root_fd.as_fd(), CacheOp::Remove(b"a".to_vec()));
    assert_eq!(fs.core.dir_fd_cache.name_index_get(parent_key, b"a"), None);
}

fn rawname_write_is_unsupported_errno(errno: i32) -> bool {
    matches!(errno, libc::EOPNOTSUPP | libc::ENOSYS | libc::EPERM)
}

fn probe_rawname_write_support_via_lxattr(
    name: &str,
    setup: impl FnOnce(&std::path::Path),
) -> Result<(), i32> {
    let tmp = TempDir::new();
    let path = tmp.path().join(name);
    setup(&path);

    let c_path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let xname = CString::new(RAWNAME_XATTR.as_bytes()).unwrap();
    let value = b"rawname-probe";
    let res = unsafe {
        libc::lsetxattr(
            c_path.as_ptr(),
            xname.as_ptr(),
            value.as_ptr() as *const libc::c_void,
            value.len(),
            0,
        )
    };
    if res == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO))
    }
}

fn expected_internal_backend_path(tmp: &TempDir, logical_name: &str) -> std::path::PathBuf {
    let backend = backend_basename_from_hash(&encode_long_name(logical_name.as_bytes()), None);
    tmp.path().join(backend)
}

fn probe_symlink_rawname_support_independently() -> bool {
    matches!(
        probe_rawname_write_support_via_lxattr("probe-link", |path| {
            symlink("target", path).unwrap();
        }),
        Ok(())
    )
}

fn probe_fifo_rawname_support_independently() -> bool {
    matches!(
        probe_rawname_write_support_via_lxattr("probe-fifo", |path| {
            nix::unistd::mkfifo(path, Mode::from_bits_truncate(0o644)).unwrap();
        }),
        Ok(())
    )
}

fn probe_socket_rawname_support_independently() -> bool {
    matches!(
        probe_rawname_write_support_via_lxattr("probe-sock", |path| {
            let _listener = std::os::unix::net::UnixListener::bind(path).unwrap();
        }),
        Ok(())
    )
}

fn assert_rename_upgrade_outcome(
    tmp: &TempDir,
    source_name: &str,
    logical_name: &str,
    kind: &str,
    rawname_supported: bool,
    result: CoreResult<DirInvalidation>,
) {
    let source_path = tmp.path().join(source_name);
    let backend_path = expected_internal_backend_path(tmp, logical_name);

    if rawname_supported {
        assert!(
            result.is_ok(),
            "{kind} short->long rename should succeed when rawname metadata is supported: {result:?}"
        );
        assert!(
            fs::symlink_metadata(&source_path).is_err(),
            "{kind} source should be gone after committed rename"
        );
        assert!(
            fs::symlink_metadata(&backend_path).is_ok(),
            "{kind} backend entry should be moved to the internal long-name target"
        );
        return;
    }

    let err = result.expect_err(&format!(
        "{kind} short->long rename must fail before commit when rawname metadata is unsupported"
    ));
    let errno = core_err_to_errno(&err);
    assert!(
        rawname_write_is_unsupported_errno(errno),
        "{kind} unsupported rawname write should surface backend unsupported errno, got {errno}: {err:?}"
    );
    assert!(
        fs::symlink_metadata(&source_path).is_ok(),
        "{kind} source must remain in place when metadata write fails before commit"
    );
    assert!(
        fs::symlink_metadata(&backend_path).is_err(),
        "{kind} backend rename target must not exist when commit never happened"
    );
}

#[test]
fn rename_upgrade_is_type_transparent_for_regular_file_symlink_fifo_and_unix_socket() {
    let tmp = TempDir::new();
    let long_name = "x".repeat(MAX_SEGMENT_ON_DISK + 8);
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    std::os::unix::fs::symlink("target", tmp.path().join("link")).unwrap();
    nix::unistd::mkfifo(&tmp.path().join("fifo"), Mode::from_bits_truncate(0o644)).unwrap();
    let _sock = std::os::unix::net::UnixListener::bind(tmp.path().join("sock")).unwrap();
    let _fifo_peer = nix::fcntl::open(
        tmp.path().join("fifo").as_path(),
        OFlag::O_RDWR | OFlag::O_NONBLOCK | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();

    let symlink_rawname_supported = probe_symlink_rawname_support_independently();
    let fifo_rawname_supported = probe_fifo_rawname_support_independently();
    let socket_rawname_supported = probe_socket_rawname_support_independently();

    let core = new_longname_test_core(&tmp, false);

    let file_result = core.rename_with_flags(
        OsStr::new("/"),
        OsStr::new("file"),
        OsStr::new("/"),
        OsStr::new(&long_name),
        0,
    );
    let link_result = core.rename_with_flags(
        OsStr::new("/"),
        OsStr::new("link"),
        OsStr::new("/"),
        OsStr::new(&(long_name.clone() + "-link")),
        0,
    );
    let fifo_result = core.rename_with_flags(
        OsStr::new("/"),
        OsStr::new("fifo"),
        OsStr::new("/"),
        OsStr::new(&(long_name.clone() + "-fifo")),
        0,
    );
    let socket_result = core.rename_with_flags(
        OsStr::new("/"),
        OsStr::new("sock"),
        OsStr::new("/"),
        OsStr::new(&(long_name.clone() + "-sock")),
        0,
    );

    assert_rename_upgrade_outcome(&tmp, "file", &long_name, "regular-file", true, file_result);
    assert_rename_upgrade_outcome(
        &tmp,
        "link",
        &(long_name.clone() + "-link"),
        "symlink",
        symlink_rawname_supported,
        link_result,
    );
    assert_rename_upgrade_outcome(
        &tmp,
        "fifo",
        &(long_name.clone() + "-fifo"),
        "fifo",
        fifo_rawname_supported,
        fifo_result,
    );
    assert_rename_upgrade_outcome(
        &tmp,
        "sock",
        &(long_name.clone() + "-sock"),
        "unix-socket",
        socket_rawname_supported,
        socket_result,
    );
}

#[test]
fn rename_upgrade_fails_before_commit_when_metadata_write_is_unsupported() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let core = new_longname_test_core(&tmp, false);
    let long_name = "y".repeat(MAX_SEGMENT_ON_DISK + 8);

    let _hook = force_internal_rawname_errno(libc::EOPNOTSUPP);
    let err = core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("file"),
            OsStr::new("/"),
            OsStr::new(&long_name),
            0,
        )
        .unwrap_err();

    assert_eq!(core_err_to_errno(&err), libc::EOPNOTSUPP);
    assert!(
        tmp.path().join("file").exists(),
        "rename must fail before backend mutation"
    );
}

#[test]
fn rename_upgrade_backend_rename_failure_cleans_staged_source_rawname() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("file"), b"hello").unwrap();
    let core = new_longname_test_core(&tmp, false);
    if !core.supports_renameat2 {
        return;
    }

    let root_fd = core.cached_root_fd().unwrap();
    let source_c = CString::new("file").unwrap();
    let initial_err = get_internal_rawname_at(root_fd.as_fd(), source_c.as_c_str())
        .expect_err("source short entry should start without rawname xattr");
    assert!(
        is_missing_rawname_xattr_error(&initial_err),
        "source short entry should begin without rawname xattr, got {initial_err:?}"
    );

    // Preload the root dir state so the later externally-created collision entries stay invisible
    // to the cached index and force rename_upgrade() down its refresh/retry failure path.
    core.resolve_dir(OsStr::new("/")).unwrap();

    let long_name = "z".repeat(MAX_SEGMENT_ON_DISK + 8);
    let hash = encode_long_name(long_name.as_bytes());
    let occupied_base = backend_basename_from_hash(&hash, None);
    let occupied_suffix = backend_basename_from_hash(&hash, Some(1));

    for (backend_name, rawname) in [
        (&occupied_base, format!("{long_name}-occupied-base")),
        (&occupied_suffix, format!("{long_name}-occupied-suffix")),
    ] {
        fs::write(tmp.path().join(backend_name), b"occupied").unwrap();
        let backend_c = CString::new(backend_name.as_bytes()).unwrap();
        set_internal_rawname_at(root_fd.as_fd(), backend_c.as_c_str(), rawname.as_bytes()).unwrap();
    }

    let err = core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("file"),
            OsStr::new("/"),
            OsStr::new(&long_name),
            libc::RENAME_NOREPLACE,
        )
        .expect_err("rename upgrade should fail when backend rename keeps colliding");

    assert_eq!(
        core_err_to_errno(&err),
        libc::EEXIST,
        "backend rename failure should surface the rename errno"
    );
    assert!(
        tmp.path().join("file").exists(),
        "source path must remain in place when backend rename never commits"
    );

    let rawname_err = get_internal_rawname_at(root_fd.as_fd(), source_c.as_c_str())
        .expect_err("failed rename upgrade must clean the staged destination rawname from source");
    assert!(
        is_missing_rawname_xattr_error(&rawname_err),
        "failed rename upgrade must remove staged rawname metadata from source, got {rawname_err:?}"
    );
}

#[test]
fn rename_long_to_long_refresh_failure_restores_source_name() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let src = "s".repeat(MAX_SEGMENT_ON_DISK + 10);
    let dst = "d".repeat(MAX_SEGMENT_ON_DISK + 10);
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    fs.core.resolve_dir(OsStr::new("/")).unwrap();

    let dst_hash = encode_long_name(dst.as_bytes());
    let colliding_backend = backend_basename_from_hash(&dst_hash, None);
    fs::write(tmp.path().join(&colliding_backend), b"collision").unwrap();

    let result = rename_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&src),
        ROOT_INODE,
        OsStr::new(&dst),
        libc::RENAME_NOREPLACE,
    );

    let err = result.expect_err("rename should surface the refresh failure");
    assert_eq!(err, libc::ENODATA);
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_ok(),
        "source name must be restored when refresh fails after src->tmp"
    );
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_err(),
        "destination name must not appear when refresh fails before final rename"
    );
    let root_fd = fs.core.cached_root_fd().unwrap();
    let src_backend_c = CString::new(created.backend_name.clone()).unwrap();
    assert_eq!(
        get_internal_rawname_at(root_fd.as_fd(), src_backend_c.as_c_str()).unwrap(),
        src.as_bytes(),
        "source backend entry must keep its original rawname after rollback"
    );
    assert!(
        fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|entry| {
                let name = CString::new(entry.file_name().as_os_str().as_bytes()).ok()?;
                get_internal_rawname_at(root_fd.as_fd(), name.as_c_str()).ok()
            })
            .all(|raw| raw != dst.as_bytes()),
        "failed rename must not strand a backend entry staged under the destination rawname"
    );
}

#[test]
fn set_internal_rawname_at_preserves_symlink_errno_when_procfs_is_unavailable() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let name = CString::new(".__ln2_symlink_errno").unwrap();
    symlinkat("target", dir_fd.as_fd(), name.as_c_str()).unwrap();

    let _guard = force_procfs_unavailable_errno(libc::ENOENT);
    let err = set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), b"rawname")
        .expect_err("procfs-unavailable symlink path should fail");
    assert_eq!(
        core_err_to_errno(&err),
        libc::ELOOP,
        "symlink procfs-unavailable path must preserve ELOOP instead of collapsing to EBADF"
    );
}

#[test]
fn rename_noreplace_without_renameat2_returns_eopnotsupp_before_mutation() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("a"), b"a").unwrap();
    let core = new_test_core(&tmp, false);

    if core.supports_renameat2 {
        return;
    }

    let result = core.rename_with_flags(
        OsStr::new("/"),
        OsStr::new("a"),
        OsStr::new("/"),
        OsStr::new("b"),
        libc::RENAME_NOREPLACE,
    );

    let err = result.expect_err("rename_noreplace must fail closed without renameat2");
    assert_eq!(core_err_to_errno(&err), libc::EOPNOTSUPP);
    assert!(
        tmp.path().join("a").exists(),
        "source must remain when noreplace is unsupported"
    );
    assert!(
        !tmp.path().join("b").exists(),
        "destination must not be created when noreplace fails before mutation"
    );
}

#[test]
fn cross_directory_rename_preserves_lock_ordering_and_succeeds() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("src")).unwrap();
    fs::create_dir(tmp.path().join("dst")).unwrap();
    fs::write(tmp.path().join("src").join("file"), b"hello").unwrap();
    let core = new_test_core(&tmp, false);

    core.rename_with_flags(
        OsStr::new("/src"),
        OsStr::new("file"),
        OsStr::new("/dst"),
        OsStr::new("file"),
        0,
    )
    .unwrap();

    assert!(tmp.path().join("dst").join("file").exists());
    assert!(!tmp.path().join("src").join("file").exists());
}
