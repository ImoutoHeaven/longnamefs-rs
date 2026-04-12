use super::common::*;
use super::*;
use crate::v2::path::MAX_SEGMENT_ON_DISK;
use std::ffi::CString;
use std::fs;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
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
            RenameBookkeepingSnapshot {
                replaced_child: replaced,
                replaced_backend: replaced.map(|snapshot| snapshot.backend),
                renamed_backend: Some(a.backend),
                renamed_backend_name: Some(b"b".to_vec()),
            },
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
            RenameBookkeepingSnapshot {
                replaced_child: Some(ReplacedChildSnapshot {
                    ino: c.ino,
                    backend: b.backend,
                }),
                replaced_backend: Some(b.backend),
                renamed_backend: Some(a.backend),
                renamed_backend_name: Some(b"b".to_vec()),
            },
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
            RenameBookkeepingSnapshot::default(),
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
    let tmp = TempDir::new();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
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
    let mut state = state;
    let backend_name = format_long_object_name(1);
    let backend_c = CString::new(backend_name.clone()).unwrap();
    let _fd = nix::fcntl::openat(
        dir_fd.as_fd(),
        backend_c.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    set_internal_rawname_at(dir_fd.as_fd(), backend_c.as_c_str(), &raw).unwrap();
    let err = map_segment_for_create(dir_fd.as_fd(), &mut state, &raw, raw.len() + 4).unwrap_err();
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

fn has_internal_backend_with_rawname(tmp: &TempDir, logical_name: &str) -> bool {
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    fs::read_dir(tmp.path()).unwrap().any(|entry| {
        let entry = entry.unwrap();
        let bytes = entry.file_name().as_os_str().as_bytes().to_vec();
        if !crate::v2::object_id::is_stable_long_object_name(&bytes) {
            return false;
        }
        let c_name = CString::new(bytes).unwrap();
        matches!(
            get_internal_rawname_at(dir_fd.as_fd(), c_name.as_c_str()),
            Ok(raw) if raw == logical_name.as_bytes()
        )
    })
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
            has_internal_backend_with_rawname(tmp, logical_name),
            "{kind} backend entry should be committed under a stable internal long-name target"
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
        !has_internal_backend_with_rawname(tmp, logical_name),
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
    let occupied_base = String::from_utf8_lossy(&format_long_object_name(1)).into_owned();
    let occupied_suffix = String::from_utf8_lossy(&format_long_object_name(2)).into_owned();

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
fn rename_long_to_long_same_dir_preserves_backend_identity() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let src = "s".repeat(MAX_SEGMENT_ON_DISK + 10);
    let dst = "d".repeat(MAX_SEGMENT_ON_DISK + 10);
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
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
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_err(),
        "source name must disappear after committed same-dir rename"
    );
    assert!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).is_ok(),
        "destination name must appear after committed same-dir rename"
    );
    let root_fd = fs.core.cached_root_fd().unwrap();
    let src_backend_c = CString::new(created.backend_name.clone()).unwrap();
    assert_eq!(
        get_internal_rawname_at(root_fd.as_fd(), src_backend_c.as_c_str()).unwrap(),
        dst.as_bytes(),
        "same-dir rename must update rawname on the existing stable backend entry"
    );
}

#[test]
fn rename_long_to_long_same_dir_rawname_write_failure_clears_txn_and_keeps_mount_usable() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("same-dir-rawname-fail-src");
    let dst = long_name("same-dir-rawname-fail-dst");
    let followup = long_name("same-dir-rawname-fail-followup");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    let backend_path = tmp.path().join(OsStr::from_bytes(&created.backend_name));
    release_result(&fs, created.ino, created.fh).unwrap();
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_internal_rawname_errno(libc::EOPNOTSUPP);
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

    assert_eq!(err, libc::EOPNOTSUPP);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 0);
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );
    assert_eq!(read_rawname_xattr(&backend_path), src.as_bytes());

    let followup_created =
        create_result(&fs, ROOT_INODE, OsStr::new(&followup), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, followup_created.ino, followup_created.fh).unwrap();
    assert!(lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&followup)).is_ok());
}

#[test]
fn long_to_long_cross_dir_uses_one_stable_backend_name() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("src"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("dst"), 0o755).unwrap();
    let src = long_name("cross-dir-src");
    let dst = long_name("cross-dir-dst");

    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    let old_backend = created.backend_name.clone();
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

    let renamed = lookup_entry_result(&fs, dst_dir.ino, OsStr::new(&dst)).unwrap();
    assert_eq!(
        backend_name_for_ino_result(&fs, renamed.ino).unwrap(),
        old_backend
    );
    assert!(lookup_entry_result(&fs, src_dir.ino, OsStr::new(&src)).is_err());
}

#[test]
fn rename_long_to_long_cross_dir_rawname_write_failure_clears_txn_and_keeps_mount_usable() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("rawname-src"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("rawname-dst"), 0o755).unwrap();
    let src = long_name("cross-dir-rawname-fail-src");
    let dst = long_name("cross-dir-rawname-fail-dst");
    let followup = long_name("cross-dir-rawname-fail-followup");
    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    let backend_path = tmp
        .path()
        .join("rawname-src")
        .join(OsStr::from_bytes(&created.backend_name));
    release_result(&fs, created.ino, created.fh).unwrap();
    crate::v2::txn::reset_test_rollback_inflight_txn_calls();

    let err = {
        let _hook = force_internal_rawname_errno(libc::EOPNOTSUPP);
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

    assert_eq!(err, libc::EOPNOTSUPP);
    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 0);
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
    assert!(lookup_entry_result(&fs, src_dir.ino, OsStr::new(&src)).is_ok());
    assert_eq!(
        lookup_entry_result(&fs, dst_dir.ino, OsStr::new(&dst)).unwrap_err(),
        libc::ENOENT
    );
    assert_eq!(read_rawname_xattr(&backend_path), src.as_bytes());

    let followup_created =
        create_result(&fs, dst_dir.ino, OsStr::new(&followup), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, followup_created.ino, followup_created.fh).unwrap();
    assert!(lookup_entry_result(&fs, dst_dir.ino, OsStr::new(&followup)).is_ok());
}

#[test]
fn long_to_long_same_dir_does_not_create_hidden_midpoint_entry() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("same-dir-midpoint-src");
    let dst = long_name("same-dir-midpoint-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
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

    assert!(
        !fs::read_dir(tmp.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .any(|name| name.as_os_str().as_bytes().starts_with(b".ln2_fs_rtmp_"))
    );
}

#[test]
fn long_to_long_cross_dir_does_not_create_hidden_midpoint_entry() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("src-mid"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("dst-mid"), 0o755).unwrap();
    let src = long_name("cross-dir-midpoint-src");
    let dst = long_name("cross-dir-midpoint-dst");
    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
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

    for dir in [tmp.path().join("src-mid"), tmp.path().join("dst-mid")] {
        assert!(
            !fs::read_dir(dir)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .any(|name| name.as_os_str().as_bytes().starts_with(b".ln2_fs_rtmp_"))
        );
    }
}

#[test]
fn long_involving_rename_rejects_distinct_destination_with_eexist() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);

    fs::write(tmp.path().join("short-src"), b"a").unwrap();
    let long_dst_1 = long_name("exists-long-1");
    let created_1 = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_dst_1),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created_1.ino, created_1.fh).unwrap();
    assert_eq!(
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-src"),
            ROOT_INODE,
            OsStr::new(&long_dst_1),
            0
        )
        .unwrap_err(),
        libc::EEXIST,
    );

    fs::write(tmp.path().join("short-dst"), b"b").unwrap();
    let long_src_2 = long_name("exists-long-2");
    let created_2 = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_src_2),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created_2.ino, created_2.fh).unwrap();
    assert_eq!(
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_src_2),
            ROOT_INODE,
            OsStr::new("short-dst"),
            0
        )
        .unwrap_err(),
        libc::EEXIST,
    );

    let long_src_3 = long_name("exists-long-3-src");
    let long_dst_3 = long_name("exists-long-3-dst");
    let created_3_src = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_src_3),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    let created_3_dst = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_dst_3),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created_3_src.ino, created_3_src.fh).unwrap();
    release_result(&fs, created_3_dst.ino, created_3_dst.fh).unwrap();
    assert_eq!(
        rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new(&long_src_3),
            ROOT_INODE,
            OsStr::new(&long_dst_3),
            0
        )
        .unwrap_err(),
        libc::EEXIST,
    );

    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("src-eexist"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("dst-eexist"), 0o755).unwrap();
    let long_src_4 = long_name("exists-long-4-src");
    let long_dst_4 = long_name("exists-long-4-dst");
    let created_4_src = create_result(
        &fs,
        src_dir.ino,
        OsStr::new(&long_src_4),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    let created_4_dst = create_result(
        &fs,
        dst_dir.ino,
        OsStr::new(&long_dst_4),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created_4_src.ino, created_4_src.fh).unwrap();
    release_result(&fs, created_4_dst.ino, created_4_dst.fh).unwrap();
    assert_eq!(
        rename_result(
            &fs,
            src_dir.ino,
            OsStr::new(&long_src_4),
            dst_dir.ino,
            OsStr::new(&long_dst_4),
            0
        )
        .unwrap_err(),
        libc::EEXIST,
    );
}

#[test]
fn long_involving_rename_noreplace_does_not_require_renameat2() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"a").unwrap();
    let long_dst = "n".repeat(MAX_SEGMENT_ON_DISK + 8);
    let collision = format_long_object_name(1);
    fs::write(tmp.path().join(OsStr::from_bytes(&collision)), b"occupied").unwrap();

    let mut core = new_longname_test_core(&tmp, false);
    core.supports_renameat2 = false;

    let root_fd = core.cached_root_fd().unwrap();
    let collision_c = CString::new(collision).unwrap();
    set_internal_rawname_at(root_fd.as_fd(), collision_c.as_c_str(), long_dst.as_bytes()).unwrap();

    let err = core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("short-src"),
            OsStr::new("/"),
            OsStr::new(&long_dst),
            libc::RENAME_NOREPLACE,
        )
        .unwrap_err();

    assert_eq!(core_err_to_errno(&err), libc::EEXIST);
    assert!(tmp.path().join("short-src").exists());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn long_hardlink_creation_is_rejected_with_eperm() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let name = long_name("hardlink-src");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = link_result(&fs, created.ino, ROOT_INODE, OsStr::new("linked")).unwrap_err();
    assert_eq!(err, libc::EPERM);
}

#[test]
fn hardlink_long_destination_is_rejected_with_eperm() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"payload").unwrap();
    let fs = new_always_sync_fs(&tmp);
    let src = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-src")).unwrap();
    let long_dst = long_name("hardlink-dst");

    let err = link_result(&fs, src.ino, ROOT_INODE, OsStr::new(&long_dst)).unwrap_err();
    assert_eq!(err, libc::EPERM);
}

#[test]
fn short_to_long_rename_rejects_multiply_linked_non_directory() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"payload").unwrap();
    fs::hard_link(tmp.path().join("short-src"), tmp.path().join("short-peer")).unwrap();
    let fs = new_always_sync_fs(&tmp);
    let long_dst = long_name("linked-long-dst");

    let err = rename_result(
        &fs,
        ROOT_INODE,
        OsStr::new("short-src"),
        ROOT_INODE,
        OsStr::new(&long_dst),
        0,
    )
    .unwrap_err();
    assert_eq!(err, libc::EPERM);
}

#[test]
fn short_rename_between_same_inode_hardlinks_is_noop_and_preserves_aliases() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"payload").unwrap();
    fs::hard_link(tmp.path().join("short-src"), tmp.path().join("short-dst")).unwrap();
    let fs = new_always_sync_fs(&tmp);

    let src = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-src")).unwrap();
    let dst = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-dst")).unwrap();
    assert_eq!(src.ino, dst.ino, "setup must point both names at one inode");

    let renamed = rename_result(
        &fs,
        ROOT_INODE,
        OsStr::new("short-src"),
        ROOT_INODE,
        OsStr::new("short-dst"),
        0,
    )
    .unwrap();
    assert!(renamed.used_callback_path);

    let cached = inode_entry_result(&fs, src.ino).unwrap();
    assert_eq!(cached.parent, ROOT_INODE);
    assert!(
        cached
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("short-src"))
    );
    assert!(
        cached
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("short-dst"))
    );

    let src_after = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-src")).unwrap();
    let dst_after = lookup_entry_result(&fs, ROOT_INODE, OsStr::new("short-dst")).unwrap();
    assert_eq!(src_after.ino, dst_after.ino);
    assert_eq!(src_after.ino, src.ino);
    assert!(
        cached
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("short-src"))
    );
    assert!(
        cached
            .parents
            .iter()
            .any(|p| p.parent == ROOT_INODE && p.name == OsStr::new("short-dst"))
    );
    assert_eq!(fs::metadata(tmp.path().join("short-src")).unwrap().nlink(), 2);
    assert_eq!(fs::metadata(tmp.path().join("short-dst")).unwrap().nlink(), 2);
}

#[test]
fn short_rename_noreplace_between_same_inode_hardlinks_returns_eexist() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"payload").unwrap();
    fs::hard_link(tmp.path().join("short-src"), tmp.path().join("short-dst")).unwrap();
    let core = new_test_core(&tmp, false);

    if !core.supports_renameat2 {
        return;
    }

    let err = core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("short-src"),
            OsStr::new("/"),
            OsStr::new("short-dst"),
            libc::RENAME_NOREPLACE,
        )
        .expect_err("same-inode noreplace rename must still report EEXIST");

    assert_eq!(core_err_to_errno(&err), libc::EEXIST);
    assert!(tmp.path().join("short-src").exists());
    assert!(tmp.path().join("short-dst").exists());
}

#[test]
fn short_rename_noreplace_same_inode_without_renameat2_fails_closed() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"payload").unwrap();
    fs::hard_link(tmp.path().join("short-src"), tmp.path().join("short-dst")).unwrap();
    let mut core = new_test_core(&tmp, false);
    core.supports_renameat2 = false;

    let err = core
        .rename_with_flags(
            OsStr::new("/"),
            OsStr::new("short-src"),
            OsStr::new("/"),
            OsStr::new("short-dst"),
            libc::RENAME_NOREPLACE,
        )
        .expect_err("same-inode noreplace rename must fail closed without renameat2");

    assert_eq!(core_err_to_errno(&err), libc::EOPNOTSUPP);
    assert!(tmp.path().join("short-src").exists());
    assert!(tmp.path().join("short-dst").exists());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
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
    let mut core = new_test_core(&tmp, false);
    core.supports_renameat2 = false;

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
