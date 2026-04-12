use super::common::*;
use super::*;
use crate::v2::index::{
    DirIndex, INDEX_NAME, JOURNAL_NAME, JournalOp, append_to_journal, read_dir_index,
    write_dir_index,
};
use crate::v2::object_id::{format_long_object_name, parse_long_object_id};
use crate::v2::path::MAX_SEGMENT_ON_DISK;
use crate::v2::txn::{TxnRecord, read_txn_record, write_txn_record};
use std::fs;

fn new_fs_with_max_name_len(backend: &TempDir, max_name_len: usize) -> LongNameFsV2Fuser {
    let config = Config::open_backend(backend.path().clone(), false, false).unwrap();
    LongNameFsV2Fuser::new(
        config,
        max_name_len,
        Some(Duration::from_secs(60)),
        1024,
        IndexSync::Off,
        Duration::from_secs(1),
        Duration::from_secs(1),
        false,
        false,
        PassthroughMetaFdConfig::disabled(),
    )
    .unwrap()
}

fn committed_short_name(seed: &str) -> String {
    format!("{seed}-{}", "s".repeat(150))
}

#[test]
fn stable_long_backend_name_uses_fixed_width_hex_id() {
    assert_eq!(
        format_long_object_name(1),
        b".__ln2_obj_0000000000000001".to_vec()
    );
    assert_eq!(
        format_long_object_name(u64::MAX),
        b".__ln2_obj_ffffffffffffffff".to_vec()
    );
}

#[test]
fn parse_long_object_id_accepts_only_new_format() {
    assert_eq!(
        parse_long_object_id(b".__ln2_obj_000000000000002a").unwrap(),
        0x2a
    );
    assert!(parse_long_object_id(b".__ln2_deadbeef").is_err());
    assert!(parse_long_object_id(b".__ln2_obj_xyz").is_err());
}

#[test]
fn legacy_hash_named_long_entry_is_rejected_at_init() {
    let tmp = TempDir::new();
    fs::write(
        tmp.path().join(".__ln2_deadbeefdeadbeefdeadbeefdeadbeef"),
        b"x",
    )
    .unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("legacy hash entry should be rejected at init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn init_creates_missing_lock_file() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();
    assert!(tmp.path().join(".ln2_fs_lock").exists());
}

#[test]
fn second_core_init_on_same_backend_fails_with_lock_conflict() {
    let tmp = TempDir::new();
    let config_a = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core_a = LongNameFsCore::new(config_a, MAX_SEGMENT_ON_DISK, None, IndexSync::Off).unwrap();

    let config_b = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config_b, MAX_SEGMENT_ON_DISK, None, IndexSync::Off) {
        Ok(_) => panic!("second init on same backend should fail with lock conflict"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EBUSY),
    }
}

#[test]
fn init_bootstraps_missing_idalloc_on_empty_backend() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();
    let bytes = fs::read(tmp.path().join(".ln2_fs_idalloc")).unwrap();
    assert_eq!(u64::from_le_bytes(bytes.try_into().unwrap()), 1);
}

#[test]
fn init_rejects_missing_idalloc_on_nonempty_new_format_backend() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"x").unwrap();
    let dir_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let rawname = vec![b'l'; MAX_SEGMENT_ON_DISK + 8];
    set_internal_rawname_at(dir_fd.as_fd(), c".__ln2_obj_0000000000000001", &rawname).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("missing idalloc on nonempty new-format backend should fail"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn missing_txn_path_means_no_inflight_transaction() {
    let tmp = TempDir::new();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn malformed_txn_file_is_rejected_at_init() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join(".ln2_fs_txn"), b"bad").unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("malformed txn file should be rejected at init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn truncated_txn_file_is_rejected_at_init() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join(".ln2_fs_txn"), &[1, 0, 0]).unwrap();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("truncated txn file should be rejected at init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn unknown_version_txn_file_is_rejected_at_init() {
    let tmp = TempDir::new();
    let root = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut record =
        TxnRecord::remove_dir(vec![], b"old".to_vec(), b".ln2_fs_deldir_seed".to_vec());
    record.version = u32::MAX;
    write_txn_record(root.as_fd(), &record).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off) {
        Ok(_) => panic!("unknown-version txn file should be rejected at init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn startup_rejects_create_short_txn_for_lock_control_name_before_rollback_side_effects() {
    let tmp = TempDir::new();

    let root = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let record = TxnRecord::create_short(vec![], b".ln2_fs_lock".to_vec(), libc::S_IFREG);
    write_txn_record(root.as_fd(), &record).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => {
            panic!("control-name create-short txn should fail before unlinking .ln2_fs_lock")
        }
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(tmp.path().join(".ln2_fs_lock").exists());
}

#[test]
fn startup_rejects_create_short_txn_with_internal_parent_segment_before_rollback_side_effects() {
    let tmp = TempDir::new();
    let internal_dir = tmp.path().join(".ln2_fs_deldir_seed");
    let victim = internal_dir.join("victim");
    fs::create_dir(&internal_dir).unwrap();
    fs::write(&victim, b"payload").unwrap();
    write_create_short_txn(tmp.path(), b"victim", &[b".ln2_fs_deldir_seed"]);

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!(
            "internal-segment txn should fail before rollback touches .ln2_fs_* subtrees"
        ),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(victim.exists());
}

#[test]
fn startup_rejects_remove_dir_txn_for_internal_control_name_before_rollback_side_effects() {
    let tmp = TempDir::new();
    let quarantine_dir = tmp.path().join(".ln2_fs_deldir_seed");
    fs::create_dir(&quarantine_dir).unwrap();
    write_remove_dir_txn(tmp.path(), b".ln2_fs_index", b".ln2_fs_deldir_seed", &[]);

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!(
            "control-name remove-dir txn should fail before rollback renames onto .ln2_fs_*"
        ),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(quarantine_dir.exists());
    assert!(!tmp.path().join(".ln2_fs_index").exists());
}

#[test]
fn startup_rejects_txn_with_logical_long_parent_segment_before_rollback_side_effects() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    let old_raw = long_name("logical-parent-old");
    let new_raw = long_name("logical-parent-new");
    let logical_parent = long_name("logical-parent-segment");
    fs::write(&backend, b"payload").unwrap();
    set_rawname_xattr(&backend, new_raw.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let record = TxnRecord::rename_long_to_long_same_dir(
        vec![logical_parent.as_bytes().to_vec()],
        b".__ln2_obj_0000000000000001".to_vec(),
        old_raw.clone().into_bytes(),
        new_raw.clone().into_bytes(),
        libc::S_IFREG,
    );
    write_txn_record(root.as_fd(), &record).unwrap();

    crate::v2::txn::reset_test_rollback_inflight_txn_calls();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!(
            "logical long parent segment should fail startup validation before rollback"
        ),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 0);
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert_eq!(read_rawname_xattr(&backend), new_raw.into_bytes());
}

#[test]
fn startup_rejects_txn_with_malformed_object_parent_segment_before_rollback_side_effects() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let residue_dir = tmp.path().join(".__ln2_obj_badbadbadbadbad");
    let victim = residue_dir.join("victim");
    fs::create_dir(&residue_dir).unwrap();
    fs::write(&victim, b"payload").unwrap();
    write_create_short_txn(tmp.path(), b"victim", &[b".__ln2_obj_badbadbadbadbad"]);

    crate::v2::txn::reset_test_rollback_inflight_txn_calls();
    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!(
            "malformed object parent segment should fail startup validation before rollback"
        ),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert_eq!(crate::v2::txn::test_rollback_inflight_txn_calls(), 0);
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(victim.exists());
}

#[test]
fn startup_rejects_malformed_decodable_long_to_short_txn_before_rollback_side_effects() {
    let tmp = TempDir::new();
    let short_path = tmp.path().join("restart-short");
    fs::write(&short_path, b"payload").unwrap();
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut record = TxnRecord::rename_long_to_short(
        vec![],
        vec![],
        b".__ln2_obj_0000000000000001".to_vec(),
        long_name("restart-long-src").into_bytes(),
        b"restart-short".to_vec(),
        libc::S_IFREG,
    );
    record.old_rawname = None;
    write_txn_record(root.as_fd(), &record).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => {
            panic!("malformed-but-decodable long-to-short txn should fail before rollback")
        }
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(short_path.exists());
    assert!(!tmp.path().join(".__ln2_obj_0000000000000001").exists());
}

#[test]
fn startup_rejects_malformed_decodable_cross_dir_long_rename_before_rollback_side_effects() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join("src")).unwrap();
    fs::create_dir(tmp.path().join("dst")).unwrap();
    let moved = tmp.path().join("dst").join(".__ln2_obj_0000000000000001");
    let new_raw = long_name("restart-cross-new");
    fs::write(&moved, b"payload").unwrap();
    set_rawname_xattr(&moved, new_raw.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut record = TxnRecord::rename_long_to_long_cross_dir(
        vec![b"src".to_vec()],
        vec![b"dst".to_vec()],
        b".__ln2_obj_0000000000000001".to_vec(),
        long_name("restart-cross-old").into_bytes(),
        new_raw.clone().into_bytes(),
        libc::S_IFREG,
    );
    record.old_rawname = None;
    write_txn_record(root.as_fd(), &record).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!(
            "malformed-but-decodable cross-dir long rename txn should fail before rollback"
        ),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }

    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(!tmp.path().join("src").join(".__ln2_obj_0000000000000001").exists());
    assert!(moved.exists());
    assert_eq!(read_rawname_xattr(&moved), new_raw.into_bytes());
}

#[test]
fn startup_rolls_back_inflight_same_dir_long_rename_txn() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    set_rawname_xattr(&backend, long_name("rollback-old").as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_same_dir_long_rename_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        long_name("rollback-old").as_bytes(),
        long_name("rollback-new").as_bytes(),
        &[],
    );
    set_rawname_xattr(&backend, long_name("rollback-new").as_bytes());

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    assert_eq!(
        read_rawname_xattr(&backend),
        long_name("rollback-old").into_bytes()
    );
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn paused_long_rename_txn_records_backend_relative_parent_segments() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let parent_name = long_name("writer-long-parent");
    let src = long_name("writer-long-src");
    let dst = long_name("writer-long-dst");

    let parent = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new(&parent_name), 0o755).unwrap();
    let parent_backend = backend_name_for_ino_result(fs.as_ref(), parent.ino).unwrap();
    let created = create_result(fs.as_ref(), parent.ino, OsStr::new(&src), 0o644, libc::O_RDWR)
        .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let src_thread = src.clone();
    let dst_thread = dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            parent.ino,
            OsStr::new(&src_thread),
            parent.ino,
            OsStr::new(&dst_thread),
            0,
        )
    });

    pause.wait_until_blocked();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let record = read_txn_record(root_fd.as_fd())
        .unwrap()
        .expect("paused rename should persist an inflight txn");

    assert_eq!(record.old_parent_segments, vec![parent_backend.clone()]);
    assert_eq!(record.new_parent_segments, vec![parent_backend]);

    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn startup_rolls_back_inflight_long_rename_txn_under_long_parent_directory() {
    let tmp = TempDir::new();
    let long_parent_backend = tmp.path().join(".__ln2_obj_0000000000000001");
    let long_child_backend = long_parent_backend.join(".__ln2_obj_0000000000000002");
    let old_raw = long_name("rollback-old-under-long-parent");
    let new_raw = long_name("rollback-new-under-long-parent");
    let parent_raw = long_name("rollback-long-parent");

    fs::create_dir(&long_parent_backend).unwrap();
    set_rawname_xattr(&long_parent_backend, parent_raw.as_bytes());
    fs::write(&long_child_backend, b"payload").unwrap();
    set_rawname_xattr(&long_child_backend, new_raw.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (3u64).to_le_bytes()).unwrap();
    write_same_dir_long_rename_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000002",
        old_raw.as_bytes(),
        new_raw.as_bytes(),
        &[b".__ln2_obj_0000000000000001"],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    assert_eq!(read_rawname_xattr(&long_child_backend), old_raw.into_bytes());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn startup_fails_when_rollback_cannot_complete() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_same_dir_long_rename_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        long_name("rollback-missing-old").as_bytes(),
        long_name("rollback-missing-new").as_bytes(),
        &[],
    );
    fs::remove_file(&backend).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("startup should fail when rollback cannot complete"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EIO),
    }
    assert!(tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn startup_recovery_failure_emits_explicit_diagnostic() {
    const STARTUP_DIAGNOSTIC_BACKEND_ENV: &str = "LN2_STARTUP_DIAGNOSTIC_BACKEND";

    if let Some(path) = std::env::var_os(STARTUP_DIAGNOSTIC_BACKEND_ENV) {
        let config = Config::open_backend(path.into(), false, false).unwrap();
        match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
            Ok(_) => panic!("startup should fail when rollback cannot complete"),
            Err(err) => assert_eq!(core_err_to_errno(&err), libc::EIO),
        }
        return;
    }

    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    set_rawname_xattr(&backend, long_name("diagnostic-old").as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_same_dir_long_rename_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        long_name("diagnostic-old").as_bytes(),
        long_name("diagnostic-new").as_bytes(),
        &[],
    );
    fs::remove_file(&backend).unwrap();

    let output = run_current_test_with_env_and_capture_stderr(
        "v2::fs::tests::repair_semantics::startup_recovery_failure_emits_explicit_diagnostic",
        STARTUP_DIAGNOSTIC_BACKEND_ENV,
        tmp.path().as_os_str(),
    );
    assert!(
        output.status.success(),
        "child startup probe failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let text = String::from_utf8_lossy(&output.stderr);
    assert!(text.contains("recovery did not complete"));
    assert!(text.contains(".ln2_fs_txn"));
    assert!(tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn startup_fails_when_short_to_long_recovery_sees_both_old_and_new_entries() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src"), b"old-payload").unwrap();
    fs::write(
        tmp.path().join(".__ln2_obj_0000000000000001"),
        b"new-payload",
    )
    .unwrap();
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_short_to_long_txn(
        tmp.path(),
        b"short-src",
        b".__ln2_obj_0000000000000001",
        long_name("dual-entry-upgrade-dst").as_bytes(),
        &[],
        &[],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => {
            panic!("startup should fail when short-to-long recovery sees both old and new entries")
        }
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EIO),
    }
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    assert!(tmp.path().join("short-src").exists());
    assert!(tmp.path().join(".__ln2_obj_0000000000000001").exists());
}

#[test]
fn startup_rolls_back_inflight_unlink_long_txn() {
    let tmp = TempDir::new();
    let quarantine = tmp.path().join(".ln2_fs_delobj_0000000000000001");
    fs::write(&quarantine, b"payload").unwrap();
    set_rawname_xattr(&quarantine, long_name("restart-unlink-name").as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_unlink_long_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        b".ln2_fs_delobj_0000000000000001",
        long_name("restart-unlink-name").as_bytes(),
        &[],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    let restored = tmp.path().join(".__ln2_obj_0000000000000001");
    assert!(restored.exists());
    assert_eq!(
        read_rawname_xattr(&restored),
        long_name("restart-unlink-name").into_bytes()
    );
    assert!(!quarantine.exists());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn startup_rolls_back_inflight_remove_dir_txn() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join(".ln2_fs_deldir_0000000000000001")).unwrap();
    write_remove_dir_txn(
        tmp.path(),
        b"rmdir-name",
        b".ln2_fs_deldir_0000000000000001",
        &[],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    assert!(tmp.path().join("rmdir-name").exists());
    assert!(!tmp.path().join(".ln2_fs_deldir_0000000000000001").exists());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn startup_rolls_back_inflight_remove_dir_txn_for_long_directory() {
    let tmp = TempDir::new();
    let quarantine = tmp.path().join(".ln2_fs_deldir_0000000000000001");
    let long_dir_backend = tmp.path().join(".__ln2_obj_0000000000000001");
    let long_dir_raw = long_name("restart-rmdir-long-dir");

    fs::create_dir(&quarantine).unwrap();
    set_rawname_xattr(&quarantine, long_dir_raw.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_remove_dir_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        b".ln2_fs_deldir_0000000000000001",
        &[],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    assert!(long_dir_backend.exists());
    assert_eq!(read_rawname_xattr(&long_dir_backend), long_dir_raw.into_bytes());
    assert!(!quarantine.exists());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn init_rejects_committed_non_directory_long_object_with_bad_link_count() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    fs::hard_link(&backend, tmp.path().join(".__ln2_obj_0000000000000002")).unwrap();
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (3u64).to_le_bytes()).unwrap();
    set_rawname_xattr(&backend, long_name("linked-long").as_bytes());

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, 1024, None, IndexSync::Off) {
        Ok(_) => panic!("bad committed long-object link count should fail init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn init_rejects_malformed_idalloc_contents() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join(".ln2_fs_idalloc"), b"bad").unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off) {
        Ok(_) => panic!("malformed id allocator contents should fail init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn successful_long_create_advances_allocator_across_restart() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let created = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_name("persisted-id")),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let reopened = new_always_sync_fs(&tmp);
    let created_again = create_result(
        &reopened,
        ROOT_INODE,
        OsStr::new(&long_name("persisted-id-next")),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    assert_eq!(
        created_again.backend_name,
        b".__ln2_obj_0000000000000002".to_vec()
    );
}

#[test]
fn failed_upgrade_burns_allocated_id_before_next_success() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    fs::write(tmp.path().join("short-src-burn"), b"payload").unwrap();
    let fs = new_always_sync_fs(&tmp);

    {
        let _hook = force_txn_write_errno(libc::EIO);
        let err = rename_result(
            &fs,
            ROOT_INODE,
            OsStr::new("short-src-burn"),
            ROOT_INODE,
            OsStr::new(&long_name("burn-target")),
            0,
        )
        .unwrap_err();
        assert_eq!(err, libc::EIO);
    }

    let created = create_result(
        &fs,
        ROOT_INODE,
        OsStr::new(&long_name("after-burn-restartless")),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    assert_eq!(
        created.backend_name,
        b".__ln2_obj_0000000000000002".to_vec()
    );
}

#[test]
fn init_rejects_old_rtmp_residue() {
    let tmp = TempDir::new();
    fs::write(tmp.path().join(".ln2_fs_rtmp_deadbeef"), b"x").unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    match LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off) {
        Ok(_) => panic!("old rename midpoint residue should fail init"),
        Err(err) => assert_eq!(core_err_to_errno(&err), libc::EINVAL),
    }
}

#[test]
fn mount_and_rmdir_clean_up_stranded_delobj_residue() {
    let tmp = TempDir::new();
    let dir = tmp.path().join("residue-dir-mount-clean");
    fs::create_dir(&dir).unwrap();
    fs::write(dir.join(".ln2_fs_delobj_deadbeef"), b"x").unwrap();

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

    rmdir_result(&fs, ROOT_INODE, OsStr::new("residue-dir-mount-clean")).unwrap();
    assert!(!dir.exists());
}

#[test]
fn mount_and_rmdir_clean_up_stranded_deldir_residue() {
    let tmp = TempDir::new();
    let dir = tmp.path().join("residue-dir-mount-clean-2");
    fs::create_dir(&dir).unwrap();
    fs::create_dir(dir.join(".ln2_fs_deldir_deadbeef")).unwrap();

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

    rmdir_result(&fs, ROOT_INODE, OsStr::new("residue-dir-mount-clean-2")).unwrap();
    assert!(!dir.exists());
}

#[test]
fn mount_and_rmdir_clean_up_stable_long_residue_with_missing_or_malformed_rawname() {
    for case in ["missing", "malformed"] {
        let tmp = TempDir::new();
        let dir = tmp.path().join(format!("stable-residue-{case}"));
        fs::create_dir(&dir).unwrap();
        let child = dir.join(".__ln2_obj_0000000000000001");
        fs::write(&child, b"payload").unwrap();
        if case == "malformed" {
            set_rawname_xattr(&child, b"short");
        }

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

        rmdir_result(&fs, ROOT_INODE, OsStr::new(dir.file_name().unwrap())).unwrap();
        assert!(!dir.exists());
    }
}

#[test]
fn startup_recovery_covers_every_supported_transaction_kind() {
    let _serial = lock_test_hooks();
    let create_backend_name = ".__ln2_obj_0000000000000001";
    let create_temp_name = ".ln2_fs_ctmp_seeded_create_long";

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
        fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

        match case {
            "create_long" => {
                write_create_long_txn(
                    tmp.path(),
                    create_backend_name.as_bytes(),
                    long_name("restart-create-old").as_bytes(),
                    &[],
                );
                fs::write(tmp.path().join(create_backend_name), b"payload").unwrap();
                fs::write(tmp.path().join(create_temp_name), b"staged-payload").unwrap();
            }
            "create_short" => {
                write_create_short_txn(tmp.path(), b"short-create", &[]);
                fs::write(tmp.path().join("short-create"), b"payload").unwrap();
            }
            "link_short" => {
                let source = tmp.path().join("short-link-src");
                fs::write(&source, b"payload").unwrap();
                fs::hard_link(&source, tmp.path().join("short-link-dst")).unwrap();
                write_link_short_txn(
                    tmp.path(),
                    b"short-link-src",
                    b"short-link-dst",
                    &[],
                    &[],
                );
            }
            "rename_short_to_short" => {
                let source = tmp.path().join("short-rename-src");
                let destination = tmp.path().join("short-rename-dst");
                fs::write(&source, b"payload").unwrap();
                write_short_to_short_txn(
                    tmp.path(),
                    b"short-rename-src",
                    b"short-rename-dst",
                    &[],
                    &[],
                );
                fs::rename(source, destination).unwrap();
            }
            "rename_short_to_long" => {
                let short = tmp.path().join("short-src");
                fs::write(&short, b"payload").unwrap();
                write_short_to_long_txn(
                    tmp.path(),
                    b"short-src",
                    b".__ln2_obj_0000000000000001",
                    long_name("restart-upgrade-dst").as_bytes(),
                    &[],
                    &[],
                );
                set_rawname_xattr(&short, long_name("restart-upgrade-dst").as_bytes());
            }
            "rename_long_to_short" => {
                fs::write(tmp.path().join("restart-short"), b"payload").unwrap();
                write_long_to_short_txn(
                    tmp.path(),
                    b".__ln2_obj_0000000000000001",
                    long_name("restart-long-src").as_bytes(),
                    b"restart-short",
                    &[],
                    &[],
                );
            }
            "rename_long_to_long_same_dir" => {
                let backend = tmp.path().join(".__ln2_obj_0000000000000001");
                fs::write(&backend, b"payload").unwrap();
                set_rawname_xattr(&backend, long_name("restart-new-name").as_bytes());
                write_same_dir_long_rename_txn(
                    tmp.path(),
                    b".__ln2_obj_0000000000000001",
                    long_name("restart-old-name").as_bytes(),
                    long_name("restart-new-name").as_bytes(),
                    &[],
                );
            }
            "rename_long_to_long_cross_dir" => {
                fs::create_dir(tmp.path().join("src")).unwrap();
                fs::create_dir(tmp.path().join("dst")).unwrap();
                let moved = tmp.path().join("dst").join(".__ln2_obj_0000000000000001");
                fs::write(&moved, b"payload").unwrap();
                set_rawname_xattr(&moved, long_name("restart-cross-new").as_bytes());
                write_cross_dir_long_rename_txn(
                    tmp.path(),
                    b".__ln2_obj_0000000000000001",
                    long_name("restart-cross-old").as_bytes(),
                    long_name("restart-cross-new").as_bytes(),
                    &[b"src"],
                    &[b"dst"],
                );
            }
            "unlink_long" => {
                let quarantine = tmp.path().join(".ln2_fs_delobj_0000000000000001");
                fs::write(&quarantine, b"payload").unwrap();
                set_rawname_xattr(&quarantine, long_name("restart-unlink-name").as_bytes());
                write_unlink_long_txn(
                    tmp.path(),
                    b".__ln2_obj_0000000000000001",
                    b".ln2_fs_delobj_0000000000000001",
                    long_name("restart-unlink-name").as_bytes(),
                    &[],
                );
            }
            "unlink_short" => {
                let source = tmp.path().join("short-unlink-src");
                let quarantine = tmp.path().join(".ln2_fs_delobj_short_seed");
                fs::write(&source, b"payload").unwrap();
                fs::rename(&source, &quarantine).unwrap();
                write_unlink_short_txn(
                    tmp.path(),
                    b"short-unlink-src",
                    b".ln2_fs_delobj_short_seed",
                    &[],
                );
            }
            "remove_dir" => {
                fs::create_dir(tmp.path().join(".ln2_fs_deldir_0000000000000001")).unwrap();
                write_remove_dir_txn(
                    tmp.path(),
                    b"rmdir-name",
                    b".ln2_fs_deldir_0000000000000001",
                    &[],
                );
            }
            _ => unreachable!(),
        }

        match case {
            "create_long" => {
                assert!(tmp.path().join(create_backend_name).exists());
                assert!(
                    tmp.path().join(create_temp_name).exists(),
                    "create_long matrix case must seed both final and temp entries"
                );
            }
            "create_short" => {
                assert!(
                    tmp.path().join("short-create").exists(),
                    "create_short matrix case must seed the staged short entry"
                );
            }
            "link_short" => {
                assert!(
                    tmp.path().join("short-link-src").exists(),
                    "link_short matrix case must keep the original short entry present"
                );
                assert!(
                    tmp.path().join("short-link-dst").exists(),
                    "link_short matrix case must seed the new hardlink entry"
                );
            }
            "rename_short_to_short" => {
                assert!(
                    !tmp.path().join("short-rename-src").exists(),
                    "rename_short_to_short matrix case must remove the old short entry"
                );
                assert!(
                    tmp.path().join("short-rename-dst").exists(),
                    "rename_short_to_short matrix case must seed the renamed short entry"
                );
            }
            "rename_short_to_long" => {
                let short = tmp.path().join("short-src");
                assert!(
                    short.exists(),
                    "rename_short_to_long matrix case must leave the old short entry present"
                );
                assert_eq!(
                    maybe_read_rawname_xattr(&short),
                    Some(long_name("restart-upgrade-dst").into_bytes()),
                    "rename_short_to_long matrix case must seed staged rawname on the old short entry"
                );
                assert!(
                    !tmp.path().join(".__ln2_obj_0000000000000001").exists(),
                    "rename_short_to_long matrix case must keep the stable long entry absent"
                );
            }
            _ => {}
        }

        let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
        let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

        match case {
            "create_long" => {
                assert!(!tmp.path().join(create_backend_name).exists());
                assert!(!tmp.path().join(create_temp_name).exists());
            }
            "create_short" => {
                assert!(!tmp.path().join("short-create").exists());
            }
            "link_short" => {
                assert!(tmp.path().join("short-link-src").exists());
                assert!(!tmp.path().join("short-link-dst").exists());
            }
            "rename_short_to_short" => {
                assert!(tmp.path().join("short-rename-src").exists());
                assert!(!tmp.path().join("short-rename-dst").exists());
            }
            "rename_short_to_long" => {
                assert!(tmp.path().join("short-src").exists());
                assert!(!tmp.path().join(".__ln2_obj_0000000000000001").exists());
                assert_eq!(
                    maybe_read_rawname_xattr(&tmp.path().join("short-src")),
                    None
                );
            }
            "rename_long_to_short" => {
                let restored = tmp.path().join(".__ln2_obj_0000000000000001");
                assert!(restored.exists());
                assert_eq!(
                    read_rawname_xattr(&restored),
                    long_name("restart-long-src").into_bytes()
                );
                assert!(!tmp.path().join("restart-short").exists());
            }
            "rename_long_to_long_same_dir" => {
                assert_eq!(
                    read_rawname_xattr(&tmp.path().join(".__ln2_obj_0000000000000001")),
                    long_name("restart-old-name").into_bytes(),
                );
            }
            "rename_long_to_long_cross_dir" => {
                assert!(
                    tmp.path()
                        .join("src")
                        .join(".__ln2_obj_0000000000000001")
                        .exists()
                );
                assert_eq!(
                    read_rawname_xattr(&tmp.path().join("src").join(".__ln2_obj_0000000000000001")),
                    long_name("restart-cross-old").into_bytes(),
                );
                assert!(
                    !tmp.path()
                        .join("dst")
                        .join(".__ln2_obj_0000000000000001")
                        .exists()
                );
            }
            "unlink_long" => {
                let restored = tmp.path().join(".__ln2_obj_0000000000000001");
                assert!(restored.exists());
                assert_eq!(
                    read_rawname_xattr(&restored),
                    long_name("restart-unlink-name").into_bytes()
                );
                assert!(!tmp.path().join(".ln2_fs_delobj_0000000000000001").exists());
            }
            "unlink_short" => {
                assert!(tmp.path().join("short-unlink-src").exists());
                assert!(!tmp.path().join(".ln2_fs_delobj_short_seed").exists());
            }
            "remove_dir" => {
                assert!(tmp.path().join("rmdir-name").exists());
                assert!(!tmp.path().join(".ln2_fs_deldir_0000000000000001").exists());
            }
            _ => unreachable!(),
        }
    }
}

#[test]
fn rmdir_cleans_invalid_long_object_residue_and_succeeds() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let _dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("ghost-dir"), 0o755).unwrap();
    let child_path = tmp.path().join("ghost-dir");
    fs::write(child_path.join(".__ln2_obj_badbadbadbadbad"), b"x").unwrap();

    rmdir_result(&fs, ROOT_INODE, OsStr::new("ghost-dir")).unwrap();
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("ghost-dir")).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn rmdir_cleans_stable_long_object_with_missing_rawname_residue() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let _dir = mkdir_result(
        &fs,
        ROOT_INODE,
        OsStr::new("ghost-dir-missing-xattr"),
        0o755,
    )
    .unwrap();
    let child_path = tmp
        .path()
        .join("ghost-dir-missing-xattr")
        .join(".__ln2_obj_0000000000000001");
    fs::write(&child_path, b"x").unwrap();

    rmdir_result(&fs, ROOT_INODE, OsStr::new("ghost-dir-missing-xattr")).unwrap();
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("ghost-dir-missing-xattr")).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn rmdir_cleans_stable_long_object_with_malformed_rawname_residue() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let _dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("ghost-dir-bad-xattr"), 0o755).unwrap();
    let child_path = tmp
        .path()
        .join("ghost-dir-bad-xattr")
        .join(".__ln2_obj_0000000000000001");
    fs::write(&child_path, b"x").unwrap();
    set_rawname_xattr(&child_path, b"short");

    rmdir_result(&fs, ROOT_INODE, OsStr::new("ghost-dir-bad-xattr")).unwrap();
    assert_eq!(
        lookup_entry_result(&fs, ROOT_INODE, OsStr::new("ghost-dir-bad-xattr")).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn rmdir_leaves_valid_committed_long_child_as_enotempty() {
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("busy-dir"), 0o755).unwrap();
    let name = long_name("valid-child");
    let created = create_result(&fs, dir.ino, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let err = rmdir_result(&fs, ROOT_INODE, OsStr::new("busy-dir")).unwrap_err();
    assert_eq!(err, libc::ENOTEMPTY);
}

#[test]
fn mount_tolerates_stranded_delobj_residue_and_rmdir_cleans_it() {
    let tmp = TempDir::new();
    let dir = tmp.path().join("residue-dir");
    fs::create_dir(&dir).unwrap();
    fs::write(dir.join(".ln2_fs_delobj_deadbeef"), b"x").unwrap();

    let fs = new_always_sync_fs(&tmp);
    rmdir_result(&fs, ROOT_INODE, OsStr::new("residue-dir")).unwrap();
    assert!(!dir.exists());
}

#[test]
fn mount_tolerates_stranded_deldir_residue_and_rmdir_cleans_it() {
    let tmp = TempDir::new();
    let dir = tmp.path().join("residue-dir-2");
    fs::create_dir(&dir).unwrap();
    fs::create_dir(dir.join(".ln2_fs_deldir_deadbeef")).unwrap();

    let fs = new_always_sync_fs(&tmp);
    rmdir_result(&fs, ROOT_INODE, OsStr::new("residue-dir-2")).unwrap();
    assert!(!dir.exists());
}

#[test]
fn init_cleans_directory_shaped_create_tmp_residue() {
    let tmp = TempDir::new();
    fs::create_dir(tmp.path().join(".ln2_fs_ctmp_deadbeef")).unwrap();

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, 1024, None, IndexSync::Off).unwrap();

    assert!(!tmp.path().join(".ln2_fs_ctmp_deadbeef").exists());
}

#[test]
fn long_create_backend_remains_mountable_after_restart() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let name = format!(
        "restart-create-{}",
        "x".repeat(crate::v2::path::MAX_SEGMENT_ON_DISK + 8)
    );

    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let reopened = new_longname_test_fs(&tmp, false, false);
    let reopened_entry = lookup_entry_result(&reopened, ROOT_INODE, OsStr::new(&name)).unwrap();
    assert_eq!(reopened_entry.backend_name, created.backend_name);
}

#[test]
fn long_same_dir_rename_backend_remains_mountable_after_restart() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let src = format!(
        "restart-rename-src-{}",
        "x".repeat(crate::v2::path::MAX_SEGMENT_ON_DISK + 8)
    );
    let dst = format!(
        "restart-rename-dst-{}",
        "x".repeat(crate::v2::path::MAX_SEGMENT_ON_DISK + 8)
    );

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
    drop(fs);

    let reopened = new_longname_test_fs(&tmp, false, false);
    let reopened_entry = lookup_entry_result(&reopened, ROOT_INODE, OsStr::new(&dst)).unwrap();
    assert_eq!(reopened_entry.backend_name, created.backend_name);
}

#[test]
fn committed_long_entry_remains_visible_across_remount_with_lower_max_name_len() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let name = long_name("remount-visible");

    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let remounted = new_fs_with_max_name_len(&tmp, MAX_SEGMENT_ON_DISK);
    let entry = lookup_entry_result(&remounted, ROOT_INODE, OsStr::new(&name)).unwrap();
    assert_eq!(entry.backend_name, created.backend_name);

    let names = readdir_names_result(&remounted, ROOT_INODE).unwrap();
    assert!(names.iter().any(|candidate| candidate == OsStr::new(&name)));
}

#[test]
fn same_object_long_rename_is_allowed_after_lower_max_name_len_remount() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let name = long_name("remount-same-object");

    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let remounted = new_fs_with_max_name_len(&tmp, MAX_SEGMENT_ON_DISK);
    rename_result(
        &remounted,
        ROOT_INODE,
        OsStr::new(&name),
        ROOT_INODE,
        OsStr::new(&name),
        0,
    )
    .unwrap();

    let reopened = lookup_entry_result(&remounted, ROOT_INODE, OsStr::new(&name)).unwrap();
    assert_eq!(reopened.backend_name, created.backend_name);
}

#[test]
fn committed_short_file_remains_operable_after_lower_max_name_len_remount() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let name = committed_short_name("short-file-remount");

    let created = create_result(&fs, ROOT_INODE, OsStr::new(&name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let remounted = new_fs_with_max_name_len(&tmp, 128);
    let names = readdir_names_result(&remounted, ROOT_INODE).unwrap();
    assert!(names.iter().any(|candidate| candidate == OsStr::new(&name)));

    let entry = lookup_entry_result(&remounted, ROOT_INODE, OsStr::new(&name)).unwrap();
    let opened = open_result(&remounted, entry.ino, libc::O_RDONLY as u32).unwrap();
    release_result(&remounted, entry.ino, opened.fh).unwrap();
    unlink_result(&remounted, ROOT_INODE, OsStr::new(&name)).unwrap();
    assert_eq!(
        lookup_entry_result(&remounted, ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn committed_short_directory_traversal_and_child_mutation_survive_lower_max_name_len_remount() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let dir_name = committed_short_name("short-dir-remount");
    let child_name = "child.txt";

    let dir = mkdir_result(&fs, ROOT_INODE, OsStr::new(&dir_name), 0o755).unwrap();
    let created = create_result(&fs, dir.ino, OsStr::new(child_name), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let remounted = new_fs_with_max_name_len(&tmp, 128);
    let root_names = readdir_names_result(&remounted, ROOT_INODE).unwrap();
    assert!(root_names.iter().any(|candidate| candidate == OsStr::new(&dir_name)));

    let dir_entry = lookup_entry_result(&remounted, ROOT_INODE, OsStr::new(&dir_name)).unwrap();
    let child = lookup_entry_result(&remounted, dir_entry.ino, OsStr::new(child_name)).unwrap();
    let opened = open_result(&remounted, child.ino, libc::O_RDONLY as u32).unwrap();
    release_result(&remounted, child.ino, opened.fh).unwrap();
    unlink_result(&remounted, dir_entry.ino, OsStr::new(child_name)).unwrap();
    assert_eq!(
        lookup_entry_result(&remounted, dir_entry.ino, OsStr::new(child_name)).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn startup_recovery_accepts_long_txn_rawnames_across_lower_max_name_len_remount() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    let old_raw = long_name("remount-recovery-old");
    let new_raw = long_name("remount-recovery-new");
    fs::write(&backend, b"payload").unwrap();
    set_rawname_xattr(&backend, new_raw.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();
    write_same_dir_long_rename_txn(
        tmp.path(),
        b".__ln2_obj_0000000000000001",
        old_raw.as_bytes(),
        new_raw.as_bytes(),
        &[],
    );

    let config = Config::open_backend(tmp.path().clone(), false, false).unwrap();
    let _core = LongNameFsCore::new(config, MAX_SEGMENT_ON_DISK, None, IndexSync::Off).unwrap();

    assert_eq!(read_rawname_xattr(&backend), old_raw.into_bytes());
    assert!(!tmp.path().join(".ln2_fs_txn").exists());
}

#[test]
fn rmdir_keeps_committed_long_child_across_remount_with_lower_max_name_len() {
    let tmp = TempDir::new();
    let fs = new_longname_test_fs(&tmp, false, false);
    let dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("remount-busy-dir"), 0o755).unwrap();
    let child = long_name("remount-busy-child");

    let created = create_result(&fs, dir.ino, OsStr::new(&child), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();
    drop(fs);

    let remounted = new_fs_with_max_name_len(&tmp, MAX_SEGMENT_ON_DISK);
    let dir_entry = lookup_entry_result(&remounted, ROOT_INODE, OsStr::new("remount-busy-dir"))
        .unwrap();
    let err = rmdir_result(&remounted, ROOT_INODE, OsStr::new("remount-busy-dir")).unwrap_err();
    assert_eq!(err, libc::ENOTEMPTY);
    assert!(lookup_entry_result(&remounted, dir_entry.ino, OsStr::new(&child)).is_ok());
}

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
    let name = CString::new(format_long_object_name(1)).unwrap();
    let _file_fd = nix::fcntl::openat(
        dir_fd.as_fd(),
        name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();

    let raw = format!("opath-probe-{}", "x".repeat(MAX_SEGMENT_ON_DISK + 8)).into_bytes();
    set_internal_rawname_at(dir_fd.as_fd(), name.as_c_str(), &raw).unwrap();

    if !opath_rawname_ebadf(dir_fd.as_fd(), name.as_c_str()) {
        return;
    }

    OPATH_XATTR_WARNED.store(false, Ordering::Relaxed);
    let (index, stderr) = capture_stderr(|| {
        rebuild_dir_index_from_backend(dir_fd.as_fd(), MAX_SEGMENT_ON_DISK + 32).unwrap()
    });
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
        let name = CString::new(format_long_object_name((i + 1) as u64)).unwrap();
        let _fd = nix::fcntl::openat(
            dir_fd.as_fd(),
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap();
        let raw = format!("parallel-{}", "x".repeat(MAX_SEGMENT_ON_DISK + 8 + i)).into_bytes();
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
    let index = rebuild_dir_index_from_backend(dir_fd.as_fd(), 1024).unwrap();
    assert!(
        parallel_rebuild_dup_helper_calls() > 0,
        "parallel rebuild must call dup helper at worker call-site"
    );

    let probe = format_long_object_name(1);
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
        let name = CString::new(format_long_object_name((i + 1) as u64)).unwrap();
        let _fd = nix::fcntl::openat(
            dir_fd.as_fd(),
            name.as_c_str(),
            OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o600),
        )
        .unwrap();
        let raw = format!("fallback-{}", "x".repeat(MAX_SEGMENT_ON_DISK + 8 + i)).into_bytes();
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
    let index = rebuild_dir_index_from_backend(dir_fd.as_fd(), 1024).unwrap();

    assert!(
        parallel_rebuild_dup_helper_calls() > 0,
        "test setup should exercise dup helper attempts"
    );
    let probe = format_long_object_name(1);
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
    let malformed_raw = b".__ln2_reserved-logical".to_vec();
    let long_name = CString::new(format_long_object_name(1)).unwrap();
    let _fd = nix::fcntl::openat(
        root_fd.as_fd(),
        long_name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    set_internal_rawname_at(root_fd.as_fd(), long_name.as_c_str(), &malformed_raw).unwrap();

    drop(core);

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
    let long_name = CString::new(format_long_object_name(1)).unwrap();
    let _fd = nix::fcntl::openat(
        root_fd.as_fd(),
        long_name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();

    drop(core);

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
fn malformed_stable_backend_residue_stays_invisible_through_rebuild_and_readdir() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let core = new_longname_test_core(&tmp, false);
    let root_fd = core.cached_root_fd().unwrap();
    let logical = format!("malformed-residue-{}", "x".repeat(MAX_SEGMENT_ON_DISK + 8));
    let backend_name = CString::new(".__ln2_obj_badbadbadbadbad").unwrap();
    let _fd = nix::fcntl::openat(
        root_fd.as_fd(),
        backend_name.as_c_str(),
        OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_WRONLY | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .unwrap();
    set_internal_rawname_at(root_fd.as_fd(), backend_name.as_c_str(), logical.as_bytes()).unwrap();

    let index = rebuild_dir_index_from_backend(root_fd.as_fd(), 1024).unwrap();
    assert!(
        !index.contains_key(backend_name.as_bytes()),
        "malformed stable backend residue must not be indexed as committed"
    );
    assert!(
        index.backend_for_raw(logical.as_bytes()).is_none(),
        "malformed stable backend residue must not claim a logical name"
    );

    drop(core);

    let fs = new_longname_test_fs(&tmp, false, false);
    let names = readdir_names_result(&fs, ROOT_INODE).unwrap();
    assert!(
        !names
            .iter()
            .any(|name| name.as_os_str() == OsStr::new(&logical)),
        "malformed stable backend residue must stay invisible in readdir"
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
    let backend = CString::new(format_long_object_name(1)).unwrap();
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
fn readdir_rebuild_discovers_stable_long_entries_by_rawname() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    let rawname = long_name("very-long-entry-name");
    set_rawname_xattr(&backend, rawname.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let names = readdir_names_result(&fs, ROOT_INODE).unwrap();

    assert!(names.iter().any(|n| n.as_os_str() == OsStr::new(&rawname)));
}

#[test]
fn lookup_does_not_probe_hash_named_candidates_for_long_entries() {
    let tmp = TempDir::new();
    let backend = tmp.path().join(".__ln2_obj_0000000000000001");
    fs::write(&backend, b"payload").unwrap();
    let rawname = long_name("hashless-long-name");
    set_rawname_xattr(&backend, rawname.as_bytes());
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    write_dir_index(root_fd.as_fd(), &DirIndex::new()).unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let entry = lookup_entry_result(&fs, ROOT_INODE, OsStr::new(&rawname)).unwrap();

    assert_eq!(
        backend_name_for_ino_result(&fs, entry.ino).unwrap(),
        b".__ln2_obj_0000000000000001".to_vec()
    );
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
        let name = CString::new(format_long_object_name((i + 1) as u64)).unwrap();
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
            format!("fail-{}", "x".repeat(MAX_SEGMENT_ON_DISK + 8 + i)).as_bytes(),
        )
        .unwrap();
        names.push(name);
    }

    let _guard = force_parallel_rebuild_dup_fail();
    let _hook = force_internal_rawname_errno(libc::EIO);
    let err = rebuild_dir_index_from_backend(root_fd.as_fd(), 1024).unwrap_err();

    assert_eq!(core_err_to_errno(&err), libc::EIO);
}

#[test]
fn pause_next_txn_before_clear_blocks_until_released() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src = long_name("pause-src");
    let dst = long_name("pause-dst");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new(&src),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(&fs);
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

    pause.wait_until_blocked();
    assert!(tmp.path().join(".ln2_fs_txn").exists());
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn lookup_does_not_observe_uncommitted_long_rename_state() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src = long_name("iso-src");
    let dst = long_name("iso-dst");
    let created = create_result(&fs, ROOT_INODE, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(&fs);
    let fs = std::sync::Arc::new(fs);
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

    pause.wait_until_blocked();
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&src)).is_ok());
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).is_err());
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn readdir_does_not_observe_uncommitted_cross_dir_move() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = new_always_sync_fs(&tmp);
    let src_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("src"), 0o755).unwrap();
    let dst_dir = mkdir_result(&fs, ROOT_INODE, OsStr::new("dst"), 0o755).unwrap();
    let src = long_name("iso-move-src");
    let dst = long_name("iso-move-dst");
    let created = create_result(&fs, src_dir.ino, OsStr::new(&src), 0o644, libc::O_RDWR).unwrap();
    release_result(&fs, created.ino, created.fh).unwrap();

    let mut pause = pause_next_txn_before_clear(&fs);
    let fs = std::sync::Arc::new(fs);
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

    pause.wait_until_blocked();
    assert!(
        readdir_names_result(fs.as_ref(), src_dir.ino)
            .unwrap()
            .iter()
            .any(|n| n == OsStr::new(&src))
    );
    assert!(
        !readdir_names_result(fs.as_ref(), dst_dir.ino)
            .unwrap()
            .iter()
            .any(|n| n == OsStr::new(&dst))
    );
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn lookup_does_not_observe_uncommitted_long_unlink_state() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let name = long_name("iso-unlink");
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

    pause.wait_until_blocked();
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).is_ok());
    assert!(
        readdir_names_result(fs.as_ref(), ROOT_INODE)
            .unwrap()
            .iter()
            .any(|entry| entry == OsStr::new(&name))
    );
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn lookup_does_not_observe_active_long_create_state_on_cold_cache() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let name = long_name("iso-create");

    let mut pause = pause_next_post_commit_flush(fs.as_ref());
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

    pause.wait_until_blocked();
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).unwrap_err(),
        libc::ENOENT
    );
    assert!(
        !readdir_names_result(fs.as_ref(), ROOT_INODE)
            .unwrap()
            .iter()
            .any(|entry| entry == OsStr::new(&name))
    );
    pause.release();
    let created = handle.join().unwrap().unwrap();
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&name)).is_ok());
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();
}

#[test]
fn writer_waits_for_preexisting_directory_reader_before_starting_txn() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src = long_name("race-src");
    let dst = long_name("race-dst");
    let created = create_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new(&src),
        0o644,
        libc::O_RDWR,
    )
    .unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let _reader = hold_dir_read_guard(fs.as_ref(), ROOT_INODE).unwrap();
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

    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
    while std::time::Instant::now() < deadline {
        assert!(
            !tmp.path().join(".ln2_fs_txn").exists(),
            "writer must not start its transaction while a preexisting reader is active"
        );
        assert!(
            !handle.is_finished(),
            "writer must wait for the preexisting reader before entering the txn path"
        );
        std::thread::yield_now();
    }

    drop(_reader);
    pause.wait_until_blocked();
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn lookup_while_writer_active_uses_committed_backend_truth_not_stale_index() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let truth = long_name("truth");
    let stale = long_name("stale");
    let rename_src = long_name("truth-rename-src");
    let rename_dst = long_name("truth-rename-dst");
    let truth_backend = b".__ln2_obj_0000000000000001";
    let rename_backend = b".__ln2_obj_0000000000000002";

    fs::write(
        tmp.path().join(OsStr::from_bytes(truth_backend)),
        b"truth-payload",
    )
    .unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(truth_backend)),
        truth.as_bytes(),
    );
    fs::write(
        tmp.path().join(OsStr::from_bytes(rename_backend)),
        b"rename-payload",
    )
    .unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(rename_backend)),
        rename_src.as_bytes(),
    );
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (3u64).to_le_bytes()).unwrap();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut stale_index = DirIndex::new();
    stale_index.upsert(truth_backend.to_vec(), stale.as_bytes().to_vec());
    stale_index.upsert(rename_backend.to_vec(), rename_src.as_bytes().to_vec());
    write_dir_index(root_fd.as_fd(), &stale_index).unwrap();

    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let rename_src_thread = rename_src.clone();
    let rename_dst_thread = rename_dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&rename_src_thread),
            ROOT_INODE,
            OsStr::new(&rename_dst_thread),
            0,
        )
    });

    pause.wait_until_blocked();
    assert!(lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&truth)).is_ok());
    assert_eq!(
        lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&stale)).unwrap_err(),
        libc::ENOENT
    );
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn readdir_while_writer_active_uses_committed_backend_truth_not_stale_index() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let truth = long_name("readdir-truth");
    let stale = long_name("readdir-stale");
    let rename_src = long_name("readdir-rename-src");
    let rename_dst = long_name("readdir-rename-dst");
    let truth_backend = b".__ln2_obj_0000000000000001";
    let rename_backend = b".__ln2_obj_0000000000000002";

    fs::write(tmp.path().join("short-committed"), b"short").unwrap();
    fs::write(
        tmp.path().join(OsStr::from_bytes(truth_backend)),
        b"truth-payload",
    )
    .unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(truth_backend)),
        truth.as_bytes(),
    );
    fs::write(
        tmp.path().join(OsStr::from_bytes(rename_backend)),
        b"rename-payload",
    )
    .unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(rename_backend)),
        rename_src.as_bytes(),
    );
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (3u64).to_le_bytes()).unwrap();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut stale_index = DirIndex::new();
    stale_index.upsert(truth_backend.to_vec(), stale.as_bytes().to_vec());
    stale_index.upsert(rename_backend.to_vec(), rename_src.as_bytes().to_vec());
    write_dir_index(root_fd.as_fd(), &stale_index).unwrap();

    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let mut pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let rename_src_thread = rename_src.clone();
    let rename_dst_thread = rename_dst.clone();
    let handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            ROOT_INODE,
            OsStr::new(&rename_src_thread),
            ROOT_INODE,
            OsStr::new(&rename_dst_thread),
            0,
        )
    });

    pause.wait_until_blocked();
    let names = readdir_names_result(fs.as_ref(), ROOT_INODE).unwrap();
    assert!(
        names
            .iter()
            .any(|name| name == OsStr::new("short-committed"))
    );
    assert!(names.iter().any(|name| name == OsStr::new(&truth)));
    assert!(names.iter().any(|name| name == OsStr::new(&rename_src)));
    assert!(!names.iter().any(|name| name == OsStr::new(&stale)));
    assert!(!names.iter().any(|name| name == OsStr::new(&rename_dst)));
    pause.release();
    handle.join().unwrap().unwrap();
}

#[test]
fn readdir_uses_xattr_truth_even_when_stable_entry_hits_stale_index() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let truth = long_name("steady-readdir-truth");
    let stale = long_name("steady-readdir-stale");
    let backend = b".__ln2_obj_0000000000000001";

    fs::write(tmp.path().join(OsStr::from_bytes(backend)), b"payload").unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(backend)),
        truth.as_bytes(),
    );
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut stale_index = DirIndex::new();
    stale_index.upsert(backend.to_vec(), stale.as_bytes().to_vec());
    write_dir_index(root_fd.as_fd(), &stale_index).unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let names = readdir_names_result(&fs, ROOT_INODE).unwrap();
    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();

    assert!(names.iter().any(|name| name == OsStr::new(&truth)));
    assert!(!names.iter().any(|name| name == OsStr::new(&stale)));
    assert!(
        snapshot.dirty,
        "stale index repair should dirty recoverable state"
    );
    assert!(
        snapshot.pending > 0,
        "stale index repair should record pending work"
    );
}

#[test]
fn paused_rename_does_not_let_next_txn_capture_stale_parent_segments() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("race-src"), 0o755).unwrap();
    let dst_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("race-dst"), 0o755).unwrap();
    let moved = mkdir_result(
        fs.as_ref(),
        src_dir.ino,
        OsStr::new("short-src"),
        0o755,
    )
    .unwrap();

    let mut rename_pause = pause_next_rename_post_commit(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let rename_handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            src_dir.ino,
            OsStr::new("short-src"),
            dst_dir.ino,
            OsStr::new("short-dst"),
            0,
        )
    });

    assert!(rename_pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    fs::create_dir(tmp.path().join("race-src").join("short-src")).unwrap();

    let mut create_pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let create_handle = std::thread::spawn(move || {
        create_result(
            fs_thread.as_ref(),
            moved.ino,
            OsStr::new("stale-child"),
            0o644,
            libc::O_RDWR,
        )
    });

    assert!(
        !create_pause.wait_until_blocked_timeout(Duration::from_millis(200)),
        "next mutator must stay blocked until rename bookkeeping refreshes inode_store"
    );

    rename_pause.release();
    rename_handle.join().unwrap().unwrap();

    assert!(create_pause.wait_until_blocked_timeout(Duration::from_secs(1)));

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let txn = read_txn_record(root_fd.as_fd())
        .unwrap()
        .expect("paused create should leave txn record visible before clear completes");
    assert_eq!(txn.new_parent_segments, vec![b"race-dst".to_vec(), b"short-dst".to_vec()]);

    create_pause.release();
    let created = create_handle.join().unwrap().unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let moved_dst = lookup_entry_result(fs.as_ref(), dst_dir.ino, OsStr::new("short-dst")).unwrap();
    let moved_names = readdir_names_result(fs.as_ref(), moved_dst.ino).unwrap();
    assert!(moved_names.iter().any(|name| name == OsStr::new("stale-child")));
}

#[test]
fn short_to_long_directory_rename_records_stable_long_parent_segments_for_later_mutation() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let dst = long_name("rename-upgrade-parent-dst");
    let moved_parent = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("short-src"), 0o755).unwrap();
    let old_backend = backend_name_for_ino_result(fs.as_ref(), moved_parent.ino).unwrap();

    rename_result(
        fs.as_ref(),
        ROOT_INODE,
        OsStr::new("short-src"),
        ROOT_INODE,
        OsStr::new(&dst),
        0,
    )
    .unwrap();

    let stable_backend = fs::read_dir(tmp.path())
        .unwrap()
        .find_map(|entry| {
            let name = entry.ok()?.file_name();
            let bytes = name.as_os_str().as_bytes().to_vec();
            bytes.starts_with(b".__ln2_obj_").then_some(bytes)
        })
        .expect("short->long rename should commit a stable long backend basename");
    assert_ne!(stable_backend, old_backend);
    assert_eq!(
        backend_name_for_ino_result(fs.as_ref(), moved_parent.ino).unwrap(),
        stable_backend,
        "rename bookkeeping must refresh the already-issued directory inode to the committed long backend name"
    );

    let mut create_pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let create_handle = std::thread::spawn(move || {
        create_result(
            fs_thread.as_ref(),
            moved_parent.ino,
            OsStr::new("nested-child"),
            0o644,
            libc::O_RDWR,
        )
    });

    assert!(create_pause.wait_until_blocked_timeout(Duration::from_secs(1)));

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let txn = read_txn_record(root_fd.as_fd())
        .unwrap()
        .expect("paused create should leave txn record visible before clear completes");
    assert_eq!(txn.new_parent_segments, vec![stable_backend.clone()]);

    create_pause.release();
    let created = create_handle.join().unwrap().unwrap();
    release_result(fs.as_ref(), created.ino, created.fh).unwrap();

    let moved_dst = lookup_entry_result(fs.as_ref(), ROOT_INODE, OsStr::new(&dst)).unwrap();
    let moved_names = readdir_names_result(fs.as_ref(), moved_dst.ino).unwrap();
    assert!(moved_names.iter().any(|name| name == OsStr::new("nested-child")));
}

#[test]
fn queued_rmdir_uses_updated_parent_segments_after_parent_rename() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let fs = std::sync::Arc::new(new_always_sync_fs(&tmp));
    let src_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("race-src"), 0o755).unwrap();
    let dst_dir = mkdir_result(fs.as_ref(), ROOT_INODE, OsStr::new("race-dst"), 0o755).unwrap();
    let moved_parent =
        mkdir_result(fs.as_ref(), src_dir.ino, OsStr::new("short-src"), 0o755).unwrap();
    mkdir_result(fs.as_ref(), moved_parent.ino, OsStr::new("victim"), 0o755).unwrap();

    let mut rename_pause = pause_next_rename_post_commit(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let rename_handle = std::thread::spawn(move || {
        rename_result(
            fs_thread.as_ref(),
            src_dir.ino,
            OsStr::new("short-src"),
            dst_dir.ino,
            OsStr::new("short-dst"),
            0,
        )
    });

    assert!(rename_pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    fs::create_dir(tmp.path().join("race-src").join("short-src")).unwrap();
    fs::create_dir(
        tmp.path()
            .join("race-src")
            .join("short-src")
            .join("victim"),
    )
    .unwrap();

    let mut rmdir_pause = pause_next_txn_before_clear(fs.as_ref());
    let fs_thread = std::sync::Arc::clone(&fs);
    let rmdir_handle = std::thread::spawn(move || {
        rmdir_result(fs_thread.as_ref(), moved_parent.ino, OsStr::new("victim"))
    });

    assert!(
        !rmdir_pause.wait_until_blocked_timeout(Duration::from_millis(200)),
        "queued rmdir must stay blocked until rename bookkeeping refreshes inode_store"
    );

    rename_pause.release();
    rename_handle.join().unwrap().unwrap();

    assert!(rmdir_pause.wait_until_blocked_timeout(Duration::from_secs(1)));
    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let txn = read_txn_record(root_fd.as_fd())
        .unwrap()
        .expect("paused rmdir should leave txn record visible before clear completes");
    assert_eq!(txn.old_parent_segments, vec![b"race-dst".to_vec(), b"short-dst".to_vec()]);

    rmdir_pause.release();
    rmdir_handle.join().unwrap().unwrap();

    assert!(
        tmp.path()
            .join("race-src")
            .join("short-src")
            .join("victim")
            .exists(),
        "replacement old-path child must remain untouched"
    );
    let moved_dst = lookup_entry_result(fs.as_ref(), dst_dir.ino, OsStr::new("short-dst")).unwrap();
    assert_eq!(
        lookup_entry_result(fs.as_ref(), moved_dst.ino, OsStr::new("victim")).unwrap_err(),
        libc::ENOENT
    );
}

#[test]
fn readdirplus_uses_xattr_truth_even_when_stable_entry_hits_stale_index() {
    let _serial = lock_test_hooks();
    let tmp = TempDir::new();
    let truth = long_name("steady-readdirplus-truth");
    let stale = long_name("steady-readdirplus-stale");
    let backend = b".__ln2_obj_0000000000000001";

    fs::write(tmp.path().join(OsStr::from_bytes(backend)), b"payload").unwrap();
    set_rawname_xattr(
        &tmp.path().join(OsStr::from_bytes(backend)),
        truth.as_bytes(),
    );
    fs::write(tmp.path().join(".ln2_fs_idalloc"), (2u64).to_le_bytes()).unwrap();

    let root_fd = nix::fcntl::open(
        tmp.path(),
        OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .unwrap();
    let mut stale_index = DirIndex::new();
    stale_index.upsert(backend.to_vec(), stale.as_bytes().to_vec());
    write_dir_index(root_fd.as_fd(), &stale_index).unwrap();

    let fs = new_longname_test_fs(&tmp, false, false);
    let root = fs.inode_store.get(ROOT_INODE).unwrap();
    let handle = std::sync::Arc::new(fs.open_dir_handle(&root).unwrap());
    let entries = fs.core.load_dir_entries_snapshot(&handle, true, 0).unwrap();
    let names: Vec<_> = entries.iter().map(|entry| entry.name.clone()).collect();
    let snapshot = state_snapshot_result(&fs, OsStr::new("/")).unwrap();

    assert!(names.iter().any(|name| name == OsStr::new(&truth)));
    assert!(!names.iter().any(|name| name == OsStr::new(&stale)));
    assert!(
        snapshot.dirty,
        "stale index repair should dirty recoverable state"
    );
    assert!(
        snapshot.pending > 0,
        "stale index repair should record pending work"
    );
}
