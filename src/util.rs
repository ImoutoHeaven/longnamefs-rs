use fuse3::FileType;
use fuse3::path::reply::FileAttr;
use libc;
use nix::errno::Errno as NixErrno;
use nix::fcntl::OFlag;
use nix::sys::stat::FileStat;
use std::ffi::CString;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn errno_from_nix(err: nix::Error) -> fuse3::Errno {
    fuse3::Errno::from(err as i32)
}

pub fn string_to_cstring(value: &str) -> Result<CString, fuse3::Errno> {
    CString::new(value.as_bytes()).map_err(|_| fuse3::Errno::from(libc::EINVAL))
}

pub fn file_type_from_mode(mode: libc::mode_t) -> FileType {
    match mode & libc::S_IFMT {
        libc::S_IFDIR => FileType::Directory,
        libc::S_IFLNK => FileType::Symlink,
        libc::S_IFCHR => FileType::CharDevice,
        libc::S_IFBLK => FileType::BlockDevice,
        libc::S_IFIFO => FileType::NamedPipe,
        libc::S_IFSOCK => FileType::Socket,
        _ => FileType::RegularFile,
    }
}

fn system_time_from_raw(sec: i64, nsec: i64) -> SystemTime {
    if sec < 0 {
        return UNIX_EPOCH;
    }
    let nanos = if nsec < 0 { 0 } else { nsec as u32 };
    UNIX_EPOCH + Duration::new(sec as u64, nanos)
}

pub fn file_attr_from_stat(stat: &FileStat) -> FileAttr {
    let kind = file_type_from_mode(stat.st_mode);
    let perm = fuse3::perm_from_mode_and_kind(kind, stat.st_mode as libc::mode_t);

    FileAttr {
        size: stat.st_size as u64,
        blocks: stat.st_blocks as u64,
        atime: system_time_from_raw(stat.st_atime, stat.st_atime_nsec.into()),
        mtime: system_time_from_raw(stat.st_mtime, stat.st_mtime_nsec.into()),
        ctime: system_time_from_raw(stat.st_ctime, stat.st_ctime_nsec.into()),
        kind,
        perm,
        nlink: stat.st_nlink as u32,
        uid: stat.st_uid,
        gid: stat.st_gid,
        rdev: stat.st_rdev as u32,
        blksize: stat.st_blksize as u32,
        #[cfg(target_os = "macos")]
        crtime: UNIX_EPOCH,
        #[cfg(target_os = "macos")]
        flags: 0,
    }
}

pub fn oflag_from_bits(flags: u32) -> OFlag {
    OFlag::from_bits_truncate(flags as i32)
}

pub fn access_mask_from_bits(mask: u32) -> nix::unistd::AccessFlags {
    nix::unistd::AccessFlags::from_bits_truncate(mask as i32)
}

pub fn retry_eintr<T, F>(mut op: F) -> Result<T, nix::Error>
where
    F: FnMut() -> Result<T, nix::Error>,
{
    loop {
        match op() {
            Err(err) if err == NixErrno::EINTR => continue,
            other => return other,
        }
    }
}
