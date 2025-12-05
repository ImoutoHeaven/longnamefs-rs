use fuse3::FileType;
use fuse3::path::reply::FileAttr;
use nix::errno::Errno as NixErrno;
use nix::fcntl::{OFlag, openat, renameat};
use nix::sys::stat::{FileStat, Mode};
use nix::unistd::{fdatasync, fsync};
use std::ffi::{CStr, CString};
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::atomic::{AtomicU32, Ordering};
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
        atime: system_time_from_raw(stat.st_atime, stat.st_atime_nsec),
        mtime: system_time_from_raw(stat.st_mtime, stat.st_mtime_nsec),
        ctime: system_time_from_raw(stat.st_ctime, stat.st_ctime_nsec),
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
            Err(NixErrno::EINTR) => continue,
            other => return other,
        }
    }
}

#[derive(Debug)]
pub struct TempFile {
    pub fd: OwnedFd,
    pub name: CString,
}

const TMP_ATTEMPTS: usize = 16;
static TMP_COUNTER: AtomicU32 = AtomicU32::new(0);

fn build_temp_name(
    base: &CStr,
    pid: u32,
    counter: u32,
    extra_suffix: &str,
) -> Result<CString, fuse3::Errno> {
    let pid_str = pid.to_string();
    let counter_str = counter.to_string();
    let mut buf = Vec::with_capacity(
        base.to_bytes().len()
            + ".tmp.".len()
            + pid_str.len()
            + 1
            + counter_str.len()
            + if extra_suffix.is_empty() {
                0
            } else {
                1 + extra_suffix.len()
            },
    );
    buf.extend_from_slice(base.to_bytes());
    buf.extend_from_slice(b".tmp.");
    buf.extend_from_slice(pid_str.as_bytes());
    buf.push(b'.');
    buf.extend_from_slice(counter_str.as_bytes());
    if !extra_suffix.is_empty() {
        buf.push(b'.');
        buf.extend_from_slice(extra_suffix.as_bytes());
    }
    CString::new(buf).map_err(|_| fuse3::Errno::from(libc::EINVAL))
}

pub fn begin_temp_file(
    dir_fd: BorrowedFd<'_>,
    final_name: &CStr,
    extra_suffix: &str,
) -> Result<TempFile, fuse3::Errno> {
    let pid = std::process::id();
    let mut counter = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);

    for _ in 0..TMP_ATTEMPTS {
        let tmp_name = build_temp_name(final_name, pid, counter, extra_suffix)?;
        match openat(
            dir_fd,
            tmp_name.as_c_str(),
            OFlag::O_WRONLY | OFlag::O_CREAT | OFlag::O_EXCL | OFlag::O_CLOEXEC,
            Mode::from_bits_truncate(0o666),
        ) {
            Ok(fd) => return Ok(TempFile { fd, name: tmp_name }),
            Err(NixErrno::EEXIST) => {
                counter = counter.wrapping_add(1);
                continue;
            }
            Err(err) => return Err(errno_from_nix(err)),
        }
    }

    Err(fuse3::Errno::from(libc::EEXIST))
}

pub fn sync_and_commit(
    dir_fd: BorrowedFd<'_>,
    temp: TempFile,
    final_name: &CStr,
) -> Result<(), fuse3::Errno> {
    retry_eintr(|| fdatasync(temp.fd.as_fd())).map_err(errno_from_nix)?;

    renameat(dir_fd, temp.name.as_c_str(), dir_fd, final_name).map_err(errno_from_nix)?;

    fsync_dir(dir_fd)
}

pub fn fsync_dir(dir_fd: BorrowedFd<'_>) -> Result<(), fuse3::Errno> {
    match retry_eintr(|| fsync(dir_fd)) {
        Ok(()) => Ok(()),
        Err(NixErrno::EBADF) => {
            // Some kernels reject fsync on O_PATH; reopen dir for fsync.
            let reopened = openat(
                dir_fd,
                ".",
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            let res = retry_eintr(|| fsync(reopened.as_fd())).map_err(errno_from_nix);
            drop(reopened);
            res
        }
        Err(err) => Err(errno_from_nix(err)),
    }
}
