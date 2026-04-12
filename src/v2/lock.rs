use crate::v2::error::{CoreError, CoreResult};
use nix::fcntl::{OFlag, openat};
use nix::sys::stat::Mode;
use std::os::fd::{AsRawFd, BorrowedFd, OwnedFd};

pub const LOCK_FILE_NAME: &str = ".ln2_fs_lock";

pub fn open_and_lock_backend(root: BorrowedFd<'_>) -> CoreResult<OwnedFd> {
    let fd = openat(
        root,
        c".ln2_fs_lock",
        OFlag::O_RDWR | OFlag::O_CREAT | OFlag::O_CLOEXEC,
        Mode::from_bits_truncate(0o600),
    )
    .map_err(CoreError::from)?;

    let res = unsafe { libc::flock(fd.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if res != 0 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or(libc::EIO);
        if errno == libc::EWOULDBLOCK || errno == libc::EAGAIN {
            return Err(CoreError::LockConflict);
        }
        return Err(CoreError::from_errno(errno));
    }

    Ok(fd)
}
