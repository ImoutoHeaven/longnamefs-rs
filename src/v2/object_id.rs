use crate::util::{core_begin_temp_file, core_fsync_dir, retry_eintr};
use crate::v2::error::{CoreError, CoreResult};
use nix::fcntl::{AtFlags, OFlag, openat, renameat};
use nix::sys::stat::{Mode, fstatat};
use nix::unistd::{fdatasync, read, write};
use std::ffi::CStr;
use std::os::fd::{AsFd, BorrowedFd};

pub const LONG_OBJECT_PREFIX: &[u8] = b".__ln2_obj_";
pub const LONG_OBJECT_HEX_WIDTH: usize = 16;
pub const FIRST_OBJECT_ID: u64 = 1;
pub const IDALLOC_FILE_NAME: &str = ".ln2_fs_idalloc";

pub fn format_long_object_name(id: u64) -> Vec<u8> {
    format!(".__ln2_obj_{id:016x}").into_bytes()
}

pub fn parse_long_object_id(name: &[u8]) -> CoreResult<u64> {
    if !name.starts_with(LONG_OBJECT_PREFIX) {
        return Err(CoreError::InternalMeta);
    }
    let hex = &name[LONG_OBJECT_PREFIX.len()..];
    if hex.len() != LONG_OBJECT_HEX_WIDTH {
        return Err(CoreError::from_errno(libc::EINVAL));
    }
    let text = std::str::from_utf8(hex).map_err(|_| CoreError::from_errno(libc::EINVAL))?;
    u64::from_str_radix(text, 16).map_err(|_| CoreError::from_errno(libc::EINVAL))
}

pub fn is_stable_long_object_name(name: &[u8]) -> bool {
    parse_long_object_id(name).is_ok()
}

fn read_u64_le_file(root: BorrowedFd<'_>, name: &CStr) -> CoreResult<u64> {
    let fd = openat(
        root,
        name,
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(CoreError::from)?;
    let mut buf = [0u8; 9];
    let mut filled = 0;
    while filled < buf.len() {
        let read_now =
            retry_eintr(|| read(fd.as_fd(), &mut buf[filled..])).map_err(CoreError::from)?;
        if read_now == 0 {
            break;
        }
        filled += read_now;
    }
    if filled != 8 {
        return Err(CoreError::BadFormat);
    }
    Ok(u64::from_le_bytes(buf[..8].try_into().unwrap()))
}

fn write_all(fd: BorrowedFd<'_>, buf: &[u8]) -> CoreResult<()> {
    let mut written = 0;
    while written < buf.len() {
        let wrote = retry_eintr(|| write(fd, &buf[written..])).map_err(CoreError::from)?;
        if wrote == 0 {
            return Err(CoreError::from_errno(libc::EIO));
        }
        written += wrote;
    }
    Ok(())
}

pub fn bootstrap_id_allocator_if_missing(
    root: BorrowedFd<'_>,
    has_any_committed_long_object: bool,
) -> CoreResult<()> {
    let name = c".ln2_fs_idalloc";
    match fstatat(root, name, AtFlags::AT_SYMLINK_NOFOLLOW) {
        Ok(_) => return Ok(()),
        Err(nix::errno::Errno::ENOENT) if has_any_committed_long_object => {
            return Err(CoreError::BadFormat);
        }
        Err(nix::errno::Errno::ENOENT) => {}
        Err(err) => return Err(CoreError::from(err)),
    }

    let tmp = core_begin_temp_file(root, name, "idalloc").map_err(CoreError::from)?;
    let next = FIRST_OBJECT_ID.to_le_bytes();
    write_all(tmp.fd.as_fd(), &next)?;
    retry_eintr(|| fdatasync(tmp.fd.as_fd())).map_err(CoreError::from)?;
    renameat(root, tmp.name.as_c_str(), root, name).map_err(CoreError::from)?;
    core_fsync_dir(root).map_err(CoreError::from)
}

pub fn allocate_long_object_id(root: BorrowedFd<'_>) -> CoreResult<u64> {
    let name = c".ln2_fs_idalloc";
    let current = read_u64_le_file(root, name)?;
    let next = current.checked_add(1).ok_or(CoreError::NoSpace)?;
    let tmp = core_begin_temp_file(root, name, "idalloc").map_err(CoreError::from)?;
    write_all(tmp.fd.as_fd(), &next.to_le_bytes())?;
    retry_eintr(|| fdatasync(tmp.fd.as_fd())).map_err(CoreError::from)?;
    renameat(root, tmp.name.as_c_str(), root, name).map_err(CoreError::from)?;
    core_fsync_dir(root).map_err(CoreError::from)?;
    Ok(current)
}
