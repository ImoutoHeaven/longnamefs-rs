use crate::pathmap::{BACKEND_HASH_STRING_LENGTH, LnfsPath, MAX_NAME_LENGTH};
use crate::util::{
    begin_temp_file, errno_from_nix, file_attr_from_stat, file_type_from_mode, fsync_dir,
    retry_eintr, string_to_cstring, sync_and_commit,
};
use fuse3::FileType;
use libc;
use nix::dir::Dir;
use nix::fcntl::{AtFlags, OFlag, openat};
use nix::sys::stat::{Mode, fstatat};
use nix::unistd::{UnlinkatFlags, ftruncate, unlinkat};
use std::ffi::OsString;
use std::os::fd::{AsFd, BorrowedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};

#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    pub name: OsString,
    pub kind: FileType,
    pub encoded: String,
    pub attr: Option<fuse3::path::reply::FileAttr>,
}

fn namefile_suffix(fname: &str) -> Result<std::ffi::CString, fuse3::Errno> {
    let mut composed = String::with_capacity(BACKEND_HASH_STRING_LENGTH + 1);
    composed.push_str(fname);
    composed.push('n');
    string_to_cstring(&composed)
}

pub fn write_namefile(path: &LnfsPath) -> Result<(), fuse3::Errno> {
    let fname = namefile_suffix(&path.fname)?;
    let temp = begin_temp_file(path.dir_fd.as_fd(), fname.as_c_str(), "tn")?;

    let data = path.raw_name.as_bytes();
    let written =
        retry_eintr(|| nix::unistd::write(temp.fd.as_fd(), data)).map_err(errno_from_nix)?;
    if written != data.len() {
        return Err(fuse3::Errno::from(libc::EIO));
    }
    ftruncate(&temp.fd, written as i64).map_err(errno_from_nix)?;
    sync_and_commit(path.dir_fd.as_fd(), temp, fname.as_c_str())
}

pub fn remove_namefile(path: &LnfsPath) -> Result<(), fuse3::Errno> {
    let fname = namefile_suffix(&path.fname)?;
    let _ = unlinkat(
        path.dir_fd.as_fd(),
        fname.as_c_str(),
        UnlinkatFlags::NoRemoveDir,
    );
    fsync_dir(path.dir_fd.as_fd())
}

pub fn list_logical_entries(
    dir_fd: BorrowedFd<'_>,
    name_buf: &mut Vec<u8>,
    entries: &mut Vec<DirEntryInfo>,
) -> Result<(), fuse3::Errno> {
    let mut dir = Dir::openat(
        dir_fd,
        ".",
        OFlag::O_RDONLY | OFlag::O_CLOEXEC,
        Mode::empty(),
    )
    .map_err(errno_from_nix)?;

    if name_buf.len() < MAX_NAME_LENGTH {
        name_buf.resize(MAX_NAME_LENGTH, 0);
    }
    entries.clear();

    for entry in dir.iter() {
        let entry = match entry {
            Ok(v) => v,
            Err(_) => continue,
        };
        let name = entry.file_name();
        let raw_bytes = name.to_bytes();
        if raw_bytes.is_empty() || *raw_bytes.last().unwrap() != b'n' {
            continue;
        }
        let stem = &raw_bytes[..raw_bytes.len() - 1];
        if stem.len() < BACKEND_HASH_STRING_LENGTH {
            continue;
        }
        if !stem[..BACKEND_HASH_STRING_LENGTH]
            .iter()
            .all(|c| c.is_ascii_hexdigit())
        {
            continue;
        }
        let suffix = &stem[BACKEND_HASH_STRING_LENGTH..];
        if !suffix.is_empty() {
            if suffix.len() < 2
                || suffix[0] != b'.'
                || !suffix[1..].iter().all(|c| c.is_ascii_digit())
            {
                continue;
            }
        }

        let namefile_fd = match openat(
            dir_fd,
            name,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => fd,
            Err(_) => continue,
        };

        let read_len = match retry_eintr(|| nix::unistd::read(&namefile_fd, name_buf)) {
            Ok(len) => len,
            Err(_) => continue,
        };
        if read_len == 0 {
            continue;
        }

        let raw_name = OsString::from_vec(name_buf[..read_len].to_vec());

        let data_name = &stem[..];
        let data_name_str = match std::str::from_utf8(data_name) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let data_cstr = match string_to_cstring(data_name_str) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let stat = match fstatat(dir_fd, data_cstr.as_c_str(), AtFlags::AT_SYMLINK_NOFOLLOW) {
            Ok(st) => st,
            Err(_) => continue,
        };
        let kind = file_type_from_mode(stat.st_mode);
        let attr = file_attr_from_stat(&stat);
        entries.push(DirEntryInfo {
            name: raw_name,
            kind,
            encoded: data_name_str.to_owned(),
            attr: Some(attr),
        });
    }

    Ok(())
}
