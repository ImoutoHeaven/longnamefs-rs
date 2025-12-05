use crate::util::errno_from_nix;
use nix::fcntl::{OFlag, open};
use nix::sys::stat::Mode;
use std::os::fd::{AsFd, OwnedFd};
use std::path::PathBuf;

#[derive(Debug)]
pub struct Config {
    #[allow(dead_code)]
    pub backend_path: PathBuf,
    pub backend_fd: OwnedFd,
    pub sync_data: bool,
}

impl Config {
    pub fn open_backend(path: PathBuf, sync_data: bool) -> Result<Self, fuse3::Errno> {
        let fd = open(
            &path,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
            Mode::empty(),
        )
        .map_err(errno_from_nix)?;

        Ok(Self {
            backend_path: path,
            backend_fd: fd,
            sync_data,
        })
    }

    pub fn backend_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.backend_fd.as_fd()
    }

    pub fn sync_data(&self) -> bool {
        self.sync_data
    }
}
