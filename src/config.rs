use crate::util::errno_from_nix;
use nix::fcntl::{OFlag, open};
use nix::sys::stat::Mode;
use nix::sys::stat::fstat;
use std::os::fd::{AsFd, OwnedFd};
use std::path::PathBuf;

#[derive(Debug)]
pub struct Config {
    #[allow(dead_code)]
    pub backend_path: PathBuf,
    pub backend_fd: OwnedFd,
    pub sync_data: bool,
    backend_ns: String,
    collision_protect: bool,
}

impl Config {
    pub fn open_backend(
        path: PathBuf,
        sync_data: bool,
        collision_protect: bool,
    ) -> Result<Self, fuse3::Errno> {
        let fd = open(
            &path,
            OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_DIRECTORY,
            Mode::empty(),
        )
        .map_err(errno_from_nix)?;
        let st = fstat(&fd).map_err(errno_from_nix)?;
        let backend_ns = format!("{}:{}", st.st_dev, st.st_ino);

        Ok(Self {
            backend_path: path,
            backend_fd: fd,
            sync_data,
            backend_ns,
            collision_protect,
        })
    }

    pub fn backend_fd(&self) -> std::os::fd::BorrowedFd<'_> {
        self.backend_fd.as_fd()
    }

    pub fn sync_data(&self) -> bool {
        self.sync_data
    }

    pub fn cache_namespace(&self) -> &str {
        &self.backend_ns
    }

    pub fn collision_protect(&self) -> bool {
        self.collision_protect
    }
}
