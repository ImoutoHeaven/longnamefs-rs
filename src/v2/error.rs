use std::io;

#[derive(Debug)]
pub enum CoreError {
    Io(io::Error),
    NameTooLong,
    ReservedPrefix,
    InternalMeta,
    NotFound,
    AlreadyExists,
    NoSpace,
    NotDir,
    IsDir,
    TooManyLinks,
    StaleInode,
    Unsupported,
}

impl CoreError {
    pub fn from_errno(errno: i32) -> Self {
        CoreError::Io(io::Error::from_raw_os_error(errno))
    }
}

impl From<io::Error> for CoreError {
    fn from(value: io::Error) -> Self {
        CoreError::Io(value)
    }
}

impl From<nix::Error> for CoreError {
    fn from(value: nix::Error) -> Self {
        CoreError::Io(io::Error::from_raw_os_error(value as i32))
    }
}

pub fn core_err_to_errno(err: &CoreError) -> i32 {
    match err {
        CoreError::Io(ioe) => ioe.raw_os_error().unwrap_or(libc::EIO),
        CoreError::NameTooLong => libc::ENAMETOOLONG,
        CoreError::ReservedPrefix | CoreError::InternalMeta => libc::EINVAL,
        CoreError::NotFound => libc::ENOENT,
        CoreError::AlreadyExists => libc::EEXIST,
        CoreError::NoSpace => libc::ENOSPC,
        CoreError::NotDir => libc::ENOTDIR,
        CoreError::IsDir => libc::EISDIR,
        CoreError::TooManyLinks => libc::EMLINK,
        CoreError::StaleInode => libc::ESTALE,
        CoreError::Unsupported => libc::EOPNOTSUPP,
    }
}

pub type CoreResult<T> = Result<T, CoreError>;

pub fn core_error_from_fuse(err: fuse3::Errno) -> CoreError {
    CoreError::from_errno(err.into())
}

impl From<CoreError> for fuse3::Errno {
    fn from(value: CoreError) -> Self {
        fuse3::Errno::from(core_err_to_errno(&value))
    }
}
