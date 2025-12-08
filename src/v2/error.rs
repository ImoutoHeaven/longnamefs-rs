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
        match errno {
            libc::ENAMETOOLONG => CoreError::NameTooLong,
            libc::ENOENT => CoreError::NotFound,
            libc::EEXIST => CoreError::AlreadyExists,
            libc::ENOSPC => CoreError::NoSpace,
            libc::ENOTDIR => CoreError::NotDir,
            libc::EISDIR => CoreError::IsDir,
            libc::EMLINK => CoreError::TooManyLinks,
            libc::ESTALE => CoreError::StaleInode,
            libc::EOPNOTSUPP => CoreError::Unsupported,
            _ => CoreError::Io(io::Error::from_raw_os_error(errno)),
        }
    }
}

impl From<io::Error> for CoreError {
    fn from(value: io::Error) -> Self {
        if let Some(errno) = value.raw_os_error() {
            CoreError::from_errno(errno)
        } else {
            CoreError::Io(value)
        }
    }
}

impl From<nix::Error> for CoreError {
    fn from(value: nix::Error) -> Self {
        CoreError::from_errno(value as i32)
    }
}

#[allow(dead_code)]
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
