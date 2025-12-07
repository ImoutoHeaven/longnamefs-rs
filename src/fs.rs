use crate::config::Config;
use crate::handle_table::HandleTable;
use crate::namefile::{DirEntryInfo, list_logical_entries, remove_namefile, write_namefile};
use crate::pathmap::{clear_dir_fd_cache, make_child_path, open_path, open_paths};
use crate::util::{
    access_mask_from_bits, errno_from_nix, file_attr_from_stat, oflag_from_bits, retry_eintr,
    string_to_cstring,
};
use bytes::Bytes;
use fuse3::notify::Notify;
use fuse3::path::prelude::*;
use fuse3::path::reply::{DirectoryEntryPlus, ReplyPoll, ReplyXAttr};
use fuse3::{FileType, SetAttr};
use nix::fcntl::{AtFlags, OFlag, openat, readlinkat, renameat};
use nix::sys::stat::{
    FchmodatFlags, Mode, SFlag, UtimensatFlags, fchmodat, fstat, fstatat, mkdirat, mknodat,
    utimensat,
};
use nix::sys::statvfs::fstatvfs;
use nix::sys::time::TimeSpec;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{
    Gid, LinkatFlags, Uid, UnlinkatFlags, faccessat, fchown, fchownat, fdatasync, fsync, linkat,
    symlinkat, unlinkat,
};
use std::collections::HashMap;
use std::ffi::{CString, OsStr, OsString};
use std::io;
use std::num::NonZeroU32;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const ATTR_TTL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct DirCacheKey {
    dev: u64,
    ino: u64,
}

#[derive(Debug)]
struct DirCacheEntry {
    expires_at: Instant,
    entries: Arc<Vec<DirEntryInfo>>,
}

const DIR_CACHE_MAX_DIRS: usize = 4096;

#[derive(Debug)]
struct DirCache {
    ttl: Duration,
    enabled: bool,
    entries: RwLock<HashMap<DirCacheKey, DirCacheEntry>>,
}

impl DirCache {
    fn new(ttl: Option<Duration>) -> Self {
        let (enabled, ttl) = match ttl {
            Some(t) => (true, t),
            None => (false, Duration::ZERO),
        };
        Self {
            ttl,
            enabled,
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, key: DirCacheKey) -> Option<Arc<Vec<DirEntryInfo>>> {
        if !self.enabled {
            return None;
        }
        let now = Instant::now();
        {
            let guard = self.entries.read().ok()?;
            if let Some(entry) = guard.get(&key)
                && entry.expires_at > now
            {
                return Some(entry.entries.clone());
            }
        }

        let mut guard = self.entries.write().ok()?;
        if let Some(entry) = guard.get(&key)
            && entry.expires_at > now
        {
            return Some(entry.entries.clone());
        }
        guard.remove(&key);
        None
    }

    fn insert(&self, key: DirCacheKey, items: Vec<DirEntryInfo>) -> Arc<Vec<DirEntryInfo>> {
        if !self.enabled {
            return Arc::new(items);
        }
        let expires_at = Instant::now() + self.ttl;
        let entries = Arc::new(items);
        let mut guard = self.entries.write().unwrap();
        if guard.len() >= DIR_CACHE_MAX_DIRS {
            guard.clear();
        }
        guard.insert(
            key,
            DirCacheEntry {
                expires_at,
                entries: entries.clone(),
            },
        );
        entries
    }

    fn invalidate(&self, key: DirCacheKey) {
        if !self.enabled {
            return;
        }
        if let Ok(mut guard) = self.entries.write() {
            guard.remove(&key);
        }
    }
}

fn dir_cache_key(fd: BorrowedFd<'_>) -> Option<DirCacheKey> {
    fstat(fd).ok().map(|stat| DirCacheKey {
        dev: stat.st_dev,
        ino: stat.st_ino,
    })
}

fn xattr_name_to_cstring(name: &OsStr) -> Result<CString, fuse3::Errno> {
    CString::new(name.as_bytes()).map_err(|_| fuse3::Errno::from(libc::EINVAL))
}

pub struct LongNameFs {
    config: Arc<Config>,
    handles: HandleTable,
    dir_cache: DirCache,
    max_write: NonZeroU32,
}

impl LongNameFs {
    pub fn new(config: Config, dir_cache_ttl: Option<Duration>, max_write_kb: u32) -> Self {
        let bytes = max_write_kb.saturating_mul(1024).max(4096);
        let max_write = NonZeroU32::new(bytes).unwrap_or_else(|| NonZeroU32::new(4096).unwrap());
        Self {
            config: Arc::new(config),
            handles: HandleTable::new(),
            dir_cache: DirCache::new(dir_cache_ttl),
            max_write,
        }
    }

    fn invalidate_dir(&self, dir_fd: BorrowedFd<'_>) {
        if let Some(key) = dir_cache_key(dir_fd) {
            self.dir_cache.invalidate(key);
        }
    }

    fn load_dir_entries(&self, dir_fd: BorrowedFd<'_>) -> Arc<Vec<DirEntryInfo>> {
        let key = dir_cache_key(dir_fd);
        if let Some(cache_key) = key
            && let Some(entries) = self.dir_cache.get(cache_key)
        {
            return entries;
        }

        let mut name_buf = Vec::new();
        let mut logical = Vec::new();
        let result = list_logical_entries(dir_fd, &mut name_buf, &mut logical);
        if let Some(cache_key) = key
            && result.is_ok()
        {
            return self.dir_cache.insert(cache_key, logical);
        }

        Arc::new(logical)
    }

    fn with_xattr_target<F, T>(&self, path: &OsStr, func: F) -> Result<T, fuse3::Errno>
    where
        F: FnOnce(BorrowedFd<'_>) -> Result<T, fuse3::Errno>,
    {
        if path == OsStr::new("/") {
            return func(self.config.backend_fd());
        }

        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        let fd = openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_PATH | OFlag::O_CLOEXEC | OFlag::O_NOFOLLOW,
            Mode::empty(),
        )
        .map_err(errno_from_nix)?;
        func(fd.as_fd())
    }

    fn stat_path(&self, path: &OsStr) -> Result<FileAttr, fuse3::Errno> {
        if path == OsStr::new("/") {
            let stat = fstatat(
                self.config.backend_fd(),
                "",
                AtFlags::AT_EMPTY_PATH | AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(errno_from_nix)?;
            return Ok(file_attr_from_stat(&stat));
        }

        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        let stat = fstatat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(errno_from_nix)?;
        Ok(file_attr_from_stat(&stat))
    }

    fn open_dir_fd(&self, path: &OsStr) -> Result<OwnedFd, fuse3::Errno> {
        if path == OsStr::new("/") {
            return openat(
                self.config.backend_fd(),
                ".",
                OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix);
        }

        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        openat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            OFlag::O_RDONLY | OFlag::O_DIRECTORY | OFlag::O_CLOEXEC,
            Mode::empty(),
        )
        .map_err(errno_from_nix)
    }

    fn open_file(&self, path: &OsStr, mode: u32, flags: u32) -> Result<u64, fuse3::Errno> {
        let mut oflag = oflag_from_bits(flags) | OFlag::O_CLOEXEC;

        let fd = if path == OsStr::new("/") {
            openat(
                self.config.backend_fd(),
                ".",
                oflag,
                Mode::from_bits_truncate(mode),
            )
            .map_err(errno_from_nix)?
        } else {
            let mapped = open_path(&self.config, path)?;
            let created = oflag.contains(OFlag::O_CREAT);
            if created {
                oflag |= OFlag::O_EXCL;
            }
            let fname = string_to_cstring(&mapped.fname)?;
            let fd = openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                oflag,
                Mode::from_bits_truncate(mode & 0o777),
            )
            .map_err(errno_from_nix)?;
            if created {
                write_namefile(&mapped)?;
                self.invalidate_dir(mapped.dir_fd.as_fd());
            }
            fd
        };

        let handle = self.handles.insert_file(fd);
        Ok(handle)
    }

    fn build_attr_reply(&self, path: &OsStr) -> Result<ReplyAttr, fuse3::Errno> {
        let attr = self.stat_path(path)?;
        Ok(ReplyAttr {
            ttl: ATTR_TTL,
            attr,
        })
    }

    fn apply_times(
        &self,
        dir: BorrowedFd<'_>,
        fname: &str,
        set_attr: &SetAttr,
    ) -> Result<(), fuse3::Errno> {
        if set_attr.atime.is_none() && set_attr.mtime.is_none() {
            return Ok(());
        }

        let fname = string_to_cstring(fname)?;
        let atime = set_attr
            .atime
            .map(|t| TimeSpec::new(t.sec, t.nsec as _))
            .unwrap_or(TimeSpec::UTIME_OMIT);
        let mtime = set_attr
            .mtime
            .map(|t| TimeSpec::new(t.sec, t.nsec as _))
            .unwrap_or(TimeSpec::UTIME_OMIT);

        utimensat(
            dir,
            fname.as_c_str(),
            &atime,
            &mtime,
            UtimensatFlags::NoFollowSymlink,
        )
        .map_err(errno_from_nix)
    }

    fn apply_root_times(&self, set_attr: &SetAttr) -> Result<(), fuse3::Errno> {
        if set_attr.atime.is_none() && set_attr.mtime.is_none() {
            return Ok(());
        }

        let atime = set_attr
            .atime
            .map(|t| TimeSpec::new(t.sec, t.nsec as _))
            .unwrap_or(TimeSpec::UTIME_OMIT);
        let mtime = set_attr
            .mtime
            .map(|t| TimeSpec::new(t.sec, t.nsec as _))
            .unwrap_or(TimeSpec::UTIME_OMIT);
        let times = [*atime.as_ref(), *mtime.as_ref()];

        let res = unsafe { libc::futimens(self.config.backend_fd().as_raw_fd(), times.as_ptr()) };
        if res < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(())
    }
}

impl PathFilesystem for LongNameFs {
    async fn init(&self, _req: Request) -> Result<ReplyInit, fuse3::Errno> {
        Ok(ReplyInit {
            max_write: self.max_write,
        })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let path = make_child_path(parent, name);
        let attr = self.stat_path(&path)?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: Option<u64>,
        _flags: u32,
    ) -> Result<ReplyAttr, fuse3::Errno> {
        if let Some(handle) = fh.and_then(|id| self.handles.get_file(id)) {
            let stat = fstat(handle.as_fd()).map_err(errno_from_nix)?;
            let attr = file_attr_from_stat(&stat);
            return Ok(ReplyAttr {
                ttl: ATTR_TTL,
                attr,
            });
        }

        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;
        self.build_attr_reply(path)
    }

    async fn setattr(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> Result<ReplyAttr, fuse3::Errno> {
        let path = path.ok_or_else(fuse3::Errno::new_not_exist)?;

        if path == OsStr::new("/") {
            if let Some(mode) = set_attr.mode {
                nix::sys::stat::fchmod(self.config.backend_fd(), Mode::from_bits_truncate(mode))
                    .map_err(errno_from_nix)?;
            }

            if set_attr.uid.is_some() || set_attr.gid.is_some() {
                let uid = set_attr.uid.map(Uid::from_raw);
                let gid = set_attr.gid.map(Gid::from_raw);
                fchown(self.config.backend_fd(), uid, gid).map_err(errno_from_nix)?;
            }

            if set_attr.size.is_some() {
                return Err(fuse3::Errno::from(libc::EFAULT));
            }

            self.apply_root_times(&set_attr)?;
            return self.build_attr_reply(path);
        }

        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;

        if let Some(mode) = set_attr.mode {
            fchmodat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                Mode::from_bits_truncate(mode),
                FchmodatFlags::FollowSymlink,
            )
            .map_err(errno_from_nix)?;
        }

        if set_attr.uid.is_some() || set_attr.gid.is_some() {
            fchownat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                set_attr.uid.map(Uid::from_raw),
                set_attr.gid.map(Gid::from_raw),
                AtFlags::AT_SYMLINK_NOFOLLOW,
            )
            .map_err(errno_from_nix)?;
        }

        if let Some(size) = set_attr.size {
            let file = openat(
                mapped.dir_fd.as_fd(),
                fname.as_c_str(),
                OFlag::O_WRONLY | OFlag::O_CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno_from_nix)?;
            nix::unistd::ftruncate(&file, size as i64).map_err(errno_from_nix)?;
        }

        self.apply_times(mapped.dir_fd.as_fd(), &mapped.fname, &set_attr)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        self.build_attr_reply(path)
    }

    async fn readlink(&self, _req: Request, path: &OsStr) -> Result<ReplyData, fuse3::Errno> {
        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        let target = readlinkat(mapped.dir_fd.as_fd(), fname.as_c_str()).map_err(errno_from_nix)?;
        let bytes = target.into_vec();
        Ok(Bytes::from(bytes).into())
    }

    async fn symlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        link_path: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let path = make_child_path(parent, name);
        let mapped = open_path(&self.config, &path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        symlinkat(link_path, mapped.dir_fd.as_fd(), fname.as_c_str()).map_err(errno_from_nix)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        write_namefile(&mapped)?;
        let attr = self.stat_path(&path)?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn mknod(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        rdev: u32,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let path = make_child_path(parent, name);
        let mapped = open_path(&self.config, &path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        let sflag = SFlag::from_bits_truncate(mode);
        let perm = Mode::from_bits_truncate(mode);
        let dev = rdev as u64;
        mknodat(mapped.dir_fd.as_fd(), fname.as_c_str(), sflag, perm, dev)
            .map_err(errno_from_nix)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        write_namefile(&mapped)?;
        let attr = self.stat_path(&path)?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        _umask: u32,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let path = make_child_path(parent, name);
        let mapped = open_path(&self.config, &path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        mkdirat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            Mode::from_bits_truncate(mode),
        )
        .map_err(errno_from_nix)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        write_namefile(&mapped)?;
        let attr = self.stat_path(&path)?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn unlink(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        let path = make_child_path(parent, name);
        let mapped = open_path(&self.config, &path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        unlinkat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::NoRemoveDir,
        )
        .map_err(errno_from_nix)?;
        remove_namefile(&mapped)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        Ok(())
    }

    async fn rmdir(&self, _req: Request, parent: &OsStr, name: &OsStr) -> Result<(), fuse3::Errno> {
        let path = make_child_path(parent, name);
        let mapped = open_path(&self.config, &path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        unlinkat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            UnlinkatFlags::RemoveDir,
        )
        .map_err(errno_from_nix)?;
        remove_namefile(&mapped)?;
        self.invalidate_dir(mapped.dir_fd.as_fd());
        Ok(())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: &OsStr,
        origin_name: &OsStr,
        parent: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        let from = make_child_path(origin_parent, origin_name);
        let to = make_child_path(parent, name);
        if from == "/" || to == "/" {
            return Err(fuse3::Errno::from(libc::EFAULT));
        }

        let (pfrom, pto) = open_paths(&self.config, &from, &to)?;
        let from_name = string_to_cstring(&pfrom.fname)?;
        let to_name = string_to_cstring(&pto.fname)?;

        renameat(
            pfrom.dir_fd.as_fd(),
            from_name.as_c_str(),
            pto.dir_fd.as_fd(),
            to_name.as_c_str(),
        )
        .map_err(errno_from_nix)?;

        self.invalidate_dir(pfrom.dir_fd.as_fd());
        self.invalidate_dir(pto.dir_fd.as_fd());
        write_namefile(&pto)?;
        remove_namefile(&pfrom)?;
        clear_dir_fd_cache();
        Ok(())
    }

    async fn link(
        &self,
        _req: Request,
        path: &OsStr,
        new_parent: &OsStr,
        new_name: &OsStr,
    ) -> Result<ReplyEntry, fuse3::Errno> {
        let target = path;
        let dest = make_child_path(new_parent, new_name);

        if target == OsStr::new("/") || dest == "/" {
            return Err(fuse3::Errno::from(libc::EFAULT));
        }

        let (pfrom, pto) = open_paths(&self.config, target, &dest)?;
        let from_name = string_to_cstring(&pfrom.fname)?;
        let to_name = string_to_cstring(&pto.fname)?;

        linkat(
            pfrom.dir_fd.as_fd(),
            from_name.as_c_str(),
            pto.dir_fd.as_fd(),
            to_name.as_c_str(),
            LinkatFlags::empty(),
        )
        .map_err(errno_from_nix)?;
        self.invalidate_dir(pto.dir_fd.as_fd());
        write_namefile(&pto)?;
        let attr = self.stat_path(&dest)?;
        Ok(ReplyEntry {
            ttl: ATTR_TTL,
            attr,
        })
    }

    async fn open(
        &self,
        _req: Request,
        path: &OsStr,
        flags: u32,
    ) -> Result<ReplyOpen, fuse3::Errno> {
        let handle = self.open_file(path, 0, flags)?;
        Ok(ReplyOpen {
            fh: handle,
            flags: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        size: u32,
    ) -> Result<ReplyData, fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        let mut buf = vec![0u8; size as usize];
        let read_len = retry_eintr(|| pread(handle.as_fd(), &mut buf, offset as i64))
            .map_err(errno_from_nix)?;
        buf.truncate(read_len);
        Ok(Bytes::from(buf).into())
    }

    async fn write(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> Result<ReplyWrite, fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        let written =
            retry_eintr(|| pwrite(handle.as_fd(), data, offset as i64)).map_err(errno_from_nix)?;

        if self.config.sync_data() {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        }

        if let Some(path) = path
            && path != "/"
            && let Ok(mapped) = open_path(&self.config, path)
        {
            self.invalidate_dir(mapped.dir_fd.as_fd());
        }

        Ok(ReplyWrite {
            written: written as u32,
        })
    }

    async fn release(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> Result<(), fuse3::Errno> {
        self.handles.remove(fh);
        Ok(())
    }

    async fn fsync(
        &self,
        _req: Request,
        path: Option<&OsStr>,
        fh: u64,
        datasync: bool,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;

        if datasync {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        } else {
            fsync(handle.as_fd()).map_err(errno_from_nix)?;
        }
        if let Some(path) = path
            && path != "/"
            && let Ok(mapped) = open_path(&self.config, path)
        {
            self.invalidate_dir(mapped.dir_fd.as_fd());
        }
        Ok(())
    }

    async fn setxattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
        value: &[u8],
        flags: u32,
        position: u32,
    ) -> Result<(), fuse3::Errno> {
        if position != 0 {
            return Err(fuse3::Errno::from(libc::EINVAL));
        }
        let name = xattr_name_to_cstring(name)?;
        self.with_xattr_target(path, |fd| {
            let res = unsafe {
                libc::fsetxattr(
                    fd.as_raw_fd(),
                    name.as_ptr(),
                    value.as_ptr() as *const libc::c_void,
                    value.len(),
                    flags as libc::c_int,
                )
            };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            Ok(())
        })
    }

    async fn getxattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr, fuse3::Errno> {
        let name = xattr_name_to_cstring(name)?;
        self.with_xattr_target(path, |fd| {
            let raw_fd = fd.as_raw_fd();
            if size == 0 {
                let res =
                    unsafe { libc::fgetxattr(raw_fd, name.as_ptr(), std::ptr::null_mut(), 0) };
                if res < 0 {
                    return Err(io::Error::last_os_error().into());
                }
                return Ok(ReplyXAttr::Size(res as u32));
            }

            let mut buf = vec![0u8; size as usize];
            let res = unsafe {
                libc::fgetxattr(
                    raw_fd,
                    name.as_ptr(),
                    buf.as_mut_ptr() as *mut libc::c_void,
                    size as usize,
                )
            };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            let read_len = res as usize;
            if read_len > size as usize {
                return Err(fuse3::Errno::from(libc::ERANGE));
            }
            buf.truncate(read_len);
            Ok(ReplyXAttr::Data(buf.into()))
        })
    }

    async fn listxattr(
        &self,
        _req: Request,
        path: &OsStr,
        size: u32,
    ) -> Result<ReplyXAttr, fuse3::Errno> {
        self.with_xattr_target(path, |fd| {
            let raw_fd = fd.as_raw_fd();
            if size == 0 {
                let res = unsafe { libc::flistxattr(raw_fd, std::ptr::null_mut(), 0) };
                if res < 0 {
                    return Err(io::Error::last_os_error().into());
                }
                return Ok(ReplyXAttr::Size(res as u32));
            }

            let mut buf = vec![0u8; size as usize];
            let res = unsafe {
                libc::flistxattr(
                    raw_fd,
                    buf.as_mut_ptr() as *mut libc::c_char,
                    size as libc::size_t,
                )
            };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            let list_len = res as usize;
            if list_len > size as usize {
                return Err(fuse3::Errno::from(libc::ERANGE));
            }
            buf.truncate(list_len);
            Ok(ReplyXAttr::Data(buf.into()))
        })
    }

    async fn removexattr(
        &self,
        _req: Request,
        path: &OsStr,
        name: &OsStr,
    ) -> Result<(), fuse3::Errno> {
        let name = xattr_name_to_cstring(name)?;
        self.with_xattr_target(path, |fd| {
            let res = unsafe { libc::fremovexattr(fd.as_raw_fd(), name.as_ptr()) };
            if res < 0 {
                return Err(io::Error::last_os_error().into());
            }
            Ok(())
        })
    }

    async fn flush(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _lock_owner: u64,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_file(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        fsync(handle.as_fd()).map_err(errno_from_nix)
    }

    async fn access(&self, _req: Request, path: &OsStr, mask: u32) -> Result<(), fuse3::Errno> {
        let flags = access_mask_from_bits(mask);
        if path == OsStr::new("/") {
            faccessat(self.config.backend_fd(), ".", flags, AtFlags::empty())
                .map_err(errno_from_nix)?;
            return Ok(());
        }

        let mapped = open_path(&self.config, path)?;
        let fname = string_to_cstring(&mapped.fname)?;
        faccessat(
            mapped.dir_fd.as_fd(),
            fname.as_c_str(),
            flags,
            AtFlags::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(errno_from_nix)?;
        Ok(())
    }

    async fn create(
        &self,
        _req: Request,
        parent: &OsStr,
        name: &OsStr,
        mode: u32,
        flags: u32,
    ) -> Result<ReplyCreated, fuse3::Errno> {
        let path = make_child_path(parent, name);
        let fh = self.open_file(&path, mode, flags | libc::O_CREAT as u32)?;
        let attr = self.stat_path(&path)?;
        Ok(ReplyCreated {
            ttl: ATTR_TTL,
            attr,
            generation: 0,
            fh,
            flags: 0,
        })
    }

    async fn opendir(
        &self,
        _req: Request,
        path: &OsStr,
        flags: u32,
    ) -> Result<ReplyOpen, fuse3::Errno> {
        let fd = self.open_dir_fd(path)?;
        let handle = self.handles.insert_dir(fd);
        Ok(ReplyOpen { fh: handle, flags })
    }

    type DirEntryStream<'a>
        = futures_util::stream::Iter<std::vec::IntoIter<fuse3::Result<DirectoryEntry>>>
    where
        Self: 'a;
    type DirEntryPlusStream<'a>
        = futures_util::stream::Iter<std::vec::IntoIter<fuse3::Result<DirectoryEntryPlus>>>
    where
        Self: 'a;

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        _path: &'a OsStr,
        fh: u64,
        offset: i64,
    ) -> Result<ReplyDirectory<Self::DirEntryStream<'a>>, fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        let logical = self.load_dir_entries(handle.as_fd());
        let mut entries: Vec<fuse3::Result<DirectoryEntry>> = Vec::with_capacity(logical.len() + 2);

        let mut idx: i64 = 0;
        entries.push(Ok(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: idx + 1,
        }));
        idx += 1;
        entries.push(Ok(DirectoryEntry {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: idx + 1,
        }));
        idx += 1;

        for entry in logical.iter() {
            idx += 1;
            entries.push(Ok(DirectoryEntry {
                kind: entry.kind,
                name: entry.name.clone(),
                offset: idx,
            }));
        }

        let skip = offset.max(0) as usize;
        let entries: Vec<_> = entries.into_iter().skip(skip).collect();
        let stream = futures_util::stream::iter(entries);
        Ok(ReplyDirectory { entries: stream })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        _parent: &'a OsStr,
        fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> Result<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>, fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        let logical = self.load_dir_entries(handle.as_fd());
        let mut entries: Vec<fuse3::Result<DirectoryEntryPlus>> =
            Vec::with_capacity(logical.len() + 2);
        let dir_attr = fstat(handle.as_fd())
            .map_err(errno_from_nix)
            .map(|stat| file_attr_from_stat(&stat))?;

        let mut idx: i64 = 0;
        entries.push(Ok(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from("."),
            offset: idx + 1,
            attr: dir_attr,
            entry_ttl: ATTR_TTL,
            attr_ttl: ATTR_TTL,
        }));
        idx += 1;
        entries.push(Ok(DirectoryEntryPlus {
            kind: FileType::Directory,
            name: OsString::from(".."),
            offset: idx + 1,
            attr: dir_attr,
            entry_ttl: ATTR_TTL,
            attr_ttl: ATTR_TTL,
        }));
        idx += 1;

        for entry in logical.iter() {
            idx += 1;
            let attr = if let Some(attr) = entry.attr.as_ref() {
                *attr
            } else {
                let c_name = match string_to_cstring(&entry.encoded) {
                    Ok(v) => v,
                    Err(err) => {
                        entries.push(Err(err));
                        continue;
                    }
                };
                let stat = match fstatat(
                    handle.as_fd(),
                    c_name.as_c_str(),
                    AtFlags::AT_SYMLINK_NOFOLLOW,
                ) {
                    Ok(st) => st,
                    Err(err) => {
                        entries.push(Err(errno_from_nix(err)));
                        continue;
                    }
                };
                file_attr_from_stat(&stat)
            };
            entries.push(Ok(DirectoryEntryPlus {
                kind: entry.kind,
                name: entry.name.clone(),
                offset: idx,
                attr,
                entry_ttl: ATTR_TTL,
                attr_ttl: ATTR_TTL,
            }));
        }

        let skip = offset as usize;
        let entries: Vec<_> = entries.into_iter().skip(skip).collect();
        let stream = futures_util::stream::iter(entries);
        Ok(ReplyDirectoryPlus { entries: stream })
    }

    async fn releasedir(
        &self,
        _req: Request,
        _path: &OsStr,
        fh: u64,
        _flags: u32,
    ) -> Result<(), fuse3::Errno> {
        self.handles.remove(fh);
        Ok(())
    }

    async fn fsyncdir(
        &self,
        _req: Request,
        _path: &OsStr,
        fh: u64,
        datasync: bool,
    ) -> Result<(), fuse3::Errno> {
        let handle = self
            .handles
            .get_dir(fh)
            .ok_or_else(|| fuse3::Errno::from(libc::EBADF))?;
        if datasync {
            fdatasync(handle.as_fd()).map_err(errno_from_nix)?;
        } else {
            fsync(handle.as_fd()).map_err(errno_from_nix)?;
        }
        self.invalidate_dir(handle.as_fd());
        Ok(())
    }

    async fn statfs(&self, _req: Request, _path: &OsStr) -> Result<ReplyStatFs, fuse3::Errno> {
        let stats = fstatvfs(self.config.backend_fd()).map_err(errno_from_nix)?;
        Ok(ReplyStatFs {
            blocks: stats.blocks(),
            bfree: stats.blocks_free(),
            bavail: stats.blocks_available(),
            files: stats.files(),
            ffree: stats.files_free(),
            bsize: stats.block_size() as u32,
            namelen: stats.name_max() as u32,
            frsize: stats.fragment_size() as u32,
        })
    }

    async fn poll(
        &self,
        _req: Request,
        _path: Option<&OsStr>,
        fh: u64,
        _kn: Option<u64>,
        _flags: u32,
        _events: u32,
        _notify: &Notify,
    ) -> Result<ReplyPoll, fuse3::Errno> {
        if self.handles.get_file(fh).is_none() {
            return Err(fuse3::Errno::from(libc::EBADF));
        }
        Ok(ReplyPoll { revents: 0 })
    }
}
