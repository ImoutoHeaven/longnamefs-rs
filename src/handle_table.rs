use std::collections::HashMap;
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone)]
pub enum Handle {
    File(Arc<OwnedFd>),
    Dir(Arc<OwnedFd>),
}

impl Handle {
    pub fn as_fd(&self) -> BorrowedFd<'_> {
        match self {
            Handle::File(fd) | Handle::Dir(fd) => fd.as_fd(),
        }
    }
}

#[derive(Debug, Default)]
pub struct HandleTable {
    next_id: AtomicU64,
    entries: RwLock<HashMap<u64, Handle>>,
}

impl HandleTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_file(&self, fd: OwnedFd) -> u64 {
        self.insert(Handle::File(Arc::new(fd)))
    }

    pub fn insert_dir(&self, fd: OwnedFd) -> u64 {
        self.insert(Handle::Dir(Arc::new(fd)))
    }

    pub fn get_file(&self, id: u64) -> Option<Handle> {
        self.get(id, true)
    }

    pub fn get_dir(&self, id: u64) -> Option<Handle> {
        self.get(id, false)
    }

    pub fn remove(&self, id: u64) -> Option<Handle> {
        self.entries.write().unwrap().remove(&id)
    }

    fn insert(&self, handle: Handle) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.entries.write().unwrap().insert(id, handle);
        id
    }

    fn get(&self, id: u64, want_file: bool) -> Option<Handle> {
        let guard = self.entries.read().unwrap();
        let handle = guard.get(&id)?;
        match (want_file, handle) {
            (true, Handle::File(_)) | (false, Handle::Dir(_)) => Some(handle.clone()),
            _ => None,
        }
    }
}
