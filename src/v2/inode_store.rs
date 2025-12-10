use crate::v2::error::{CoreError, CoreResult};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

pub type InodeId = u64;

pub const ROOT_INODE: InodeId = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct BackendKey {
    pub dev: u64,
    pub ino: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InodeKind {
    Directory,
    File,
    Symlink,
    BlockDevice,
    CharDevice,
    NamedPipe,
    Socket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentName {
    pub parent: InodeId,
    pub name: OsString,
}

#[derive(Clone, Debug)]
pub struct InodeEntry {
    pub ino: InodeId,
    pub kind: InodeKind,
    pub backend: BackendKey,
    pub parent: InodeId,
    pub name: OsString,
    pub parents: Vec<ParentName>,
    pub lookup_count: u64,
    pub open_count: u32,
}

#[derive(Default)]
struct InodeStoreInner {
    entries: HashMap<InodeId, InodeEntry>,
    backend: HashMap<BackendKey, InodeId>,
}

#[derive(Default)]
pub struct InodeStore {
    next_ino: AtomicU64,
    inner: RwLock<InodeStoreInner>,
}

impl InodeStore {
    pub fn new() -> Self {
        Self {
            next_ino: AtomicU64::new(ROOT_INODE + 1),
            inner: RwLock::new(InodeStoreInner::default()),
        }
    }

    pub fn init_root(&self, backend: BackendKey) -> InodeEntry {
        let mut inner = self.inner.write();
        if inner.entries.contains_key(&ROOT_INODE) {
            let updated = {
                let existing = inner
                    .entries
                    .get_mut(&ROOT_INODE)
                    .expect("root entry missing during init");
                existing.backend = backend;
                existing.lookup_count = existing.lookup_count.max(1);
                existing.clone()
            };
            inner.backend.insert(backend, ROOT_INODE);
            return updated;
        }

        let entry = InodeEntry {
            ino: ROOT_INODE,
            kind: InodeKind::Directory,
            backend,
            parent: ROOT_INODE,
            name: OsString::from("/"),
            parents: Vec::new(),
            lookup_count: 1,
            open_count: 0,
        };
        inner.backend.insert(backend, ROOT_INODE);
        inner.entries.insert(ROOT_INODE, entry.clone());
        entry
    }

    pub fn get(&self, ino: InodeId) -> Option<InodeEntry> {
        self.inner.read().entries.get(&ino).cloned()
    }

    pub fn get_path(&self, ino: InodeId) -> CoreResult<OsString> {
        if ino == ROOT_INODE {
            return Ok(OsString::from("/"));
        }
        let inner = self.inner.read();
        let mut components = Vec::new();
        let mut current_ino = ino;
        let mut depth = 0usize;
        const MAX_DEPTH: usize = 256;

        while current_ino != ROOT_INODE {
            if depth >= MAX_DEPTH {
                return Err(CoreError::InternalMeta);
            }
            let entry = inner.entries.get(&current_ino).ok_or(CoreError::NotFound)?;
            components.push(entry.name.clone());
            if entry.parent == current_ino {
                return Err(CoreError::InternalMeta);
            }
            current_ino = entry.parent;
            depth += 1;
        }

        let mut path = OsString::from("/");
        for component in components.iter().rev() {
            if path.len() > 1 {
                path.push(OsStr::new("/"));
            }
            path.push(component);
        }
        Ok(path)
    }

    pub fn move_entry(&self, ino: InodeId, new_parent: ParentName) -> CoreResult<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino).ok_or(CoreError::NotFound)?;
        Self::set_primary_parent(entry, &new_parent);
        Ok(entry.clone())
    }

    pub fn lookup_or_create(
        &self,
        backend: BackendKey,
        kind: InodeKind,
        parent: ParentName,
    ) -> InodeEntry {
        self.get_or_insert(backend, kind, parent, 1)
    }

    pub fn get_or_insert(
        &self,
        backend: BackendKey,
        kind: InodeKind,
        parent: ParentName,
        lookup_inc: u64,
    ) -> InodeEntry {
        let mut inner = self.inner.write();
        if let Some(&ino) = inner.backend.get(&backend) {
            let entry = inner.entries.get_mut(&ino).expect("inode map out of sync");
            entry.lookup_count = entry.lookup_count.saturating_add(lookup_inc);
            Self::set_primary_parent(entry, &parent);
            return entry.clone();
        }

        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        let entry = InodeEntry {
            ino,
            kind,
            backend,
            parent: parent.parent,
            name: parent.name.clone(),
            parents: vec![parent],
            lookup_count: lookup_inc,
            open_count: 0,
        };
        inner.backend.insert(backend, ino);
        inner.entries.insert(ino, entry.clone());
        entry
    }

    pub fn inc_lookup(&self, ino: InodeId, n: u64) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        entry.lookup_count = entry.lookup_count.saturating_add(n);
        Some(entry.clone())
    }

    pub fn dec_lookup(&self, ino: InodeId, n: u64) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        entry.lookup_count = entry.lookup_count.saturating_sub(n);
        if entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE {
            let removed = inner.entries.remove(&ino)?;
            inner.backend.remove(&removed.backend);
            return Some(removed);
        }
        None
    }

    pub fn inc_open(&self, ino: InodeId) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        entry.open_count = entry.open_count.saturating_add(1);
        Some(entry.clone())
    }

    pub fn dec_open(&self, ino: InodeId) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        entry.open_count = entry.open_count.saturating_sub(1);
        if entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE {
            let removed = inner.entries.remove(&ino)?;
            inner.backend.remove(&removed.backend);
            return Some(removed);
        }
        None
    }

    pub fn add_parent_name(&self, ino: InodeId, parent: ParentName) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        Self::push_parent(entry, parent);
        Some(entry.clone())
    }

    pub fn remove_parent_name(&self, ino: InodeId, parent: &ParentName) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        let removing_primary = entry.parent == parent.parent && entry.name == parent.name;
        entry.parents.retain(|p| p != parent);
        if removing_primary {
            if let Some(new_primary) = entry.parents.first().cloned() {
                Self::set_primary_parent(entry, &new_primary);
            } else if ino != ROOT_INODE {
                entry.parent = ROOT_INODE;
                entry.name = OsString::new();
            }
        }

        inner.entries.get(&ino).cloned()
    }

    fn push_parent(entry: &mut InodeEntry, parent: ParentName) {
        if entry.parents.iter().any(|p| p == &parent) {
            return;
        }
        if entry.parents.is_empty() {
            entry.parent = parent.parent;
            entry.name = parent.name.clone();
        }
        entry.parents.push(parent);
    }

    fn set_primary_parent(entry: &mut InodeEntry, parent: &ParentName) {
        entry.parent = parent.parent;
        entry.name = parent.name.clone();
        if let Some(pos) = entry.parents.iter().position(|p| p == parent) {
            entry.parents.swap(0, pos);
        } else {
            entry.parents.insert(0, parent.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_path_reconstructs_from_parents() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let dir = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::Directory,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("old"),
            },
        );
        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 3 },
            InodeKind::File,
            ParentName {
                parent: dir.ino,
                name: OsString::from("file"),
            },
        );

        assert_eq!(
            store.get_path(child.ino).unwrap(),
            OsString::from("/old/file")
        );
    }

    #[test]
    fn move_entry_updates_primary_path_only() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let dir = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::Directory,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("old"),
            },
        );
        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 3 },
            InodeKind::File,
            ParentName {
                parent: dir.ino,
                name: OsString::from("file"),
            },
        );

        let _ = store.move_entry(
            dir.ino,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
            },
        );

        assert_eq!(store.get_path(dir.ino).unwrap(), OsString::from("/new"));
        assert_eq!(
            store.get_path(child.ino).unwrap(),
            OsString::from("/new/file")
        );
    }
}
