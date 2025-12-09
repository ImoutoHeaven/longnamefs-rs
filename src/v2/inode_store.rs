use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
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
    pub path: OsString,
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
            path: OsString::from("/"),
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

    pub fn lookup_or_create(
        &self,
        backend: BackendKey,
        kind: InodeKind,
        path: OsString,
        parent: Option<ParentName>,
    ) -> InodeEntry {
        self.get_or_insert(backend, kind, path, parent, 1)
    }

    pub fn get_or_insert(
        &self,
        backend: BackendKey,
        kind: InodeKind,
        path: OsString,
        parent: Option<ParentName>,
        lookup_inc: u64,
    ) -> InodeEntry {
        let mut inner = self.inner.write();
        if let Some(&ino) = inner.backend.get(&backend) {
            let entry = inner.entries.get_mut(&ino).expect("inode map out of sync");
            entry.lookup_count = entry.lookup_count.saturating_add(lookup_inc);
            entry.path = path.clone();
            if let Some(parent) = parent {
                Self::push_parent(entry, parent);
            }
            return entry.clone();
        }

        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        let mut entry = InodeEntry {
            ino,
            kind,
            backend,
            path,
            parents: Vec::new(),
            lookup_count: lookup_inc,
            open_count: 0,
        };
        if let Some(parent) = parent {
            Self::push_parent(&mut entry, parent);
        }
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
        entry.parents.retain(|p| p != parent);
        Some(entry.clone())
    }

    pub fn update_path(&self, ino: InodeId, path: OsString) -> Option<InodeEntry> {
        let mut inner = self.inner.write();
        let entry = inner.entries.get_mut(&ino)?;
        entry.path = path;
        Some(entry.clone())
    }

    pub fn rename_descendants(&self, old_prefix: &OsStr, new_prefix: &OsStr) {
        let old_bytes = old_prefix.as_bytes();
        let new_bytes = new_prefix.as_bytes();

        let mut inner = self.inner.write();
        for entry in inner.entries.values_mut() {
            let path_bytes = entry.path.as_bytes();
            if path_bytes.len() < old_bytes.len() {
                continue;
            }
            if !path_bytes.starts_with(old_bytes) {
                continue;
            }
            if path_bytes.len() > old_bytes.len() && path_bytes.get(old_bytes.len()) != Some(&b'/')
            {
                continue;
            }

            let mut replaced =
                Vec::with_capacity(new_bytes.len() + path_bytes.len() - old_bytes.len());
            replaced.extend_from_slice(new_bytes);
            replaced.extend_from_slice(&path_bytes[old_bytes.len()..]);
            entry.path = OsString::from_vec(replaced);
        }
    }

    fn push_parent(entry: &mut InodeEntry, parent: ParentName) {
        if entry.parents.iter().any(|p| p == &parent) {
            return;
        }
        entry.parents.push(parent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rename_descendants_updates_subtree_paths() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let dir = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::Directory,
            OsString::from("/old"),
            Some(ParentName {
                parent: ROOT_INODE,
                name: OsString::from("old"),
            }),
        );
        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 3 },
            InodeKind::File,
            OsString::from("/old/file"),
            Some(ParentName {
                parent: dir.ino,
                name: OsString::from("file"),
            }),
        );
        let outside = store.lookup_or_create(
            BackendKey { dev: 1, ino: 4 },
            InodeKind::File,
            OsString::from("/oldest/file"),
            Some(ParentName {
                parent: ROOT_INODE,
                name: OsString::from("oldest"),
            }),
        );

        store.rename_descendants(OsStr::new("/old"), OsStr::new("/new"));

        assert_eq!(store.get(dir.ino).unwrap().path, OsString::from("/new"));
        assert_eq!(
            store.get(child.ino).unwrap().path,
            OsString::from("/new/file")
        );
        // Prefix matches must respect path boundaries.
        assert_eq!(
            store.get(outside.ino).unwrap().path,
            OsString::from("/oldest/file")
        );
    }
}
