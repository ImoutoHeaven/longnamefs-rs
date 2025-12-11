use crate::v2::error::{CoreError, CoreResult};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::sync::atomic::{AtomicU64, Ordering};

pub type InodeId = u64;

pub const ROOT_INODE: InodeId = 1;
const INODE_SHARD_COUNT: usize = 64;

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
struct InodeShard {
    entries: HashMap<InodeId, InodeEntry>,
}

pub struct InodeStore {
    next_ino: AtomicU64,
    shards: Vec<RwLock<InodeShard>>,
    backend_map: RwLock<HashMap<BackendKey, InodeId>>,
}

impl InodeStore {
    pub fn new() -> Self {
        Self {
            next_ino: AtomicU64::new(ROOT_INODE + 1),
            shards: (0..INODE_SHARD_COUNT)
                .map(|_| RwLock::new(InodeShard::default()))
                .collect(),
            backend_map: RwLock::new(HashMap::new()),
        }
    }

    #[inline]
    fn shard_index(ino: InodeId) -> usize {
        debug_assert!(INODE_SHARD_COUNT.is_power_of_two());
        (ino as usize) & (INODE_SHARD_COUNT - 1)
    }

    #[inline]
    fn shard(&self, ino: InodeId) -> &RwLock<InodeShard> {
        &self.shards[Self::shard_index(ino)]
    }

    pub fn init_root(&self, backend: BackendKey) -> InodeEntry {
        let shard_idx = Self::shard_index(ROOT_INODE);
        let mut backend_map = self.backend_map.write();
        let mut shard = self.shards[shard_idx].write();

        if let Some(existing) = shard.entries.get_mut(&ROOT_INODE) {
            existing.backend = backend;
            existing.lookup_count = existing.lookup_count.max(1);
            backend_map.insert(backend, ROOT_INODE);
            return existing.clone();
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
        backend_map.insert(backend, ROOT_INODE);
        shard.entries.insert(ROOT_INODE, entry.clone());
        entry
    }

    pub fn get(&self, ino: InodeId) -> Option<InodeEntry> {
        let shard = self.shard(ino).read();
        shard.entries.get(&ino).cloned()
    }

    pub fn get_path(&self, ino: InodeId) -> CoreResult<OsString> {
        if ino == ROOT_INODE {
            return Ok(OsString::from("/"));
        }
        let mut components = Vec::new();
        let mut current_ino = ino;
        let mut depth = 0usize;
        const MAX_DEPTH: usize = 256;

        while current_ino != ROOT_INODE {
            if depth >= MAX_DEPTH {
                return Err(CoreError::InternalMeta);
            }
            let shard = self.shard(current_ino).read();
            let entry = shard.entries.get(&current_ino).ok_or(CoreError::NotFound)?;
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
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino).ok_or(CoreError::NotFound)?;
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
        if let Some(ino) = self.backend_map.read().get(&backend).copied() {
            let mut shard = self.shard(ino).write();
            if let Some(entry) = shard.entries.get_mut(&ino) {
                entry.lookup_count = entry.lookup_count.saturating_add(lookup_inc);
                Self::set_primary_parent(entry, &parent);
                return entry.clone();
            }
        }

        let mut backend_guard = self.backend_map.write();
        if let Some(&ino) = backend_guard.get(&backend) {
            let mut shard = self.shard(ino).write();
            if let Some(entry) = shard.entries.get_mut(&ino) {
                entry.lookup_count = entry.lookup_count.saturating_add(lookup_inc);
                Self::set_primary_parent(entry, &parent);
                return entry.clone();
            }
            backend_guard.remove(&backend);
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
        let shard_idx = Self::shard_index(ino);
        backend_guard.insert(backend, ino);
        {
            let mut shard = self.shards[shard_idx].write();
            shard.entries.insert(ino, entry.clone());
        }
        entry
    }

    pub fn inc_lookup(&self, ino: InodeId, n: u64) -> Option<InodeEntry> {
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
        entry.lookup_count = entry.lookup_count.saturating_add(n);
        Some(entry.clone())
    }

    pub fn dec_lookup(&self, ino: InodeId, n: u64) -> Option<InodeEntry> {
        let shard_idx = Self::shard_index(ino);
        let should_remove = {
            let mut shard = self.shards[shard_idx].write();
            let entry = shard.entries.get_mut(&ino)?;
            entry.lookup_count = entry.lookup_count.saturating_sub(n);
            entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE
        };

        if !should_remove {
            return None;
        }

        let mut backend_map = self.backend_map.write();
        let mut shard = self.shards[shard_idx].write();
        let entry = shard.entries.get(&ino)?;
        if entry.lookup_count > 0 || entry.open_count > 0 || ino == ROOT_INODE {
            return None;
        }
        let removed = shard.entries.remove(&ino)?;
        backend_map.remove(&removed.backend);
        Some(removed)
    }

    pub fn inc_open(&self, ino: InodeId) -> Option<InodeEntry> {
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
        entry.open_count = entry.open_count.saturating_add(1);
        Some(entry.clone())
    }

    pub fn dec_open(&self, ino: InodeId) -> Option<InodeEntry> {
        let shard_idx = Self::shard_index(ino);
        let should_remove = {
            let mut shard = self.shards[shard_idx].write();
            let entry = shard.entries.get_mut(&ino)?;
            entry.open_count = entry.open_count.saturating_sub(1);
            entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE
        };

        if !should_remove {
            return None;
        }

        let mut backend_map = self.backend_map.write();
        let mut shard = self.shards[shard_idx].write();
        let entry = shard.entries.get(&ino)?;
        if entry.lookup_count > 0 || entry.open_count > 0 || ino == ROOT_INODE {
            return None;
        }
        let removed = shard.entries.remove(&ino)?;
        backend_map.remove(&removed.backend);
        Some(removed)
    }

    pub fn add_parent_name(&self, ino: InodeId, parent: ParentName) -> Option<InodeEntry> {
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
        Self::push_parent(entry, parent);
        Some(entry.clone())
    }

    pub fn remove_parent_name(&self, ino: InodeId, parent: &ParentName) -> Option<InodeEntry> {
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
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

        shard.entries.get(&ino).cloned()
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

impl Default for InodeStore {
    fn default() -> Self {
        Self::new()
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
