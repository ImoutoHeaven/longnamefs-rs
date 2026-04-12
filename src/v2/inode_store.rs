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
    pub backend_name: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct InodeEntry {
    pub ino: InodeId,
    pub kind: InodeKind,
    pub backend: BackendKey,
    pub parent: InodeId,
    pub name: OsString,
    pub backend_name: Vec<u8>,
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
            let previous_backend = existing.backend;
            existing.backend = backend;
            existing.lookup_count = existing.lookup_count.max(1);
            if previous_backend != backend
                && backend_map.get(&previous_backend).copied() == Some(ROOT_INODE)
            {
                backend_map.remove(&previous_backend);
            }
            backend_map.insert(backend, ROOT_INODE);
            return existing.clone();
        }

        let entry = InodeEntry {
            ino: ROOT_INODE,
            kind: InodeKind::Directory,
            backend,
            parent: ROOT_INODE,
            name: OsString::from("/"),
            backend_name: Vec::new(),
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

    pub fn get_by_backend(&self, backend: BackendKey) -> Option<InodeEntry> {
        let ino = self.backend_map.read().get(&backend).copied()?;
        self.get(ino)
    }

    pub fn get_path(&self, ino: InodeId) -> CoreResult<OsString> {
        if ino == ROOT_INODE {
            return Ok(OsString::from("/"));
        }

        // Orphaned inodes (no parents, primary set to root with empty name) are invalid.
        {
            let shard = self.shard(ino).read();
            if let Some(entry) = shard.entries.get(&ino)
                && entry.parent == ROOT_INODE
                && entry.name.is_empty()
                && entry.parents.is_empty()
            {
                return Err(CoreError::StaleInode);
            }
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

    pub fn get_backend_path_segments(&self, ino: InodeId) -> CoreResult<Vec<Vec<u8>>> {
        if ino == ROOT_INODE {
            return Ok(Vec::new());
        }

        {
            let shard = self.shard(ino).read();
            if let Some(entry) = shard.entries.get(&ino)
                && entry.parent == ROOT_INODE
                && entry.name.is_empty()
                && entry.parents.is_empty()
            {
                return Err(CoreError::StaleInode);
            }
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
            if entry.backend_name.is_empty() {
                return Err(CoreError::InternalMeta);
            }
            components.push(entry.backend_name.clone());
            if entry.parent == current_ino {
                return Err(CoreError::InternalMeta);
            }
            current_ino = entry.parent;
            depth += 1;
        }

        components.reverse();
        Ok(components)
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
            backend_name: parent.backend_name.clone(),
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
        {
            let mut shard = self.shards[shard_idx].write();
            let entry = shard.entries.get_mut(&ino)?;
            entry.lookup_count = entry.lookup_count.saturating_sub(n);
            let should_remove =
                entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE;
            if !should_remove {
                return None;
            }
        }

        let mut backend_map = self.backend_map.write();
        let mut shard = self.shards[shard_idx].write();
        let entry = shard.entries.get(&ino)?;
        if entry.lookup_count > 0 || entry.open_count > 0 || ino == ROOT_INODE {
            return None;
        }
        let removed = shard.entries.remove(&ino)?;
        if backend_map.get(&removed.backend).copied() == Some(ino) {
            backend_map.remove(&removed.backend);
        }
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
        {
            let mut shard = self.shards[shard_idx].write();
            let entry = shard.entries.get_mut(&ino)?;
            entry.open_count = entry.open_count.saturating_sub(1);
            let should_remove =
                entry.lookup_count == 0 && entry.open_count == 0 && ino != ROOT_INODE;
            if !should_remove {
                return None;
            }
        }

        let mut backend_map = self.backend_map.write();
        let mut shard = self.shards[shard_idx].write();
        let entry = shard.entries.get(&ino)?;
        if entry.lookup_count > 0 || entry.open_count > 0 || ino == ROOT_INODE {
            return None;
        }
        let removed = shard.entries.remove(&ino)?;
        if backend_map.get(&removed.backend).copied() == Some(ino) {
            backend_map.remove(&removed.backend);
        }
        Some(removed)
    }

    pub fn add_parent_name(&self, ino: InodeId, parent: ParentName) -> Option<InodeEntry> {
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
        Self::push_parent(entry, parent);
        Some(entry.clone())
    }

    pub fn remove_parent_name(&self, ino: InodeId, parent: &ParentName) -> Option<InodeEntry> {
        let mut backend_map = self.backend_map.write();
        let mut shard = self.shard(ino).write();
        let entry = shard.entries.get_mut(&ino)?;
        let removing_primary = entry.parent == parent.parent && entry.name == parent.name;
        entry
            .parents
            .retain(|p| !(p.parent == parent.parent && p.name == parent.name));
        if removing_primary {
            if let Some(new_primary) = entry.parents.first().cloned() {
                Self::set_primary_parent(entry, &new_primary);
            } else if ino != ROOT_INODE {
                let backend = entry.backend;
                entry.parent = ROOT_INODE;
                entry.name = OsString::new();
                entry.backend_name = Vec::new();
                if backend_map.get(&backend).copied() == Some(ino) {
                    backend_map.remove(&backend);
                }
            }
        }

        shard.entries.get(&ino).cloned()
    }

    fn push_parent(entry: &mut InodeEntry, parent: ParentName) {
        if let Some(existing) = entry
            .parents
            .iter_mut()
            .find(|p| p.parent == parent.parent && p.name == parent.name)
        {
            existing.backend_name = parent.backend_name;
            return;
        }
        if entry.parents.is_empty() {
            entry.parent = parent.parent;
            entry.name = parent.name.clone();
            entry.backend_name = parent.backend_name.clone();
        }
        entry.parents.push(parent);
    }

    fn set_primary_parent(entry: &mut InodeEntry, parent: &ParentName) {
        entry.parent = parent.parent;
        entry.name = parent.name.clone();
        entry.backend_name = parent.backend_name.clone();
        if let Some(pos) = entry
            .parents
            .iter()
            .position(|p| p.parent == parent.parent && p.name == parent.name)
        {
            entry.parents[pos].backend_name = parent.backend_name.clone();
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
    use std::sync::{Arc, Barrier};
    use std::thread;

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
                backend_name: b"old".to_vec(),
            },
        );
        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 3 },
            InodeKind::File,
            ParentName {
                parent: dir.ino,
                name: OsString::from("file"),
                backend_name: b"file".to_vec(),
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
                backend_name: b"old".to_vec(),
            },
        );
        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 3 },
            InodeKind::File,
            ParentName {
                parent: dir.ino,
                name: OsString::from("file"),
                backend_name: b"file".to_vec(),
            },
        );

        let _ = store.move_entry(
            dir.ino,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
                backend_name: b"new".to_vec(),
            },
        );

        assert_eq!(store.get_path(dir.ino).unwrap(), OsString::from("/new"));
        assert_eq!(
            store.get_path(child.ino).unwrap(),
            OsString::from("/new/file")
        );
    }

    #[test]
    fn get_or_insert_sets_backend_name() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let child = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("x"),
                backend_name: b"bx".to_vec(),
            },
        );
        assert_eq!(child.backend_name, b"bx".to_vec());
        assert_eq!(child.parents.len(), 1);
        assert_eq!(child.parents[0].backend_name, b"bx".to_vec());
    }

    #[test]
    fn move_entry_updates_backend_name() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let dir = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::Directory,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("a"),
                backend_name: b"ba".to_vec(),
            },
        );

        let moved = store
            .move_entry(
                dir.ino,
                ParentName {
                    parent: ROOT_INODE,
                    name: OsString::from("b"),
                    backend_name: b"bb".to_vec(),
                },
            )
            .unwrap();
        assert_eq!(moved.backend_name, b"bb".to_vec());
    }

    #[test]
    fn remove_parent_name_switches_primary_and_backend_name() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let file = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("p1"),
                backend_name: b"bp1".to_vec(),
            },
        );
        let _ = store.add_parent_name(
            file.ino,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("p2"),
                backend_name: b"bp2".to_vec(),
            },
        );

        let updated = store
            .remove_parent_name(
                file.ino,
                &ParentName {
                    parent: ROOT_INODE,
                    name: OsString::from("p1"),
                    backend_name: Vec::new(),
                },
            )
            .unwrap();
        assert_eq!(updated.name, OsString::from("p2"));
        assert_eq!(updated.backend_name, b"bp2".to_vec());
    }

    #[test]
    fn multiple_parents_track_distinct_backend_names() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let file = store.lookup_or_create(
            BackendKey { dev: 1, ino: 2 },
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("a"),
                backend_name: b"ba".to_vec(),
            },
        );
        let _ = store.add_parent_name(
            file.ino,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("b"),
                backend_name: b"bb".to_vec(),
            },
        );

        let fetched = store.get(file.ino).unwrap();
        assert_eq!(fetched.parents.len(), 2);
        assert!(
            fetched
                .parents
                .iter()
                .any(|p| p.name == "a" && p.backend_name == b"ba".to_vec())
        );
        assert!(
            fetched
                .parents
                .iter()
                .any(|p| p.name == "b" && p.backend_name == b"bb".to_vec())
        );
    }

    #[test]
    fn get_by_backend_returns_none_when_missing() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        assert!(
            store
                .get_by_backend(BackendKey { dev: 99, ino: 99 })
                .is_none()
        );
    }

    #[test]
    fn get_by_backend_returns_entry_when_present() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let child = store.lookup_or_create(
            BackendKey { dev: 2, ino: 3 },
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("hit"),
                backend_name: b"hit".to_vec(),
            },
        );

        let hit = store
            .get_by_backend(BackendKey { dev: 2, ino: 3 })
            .expect("backend key should resolve existing inode");
        assert_eq!(hit.ino, child.ino);
        assert_eq!(hit.name, OsString::from("hit"));
    }

    #[test]
    fn orphaned_open_inode_does_not_alias_reused_backend_key() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let backend = BackendKey { dev: 7, ino: 11 };
        let original_parent = ParentName {
            parent: ROOT_INODE,
            name: OsString::from("old"),
            backend_name: b"old".to_vec(),
        };
        let original = store.lookup_or_create(backend, InodeKind::File, original_parent.clone());
        let _ = store.inc_open(original.ino);

        let orphaned = store
            .remove_parent_name(original.ino, &original_parent)
            .expect("inode should still exist while open");
        assert_eq!(orphaned.parents.len(), 0);
        assert_eq!(orphaned.name, OsString::new());

        let reused = store.get_or_insert(
            backend,
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
                backend_name: b"new".to_vec(),
            },
            1,
        );

        assert_ne!(reused.ino, original.ino);
        let still_open_old = store
            .get(original.ino)
            .expect("orphaned inode must remain while open");
        assert_eq!(still_open_old.parents.len(), 0);
        assert_eq!(still_open_old.name, OsString::new());
        assert_eq!(still_open_old.open_count, 1);
        assert_eq!(
            store
                .get_by_backend(backend)
                .expect("backend should resolve to new inode")
                .ino,
            reused.ino
        );
    }

    #[test]
    fn dec_lookup_cleanup_keeps_reused_backend_mapping() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let backend = BackendKey { dev: 7, ino: 21 };
        let old_parent = ParentName {
            parent: ROOT_INODE,
            name: OsString::from("old"),
            backend_name: b"old".to_vec(),
        };
        let old = store.lookup_or_create(backend, InodeKind::File, old_parent.clone());
        let _ = store.inc_open(old.ino);
        let _ = store
            .remove_parent_name(old.ino, &old_parent)
            .expect("old inode should stay while open");

        let reused = store.get_or_insert(
            backend,
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
                backend_name: b"new".to_vec(),
            },
            1,
        );
        assert_ne!(reused.ino, old.ino);

        let _ = store.dec_open(old.ino);
        let removed = store
            .dec_lookup(old.ino, 1)
            .expect("old inode should be removed when lookup reaches zero");
        assert_eq!(removed.ino, old.ino);

        assert_eq!(
            store
                .get_by_backend(backend)
                .expect("backend mapping should remain on reused inode")
                .ino,
            reused.ino
        );
    }

    #[test]
    fn dec_open_cleanup_keeps_reused_backend_mapping() {
        let store = InodeStore::new();
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let backend = BackendKey { dev: 7, ino: 31 };
        let old_parent = ParentName {
            parent: ROOT_INODE,
            name: OsString::from("old"),
            backend_name: b"old".to_vec(),
        };
        let old = store.lookup_or_create(backend, InodeKind::File, old_parent.clone());
        let _ = store.inc_open(old.ino);
        let _ = store
            .remove_parent_name(old.ino, &old_parent)
            .expect("old inode should stay while open");
        let _ = store.dec_lookup(old.ino, 1);

        let reused = store.get_or_insert(
            backend,
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
                backend_name: b"new".to_vec(),
            },
            1,
        );
        assert_ne!(reused.ino, old.ino);

        let removed = store
            .dec_open(old.ino)
            .expect("old inode should be removed when open reaches zero");
        assert_eq!(removed.ino, old.ino);

        assert_eq!(
            store
                .get_by_backend(backend)
                .expect("backend mapping should remain on reused inode")
                .ino,
            reused.ino
        );
    }

    #[test]
    fn concurrent_cleanup_keeps_reused_backend_mapping() {
        let store = Arc::new(InodeStore::new());
        store.init_root(BackendKey { dev: 1, ino: 1 });

        let backend = BackendKey { dev: 7, ino: 41 };
        let old_parent = ParentName {
            parent: ROOT_INODE,
            name: OsString::from("old"),
            backend_name: b"old".to_vec(),
        };
        let old = store.lookup_or_create(backend, InodeKind::File, old_parent.clone());
        let _ = store.inc_open(old.ino);
        let _ = store
            .remove_parent_name(old.ino, &old_parent)
            .expect("old inode should stay while open");

        let reused = store.get_or_insert(
            backend,
            InodeKind::File,
            ParentName {
                parent: ROOT_INODE,
                name: OsString::from("new"),
                backend_name: b"new".to_vec(),
            },
            1,
        );
        assert_ne!(reused.ino, old.ino);

        let start = Arc::new(Barrier::new(3));
        let store_for_lookup = Arc::clone(&store);
        let start_for_lookup = Arc::clone(&start);
        let lookup_handle = thread::spawn(move || {
            start_for_lookup.wait();
            store_for_lookup.dec_lookup(old.ino, 1)
        });

        let store_for_open = Arc::clone(&store);
        let start_for_open = Arc::clone(&start);
        let open_handle = thread::spawn(move || {
            start_for_open.wait();
            store_for_open.dec_open(old.ino)
        });

        start.wait();
        let lookup_removed = lookup_handle
            .join()
            .expect("lookup thread should not panic");
        let open_removed = open_handle.join().expect("open thread should not panic");

        let removed_count = lookup_removed.is_some() as u8 + open_removed.is_some() as u8;
        assert_eq!(
            removed_count, 1,
            "exactly one cleanup path should remove old inode"
        );
        assert!(store.get(old.ino).is_none());
        assert_eq!(
            store
                .get_by_backend(backend)
                .expect("backend mapping should remain on reused inode")
                .ino,
            reused.ino
        );
    }

    #[test]
    fn init_root_removes_stale_backend_mapping_when_backend_changes() {
        let store = InodeStore::new();
        let old_backend = BackendKey { dev: 1, ino: 1 };
        let new_backend = BackendKey { dev: 2, ino: 2 };

        store.init_root(old_backend);
        store.init_root(new_backend);

        assert!(store.get_by_backend(old_backend).is_none());
        assert_eq!(
            store
                .get_by_backend(new_backend)
                .expect("new root backend should be mapped")
                .ino,
            ROOT_INODE
        );
    }
}
