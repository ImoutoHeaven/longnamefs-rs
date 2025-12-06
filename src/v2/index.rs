#![allow(dead_code)]

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirIndexEntry {
    pub backend_name: String,
    pub raw_name: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct DirIndex {
    entries: HashMap<String, DirIndexEntry>,
    dirty: bool,
}

impl DirIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            dirty: false,
        }
    }

    pub fn upsert(&mut self, backend_name: String, raw_name: Vec<u8>) {
        let entry = DirIndexEntry {
            backend_name: backend_name.clone(),
            raw_name,
        };
        self.entries.insert(backend_name, entry);
        self.dirty = true;
    }

    pub fn remove(&mut self, backend_name: &str) -> Option<DirIndexEntry> {
        let removed = self.entries.remove(backend_name);
        if removed.is_some() {
            self.dirty = true;
        }
        removed
    }

    pub fn get(&self, backend_name: &str) -> Option<&DirIndexEntry> {
        self.entries.get(backend_name)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &DirIndexEntry)> {
        self.entries.iter()
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn clear_dirty(&mut self) {
        self.dirty = false;
    }

    pub fn is_dirty(&self) -> bool {
        self.dirty
    }
}
