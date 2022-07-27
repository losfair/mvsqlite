use std::collections::BTreeMap;

use rand::{thread_rng, Rng};
use rpds::RedBlackTreeMapSync;

pub struct Inmem {
    versions: BTreeMap<String, RedBlackTreeMapSync<u32, [u8; 32]>>,
    inflight: BTreeMap<u64, RedBlackTreeMapSync<u32, [u8; 32]>>,
    next_inflight_id: u64,
    version_list: Vec<String>,
}

impl Inmem {
    pub fn new() -> Self {
        Self {
            versions: BTreeMap::new(),
            inflight: BTreeMap::new(),
            next_inflight_id: 1,
            version_list: Vec::new(),
        }
    }

    pub fn start_transaction(&mut self, from_version: &str) -> u64 {
        let id = self.next_inflight_id;
        self.next_inflight_id += 1;
        let snapshot = self
            .versions
            .range(..=String::from(from_version))
            .last()
            .map(|x| x.1.clone())
            .unwrap_or_default();
        self.inflight.insert(id, snapshot);
        id
    }

    pub fn commit_transaction(&mut self, id: u64, version: &str) {
        let map = self.inflight.remove(&id).expect("inflight not found");
        self.versions.insert(version.to_string(), map);
        self.version_list.push(version.to_string());
    }

    pub fn drop_transaction(&mut self, id: u64) {
        self.inflight.remove(&id).expect("inflight not found");
    }

    pub fn write_page(&mut self, id: u64, index: u32, data: &[u8]) {
        let version = self.inflight.get_mut(&id).expect("inflight not found");
        let hash = blake3::hash(data);
        version.insert_mut(index, *hash.as_bytes());
    }

    pub fn verify_page(&self, id: u64, index: u32, data: &[u8]) {
        let version = self.inflight.get(&id).expect("inflight not found");

        if data.is_empty() {
            assert!(version.get(&index).is_none(), "page should not exist");
        } else {
            let computed_hash = blake3::hash(data);
            let stored_hash = *version.get(&index).expect("page not found");
            if stored_hash != *computed_hash.as_bytes() {
                panic!("hash mismatch at index {}", index);
            }
        }
    }

    pub fn pick_random_version(&self) -> Option<&str> {
        if self.version_list.is_empty() {
            None
        } else {
            let index = thread_rng().gen_range(0..self.version_list.len());
            Some(&self.version_list[index])
        }
    }
}
