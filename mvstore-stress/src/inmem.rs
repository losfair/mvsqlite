use std::collections::{BTreeMap, HashSet};

use rand::{thread_rng, Rng};
use rpds::RedBlackTreeMapSync;

pub struct Inmem {
    pub versions: BTreeMap<String, TxnInfo>,
    inflight: BTreeMap<u64, TxnInfo>,
    next_inflight_id: u64,
    pub version_list: Vec<String>,
}

pub struct TxnInfo {
    snapshot: RedBlackTreeMapSync<u32, [u8; 32]>,
    write_set: HashSet<u32>,
    read_set: HashSet<u32>,
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
            .map(|x| x.1.snapshot.clone())
            .unwrap_or_default();
        self.inflight.insert(
            id,
            TxnInfo {
                snapshot,
                write_set: Default::default(),
                read_set: Default::default(),
            },
        );
        id
    }

    pub fn commit_transaction(&mut self, id: u64, version: &str, from_version: &str) {
        if let Some((last_version, _)) = self.versions.iter().rev().next() {
            assert!(version > last_version.as_str());
        }

        let mut this = self.inflight.remove(&id).expect("inflight not found");
        let prev_versions = self
            .versions
            .range(format!("{}\0", from_version)..version.to_string());

        let mut write_set: HashSet<u32> = HashSet::new();
        let mut num_prev_versions = 0;
        for (_, prev) in prev_versions {
            write_set.extend(prev.write_set.iter().copied());
            num_prev_versions += 1;
        }

        // Ensure no intersection
        if write_set
            .iter()
            .copied()
            .chain(this.read_set.iter().copied())
            .collect::<HashSet<_>>()
            .len()
            != write_set.len() + this.read_set.len()
        {
            panic!("conflict detected");
        }

        tracing::debug!(
            from = from_version,
            num_prev_versions,
            their_write_set_size = write_set.len(),
            our_read_set_size = this.read_set.len(),
            our_write_set_size = this.write_set.len(),
            "merged"
        );

        if let Some((_, prev)) = self.versions.iter().rev().next() {
            let mut new_map = prev.snapshot.clone();
            for index in &this.write_set {
                new_map.insert_mut(*index, this.snapshot[index]);
            }
            this.snapshot = new_map;
        }

        tracing::debug!(id, version, from = from_version, write_set = ?this.write_set, "commit transaction");
        self.versions.insert(version.to_string(), this);
        self.version_list.push(version.to_string());
    }

    pub fn drop_transaction(&mut self, id: u64) {
        self.inflight.remove(&id).expect("inflight not found");
    }

    pub fn write_page(&mut self, id: u64, index: u32, data: &[u8]) {
        let txn_info = self.inflight.get_mut(&id).expect("inflight not found");
        let hash = blake3::hash(data);
        txn_info.snapshot.insert_mut(index, *hash.as_bytes());
        txn_info.write_set.insert(index);
    }

    pub fn verify_page(&mut self, id: u64, index: u32, data: &[u8], desc: &str) {
        let txn_info = self.inflight.get_mut(&id).expect("inflight not found");

        if data.is_empty() {
            assert!(
                txn_info.snapshot.get(&index).is_none(),
                "page should not exist ({})",
                desc
            );
        } else {
            let computed_hash = blake3::hash(data);
            let stored_hash = match txn_info.snapshot.get(&index) {
                Some(x) => *x,
                None => panic!("page not found: {} ({})", index, desc),
            };
            if stored_hash != *computed_hash.as_bytes() {
                panic!("hash mismatch at index {}", index);
            }
        }

        txn_info.read_set.insert(index);
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
