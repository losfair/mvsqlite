use std::path::Path;

use anyhow::Result;
use bytes::Bytes;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RwTransaction, Transaction, WriteFlags,
};
use moka::sync::Cache;

pub struct PageCache {
    env: Environment,
    metadata_db: Database,
    db1: Database,
    db2: Database,
    index: Cache<u32, IndexEntry>,
    generation: u64,
    threshold_size: u64,
}

#[derive(Clone, Copy)]
struct IndexEntry {
    generation: u64,
    version: [u8; 10],
}

pub enum PageCacheEntry {
    Fresh { data: Bytes },
    Stale { data: Bytes, candidate: String },
    NotFound,
}

impl PageCache {
    pub fn new(path: &Path, index_cache_size: u64, threshold_size: u64) -> Result<Self> {
        let env = Environment::new()
            .set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_READAHEAD)
            .set_max_dbs(2)
            .set_map_size((threshold_size * 3) as usize)
            .open(path)?;
        let metadata_db = env.create_db(None, DatabaseFlags::empty())?;
        let db1 = env.create_db(Some("db1"), DatabaseFlags::empty())?;
        let db2 = env.create_db(Some("db2"), DatabaseFlags::empty())?;

        // Initialize primary
        {
            let mut txn = env.begin_rw_txn()?;
            match txn.get(metadata_db, &"primary") {
                Ok(_) => {}
                Err(lmdb::Error::NotFound) => {
                    txn.put(metadata_db, &"primary", &"db1", WriteFlags::empty())?;
                    txn.commit()?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(Self {
            env,
            metadata_db,
            db1,
            db2,
            index: Cache::new(index_cache_size),
            generation: 0,
            threshold_size,
        })
    }

    fn primary_db<T: Transaction>(&self, txn: &T) -> Database {
        match txn.get(self.metadata_db, b"primary").unwrap() {
            b"db1" => self.db1,
            b"db2" => self.db2,
            _ => panic!("Invalid primary"),
        }
    }

    fn secondary_db<T: Transaction>(&self, txn: &T) -> Database {
        match txn.get(self.metadata_db, b"primary").unwrap() {
            b"db1" => self.db2,
            b"db2" => self.db1,
            _ => panic!("Invalid primary"),
        }
    }

    fn swap_primary(&self, txn: &mut RwTransaction) {
        let mut primary = txn.get(self.metadata_db, b"primary").unwrap();
        if primary == b"db1" {
            txn.clear_db(self.db2).unwrap();
            primary = b"db2";
        } else {
            txn.clear_db(self.db1).unwrap();
            primary = b"db1";
        }
        txn.put(self.metadata_db, &b"primary", &primary, WriteFlags::empty())
            .unwrap();
    }

    pub fn mark_all_as_stale(&mut self) {
        self.generation += 1;
    }

    pub fn invalidate(&mut self, page_index: u32) {
        self.index.invalidate(&page_index);
    }

    pub fn maybe_contains_key(&self, page_index: u32) -> bool {
        self.index.contains_key(&page_index)
    }

    pub fn insert(&mut self, page_index: u32, page_version: [u8; 10], data: &[u8]) {
        let key = construct_page_key(page_index, page_version);
        let mut txn = self.env.begin_rw_txn().unwrap();
        let primary = self.primary_db(&txn);
        let secondary = self.secondary_db(&txn);
        let _ = txn.del(secondary, &key, None);
        match txn.put(primary, &key, &data, WriteFlags::NO_OVERWRITE) {
            Ok(()) => {}
            Err(lmdb::Error::KeyExist) => {}
            Err(e) => panic!("txn put failed: {}", e),
        }

        let primary_stat = txn.stat(primary).unwrap();
        if (primary_stat.branch_pages() + primary_stat.leaf_pages() + primary_stat.overflow_pages())
            as u64
            * primary_stat.page_size() as u64
            > self.threshold_size
        {
            tracing::info!("swapping primary cache space");
            self.swap_primary(&mut txn);
        }

        txn.commit().unwrap();
        self.index.insert(
            page_index,
            IndexEntry {
                generation: self.generation,
                version: page_version,
            },
        );
    }

    pub fn get(&self, page_index: u32) -> PageCacheEntry {
        let entry = match self.index.get(&page_index) {
            Some(entry) => entry,
            None => return PageCacheEntry::NotFound,
        };
        let key = construct_page_key(page_index, entry.version);
        let txn = self.env.begin_ro_txn().unwrap();
        let primary = self.primary_db(&txn);
        let secondary = self.secondary_db(&txn);
        let data = match txn.get(primary, &key) {
            Ok(x) => x,
            Err(lmdb::Error::NotFound) => match txn.get(secondary, &key) {
                Ok(x) => {
                    let mut txn = self.env.begin_rw_txn().unwrap();
                    let _ = txn.del(secondary, &key, None);
                    txn.put(primary, &key, &x, WriteFlags::empty()).unwrap();
                    txn.commit().unwrap();
                    x
                }
                Err(lmdb::Error::NotFound) => {
                    return PageCacheEntry::NotFound;
                }
                Err(e) => panic!("lmdb get error: {}", e),
            },
            Err(e) => panic!("lmdb get error: {}", e),
        };
        let data = Bytes::from(data.to_vec());

        if entry.generation != self.generation {
            return PageCacheEntry::Stale {
                data,
                candidate: hex::encode(&entry.version),
            };
        }

        PageCacheEntry::Fresh { data }
    }
}

fn construct_page_key(page_index: u32, page_version: [u8; 10]) -> [u8; 15] {
    let mut key = [0; 15];
    key[0] = b'p';
    key[1..5].copy_from_slice(&page_index.to_be_bytes());
    key[5..].copy_from_slice(&page_version);
    key
}
