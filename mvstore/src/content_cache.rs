use anyhow::Result;
use bytes::Bytes;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, RwTransaction, Transaction, WriteFlags,
};
use std::path::Path;

pub struct ContentCache {
    env: Environment,
    metadata_db: Database,
    db1: Database,
    db2: Database,
    threshold_size: u64,
}

const ENTRY_TYPE_V0: u8 = 0;

impl ContentCache {
    pub fn new(path: &Path, threshold_size: u64) -> Result<Self> {
        std::fs::create_dir_all(path)?;
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

    fn check_size_and_commit(&self, mut txn: RwTransaction) {
        let stat = txn.stat(self.primary_db(&txn)).unwrap();
        if (stat.branch_pages() + stat.leaf_pages() + stat.overflow_pages()) as u64
            * stat.page_size() as u64
            > self.threshold_size
        {
            tracing::info!("swapping primary cache db");
            self.swap_primary(&mut txn);
        }

        txn.commit().unwrap();
    }

    pub fn set(&self, hash: [u8; 32], data: &[u8]) {
        let mut txn = self.env.begin_rw_txn().unwrap();
        let db = self.primary_db(&txn);

        let buf = match txn.reserve(db, &hash, 1 + data.len(), WriteFlags::NO_OVERWRITE) {
            Ok(buf) => buf,
            Err(lmdb::Error::KeyExist) => return,
            Err(e) => panic!("lmdb txn set error: {}", e),
        };
        buf[0] = ENTRY_TYPE_V0;
        buf[1..].copy_from_slice(data);

        self.check_size_and_commit(txn);
    }

    pub fn get(&self, hash: [u8; 32]) -> Option<Bytes> {
        let txn = self.env.begin_ro_txn().unwrap();
        let db = self.primary_db(&txn);
        let buf = match txn.get(db, &hash) {
            Ok(x) => Bytes::copy_from_slice(x),
            Err(lmdb::Error::NotFound) => {
                let secondary = self.secondary_db(&txn);
                match txn.get(secondary, &hash) {
                    Ok(x) => {
                        let x = Bytes::copy_from_slice(x);
                        drop(txn);
                        let mut txn = self.env.begin_rw_txn().unwrap();
                        txn.put(db, &hash, &x, WriteFlags::empty()).unwrap();
                        self.check_size_and_commit(txn);
                        x
                    }
                    Err(lmdb::Error::NotFound) => return None,
                    Err(e) => panic!("lmdb txn get error: {}", e),
                }
            }
            Err(e) => panic!("lmdb txn get error: {}", e),
        };
        if buf[0] != ENTRY_TYPE_V0 {
            return None;
        }
        Some(buf.slice(1..))
    }
}
