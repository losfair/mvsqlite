use std::{path::Path, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use blake3::Hash;
use bytes::Bytes;
use futures::lock::Mutex;
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use moka::future::Cache;

use crate::{CacheLoader, LoadOutput, VersionedPageCache};

pub struct LmdbBackend {
    index: Cache<u32, Arc<Mutex<IndexEntry>>>,
    generation: u64,
    env: Environment,
    db: Database,
}

#[derive(Copy, Clone)]
struct IndexEntry {
    generation: u64,
    version: [u8; 10],
    hash: Option<Hash>,
}

impl LmdbBackend {
    pub fn new(hash_cache_size: usize, path: &Path) -> Result<Self> {
        let env = Environment::new()
            .set_flags(EnvironmentFlags::NO_SYNC | EnvironmentFlags::NO_READAHEAD)
            .open(path)?;
        let db = env.create_db(None, DatabaseFlags::empty())?;
        Ok(Self {
            index: Cache::new(hash_cache_size as u64),
            generation: 0,
            env,
            db,
        })
    }

    fn add_to_content_store<T: AsRef<[u8]>>(&self, data: Option<T>) -> Option<Hash> {
        if let Some(data) = data {
            let hash = blake3::hash(data.as_ref());
            let mut txn = self.env.begin_rw_txn().unwrap();
            match txn.put(self.db, hash.as_bytes(), &data, WriteFlags::NO_OVERWRITE) {
                Ok(()) => {}
                Err(lmdb::Error::KeyExist) => {}
                Err(e) => panic!("put error: {}", e),
            }
            txn.commit().unwrap();
            Some(hash)
        } else {
            None
        }
    }
}

#[async_trait]
impl VersionedPageCache for LmdbBackend {
    async fn get(&self, key: u32, load: CacheLoader) -> anyhow::Result<Option<Bytes>> {
        if let Some(x) = self.index.get(&key) {
            let mut x = x.lock().await;
            if x.generation != self.generation {
                let load_output = load(Some(x.version)).await?;
                x.generation = self.generation;
                match load_output {
                    LoadOutput::Fresh => {}
                    LoadOutput::Replace { version, data } => {
                        x.version = version;
                        x.hash = self.add_to_content_store(data);
                    }
                }
            }
            let data = if let Some(hash) = x.hash {
                let txn = self.env.begin_ro_txn().unwrap();
                Some(Bytes::from(
                    txn.get(self.db, hash.as_bytes()).unwrap().to_vec(),
                ))
            } else {
                None
            };
            return Ok(data);
        }

        let entry = self
            .index
            .try_get_with(key, async {
                match load(None).await {
                    Ok(LoadOutput::Fresh) => {
                        panic!("CacheLoader must not return Fresh for None inputs")
                    }
                    Ok(LoadOutput::Replace { version, data }) => {
                        let hash = self.add_to_content_store(data);
                        Ok(Arc::new(Mutex::new(IndexEntry {
                            generation: self.generation,
                            version,
                            hash,
                        })))
                    }
                    Err(e) => Err(e),
                }
            })
            .await;

        let entry = match entry {
            Ok(x) => {
                let x = x.lock().await;
                assert!(x.generation == self.generation);

                let data = if let Some(hash) = x.hash {
                    let txn = self.env.begin_ro_txn().unwrap();
                    Some(Bytes::from(
                        txn.get(self.db, hash.as_bytes()).unwrap().to_vec(),
                    ))
                } else {
                    None
                };
                Ok(data)
            }
            Err(e) => Err(e),
        };

        Ok(flatten_get_output(entry)?)
    }

    fn mark_all_as_stale(&mut self) {
        self.generation += 1;
    }

    async fn invalidate(&mut self, keys: &[u32]) {
        for k in keys {
            self.index.invalidate(k).await;
        }
    }
}

fn flatten_get_output<T>(res: Result<T, Arc<anyhow::Error>>) -> Result<T> {
    match res {
        Ok(val) => Ok(val),
        Err(e) => Err(anyhow::anyhow!("get error: {}", e)),
    }
}
