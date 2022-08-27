use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures::lock::Mutex;
use moka::future::Cache;

use crate::{CacheLoader, LoadOutput, VersionedPageCache};

pub struct MokaBackend {
    inner: Cache<u32, Arc<Mutex<CacheEntry>>>,
    generation: u64,
}

impl MokaBackend {
    pub fn new(size: usize) -> Self {
        MokaBackend {
            inner: Cache::new(size as u64),
            generation: 0,
        }
    }
}

struct CacheEntry {
    generation: u64,
    version: [u8; 10],
    data: Option<Bytes>,
}

fn flatten_get_output<T>(res: Result<T, Arc<anyhow::Error>>) -> Result<T> {
    match res {
        Ok(val) => Ok(val),
        Err(e) => Err(anyhow::anyhow!("get error: {}", e)),
    }
}

#[async_trait]
impl VersionedPageCache for MokaBackend {
    async fn get(&self, key: u32, load: CacheLoader) -> anyhow::Result<Option<Bytes>> {
        if let Some(x) = self.inner.get(&key) {
            let mut x = x.lock().await;
            if x.generation != self.generation {
                let load_output = load(Some(x.version)).await?;
                x.generation = self.generation;
                match load_output {
                    LoadOutput::Fresh => {}
                    LoadOutput::Replace { version, data } => {
                        x.version = version;
                        x.data = data;
                    }
                }
            }
            return Ok(x.data.clone());
        }

        let entry = self
            .inner
            .try_get_with(key, async {
                match load(None).await {
                    Ok(LoadOutput::Fresh) => {
                        panic!("CacheLoader must not return Fresh for None inputs")
                    }
                    Ok(LoadOutput::Replace { version, data }) => {
                        Ok(Arc::new(Mutex::new(CacheEntry {
                            generation: self.generation,
                            version,
                            data,
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
                Ok(x.data.clone())
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
            self.inner.invalidate(k).await;
        }
    }
}
