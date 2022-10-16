use std::{sync::Arc, time::Duration};

use anyhow::Result;
use foundationdb::Transaction;
use moka::future::Cache;
use serde::{Deserialize, Serialize};

use crate::keys::KeyCodec;

#[derive(Serialize, Deserialize, Default)]
pub struct NamespaceMetadata {
    pub lock: Option<NamespaceLock>,
}

#[derive(Serialize, Deserialize)]
pub struct NamespaceLock {
    pub snapshot_version: String,
    pub owner: String,
}

pub struct NamespaceMetadataCache {
    cache: Cache<(i64, [u8; 10]), Arc<NamespaceMetadata>>,
}

impl NamespaceMetadataCache {
    pub fn new() -> Self {
        Self {
            cache: Cache::builder()
                .time_to_idle(Duration::from_secs(30))
                .max_capacity(1000)
                .build(),
        }
    }

    pub async fn get(
        &self,
        txn: &Transaction,
        key_codec: &KeyCodec,
        ns_id: [u8; 10],
    ) -> Result<Arc<NamespaceMetadata>> {
        let metadata_version = txn.get_metadata_version(false).await?.unwrap_or(0);
        let cached: Arc<NamespaceMetadata> = self
            .cache
            .try_get_with((metadata_version, ns_id), async {
                let key = key_codec.construct_nsmd_key(ns_id);

                // Snapshot read is correct here because `metadata_version` read already guards against concurrent mutations
                let metadata = txn.get(&key, true).await;

                match metadata {
                    Ok(x) => Ok(Arc::new(
                        serde_json::from_slice(x.as_ref().map(|x| &x[..]).unwrap_or_default())
                            .unwrap_or_default(),
                    )),
                    Err(e) => Err(e),
                }
            })
            .await?;
        Ok(cached)
    }

    pub fn set(
        &self,
        txn: &Transaction,
        key_codec: &KeyCodec,
        ns_id: [u8; 10],
        metadata: Arc<NamespaceMetadata>,
    ) -> Result<()> {
        txn.set(
            &key_codec.construct_nsmd_key(ns_id),
            &serde_json::to_vec(&*metadata)?,
        );
        txn.update_metadata_version();
        Ok(())
    }
}
