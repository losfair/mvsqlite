use std::{sync::Arc, time::Duration};

use anyhow::Result;
use foundationdb::{FdbError, Transaction};
use moka::future::Cache;
use serde::{Deserialize, Serialize};

use crate::{keys::KeyCodec, util::add_single_key_read_conflict_range};

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct NamespaceMetadata {
    #[serde(default)]
    pub lock: Option<NamespaceLock>,

    #[serde(default)]
    pub overlay_base: Option<NamespaceOverlayBase>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NamespaceLock {
    pub snapshot_version: String,
    pub owner: String,

    pub nonce: String,

    #[serde(default)]
    pub rolling_back: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct NamespaceOverlayBase {
    pub ns_id: String,
    pub snapshot_version: String,
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
    ) -> Result<Arc<NamespaceMetadata>, Arc<FdbError>> {
        let metadata_version = txn
            .get_metadata_version(true)
            .await
            .map_err(Arc::new)?
            .unwrap_or(0);
        let key = key_codec.construct_nsmd_key(ns_id);
        let cached: Arc<NamespaceMetadata> = self
            .cache
            .try_get_with((metadata_version, ns_id), async {
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
        add_single_key_read_conflict_range(&txn, &key)?;
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
