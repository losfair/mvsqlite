use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::util::{decode_version, get_last_write_version};
use crate::{server::Server, util::GoneError};
use anyhow::Result;
use foundationdb::{
    options::{StreamingMode, TransactionOption},
    RangeOption, Transaction,
};
use futures::TryStreamExt;
use serde::Serialize;

static INTERVAL_SCAN_SIZE: AtomicUsize = AtomicUsize::new(100);
static INTERVAL_MAX_SIZE: AtomicUsize = AtomicUsize::new(1000);

#[derive(Serialize)]
pub struct StatResponse {
    pub version: String,
    pub metadata: String,
    pub read_only: bool,
    pub interval: Option<Vec<u32>>,
}

impl Server {
    pub async fn stat(
        &self,
        ns_id: [u8; 10],
        from_version: &str,
        crr: bool,
        lock_owner: &str,
    ) -> Result<StatResponse> {
        let txn = self.db.create_trx()?;
        if self.is_read_only() {
            txn.set_option(TransactionOption::ReadLockAware).unwrap();
        }

        if crr {
            txn.set_option(TransactionOption::CausalReadRisky).unwrap();
        }

        let rv = txn.get_read_version().await?;
        let metadata = self
            .ns_metadata_cache
            .get(&txn, &self.key_codec, ns_id)
            .await?;
        let mut version: Option<[u8; 10]> = None;

        if let Some(lock) = &metadata.lock {
            if lock_owner.is_empty() {
                // Concurrent read-only transaction, return the latest snapshot
                version = Some(decode_version(&lock.snapshot_version)?);
            } else {
                // Validate lock ownership and state
                if lock.owner.as_str() != lock_owner {
                    return Err(GoneError("you no longer own the lock").into());
                }
                if lock.rolling_back {
                    return Err(GoneError("rolling back").into());
                }
            }
        } else {
            if !lock_owner.is_empty() {
                return Err(GoneError("you do not own the lock").into());
            }
        }

        // If not locked or the client is the lock owner, return LWV
        let version = match version {
            Some(x) => x,
            None => {
                self.read_version_and_nsid_to_lwv_cache
                    .try_get_with((rv, ns_id), async {
                        get_last_write_version(&txn, &self.key_codec, ns_id, true).await
                    })
                    .await?
            }
        };

        let mut interval: Option<Vec<u32>> = None;
        if !from_version.is_empty() {
            let from_version = decode_version(from_version)?;
            interval = self
                .read_interval(&txn, ns_id, from_version, version, true)
                .await?;
        }

        let stat = StatResponse {
            version: hex::encode(&version),
            metadata: "".into(),
            read_only: self.is_read_only(),
            interval,
        };

        Ok(stat)
    }

    pub async fn read_interval(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        from_version: [u8; 10],
        to_version: [u8; 10],
        to_version_is_inclusive: bool,
    ) -> Result<Option<Vec<u32>>> {
        if from_version > to_version {
            return Ok(None);
        }

        if from_version == to_version {
            return Ok(Some(vec![]));
        }

        let start = self.key_codec.construct_changelog_key(ns_id, from_version);
        let end = self.key_codec.construct_changelog_key(ns_id, to_version);
        let range: Vec<_> = txn
            .get_ranges_keyvalues(
                RangeOption {
                    limit: Some(INTERVAL_SCAN_SIZE.load(Ordering::Relaxed)),
                    reverse: false,
                    mode: StreamingMode::WantAll,
                    ..RangeOption::from(start.as_slice()..=end.as_slice())
                },
                true,
            )
            .try_collect()
            .await?;
        let range = &range[..];
        if range.len() < 2 {
            return Ok(None);
        }

        if range[0].key() != &start[..] || range[range.len() - 1].key() != &end[..] {
            return Ok(None);
        }
        let range = if to_version_is_inclusive {
            &range[1..]
        } else {
            &range[1..range.len() - 1]
        };

        let mut out: BTreeSet<u32> = BTreeSet::new();
        for item in range {
            let key = item.key();
            let value = item.value();

            let version = <[u8; 10]>::try_from(&key[key.len() - 10..]).unwrap();

            // Header
            if value.len() < 1 {
                tracing::error!(
                    ns = hex::encode(&ns_id),
                    version = hex::encode(&version),
                    "empty changelog entry"
                );
                return Ok(None);
            }
            let ty = value[0];
            let value = &value[1..];

            match ty {
                0 => {
                    let mut value = value;
                    if value.len() % 4 != 0 {
                        tracing::error!(
                            ns = hex::encode(&ns_id),
                            version = hex::encode(&version),
                            ty,
                            "invalid changelog"
                        );
                        return Ok(None);
                    }
                    let max_size = INTERVAL_MAX_SIZE.load(Ordering::Relaxed);
                    while !value.is_empty() {
                        let id = u32::from_be_bytes(value[..4].try_into().unwrap());
                        out.insert(id);
                        if out.len() > max_size {
                            return Ok(None);
                        }
                        value = &value[4..];
                    }
                }
                1 => {
                    // Infinite
                    return Ok(None);
                }
                _ => {
                    tracing::error!(
                        ns = hex::encode(&ns_id),
                        version = hex::encode(&version),
                        ty,
                        "unknown changelog type"
                    );
                    return Ok(None);
                }
            }
        }

        Ok(Some(out.iter().copied().collect()))
    }
}
