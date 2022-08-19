use std::{
    collections::BTreeSet,
    sync::atomic::{AtomicUsize, Ordering},
};

use crate::server::{decode_version, Server};
use anyhow::Result;
use foundationdb::{
    options::{StreamingMode, TransactionOption},
    RangeOption, Transaction,
};
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
    pub async fn stat(&self, ns_id: [u8; 10], from_version: &str) -> Result<StatResponse> {
        let txn = self.db.create_trx()?;
        if self.read_only {
            txn.set_option(TransactionOption::ReadLockAware).unwrap();
        }

        let mut version = [0u8; 10];
        let last_write_version_key = self.construct_last_write_version_key(ns_id);
        if let Some(t) = txn.get(&last_write_version_key, false).await? {
            if t.len() == 16 + 10 {
                version = <[u8; 10]>::try_from(&t[16..26]).unwrap();
            }
        }

        let nsmd = txn.get(&self.construct_nsmd_key(ns_id), false).await?;
        let nsmd = nsmd
            .as_ref()
            .map(|x| std::str::from_utf8(&x[..]).unwrap_or_default().to_string())
            .unwrap_or_default();

        let mut interval: Option<Vec<u32>> = None;
        if !from_version.is_empty() {
            let from_version = decode_version(from_version)?;
            interval = self
                .read_interval(&txn, ns_id, from_version, version, true)
                .await?;
        }

        let stat = StatResponse {
            version: hex::encode(&version),
            metadata: nsmd,
            read_only: self.read_only,
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

        let start = self.construct_changelog_key(ns_id, from_version);
        let end = self.construct_changelog_key(ns_id, to_version);
        let range = txn
            .get_range(
                &RangeOption {
                    limit: Some(INTERVAL_SCAN_SIZE.load(Ordering::Relaxed)),
                    reverse: false,
                    mode: StreamingMode::WantAll,
                    ..RangeOption::from(start.clone()..=end.clone())
                },
                0,
                true,
            )
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
