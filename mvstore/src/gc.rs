use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use foundationdb::{options::StreamingMode, RangeOption};

use crate::{lock::DistributedLock, server::Server};

impl Server {
    pub async fn truncate_versions(
        self: Arc<Self>,
        dry_run: bool,
        ns_id: [u8; 10],
        before_version: [u8; 10],
        mut progress_callback: impl FnMut(Option<u64>),
    ) -> Result<()> {
        let scan_start = self.construct_page_key(ns_id, 0, [0u8; 10]);
        let scan_end = self.construct_page_key(ns_id, std::u32::MAX, [0xffu8; 10]);
        let mut scan_cursor = scan_start.clone();
        let mut lock = DistributedLock::new(
            self.construct_nstask_key(ns_id, "truncate_versions"),
            "truncate_versions".into(),
        );

        let me = self.clone();
        let locked = lock
            .lock(
                move || {
                    me.db
                        .create_trx()
                        .with_context(|| "transaction creation failed")
                },
                Duration::from_secs(5),
            )
            .await?;
        if !locked {
            anyhow::bail!("failed to acquire lock");
        }

        let mut total_count = 0u64;

        loop {
            let scan_result = loop {
                let txn = lock.create_txn_and_check_sync(&self.db).await?;
                let range = match txn
                    .get_range(
                        &RangeOption {
                            limit: Some(10000),
                            reverse: false,
                            mode: StreamingMode::WantAll,
                            ..RangeOption::from(scan_cursor.clone()..=scan_end.clone())
                        },
                        0,
                        true,
                    )
                    .await
                {
                    Ok(x) => x,
                    Err(e) => {
                        txn.on_error(e).await?;
                        continue;
                    }
                };
                break range;
            };

            // In the one-page case, we are sure we don't want to gc that
            if scan_result.len() <= 1 {
                break;
            }

            let mut deletion_set: Vec<Vec<u8>> = vec![];

            {
                let scan_result = &scan_result[..];

                // Yes, we do want to rescan the last item
                scan_cursor = scan_result.last().unwrap().key().to_vec();

                for (kv, next) in scan_result[..scan_result.len() - 1]
                    .iter()
                    .zip(scan_result[1..].iter())
                {
                    // Never truncate the current version of a page
                    if truncate_10_byte_suffix(kv.key()) != truncate_10_byte_suffix(next.key()) {
                        continue;
                    }

                    let this_version = extract_10_byte_suffix(kv.key());
                    if this_version < before_version {
                        deletion_set.push(kv.key().to_vec());
                    }
                }
            }
            drop(scan_result);

            if !deletion_set.is_empty() {
                if !dry_run {
                    loop {
                        let txn = lock.create_txn_and_check_sync(&self.db).await?;
                        for item in &deletion_set {
                            txn.clear(item);
                        }
                        match txn.commit().await {
                            Ok(_) => break,
                            Err(e) => {
                                e.on_error().await?;
                            }
                        }
                    }
                }
                tracing::info!(
                    ns = hex::encode(&ns_id),
                    count = deletion_set.len(),
                    dry = dry_run,
                    "truncated pages"
                );
                total_count += deletion_set.len() as u64;
                progress_callback(Some(total_count));
            }
        }

        progress_callback(None);
        Ok(())
    }
}

fn truncate_10_byte_suffix(data: &[u8]) -> &[u8] {
    assert!(data.len() >= 10);
    &data[..data.len() - 10]
}

fn extract_10_byte_suffix(data: &[u8]) -> [u8; 10] {
    assert!(data.len() >= 10);
    <[u8; 10]>::try_from(&data[data.len() - 10..]).unwrap()
}
