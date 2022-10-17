use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};

use anyhow::{Context, Result};
use bloom::{BloomFilter, ASMS};
use foundationdb::{future::FdbKeyValue, options::StreamingMode, RangeOption};
use futures::TryStreamExt;

use crate::{
    fixed::FixedKeyVec,
    lock::DistributedLock,
    server::Server,
    util::{
        add_single_key_read_conflict_range, decode_version, extract_10_byte_suffix,
        get_txn_read_version_as_versionstamp, truncate_10_byte_suffix, ContentIndex,
    },
};

pub static GC_SCAN_BATCH_SIZE: AtomicUsize = AtomicUsize::new(5000);
pub static GC_FRESH_PAGE_TTL_SECS: AtomicU64 = AtomicU64::new(3600);

impl Server {
    pub async fn truncate_versions(
        self: Arc<Self>,
        dry_run: bool,
        ns_id: [u8; 10],
        mut before_version: [u8; 10],
        mut progress_callback: impl FnMut(Option<u64>),
    ) -> Result<()> {
        // Fix up `before_version` to be the minimum of the three:
        // - The supplied value
        // - The cluster's current read version as seen by this snapshot
        // - The NS lock version in the same snapshot
        {
            let txn = self.db.create_trx()?;

            before_version = before_version.min(get_txn_read_version_as_versionstamp(&txn).await?);

            let metadata = self
                .ns_metadata_cache
                .get(&txn, &self.key_codec, ns_id)
                .await?;
            if let Some(lock) = &metadata.lock {
                before_version = before_version.min(decode_version(&lock.snapshot_version)?);
            }
        }

        tracing::info!(
            ns = hex::encode(&ns_id),
            before_version = hex::encode(&before_version),
            "starting version truncation"
        );
        let scan_start = self.key_codec.construct_page_key(ns_id, 0, [0u8; 10]);
        let scan_end = self
            .key_codec
            .construct_page_key(ns_id, std::u32::MAX, [0xffu8; 10]);
        let mut scan_cursor = scan_start.clone();
        let mut lock = DistributedLock::new(
            self.key_codec
                .construct_nstask_key(ns_id, "truncate_versions"),
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
                let range: Vec<_> = match txn
                    .get_ranges_keyvalues(
                        RangeOption {
                            limit: Some(GC_SCAN_BATCH_SIZE.load(Ordering::Relaxed)),
                            reverse: false,
                            mode: StreamingMode::WantAll,
                            ..RangeOption::from(scan_cursor.as_slice()..=scan_end.as_slice())
                        },
                        true,
                    )
                    .try_collect()
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
                scan_cursor = FixedKeyVec::from_slice(scan_result.last().unwrap().key()).unwrap();

                for (kv, next) in scan_result[..scan_result.len() - 1]
                    .iter()
                    .zip(scan_result[1..].iter())
                {
                    let this_version = extract_10_byte_suffix(kv.key());
                    let next_version = extract_10_byte_suffix(next.key());

                    // Never truncate the latest version of a page in the specified version range
                    let is_latest_version_in_range = truncate_10_byte_suffix(kv.key())
                        != truncate_10_byte_suffix(next.key())
                        || next_version > before_version;
                    if !is_latest_version_in_range && this_version < before_version {
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

        // Delete changelog
        loop {
            let txn = lock.create_txn_and_check_sync(&self.db).await?;
            let start = self.key_codec.construct_changelog_key(ns_id, [0u8; 10]);
            let end = self
                .key_codec
                .construct_changelog_key(ns_id, before_version);
            // End is exclusive - `before_version` itself should not be deleted
            txn.clear_range(start.as_slice(), end.as_slice());
            match txn.commit().await {
                Ok(_) => break,
                Err(e) => {
                    e.on_error().await?;
                }
            }
        }

        progress_callback(None);
        Ok(())
    }

    async fn scan_range_simple(
        self: &Arc<Self>,
        lock: &DistributedLock,
        scan_start: FixedKeyVec,
        scan_end: FixedKeyVec,
        mut cb: impl FnMut(&FdbKeyValue),
    ) -> Result<()> {
        let mut scan_cursor = scan_start.clone();

        loop {
            let scan_result = loop {
                let txn = lock.create_txn_and_check_sync(&self.db).await?;
                let range: Vec<_> = match txn
                    .get_ranges_keyvalues(
                        RangeOption {
                            limit: Some(GC_SCAN_BATCH_SIZE.load(Ordering::Relaxed)),
                            reverse: false,
                            mode: StreamingMode::WantAll,
                            ..RangeOption::from(scan_cursor.as_slice()..=scan_end.as_slice())
                        },
                        true,
                    )
                    .try_collect()
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

            if scan_result.len() == 0 {
                break;
            }

            let scan_result = &scan_result[..];
            scan_cursor = FixedKeyVec::from_slice(scan_result.last().unwrap().key()).unwrap();
            scan_cursor.push(0x00).unwrap();

            for kv in scan_result {
                cb(kv);
            }
        }
        Ok(())
    }

    async fn scan_page_index_simple(
        self: &Arc<Self>,
        lock: &DistributedLock,
        ns_id: [u8; 10],
        mut cb: impl FnMut([u8; 32]),
    ) -> Result<()> {
        let scan_start = self.key_codec.construct_page_key(ns_id, 0, [0u8; 10]);
        let scan_end = self
            .key_codec
            .construct_page_key(ns_id, std::u32::MAX, [0xffu8; 10]);
        self.scan_range_simple(lock, scan_start, scan_end, |kv| {
            if let Ok(x) = <[u8; 32]>::try_from(kv.value()) {
                cb(x);
            }
        })
        .await
    }

    async fn scan_delta_referrer_simple(
        self: &Arc<Self>,
        lock: &DistributedLock,
        ns_id: [u8; 10],
        mut cb: impl FnMut([u8; 32]),
    ) -> Result<()> {
        let scan_start = self
            .key_codec
            .construct_delta_referrer_key(ns_id, [0u8; 32]);
        let scan_end = self
            .key_codec
            .construct_delta_referrer_key(ns_id, [0xffu8; 32]);
        self.scan_range_simple(lock, scan_start, scan_end, |kv| {
            if let Ok(x) = <[u8; 32]>::try_from(kv.value()) {
                cb(x);
            }
        })
        .await
    }

    pub async fn delete_unreferenced_content(
        self: Arc<Self>,
        dry_run: bool,
        ns_id: [u8; 10],
        mut progress_callback: impl FnMut(String),
    ) -> Result<()> {
        let ns_id_hex = hex::encode(&ns_id);
        let commit_token_key = self.key_codec.construct_ns_commit_token_key(ns_id);
        let mut lock = DistributedLock::new(
            self.key_codec
                .construct_nstask_key(ns_id, "delete_unreferenced_content"),
            "delete_unreferenced_content".into(),
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

        // Step 1: Fetch read version RAW-RV.
        let read_version = self.db.create_trx()?.get_read_version().await?;

        // Step 2: Collect inconsistent snapshot of hashes.
        // 2a. Scan the page index incrementally and collect all hashes.
        // 2b. Scan the delta referrer index incrementally and collect all hashes.

        // First, estimate the set size.
        let mut page_ref_set_size = 0usize;
        self.scan_page_index_simple(&lock, ns_id, |_| page_ref_set_size += 1)
            .await?;
        self.scan_delta_referrer_simple(&lock, ns_id, |_| page_ref_set_size += 1)
            .await?;

        // Nothing to do
        if page_ref_set_size == 0 {
            progress_callback(format!("DONE\n"));
            return Ok(());
        }

        let mut page_ref_set: BloomFilter =
            BloomFilter::with_rate(0.01, page_ref_set_size.min(std::u32::MAX as usize) as u32);
        tracing::info!(ns = %ns_id_hex, size = page_ref_set_size, num_bits = page_ref_set.num_bits(), num_hashes = page_ref_set.num_hashes(), "created bloom filter");

        // Then, collect the hashes
        self.scan_page_index_simple(&lock, ns_id, |hash| {
            page_ref_set.insert(&hash);
        })
        .await?;
        self.scan_delta_referrer_simple(&lock, ns_id, |hash| {
            page_ref_set.insert(&hash);
        })
        .await?;

        // Step 3: scan the content index
        {
            let scan_start = self.key_codec.construct_contentindex_key(ns_id, [0u8; 32]);
            let scan_end = self
                .key_codec
                .construct_contentindex_key(ns_id, [0xffu8; 32]);
            let mut scan_cursor = scan_start.clone();
            let prefix_len = scan_start.len() - 32;
            let mut count = 0usize;
            loop {
                // Step 3, TXN1
                let mut txn = lock.create_txn_and_check_sync(&self.db).await?;
                let txn1_rv = match txn.get_read_version().await {
                    Ok(x) => x,
                    Err(e) => {
                        txn.on_error(e).await?;
                        continue;
                    }
                };
                let scan_result: Vec<_> = match txn
                    .get_ranges_keyvalues(
                        RangeOption {
                            limit: Some(GC_SCAN_BATCH_SIZE.load(Ordering::Relaxed)),
                            reverse: false,
                            mode: StreamingMode::WantAll,
                            ..RangeOption::from(scan_cursor.as_slice()..=scan_end.as_slice())
                        },
                        true,
                    )
                    .try_collect()
                    .await
                {
                    Ok(x) => x,
                    Err(e) => {
                        txn.on_error(e).await?;
                        continue;
                    }
                };

                if scan_result.len() == 0 {
                    break;
                }

                let mut next_scan_cursor =
                    FixedKeyVec::from_slice(scan_result.last().unwrap().key()).unwrap();
                next_scan_cursor.push(0x00).unwrap();

                let mut delete_queue: Vec<[u8; 32]> = vec![];
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();

                for kv in &scan_result {
                    let ci = match ContentIndex::decode(kv.value()) {
                        Ok(x) => x,
                        Err(_) => continue,
                    };

                    let hash = <[u8; 32]>::try_from(&kv.key()[prefix_len..]).unwrap();

                    // 3a. Filter out those hashes seen in step 2.
                    if page_ref_set.contains(&hash) {
                        continue;
                    }

                    // 3b. Filter out those hashes added within a time duration.
                    // This is not necessary for correctness, but removing this may cause transactions to fail.
                    let their_secs = ci.time.as_secs();
                    let our_secs = now.as_secs();
                    if their_secs > our_secs
                        || our_secs - their_secs < GC_FRESH_PAGE_TTL_SECS.load(Ordering::Relaxed)
                    {
                        continue;
                    }

                    // 3c. Filter out those CAM entries modified after RAW-RV.
                    let their_version =
                        i64::from_be_bytes(ci.versionstamp[0..8].try_into().unwrap());
                    if their_version > read_version {
                        continue;
                    }
                    delete_queue.push(hash);
                }
                drop(scan_result);

                // Fast path: do nothing if there's nothing to do
                if delete_queue.len() == 0 {
                    scan_cursor = next_scan_cursor;
                    continue;
                }

                // 3d. Set TXN2.RV to TXN1.RV (after creating TXN2)
                txn.reset();
                txn.set_read_version(txn1_rv);

                for hash in &delete_queue {
                    let ci_key = self.key_codec.construct_contentindex_key(ns_id, *hash);
                    let content_key = self.key_codec.construct_content_key(ns_id, *hash);
                    let delta_referrer_key =
                        self.key_codec.construct_delta_referrer_key(ns_id, *hash);

                    // 3e. Add the CAM index of the remaining pages to the conflict set.
                    add_single_key_read_conflict_range(&txn, &ci_key)?;

                    // 3f. Delete the remaining pages from the CAM.
                    txn.clear(&ci_key);
                    txn.clear(&content_key);
                    txn.clear(&delta_referrer_key);
                }

                // 3g. Delete COMMIT-TOKEN.
                txn.clear(&commit_token_key);

                if !dry_run {
                    match txn.commit().await {
                        Ok(_) => {}
                        Err(e) => {
                            // If this is a busy range we may keep getting
                            // conflict on contentindex. It isn't worthy to retry in that case.
                            //
                            // Just defer to the next GC run.
                            tracing::warn!(error = %e, "failed to commit GC clear operation, skipping");
                        }
                    }
                }

                count += delete_queue.len();
                progress_callback(format!("{}\n", count));

                scan_cursor = next_scan_cursor;
            }
        }

        progress_callback(format!("DONE\n"));
        Ok(())
    }
}
