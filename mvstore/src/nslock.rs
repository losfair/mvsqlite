use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::{
    fixed::FixedKeyVec,
    keys::KeyCodec,
    metadata::{NamespaceLock, NamespaceMetadataCache},
    util::{
        decode_version, extract_10_byte_suffix, extract_beu32_suffix, get_last_write_version,
        truncate_10_byte_suffix,
    },
};
use anyhow::Result;
use foundationdb::{
    options::{MutationType, StreamingMode},
    Database, RangeOption, Transaction,
};
use futures::TryStreamExt;
use hyper::{Body, Response};
use rand::Rng;
use serde::Deserialize;

const MAX_OWNER_STR_BYTE_SIZE: usize = 256;
pub static NSLOCK_ROLLBACK_SCAN_BATCH_SIZE: AtomicUsize = AtomicUsize::new(1000);

#[derive(Deserialize, Copy, Clone)]
pub enum NslockReleaseMode {
    #[serde(rename = "commit")]
    Commit,
    #[serde(rename = "rollback")]
    Rollback,
}
pub async fn acquire_nslock(
    db: &Database,
    key_codec: &KeyCodec,
    ns_metadata_cache: &NamespaceMetadataCache,
    ns_id: [u8; 10],
    owner: &str,
    version: Option<&str>,
) -> Result<Response<Body>> {
    if owner.is_empty() || owner.as_bytes().len() > MAX_OWNER_STR_BYTE_SIZE {
        return Ok(Response::builder()
            .status(400)
            .body(Body::from("invalid owner\n"))?);
    }

    let mut txn = db.create_trx()?;
    loop {
        let metadata = ns_metadata_cache.get(&txn, key_codec, ns_id).await?;
        let mut metadata = (*metadata).clone();

        // Is this lock already held?
        if let Some(lock) = &metadata.lock {
            if lock.owner == owner {
                return Ok(Response::builder().status(201).body(Body::empty())?);
            } else {
                return Ok(Response::builder()
                    .status(409)
                    .header("x-lock-owner", &lock.owner)
                    .body(Body::from("namespace is locked by another owner\n"))?);
            }
        }

        let snapshot_version = if let Some(version) = version {
            decode_version(version)?
        } else {
            get_last_write_version(&txn, key_codec, ns_id, false).await?
        };

        let mut nonce: [u8; 16] = [0u8; 16];
        rand::thread_rng().fill(&mut nonce);

        metadata.lock = Some(NamespaceLock {
            snapshot_version: hex::encode(&snapshot_version),
            owner: owner.to_string(),
            nonce: hex::encode(&nonce),
            rolling_back: false,
        });
        ns_metadata_cache.set(&txn, key_codec, ns_id, Arc::new(metadata))?;
        match txn.commit().await {
            Ok(_) => {
                return Ok(Response::builder().status(201).body(Body::empty())?);
            }
            Err(e) => {
                txn = e.on_error().await?;
            }
        }
    }
}

pub async fn release_nslock(
    db: &Database,
    key_codec: &KeyCodec,
    ns_metadata_cache: &NamespaceMetadataCache,
    ns_id: [u8; 10],
    owner: &str,
    mode: NslockReleaseMode,
) -> Result<Response<Body>> {
    if owner.is_empty() {
        return Ok(Response::builder()
            .status(400)
            .body(Body::from("invalid owner\n"))?);
    }

    let rollback_cursor_key = key_codec.construct_nsrollbackcursor_key(ns_id);

    let mut txn = db.create_trx()?;
    let nonce: String;
    let snapshot_version: [u8; 10];

    loop {
        let metadata = ns_metadata_cache.get(&txn, key_codec, ns_id).await?;
        let mut metadata = (*metadata).clone();

        if metadata.lock.is_none() {
            return Ok(Response::builder()
                .status(422)
                .body(Body::from("namespace is not locked"))?);
        }

        // Can we unlock?
        let lock = metadata.lock.as_mut().unwrap();
        if lock.owner != owner {
            return Ok(Response::builder()
                .status(422)
                .header("x-lock-owner", &lock.owner)
                .body(Body::from("namespace is locked by another owner"))?);
        }

        match mode {
            NslockReleaseMode::Commit => {
                // Commit mode: delete the lock and we're done.
                metadata.lock = None;
                ns_metadata_cache.set(&txn, key_codec, ns_id, Arc::new(metadata))?;

                match txn.commit().await {
                    Ok(_) => {
                        return Ok(Response::builder().status(201).body(Body::empty())?);
                    }
                    Err(e) => {
                        txn = e.on_error().await?;
                        continue;
                    }
                }
            }

            NslockReleaseMode::Rollback => {
                // Rollback mode: first mark this lock as being rolled back.
                if !lock.rolling_back {
                    lock.rolling_back = true;
                    let metadata = Arc::new(metadata);
                    ns_metadata_cache.set(&txn, key_codec, ns_id, metadata.clone())?;
                    txn.clear(&rollback_cursor_key);

                    match txn.commit().await {
                        Ok(output) => {
                            txn = output.reset();
                            nonce = metadata.lock.as_ref().unwrap().nonce.clone();
                            snapshot_version = <[u8; 10]>::try_from(
                                &hex::decode(&metadata.lock.as_ref().unwrap().snapshot_version)?[..],
                            )?;
                            break;
                        }
                        Err(e) => {
                            txn = e.on_error().await?;
                            continue;
                        }
                    }
                }
            }
        }
    }

    // We get here when rollback is required
    // Scan and delete
    let mut total_count = 0u64;

    // Snapshot read is correct here - running through a range twice is fine
    let mut rollback_cursor = u32::from_le_bytes(
        txn.get(&rollback_cursor_key, true)
            .await?
            .and_then(|x| <[u8; 4]>::try_from(&x[..]).ok())
            .unwrap_or_default(),
    );
    let scan_end = key_codec.construct_page_key(ns_id, std::u32::MAX, [0xffu8; 10]);
    let mut scan_cursor = key_codec.construct_page_key(ns_id, rollback_cursor, [0u8; 10]);

    loop {
        let valid = lock_is_still_valid(&txn, key_codec, ns_metadata_cache, ns_id, &nonce).await?;
        if !valid {
            return Ok(Response::builder()
                .status(410)
                .body(Body::from("lock is gone"))?);
        }

        // Snapshot read is correct here because conflict range added by lock_is_still_valid is enough
        let scan_result: Vec<_> = txn
            .get_ranges_keyvalues(
                RangeOption {
                    limit: Some(NSLOCK_ROLLBACK_SCAN_BATCH_SIZE.load(Ordering::Relaxed)),
                    reverse: false,
                    mode: StreamingMode::WantAll,
                    ..RangeOption::from(scan_cursor.as_slice()..=scan_end.as_slice())
                },
                true,
            )
            .try_collect()
            .await?;

        let mut delete_count: usize = 0;

        for entry in &scan_result[..] {
            let version = extract_10_byte_suffix(entry.key());
            if version > snapshot_version {
                txn.clear(entry.key());
                delete_count += 1;
            }
        }

        if scan_result.is_empty() {
            // Done
            txn.reset();
            break;
        }

        let last_key = scan_result.last().unwrap().key().to_vec();
        let new_rollback_cursor = extract_beu32_suffix(truncate_10_byte_suffix(&last_key));
        if new_rollback_cursor > rollback_cursor {
            txn.atomic_op(
                &rollback_cursor_key,
                &new_rollback_cursor.to_le_bytes(),
                MutationType::Max,
            );
        }

        match txn.commit().await {
            Ok(output) => {
                txn = output.reset();
                total_count += delete_count as u64;
                rollback_cursor = new_rollback_cursor;
                scan_cursor = FixedKeyVec::from_slice(&last_key).unwrap();
                scan_cursor.push(0).unwrap();
            }
            Err(e) => {
                txn = e.on_error().await?;
            }
        }
    }

    loop {
        let valid = lock_is_still_valid(&txn, key_codec, ns_metadata_cache, ns_id, &nonce).await?;
        if !valid {
            return Ok(Response::builder()
                .status(410)
                .body(Body::from("lock is gone"))?);
        }

        // Patch LWV
        {
            let last_write_version_key = key_codec.construct_last_write_version_key(ns_id);

            let mut new_lwv_value = [0u8; 16 + 10];
            new_lwv_value[16..26].copy_from_slice(&snapshot_version);
            txn.set(&last_write_version_key, &new_lwv_value);
        }

        // Delete changelog
        {
            let mut start = key_codec.construct_changelog_key(ns_id, snapshot_version);
            let end = key_codec.construct_changelog_key(ns_id, [0xffu8; 10]);
            start.push(0).unwrap();
            txn.clear_range(start.as_slice(), end.as_slice());
        }

        // Unlock
        let metadata = ns_metadata_cache.get(&txn, key_codec, ns_id).await?;
        let mut metadata = (*metadata).clone();
        metadata.lock = None;
        ns_metadata_cache.set(&txn, key_codec, ns_id, Arc::new(metadata))?;

        match txn.commit().await {
            Ok(_) => break,
            Err(e) => {
                txn = e.on_error().await?;
            }
        }
    }

    Ok(Response::builder().status(200).body(Body::empty())?)
}

async fn lock_is_still_valid(
    txn: &Transaction,
    key_codec: &KeyCodec,
    ns_metadata_cache: &NamespaceMetadataCache,
    ns_id: [u8; 10],
    nonce: &str,
) -> Result<bool> {
    let metadata = ns_metadata_cache.get(&txn, key_codec, ns_id).await?;
    if let Some(lock) = &metadata.lock {
        if lock.nonce == nonce {
            return Ok(true);
        }
    }
    Ok(false)
}
