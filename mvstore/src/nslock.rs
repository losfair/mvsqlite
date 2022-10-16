use std::sync::Arc;

use crate::{
    keys::KeyCodec,
    metadata::{NamespaceLock, NamespaceMetadataCache},
    util::get_last_write_version,
};
use anyhow::Result;
use foundationdb::Database;
use hyper::{Body, Response};
use serde::Deserialize;

const MAX_OWNER_STR_BYTE_SIZE: usize = 256;

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

        let lwv = get_last_write_version(&txn, key_codec, ns_id, false).await?;
        metadata.lock = Some(NamespaceLock {
            snapshot_version: hex::encode(&lwv),
            owner: owner.to_string(),
            rolling_back: false,
        });
        ns_metadata_cache.set(&txn, key_codec, ns_id, Arc::new(metadata))?;
        match txn.commit().await {
            Ok(_) => {
                return Ok(Response::builder().status(201).body(Body::empty())?);
            }
            Err(e) => {
                txn = match e.on_error().await {
                    Ok(x) => x,
                    Err(e) => {
                        return Ok(Response::builder()
                            .status(400)
                            .body(Body::from(format!("{}", e)))?)
                    }
                };
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

    let mut txn = db.create_trx()?;
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
                        txn = match e.on_error().await {
                            Ok(x) => x,
                            Err(e) => {
                                return Ok(Response::builder()
                                    .status(400)
                                    .body(Body::from(format!("{}", e)))?)
                            }
                        };
                        continue;
                    }
                }
            }

            NslockReleaseMode::Rollback => {
                // Rollback mode: first mark this lock as being rolled back.
                if !lock.rolling_back {
                    lock.rolling_back = true;
                    ns_metadata_cache.set(&txn, key_codec, ns_id, Arc::new(metadata))?;

                    match txn.commit().await {
                        Ok(output) => {
                            txn = output.reset();
                            break;
                        }
                        Err(e) => {
                            txn = match e.on_error().await {
                                Ok(x) => x,
                                Err(e) => {
                                    return Ok(Response::builder()
                                        .status(400)
                                        .body(Body::from(format!("{}", e)))?)
                                }
                            };
                            continue;
                        }
                    }
                }
            }
        }
    }

    // We get here when rollback is required

    Ok(Response::builder().status(200).body(Body::empty())?)
}
