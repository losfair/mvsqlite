use std::{
    collections::HashSet,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use blake3::Hash;
use bytes::Bytes;
use foundationdb::{options::MutationType, Transaction};
use futures::{stream::FuturesUnordered, StreamExt};
use itertools::Itertools;
use moka::future::Cache;

use crate::{
    delta::writer::DeltaWriter,
    fixed::FixedKeyVec,
    keys::KeyCodec,
    page::{Page, MAX_PAGE_SIZE},
    util::ContentIndex,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct WriteRequest<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],

    pub delta_base: Option<u32>,
}

#[derive(Serialize)]
pub struct WriteResponse {
    #[serde(with = "serde_bytes")]
    pub hash: Vec<u8>,
}

pub struct WriteApplier<'a> {
    txn: &'a Transaction,
    ns_id: [u8; 10],
    key_codec: &'a KeyCodec,
    now: Duration,
    seen_hashes: HashSet<Hash>,
    content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

pub struct WriteApplierContext<'a> {
    pub txn: &'a Transaction,
    pub ns_id: [u8; 10],
    pub key_codec: &'a KeyCodec,
    pub now: Duration,
    pub content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

struct WriteContext<'a> {
    req: &'a WriteRequest<'a>,
    hash: Hash,
    content_key: FixedKeyVec,
    content_index_key: FixedKeyVec,
    early_completion: AtomicBool,
}

impl<'a> WriteApplier<'a> {
    pub fn new(ctx: WriteApplierContext<'a>) -> Self {
        Self {
            txn: ctx.txn,
            ns_id: ctx.ns_id,
            key_codec: ctx.key_codec,
            now: ctx.now,
            seen_hashes: HashSet::new(),
            content_cache: ctx.content_cache,
        }
    }

    pub async fn apply_write<'b>(
        &mut self,
        write_reqs: &[WriteRequest<'b>],
    ) -> Option<Vec<WriteResponse>> {
        for req in write_reqs {
            if req.data.len() > MAX_PAGE_SIZE {
                tracing::warn!(
                    ns = hex::encode(&self.ns_id),
                    len = req.data.len(),
                    limit = MAX_PAGE_SIZE,
                    "page is too large"
                );
                return None;
            }
        }

        let write_reqs = write_reqs
            .iter()
            .map(|req| (req, blake3::hash(req.data)))
            .collect::<Vec<_>>();
        let pregenerated_res = write_reqs
            .iter()
            .map(|req| WriteResponse {
                hash: req.1.as_bytes().to_vec(),
            })
            .collect::<Vec<_>>();

        // Here we filter by `seen_hashes`, because writing a same hash twice in the same transaction will fail
        // since we cannot read from a key previously written with `SetVersionstampedValue`.
        let write_reqs = write_reqs
            .iter()
            .dedup_by(|a, b| a.1 == b.1)
            .filter(|x| !self.seen_hashes.contains(&x.1))
            .map(|x| WriteContext {
                req: x.0,
                hash: x.1,
                content_key: self
                    .key_codec
                    .construct_content_key(self.ns_id, *x.1.as_bytes()),
                content_index_key: self
                    .key_codec
                    .construct_contentindex_key(self.ns_id, *x.1.as_bytes()),
                early_completion: AtomicBool::new(false),
            })
            .collect::<Vec<_>>();

        // This is not only an optimization. Without doing this check it is possible to form
        // loops in delta page construction.
        {
            let mut fut = FuturesUnordered::new();
            for req in &write_reqs {
                fut.push(async {
                    match self.txn.get(&req.content_index_key, false).await {
                        Ok(x) => {
                            if x.is_some() {
                                req.early_completion.store(true, Ordering::Relaxed);
                            }
                            Ok(())
                        }
                        Err(e) => {
                            tracing::warn!(ns = hex::encode(&self.ns_id), error = %e, "error getting content");
                            Err(())
                        }
                    }
                });
            }
            while let Some(res) = fut.next().await {
                if res.is_err() {
                    return None;
                }
            }
        }

        // Attempt delta-encoding
        {
            let mut fut = FuturesUnordered::new();
            for req in &write_reqs {
                if req.early_completion.load(Ordering::Relaxed) {
                    continue;
                }

                fut.push(async {
                    let writer = DeltaWriter {
                        txn: self.txn,
                        ns_id: self.ns_id,
                        key_codec: self.key_codec,
                        content_cache: self.content_cache,
                    };
                    if let Some(delta_base_index) = req.req.delta_base {
                        match writer.delta_encode(delta_base_index, &req.req.data).await {
                            Ok(x) => {
                                if let Some((x, delta_base_hash)) = x {
                                    let delta_referrer_key =
                                        self.key_codec.construct_delta_referrer_key(
                                            self.ns_id,
                                            *req.hash.as_bytes(),
                                        );
                                    self.txn.set(&req.content_key, &x);
                                    self.txn.set(&delta_referrer_key, &delta_base_hash);
                                    let base_content_index_key = self
                                        .key_codec
                                        .construct_contentindex_key(self.ns_id, delta_base_hash);
                                    self.txn.atomic_op(
                                        &base_content_index_key,
                                        &ContentIndex::generate_mutation_payload(self.now),
                                        MutationType::SetVersionstampedValue,
                                    );
                                    req.early_completion.store(true, Ordering::Relaxed);
                                    Ok(Some(delta_base_hash))
                                } else {
                                    Ok(None)
                                }
                            }
                            Err(e) => {
                                tracing::warn!(
                                    ns = hex::encode(&self.ns_id),
                                    error = %e,
                                    "delta encoding failed"
                                );
                                Err(())
                            }
                        }
                    } else {
                        Ok(None)
                    }
                });
            }

            let mut delta_base_hashes: Vec<Hash> = Vec::new();

            while let Some(delta_base_hash) = fut.next().await {
                let delta_base_hash = match delta_base_hash {
                    Ok(x) => x,
                    Err(()) => return None,
                };
                if let Some(delta_base_hash) = delta_base_hash {
                    delta_base_hashes.push(delta_base_hash.into());
                }
            }

            drop(fut);
            for h in &delta_base_hashes {
                self.seen_hashes.insert(*h);
            }
        }
        // Finally...
        for req in &write_reqs {
            if !req.early_completion.load(Ordering::Relaxed) {
                self.txn
                    .set(&req.content_key, &Page::compress_zstd(req.req.data));
            }
            // Set content index
            self.txn.atomic_op(
                &req.content_index_key,
                &ContentIndex::generate_mutation_payload(self.now),
                MutationType::SetVersionstampedValue,
            );
            self.seen_hashes.insert(req.hash);
        }

        Some(pregenerated_res)
    }
}
