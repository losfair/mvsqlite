use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};

use blake3::Hash;
use foundationdb::{options::MutationType, Transaction};

use crate::{
    delta::writer::DeltaWriter,
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
}

pub struct WriteApplierContext<'a> {
    pub txn: &'a Transaction,
    pub ns_id: [u8; 10],
    pub key_codec: &'a KeyCodec,
    pub now: Duration,
}

impl<'a> WriteApplier<'a> {
    pub fn new(ctx: WriteApplierContext<'a>) -> Self {
        Self {
            txn: ctx.txn,
            ns_id: ctx.ns_id,
            key_codec: ctx.key_codec,
            now: ctx.now,
            seen_hashes: HashSet::new(),
        }
    }

    pub async fn apply_write<'b>(&mut self, write_req: &WriteRequest<'b>) -> Option<WriteResponse> {
        if write_req.data.len() > MAX_PAGE_SIZE {
            tracing::warn!(
                ns = hex::encode(&self.ns_id),
                len = write_req.data.len(),
                limit = MAX_PAGE_SIZE,
                "page is too large"
            );
            return None;
        }
        let hash = blake3::hash(write_req.data);

        // Writing a same hash twice in the same transaction will fail because
        // we cannot read from a key previously written with `SetVersionstampedValue`.
        if self.seen_hashes.contains(&hash) {
            return Some(WriteResponse {
                hash: hash.as_bytes().to_vec(),
            });
        }

        let content_key = self
            .key_codec
            .construct_content_key(self.ns_id, *hash.as_bytes());
        let content_index_key = self
            .key_codec
            .construct_contentindex_key(self.ns_id, *hash.as_bytes());

        let mut early_completion = false;

        // This is not only an optimization. Without doing this check it is possible to form
        // loops in delta page construction.
        match self.txn.get(&content_index_key, false).await {
            Ok(x) => {
                if x.is_some() {
                    early_completion = true;
                }
            }
            Err(e) => {
                tracing::warn!(ns = hex::encode(&self.ns_id), error = %e, "error getting content");
                return None;
            }
        }

        // Attempt delta-encoding
        if !early_completion {
            let writer = DeltaWriter {
                txn: self.txn,
                ns_id: self.ns_id,
                key_codec: self.key_codec,
            };
            if let Some(delta_base_index) = write_req.delta_base {
                match writer.delta_encode(delta_base_index, &write_req.data).await {
                    Ok(x) => {
                        if let Some((x, delta_base_hash)) = x {
                            let delta_referrer_key = self
                                .key_codec
                                .construct_delta_referrer_key(self.ns_id, *hash.as_bytes());
                            self.txn.set(&content_key, &x);
                            self.txn.set(&delta_referrer_key, &delta_base_hash);
                            let base_content_index_key = self
                                .key_codec
                                .construct_contentindex_key(self.ns_id, delta_base_hash);
                            let now = SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap();
                            self.txn.atomic_op(
                                &base_content_index_key,
                                &ContentIndex::generate_mutation_payload(now),
                                MutationType::SetVersionstampedValue,
                            );
                            early_completion = true;
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            ns = hex::encode(&self.ns_id),
                            error = %e,
                            "delta encoding failed"
                        );
                        return None;
                    }
                }
            }
        }

        // Finally...
        if !early_completion {
            self.txn
                .set(&content_key, &Page::compress_zstd(write_req.data));
        }

        // Set content index
        let content_index_key = self
            .key_codec
            .construct_contentindex_key(self.ns_id, *hash.as_bytes());
        self.txn.atomic_op(
            &content_index_key,
            &ContentIndex::generate_mutation_payload(self.now),
            MutationType::SetVersionstampedValue,
        );
        self.seen_hashes.insert(hash);
        Some(WriteResponse {
            hash: hash.as_bytes().to_vec(),
        })
    }
}
