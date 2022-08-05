use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::SystemTime,
};

use anyhow::Result;
use foundationdb::{
    options::{ConflictRangeType, MutationType, TransactionOption},
    FdbError,
};
use futures::stream::FuturesOrdered;
use futures::StreamExt;
use rand::RngCore;

use crate::server::{generate_suffix_versionstamp_atomic_op, ContentIndex, Server};

pub static COMMIT_MULTI_PHASE_THRESHOLD: AtomicUsize = AtomicUsize::new(1000);

pub enum CommitResult {
    Committed { versionstamp: [u8; 10] },
    Conflict,
    BadPageReference,
}

pub struct CommitContext<'a> {
    pub idempotency_key: [u8; 16],
    pub namespaces: &'a [CommitNamespaceContext],
}

pub struct CommitNamespaceContext {
    pub ns_id: [u8; 10],
    pub client_assumed_version: [u8; 10],
    pub index_writes: Vec<(u32, [u8; 32])>,
    pub metadata: Option<String>,
}

impl Server {
    pub async fn commit<'a>(&self, ctx: CommitContext<'a>) -> Result<CommitResult> {
        // Begin the writes.
        // We do two-phase commit (not that 2PC!) for large transactions here.
        let num_total_writes = ctx
            .namespaces
            .iter()
            .map(|x| x.index_writes.len())
            .sum::<usize>();
        let multi_phase = num_total_writes >= COMMIT_MULTI_PHASE_THRESHOLD.load(Ordering::Relaxed);
        let mut commit_token = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut commit_token);
        tracing::debug!(
            multi_phase = multi_phase,
            commit_token = hex::encode(&commit_token),
            num_namespaces = ctx.namespaces.len(),
            total_writes = num_total_writes,
            "entering commit"
        );

        // Phase 1 - Idempotency && Check page existence
        let mut txn = self.db.create_trx()?;
        let mut phase_1_ci_get_futures = FuturesOrdered::new();
        for ns in ctx.namespaces.iter() {
            let last_write_version_key = self.construct_last_write_version_key(ns.ns_id);
            let last_write_version = txn.get(&last_write_version_key, false).await?;
            match &last_write_version {
                Some(x) if x.len() != 16 + 10 => {
                    anyhow::bail!("invalid last write version");
                }
                Some(x) => {
                    let actual_idempotency_key = <[u8; 16]>::try_from(&x[0..16]).unwrap();
                    let actual_last_write_version = <[u8; 10]>::try_from(&x[16..26]).unwrap();

                    if actual_idempotency_key == ctx.idempotency_key {
                        return Ok(CommitResult::Committed {
                            versionstamp: actual_last_write_version,
                        });
                    }

                    if ns.client_assumed_version < actual_last_write_version {
                        return Ok(CommitResult::Conflict);
                    }
                }
                None => {}
            }

            for (_, page_hash) in &ns.index_writes {
                let content_index_key = self.construct_contentindex_key(ns.ns_id, *page_hash);
                phase_1_ci_get_futures.push(txn.get(&content_index_key, false));
            }
        }

        while let Some(data) = phase_1_ci_get_futures.next().await {
            if data?.is_none() {
                return Ok(CommitResult::BadPageReference);
            }
        }

        if multi_phase {
            let commit_token_keys = ctx
                .namespaces
                .iter()
                .map(|x| self.construct_ns_commit_token_key(x.ns_id))
                .collect::<Vec<_>>();
            for k in &commit_token_keys {
                txn.set(k, &commit_token);
            }
            txn = txn.commit().await.map_err(|e| FdbError::from(e))?.reset();
            let mut current_tokens = FuturesOrdered::new();
            for k in &commit_token_keys {
                current_tokens.push(txn.get(k, false));
            }
            while let Some(t) = current_tokens.next().await {
                if t?.as_ref().map(|x| &x[..]).unwrap_or_default() != commit_token {
                    anyhow::bail!("commit interrupted before phase 2");
                }
            }
            tracing::debug!(commit_token = hex::encode(&commit_token), "commit phase 2");
        }

        // Phase 2 - content index insertion

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();

        for ns in ctx.namespaces {
            if let Some(md) = &ns.metadata {
                let metadata_key = self.construct_nsmd_key(ns.ns_id);
                txn.set(&metadata_key, md.as_bytes());
            }

            for (page_index, page_hash) in &ns.index_writes {
                let page_key_template = self.construct_page_key(ns.ns_id, *page_index, [0u8; 10]);
                let page_key_atomic_op = generate_suffix_versionstamp_atomic_op(&page_key_template);
                let ci_key = self.construct_contentindex_key(ns.ns_id, *page_hash);
                let ci_atomic_op = ContentIndex::generate_mutation_payload(now);
                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }
                txn.atomic_op(
                    &page_key_atomic_op,
                    page_hash,
                    MutationType::SetVersionstampedKey,
                );
                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }
                txn.atomic_op(&ci_key, &ci_atomic_op, MutationType::SetVersionstampedValue);
            }
            if multi_phase {
                txn.add_conflict_range(
                    &self.construct_page_key(ns.ns_id, std::u32::MIN, [0u8; 10]),
                    &self.construct_page_key(ns.ns_id, std::u32::MAX, [0xffu8; 10]),
                    ConflictRangeType::Write,
                )?;
                txn.add_conflict_range(
                    &self.construct_contentindex_key(ns.ns_id, [0u8; 32]),
                    &self.construct_contentindex_key(ns.ns_id, [0xffu8; 32]),
                    ConflictRangeType::Write,
                )?;
            }
            let mut last_write_version_atomic_op_value = [0u8; 16 + 10 + 4];
            last_write_version_atomic_op_value[0..16].copy_from_slice(&ctx.idempotency_key);
            last_write_version_atomic_op_value[26..30].copy_from_slice(&16u32.to_le_bytes()[..]);

            let last_write_version_key = self.construct_last_write_version_key(ns.ns_id);
            txn.atomic_op(
                &last_write_version_key,
                &last_write_version_atomic_op_value,
                MutationType::SetVersionstampedValue,
            );
        }

        let versionstamp_fut = txn.get_versionstamp();
        txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp_fut.await?;
        Ok(CommitResult::Committed {
            versionstamp: <[u8; 10]>::try_from(&versionstamp[..]).unwrap(),
        })
    }
}
