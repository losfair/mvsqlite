use std::{
    collections::HashSet,
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

use crate::server::{decode_version, generate_suffix_versionstamp_atomic_op, ContentIndex, Server};

pub static COMMIT_MULTI_PHASE_THRESHOLD: AtomicUsize = AtomicUsize::new(1000);
pub static PLCC_READ_SET_SIZE_THRESHOLD: AtomicUsize = AtomicUsize::new(500);

pub enum CommitResult {
    Committed {
        versionstamp: [u8; 10],
        last_write_version: [u8; 10],
    },
    Conflict,
    BadPageReference,
    NamespaceNotDistinct,
}

pub struct CommitContext<'a> {
    pub idempotency_key: [u8; 16],
    pub namespaces: &'a [CommitNamespaceContext],
}

pub struct CommitNamespaceContext {
    pub ns_id: [u8; 10],
    pub client_assumed_version: [u8; 10],
    pub use_read_set: bool,
    pub read_set: HashSet<u32>,
    pub index_writes: Vec<(u32, [u8; 32])>,
    pub metadata: Option<String>,
}

impl Server {
    pub async fn commit<'a>(&self, ctx: CommitContext<'a>) -> Result<CommitResult> {
        let num_distinct_ns_id = ctx
            .namespaces
            .iter()
            .map(|x| x.ns_id)
            .collect::<HashSet<_>>()
            .len();
        if num_distinct_ns_id != ctx.namespaces.len() {
            // conflict with itself
            return Ok(CommitResult::NamespaceNotDistinct);
        }

        let mut last_write_version: [u8; 10] = [0u8; 10];

        // Begin the writes.
        // We do two-phase commit (not that 2PC!) for large transactions here.
        let num_total_writes = ctx
            .namespaces
            .iter()
            .map(|x| x.index_writes.len())
            .sum::<usize>();
        let multi_phase = num_total_writes >= COMMIT_MULTI_PHASE_THRESHOLD.load(Ordering::Relaxed);
        let plcc_enable = !multi_phase
            && ctx
                .namespaces
                .iter()
                .map(|x| x.read_set.len() + x.index_writes.len())
                .sum::<usize>()
                <= PLCC_READ_SET_SIZE_THRESHOLD.load(Ordering::Relaxed);
        let mut commit_token = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut commit_token);
        tracing::debug!(
            multi_phase = multi_phase,
            commit_token = hex::encode(&commit_token),
            num_namespaces = ctx.namespaces.len(),
            total_writes = num_total_writes,
            "entering commit"
        );

        // Phase 1 - Check page existence
        let mut txn = self.db.create_trx()?;
        let mut phase_1_ci_get_futures = FuturesOrdered::new();
        for ns in ctx.namespaces.iter() {
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

            let plcc_enable_ns = plcc_enable && ns.use_read_set;

            // Idempotency & non-plcc conflict check
            {
                let last_write_version_key = self.construct_last_write_version_key(ns.ns_id);
                let actual_lwv_value = txn.get(&last_write_version_key, false).await?;

                if let Some(t) = actual_lwv_value {
                    if t.len() == 16 + 10 {
                        let actual_idempotency_token = <[u8; 16]>::try_from(&t[0..16]).unwrap();
                        let actual_last_write_version = <[u8; 10]>::try_from(&t[16..26]).unwrap();
                        if actual_idempotency_token == ctx.idempotency_key {
                            return Ok(CommitResult::Committed {
                                versionstamp: actual_last_write_version,
                                last_write_version: [0xff; 10],
                            });
                        }

                        if ns.client_assumed_version < actual_last_write_version {
                            if !plcc_enable_ns {
                                return Ok(CommitResult::Conflict);
                            }
                        }

                        last_write_version = last_write_version.max(actual_last_write_version);
                    }
                }
                let mut new_lwv_value = [0u8; 16 + 10 + 4];
                new_lwv_value[0..16].copy_from_slice(&ctx.idempotency_key);
                new_lwv_value[26..30].copy_from_slice(&16u32.to_le_bytes()[..]);
                txn.atomic_op(
                    &last_write_version_key,
                    &new_lwv_value,
                    MutationType::SetVersionstampedValue,
                );
            }

            // Fine-grained conflict check
            if plcc_enable_ns {
                let mut fut_list = FuturesOrdered::new();
                let check_set: HashSet<u32> = ns
                    .read_set
                    .iter()
                    .copied()
                    .chain(ns.index_writes.iter().map(|x| x.0))
                    .collect();
                for &page in &check_set {
                    let read_page_hash_fut = self.read_page_hash(&txn, ns.ns_id, page, None);
                    fut_list.push(async move { (page, read_page_hash_fut.await) });
                }

                while let Some((page, data)) = fut_list.next().await {
                    let data = data?;
                    if let Some((version, _)) = data {
                        let version = decode_version(&version)?;
                        if version > ns.client_assumed_version {
                            tracing::warn!(
                                page,
                                page_version = hex::encode(&version),
                                client_assumed_version = hex::encode(&ns.client_assumed_version),
                                "page-level conflict check failed"
                            );
                            return Ok(CommitResult::Conflict);
                        }
                    }
                }
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
        }

        let versionstamp_fut = txn.get_versionstamp();
        txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp_fut.await?;
        Ok(CommitResult::Committed {
            versionstamp: <[u8; 10]>::try_from(&versionstamp[..]).unwrap(),
            last_write_version,
        })
    }
}
