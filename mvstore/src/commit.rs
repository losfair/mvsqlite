use std::{
    collections::{BTreeSet, HashMap, HashSet},
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
pub static PLCC_READ_SET_SIZE_THRESHOLD: AtomicUsize = AtomicUsize::new(2000);
pub static INTERVAL_ENTRY_MAX_SIZE: AtomicUsize = AtomicUsize::new(500);

pub enum CommitResult {
    Committed {
        versionstamp: [u8; 10],
        last_write_version: [u8; 10],
        changelog: HashMap<String, Vec<u32>>,
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
    pub ns_key: String,
    pub ns_id: [u8; 10],
    pub client_assumed_version: [u8; 10],
    pub use_read_set: bool,
    pub read_set: HashSet<u32>,
    pub index_writes: Vec<(u32, [u8; 32])>,
    pub metadata: Option<String>,
    pub lock_token: Option<String>,
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
                .map(|x| x.read_set.len())
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

            let mut written_pages: BTreeSet<u32> = BTreeSet::new();
            let versionstamp_override: Option<[u8; 10]>;

            let plcc_enable_ns = plcc_enable && ns.use_read_set;

            // Idempotency & non-plcc conflict check
            {
                let last_write_version_key = self.construct_last_write_version_key(ns.ns_id);
                let lock_token_key = self.construct_lock_token_key(ns.ns_id);

                // If we rely on PLCC to check conflicts, it is not necessary to add LWV to conflict set.
                let actual_lwv_value = txn.get(&last_write_version_key, plcc_enable_ns);
                let actual_lock_token = txn.get(&lock_token_key, false);

                let actual_lwv_value = actual_lwv_value.await?;
                let actual_lock_token: Option<([u8; 10], Vec<u8>)> = actual_lock_token
                    .await?
                    .map(|x| {
                        if x.len() >= 10 {
                            Ok((x[0..10].try_into().unwrap(), x[10..].to_vec()))
                        } else {
                            Err(anyhow::anyhow!("invalid lock token"))
                        }
                    })
                    .transpose()?;

                let requested = ns.lock_token.as_ref().map(|x| x.as_str());
                let actual = actual_lock_token
                    .as_ref()
                    .map(|x| format!("{}.{}", hex::encode(&x.0), String::from_utf8_lossy(&x.1)));
                if requested != actual.as_ref().map(|x| x.as_str()) {
                    tracing::warn!(
                        ns_key = ns.ns_key,
                        ns_id = hex::encode(&ns.ns_id),
                        ?requested,
                        ?actual,
                        "lock token mismatch"
                    );
                    return Ok(CommitResult::Conflict);
                }

                versionstamp_override = actual_lock_token.as_ref().map(|x| x.0);

                if let Some(t) = actual_lwv_value {
                    if t.len() == 16 + 10 {
                        let actual_idempotency_token = <[u8; 16]>::try_from(&t[0..16]).unwrap();
                        let actual_last_write_version = <[u8; 10]>::try_from(&t[16..26]).unwrap();
                        if actual_idempotency_token == ctx.idempotency_key {
                            // This is an idempotent retry - return conservative values
                            return Ok(CommitResult::Committed {
                                versionstamp: actual_last_write_version,
                                last_write_version: [0xff; 10],
                                changelog: HashMap::new(),
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

                // Do not write LWV if this is a long-running write transaction. Defer to lock release.
                if versionstamp_override.is_none() {
                    let mut new_lwv_value = [0u8; 16 + 10 + 4];
                    new_lwv_value[0..16].copy_from_slice(&ctx.idempotency_key);
                    new_lwv_value[26..30].copy_from_slice(&16u32.to_le_bytes()[..]);
                    txn.atomic_op(
                        &last_write_version_key,
                        &new_lwv_value,
                        MutationType::SetVersionstampedValue,
                    );
                }
            }

            // Fine-grained conflict check
            if plcc_enable_ns {
                let mut fut_list = FuturesOrdered::new();
                for &page in &ns.read_set {
                    let read_page_hash_fut = self.read_page_hash(&txn, ns.ns_id, page, None, false);
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
                let ci_key = self.construct_contentindex_key(ns.ns_id, *page_hash);

                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }

                if let Some(v) = versionstamp_override {
                    let page_key = self.construct_page_key(ns.ns_id, *page_index, v);
                    txn.set(&page_key, page_hash);
                } else {
                    let page_key_template =
                        self.construct_page_key(ns.ns_id, *page_index, [0u8; 10]);
                    let page_key_atomic_op =
                        generate_suffix_versionstamp_atomic_op(&page_key_template);
                    txn.atomic_op(
                        &page_key_atomic_op,
                        page_hash,
                        MutationType::SetVersionstampedKey,
                    );
                }

                if multi_phase {
                    txn.set_option(TransactionOption::NextWriteNoWriteConflictRange)?;
                }

                // Always write the current versionstamp to the content index. This is necessary for
                // correctness because of the GC DUC process. Consider this sequence:
                //
                // 1. NS lock acquire
                // 2. DUC stage 1 (Fetch read version)
                // 3. DUC stage 2 (scan inconsistent snapshot)
                // 4. Write + Commit
                // 5. DUC stage 3 (CAM index scan)
                //
                // The new pages added in step 4 are not present in the snapshot created in step 3, and are left to
                // step 5 for detection. If the content index versionstamps of the new pages are rewritten to lock
                // version, step 5 would think it is a stale old page and delete it.
                let ci_atomic_op = ContentIndex::generate_mutation_payload(now);
                txn.atomic_op(&ci_key, &ci_atomic_op, MutationType::SetVersionstampedValue);
                written_pages.insert(*page_index);
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

            if let Some(v) = versionstamp_override {
                let changelog_key = self.construct_changelog_key(ns.ns_id, v);
                txn.set(
                    &changelog_key,
                    &[1u8], // infinite
                );
            } else {
                let mut changelog: Vec<u8> = vec![];

                if written_pages.len() >= INTERVAL_ENTRY_MAX_SIZE.load(Ordering::Relaxed) {
                    changelog.push(1); // infinite
                } else {
                    changelog.reserve(1 + written_pages.len() * 4);
                    changelog.push(0);
                    for index in written_pages {
                        changelog.extend_from_slice(&index.to_be_bytes());
                    }
                }

                let changelog_atomic_op_key = generate_suffix_versionstamp_atomic_op(
                    &self.construct_changelog_key(ns.ns_id, [0u8; 10]),
                );
                txn.atomic_op(
                    &changelog_atomic_op_key,
                    &changelog,
                    MutationType::SetVersionstampedKey,
                );
            }
        }

        let versionstamp_fut = txn.get_versionstamp();
        txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp_fut.await?;

        // Read changelog
        let mut changelog: HashMap<String, Vec<u32>> = HashMap::new();
        {
            let txn = self.db.create_trx()?;
            for ns in ctx.namespaces {
                let interval = self
                    .read_interval(
                        &txn,
                        ns.ns_id,
                        ns.client_assumed_version,
                        versionstamp[..].try_into().unwrap(),
                        false,
                    )
                    .await?;
                if let Some(interval) = interval {
                    changelog.insert(ns.ns_key.clone(), interval);
                }
            }
        }
        Ok(CommitResult::Committed {
            versionstamp: <[u8; 10]>::try_from(&versionstamp[..]).unwrap(),
            last_write_version,
            changelog,
        })
    }
}
