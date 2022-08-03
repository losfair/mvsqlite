use std::time::SystemTime;

use anyhow::Result;
use foundationdb::{options::MutationType, FdbError};
use rand::RngCore;

use crate::server::{generate_suffix_versionstamp_atomic_op, ContentIndex, Server};

pub enum CommitResult {
    Committed { versionstamp: [u8; 10] },
    Conflict,
    BadPageReference,
}

pub struct CommitContext<'a> {
    pub ns_id: [u8; 10],
    pub client_assumed_version: [u8; 10],
    pub idempotency_key: [u8; 16],
    pub index_writes: &'a [(u32, [u8; 32])],
    pub metadata: Option<&'a str>,
}

impl Server {
    pub async fn commit<'a>(&self, ctx: CommitContext<'a>) -> Result<CommitResult> {
        // Begin the writes.
        // We do three-phase commit for large transactions here.
        let multi_phase = ctx.index_writes.len() >= 1000;
        let commit_token_key = self.construct_ns_commit_token_key(ctx.ns_id);
        let mut commit_token = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut commit_token);

        // Phase 1 - Idempotency && Check page existence
        let mut txn = self.db.create_trx()?;
        let last_write_version_key = self.construct_last_write_version_key(ctx.ns_id);
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

                if ctx.client_assumed_version < actual_last_write_version {
                    return Ok(CommitResult::Conflict);
                }
            }
            None => {}
        }

        for (_, page_hash) in ctx.index_writes {
            let content_index_key = self.construct_contentindex_key(ctx.ns_id, *page_hash);
            let data = txn.get(&content_index_key, false).await?;
            if data.is_none() {
                return Ok(CommitResult::BadPageReference);
            }
        }

        if multi_phase {
            txn.set(&commit_token_key, &commit_token);
            txn = txn.commit().await.map_err(|e| FdbError::from(e))?.reset();
            if txn
                .get(&commit_token_key, false)
                .await?
                .as_ref()
                .map(|x| &x[..])
                .unwrap_or_default()
                != commit_token
            {
                anyhow::bail!("commit interrupted before phase 2");
            }
        }

        // Phase 2 - content index insertion

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        for (_, page_hash) in ctx.index_writes {
            let key = self.construct_contentindex_key(ctx.ns_id, *page_hash);
            let value = ContentIndex::generate_mutation_payload(now);
            txn.atomic_op(&key, &value, MutationType::SetVersionstampedValue);
        }

        if multi_phase {
            txn = txn.commit().await.map_err(|e| FdbError::from(e))?.reset();
            if txn
                .get(&commit_token_key, false)
                .await?
                .as_ref()
                .map(|x| &x[..])
                .unwrap_or_default()
                != commit_token
            {
                anyhow::bail!("commit interrupted before phase 3");
            }
        }

        // Phase 3 - content insertion
        if let Some(md) = ctx.metadata {
            let metadata_key = self.construct_nsmd_key(ctx.ns_id);
            txn.set(&metadata_key, md.as_bytes());
        }

        for (page_index, page_hash) in ctx.index_writes {
            let page_key_template = self.construct_page_key(ctx.ns_id, *page_index, [0u8; 10]);
            let page_key_atomic_op = generate_suffix_versionstamp_atomic_op(&page_key_template);
            txn.atomic_op(
                &page_key_atomic_op,
                page_hash,
                MutationType::SetVersionstampedKey,
            );
        }

        let mut last_write_version_atomic_op_value = [0u8; 16 + 10 + 4];
        last_write_version_atomic_op_value[0..16].copy_from_slice(&ctx.idempotency_key);
        last_write_version_atomic_op_value[26..30].copy_from_slice(&16u32.to_le_bytes()[..]);
        txn.atomic_op(
            &last_write_version_key,
            &last_write_version_atomic_op_value,
            MutationType::SetVersionstampedValue,
        );
        let versionstamp_fut = txn.get_versionstamp();
        txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp_fut.await?;
        Ok(CommitResult::Committed {
            versionstamp: <[u8; 10]>::try_from(&versionstamp[..]).unwrap(),
        })
    }
}
