use anyhow::Result;
use foundationdb::{options::MutationType, FdbError};
use rand::RngCore;

use crate::{error::UnretryableError, server::Server};

lazy_static::lazy_static! {
  static ref LOCK_TOKEN_REGEX: regex::Regex = regex::Regex::new(r"^([0-9a-f]{20})\.(.+)$").unwrap();
}

impl Server {
    pub async fn ns_lock_acquire(&self, ns_id: [u8; 10], lock_id_str: &str) -> Result<String> {
        let lock_id = lock_id_str.as_bytes();
        let lock_token_key = self.construct_lock_token_key(ns_id);
        let txn = self.db.create_trx()?;
        let token = txn.get(&lock_token_key, false).await?;

        // Idempotent?
        if let Some(token) = token {
            if token.len() >= 10 {
                if &token[10..] == lock_id {
                    return Ok(format!("{}.{}", hex::encode(&token[..10]), lock_id_str));
                }
            }
        }

        let mut atomic_payload: Vec<u8> = vec![0u8; 10 + lock_id.len() + 4];
        atomic_payload[10..10 + lock_id.len()].copy_from_slice(lock_id);
        txn.atomic_op(
            &lock_token_key,
            &atomic_payload,
            MutationType::SetVersionstampedValue,
        );
        let versionstamp = txn.get_versionstamp();
        txn.commit().await.map_err(|e| FdbError::from(e))?;
        let versionstamp = versionstamp.await?;

        Ok(format!(
            "{}.{}",
            hex::encode(&versionstamp[..]),
            lock_id_str
        ))
    }

    pub async fn ns_lock_release(&self, ns_id: [u8; 10], lock_token: &str) -> Result<bool> {
        let mut locked_version = [0u8; 10];
        let captures = match LOCK_TOKEN_REGEX.captures(lock_token) {
            Some(x) => x,
            None => {
                return Err(UnretryableError(anyhow::anyhow!("invalid lock token")).into());
            }
        };

        // should not fail, validated by regex
        hex::decode_to_slice(captures.get(1).unwrap().as_str(), &mut locked_version).unwrap();
        let lock_id = captures.get(2).unwrap().as_str();

        let lock_token_key = self.construct_lock_token_key(ns_id);
        let txn = self.db.create_trx()?;
        let token = txn.get(&lock_token_key, false).await?;

        let mut unlocked = false;

        if let Some(token) = token {
            if token.len() >= 10 {
                if &token[0..10] == &locked_version[..] && &token[10..] == lock_id.as_bytes() {
                    txn.clear(&lock_token_key);

                    let mut new_lwv_value = [0u8; 16 + 10];

                    // Fake idempotency key
                    rand::thread_rng().fill_bytes(&mut new_lwv_value[..16]);
                    new_lwv_value[16..16 + 10].copy_from_slice(&locked_version);

                    txn.set(
                        &self.construct_last_write_version_key(ns_id),
                        &new_lwv_value,
                    );
                    txn.commit().await.map_err(|e| FdbError::from(e))?;
                    unlocked = true;
                }
            }
        }

        Ok(unlocked)
    }
}
