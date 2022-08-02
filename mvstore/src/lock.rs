use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use foundationdb::{Database, FdbError, Transaction};
use rand::{thread_rng, RngCore};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize)]
pub struct LockInfo<'a> {
    expiry: u64,
    #[serde(with = "serde_bytes")]
    owner_id: &'a [u8],
}

pub struct DistributedLock {
    key: Vec<u8>,
    description: String,
    owner_id: [u8; 16],

    bg: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
}

impl DistributedLock {
    pub fn new(key: Vec<u8>, description: String) -> Self {
        let mut owner_id = [0u8; 16];
        thread_rng().fill_bytes(&mut owner_id);
        Self {
            key,
            description,
            owner_id,
            bg: None,
        }
    }

    async fn raw_check_sync(key: &[u8], owner_id: [u8; 16], txn: &Transaction) -> Result<bool> {
        let data = txn.get(key, false).await?;
        if let Some(x) = data {
            let info: LockInfo = match rmp_serde::from_slice(&x) {
                Ok(info) => info,
                Err(_) => {
                    return Ok(false);
                }
            };
            Ok(info.owner_id == owner_id)
        } else {
            Ok(false)
        }
    }

    pub async fn check_sync(&self, txn: &Transaction) -> Result<()> {
        if self.bg.is_none() {
            anyhow::bail!("lock is not acquired");
        }

        if !Self::raw_check_sync(&self.key, self.owner_id, txn).await? {
            anyhow::bail!("lock no longer hold by us");
        }

        Ok(())
    }

    pub async fn create_txn_and_check_sync(&self, db: &Database) -> Result<Transaction> {
        let mut txn = db.create_trx()?;
        loop {
            match self.check_sync(&txn).await {
                Ok(()) => return Ok(txn),
                Err(e) => match e.downcast::<FdbError>() {
                    Ok(x) => {
                        txn = txn.on_error(x).await?;
                    }
                    Err(e) => return Err(e),
                },
            }
        }
    }

    pub async fn lock(
        &mut self,
        mut txn_gen: impl FnMut() -> Result<Transaction> + Send + Sync + 'static,
        ttl: Duration,
    ) -> Result<bool> {
        if self.bg.is_some() {
            anyhow::bail!("already locked");
        }
        let mut txn = txn_gen()?;
        loop {
            let data = txn.get(&self.key, false).await?;
            let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
            if let Some(x) = data {
                let info: LockInfo = rmp_serde::from_slice(&x)
                    .with_context(|| "failed to deserialize DistributedLock")?;
                if now.as_secs() < info.expiry {
                    return Ok(false);
                }
            }
            let info = LockInfo {
                expiry: (now + ttl).as_secs(),
                owner_id: &self.owner_id,
            };
            let info = rmp_serde::to_vec_named(&info)
                .with_context(|| "failed to serialize DistributedLock")?;
            txn.set(&self.key, &info);
            match txn.commit().await {
                Ok(_) => break,
                Err(e) => {
                    txn = e.on_error().await?;
                }
            }
        }
        let (close_req_tx, close_req_rx) = oneshot::channel();
        let (close_ack_tx, close_ack_rx) = oneshot::channel();
        self.bg = Some((close_req_tx, close_ack_rx));
        let key = self.key.clone();
        let description = self.description.clone();
        let owner_id = self.owner_id.clone();
        tokio::spawn(async move {
            let _close_ack_tx = close_ack_tx;
            tokio::select! {
                _ = close_req_rx => {

                }
                _ = renew_loop(&mut txn_gen, &key, &description, owner_id, ttl) => {
                    tracing::error!(desc = description, "lock dropped");
                }
            }
            let mut unlock_ok = false;
            if let Ok(txn) = txn_gen() {
                if let Ok(true) = Self::raw_check_sync(&key, owner_id, &txn).await {
                    txn.clear(&key);
                    if txn.commit().await.is_ok() {
                        unlock_ok = true;
                        tracing::debug!(desc = description, "unlocked");
                    }
                }
            }

            if !unlock_ok {
                tracing::warn!(desc = description, "failed to unlock");
            }
        });
        Ok(true)
    }

    #[allow(dead_code)]
    pub async fn unlock(&mut self) {
        let (close_req_tx, close_ack_rx) = self.bg.take().expect("unlock without lock");
        drop(close_req_tx);
        let _ = close_ack_rx.await;
    }
}

async fn renew_loop(
    txn_gen: &mut (impl FnMut() -> Result<Transaction> + Send + Sync),
    key: &[u8],
    description: &str,
    owner_id: [u8; 16],
    ttl: Duration,
) {
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let mut txn = match txn_gen() {
            Ok(txn) => txn,
            Err(e) => {
                tracing::error!(error = %e, desc = description, "renew_loop failed to create txn");
                return;
            }
        };
        loop {
            let data = match txn.get(key, false).await {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!(error = %e, desc = description, "renew_loop read failed");
                    match txn.on_error(e).await {
                        Ok(recovered) => {
                            txn = recovered;
                            continue;
                        }
                        Err(_) => {
                            return;
                        }
                    }
                }
            };
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();
            if let Some(x) = data {
                let mut info: LockInfo = match rmp_serde::from_slice(&x) {
                    Ok(info) => info,
                    Err(e) => {
                        tracing::error!(error = %e, desc = description, "renew_loop failed to deserialize lock info");
                        return;
                    }
                };
                if info.owner_id != owner_id {
                    tracing::error!(
                        desc = description,
                        new_owner_id = hex::encode(&info.owner_id),
                        "lock preempted"
                    );
                    return;
                }
                info.expiry = (now + ttl).as_secs();
                let info = rmp_serde::to_vec_named(&info).unwrap();
                txn.set(key, &info);
                match txn.commit().await {
                    Ok(_) => {
                        tracing::debug!(desc = description, "renewed distributed lock");
                        break;
                    }
                    Err(e) => {
                        tracing::error!(error = %e, desc = description, "renew_loop commit failed");
                        match e.on_error().await {
                            Ok(recovered) => {
                                txn = recovered;
                                continue;
                            }
                            Err(_) => {
                                return;
                            }
                        }
                    }
                }
            } else {
                tracing::error!(desc = description, "lock was lost");
                return;
            }
        }
    }
}
