use anyhow::{Context, Result};
use foundationdb::{options::TransactionOption, tuple::pack, Database, Transaction};

const BACKUP_AGENT_PREFIX: &[u8] = b"\xff\x02/db-backup-agent/";

pub struct ReplicaManager {
    pub dr_uid: Vec<u8>,
}

impl ReplicaManager {
    pub async fn new(db: &Database, dr_tag: &str) -> Result<Self> {
        let tuple = pack(&(&b"tagname"[..], dr_tag.as_bytes()));
        let key = BACKUP_AGENT_PREFIX
            .iter()
            .chain(&tuple)
            .copied()
            .collect::<Vec<_>>();
        let txn = db.create_trx()?;
        txn.set_option(TransactionOption::ReadLockAware).unwrap();
        txn.set_option(TransactionOption::ReadSystemKeys).unwrap();
        let uid = txn
            .get(&key, true)
            .await?
            .with_context(|| "DR tag not found")?
            .to_vec();
        tracing::info!(
            dr_tag = dr_tag,
            dr_uid = hex::encode(&uid),
            "dr replica mode enabled"
        );
        Ok(Self { dr_uid: uid })
    }

    pub async fn replica_version(&self, txn: &Transaction) -> Result<i64> {
        let dr_uid = self.dr_uid.as_slice();
        let tuple = pack(&(&b"state"[..], dr_uid, &b"last_begin_version"[..]));
        let key = BACKUP_AGENT_PREFIX
            .iter()
            .chain(&tuple)
            .copied()
            .collect::<Vec<_>>();
        txn.set_option(TransactionOption::ReadSystemKeys).unwrap();
        let value = txn.get(&key, true).await?;
        match value {
            Some(x) if x.len() == 8 => Ok(i64::from_le_bytes(x[..].try_into().unwrap())),
            _ => anyhow::bail!("bad last begin version"),
        }
    }
}
