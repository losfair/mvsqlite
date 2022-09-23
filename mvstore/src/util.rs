use std::time::Duration;

use anyhow::{Context, Result};
use foundationdb::Transaction;

pub async fn get_txn_read_version_as_versionstamp(txn: &Transaction) -> Result<[u8; 10]> {
    let read_version = txn.get_read_version().await? as u64;
    let mut buf = [0u8; 10];
    buf[0..8].copy_from_slice(&read_version.to_be_bytes());

    // Now we can observe all changes with `committed_version == read_version`.
    buf[8] = 255;
    buf[9] = 255;
    Ok(buf)
}

pub fn decode_version(version: &str) -> Result<[u8; 10]> {
    let mut bytes = [0u8; 10];
    hex::decode_to_slice(version, &mut bytes).with_context(|| "cannot decode version")?;
    Ok(bytes)
}

pub fn generate_suffix_versionstamp_atomic_op(template: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(template.len() + 4);
    out.extend_from_slice(template);
    out.extend_from_slice(&(template.len() as u32 - 10).to_le_bytes());
    out
}

pub struct ContentIndex {
    pub time: Duration,
    pub versionstamp: [u8; 10],
}

impl ContentIndex {
    pub fn generate_mutation_payload(now: Duration) -> [u8; 22] {
        let mut buf = [0u8; 22];
        buf[0..8].copy_from_slice(&now.as_secs().to_be_bytes());
        buf[18..22].copy_from_slice(&8u32.to_le_bytes()[..]);
        buf
    }

    pub fn decode(x: &[u8]) -> Result<Self> {
        if x.len() != 18 {
            return Err(anyhow::anyhow!("invalid content index"));
        }
        let time = Duration::from_secs(u64::from_be_bytes(x[0..8].try_into().unwrap()));
        let versionstamp = x[8..18].try_into().unwrap();
        Ok(Self { time, versionstamp })
    }
}
