use bytes::Bytes;
use foundationdb::Transaction;
use moka::future::Cache;

use crate::{
    delta::reader::DeltaReader, keys::KeyCodec, page::PAGE_ENCODING_DELTA,
    util::get_txn_read_version_as_versionstamp,
};
use anyhow::Result;

pub struct DeltaWriter<'a> {
    pub txn: &'a Transaction,
    pub ns_id: [u8; 10],
    pub key_codec: &'a KeyCodec,
    pub content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

impl<'a> DeltaWriter<'a> {
    pub async fn delta_encode(
        &self,
        base_page_index: u32,
        this_page: &[u8],
    ) -> Result<Option<(Vec<u8>, [u8; 32])>> {
        let reader = DeltaReader {
            txn: self.txn,
            ns_id: self.ns_id,
            key_codec: self.key_codec,
            replica_manager: None,
            content_cache: self.content_cache,
        };
        let version_hex = hex::encode(&get_txn_read_version_as_versionstamp(self.txn).await?);

        let (_, delta_base_hash) = match reader
            .read_page_hash(base_page_index, Some(version_hex.as_str()), false)
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let (base_page, delta_base_hash) = {
            let base_page_key = self
                .key_codec
                .construct_content_key(self.ns_id, delta_base_hash);
            let base = match self.txn.get(&base_page_key, true).await? {
                Some(x) => x,
                None => return Ok(None),
            };
            if base.len() == 0 {
                return Ok(None);
            }
            if base[0] == PAGE_ENCODING_DELTA {
                // Flatten the link
                if base.len() < 33 {
                    return Ok(None);
                }
                let flattened_base_hash = <[u8; 32]>::try_from(&base[1..33]).unwrap();
                let flattened_base = match reader
                    .get_page_content_undecoded_snapshot(flattened_base_hash)
                    .await?
                {
                    Some(x) => reader.decode_page_no_delta(x).await?,
                    None => return Ok(None),
                };
                tracing::debug!(
                    from = hex::encode(&delta_base_hash),
                    to = hex::encode(&flattened_base_hash),
                    "flattened delta page"
                );
                (flattened_base, flattened_base_hash)
            } else {
                (reader.decode_page_no_delta(base).await?, delta_base_hash)
            }
        };
        if base_page.len() != this_page.len() || this_page.is_empty() {
            return Ok(None);
        }

        let num_diff_bytes = base_page
            .iter()
            .zip(this_page.iter())
            .filter(|(b, t)| b != t)
            .count();
        if num_diff_bytes >= this_page.len() / 5 {
            return Ok(None);
        }

        let xor_image = base_page
            .iter()
            .zip(this_page.iter())
            .map(|(b, t)| b ^ t)
            .collect::<Vec<_>>();
        let compressed = zstd::bulk::compress(&xor_image, 0)?;
        if compressed.len() >= this_page.len() / 3 {
            return Ok(None);
        }

        let mut output: Vec<u8> = Vec::with_capacity(1 + 32 + compressed.len());
        output.push(PAGE_ENCODING_DELTA);
        output.extend_from_slice(&delta_base_hash);
        output.extend_from_slice(&compressed);

        tracing::debug!(
            ns = hex::encode(&self.ns_id),
            base = hex::encode(&delta_base_hash),
            "delta encoded"
        );
        Ok(Some((output, delta_base_hash)))
    }
}
