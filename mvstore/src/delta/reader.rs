use std::{
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use foundationdb::{
    options::{ConflictRangeType, StreamingMode},
    RangeOption, Transaction,
};
use futures::TryStreamExt;
use moka::future::Cache;
use thiserror::Error;
use tokio::task::block_in_place;

use crate::{
    fixed::FixedString,
    keys::KeyCodec,
    page::{MAX_PAGE_SIZE, PAGE_ENCODING_DELTA, PAGE_ENCODING_NONE, PAGE_ENCODING_ZSTD},
    replica::ReplicaManager,
    util::decode_version,
};
use anyhow::{Context, Result};

pub static WIRE_ZSTD: AtomicBool = AtomicBool::new(false);

pub struct DeltaReader<'a> {
    pub txn: &'a Transaction,
    pub ns_id: [u8; 10],
    pub key_codec: &'a KeyCodec,
    pub replica_manager: Option<&'a ReplicaManager>,
    pub content_cache: Option<&'a Cache<[u8; 32], Bytes>>,
}

impl<'a> DeltaReader<'a> {
    pub async fn read_page_hash(
        &self,
        page_index: u32,
        page_version_hex: Option<&str>,
        snapshot: bool,
    ) -> Result<Option<(FixedString, [u8; 32])>> {
        let page_version = match page_version_hex {
            Some(x) => decode_version(x)?,
            None => [0xffu8; 10],
        };
        let scan_end = self
            .key_codec
            .construct_page_key(self.ns_id, page_index, page_version);
        let scan_start = self
            .key_codec
            .construct_page_key(self.ns_id, page_index, [0u8; 10]);
        let page_vec: Vec<_> = self
            .txn
            .get_ranges_keyvalues(
                RangeOption {
                    limit: Some(1),
                    reverse: true,
                    mode: StreamingMode::Small,
                    ..RangeOption::from(scan_start.as_slice()..=scan_end.as_slice())
                },
                true,
            )
            .try_collect()
            .await?;
        assert!(page_vec.len() <= 1);
        if page_vec.is_empty() {
            // The reason we get an empty range is that there is no version of this page. Encode this causality.
            if !snapshot {
                self.txn.add_conflict_range(
                    &scan_start[..],
                    &scan_end
                        .iter()
                        .copied()
                        .chain(std::iter::once(0u8))
                        .collect::<Vec<u8>>(),
                    ConflictRangeType::Read,
                )?;
            }
            Ok(None)
        } else {
            let page = page_vec.into_iter().next().unwrap();
            let key = page.key();

            // The reason we get this page is that there are no more versions after. Encode this causality.
            if !snapshot {
                self.txn.add_conflict_range(
                    key,
                    &scan_end
                        .iter()
                        .copied()
                        .chain(std::iter::once(0u8))
                        .collect::<Vec<u8>>(),
                    ConflictRangeType::Read,
                )?;
            }

            let mut version = [0u8; 20];
            hex::encode_to_slice(&key[key.len() - 10..], &mut version).unwrap();
            let hash = page.value();
            let hash = <[u8; 32]>::try_from(hash).with_context(|| "invalid content hash")?;
            Ok(Some((
                FixedString::from_str(std::str::from_utf8(&version).unwrap()).unwrap(),
                hash,
            )))
        }
    }

    pub async fn get_page_content_decoded_snapshot_compressed(
        &self,
        hash: [u8; 32],
    ) -> Result<Option<Bytes>> {
        let fetch_fut = async {
            match self.get_page_content_undecoded_snapshot(hash).await? {
                Some(x) => match self.decode_page_with_delta(x).await {
                    Ok(x) => Ok(Some(if WIRE_ZSTD.load(Ordering::Relaxed) {
                        Bytes::from(zstd::bulk::compress(&x, 0)?)
                    } else {
                        x
                    })),
                    Err(e) => Err(e),
                },
                None => Ok(None),
            }
        };

        let out = match self.content_cache {
            Some(cache) => {
                #[derive(Error, Debug)]
                #[error("not found")]
                struct NotFoundError;

                let res = cache
                    .try_get_with(hash, async {
                        fetch_fut
                            .await
                            .map_err(anyhow::Error::from)
                            .and_then(|x| x.ok_or_else(|| anyhow::Error::from(NotFoundError)))
                    })
                    .await;

                match res {
                    Ok(x) => Some(x),
                    Err(e) => {
                        if e.chain()
                            .any(|x| x.downcast_ref::<NotFoundError>().is_some())
                        {
                            None
                        } else {
                            return Err(anyhow::anyhow!(
                                "failed to load decoded page content into cache: {}",
                                e
                            ));
                        }
                    }
                }
            }
            None => fetch_fut.await?,
        };

        Ok(out)
    }

    pub(super) async fn get_page_content_undecoded_snapshot(
        &self,
        hash: [u8; 32],
    ) -> Result<Option<impl AsRef<[u8]> + Send + Sync + 'static>> {
        let key = self.key_codec.construct_content_key(self.ns_id, hash);
        let undecoded = self.txn.get(&key, true).await?;
        let undecoded = match undecoded {
            Some(x) => x,
            None => return Ok(None),
        };

        Ok(Some(undecoded))
    }

    pub(super) async fn decode_page_no_delta<T: AsRef<[u8]> + Send + Sync + 'static>(
        &self,
        data_container: T,
    ) -> Result<Bytes> {
        let data = data_container.as_ref();
        if data.len() == 0 {
            return Ok(Bytes::new());
        }

        let encode_type = data[0];
        match encode_type {
            PAGE_ENCODING_NONE => {
                // not compressed
                Ok(Bytes::from(data[1..].to_vec()))
            }
            PAGE_ENCODING_ZSTD => {
                // zstd
                let data = block_in_place(|| {
                    zstd::bulk::decompress(&data_container.as_ref()[1..], MAX_PAGE_SIZE)
                })
                .with_context(|| "zstd decompress failed")?;
                Ok(Bytes::from(data))
            }
            _ => Err(anyhow::anyhow!(
                "decode_page_no_delta: unknown page encoding: {}",
                encode_type
            )),
        }
    }

    async fn decode_page_with_delta<T: AsRef<[u8]> + Send + Sync + 'static>(
        &self,
        data_container: T,
    ) -> Result<Bytes> {
        let data = data_container.as_ref();
        if data.len() == 0 {
            return Ok(Bytes::new());
        }

        let encode_type = data[0];
        match encode_type {
            PAGE_ENCODING_DELTA => {
                if data.len() < 33 {
                    anyhow::bail!("invalid delta encoding");
                }
                let base_page_hash = <[u8; 32]>::try_from(&data[1..33]).unwrap();
                let base_page = self
                    .get_page_content_undecoded_snapshot(base_page_hash)
                    .await?;
                let base_page = match base_page {
                    Some(x) => self.decode_page_no_delta(x).await?,
                    None => anyhow::bail!("base page not found"),
                };
                let mut delta_data = block_in_place(|| {
                    zstd::bulk::decompress(&data_container.as_ref()[33..], MAX_PAGE_SIZE)
                })?;
                if delta_data.len() != base_page.len() {
                    anyhow::bail!("delta and base have different sizes");
                }

                for (i, b) in delta_data.iter_mut().enumerate() {
                    *b ^= base_page[i];
                }

                Ok(Bytes::from(delta_data))
            }
            _ => self.decode_page_no_delta(data_container).await,
        }
    }
}
