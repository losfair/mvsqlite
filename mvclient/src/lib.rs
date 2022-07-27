mod backoff;

use anyhow::{Context, Result};
use backoff::RandomizedExponentialBackoff;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    RequestBuilder, StatusCode, Url,
};
use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

pub struct MultiVersionClient {
    client: reqwest::Client,
    config: MultiVersionClientConfig,
}

#[derive(Clone, Debug)]
pub struct MultiVersionClientConfig {
    /// Data plane URL.
    pub data_plane: Url,

    /// Namespace key.
    pub ns_key: String,
}

#[derive(Deserialize)]
pub struct StatResponse {
    pub version: String,
    pub metadata: String,
}

#[derive(Serialize)]
pub struct ReadRequest<'a> {
    pub page_index: u32,
    pub version: &'a str,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub hash: Option<&'a [u8]>,
}

#[derive(Deserialize)]
pub struct ReadResponse<'a> {
    pub version: &'a str,
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Serialize)]
pub struct WriteRequest<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Deserialize)]
pub struct WriteResponse<'a> {
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

#[derive(Serialize)]
pub struct CommitInit<'a> {
    pub version: &'a str,
    pub metadata: Option<&'a str>,
    pub num_pages: u32,
}

#[derive(Serialize)]
pub struct CommitRequest<'a> {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

pub struct CommitResult {
    pub version: String,
}

impl MultiVersionClient {
    pub fn new(config: MultiVersionClientConfig) -> Result<Arc<Self>> {
        let mut headers = HeaderMap::new();
        headers.insert("x-namespace-key", HeaderValue::from_str(&config.ns_key)?);

        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()?;
        Ok(Arc::new(Self { client, config }))
    }

    pub async fn create_transaction(self: &Arc<Self>) -> Result<Transaction> {
        let mut url = self.config.data_plane.clone();
        url.set_path("/stat");

        let mut boff = RandomizedExponentialBackoff::default();
        let stat_res: StatResponse = loop {
            let resp = request_and_check(self.client.get(url.clone())).await?;
            match resp {
                Some((_, body)) => break serde_json::from_slice(&body)?,
                None => {
                    boff.wait().await;
                    continue;
                }
            }
        };

        tracing::debug!(
            version = stat_res.version,
            metadata = stat_res.metadata,
            "created transaction"
        );
        Ok(self.create_transaction_at_version(&stat_res.version))
    }

    pub fn create_transaction_at_version(self: &Arc<Self>, version: &str) -> Transaction {
        let txn = Transaction {
            c: self.clone(),
            version: version.into(),
            page_buffer: HashMap::new(),
        };

        txn
    }
}

pub struct Transaction {
    c: Arc<MultiVersionClient>,
    version: String,
    page_buffer: HashMap<u32, [u8; 32]>,
}

impl Transaction {
    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    pub async fn read_many(&self, page_id_list: &[u32]) -> Result<Vec<Vec<u8>>> {
        let mut raw_request: Vec<u8> = Vec::new();

        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/read");

        for &page_index in page_id_list {
            let buffered = self.page_buffer.get(&page_index).map(|x| x.as_slice());
            let req = ReadRequest {
                page_index,
                version: self.version.as_str(),
                hash: buffered,
            };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }
        let raw_request = Bytes::from(raw_request);
        let mut boff = RandomizedExponentialBackoff::default();
        loop {
            let response =
                request_and_check(self.c.client.post(url.clone()).body(raw_request.clone()))
                    .await?;
            let (_, raw_response) = match response {
                Some(x) => x,
                None => {
                    boff.wait().await;
                    continue;
                }
            };
            let mut raw_response = &raw_response[..];
            let mut out: Vec<Vec<u8>> = Vec::with_capacity(page_id_list.len());
            while !raw_response.is_empty() {
                let len = raw_response.read_u32::<BigEndian>()? as usize;
                let serialized = &raw_response[..len];
                raw_response = &raw_response[len..];

                let data: ReadResponse = rmp_serde::from_slice(serialized)?;
                out.push(data.data.to_vec());
            }

            if out.len() != page_id_list.len() {
                tracing::error!("response length mismatch, retrying");
                boff.wait().await;
                continue;
            }
            return Ok(out);
        }
    }

    pub async fn write_many(&mut self, pages: &[(u32, &[u8])]) -> Result<Vec<[u8; 32]>> {
        let mut raw_request: Vec<u8> = Vec::new();
        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/write");
        for &(_, data) in pages {
            let req = WriteRequest { data };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        // Finalization frame
        raw_request.write_u32::<BigEndian>(0)?;

        let raw_request = Bytes::from(raw_request);
        let mut boff = RandomizedExponentialBackoff::default();
        loop {
            let response =
                request_and_check(self.c.client.post(url.clone()).body(raw_request.clone()))
                    .await?;
            let (_, raw_response) = match response {
                Some(x) => x,
                None => {
                    boff.wait().await;
                    continue;
                }
            };
            let mut raw_response = &raw_response[..];
            let mut out: Vec<[u8; 32]> = Vec::with_capacity(pages.len());
            while !raw_response.is_empty() {
                let len = raw_response.read_u32::<BigEndian>()? as usize;
                let serialized = &raw_response[..len];
                raw_response = &raw_response[len..];
                let data: WriteResponse = rmp_serde::from_slice(serialized)?;

                if data.hash.is_empty() {
                    if out.len() != pages.len() {
                        anyhow::bail!("got unexpected completion");
                    }
                    for ((page_index, _), out) in pages.iter().zip(out.iter()) {
                        self.page_buffer.insert(*page_index, *out);
                    }
                    return Ok(out);
                }
                let hash = <[u8; 32]>::try_from(&data.hash[..])?;
                out.push(hash);
            }
            tracing::error!("incomplete write, retrying");
            boff.wait().await;
        }
    }

    pub async fn commit(self) -> Result<Option<CommitResult>> {
        // XXX: Check metadata update when it get implemented.
        if self.page_buffer.is_empty() {
            return Ok(Some(CommitResult {
                version: self.version,
            }));
        }

        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/commit");
        let mut raw_request: Vec<u8> = Vec::new();

        let init = CommitInit {
            version: self.version.as_str(),
            metadata: None,
            num_pages: self.page_buffer.len() as u32,
        };

        {
            let serialized = rmp_serde::to_vec_named(&init)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        for (&page_index, hash) in self.page_buffer.iter() {
            let req = CommitRequest { page_index, hash };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        if raw_request.len() > 8 * 1024 * 1024 {
            anyhow::bail!("transaction too large");
        }

        let raw_request = Bytes::from(raw_request);
        let mut boff = RandomizedExponentialBackoff::default();

        loop {
            let response = request_and_check_returning_status(
                self.c.client.post(url.clone()).body(raw_request.clone()),
            )
            .await;
            let (headers, _) = match response {
                Ok(Some(x)) => x,
                Ok(None) => {
                    boff.wait().await;
                    continue;
                }
                Err(status) => {
                    if status.as_u16() == 409 {
                        return Ok(None);
                    }
                    anyhow::bail!("commit failed: {}", status);
                }
            };
            let committed_version = headers
                .get("x-committed-version")
                .with_context(|| format!("missing committed version header"))?
                .to_str()?;
            tracing::debug!(version = committed_version, "committed transaction");
            return Ok(Some(CommitResult {
                version: committed_version.into(),
            }));
        }
    }
}

async fn request_and_check(r: RequestBuilder) -> Result<Option<(HeaderMap, Bytes)>> {
    request_and_check_returning_status(r)
        .await
        .map_err(|e| anyhow::anyhow!("status {}", e))
}

async fn request_and_check_returning_status(
    r: RequestBuilder,
) -> Result<Option<(HeaderMap, Bytes)>, StatusCode> {
    let res = match r.send().await {
        Ok(x) => x,
        Err(e) => {
            tracing::error!(error = %e, "network error");
            return Ok(None);
        }
    };
    if res.status().is_success() {
        let headers = res.headers().clone();
        let body = match res.bytes().await {
            Ok(x) => x,
            Err(e) => {
                tracing::error!(error = %e, "failed to read response body");
                return Ok(None);
            }
        };
        Ok(Some((headers, body)))
    } else if res.status().is_server_error() {
        tracing::error!(status = %res.status(), "server error");
        Ok(None)
    } else {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        tracing::warn!(status = %status, text = %text, "client error");
        Err(status)
    }
}
