mod backoff;

use anyhow::{Context, Result};
use backoff::RandomizedExponentialBackoff;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use rand::RngCore;
use reqwest::{
    header::{HeaderMap, HeaderValue},
    RequestBuilder, StatusCode, Url,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore};

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

    pub delta_base: Option<u32>,
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

    #[serde(with = "serde_bytes")]
    pub idempotency_key: &'a [u8],
}

#[derive(Serialize)]
pub struct CommitRequest<'a> {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

pub struct CommitResult {
    pub version: String,
    pub duration: Duration,
    pub num_pages: u64,
}

impl MultiVersionClient {
    pub fn new(config: MultiVersionClientConfig) -> Result<Arc<Self>> {
        let mut headers = HeaderMap::new();
        headers.insert("x-namespace-key", HeaderValue::from_str(&config.ns_key)?);

        let client = reqwest::ClientBuilder::new()
            .http2_prior_knowledge()
            .default_headers(headers)
            .build()?;
        Ok(Arc::new(Self { client, config }))
    }

    pub fn config(&self) -> &MultiVersionClientConfig {
        &self.config
    }

    pub async fn create_transaction(self: &Arc<Self>) -> Result<Transaction> {
        self.create_transaction_with_metadata().await.map(|x| x.0)
    }

    pub async fn create_transaction_with_metadata(
        self: &Arc<Self>,
    ) -> Result<(Transaction, String)> {
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
        Ok((
            self.create_transaction_at_version(&stat_res.version),
            stat_res.metadata,
        ))
    }

    pub fn create_transaction_at_version(self: &Arc<Self>, version: &str) -> Transaction {
        let txn = Transaction {
            c: self.clone(),
            version: version.into(),
            page_buffer: HashMap::new(),
            async_ctx: Arc::new(TxnAsyncCtx {
                background_sem: Arc::new(Semaphore::new(32)),
                background_completion: Arc::new(tokio::sync::RwLock::new(())),
                has_error: AtomicBool::new(false),
            }),
            seen_hashes: Mutex::new(HashSet::new()),
            start_time: Instant::now(),
        };

        txn
    }
}

pub struct Transaction {
    c: Arc<MultiVersionClient>,
    version: String,
    page_buffer: HashMap<u32, [u8; 32]>,
    async_ctx: Arc<TxnAsyncCtx>,
    seen_hashes: Mutex<HashSet<[u8; 32]>>,
    start_time: Instant,
}

struct TxnAsyncCtx {
    background_sem: Arc<Semaphore>,
    background_completion: Arc<tokio::sync::RwLock<()>>,
    has_error: AtomicBool,
}

impl Transaction {
    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    fn check_async_error(&self) -> Result<()> {
        if self.async_ctx.has_error.load(Ordering::Relaxed) {
            Err(anyhow::anyhow!("async error"))
        } else {
            Ok(())
        }
    }
    pub async fn read_many(&self, page_id_list: &[u32]) -> Result<Vec<Vec<u8>>> {
        // wait for async completion
        self.async_ctx.background_completion.write().await;
        self.check_async_error()?;

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
                self.seen_hashes
                    .lock()
                    .unwrap()
                    .insert(*blake3::hash(&data.data).as_bytes());
            }

            if out.len() != page_id_list.len() {
                tracing::error!("response length mismatch, retrying");
                boff.wait().await;
                continue;
            }
            return Ok(out);
        }
    }

    async fn bg_write_task(
        c: Arc<MultiVersionClient>,
        _completion_guard: OwnedRwLockReadGuard<()>,
        _sem_permit: OwnedSemaphorePermit,
        async_ctx: Arc<TxnAsyncCtx>,
        raw_request: Bytes,
        num_pages: usize,
    ) {
        if async_ctx.has_error.load(Ordering::Relaxed) {
            return;
        }

        let mut url = c.config.data_plane.clone();
        url.set_path("/batch/write");

        let mut boff = RandomizedExponentialBackoff::default();
        loop {
            let response =
                match request_and_check(c.client.post(url.clone()).body(raw_request.clone())).await
                {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(error = %e, "background page write failed");
                        async_ctx.has_error.store(true, Ordering::Relaxed);
                        return;
                    }
                };
            let (_, raw_response) = match response {
                Some(x) => x,
                None => {
                    boff.wait().await;
                    continue;
                }
            };
            let mut raw_response = &raw_response[..];
            let mut counter: usize = 0;
            while !raw_response.is_empty() {
                let len = raw_response.read_u32::<BigEndian>().unwrap_or(0) as usize;
                let serialized = &raw_response[..len];
                raw_response = &raw_response[len..];
                let data: WriteResponse = match rmp_serde::from_slice(serialized) {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::error!(error = %e, "background page write could not decode server response");
                        async_ctx.has_error.store(true, Ordering::Relaxed);
                        return;
                    }
                };

                if data.hash.is_empty() {
                    assert_eq!(counter, num_pages);
                    return;
                }
                counter += 1;
            }
            tracing::error!("incomplete write, retrying");
            boff.wait().await;
        }
    }

    pub async fn write_many(&mut self, raw_pages: &[(u32, &[u8])]) -> Result<()> {
        let all_pages = raw_pages
            .iter()
            .map(|&(page_index, data)| {
                let hash = blake3::hash(data);
                (page_index, data, hash)
            })
            .collect::<Vec<_>>();
        let pages_to_push = all_pages
            .iter()
            .filter(|(_, _, hash)| !self.seen_hashes.lock().unwrap().contains(hash.as_bytes()))
            .collect::<Vec<_>>();
        let mut raw_request: Vec<u8> = Vec::new();
        for &(page_index, data, _) in &pages_to_push {
            let req = WriteRequest {
                data,
                delta_base: Some(*page_index),
            };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        // Finalization frame
        raw_request.write_u32::<BigEndian>(0)?;

        let raw_request = Bytes::from(raw_request);
        for (page_index, _, hash) in &all_pages {
            self.page_buffer.insert(*page_index, *hash.as_bytes());
            self.seen_hashes.lock().unwrap().insert(*hash.as_bytes());
        }
        let completion_guard = self
            .async_ctx
            .background_completion
            .clone()
            .read_owned()
            .await;
        let sem_permit = self
            .async_ctx
            .background_sem
            .clone()
            .acquire_owned()
            .await
            .unwrap();
        let async_ctx = self.async_ctx.clone();
        tokio::spawn(Self::bg_write_task(
            self.c.clone(),
            completion_guard,
            sem_permit,
            async_ctx,
            raw_request,
            pages_to_push.len(),
        ));
        Ok(())
    }

    pub async fn commit(self, metadata: Option<&str>) -> Result<Option<CommitResult>> {
        if self.page_buffer.is_empty() && metadata.is_none() {
            return Ok(Some(CommitResult {
                version: self.version,
                duration: self.start_time.elapsed(),
                num_pages: 0,
            }));
        }

        let mut idempotency_key: [u8; 16] = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut idempotency_key);

        // wait for async completion
        self.async_ctx.background_completion.write().await;
        self.check_async_error()?;

        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/commit");
        let mut raw_request: Vec<u8> = Vec::new();

        let init = CommitInit {
            version: self.version.as_str(),
            metadata,
            num_pages: self.page_buffer.len() as u32,
            idempotency_key: &idempotency_key[..],
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
                duration: self.start_time.elapsed(),
                num_pages: self.page_buffer.len() as u64,
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
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        tracing::error!(status = %status, text = text, "server error");
        Ok(None)
    } else {
        let status = res.status();
        let text = res.text().await.unwrap_or_default();
        tracing::warn!(status = %status, text = %text, "client error");
        Err(status)
    }
}
