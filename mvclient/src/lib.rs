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
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::{OwnedRwLockReadGuard, OwnedSemaphorePermit, Semaphore};

#[derive(Debug, Error)]
pub enum CommitError {
    #[error("commit error: {0:?}")]
    Status(StatusCode),
}

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

    pub ns_key_hashproof: Option<String>,
}

#[derive(Deserialize)]
pub struct StatResponse {
    pub version: String,
    pub metadata: String,
    pub read_only: bool,
    pub interval: Option<Vec<u32>>,
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
pub struct CommitGlobalInit<'a> {
    #[serde(with = "serde_bytes")]
    pub idempotency_key: &'a [u8],

    pub num_namespaces: usize,
}

#[derive(Serialize)]
pub struct CommitNamespaceInit {
    pub ns_key: String,
    pub ns_key_hashproof: Option<String>,
    pub version: String,
    pub metadata: Option<String>,
    pub num_pages: u32,
    pub read_set: Option<HashSet<u32>>,
}

#[derive(Serialize)]
pub struct CommitRequest {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: Vec<u8>,
}

#[derive(Deserialize)]
pub struct CommitResponse {
    pub changelog: HashMap<String, Vec<u32>>,
}

pub struct CommitResult {
    pub version: String,
    pub duration: Duration,
    pub num_pages: u64,
    pub last_version: String,
    pub changelog: HashMap<String, Vec<u32>>,
}

pub enum CommitOutput {
    Committed(CommitResult),
    Conflict,
    Empty,
}

pub struct NamespaceCommitIntent {
    pub init: CommitNamespaceInit,
    pub requests: Vec<CommitRequest>,
}

impl MultiVersionClient {
    pub fn new(config: MultiVersionClientConfig, client: reqwest::Client) -> Result<Arc<Self>> {
        Ok(Arc::new(Self { client, config }))
    }

    pub fn config(&self) -> &MultiVersionClientConfig {
        &self.config
    }

    pub async fn create_transaction(self: &Arc<Self>) -> Result<Transaction> {
        self.create_transaction_with_info(None).await.map(|x| x.0)
    }

    pub async fn create_transaction_with_info(
        self: &Arc<Self>,
        from_version: Option<&str>,
    ) -> Result<(Transaction, TransactionInfo)> {
        let mut url = self.config.data_plane.clone();
        url.set_path("/stat");

        if let Some(from_version) = from_version {
            url.query_pairs_mut()
                .append_pair("from_version", from_version);
        }

        let mut boff = RandomizedExponentialBackoff::default();
        let stat_res: StatResponse = loop {
            let resp = request_and_check(self.client.get(url.clone()).decorate(self)).await?;
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
            self.create_transaction_at_version(&stat_res.version, stat_res.read_only),
            TransactionInfo {
                metadata: stat_res.metadata,
                interval: stat_res.interval,
            },
        ))
    }

    pub fn create_transaction_at_version(
        self: &Arc<Self>,
        version: &str,
        read_only: bool,
    ) -> Transaction {
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
            read_only,
            read_set: None,
        };

        txn
    }

    pub async fn apply_commit_intents(
        &self,
        intents: &[NamespaceCommitIntent],
    ) -> Result<Option<CommitResult>> {
        if intents.is_empty() {
            anyhow::bail!("no commit intents");
        }

        let start_time = Instant::now();
        let mut url = self.config.data_plane.clone();
        url.set_path("/batch/commit");
        let mut raw_request: Vec<u8> = Vec::new();

        let mut idempotency_key: [u8; 16] = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut idempotency_key);

        let global_init = CommitGlobalInit {
            idempotency_key: &idempotency_key[..],
            num_namespaces: intents.len(),
        };

        {
            let serialized = rmp_serde::to_vec_named(&global_init)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        let mut total_num_pages: usize = 0;

        for intent in intents {
            let serialized = rmp_serde::to_vec_named(&intent.init)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
            for req in &intent.requests {
                let serialized = rmp_serde::to_vec_named(&req)?;
                raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
                raw_request.extend_from_slice(&serialized);
            }
            total_num_pages += intent.requests.len();
        }

        let raw_request = Bytes::from(raw_request);
        let mut boff = RandomizedExponentialBackoff::default();

        loop {
            let response = request_and_check_returning_status(
                self.client.post(url.clone()).body(raw_request.clone()),
            )
            .await;
            let (headers, body) = match response {
                Ok(Some(x)) => x,
                Ok(None) => {
                    boff.wait().await;
                    continue;
                }
                Err(status) => {
                    if status.as_u16() == 409 {
                        return Ok(None);
                    }
                    return Err(CommitError::Status(status).into());
                }
            };
            let body: CommitResponse = rmp_serde::from_slice(&body)?;
            let committed_version = headers
                .get("x-committed-version")
                .with_context(|| format!("missing committed version header"))?
                .to_str()?;
            let last_version = headers
                .get("x-last-version")
                .with_context(|| format!("missing last version header"))?
                .to_str()?;
            tracing::debug!(version = committed_version, "committed transaction");
            return Ok(Some(CommitResult {
                version: committed_version.into(),
                duration: start_time.elapsed(),
                num_pages: total_num_pages as u64,
                last_version: last_version.into(),
                changelog: body.changelog,
            }));
        }
    }
}

pub struct Transaction {
    c: Arc<MultiVersionClient>,
    version: String,
    page_buffer: HashMap<u32, [u8; 32]>,
    async_ctx: Arc<TxnAsyncCtx>,
    seen_hashes: Mutex<HashSet<[u8; 32]>>,
    read_only: bool,
    read_set: Option<Mutex<HashSet<u32>>>,
}

pub struct TransactionInfo {
    pub metadata: String,
    pub interval: Option<Vec<u32>>,
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

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn page_is_written(&self, page_id: u32) -> bool {
        self.page_buffer.contains_key(&page_id)
    }

    fn check_async_error(&self) -> Result<()> {
        if self.async_ctx.has_error.load(Ordering::Relaxed) {
            Err(anyhow::anyhow!("async error"))
        } else {
            Ok(())
        }
    }

    pub fn enable_read_set(&mut self) {
        if self.read_set.is_none() {
            self.read_set = Some(Mutex::new(HashSet::new()));
        }
    }

    pub fn disable_read_set(&mut self) {
        self.read_set = None;
    }

    pub fn read_set_size(&self) -> usize {
        self.read_set
            .as_ref()
            .map(|x| x.lock().unwrap().len())
            .unwrap_or(0)
    }

    pub fn is_read_set_enabled(&self) -> bool {
        self.read_set.is_some()
    }

    pub fn mark_read(&self, page_id: u32) {
        if let Some(read_set) = &self.read_set {
            read_set.lock().unwrap().insert(page_id);
        }
    }

    pub async fn read_many_nomark(&self, page_id_list: &[u32]) -> Result<Vec<Vec<u8>>> {
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
            let response = request_and_check(
                self.c
                    .client
                    .post(url.clone())
                    .decorate(&self.c)
                    .body(raw_request.clone()),
            )
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
            let response = match request_and_check(
                c.client
                    .post(url.clone())
                    .decorate(&c)
                    .body(raw_request.clone()),
            )
            .await
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

    pub async fn commit_intent(
        &self,
        metadata: Option<String>,
    ) -> Result<Option<NamespaceCommitIntent>> {
        // wait for async completion
        self.async_ctx.background_completion.write().await;
        self.check_async_error()?;

        if self.page_buffer.is_empty() && metadata.is_none() {
            return Ok(None);
        }

        let mut out = NamespaceCommitIntent {
            init: CommitNamespaceInit {
                version: self.version.clone(),
                metadata,
                num_pages: self.page_buffer.len() as u32,
                ns_key: self.c.config.ns_key.clone(),
                ns_key_hashproof: self.c.config.ns_key_hashproof.clone(),
                read_set: self.read_set.as_ref().map(|x| x.lock().unwrap().clone()),
            },
            requests: Vec::with_capacity(self.page_buffer.len()),
        };

        for (&page_index, hash) in self.page_buffer.iter() {
            let req = CommitRequest {
                page_index,
                hash: hash.to_vec(),
            };
            out.requests.push(req);
        }

        Ok(Some(out))
    }

    pub async fn commit(self, metadata: Option<&str>) -> Result<CommitOutput> {
        let intent = match self.commit_intent(metadata.map(|x| x.to_string())).await? {
            Some(x) => x,
            None => return Ok(CommitOutput::Empty),
        };
        Ok(match self.c.apply_commit_intents(&[intent]).await? {
            Some(x) => CommitOutput::Committed(x),
            None => CommitOutput::Conflict,
        })
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

trait DecorateRequest {
    fn decorate(self, c: &MultiVersionClient) -> RequestBuilder;
}

impl DecorateRequest for RequestBuilder {
    fn decorate(self, c: &MultiVersionClient) -> RequestBuilder {
        let mut me = self.header(
            "x-namespace-key",
            HeaderValue::from_str(&c.config.ns_key).unwrap(),
        );

        if let Some(x) = &c.config.ns_key_hashproof {
            me = me.header("x-namespace-hashproof", HeaderValue::from_str(x).unwrap())
        }
        me
    }
}
