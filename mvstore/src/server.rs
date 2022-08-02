use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use foundationdb::{
    future::FdbSlice,
    options::{MutationType, StreamingMode},
    tuple::pack,
    Database, RangeOption, Transaction,
};
use futures::StreamExt;
use hyper::{body::HttpBody, Body, Request, Response};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::Infallible, sync::Arc, time::Duration};
use tokio::io::AsyncRead;
use tokio_util::{
    codec::{Decoder, FramedRead, LengthDelimitedCodec},
    io::StreamReader,
};

const MAX_MESSAGE_SIZE: usize = 10 * 1024; // 10 KiB
const COMMIT_MESSAGE_SIZE: usize = 9 * 1024 * 1024; // 9 MiB
const MAX_PAGE_SIZE: usize = 8192;
const COMMITTED_VERSION_HDR_NAME: &str = "x-committed-version";

pub struct Server {
    db: Database,
    raw_data_prefix: Vec<u8>,
    metadata_prefix: String,

    nskey_cache: Cache<String, [u8; 10]>,
    read_version_cache: Cache<[u8; 10], i64>,
}

pub struct ServerConfig {
    pub cluster: String,
    pub raw_data_prefix: String,
    pub metadata_prefix: String,
}

#[derive(Serialize)]
pub struct StatResponse<'a> {
    pub version: String,
    pub metadata: &'a str,
}

#[derive(Deserialize)]
pub struct ReadRequest<'a> {
    pub page_index: u32,
    pub version: &'a str,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub hash: Option<&'a [u8]>,
}

#[derive(Serialize)]
pub struct ReadResponse<'a> {
    pub version: &'a str,
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
}

#[derive(Deserialize)]
pub struct WriteRequest<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],

    pub delta_base: Option<u32>,
}

#[derive(Serialize)]
pub struct WriteResponse<'a> {
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

#[derive(Deserialize)]
pub struct CommitInit<'a> {
    pub version: &'a str,
    pub metadata: Option<&'a str>,
    pub num_pages: u32,

    #[serde(with = "serde_bytes")]
    pub idempotency_key: &'a [u8],
}

#[derive(Deserialize)]
pub struct CommitRequest<'a> {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

#[derive(Deserialize)]
pub struct AdminCreateNamespaceRequest {
    pub key: String,
    pub metadata: String,
}

#[derive(Deserialize)]
pub struct AdminDeleteNamespaceRequest {
    pub key: String,
}

pub struct Page {
    pub version: String,
    pub data: FdbSlice,
}

#[derive(Default)]
pub struct DecodedPage {
    pub data: Vec<u8>,
}

const PAGE_ENCODING_NONE: u8 = 0;
const PAGE_ENCODING_ZSTD: u8 = 1;
const PAGE_ENCODING_DELTA: u8 = 2;

impl Page {
    fn compress_zstd(data: &[u8]) -> Vec<u8> {
        let max_compressed_size = zstd::zstd_safe::compress_bound(data.len());
        let mut buf = vec![0u8; max_compressed_size + 1];
        buf[0] = PAGE_ENCODING_ZSTD;
        let compressed_size = zstd::bulk::compress_to_buffer(data, &mut buf[1..], 0)
            .expect("compress_to_buffer failed");
        buf.truncate(compressed_size + 1);
        buf
    }
}

impl Server {
    pub fn open(config: ServerConfig) -> Result<Arc<Self>> {
        let db = Database::new(Some(config.cluster.as_str()))
            .with_context(|| "cannot open fdb cluster")?;
        let raw_data_prefix = config.raw_data_prefix.as_bytes().to_vec();
        Ok(Arc::new(Self {
            db,
            raw_data_prefix,
            metadata_prefix: config.metadata_prefix,
            nskey_cache: Cache::builder()
                .time_to_live(Duration::from_secs(120))
                .time_to_idle(Duration::from_secs(5))
                .max_capacity(10000)
                .build(),
            // FDB read versions are valid for 5 seconds.
            // We conservatively cache them for only 2 seconds here. If for some reason
            // these versions still lived too long, FDB will error and the client will retry.
            read_version_cache: Cache::builder()
                .time_to_live(Duration::from_secs(2))
                .max_capacity(1000)
                .build(),
        }))
    }

    pub async fn serve_admin_api(
        self: Arc<Self>,
        req: Request<Body>,
    ) -> Result<Response<Body>, Infallible> {
        match self.do_serve_admin_api(req).await {
            Ok(res) => Ok(res),
            Err(e) => {
                tracing::error!(error = %e, "admin api failure");
                Ok(Response::builder()
                    .status(500)
                    .body(Body::from(format!("{}", e)))
                    .unwrap())
            }
        }
    }

    async fn do_serve_admin_api(self: Arc<Self>, mut req: Request<Body>) -> Result<Response<Body>> {
        let uri = req.uri();
        let res: Response<Body>;
        match uri.path() {
            "/api/create_namespace" => {
                let body = hyper::body::to_bytes(req.body_mut()).await?;
                let body: AdminCreateNamespaceRequest = serde_json::from_slice(&body)?;
                let nskey_key = self.construct_nskey_key(&body.key);

                let mut txn = self.db.create_trx()?;

                loop {
                    if txn.get(&nskey_key, false).await?.is_some() {
                        return Ok(Response::builder()
                            .status(400)
                            .body(Body::from("this key already exists"))?);
                    }

                    let nsmd_atomic_op_key =
                        generate_suffix_versionstamp_atomic_op(&self.construct_nsmd_key([0u8; 10]));
                    let nskey_atomic_op_value = [0u8; 14];
                    txn.atomic_op(
                        &nsmd_atomic_op_key,
                        body.metadata.as_bytes(),
                        MutationType::SetVersionstampedKey,
                    );
                    txn.atomic_op(
                        &nskey_key,
                        &nskey_atomic_op_value,
                        MutationType::SetVersionstampedValue,
                    );
                    match txn.commit().await {
                        Ok(_) => {
                            res = Response::builder().status(200).body(Body::from("ok"))?;
                            break;
                        }
                        Err(e) => {
                            txn = match e.on_error().await {
                                Ok(x) => x,
                                Err(e) => {
                                    return Ok(Response::builder()
                                        .status(400)
                                        .body(Body::from(format!("{}", e)))?)
                                }
                            };
                        }
                    }
                }
            }

            "/api/delete_namespace" => {
                let body = hyper::body::to_bytes(req.body_mut()).await?;
                let body: AdminDeleteNamespaceRequest = serde_json::from_slice(&body)?;
                let nskey_key = self.construct_nskey_key(&body.key);

                let mut txn = self.db.create_trx()?;

                loop {
                    let ns_id = match txn.get(&nskey_key, false).await? {
                        Some(v) => v,
                        None => {
                            return Ok(Response::builder()
                                .status(400)
                                .body(Body::from("this key does not exist"))?);
                        }
                    };
                    let ns_id =
                        <[u8; 10]>::try_from(&ns_id[..]).with_context(|| "cannot parse ns_id")?;
                    let ns_data_start = self.construct_ns_data_prefix(ns_id);
                    let mut ns_data_end = ns_data_start.clone();
                    ns_data_end.push(0xff);

                    txn.clear_range(&ns_data_start, &ns_data_end);
                    txn.clear(&nskey_key);

                    let nsmd_key = self.construct_nsmd_key(ns_id);
                    txn.clear(&nsmd_key);
                    match txn.commit().await {
                        Ok(_) => {
                            res = Response::builder().status(200).body(Body::from("ok"))?;
                            break;
                        }
                        Err(e) => {
                            txn = match e.on_error().await {
                                Ok(x) => x,
                                Err(e) => {
                                    return Ok(Response::builder()
                                        .status(400)
                                        .body(Body::from(format!("{}", e)))?)
                                }
                            };
                        }
                    }
                }
            }
            _ => {
                res = Response::builder().status(404).body(Body::empty())?;
            }
        }
        Ok(res)
    }

    pub async fn serve_data_plane(
        self: Arc<Self>,
        req: Request<Body>,
    ) -> Result<Response<Body>, Infallible> {
        match self.do_serve_data_plane_stage1(req).await {
            Ok(res) => Ok(res),
            Err(e) => {
                tracing::warn!(error = %e, "stage 1 failure");
                Ok(Response::builder().status(500).body(Body::empty()).unwrap())
            }
        }
    }

    async fn lookup_nskey(&self, nskey: &str) -> Result<Option<[u8; 10]>> {
        enum GetError {
            NotFound,
            Other(anyhow::Error),
        }
        let res = self
            .nskey_cache
            .try_get_with(nskey.to_string(), async {
                let txn = self.db.create_trx();
                match txn {
                    Ok(txn) => match txn.get(&self.construct_nskey_key(nskey), true).await {
                        Ok(Some(x)) => <[u8; 10]>::try_from(&x[..])
                            .with_context(|| "invalid namespace id")
                            .map_err(GetError::Other),
                        Ok(None) => Err(GetError::NotFound),
                        Err(e) => Err(GetError::Other(
                            anyhow::Error::from(e).context("transaction failed"),
                        )),
                    },
                    Err(e) => Err(GetError::Other(
                        anyhow::Error::from(e).context("transaction creation failed"),
                    )),
                }
            })
            .await;
        match res.as_ref().map_err(|e| &**e) {
            Ok(x) => Ok(Some(*x)),
            Err(GetError::NotFound) => Ok(None),
            Err(GetError::Other(x)) => Err(anyhow::anyhow!("nskey lookup failed: {}", x)),
        }
    }

    async fn do_serve_data_plane_stage1(
        self: Arc<Self>,
        req: Request<Body>,
    ) -> Result<Response<Body>> {
        let ns_key = match req
            .headers()
            .get("x-namespace-key")
            .and_then(|x| x.to_str().ok())
        {
            Some(x) => x,
            None => {
                return Ok(Response::builder()
                    .status(400)
                    .body(Body::from("missing or invalid x-namespace-key"))
                    .unwrap())
            }
        };
        let ns_id = self.lookup_nskey(ns_key).await?;
        let ns_id = match ns_id {
            Some(x) => x,
            None => {
                return Ok(Response::builder()
                    .status(404)
                    .body(Body::from("namespace not found"))?)
            }
        };
        match self.do_serve_data_plane_stage2(ns_id, req).await {
            Ok(res) => Ok(res),
            Err(e) => {
                let ns_id_hex = hex::encode(&ns_id);
                tracing::warn!(ns = ns_id_hex, error = %e, "stage 2 failure");
                Ok(Response::builder().status(500).body(Body::empty())?)
            }
        }
    }

    async fn get_read_version_as_versionstamp(&self, txn: &Transaction) -> Result<[u8; 10]> {
        let read_version = txn.get_read_version().await? as u64;
        let mut buf = [0u8; 10];
        buf[0..8].copy_from_slice(&read_version.to_be_bytes());

        // Now we can observe all changes with `committed_version == read_version`.
        buf[8] = 255;
        buf[9] = 255;
        Ok(buf)
    }

    async fn create_versioned_read_txn(&self, version: &str) -> Result<Transaction> {
        let version = decode_version(version)?;
        let txn = self.db.create_trx()?;
        let mut grv_called = false;
        let fdb_rv = self
            .read_version_cache
            .try_get_with(version, async {
                grv_called = true;
                txn.get_read_version().await
            })
            .await
            .with_context(|| "cannot get read version")?;
        if !grv_called {
            txn.set_read_version(fdb_rv);
        }
        Ok(txn)
    }

    async fn do_serve_data_plane_stage2(
        self: Arc<Self>,
        ns_id: [u8; 10],
        mut req: Request<Body>,
    ) -> Result<Response<Body>> {
        let ns_id_hex = hex::encode(&ns_id);
        let uri = req.uri();
        let res: Response<Body>;
        match uri.path() {
            "/stat" => {
                let txn = self.db.create_trx()?;
                let buf = self.get_read_version_as_versionstamp(&txn).await?;
                let nsmd = txn.get(&self.construct_nsmd_key(ns_id), true).await?;
                let nsmd = nsmd
                    .as_ref()
                    .map(|x| std::str::from_utf8(&x[..]).unwrap_or_default())
                    .unwrap_or_default();

                let stat = StatResponse {
                    version: hex::encode(&buf),
                    metadata: nsmd,
                };
                let stat =
                    serde_json::to_vec(&stat).with_context(|| "cannot serialize stat response")?;

                res = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(stat))?;
            }
            "/read" => {
                let query: HashMap<String, String> = uri
                    .query()
                    .map(|v| {
                        url::form_urlencoded::parse(v.as_bytes())
                            .into_owned()
                            .collect()
                    })
                    .unwrap_or_else(HashMap::new);
                let page_index = query
                    .get("page_index")
                    .with_context(|| "missing page_index")?
                    .parse::<u32>()
                    .with_context(|| "invalid page_index")?;
                let page_version_hex = query
                    .get("page_version")
                    .with_context(|| "missing page_version")?;
                let txn = self.create_versioned_read_txn(page_version_hex).await?;
                let page = self
                    .read_page(&txn, ns_id, page_index, &page_version_hex)
                    .await?;
                match page {
                    Some(page) => {
                        let decoded = self.decode_page(&txn, ns_id, &page.data).await?;
                        res = Response::builder()
                            .header("x-page-version", page.version)
                            .body(Body::from(decoded.data))?;
                    }
                    None => {
                        res = Response::builder().body(Body::empty())?;
                    }
                }
            }
            "/batch/read" => {
                let body = req.into_body();
                let mut body = new_body_reader(body);
                let (mut res_sender, res_body) = Body::channel();
                res = Response::builder().body(res_body)?;
                let me = self.clone();
                tokio::spawn(async move {
                    loop {
                        let message = match body.next().await {
                            Some(Ok(x)) => x,
                            Some(Err(e)) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "client disconnected with error");
                                break;
                            }
                            None => break,
                        };
                        let read_req: ReadRequest = match rmp_serde::from_slice(&message) {
                            Ok(x) => x,
                            Err(e) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "invalid message");
                                break;
                            }
                        };
                        let txn: Transaction;
                        let page = if let Some(hash) = read_req.hash {
                            // This path enables read-your-writes in the same transaction. We cannot use the read-version cache,
                            // because the snapshotted version may not contain newly written pages.
                            //
                            // This is a rare case anyway, because the client has its own read cache.
                            tracing::debug!(
                                ns = ns_id_hex,
                                hash = hex::encode(hash),
                                "entering read-your-writes logic"
                            );
                            txn = match self.db.create_trx() {
                                Ok(x) => x,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, "failed to create transaction");
                                    break;
                                }
                            };
                            let hash = match <[u8; 32]>::try_from(hash) {
                                Ok(x) => x,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, "hash is not 32 bytes");
                                    break;
                                }
                            };
                            let content_key = self.construct_content_key(ns_id, hash);
                            match txn.get(&content_key, true).await {
                                Ok(Some(x)) => Some(Page {
                                    version: read_req.version.to_string(),
                                    data: x,
                                }),
                                Ok(None) => None,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, "failed to get content by hash");
                                    break;
                                }
                            }
                        } else {
                            txn = match self.create_versioned_read_txn(read_req.version).await {
                                Ok(x) => x,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, "failed to create versioned read txn");
                                    break;
                                }
                            };
                            match me
                                .read_page(&txn, ns_id, read_req.page_index, read_req.version)
                                .await
                            {
                                Ok(x) => x,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, page_index = read_req.page_index, version = read_req.version, "error reading page");
                                    break;
                                }
                            }
                        };
                        let payload = match &page {
                            Some(x) => ReadResponse {
                                version: x.version.as_str(),
                                data: match self.decode_page(&txn, ns_id, &x.data).await {
                                    Ok(x) => x.data,
                                    Err(e) => {
                                        tracing::warn!(ns = ns_id_hex, error = %e, "error decoding page");
                                        break;
                                    }
                                },
                            },
                            None => ReadResponse {
                                version: "",
                                data: Vec::new(),
                            },
                        };
                        let payload = match rmp_serde::to_vec_named(&payload) {
                            Ok(x) => x,
                            Err(e) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "error serializing response");
                                break;
                            }
                        };
                        if let Err(e) = res_sender
                            .send_data(Bytes::from(prepend_length(&payload)))
                            .await
                        {
                            tracing::warn!(ns = ns_id_hex, error = %e, "error sending response");
                            break;
                        }
                    }
                });
            }
            "/batch/write" => {
                let body = req.into_body();
                let mut body = new_body_reader(body);
                let (mut res_sender, res_body) = Body::channel();
                res = Response::builder().body(res_body)?;
                let me = self.clone();
                let txn = self.db.create_trx()?;
                tokio::spawn(async move {
                    loop {
                        let message = match body.next().await {
                            Some(Ok(x)) => x,
                            Some(Err(e)) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "client disconnected with error");
                                break;
                            }
                            None => {
                                tracing::warn!(
                                    ns = ns_id_hex,
                                    "client disconnected before completion"
                                );
                                break;
                            }
                        };
                        if message.len() == 0 {
                            // completion
                            if let Err(e) = txn.commit().await {
                                tracing::warn!(ns = ns_id_hex, error = %e, "error committing transaction");
                                break;
                            }
                            let payload = match rmp_serde::to_vec_named(&WriteResponse {
                                hash: b"",
                            }) {
                                Ok(x) => x,
                                Err(e) => {
                                    tracing::warn!(ns = ns_id_hex, error = %e, "error serializing completion");
                                    break;
                                }
                            };
                            if let Err(e) = res_sender
                                .send_data(Bytes::from(prepend_length(&payload)))
                                .await
                            {
                                tracing::warn!(ns = ns_id_hex, error = %e, "error sending completion");
                            }
                            break;
                        }

                        let write_req: WriteRequest = match rmp_serde::from_slice(&message) {
                            Ok(x) => x,
                            Err(e) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "invalid message");
                                break;
                            }
                        };
                        if write_req.data.len() > MAX_PAGE_SIZE {
                            tracing::warn!(
                                ns = ns_id_hex,
                                len = write_req.data.len(),
                                limit = MAX_PAGE_SIZE,
                                "page is too large"
                            );
                            break;
                        }
                        let hash = blake3::hash(write_req.data);
                        let content_key = me.construct_content_key(ns_id, *hash.as_bytes());

                        let mut early_completion = false;

                        // This is not only an optimization. Without doing this check it is possible to form
                        // loops in delta page construction.
                        match txn.get(&content_key, true).await {
                            Ok(x) => {
                                if x.is_some() {
                                    early_completion = true;
                                }
                            }
                            Err(e) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "error getting content");
                                break;
                            }
                        }

                        // Attempt delta-encoding
                        if !early_completion {
                            if let Some(delta_base_index) = write_req.delta_base {
                                match me
                                    .delta_encode(&txn, ns_id, delta_base_index, &write_req.data)
                                    .await
                                {
                                    Ok(x) => {
                                        if let Some((x, delta_base_hash)) = x {
                                            let delta_referrer_key = self
                                                .construct_delta_referrer_key(
                                                    ns_id,
                                                    *hash.as_bytes(),
                                                    delta_base_hash,
                                                );
                                            txn.set(&content_key, &x);
                                            txn.set(&delta_referrer_key, b"");
                                            early_completion = true;
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!(
                                            ns = ns_id_hex,
                                            error = %e,
                                            "delta encoding failed"
                                        );
                                        break;
                                    }
                                }
                            }
                        }

                        // Finally...
                        if !early_completion {
                            txn.set(&content_key, &Page::compress_zstd(write_req.data));
                        }

                        let payload = match rmp_serde::to_vec_named(&WriteResponse {
                            hash: hash.as_bytes(),
                        }) {
                            Ok(x) => x,
                            Err(e) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "error serializing response");
                                break;
                            }
                        };
                        if let Err(e) = res_sender
                            .send_data(Bytes::from(prepend_length(&payload)))
                            .await
                        {
                            tracing::warn!(ns = ns_id_hex, error = %e, "error sending response");
                            break;
                        }
                    }
                });
            }
            "/batch/commit" => {
                let commit_message = self.check_and_read_commit_message(req.body_mut()).await?;
                let mut reader = FramedRead::new(
                    &commit_message[..],
                    LengthDelimitedCodec::builder()
                        .max_frame_length(MAX_MESSAGE_SIZE)
                        .new_codec(),
                );

                let commit_init = reader
                    .next()
                    .await
                    .with_context(|| "missing commit init")?
                    .with_context(|| "invalid commit init")?;
                let commit_init: CommitInit = rmp_serde::from_slice(&commit_init)
                    .with_context(|| "error deserializing commit init")?;

                let client_assumed_version = decode_version(&commit_init.version)?;
                let idempotency_key = <[u8; 16]>::try_from(commit_init.idempotency_key)
                    .with_context(|| "invalid idempotency key")?;

                // Ensure that we finish the commit as quickly as possible - there's the five-second limit here.
                let mut txn = self.db.create_trx()?;
                loop {
                    // Check version
                    let last_write_version_key = self.construct_last_write_version_key(ns_id);
                    let last_write_version = txn.get(&last_write_version_key, false).await?;
                    match &last_write_version {
                        Some(x) if x.len() != 16 + 10 => {
                            anyhow::bail!("invalid last write version");
                        }
                        Some(x) => {
                            let actual_idempotency_key = <[u8; 16]>::try_from(&x[0..16]).unwrap();
                            let actual_last_write_version =
                                <[u8; 10]>::try_from(&x[16..26]).unwrap();

                            if actual_idempotency_key == idempotency_key {
                                return Ok(Response::builder()
                                    .header(
                                        COMMITTED_VERSION_HDR_NAME,
                                        hex::encode(&actual_last_write_version),
                                    )
                                    .body(Body::empty())?);
                            }

                            if client_assumed_version < actual_last_write_version {
                                return Ok(Response::builder().status(409).body(Body::empty())?);
                            }
                        }
                        None => {}
                    }

                    // Write metadata
                    if let Some(md) = commit_init.metadata {
                        let metadata_key = self.construct_nsmd_key(ns_id);
                        txn.set(&metadata_key, md.as_bytes());
                    }
                    for _ in 0..commit_init.num_pages {
                        let message = match reader.next().await {
                            Some(x) => x.with_context(|| "error reading commit request")?,
                            None => anyhow::bail!("early end of commit stream"),
                        };
                        let commit_req: CommitRequest = rmp_serde::from_slice(&message)
                            .with_context(|| "error deserializing commit request")?;
                        if commit_req.hash.len() != 32 {
                            return Ok(Response::builder()
                                .status(400)
                                .body(Body::from("invalid hash"))?);
                        }

                        let page_key_template =
                            self.construct_page_key(ns_id, commit_req.page_index, [0u8; 10]);
                        let page_key_atomic_op =
                            generate_suffix_versionstamp_atomic_op(&page_key_template);
                        txn.atomic_op(
                            &page_key_atomic_op,
                            commit_req.hash,
                            MutationType::SetVersionstampedKey,
                        );
                    }
                    let mut last_write_version_atomic_op_value = [0u8; 16 + 10 + 4];
                    last_write_version_atomic_op_value[0..16].copy_from_slice(&idempotency_key);
                    last_write_version_atomic_op_value[26..30]
                        .copy_from_slice(&16u32.to_le_bytes()[..]);
                    txn.atomic_op(
                        &last_write_version_key,
                        &last_write_version_atomic_op_value,
                        MutationType::SetVersionstampedValue,
                    );
                    let versionstamp_fut = txn.get_versionstamp();
                    match txn.commit().await {
                        Ok(_) => {
                            let versionstamp = versionstamp_fut.await?;
                            let versionstamp = hex::encode(&versionstamp);
                            res = Response::builder()
                                .header(COMMITTED_VERSION_HDR_NAME, versionstamp)
                                .body(Body::empty())?;
                            break;
                        }
                        Err(e) => {
                            txn = match e.on_error().await {
                                Ok(x) => x,
                                Err(e) => {
                                    return Ok(Response::builder()
                                        .status(400)
                                        .body(Body::from(format!("{}", e)))?)
                                }
                            };
                        }
                    }
                }
            }
            _ => {
                res = Response::builder().status(404).body("not found".into())?;
            }
        }

        Ok(res)
    }

    async fn check_and_read_commit_message(&self, body: &mut Body) -> Result<Bytes> {
        let response_content_length = match HttpBody::size_hint(body).upper() {
            Some(v) => v,
            None => (COMMIT_MESSAGE_SIZE as u64) + 1,
        };

        if response_content_length <= COMMIT_MESSAGE_SIZE as u64 {
            let body_bytes = hyper::body::to_bytes(body)
                .await
                .with_context(|| "failed to read commit message")?;
            Ok(body_bytes)
        } else {
            anyhow::bail!("commit message too large");
        }
    }

    async fn read_page_hash(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        page_version_hex: &str,
    ) -> Result<Option<(String, [u8; 32])>> {
        let page_version = decode_version(&page_version_hex)?;
        let scan_end = self.construct_page_key(ns_id, page_index, page_version);
        let scan_start = self.construct_page_key(ns_id, page_index, [0u8; 10]);
        let page_vec = txn
            .get_range(
                &RangeOption {
                    limit: Some(1),
                    reverse: true,
                    mode: StreamingMode::Small,
                    ..RangeOption::from(scan_start..=scan_end)
                },
                0,
                true,
            )
            .await?;
        assert!(page_vec.len() <= 1);
        if page_vec.is_empty() {
            Ok(None)
        } else {
            let page = page_vec.into_iter().next().unwrap();
            let key = page.key();
            let version = hex::encode(&key[key.len() - 10..]);
            let hash = page.value();
            let hash = <[u8; 32]>::try_from(hash).with_context(|| "invalid content hash")?;
            Ok(Some((version, hash)))
        }
    }

    async fn read_page(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        block_version_hex: &str,
    ) -> Result<Option<Page>> {
        let (version, hash) = match self
            .read_page_hash(txn, ns_id, page_index, block_version_hex)
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let content_key = self.construct_content_key(ns_id, hash);
        let content = txn
            .get(&content_key, true)
            .await?
            .with_context(|| "cannot find content for the provided hash")?;
        Ok(Some(Page {
            version,
            data: content,
        }))
    }

    fn construct_nsmd_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nsmd"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    fn construct_nskey_key(&self, ns_key: &str) -> Vec<u8> {
        pack(&(self.metadata_prefix.as_str(), "nskey", ns_key))
    }

    fn construct_last_write_version_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len() + 1);
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'v');
        buf
    }

    fn construct_ns_data_prefix(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len());
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf
    }

    fn construct_page_key(
        &self,
        ns_id: [u8; 10],
        page_index: u32,
        block_version: [u8; 10],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(
            self.raw_data_prefix.len()
                + ns_id.len()
                + 1
                + std::mem::size_of::<u32>()
                + block_version.len(),
        );
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'p');
        buf.extend_from_slice(&page_index.to_be_bytes());
        buf.extend_from_slice(&block_version);
        buf
    }

    fn construct_content_key(&self, ns_id: [u8; 10], hash: [u8; 32]) -> Vec<u8> {
        let mut buf: Vec<u8> =
            Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len() + 1 + hash.len());
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'c');
        buf.extend_from_slice(&hash);
        buf
    }

    fn construct_delta_referrer_key(
        &self,
        ns_id: [u8; 10],
        from_hash: [u8; 32],
        to_hash: [u8; 32],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(
            self.raw_data_prefix.len() + ns_id.len() + 1 + to_hash.len() + from_hash.len(),
        );
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'r');
        buf.extend_from_slice(&to_hash);
        buf.extend_from_slice(&from_hash);
        buf
    }

    async fn delta_encode(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        base_page_index: u32,
        this_page: &[u8],
    ) -> Result<Option<(Vec<u8>, [u8; 32])>> {
        let version_hex = hex::encode(&self.get_read_version_as_versionstamp(&txn).await?);

        let (_, delta_base_hash) = match self
            .read_page_hash(&txn, ns_id, base_page_index, &version_hex)
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let (undecoded_base, delta_base_hash) = {
            let base_page_key = self.construct_content_key(ns_id, delta_base_hash);
            let base = match txn.get(&base_page_key, true).await? {
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
                let flattened_base_key = self.construct_content_key(ns_id, flattened_base_hash);
                let flattened_base = match txn.get(&flattened_base_key, true).await? {
                    Some(x) => x,
                    None => return Ok(None),
                };
                tracing::debug!(
                    from = hex::encode(&delta_base_hash),
                    to = hex::encode(&flattened_base_hash),
                    "flattened delta page"
                );
                (flattened_base, flattened_base_hash)
            } else {
                (base, delta_base_hash)
            }
        };
        let base_page = self.decode_page_no_delta(&undecoded_base)?;
        if base_page.data.len() != this_page.len() || this_page.is_empty() {
            return Ok(None);
        }

        let num_diff_bytes = base_page
            .data
            .iter()
            .zip(this_page.iter())
            .filter(|(b, t)| b != t)
            .count();
        if num_diff_bytes >= this_page.len() / 5 {
            return Ok(None);
        }

        let xor_image = base_page
            .data
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
            ns = hex::encode(&ns_id),
            base = hex::encode(&delta_base_hash),
            "delta encoded"
        );
        Ok(Some((output, delta_base_hash)))
    }

    fn decode_page_no_delta(&self, data: &[u8]) -> Result<DecodedPage> {
        if data.len() == 0 {
            return Ok(DecodedPage::default());
        }

        let encode_type = data[0];
        match encode_type {
            PAGE_ENCODING_NONE => {
                // not compressed
                Ok(DecodedPage {
                    data: data[1..].to_vec(),
                })
            }
            PAGE_ENCODING_ZSTD => {
                // zstd
                let data = zstd::bulk::decompress(&data[1..], MAX_PAGE_SIZE)
                    .with_context(|| "zstd decompress failed")?;
                Ok(DecodedPage { data })
            }
            _ => Err(anyhow::anyhow!(
                "decode_page_no_delta: unknown page encoding: {}",
                encode_type
            )),
        }
    }

    async fn decode_page(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        data: &[u8],
    ) -> Result<DecodedPage> {
        if data.len() == 0 {
            return Ok(DecodedPage::default());
        }

        let encode_type = data[0];
        match encode_type {
            PAGE_ENCODING_DELTA => {
                if data.len() < 33 {
                    anyhow::bail!("invalid delta encoding");
                }
                let base_page_hash = <[u8; 32]>::try_from(&data[1..33]).unwrap();
                let base_page_key = self.construct_content_key(ns_id, base_page_hash);
                let base_page = match txn.get(&base_page_key, true).await? {
                    Some(x) => x,
                    None => anyhow::bail!("base page not found"),
                };
                let base_page = self.decode_page_no_delta(&base_page)?;
                let mut delta_data = zstd::bulk::decompress(&data[33..], MAX_PAGE_SIZE)?;
                if delta_data.len() != base_page.data.len() {
                    anyhow::bail!("delta and base have different sizes");
                }

                for (i, b) in delta_data.iter_mut().enumerate() {
                    *b ^= base_page.data[i];
                }

                Ok(DecodedPage { data: delta_data })
            }
            _ => self.decode_page_no_delta(data),
        }
    }
}

fn new_body_reader(
    body: Body,
) -> FramedRead<impl AsyncRead, impl Decoder<Item = BytesMut, Error = std::io::Error>> {
    let body = StreamReader::new(
        body.map(|x| x.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
    );
    let reader = FramedRead::new(
        body,
        LengthDelimitedCodec::builder()
            .max_frame_length(MAX_MESSAGE_SIZE)
            .new_codec(),
    );
    reader
}

fn generate_suffix_versionstamp_atomic_op(template: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(template.len() + 4);
    out.extend_from_slice(template);
    out.extend_from_slice(&(template.len() as u32 - 10).to_le_bytes());
    out
}

fn prepend_length(data: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(data.len() + 4);
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(&data);
    out
}

fn decode_version(version: &str) -> Result<[u8; 10]> {
    let mut bytes = [0u8; 10];
    hex::decode_to_slice(version, &mut bytes).with_context(|| "cannot decode version")?;
    Ok(bytes)
}
