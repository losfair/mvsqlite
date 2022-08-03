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
use std::{
    collections::HashMap,
    convert::Infallible,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{io::AsyncRead, sync::oneshot};
use tokio_util::{
    codec::{Decoder, FramedRead, LengthDelimitedCodec},
    io::StreamReader,
};

use crate::{
    commit::{CommitContext, CommitResult},
    lock::DistributedLock,
};

const MAX_MESSAGE_SIZE: usize = 10 * 1024; // 10 KiB
const COMMIT_MESSAGE_SIZE: usize = 9 * 1024 * 1024; // 9 MiB
const MAX_PAGE_SIZE: usize = 8192;
const COMMITTED_VERSION_HDR_NAME: &str = "x-committed-version";

pub struct Server {
    pub db: Database,
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

#[derive(Deserialize)]
pub struct AdminTruncateNamespaceRequest {
    pub key: String,
    pub before_version: String,
    #[serde(default)]
    pub apply: bool,
}

#[derive(Deserialize)]
pub struct AdminDeleteUnreferencedContentInNamespaceRequest {
    pub key: String,
    #[serde(default)]
    pub apply: bool,
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

            "/api/truncate_namespace" => {
                let body = hyper::body::to_bytes(req.body_mut()).await?;
                let body: AdminTruncateNamespaceRequest = serde_json::from_slice(&body)?;
                let nskey_key = self.construct_nskey_key(&body.key);

                let txn = self.db.create_trx()?;
                let ns_id = match txn.get(&nskey_key, false).await? {
                    Some(v) => v,
                    None => {
                        return Ok(Response::builder()
                            .status(400)
                            .body(Body::from("this key does not exist"))?);
                    }
                };
                drop(txn);

                let ns_id =
                    <[u8; 10]>::try_from(&ns_id[..]).with_context(|| "cannot parse ns_id")?;
                let before_version = decode_version(&body.before_version)?;
                let (mut res_sender, res_body) = Body::channel();
                let (progress_ch_tx, mut progress_ch_rx) =
                    tokio::sync::mpsc::channel::<Option<u64>>(1000);
                let (stop_tx, stop_rx) = oneshot::channel::<()>();
                let apply = body.apply;
                tokio::spawn(async move {
                    let work = async move {
                        if let Err(e) = self
                            .truncate_versions(!body.apply, ns_id, before_version, |progress| {
                                let _ = progress_ch_tx.try_send(progress);
                            })
                            .await
                        {
                            tracing::error!(ns = hex::encode(&ns_id), before_version = hex::encode(&before_version), apply = apply, error = %e, "truncate_namespace failed");
                        }
                    };
                    tokio::select! {
                        _ = work => {

                        }
                        _ = stop_rx => {
                            tracing::error!(ns = hex::encode(&ns_id), before_version = hex::encode(&before_version), apply = apply, "truncate_namespace interrupted");
                        }
                    }
                });
                tokio::spawn(async move {
                    let _stop_tx = stop_tx;
                    loop {
                        let mut exit = false;
                        let text = match progress_ch_rx.recv().await {
                            Some(progress) => match progress {
                                Some(x) => {
                                    format!("{}\n", x)
                                }
                                None => {
                                    exit = true;
                                    "DONE\n".to_string()
                                }
                            },
                            None => {
                                exit = true;
                                "ERROR\n".to_string()
                            }
                        };
                        if res_sender.send_data(Bytes::from(text)).await.is_err() {
                            break;
                        }
                        if exit {
                            break;
                        }
                    }
                });
                res = Response::builder().status(200).body(res_body)?;
            }

            "/api/delete_unreferenced_content" => {
                let body = hyper::body::to_bytes(req.body_mut()).await?;
                let body: AdminDeleteUnreferencedContentInNamespaceRequest =
                    serde_json::from_slice(&body)?;
                let nskey_key = self.construct_nskey_key(&body.key);

                let txn = self.db.create_trx()?;
                let ns_id = match txn.get(&nskey_key, false).await? {
                    Some(v) => v,
                    None => {
                        return Ok(Response::builder()
                            .status(400)
                            .body(Body::from("this key does not exist"))?);
                    }
                };
                drop(txn);

                let ns_id =
                    <[u8; 10]>::try_from(&ns_id[..]).with_context(|| "cannot parse ns_id")?;
                let (mut res_sender, res_body) = Body::channel();
                let (progress_ch_tx, mut progress_ch_rx) =
                    tokio::sync::mpsc::channel::<String>(1000);
                let (stop_tx, stop_rx) = oneshot::channel::<()>();
                let apply = body.apply;
                tokio::spawn(async move {
                    let work = async move {
                        if let Err(e) = self
                            .delete_unreferenced_content(!body.apply, ns_id, |progress| {
                                let _ = progress_ch_tx.try_send(progress);
                            })
                            .await
                        {
                            tracing::error!(ns = hex::encode(&ns_id), apply = apply, error = %e, "delete_unreferenced_content failed");
                        }
                    };
                    tokio::select! {
                        _ = work => {

                        }
                        _ = stop_rx => {
                            tracing::error!(ns = hex::encode(&ns_id), apply = apply, "delete_unreferenced_content interrupted");
                        }
                    }
                });
                tokio::spawn(async move {
                    let _stop_tx = stop_tx;
                    loop {
                        let mut exit = false;
                        let text = match progress_ch_rx.recv().await {
                            Some(progress) => {
                                if progress == "DONE\n" {
                                    exit = true;
                                }
                                progress
                            }
                            None => {
                                exit = true;
                                "ERROR\n".to_string()
                            }
                        };
                        if res_sender.send_data(Bytes::from(text)).await.is_err() {
                            break;
                        }
                        if exit {
                            break;
                        }
                    }
                });
                res = Response::builder().status(200).body(res_body)?;
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

    pub fn spawn_background_tasks(self: Arc<Self>) {
        tokio::spawn(self.clone().globaltask_timekeeper());
    }

    async fn globaltask_timekeeper(self: Arc<Self>) {
        let mut lock = DistributedLock::new(
            self.construct_globaltask_key("timekeeper"),
            "timekeeper".into(),
        );
        'outer: loop {
            loop {
                let me = self.clone();
                match lock
                    .lock(
                        move || me.db.create_trx().with_context(|| "failed to create txn"),
                        Duration::from_secs(5),
                    )
                    .await
                {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(e) => {
                        tracing::error!(error = %e, "timekeeper lock error");
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            tracing::info!("timekeeper started");

            loop {
                loop {
                    let txn = match lock.create_txn_and_check_sync(&self.db).await {
                        Ok(x) => x,
                        Err(e) => {
                            tracing::error!(error = %e, "timekeeper create_txn_and_check_sync error");
                            lock.unlock().await;
                            continue 'outer;
                        }
                    };
                    let now = SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    let key = self.construct_time2version_key(now);
                    let value = [0u8; 14];
                    txn.atomic_op(&key, &value, MutationType::SetVersionstampedValue);
                    match txn.commit().await {
                        Ok(_) => break,
                        Err(e) => match e.on_error().await {
                            Ok(_) => {}
                            Err(e) => {
                                tracing::error!(error = %e, "timekeeper commit error");
                                lock.unlock().await;
                                continue 'outer;
                            }
                        },
                    }
                }

                tokio::time::sleep(Duration::from_secs(5)).await;
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
                    Ok(txn) => match txn.get(&self.construct_nskey_key(nskey), false).await {
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
                let nsmd = txn.get(&self.construct_nsmd_key(ns_id), false).await?;
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
                            match txn.get(&content_key, false).await {
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
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
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
                        match txn.get(&content_key, false).await {
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
                                            let base_content_index_key = self
                                                .construct_contentindex_key(ns_id, delta_base_hash);
                                            let now = SystemTime::now()
                                                .duration_since(SystemTime::UNIX_EPOCH)
                                                .unwrap();
                                            txn.atomic_op(
                                                &base_content_index_key,
                                                &ContentIndex::generate_mutation_payload(now),
                                                MutationType::SetVersionstampedValue,
                                            );
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

                        // Set content index
                        let content_index_key =
                            self.construct_contentindex_key(ns_id, *hash.as_bytes());
                        txn.atomic_op(
                            &content_index_key,
                            &ContentIndex::generate_mutation_payload(now),
                            MutationType::SetVersionstampedValue,
                        );

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

                // Decode data
                let mut index_writes: Vec<(u32, [u8; 32])> = Vec::new();

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
                    index_writes.push((
                        commit_req.page_index,
                        <[u8; 32]>::try_from(commit_req.hash).unwrap(),
                    ));
                }

                match self
                    .commit(CommitContext {
                        ns_id,
                        client_assumed_version,
                        idempotency_key,
                        metadata: commit_init.metadata,
                        index_writes: &index_writes,
                    })
                    .await?
                {
                    CommitResult::BadPageReference => {
                        res = Response::builder()
                            .status(400)
                            .body(Body::from("bad page reference"))?;
                    }
                    CommitResult::Committed { versionstamp } => {
                        res = Response::builder()
                            .header(COMMITTED_VERSION_HDR_NAME, hex::encode(&versionstamp))
                            .body(Body::empty())?;
                    }
                    CommitResult::Conflict => {
                        res = Response::builder().status(409).body(Body::empty())?;
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
        page_version_hex: &str,
    ) -> Result<Option<Page>> {
        let (version, hash) = match self
            .read_page_hash(txn, ns_id, page_index, page_version_hex)
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let content_key = self.construct_content_key(ns_id, hash);
        let content = txn
            .get(&content_key, false)
            .await?
            .with_context(|| "cannot find content for the provided hash")?;
        Ok(Some(Page {
            version,
            data: content,
        }))
    }

    pub fn construct_nsmd_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nsmd"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    pub fn construct_nstask_key(&self, ns_id: [u8; 10], task: &str) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "nstask"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key.extend_from_slice(&pack(&(task,)));
        key
    }

    pub fn construct_globaltask_key(&self, task: &str) -> Vec<u8> {
        let key = pack(&(self.metadata_prefix.as_str(), "globaltask", task));
        key
    }

    pub fn construct_time2version_key(&self, time_secs: u64) -> Vec<u8> {
        let key = pack(&(self.metadata_prefix.as_str(), "time2version", time_secs));
        key
    }

    pub fn construct_nskey_key(&self, ns_key: &str) -> Vec<u8> {
        pack(&(self.metadata_prefix.as_str(), "nskey", ns_key))
    }

    pub fn construct_ns_commit_token_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut key = pack(&(self.metadata_prefix.as_str(), "ns_commit_token"));
        key.push(0x32);
        key.extend_from_slice(&ns_id);
        key
    }

    pub fn construct_last_write_version_key(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len() + 1);
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'v');
        buf
    }

    pub fn construct_ns_data_prefix(&self, ns_id: [u8; 10]) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len());
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf
    }

    pub fn construct_page_key(
        &self,
        ns_id: [u8; 10],
        page_index: u32,
        page_version: [u8; 10],
    ) -> Vec<u8> {
        let mut buf: Vec<u8> = Vec::with_capacity(
            self.raw_data_prefix.len()
                + ns_id.len()
                + 1
                + std::mem::size_of::<u32>()
                + page_version.len(),
        );
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'p');
        buf.extend_from_slice(&page_index.to_be_bytes());
        buf.extend_from_slice(&page_version);
        buf
    }

    pub fn construct_content_key(&self, ns_id: [u8; 10], hash: [u8; 32]) -> Vec<u8> {
        let mut buf: Vec<u8> =
            Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len() + 1 + hash.len());
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'c');
        buf.extend_from_slice(&hash);
        buf
    }

    pub fn construct_contentindex_key(&self, ns_id: [u8; 10], hash: [u8; 32]) -> Vec<u8> {
        let mut buf: Vec<u8> =
            Vec::with_capacity(self.raw_data_prefix.len() + ns_id.len() + 1 + hash.len());
        buf.extend_from_slice(&self.raw_data_prefix);
        buf.extend_from_slice(&ns_id);
        buf.push(b'd');
        buf.extend_from_slice(&hash);
        buf
    }

    pub fn construct_delta_referrer_key(
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
            let base = match txn.get(&base_page_key, false).await? {
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
                let flattened_base = match txn.get(&flattened_base_key, false).await? {
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
                let base_page = match txn.get(&base_page_key, false).await? {
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

pub fn generate_suffix_versionstamp_atomic_op(template: &[u8]) -> Vec<u8> {
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
