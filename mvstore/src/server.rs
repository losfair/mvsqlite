use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use foundationdb::{
    future::FdbValues,
    options::{MutationType, StreamingMode, TransactionOption},
    tuple::unpack,
    Database, RangeOption, Transaction,
};
use futures::StreamExt;
use hyper::{Body, Request, Response};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    io::AsyncRead,
    sync::{oneshot, Semaphore},
    task::{block_in_place, JoinHandle},
};
use tokio_util::{
    codec::{Decoder, FramedRead, LengthDelimitedCodec},
    io::StreamReader,
};

use crate::{
    commit::{CommitContext, CommitNamespaceContext, CommitResult},
    lock::DistributedLock,
};

const MAX_MESSAGE_SIZE: usize = 40 * 1024; // 40 KiB
const MAX_PAGE_SIZE: usize = 32768;
const MAX_PAGES_PER_BATCH_READ: usize = 200;
const MAX_READ_CONCURRENCY: usize = 50;
const MAX_PAGES_PER_COMMIT: usize = 50000; // ~390MiB with 8KiB pages
const MAX_NUM_NAMESPACES_PER_COMMIT: usize = 16;
const COMMITTED_VERSION_HDR_NAME: &str = "x-committed-version";
const LAST_VERSION_HDR_NAME: &str = "x-last-version";

enum GetError {
    NotFound,
    Other(anyhow::Error),
}

pub struct Server {
    pub db: Database,
    pub raw_data_prefix: Vec<u8>,
    pub metadata_prefix: String,

    nskey_cache: Cache<String, [u8; 10]>,
    read_version_cache: Cache<[u8; 10], i64>,
    pub read_version_and_nsid_to_lwv_cache: Cache<(i64, [u8; 10]), [u8; 10]>,
    content_cache: Cache<([u8; 10], [u8; 32]), Bytes>,

    pub read_only: bool,
}

pub struct ServerConfig {
    pub cluster: String,
    pub raw_data_prefix: String,
    pub metadata_prefix: String,
    pub read_only: bool,
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
pub struct ReadResponse {
    pub version: String,
    pub data: Bytes,
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
pub struct CommitGlobalInit<'a> {
    #[serde(with = "serde_bytes")]
    pub idempotency_key: &'a [u8],

    pub num_namespaces: usize,
}

#[derive(Deserialize)]
pub struct CommitNamespaceInit<'a> {
    pub version: &'a str,
    pub ns_key: &'a str,
    pub ns_key_hashproof: Option<&'a str>,
    pub metadata: Option<&'a str>,
    pub num_pages: u32,
    pub read_set: Option<HashSet<u32>>,
}

#[derive(Deserialize)]
pub struct CommitRequest<'a> {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

#[derive(Serialize)]
pub struct CommitResponse {
    pub changelog: HashMap<String, Vec<u32>>,
}

#[derive(Serialize)]
pub struct TimeToVersionResponse {
    pub after: Option<TimeToVersionPoint>,
    pub not_after: Option<TimeToVersionPoint>,
}

#[derive(Serialize)]
pub struct TimeToVersionPoint {
    pub version: String,
    pub time: u64,
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
pub struct AdminRenameNamespaceRequest {
    pub old_key: String,
    pub new_key: String,
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
    pub data: Bytes,
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
            read_version_and_nsid_to_lwv_cache: Cache::builder()
                .time_to_idle(Duration::from_secs(2))
                .build(),
            content_cache: Cache::builder()
                .time_to_idle(Duration::from_secs(60))
                .max_capacity(5000)
                .build(),
            read_only: config.read_only,
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

            "/api/rename_namespace" => {
                let body = hyper::body::to_bytes(req.body_mut()).await?;
                let body: AdminRenameNamespaceRequest = serde_json::from_slice(&body)?;
                let old_nskey_key = self.construct_nskey_key(&body.old_key);
                let new_nskey_key = self.construct_nskey_key(&body.new_key);

                let mut txn = self.db.create_trx()?;

                loop {
                    if txn.get(&new_nskey_key, false).await?.is_some() {
                        return Ok(Response::builder()
                            .status(400)
                            .body(Body::from("new key already exists"))?);
                    }
                    let ns_id = match txn.get(&old_nskey_key, false).await? {
                        Some(v) => v,
                        None => {
                            return Ok(Response::builder()
                                .status(400)
                                .body(Body::from("old key does not exist"))?);
                        }
                    };
                    txn.clear(&old_nskey_key);
                    txn.set(&new_nskey_key, &ns_id);
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
                    ns_data_end.push(0xff).unwrap();

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

            "/api/list_namespace" => {
                let query: HashMap<String, String> = uri
                    .query()
                    .map(|v| {
                        url::form_urlencoded::parse(v.as_bytes())
                            .into_owned()
                            .collect()
                    })
                    .unwrap_or_else(HashMap::new);

                let text_prefix = query.get("prefix").map(|x| x.as_str()).unwrap_or_default();
                let start = self.construct_nskey_key(text_prefix);
                let key_prefix = self.construct_nskey_prefix();
                let mut cursor = start.clone();
                let mut end = start.clone();
                *end.last_mut().unwrap() = 0xff;

                let (mut res_sender, res_body) = Body::channel();
                res = Response::builder().body(res_body)?;
                let me = self.clone();

                tokio::spawn(async move {
                    'outer: loop {
                        let mut txn = match me.db.create_trx() {
                            Ok(txn) => txn,
                            Err(e) => {
                                tracing::error!(error = %e, "create_trx failed");
                                res_sender.abort();
                                break;
                            }
                        };
                        let range = loop {
                            let range = txn
                                .get_range(
                                    &RangeOption {
                                        limit: Some(100),
                                        reverse: false,
                                        mode: StreamingMode::WantAll,
                                        ..RangeOption::from(cursor.clone()..end.clone())
                                    },
                                    0,
                                    true,
                                )
                                .await;
                            match range {
                                Ok(x) => break x,
                                Err(e) => match txn.on_error(e).await {
                                    Ok(x) => {
                                        txn = x;
                                        continue;
                                    }
                                    Err(e) => {
                                        tracing::error!(error = %e, "list_namespace range scan failed");
                                        res_sender.abort();
                                        break 'outer;
                                    }
                                },
                            }
                        };
                        if range.is_empty() {
                            break;
                        }
                        cursor = range.last().unwrap().key().to_vec();
                        cursor.push(0x00);

                        for item in range {
                            let key = item.key();
                            let value = item.value();
                            if key.len() >= key_prefix.len() {
                                if let Ok(nskey) = unpack::<String>(&key[key_prefix.len()..]) {
                                    let entry = serde_json::json!({
                                        "nskey": nskey,
                                        "nsid": hex::encode(value),
                                    });
                                    let mut entry = entry.to_string();
                                    entry.push('\n');
                                    if res_sender.send_data(Bytes::from(entry)).await.is_err() {
                                        break 'outer;
                                    }
                                }
                            }
                        }
                    }
                });
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

    async fn lookup_nskey(&self, nskey: &str, hashproof: Option<&str>) -> Result<Option<[u8; 10]>> {
        let hashproof_hash = {
            let segs = nskey.split(":").collect::<Vec<_>>();
            if segs.len() < 2 {
                None
            } else {
                Some(segs[1])
            }
        };
        if let Some(hashproof_hash) = hashproof_hash {
            let mut hash: [u8; 32] = [0u8; 32];
            if hex::decode_to_slice(hashproof_hash, &mut hash).is_err() {
                tracing::error!(nskey, "hashproof_hash hex decode failed");
                return Ok(None);
            }
            let proof = match hex::decode(hashproof.unwrap_or("")) {
                Ok(x) => x,
                Err(_) => {
                    tracing::error!(nskey, "hashproof hex decode failed");
                    return Ok(None);
                }
            };
            let hashed_proof = blake3::hash(&proof);
            if !constant_time_eq::constant_time_eq_n(hashed_proof.as_bytes(), &hash) {
                tracing::error!(nskey, "hashproof mismatch");
                return Ok(None);
            }
        }

        let res = self
            .nskey_cache
            .try_get_with(nskey.to_string(), async {
                let txn = self.db.create_trx();
                match txn {
                    Ok(txn) => {
                        if self.read_only {
                            txn.set_option(TransactionOption::ReadLockAware).unwrap();
                        }
                        match txn.get(&self.construct_nskey_key(nskey), false).await {
                            Ok(Some(x)) => <[u8; 10]>::try_from(&x[..])
                                .with_context(|| "invalid namespace id")
                                .map_err(GetError::Other),
                            Ok(None) => Err(GetError::NotFound),
                            Err(e) => Err(GetError::Other(
                                anyhow::Error::from(e).context("transaction failed"),
                            )),
                        }
                    }
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
        let ns_id = if let Some(ns_key) = req.headers().get("x-namespace-key") {
            let ns_key = match ns_key.to_str() {
                Ok(x) => x,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(400)
                        .body(Body::from("invalid x-namespace-key"))
                        .unwrap())
                }
            };
            let hashproof = req
                .headers()
                .get("x-namespace-hashproof")
                .map(|x| x.to_str().unwrap_or_default());
            let ns_id = self.lookup_nskey(ns_key, hashproof).await?;
            let ns_id = match ns_id {
                Some(x) => x,
                None => {
                    return Ok(Response::builder()
                        .status(404)
                        .body(Body::from("namespace not found"))?)
                }
            };
            Some(ns_id)
        } else {
            None
        };
        match self.do_serve_data_plane_stage2(ns_id, req).await {
            Ok(res) => Ok(res),
            Err(e) => {
                let ns_id_hex = ns_id.map(|x| hex::encode(&x)).unwrap_or_default();
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
        if self.read_only {
            txn.set_option(TransactionOption::ReadLockAware).unwrap();
        }
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

    async fn get_page_content_decoded_snapshot(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        hash: [u8; 32],
    ) -> Result<Option<Bytes>> {
        let key = self.construct_content_key(ns_id, hash);
        let res = self
            .content_cache
            .try_get_with((ns_id, hash), async {
                let kv = txn.get(&key, true).await;

                match kv {
                    Ok(Some(x)) => {
                        let page = self.decode_page_for_read_caching(txn, ns_id, x).await;
                        match page {
                            Ok(x) => Ok(x),
                            Err(e) => Err(GetError::Other(e)),
                        }
                    }
                    Ok(None) => Err(GetError::NotFound),
                    Err(e) => Err(GetError::Other(e.into())),
                }
            })
            .await;
        match res {
            Ok(x) => Ok(Some(x)),
            Err(e) => match &*e {
                GetError::NotFound => Ok(None),
                GetError::Other(e) => Err(anyhow::anyhow!("content read failed: {}", e)),
            },
        }
    }

    async fn get_page_content_decoded_no_delta_snapshot(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        hash: [u8; 32],
    ) -> Result<Option<Bytes>> {
        let key = self.construct_content_key(ns_id, hash);
        let res = self
            .content_cache
            .try_get_with((ns_id, hash), async {
                let kv = txn.get(&key, true).await;

                match kv {
                    Ok(Some(x)) => {
                        let page = self.decode_page_no_delta(x).await;
                        match page {
                            Ok(x) => Ok(x),
                            Err(e) => Err(GetError::Other(e)),
                        }
                    }
                    Ok(None) => Err(GetError::NotFound),
                    Err(e) => Err(GetError::Other(e.into())),
                }
            })
            .await;
        match res {
            Ok(x) => Ok(Some(x)),
            Err(e) => match &*e {
                GetError::NotFound => Ok(None),
                GetError::Other(e) => Err(anyhow::anyhow!("content read failed: {}", e)),
            },
        }
    }

    async fn do_serve_data_plane_stage2(
        self: Arc<Self>,
        ns_id: Option<[u8; 10]>,
        req: Request<Body>,
    ) -> Result<Response<Body>> {
        let require_ns_id = || match ns_id {
            Some(x) => Ok((x, hex::encode(&x))),
            None => Err(Response::builder()
                .status(400)
                .body(Body::from("this operation requires x-namespace-key"))
                .unwrap()),
        };
        let uri = req.uri();
        let res: Response<Body>;
        match uri.path() {
            "/stat" => {
                let query: HashMap<String, String> = uri
                    .query()
                    .map(|v| {
                        url::form_urlencoded::parse(v.as_bytes())
                            .into_owned()
                            .collect()
                    })
                    .unwrap_or_else(HashMap::new);

                let (ns_id, _) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };
                let from_version = query
                    .get("from_version")
                    .map(|x| x.as_str())
                    .unwrap_or_default();
                let stat = self.stat(ns_id, from_version).await?;
                let stat =
                    serde_json::to_vec(&stat).with_context(|| "cannot serialize stat response")?;

                res = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(stat))?;
            }
            "/read" => {
                let (ns_id, _) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };
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
                    .read_page_decoded_snapshot(&txn, ns_id, page_index, &page_version_hex)
                    .await?;
                match page {
                    Some(page) => {
                        res = Response::builder()
                            .header("x-page-version", page.version)
                            .body(Body::from(page.data))?;
                    }
                    None => {
                        res = Response::builder().body(Body::empty())?;
                    }
                }
            }
            "/batch/read" => {
                let (ns_id, ns_id_hex) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };
                let body = req.into_body();
                let mut body = new_body_reader(body);
                let (mut res_sender, res_body) = Body::channel();
                res = Response::builder().body(res_body)?;
                let me = self.clone();
                tokio::spawn(async move {
                    let sem = Arc::new(Semaphore::new(MAX_READ_CONCURRENCY));
                    let mut read_futures: Vec<JoinHandle<Result<ReadResponse, ()>>> = Vec::new();
                    loop {
                        let ns_id_hex = ns_id_hex.clone();
                        let message = match body.next().await {
                            Some(Ok(x)) => x,
                            Some(Err(e)) => {
                                tracing::warn!(ns = ns_id_hex, error = %e, "client disconnected with error");
                                break;
                            }
                            None => break,
                        };
                        if read_futures.len() >= MAX_PAGES_PER_BATCH_READ {
                            tracing::warn!(
                                ns = ns_id_hex,
                                max = MAX_PAGES_PER_BATCH_READ,
                                "too many pages in read batch"
                            );
                            break;
                        }
                        let me = me.clone();
                        let sem_permit = sem.clone().acquire_owned().await.unwrap();
                        read_futures.push(tokio::spawn(async move {
                            let _sem_permit = sem_permit;
                            loop {
                                let read_req: ReadRequest = match rmp_serde::from_slice(&message) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        tracing::warn!(ns = %ns_id_hex, error = %e, "invalid message");
                                        break Err(());
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
                                    txn = match me.db.create_trx() {
                                        Ok(x) => x,
                                        Err(e) => {
                                            tracing::warn!(ns = ns_id_hex, error = %e, "failed to create transaction");
                                            break Err(());
                                        }
                                    };

                                    if me.read_only {
                                        txn.set_option(TransactionOption::ReadLockAware).unwrap();
                                    }

                                    let hash = match <[u8; 32]>::try_from(hash) {
                                        Ok(x) => x,
                                        Err(e) => {
                                            tracing::warn!(ns = ns_id_hex, error = %e, "hash is not 32 bytes");
                                            break Err(());
                                        }
                                    };
                                    let content = me.get_page_content_decoded_snapshot(&txn, ns_id, hash).await;
                                    match content {
                                        Ok(Some(x)) => Some(Page {
                                            version: read_req.version.to_string(),
                                            data: x,
                                        }),
                                        Ok(None) => None,
                                        Err(e) => {
                                            tracing::warn!(ns = ns_id_hex, error = %e, "failed to get content by hash");
                                            break Err(());
                                        }
                                    }
                                } else {
                                    txn = match me.create_versioned_read_txn(read_req.version).await {
                                        Ok(x) => x,
                                        Err(e) => {
                                            tracing::warn!(ns = ns_id_hex, error = %e, "failed to create versioned read txn");
                                            break Err(());
                                        }
                                    };
                                    match me
                                        .read_page_decoded_snapshot(&txn, ns_id, read_req.page_index, read_req.version)
                                        .await
                                    {
                                        Ok(x) => x,
                                        Err(e) => {
                                            tracing::warn!(ns = ns_id_hex, error = %e, page_index = read_req.page_index, version = read_req.version, "error reading page");
                                            break Err(());
                                        }
                                    }
                                };
                                let payload = match page {
                                    Some(x) => ReadResponse {
                                        version: x.version.clone(),
                                        data: x.data,
                                    },
                                    None => ReadResponse {
                                        version: "".into(),
                                        data: Bytes::new(),
                                    },
                                };
                                break Ok(payload);
                            }
                        }));
                    }

                    for fut in read_futures {
                        let fut_output = match fut.await {
                            Ok(x) => x,
                            Err(_) => {
                                break;
                            }
                        };
                        let payload = match fut_output {
                            Ok(x) => x,
                            Err(()) => {
                                break;
                            }
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
                if self.read_only {
                    return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
                }

                let (ns_id, ns_id_hex) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };
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
                                                );
                                            txn.set(&content_key, &x);
                                            txn.set(&delta_referrer_key, &delta_base_hash);
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
                if self.read_only {
                    return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
                }

                // x-namespace-key ignored
                let body = req.into_body();
                let mut reader = new_body_reader(body);

                let commit_global_init = reader
                    .next()
                    .await
                    .with_context(|| "missing commit global init")?
                    .with_context(|| "invalid commit global init")?;
                let commit_global_init: CommitGlobalInit =
                    rmp_serde::from_slice(&commit_global_init)
                        .with_context(|| "error deserializing commit global init")?;

                if commit_global_init.num_namespaces < 1
                    || commit_global_init.num_namespaces > MAX_NUM_NAMESPACES_PER_COMMIT
                {
                    return Ok(Response::builder()
                        .status(412)
                        .body(Body::from("num_namespaces out of bound"))?);
                }

                let idempotency_key = <[u8; 16]>::try_from(commit_global_init.idempotency_key)
                    .with_context(|| "invalid idempotency key")?;

                let mut ns_contexts: Vec<CommitNamespaceContext> =
                    Vec::with_capacity(commit_global_init.num_namespaces);
                let mut total_page_count: usize = 0;

                for _ in 0..commit_global_init.num_namespaces {
                    let init = reader
                        .next()
                        .await
                        .with_context(|| "missing commit init")?
                        .with_context(|| "invalid commit init")?;
                    let mut init: CommitNamespaceInit = rmp_serde::from_slice(&init)
                        .with_context(|| "error deserializing commit init")?;
                    if total_page_count.saturating_add(init.num_pages as usize)
                        > MAX_PAGES_PER_COMMIT
                    {
                        return Ok(Response::builder()
                            .status(413)
                            .body(Body::from("too many pages in commit"))?);
                    }
                    total_page_count += init.num_pages as usize;

                    let ns_id = match self
                        .lookup_nskey(init.ns_key, init.ns_key_hashproof)
                        .await?
                    {
                        Some(x) => x,
                        None => {
                            return Ok(Response::builder()
                                .status(404)
                                .body(Body::from("namespace not found"))?);
                        }
                    };

                    let client_assumed_version = decode_version(&init.version)?;

                    let mut ns_ctx = CommitNamespaceContext {
                        ns_id,
                        ns_key: init.ns_key.to_string(),
                        client_assumed_version,
                        index_writes: Vec::new(),
                        metadata: init.metadata.map(|x| x.to_string()),
                        use_read_set: init.read_set.is_some(),
                        read_set: init.read_set.take().unwrap_or_default(),
                    };

                    for _ in 0..init.num_pages {
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
                        ns_ctx.index_writes.push((
                            commit_req.page_index,
                            <[u8; 32]>::try_from(commit_req.hash).unwrap(),
                        ));
                    }

                    ns_contexts.push(ns_ctx);
                }

                match self
                    .commit(CommitContext {
                        idempotency_key,
                        namespaces: &ns_contexts,
                    })
                    .await?
                {
                    CommitResult::BadPageReference => {
                        res = Response::builder()
                            .status(410)
                            .body(Body::from("bad page reference"))?;
                    }
                    CommitResult::Committed {
                        versionstamp,
                        last_write_version,
                        changelog,
                    } => {
                        let data: CommitResponse = CommitResponse { changelog };
                        let body = rmp_serde::to_vec_named(&data)
                            .with_context(|| "cannot serialize commit response")?;
                        res = Response::builder()
                            .header(COMMITTED_VERSION_HDR_NAME, hex::encode(&versionstamp))
                            .header(LAST_VERSION_HDR_NAME, hex::encode(&last_write_version))
                            .body(Body::from(body))?;
                    }
                    CommitResult::Conflict => {
                        res = Response::builder().status(409).body(Body::empty())?;
                    }
                    CommitResult::NamespaceNotDistinct => {
                        res = Response::builder()
                            .status(400)
                            .body(Body::from("namespace not distinct"))?;
                    }
                }
            }
            "/time2version" => {
                let query: HashMap<String, String> = uri
                    .query()
                    .map(|v| {
                        url::form_urlencoded::parse(v.as_bytes())
                            .into_owned()
                            .collect()
                    })
                    .unwrap_or_else(HashMap::new);
                let time_in_seconds = query
                    .get("t")
                    .with_context(|| "missing t")?
                    .parse::<u64>()
                    .with_context(|| "invalid t")?;
                let key = self.construct_time2version_key(time_in_seconds);
                let lower_bound = self.construct_time2version_key(std::u64::MIN);
                let upper_bound = self.construct_time2version_key(std::u64::MAX);
                let prefix = self.construct_time2version_prefix();
                let txn = self.db.create_trx()?;

                if self.read_only {
                    txn.set_option(TransactionOption::ReadLockAware).unwrap();
                }

                let after = txn
                    .get_range(
                        &RangeOption {
                            limit: Some(1),
                            reverse: true,
                            mode: StreamingMode::Small,
                            ..RangeOption::from(lower_bound.clone()..key.clone())
                        },
                        0,
                        true,
                    )
                    .await?;

                let not_after = txn
                    .get_range(
                        &RangeOption {
                            limit: Some(1),
                            reverse: false,
                            mode: StreamingMode::Small,
                            ..RangeOption::from(key.clone()..upper_bound.clone())
                        },
                        0,
                        true,
                    )
                    .await?;
                let map_it = |x: FdbValues| -> Option<TimeToVersionPoint> {
                    if x.len() == 0 || x[0].key().len() < prefix.len() {
                        None
                    } else {
                        let item = &x[0];
                        let time_suffix = &item.key()[prefix.len()..];
                        match unpack::<u64>(time_suffix) {
                            Ok(time_secs) => match <[u8; 10]>::try_from(item.value()) {
                                Ok(version) => Some(TimeToVersionPoint {
                                    version: hex::encode(&version),
                                    time: time_secs,
                                }),
                                Err(_) => None,
                            },
                            Err(_) => None,
                        }
                    }
                };
                let after = map_it(after);
                let not_after = map_it(not_after);

                let body = serde_json::to_vec(&TimeToVersionResponse { after, not_after })
                    .with_context(|| "cannot serialize time2version response")?;

                res = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(body))?;
            }
            _ => {
                res = Response::builder().status(404).body("not found".into())?;
            }
        }

        Ok(res)
    }

    pub async fn read_page_hash(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        page_version_hex: Option<&str>,
    ) -> Result<Option<(String, [u8; 32])>> {
        let page_version = match page_version_hex {
            Some(x) => decode_version(x)?,
            None => [0xffu8; 10],
        };
        if self.read_only && page_version != [0xffu8; 10] {
            let current_rv = txn.get_read_version().await?;
            let requested_rv = i64::from_be_bytes(page_version[0..8].try_into().unwrap());
            if current_rv < requested_rv {
                anyhow::bail!("this replica does not have the requested read version");
            }
        }
        let scan_end = self.construct_page_key(ns_id, page_index, page_version);
        let scan_start = self.construct_page_key(ns_id, page_index, [0u8; 10]);
        let page_vec = txn
            .get_range(
                &RangeOption {
                    limit: Some(1),
                    reverse: true,
                    mode: StreamingMode::Small,
                    ..RangeOption::from(scan_start.as_slice()..=scan_end.as_slice())
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

    async fn read_page_decoded_snapshot(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        page_version_hex: &str,
    ) -> Result<Option<Page>> {
        let (version, hash) = match self
            .read_page_hash(txn, ns_id, page_index, Some(page_version_hex))
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let data = self
            .get_page_content_decoded_snapshot(txn, ns_id, hash)
            .await?
            .with_context(|| "cannot find content for the provided hash")?;
        Ok(Some(Page { version, data }))
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
            .read_page_hash(&txn, ns_id, base_page_index, Some(version_hex.as_str()))
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
        let base_page = self.decode_page_no_delta(undecoded_base).await?;
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
            ns = hex::encode(&ns_id),
            base = hex::encode(&delta_base_hash),
            "delta encoded"
        );
        Ok(Some((output, delta_base_hash)))
    }

    async fn decode_page_no_delta<T: AsRef<[u8]> + Send + Sync + 'static>(
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

    async fn decode_page_for_read_caching<T: AsRef<[u8]> + Send + Sync + 'static>(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
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
                    .get_page_content_decoded_no_delta_snapshot(txn, ns_id, base_page_hash)
                    .await?;
                let base_page = match base_page {
                    Some(x) => x,
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

pub fn decode_version(version: &str) -> Result<[u8; 10]> {
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
