use anyhow::{Context, Result};
use bumpalo::Bump;
use bytes::{Bytes, BytesMut};
use foundationdb::{
    options::{MutationType, StreamingMode, TransactionOption},
    tuple::unpack,
    Database, FdbError, RangeOption, Transaction,
};
use futures::{StreamExt, TryStreamExt};
use hyper::{
    body::{HttpBody, Sender},
    Body, Request, Response,
};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    str::FromStr,
    sync::{atomic::Ordering, Arc},
    time::{Duration, SystemTime},
};
use tokio::{
    io::AsyncRead,
    sync::{oneshot, Semaphore},
    task::JoinHandle,
};
use tokio_util::{
    codec::{Decoder, FramedRead, LengthDelimitedCodec},
    io::StreamReader,
};

use crate::{
    commit::{CommitContext, CommitNamespaceContext, CommitResult},
    delta::reader::{DeltaReader, WIRE_ZSTD},
    fixed::FixedString,
    keys::KeyCodec,
    lock::DistributedLock,
    metadata::{NamespaceMetadata, NamespaceMetadataCache, NamespaceOverlayBase},
    nslock::{acquire_nslock, release_nslock, NslockReleaseMode},
    page::{Page, MAX_PAGE_SIZE},
    replica::ReplicaManager,
    time2version::time2version,
    util::{decode_version, generate_suffix_versionstamp_atomic_op, GoneError},
    write::{WriteApplier, WriteApplierContext, WriteRequest, WriteResponse},
};

const MAX_MESSAGE_SIZE: usize = 40 * 1024; // 40 KiB
const MAX_PAGES_PER_BATCH_READ: usize = 200;
const MAX_READ_CONCURRENCY: usize = 50;
const MAX_PAGES_PER_COMMIT: usize = 50000; // ~390MiB with 8KiB pages
const MAX_NUM_NAMESPACES_PER_COMMIT: usize = 16;
const MAX_PAGES_PER_BATCH_WRITE: usize = 20;
const COMMITTED_VERSION_HDR_NAME: &str = "x-committed-version";
const LAST_VERSION_HDR_NAME: &str = "x-last-version";

enum GetError {
    NotFound,
    Other(anyhow::Error),
}

#[derive(Debug)]
enum CreateNamespaceError {
    AlreadyExist,
}

impl std::fmt::Display for CreateNamespaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CreateNamespaceError {}

pub struct Server {
    pub db: Database,

    pub key_codec: Arc<KeyCodec>,

    nskey_cache: Cache<String, [u8; 10]>,
    read_version_cache: Cache<[u8; 10], i64>,
    pub read_version_and_nsid_to_lwv_cache: Cache<(i64, [u8; 10]), [u8; 10]>,
    pub replica_manager: Option<ReplicaManager>,
    pub ns_metadata_cache: NamespaceMetadataCache,

    pub content_cache: Option<Cache<[u8; 32], Bytes>>,

    pub auto_create_ns: bool,
}

pub struct ServerConfig {
    pub cluster: String,
    pub raw_data_prefix: String,
    pub metadata_prefix: String,
    pub read_only: bool,
    pub dr_tag: String,
    pub content_cache_size: usize,
    pub auto_create_ns: bool,
}

#[derive(Deserialize)]
pub struct ReadRequest<'a> {
    pub page_index: u32,
    pub version: &'a str,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub hash: Option<&'a [u8]>,

    #[serde(default)]
    pub accept_zstd: bool,
}

#[derive(Serialize)]
pub struct ReadResponse {
    pub version: FixedString,
    pub data: Bytes,
    pub zstd: bool,
}

#[derive(Deserialize)]
pub struct CommitGlobalInit<'a> {
    #[serde(with = "serde_bytes")]
    pub idempotency_key: &'a [u8],

    #[serde(default)]
    pub allow_skip_idempotency_check: bool,

    pub num_namespaces: usize,

    #[serde(default)]
    pub lock_owner: Option<&'a str>,
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
    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub data: Option<&'a [u8]>,
}

#[derive(Serialize)]
pub struct CommitResponse {
    pub changelog: HashMap<String, Vec<u32>>,
}

#[derive(Deserialize)]
pub struct NslockAcquireRequest<'a> {
    pub owner: &'a str,
    #[serde(default)]
    pub version: Option<&'a str>,
}

#[derive(Deserialize)]
pub struct NslockReleaseRequest<'a> {
    pub owner: &'a str,
    pub mode: NslockReleaseMode,
}

#[derive(Deserialize)]
pub struct AdminCreateNamespaceRequest {
    pub key: String,

    #[serde(default)]
    pub overlay_base: Option<NamespaceOverlayBase>,
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

impl Server {
    pub async fn open(config: ServerConfig) -> Result<Arc<Self>> {
        let db = Database::new(Some(config.cluster.as_str()))
            .with_context(|| "cannot open fdb cluster")?;
        let raw_data_prefix = config.raw_data_prefix.as_bytes().to_vec();

        // Read DR replica UID
        let replica_manager = if config.read_only {
            Some(ReplicaManager::new(&db, &config.dr_tag).await?)
        } else {
            None
        };
        Ok(Arc::new(Self {
            db,
            key_codec: Arc::new(KeyCodec {
                raw_data_prefix,
                metadata_prefix: config.metadata_prefix,
            }),
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
            replica_manager,
            ns_metadata_cache: NamespaceMetadataCache::new(),
            content_cache: if config.content_cache_size > 0 {
                Some(
                    Cache::builder()
                        .max_capacity(config.content_cache_size as u64)
                        .build(),
                )
            } else {
                None
            },
            auto_create_ns: config.auto_create_ns,
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

    async fn create_namespace(
        &self,
        key: &str,
        overlay_base: Option<NamespaceOverlayBase>,
    ) -> Result<()> {
        let nskey_key = self.key_codec.construct_nskey_key(&key);
        let nsmd = NamespaceMetadata {
            lock: None,
            overlay_base,
        };
        if let Some(base) = &nsmd.overlay_base {
            decode_version(&base.ns_id).with_context(|| "overlay_base: invalid ns_id")?;
            decode_version(&base.snapshot_version)
                .with_context(|| "overlay_base: invalid snapshot_version")?;
        }
        let nsmd = serde_json::to_string(&nsmd)?;
        let mut txn = self.db.create_trx()?;

        loop {
            if txn.get(&nskey_key, false).await?.is_some() {
                return Err(anyhow::Error::new(CreateNamespaceError::AlreadyExist));
            }

            let nsmd_atomic_op_key = generate_suffix_versionstamp_atomic_op(
                &self.key_codec.construct_nsmd_key([0u8; 10]),
            );
            let nskey_atomic_op_value = [0u8; 14];
            txn.atomic_op(
                &nsmd_atomic_op_key,
                nsmd.as_bytes(),
                MutationType::SetVersionstampedKey,
            );
            txn.atomic_op(
                &nskey_key,
                &nskey_atomic_op_value,
                MutationType::SetVersionstampedValue,
            );
            match txn.commit().await {
                Ok(_) => {
                    return Ok(());
                }
                Err(e) => {
                    txn = match e.on_error().await {
                        Ok(x) => x,
                        Err(e) => {
                            return Err(e.into());
                        }
                    };
                }
            }
        }
    }

    async fn do_serve_admin_api(self: Arc<Self>, mut req: Request<Body>) -> Result<Response<Body>> {
        let uri = req.uri();
        let res: Response<Body>;
        match uri.path() {
            "/api/create_namespace" => {
                let body = read_full_body_with_limit(req.body_mut()).await?;
                let body: AdminCreateNamespaceRequest = serde_json::from_slice(&body)?;
                match self.create_namespace(&body.key, body.overlay_base).await {
                    Ok(_) => {
                        return Ok(Response::builder()
                            .status(201)
                            .body(Body::from("created\n"))?);
                    }
                    Err(e) => match e.downcast_ref() {
                        Some(CreateNamespaceError::AlreadyExist) => {
                            return Ok(Response::builder()
                                .status(422)
                                .body(Body::from("this key already exists\n"))?);
                        }
                        _ => {
                            return Ok(Response::builder()
                                .status(400)
                                .body(Body::from(format!("{}", e)))?);
                        }
                    },
                }
            }

            "/api/rename_namespace" => {
                let body = read_full_body_with_limit(req.body_mut()).await?;
                let body: AdminRenameNamespaceRequest = serde_json::from_slice(&body)?;
                let old_nskey_key = self.key_codec.construct_nskey_key(&body.old_key);
                let new_nskey_key = self.key_codec.construct_nskey_key(&body.new_key);

                let mut txn = self.db.create_trx()?;

                loop {
                    if txn.get(&new_nskey_key, false).await?.is_some() {
                        return Ok(Response::builder()
                            .status(422)
                            .body(Body::from("new key already exists\n"))?);
                    }
                    let ns_id = match txn.get(&old_nskey_key, false).await? {
                        Some(v) => v,
                        None => {
                            return Ok(Response::builder()
                                .status(404)
                                .body(Body::from("old key does not exist\n"))?);
                        }
                    };
                    txn.clear(&old_nskey_key);
                    txn.set(&new_nskey_key, &ns_id);
                    match txn.commit().await {
                        Ok(_) => {
                            res = Response::builder()
                                .status(200)
                                .body(Body::from("renamed\n"))?;
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
                let body = read_full_body_with_limit(req.body_mut()).await?;
                let body: AdminDeleteNamespaceRequest = serde_json::from_slice(&body)?;
                let nskey_key = self.key_codec.construct_nskey_key(&body.key);

                let mut txn = self.db.create_trx()?;

                loop {
                    let ns_id = match txn.get(&nskey_key, false).await? {
                        Some(v) => v,
                        None => {
                            return Ok(Response::builder()
                                .status(404)
                                .body(Body::from("this key does not exist\n"))?);
                        }
                    };
                    let ns_id =
                        <[u8; 10]>::try_from(&ns_id[..]).with_context(|| "cannot parse ns_id")?;
                    let ns_data_start = self.key_codec.construct_ns_data_prefix(ns_id);
                    let mut ns_data_end = ns_data_start.clone();
                    ns_data_end.push(0xff).unwrap();

                    txn.clear_range(&ns_data_start, &ns_data_end);
                    txn.clear(&nskey_key);

                    let nsmd_key = self.key_codec.construct_nsmd_key(ns_id);
                    txn.clear(&nsmd_key);
                    txn.update_metadata_version();
                    match txn.commit().await {
                        Ok(_) => {
                            res = Response::builder()
                                .status(200)
                                .body(Body::from("deleted\n"))?;
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
                let body = read_full_body_with_limit(req.body_mut()).await?;
                let body: AdminTruncateNamespaceRequest = serde_json::from_slice(&body)?;
                let nskey_key = self.key_codec.construct_nskey_key(&body.key);

                let txn = self.db.create_trx()?;
                let ns_id = match txn.get(&nskey_key, false).await? {
                    Some(v) => v,
                    None => {
                        return Ok(Response::builder()
                            .status(404)
                            .body(Body::from("this key does not exist\n"))?);
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
                let body = read_full_body_with_limit(req.body_mut()).await?;
                let body: AdminDeleteUnreferencedContentInNamespaceRequest =
                    serde_json::from_slice(&body)?;
                let nskey_key = self.key_codec.construct_nskey_key(&body.key);

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
                let start = self.key_codec.construct_nskey_key(text_prefix);
                let key_prefix = self.key_codec.construct_nskey_prefix();
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
                        let range: Vec<_> = loop {
                            let range = txn
                                .get_ranges_keyvalues(
                                    RangeOption {
                                        limit: Some(100),
                                        reverse: false,
                                        mode: StreamingMode::WantAll,
                                        ..RangeOption::from(cursor.clone()..end.clone())
                                    },
                                    true,
                                )
                                .try_collect()
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
            self.key_codec.construct_globaltask_key("timekeeper"),
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
                    let key = self.key_codec.construct_time2version_key(now);
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
                        if self.is_read_only() {
                            txn.set_option(TransactionOption::ReadLockAware).unwrap();
                        }
                        match txn
                            .get(&self.key_codec.construct_nskey_key(nskey), false)
                            .await
                        {
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
                    if self.auto_create_ns {
                        self.create_namespace(ns_key, None).await?;
                        self.lookup_nskey(ns_key, None).await?.unwrap()
                    } else {
                        return Ok(Response::builder()
                            .status(404)
                            .body(Body::from("namespace not found"))?);
                    }
                }
            };
            Some(ns_id)
        } else {
            None
        };
        let uri_path = req.uri().path().to_string();
        match self.do_serve_data_plane_stage2(ns_id, req).await {
            Ok(res) => Ok(res),
            Err(e) => {
                let ns_id_hex = ns_id.map(|x| hex::encode(&x)).unwrap_or_default();

                if e.chain().any(|x| x.downcast_ref::<GoneError>().is_some()) {
                    tracing::warn!(ns = ns_id_hex, error = %e, "the requested resource is no longer available");
                    return Ok(Response::builder().status(410).body(Body::empty())?);
                }

                if e.chain().any(|x| {
                    x.downcast_ref::<FdbError>()
                        .map(|x| x.code() == 1020)
                        .unwrap_or_default()
                }) {
                    // conflict
                    tracing::debug!(ns = ns_id_hex, endpoint = uri_path, error = %e, "fdb conflict");
                } else {
                    tracing::warn!(ns = ns_id_hex, error = %e, "stage 2 failure");
                }
                Ok(Response::builder().status(500).body(Body::empty())?)
            }
        }
    }

    async fn create_versioned_read_txn(&self, version: &str) -> Result<Transaction> {
        let version = decode_version(version)?;
        let txn = self.db.create_trx()?;
        if self.is_read_only() {
            txn.set_option(TransactionOption::ReadLockAware).unwrap();
        }

        // It's safe to set CRR here. We do our own version check below.
        txn.set_option(TransactionOption::CausalReadRisky).unwrap();

        let mut grv_called = false;
        let fdb_rv = self
            .read_version_cache
            .try_get_with(version, async {
                grv_called = true;
                let fdb_rv = txn.get_read_version().await;
                match fdb_rv {
                    Ok(fdb_rv) => {
                        // XXX: We are only checking for primary read here due to performance reasons
                        if !self.is_read_only() && fdb_rv < i64::from_be_bytes(<[u8; 8]>::try_from(&version[0..8]).unwrap()) {
                            Err(anyhow::anyhow!("fdb read version older than requested version - causal read fault?"))
                        } else {
                            Ok(fdb_rv)
                        }
                    },
                    Err(e) => Err(anyhow::Error::from(e))
                }
            })
            .await
            .map_err(|e| anyhow::anyhow!("cannot get read version: {}", e))?;
        if !grv_called {
            txn.set_read_version(fdb_rv);
        }
        Ok(txn)
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

                let crr = query
                    .get("causal_read_risky")
                    .map(|x| x.as_str() == "1")
                    .unwrap_or_default();
                let lock_owner = query
                    .get("lock_owner")
                    .map(|x| x.as_str())
                    .unwrap_or_default();
                let stat = self.stat(ns_id, from_version, crr, lock_owner).await?;
                let stat =
                    serde_json::to_vec(&stat).with_context(|| "cannot serialize stat response")?;

                res = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(stat))?;
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
                            match rmp_serde::from_slice::<ReadRequest>(&message) {
                                Ok(read_req) => match me.handle_read_req(ns_id, &ns_id_hex, read_req).await {
                                    Ok(x) => Ok(x),
                                    Err(e) => {
                                        tracing::warn!(ns = %ns_id_hex, error = %e, "read failed");
                                        Err(())
                                    }
                                },
                                Err(e) => {
                                    tracing::warn!(ns = %ns_id_hex, error = %e, "invalid message");
                                    Err(())
                                }
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
                if self.is_read_only() {
                    return Ok(Response::builder().status(403).body(Body::empty()).unwrap());
                }

                let (ns_id, ns_id_hex) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };
                let body = req.into_body();
                let body = new_body_reader(body);
                let (res_sender, res_body) = Body::channel();
                res = Response::builder().body(res_body)?;
                let me = self.clone();
                let now = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap();
                tokio::spawn(async move {
                    match me.batch_write(ns_id, now, body, res_sender).await {
                        Ok(()) => {}
                        Err(e) => {
                            tracing::warn!(ns = ns_id_hex, error = %e, "error in batch write");
                        }
                    }
                });
            }
            "/batch/commit" => {
                if self.is_read_only() {
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

                let page_alloc = Bump::new();
                let mut total_allocated_pages: usize = 0;

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
                        page_writes: Vec::new(),
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

                        if let Some(data) = commit_req.data {
                            if total_allocated_pages >= MAX_PAGES_PER_BATCH_WRITE {
                                return Ok(Response::builder()
                                    .status(413)
                                    .body(Body::from("too many fast writes"))?);
                            }

                            if data.len() > MAX_PAGE_SIZE {
                                return Ok(Response::builder()
                                    .status(413)
                                    .body(Body::from("fast write page too large"))?);
                            }
                            let data = page_alloc.alloc_slice_copy(data);
                            ns_ctx.page_writes.push(WriteRequest {
                                data,
                                delta_base: Some(commit_req.page_index),
                            });
                            total_allocated_pages += 1;
                        }
                    }

                    ns_contexts.push(ns_ctx);
                }

                match self
                    .commit(CommitContext {
                        idempotency_key,
                        allow_skip_idempotency_check: commit_global_init
                            .allow_skip_idempotency_check,
                        namespaces: &ns_contexts,
                        lock_owner: commit_global_init.lock_owner,
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
                        changelog,
                    } => {
                        let data: CommitResponse = CommitResponse { changelog };
                        let body = rmp_serde::to_vec_named(&data)
                            .with_context(|| "cannot serialize commit response")?;
                        let last_write_version = [0xffu8; 10]; // backward compatibility
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

                let txn = self.db.create_trx()?;

                if self.is_read_only() {
                    txn.set_option(TransactionOption::ReadLockAware).unwrap();
                }

                // It's safe to set CRR for time2version. Not seeing something written
                // by the timekeeper is okay.
                txn.set_option(TransactionOption::CausalReadRisky).unwrap();

                let ttv_res = time2version(
                    &txn,
                    &self.key_codec,
                    time_in_seconds,
                    self.replica_manager.as_ref(),
                )
                .await?;
                let body = serde_json::to_vec(&ttv_res)
                    .with_context(|| "cannot serialize time2version response")?;

                res = Response::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(Body::from(body))?;
            }
            "/nslock/acquire" => {
                let (ns_id, _) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };

                let body = read_full_body_with_limit(req.into_body()).await?;
                let body: NslockAcquireRequest = serde_json::from_slice(&body)?;
                res = acquire_nslock(
                    &self.db,
                    &self.key_codec,
                    &self.ns_metadata_cache,
                    ns_id,
                    body.owner,
                    body.version,
                )
                .await?;
            }
            "/nslock/release" => {
                let (ns_id, _) = match require_ns_id() {
                    Ok(x) => x,
                    Err(x) => return Ok(x),
                };

                let body = read_full_body_with_limit(req.into_body()).await?;
                let body: NslockReleaseRequest = serde_json::from_slice(&body)?;
                res = release_nslock(
                    &self.db,
                    &self.key_codec,
                    &self.ns_metadata_cache,
                    ns_id,
                    body.owner,
                    body.mode,
                )
                .await?;
            }
            _ => {
                res = Response::builder().status(404).body("not found".into())?;
            }
        }

        Ok(res)
    }

    async fn read_page_decoded_snapshot_compressed(
        &self,
        txn: &Transaction,
        ns_id: [u8; 10],
        page_index: u32,
        page_version_hex: &str,
    ) -> Result<Option<Page>> {
        let reader = DeltaReader {
            txn: &txn,
            ns_id,
            key_codec: &self.key_codec,
            replica_manager: self.replica_manager.as_ref(),
            content_cache: self.content_cache.as_ref(),
        };
        let (version, hash) = match reader
            .read_page_hash(page_index, Some(page_version_hex), true)
            .await?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let data = reader
            .get_page_content_decoded_snapshot_compressed(hash)
            .await?
            .with_context(|| "cannot find content for the provided hash")?;
        Ok(Some(Page { version, data }))
    }

    pub fn is_read_only(&self) -> bool {
        self.replica_manager.is_some()
    }

    async fn batch_write(
        &self,
        ns_id: [u8; 10],
        now: Duration,
        mut body: FramedRead<
            impl AsyncRead + Unpin,
            impl Decoder<Item = BytesMut, Error = std::io::Error>,
        >,
        mut res_sender: Sender,
    ) -> Result<()> {
        let txn = self.db.create_trx()?;

        // It's safe to set CRR for the write path. Seeing stale data doesn't affect correctness.
        txn.set_option(TransactionOption::CausalReadRisky).unwrap();

        let mut messages: Vec<BytesMut> = Vec::new();
        loop {
            let message = match body.next().await {
                Some(Ok(x)) => x,
                Some(Err(e)) => {
                    return Err(anyhow::Error::from(e).context("client disconnected with error"));
                }
                None => {
                    anyhow::bail!("client disconnected before completion");
                }
            };
            if message.len() == 0 {
                // completion
                break;
            }

            if messages.len() > MAX_PAGES_PER_BATCH_WRITE {
                anyhow::bail!("too many pages in batch write");
            }
            messages.push(message);
        }

        let write_reqs: Vec<WriteRequest> = messages
            .iter()
            .map(|x| rmp_serde::from_slice(x))
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| "error deserializing write requests")?;
        let mut applier = WriteApplier::new(WriteApplierContext {
            txn: &txn,
            ns_id,
            key_codec: &self.key_codec,
            now,
            content_cache: self.content_cache.as_ref(),
        });
        let res = applier.apply_write(&write_reqs).await;
        let res = match res {
            Some(x) => x,
            None => {
                anyhow::bail!("cannot apply write");
            }
        };
        for res in res {
            let payload =
                rmp_serde::to_vec_named(&res).with_context(|| "error serializing response")?;
            res_sender
                .send_data(Bytes::from(prepend_length(&payload)))
                .await
                .with_context(|| "cannot send response")?;
        }

        txn.commit()
            .await
            .map_err(FdbError::from)
            .with_context(|| "error committing transaction")?;
        let payload = rmp_serde::to_vec_named(&WriteResponse {
            hash: Default::default(),
        })
        .with_context(|| "error serializing completion")?;
        res_sender
            .send_data(Bytes::from(prepend_length(&payload)))
            .await
            .with_context(|| "error sending completion")?;
        Ok(())
    }

    async fn handle_read_req(
        &self,
        ns_id: [u8; 10],
        ns_id_hex: &str,
        read_req: ReadRequest<'_>,
    ) -> Result<ReadResponse> {
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
            txn = self.db.create_trx()?;

            if self.is_read_only() {
                txn.set_option(TransactionOption::ReadLockAware).unwrap();
            }

            let hash = <[u8; 32]>::try_from(hash).with_context(|| "hash is not 32 bytes")?;

            let reader = DeltaReader {
                txn: &txn,
                ns_id,
                key_codec: &self.key_codec,
                replica_manager: self.replica_manager.as_ref(),
                content_cache: self.content_cache.as_ref(),
            };
            let content = reader
                .get_page_content_decoded_snapshot_compressed(hash)
                .await
                .with_context(|| "failed to get content by hash")?;
            match content {
                Some(x) => Some(Page {
                    version: FixedString::from_str(read_req.version).unwrap_or_default(),
                    data: x,
                }),
                None => None,
            }
        } else {
            // The normal path.

            txn = self
                .create_versioned_read_txn(read_req.version)
                .await
                .with_context(|| "failed to create versioned read txn")?;
            let mut page = self
                .read_page_decoded_snapshot_compressed(
                    &txn,
                    ns_id,
                    read_req.page_index,
                    read_req.version,
                )
                .await
                .with_context(|| "error reading page")?;

            if page.is_none() {
                // Overlay
                let metadata = self
                    .ns_metadata_cache
                    .get(&txn, &self.key_codec, ns_id)
                    .await?;
                if let Some(base) = &metadata.overlay_base {
                    let base_ns_id = decode_version(&base.ns_id)?;
                    let base_page = self
                        .read_page_decoded_snapshot_compressed(
                            &txn,
                            base_ns_id,
                            read_req.page_index,
                            &base.snapshot_version,
                        )
                        .await?;
                    page = base_page;
                }
            }

            page
        };
        let wire_zstd = WIRE_ZSTD.load(Ordering::Relaxed);
        let payload = match page {
            Some(x) => ReadResponse {
                version: x.version.clone(),
                data: if wire_zstd {
                    if read_req.accept_zstd {
                        x.data
                    } else {
                        Bytes::from(zstd::bulk::decompress(&x.data, 1048576)?)
                    }
                } else {
                    x.data
                },
                zstd: wire_zstd && read_req.accept_zstd,
            },
            None => ReadResponse {
                version: "".into(),
                data: Bytes::new(),
                zstd: false,
            },
        };
        Ok(payload)
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

async fn read_full_body_with_limit<T: HttpBody>(body: T) -> Result<Bytes>
where
    T::Error: std::error::Error + Send + Sync + 'static,
{
    let response_content_length = match body.size_hint().upper() {
        Some(v) => v,
        None => (MAX_MESSAGE_SIZE as u64) + 1,
    };

    if response_content_length <= MAX_MESSAGE_SIZE as u64 {
        let body_bytes = hyper::body::to_bytes(body).await?;
        Ok(body_bytes)
    } else {
        anyhow::bail!("response too large");
    }
}

fn prepend_length(data: &[u8]) -> Vec<u8> {
    let mut out: Vec<u8> = Vec::with_capacity(data.len() + 4);
    out.extend_from_slice(&(data.len() as u32).to_be_bytes());
    out.extend_from_slice(&data);
    out
}
