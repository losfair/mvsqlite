mod commit;
mod gc;
mod lock;
mod server;
mod stat;

use std::{net::SocketAddr, sync::atomic::Ordering};

use anyhow::{Context, Result};
use foundationdb::{api::FdbApiBuilder, options::NetworkOption};
use hyper::service::{make_service_fn, service_fn};
use server::{Server, ServerConfig};
use structopt::StructOpt;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

fn main() -> Result<()> {
    let opt = Opt::from_args();
    if opt.json {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    } else {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .pretty()
            .init();
    }
    let mut network_builder = FdbApiBuilder::default()
        .build()
        .expect("fdb api initialization failed");
    if opt.fdb_buggify {
        tracing::error!("fdb_buggify is enabled");
        network_builder = network_builder
            .set_option(NetworkOption::ClientBuggifyEnable)
            .unwrap()
            .set_option(NetworkOption::ClientBuggifySectionActivatedProbability(100))
            .unwrap();
    }
    let network = unsafe { network_builder.boot() }.expect("fdb network initialization failed");

    // Have fun with the FDB API
    let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { async_main(opt).await });

    drop(network); // required for safety
    res
}

#[derive(Debug, StructOpt)]
#[structopt(name = "mvstore", about = "mvsqlite store service")]
struct Opt {
    /// Data plane listen address.
    #[structopt(long, env = "MVSTORE_DATA_PLANE")]
    data_plane: SocketAddr,

    /// Admin API listen address.
    #[structopt(long, env = "MVSTORE_ADMIN_API")]
    admin_api: SocketAddr,

    /// Output log in JSON format.
    #[structopt(long)]
    json: bool,

    /// Enable FDB buggify. DO NOT USE IN PRODUCTION!
    #[structopt(long)]
    fdb_buggify: bool,

    /// Path to FoundationDB cluster file.
    #[structopt(
        long,
        default_value = "/etc/foundationdb/fdb.cluster",
        env = "MVSTORE_CLUSTER"
    )]
    cluster: String,

    /// Data prefix. This value is NOT tuple-encoded, for maximum efficiency.
    #[structopt(long, env = "MVSTORE_RAW_DATA_PREFIX")]
    raw_data_prefix: String,

    /// Metadata prefix. This value is tuple-encoded as a string.
    #[structopt(long, env = "MVSTORE_METADATA_PREFIX")]
    metadata_prefix: String,

    /// Whether this instance is read-only. This enables replica-read from FDB DR replica.
    #[structopt(long)]
    read_only: bool,

    /// ADVANCED. Configure the GC scan batch size.
    #[structopt(long, env = "MVSTORE_KNOB_GC_SCAN_BATCH_SIZE")]
    knob_gc_scan_batch_size: Option<usize>,

    /// ADVANCED. Configure the max time-to-live for unreferenced fresh pages. This will also be the max time an SQLite transaction can be active.
    #[structopt(long, env = "MVSTORE_KNOB_GC_FRESH_PAGE_TTL_SECS")]
    knob_gc_fresh_page_ttl_secs: Option<u64>,

    /// ADVANCED. Configure the threshold (in number of pages) above which multi-phase commit is enabled (inclusive).
    #[structopt(long, env = "MVSTORE_KNOB_COMMIT_MULTI_PHASE_THRESHOLD")]
    knob_commit_multi_phase_threshold: Option<usize>,

    /// ADVANCED. Configure the threshold (in number of pages) below which page-level conflict check is enabled (inclusive).
    #[structopt(long, env = "MVSTORE_KNOB_PLCC_READ_SET_SIZE_THRESHOLD")]
    knob_plcc_read_set_size_threshold: Option<usize>,
}

async fn async_main(opt: Opt) -> Result<()> {
    if let Some(x) = opt.knob_gc_scan_batch_size {
        gc::GC_SCAN_BATCH_SIZE.store(x, Ordering::Relaxed);
        tracing::info!(value = x, "configured gc scan batch size");
    }

    if let Some(x) = opt.knob_gc_fresh_page_ttl_secs {
        gc::GC_FRESH_PAGE_TTL_SECS.store(x, Ordering::Relaxed);
        tracing::info!(value = x, "configured gc fresh page ttl");
    }

    if let Some(x) = opt.knob_commit_multi_phase_threshold {
        commit::COMMIT_MULTI_PHASE_THRESHOLD.store(x, Ordering::Relaxed);
        tracing::info!(value = x, "configured commit multi-phase threshold");
    }

    if let Some(x) = opt.knob_plcc_read_set_size_threshold {
        commit::PLCC_READ_SET_SIZE_THRESHOLD.store(x, Ordering::Relaxed);
        tracing::info!(value = x, "configured plcc read set size threshold");
    }

    let server = Server::open(ServerConfig {
        cluster: opt.cluster.clone(),
        raw_data_prefix: opt.raw_data_prefix.clone(),
        metadata_prefix: opt.metadata_prefix.clone(),
        read_only: opt.read_only,
    })
    .with_context(|| "failed to initialize server")?;

    if !opt.read_only {
        server.clone().spawn_background_tasks();
    }

    let data_plane_server = {
        let server = server.clone();
        hyper::Server::bind(&opt.data_plane).serve(make_service_fn(move |_conn| {
            let server = server.clone();
            async move {
                Ok::<_, anyhow::Error>(service_fn(move |req| server.clone().serve_data_plane(req)))
            }
        }))
    };

    let admin_api_server = if opt.read_only {
        None
    } else {
        Some(
            hyper::Server::bind(&opt.admin_api).serve(make_service_fn(move |_conn| {
                let server = server.clone();
                async move {
                    Ok::<_, anyhow::Error>(service_fn(move |req| {
                        server.clone().serve_admin_api(req)
                    }))
                }
            })),
        )
    };
    let admin_api_server = async move {
        match admin_api_server {
            Some(x) => x.await,
            None => futures::future::pending().await,
        }
    };

    tracing::info!("server initialized");
    tokio::select! {
        x = data_plane_server => {
            anyhow::bail!("data plane exit: {:?}", x);
        }
        x = admin_api_server => {
            anyhow::bail!("admin api exit: {:?}", x);
        }
    }
}
