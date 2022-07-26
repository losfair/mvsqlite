mod server;

use std::net::SocketAddr;

use anyhow::{Context, Result};
use hyper::service::{make_service_fn, service_fn};
use server::{Server, ServerConfig};
use structopt::StructOpt;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

fn main() -> Result<()> {
    let network = unsafe { foundationdb::boot() };

    // Have fun with the FDB API
    let res = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { async_main().await });

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
}

async fn async_main() -> Result<()> {
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
    let server = Server::open(ServerConfig {
        cluster: opt.cluster.clone(),
        raw_data_prefix: opt.raw_data_prefix.clone(),
        metadata_prefix: opt.metadata_prefix.clone(),
    })
    .with_context(|| "failed to initialize server")?;

    let data_plane_server = {
        let server = server.clone();
        hyper::Server::bind(&opt.data_plane).serve(make_service_fn(move |_conn| {
            let server = server.clone();
            async move {
                Ok::<_, anyhow::Error>(service_fn(move |req| server.clone().serve_data_plane(req)))
            }
        }))
    };

    let admin_api_server =
        hyper::Server::bind(&opt.admin_api).serve(make_service_fn(move |_conn| {
            let server = server.clone();
            async move {
                Ok::<_, anyhow::Error>(service_fn(move |req| server.clone().serve_admin_api(req)))
            }
        }));

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
