mod inmem;
mod tester;

use anyhow::Result;
use backtrace::Backtrace;
use mvclient::{MultiVersionClient, MultiVersionClientConfig};
use structopt::StructOpt;
use tester::{Tester, TesterConfig};
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

#[derive(Debug, StructOpt)]
#[structopt(name = "mvstore-stress", about = "stress test mvstore")]
struct Opt {
    /// Data plane URL.
    #[structopt(long)]
    data_plane: String,

    /// Admin API URL.
    #[structopt(long)]
    admin_api: String,

    /// Output log in JSON format.
    #[structopt(long)]
    json: bool,

    /// Namespace key.
    #[structopt(long, env = "NS_KEY")]
    ns_key: String,

    /// Number of concurrent tasks.
    #[structopt(long)]
    concurrency: u64,

    /// Number of iterations.
    #[structopt(long)]
    iterations: u64,

    /// Number of pages.
    #[structopt(long)]
    pages: u32,

    /// Disable read-your-writes tests.
    #[structopt(long)]
    disable_ryw: bool,

    /// Permit HTTP 410 commit responses.
    #[structopt(long)]
    permit_410: bool,

    /// Disable read sets.
    #[structopt(long)]
    disable_read_set: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
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

    std::panic::set_hook(Box::new(|info| {
        let bt = Backtrace::new();
        tracing::error!(backtrace = ?bt, info = %info, "panic");
        std::process::abort();
    }));

    let client = MultiVersionClient::new(
        MultiVersionClientConfig {
            data_plane: vec![opt.data_plane.parse()?],
            ns_key: opt.ns_key.clone(),
            ns_key_hashproof: None,
            lock_owner: None,
        },
        reqwest::Client::new(),
    )?;
    let t = Tester::new(
        client.clone(),
        TesterConfig {
            admin_api: opt.admin_api.clone(),
            num_pages: opt.pages,
            disable_ryw: opt.disable_ryw,
            permit_410: opt.permit_410,
            disable_read_set: opt.disable_read_set,
        },
    );
    t.run(opt.concurrency as _, opt.iterations as _).await;
    println!("Test succeeded.");

    // Otherwise we might get "ERROR mvstore_stress: panicked at 'dispatch dropped without returning error', /home/runner/.cargo/registry/src/github.com-1ecc6299db9ec823/hyper-0.14.20/src/client/conn.rs:397:35".
    // https://github.com/losfair/mvsqlite/runs/7676519092
    std::process::exit(0);
}
