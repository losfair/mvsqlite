mod inmem;
mod tester;

use anyhow::Result;
use mvclient::{MultiVersionClient, MultiVersionClientConfig};
use structopt::StructOpt;
use tester::Tester;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

#[derive(Debug, StructOpt)]
#[structopt(name = "mvstore-stress", about = "stress test mvstore")]
struct Opt {
    /// Data plane URL.
    #[structopt(long)]
    data_plane: String,

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

    let client = MultiVersionClient::new(MultiVersionClientConfig {
        data_plane: opt.data_plane.parse()?,
        ns_key: opt.ns_key.clone(),
    })?;
    let t = Tester::new(client.clone(), opt.pages);
    t.run(opt.concurrency as _, opt.iterations as _).await;
    Ok(())
}
