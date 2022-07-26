use anyhow::Result;
use mvclient::{MultiVersionClient, MultiVersionClientConfig};
use rand::{Rng, RngCore};
use structopt::StructOpt;
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

    /// Number of iterations.
    #[structopt(long)]
    iterations: u64,
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

    let num_pages = 1000;

    let mut txn = client.create_transaction().await?;
    for i in 0..opt.iterations {
        let mode = rand::thread_rng().gen_range(0..4);
        tracing::info!(counter = i, mode = mode, "iteration");
        match mode {
            0 => {
                let mut rng = rand::thread_rng();
                let num_reads = rng.gen_range(1..=10);
                let reads = (0..num_reads)
                    .map(|_| rng.gen_range::<u32, _>(0..num_pages))
                    .collect::<Vec<_>>();
                drop(rng);
                let _pages = txn.read_many(&reads).await?;
            }
            1 => {
                let mut rng = rand::thread_rng();
                let num_writes = rng.gen_range(1..=10);
                let writes = (0..num_writes)
                    .map(|_| {
                        let index = rng.gen_range::<u32, _>(0..num_pages);
                        let mut data = vec![0u8; 4096];
                        rng.fill_bytes(&mut data);
                        (index, data)
                    })
                    .collect::<Vec<_>>();
                drop(rng);
                let writes = writes
                    .iter()
                    .map(|(index, data)| (*index, data.as_slice()))
                    .collect::<Vec<_>>();
                let _hashes = txn.write_many(&writes).await?;
            }
            2 => {
                txn.commit().await?;
                txn = client.create_transaction().await?;
            }
            3 => {
                txn = client.create_transaction().await?;
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
