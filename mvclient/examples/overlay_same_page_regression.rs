use std::{
    collections::HashMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use mvclient::{CommitOutput, MultiVersionClient, MultiVersionClientConfig};
use reqwest::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug)]
struct Opt {
    data_plane: String,
    admin_api: String,
    admin_api_key: String,
}

#[derive(Debug, Deserialize)]
struct StatNamespaceResponse {
    nsid: String,
}

#[derive(Debug, Serialize)]
struct CreateNamespaceOverlay<'a> {
    ns_id: &'a str,
    snapshot_version: &'a str,
}

impl Opt {
    fn parse() -> Result<Self> {
        let mut data_plane = "http://127.0.0.1:7000".to_string();
        let mut admin_api = "http://127.0.0.1:7001".to_string();
        let mut admin_api_key = String::new();

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--data-plane" => {
                    data_plane = args.next().context("missing value for --data-plane")?
                }
                "--admin-api" => {
                    admin_api = args.next().context("missing value for --admin-api")?
                }
                "--admin-api-key" => {
                    admin_api_key = args.next().context("missing value for --admin-api-key")?
                }
                _ => return Err(anyhow!("unknown argument: {arg}")),
            }
        }

        Ok(Self {
            data_plane,
            admin_api,
            admin_api_key,
        })
    }
}

fn admin_request(client: &Client, opt: &Opt, url: String) -> RequestBuilder {
    let builder = client.post(url);
    if opt.admin_api_key.is_empty() {
        builder
    } else {
        builder.header("x-api-key", &opt.admin_api_key)
    }
}

fn plain_client(data_plane: &str, ns_key: &str) -> Result<std::sync::Arc<MultiVersionClient>> {
    MultiVersionClient::new(
        MultiVersionClientConfig {
            data_plane: vec![data_plane.parse()?],
            ns_key: ns_key.to_string(),
            ns_key_hashproof: None,
            lock_owner: None,
        },
        Client::new(),
    )
}

async fn create_namespace(client: &Client, opt: &Opt, key: &str) -> Result<()> {
    let resp = admin_request(
        client,
        opt,
        format!("{}/api/create_namespace", opt.admin_api),
    )
    .json(&json!({ "key": key }))
    .send()
    .await
    .context("create namespace request")?;
    if !resp.status().is_success() {
        anyhow::bail!("create namespace {key} failed: {}", resp.text().await?);
    }
    Ok(())
}

async fn create_overlay_namespace(
    client: &Client,
    opt: &Opt,
    key: &str,
    ns_id: &str,
    snapshot_version: &str,
) -> Result<()> {
    let resp = admin_request(
        client,
        opt,
        format!("{}/api/create_namespace", opt.admin_api),
    )
    .json(&json!({
        "key": key,
        "overlay_base": CreateNamespaceOverlay {
            ns_id,
            snapshot_version,
        }
    }))
    .send()
    .await
    .context("create overlay request")?;
    if !resp.status().is_success() {
        anyhow::bail!(
            "create overlay namespace {key} failed: {}",
            resp.text().await?
        );
    }
    Ok(())
}

async fn delete_namespace(client: &Client, opt: &Opt, key: &str) -> Result<()> {
    let resp = admin_request(
        client,
        opt,
        format!("{}/api/delete_namespace", opt.admin_api),
    )
    .json(&json!({ "key": key }))
    .send()
    .await
    .context("delete namespace request")?;
    if !resp.status().is_success() {
        anyhow::bail!("delete namespace {key} failed: {}", resp.text().await?);
    }
    Ok(())
}

async fn stat_namespace(client: &Client, opt: &Opt, key: &str) -> Result<StatNamespaceResponse> {
    let resp = admin_request(client, opt, format!("{}/api/stat_namespace", opt.admin_api))
        .json(&json!({ "key": key }))
        .send()
        .await
        .context("stat namespace request")?;
    if !resp.status().is_success() {
        anyhow::bail!("stat namespace {key} failed: {}", resp.text().await?);
    }
    Ok(resp
        .json()
        .await
        .context("decode stat namespace response")?)
}

fn unique_name(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    format!("{prefix}-{nanos}")
}

async fn run_case(opt: &Opt, mutate: bool) -> Result<()> {
    let http = Client::new();
    let suffix = if mutate { "different" } else { "same" };
    let base_ns = unique_name(&format!("overlay-regression-base-{suffix}"));
    let child_ns = unique_name(&format!("overlay-regression-child-{suffix}"));

    let result = async {
        create_namespace(&http, opt, &base_ns).await?;

        let base_client = plain_client(&opt.data_plane, &base_ns)?;
        let mut base_txn = base_client.create_transaction(None).await?;
        let page = vec![b'X'; 4096];
        base_txn.write_many(&[(1, page.as_slice())]).await?;
        let base_commit = base_txn
            .commit(None, &HashMap::new())
            .await
            .context("commit base page")?;
        let base_version = match base_commit {
            CommitOutput::Committed(result) => result.version,
            CommitOutput::Conflict => anyhow::bail!("base commit conflicted unexpectedly"),
            CommitOutput::Empty => anyhow::bail!("base commit was unexpectedly empty"),
        };

        let stat = stat_namespace(&http, opt, &base_ns).await?;
        create_overlay_namespace(&http, opt, &child_ns, &stat.nsid, &base_version).await?;

        let child_client = plain_client(&opt.data_plane, &child_ns)?;
        let mut child_txn = child_client.create_transaction(None).await?;
        let pages = child_txn.read_many_nomark(&[1]).await?;
        let child_page = pages.first().context("child read returned no pages")?;
        if child_page != &page {
            anyhow::bail!("overlay read mismatch");
        }

        let next_page = if mutate {
            let mut changed = child_page.clone();
            changed[0] = b'Y';
            changed
        } else {
            child_page.clone()
        };
        child_txn.write_many(&[(1, next_page.as_slice())]).await?;
        match child_txn.commit(None, &HashMap::new()).await? {
            CommitOutput::Committed(_) => Ok(()),
            CommitOutput::Conflict => Err(anyhow!("child commit conflicted unexpectedly")),
            CommitOutput::Empty => Err(anyhow!("child commit was unexpectedly empty")),
        }
    }
    .await;

    if let Err(err) = delete_namespace(&http, opt, &child_ns).await {
        eprintln!("cleanup warning: {err:#}");
    }
    if let Err(err) = delete_namespace(&http, opt, &base_ns).await {
        eprintln!("cleanup warning: {err:#}");
    }

    result
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse()?;
    run_case(&opt, false)
        .await
        .context("same-byte overlay rewrite should commit successfully")?;
    run_case(&opt, true)
        .await
        .context("different-byte overlay rewrite should commit successfully")?;
    println!("overlay same-page regression passed");
    Ok(())
}
