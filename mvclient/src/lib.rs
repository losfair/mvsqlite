use anyhow::{Context, Result};
use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    RequestBuilder, Url,
};
use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};

pub struct MultiVersionClient {
    client: reqwest::Client,
    config: MultiVersionClientConfig,
}

#[derive(Clone, Debug)]
pub struct MultiVersionClientConfig {
    /// Data plane URL.
    pub data_plane: Url,

    /// Namespace key.
    pub ns_key: String,
}

#[derive(Deserialize)]
pub struct StatResponse {
    pub version: String,
    pub metadata: String,
}

#[derive(Serialize)]
pub struct ReadRequest<'a> {
    pub page_index: u32,
    pub version: &'a str,

    #[serde(default)]
    #[serde(with = "serde_bytes")]
    pub hash: Option<&'a [u8]>,
}

#[derive(Deserialize)]
pub struct ReadResponse<'a> {
    pub version: &'a str,
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Serialize)]
pub struct WriteRequest<'a> {
    #[serde(with = "serde_bytes")]
    pub data: &'a [u8],
}

#[derive(Deserialize)]
pub struct WriteResponse<'a> {
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

#[derive(Serialize)]
pub struct CommitInit<'a> {
    pub version: &'a str,
    pub metadata: Option<&'a str>,
    pub num_pages: u32,
}

#[derive(Serialize)]
pub struct CommitRequest<'a> {
    pub page_index: u32,
    #[serde(with = "serde_bytes")]
    pub hash: &'a [u8],
}

impl MultiVersionClient {
    pub fn new(config: MultiVersionClientConfig) -> Result<Arc<Self>> {
        let mut headers = HeaderMap::new();
        headers.insert("x-namespace-key", HeaderValue::from_str(&config.ns_key)?);

        let client = reqwest::ClientBuilder::new()
            .default_headers(headers)
            .build()?;
        Ok(Arc::new(Self { client, config }))
    }

    pub async fn create_transaction(self: &Arc<Self>) -> Result<Transaction> {
        let mut url = self.config.data_plane.clone();
        url.set_path("/stat");

        let stat_res: StatResponse = request_and_check(self.client.get(url))
            .await?
            .json()
            .await?;

        tracing::debug!(
            version = stat_res.version,
            metadata = stat_res.metadata,
            "created transaction"
        );
        Ok(self.create_transaction_at_version(&stat_res.version))
    }

    pub fn create_transaction_at_version(self: &Arc<Self>, version: &str) -> Transaction {
        let txn = Transaction {
            c: self.clone(),
            version: version.into(),
            page_buffer: HashMap::new(),
        };

        txn
    }
}

pub struct Transaction {
    c: Arc<MultiVersionClient>,
    version: String,
    page_buffer: HashMap<u32, [u8; 32]>,
}

impl Transaction {
    pub fn version(&self) -> &str {
        self.version.as_str()
    }

    pub async fn read_many(&self, page_id_list: &[u32]) -> Result<Vec<Vec<u8>>> {
        let mut raw_request: Vec<u8> = Vec::new();

        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/read");

        for &page_index in page_id_list {
            let buffered = self.page_buffer.get(&page_index).map(|x| x.as_slice());
            let req = ReadRequest {
                page_index,
                version: self.version.as_str(),
                hash: buffered,
            };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }
        let response = request_and_check(self.c.client.post(url).body(raw_request)).await?;
        let raw_response = response.bytes().await?;
        let mut raw_response = &raw_response[..];
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(page_id_list.len());
        while !raw_response.is_empty() {
            let len = raw_response.read_u32::<BigEndian>()? as usize;
            let serialized = &raw_response[..len];
            raw_response = &raw_response[len..];

            let data: ReadResponse = rmp_serde::from_slice(serialized)?;
            out.push(data.data.to_vec());
        }

        if out.len() != page_id_list.len() {
            return Err(anyhow::anyhow!("response length mismatch"));
        }
        Ok(out)
    }

    pub async fn write_many(&mut self, pages: &[(u32, &[u8])]) -> Result<Vec<[u8; 32]>> {
        let mut raw_request: Vec<u8> = Vec::new();
        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/write");
        for &(_, data) in pages {
            let req = WriteRequest { data };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }
        raw_request.write_u32::<BigEndian>(0)?;

        let response = request_and_check(self.c.client.post(url).body(raw_request)).await?;
        let raw_response = response.bytes().await?;
        let mut raw_response = &raw_response[..];
        let mut out: Vec<[u8; 32]> = Vec::with_capacity(pages.len());
        while !raw_response.is_empty() {
            let len = raw_response.read_u32::<BigEndian>()? as usize;
            let serialized = &raw_response[..len];
            raw_response = &raw_response[len..];
            let data: WriteResponse = rmp_serde::from_slice(serialized)?;

            if data.hash.is_empty() {
                if out.len() != pages.len() {
                    anyhow::bail!("got unexpected completion");
                }
                for ((page_index, _), out) in pages.iter().zip(out.iter()) {
                    self.page_buffer.insert(*page_index, *out);
                }
                return Ok(out);
            }
            let hash = <[u8; 32]>::try_from(&data.hash[..])?;
            out.push(hash);
        }
        anyhow::bail!("incomplete write");
    }

    pub async fn commit(self) -> Result<()> {
        if self.page_buffer.is_empty() {
            return Ok(());
        }
        let mut url = self.c.config.data_plane.clone();
        url.set_path("/batch/commit");
        let mut raw_request: Vec<u8> = Vec::new();

        let init = CommitInit {
            version: self.version.as_str(),
            metadata: None,
            num_pages: self.page_buffer.len() as u32,
        };

        {
            let serialized = rmp_serde::to_vec_named(&init)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        for (&page_index, hash) in self.page_buffer.iter() {
            let req = CommitRequest { page_index, hash };
            let serialized = rmp_serde::to_vec_named(&req)?;
            raw_request.write_u32::<BigEndian>(serialized.len() as u32)?;
            raw_request.extend_from_slice(&serialized);
        }

        let response = request_and_check(self.c.client.post(url).body(raw_request)).await?;
        let committed_version = response
            .headers()
            .get("x-committed-version")
            .with_context(|| format!("missing committed version header"))?
            .to_str()?;
        tracing::debug!(version = committed_version, "committed transaction");
        Ok(())
    }
}

async fn request_and_check(r: RequestBuilder) -> Result<reqwest::Response> {
    let res = r.send().await?;
    if res.status().is_success() {
        Ok(res)
    } else {
        let status = res.status();
        let text = res.text().await?;
        Err(anyhow::anyhow!("remote error: {}: {}", status, text))
    }
}
