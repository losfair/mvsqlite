mod moka_backend;

#[cfg(test)]
mod lib_test;

use std::{future::Future, pin::Pin};

use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;

pub type CacheLoader = Box<
    dyn FnOnce(Option<[u8; 10]>) -> Pin<Box<dyn Future<Output = Result<LoadOutput>> + Send>> + Send,
>;

pub enum LoadOutput {
    Fresh,
    Replace {
        version: [u8; 10],
        data: Option<Bytes>,
    },
}

#[async_trait]
pub trait VersionedPageCache {
    async fn get(&self, key: u32, load: CacheLoader) -> Result<Option<Bytes>>;
    fn mark_all_as_stale(&mut self);
    async fn invalidate(&mut self, keys: &[u32]);
}
