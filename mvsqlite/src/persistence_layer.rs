use async_trait::async_trait;
use anyhow::Result;

#[async_trait]
pub trait PersistenceLayer: Sync {
    async fn open(&self, name: &str) -> Result<Box<dyn PersistenceConnection>, std::io::Error>;
}

#[async_trait]
pub trait PersistenceConnection: Sync {

}

pub enum BranchKind {
    Main,
    Fork,
}
