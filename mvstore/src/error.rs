use thiserror::Error;

#[derive(Debug, Error)]
#[error("unretryable error")]
pub struct UnretryableError(#[source] pub anyhow::Error);
