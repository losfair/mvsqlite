use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
};

use anyhow::{bail, Context, Result};
use futures::future::FutureExt;
use mvclient::{CommitResult, MultiVersionClient, NamespaceCommitIntent};
use reqwest::Url;

pub struct CommitGroup {
    intents: Vec<NamespaceCommitIntent>,
    client: Option<Arc<MultiVersionClient>>,
    dp: Option<Url>,
}

impl Default for CommitGroup {
    fn default() -> Self {
        Self {
            intents: Vec::new(),
            client: None,
            dp: None,
        }
    }
}

pub enum TransactionStart {
    Normal,
    Reject,
}

pub enum CommitOutput {
    Empty,
    Committed(CommitResult),
    Conflict,
}

/// Stores the result of the last successful commit group so that participating
/// connections can update their page caches and `last_known_write_version` when
/// they next acquire a lock.
struct CommitGroupResult {
    version: String,
    /// Namespace keys that staged intents in this commit group.
    ns_keys: Vec<String>,
    /// Per-namespace changelog of pages modified by *concurrent* transactions
    /// between each namespace's assumed version and the committed version.
    /// A missing entry means the interval was too large to compute.
    changelog: HashMap<String, Vec<u32>>,
}

thread_local! {
    static CURRENT_COMMIT_GROUP: RefCell<Option<CommitGroup>> = RefCell::new(None);
    static LAST_COMMIT_GROUP_RESULT: RefCell<Option<CommitGroupResult>> = RefCell::new(None);
}

pub fn begin() -> Result<()> {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        if cg.is_some() {
            bail!("mv_commit_group_begin called recursively in a commit group");
        }
        *cg = Some(CommitGroup::default());
        Ok(())
    })?;
    LAST_COMMIT_GROUP_RESULT.with(|r| {
        *r.borrow_mut() = None;
    });
    Ok(())
}

pub fn is_active() -> bool {
    CURRENT_COMMIT_GROUP.with(|cg| cg.borrow().is_some())
}

pub fn transaction_start() -> TransactionStart {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let cg = cg.borrow();
        match &*cg {
            Some(cg) if !cg.intents.is_empty() => TransactionStart::Reject,
            Some(_) => TransactionStart::Normal,
            None => TransactionStart::Normal,
        }
    })
}

pub fn append_intent(
    client: &Arc<MultiVersionClient>,
    dp: Option<&Url>,
    intent: NamespaceCommitIntent,
) -> Result<()> {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        let cg = cg
            .as_mut()
            .context("transaction commit attempted without a commit group open")?;

        if cg.client.is_none() {
            cg.client = Some(client.clone());
        }
        if cg.dp.is_none() {
            cg.dp = dp.cloned();
        }

        cg.intents.push(intent);
        Ok(())
    })
}

pub fn commit(
    run: impl for<'a> FnOnce(
        Pin<Box<dyn Future<Output = Result<Option<CommitResult>>> + Send + 'a>>,
    ) -> Result<Option<CommitResult>>,
) -> Result<CommitOutput> {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| cg.borrow_mut().take());
    let mut cg = cg.context("mv_commit_group_commit called without a commit group open")?;

    if cg.intents.is_empty() {
        return Ok(CommitOutput::Empty);
    }

    let ns_keys: Vec<String> = cg.intents.iter().map(|i| i.init.ns_key.clone()).collect();

    let client = cg
        .client
        .take()
        .context("mv_commit_group_commit called without a client")?;
    let result = run(client
        .apply_commit_intents(cg.dp.as_ref(), &cg.intents)
        .boxed())?;

    Ok(match result {
        Some(result) => {
            LAST_COMMIT_GROUP_RESULT.with(|r| {
                *r.borrow_mut() = Some(CommitGroupResult {
                    version: result.version.clone(),
                    ns_keys,
                    changelog: result.changelog.clone(),
                });
            });
            CommitOutput::Committed(result)
        }
        None => CommitOutput::Conflict,
    })
}

pub fn rollback() -> Result<()> {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| cg.borrow_mut().take());
    let _cg = cg.context("mv_commit_group_rollback called without a commit group open")?;
    Ok(())
}

/// If a commit group recently succeeded and `ns_key` participated in it,
/// returns `(committed_version, changelog)`.  `changelog` is `None` when
/// the server could not compute the interval (too many concurrent changes);
/// in that case the caller should do a full cache invalidation.
pub fn take_commit_group_result_for_ns(ns_key: &str) -> Option<(String, Option<Vec<u32>>)> {
    LAST_COMMIT_GROUP_RESULT.with(|r| {
        let r = r.borrow();
        r.as_ref().and_then(|result| {
            if result.ns_keys.iter().any(|k| k == ns_key) {
                let changelog = result.changelog.get(ns_key).cloned();
                Some((result.version.clone(), changelog))
            } else {
                None
            }
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mvclient::MultiVersionClientConfig;

    fn test_client() -> Arc<MultiVersionClient> {
        MultiVersionClient::new(
            MultiVersionClientConfig {
                data_plane: vec![Url::parse("http://localhost:7000").unwrap()],
                ns_key: "test".into(),
                ns_key_hashproof: None,
                lock_owner: None,
            },
            reqwest::Client::new(),
        )
        .unwrap()
    }

    fn test_intent() -> NamespaceCommitIntent {
        NamespaceCommitIntent {
            init: mvclient::CommitNamespaceInit {
                ns_key: "test".into(),
                ns_key_hashproof: None,
                version: "v1".into(),
                metadata: None,
                num_pages: 1,
                read_set: None,
            },
            requests: vec![mvclient::CommitRequest {
                page_index: 1,
                hash: vec![0; 32],
                data: None,
            }],
        }
    }

    #[test]
    fn rollback_clears_active_group_with_intents() {
        begin().unwrap();
        append_intent(&test_client(), None, test_intent()).unwrap();

        rollback().unwrap();
        assert!(!is_active());
    }

    #[test]
    fn rollback_clears_empty_active_group() {
        begin().unwrap();

        rollback().unwrap();
        assert!(!is_active());
    }

    #[test]
    fn transaction_start_stays_normal_until_first_intent() {
        begin().unwrap();
        assert!(matches!(transaction_start(), TransactionStart::Normal));

        append_intent(&test_client(), None, test_intent()).unwrap();
        assert!(matches!(transaction_start(), TransactionStart::Reject));

        rollback().unwrap();
    }
}
