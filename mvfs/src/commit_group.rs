use std::{
    cell::RefCell,
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};

use anyhow::{bail, Context, Result};
use futures::future::FutureExt;
use mvclient::{CommitResult, MultiVersionClient, NamespaceCommitIntent};
use reqwest::Url;

pub struct CommitGroup {
    intents: Vec<NamespaceCommitIntent>,
    client: Option<Arc<MultiVersionClient>>,
    dp: Option<Url>,
    result_slot: Arc<CommitGroupResultSlot>,
}

impl Default for CommitGroup {
    fn default() -> Self {
        Self {
            intents: Vec::new(),
            client: None,
            dp: None,
            result_slot: Arc::new(CommitGroupResultSlot {
                inner: OnceLock::new(),
            }),
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

/// Shared slot that `commit()` writes into and each participating
/// `Connection` reads from on its next `lock()` call.  Thread-safe
/// because connections may migrate threads after the commit group ends.
pub struct CommitGroupResultSlot {
    inner: OnceLock<CommitGroupResultData>,
}

struct CommitGroupResultData {
    version: String,
    /// Per-namespace changelog of pages modified by *concurrent* transactions
    /// between each namespace's assumed version and the committed version.
    /// A missing entry means the interval was too large to compute.
    changelog: HashMap<String, Vec<u32>>,
}

impl CommitGroupResultSlot {
    /// Returns the committed version and the changelog entry for `ns_key`.
    /// `changelog` is `None` when the server could not compute the interval;
    /// the caller should do a full cache invalidation in that case.
    pub fn get_for_ns(&self, ns_key: &str) -> Option<(String, Option<Vec<u32>>)> {
        self.inner.get().map(|data| {
            (data.version.clone(), data.changelog.get(ns_key).cloned())
        })
    }
}

thread_local! {
    static CURRENT_COMMIT_GROUP: RefCell<Option<CommitGroup>> = RefCell::new(None);
}

pub fn begin() -> Result<()> {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        if cg.is_some() {
            bail!("mv_commit_group_begin called recursively in a commit group");
        }
        *cg = Some(CommitGroup::default());
        Ok(())
    })
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
) -> Result<Arc<CommitGroupResultSlot>> {
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

        let slot = cg.result_slot.clone();
        cg.intents.push(intent);
        Ok(slot)
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

    let result_slot = cg.result_slot.clone();

    let client = cg
        .client
        .take()
        .context("mv_commit_group_commit called without a client")?;
    let result = run(client
        .apply_commit_intents(cg.dp.as_ref(), &cg.intents)
        .boxed())?;

    Ok(match result {
        Some(result) => {
            let _ = result_slot.inner.set(CommitGroupResultData {
                version: result.version.clone(),
                changelog: result.changelog.clone(),
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

    #[test]
    fn result_slot_empty_before_commit() {
        let slot = Arc::new(CommitGroupResultSlot {
            inner: OnceLock::new(),
        });
        assert!(slot.get_for_ns("test").is_none());
    }

    #[test]
    fn result_slot_returns_changelog_after_population() {
        let slot = Arc::new(CommitGroupResultSlot {
            inner: OnceLock::new(),
        });

        let _ = slot.inner.set(CommitGroupResultData {
            version: "abc123".into(),
            changelog: HashMap::from([("ns1".into(), vec![1, 2, 3])]),
        });

        // Participating namespace gets version + changelog
        let (version, changelog) = slot.get_for_ns("ns1").unwrap();
        assert_eq!(version, "abc123");
        assert_eq!(changelog, Some(vec![1, 2, 3]));

        // Non-participating namespace gets version but no changelog entry
        let (version, changelog) = slot.get_for_ns("ns2").unwrap();
        assert_eq!(version, "abc123");
        assert!(changelog.is_none());
    }

    #[test]
    fn append_intent_returns_shared_slot() {
        begin().unwrap();

        let slot1 = append_intent(&test_client(), None, test_intent()).unwrap();
        let slot2 = append_intent(&test_client(), None, test_intent()).unwrap();

        // Both intents share the same slot
        assert!(Arc::ptr_eq(&slot1, &slot2));

        rollback().unwrap();
    }
}
