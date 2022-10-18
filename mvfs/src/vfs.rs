use anyhow::{Context, Result};
use bytes::Bytes;
use moka::sync::Cache;
use reqwest::Url;
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use mvclient::{
    CommitOutput, MultiVersionClient, MultiVersionClientConfig, StatusCodeError,
    TimeToVersionResponse, Transaction,
};

use crate::types::LockKind;
const TRANSITION_HISTORY_SIZE: usize = 10;
static FIRST_PAGE_TEMPLATE_4K: &'static [u8; 4096] = include_bytes!("../template_4k.db");
static FIRST_PAGE_TEMPLATE_8K: &'static [u8; 8192] = include_bytes!("../template_8k.db");
static FIRST_PAGE_TEMPLATE_16K: &'static [u8; 16384] = include_bytes!("../template_16k.db");
static FIRST_PAGE_TEMPLATE_32K: &'static [u8; 32768] = include_bytes!("../template_32k.db");
pub static PAGE_CACHE_SIZE: AtomicUsize = AtomicUsize::new(5000);
pub static WRITE_CHUNK_SIZE: AtomicUsize = AtomicUsize::new(10);
pub static PREFETCH_DEPTH: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub enum AbstractHttpClient {
    Prebuilt(reqwest::Client),
    Builder(Arc<Box<dyn Fn() -> reqwest::Client + Send + Sync + 'static>>),
}

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub sector_size: usize,
    pub http_client: AbstractHttpClient,
    pub db_name_map: Arc<HashMap<String, String>>,
    pub lock_owner: Option<String>,
    pub fork_tolerant: bool,
}

impl MultiVersionVfs {
    pub fn open(&self, db: &str, map_name: bool) -> Result<Connection> {
        if map_name {
            if let Some(mapped) = self.db_name_map.get(db) {
                return self.open(mapped, false);
            }
        }

        let (dp, db) = if db.starts_with("http://") || db.starts_with("https://") {
            let url = Url::parse(db)?;
            let dp = Url::parse(&url.origin().ascii_serialization())?;
            (Some(dp), url.path().to_string())
        } else {
            (None, db.to_string())
        };
        let db = db.trim_start_matches("/");

        let db_str_segs = db.split("@").collect::<Vec<_>>();
        let (ns_key, fixed_version) = if db_str_segs.len() < 2 {
            (db_str_segs[0], None)
        } else {
            (
                db_str_segs[0],
                if db_str_segs[1].is_empty() {
                    None
                } else {
                    Some(db_str_segs[1].to_string())
                },
            )
        };
        let (ns_key, ns_key_hashproof) = {
            let segs = ns_key.split(":").collect::<Vec<_>>();
            if segs.len() < 2 {
                (ns_key.to_string(), None)
            } else {
                let first_part = segs[0];
                let segs = segs[1].split(".").collect::<Vec<_>>();
                if segs.len() < 2 {
                    (ns_key.to_string(), None)
                } else {
                    (
                        format!("{}:{}", first_part, segs[0]),
                        Some(segs[1].to_string()),
                    )
                }
            }
        };

        let client = MultiVersionClient::new(
            MultiVersionClientConfig {
                data_plane: self
                    .data_plane
                    .split(",")
                    .map(|s| {
                        s.parse()
                            .with_context(|| format!("failed to parse data plane address: {}", s))
                    })
                    .collect::<Result<Vec<_>>>()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                ns_key: ns_key.to_string(),
                ns_key_hashproof,
                lock_owner: self.lock_owner.clone(),
            },
            match &self.http_client {
                AbstractHttpClient::Prebuilt(c) => c.clone(),
                AbstractHttpClient::Builder(f) => f(),
            },
        )?;

        let first_page = match self.sector_size {
            4096 => FIRST_PAGE_TEMPLATE_4K.to_vec(),
            8192 => FIRST_PAGE_TEMPLATE_8K.to_vec(),
            16384 => FIRST_PAGE_TEMPLATE_16K.to_vec(),
            32768 => FIRST_PAGE_TEMPLATE_32K.to_vec(),
            _ => panic!("unsupported sector size"),
        };

        let conn = Connection {
            client,
            dp,
            fixed_version,
            txn: None,
            lock: LockKind::None,
            commit_confirmed: false,
            history: TransitionHistory::default(),
            sector_size: self.sector_size,
            first_page,
            page_cache: Cache::builder()
                .max_capacity(PAGE_CACHE_SIZE.load(Ordering::Relaxed) as u64)
                .thread_pool_enabled(!self.fork_tolerant)
                .build(),
            write_buffer: HashMap::new(),
            virtual_version_counter: 0,
            last_known_write_version: None,
        };
        Ok(conn)
    }

    pub fn sector_size(&self) -> usize {
        self.sector_size
    }
}

pub struct Connection {
    client: Arc<MultiVersionClient>,
    dp: Option<Url>,
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
    commit_confirmed: bool,
    history: TransitionHistory,
    sector_size: usize,

    first_page: Vec<u8>,

    page_cache: Cache<u32, Bytes>,
    write_buffer: HashMap<u32, Bytes>,
    virtual_version_counter: u32,

    last_known_write_version: Option<String>,
}

#[derive(Default)]
struct TransitionHistory {
    history: [i64; TRANSITION_HISTORY_SIZE],
    prev_index: u32,
}

impl TransitionHistory {
    fn predict(&self, current_index: u32) -> Vec<u32> {
        let mut count: HashMap<i64, u32> = HashMap::with_capacity(TRANSITION_HISTORY_SIZE);
        for &destination in self.history.iter() {
            *count.entry(destination).or_insert(0) += 1;
        }

        let mut items = count
            .iter()
            .filter_map(|(&destination, &count)| {
                let probability = (count as f64) / (TRANSITION_HISTORY_SIZE as f64);
                if probability >= 0.2 && destination != 0 {
                    Some((destination, count))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        items.sort_by_key(|x| -(x.1 as i64));
        items
            .iter()
            .filter_map(|x| u32::try_from(current_index as i64 + x.0).ok())
            .collect()
    }

    fn record(&mut self, current_index: u32) {
        let diff = current_index as i64 - self.prev_index as i64;
        self.history.rotate_right(1);
        self.history[0] = diff;
        self.prev_index = current_index;
    }

    fn multi_predict(&self, current_index: u32, depth: usize) -> HashSet<u32> {
        let mut predictions: HashSet<u32> = HashSet::with_capacity(depth * 2);
        let mut last = current_index;
        for _ in 0..depth {
            let local_pred = self.predict(last);
            if local_pred.is_empty() || predictions.contains(&local_pred[0]) {
                break;
            } else {
                last = local_pred[0];
                predictions.extend(local_pred);
            }
        }
        predictions.remove(&current_index);
        predictions
    }
}

impl Connection {
    pub fn last_known_version(&self) -> Option<&str> {
        self.last_known_write_version.as_deref()
    }

    pub async fn do_read_raw(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % self.sector_size == 0);
        assert!(buf.len() == self.sector_size);

        let num_pages = buf.len() / self.sector_size;
        let page_offset = u32::try_from(offset as usize / self.sector_size).unwrap();
        self.txn.as_mut().unwrap().mark_read(page_offset);
        self.history.record(page_offset);

        if let Some(x) = &self.page_cache.get(&page_offset) {
            buf.copy_from_slice(x);
            tracing::trace!("page cache hit");
            return Ok(());
        }

        if let Some(x) = &self.write_buffer.get(&page_offset) {
            buf.copy_from_slice(x);
            tracing::trace!("write buffer hit");
            return Ok(());
        }

        let txn = self.txn.as_mut().unwrap();

        tracing::debug!(
            num_pages = num_pages,
            page_offset = page_offset,
            txn_version = txn.version(),
            "read_exact_at"
        );

        let predict_depth: usize = 10;
        let prefetch_depth: usize = PREFETCH_DEPTH.load(Ordering::Relaxed);
        let mut predicted_next = self.history.multi_predict(page_offset, predict_depth);

        // Prefetch until the target depth
        {
            let mut i: u32 = 1;
            while predicted_next.len() < prefetch_depth {
                predicted_next.insert(page_offset + i);
                i += 1;
            }
        }

        let predicted_next = predicted_next
            .iter()
            .filter(|x| !self.page_cache.contains_key(*x))
            .copied()
            .collect::<Vec<_>>();
        tracing::debug!(index = page_offset, next = ?predicted_next, "prefetch miss");
        let mut read_vec: Vec<u32> = Vec::with_capacity(1 + predicted_next.len());
        read_vec.push(page_offset);
        for &predicted_next_page in &predicted_next {
            read_vec.push(predicted_next_page);
        }

        let pages = txn
            .read_many_nomark(&read_vec)
            .await
            .expect("unrecoverable read failure");
        assert_eq!(pages.len(), 1 + predicted_next.len());
        let page = &pages[0];

        if page.is_empty() {
            if offset == 0 {
                buf.copy_from_slice(&self.first_page);
            } else {
                panic!("read on non-existing page: offset={}", offset);
            }
        } else {
            if page.len() != self.sector_size {
                tracing::error!(
                    actual = page.len(),
                    sector_size = self.sector_size,
                    "page size mismatch"
                );
                return Err(std::io::Error::new(
                    ErrorKind::Other,
                    anyhow::anyhow!("page size mismatch"),
                ));
            }
            buf.copy_from_slice(&page);
        }

        self.insert_to_page_cache(page_offset, Bytes::copy_from_slice(&*buf));

        for (maybe_other_page, their_index) in pages.iter().skip(1).zip(read_vec.iter().skip(1)) {
            if *their_index != 0 && !maybe_other_page.is_empty() {
                self.insert_to_page_cache(
                    *their_index,
                    Bytes::copy_from_slice(&maybe_other_page[..]),
                );
            }
        }
        Ok(())
    }

    fn insert_to_page_cache(&mut self, page_index: u32, buf: Bytes) {
        self.page_cache.insert(page_index, buf);
    }

    pub async fn do_read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.do_read_raw(buf, offset).await?;
        if offset == 0 {
            buf[24..28].copy_from_slice(&self.virtual_version_counter.to_be_bytes());
            buf[92..96].copy_from_slice(&self.virtual_version_counter.to_be_bytes());
        }
        Ok(())
    }

    pub async fn force_flush_write_buffer(&mut self) {
        let txn = self.txn.as_mut().unwrap();

        if self.write_buffer.is_empty() {
            return;
        }

        let entries: Vec<(u32, &[u8])> =
            self.write_buffer.iter().map(|x| (*x.0, &x.1[..])).collect();

        for chunk in entries.chunks(WRITE_CHUNK_SIZE.load(Ordering::Relaxed)) {
            txn.write_many(chunk)
                .await
                .expect("unrecoverable write failure")
        }
        self.write_buffer.clear();
    }

    pub async fn maybe_flush_write_buffer(&mut self) {
        if self.write_buffer.len() >= 1000 {
            self.force_flush_write_buffer().await;
        }
    }

    pub async fn time2version(&mut self, timestamp: u64) -> TimeToVersionResponse {
        let res = self
            .client
            .time2version(self.dp.as_ref(), timestamp)
            .await
            .expect("unrecoverable time2version failure");
        res
    }

    pub fn pin_version(&mut self, version: String) -> Result<()> {
        if self.txn.is_some() {
            anyhow::bail!("cannot pin version while transaction is active");
        }

        self.fixed_version = Some(version);
        Ok(())
    }

    pub fn unpin_version(&mut self) -> Result<()> {
        if self.txn.is_some() {
            anyhow::bail!("cannot unpin version while transaction is active");
        }

        self.fixed_version = None;
        Ok(())
    }

    async fn finalize_transaction(&mut self, commit: bool) -> bool {
        if self.write_buffer.len() > WRITE_CHUNK_SIZE.load(Ordering::Relaxed) {
            self.force_flush_write_buffer().await;
        }

        let mut txn = self
            .txn
            .take()
            .expect("did not find transaction for commit");

        let read_version = txn.version().to_string();
        let ns_key = self.client.config().ns_key.as_str();

        // If it is unlikely that a plcc commit can succeed, disable it locally first to save
        // bandwidth and prevent message-too-large errors.
        if txn.read_set_size() > 2000 {
            txn.disable_read_set();
        }

        let dirty_pages = txn
            .written_pages()
            .iter()
            .copied()
            .chain(self.write_buffer.keys().copied())
            .collect::<HashSet<_>>();

        let mut discard_reason: Option<&'static str> = None;

        if !commit {
            // sqlite didn't confirm to commit
            discard_reason = Some("did not receive confirmation from sqlite");
        } else if self.fixed_version.is_some() {
            // Disallow write to snapshot
            discard_reason = Some("write to snapshot is dropped");
        }

        if let Some(discard_reason) = discard_reason {
            for index in &dirty_pages {
                self.page_cache.invalidate(index);
            }
            self.write_buffer.clear();
            tracing::warn!(
                dirty_page_count = dirty_pages.len(),
                reason = discard_reason,
                "discarding transaction"
            );
            return true;
        }

        let fast_write_size = self.write_buffer.len();
        let result = txn.commit(None, &self.write_buffer).await;
        self.write_buffer.clear();

        let result = result.expect("transaction commit failed");
        match result {
            CommitOutput::Committed(result) => {
                self.last_known_write_version = Some(result.version.clone());
                let changelog = result.changelog.get(ns_key);

                // Invalidate page cache
                if let Some(changelog) = changelog {
                    if !changelog.is_empty() {
                        for &index in changelog {
                            self.page_cache.invalidate(&index);
                        }
                        tracing::info!(
                            count = changelog.len(),
                            "non-local concurrent transaction detected, performed partial cache flush"
                        );
                    }
                } else {
                    // Changelog is not available - do a full flush
                    self.page_cache.invalidate_all();
                    tracing::warn!("non-local concurrent transaction detected, invalidating cache");
                }

                self.txn = Some(self.client.create_transaction_at_version(
                    self.dp.as_ref(),
                    &result.version,
                    false,
                ));

                tracing::info!(
                        version = result.version,
                        duration = ?result.duration,
                        num_pages = result.num_pages,
                        fast_write_size,
                        read_version,
                        "transaction committed");
                true
            }
            CommitOutput::Conflict => {
                for index in &dirty_pages {
                    self.page_cache.invalidate(index);
                }
                tracing::warn!(dirty_page_count = dirty_pages.len(), "transaction conflict");
                false
            }
            CommitOutput::Empty => {
                tracing::info!("transaction is empty");
                self.txn = Some(self.client.create_transaction_at_version(
                    self.dp.as_ref(),
                    &read_version,
                    false,
                ));
                true
            }
        }
    }
}

impl Connection {
    pub async fn size(&mut self) -> Result<u64, std::io::Error> {
        if self.txn.is_none() {
            tracing::warn!("file_size called without a transaction");
            return Ok(self.sector_size as _);
        }
        let mut pzero = vec![0u8; self.sector_size];
        self.read_exact_at(&mut pzero, 0).await?;
        let num_pages = u32::from_be_bytes(pzero[28..32].try_into().unwrap());
        tracing::debug!(num_pages, "db size");
        let size = self.sector_size as u64 * num_pages as u64;
        Ok(size)
    }

    pub async fn read_exact_at(
        &mut self,
        buf: &mut [u8],
        offset: u64,
    ) -> Result<(), std::io::Error> {
        if self.txn.is_none() {
            assert!(offset == 0);
            let buf_len = buf.len();

            if buf_len != 100 {
                tracing::warn!(
                    buf_len,
                    "read_exact_at on the first page called without a transaction with non-standard size"
                );
            }

            buf.copy_from_slice(&self.first_page[0..buf_len]);
            return Ok(());
        }

        if offset as usize % self.sector_size != 0 || buf.len() % self.sector_size != 0 {
            // special case
            assert!(
                (offset as usize) < self.sector_size
                    && buf.len() < self.sector_size
                    && offset as usize + buf.len() <= self.sector_size,
                "unexpected read"
            );
            let mut page_buf = vec![0; self.sector_size];
            self.do_read(&mut page_buf, 0).await?;
            buf.copy_from_slice(&page_buf[offset as usize..offset as usize + buf.len()]);
            return Ok(());
        }

        self.do_read(buf, offset).await
    }

    pub async fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % self.sector_size == 0);
        assert!(buf.len() == self.sector_size);

        let mut buf = buf.to_vec();
        self.history.prev_index = 0;

        let page_offset = offset as usize / self.sector_size;

        {
            let txn = self
                .txn
                .as_mut()
                .expect("Cannot write to a database without a transaction");

            if offset == 0 {
                // validate first page
                let page_size = u16::from_be_bytes(<[u8; 2]>::try_from(&buf[16..18]).unwrap());
                if page_size as usize != self.sector_size {
                    panic!("attempting to change page size");
                }

                if buf[18] == 2 || buf[19] == 2 {
                    panic!("attempting to enable wal mode");
                }

                buf[24..28].copy_from_slice(&[0u8; 4]);
                buf[92..96].copy_from_slice(&[0u8; 4]);
            }

            if let Some(x) = self.page_cache.get(&(page_offset as u32)) {
                if &x[..] == buf {
                    tracing::info!(page = page_offset, "identity write ignored");
                    return Ok(());
                }
            }
            tracing::trace!(
                page_offset = page_offset,
                txn_version = txn.version(),
                "write_all_at"
            );
        }

        let buf = Bytes::from(buf);

        self.insert_to_page_cache(page_offset as u32, buf.clone());
        self.write_buffer.insert(page_offset as u32, buf);

        self.maybe_flush_write_buffer().await;

        Ok(())
    }

    pub async fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        tracing::trace!(lock = ?lock, "lock");
        assert!(lock != LockKind::None);
        if self.lock == lock {
            return Ok(true);
        }

        if self.txn.is_none() {
            let mut interval: Option<Vec<u32>> = None;

            let txn_info = if let Some(version) = &self.fixed_version {
                Ok(self
                    .client
                    .create_transaction_at_version(self.dp.as_ref(), version, true))
            } else {
                let client = self.client.clone();
                let res = client
                    .create_transaction_with_info(
                        self.dp.as_ref(),
                        self.last_known_write_version.as_ref().map(|x| x.as_str()),
                    )
                    .await;
                match res {
                    Ok((txn, info)) => {
                        interval = info.interval;
                        Ok(txn)
                    }
                    Err(e) => Err(e),
                }
            };
            let mut txn = match txn_info {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(ns_key = self.client.config().ns_key, error = %e, "transaction initialization failed");

                    if let Some(sc_err) =
                        e.chain().find_map(|x| x.downcast_ref::<StatusCodeError>())
                    {
                        if sc_err.0.as_u16() == 410 {
                            tracing::error!(
                                "this client can no longer start transaction on this database"
                            );
                            return Err(std::io::Error::new(std::io::ErrorKind::Other, "error"));
                        }
                    }

                    return Ok(false);
                }
            };

            // Flush SQLite page cache
            if self.last_known_write_version.is_none()
                || self.last_known_write_version.as_ref().unwrap() != txn.version()
            {
                match interval {
                    Some(interval) => {
                        for index in &interval {
                            self.page_cache.invalidate(index);
                        }
                        tracing::info!(
                            count = interval.len(),
                            "non-local change detected, performed partial cache flush"
                        );
                    }
                    None => {
                        self.page_cache.invalidate_all();
                        if self.last_known_write_version.is_some() {
                            tracing::warn!("non-local change detected, invalidating cache");
                        }
                    }
                }
                self.last_known_write_version = Some(txn.version().to_string());
            }

            txn.enable_read_set();
            self.txn = Some(txn);

            // Ensure that SQLite's own cache is invalidated
            self.virtual_version_counter = self.virtual_version_counter.wrapping_add(2);
        }
        self.lock = lock;
        assert!(!self.commit_confirmed);

        Ok(true)
    }

    pub async fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        tracing::trace!(lock = ?lock, "unlock");

        if lock == self.lock {
            return Ok(true);
        }
        let prev_lock = self.lock;
        self.lock = lock;

        let reserved_level = LockKind::Reserved.level();
        let commit_ok = if prev_lock.level() >= reserved_level && lock.level() < reserved_level {
            let commit_confirmed = self.commit_confirmed;
            self.commit_confirmed = false;
            self.finalize_transaction(commit_confirmed).await
        } else {
            true
        };

        if lock == LockKind::None {
            // All locks dropped
            self.txn = None;
            self.history.prev_index = 0;
        }

        Ok(commit_ok)
    }

    pub fn confirm_commit(&mut self) {
        assert!(!self.commit_confirmed);
        self.commit_confirmed = true;
    }

    pub fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    pub fn current_lock(&self) -> LockKind {
        self.lock
    }
}
