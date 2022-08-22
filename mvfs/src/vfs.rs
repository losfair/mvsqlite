use moka::sync::Cache;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use mvclient::{CommitOutput, MultiVersionClient, MultiVersionClientConfig, Transaction};

use crate::types::LockKind;
const TRANSITION_HISTORY_SIZE: usize = 10;
static FIRST_PAGE_TEMPLATE_4K: &'static [u8; 4096] = include_bytes!("../template_4k.db");
static FIRST_PAGE_TEMPLATE_8K: &'static [u8; 8192] = include_bytes!("../template_8k.db");
static FIRST_PAGE_TEMPLATE_16K: &'static [u8; 16384] = include_bytes!("../template_16k.db");
static FIRST_PAGE_TEMPLATE_32K: &'static [u8; 32768] = include_bytes!("../template_32k.db");
pub static PAGE_CACHE_SIZE: AtomicUsize = AtomicUsize::new(5000);
pub static WRITE_CHUNK_SIZE: AtomicUsize = AtomicUsize::new(10);
pub static PREFETCH_DEPTH: AtomicUsize = AtomicUsize::new(10);

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub sector_size: usize,
    pub http_client: reqwest::Client,
}

impl MultiVersionVfs {
    pub fn open(&self, db: &str) -> Result<Connection, std::io::Error> {
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
                    .parse()
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                ns_key: ns_key.to_string(),
                ns_key_hashproof,
            },
            self.http_client.clone(),
        )
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let first_page = match self.sector_size {
            4096 => FIRST_PAGE_TEMPLATE_4K.to_vec(),
            8192 => FIRST_PAGE_TEMPLATE_8K.to_vec(),
            16384 => FIRST_PAGE_TEMPLATE_16K.to_vec(),
            32768 => FIRST_PAGE_TEMPLATE_32K.to_vec(),
            _ => panic!("unsupported sector size"),
        };

        let conn = Connection {
            client,
            fixed_version,
            txn: None,
            lock: LockKind::None,
            history: TransitionHistory::default(),
            sector_size: self.sector_size,
            first_page,
            page_cache: Cache::new(PAGE_CACHE_SIZE.load(Ordering::Relaxed) as u64),
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
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
    history: TransitionHistory,
    sector_size: usize,

    first_page: Vec<u8>,

    page_cache: Cache<u32, Arc<[u8]>>,
    write_buffer: HashMap<u32, Arc<[u8]>>,
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

        // Ensure we see the latest data
        self.force_flush_write_buffer().await;
        let txn = self.txn.as_mut().unwrap();

        tracing::debug!(
            num_pages = num_pages,
            page_offset = page_offset,
            txn_version = txn.version(),
            "read_exact_at"
        );

        let predict_depth: usize = 10;
        let prefetch_depth: usize = PREFETCH_DEPTH.load(Ordering::Relaxed);
        let mut predicted_next = self
            .history
            .multi_predict(page_offset, predict_depth)
            .iter()
            .filter(|x| !self.page_cache.contains_key(x))
            .copied()
            .collect::<HashSet<_>>();

        // Prefetch until the target depth
        {
            let mut i: u32 = 1;
            while predicted_next.len() < prefetch_depth {
                predicted_next.insert(page_offset + i);
                i += 1;
            }
        }
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
            if offset == 0 {
                let actual_page_size =
                    u16::from_be_bytes(page[16..18].try_into().unwrap()) as usize;
                if actual_page_size != self.sector_size {
                    panic!("page size mismatch with sector size. actual page size = {}, sector size = {}", actual_page_size, self.sector_size);
                }
            }
            buf.copy_from_slice(&page);
        }

        self.page_cache.insert(page_offset, Arc::from(&*buf));

        for (maybe_other_page, their_index) in pages.iter().skip(1).zip(read_vec.iter().skip(1)) {
            if *their_index != 0 && !maybe_other_page.is_empty() {
                self.page_cache
                    .insert(*their_index, Arc::from(&maybe_other_page[..]));
            }
        }
        Ok(())
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
            tracing::warn!(
                offset = offset,
                len = buf.len(),
                "read_exact_at called without a transaction"
            );
            let len = buf.len();
            buf.copy_from_slice(&self.first_page[offset as usize..(offset as usize) + len]);
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

        let mut buf_arc: Arc<[u8]> = Arc::from(buf);
        let buf = Arc::get_mut(&mut buf_arc).unwrap();
        self.history.prev_index = 0;

        let page_offset = offset as usize / self.sector_size;
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
            if &*x == &*buf {
                tracing::info!(page = page_offset, "identity write ignored");
                return Ok(());
            }
        }

        self.page_cache
            .insert(page_offset as u32, Arc::clone(&buf_arc));
        self.write_buffer
            .insert(page_offset as u32, Arc::clone(&buf_arc));

        tracing::trace!(
            page_offset = page_offset,
            txn_version = txn.version(),
            "write_all_at"
        );
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
                Ok(self.client.create_transaction_at_version(version, true))
            } else {
                let client = self.client.clone();
                let res = client
                    .create_transaction_with_info(
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
        let mut commit_ok = true;

        if prev_lock.level() >= reserved_level && lock.level() < reserved_level {
            // Write
            self.force_flush_write_buffer().await;

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

            let dirty_pages = txn.written_pages();

            let result = txn.commit(None).await;

            let result = result.expect("transaction commit failed");
            match result {
                CommitOutput::Committed(result) => {
                    self.last_known_write_version = Some(result.version.clone());
                    let changelog = result.changelog.get(ns_key);
                    if result.last_version > read_version {
                        // Invalidate page cache
                        if let Some(changelog) = changelog {
                            for &index in changelog {
                                self.page_cache.invalidate(&index);
                            }
                            tracing::info!(
                                    count = changelog.len(),
                                    "non-local concurrent transaction detected, performed partial cache flush"
                                );
                        } else {
                            // Changelog is not available - do a full flush
                            self.page_cache.invalidate_all();
                            tracing::warn!(
                                "non-local concurrent transaction detected, invalidating cache"
                            );
                        }
                    }
                    tracing::info!(
                            version = result.version,
                            duration = ?result.duration,
                            num_pages = result.num_pages,
                            read_version,
                            last_version = result.last_version,
                            "transaction committed");
                }
                CommitOutput::Conflict => {
                    for index in &dirty_pages {
                        self.page_cache.invalidate(index);
                    }
                    tracing::warn!(dirty_page_count = dirty_pages.len(), "transaction conflict");
                    commit_ok = false;
                }
                CommitOutput::Empty => {
                    tracing::info!("transaction is empty");
                }
            }
        }

        if lock == LockKind::None {
            // All locks dropped
            self.txn = None;
            self.history.prev_index = 0;
        }

        Ok(commit_ok)
    }

    pub fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    pub fn current_lock(&self) -> LockKind {
        self.lock
    }
}
