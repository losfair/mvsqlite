use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use mvclient::{CommitOutput, MultiVersionClient, MultiVersionClientConfig, Transaction};

use crate::{
    commit_group::CURRENT_COMMIT_GROUP,
    io_engine::IoEngine,
    sqlite_vfs::{wip::WalIndex, DatabaseHandle, LockKind, OpenKind, Vfs, WalDisabled},
    tempfile::TempFile,
};

const TRANSITION_HISTORY_SIZE: usize = 10;
static FIRST_PAGE_TEMPLATE_4K: &'static [u8; 4096] = include_bytes!("../template_4k.db");
static FIRST_PAGE_TEMPLATE_8K: &'static [u8; 8192] = include_bytes!("../template_8k.db");
static FIRST_PAGE_TEMPLATE_16K: &'static [u8; 16384] = include_bytes!("../template_16k.db");
static FIRST_PAGE_TEMPLATE_32K: &'static [u8; 32768] = include_bytes!("../template_32k.db");

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub io: Arc<IoEngine>,
    pub sector_size: usize,
    pub http_client: reqwest::Client,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct NsMetadata {
    pub lock: Option<u64>,
}

impl Vfs for MultiVersionVfs {
    type Handle = Box<dyn DatabaseHandle<WalIndex = WalDisabled>>;

    fn open(
        &self,
        db: &str,
        opts: crate::sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
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

        tracing::info!(kind = ?opts.kind, access = ?opts.access, ns_key = ns_key, "open file");
        if !matches!(opts.kind, OpenKind::MainDb) {
            return Ok(Box::new(TempFile::new()));
        }

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
            io: self.io.clone(),
            fixed_version,
            txn: None,
            lock: LockKind::None,
            history: TransitionHistory::default(),
            txn_buffered_page: HashMap::new(),
            txn_metadata: None,
            sector_size: self.sector_size,
            first_page,
            file_change_counter: 0,
            page_0_cache: None,
            last_known_write_version: None,
        };
        let conn = Box::new(conn);

        Ok(conn)
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        tracing::debug!(db = db, "delete db");
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        tracing::debug!(db = db, "exists db");
        Ok(false)
    }

    fn temporary_name(&self) -> String {
        "[temp]".into()
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::Rng::fill(&mut rand::thread_rng(), buffer);
    }

    fn sleep(&self, duration: std::time::Duration) -> std::time::Duration {
        self.io.run(async {
            tokio::time::sleep(duration).await;
        });
        duration
    }

    fn sector_size(&self) -> usize {
        self.sector_size
    }
}

pub struct Connection {
    client: Arc<MultiVersionClient>,
    io: Arc<IoEngine>,
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
    history: TransitionHistory,
    sector_size: usize,

    /// Clear this when writing or dropping txn!
    txn_buffered_page: HashMap<u32, Vec<u8>>,
    txn_metadata: Option<NsMetadata>,

    first_page: Vec<u8>,

    file_change_counter: u32,
    page_0_cache: Option<Vec<u8>>,

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

    fn record_and_predict(&mut self, current_index: u32, depth: usize) -> HashSet<u32> {
        self.record(current_index);

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
        predictions
    }
}

impl Connection {
    fn do_read_raw(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % self.sector_size == 0);
        assert!(buf.len() == self.sector_size);

        if offset == 0 {
            if let Some(x) = &self.page_0_cache {
                buf.copy_from_slice(x);
                tracing::debug!("page 0 cache reuse");
                return Ok(());
            }
        }

        let txn = self.txn.as_mut().unwrap();
        let num_pages = buf.len() / self.sector_size;
        let page_offset = u32::try_from(offset as usize / self.sector_size).unwrap();

        tracing::debug!(
            num_pages = num_pages,
            page_offset = page_offset,
            txn_version = txn.version(),
            "read_exact_at"
        );

        let page: Vec<u8>;

        match self.txn_buffered_page.get(&page_offset) {
            Some(buffered_page) => {
                self.history.record(page_offset);
                tracing::debug!(index = page_offset, "prefetch hit");
                page = buffered_page.clone();
            }
            None => {
                let predicted_next = self.history.record_and_predict(page_offset, 10);
                tracing::debug!(index = page_offset, next = ?predicted_next, "prefetch miss");
                let mut read_vec: Vec<u32> = Vec::with_capacity(1 + predicted_next.len());
                read_vec.push(page_offset);
                for &predicted_next_page in &predicted_next {
                    read_vec.push(predicted_next_page);
                }

                let pages = self.io.run(async {
                    txn.read_many(&read_vec)
                        .await
                        .expect("unrecoverable read failure")
                });
                assert_eq!(pages.len(), 1 + predicted_next.len());

                let mut pages = pages.into_iter();
                page = pages.next().expect("missing page from read response");
                self.txn_buffered_page = predicted_next.into_iter().zip(pages).collect();
            }
        }

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

        if offset == 0 {
            self.page_0_cache = Some(buf.to_vec());
            tracing::info!(header = base64::encode(&buf[0..100]), "page 0 read");
        }
        Ok(())
    }

    fn do_read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.do_read_raw(buf, offset)?;
        if offset == 0 {
            buf[24..28].copy_from_slice(&self.file_change_counter.to_be_bytes());
            buf[92..96].copy_from_slice(&self.file_change_counter.to_be_bytes());
        }
        Ok(())
    }
}

impl DatabaseHandle for Connection {
    type WalIndex = WalDisabled;

    fn size(&mut self) -> Result<u64, std::io::Error> {
        if self.txn.is_none() {
            tracing::warn!("file_size called without a transaction");
            return Ok(self.sector_size as u64 * u32::MAX as u64);
        }
        let mut pzero = vec![0u8; self.sector_size];
        self.read_exact_at(&mut pzero, 0)?;
        let num_pages = u32::from_be_bytes(pzero[28..32].try_into().unwrap());
        tracing::debug!(num_pages, "db size");
        let size = self.sector_size as u64 * num_pages as u64;
        Ok(size)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
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
            self.do_read(&mut page_buf, 0)?;
            buf.copy_from_slice(&page_buf[offset as usize..offset as usize + buf.len()]);
            return Ok(());
        }

        self.do_read(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % self.sector_size == 0);
        assert!(buf.len() == self.sector_size);

        let mut buf = buf.to_vec();
        self.txn_buffered_page = HashMap::new();
        self.history.prev_index = 0;

        let num_pages = buf.len() / self.sector_size;
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

            self.file_change_counter = u32::from_be_bytes(buf[24..28].try_into().unwrap());
            buf[24..28].copy_from_slice(&[0u8; 4]);
            buf[92..96].copy_from_slice(&[0u8; 4]);
            if self.page_0_cache.is_some() && self.page_0_cache.as_ref().unwrap() == &buf {
                tracing::info!("page 0 identity write ignored");
                return Ok(());
            }

            self.page_0_cache = Some(buf.clone());
            tracing::info!(header = base64::encode(&buf[0..100]), "page 0 write");
        }

        tracing::debug!(
            num_pages = num_pages,
            page_offset = page_offset,
            txn_version = txn.version(),
            "write_all_at"
        );
        let pages = (0..num_pages)
            .map(|i| {
                (
                    (page_offset + i) as u32,
                    &buf[i * self.sector_size..(i + 1) * self.sector_size],
                )
            })
            .collect::<Vec<_>>();

        self.io.run(async {
            txn.write_many(&pages)
                .await
                .expect("unrecoverable write failure")
        });
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn set_len(&mut self, _size: u64) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        tracing::trace!(lock = ?lock, "lock");
        assert!(lock != LockKind::None);
        if self.lock == lock {
            return Ok(true);
        }

        if self.txn.is_none() {
            let txn_info = if let Some(version) = &self.fixed_version {
                Ok((
                    self.client.create_transaction_at_version(version, true),
                    None,
                ))
            } else {
                let mut early_fail = false;
                let res = CURRENT_COMMIT_GROUP.with(|cg| {
                    let cg = cg.borrow();
                    if let Some(cg) = &*cg {
                        if !cg.is_empty() {
                            tracing::error!("no new transaction can begin after the first commit in a commit group");
                            early_fail = true;
                            return None;
                        }
                            if let Some(version) = &cg.current_version {
                                return Some((
                                    self.client.create_transaction_at_version(version, false),
                                    None,
                                ));
                            }
                    }
                    None
                });
                if early_fail {
                    return Ok(false);
                }
                if let Some(res) = res {
                    Ok(res)
                } else {
                    let client = self.client.clone();
                    let res = self
                        .io
                        .run(async move { client.create_transaction_with_info().await })
                        .map(|(txn, md)| (txn, Some(md)));
                    if let Ok(res) = &res {
                        CURRENT_COMMIT_GROUP.with(|cg| {
                            let mut cg = cg.borrow_mut();
                            if let Some(cg) = &mut *cg {
                                cg.current_version.replace(res.0.version().to_string());
                            }
                        })
                    }
                    res
                }
            };
            let (mut txn, md) = match txn_info {
                Ok(x) => x,
                Err(e) => {
                    tracing::error!(ns_key = self.client.config().ns_key, error = %e, "transaction initialization failed");
                    return Ok(false);
                }
            };
            let md: Option<NsMetadata> =
                md.map(|x| serde_json::from_str(&x.metadata).unwrap_or_default());

            // Flush SQLite page cache
            if self.last_known_write_version.is_none()
                || self.last_known_write_version.as_ref().unwrap() != txn.version()
            {
                if self.last_known_write_version.is_some() {
                    tracing::warn!("non-local change detected, invalidating cache");
                }
                self.last_known_write_version = Some(txn.version().to_string());
                self.file_change_counter = self.file_change_counter.wrapping_add(1);
                self.page_0_cache = None;
            }

            txn.enable_read_set();
            self.txn = Some(txn);
            self.txn_metadata = md;
        }
        self.lock = lock;

        Ok(true)
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        tracing::trace!(lock = ?lock, "unlock");

        if lock == self.lock {
            return Ok(true);
        }
        let prev_lock = self.lock;
        self.lock = lock;

        let reserved_level = lock_level(LockKind::Reserved);
        let mut commit_ok = true;

        if lock_level(prev_lock) >= reserved_level && lock_level(lock) < reserved_level {
            // Write
            let md = if let Some(md) = self.txn_metadata.as_mut() {
                md.lock = None;
                Some(serde_json::to_string(md).expect("failed to serialize metadata"))
            } else {
                None
            };
            let mut txn = self
                .txn
                .take()
                .expect("did not find transaction for commit");

            let mut cg_ok = false;

            CURRENT_COMMIT_GROUP
                .with(|cg| {
                    let mut cg = cg.borrow_mut();
                    if let Some(cg) = &mut *cg {
                        cg_ok = true;
                        let intent = self.io.run(async { txn.commit_intent(md.clone()).await })?;
                        if let Some(intent) = intent {
                            cg.set_client_and_io(&self.client, &self.io);
                            cg.append(intent);
                            tracing::info!("added intent to commit group");
                        }
                    }
                    Ok::<(), anyhow::Error>(())
                })
                .expect("failed to append to commit group");

            if !cg_ok {
                let read_version = txn.version().to_string();

                // If it is unlikely that a plcc commit can succeed, disable it locally first to save
                // bandwidth and prevent message-too-large errors.
                if txn.read_write_set_total_size() > 2000 {
                    txn.disable_read_set();
                }

                let result = self
                    .io
                    .run(async { txn.commit(md.as_ref().map(|x| x.as_str())).await });

                let result = result.expect("transaction commit failed");
                match result {
                    CommitOutput::Committed(result) => {
                        self.last_known_write_version = Some(result.version.clone());
                        if result.last_version > read_version {
                            // Invalidate page cache
                            self.file_change_counter = self.file_change_counter.wrapping_add(1);
                            self.page_0_cache = None;
                            tracing::warn!(
                                "non-local concurrent transaction detected, invalidating cache"
                            );
                        }
                        tracing::info!(
                            version = result.version,
                            duration = ?result.duration,
                            num_pages = result.num_pages,
                            read_version,
                            last_version = result.last_version,
                            new_file_change_counter = self.file_change_counter,
                            "transaction committed");
                    }
                    CommitOutput::Conflict => {
                        tracing::warn!("transaction conflict");
                        commit_ok = false;
                    }
                    CommitOutput::Empty => {
                        tracing::info!("transaction is empty");
                    }
                }
            }
        }

        if lock == LockKind::None {
            // All locks dropped
            self.txn = None;
            self.txn_buffered_page = HashMap::new();
            self.txn_metadata = None;
            self.history.prev_index = 0;
        }

        Ok(commit_ok)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<crate::sqlite_vfs::LockKind, std::io::Error> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        unimplemented!("wal_index not implemented")
    }
}

impl<W: WalIndex> DatabaseHandle for Box<dyn DatabaseHandle<WalIndex = W>> {
    type WalIndex = W;

    fn size(&mut self) -> Result<u64, std::io::Error> {
        (**self).size()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        (**self).read_exact_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        (**self).write_all_at(buf, offset)
    }

    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error> {
        (**self).sync(data_only)
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        (**self).set_len(size)
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        (**self).lock(lock)
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        (**self).unlock(lock)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        (**self).reserved()
    }

    fn current_lock(&self) -> Result<LockKind, std::io::Error> {
        (**self).current_lock()
    }

    fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        (**self).wal_index(readonly)
    }
}

fn lock_level(kind: LockKind) -> u32 {
    match kind {
        LockKind::None => 0,
        LockKind::Shared => 1,
        LockKind::Reserved => 2,
        LockKind::Pending => 3,
        LockKind::Exclusive => 4,
    }
}
