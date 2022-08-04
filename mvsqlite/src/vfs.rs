use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use mvclient::{MultiVersionClient, MultiVersionClientConfig, Transaction};

use crate::{
    io_engine::IoEngine,
    sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, Vfs, WalDisabled},
};

const PAGE_SIZE: usize = 8192;
const TRANSITION_HISTORY_SIZE: usize = 10;
static FIRST_PAGE_TEMPLATE: &'static [u8; 8192] = include_bytes!("../template.db");

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub io: Arc<IoEngine>,
}

#[derive(Serialize, Deserialize, Default, Clone)]
pub struct NsMetadata {
    pub lock: Option<u64>,
}

impl Vfs for MultiVersionVfs {
    type Handle = Box<Connection>;

    fn open(
        &self,
        db: &str,
        opts: crate::sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        tracing::debug!(kind = ?opts.kind, access = ?opts.access, db = db, "open db");
        if !matches!(opts.kind, OpenKind::MainDb) {
            return Err(ErrorKind::NotFound.into());
        }

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
        let client = MultiVersionClient::new(MultiVersionClientConfig {
            data_plane: self
                .data_plane
                .parse()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
            ns_key: ns_key.to_string(),
        })
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        let conn = Connection {
            client,
            io: self.io.clone(),
            fixed_version,
            txn: None,
            lock: LockKind::None,
            history: TransitionHistory::default(),
            txn_buffered_page: HashMap::new(),
            txn_metadata: None,
            mvcc_aware: false,
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
        panic!("Not implemented: temporary_name")
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
}

pub struct Connection {
    client: Arc<MultiVersionClient>,
    io: Arc<IoEngine>,
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
    history: TransitionHistory,

    /// Clear this when writing or dropping txn!
    txn_buffered_page: HashMap<u32, Vec<u8>>,
    txn_metadata: Option<NsMetadata>,

    mvcc_aware: bool,
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
    fn do_read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % PAGE_SIZE == 0);
        assert!(buf.len() == PAGE_SIZE);

        let txn = self.txn.as_mut().unwrap();
        let num_pages = buf.len() / PAGE_SIZE;
        let page_offset = u32::try_from(offset as usize / PAGE_SIZE).unwrap();

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
                buf.copy_from_slice(FIRST_PAGE_TEMPLATE);
            } else {
                buf.iter_mut().for_each(|b| *b = 0);
            }
        } else {
            buf.copy_from_slice(&page);
        }
        Ok(())
    }
}

impl DatabaseHandle for Box<Connection> {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, std::io::Error> {
        Ok(PAGE_SIZE as u64 * u32::MAX as u64)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        if self.txn.is_none() {
            let len = buf.len();
            buf.copy_from_slice(&FIRST_PAGE_TEMPLATE[offset as usize..(offset as usize) + len]);
            return Ok(());
        }

        if offset as usize % PAGE_SIZE != 0 || buf.len() % PAGE_SIZE != 0 {
            // special case
            assert!(
                (offset as usize) < PAGE_SIZE
                    && buf.len() < PAGE_SIZE
                    && offset as usize + buf.len() <= PAGE_SIZE,
                "unexpected read"
            );
            let mut page_buf = vec![0; PAGE_SIZE];
            self.do_read(&mut page_buf, 0)?;
            buf.copy_from_slice(&page_buf[offset as usize..offset as usize + buf.len()]);
            return Ok(());
        }

        self.do_read(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % PAGE_SIZE == 0);
        assert!(buf.len() % PAGE_SIZE == 0);

        self.txn_buffered_page = HashMap::new();
        self.history.prev_index = 0;

        let num_pages = buf.len() / PAGE_SIZE;
        let page_offset = offset as usize / PAGE_SIZE;
        let txn = self
            .txn
            .as_mut()
            .expect("Cannot write to a database without a transaction");

        if offset == 0 {
            // validate first page
            let page_size = u16::from_be_bytes(<[u8; 2]>::try_from(&buf[16..18]).unwrap());
            if page_size as usize != PAGE_SIZE {
                panic!("attempting to change page size");
            }

            if buf[18] == 2 || buf[19] == 2 {
                panic!("attempting to enable wal mode");
            }
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
                    &buf[i * PAGE_SIZE..(i + 1) * PAGE_SIZE],
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
                Ok((self.client.create_transaction_at_version(version), None))
            } else {
                let client = self.client.clone();
                self.io
                    .run(async move { client.create_transaction_with_metadata().await })
                    .map(|(txn, md)| (txn, Some(md)))
            };
            let (txn, md) = match txn_info {
                Ok(x) => x,
                Err(e) => {
                    panic!(
                        "unrecoverable transaction initialization failure on nskey {}: {}",
                        self.client.config().ns_key,
                        e
                    );
                }
            };
            let md: Option<NsMetadata> = md.map(|x| serde_json::from_str(&x).unwrap_or_default());
            self.txn = Some(txn);
            self.txn_metadata = md;
        }

        let reserved_level = lock_level(LockKind::Reserved);

        if lock_level(self.lock) < reserved_level && lock_level(lock) >= reserved_level {
            let md = match self.txn_metadata.as_mut() {
                Some(x) => x,
                None => {
                    tracing::error!(
                        "cannot promote the lock on a fixed-version transaction to exclusive"
                    );
                    return Ok(false);
                }
            };

            if !self.mvcc_aware {
                // Acquire a one-minute lock.
                // TODO: Lock renew
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                if let Some(lock) = md.lock {
                    if now < lock {
                        tracing::error!("failed to acquire lock: hold by another connection");
                        return Ok(false);
                    }
                }

                let mut new_md = md.clone();
                new_md.lock = Some(now + 60);
                let metadata =
                    serde_json::to_string(&new_md).expect("failed to serialize metadata");
                let txn = self.txn.as_ref().unwrap();
                let lock_res = self.io.run(async {
                    let lock_txn = self.client.create_transaction_at_version(txn.version());
                    lock_txn
                        .commit(Some(metadata.as_str()))
                        .await
                        .expect("lock txn commit failed")
                });
                match lock_res {
                    Some(info) => {
                        self.txn = Some(self.client.create_transaction_at_version(&info.version));
                        self.lock = lock;
                        *md = new_md;
                        return Ok(true);
                    }
                    None => {
                        tracing::error!("failed to acquire lock: conflict");
                        return Ok(false);
                    }
                }
            } else {
                self.lock = lock;
            }
        } else {
            self.lock = lock;
        }

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

        if lock_level(prev_lock) >= reserved_level && lock_level(lock) < reserved_level {
            // Write
            let md = if !self.mvcc_aware {
                if let Some(md) = self.txn_metadata.as_mut() {
                    md.lock = None;
                    Some(serde_json::to_string(md).expect("failed to serialize metadata"))
                } else {
                    None
                }
            } else {
                None
            };
            let txn = self
                .txn
                .take()
                .expect("did not find transaction for commit");
            let result = self
                .io
                .run(async { txn.commit(md.as_ref().map(|x| x.as_str())).await });

            // At this point we don't have a reliable way to propagate the error. So, we have
            // to abort the process.
            let result = result.expect("transaction commit failed");
            let result = result.expect("transaction conflict");
            tracing::info!(version = result.version, duration = ?result.duration, num_pages = result.num_pages, "transaction committed");
            self.txn = Some(self.client.create_transaction_at_version(&result.version));
        }

        if lock == LockKind::None {
            // All locks dropped
            self.txn.take().expect("unlocked without locking");
            self.txn_buffered_page = HashMap::new();
            self.txn_metadata = None;
            self.history.prev_index = 0;
        }

        Ok(true)
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

fn lock_level(kind: LockKind) -> u32 {
    match kind {
        LockKind::None => 0,
        LockKind::Shared => 1,
        LockKind::Reserved => 2,
        LockKind::Pending => 3,
        LockKind::Exclusive => 4,
    }
}
