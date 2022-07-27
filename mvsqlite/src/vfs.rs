use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{Arc, Mutex, Weak},
};

use mvclient::{MultiVersionClient, MultiVersionClientConfig, Transaction};

use crate::{
    io_engine::IoEngine,
    sqlite_vfs::{DatabaseHandle, LockKind, OpenAccess, OpenKind, Vfs, WalDisabled},
};

const PAGE_SIZE: usize = 4096;
static FIRST_PAGE_TEMPLATE: &'static [u8; 4096] = include_bytes!("../template.db");

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub io: Arc<IoEngine>,
    pub shared: Arc<Mutex<SharedState>>,
}

#[derive(Default)]
pub struct SharedState {
    connections: HashMap<String, Arc<Mutex<ConnState>>>,
}

struct ConnState {
    txn_invalidated: bool,
}

impl Vfs for MultiVersionVfs {
    type Handle = ConnWrapper;

    fn open(
        &self,
        db: &str,
        opts: crate::sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        tracing::debug!(kind = ?opts.kind, access = ?opts.access, db = db, "open db");
        let mut shared = self.shared.lock().unwrap();
        if !matches!(opts.kind, OpenKind::MainDb) {
            match opts.access {
                OpenAccess::Read | OpenAccess::Write => {
                    return Err(ErrorKind::NotFound.into());
                }
                OpenAccess::Create | OpenAccess::CreateNew => {}
            }

            if matches!(opts.kind, OpenKind::MainJournal) {
                let db = db
                    .strip_suffix("-journal")
                    .expect("journal file does not have the expected suffix");
                let main = shared
                    .connections
                    .get(db)
                    .expect("main db not found for requested journal");
                let blackhole = BlackholeJournal {
                    state: Arc::downgrade(main),
                    size: 0,
                    lock: LockKind::None,
                    written: false,
                };
                return Ok(ConnWrapper {
                    inner: Box::new(blackhole),
                });
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Only main database is supported",
            ));
        }

        if shared.connections.contains_key(db) {
            panic!(
                "cannot open more than one connections to the same database: {}",
                db
            );
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
        let state = Arc::new(Mutex::new(ConnState {
            txn_invalidated: false,
        }));
        shared.connections.insert(db.to_string(), state.clone());
        let conn = Connection {
            client,
            db: db.to_string(),
            io: self.io.clone(),
            shared: self.shared.clone(),
            fixed_version,
            txn: None,
            lock: LockKind::None,
            state,
        };
        Ok(ConnWrapper {
            inner: Box::new(conn),
        })
    }

    fn delete(&self, _db: &str) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn exists(&self, db: &str) -> Result<bool, std::io::Error> {
        tracing::debug!(db = db, "exists db");
        Ok(
            if db.ends_with("-journal") || db.ends_with("-wal") || db.ends_with("-shm") {
                false
            } else {
                true
            },
        )
    }

    fn temporary_name(&self) -> String {
        panic!("Not implemented: temporary_name")
    }

    fn random(&self, buffer: &mut [i8]) {
        rand::Rng::fill(&mut rand::thread_rng(), buffer);
    }

    fn sleep(&self, _duration: std::time::Duration) -> std::time::Duration {
        panic!("sleep inside vfs is not allowed");
    }
}

pub struct Connection {
    client: Arc<MultiVersionClient>,
    db: String,
    io: Arc<IoEngine>,
    shared: Arc<Mutex<SharedState>>,
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
    state: Arc<Mutex<ConnState>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        let removed = self.shared.lock().unwrap().connections.remove(&self.db);
        assert!(removed.is_some());
    }
}

impl Connection {
    fn do_read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % PAGE_SIZE == 0);
        assert!(buf.len() % PAGE_SIZE == 0);
        let txn = self.txn.as_ref().unwrap();
        let num_pages = buf.len() / PAGE_SIZE;
        let page_offset = offset as usize / PAGE_SIZE;

        tracing::debug!(
            num_pages = num_pages,
            page_offset = page_offset,
            txn_version = txn.version(),
            "read_exact_at"
        );
        let page_id_list = (0..num_pages)
            .map(|i| {
                let page_id = (page_offset + i) as u32;
                page_id
            })
            .collect::<Vec<_>>();

        let pages = self.io.run(async move {
            txn.read_many(&page_id_list)
                .await
                .expect("unrecoverable read failure")
        });

        for (i, page) in pages.iter().enumerate() {
            let page_buf = &mut buf[i * PAGE_SIZE..(i + 1) * PAGE_SIZE];

            if page.is_empty() {
                if offset == 0 && i == 0 {
                    page_buf.copy_from_slice(FIRST_PAGE_TEMPLATE);
                } else {
                    page_buf.iter_mut().for_each(|b| *b = 0);
                }
            } else {
                page_buf.copy_from_slice(&page);
            }
        }

        Ok(())
    }
}

impl DatabaseHandle for Connection {
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

        if self.state.lock().unwrap().txn_invalidated {
            tracing::error!("write_all_at: txn invalidated");
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Transaction has been invalidated",
            ));
        }

        let num_pages = buf.len() / PAGE_SIZE;
        let page_offset = offset as usize / PAGE_SIZE;
        let txn = self.txn.as_mut().expect("Cannot write to a database without a transaction");

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

        self.lock = lock;
        if self.txn.is_none() {
            let txn = if let Some(version) = &self.fixed_version {
                Ok(self.client.create_transaction_at_version(version))
            } else {
                let client = self.client.clone();
                self.io
                    .run(async move { client.create_transaction().await })
            };
            let txn = txn.expect("unrecoverable transaction initialization failure");
            self.txn = Some(txn);
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

        if prev_lock == LockKind::Exclusive {
            // Optimistic commit
            // Rolled back?
            if !self.state.lock().unwrap().txn_invalidated {
                // Write
                let txn = self
                    .txn
                    .take()
                    .expect("did not find transaction for commit");
                let result = self.io.run(async { txn.commit().await });

                // At this point we don't have a reliable way to propagate the error. So, we have
                // to abort the process.
                let result = result.expect("transaction commit failed");
                let result = result.expect("transaction conflict");
                tracing::info!(version = result.version, "transaction committed");
                self.txn = Some(self.client.create_transaction_at_version(&result.version));
            }
        }

        if lock == LockKind::None {
            // All locks dropped
            self.txn.take().expect("unlocked without locking");
            self.state.lock().unwrap().txn_invalidated = false;
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

struct BlackholeJournal {
    state: Weak<Mutex<ConnState>>,
    size: u64,
    lock: LockKind,
    written: bool,
}

impl DatabaseHandle for BlackholeJournal {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, std::io::Error> {
        Ok(self.size)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        buf.iter_mut().for_each(|b| *b = 0);
        if buf.len() > 8 && self.written {
            self.state
                .upgrade()
                .expect("failed to get conn state for journal")
                .lock()
                .unwrap()
                .txn_invalidated = true;
        }

        if buf.len() as u64 + offset > self.size {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }

        Ok(())
    }

    fn write_all_at(&mut self, _buf: &[u8], _offset: u64) -> Result<(), std::io::Error> {
        self.written = true;
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.size = size;
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.lock = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<LockKind, std::io::Error> {
        Ok(self.lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        todo!()
    }
}

pub struct ConnWrapper {
    inner: Box<dyn DatabaseHandle<WalIndex = WalDisabled>>,
}

impl DatabaseHandle for ConnWrapper {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, std::io::Error> {
        self.inner.size()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.inner.read_exact_at(buf, offset)
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        self.inner.write_all_at(buf, offset)
    }

    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error> {
        self.inner.sync(data_only)
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        self.inner.set_len(size)
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.inner.lock(lock)
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.inner.unlock(lock)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        self.inner.reserved()
    }

    fn current_lock(&self) -> Result<LockKind, std::io::Error> {
        self.inner.current_lock()
    }

    fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        self.inner.wal_index(readonly)
    }
}
