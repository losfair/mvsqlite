use std::{io::ErrorKind, sync::Arc};

use mvclient::{MultiVersionClient, MultiVersionClientConfig, Transaction};

use crate::{
    io_engine::IoEngine,
    sqlite_vfs::{DatabaseHandle, LockKind, OpenKind, Vfs, WalDisabled},
};

const PAGE_SIZE: usize = 2048;
static FIRST_PAGE_TEMPLATE: &'static [u8; 2048] = include_bytes!("../template.db");

pub struct MultiVersionVfs {
    pub data_plane: String,
    pub io: Arc<IoEngine>,
}

impl Vfs for MultiVersionVfs {
    type Handle = Connection;

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
        };
        Ok(conn)
    }

    fn delete(&self, _db: &str) -> Result<(), std::io::Error> {
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

    fn sleep(&self, _duration: std::time::Duration) -> std::time::Duration {
        panic!("sleep inside vfs is not allowed");
    }
}

pub struct Connection {
    client: Arc<MultiVersionClient>,
    io: Arc<IoEngine>,
    fixed_version: Option<String>,
    txn: Option<Transaction>,
    lock: LockKind,
}

impl Connection {
    fn do_read(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        assert!(offset as usize % PAGE_SIZE == 0);
        assert!(buf.len() % PAGE_SIZE == 0);
        let txn = self.txn.as_mut().unwrap();
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

        let pages = self.io.run(async {
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

        if lock == LockKind::None {
            // All locks dropped
            self.txn.take().expect("unlocked without locking");
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
