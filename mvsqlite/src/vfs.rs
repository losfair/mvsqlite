use std::{path::Path, sync::Arc};

use crate::{
    io_engine::IoEngine,
    sqlite_vfs::{wip::WalIndex, DatabaseHandle, LockKind, OpenKind, Vfs, WalDisabled},
    tempfile::TempFile,
};

use mvfs::page_cache::PageCache;
pub use mvfs::vfs::{PAGE_CACHE_SIZE, PREFETCH_DEPTH, WRITE_CHUNK_SIZE};

pub struct MultiVersionVfs {
    pub io: Arc<IoEngine>,
    pub inner: mvfs::MultiVersionVfs,
    pub page_cache_path: String,
    pub page_cache_index_size: u64,
    pub page_cache_threshold_size: u64,
}

impl Vfs for MultiVersionVfs {
    type Handle = Box<dyn DatabaseHandle<WalIndex = WalDisabled>>;

    fn open(
        &self,
        db: &str,
        opts: crate::sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        if !matches!(opts.kind, OpenKind::MainDb) {
            return Ok(Box::new(TempFile::new()));
        }

        let page_cache = PageCache::new(
            Path::new(self.page_cache_path.as_str()),
            self.page_cache_index_size,
            self.page_cache_threshold_size,
        )
        .expect("failed to open page cache");

        let conn = self.inner.open(db, page_cache)?;
        Ok(Box::new(Connection {
            io: self.io.clone(),
            inner: conn,
        }))
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
        self.inner.sector_size()
    }
}

pub struct Connection {
    io: Arc<IoEngine>,
    inner: mvfs::Connection,
}

impl DatabaseHandle for Connection {
    type WalIndex = WalDisabled;

    fn size(&mut self) -> Result<u64, std::io::Error> {
        self.io.run(async { self.inner.size().await })
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        self.io
            .run(async { self.inner.read_exact_at(buf, offset).await })
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        self.io
            .run(async { self.inner.write_all_at(buf, offset).await })
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn set_len(&mut self, _size: u64) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.io.run(async { self.inner.lock(lock.into()).await })
    }

    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.io.run(async { self.inner.unlock(lock.into()).await })
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<crate::sqlite_vfs::LockKind, std::io::Error> {
        Ok(self.inner.current_lock().into())
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

impl From<LockKind> for mvfs::types::LockKind {
    fn from(lock: LockKind) -> Self {
        match lock {
            LockKind::None => mvfs::types::LockKind::None,
            LockKind::Shared => mvfs::types::LockKind::Shared,
            LockKind::Reserved => mvfs::types::LockKind::Reserved,
            LockKind::Pending => mvfs::types::LockKind::Pending,
            LockKind::Exclusive => mvfs::types::LockKind::Exclusive,
        }
    }
}

impl From<mvfs::types::LockKind> for LockKind {
    fn from(lock: mvfs::types::LockKind) -> Self {
        match lock {
            mvfs::types::LockKind::None => LockKind::None,
            mvfs::types::LockKind::Shared => LockKind::Shared,
            mvfs::types::LockKind::Reserved => LockKind::Reserved,
            mvfs::types::LockKind::Pending => LockKind::Pending,
            mvfs::types::LockKind::Exclusive => LockKind::Exclusive,
        }
    }
}
