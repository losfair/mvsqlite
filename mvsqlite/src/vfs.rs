use std::{io::ErrorKind, sync::Arc};

use crate::{
    io_engine::IoEngine,
    sqlite_vfs::{wip::WalIndex, DatabaseHandle, LockKind, OpenKind, Vfs, WalDisabled},
    tempfile::TempFile,
};

pub use mvfs::vfs::{PAGE_CACHE_SIZE, PREFETCH_DEPTH, WRITE_CHUNK_SIZE};

pub struct MultiVersionVfs {
    pub io: AbstractIoEngine,
    pub inner: mvfs::MultiVersionVfs,
}

#[derive(Clone)]
pub enum AbstractIoEngine {
    Prebuilt(Arc<IoEngine>),
    Builder(Arc<Box<dyn Fn() -> Arc<IoEngine> + Send + Sync + 'static>>),
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

        let conn = self
            .inner
            .open(db, true)
            .map_err(|e| std::io::Error::new(ErrorKind::Other, e))?;
        Ok(Box::new(Connection {
            io: match &self.io {
                AbstractIoEngine::Prebuilt(io) => io.clone(),
                AbstractIoEngine::Builder(io) => io(),
            },
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
        match &self.io {
            AbstractIoEngine::Prebuilt(io) => io.run(tokio::time::sleep(duration)),
            _ => std::thread::sleep(duration),
        }
        duration
    }

    fn sector_size(&self) -> usize {
        self.inner.sector_size()
    }
}

pub struct Connection {
    pub io: Arc<IoEngine>,
    pub inner: mvfs::Connection,
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

    fn commit_phasetwo(&mut self) {
        self.inner.confirm_commit();
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl<W: WalIndex + 'static> DatabaseHandle for Box<dyn DatabaseHandle<WalIndex = W>> {
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

    fn commit_phasetwo(&mut self) {
        (**self).commit_phasetwo()
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
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
