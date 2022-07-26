use stackful::wait;

use crate::{
    persistence_layer::{PersistenceConnection, PersistenceLayer},
    sqlite_vfs::{DatabaseHandle, OpenKind, Vfs, WalDisabled},
};

pub struct MultiVersionVfs {
    persist: Box<dyn PersistenceLayer>,
}

impl Vfs for MultiVersionVfs {
    type Handle = Connection;

    fn open(
        &self,
        db: &str,
        opts: crate::sqlite_vfs::OpenOptions,
    ) -> Result<Self::Handle, std::io::Error> {
        if !matches!(opts.kind, OpenKind::MainDb) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Only main database is supported",
            ));
        }
        let conn = wait(self.persist.open(db))?;
        Ok(Connection { persist: conn })
    }

    fn delete(&self, db: &str) -> Result<(), std::io::Error> {
        panic!("Not implemented: delete");
    }

    fn exists(&self, _db: &str) -> Result<bool, std::io::Error> {
        Ok(true)
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
    persist: Box<dyn PersistenceConnection>,
}

impl DatabaseHandle for Connection {
    type WalIndex = WalDisabled;

    fn size(&self) -> Result<u64, std::io::Error> {
        todo!()
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error> {
        todo!()
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        todo!()
    }

    fn lock(&mut self, lock: crate::sqlite_vfs::LockKind) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        todo!()
    }

    fn current_lock(&self) -> Result<crate::sqlite_vfs::LockKind, std::io::Error> {
        todo!()
    }

    fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        todo!()
    }
}
