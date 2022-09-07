use std::sync::Mutex;

use crate::sqlite_vfs::{DatabaseHandle, LockKind, WalDisabled};

pub struct TempFile {
    state: Mutex<State>,
}

struct State {
    data: Vec<u8>,
    lock: LockKind,
}

impl TempFile {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(State {
                data: Vec::new(),
                lock: LockKind::None,
            }),
        }
    }
}

impl DatabaseHandle for TempFile {
    type WalIndex = WalDisabled;

    fn size(&mut self) -> Result<u64, std::io::Error> {
        Ok(self.state.lock().unwrap().data.len() as u64)
    }

    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error> {
        let state = self.state.lock().unwrap();

        if buf.len() + offset as usize > state.data.len() {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let data = &state.data[offset as usize..offset as usize + buf.len()];
        buf.copy_from_slice(data);
        Ok(())
    }

    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();

        let required_len = offset as usize + buf.len();
        if required_len > state.data.len() {
            state.data.resize(required_len, 0);
        }
        state.data[offset as usize..offset as usize + buf.len()].copy_from_slice(buf);
        Ok(())
    }

    fn sync(&mut self, _data_only: bool) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error> {
        let mut state = self.state.lock().unwrap();
        state.data.resize(size as usize, 0);
        Ok(())
    }

    fn lock(&mut self, lock: crate::sqlite_vfs::LockKind) -> Result<bool, std::io::Error> {
        let mut state = self.state.lock().unwrap();
        state.lock = lock;
        Ok(true)
    }

    fn reserved(&mut self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    fn current_lock(&self) -> Result<crate::sqlite_vfs::LockKind, std::io::Error> {
        Ok(self.state.lock().unwrap().lock)
    }

    fn wal_index(&self, _readonly: bool) -> Result<Self::WalIndex, std::io::Error> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}
