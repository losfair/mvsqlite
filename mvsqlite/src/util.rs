use std::ffi::CString;
use std::ops::{Deref, DerefMut};

use libc::c_void;

use crate::sqlite_c;
use crate::sqlite_vfs::{DatabaseHandle, WalDisabled};
use crate::vfs::{Connection, MultiVersionVfs};

pub struct ConnectionGuard {
    conn: *mut Connection,
}

impl Deref for ConnectionGuard {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.conn }
    }
}

impl DerefMut for ConnectionGuard {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.conn }
    }
}

pub unsafe fn get_conn(db: *mut sqlite_c::sqlite3, db_name: &str) -> ConnectionGuard {
    let db_name = CString::new(db_name).unwrap();
    let mut fileptr: *mut crate::sqlite_vfs::ffi::sqlite3_file = std::ptr::null_mut();
    let ret = sqlite_c::sqlite3_file_control(
        db,
        db_name.as_ptr(),
        sqlite_c::SQLITE_FCNTL_FILE_POINTER,
        &mut fileptr as *mut _ as *mut c_void,
    );
    if ret != sqlite_c::SQLITE_OK {
        panic!("sqlite3_file_control failed: {}", ret);
    }
    let boxed = crate::sqlite_vfs::get_file::<
        MultiVersionVfs,
        Box<dyn DatabaseHandle<WalIndex = WalDisabled>>,
    >(fileptr)
    .unwrap();
    let conn: &mut Connection = (**boxed)
        .as_any_mut()
        .downcast_mut()
        .expect("the backing file is not a connection");
    ConnectionGuard { conn }
}
