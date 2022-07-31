use libc::c_void;

#[repr(transparent)]
#[derive(Copy, Clone, Eq, PartialEq)]
pub struct SqlitePtr {
    pub ptr: *mut c_void,
}

pub type Sqlite3CommitHookCallback = unsafe extern "C" fn(data: *mut c_void) -> i32;
pub type Sqlite3RollbackHookCallback = unsafe extern "C" fn(data: *mut c_void);

extern "C" {
    pub fn sqlite3_commit_hook(
        s: SqlitePtr,
        callback: Sqlite3CommitHookCallback,
        data: *mut c_void,
    ) -> *mut c_void;
    pub fn sqlite3_rollback_hook(
        s: SqlitePtr,
        callback: Sqlite3RollbackHookCallback,
        data: *mut c_void,
    ) -> *mut c_void;
}
