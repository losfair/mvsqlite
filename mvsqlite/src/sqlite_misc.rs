//! https://github.com/rusqlite/rusqlite/blob/master/libsqlite3-sys/src/lib.rs

use crate::sqlite_c::sqlite3_destructor_type;

#[must_use]
pub fn SQLITE_STATIC() -> sqlite3_destructor_type {
    None
}

#[must_use]
pub fn SQLITE_TRANSIENT() -> sqlite3_destructor_type {
    Some(unsafe { std::mem::transmute(-1_isize) })
}
