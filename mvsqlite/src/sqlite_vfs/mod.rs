#![allow(clippy::question_mark)]
//! Create a custom SQLite virtual file system by implementing the [Vfs] trait and registering it
//! using [register].
//!
//! https://github.com/rkusa/sqlite-vfs/blob/main/src/lib.rs

use std::any::Any;
use std::borrow::Cow;
use std::ffi::{c_void, CStr, CString};
use std::io::ErrorKind;
use std::mem::{size_of, ManuallyDrop, MaybeUninit};
use std::ops::Range;
use std::os::raw::{c_char, c_int};
use std::ptr::null_mut;
use std::slice;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;

pub mod ffi;

/// A file opened by [Vfs].
pub trait DatabaseHandle: Sync {
    /// An optional trait used to store a WAL (write-ahead log).
    type WalIndex: wip::WalIndex;

    /// Return the current size in bytes of the database.
    fn size(&mut self) -> Result<u64, std::io::Error>;

    /// Reads the exact number of byte required to fill `buf` from the given `offset`.
    fn read_exact_at(&mut self, buf: &mut [u8], offset: u64) -> Result<(), std::io::Error>;

    /// Attempts to write an entire `buf` starting from the given `offset`.
    fn write_all_at(&mut self, buf: &[u8], offset: u64) -> Result<(), std::io::Error>;

    /// Make sure all writes are committed to the underlying storage. If `data_only` is set to
    /// `true`, only the data and not the metadata (like size, access time, etc) should be synced.
    fn sync(&mut self, data_only: bool) -> Result<(), std::io::Error>;

    /// Set the database file to the specified `size`. Truncates or extends the underlying storage.
    fn set_len(&mut self, size: u64) -> Result<(), std::io::Error>;

    /// Lock the database. Returns whether the requested lock could be acquired.
    /// Locking sequence:
    /// - The lock is never moved from [LockKind::None] to anything higher than [LockKind::Shared].
    /// - A [LockKind::Pending] is never requested explicitly.
    /// - A [LockKind::Shared] is always held when a [LockKind::Reserved] lock is requested
    fn lock(&mut self, lock: LockKind) -> Result<bool, std::io::Error>;

    /// Unlock the database.
    fn unlock(&mut self, lock: LockKind) -> Result<bool, std::io::Error> {
        self.lock(lock)
    }

    fn commit_phasetwo(&mut self) {}

    /// Check if the database this handle points to holds a [LockKind::Reserved],
    /// [LockKind::Pending] or [LockKind::Exclusive] lock.
    fn reserved(&mut self) -> Result<bool, std::io::Error>;

    /// Return the current [LockKind] of the this handle.
    fn current_lock(&self) -> Result<LockKind, std::io::Error>;

    /// Change the chunk size of the database to `chunk_size`.
    fn set_chunk_size(&self, _chunk_size: usize) -> Result<(), std::io::Error> {
        Ok(())
    }

    /// Check if the underlying data of the handle got moved or deleted. When moved, the handle can
    /// still be read from, but not written to anymore.
    fn moved(&self) -> Result<bool, std::io::Error> {
        Ok(false)
    }

    fn wal_index(&self, readonly: bool) -> Result<Self::WalIndex, std::io::Error>;

    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// A virtual file system for SQLite.
pub trait Vfs: Sync {
    /// The file returned by [Vfs::open].
    type Handle: DatabaseHandle;

    /// Open the database `db` (of type `opts.kind`).
    fn open(&self, db: &str, opts: OpenOptions) -> Result<Self::Handle, std::io::Error>;

    /// Delete the database `db`.
    fn delete(&self, db: &str) -> Result<(), std::io::Error>;

    /// Check if a database `db` already exists.
    fn exists(&self, db: &str) -> Result<bool, std::io::Error>;

    /// Generate and return a path for a temporary database.
    fn temporary_name(&self) -> String;

    /// Populate the `buffer` with random data.
    fn random(&self, buffer: &mut [i8]);

    /// Sleep for `duration`. Return the duration actually slept.
    fn sleep(&self, duration: Duration) -> Duration;

    /// Check access to `db`. The default implementation always returns `true`.
    fn access(&self, _db: &str, _write: bool) -> Result<bool, std::io::Error> {
        Ok(true)
    }

    /// Retrieve the full pathname of a database `db`.
    fn full_pathname<'a>(&self, db: &'a str) -> Result<Cow<'a, str>, std::io::Error> {
        Ok(db.into())
    }

    fn sector_size(&self) -> usize {
        4096
    }
}

#[doc(hidden)]
pub mod wip {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(u16)]
    pub enum WalIndexLock {
        None = 1,
        Shared,
        Exclusive,
    }

    pub trait WalIndex: Sync {
        fn enabled() -> bool {
            true
        }

        fn map(&mut self, region: u32) -> Result<[u8; 32768], std::io::Error>;
        fn lock(&mut self, locks: Range<u8>, lock: WalIndexLock) -> Result<bool, std::io::Error>;
        fn delete(self) -> Result<(), std::io::Error>;

        fn pull(&mut self, _region: u32, _data: &mut [u8; 32768]) -> Result<(), std::io::Error> {
            Ok(())
        }

        fn push(&mut self, _region: u32, _data: &[u8; 32768]) -> Result<(), std::io::Error> {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpenOptions {
    /// The object type that is being opened.
    pub kind: OpenKind,

    /// The access an object is opened with.
    pub access: OpenAccess,

    /// The file should be deleted when it is closed.
    delete_on_close: bool,
}

/// The object type that is being opened.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenKind {
    MainDb,
    MainJournal,
    TempDb,
    TempJournal,
    TransientDb,
    SubJournal,
    SuperJournal,
    Wal,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OpenAccess {
    /// Read access.
    Read,

    /// Write access (includes read access).
    Write,

    /// Create the file if it does not exist (includes write and read access).
    Create,

    /// Create the file, but throw if it it already exist (includes write and read access).
    CreateNew,
}

/// The access an object is opened with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockKind {
    /// No locks are held. The database may be neither read nor written. Any internally cached data
    /// is considered suspect and subject to verification against the database file before being
    /// used. Other processes can read or write the database as their own locking states permit.
    /// This is the default state.
    None,

    /// The database may be read but not written. Any number of processes can hold
    /// [LockKind::Shared] locks at the same time, hence there can be many simultaneous readers. But
    /// no other thread or process is allowed to write to the database file while one or more
    /// [LockKind::Shared] locks are active.
    Shared,

    /// A [LockKind::Reserved] lock means that the process is planning on writing to the database
    /// file at some point in the future but that it is currently just reading from the file. Only a
    /// single [LockKind::Reserved] lock may be active at one time, though multiple
    /// [LockKind::Shared] locks can coexist with a single [LockKind::Reserved] lock.
    /// [LockKind::Reserved] differs from [LockKind::Pending] in that new [LockKind::Shared] locks
    /// can be acquired while there is a [LockKind::Reserved] lock.
    Reserved,

    /// A [LockKind::Pending] lock means that the process holding the lock wants to write to the
    /// database as soon as possible and is just waiting on all current [LockKind::Shared] locks to
    /// clear so that it can get an [LockKind::Exclusive] lock. No new [LockKind::Shared] locks are
    /// permitted against the database if a [LockKind::Pending] lock is active, though existing
    /// [LockKind::Shared] locks are allowed to continue.
    Pending,

    /// An [LockKind::Exclusive] lock is needed in order to write to the database file. Only one
    /// [LockKind::Exclusive] lock is allowed on the file and no other locks of any kind are allowed
    /// to coexist with an [LockKind::Exclusive] lock. In order to maximize concurrency, SQLite
    /// works to minimize the amount of time that [LockKind::Exclusive] locks are held.
    Exclusive,
}

struct State<V> {
    name: CString,
    vfs: Arc<V>,
    #[cfg(any(feature = "syscall", feature = "loadext"))]
    parent_vfs: *mut ffi::sqlite3_vfs,
    io_methods: ffi::sqlite3_io_methods,
    last_error: Arc<Mutex<Option<(i32, std::io::Error)>>>,
    next_id: usize,
}

/// Register a virtual file system ([Vfs]) to SQLite.
pub fn register<F: DatabaseHandle, V: Vfs<Handle = F>>(
    name: &str,
    vfs: V,
    as_default: bool,
) -> Result<(), RegisterError> {
    let io_methods = ffi::sqlite3_io_methods {
        iVersion: 2,
        xClose: Some(io::close::<V, F>),
        xRead: Some(io::read::<V, F>),
        xWrite: Some(io::write::<V, F>),
        xTruncate: Some(io::truncate::<V, F>),
        xSync: Some(io::sync::<V, F>),
        xFileSize: Some(io::file_size::<V, F>),
        xLock: Some(io::lock::<V, F>),
        xUnlock: Some(io::unlock::<V, F>),
        xCheckReservedLock: Some(io::check_reserved_lock::<V, F>),
        xFileControl: Some(io::file_control::<V, F>),
        xSectorSize: Some(io::sector_size::<V, F>),
        xDeviceCharacteristics: Some(io::device_characteristics::<V, F>),
        xShmMap: None,
        xShmLock: None,
        xShmBarrier: None,
        xShmUnmap: None,
        xFetch: None,
        xUnfetch: None,
    };
    let name = CString::new(name)?;
    let name_ptr = name.as_ptr();
    let ptr = Box::into_raw(Box::new(State {
        name,
        vfs: Arc::new(vfs),
        #[cfg(any(feature = "syscall", feature = "loadext"))]
        parent_vfs: unsafe { ffi::sqlite3_vfs_find(std::ptr::null_mut()) },
        io_methods,
        last_error: Default::default(),
        next_id: 0,
    }));
    let vfs = Box::into_raw(Box::new(ffi::sqlite3_vfs {
        #[cfg(not(feature = "syscall"))]
        iVersion: 2,
        #[cfg(feature = "syscall")]
        iVersion: 3,
        szOsFile: size_of::<FileState<V, F>>() as i32,
        mxPathname: MAX_PATH_LENGTH as i32, // max path length supported by VFS
        pNext: null_mut(),
        zName: name_ptr,
        pAppData: ptr as _,
        xOpen: Some(vfs::open::<F, V>),
        xDelete: Some(vfs::delete::<V>),
        xAccess: Some(vfs::access::<V>),
        xFullPathname: Some(vfs::full_pathname::<V>),
        xDlOpen: Some(vfs::dlopen::<V>),
        xDlError: Some(vfs::dlerror::<V>),
        xDlSym: Some(vfs::dlsym::<V>),
        xDlClose: Some(vfs::dlclose::<V>),
        xRandomness: Some(vfs::randomness::<V>),
        xSleep: Some(vfs::sleep::<V>),
        xCurrentTime: Some(vfs::current_time::<V>),
        xGetLastError: Some(vfs::get_last_error::<V>),
        xCurrentTimeInt64: Some(vfs::current_time_int64::<V>),

        #[cfg(not(feature = "syscall"))]
        xSetSystemCall: None,
        #[cfg(not(feature = "syscall"))]
        xGetSystemCall: None,
        #[cfg(not(feature = "syscall"))]
        xNextSystemCall: None,

        #[cfg(feature = "syscall")]
        xSetSystemCall: Some(vfs::set_system_call::<V>),
        #[cfg(feature = "syscall")]
        xGetSystemCall: Some(vfs::get_system_call::<V>),
        #[cfg(feature = "syscall")]
        xNextSystemCall: Some(vfs::next_system_call::<V>),
    }));

    let result = unsafe { ffi::sqlite3_vfs_register(vfs, as_default as i32) };
    if result != ffi::SQLITE_OK {
        return Err(RegisterError::Register(result));
    }

    // TODO: return object that allows to unregister (and cleanup the memory)?

    Ok(())
}

// TODO: add to [Vfs]?
const MAX_PATH_LENGTH: usize = 512;

#[repr(C)]
struct FileState<V, F: DatabaseHandle> {
    base: ffi::sqlite3_file,
    ext: MaybeUninit<FileExt<V, F>>,
}

#[repr(C)]
struct FileExt<V, F: DatabaseHandle> {
    vfs: Arc<V>,
    vfs_name: CString,
    db_name: String,
    file: F,
    delete_on_close: bool,
    /// The last error; shared with the VFS.
    last_error: Arc<Mutex<Option<(i32, std::io::Error)>>>,
    /// The last error number of this file/connection (not shared with the VFS).
    last_errno: i32,
    id: usize,
    chunk_size: Option<usize>,
    powersafe_overwrite: bool,
}

// Example mem-fs implementation:
// https://github.com/sqlite/sqlite/blob/a959bf53110bfada67a3a52187acd57aa2f34e19/ext/misc/memvfs.c
mod vfs {
    use super::*;

    /// Open a new file handler.
    pub unsafe extern "C" fn open<F: DatabaseHandle, V: Vfs<Handle = F>>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const c_char,
        p_file: *mut ffi::sqlite3_file,
        flags: c_int,
        p_out_flags: *mut c_int,
    ) -> c_int {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        let name = if z_name.is_null() {
            None
        } else {
            match CStr::from_ptr(z_name).to_str() {
                Ok(name) => Some(name),
                Err(_) => {
                    return state.set_last_error(
                        ffi::SQLITE_CANTOPEN,
                        std::io::Error::new(
                            ErrorKind::Other,
                            format!(
                                "open failed: database must be valid utf8 (received: {:?})",
                                CStr::from_ptr(z_name)
                            ),
                        ),
                    )
                }
            }
        };
        log::trace!("open z_name={:?} flags={}", name, flags);

        let mut opts = match OpenOptions::from_flags(flags) {
            Some(opts) => opts,
            None => {
                return state.set_last_error(
                    ffi::SQLITE_CANTOPEN,
                    std::io::Error::new(ErrorKind::Other, "invalid open flags"),
                );
            }
        };

        if z_name.is_null() && !opts.delete_on_close {
            return state.set_last_error(
                ffi::SQLITE_CANTOPEN,
                std::io::Error::new(
                    ErrorKind::Other,
                    "delete on close expected for temporary database",
                ),
            );
        }

        let out_file = match (p_file as *mut FileState<V, F>).as_mut() {
            Some(f) => f,
            None => {
                return state.set_last_error(
                    ffi::SQLITE_CANTOPEN,
                    std::io::Error::new(ErrorKind::Other, "invalid file pointer"),
                );
            }
        };

        let mut powersafe_overwrite = true;
        if flags & ffi::SQLITE_OPEN_URI > 0 && name.is_some() {
            let param = b"psow\0";
            if ffi::sqlite3_uri_boolean(z_name, param.as_ptr() as *const c_char, 1) == 0 {
                powersafe_overwrite = false;
            }
        }

        let name = name.map_or_else(|| state.vfs.temporary_name(), String::from);
        let result = state.vfs.open(&name, opts.clone());
        let result = match result {
            Ok(f) => Ok(f),
            // handle creation failure due to readonly directory
            Err(err)
                if err.kind() == ErrorKind::PermissionDenied
                    && matches!(
                        opts.kind,
                        OpenKind::SuperJournal | OpenKind::MainJournal | OpenKind::Wal
                    )
                    && matches!(opts.access, OpenAccess::Create | OpenAccess::CreateNew)
                    && !state.vfs.exists(&name).unwrap_or(false) =>
            {
                return state.set_last_error(ffi::SQLITE_READONLY_DIRECTORY, err);
            }

            // Try again as readonly
            Err(err)
                if err.kind() == ErrorKind::PermissionDenied && opts.access != OpenAccess::Read =>
            {
                opts.access = OpenAccess::Read;
                state.vfs.open(&name, opts.clone()).map_err(|_| err)
            }

            // e.g. tried to open a directory
            Err(err) if err.kind() == ErrorKind::Other && opts.access == OpenAccess::Read => {
                return state.set_last_error(ffi::SQLITE_IOERR, err);
            }

            Err(err) => Err(err),
        };
        let file = match result {
            Ok(f) => f,
            Err(err) => {
                return state.set_last_error(ffi::SQLITE_CANTOPEN, err);
            }
        };

        if let Some(p_out_flags) = p_out_flags.as_mut() {
            *p_out_flags = opts.to_flags();
        }

        out_file.base.pMethods = &state.io_methods;
        out_file.ext.write(FileExt {
            vfs: state.vfs.clone(),
            vfs_name: state.name.clone(),
            db_name: name,
            file,
            delete_on_close: opts.delete_on_close,
            last_error: Arc::clone(&state.last_error),
            last_errno: 0,
            id: state.next_id,
            chunk_size: None,
            powersafe_overwrite,
        });
        state.next_id = state.next_id.overflowing_add(1).0;

        #[cfg(feature = "sqlite_test")]
        ffi::sqlite3_inc_open_file_count();

        ffi::SQLITE_OK
    }

    /// Delete the file located at `z_path`. If the `sync_dir` argument is true, ensure the
    /// file-system modifications are synced to disk before returning.
    pub unsafe extern "C" fn delete<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        _sync_dir: c_int,
    ) -> c_int {
        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_ERROR;
        // }

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_DELETE,
        };

        let path = match CStr::from_ptr(z_path).to_str() {
            Ok(name) => name,
            Err(_) => {
                return state.set_last_error(
                    ffi::SQLITE_ERROR,
                    std::io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "delete failed: database must be valid utf8 (received: {:?})",
                            CStr::from_ptr(z_path)
                        ),
                    ),
                )
            }
        };
        log::trace!("delete name={}", path);

        match state.vfs.delete(path) {
            Ok(_) => ffi::SQLITE_OK,
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    ffi::SQLITE_IOERR_DELETE_NOENT
                } else {
                    state.set_last_error(ffi::SQLITE_DELETE, err)
                }
            }
        }
    }

    /// Test for access permissions. Return true if the requested permission is available, or false
    /// otherwise.
    pub unsafe extern "C" fn access<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        flags: c_int,
        p_res_out: *mut c_int,
    ) -> c_int {
        #[cfg(feature = "sqlite_test")]
        if simulate_io_error() {
            return ffi::SQLITE_IOERR_ACCESS;
        }

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        let path = match CStr::from_ptr(z_path).to_str() {
            Ok(name) => name,
            Err(_) => {
                log::warn!(
                    "access failed: database must be valid utf8 (received: {:?})",
                    CStr::from_ptr(z_path)
                );

                if let Ok(p_res_out) = p_res_out.as_mut().ok_or_else(null_ptr_error) {
                    *p_res_out = false as i32;
                }

                return ffi::SQLITE_OK;
            }
        };
        log::trace!("access z_name={} flags={}", path, flags);

        let result = match flags {
            ffi::SQLITE_ACCESS_EXISTS => state.vfs.exists(path),
            ffi::SQLITE_ACCESS_READ => state.vfs.access(path, false),
            ffi::SQLITE_ACCESS_READWRITE => state.vfs.access(path, true),
            _ => return ffi::SQLITE_IOERR_ACCESS,
        };

        if let Err(err) = result.and_then(|ok| {
            let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
            *p_res_out = ok as i32;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_ACCESS, err);
        }

        ffi::SQLITE_OK
    }

    /// Populate buffer `z_out` with the full canonical pathname corresponding to the pathname in
    /// `z_path`. `z_out` is guaranteed to point to a buffer of at least (INST_MAX_PATHNAME+1)
    /// bytes.
    pub unsafe extern "C" fn full_pathname<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
        n_out: c_int,
        z_out: *mut c_char,
    ) -> c_int {
        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_ERROR;
        // }

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        let path = match CStr::from_ptr(z_path).to_str() {
            Ok(name) => name,
            Err(_) => {
                return state.set_last_error(
                    ffi::SQLITE_ERROR,
                    std::io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "full_pathname failed: database must be valid utf8 (received: {:?})",
                            CStr::from_ptr(z_path)
                        ),
                    ),
                )
            }
        };
        log::trace!("full_pathname name={}", path);

        let name = match state.vfs.full_pathname(path).and_then(|name| {
            CString::new(name.to_string()).map_err(|_| {
                std::io::Error::new(ErrorKind::Other, "name must not contain a nul byte")
            })
        }) {
            Ok(name) => name,
            Err(err) => return state.set_last_error(ffi::SQLITE_ERROR, err),
        };

        let name = name.to_bytes_with_nul();
        if name.len() > n_out as usize || name.len() > MAX_PATH_LENGTH {
            return state.set_last_error(
                ffi::SQLITE_CANTOPEN,
                std::io::Error::new(ErrorKind::Other, "full pathname is too long"),
            );
        }
        let out = slice::from_raw_parts_mut(z_out as *mut u8, name.len());
        out.copy_from_slice(name);

        ffi::SQLITE_OK
    }

    /// Open the dynamic library located at `z_path` and return a handle.
    #[allow(unused_variables)]
    pub unsafe extern "C" fn dlopen<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_path: *const c_char,
    ) -> *mut c_void {
        log::trace!("dlopen");

        #[cfg(feature = "loadext")]
        {
            let state = match vfs_state::<V>(p_vfs) {
                Ok(state) => state,
                Err(_) => return null_mut(),
            };

            if let Some(dlopen) = state.parent_vfs.as_ref().and_then(|v| v.xDlOpen) {
                return dlopen(state.parent_vfs, z_path);
            }
        }

        null_mut()
    }

    /// Populate the buffer `z_err_msg` (size `n_byte` bytes) with a human readable utf-8 string
    /// describing the most recent error encountered associated with dynamic libraries.
    #[allow(unused_variables)]
    pub unsafe extern "C" fn dlerror<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) {
        log::trace!("dlerror");

        #[cfg(feature = "loadext")]
        {
            let state = match vfs_state::<V>(p_vfs) {
                Ok(state) => state,
                Err(_) => return,
            };

            if let Some(dlerror) = state.parent_vfs.as_ref().and_then(|v| v.xDlError) {
                return dlerror(state.parent_vfs, n_byte, z_err_msg);
            }

            return;
        }

        #[cfg(not(feature = "loadext"))]
        {
            let msg = concat!("Loadable extensions are not supported", "\0");
            ffi::sqlite3_snprintf(n_byte, z_err_msg, msg.as_ptr() as _);
        }
    }

    /// Return a pointer to the symbol `z_sym` in the dynamic library pHandle.
    #[allow(unused_variables)]
    pub unsafe extern "C" fn dlsym<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        p: *mut c_void,
        z_sym: *const c_char,
    ) -> Option<unsafe extern "C" fn(*mut ffi::sqlite3_vfs, *mut c_void, *const c_char)> {
        log::trace!("dlsym");

        #[cfg(feature = "loadext")]
        {
            let state = match vfs_state::<V>(p_vfs) {
                Ok(state) => state,
                Err(_) => return None,
            };

            if let Some(dlsym) = state.parent_vfs.as_ref().and_then(|v| v.xDlSym) {
                return dlsym(state.parent_vfs, p, z_sym);
            }
        }

        None
    }

    /// Close the dynamic library handle `p_handle`.
    #[allow(unused_variables)]
    pub unsafe extern "C" fn dlclose<V>(p_vfs: *mut ffi::sqlite3_vfs, p_handle: *mut c_void) {
        log::trace!("dlclose");

        #[cfg(feature = "loadext")]
        {
            let state = match vfs_state::<V>(p_vfs) {
                Ok(state) => state,
                Err(_) => return,
            };

            if let Some(dlclose) = state.parent_vfs.as_ref().and_then(|v| v.xDlClose) {
                return dlclose(state.parent_vfs, p_handle);
            }
        }
    }

    /// Populate the buffer pointed to by `z_buf_out` with `n_byte` bytes of random data.
    pub unsafe extern "C" fn randomness<V: Vfs>(
        p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_buf_out: *mut c_char,
    ) -> c_int {
        log::trace!("randomness");

        let bytes = slice::from_raw_parts_mut(z_buf_out as *mut i8, n_byte as usize);
        if cfg!(feature = "sqlite_test") {
            // During testing, the buffer is simply initialized to all zeroes for repeatability
            bytes.fill(0);
        } else {
            let state = match vfs_state::<V>(p_vfs) {
                Ok(state) => state,
                Err(_) => return 0,
            };

            state.vfs.random(bytes);
        }
        bytes.len() as c_int
    }

    /// Sleep for `n_micro` microseconds. Return the number of microseconds actually slept.
    pub unsafe extern "C" fn sleep<V: Vfs>(p_vfs: *mut ffi::sqlite3_vfs, n_micro: c_int) -> c_int {
        log::trace!("sleep");

        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        state
            .vfs
            .sleep(Duration::from_micros(n_micro as u64))
            .as_micros() as c_int
    }

    /// Return the current time as a Julian Day number in `p_time_out`.
    pub unsafe extern "C" fn current_time<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        p_time_out: *mut f64,
    ) -> c_int {
        log::trace!("current_time");

        let mut i = 0i64;
        current_time_int64::<V>(p_vfs, &mut i);

        *p_time_out = i as f64 / 86400000.0;
        ffi::SQLITE_OK
    }

    pub unsafe extern "C" fn current_time_int64<V>(
        _p_vfs: *mut ffi::sqlite3_vfs,
        p: *mut i64,
    ) -> i32 {
        log::trace!("current_time_int64");

        const UNIX_EPOCH: i64 = 24405875 * 8640000;
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + UNIX_EPOCH;
        #[cfg(feature = "sqlite_test")]
        let now = if ffi::sqlite3_get_current_time() > 0 {
            ffi::sqlite3_get_current_time() as i64 * 1000 + UNIX_EPOCH
        } else {
            now
        };

        *p = now;
        ffi::SQLITE_OK
    }

    #[cfg(feature = "syscall")]
    pub unsafe extern "C" fn set_system_call<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const ::std::os::raw::c_char,
        p_new_func: ffi::sqlite3_syscall_ptr,
    ) -> ::std::os::raw::c_int {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };

        if let Some(set_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xSetSystemCall) {
            return set_system_call(state.parent_vfs, z_name, p_new_func);
        }

        ffi::SQLITE_ERROR
    }

    #[cfg(feature = "syscall")]
    pub unsafe extern "C" fn get_system_call<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const ::std::os::raw::c_char,
    ) -> ffi::sqlite3_syscall_ptr {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return None,
        };

        if let Some(get_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xGetSystemCall) {
            return get_system_call(state.parent_vfs, z_name);
        }

        None
    }

    #[cfg(feature = "syscall")]
    pub unsafe extern "C" fn next_system_call<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        z_name: *const ::std::os::raw::c_char,
    ) -> *const ::std::os::raw::c_char {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return std::ptr::null(),
        };

        if let Some(next_system_call) = state.parent_vfs.as_ref().and_then(|v| v.xNextSystemCall) {
            return next_system_call(state.parent_vfs, z_name);
        }

        std::ptr::null()
    }

    pub unsafe extern "C" fn get_last_error<V>(
        p_vfs: *mut ffi::sqlite3_vfs,
        n_byte: c_int,
        z_err_msg: *mut c_char,
    ) -> c_int {
        let state = match vfs_state::<V>(p_vfs) {
            Ok(state) => state,
            Err(_) => return ffi::SQLITE_ERROR,
        };
        if let Some((eno, err)) = state.last_error.lock().unwrap().as_ref() {
            let msg = match CString::new(err.to_string()) {
                Ok(msg) => msg,
                Err(_) => return ffi::SQLITE_ERROR,
            };

            let msg = msg.to_bytes_with_nul();
            if msg.len() > n_byte as usize {
                return ffi::SQLITE_ERROR;
            }
            let out = slice::from_raw_parts_mut(z_err_msg as *mut u8, msg.len());
            out.copy_from_slice(msg);

            return *eno;
        }
        ffi::SQLITE_OK
    }
}

pub unsafe fn get_file<V: Vfs, F: DatabaseHandle>(
    raw: *mut ffi::sqlite3_file,
) -> Result<*mut F, std::io::Error> {
    let state = file_state::<V, F>(raw)?;
    Ok(&mut state.file)
}

mod io {
    use std::mem;

    use super::*;

    /// Close a file.
    pub unsafe extern "C" fn close<V: Vfs, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
    ) -> c_int {
        if let Some(f) = (p_file as *mut FileState<V, F>).as_mut() {
            let ext = f.ext.assume_init_mut();
            if ext.delete_on_close {
                if let Err(err) = <V as Vfs>::delete(&ext.vfs, &ext.db_name) {
                    return ext.set_last_error(ffi::SQLITE_DELETE, err);
                }
            }

            let ext = mem::replace(&mut f.ext, MaybeUninit::uninit());
            let ext = ext.assume_init(); // extract the value to drop it
            log::trace!("[{}] close ({})", ext.id, ext.db_name);
        }

        #[cfg(feature = "sqlite_test")]
        ffi::sqlite3_dec_open_file_count();

        ffi::SQLITE_OK
    }

    /// Read data from a file.
    pub unsafe extern "C" fn read<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        z_buf: *mut c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CLOSE,
        };
        log::trace!(
            "[{}] read offset={} len={} ({})",
            state.id,
            i_ofst,
            i_amt,
            state.db_name
        );

        let out = slice::from_raw_parts_mut(z_buf as *mut u8, i_amt as usize);
        if let Err(err) = state.file.read_exact_at(out, i_ofst as u64) {
            let kind = err.kind();
            if kind == ErrorKind::UnexpectedEof {
                return ffi::SQLITE_IOERR_SHORT_READ;
            } else {
                return state.set_last_error(ffi::SQLITE_IOERR_READ, err);
            }
        }

        ffi::SQLITE_OK
    }

    /// Write data to a file.
    pub unsafe extern "C" fn write<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        z: *const c_void,
        i_amt: c_int,
        i_ofst: ffi::sqlite3_int64,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_WRITE,
        };
        log::trace!(
            "[{}] write offset={} len={} ({})",
            state.id,
            i_ofst,
            i_amt,
            state.db_name
        );

        let data = slice::from_raw_parts(z as *mut u8, i_amt as usize);
        let result = state.file.write_all_at(data, i_ofst as u64);

        #[cfg(feature = "sqlite_test")]
        let result = if simulate_io_error() {
            Err(ErrorKind::Other.into())
        } else {
            result
        };

        #[cfg(feature = "sqlite_test")]
        let result = if simulate_diskfull_error() {
            Err(ErrorKind::WriteZero.into())
        } else {
            result
        };

        match result {
            Ok(_) => {}
            Err(err) if err.kind() == ErrorKind::WriteZero => {
                return ffi::SQLITE_FULL;
            }
            Err(err) => return state.set_last_error(ffi::SQLITE_IOERR_WRITE, err),
        }

        ffi::SQLITE_OK
    }

    /// Truncate a file.
    pub unsafe extern "C" fn truncate<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        size: ffi::sqlite3_int64,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSYNC,
        };

        let size: u64 = if let Some(chunk_size) = state.chunk_size {
            (((size as usize + chunk_size - 1) / chunk_size) * chunk_size) as u64
        } else {
            size as u64
        };

        log::trace!("[{}] truncate size={} ({})", state.id, size, state.db_name);

        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_IOERR_TRUNCATE;
        // }

        if let Err(err) = state.file.set_len(size) {
            return state.set_last_error(ffi::SQLITE_IOERR_TRUNCATE, err);
        }

        ffi::SQLITE_OK
    }

    /// Persist changes to a file.
    pub unsafe extern "C" fn sync<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        flags: c_int,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSYNC,
        };
        log::trace!("[{}] sync ({})", state.id, state.db_name);

        #[cfg(feature = "sqlite_test")]
        {
            let is_full_sync = flags & 0x0F == ffi::SQLITE_SYNC_FULL;
            if is_full_sync {
                ffi::sqlite3_inc_fullsync_count();
            }
            ffi::sqlite3_inc_sync_count();
        }

        if let Err(err) = state.file.sync(flags & ffi::SQLITE_SYNC_DATAONLY > 0) {
            return state.set_last_error(ffi::SQLITE_IOERR_FSYNC, err);
        }

        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_ERROR;
        // }

        ffi::SQLITE_OK
    }

    /// Return the current file-size of a file.
    pub unsafe extern "C" fn file_size<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        p_size: *mut ffi::sqlite3_int64,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_FSTAT,
        };
        log::trace!("[{}] file_size ({})", state.id, state.db_name);

        if let Err(err) = state.file.size().and_then(|n| {
            let p_size: &mut ffi::sqlite3_int64 = p_size.as_mut().ok_or_else(null_ptr_error)?;
            *p_size = n as ffi::sqlite3_int64;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_FSTAT, err);
        }

        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_ERROR;
        // }

        ffi::SQLITE_OK
    }

    /// Lock a file.
    pub unsafe extern "C" fn lock<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        e_lock: c_int,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_LOCK,
        };
        log::trace!("[{}] lock ({})", state.id, state.db_name);

        let lock = match LockKind::from_i32(e_lock) {
            Some(lock) => lock,
            None => return ffi::SQLITE_IOERR_LOCK,
        };
        match state.file.lock(lock) {
            Ok(true) => {
                log::trace!("[{}] lock={:?} ({})", state.id, lock, state.db_name);

                ffi::SQLITE_OK
            }
            Ok(false) => {
                log::trace!(
                    "[{}] busy (denied {:?}) ({})",
                    state.id,
                    lock,
                    state.db_name
                );
                ffi::SQLITE_BUSY
            }
            Err(err) => state.set_last_error(ffi::SQLITE_IOERR_LOCK, err),
        }
    }

    /// Unlock a file.
    pub unsafe extern "C" fn unlock<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        e_lock: c_int,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_UNLOCK,
        };
        log::trace!("[{}] unlock ({})", state.id, state.db_name);

        let lock = match LockKind::from_i32(e_lock) {
            Some(lock) => lock,
            None => return ffi::SQLITE_IOERR_UNLOCK,
        };
        match state.file.unlock(lock) {
            Ok(true) => {
                log::trace!("[{}] unlock={:?} ({})", state.id, lock, state.db_name);
                ffi::SQLITE_OK
            }
            Ok(false) => ffi::SQLITE_BUSY,
            Err(err) => state.set_last_error(ffi::SQLITE_IOERR_UNLOCK, err),
        }
    }

    /// Check if another file-handle holds a [LockKind::Reserved] lock on a file.
    pub unsafe extern "C" fn check_reserved_lock<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        p_res_out: *mut c_int,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK,
        };
        log::trace!("[{}] check_reserved_lock ({})", state.id, state.db_name);

        // #[cfg(feature = "sqlite_test")]
        // if simulate_io_error() {
        //     return ffi::SQLITE_IOERR_CHECKRESERVEDLOCK;
        // }

        if let Err(err) = state.file.reserved().and_then(|is_reserved| {
            let p_res_out: &mut c_int = p_res_out.as_mut().ok_or_else(null_ptr_error)?;
            *p_res_out = is_reserved as c_int;
            Ok(())
        }) {
            return state.set_last_error(ffi::SQLITE_IOERR_UNLOCK, err);
        }

        ffi::SQLITE_OK
    }

    /// File control method. For custom operations on a mem-file.
    pub unsafe extern "C" fn file_control<V: Vfs, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
        op: c_int,
        p_arg: *mut c_void,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_NOTFOUND,
        };
        log::trace!("[{}] file_control op={} ({})", state.id, op, state.db_name);

        // Docs: https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html
        match op {
            // The following op codes are alreay handled by sqlite before, so no need to handle them
            // in a custom VFS.
            ffi::SQLITE_FCNTL_FILE_POINTER
            | ffi::SQLITE_FCNTL_VFS_POINTER
            | ffi::SQLITE_FCNTL_JOURNAL_POINTER
            | ffi::SQLITE_FCNTL_DATA_VERSION
            | ffi::SQLITE_FCNTL_RESERVE_BYTES => ffi::SQLITE_NOTFOUND,

            // The following op codes are no longer used and thus ignored.
            ffi::SQLITE_FCNTL_SYNC_OMITTED => ffi::SQLITE_NOTFOUND,

            // Used for debugging. Write current state of the lock into (int)pArg.
            ffi::SQLITE_FCNTL_LOCKSTATE => match state.file.current_lock() {
                Ok(lock) => {
                    if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                        *p_arg = lock as i32;
                    }
                    ffi::SQLITE_OK
                }
                Err(err) => state.set_last_error(ffi::SQLITE_ERROR, err),
            },

            // Relevant for proxy-type locking. Not implemented.
            ffi::SQLITE_FCNTL_GET_LOCKPROXYFILE | ffi::SQLITE_FCNTL_SET_LOCKPROXYFILE => {
                ffi::SQLITE_NOTFOUND
            }

            // Write last error number into (int)pArg.
            ffi::SQLITE_FCNTL_LAST_ERRNO => {
                if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                    *p_arg = state.last_errno;
                }
                ffi::SQLITE_OK
            }

            // Give the VFS layer a hint of how large the database file will grow to be during the
            // current transaction.
            ffi::SQLITE_FCNTL_SIZE_HINT => {
                let size_hint = match (p_arg as *mut i64)
                    .as_ref()
                    .cloned()
                    .and_then(|s| u64::try_from(s).ok())
                {
                    Some(chunk_size) => chunk_size,
                    None => {
                        return state.set_last_error(
                            ffi::SQLITE_NOTFOUND,
                            std::io::Error::new(ErrorKind::Other, "expect size hint arg"),
                        );
                    }
                };

                // #[cfg(feature = "sqlite_test")]
                // let _benign = simulate_io_error_benign();

                let current = match state.file.size() {
                    Ok(size) => size,
                    Err(err) => return state.set_last_error(ffi::SQLITE_ERROR, err),
                };

                if current > size_hint {
                    return ffi::SQLITE_OK;
                }

                if let Some(chunk_size) = state.chunk_size {
                    let chunk_size = chunk_size as u64;
                    let size = ((size_hint + chunk_size - 1) / chunk_size) * chunk_size;
                    if let Err(err) = state.file.set_len(size) {
                        return state.set_last_error(ffi::SQLITE_IOERR_TRUNCATE, err);
                    }
                } else if let Err(err) = state.file.set_len(size_hint) {
                    return state.set_last_error(ffi::SQLITE_IOERR_TRUNCATE, err);
                }

                // #[cfg(feature = "sqlite_test")]
                // if simulate_io_error() {
                //     return ffi::SQLITE_IOERR_TRUNCATE;
                // }

                ffi::SQLITE_OK
            }

            // Request that the VFS extends and truncates the database file in chunks of a size
            // specified by the user. Return an error as this is not forwarded to the [Vfs] trait
            // right now.
            ffi::SQLITE_FCNTL_CHUNK_SIZE => {
                let chunk_size = match (p_arg as *mut i32)
                    .as_ref()
                    .cloned()
                    .and_then(|s| usize::try_from(s).ok())
                {
                    Some(chunk_size) => chunk_size,
                    None => {
                        return state.set_last_error(
                            ffi::SQLITE_NOTFOUND,
                            std::io::Error::new(ErrorKind::Other, "expect chunk_size arg"),
                        );
                    }
                };

                if let Err(err) = state.file.set_chunk_size(chunk_size) {
                    return state.set_last_error(ffi::SQLITE_ERROR, err);
                }

                state.chunk_size = Some(chunk_size);

                ffi::SQLITE_OK
            }

            // Configure automatic retry counts and intervals for certain disk I/O operations for
            // the windows VFS in order to provide robustness in the presence of anti-virus
            // programs. Not implemented.
            ffi::SQLITE_FCNTL_WIN32_AV_RETRY => ffi::SQLITE_NOTFOUND,

            // Enable or disable the persistent WAL setting.
            ffi::SQLITE_FCNTL_PERSIST_WAL => ffi::SQLITE_NOTFOUND,

            // Indicate that, unless it is rolled back for some reason, the entire database file
            // will be overwritten by the current transaction. Not implemented.
            ffi::SQLITE_FCNTL_OVERWRITE => ffi::SQLITE_NOTFOUND,

            // Used to obtain the names of all VFSes in the VFS stack.
            ffi::SQLITE_FCNTL_VFSNAME => {
                if let Some(p_arg) = (p_arg as *mut *const c_char).as_mut() {
                    let name = ManuallyDrop::new(state.vfs_name.clone());
                    *p_arg = name.as_ptr();
                };

                ffi::SQLITE_OK
            }

            // Set or query the persistent "powersafe-overwrite" or "PSOW" setting.
            ffi::SQLITE_FCNTL_POWERSAFE_OVERWRITE => {
                if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                    if *p_arg < 0 {
                        // query current setting
                        *p_arg = state.powersafe_overwrite as i32;
                    } else {
                        state.powersafe_overwrite = *p_arg == 1;
                    }
                };

                ffi::SQLITE_OK
            }

            // Optionally intercept PRAGMA statements. Always fall back to normal pragma processing.
            ffi::SQLITE_FCNTL_PRAGMA => ffi::SQLITE_NOTFOUND,

            // May be invoked by SQLite on the database file handle shortly after it is opened in
            // order to provide a custom VFS with access to the connection's busy-handler callback.
            // Not implemented.
            ffi::SQLITE_FCNTL_BUSYHANDLER => ffi::SQLITE_NOTFOUND,

            // Generate a temporary filename. Not implemented.
            ffi::SQLITE_FCNTL_TEMPFILENAME => {
                if let Some(p_arg) = (p_arg as *mut *const c_char).as_mut() {
                    let name = state.vfs.temporary_name();
                    // unwrap() is fine as os strings are an arbitrary sequences of non-zero bytes
                    let name = CString::new(name.as_bytes()).unwrap();
                    let name = ManuallyDrop::new(name);
                    *p_arg = name.as_ptr();
                };

                ffi::SQLITE_OK
            }

            // Query or set the maximum number of bytes that will be used for memory-mapped I/O.
            // Not implemented.
            ffi::SQLITE_FCNTL_MMAP_SIZE => ffi::SQLITE_NOTFOUND,

            // Advisory information to the VFS about what the higher layers of the SQLite stack are
            // doing.
            ffi::SQLITE_FCNTL_TRACE => {
                let trace = CStr::from_ptr(p_arg as *const c_char);
                log::trace!("{}", trace.to_string_lossy());
                ffi::SQLITE_OK
            }

            // Check whether or not the file has been renamed, moved, or deleted since it was first
            // opened.
            ffi::SQLITE_FCNTL_HAS_MOVED => match state.file.moved() {
                Ok(moved) => {
                    if let Some(p_arg) = (p_arg as *mut i32).as_mut() {
                        *p_arg = moved as i32;
                    }
                    ffi::SQLITE_OK
                }
                Err(err) => state.set_last_error(ffi::SQLITE_ERROR, err),
            },

            // Sent to the VFS immediately before the xSync method is invoked on a database file
            // descriptor. Silently ignored.
            ffi::SQLITE_FCNTL_SYNC => ffi::SQLITE_OK,

            // Sent to the VFS after a transaction has been committed immediately but before the
            // database is unlocked. Silently ignored.
            ffi::SQLITE_FCNTL_COMMIT_PHASETWO => {
                state.file.commit_phasetwo();
                ffi::SQLITE_OK
            }

            // Used for debugging. Swap the file handle with the one pointed to by the pArg
            // argument. This capability is used during testing and only needs to be supported when
            // SQLITE_TEST is defined. Not implemented.
            ffi::SQLITE_FCNTL_WIN32_SET_HANDLE => ffi::SQLITE_NOTFOUND,

            // Signal to the VFS layer that it might be advantageous to block on the next WAL lock
            // if the lock is not immediately available. The WAL subsystem issues this signal during
            // rare circumstances in order to fix a problem with priority inversion.
            // Not implemented.
            ffi::SQLITE_FCNTL_WAL_BLOCK => ffi::SQLITE_NOTFOUND,

            // Implemented by zipvfs only.
            ffi::SQLITE_FCNTL_ZIPVFS => ffi::SQLITE_NOTFOUND,

            // Implemented by the special VFS used by the RBU extension only.
            ffi::SQLITE_FCNTL_RBU => ffi::SQLITE_NOTFOUND,

            // Obtain the underlying native file handle associated with a file handle.
            // Not implemented.
            ffi::SQLITE_FCNTL_WIN32_GET_HANDLE => ffi::SQLITE_NOTFOUND,

            // Usage is not documented. Not implemented.
            ffi::SQLITE_FCNTL_PDB => ffi::SQLITE_NOTFOUND,

            // Used for "batch write mode". Not supported.
            ffi::SQLITE_FCNTL_BEGIN_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_COMMIT_ATOMIC_WRITE
            | ffi::SQLITE_FCNTL_ROLLBACK_ATOMIC_WRITE => ffi::SQLITE_NOTFOUND,

            // Configure a VFS to block for up to M milliseconds before failing when attempting to
            // obtain a file lock using the xLock or xShmLock methods of the VFS. Not implemented.
            ffi::SQLITE_FCNTL_LOCK_TIMEOUT => ffi::SQLITE_NOTFOUND,

            // Used by in-memory VFS.
            ffi::SQLITE_FCNTL_SIZE_LIMIT => ffi::SQLITE_NOTFOUND,

            // Invoked from within a checkpoint in wal mode after the client has finished copying
            // pages from the wal file to the database file, but before the *-shm file is updated to
            // record the fact that the pages have been checkpointed. Silently ignored.
            ffi::SQLITE_FCNTL_CKPT_DONE => ffi::SQLITE_OK,

            // Invoked from within a checkpoint in wal mode before the client starts to copy pages
            // from the wal file to the database file. Silently ignored.
            ffi::SQLITE_FCNTL_CKPT_START => ffi::SQLITE_OK,

            // Detect whether or not there is a database client in another process with a wal-mode
            // transaction open on the database or not. Not implemented because it is a
            // unix-specific feature.
            ffi::SQLITE_FCNTL_EXTERNAL_READER => ffi::SQLITE_NOTFOUND,

            // Unknown use-case. Ignored.
            ffi::SQLITE_FCNTL_CKSM_FILE => ffi::SQLITE_NOTFOUND,

            _ => ffi::SQLITE_NOTFOUND,
        }
    }

    /// Return the sector-size in bytes for a file.
    pub unsafe extern "C" fn sector_size<V: Vfs, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR,
        };
        state.vfs.sector_size() as _
    }

    /// Return the device characteristic flags supported by a file.
    pub unsafe extern "C" fn device_characteristics<V, F: DatabaseHandle>(
        p_file: *mut ffi::sqlite3_file,
    ) -> c_int {
        let state = match file_state::<V, F>(p_file) {
            Ok(f) => f,
            Err(_) => return ffi::SQLITE_IOERR_SHMMAP,
        };

        log::trace!("[{}] device_characteristics", state.id,);

        // The following characteristics are needed to match the expected behavior of the tests.

        // after reboot following a crash or power loss, the only bytes in a file that were written
        // at the application level might have changed and that adjacent bytes, even bytes within
        // the same sector are guaranteed to be unchanged
        if state.powersafe_overwrite {
            ffi::SQLITE_IOCAP_POWERSAFE_OVERWRITE
        } else {
            0
        }
    }
}

// #[cfg(feature = "sqlite_test")]
// struct Benign;

// #[cfg(feature = "sqlite_test")]
// #[inline]
// unsafe fn simulate_io_error_benign() -> Benign {
//     ffi::sqlite3_set_io_error_benign(1);
//     Benign
// }

// #[cfg(feature = "sqlite_test")]
// impl Drop for Benign {
//     fn drop(&mut self) {
//         unsafe { ffi::sqlite3_set_io_error_benign(0) }
//     }
// }

// Note: When adding additional simulate_io_error() calls, retest:
// - malloc.test
// - ioerr2.test
// - backup_ioerr.test
#[cfg(feature = "sqlite_test")]
#[inline]
unsafe fn simulate_io_error() -> bool {
    if (ffi::sqlite3_get_io_error_persist() != 0 && ffi::sqlite3_get_io_error_hit() != 0)
        || ffi::sqlite3_dec_io_error_pending() == 1
    {
        ffi::sqlite3_inc_io_error_hit();
        if ffi::sqlite3_get_io_error_benign() == 0 {
            ffi::sqlite3_inc_io_error_hardhit();
        }

        return true;
    }

    false
}

#[cfg(feature = "sqlite_test")]
#[inline]
unsafe fn simulate_diskfull_error() -> bool {
    if ffi::sqlite3_get_diskfull_pending() != 0 {
        if ffi::sqlite3_get_diskfull_pending() == 1 {
            if ffi::sqlite3_get_io_error_benign() == 0 {
                ffi::sqlite3_inc_io_error_hardhit();
            }
            ffi::sqlite3_set_diskfull();
            ffi::sqlite3_set_io_error_hit(1);
            return true;
        } else {
            ffi::sqlite3_dec_diskfull_pending();
        }
    }

    false
}

impl<V> State<V> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        no
    }
}

impl<V, F: DatabaseHandle> FileExt<V, F> {
    fn set_last_error(&mut self, no: i32, err: std::io::Error) -> i32 {
        // log::error!("{} ({})", err, no);
        *(self.last_error.lock().unwrap()) = Some((no, err));
        self.last_errno = no;
        no
    }
}

fn null_ptr_error() -> std::io::Error {
    std::io::Error::new(ErrorKind::Other, "received null pointer")
}

unsafe fn vfs_state<'a, V>(ptr: *mut ffi::sqlite3_vfs) -> Result<&'a mut State<V>, std::io::Error> {
    let vfs: &mut ffi::sqlite3_vfs = ptr.as_mut().ok_or_else(null_ptr_error)?;
    let state = (vfs.pAppData as *mut State<V>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    Ok(state)
}

unsafe fn file_state<'a, V, F: DatabaseHandle>(
    ptr: *mut ffi::sqlite3_file,
) -> Result<&'a mut FileExt<V, F>, std::io::Error> {
    let f = (ptr as *mut FileState<V, F>)
        .as_mut()
        .ok_or_else(null_ptr_error)?;
    let ext = f.ext.assume_init_mut();
    Ok(ext)
}

impl OpenOptions {
    fn from_flags(flags: i32) -> Option<Self> {
        Some(OpenOptions {
            kind: OpenKind::from_flags(flags)?,
            access: OpenAccess::from_flags(flags)?,
            delete_on_close: flags & ffi::SQLITE_OPEN_DELETEONCLOSE > 0,
        })
    }

    fn to_flags(&self) -> i32 {
        self.kind.to_flags()
            | self.access.to_flags()
            | if self.delete_on_close {
                ffi::SQLITE_OPEN_DELETEONCLOSE
            } else {
                0
            }
    }
}

impl OpenKind {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags if flags & ffi::SQLITE_OPEN_MAIN_DB > 0 => Some(Self::MainDb),
            flags if flags & ffi::SQLITE_OPEN_MAIN_JOURNAL > 0 => Some(Self::MainJournal),
            flags if flags & ffi::SQLITE_OPEN_TEMP_DB > 0 => Some(Self::TempDb),
            flags if flags & ffi::SQLITE_OPEN_TEMP_JOURNAL > 0 => Some(Self::TempJournal),
            flags if flags & ffi::SQLITE_OPEN_TRANSIENT_DB > 0 => Some(Self::TransientDb),
            flags if flags & ffi::SQLITE_OPEN_SUBJOURNAL > 0 => Some(Self::SubJournal),
            flags if flags & ffi::SQLITE_OPEN_SUPER_JOURNAL > 0 => Some(Self::SuperJournal),
            flags if flags & ffi::SQLITE_OPEN_WAL > 0 => Some(Self::Wal),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenKind::MainDb => ffi::SQLITE_OPEN_MAIN_DB,
            OpenKind::MainJournal => ffi::SQLITE_OPEN_MAIN_JOURNAL,
            OpenKind::TempDb => ffi::SQLITE_OPEN_TEMP_DB,
            OpenKind::TempJournal => ffi::SQLITE_OPEN_TEMP_JOURNAL,
            OpenKind::TransientDb => ffi::SQLITE_OPEN_TRANSIENT_DB,
            OpenKind::SubJournal => ffi::SQLITE_OPEN_SUBJOURNAL,
            OpenKind::SuperJournal => ffi::SQLITE_OPEN_SUPER_JOURNAL,
            OpenKind::Wal => ffi::SQLITE_OPEN_WAL,
        }
    }
}

impl OpenAccess {
    fn from_flags(flags: i32) -> Option<Self> {
        match flags {
            flags
                if (flags & ffi::SQLITE_OPEN_CREATE > 0)
                    && (flags & ffi::SQLITE_OPEN_EXCLUSIVE > 0) =>
            {
                Some(Self::CreateNew)
            }
            flags if flags & ffi::SQLITE_OPEN_CREATE > 0 => Some(Self::Create),
            flags if flags & ffi::SQLITE_OPEN_READWRITE > 0 => Some(Self::Write),
            flags if flags & ffi::SQLITE_OPEN_READONLY > 0 => Some(Self::Read),
            _ => None,
        }
    }

    fn to_flags(self) -> i32 {
        match self {
            OpenAccess::Read => ffi::SQLITE_OPEN_READONLY,
            OpenAccess::Write => ffi::SQLITE_OPEN_READWRITE,
            OpenAccess::Create => ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
            OpenAccess::CreateNew => {
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE | ffi::SQLITE_OPEN_EXCLUSIVE
            }
        }
    }
}

impl LockKind {
    fn from_i32(lock: i32) -> Option<Self> {
        Some(match lock {
            ffi::SQLITE_LOCK_NONE => Self::None,
            ffi::SQLITE_LOCK_SHARED => Self::Shared,
            ffi::SQLITE_LOCK_RESERVED => Self::Reserved,
            ffi::SQLITE_LOCK_PENDING => Self::Pending,
            ffi::SQLITE_LOCK_EXCLUSIVE => Self::Exclusive,
            _ => return None,
        })
    }

    fn to_i32(self) -> i32 {
        match self {
            Self::None => ffi::SQLITE_LOCK_NONE,
            Self::Shared => ffi::SQLITE_LOCK_SHARED,
            Self::Reserved => ffi::SQLITE_LOCK_RESERVED,
            Self::Pending => ffi::SQLITE_LOCK_PENDING,
            Self::Exclusive => ffi::SQLITE_LOCK_EXCLUSIVE,
        }
    }
}

impl PartialOrd for LockKind {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.to_i32().partial_cmp(&other.to_i32())
    }
}

impl Default for LockKind {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default)]
pub struct WalDisabled;

impl wip::WalIndex for WalDisabled {
    fn enabled() -> bool {
        false
    }

    fn map(&mut self, _region: u32) -> Result<[u8; 32768], std::io::Error> {
        Err(std::io::Error::new(ErrorKind::Other, "wal is disabled"))
    }

    fn lock(
        &mut self,
        _locks: Range<u8>,
        _lock: wip::WalIndexLock,
    ) -> Result<bool, std::io::Error> {
        Err(std::io::Error::new(ErrorKind::Other, "wal is disabled"))
    }

    fn delete(self) -> Result<(), std::io::Error> {
        Ok(())
    }
}

#[derive(Debug)]
pub enum RegisterError {
    Nul(std::ffi::NulError),
    Register(i32),
}

impl std::error::Error for RegisterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Nul(err) => Some(err),
            Self::Register(_) => None,
        }
    }
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nul(_) => f.write_str("interior nul byte in name found"),
            Self::Register(code) => {
                write!(f, "registering sqlite vfs failed with error code: {}", code)
            }
        }
    }
}

impl From<std::ffi::NulError> for RegisterError {
    fn from(err: std::ffi::NulError) -> Self {
        Self::Nul(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_order() {
        assert!(LockKind::None < LockKind::Shared);
        assert!(LockKind::Shared < LockKind::Reserved);
        assert!(LockKind::Reserved < LockKind::Pending);
        assert!(LockKind::Pending < LockKind::Exclusive);
    }
}
