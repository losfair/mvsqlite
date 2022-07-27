pub mod io_engine;
pub mod sqlite_vfs;
pub mod vfs;
use std::sync::{Arc, Mutex};

use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

use crate::{io_engine::IoEngine, vfs::MultiVersionVfs};

//type Sqlite3OpenFn = unsafe extern "C" fn(filename: *const c_char, pp_db: *mut *mut c_void) -> i32;

//static mut REAL_SQLITE3_OPEN: Option<Sqlite3OpenFn> = None;

#[no_mangle]
pub extern "C" fn init_mvsqlite() {
    if std::env::var("MVSQLITE_LOG_JSON").unwrap_or_default() == "1" {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .json()
            .init();
    } else {
        SubscriberBuilder::default()
            .with_env_filter(EnvFilter::from_default_env())
            .pretty()
            .init();
    }

    let data_plane = std::env::var("MVSQLITE_DATA_PLANE").expect("MVSQLITE_DATA_PLANE is not set");
    let io_engine = Arc::new(IoEngine::new(false));
    let vfs = MultiVersionVfs {
        data_plane,
        io: io_engine,
        shared: Arc::new(Mutex::new(Default::default())),
    };

    /*let real_sqlite3_open = unsafe {
        REAL_SQLITE3_OPEN = std::mem::transmute::<_, Option<Sqlite3OpenFn>>(libc::dlsym(
            libc::RTLD_NEXT,
            b"sqlite3_open\0".as_ptr() as *const c_char,
        ));
        REAL_SQLITE3_OPEN.expect("real sqlite3_open not found") as usize
    };*/
    sqlite_vfs::register("mv-vfs", vfs, true).expect("Failed to register VFS");
    tracing::info!(
        //real_sqlite3_open = format!("{:p}", real_sqlite3_open as *const c_void),
        "mvsqlite initialized"
    );
}
