pub mod commit_group;
pub mod io_engine;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_c;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_misc;
pub mod sqlite_vfs;
pub mod vfs;

use std::sync::Arc;

use backtrace::Backtrace;
use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

use crate::{io_engine::IoEngine, vfs::MultiVersionVfs};

pub static VFS_NAME: &'static str = "mv-vfs";

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

    std::panic::set_hook(Box::new(|info| {
        let bt = Backtrace::new();
        tracing::error!(backtrace = ?bt, "{}", info);
        std::process::abort();
    }));

    let data_plane = std::env::var("MVSQLITE_DATA_PLANE").expect("MVSQLITE_DATA_PLANE is not set");
    let io_engine = Arc::new(IoEngine::new(false));
    let vfs = MultiVersionVfs {
        data_plane,
        io: io_engine,
    };

    sqlite_vfs::register(VFS_NAME, vfs, true).expect("Failed to register VFS");
    tracing::info!("mvsqlite initialized");
}

#[no_mangle]
pub unsafe extern "C" fn init_mvsqlite_connection(db: *mut sqlite_c::sqlite3) {
    let mv_commitgroup_begin_name = b"mv_commitgroup_begin\0";
    let mv_commitgroup_commit_name = b"mv_commitgroup_commit\0";
    let mv_commitgroup_rollback_name = b"mv_commitgroup_rollback\0";
    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_commitgroup_begin_name.as_ptr() as *const i8,
        0,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(commit_group::mv_commitgroup_begin),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);
    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_commitgroup_commit_name.as_ptr() as *const i8,
        0,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(commit_group::mv_commitgroup_commit),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);
    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_commitgroup_rollback_name.as_ptr() as *const i8,
        0,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(commit_group::mv_commitgroup_rollback),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);
}
