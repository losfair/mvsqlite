pub mod commit_group;
pub mod io_engine;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_c;
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
pub extern "C" fn init_mvsqlite_connection(_db: *mut sqlite_c::sqlite3) {}
