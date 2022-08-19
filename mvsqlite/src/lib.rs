pub mod commit_group;
pub mod io_engine;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_c;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_misc;
pub mod sqlite_vfs;
pub mod tempfile;
pub mod vfs;

use std::sync::{atomic::Ordering, Arc};

use crate::{io_engine::IoEngine, vfs::MultiVersionVfs};

pub static VFS_NAME: &'static str = "mv-vfs";

static GLOBAL_INIT_DONE: std::sync::Once = std::sync::Once::new();

pub struct InitOptions {
    pub coroutine: bool,
}

pub fn init_with_options(opts: InitOptions) {
    let mut ok = false;
    GLOBAL_INIT_DONE.call_once(|| {
        init_with_options_impl(opts);
        ok = true;
    });
    if !ok {
        eprintln!("mvsqlite: already initialized");
        std::process::abort();
    }
}

fn init_with_options_impl(opts: InitOptions) {
    let mut sector_size: usize = 8192;
    if let Ok(s) = std::env::var("MVSQLITE_SECTOR_SIZE") {
        let requested_ss = s
            .parse::<usize>()
            .expect("MVSQLITE_SECTOR_SIZE must be a usize");
        if ![4096, 8192, 16384, 32768].contains(&requested_ss) {
            panic!("MVSQLITE_SECTOR_SIZE must be one of 4096, 8192, 16384, 32768");
        }
        sector_size = requested_ss;
    }
    if let Ok(s) = std::env::var("MVSQLITE_PAGE_CACHE_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_PAGE_CACHE_SIZE must be a usize");
        vfs::PAGE_CACHE_SIZE.store(requested, Ordering::Relaxed);
    }
    if let Ok(s) = std::env::var("MVSQLITE_WRITE_CHUNK_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_WRITE_CHUNK_SIZE must be a usize");
        vfs::WRITE_CHUNK_SIZE.store(requested, Ordering::Relaxed);
    }

    let data_plane = std::env::var("MVSQLITE_DATA_PLANE").expect("MVSQLITE_DATA_PLANE is not set");
    let io_engine = Arc::new(IoEngine::new(opts.coroutine));
    let vfs = MultiVersionVfs {
        data_plane,
        io: io_engine,
        sector_size,
        http_client: reqwest::Client::new(),
    };

    sqlite_vfs::register(VFS_NAME, vfs, true).expect("Failed to register VFS");
    tracing::info!(sector_size = sector_size, "mvsqlite initialized");
}

#[no_mangle]
#[cfg(not(feature = "global-init"))]
pub extern "C" fn init_mvsqlite() {
    GLOBAL_INIT_DONE.call_once(|| {
        eprintln!("mvsqlite: global init not enabled");
        std::process::abort();
    });
}

#[no_mangle]
#[cfg(feature = "global-init")]
pub extern "C" fn init_mvsqlite() {
    use backtrace::Backtrace;
    use tracing_subscriber::{fmt::SubscriberBuilder, EnvFilter};

    GLOBAL_INIT_DONE.call_once(|| {
        if std::env::var("MVSQLITE_LOG_JSON").unwrap_or_default() == "1" {
            SubscriberBuilder::default()
                .with_env_filter(EnvFilter::from_default_env())
                .with_writer(std::io::stderr)
                .json()
                .init();
        } else {
            SubscriberBuilder::default()
                .with_env_filter(EnvFilter::from_default_env())
                .with_writer(std::io::stderr)
                .pretty()
                .init();
        }

        std::panic::set_hook(Box::new(|info| {
            let bt = Backtrace::new();
            tracing::error!(backtrace = ?bt, "{}", info);
            std::process::abort();
        }));

        init_with_options_impl(InitOptions { coroutine: false });
    });
}

#[no_mangle]
pub unsafe extern "C" fn init_mvsqlite_connection(db: *mut sqlite_c::sqlite3) {
    if std::env::var("MVSQLITE_EXPERIMENTAL_COMMIT_GROUP")
        .map(|x| x == "1")
        .unwrap_or(false)
    {
        let mv_commitgroup_begin_name = b"mv_commitgroup_begin\0";
        let mv_commitgroup_commit_name = b"mv_commitgroup_commit\0";
        let mv_commitgroup_rollback_name = b"mv_commitgroup_rollback\0";
        let mv_commitgroup_lock_disable_name = b"mv_commitgroup_lock_disable\0";
        let mv_commitgroup_lock_enable_name = b"mv_commitgroup_lock_enable\0";
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
        let ret = sqlite_c::sqlite3_create_function_v2(
            db,
            mv_commitgroup_lock_disable_name.as_ptr() as *const i8,
            0,
            sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
            std::ptr::null_mut(),
            Some(commit_group::mv_commitgroup_lock_disable),
            None,
            None,
            None,
        );
        assert_eq!(ret, sqlite_c::SQLITE_OK);
        let ret = sqlite_c::sqlite3_create_function_v2(
            db,
            mv_commitgroup_lock_enable_name.as_ptr() as *const i8,
            0,
            sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
            std::ptr::null_mut(),
            Some(commit_group::mv_commitgroup_lock_enable),
            None,
            None,
            None,
        );
        assert_eq!(ret, sqlite_c::SQLITE_OK);
    }
}
