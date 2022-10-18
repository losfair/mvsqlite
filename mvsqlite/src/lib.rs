pub mod io_engine;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_c;
#[allow(non_snake_case, non_camel_case_types)]
pub mod sqlite_misc;
pub mod sqlite_vfs;
pub mod tempfile;
mod util;
pub mod vfs;

use std::{
    collections::HashMap,
    ffi::CString,
    os::raw::c_char,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use io_engine::IoEngineKind;
use mvfs::vfs::AbstractHttpClient;

use crate::{
    io_engine::IoEngine,
    util::get_conn,
    vfs::{AbstractIoEngine, MultiVersionVfs},
};

pub static VFS_NAME: &'static str = "mvsqlite";

static GLOBAL_INIT_DONE: std::sync::Once = std::sync::Once::new();

pub struct InitOptions {
    pub io_engine_kind: IoEngineKind,
    pub fork_tolerant: bool,
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
    let mut force_http2 = false;

    if let Ok(s) = std::env::var("MVSQLITE_SECTOR_SIZE") {
        let requested_ss = s
            .parse::<usize>()
            .expect("MVSQLITE_SECTOR_SIZE must be a usize");
        if ![4096, 8192, 16384, 32768].contains(&requested_ss) {
            panic!("MVSQLITE_SECTOR_SIZE must be one of 4096, 8192, 16384, 32768");
        }
        sector_size = requested_ss;
        tracing::debug!(requested = requested_ss, "setting sector size",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_PAGE_CACHE_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_PAGE_CACHE_SIZE must be a usize");
        vfs::PAGE_CACHE_SIZE.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting page cache size",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_WRITE_CHUNK_SIZE") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_WRITE_CHUNK_SIZE must be a usize");
        vfs::WRITE_CHUNK_SIZE.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting write chunk size",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_PREFETCH_DEPTH") {
        let requested = s
            .parse::<usize>()
            .expect("MVSQLITE_PREFETCH_DEPTH must be a usize");
        vfs::PREFETCH_DEPTH.store(requested, Ordering::Relaxed);
        tracing::debug!(requested, "setting prefetch depth",);
    }
    if let Ok(s) = std::env::var("MVSQLITE_FORCE_HTTP2") {
        if s.as_str() == "1" {
            force_http2 = true;
            tracing::debug!("enabling forced http2");
        }
    }

    let timeout_secs = if let Ok(s) = std::env::var("MVSQLITE_HTTP_TIMEOUT_SECS") {
        let requested = s
            .parse::<u64>()
            .expect("MVSQLITE_HTTP_TIMEOUT_SECS must be a u64");
        tracing::debug!(requested, "configuring http timeout secs");
        requested
    } else {
        10
    };

    let mut db_name_map: HashMap<String, String> = HashMap::new();
    if let Ok(s) = std::env::var("MVSQLITE_DB_NAME_MAP") {
        for mapping in s.split(',').map(|x| x.trim()).filter(|x| !x.is_empty()) {
            let mut parts = mapping.splitn(2, '=');
            let from = parts.next().unwrap();
            let to = parts.next().unwrap();
            db_name_map.insert(from.to_string(), to.to_string());
        }
        tracing::debug!(num_entries = db_name_map.len(), "configuring db name map");
    }
    let db_name_map = Arc::new(db_name_map);

    let mut lock_owner: Option<String> = None;
    if let Ok(s) = std::env::var("MVSQLITE_LOCK_OWNER") {
        if !s.is_empty() {
            tracing::debug!(lock_owner = s, "configuring lock owner");
            lock_owner = Some(s);
        }
    }

    let build_http_client = move || {
        let mut builder = reqwest::ClientBuilder::new();
        builder = builder.timeout(Duration::from_secs(timeout_secs));
        if force_http2 {
            builder = builder.http2_prior_knowledge();
        }
        builder.build().expect("failed to build http client")
    };

    let data_plane = std::env::var("MVSQLITE_DATA_PLANE").expect("MVSQLITE_DATA_PLANE is not set");
    let io_engine = if opts.fork_tolerant {
        let kind = opts.io_engine_kind;
        AbstractIoEngine::Builder(Arc::new(Box::new(move || Arc::new(IoEngine::new(kind)))))
    } else {
        AbstractIoEngine::Prebuilt(Arc::new(IoEngine::new(opts.io_engine_kind)))
    };

    let http_client = if opts.fork_tolerant {
        AbstractHttpClient::Builder(Arc::new(Box::new(build_http_client)))
    } else {
        AbstractHttpClient::Prebuilt(build_http_client())
    };

    let default_vfs = MultiVersionVfs {
        io: io_engine.clone(),
        inner: mvfs::MultiVersionVfs {
            data_plane: data_plane.clone(),
            sector_size,
            http_client: http_client.clone(),
            db_name_map: db_name_map.clone(),
            lock_owner: lock_owner.clone(),
            fork_tolerant: opts.fork_tolerant,
        },
    };

    sqlite_vfs::register(VFS_NAME, default_vfs, true).expect("Failed to register VFS");

    for sector_size in [4096, 8192, 16384, 32768] {
        let vfs = MultiVersionVfs {
            io: io_engine.clone(),
            inner: mvfs::MultiVersionVfs {
                data_plane: data_plane.clone(),
                sector_size,
                http_client: http_client.clone(),
                db_name_map: db_name_map.clone(),
                lock_owner: lock_owner.clone(),
                fork_tolerant: opts.fork_tolerant,
            },
        };
        sqlite_vfs::register(&format!("{}-{}", VFS_NAME, sector_size), vfs, false)
            .expect("Failed to register VFS");
    }

    tracing::info!(default_sector_size = sector_size, "mvsqlite initialized");
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
            tracing::error!(backtrace = ?bt, info = %info, "panic");
            std::process::abort();
        }));

        let mut io_engine_kind = IoEngineKind::MultiThread;
        let mut fork_tolerant = false;

        if let Ok(s) = std::env::var("MVSQLITE_FORK_TOLERANT") {
            if s.as_str() == "1" {
                io_engine_kind = IoEngineKind::CurrentThread;
                fork_tolerant = true;
                tracing::debug!("enabling fork_tolerant");
            }
        }

        init_with_options_impl(InitOptions {
            io_engine_kind,
            fork_tolerant,
        });
    });
}

#[no_mangle]
pub unsafe extern "C" fn init_mvsqlite_connection(db: *mut sqlite_c::sqlite3) {
    let mv_last_known_version_name = b"mv_last_known_version\0";
    let mv_time2version_name = b"mv_time2version\0";
    let mv_pin_version_name = b"mv_pin_version\0";
    let mv_unpin_version_name = b"mv_unpin_version\0";

    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_last_known_version_name.as_ptr() as *const c_char,
        1,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(mv_last_known_version),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);

    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_time2version_name.as_ptr() as *const c_char,
        2,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(mv_time2version),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);

    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_pin_version_name.as_ptr() as *const c_char,
        2,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(mv_pin_version),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);

    let ret = sqlite_c::sqlite3_create_function_v2(
        db,
        mv_unpin_version_name.as_ptr() as *const c_char,
        1,
        sqlite_c::SQLITE_UTF8 | sqlite_c::SQLITE_DIRECTONLY,
        std::ptr::null_mut(),
        Some(mv_unpin_version),
        None,
        None,
        None,
    );
    assert_eq!(ret, sqlite_c::SQLITE_OK);
}

unsafe extern "C" fn mv_last_known_version(
    ctx: *mut sqlite_c::sqlite3_context,
    argc: std::os::raw::c_int,
    argv: *mut *mut sqlite_c::sqlite3_value,
) {
    assert_eq!(argc, 1);
    let db = sqlite_c::sqlite3_context_db_handle(ctx);
    let selected_db = sqlite_c::sqlite3_value_text(*argv.add(0));
    let selected_db = std::ffi::CStr::from_ptr(selected_db as *const c_char)
        .to_str()
        .unwrap();
    let conn = get_conn(db, selected_db);

    if let Some(version) = conn.inner.last_known_version() {
        let version = CString::new(version).unwrap();
        sqlite_c::sqlite3_result_text(
            ctx,
            version.as_ptr(),
            version.as_bytes().len() as i32,
            crate::sqlite_misc::SQLITE_TRANSIENT(),
        );
    } else {
        sqlite_c::sqlite3_result_null(ctx);
    }
}

unsafe extern "C" fn mv_time2version(
    ctx: *mut sqlite_c::sqlite3_context,
    argc: std::os::raw::c_int,
    argv: *mut *mut sqlite_c::sqlite3_value,
) {
    assert_eq!(argc, 2);
    let db = sqlite_c::sqlite3_context_db_handle(ctx);
    let selected_db = sqlite_c::sqlite3_value_text(*argv.add(0));
    let selected_db = std::ffi::CStr::from_ptr(selected_db as *const c_char)
        .to_str()
        .unwrap();
    let mut conn = get_conn(db, selected_db);
    let timestamp_secs = sqlite_c::sqlite3_value_int64(*argv.add(1));
    let io = conn.io.clone();
    let info = io.run(conn.inner.time2version(timestamp_secs as u64));
    if let Some(after) = &info.after {
        let version = CString::new(after.version.as_str()).unwrap();
        sqlite_c::sqlite3_result_text(
            ctx,
            version.as_ptr(),
            version.as_bytes().len() as i32,
            crate::sqlite_misc::SQLITE_TRANSIENT(),
        );
    } else {
        sqlite_c::sqlite3_result_null(ctx);
    }
}

unsafe extern "C" fn mv_pin_version(
    ctx: *mut sqlite_c::sqlite3_context,
    argc: std::os::raw::c_int,
    argv: *mut *mut sqlite_c::sqlite3_value,
) {
    assert_eq!(argc, 2);
    let db = sqlite_c::sqlite3_context_db_handle(ctx);
    let selected_db = sqlite_c::sqlite3_value_text(*argv.add(0));
    let selected_db = std::ffi::CStr::from_ptr(selected_db as *const c_char)
        .to_str()
        .unwrap();
    let mut conn = get_conn(db, selected_db);

    let version = sqlite_c::sqlite3_value_text(*argv.add(1));
    let version = std::ffi::CStr::from_ptr(version as *const c_char)
        .to_str()
        .unwrap();
    match conn.inner.pin_version(version.to_string()) {
        Ok(()) => {
            sqlite_c::sqlite3_result_null(ctx);
        }
        Err(e) => {
            let error = CString::new(format!("{}", e)).unwrap();
            sqlite_c::sqlite3_result_error(ctx, error.as_ptr(), error.as_bytes().len() as i32);
        }
    }
}

unsafe extern "C" fn mv_unpin_version(
    ctx: *mut sqlite_c::sqlite3_context,
    argc: std::os::raw::c_int,
    argv: *mut *mut sqlite_c::sqlite3_value,
) {
    assert_eq!(argc, 1);
    let db = sqlite_c::sqlite3_context_db_handle(ctx);
    let selected_db = sqlite_c::sqlite3_value_text(*argv.add(0));
    let selected_db = std::ffi::CStr::from_ptr(selected_db as *const c_char)
        .to_str()
        .unwrap();
    let mut conn = get_conn(db, selected_db);

    match conn.inner.unpin_version() {
        Ok(()) => {
            sqlite_c::sqlite3_result_null(ctx);
        }
        Err(e) => {
            let error = CString::new(format!("{}", e)).unwrap();
            sqlite_c::sqlite3_result_error(ctx, error.as_ptr(), error.as_bytes().len() as i32);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn mvsqlite_autocommit_backoff(db: *mut sqlite_c::sqlite3) {
    tracing::warn!("autocommit backoff");
    let conn = get_conn(db, "main");
    conn.io.run(async {
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
}
