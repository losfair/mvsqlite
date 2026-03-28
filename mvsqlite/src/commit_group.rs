use std::ffi::CString;

use mvfs::commit_group::{self, CommitOutput};

use crate::{
    sqlite_c::{
        sqlite3_context, sqlite3_context_db_handle, sqlite3_result_error,
        sqlite3_result_error_code, sqlite3_result_null, sqlite3_result_text, sqlite3_value,
    },
    util::get_conn,
};

pub unsafe extern "C" fn mv_commit_group_begin(
    ctx: *mut sqlite3_context,
    argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    assert_eq!(argc, 0);
    if let Err(e) = commit_group::begin() {
        throw(ctx, &e.to_string());
    } else {
        sqlite3_result_null(ctx);
    }
}

pub unsafe extern "C" fn mv_commit_group_commit(
    ctx: *mut sqlite3_context,
    argc: ::std::os::raw::c_int,
    argv: *mut *mut sqlite3_value,
) {
    assert_eq!(argc, 1);
    let db = sqlite3_context_db_handle(ctx);
    let selected_db = std::ffi::CStr::from_ptr(
        crate::sqlite_c::sqlite3_value_text(*argv.add(0)) as *const std::os::raw::c_char
    )
    .to_str()
    .unwrap();
    let conn = get_conn(db, selected_db);
    let result = commit_group::commit(|x| conn.io.run(x));

    match result {
        Ok(CommitOutput::Empty) => sqlite3_result_null(ctx),
        Ok(CommitOutput::Committed(result)) => {
            tracing::info!(
                version = result.version,
                duration = ?result.duration,
                num_pages = result.num_pages,
                "transaction committed (commit group)"
            );
            let version = CString::new(result.version).unwrap();
            sqlite3_result_text(
                ctx,
                version.as_ptr(),
                version.as_bytes().len() as i32,
                crate::sqlite_misc::SQLITE_TRANSIENT(),
            );
        }
        Ok(CommitOutput::Conflict) => {
            tracing::warn!("transaction conflict (commit group)");
            throw_with_code(ctx, crate::sqlite_c::SQLITE_PERM, "commit group conflict");
        }
        Err(e) => throw(ctx, &e.to_string()),
    }
}

pub unsafe extern "C" fn mv_commit_group_rollback(
    ctx: *mut sqlite3_context,
    argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    assert_eq!(argc, 1);
    let result = commit_group::rollback();

    match result {
        Ok(()) => tracing::info!("transaction rolled back (commit group)"),
        Err(e) => throw(ctx, &e.to_string()),
    }
}

fn throw(ctx: *mut sqlite3_context, msg: &str) {
    let msg = msg.as_bytes();
    unsafe {
        sqlite3_result_error(ctx, msg.as_ptr() as *const _, msg.len() as i32);
    }
}

fn throw_with_code(ctx: *mut sqlite3_context, code: i32, msg: &str) {
    let msg = msg.as_bytes();
    unsafe {
        sqlite3_result_error(ctx, msg.as_ptr() as *const _, msg.len() as i32);
        sqlite3_result_error_code(ctx, code);
    }
}
