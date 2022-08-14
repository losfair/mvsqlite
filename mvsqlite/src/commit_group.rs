use std::{cell::RefCell, ffi::CString, sync::Arc};

use mvclient::{MultiVersionClient, NamespaceCommitIntent};

use crate::{
    io_engine::IoEngine,
    sqlite_c::{
        sqlite3_context, sqlite3_result_error, sqlite3_result_null, sqlite3_result_text,
        sqlite3_value,
    },
};

pub struct CommitGroup {
    intents: Vec<NamespaceCommitIntent>,
    client: Option<Arc<MultiVersionClient>>,
    io: Option<Arc<IoEngine>>,
    pub current_version: Option<String>,
    pub lock_disabled: bool,
}

impl Default for CommitGroup {
    fn default() -> Self {
        Self {
            intents: Vec::new(),
            client: None,
            io: None,
            current_version: None,
            lock_disabled: false,
        }
    }
}

impl CommitGroup {
    pub fn set_client_and_io(&mut self, client: &Arc<MultiVersionClient>, io: &Arc<IoEngine>) {
        if self.client.is_none() {
            self.client = Some(client.clone());
        }

        if self.io.is_none() {
            self.io = Some(io.clone());
        }
    }
    pub fn append(&mut self, intent: NamespaceCommitIntent) {
        self.intents.push(intent);
    }
}

thread_local! {
  pub static CURRENT_COMMIT_GROUP: RefCell<Option<CommitGroup>> = RefCell::new(None);
}

pub unsafe extern "C" fn mv_commitgroup_begin(
    ctx: *mut sqlite3_context,
    _argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        if cg.is_some() {
            throw(
                ctx,
                "mv_commitgroup_begin called recursively in a commit group",
            );
        } else {
            *cg = Some(CommitGroup::default());
        }
    })
}

pub unsafe extern "C" fn mv_commitgroup_commit(
    ctx: *mut sqlite3_context,
    _argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        cg.take()
    });

    let mut cg = match cg {
        Some(x) => x,
        None => {
            throw(
                ctx,
                "mv_commitgroup_commit called without a commit group open",
            );
            return;
        }
    };

    if cg.intents.is_empty() {
        sqlite3_result_null(ctx);
        return;
    }

    let client = cg
        .client
        .take()
        .expect("mv_commitgroup_commit called without a client");
    let io = cg
        .io
        .take()
        .expect("mv_commitgroup_commit called without an io engine");
    let res = io
        .run(async { client.apply_commit_intents(&cg.intents).await })
        .expect("Failed to apply commit intents");
    match res {
        Some(result) => {
            tracing::info!(version = result.version, duration = ?result.duration, num_pages = result.num_pages, "transaction committed (commit group)");
            let version = CString::new(result.version).unwrap();
            sqlite3_result_text(
                ctx,
                version.as_ptr(),
                version.as_bytes().len() as i32,
                crate::sqlite_misc::SQLITE_TRANSIENT(),
            );
        }
        None => {
            tracing::warn!("transaction conflict (commit group)");
            let fake_version = CString::new("conflict").unwrap();
            sqlite3_result_text(
                ctx,
                fake_version.as_ptr(),
                fake_version.as_bytes().len() as i32,
                crate::sqlite_misc::SQLITE_TRANSIENT(),
            );
        }
    }
}

pub unsafe extern "C" fn mv_commitgroup_rollback(
    ctx: *mut sqlite3_context,
    _argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    let cg = CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        cg.take()
    });

    let mut cg = match cg {
        Some(x) => x,
        None => {
            throw(
                ctx,
                "mv_commitgroup_rollback called without a commit group open",
            );
            return;
        }
    };

    if cg.intents.is_empty() {
        return;
    }

    // Discard all page changes
    for intent in &mut cg.intents {
        intent.init.num_pages = 0;
        intent.requests.clear();
    }

    let client = cg
        .client
        .take()
        .expect("mv_commitgroup_rollback called without a client");
    let io = cg
        .io
        .take()
        .expect("mv_commitgroup_rollback called without an io engine");
    let res = io
        .run(async { client.apply_commit_intents(&cg.intents).await })
        .expect("Failed to apply commit intents");
    match res {
        Some(result) => {
            tracing::info!(version = result.version, duration = ?result.duration, num_pages = result.num_pages, "transaction rolled back (commit group)");
        }
        None => {
            tracing::warn!("conflict during rollback (commit group)");
        }
    }
}

pub unsafe extern "C" fn mv_commitgroup_lock_disable(
    ctx: *mut sqlite3_context,
    _argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        match &mut *cg {
            Some(cg) => {
                cg.lock_disabled = true;
            }
            None => {
                throw(
                    ctx,
                    "mv_commitgroup_lock_disable called outside a commit group",
                );
            }
        }
    });
}

pub unsafe extern "C" fn mv_commitgroup_lock_enable(
    ctx: *mut sqlite3_context,
    _argc: ::std::os::raw::c_int,
    _argv: *mut *mut sqlite3_value,
) {
    CURRENT_COMMIT_GROUP.with(|cg| {
        let mut cg = cg.borrow_mut();
        match &mut *cg {
            Some(cg) => {
                cg.lock_disabled = false;
            }
            None => {
                throw(
                    ctx,
                    "mv_commitgroup_lock_enable called outside a commit group",
                );
            }
        }
    });
}

fn throw(ctx: *mut sqlite3_context, msg: &str) {
    let msg = msg.as_bytes();
    unsafe {
        sqlite3_result_error(ctx, msg.as_ptr() as *const _, msg.len() as i32);
    }
}
