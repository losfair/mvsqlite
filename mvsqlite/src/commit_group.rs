use crate::sqlite_c::{sqlite3_context, sqlite3_value};

pub unsafe extern "C" fn mv_commitgroup_begin(
  ctx: *mut sqlite3_context,
  n_args: ::std::os::raw::c_int,
  args: *mut *mut sqlite3_value,
) {
  
}
