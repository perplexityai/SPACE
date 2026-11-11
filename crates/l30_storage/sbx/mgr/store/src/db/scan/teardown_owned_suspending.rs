//! `suspending` rows an active teardown-suspend job owns.

use crate::db::{Connection, Result, TeardownOwnedSuspendingRow};

/// Every `suspending` row under exactly one active teardown-suspend job, with
/// its generation, in id order. Startup leaves such a row to its job, so the
/// service resumes the job's driver once startup recovery ends.
pub fn teardown_owned_suspending(conn: &Connection) -> Result<Vec<TeardownOwnedSuspendingRow>> {
    crate::db::teardown_owned_suspending_rows(conn)
}
