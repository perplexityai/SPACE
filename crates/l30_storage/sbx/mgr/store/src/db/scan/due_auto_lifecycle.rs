//! Sandboxes whose auto-lifecycle deadline has passed.

use pplx_space_common_core::types::SandboxId;

use crate::db::{AutoLifecycleScan, Connection, DueAutoLifecycleSandbox, Result};

/// Up to `available` due sandboxes not in `active`, from two lanes: the
/// deadline lane admits newly due work promptly, and the rowid lane, which
/// `cursor` positions, makes progress past rows it cannot act on. Malformed
/// rows are skipped.
pub fn due_auto_lifecycle(
    conn: &Connection,
    now_secs: u64,
    active: &[SandboxId],
    available: usize,
    cursor: &mut AutoLifecycleScan,
) -> Result<Vec<DueAutoLifecycleSandbox>> {
    crate::db::due_auto_lifecycle_batch(conn, now_secs, active, available, cursor)
}
