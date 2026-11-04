//! Restore transfer scopes whose `sandboxes` row is gone.

use crate::db::inventory::RestoreTransferScopeScan;
use crate::db::{Connection, RestoreTransferScopeRow, Result};

/// How [`rowless_restore_scopes`] reads.
#[derive(Debug)]
pub enum RowlessRestoreScopes<'a> {
    /// The next page of a periodic sweep: at most eight rowless scopes from
    /// at most 128 scope rows, live and malformed ones included in the scan
    /// budget.
    Cursor(&'a mut RestoreTransferScopeScan),
    /// Every rowless scope in one unbounded read, for startup.
    All,
}

/// `(sandbox_id, scope)` of rowless scopes. Malformed scopes are retained
/// and skipped.
pub fn rowless_restore_scopes(
    conn: &Connection,
    read: RowlessRestoreScopes<'_>,
) -> Result<Vec<(String, RestoreTransferScopeRow)>> {
    match read {
        RowlessRestoreScopes::Cursor(scan) => scan.next_batch(conn),
        RowlessRestoreScopes::All => crate::db::rowless_restore_transfer_scopes(conn),
    }
}
