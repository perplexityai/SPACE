//! Runtime upgrades that still owe work.

use crate::db::{Connection, Result, RuntimeUpgradeRecord};

/// Every upgrade not yet `retired`, oldest first, in one unbounded read:
/// unsettled rows are transient. A row that doesn't decode is skipped with a
/// warning and its fence stays.
pub fn unsettled_runtime_upgrades(conn: &Connection) -> Result<Vec<RuntimeUpgradeRecord>> {
    crate::db::unsettled_runtime_upgrades(conn)
}
