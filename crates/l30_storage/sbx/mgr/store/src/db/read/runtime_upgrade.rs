//! One runtime upgrade by identity, and the upgrade fencing a sandbox.

use crate::db::{Connection, Result, RuntimeUpgradeOperationId, RuntimeUpgradeRecord};

/// The upgrade row, `None` without one; a row that does not decode fails the
/// read.
pub fn runtime_upgrade(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<Option<RuntimeUpgradeRecord>> {
    crate::db::runtime_upgrade(conn, operation_id)
}

/// The upgrade of `sandbox_id` still owing the retirement of the checkpoint
/// it superseded: one that fences it, or a verified one.
pub fn unretired_runtime_upgrade(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RuntimeUpgradeOperationId>> {
    crate::db::unretired_runtime_upgrade(conn, sandbox_id)
}

/// The upgrade that refuses other activations of `sandbox_id`, if one does.
pub fn runtime_upgrade_fence(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RuntimeUpgradeOperationId>> {
    Ok(crate::db::fencing_runtime_upgrade(conn, sandbox_id)?.map(|(operation_id, _)| operation_id))
}
