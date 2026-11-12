//! One runtime upgrade by identity, its report, and the upgrade fencing a
//! sandbox.

use crate::db::{
    Connection, Result, RuntimeUpgradeAttemptOutcome, RuntimeUpgradeOperationId,
    RuntimeUpgradeRecord, RuntimeUpgradeRefusedAttempt, RuntimeUpgradeReport,
    RuntimeUpgradeRequestIdentity,
};

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

/// The upgrade of `sandbox_id` that must leave it `stopped` while its boot's
/// execution may yet run it, if one exists: a fencing one, or a verified one
/// whose boot's execution still runs. Other unretired upgrades of the sandbox
/// don't hide it.
pub fn stay_stopped_runtime_upgrade_awaiting_its_stop(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RuntimeUpgradeOperationId>> {
    crate::db::stay_stopped_runtime_upgrade_awaiting_its_stop(conn, sandbox_id)
}

/// The upgrade that refuses other activations of `sandbox_id`, if one does.
pub fn runtime_upgrade_fence(
    conn: &Connection,
    sandbox_id: &str,
) -> Result<Option<RuntimeUpgradeOperationId>> {
    Ok(crate::db::fencing_runtime_upgrade(conn, sandbox_id)?.map(|(operation_id, _)| operation_id))
}

/// Where `operation_id` stands on this node, `None` when the node holds no
/// record of it; see [`RuntimeUpgradeReport`].
pub fn runtime_upgrade_report(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<Option<RuntimeUpgradeReport>> {
    crate::db::runtime_upgrade_report(conn, operation_id)
}

/// What the operation behind a recorded outcome asked for, `None` without
/// one; see [`RuntimeUpgradeRequestIdentity`].
pub fn recorded_runtime_upgrade_request(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
) -> Result<Option<RuntimeUpgradeRequestIdentity>> {
    crate::db::recorded_runtime_upgrade_request(conn, operation_id)
}

/// The attempt `attempt_id` the operation refused after reaching its
/// target, `None` when it refused no such attempt.
pub fn runtime_upgrade_refused_attempt(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) -> Result<Option<RuntimeUpgradeRefusedAttempt>> {
    crate::db::runtime_upgrade_refused_attempt(conn, operation_id, attempt_id)
}

/// The rollback or refusal final for the attempt `attempt_id` under
/// `operation_id`, whatever later attempts recorded since; `None`
/// without one.
pub fn runtime_upgrade_attempt_outcome(
    conn: &Connection,
    operation_id: RuntimeUpgradeOperationId,
    attempt_id: &str,
) -> Result<Option<RuntimeUpgradeAttemptOutcome>> {
    crate::db::runtime_upgrade_attempt_outcome(conn, operation_id, attempt_id)
}
