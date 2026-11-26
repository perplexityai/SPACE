//! The internal API's retirement of a quarantine episode after the node that
//! published a recovery receipt validated the sandbox's canonical suspend
//! artifact.
//!
//! Two guarded steps, each bound to the exact episode the caller selected:
//! [`record_node_recovery`] appends the episode's recovery event and
//! [`retire_node_recovered_episode`] closes it. Each returns how many rows it
//! changed, `0` when the episode moved on. The caller runs both in its
//! publication transaction and decides what a count other than `1` means.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{
    SandboxId, SandboxQuarantineHistoryAction, SandboxQuarantineReasonCode, SandboxQuarantineSource,
};

use pplx_space_sql_locator::sql;
use uuid::Uuid;

/// The episode a node recovery retires and the event it records.
#[derive(Debug, Clone, Copy)]
pub struct NodeRecovery {
    /// The history event's id.
    pub history_id: Uuid,
    pub sandbox_id: SandboxId,
    /// The episode the caller selected.
    pub quarantine_id: Uuid,
    /// The source the episode must have been opened by.
    pub quarantined_by: SandboxQuarantineSource,
    /// The reason the episode must carry.
    pub reason_code: SandboxQuarantineReasonCode,
    /// The source recorded on the event.
    pub recorded_by: SandboxQuarantineSource,
    pub action: SandboxQuarantineHistoryAction,
    /// The publishing node, recorded in the event's evidence.
    pub node_id: NodeId,
}

/// Append the recovery event to the sandbox's open episode when it is still
/// `quarantine_id`, opened by `quarantined_by` for `reason_code`. Returns the
/// rows inserted: `0` when the episode is gone or isn't that one.
pub async fn record_node_recovery(
    executor: impl Executor<'_>,
    recovery: &NodeRecovery,
) -> Result<u64, DbError> {
    let result = sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history (
                 id, quarantine_id, sandbox_id, occurred_at, source, action,
                 reason_code, message, next_action, attempt_count, evidence
             )
             SELECT $1, qs.quarantine_id, qs.sandbox_id, NOW(), $2, $3,
                    qs.reason_code,
                    'node validated its canonical suspend artifact after runtime cleanup',
                    NULL, qs.attempt_count,
                    qs.evidence || jsonb_build_object('recovered_by_node_id', $4::TEXT)
               FROM sandbox_quarantine_state qs
              WHERE qs.sandbox_id = $5
                AND qs.quarantine_id = $6
                AND qs.source = $8
                AND qs.reason_code = $7"
    ))
    .bind(recovery.history_id)
    .bind(recovery.recorded_by.as_str())
    .bind(recovery.action.as_str())
    .bind(recovery.node_id.to_string())
    .bind(recovery.sandbox_id)
    .bind(recovery.quarantine_id)
    .bind(recovery.reason_code.as_str())
    .bind(recovery.quarantined_by.as_str())
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected())
}

/// Close the sandbox's open episode when it is still `quarantine_id`.
/// Returns the rows deleted: `0` when the episode is gone or was replaced.
pub async fn retire_node_recovered_episode(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    quarantine_id: Uuid,
) -> Result<u64, DbError> {
    let result = sqlx::query(sql!(
        "DELETE FROM sandbox_quarantine_state
              WHERE sandbox_id = $1 AND quarantine_id = $2"
    ))
    .bind(sandbox_id)
    .bind(quarantine_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests;
