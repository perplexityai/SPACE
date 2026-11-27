//! The quarantine writes of the cluster controller's orphaned-sandbox sweep:
//! opening the sandbox's episode and recording that it entered quarantine,
//! one sandbox at a time.
//!
//! Unguarded writes. The sweep runs both in the transaction whose
//! `pplx_space_sandbox_store::orphan::quarantine_orphan` just moved the
//! sandbox to `quarantined`, state first and history second.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{
    SandboxId, SandboxLifecycle, SandboxQuarantineHistoryAction, SandboxQuarantineNextAction,
    SandboxQuarantineReasonCode, SandboxQuarantineSource,
};
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};
use uuid::Uuid;

/// What the sweep decided about one sandbox it quarantines, shared by
/// [`upsert_state`] and [`insert_history`].
#[derive(Debug, Clone, Copy)]
pub struct OrphanQuarantine<'a> {
    pub sandbox_id: SandboxId,
    pub quarantine_id: Uuid,
    pub source: SandboxQuarantineSource,
    pub reason_code: SandboxQuarantineReasonCode,
    pub reason_message: &'a str,
    pub next_action: SandboxQuarantineNextAction,
    pub evidence: &'a JsonValue,
}

/// Open episode `quarantine.quarantine_id` for the sandbox, entered now from
/// `from_lifecycle` with `last_observed_node_id` as the node last seen
/// running it. A sandbox that already has an episode has it replaced: the
/// schedule, attempt count, claim and last error start over.
pub async fn upsert_state(
    executor: impl Executor<'_>,
    quarantine: &OrphanQuarantine<'_>,
    from_lifecycle: SandboxLifecycle,
    last_observed_node_id: Option<NodeId>,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_state (sandbox_id,
             quarantine_id,
             entered_at,
             updated_at,
             entered_from_lifecycle,
             source,
             reason_code,
             reason_message,
             evidence,
             next_action,
             last_observed_node_id)
         VALUES ($1, $2, NOW(), NOW(), $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (sandbox_id) DO UPDATE SET quarantine_id = EXCLUDED.quarantine_id,
             entered_at = EXCLUDED.entered_at,
             updated_at = EXCLUDED.updated_at,
             entered_from_lifecycle = EXCLUDED.entered_from_lifecycle,
             source = EXCLUDED.source,
             reason_code = EXCLUDED.reason_code,
             reason_message = EXCLUDED.reason_message,
             evidence = EXCLUDED.evidence,
             next_action = EXCLUDED.next_action,
             next_action_after = NULL,
             next_action_owner = NULL,
             attempt_count = 0,
             claim_token = NULL,
             claim_expires_at = NULL,
             last_error = NULL,
             last_observed_node_id = EXCLUDED.last_observed_node_id"
    ))
    .bind(quarantine.sandbox_id)
    .bind(quarantine.quarantine_id)
    .bind(from_lifecycle)
    .bind(quarantine.source.as_str())
    .bind(quarantine.reason_code.as_str())
    .bind(quarantine.reason_message)
    .bind(Json(quarantine.evidence))
    .bind(quarantine.next_action.as_str())
    .bind(last_observed_node_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Record history event `history_id`, `action`, at now in episode
/// `quarantine.quarantine_id`.
pub async fn insert_history(
    executor: impl Executor<'_>,
    quarantine: &OrphanQuarantine<'_>,
    history_id: Uuid,
    action: SandboxQuarantineHistoryAction,
) -> Result<(), DbError> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history (id,
             quarantine_id,
             sandbox_id,
             occurred_at,
             source,
             action,
             reason_code,
             message,
             next_action,
             evidence)
         VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7, $8, $9)"
    ))
    .bind(history_id)
    .bind(quarantine.quarantine_id)
    .bind(quarantine.sandbox_id)
    .bind(quarantine.source.as_str())
    .bind(action.as_str())
    .bind(quarantine.reason_code.as_str())
    .bind(quarantine.reason_message)
    .bind(quarantine.next_action.as_str())
    .bind(Json(quarantine.evidence))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
