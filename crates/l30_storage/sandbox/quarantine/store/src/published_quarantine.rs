//! The quarantine writes of the internal API's published-state writer: the
//! episodes of the sandboxes a node's publication moved to `quarantined`,
//! and the history events that they entered quarantine, a batch at a time.
//!
//! Unguarded writes. The writer runs both in the publication's transaction,
//! after `pplx_space_sandbox_store::ingest::mark_published_quarantined` moved
//! the rows, and passes only the sandboxes it moved, state first and history
//! second.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::{SandboxQuarantineHistoryAction, SandboxQuarantineNextAction};
use pplx_space_sql_locator::sql;
use sqlx::types::{Json, JsonValue};

/// Open an episode, entered now, for each record in `quarantined` (a JSON
/// array of `{sandbox_id, from_lifecycle, failure_reason, source,
/// reason_code, quarantine_id}`), with `next_action` and `node_id` as the
/// node last seen running the sandbox and the evidence. A sandbox that
/// already has an episode has it replaced: the schedule, attempt count,
/// claim and last error start over.
pub async fn upsert_states(
    executor: impl Executor<'_>,
    node_id: NodeId,
    next_action: SandboxQuarantineNextAction,
    quarantined: JsonValue,
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
         SELECT
             input.sandbox_id,
             input.quarantine_id,
             NOW(),
             NOW(),
             input.from_lifecycle,
             input.source,
             input.reason_code,
             NULLIF(BTRIM(input.failure_reason), ''),
             jsonb_build_object('node_id', $3::TEXT),
             $2,
             $1
         FROM jsonb_to_recordset($4::jsonb) AS input(
             sandbox_id UUID,
             from_lifecycle TEXT,
             failure_reason TEXT,
             source TEXT,
             reason_code TEXT,
             quarantine_id UUID
         )
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
    .bind(node_id)
    .bind(next_action.as_str())
    .bind(node_id.to_string())
    .bind(Json(quarantined))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Record a history event, `action` at now with `next_action` and `node_id`
/// as the evidence, for each record in `quarantined` (a JSON array of
/// `{sandbox_id, failure_reason, source, reason_code, quarantine_id,
/// history_id}`).
pub async fn insert_history(
    executor: impl Executor<'_>,
    node_id: NodeId,
    action: SandboxQuarantineHistoryAction,
    next_action: SandboxQuarantineNextAction,
    quarantined: JsonValue,
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
         SELECT
             input.history_id,
             input.quarantine_id,
             input.sandbox_id,
             NOW(),
             input.source,
             $2,
             input.reason_code,
             NULLIF(BTRIM(input.failure_reason), ''),
             $3,
             jsonb_build_object('node_id', $1::TEXT)
         FROM jsonb_to_recordset($4::jsonb) AS input(
             sandbox_id UUID,
             failure_reason TEXT,
             source TEXT,
             reason_code TEXT,
             quarantine_id UUID,
             history_id UUID
         )"
    ))
    .bind(node_id.to_string())
    .bind(action.as_str())
    .bind(next_action.as_str())
    .bind(Json(quarantined))
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}
