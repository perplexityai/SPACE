//! Storage for the control-plane Postgres `sandbox_quarantine_state` and
//! `sandbox_quarantine_history` tables.
//!
//! Two parts. The column lists and conflict assignments below are SQL
//! fragments that the cluster controller's orphaned-sandbox sweep and the
//! internal API's published-state writer splice into their statements; other
//! writers of those tables still write their SQL inline. [`active_quarantine`]
//! and [`quarantine_history`] are the read verbs;
//! [`record_recovered_to_suspended`] and [`delete_quarantine_state`] are the
//! cluster controller's recovery of an abandoned publication, run in its
//! transaction under its locks and rechecks. sbx-mgr's SQLite tables of
//! the same names belong to `pplx-space-sbx-mgr-store`, not here. The fragments
//! stay here until store functions for these tables replace them.
//!
//! [`locks`] holds episode-only locks callers take on their transaction, and
//! [`node_recovery_receipt`] the internal API's guarded retirement of an
//! episode a node recovered. The internal API's pause-failed recovery locks
//! episodes together with their `sandboxes` rows in one statement, in
//! `pplx-space-sandbox-store`'s `quarantine_publication` module.

use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

pub mod locks;
pub mod node_recovery_receipt;

/// A sandbox's open quarantine episode, from `sandbox_quarantine_state`.
///
/// The vocabulary columns stay text so a value written by a newer build still
/// reads; their closed sets live in `pplx_space_sandbox_model`
/// (`SandboxQuarantineSource`, `SandboxQuarantineReasonCode`,
/// `SandboxQuarantineNextAction`).
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct ActiveQuarantine {
    pub quarantine_id: Uuid,
    pub entered_at: OffsetDateTime,
    pub updated_at: OffsetDateTime,
    pub entered_from_lifecycle: Option<String>,
    pub source: String,
    pub reason_code: String,
    pub reason_message: Option<String>,
    pub next_action: String,
    pub next_action_after: Option<OffsetDateTime>,
    pub attempt_count: i64,
    pub last_error: Option<String>,
    pub last_observed_node_id: Option<NodeId>,
}

/// One event of a sandbox's quarantine episodes, from
/// `sandbox_quarantine_history`: entering, reconfirming, or recovering
/// (`SandboxQuarantineHistoryAction`).
#[derive(Debug, Clone, PartialEq, Eq, FromRow)]
pub struct QuarantineHistoryEntry {
    pub id: Uuid,
    pub quarantine_id: Uuid,
    pub occurred_at: OffsetDateTime,
    pub source: String,
    pub action: String,
    pub reason_code: Option<String>,
    pub message: Option<String>,
    pub next_action: Option<String>,
    pub error: Option<String>,
}

/// The sandbox's open quarantine episode; `None` when it is not quarantined.
pub async fn active_quarantine(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<ActiveQuarantine>, sqlx::Error> {
    sqlx::query_as::<_, ActiveQuarantine>(sql!(
        "SELECT quarantine_id,
                entered_at,
                updated_at,
                entered_from_lifecycle,
                source,
                reason_code,
                reason_message,
                next_action,
                next_action_after,
                attempt_count,
                last_error,
                last_observed_node_id
         FROM sandbox_quarantine_state
         WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

/// The sandbox's newest `limit` quarantine events across all its episodes,
/// newest first. A sandbox never quarantined has none.
pub async fn quarantine_history(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    limit: u32,
) -> Result<Vec<QuarantineHistoryEntry>, sqlx::Error> {
    sqlx::query_as::<_, QuarantineHistoryEntry>(sql!(
        "SELECT id,
                quarantine_id,
                occurred_at,
                source,
                action,
                reason_code,
                message,
                next_action,
                error
         FROM sandbox_quarantine_history
         WHERE sandbox_id = $1
         ORDER BY occurred_at DESC, id DESC
         LIMIT $2"
    ))
    .bind(sandbox_id)
    .bind(i64::from(limit))
    .fetch_all(executor)
    .await
}

/// Append a `recovered_to_suspended` history event, `history_id`, to the
/// sandbox's open episode, recording `snapshot_id` in its evidence: the
/// cluster controller found the worker row and execution pod gone and a
/// ready suspend snapshot matching the recorded activity. Writes nothing when
/// the sandbox isn't quarantined. The caller holds the sandbox's and the
/// episode's row locks and has rechecked the recovery.
pub async fn record_recovered_to_suspended(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    history_id: Uuid,
    snapshot_id: SnapshotId,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "INSERT INTO sandbox_quarantine_history
            (id, quarantine_id, sandbox_id, source, action, reason_code, message, attempt_count, evidence)
         SELECT $2, quarantine_id, sandbox_id, source, 'recovered_to_suspended', reason_code,
                'Worker row and execution pod absent; ready suspend snapshot metadata matches recorded activity',
                attempt_count, evidence || jsonb_build_object('snapshot_id', $3::TEXT)
           FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(history_id)
    .bind(snapshot_id.to_string())
    .execute(executor)
    .await?;
    Ok(())
}

/// Close the sandbox's open episode, whichever it is. `false` when it had
/// none.
pub async fn delete_quarantine_state(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query(sql!(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(result.rows_affected() > 0)
}

pub fn quarantine_state_insert_columns() -> &'static str {
    "sandbox_id,
     quarantine_id,
     entered_at,
     updated_at,
     entered_from_lifecycle,
     source,
     reason_code,
     reason_message,
     evidence,
     next_action,
     last_observed_node_id"
}

pub fn quarantine_state_conflict_assignments() -> &'static str {
    "quarantine_id = EXCLUDED.quarantine_id,
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
}

pub fn quarantine_history_insert_columns() -> &'static str {
    "id,
     quarantine_id,
     sandbox_id,
     occurred_at,
     source,
     action,
     reason_code,
     message,
     next_action,
     evidence"
}

#[cfg(test)]
mod tests;
