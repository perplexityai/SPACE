//! Storage for the control-plane Postgres `sandbox_quarantine_state` and
//! `sandbox_quarantine_history` tables.
//!
//! [`active_quarantine`] and [`quarantine_history`] are the read verbs;
//! [`record_recovered_to_suspended`] and [`delete_quarantine_state`] are the
//! cluster controller's recovery of an abandoned publication, run in its
//! transaction under its locks and rechecks. Two modules hold the writes
//! that enter quarantine, each named for the path that issues them:
//! [`orphan_quarantine`], the cluster controller's orphaned-sandbox sweep,
//! and [`published_quarantine`], the internal API's published-state writer.
//! Other writers of these tables still write their SQL inline. sbx-mgr's
//! SQLite tables of the same names belong to `pplx-space-sbx-mgr-store`, not
//! here.
//!
//! [`locks`] holds episode-only locks callers take on their transaction, and
//! [`node_recovery_receipt`] the internal API's guarded retirement of an
//! episode a node recovered. The internal API's pause-failed recovery locks
//! episodes together with their `sandboxes` rows in one statement, in
//! `pplx-space-sandbox-store`'s `quarantine_publication` module.

use pplx_space_accounts::NodeId;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::SnapshotId;
use pplx_space_sql_locator::sql;
use sqlx::FromRow;
use time::OffsetDateTime;
use uuid::Uuid;

pub mod locks;
pub mod node_recovery_receipt;
pub mod orphan_quarantine;
pub mod published_quarantine;

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
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<ActiveQuarantine>, DbError> {
    Ok(sqlx::query_as::<_, ActiveQuarantine>(sql!(
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
    .fetch_optional(executor.into_sqlx())
    .await?)
}

/// The sandbox's newest `limit` quarantine events across all its episodes,
/// newest first. A sandbox never quarantined has none.
pub async fn quarantine_history(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    limit: u32,
) -> Result<Vec<QuarantineHistoryEntry>, DbError> {
    Ok(sqlx::query_as::<_, QuarantineHistoryEntry>(sql!(
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
    .fetch_all(executor.into_sqlx())
    .await?)
}

/// Append a `recovered_to_suspended` history event, `history_id`, to the
/// sandbox's open episode, recording `snapshot_id` in its evidence: the
/// cluster controller found the worker row and execution pod gone and a
/// ready suspend snapshot matching the recorded activity. Writes nothing when
/// the sandbox isn't quarantined. The caller holds the sandbox's and the
/// episode's row locks and has rechecked the recovery.
pub async fn record_recovered_to_suspended(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
    history_id: Uuid,
    snapshot_id: SnapshotId,
) -> Result<(), DbError> {
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
    .execute(executor.into_sqlx())
    .await?;
    Ok(())
}

/// Close the sandbox's open episode, whichever it is. `false` when it had
/// none.
pub async fn delete_quarantine_state(
    executor: impl Executor<'_>,
    sandbox_id: SandboxId,
) -> Result<bool, DbError> {
    let result = sqlx::query(sql!(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .execute(executor.into_sqlx())
    .await?;
    Ok(result.rows_affected() > 0)
}

#[cfg(test)]
mod tests;
