//! Writes of an owner's requested settings onto `sandboxes`: the network
//! policy override and the snapshot opt-in. Each records what the caller
//! asked for; the node converges on it through the reconcile the caller
//! enqueues in the same transaction.
//!
//! Each verb is one `UPDATE sandboxes` statement keyed by the row's id. The
//! caller owns everything around it: the transaction and the reconcile
//! enqueue that has to commit with the write.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use sqlx::types::JsonValue;

/// **Intent.** Replace the sandbox's network policy override
/// (`policy_override_json`, `None` to inherit the template default) and
/// return the row's `network_policy_generation` afterwards. The generation
/// advances only when the stored override changes, so rewriting the same
/// value is idempotent. Returns `None` when the row is gone or
/// `expected_generation` (the caller's `If-Match`, `None` for no
/// precondition) no longer matches.
///
/// The generation check is the only guard: there is no lifecycle predicate,
/// so the override is recorded on a row in any lifecycle.
pub async fn set_network_policy_override(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    policy_override_json: Option<JsonValue>,
    expected_generation: Option<i64>,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar::<_, i64>(sql!(
        "UPDATE sandboxes
             SET network_policy_override_json = $1,
                 network_policy_generation =
                     CASE
                         WHEN network_policy_override_json IS DISTINCT FROM $1
                         THEN network_policy_generation + 1
                         ELSE network_policy_generation
                     END
             WHERE id = $2
               AND ($3::BIGINT IS NULL OR network_policy_generation = $3)
             RETURNING network_policy_generation"
    ))
    .bind(policy_override_json)
    .bind(sandbox_id)
    .bind(expected_generation)
    .fetch_optional(executor)
    .await
}

/// **Intent.** Record whether the owner wants snapshots taken for the
/// sandbox (`snapshots_enabled_desired`).
///
/// No guard beyond the id, and the row count is not reported: a missing row
/// writes nothing and is not an error, as before the move.
pub async fn set_snapshots_enabled_desired(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    enabled: bool,
) -> Result<(), sqlx::Error> {
    sqlx::query(sql!(
        "UPDATE sandboxes
             SET snapshots_enabled_desired = $1
             WHERE id = $2"
    ))
    .bind(enabled)
    .bind(sandbox_id)
    .execute(executor)
    .await?;
    Ok(())
}
