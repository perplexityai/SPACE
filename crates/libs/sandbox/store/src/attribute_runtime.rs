//! The guarded write of `sandboxes.activating_client_id`, the record of whose
//! activation created the sandbox's current runtime.
//!
//! Besides the row inserts, the record has one writer: the substrate, when it
//! starts tracking a new runtime. On Kubernetes that's the statement that
//! moves the active execution row to a different pod, which sets the record
//! to that pod's `space.pplx.net/client-id` label. The caller owns the locks
//! (`sandboxes`, then the execution row) and re-checks the generation and pod
//! itself, so this statement touches `sandboxes` alone. On Direct, which has
//! no execution row, it's the transaction that commits a start, wake or owned
//! restore receipt: the statement runs before that transaction's route
//! identity rows, or under the restore's `sandboxes` row lock, so `sandboxes`
//! is locked first either way.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{ClientId, SandboxId, SandboxLifecycle};

/// Lifecycles in which no activation can be creating a runtime. The record
/// isn't written in them, so a teardown, a stop or a suspend that observes a
/// pod doesn't re-attribute the sandbox. The typed lifecycles here are exactly
/// those [`attributes_runtime`] rejects; `deleted` and `cleanup_required` are
/// stored states with no [`SandboxLifecycle`] variant.
pub const UNATTRIBUTED_LIFECYCLES: [&str; 7] = [
    "deleting",
    "deleted",
    "suspending",
    "stopping",
    "failed",
    "quarantined",
    "cleanup_required",
];

/// Whether an activation can be creating a runtime for a sandbox in
/// `lifecycle`, so pod tracking may write its record. Exhaustive, so a new
/// lifecycle has to be classified here; a test keeps
/// [`UNATTRIBUTED_LIFECYCLES`] in step with it.
pub const fn attributes_runtime(lifecycle: SandboxLifecycle) -> bool {
    match lifecycle {
        SandboxLifecycle::Created
        | SandboxLifecycle::Creating
        | SandboxLifecycle::Starting
        | SandboxLifecycle::Started
        | SandboxLifecycle::Restoring
        | SandboxLifecycle::Pausing
        | SandboxLifecycle::Paused
        | SandboxLifecycle::Resuming
        | SandboxLifecycle::Suspended
        | SandboxLifecycle::Stopped
        | SandboxLifecycle::Checkpointing => true,
        SandboxLifecycle::Suspending
        | SandboxLifecycle::Stopping
        | SandboxLifecycle::Deleting
        | SandboxLifecycle::Quarantined
        | SandboxLifecycle::Failed => false,
    }
}

/// Set the record to `client_id`, or to `NULL` for a runtime that carries no
/// attribution. Returns whether the row changed: `false` when the record
/// already holds that value, or the sandbox is in one of the
/// [`UNATTRIBUTED_LIFECYCLES`] or gone.
pub async fn set_activating_client(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
    client_id: Option<&ClientId>,
) -> Result<bool, sqlx::Error> {
    let changed = sqlx::query(sql!(
        "UPDATE sandboxes
            SET activating_client_id = $2
          WHERE id = $1
            AND lifecycle <> ALL($3)
            AND activating_client_id IS DISTINCT FROM $2"
    ))
    .bind(sandbox_id)
    .bind(client_id)
    .bind(&UNATTRIBUTED_LIFECYCLES[..])
    .execute(executor)
    .await?
    .rows_affected();
    Ok(changed == 1)
}

/// The sandbox's current record, read without a lock: `Ok(None)` when the
/// sandbox is gone, `Ok(Some(None))` when nothing is recorded.
pub async fn activating_client(
    executor: impl sqlx::PgExecutor<'_>,
    sandbox_id: SandboxId,
) -> Result<Option<Option<ClientId>>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<ClientId>>(sql!(
        "SELECT activating_client_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_optional(executor)
    .await
}

#[cfg(test)]
#[path = "attribute_runtime/tests.rs"]
mod tests;
