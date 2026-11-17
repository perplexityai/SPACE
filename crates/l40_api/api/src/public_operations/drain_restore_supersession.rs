//! A user's lifecycle request supersedes a drain restore of the same sandbox
//! that hasn't started: the user said what the sandbox should be, and the
//! drain's restore would undo it. Three places cancel one: the admission of a
//! user's operation ([`super::create_operation_tx`]), a lifecycle call that
//! records no operation ([`cancel_drain_restores_for_action`]), and the release
//! of a drain restore's attempt when a user's operation has queued behind it.
//!
//! A request that doesn't apply to a suspended sandbox, a `pause` or a
//! `resume` of the VM it no longer has, supersedes nothing: it gets the
//! answer it gets without a drain, or, queued as an operation, waits behind
//! the restore and runs on what the restore leaves, the VM it brought back
//! or the sandbox still suspended.
//!
//! None cancels a restore whose attempt holds the row: the keyed queue runs
//! the user's operation after it. Between attempts the row is `suspended`,
//! or, after an attempt whose Wake was sent and timed out, `restoring` under
//! the restore's lease until the node's receipt lands it `started` or the
//! lease lapses and the transient-lifecycle sweeper settles it. A canceled
//! restore leaves nothing else behind: the user's operation meets that row
//! as it meets any restore in flight.

use pplx_space_accounts::OrgId;
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxId;
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicErrorCode,
    PublicOperationEventType, PublicSandboxId, SandboxLifecycleAction,
};
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{
    ApiProduct, PublicOperationStoreError, StoredOperationKind, append_operation_event_tx,
};

/// Whether a user's operation of `kind` on a sandbox supersedes a drain
/// restore of it: whether running it on the `suspended` sandbox the restore
/// waits for changes its lifecycle or deletes it. Admission looks for a drain
/// restore to cancel only for these kinds.
pub fn supersedes_drain_restore(kind: StoredOperationKind) -> bool {
    let kind = match kind {
        StoredOperationKind::DrainRestoreSandbox => return false,
        StoredOperationKind::Public(kind) => kind,
    };
    match kind {
        OperationKind::DeleteSandbox
        | OperationKind::StartSandbox
        | OperationKind::StopSandbox
        | OperationKind::SuspendSandbox
        | OperationKind::RestoreSandbox
        | OperationKind::WakeSandbox
        | OperationKind::KillSandbox
        | OperationKind::RollbackSandbox => true,
        // Both act on a resident VM. A drain restore waits while the
        // sandbox has none to act on, `suspended` or `restoring` under the
        // restore's own lease, so neither applies to it: each waits behind
        // the restore and runs on what it leaves.
        OperationKind::PauseSandbox | OperationKind::ResumeSandbox => false,
        // Metadata: none of these moves the lifecycle.
        OperationKind::UpdateSandbox
        | OperationKind::EnableSandboxSnapshots
        | OperationKind::DisableSandboxSnapshots
        | OperationKind::ForkSandbox => false,
        // A checkpoint snapshots a running VM; on a suspended sandbox it
        // answers unavailable without waking it, and leaves the lifecycle.
        OperationKind::CheckpointSandbox => false,
        // A session's own operation is recorded already succeeded and does
        // nothing to the sandbox. The wake a session's use implies is a
        // `wake_sandbox` operation of its own, which supersedes.
        OperationKind::CreateSession => false,
        // Names a template, never a sandbox.
        OperationKind::DeleteTemplate => false,
        // Names a sandbox that doesn't exist yet, which no drain suspended,
        // so admitting one looks for no drain restore.
        OperationKind::CreateSandbox => false,
    }
}

/// The labels of every public kind that supersedes a drain restore, as SQL
/// binds them.
pub(super) fn superseding_kind_labels() -> Vec<&'static str> {
    OperationKind::ALL
        .iter()
        .copied()
        .filter(|kind| supersedes_drain_restore(StoredOperationKind::Public(*kind)))
        .map(OperationKind::as_str)
        .collect()
}

/// What superseded a drain restore.
#[derive(Debug, Clone, Copy)]
pub enum DrainRestoreSupersededBy {
    /// A user's operation.
    Operation(OperationId),
    /// A user's lifecycle call that recorded no operation.
    Action(SandboxLifecycleAction),
}

/// What a supersession did to the drain restores it found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainRestoreSupersession {
    /// The drain restores it canceled.
    pub canceled: Vec<OperationId>,
}

/// Count drain restore `restore`, canceled as superseded by `by`, and log
/// both; call once the transaction that canceled it commits.
pub(super) fn record_superseded_drain_restore(restore: OperationId, by: DrainRestoreSupersededBy) {
    crate::metrics::operations::completed(
        ApiProduct::Native.as_str(),
        StoredOperationKind::DrainRestoreSandbox.as_str(),
        OperationState::Canceled.as_str(),
    );
    match by {
        DrainRestoreSupersededBy::Operation(by) => tracing::info!(
            operation_id = %restore,
            superseding_operation_id = %by,
            "drain restore canceled: a later user request supersedes it"
        ),
        DrainRestoreSupersededBy::Action(action) => tracing::info!(
            operation_id = %restore,
            superseding_action = action.as_str(),
            "drain restore canceled: a later user request supersedes it"
        ),
    }
}

/// Cancel the drain restores of `target_id` that `by` supersedes and that
/// no claim holds. Every active one is locked first, waiting for a claim or
/// a release that holds one, so none slips past both this and the release's
/// own check: a release that requeues one without seeing `by` has locked it
/// first, and this then sees it requeued.
pub(super) async fn cancel_drain_restores_superseded_tx(
    tx: &mut Transaction<'_, Postgres>,
    org_id: OrgId,
    target_id: &str,
    by: DrainRestoreSupersededBy,
) -> Result<DrainRestoreSupersession, PublicOperationStoreError> {
    let active = sqlx::query_as::<_, (Uuid, bool)>(sql!(
        "SELECT operation_id, state = $5 AND claim_token = ''
         FROM public_operations
         WHERE org_id = $1
           AND target_type = $2
           AND target_id = $3
           AND kind = $4
           AND state IN ($5, $6)
         ORDER BY requested_at, operation_id
         FOR UPDATE"
    ))
    .bind(org_id)
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(target_id)
    .bind(StoredOperationKind::DrainRestoreSandbox.as_str())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_all(&mut **tx)
    .await?;
    let unclaimed = active
        .into_iter()
        .filter_map(|(operation, unclaimed)| unclaimed.then_some(OperationId::from_uuid(operation)))
        .collect::<Vec<_>>();
    for operation in &unclaimed {
        cancel_superseded_drain_restore_tx(tx, *operation, by).await?;
    }
    Ok(DrainRestoreSupersession {
        canceled: unclaimed,
    })
}

/// [`cancel_drain_restores_superseded_tx`] in a transaction of its own, for a
/// lifecycle call that records no operation, when `action` supersedes a
/// drain restore by the rule admission applies to its operation kind
/// ([`supersedes_drain_restore`]). Any other action cancels nothing and
/// reads nothing.
pub(super) async fn cancel_drain_restores_for_action(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: SandboxId,
    action: SandboxLifecycleAction,
) -> Result<DrainRestoreSupersession, PublicOperationStoreError> {
    if !supersedes_drain_restore(StoredOperationKind::Public(action.operation_kind())) {
        return Ok(DrainRestoreSupersession::default());
    }
    let target_id = PublicSandboxId::from(sandbox_id).to_string();
    let by = DrainRestoreSupersededBy::Action(action);
    let mut tx = pool.begin().await?;
    let outcome = cancel_drain_restores_superseded_tx(&mut tx, org_id, &target_id, by).await?;
    tx.commit().await?;
    for restore in &outcome.canceled {
        record_superseded_drain_restore(*restore, by);
    }
    Ok(outcome)
}

/// The earliest user's operation on the sandbox of drain restore
/// `operation_id`, requested after it and still active, that supersedes it.
/// The caller holds the restore's row lock.
pub(super) async fn superseding_user_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
) -> Result<Option<OperationId>, PublicOperationStoreError> {
    let superseding = sqlx::query_scalar::<_, Uuid>(sql!(
        "SELECT later.operation_id
         FROM public_operations restore
         JOIN public_operations later
           ON later.org_id = restore.org_id
          AND later.target_type = restore.target_type
          AND later.target_id = restore.target_id
         WHERE restore.operation_id = $1
           AND restore.kind = $2
           AND restore.target_type = $3
           AND later.kind = ANY($4)
           AND later.state IN ($5, $6)
           AND (later.requested_at, later.operation_id) > (restore.requested_at, restore.operation_id)
         ORDER BY later.requested_at, later.operation_id
         LIMIT 1"
    ))
    .bind(operation_id.as_uuid())
    .bind(StoredOperationKind::DrainRestoreSandbox.as_str())
    .bind(OperationTargetType::Sandbox.as_str())
    .bind(superseding_kind_labels())
    .bind(OperationState::Queued.as_str())
    .bind(OperationState::Running.as_str())
    .fetch_optional(&mut **tx)
    .await?;
    Ok(superseding.map(OperationId::from_uuid))
}

/// Record drain restore `operation_id`, whose row this transaction has
/// locked, `canceled` as superseded by `by`. The record a client reads names
/// no system kind.
pub(super) async fn cancel_superseded_drain_restore_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
    by: DrainRestoreSupersededBy,
) -> Result<(), PublicOperationStoreError> {
    let code = PublicErrorCode::OperationFailed;
    let mut fields = serde_json::Map::new();
    fields.insert("operation_id".into(), operation_id.to_string().into());
    fields.insert("reason".into(), "superseded_by_user_request".into());
    match by {
        DrainRestoreSupersededBy::Operation(by) => {
            fields.insert("superseding_operation_id".into(), by.to_string().into());
        }
        DrainRestoreSupersededBy::Action(action) => {
            fields.insert("superseding_action".into(), action.as_str().into());
        }
    }
    let error = serde_json::Value::Object(fields);
    let progress_message = "Canceled: a later lifecycle request for the sandbox supersedes it.";
    sqlx::query(sql!(
        "UPDATE public_operations
         SET state = $2,
             claim_token = '',
             completed_at = NOW(),
             progress_message = $3,
             terminal_error_code = $4,
             terminal_error_json = $5,
             updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .bind(OperationState::Canceled.as_str())
    .bind(progress_message)
    .bind(code.as_str())
    .bind(Json(error.clone()))
    .execute(&mut **tx)
    .await?;
    append_operation_event_tx(
        tx,
        operation_id,
        PublicOperationEventType::Canceled,
        serde_json::json!({
            "state": OperationState::Canceled.as_str(),
            "code": code.as_str(),
            "error": error,
        }),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests;
