//! A user's lifecycle request supersedes a drain restore of the same sandbox
//! that hasn't started: the user said what the sandbox should be, and the
//! drain's restore would undo it. Three places cancel one: the admission of a
//! user's operation ([`super::create_operation_tx`]), a lifecycle call that
//! records no operation (`SandboxController::action`), and the release of a
//! drain restore's attempt when a user's operation has queued behind it.
//!
//! None cancels a drain restore whose restore episode is open (the row is
//! `restoring` toward its target): nothing but a drain restore settles that
//! episode, since the transient sweeper leaves it alone. Such a restore is
//! made due at once instead, so it settles the episode and the user's
//! operation, queued behind it, runs next.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{OrgId, SandboxId};
use pplx_space_public_api::{
    OperationId, OperationKind, OperationState, OperationTargetType, PublicErrorCode,
    PublicOperationEventType, PublicSandboxId,
};
use pplx_space_sandbox_store::drain_restore::drain_restore_episode_open;
use sqlx::types::Json;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{PublicOperationStoreError, StoredOperationKind, append_operation_event_tx};

/// Whether a user's operation of `kind` on a sandbox supersedes a drain
/// restore of it: whether running it changes the sandbox's lifecycle or
/// deletes it.
pub fn supersedes_drain_restore(kind: StoredOperationKind) -> bool {
    let kind = match kind {
        StoredOperationKind::DrainRestoreSandbox => return false,
        StoredOperationKind::Public(kind) => kind,
    };
    match kind {
        // A create names a sandbox that doesn't exist yet, so it never meets
        // a drain restore; it sets the lifecycle like the rest.
        OperationKind::CreateSandbox
        | OperationKind::DeleteSandbox
        | OperationKind::StartSandbox
        | OperationKind::StopSandbox
        | OperationKind::PauseSandbox
        | OperationKind::ResumeSandbox
        | OperationKind::SuspendSandbox
        | OperationKind::RestoreSandbox
        | OperationKind::WakeSandbox
        | OperationKind::KillSandbox
        | OperationKind::RollbackSandbox => true,
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
    }
}

/// The labels of every public kind that supersedes a drain restore, as SQL
/// binds them.
fn superseding_kind_labels() -> Vec<&'static str> {
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
    Action(&'static str),
}

/// What a supersession did to the drain restores it found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DrainRestoreSupersession {
    pub canceled: usize,
    /// Restores left to settle an open episode, made due at once.
    pub hastened: usize,
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
    let mut outcome = DrainRestoreSupersession::default();
    if unclaimed.is_empty() {
        return Ok(outcome);
    }
    if episode_open(tx, target_id).await? {
        for operation in unclaimed {
            hasten_drain_restore_tx(tx, operation).await?;
            outcome.hastened += 1;
        }
        return Ok(outcome);
    }
    for operation in unclaimed {
        cancel_superseded_drain_restore_tx(tx, operation, by).await?;
        outcome.canceled += 1;
    }
    Ok(outcome)
}

/// [`cancel_drain_restores_superseded_tx`] in a transaction of its own, for a
/// lifecycle call that records no operation.
pub(super) async fn cancel_drain_restores_for_action(
    pool: &PgPool,
    org_id: OrgId,
    sandbox_id: SandboxId,
    action: &'static str,
) -> Result<DrainRestoreSupersession, PublicOperationStoreError> {
    let target_id = PublicSandboxId::from(sandbox_id).to_string();
    let mut tx = pool.begin().await?;
    let outcome = cancel_drain_restores_superseded_tx(
        &mut tx,
        org_id,
        &target_id,
        DrainRestoreSupersededBy::Action(action),
    )
    .await?;
    tx.commit().await?;
    Ok(outcome)
}

/// The earliest user's operation on the sandbox of drain restore
/// `operation_id`, requested after it and still active, that supersedes it,
/// unless its restore episode is open. The caller holds the restore's row
/// lock.
pub(super) async fn superseding_user_operation_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
) -> Result<Option<OperationId>, PublicOperationStoreError> {
    let superseding = sqlx::query_as::<_, (Uuid, String)>(sql!(
        "SELECT later.operation_id, restore.target_id
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
    let Some((later, target_id)) = superseding else {
        return Ok(None);
    };
    if episode_open(tx, &target_id).await? {
        return Ok(None);
    }
    Ok(Some(OperationId::from_uuid(later)))
}

/// Whether the sandbox `target_id` names is mid-episode of a drain restore.
/// A target that names no sandbox has none.
async fn episode_open(
    tx: &mut Transaction<'_, Postgres>,
    target_id: &str,
) -> Result<bool, PublicOperationStoreError> {
    let Ok(sandbox_id) = target_id.parse::<PublicSandboxId>() else {
        return Ok(false);
    };
    Ok(drain_restore_episode_open(&mut **tx, sandbox_id.into()).await?)
}

/// Make queued drain restore `operation_id` due now.
async fn hasten_drain_restore_tx(
    tx: &mut Transaction<'_, Postgres>,
    operation_id: OperationId,
) -> Result<(), PublicOperationStoreError> {
    sqlx::query(sql!(
        "UPDATE public_operations
         SET next_attempt_at = LEAST(next_attempt_at, NOW()),
             updated_at = NOW()
         WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .execute(&mut **tx)
    .await?;
    Ok(())
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
            fields.insert("superseding_action".into(), action.into());
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
