use crate::node_publish::plan::lifecycle::{FailedClaim, RestoreClaim, StoppedClaim};
use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId, SandboxTransitionReceiptKind, SubstrateKind},
};
use sqlx::PgConnection;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConvergeRestoredError {
    #[error("converge restored sandbox lifecycle: {0}")]
    Update(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConvergeStoppedError {
    #[error("promote stopped sandbox lifecycle: {0}")]
    Update(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConvergeFailedError {
    #[error("mark failed sandbox lifecycle: {0}")]
    Update(#[source] sqlx::Error),
}

/// Converge only the publisher's restoring rows, requiring a routable execution
/// for Kubernetes and excluding activation receipts and evacuation-owned restores.
/// The update locks matching sandbox rows without a separate lock acquisition.
/// An empty result means no row passed every guard.
pub(crate) async fn converge_restored(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[RestoreClaim],
) -> Result<Vec<SandboxId>, ConvergeRestoredError> {
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    // Publishing `started` proves the node-local runtime finished, but for
    // Kubernetes sandboxes the control plane also needs a routable active
    // execution row. The blocking wake/restore path is what hydrates that row
    // with `running` + pod IP; if it timed out first, keep the row in
    // `restoring` so the next connect can wait on and hydrate the in-flight
    // pod instead of treating it as a started sandbox with a broken execution.
    // Evacuation-marked rows are owned by the node-drain redrive (the same
    // exemption the sweeper makes) and are left alone.
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "WITH input AS (
             SELECT sandbox_id, transition_receipt_kind
             FROM UNNEST($2::UUID[], $3::TEXT[])
                 AS input(sandbox_id, transition_receipt_kind)
         )
         UPDATE sandboxes s
         SET lifecycle = 'started'
         FROM input
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1
           AND s.evacuation_restore_target IS NULL
           AND input.transition_receipt_kind IS DISTINCT FROM $4
           AND LOWER(TRIM(s.lifecycle)) = 'restoring'
           AND (
               s.managed_by <> $5
               OR EXISTS (
                   SELECT 1
                     FROM sandbox_k8s_executions rex
                    WHERE rex.sandbox_id = s.id
                      AND rex.active
                      AND rex.state = 'running'
                      AND rex.pod_ip IS NOT NULL
               )
           )
         RETURNING s.id"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.transition_receipt_kind.map(|kind| kind.as_str()))
            .collect::<Vec<_>>(),
    )
    .bind(SandboxTransitionReceiptKind::ActivationCompleted.as_str())
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(&mut *conn)
    .await
    .map_err(ConvergeRestoredError::Update)
}

/// Converge the publisher's stopped claims only from the permitted source
/// lifecycles. The update locks matching sandbox rows without a separate lock
/// acquisition; an empty result means no row passed the owner and state guards.
pub(crate) async fn converge_stopped(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[StoppedClaim],
) -> Result<Vec<SandboxId>, ConvergeStoppedError> {
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    sqlx::query_scalar::<_, SandboxId>(sql!(
        "WITH input AS (
             SELECT sandbox_id
             FROM UNNEST($2::UUID[]) AS input(sandbox_id)
         )
         UPDATE sandboxes s
         SET lifecycle = 'stopped'
         FROM input
         WHERE s.id = input.sandbox_id
           AND s.active_node_id = $1
           AND LOWER(TRIM(s.lifecycle)) IN (
               'started',
               'paused',
               'suspended',
               'stopping',
               'restoring'
           )
         RETURNING s.id"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .fetch_all(&mut *conn)
    .await
    .map_err(ConvergeStoppedError::Update)
}

/// Moves the reporting node's `k8s` rows that it published `failed` to
/// Postgres `public.sandboxes.lifecycle = 'failed'`, returning each moved row
/// with the lifecycle it left. An empty result means no claim passed the guards.
///
/// The claim locks before it checks, as the suspended projection does. A row a
/// concurrent writer holds is read again at its committed version once the lock
/// is granted, so a `deleting` or `quarantined` committed meanwhile, or a row
/// that left this node, is refused rather than overwritten, and the returned
/// from-state is the lifecycle the update actually replaced. `FOR NO KEY UPDATE`
/// is the mode the `UPDATE` itself takes on the same rows, so taking it in the
/// claim moves the lock earlier in the statement without adding a conflict.
pub(crate) async fn converge_failed(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[FailedClaim],
) -> Result<Vec<(SandboxId, String)>, ConvergeFailedError> {
    if claims.is_empty() {
        return Ok(Vec::new());
    }
    let failed_sandboxes = sqlx::query_as::<_, (Uuid, String)>(sql!(
        "WITH input AS (
             SELECT sandbox_id
             FROM UNNEST($2::UUID[]) AS input(sandbox_id)
         ),
         claimed AS (
             SELECT s.id, s.lifecycle
             FROM sandboxes s
             JOIN input ON input.sandbox_id = s.id
             WHERE s.active_node_id = $1
               AND s.managed_by = $3
               AND LOWER(TRIM(s.lifecycle)) NOT IN (
                   'failed',
                   'quarantined',
                   'deleted',
                   'cleanup_required',
                   'deleting'
               )
             FOR NO KEY UPDATE OF s
         )
         UPDATE sandboxes s
         SET lifecycle = 'failed'
         FROM claimed
         WHERE s.id = claimed.id
         RETURNING s.id, claimed.lifecycle"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(SubstrateKind::Kubernetes)
    .fetch_all(&mut *conn)
    .await
    .map_err(ConvergeFailedError::Update)?;
    Ok(failed_sandboxes
        .into_iter()
        .map(|(sandbox_id, lifecycle)| (SandboxId::from_uuid(sandbox_id), lifecycle))
        .collect())
}
