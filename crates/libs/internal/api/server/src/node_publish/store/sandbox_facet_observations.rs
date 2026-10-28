use pplx_space_common_core::{
    sql,
    types::{NodeId, SubstrateKind},
};
use pplx_space_common_svc::types::{FACET_SANDBOX_DESIRED_STATE, SANDBOX_LOCAL_SUSPEND_KEY};
use sqlx::{PgConnection, types::Json};

use crate::node_publish::plan::recovery_observation::{
    ClearRecoveryObservationClaim, RecoveryObservationClaim,
};
use crate::node_publish::plan::suspend_handoff::SuspendHandoffClaim;

const QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS: i64 = 60 * 60;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertSuspendHandoffError {
    #[error("publish local suspend handoff observation: {0}")]
    Upsert(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReconcileRecoveryObservationError {
    #[error("write published quarantine recovery observation: {0}")]
    Upsert(#[source] sqlx::Error),
    #[error("clear published quarantine recovery observation: {0}")]
    Clear(#[source] sqlx::Error),
}

/// Preserve the node's exact local handoff before its backup is durable. The
/// publisher, lifecycle and execution-identity guards authorize only a bounded
/// wait, not a committed artifact. Writes lock facet rows, with no explicit
/// sandbox locks; false means no new or changed handoff matched those guards.
pub(crate) async fn upsert_suspend_handoff(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[SuspendHandoffClaim],
) -> Result<bool, UpsertSuspendHandoffError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
             SELECT sandbox_id, status, execution_generation, pod_uid,
                    current_local_artifact_id, current_local_artifact_kind,
                    current_local_artifact_origin
             FROM UNNEST(
                 $2::UUID[], $6::TEXT[], $7::BIGINT[], $8::TEXT[],
                 $9::UUID[], $10::TEXT[], $11::TEXT[]
             ) AS input(
                 sandbox_id, status, execution_generation, pod_uid,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin
             )
         ), observation AS (
             SELECT input.sandbox_id,
                    CASE WHEN LOWER(TRIM(input.status)) = 'suspended'
                               AND input.current_local_artifact_id IS NOT NULL
                               AND input.current_local_artifact_kind = 'full'
                               AND input.current_local_artifact_origin = 'suspend'
                         THEN jsonb_build_object($4::TEXT, jsonb_build_object(
                             'execution_generation', input.execution_generation,
                             'pod_uid', input.pod_uid,
                             'local_artifact_id', input.current_local_artifact_id
                         ))
                         ELSE '{}'::JSONB END AS payload
               FROM input
               JOIN sandboxes s ON s.id = input.sandbox_id
               JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
              WHERE s.active_node_id = $1
                AND s.managed_by = $5
                AND s.lifecycle = 'started'
                AND e.active AND e.state IN ('running', 'recovering')
                AND e.generation = input.execution_generation
                AND e.pod_uid = input.pod_uid
                AND input.status IS NOT NULL
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT $1, $3, observation.sandbox_id::TEXT, observation.payload, NOW(), NOW()
           FROM observation
          WHERE observation.payload ? $4::TEXT
             OR EXISTS (
                 SELECT 1 FROM node_facet_observed_status prior
                  WHERE prior.node_id = $1 AND prior.facet = $3
                    AND prior.element = observation.sandbox_id::TEXT
                    AND prior.observed_payload ? $4::TEXT
             )
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload = (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                                    - $4::TEXT) || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> $4::TEXT
               IS DISTINCT FROM EXCLUDED.observed_payload -> $4::TEXT"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_LOCAL_SUSPEND_KEY)
    .bind(SubstrateKind::Kubernetes)
    .bind(
        claims
            .iter()
            .map(|claim| claim.status.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.execution_generation.get())
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.pod_uid.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.id().as_uuid())
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.kind().as_str())
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| {
                claim
                    .artifact
                    .as_ref()
                    .map(|artifact| artifact.origin().as_str())
            })
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(UpsertSuspendHandoffError::Upsert)?;
    Ok(result.rows_affected() != 0)
}

/// Write or clear recovery evidence only for non-degraded reports. Preserve the
/// database-clock heartbeat and unrelated payload keys. Upserts precede clears
/// on the same connection and lock facet rows; false is a normal unchanged result.
pub(crate) async fn reconcile_quarantine_recovery(
    conn: &mut PgConnection,
    node_id: NodeId,
    observations: &[RecoveryObservationClaim],
    clears: &[ClearRecoveryObservationClaim],
) -> Result<bool, ReconcileRecoveryObservationError> {
    let written = upsert_quarantine_recovery(conn, node_id, observations).await?;
    let cleared = clear_quarantine_recovery(conn, node_id, clears).await?;
    Ok(written || cleared)
}

/// Update only supplied evidence, under the facet-row write lock. Identical
/// evidence remains unchanged until its heartbeat; false means no row changed.
async fn upsert_quarantine_recovery(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[RecoveryObservationClaim],
) -> Result<bool, ReconcileRecoveryObservationError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let written = sqlx::query(sql!(
        "WITH publishable AS (
             SELECT sandbox_id, quarantine_recovery_payload
             FROM UNNEST($2::UUID[], $3::JSONB[])
                 AS input(sandbox_id, quarantine_recovery_payload)
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $4,
             publishable.sandbox_id::TEXT,
             jsonb_build_object(
                 'quarantine_recovery',
                 publishable.quarantine_recovery_payload
             ),
             NOW(),
             NOW()
         FROM publishable
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                 || EXCLUDED.observed_payload,
             observed_at = EXCLUDED.observed_at,
             updated_at = NOW()
         WHERE node_facet_observed_status.observed_payload -> 'quarantine_recovery'
               IS DISTINCT FROM EXCLUDED.observed_payload -> 'quarantine_recovery'
            OR node_facet_observed_status.observed_at
               < NOW() - make_interval(secs => $5::BIGINT)"
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
            .map(|claim| Json(claim.payload()))
            .collect::<Vec<_>>(),
    )
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(QUARANTINE_RECOVERY_OBSERVATION_HEARTBEAT_SECS)
    .execute(&mut *conn)
    .await
    .map_err(ReconcileRecoveryObservationError::Upsert)?;
    Ok(written.rows_affected() != 0)
}

/// Clear only explicitly omitted evidence, locking matching facet rows. Missing
/// keys are a no-op, and a degraded report cannot enter this claim slice.
async fn clear_quarantine_recovery(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[ClearRecoveryObservationClaim],
) -> Result<bool, ReconcileRecoveryObservationError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let cleared = sqlx::query(sql!(
        "WITH clearable AS (
             SELECT sandbox_id
             FROM UNNEST($2::UUID[]) AS input(sandbox_id)
         )
         UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - 'quarantine_recovery',
             observed_at = NOW(),
             updated_at = NOW()
         FROM clearable
         WHERE observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = clearable.sandbox_id::TEXT
           AND observed.observed_payload ? 'quarantine_recovery'"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .execute(&mut *conn)
    .await
    .map_err(ReconcileRecoveryObservationError::Clear)?;
    Ok(cleared.rows_affected() != 0)
}
