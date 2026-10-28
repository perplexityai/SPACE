use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
    SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
    SANDBOX_FAILURE_POD_UID_KEY,
};
use sqlx::PgConnection;

use crate::node_publish::plan::failure_episode::{
    ClearFailureEpisodeClaim, FailureEpisodeObservation,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReconcileFailureEpisodesError {
    #[error("write sandbox failure episode: {0}")]
    Write(#[source] sqlx::Error),
    #[error("clear sandbox failure episode: {0}")]
    Clear(#[source] sqlx::Error),
}

/// Reconcile optional failure identity evidence under the current node-owner
/// guard. Writes acquire facet-row locks in statement order, without explicit
/// sandbox locks. Returns false when neither guarded statement changes a row.
pub(crate) async fn reconcile(
    conn: &mut PgConnection,
    node_id: NodeId,
    observations: &[FailureEpisodeObservation],
    clears: &[ClearFailureEpisodeClaim],
) -> Result<bool, ReconcileFailureEpisodesError> {
    let written = upsert(conn, node_id, observations).await?;
    let cleared = clear(conn, node_id, clears).await?;
    Ok(written || cleared)
}

/// Persist supplied failure identity only for the current owner, locking matching
/// facet rows. Empty input or unchanged evidence is a normal no-write outcome.
async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[FailureEpisodeObservation],
) -> Result<bool, ReconcileFailureEpisodesError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let written = sqlx::query(sql!(
        "INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_payload, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             input.sandbox_id::TEXT,
             jsonb_build_object(
                 $4::TEXT,
                 jsonb_strip_nulls(jsonb_build_object(
                     $5::TEXT, input.execution_generation,
                     $6::TEXT, input.pod_uid,
                     $7::TEXT, input.activation_attempt_id
                 ))
             ),
             NOW(),
             NOW()
         FROM UNNEST($2::UUID[], $8::BIGINT[], $9::TEXT[], $10::UUID[])
             AS input(sandbox_id, execution_generation, pod_uid, activation_attempt_id)
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
         ON CONFLICT (node_id, facet, element) DO UPDATE
         SET observed_payload =
                 (COALESCE(node_facet_observed_status.observed_payload, '{}'::JSONB)
                     - $4::TEXT)
                 || EXCLUDED.observed_payload,
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
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .bind(
        claims
            .iter()
            .map(|claim| claim.execution_generation.get())
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.pod_uid.as_ref().map(|pod| pod.as_str()))
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.activation_attempt_id.map(|attempt| attempt.as_uuid()))
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(ReconcileFailureEpisodesError::Write)?;
    Ok(written.rows_affected() != 0)
}

/// Omitted failure evidence can clear only the current owner's facet row.
/// Degraded failed reports are excluded; false means no stored episode matched.
async fn clear(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[ClearFailureEpisodeClaim],
) -> Result<bool, ReconcileFailureEpisodesError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let cleared = sqlx::query(sql!(
        "UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - $4::TEXT,
             observed_at = NOW(),
             updated_at = NOW()
         FROM UNNEST($2::UUID[]) AS input(sandbox_id)
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = input.sandbox_id::TEXT
           AND observed.observed_payload ? $4::TEXT"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .execute(&mut *conn)
    .await
    .map_err(ReconcileFailureEpisodesError::Clear)?;
    Ok(cleared.rows_affected() != 0)
}
