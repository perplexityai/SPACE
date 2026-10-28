use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::{
    FACET_SANDBOX_DESIRED_STATE, SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY,
    SANDBOX_FAILURE_EPISODE_KEY, SANDBOX_FAILURE_EXECUTION_GENERATION_KEY,
    SANDBOX_FAILURE_POD_UID_KEY,
};
use sqlx::{PgConnection, types::Json};

use crate::node_publish::validate::failure_episode::FailureEpisodeClaim;

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
    claims: &[FailureEpisodeClaim],
) -> Result<bool, ReconcileFailureEpisodesError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let rows = Json(serde_json::Value::Array(
        claims.iter().map(FailureEpisodeClaim::legacy_row).collect(),
    ));
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
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             status TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND LOWER(TRIM(input.status)) = 'failed'
           AND NOT input.state_degraded
           AND input.execution_generation IS NOT NULL
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
    .bind(&rows)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .bind(SANDBOX_FAILURE_EXECUTION_GENERATION_KEY)
    .bind(SANDBOX_FAILURE_POD_UID_KEY)
    .bind(SANDBOX_FAILURE_ACTIVATION_ATTEMPT_ID_KEY)
    .execute(&mut *conn)
    .await
    .map_err(ReconcileFailureEpisodesError::Write)?;

    let cleared = sqlx::query(sql!(
        "UPDATE node_facet_observed_status observed
         SET observed_payload = COALESCE(observed.observed_payload, '{}'::JSONB)
                                    - $4::TEXT,
             observed_at = NOW(),
             updated_at = NOW()
         FROM jsonb_to_recordset($2::JSONB) AS input(
             sandbox_id UUID,
             status TEXT,
             state_degraded BOOLEAN,
             execution_generation BIGINT,
             pod_uid TEXT,
             activation_attempt_id TEXT
         )
         JOIN sandboxes s
           ON s.id = input.sandbox_id
         LEFT JOIN sandbox_direct_route_identities direct_route
           ON direct_route.sandbox_id = s.id
         WHERE COALESCE(direct_route.current_node_id, s.active_node_id) = $1
           AND observed.node_id = $1
           AND observed.facet = $3
           AND observed.element = input.sandbox_id::TEXT
           AND (
               LOWER(TRIM(input.status)) IS DISTINCT FROM 'failed'
               OR (
                   NOT input.state_degraded
                   AND input.execution_generation IS NULL
               )
           )
           AND observed.observed_payload ? $4::TEXT"
    ))
    .bind(node_id)
    .bind(rows)
    .bind(FACET_SANDBOX_DESIRED_STATE)
    .bind(SANDBOX_FAILURE_EPISODE_KEY)
    .execute(&mut *conn)
    .await
    .map_err(ReconcileFailureEpisodesError::Clear)?;
    Ok(written.rows_affected() != 0 || cleared.rows_affected() != 0)
}
