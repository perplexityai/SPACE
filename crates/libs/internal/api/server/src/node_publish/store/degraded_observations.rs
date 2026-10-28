use pplx_space_common_core::{
    sql,
    types::{NodeId, SandboxId},
};
use sqlx::{PgConnection, types::Json};

use crate::node_publish::validate::sandbox_inventory::DegradedSandboxClaim;

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertDegradedObservationsError {
    #[error("write degraded published sandbox observed state: {0}")]
    Write(#[source] sqlx::Error),
}

/// Write presence and optional status without erasing rich observed fields.
/// Ownership is checked against the current route/assignment; only the observed
/// rows are write-locked. Zero accepted rows is a normal guarded outcome.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[DegradedSandboxClaim],
) -> Result<u64, UpsertDegradedObservationsError> {
    if claims.is_empty() {
        return Ok(0);
    }
    let accepted = sqlx::query_scalar::<_, SandboxId>(sql!(
        "WITH input AS (
                 SELECT sandbox_id, status
                 FROM jsonb_to_recordset($2::jsonb) AS input(
                     sandbox_id UUID,
                     status TEXT
                 )
             ),
             publishable AS (
                 SELECT input.sandbox_id, input.status
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 LEFT JOIN sandbox_direct_route_identities direct_route
                   ON direct_route.sandbox_id = s.id
                 WHERE s.id IS NULL
                    OR COALESCE(direct_route.current_node_id, s.active_node_id) = $1
             )
             INSERT INTO sandbox_observed_states (
                 sandbox_id, node_id, status, snapshots_enabled,
                 missing_publish_count, observed_at, updated_at
             )
             SELECT sandbox_id, $1, status, FALSE, 0, NOW(), NOW()
             FROM publishable
             ON CONFLICT (sandbox_id) DO UPDATE
             SET node_id = EXCLUDED.node_id,
                 status = COALESCE(EXCLUDED.status, sandbox_observed_states.status),
                 missing_publish_count = 0,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             RETURNING sandbox_id"
    ))
    .bind(node_id)
    .bind(Json(serde_json::Value::Array(
        claims
            .iter()
            .map(DegradedSandboxClaim::legacy_row)
            .collect(),
    )))
    .fetch_all(&mut *conn)
    .await
    .map_err(UpsertDegradedObservationsError::Write)?;
    Ok(u64::try_from(accepted.len()).unwrap_or(u64::MAX))
}
