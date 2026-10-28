use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::FACET_NETWORK_POLICY;
use pplx_space_net_common::proto::NetworkPolicySource;
use sqlx::{PgConnection, types::Json};

use crate::node_publish::plan::network_policy::{
    ClearPolicyFailureClaim, NetworkPolicyObservation, PolicyAuthorityClaim,
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpdateAuthorityError {
    #[error("update sandbox observed network policy authority: {0}")]
    Update(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpsertPolicyError {
    #[error("upsert network policy facet status: {0}")]
    Upsert(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ClearApplyFailureError {
    #[error("clear network policy apply condition: {0}")]
    Delete(#[source] sqlx::Error),
}

/// Update authority for this node's observations when a generation is supplied
/// and the sandbox is unknown or assigned here. The update locks matching
/// observation rows; false means no authority changed.
pub(crate) async fn update_authority(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[PolicyAuthorityClaim],
) -> Result<bool, UpdateAuthorityError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(
        sql!("WITH input AS (
                 SELECT sandbox_id, observed_network_policy_authority
                 FROM UNNEST($2::UUID[], $3::TEXT[])
                     AS input(sandbox_id, observed_network_policy_authority)
             ),
             publishable AS (
                 SELECT input.sandbox_id, input.observed_network_policy_authority
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE (s.id IS NULL OR s.active_node_id = $1)
             )
             UPDATE sandbox_observed_states sos
             SET network_policy_authority = publishable.observed_network_policy_authority
             FROM publishable
             WHERE sos.sandbox_id = publishable.sandbox_id
               AND sos.node_id = $1
               AND sos.network_policy_authority IS DISTINCT FROM publishable.observed_network_policy_authority"),
    )
    .bind(node_id)
    .bind(claims.iter().map(|claim| claim.sandbox_id).collect::<Vec<_>>())
    .bind(claims.iter().map(|claim| claim.authority.map(|authority| authority.as_str())).collect::<Vec<_>>())
    .execute(&mut *conn)
    .await
    .map_err(UpdateAuthorityError::Update)?;
    Ok(result.rows_affected() > 0)
}

/// Upsert facets for generated policy observations whose sandboxes are unknown
/// or assigned here. Conflict handling locks facet rows even for no-op updates;
/// false means no facet was inserted or changed.
pub(crate) async fn upsert(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[NetworkPolicyObservation],
) -> Result<bool, UpsertPolicyError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
                 SELECT sandbox_id, observed_network_policy_json,
                        observed_network_policy_generation,
                        observed_network_policy_source, observed_network_policy_authority
                 FROM UNNEST($2::UUID[], $4::JSONB[], $5::BIGINT[], $6::TEXT[], $7::TEXT[])
                     AS input(sandbox_id, observed_network_policy_json,
                              observed_network_policy_generation,
                              observed_network_policy_source, observed_network_policy_authority)
             ),
             publishable AS (
                 SELECT input.sandbox_id, input.observed_network_policy_json,
                        input.observed_network_policy_generation,
                        input.observed_network_policy_source,
                        input.observed_network_policy_authority
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE s.id IS NULL
                    OR s.active_node_id = $1
             )
             INSERT INTO node_facet_observed_status (
                 node_id, facet, element, observed_generation, observed_payload,
                 applied_source, applied_authority, observed_at, updated_at
             )
             SELECT
                 $1,
                 $3,
                 'sandbox:' || sandbox_id::text,
                 observed_network_policy_generation,
                 observed_network_policy_json,
                 observed_network_policy_source,
                 observed_network_policy_authority,
                 NOW(),
                 NOW()
             FROM publishable
             ON CONFLICT (node_id, facet, element) DO UPDATE
             SET observed_generation = EXCLUDED.observed_generation,
                 observed_payload = EXCLUDED.observed_payload,
                 applied_source = EXCLUDED.applied_source,
                 applied_authority = EXCLUDED.applied_authority,
                 observed_at = EXCLUDED.observed_at,
                 updated_at = NOW()
             WHERE (
                 node_facet_observed_status.observed_generation,
                 node_facet_observed_status.observed_payload,
                 node_facet_observed_status.applied_source,
                 node_facet_observed_status.applied_authority
             ) IS DISTINCT FROM (
                 EXCLUDED.observed_generation,
                 EXCLUDED.observed_payload,
                 EXCLUDED.applied_source,
                 EXCLUDED.applied_authority
             )"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(FACET_NETWORK_POLICY)
    .bind(
        claims
            .iter()
            .map(|claim| claim.policy.as_ref().map(Json))
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.generation)
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| match claim.source {
                NetworkPolicySource::Template => Some("template"),
                NetworkPolicySource::Override => Some("override"),
                NetworkPolicySource::Unspecified => None,
            })
            .collect::<Vec<_>>(),
    )
    .bind(
        claims
            .iter()
            .map(|claim| claim.authority.map(|authority| authority.as_str()))
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(UpsertPolicyError::Upsert)?;
    Ok(result.rows_affected() > 0)
}

/// Clear ApplyFailed conditions for generated policy observations whose
/// sandboxes are unknown or assigned here. The delete locks matching condition
/// rows; false means there was no eligible condition to remove.
pub(crate) async fn clear_apply_failure(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[ClearPolicyFailureClaim],
) -> Result<bool, ClearApplyFailureError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
                 SELECT sandbox_id
                 FROM UNNEST($2::UUID[]) AS input(sandbox_id)
             ),
             publishable AS (
                 SELECT input.sandbox_id
                 FROM input
                 LEFT JOIN sandboxes s ON s.id = input.sandbox_id
                 WHERE (s.id IS NULL OR s.active_node_id = $1)
             )
             DELETE FROM node_facet_conditions nfc
             USING publishable
             WHERE nfc.node_id = $1
               AND nfc.facet = $3
               AND nfc.element = 'sandbox:' || publishable.sandbox_id::text
               AND nfc.condition_type = 'ApplyFailed'"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.sandbox_id)
            .collect::<Vec<_>>(),
    )
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(ClearApplyFailureError::Delete)?;
    Ok(result.rows_affected() > 0)
}
