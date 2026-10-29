use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::FACET_NETWORK_POLICY;
use sqlx::{PgConnection, types::Json};

use crate::node_publish::plan::templates::{TemplatePolicyAppliedClaim, TemplatePolicyClaim};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ObserveTemplatePolicyError {
    #[error("upsert template network policy facet status: {0}")]
    Upsert(#[source] sqlx::Error),
    #[error("clear template network policy apply condition: {0}")]
    ClearCondition(#[source] sqlx::Error),
}

/// Observe generated policy claims only for this node's existing inventory.
/// Conflict checks lock facet rows even for unchanged observations. False means
/// there were no claims, no matching inventory, or no changed observations.
pub(crate) async fn observe(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[TemplatePolicyClaim],
) -> Result<bool, ObserveTemplatePolicyError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
             SELECT template_name, network_policy_json,
                    network_policy_generation, network_policy_authority
             FROM UNNEST($2::TEXT[], $4::JSONB[], $5::BIGINT[], $6::TEXT[])
                 AS input(template_name, network_policy_json,
                          network_policy_generation, network_policy_authority)
         )
         INSERT INTO node_facet_observed_status (
             node_id, facet, element, observed_generation, observed_payload,
             applied_source, applied_authority, observed_at, updated_at
         )
         SELECT
             $1,
             $3,
             'template:' || template_name,
             network_policy_generation,
             network_policy_json,
             NULL,
             network_policy_authority,
             NOW(),
             NOW()
         FROM input
         WHERE EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )
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
            .map(|claim| claim.template.as_str())
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
            .map(|claim| claim.authority.map(|authority| authority.as_str()))
            .collect::<Vec<_>>(),
    )
    .execute(&mut *conn)
    .await
    .map_err(ObserveTemplatePolicyError::Upsert)?;
    Ok(result.rows_affected() > 0)
}

/// Clear apply failures after observing a generated policy claim, guarded by
/// this node's inventory. Deletes lock matching condition rows. False means
/// no claims, no matching inventory, or no failure condition to clear.
pub(crate) async fn clear_apply_failures(
    conn: &mut PgConnection,
    node_id: NodeId,
    claims: &[TemplatePolicyAppliedClaim],
) -> Result<bool, ObserveTemplatePolicyError> {
    if claims.is_empty() {
        return Ok(false);
    }
    let result = sqlx::query(sql!(
        "WITH input AS (
             SELECT template_name
             FROM UNNEST($2::TEXT[]) AS input(template_name)
         )
         DELETE FROM node_facet_conditions nfc
         USING input
         WHERE nfc.node_id = $1
           AND nfc.facet = $3
           AND nfc.element = 'template:' || input.template_name
           AND nfc.condition_type = 'ApplyFailed'
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )"
    ))
    .bind(node_id)
    .bind(
        claims
            .iter()
            .map(|claim| claim.template.as_str())
            .collect::<Vec<_>>(),
    )
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(ObserveTemplatePolicyError::ClearCondition)?;
    Ok(result.rows_affected() > 0)
}
