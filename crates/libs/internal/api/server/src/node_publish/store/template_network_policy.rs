use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::FACET_NETWORK_POLICY;
use sqlx::{PgConnection, types::Json};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ObserveTemplatePolicyError {
    #[error("upsert template network policy facet status: {0}")]
    Upsert(#[source] sqlx::Error),
    #[error("clear template network policy apply condition: {0}")]
    ClearCondition(#[source] sqlx::Error),
}

/// Observe policies only for this node's existing inventory and reports with
/// a generation, then clear their apply failures. Ordinary facet-row write
/// locks follow that order; conflict checks also lock unchanged observations.
/// No matching inventory or no reported generation is a normal zero-row result.
pub(crate) async fn observe(
    conn: &mut PgConnection,
    node_id: NodeId,
    rows_json: &Json<serde_json::Value>,
) -> Result<(), ObserveTemplatePolicyError> {
    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_json JSONB,
                 network_policy_generation BIGINT,
                 network_policy_authority TEXT
             )
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
         WHERE network_policy_generation IS NOT NULL
           AND EXISTS (
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
    .bind(rows_json.clone())
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(ObserveTemplatePolicyError::Upsert)?;

    sqlx::query(sql!(
        "WITH input AS (
             SELECT *
             FROM jsonb_to_recordset($2::jsonb) AS input(
                 template_name TEXT,
                 network_policy_generation BIGINT
             )
         )
         DELETE FROM node_facet_conditions nfc
         USING input
         WHERE nfc.node_id = $1
           AND nfc.facet = $3
           AND nfc.element = 'template:' || input.template_name
           AND input.network_policy_generation IS NOT NULL
           AND nfc.condition_type = 'ApplyFailed'
           AND EXISTS (
               SELECT 1 FROM node_template_inventory inventory
               WHERE inventory.node_id = $1 AND inventory.template_name = input.template_name
           )"
    ))
    .bind(node_id)
    .bind(rows_json.clone())
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(ObserveTemplatePolicyError::ClearCondition)?;
    Ok(())
}
