use pplx_space_common_core::{sql, types::NodeId};
use pplx_space_common_svc::types::FACET_NETWORK_POLICY;
use sqlx::PgConnection;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RemoveAbsentTemplatesError {
    #[error("delete absent published template: {0}")]
    Inventory(#[source] sqlx::Error),
    #[error("delete absent template network policy facet status: {0}")]
    Policy(#[source] sqlx::Error),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PruneTemplateCensusError {
    #[error("prune absent published templates: {0}")]
    Prune(#[source] sqlx::Error),
}

/// Remove explicit absence claims only from this node's inventory, then remove
/// their policy observations. Ordinary delete locks follow that statement order
/// on the caller's connection. Empty claims do nothing; zero inventory rows
/// still allows clearing stale policy observations for the named templates.
pub(crate) async fn remove_absent(
    conn: &mut PgConnection,
    node_id: NodeId,
    absent_templates: &[String],
) -> Result<u64, RemoveAbsentTemplatesError> {
    if absent_templates.is_empty() {
        return Ok(0);
    }

    let result = sqlx::query(sql!(
        "DELETE FROM node_template_inventory
             WHERE node_id = $1 AND template_name = ANY($2)"
    ))
    .bind(node_id)
    .bind(absent_templates)
    .execute(&mut *conn)
    .await
    .map_err(RemoveAbsentTemplatesError::Inventory)?;
    let elements = absent_templates
        .iter()
        .map(|template| format!("template:{template}"))
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "DELETE FROM node_facet_observed_status
             WHERE node_id = $1
               AND facet = $2
               AND element = ANY($3)"
    ))
    .bind(node_id)
    .bind(FACET_NETWORK_POLICY)
    .bind(&elements)
    .execute(&mut *conn)
    .await
    .map_err(RemoveAbsentTemplatesError::Policy)?;
    Ok(result.rows_affected())
}

/// Prune releases omitted from this node's complete census and the policy
/// observations named by removed rows. This takes ordinary delete locks, not
/// explicit template locks. Empty present keys mean a complete-empty census,
/// so all node inventory is removed; zero matching rows is an ordinary no-op.
pub(crate) async fn prune_census(
    conn: &mut PgConnection,
    node_id: NodeId,
    present_keys: &[(String, String)],
) -> Result<(), PruneTemplateCensusError> {
    let templates = present_keys
        .iter()
        .map(|(template, _)| template.clone())
        .collect::<Vec<_>>();
    let content_hashes = present_keys
        .iter()
        .map(|(_, content_hash)| content_hash.clone())
        .collect::<Vec<_>>();
    sqlx::query(sql!(
        "WITH present AS (
             SELECT *
             FROM UNNEST($2::TEXT[], $3::TEXT[]) AS present(template_name, content_hash)
         ),
         removed AS (
             DELETE FROM node_template_inventory nti
             WHERE nti.node_id = $1
               AND NOT EXISTS (
                   SELECT 1
                   FROM present
                   WHERE present.template_name = nti.template_name
                     AND present.content_hash = nti.content_hash
               )
             RETURNING nti.template_name
         ),
         removed_elements AS (
             SELECT DISTINCT 'template:' || template_name AS element
             FROM removed
         )
         DELETE FROM node_facet_observed_status nfos
         USING removed_elements
         WHERE nfos.node_id = $1
           AND nfos.facet = $4
           AND nfos.element = removed_elements.element"
    ))
    .bind(node_id)
    .bind(templates)
    .bind(content_hashes)
    .bind(FACET_NETWORK_POLICY)
    .execute(&mut *conn)
    .await
    .map_err(PruneTemplateCensusError::Prune)?;
    Ok(())
}
