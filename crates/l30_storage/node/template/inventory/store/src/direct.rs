//! Inventory reads that also ask a Direct question: whether a node's
//! `node_direct` registry row, which `pplx-space-direct-store` owns, is live.
//! They join it and `nodes` (`pplx-space-node-registration-store`'s) and
//! write nothing.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_postgres_pool::{DbError, Executor};
use pplx_space_sql_locator::sql;
use pplx_space_template_model::TemplateName;

/// The nodes whose inventory lists `template`, each once: those of every
/// substrate other than `direct`, and `direct`'s whose `node_direct` row is
/// live.
pub async fn template_holder_ids(
    executor: impl Executor<'_>,
    template: &TemplateName,
    direct: SubstrateKind,
) -> Result<Vec<NodeId>, DbError> {
    Ok(sqlx::query_scalar::<_, NodeId>(sql!(
        "SELECT DISTINCT nti.node_id
         FROM node_template_inventory nti
         JOIN nodes n ON n.id = nti.node_id
         LEFT JOIN node_direct nd ON nd.node_id = n.id
         WHERE nti.template_name = $1
           AND (
             n.managed_by != $2
             OR (nd.node_id IS NOT NULL AND nd.deleted_at IS NULL)
           )"
    ))
    .bind(template.as_str())
    .bind(direct)
    .fetch_all(executor.into_sqlx())
    .await?)
}

#[cfg(test)]
mod tests;
