use anyhow::Result;
use pplx_space_common_core::types::NodeId;
use pplx_space_db_test_fixtures::publish_load::TEMPLATE_NAME;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;

/// Read this fixture template's releases without explicit row locks. An empty
/// result distinguishes successful absence/census pruning from retained rows.
pub async fn template_inventory_hashes(pool: &PgPool, node_id: NodeId) -> Result<Vec<String>> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT content_hash FROM node_template_inventory
         WHERE node_id = $1 AND template_name = $2
         ORDER BY content_hash"
    ))
    .bind(node_id)
    .bind(TEMPLATE_NAME)
    .fetch_all(pool)
    .await?)
}
