use pplx_space_common_core::types::NodeId;

use pplx_space_db_test_fixtures::pg_test_db;

use super::*;

use pplx_space_test_env::TestResult;

#[tokio::test]
async fn template_delete_inventory_query_uses_direct_node_deleted_state() -> TestResult {
    let db = pg_test_db().await?;
    let template = TemplateName::parse("app-runtime")?;
    insert_template(&db.pool, &template).await?;

    let active_direct_node = NodeId::new();
    let deleted_direct_node = NodeId::new();
    let k8s_node = NodeId::new();
    let retired_k8s_node = NodeId::new();

    insert_node(&db.pool, active_direct_node, "worker", "space", 10).await?;
    insert_direct_node(&db.pool, active_direct_node, false).await?;
    insert_inventory(&db.pool, active_direct_node, &template).await?;

    insert_node(&db.pool, deleted_direct_node, "worker", "space", 11).await?;
    insert_direct_node(&db.pool, deleted_direct_node, true).await?;
    insert_inventory(&db.pool, deleted_direct_node, &template).await?;

    insert_node(&db.pool, k8s_node, "worker", "k8s", 12).await?;
    insert_inventory(&db.pool, k8s_node, &template).await?;

    // A retired registry row excludes only Direct nodes.
    insert_node(&db.pool, retired_k8s_node, "worker", "k8s", 13).await?;
    insert_direct_node(&db.pool, retired_k8s_node, true).await?;
    insert_inventory(&db.pool, retired_k8s_node, &template).await?;

    let mut nodes = nodes_with_template_inventory(&db.pool, &template).await?;
    nodes.sort_unstable();
    let mut expected = vec![active_direct_node, k8s_node, retired_k8s_node];
    expected.sort_unstable();
    assert_eq!(nodes, expected);

    Ok(())
}

async fn insert_template(pool: &PgPool, template: &TemplateName) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, description, org_id)
         VALUES ($1, $2, NULL, NULL)"
    ))
    .bind(Uuid::now_v7())
    .bind(template.as_str())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_node(
    pool: &PgPool,
    node_id: NodeId,
    role: &str,
    managed_by: &str,
    address_octet: u8,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, $3, $4, 'ready')"
    ))
    .bind(node_id)
    .bind(format!("10.0.0.{address_octet}"))
    .bind(role)
    .bind(managed_by)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_direct_node(pool: &PgPool, node_id: NodeId, deleted: bool) -> TestResult {
    let endpoint = format!("10.0.0.{}:9802", node_id.as_uuid().as_bytes()[15]);
    if deleted {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NOW())"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(pool)
        .await?;
    } else {
        sqlx::query(sql!(
            "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NULL)"
        ))
        .bind(node_id)
        .bind(endpoint)
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_inventory(pool: &PgPool, node_id: NodeId, template: &TemplateName) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime
         )
         VALUES ($1, $2, 'sha256:test', 'fc')"
    ))
    .bind(node_id)
    .bind(template.as_str())
    .execute(pool)
    .await?;
    Ok(())
}
