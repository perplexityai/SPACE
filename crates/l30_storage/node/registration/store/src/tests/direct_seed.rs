//! Node rows the node and placement tests seed.

use pplx_space_accounts::NodeId;
use pplx_space_db_test_fixtures::TestDbResult;
use pplx_space_sql_locator::sql;
use sqlx::PgPool;
use uuid::Uuid;

/// A `nodes` row with no `node_direct` row.
pub(crate) async fn node(
    pool: &PgPool,
    role: &str,
    managed_by: &str,
    state: Option<&str>,
) -> TestDbResult<NodeId> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.0.0.1'::INET, $2, $3, $4)"
    ))
    .bind(id)
    .bind(role)
    .bind(managed_by)
    .bind(state)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A Direct node: a `nodes` row in `state` and its `node_direct` row, deleted
/// when `deleted`.
pub(crate) async fn direct_node(
    pool: &PgPool,
    role: &str,
    state: &str,
    name: &str,
    deleted: bool,
) -> TestDbResult<NodeId> {
    let id = node(pool, role, "space", Some(state)).await?;
    sqlx::query(sql!(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, metadata, deleted_at)
         VALUES ($1, $2, $3, '{\"rack\": \"r1\"}'::JSONB, CASE WHEN $4 THEN NOW() END)"
    ))
    .bind(id)
    .bind(name)
    .bind(endpoint(name))
    .bind(deleted)
    .execute(pool)
    .await?;
    Ok(id)
}

/// The node-api endpoint [`direct_node`] records for `name`.
pub(crate) fn endpoint(name: &str) -> String {
    format!("http://{name}:7070")
}

/// A live, registered Direct worker.
pub(crate) async fn worker(pool: &PgPool, name: &str) -> TestDbResult<NodeId> {
    direct_node(pool, "worker", "registered", name, false).await
}

/// A reachable snapshot of `node_id` in `scheduling_status`, stale when
/// `stale`.
pub(crate) async fn snapshot(
    pool: &PgPool,
    node_id: NodeId,
    scheduling_status: &str,
    stale: bool,
) -> TestDbResult<()> {
    snapshot_reachable(pool, node_id, scheduling_status, stale, true).await
}

pub(crate) async fn snapshot_reachable(
    pool: &PgPool,
    node_id: NodeId,
    scheduling_status: &str,
    stale: bool,
    reachable: bool,
) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, health_healthy, services, updated_at
         )
         VALUES (
             $1, NOW() - INTERVAL '2 hours',
             CASE WHEN $3 THEN NOW() - INTERVAL '1 hour' ELSE NOW() + INTERVAL '1 hour' END,
             $4, 'worker', $2, TRUE, '[]'::JSONB, NOW()
         )"
    ))
    .bind(node_id)
    .bind(scheduling_status)
    .bind(stale)
    .bind(reachable)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `node_k8s` row naming `node_id`'s Kubernetes node.
pub(crate) async fn k8s_name(pool: &PgPool, node_id: NodeId, name: &str) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
    ))
    .bind(node_id)
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// A template row, once per name.
pub(crate) async fn template(pool: &PgPool, name: &str) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')
         ON CONFLICT (name) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(name)
    .execute(pool)
    .await?;
    Ok(())
}

/// An `fc` build of `template` cached on `node_id`, with a base snapshot
/// when `base_snapshot` and `pool_current` warm members.
pub(crate) async fn inventory(
    pool: &PgPool,
    node_id: NodeId,
    template_name: &str,
    content_hash: &str,
    base_snapshot: bool,
    pool_current: i32,
) -> TestDbResult<()> {
    template(pool, template_name).await?;
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (
             node_id, template_name, content_hash, runtime, arch,
             has_base_snapshot, pool_current, vcpu, memory_mib, guest_os
         )
         VALUES ($1, $2, $3, 'fc', 'aarch64', $4, $5, 2, 1024, 'linux')"
    ))
    .bind(node_id)
    .bind(template_name)
    .bind(content_hash)
    .bind(base_snapshot)
    .bind(pool_current)
    .execute(pool)
    .await?;
    Ok(())
}
