//! The guarded delete's statements against Postgres: the heartbeat read, and
//! the delete refusing a node that heartbeats or that something names,
//! writing nothing.

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDbResult, pg_test_db};
use sqlx::PgPool;

use super::*;

fn name_of(node_id: NodeId) -> String {
    format!("worker-{}.synthetic.internal", node_id.as_uuid().simple())
}

/// A Kubernetes node with no heartbeat, registered under [`name_of`].
async fn node(pool: &PgPool) -> TestDbResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.8'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'uid-synthetic')"
    ))
    .bind(node_id)
    .bind(name_of(node_id))
    .execute(pool)
    .await?;
    Ok(node_id)
}

async fn heartbeat(pool: &PgPool, node_id: NodeId) -> TestDbResult<()> {
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at)
         VALUES ($1, NOW(), NOW() + INTERVAL '1 minute', TRUE, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

/// A `started` sandbox naming `node_id` as `guard` says: through
/// `active_node_id`, `target_node_id`, or an active execution on its name.
async fn reference(pool: &PgPool, node_id: NodeId, guard: &str) -> TestDbResult<()> {
    let id: String = sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation)
         VALUES (space_uuid_v7(), $1, $2, 'k8s', 'started', 'on_node',
                 CASE WHEN $4 = 'active' THEN $3::UUID END,
                 CASE WHEN $4 = 'target' THEN $3::UUID END, 1)
         RETURNING id::TEXT"
    ))
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(node_id)
    .bind(guard)
    .fetch_one(pool)
    .await?;
    if guard == "execution" {
        sqlx::query(sql!(
            "INSERT INTO sandbox_k8s_executions
                (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active)
             VALUES ($1::UUID, 1, 'space-sbx-synthetic', 'sandboxes', $2, 'running', TRUE)"
        ))
        .bind(&id)
        .bind(name_of(node_id))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn exists(pool: &PgPool, node_id: NodeId) -> TestDbResult<bool> {
    Ok(
        sqlx::query_scalar(sql!("SELECT EXISTS (SELECT 1 FROM nodes WHERE id = $1)"))
            .bind(node_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Run the delete on `node_id` in a transaction of its own, under the row
/// lock, as the caller does.
async fn delete(pool: &PgPool, node_id: NodeId) -> TestDbResult<bool> {
    let name = name_of(node_id);
    let mut tx = pool.begin().await?;
    assert!(crate::locks::lock_node_for_update(&mut *tx, node_id).await?);
    let deleted =
        delete_unreferenced_stale_node(&mut *tx, node_id, SubstrateKind::Kubernetes, Some(&name))
            .await?;
    tx.commit().await?;
    Ok(deleted)
}

/// The heartbeat read is TRUE only for a reachable, unexpired heartbeat.
#[tokio::test]
async fn heartbeat_fresh_reads_the_nodes_heartbeat() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let silent = node(&db.pool).await?;
    let beating = node(&db.pool).await?;
    heartbeat(&db.pool, beating).await?;
    let mut tx = db.pool.begin().await?;
    assert!(!heartbeat_fresh(&mut *tx, silent).await?);
    assert!(heartbeat_fresh(&mut *tx, beating).await?);
    assert!(!heartbeat_fresh(&mut *tx, NodeId::new()).await?);
    tx.rollback().await?;
    Ok(())
}

/// The delete statement holds the heartbeat and require-empty guards on its
/// own, as it must for a row or a heartbeat that commits between the
/// caller's checks and the delete: it deletes nothing while the node
/// heartbeats or anything names it, and deletes a stale node nothing names,
/// with its `node_k8s` row. Another substrate's row is never deleted.
#[tokio::test]
async fn the_delete_statement_holds_its_guards_by_itself() -> TestDbResult<()> {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    for (case, deletes) in [
        ("heartbeat fresh", false),
        ("active", false),
        ("target", false),
        ("execution", false),
        ("nothing names it", true),
    ] {
        let node_id = node(pool).await?;
        match case {
            "heartbeat fresh" => heartbeat(pool, node_id).await?,
            "nothing names it" => {}
            guard => reference(pool, node_id, guard).await?,
        }
        assert_eq!(delete(pool, node_id).await?, deletes, "{case}");
        assert_eq!(exists(pool, node_id).await?, !deletes, "{case}");
    }

    let direct = node(pool).await?;
    let mut tx = pool.begin().await?;
    assert!(
        !delete_unreferenced_stale_node(
            &mut *tx,
            direct,
            SubstrateKind::Space,
            Some(&name_of(direct))
        )
        .await?
    );
    tx.rollback().await?;
    assert!(exists(pool, direct).await?);

    let stale = node(pool).await?;
    assert!(delete(pool, stale).await?);
    let registered: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (SELECT 1 FROM node_k8s WHERE node_id = $1)"
    ))
    .bind(stale)
    .fetch_one(pool)
    .await?;
    assert!(!registered);
    Ok(())
}
