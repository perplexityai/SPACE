use pplx_space_db_test_fixtures::try_pg_test_db;
use pplx_space_test_env::TestResult;

use super::*;

#[derive(Debug, Clone, Copy)]
enum Status {
    /// Reachable, `stale_after` in the future.
    Fresh,
    /// Reachable, `stale_after` past.
    Expired,
    /// `stale_after` in the future, but the last poll found it unreachable.
    Unreachable,
    /// No `node_status_snapshots` row.
    Missing,
}

async fn insert_node(
    pool: &PgPool,
    managed_by: SubstrateKind,
    drain_phase: Option<&str>,
    status: Status,
    uid: Option<&str>,
) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, drain_phase)
         VALUES ($1, '10.0.0.8'::INET, 'worker', $2, $3)"
    ))
    .bind(node_id)
    .bind(managed_by)
    .bind(drain_phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!(
        "worker-{}.synthetic.internal",
        node_id.as_uuid().simple()
    ))
    .bind(uid)
    .execute(pool)
    .await?;
    let (reachable, stale_after_secs) = match status {
        Status::Fresh => (true, 60),
        Status::Expired => (true, -60),
        Status::Unreachable => (false, 60),
        Status::Missing => return Ok(node_id),
    };
    sqlx::query(sql!(
        "INSERT INTO node_status_snapshots
            (node_id, observed_at, stale_after, reachable, services, updated_at)
         VALUES ($1, NOW(), NOW() + make_interval(secs => $2::BIGINT), $3, '[]'::JSONB, NOW())"
    ))
    .bind(node_id)
    .bind(stale_after_secs)
    .bind(reachable)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// Judged: nodes of the substrate outside any drain whose heartbeat is not
/// fresh by the node-status rule. A fresh heartbeat, any drain phase, or the
/// other substrate keeps a node out; the cursor pages through the rest.
#[tokio::test(flavor = "current_thread")]
async fn stale_nodes_are_the_undrained_nodes_without_a_fresh_heartbeat() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let k8s = SubstrateKind::Kubernetes;
    let mut stale = vec![
        insert_node(pool, k8s, None, Status::Expired, Some("uid-a")).await?,
        insert_node(pool, k8s, None, Status::Unreachable, Some("uid-b")).await?,
        insert_node(pool, k8s, None, Status::Missing, None).await?,
    ];
    stale.sort();
    insert_node(pool, k8s, None, Status::Fresh, Some("uid-fresh")).await?;
    for phase in ["draining", "drained", "terminating"] {
        insert_node(pool, k8s, Some(phase), Status::Expired, Some("uid-drain")).await?;
    }
    insert_node(pool, SubstrateKind::Space, None, Status::Expired, None).await?;

    let store = DeadNodeDbStore::new(pool.clone());
    let all: Vec<NodeId> = store
        .stale_nodes(k8s, None, 100)
        .await?
        .into_iter()
        .map(|node| node.node_id)
        .collect();
    assert_eq!(all, stale);

    let first = store.stale_nodes(k8s, None, 2).await?;
    let rest = store
        .stale_nodes(k8s, first.last().map(|n| n.node_id), 2)
        .await?;
    let paged: Vec<NodeId> = first.iter().chain(&rest).map(|node| node.node_id).collect();
    assert_eq!(paged, stale);
    Ok(())
}

/// References: fresh nodes of the substrate with a recorded uid, whatever
/// their drain phase.
#[tokio::test(flavor = "current_thread")]
async fn reference_nodes_are_fresh_nodes_with_a_recorded_uid() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let k8s = SubstrateKind::Kubernetes;
    insert_node(pool, k8s, None, Status::Fresh, Some("uid-fresh")).await?;
    insert_node(
        pool,
        k8s,
        Some("draining"),
        Status::Fresh,
        Some("uid-draining"),
    )
    .await?;
    insert_node(pool, k8s, None, Status::Fresh, None).await?;
    insert_node(pool, k8s, None, Status::Expired, Some("uid-expired")).await?;
    insert_node(
        pool,
        k8s,
        None,
        Status::Unreachable,
        Some("uid-unreachable"),
    )
    .await?;
    insert_node(
        pool,
        SubstrateKind::Space,
        None,
        Status::Fresh,
        Some("uid-direct"),
    )
    .await?;

    let mut uids: Vec<String> = DeadNodeDbStore::new(pool.clone())
        .reference_nodes(k8s, 10)
        .await?
        .into_iter()
        .map(|node| node.k8s_node_uid)
        .collect();
    uids.sort();
    assert_eq!(uids, ["uid-draining", "uid-fresh"]);
    Ok(())
}
