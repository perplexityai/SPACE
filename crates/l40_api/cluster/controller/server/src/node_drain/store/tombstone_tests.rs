use pplx_space_common_core::types::NodeId;

use std::time::Duration;

use pplx_space_db_test_fixtures::{try_pg_test_db, try_pg_test_db_with_max_connections};

use super::*;

use pplx_space_test_env::TestResult;

async fn insert_terminating_k8s_node(
    pool: &PgPool,
    node_id: NodeId,
    node_name: &str,
) -> TestResult {
    insert_terminating_node(pool, node_id, node_name, SubstrateKind::Kubernetes).await
}

/// A `terminating` node of `managed_by` with a `node_k8s` identity.
async fn insert_terminating_node(
    pool: &PgPool,
    node_id: NodeId,
    node_name: &str,
    managed_by: SubstrateKind,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.99'::inet, 'worker', $2, 'registered', 'terminating', NOW())"
    ))
    .bind(node_id)
    .bind(managed_by)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'kube-uid')"
    ))
    .bind(node_id)
    .bind(node_name)
    .execute(pool)
    .await?;
    Ok(())
}

/// Deleting a terminating node leaves a tombstone keyed on the
/// deterministic SPACE node id, even after the `node_k8s` CASCADE
/// drops `k8s_node_name`. Without the tombstone, the daemon on the
/// still-running EC2 would re-bootstrap the row on its next heartbeat
/// and the orchestrator would re-drain the same node.
#[tokio::test(flavor = "current_thread")]
async fn delete_terminating_node_inserts_tombstone() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());
    insert_terminating_k8s_node(&db.pool, node_id, &node_name).await?;

    let deleted = delete_terminating_node_and_record_tombstone(&db.pool, node_id).await?;
    assert!(deleted, "expected terminating row to be deleted");

    let remaining: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(remaining, 0, "nodes row must be gone");

    let (tomb_name, role, reason): (String, String, Option<String>) = sqlx::query_as(sql!(
        "SELECT k8s_node_name, role, reason FROM node_k8s_tombstones WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(tomb_name, node_name);
    assert_eq!(role, "worker");
    assert_eq!(reason.as_deref(), Some("admin-terminate"));
    Ok(())
}

/// A node not in `terminating` (operator undrained between
/// `:terminate` and the reconciler tick) must be left alone and no
/// tombstone written.
#[tokio::test(flavor = "current_thread")]
async fn delete_terminating_node_is_noop_when_phase_drifted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase)
         VALUES ($1, '10.0.0.99'::inet, 'worker', 'k8s', 'registered', NULL)"
    ))
    .bind(node_id)
    .execute(&db.pool)
    .await?;

    let deleted = delete_terminating_node_and_record_tombstone(&db.pool, node_id).await?;
    assert!(!deleted, "phase != terminating must be a no-op");

    let still_present: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM nodes WHERE id = $1"))
        .bind(node_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(still_present, 1, "row must survive the no-op");

    let tomb_count: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM node_k8s_tombstones WHERE node_id = $1"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(tomb_count, 0, "no tombstone for a no-op delete");
    Ok(())
}

/// Both terminating-node statements bind the Kubernetes substrate: a direct
/// node in `terminating` is neither listed nor deleted, and gets no tombstone.
/// The delete's snapshot SELECT rejects the direct node before its DELETE
/// runs; the racing tests below cover the DELETE's own guard.
#[tokio::test(flavor = "current_thread")]
async fn terminating_direct_node_is_neither_listed_nor_deleted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let k8s_node = NodeId::new();
    let direct_node = NodeId::new();
    for (node_id, managed_by) in [
        (k8s_node, SubstrateKind::Kubernetes),
        (direct_node, SubstrateKind::Space),
    ] {
        let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());
        insert_terminating_node(&db.pool, node_id, &node_name, managed_by).await?;
    }

    let listed: Vec<NodeId> = sqlx::query_scalar(terminating_nodes_query())
        .bind(PHASE_TERMINATING)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(&db.pool)
        .await?;
    assert_eq!(listed, vec![k8s_node]);

    assert!(!delete_terminating_node_and_record_tombstone(&db.pool, direct_node).await?);
    let (present, tombstones): (i64, i64) = sqlx::query_as(sql!(
        "SELECT (SELECT COUNT(*) FROM nodes WHERE id = $1),
                (SELECT COUNT(*) FROM node_k8s_tombstones WHERE node_id = $1)"
    ))
    .bind(direct_node)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        (present, tombstones),
        (1, 0),
        "the direct node is left alone"
    );

    assert!(delete_terminating_node_and_record_tombstone(&db.pool, k8s_node).await?);
    Ok(())
}

/// Run `delete_terminating_node_and_record_tombstone` on a terminating
/// Kubernetes node while a concurrent writer holds the row lock with an
/// uncommitted `managed_by = managed_by_at_delete`. The delete's snapshot
/// SELECT sees the committed Kubernetes row; its DELETE parks on the row
/// lock and re-evaluates its `WHERE` against the writer's committed row.
/// Returns whether the delete reported success, then the remaining node and
/// tombstone counts.
///
/// Waiting for the DELETE to block on the writer and joining the delete task
/// are each bounded by [`INTERLEAVING_BOUND`].
async fn delete_racing_managed_by_write(
    managed_by_at_delete: SubstrateKind,
) -> TestResult<Option<(bool, i64, i64)>> {
    let Some(db) = try_pg_test_db_with_max_connections(3).await? else {
        return Ok(None);
    };
    let node_id = NodeId::new();
    let node_name = format!("space-test-node-{}", node_id.as_uuid().simple());
    insert_terminating_k8s_node(&db.pool, node_id, &node_name).await?;

    let mut writer = db.pool.begin().await?;
    let writer_pid: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
        .fetch_one(&mut *writer)
        .await?;
    sqlx::query(sql!("UPDATE nodes SET managed_by = $2 WHERE id = $1"))
        .bind(node_id)
        .bind(managed_by_at_delete)
        .execute(&mut *writer)
        .await?;

    let delete = tokio::spawn({
        let pool = db.pool.clone();
        async move { delete_terminating_node_and_record_tombstone(&pool, node_id).await }
    });
    wait_for_backend_blocked_by(&db.pool, writer_pid).await?;
    writer.commit().await?;
    let deleted = tokio::time::timeout(INTERLEAVING_BOUND, delete)
        .await
        .map_err(|_| {
            format!(
                "terminating-node delete did not finish within {INTERLEAVING_BOUND:?} \
                 after the racing writer committed"
            )
        })???;

    let (present, tombstones): (i64, i64) = sqlx::query_as(sql!(
        "SELECT (SELECT COUNT(*) FROM nodes WHERE id = $1),
                (SELECT COUNT(*) FROM node_k8s_tombstones WHERE node_id = $1)"
    ))
    .bind(node_id)
    .fetch_one(&db.pool)
    .await?;
    Ok(Some((deleted, present, tombstones)))
}

/// Bound on each step of the racing tests that waits on another connection.
const INTERLEAVING_BOUND: Duration = Duration::from_secs(5);

/// Poll until some backend is blocked by `holder_pid`: proof the delete passed
/// its snapshot SELECT and reached its DELETE before the racing writer
/// commits. Fails after [`INTERLEAVING_BOUND`].
async fn wait_for_backend_blocked_by(pool: &PgPool, holder_pid: i32) -> TestResult {
    let poll = async {
        let mut cadence = tokio::time::interval(Duration::from_millis(25));
        loop {
            cadence.tick().await;
            let blocked: bool = sqlx::query_scalar(sql!(
                "SELECT EXISTS (
                     SELECT 1
                       FROM pg_stat_activity
                      WHERE $1 = ANY(pg_blocking_pids(pid))
                 )"
            ))
            .bind(holder_pid)
            .fetch_one(pool)
            .await?;
            if blocked {
                return TestResult::Ok(());
            }
        }
    };
    tokio::time::timeout(INTERLEAVING_BOUND, poll)
        .await
        .map_err(|_| {
            format!(
                "terminating-node DELETE did not block on the racing writer (pid {holder_pid}) \
                 within {INTERLEAVING_BOUND:?}"
            )
        })?
}

/// The DELETE re-checks the Kubernetes substrate itself: a node that becomes
/// direct after the snapshot SELECT is left in place with no tombstone.
#[tokio::test(flavor = "current_thread")]
async fn delete_skips_node_that_turned_direct_after_snapshot() -> TestResult {
    let Some(outcome) = delete_racing_managed_by_write(SubstrateKind::Space).await? else {
        return Ok(());
    };
    assert_eq!(
        outcome,
        (false, 1, 0),
        "the DELETE must not remove a node that is direct at delete time"
    );
    Ok(())
}

/// The same race with the writer keeping the Kubernetes substrate: the parked
/// DELETE still matches, and the node is replaced by its tombstone.
#[tokio::test(flavor = "current_thread")]
async fn delete_removes_node_still_kubernetes_after_racing_write() -> TestResult {
    let Some(outcome) = delete_racing_managed_by_write(SubstrateKind::Kubernetes).await? else {
        return Ok(());
    };
    assert_eq!(outcome, (true, 0, 1));
    Ok(())
}
