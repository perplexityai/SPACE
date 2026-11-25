//! The terminate's sandbox reads and writes against Postgres: which
//! suspended rows the release takes, what still names a node, and the
//! retire of a released row's execution.

use pplx_space_accounts::NodeId;
use pplx_space_node_model::SubstrateKind;
use pplx_space_sandbox_model::SandboxId;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_residency::node_unreferenced;
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

/// The claim's ownership window the tests release under.
const TTL_SECS: i64 = 600;
const NODE_NAME: &str = "worker-drain-terminate.synthetic.internal";

async fn insert_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.6'::INET, 'worker', $2)"
    ))
    .bind(node_id)
    .bind(SubstrateKind::Kubernetes)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A row placed an hour ago.
async fn insert_sandbox(
    pool: &PgPool,
    lifecycle: &str,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             placement_changed_at, command_ready_pending)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1, NOW() - INTERVAL '1 hour', TRUE)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(SubstrateKind::Kubernetes)
    .bind(lifecycle)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A ready full snapshot of the row with a storage location, taken `age`
/// ago.
async fn durable_snapshot(pool: &PgPool, id: SandboxId, age_secs: i64) -> TestResult {
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, timestamp, org_id)
         VALUES ($1, $2, 'full', 'ready', NOW() - make_interval(secs => $3::BIGINT), $4::UUID)"
    ))
    .bind(snapshot)
    .bind(id)
    .bind(age_secs)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (id, snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, 's3://test-bucket/drain-terminate?region=us-east-1', $3::UUID)"
    ))
    .bind(Uuid::now_v7())
    .bind(snapshot)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(())
}

/// An active execution of the row on [`NODE_NAME`] in `state`, last updated
/// `age` ago.
async fn insert_execution(
    pool: &PgPool,
    id: SandboxId,
    state: &str,
    age_secs: i64,
) -> TestResult<Uuid> {
    Ok(sqlx::query_scalar(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             updated_at)
         VALUES ($1, 1, $2, 'sandboxes', $3, $4, TRUE,
                 NOW() - make_interval(secs => $5::BIGINT))
         RETURNING id"
    ))
    .bind(id)
    .bind(format!("space-sbx-{}", id.as_uuid().simple()))
    .bind(NODE_NAME)
    .bind(state)
    .bind(age_secs)
    .fetch_one(pool)
    .await?)
}

/// `(lifecycle, placement, active_node_id, target_node_id)`.
type Placement = (String, String, Option<NodeId>, Option<NodeId>);

async fn placement(pool: &PgPool, id: SandboxId) -> TestResult<Placement> {
    Ok(sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

async fn execution_active(pool: &PgPool, execution_id: Uuid) -> TestResult<bool> {
    Ok(sqlx::query_scalar(sql!(
        "SELECT active FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(execution_id)
    .fetch_one(pool)
    .await?)
}

async fn release(pool: &PgPool, node: NodeId) -> TestResult<Vec<SandboxId>> {
    let mut tx = pool.begin().await?;
    let released = release_durable_suspended(&mut tx, node, Some(NODE_NAME), TTL_SECS).await?;
    tx.commit().await?;
    Ok(released)
}

/// The release takes a `suspended` row naming the node, active or target,
/// whose state a durable snapshot holds and whose restore isn't in flight:
/// it is unplaced from the node, keeping its lifecycle, and its execution on
/// the node is retired. A row without a durable snapshot of its current
/// placement, one with a restore in flight, one in another lifecycle and
/// one on another node stay as they were. A second release changes nothing.
#[tokio::test(flavor = "current_thread")]
async fn the_release_takes_only_durable_suspended_rows_with_no_restore_in_flight() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let other = insert_node(pool).await?;

    let by_target = insert_sandbox(pool, "suspended", None, Some(node)).await?;
    durable_snapshot(pool, by_target, 60).await?;
    let by_active = insert_sandbox(pool, "suspended", Some(node), Some(node)).await?;
    durable_snapshot(pool, by_active, 60).await?;
    let retired = insert_execution(pool, by_active, "suspended", 0).await?;
    let stale_execution = insert_sandbox(pool, "suspended", None, Some(node)).await?;
    durable_snapshot(pool, stale_execution, 60).await?;
    let stale = insert_execution(pool, stale_execution, "running", TTL_SECS * 2).await?;

    let no_snapshot = insert_sandbox(pool, "suspended", None, Some(node)).await?;
    let older_snapshot = insert_sandbox(pool, "suspended", None, Some(node)).await?;
    durable_snapshot(pool, older_snapshot, 7200).await?;
    let restoring = insert_sandbox(pool, "suspended", None, Some(node)).await?;
    durable_snapshot(pool, restoring, 60).await?;
    let in_flight = insert_execution(pool, restoring, "running", 0).await?;
    let started = insert_sandbox(pool, "started", Some(node), Some(node)).await?;
    durable_snapshot(pool, started, 60).await?;
    let elsewhere = insert_sandbox(pool, "suspended", None, Some(other)).await?;
    durable_snapshot(pool, elsewhere, 60).await?;

    let mut expected = vec![by_target, by_active, stale_execution];
    expected.sort();
    assert_eq!(release(pool, node).await?, expected);
    for id in &expected {
        assert_eq!(
            placement(pool, *id).await?,
            ("suspended".to_owned(), "unplaced".to_owned(), None, None),
            "{id}"
        );
    }
    assert!(!execution_active(pool, retired).await?);
    assert!(!execution_active(pool, stale).await?);
    assert!(execution_active(pool, in_flight).await?);
    for (id, active) in [
        (no_snapshot, None),
        (older_snapshot, None),
        (restoring, None),
        (started, Some(node)),
    ] {
        let (_, _, active_node, target) = placement(pool, id).await?;
        assert_eq!((active_node, target), (active, Some(node)), "{id}");
    }
    assert_eq!(placement(pool, elsewhere).await?.3, Some(other));
    assert!(release(pool, node).await?.is_empty());
    Ok(())
}

/// A suspended row with an activation in flight, an active execution whose
/// activation attempt hasn't expired, is not released: it keeps naming the
/// node and its execution, so the terminate's reference check refuses the
/// node. Once the attempt expires the release takes it, and nothing names
/// the node.
#[tokio::test(flavor = "current_thread")]
async fn the_release_skips_a_row_with_an_activation_in_flight() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let unreferenced = || async move {
        let fragment = node_unreferenced("$1", "$2");
        let empty: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(
            format!("SELECT {fragment}").as_str()
        )))
        .bind(node)
        .bind(NODE_NAME)
        .fetch_one(pool)
        .await?;
        TestResult::Ok(empty)
    };
    let id = insert_sandbox(pool, "suspended", Some(node), Some(node)).await?;
    durable_snapshot(pool, id, 60).await?;
    // A suspended execution, which no restore window counts: only the
    // activation attempt holds the row.
    let execution = insert_execution(pool, id, "suspended", 0).await?;
    let attempt = |expires_in_secs: i64| async move {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions
                SET activation_attempt_id = $2,
                    activation_attempt_expires_at = NOW() + make_interval(secs => $3::BIGINT),
                    activation_attempt_target_state = 'running'
              WHERE id = $1"
        ))
        .bind(execution)
        .bind(Uuid::now_v7())
        .bind(expires_in_secs)
        .execute(pool)
        .await?;
        TestResult::Ok(())
    };

    attempt(60).await?;
    assert!(release(pool, node).await?.is_empty());
    assert_eq!(
        placement(pool, id).await?,
        (
            "suspended".to_owned(),
            "on_node".to_owned(),
            Some(node),
            Some(node)
        )
    );
    assert!(execution_active(pool, execution).await?);
    let references = node_references(pool, node, Some(NODE_NAME), 10).await?;
    assert_eq!(
        (references.active, references.target, references.executions),
        (vec![id], vec![id], vec![id])
    );
    assert!(!unreferenced().await?, "the terminate refuses the node");

    attempt(-60).await?;
    assert_eq!(release(pool, node).await?, [id]);
    assert_eq!(
        placement(pool, id).await?,
        ("suspended".to_owned(), "unplaced".to_owned(), None, None)
    );
    assert!(!execution_active(pool, execution).await?);
    assert!(
        node_references(pool, node, Some(NODE_NAME), 10)
            .await?
            .is_empty()
    );
    assert!(unreferenced().await?);
    Ok(())
}

/// What names a node is what the delete's guards read: a row outside
/// `failed` and `deleted` naming it active or target, and an active
/// execution on its name, whatever the row. The read and the SQL fragment
/// agree on each.
#[tokio::test(flavor = "current_thread")]
async fn node_references_and_the_fragment_read_the_same_guards() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let unreferenced = |node: NodeId| async move {
        let fragment = node_unreferenced("$1", "$2");
        let empty: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(
            format!("SELECT {fragment}").as_str()
        )))
        .bind(node)
        .bind(NODE_NAME)
        .fetch_one(pool)
        .await?;
        TestResult::Ok(empty)
    };
    for case in ["terminal", "active", "target", "execution"] {
        let node = insert_node(pool).await?;
        let mut expected = NodeReferences::default();
        match case {
            "terminal" => {
                insert_sandbox(pool, "failed", None, Some(node)).await?;
                insert_sandbox(pool, "deleted", Some(node), Some(node)).await?;
            }
            "active" => expected
                .active
                .push(insert_sandbox(pool, "quarantined", Some(node), None).await?),
            "target" => expected
                .target
                .push(insert_sandbox(pool, "suspended", None, Some(node)).await?),
            _ => {
                let id = insert_sandbox(pool, "deleted", None, None).await?;
                insert_execution(pool, id, "running", 0).await?;
                expected.executions.push(id);
            }
        }
        let references = node_references(pool, node, Some(NODE_NAME), 10).await?;
        assert_eq!(references, expected, "{case}");
        assert_eq!(unreferenced(node).await?, expected.is_empty(), "{case}");
        // The execution guard is by name: without one, nothing matches it.
        let nameless = node_references(pool, node, None, 10).await?;
        assert!(nameless.executions.is_empty(), "{case}");
        // Each id fills its own execution, so clear them for the next case.
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE k8s_node_name = $1"
        ))
        .bind(NODE_NAME)
        .execute(pool)
        .await?;
    }
    Ok(())
}

/// The retire takes only the execution it names, and only while active.
#[tokio::test(flavor = "current_thread")]
async fn the_retire_takes_only_the_named_active_execution() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = insert_node(pool).await?;
    let id = insert_sandbox(pool, "suspended", Some(node), Some(node)).await?;
    let active = insert_execution(pool, id, "suspended", 0).await?;
    let mut tx = pool.begin().await?;
    assert!(!retire_source_execution(&mut tx, id, Uuid::now_v7()).await?);
    assert!(retire_source_execution(&mut tx, id, active).await?);
    assert!(!retire_source_execution(&mut tx, id, active).await?);
    tx.commit().await?;
    assert!(!execution_active(pool, active).await?);
    Ok(())
}
