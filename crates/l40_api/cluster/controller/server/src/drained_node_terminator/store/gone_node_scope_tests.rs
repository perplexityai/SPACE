//! What the drained-node give-up's write-off leaves behind, through
//! [`NodeDbStore`] against the real Postgres fixture, where
//! `inflight_upload_terminate_gate_tests` leaves it open: which uploads and
//! executions it finalizes, what the unplaced row keeps, and the kube uid lock.

use pplx_space_common_core::sql;
use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};
use pplx_space_common_svc::sql::sandbox_residency::ResidencyEvidence;
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;

use super::{DEAD_UPLOAD_SETTLE, DrainedNodeTerminatorSink, NodeDbStore};

const UID: &str = "uid-observed";

/// A drained `k8s` worker whose kube Node carries [`UID`].
async fn insert_drained_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.5'::inet, 'worker', 'k8s', 'registered', 'drained', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("synthetic-node-{node_id}"))
    .bind(UID)
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A `suspended` row without snapshots, still placed on `node_id` with an
/// active execution at generation 2 and a retired one at generation 1.
async fn insert_placed_suspended(pool: &PgPool, node_id: NodeId) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, target_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'on_node', 'k8s',
             $2, $2, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active, terminated_at)
         VALUES ($1, 1, $2, 'sandboxes', 'suspended', FALSE, NOW() - INTERVAL '1 day'),
                ($1, 2, $3, 'sandboxes', 'running', TRUE, NULL)"
    ))
    .bind(sandbox_id)
    .bind(format!("synthetic-pod-{sandbox_id}-g1"))
    .bind(format!("synthetic-pod-{sandbox_id}-g2"))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// A `creating` upload of `kind` from `source_node_id`, quiet past
/// [`DEAD_UPLOAD_SETTLE`].
async fn insert_quiet_upload(
    pool: &PgPool,
    sandbox_id: SandboxId,
    kind: &str,
    source_node_id: NodeId,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, $3, 'creating', $4, NOW() - make_interval(secs => $5))"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(kind)
    .bind(source_node_id)
    .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64 + 60)
    .execute(pool)
    .await?;
    Ok(snapshot_id)
}

/// The write-off finalizes the node's dead full upload `failed` without
/// recording a reason, leaves a `disk` upload `creating`, retires only the
/// active execution, and unplaces the row while keeping its target node.
#[tokio::test(flavor = "current_thread")]
async fn the_give_up_writes_off_only_full_uploads_and_the_active_execution() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());
    let node_id = insert_drained_node(pool).await?;
    let sandbox_id = insert_placed_suspended(pool, node_id).await?;
    let full = insert_quiet_upload(pool, sandbox_id, "full", node_id).await?;
    let disk = insert_quiet_upload(pool, sandbox_id, "disk", node_id).await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node_id, Some(UID.to_owned()))
        .await?;
    assert_eq!(marked, vec![sandbox_id]);

    let uploads: Vec<(SnapshotId, String, Option<String>)> = sqlx::query_as(sql!(
        "SELECT id, state, failure_reason FROM snapshots WHERE sandbox_id = $1 ORDER BY id"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    let mut expected = vec![
        (full, "failed".to_owned(), None),
        (disk, "creating".to_owned(), None),
    ];
    expected.sort();
    assert_eq!(uploads, expected);

    let executions: Vec<(i64, String, bool, bool)> = sqlx::query_as(sql!(
        "SELECT generation, state, active, terminated_at < NOW() - INTERVAL '1 hour'
           FROM sandbox_k8s_executions WHERE sandbox_id = $1 ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    assert_eq!(
        executions,
        vec![
            (1, "suspended".to_owned(), false, true),
            (2, "forgotten".to_owned(), false, false),
        ]
    );

    let row: (String, String, Option<NodeId>, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        row,
        (
            "failed".to_owned(),
            "unplaced".to_owned(),
            None,
            Some(node_id)
        )
    );
    Ok(())
}

/// A same-name replacement's uid bump that is still uncommitted when the
/// give-up starts holds the give-up until it commits, and the give-up then
/// refuses.
#[tokio::test(flavor = "current_thread")]
async fn the_give_up_refuses_when_a_concurrent_uid_bump_commits_first() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let node_id = insert_drained_node(pool).await?;
    let sandbox_id = insert_placed_suspended(pool, node_id).await?;

    let mut bump = pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE node_k8s SET k8s_node_uid = 'uid-replacement' WHERE node_id = $1"
    ))
    .bind(node_id)
    .execute(&mut *bump)
    .await?;

    let racing = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());
    let racer = tokio::spawn(async move {
        racing
            .mark_unrecoverable_sandboxes_lost(node_id, Some(UID.to_owned()))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    bump.commit().await?;

    let marked = racer.await.expect("the give-up must not panic")?;
    assert_eq!(marked, Vec::<SandboxId>::new());
    let lifecycle: String =
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(lifecycle, "suspended");
    Ok(())
}
