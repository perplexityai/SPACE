use pplx_space_accounts::NodeId;
use pplx_space_sandbox_model::SandboxId;
use pplx_space_snapshot_model::{SnapshotId, SnapshotKind, SnapshotState};

use std::time::Duration;

use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sql_locator::sql;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    FailedUpload, fail_inflight_uploads_from_gone_node, fail_quiet_full_uploads_from_node,
};

const SETTLE: Duration = Duration::from_mins(10);
/// Past [`SETTLE`].
const QUIET: Duration = Duration::from_mins(11);
/// Inside [`SETTLE`].
const FRESH: Duration = Duration::from_mins(9);

/// Every [`SnapshotState`]. The match stops compiling when one is added.
fn every_state() -> [SnapshotState; 4] {
    let _exhaustive = |state: SnapshotState| match state {
        SnapshotState::Creating
        | SnapshotState::Ready
        | SnapshotState::Failed
        | SnapshotState::Deleting => {}
    };
    [
        SnapshotState::Creating,
        SnapshotState::Ready,
        SnapshotState::Failed,
        SnapshotState::Deleting,
    ]
}

async fn insert_sandbox(pool: &PgPool) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2::UUID, $3::UUID, 1)"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(id)
}

/// A snapshot from `node_id` created `created_ago`, last heartbeated
/// `heartbeat_ago` (`None` = never).
async fn insert_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    kind: SnapshotKind,
    state: SnapshotState,
    created_ago: Duration,
    heartbeat_ago: Option<Duration>,
) -> TestResult<SnapshotId> {
    let id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, type, state, timestamp, last_attempt_at, source_node_id)
         VALUES ($1, $2, $3, $4, NOW() - make_interval(secs => $5),
                 NOW() - make_interval(secs => $6), $7)"
    ))
    .bind(id)
    .bind(sandbox_id)
    .bind(kind.as_str())
    .bind(state.as_str())
    .bind(created_ago.as_secs_f64())
    .bind(heartbeat_ago.map(|ago| ago.as_secs_f64()))
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn snapshot_state(pool: &PgPool, id: SnapshotId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

/// Only full `creating` uploads from the gone node, for the given sandboxes,
/// quiet past the settle window, are failed. Every other kind and state, a
/// fresh row, a row with a fresh heartbeat, a row from another node and a row
/// of another sandbox stay as they were, and so do the sandbox's execution and
/// backup-queue row.
#[tokio::test(flavor = "current_thread")]
async fn fail_quiet_full_uploads_from_node_fails_only_the_nodes_dead_uploads() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let gone = NodeId::new();
    let live = NodeId::new();
    let lost = insert_sandbox(pool).await?;
    let bystander = insert_sandbox(pool).await?;

    let mut sweep = Vec::new();
    for &kind in SnapshotKind::all() {
        for state in every_state() {
            let id = insert_snapshot(pool, lost, gone, kind, state, QUIET, None).await?;
            let expected = if kind == SnapshotKind::Full && state == SnapshotState::Creating {
                SnapshotState::Failed
            } else {
                state
            };
            sweep.push((id, kind, state, expected));
        }
    }
    let creating = |node_id, created_ago, heartbeat_ago| {
        insert_snapshot(
            pool,
            lost,
            node_id,
            SnapshotKind::Full,
            SnapshotState::Creating,
            created_ago,
            heartbeat_ago,
        )
    };
    let fresh = creating(gone, FRESH, None).await?;
    let heartbeating = creating(gone, Duration::from_hours(1), Some(FRESH)).await?;
    let stale_heartbeat = creating(gone, Duration::from_hours(1), Some(QUIET)).await?;
    let other_node = creating(live, QUIET, None).await?;
    let other_sandbox = insert_snapshot(
        pool,
        bystander,
        gone,
        SnapshotKind::Full,
        SnapshotState::Creating,
        QUIET,
        None,
    )
    .await?;

    let execution = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (id, sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, $2, 1, 'sbx-lost-test', 'space-sandboxes', 'running', TRUE)"
    ))
    .bind(execution)
    .bind(lost)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending
            (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, 'full')"
    ))
    .bind(gone)
    .bind(lost)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;

    let mut tx = pool.begin().await?;
    fail_quiet_full_uploads_from_node(&mut *tx, gone, &[lost], SETTLE).await?;
    tx.commit().await?;

    for (id, kind, state, expected) in sweep {
        assert_eq!(
            snapshot_state(pool, id).await?,
            expected.as_str(),
            "quiet {kind:?} {state:?} upload from the gone node"
        );
    }
    for (id, case) in [
        (fresh, "created inside the settle window"),
        (heartbeating, "heartbeated inside the settle window"),
        (other_node, "from another node"),
        (other_sandbox, "of a sandbox not in the list"),
    ] {
        assert_eq!(snapshot_state(pool, id).await?, "creating", "{case}");
    }
    assert_eq!(
        snapshot_state(pool, stale_heartbeat).await?,
        "failed",
        "heartbeated, then quiet past the settle window"
    );

    let (active, state): (bool, String) = sqlx::query_as(sql!(
        "SELECT active, state FROM sandbox_k8s_executions WHERE id = $1"
    ))
    .bind(execution)
    .fetch_one(pool)
    .await?;
    assert_eq!((active, state.as_str()), (true, "running"));
    let queued: bool = sqlx::query_scalar(sql!(
        "SELECT EXISTS (
            SELECT 1 FROM node_local_backup_pending
             WHERE node_id = $1 AND sandbox_id = $2
         )"
    ))
    .bind(gone)
    .bind(lost)
    .fetch_one(pool)
    .await?;
    assert!(queued);
    Ok(())
}

const GONE_UID: &str = "uid-gone";
const REASON: &str = "the upload worker died with the instance";

/// A `draining` Kubernetes node registered under `uid`.
async fn insert_draining_k8s_node(pool: &PgPool, node_id: NodeId, uid: Option<&str>) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.50'::inet, 'worker', 'k8s', 'ready', 'draining', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, 'ip-10-0-0-50.ec2.internal', $2)"
    ))
    .bind(node_id)
    .bind(uid)
    .execute(pool)
    .await?;
    Ok(())
}

async fn snapshot_state_and_reason(
    pool: &PgPool,
    id: SnapshotId,
) -> TestResult<(String, Option<String>)> {
    Ok(sqlx::query_as(sql!(
        "SELECT state, failure_reason FROM snapshots WHERE id = $1"
    ))
    .bind(id)
    .fetch_one(pool)
    .await?)
}

/// Under the identity the caller observed, every `creating` full upload from
/// the node is finalized `failed` with the reason, and returned; a recorded
/// reason is kept. Other kinds and states, and uploads from another node, are
/// untouched.
#[tokio::test(flavor = "current_thread")]
async fn fail_inflight_uploads_from_gone_node_fails_the_nodes_creating_full_uploads() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let gone = NodeId::new();
    let live = NodeId::new();
    insert_draining_k8s_node(pool, gone, Some(GONE_UID)).await?;
    let sandbox = insert_sandbox(pool).await?;
    let insert =
        |node_id, kind, state| insert_snapshot(pool, sandbox, node_id, kind, state, FRESH, None);
    let creating = insert(gone, SnapshotKind::Full, SnapshotState::Creating).await?;
    let with_reason = insert(gone, SnapshotKind::Full, SnapshotState::Creating).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET failure_reason = 'recorded by the worker' WHERE id = $1"
    ))
    .bind(with_reason)
    .execute(pool)
    .await?;
    let ready = insert(gone, SnapshotKind::Full, SnapshotState::Ready).await?;
    let other_node = insert(live, SnapshotKind::Full, SnapshotState::Creating).await?;
    let disk = insert(gone, SnapshotKind::Disk, SnapshotState::Creating).await?;

    let mut failed =
        fail_inflight_uploads_from_gone_node(pool, gone, Some(GONE_UID), REASON).await?;
    failed.sort_by_key(|row| row.snapshot_id.as_uuid());
    let mut expected = vec![
        FailedUpload {
            snapshot_id: creating,
            sandbox_id: sandbox,
        },
        FailedUpload {
            snapshot_id: with_reason,
            sandbox_id: sandbox,
        },
    ];
    expected.sort_by_key(|row| row.snapshot_id.as_uuid());
    assert_eq!(failed, expected);
    assert_eq!(
        snapshot_state_and_reason(pool, creating).await?,
        ("failed".to_owned(), Some(REASON.to_owned()))
    );
    assert_eq!(
        snapshot_state_and_reason(pool, with_reason).await?,
        (
            "failed".to_owned(),
            Some("recorded by the worker".to_owned())
        )
    );
    assert_eq!(snapshot_state(pool, ready).await?, "ready");
    assert_eq!(snapshot_state(pool, other_node).await?, "creating");
    assert_eq!(
        snapshot_state_and_reason(pool, disk).await?,
        ("creating".to_owned(), None)
    );
    Ok(())
}

/// The losing cases: a uid that changed since the caller observed it, a node
/// that left `draining`, and a node with no row at all each match no row, the
/// verb returns nothing, and the upload stays `creating` with no reason.
#[tokio::test(flavor = "current_thread")]
async fn fail_inflight_uploads_from_gone_node_changes_nothing_when_its_guards_refuse() -> TestResult
{
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_draining_k8s_node(pool, node, Some(GONE_UID)).await?;
    let sandbox = insert_sandbox(pool).await?;
    let upload = insert_snapshot(
        pool,
        sandbox,
        node,
        SnapshotKind::Full,
        SnapshotState::Creating,
        FRESH,
        None,
    )
    .await?;
    let untouched = ("creating".to_owned(), None);

    for observed in [Some("uid-replacement"), None] {
        let failed = fail_inflight_uploads_from_gone_node(pool, node, observed, REASON).await?;
        assert!(failed.is_empty(), "observed uid {observed:?}");
        assert_eq!(snapshot_state_and_reason(pool, upload).await?, untouched);
    }

    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'drained' WHERE id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    let failed = fail_inflight_uploads_from_gone_node(pool, node, Some(GONE_UID), REASON).await?;
    assert!(failed.is_empty(), "a node no longer draining");
    assert_eq!(snapshot_state_and_reason(pool, upload).await?, untouched);

    let unregistered = NodeId::new();
    let orphan_upload = insert_snapshot(
        pool,
        sandbox,
        unregistered,
        SnapshotKind::Full,
        SnapshotState::Creating,
        FRESH,
        None,
    )
    .await?;
    let failed =
        fail_inflight_uploads_from_gone_node(pool, unregistered, Some(GONE_UID), REASON).await?;
    assert!(failed.is_empty(), "a node with no row");
    assert_eq!(
        snapshot_state_and_reason(pool, orphan_upload).await?,
        untouched
    );
    Ok(())
}
