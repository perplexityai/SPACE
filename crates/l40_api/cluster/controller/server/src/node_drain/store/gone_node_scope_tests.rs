//! What node drain's gone-node give-ups write, through [`NodeDrainSink`] on the
//! production [`SubstrateSink`] against the real Postgres fixture, where
//! `durable_upload_gate_tests` and `lifecycle_writer_tests` leave it open: the
//! rows the give-ups leave alone, which uploads the upload flip finalizes and
//! how it records why, and the vetoes of the per-sandbox adopt and mark-lost.

use pplx_space_common_core::types::{NodeId, SandboxId, SandboxLifecycle, SnapshotId};
use pplx_space_db_test_fixtures::pg_test_db;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::lifecycle_writer_test_support::{
    ExecutionSeed, SandboxSeed, insert_execution, insert_node, insert_sandbox, pin_local_artifact,
};

const UID: &str = "uid-observed";

fn sink(pool: &PgPool) -> SubstrateSink {
    SubstrateSink::with_suspend_execution(pool.clone(), |_| async {
        unreachable!("no gone-node give-up suspends")
    })
}

/// A draining `k8s` worker whose kube Node carries [`UID`].
async fn insert_gone_draining_node(pool: &PgPool) -> TestResult<NodeId> {
    let node_id = insert_node(pool, Some("draining"), None).await?;
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

/// A snapshot of `kind` in `state` from `source_node_id`, registered now; a
/// `ready` one gets a durable storage location.
async fn insert_upload(
    pool: &PgPool,
    sandbox_id: SandboxId,
    kind: &str,
    state: &str,
    source_node_id: NodeId,
) -> TestResult<SnapshotId> {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, $3, $4, $5, NOW())"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(kind)
    .bind(state)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    if state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
    }
    Ok(snapshot_id)
}

/// A full backup in `state`, registered now, of the local artifact the row
/// carries; a `ready` one gets a durable storage location.
async fn insert_backup_of_pinned_artifact(
    pool: &PgPool,
    sandbox_id: SandboxId,
    state: &str,
    source_node_id: NodeId,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, source_node_id, timestamp,
              local_artifact_id, artifact_kind, artifact_origin)
         SELECT $1, id, 'full', $4, $3, NOW(),
                current_local_artifact_id, current_local_artifact_kind,
                current_local_artifact_origin
           FROM sandboxes WHERE id = $2"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(source_node_id)
    .bind(state)
    .execute(pool)
    .await?;
    if state != "ready" {
        return Ok(());
    }
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(())
}

async fn enqueue_backup(pool: &PgPool, node_id: NodeId, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, 'full')"
    ))
    .bind(node_id)
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

/// What a give-up could write beside the lifecycle: the row's placement, its
/// execution, its backup queue rows, and the state of each of its uploads.
#[derive(Debug, PartialEq, Eq)]
struct Bookkeeping {
    lifecycle: String,
    placement: String,
    active_node_id: Option<NodeId>,
    target_node_id: Option<NodeId>,
    /// `(state, active, terminated)` of each execution, by generation.
    executions: Vec<(String, bool, bool)>,
    queued_on: Vec<NodeId>,
    upload_states: Vec<String>,
}

async fn bookkeeping(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<Bookkeeping> {
    let (lifecycle, placement, active_node_id, target_node_id) = sqlx::query_as(sql!(
        "SELECT lifecycle, placement, active_node_id, target_node_id
           FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(pool)
    .await?;
    let executions = sqlx::query_as(sql!(
        "SELECT state, active, terminated_at IS NOT NULL
           FROM sandbox_k8s_executions WHERE sandbox_id = $1 ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    let queued_on = sqlx::query_scalar(sql!(
        "SELECT node_id FROM node_local_backup_pending WHERE sandbox_id = $1 ORDER BY node_id"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    let upload_states = sqlx::query_scalar(sql!(
        "SELECT state FROM snapshots WHERE sandbox_id = $1 ORDER BY id"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    Ok(Bookkeeping {
        lifecycle,
        placement,
        active_node_id,
        target_node_id,
        executions,
        queued_on,
        upload_states,
    })
}

/// Node drain's two terminal gone-node give-ups flip the lifecycle and nothing
/// else: the execution stays active and untouched, the row keeps its node,
/// the node's backup queue row stays, and the bulk fail leaves the resident's
/// in-flight upload to the separate upload flip. Each seed starts in the state
/// the drained-node terminator's write-off would change.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_give_ups_flip_only_the_lifecycle() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool);
    let node_id = insert_gone_draining_node(pool).await?;

    let resident = SandboxId::new();
    insert_sandbox(
        pool,
        resident,
        &SandboxSeed::new("started", "k8s").on_node(node_id),
    )
    .await?;
    insert_execution(pool, resident, ExecutionSeed::RUNNING).await?;
    enqueue_backup(pool, node_id, resident).await?;
    insert_upload(pool, resident, "full", "creating", node_id).await?;

    let suspended = SandboxId::new();
    insert_sandbox(
        pool,
        suspended,
        &SandboxSeed::new("suspended", "k8s").on_node(node_id),
    )
    .await?;
    insert_execution(pool, suspended, ExecutionSeed::RUNNING).await?;
    enqueue_backup(pool, node_id, suspended).await?;

    let before_resident = bookkeeping(pool, resident).await?;
    let before_suspended = bookkeeping(pool, suspended).await?;

    assert_eq!(
        sink.fail_resident_sandboxes_on_gone_node(node_id, Some(UID))
            .await?,
        1
    );
    assert_eq!(
        sink.mark_sandbox_lost_on_gone_node(suspended, node_id, Some(UID))
            .await?,
        1
    );

    for (sandbox_id, before) in [(resident, before_resident), (suspended, before_suspended)] {
        let expected = Bookkeeping {
            lifecycle: "failed".to_owned(),
            ..before
        };
        assert_eq!(bookkeeping(pool, sandbox_id).await?, expected);
    }
    Ok(())
}

/// The upload flip finalizes every `creating` full upload from the node at
/// once, however fresh, keeps a reason the worker already recorded and
/// stamps its own otherwise, and leaves a `disk` upload alone.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_upload_flip_fails_fresh_full_uploads_keeping_a_recorded_reason() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool);
    let node_id = insert_gone_draining_node(pool).await?;

    let sandbox_id = SandboxId::new();
    insert_sandbox(
        pool,
        sandbox_id,
        &SandboxSeed::new("failed", "k8s").on_node(node_id),
    )
    .await?;
    let unexplained = insert_upload(pool, sandbox_id, "full", "creating", node_id).await?;
    let explained = insert_upload(pool, sandbox_id, "full", "creating", node_id).await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET failure_reason = 'worker-recorded reason' WHERE id = $1"
    ))
    .bind(explained)
    .execute(pool)
    .await?;
    let disk = insert_upload(pool, sandbox_id, "disk", "creating", node_id).await?;

    assert_eq!(
        sink.fail_inflight_snapshots_on_gone_node(node_id, Some(UID))
            .await?,
        2
    );

    let rows: Vec<(SnapshotId, String, Option<String>)> = sqlx::query_as(sql!(
        "SELECT id, state, failure_reason FROM snapshots WHERE sandbox_id = $1 ORDER BY id"
    ))
    .bind(sandbox_id)
    .fetch_all(pool)
    .await?;
    let mut expected = vec![
        (
            unexplained,
            "failed".to_owned(),
            Some(GONE_NODE_UPLOAD_FAILURE_REASON.to_owned()),
        ),
        (
            explained,
            "failed".to_owned(),
            Some("worker-recorded reason".to_owned()),
        ),
        (disk, "creating".to_owned(), None),
    ];
    expected.sort();
    assert_eq!(rows, expected);
    Ok(())
}

/// The per-sandbox mark-lost refuses a row targeted at another node, and
/// refuses while an upload is in flight from any node, not only from the gone
/// one. The control row, targeted at the node with no upload, flips.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_mark_lost_refuses_other_targets_and_uploads_from_any_node() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool);
    let node_id = insert_gone_draining_node(pool).await?;

    let suspended_on = |target: NodeId| {
        let mut seed = SandboxSeed::new("suspended", "k8s");
        seed.target_node_id = Some(target);
        seed
    };

    let targeted_elsewhere = SandboxId::new();
    insert_sandbox(pool, targeted_elsewhere, &suspended_on(NodeId::new())).await?;

    let uploading_elsewhere = SandboxId::new();
    insert_sandbox(pool, uploading_elsewhere, &suspended_on(node_id)).await?;
    insert_upload(pool, uploading_elsewhere, "full", "creating", NodeId::new()).await?;

    let control = SandboxId::new();
    insert_sandbox(pool, control, &suspended_on(node_id)).await?;

    let mut outcomes = Vec::new();
    for sandbox_id in [targeted_elsewhere, uploading_elsewhere, control] {
        let flipped = sink
            .mark_sandbox_lost_on_gone_node(sandbox_id, node_id, Some(UID))
            .await?;
        outcomes.push((flipped, bookkeeping(pool, sandbox_id).await?.lifecycle));
    }
    assert_eq!(
        outcomes,
        vec![
            (0, "suspended".to_owned()),
            (0, "suspended".to_owned()),
            (1, "failed".to_owned()),
        ]
    );
    Ok(())
}

/// The per-sandbox adopt waits out the settle window and any upload in
/// flight, refuses a row targeted at another node, and, when it lands, drops
/// the local artifact, the evacuation marker and the placement clock while
/// the row stays `suspended`.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_adopt_waits_for_settle_and_uploads_and_drops_the_local_pin() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool);
    let node_id = insert_gone_draining_node(pool).await?;
    let settle = PositiveDuration::new_panic(std::time::Duration::from_secs(600));

    // Suspended for `age_secs`, targeted at `target`, its pinned artifact
    // backed up ready since.
    let adoptable = |age_secs: i64, target: NodeId| async move {
        let sandbox_id = SandboxId::new();
        let mut seed = SandboxSeed::new("suspended", "k8s").marked("started");
        seed.target_node_id = Some(target);
        seed.lifecycle_age_secs = age_secs;
        insert_sandbox(pool, sandbox_id, &seed).await?;
        pin_local_artifact(pool, sandbox_id).await?;
        insert_backup_of_pinned_artifact(pool, sandbox_id, "ready", target).await?;
        TestResult::Ok(sandbox_id)
    };

    let unsettled = adoptable(60, node_id).await?;
    let uploading = adoptable(3600, node_id).await?;
    insert_backup_of_pinned_artifact(pool, uploading, "creating", NodeId::new()).await?;
    let targeted_elsewhere = adoptable(3600, NodeId::new()).await?;
    let control = adoptable(3600, node_id).await?;

    let mut flips = Vec::new();
    for sandbox_id in [unsettled, uploading, targeted_elsewhere, control] {
        flips.push(
            sink.make_sandbox_durable_on_gone_node(sandbox_id, node_id, settle, Some(UID))
                .await?,
        );
    }
    assert_eq!(flips, vec![0, 0, 0, 1]);

    let adopted: (
        String,
        Option<Uuid>,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
    ) = sqlx::query_as(sql!(
        "SELECT lifecycle, current_local_artifact_id, current_local_artifact_kind,
                    current_local_artifact_origin, evacuation_restore_target,
                    placement_changed_at IS NULL
               FROM sandboxes WHERE id = $1"
    ))
    .bind(control)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        adopted,
        ("suspended".to_owned(), None, None, None, None, true)
    );
    for refused in [unsettled, uploading, targeted_elsewhere] {
        let pinned: bool = sqlx::query_scalar(sql!(
            "SELECT current_local_artifact_id IS NOT NULL FROM sandboxes WHERE id = $1"
        ))
        .bind(refused)
        .fetch_one(pool)
        .await?;
        assert!(pinned, "{refused}");
    }
    Ok(())
}

/// The per-sandbox adopt lands only on a `suspended` row: every other
/// lifecycle, with the same adoptable backup, is refused and keeps its pin.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_adopt_lands_only_on_suspended_rows() -> TestResult {
    let db = pg_test_db().await?;
    let pool = &db.pool;
    let sink = sink(pool);
    let node_id = insert_gone_draining_node(pool).await?;
    let settle = PositiveDuration::new_panic(std::time::Duration::from_secs(600));

    let mut mismatches = Vec::new();
    for lifecycle in SandboxLifecycle::all() {
        let sandbox_id = SandboxId::new();
        let mut seed = SandboxSeed::new(lifecycle.as_str(), "k8s");
        seed.target_node_id = Some(node_id);
        insert_sandbox(pool, sandbox_id, &seed).await?;
        pin_local_artifact(pool, sandbox_id).await?;
        insert_backup_of_pinned_artifact(pool, sandbox_id, "ready", node_id).await?;

        let flipped = sink
            .make_sandbox_durable_on_gone_node(sandbox_id, node_id, settle, Some(UID))
            .await?;
        let (stored, pinned): (String, bool) = sqlx::query_as(sql!(
            "SELECT lifecycle, current_local_artifact_id IS NOT NULL
               FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?;
        let accepts = *lifecycle == SandboxLifecycle::Suspended;
        if flipped != u64::from(accepts) || stored != lifecycle.as_str() || pinned == accepts {
            mismatches.push(format!(
                "{lifecycle:?}: flipped {flipped}, lifecycle {stored}, pinned {pinned}"
            ));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}
