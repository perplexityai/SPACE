//! The outcome rule over rows read from Postgres, so each fact is the shared
//! predicate's answer rather than a hand-set flag.

use pplx_space_common_core::types::{NodeId, SandboxId, SubstrateKind};

use std::collections::BTreeMap;

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, try_pg_test_db};
use pplx_space_sandbox_store::dead_node::rows_on_node;
use pplx_space_sandbox_store::lost::DEAD_UPLOAD_SETTLE;
use pplx_space_sandbox_store::test_support::NON_CANONICAL_ROWS;
use pplx_space_test_env::TestResult;
use sqlx::PgPool;
use uuid::Uuid;

use super::*;

const DEAD_NAME: &str = "worker-dead.synthetic.internal";
const LIVE_NAME: &str = "worker-live.synthetic.internal";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Snapshot {
    /// A durable full snapshot taken after the row's last placement: the
    /// portability predicate covers it.
    Covered,
    /// A durable full snapshot from before the last placement: ready, not
    /// covering.
    Uncovered,
    None,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pin {
    Active,
    Target,
    Both,
}

async fn insert_node(pool: &PgPool, name: &str) -> TestResult<NodeId> {
    let node_id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by)
         VALUES ($1, '10.0.0.9'::INET, 'worker', 'k8s')"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(name)
    .bind(format!("uid-{name}"))
    .execute(pool)
    .await?;
    Ok(node_id)
}

/// A row last placed two hours ago, its lifecycle changed an hour ago.
async fn insert_row(
    pool: &PgPool,
    lifecycle: &str,
    managed_by: SubstrateKind,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult<SandboxId> {
    let id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             lifecycle_changed_at, placement_changed_at)
         VALUES ($1, $2::UUID, $3::UUID, $4, $5,
                 CASE WHEN $6::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END,
                 $6, $7, 1, NOW() - INTERVAL '1 hour', NOW() - INTERVAL '2 hours')"
    ))
    .bind(id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(managed_by)
    .bind(lifecycle)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(id)
}

async fn insert_pinned(
    pool: &PgPool,
    lifecycle: &str,
    pin: Pin,
    node: NodeId,
) -> TestResult<SandboxId> {
    let (active, target) = match pin {
        Pin::Active => (Some(node), None),
        Pin::Target => (None, Some(node)),
        Pin::Both => (Some(node), Some(node)),
    };
    insert_row(pool, lifecycle, SubstrateKind::Kubernetes, active, target).await
}

/// One snapshot row. `age_secs` sets `timestamp`; `attempt_age_secs`, when
/// given, sets `last_attempt_at`. A `ready` one gets a storage location.
#[derive(Debug, Clone, Copy)]
struct SnapshotSeed {
    kind: &'static str,
    state: &'static str,
    source_node: Option<NodeId>,
    age_secs: i64,
    attempt_age_secs: Option<i64>,
    artifact: Option<(Uuid, &'static str)>,
}

impl SnapshotSeed {
    fn full(state: &'static str, age_secs: i64) -> Self {
        Self {
            kind: "full",
            state,
            source_node: None,
            age_secs,
            attempt_age_secs: None,
            artifact: None,
        }
    }
}

async fn insert_snapshot(pool: &PgPool, sandbox_id: SandboxId, seed: SnapshotSeed) -> TestResult {
    let snapshot_id = Uuid::now_v7();
    let (artifact_id, artifact_origin) = match seed.artifact {
        Some((id, origin)) => (Some(id), Some(origin)),
        None => (None, None),
    };
    sqlx::query(sql!(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, source_node_id, timestamp,
             last_attempt_at, local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3::UUID, $4, $5, $6,
                 NOW() - make_interval(secs => $7::BIGINT),
                 NOW() - make_interval(secs => $8::BIGINT),
                 $9, CASE WHEN $9::UUID IS NULL THEN NULL ELSE $4 END, $10)"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .bind(seed.kind)
    .bind(seed.state)
    .bind(seed.source_node)
    .bind(seed.age_secs)
    .bind(seed.attempt_age_secs)
    .bind(artifact_id)
    .bind(artifact_origin)
    .execute(pool)
    .await?;
    if seed.state == "ready" {
        sqlx::query(sql!(
            "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(snapshot_id)
        .bind(format!("s3://synthetic-bucket/{snapshot_id}"))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn seed_snapshot(pool: &PgPool, sandbox_id: SandboxId, snapshot: Snapshot) -> TestResult {
    match snapshot {
        Snapshot::Covered => {
            insert_snapshot(pool, sandbox_id, SnapshotSeed::full("ready", 1800)).await
        }
        Snapshot::Uncovered => {
            insert_snapshot(pool, sandbox_id, SnapshotSeed::full("ready", 3 * 3600)).await
        }
        Snapshot::None => Ok(()),
    }
}

/// A `creating` full upload from `node` that started just now.
async fn seed_pending_upload(pool: &PgPool, sandbox_id: SandboxId, node: NodeId) -> TestResult {
    insert_snapshot(
        pool,
        sandbox_id,
        SnapshotSeed {
            source_node: Some(node),
            ..SnapshotSeed::full("creating", 0)
        },
    )
    .await
}

/// An execution of the sandbox on `node_name`; `None` is a pod not scheduled
/// yet.
async fn insert_execution(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_name: Option<&str>,
    active: bool,
) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, k8s_node_name, state, active,
             terminated_at)
         VALUES ($1, 1, $2, 'sandboxes', $3, 'running', $4,
                 CASE WHEN $4 THEN NULL ELSE NOW() END)"
    ))
    .bind(sandbox_id)
    .bind(format!("space-sbx-{}", sandbox_id.as_uuid().simple()))
    .bind(node_name)
    .bind(active)
    .execute(pool)
    .await?;
    Ok(())
}

async fn outcomes_on(
    pool: &PgPool,
    node: NodeId,
    name: &str,
) -> TestResult<BTreeMap<SandboxId, RowOutcome>> {
    let rows = rows_on_node(
        pool,
        node,
        Some(name),
        SubstrateKind::Kubernetes,
        DEAD_UPLOAD_SETTLE,
        10_000,
    )
    .await?;
    let mut outcomes = BTreeMap::new();
    for row in &rows {
        assert!(
            outcomes.insert(row.sandbox_id, row_outcome(row)).is_none(),
            "{} read twice",
            row.sandbox_id
        );
    }
    Ok(outcomes)
}

fn mark(code: &str) -> RowOutcome {
    match code {
        "FD" => RowOutcome::FinishDelete,
        "SS" => RowOutcome::StaysSuspended,
        "ST" => RowOutcome::StaysStopped,
        "QK" => RowOutcome::QuarantineKept,
        "QL" => RowOutcome::QuarantineLost,
        "LR" => RowOutcome::LostSnapshotRetained,
        "L" => RowOutcome::Lost,
        "RC" => RowOutcome::ReferencesCleared,
        "W" => RowOutcome::Wait,
        "HE" => RowOutcome::HeldByExecution,
        other => panic!("unknown mark {other:?}"),
    }
}

/// Stored lifecycle × snapshot × pending upload, as the outcome a row on a
/// lost node gets. Columns: covered snapshot, uncovered ready snapshot, no
/// snapshot, each without a pending upload; then with one, whatever the
/// snapshot. `stopped` rows here carry no stop artifact (see
/// [`stop_artifacts_and_upload_windows_decide_the_edges`]).
const MATRIX: &[(&str, [&str; 3], &str)] = &[
    ("created", ["LR", "LR", "L"], "W"),
    ("creating", ["LR", "LR", "L"], "W"),
    ("starting", ["LR", "LR", "L"], "W"),
    ("started", ["LR", "LR", "L"], "W"),
    ("restoring", ["LR", "LR", "L"], "W"),
    ("pausing", ["LR", "LR", "L"], "W"),
    ("paused", ["LR", "LR", "L"], "W"),
    ("resuming", ["LR", "LR", "L"], "W"),
    ("suspending", ["LR", "LR", "L"], "W"),
    ("suspended", ["SS", "LR", "L"], "W"),
    ("stopping", ["LR", "LR", "L"], "W"),
    ("stopped", ["LR", "LR", "L"], "W"),
    ("deleting", ["FD", "FD", "FD"], "FD"),
    ("checkpointing", ["LR", "LR", "L"], "W"),
    ("quarantined", ["QK", "QL", "QL"], "W"),
    ("failed", ["RC", "RC", "RC"], "W"),
    ("deleted", ["RC", "RC", "RC"], "W"),
    ("bogus", ["LR", "LR", "L"], "W"),
    // Parses as `suspended`, and the portability predicate has no lifecycle
    // term, so it is covered like the canonical spelling.
    (" Suspended ", ["SS", "LR", "L"], "W"),
];

/// Every stored lifecycle × {covered, uncovered ready, no snapshot} ×
/// {pending upload, none} × {active, target, both}, each with a control row
/// on a live node, gets the matrix's outcome, and no control row is read.
#[tokio::test(flavor = "current_thread")]
async fn every_stored_lifecycle_gets_the_matrix_outcome_and_live_rows_stay_out() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let labels: Vec<&str> = MATRIX.iter().map(|(label, _, _)| *label).collect();
    let vocabulary: Vec<&str> = StoredSandboxLifecycle::all_as_str()
        .chain(NON_CANONICAL_ROWS.iter().map(|(_, value)| *value))
        .collect();
    assert_eq!(labels, vocabulary, "one matrix row per stored lifecycle");

    let dead = insert_node(pool, DEAD_NAME).await?;
    let live = insert_node(pool, LIVE_NAME).await?;
    let mut expected = BTreeMap::new();
    let mut controls = Vec::new();
    for (lifecycle, by_snapshot, with_upload) in MATRIX {
        for (snapshot, without_upload) in [Snapshot::Covered, Snapshot::Uncovered, Snapshot::None]
            .into_iter()
            .zip(by_snapshot)
        {
            for upload in [false, true] {
                for pin in [Pin::Active, Pin::Target, Pin::Both] {
                    let id = insert_pinned(pool, lifecycle, pin, dead).await?;
                    seed_snapshot(pool, id, snapshot).await?;
                    if upload {
                        seed_pending_upload(pool, id, dead).await?;
                    }
                    if pin == Pin::Both {
                        // Selected three ways, read once.
                        insert_execution(pool, id, Some(DEAD_NAME), true).await?;
                    }
                    let outcome = mark(if upload { with_upload } else { without_upload });
                    expected.insert(id, outcome);

                    let control = insert_pinned(pool, lifecycle, pin, live).await?;
                    seed_snapshot(pool, control, snapshot).await?;
                    if upload {
                        seed_pending_upload(pool, control, live).await?;
                    }
                    insert_execution(pool, control, Some(LIVE_NAME), true).await?;
                    controls.push(control);
                }
            }
        }
    }

    let outcomes = outcomes_on(pool, dead, DEAD_NAME).await?;
    for control in &controls {
        assert!(!outcomes.contains_key(control), "control {control} read");
    }
    let mismatches: Vec<_> = expected
        .iter()
        .filter(|(id, outcome)| outcomes.get(id) != Some(outcome))
        .map(|(id, outcome)| (*id, *outcome, outcomes.get(id).copied()))
        .collect();
    assert!(mismatches.is_empty(), "(id, expected, got): {mismatches:?}");
    assert_eq!(outcomes.len(), expected.len());
    Ok(())
}

/// The selection beyond pins: an active execution on the node's name selects
/// an unpinned row, while an inactive one, one on another node, and a row of
/// the other substrate pinned to the node do not.
#[tokio::test(flavor = "current_thread")]
async fn active_executions_on_the_node_select_rows_and_nothing_else_does() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let dead = insert_node(pool, DEAD_NAME).await?;
    let live = insert_node(pool, LIVE_NAME).await?;

    let running_here = insert_row(pool, "started", SubstrateKind::Kubernetes, None, None).await?;
    insert_execution(pool, running_here, Some(DEAD_NAME), true).await?;
    let ran_here = insert_row(pool, "started", SubstrateKind::Kubernetes, None, None).await?;
    insert_execution(pool, ran_here, Some(DEAD_NAME), false).await?;
    let running_elsewhere = insert_row(
        pool,
        "started",
        SubstrateKind::Kubernetes,
        Some(live),
        Some(live),
    )
    .await?;
    insert_execution(pool, running_elsewhere, Some(LIVE_NAME), true).await?;
    let other_substrate = insert_row(
        pool,
        "started",
        SubstrateKind::Space,
        Some(dead),
        Some(dead),
    )
    .await?;

    let outcomes = outcomes_on(pool, dead, DEAD_NAME).await?;
    assert_eq!(
        outcomes,
        BTreeMap::from([(running_here, RowOutcome::Lost)]),
        "ran_here {ran_here}, running_elsewhere {running_elsewhere}, \
         other_substrate {other_substrate}"
    );
    Ok(())
}

/// A row an active execution off the node owns, whether its pod is scheduled
/// on another node or not yet, is held by it; an execution on the node, an
/// inactive one elsewhere, or none leaves the row its lifecycle's outcome.
#[tokio::test(flavor = "current_thread")]
async fn executions_off_the_node_hold_their_rows() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let dead = insert_node(pool, DEAD_NAME).await?;

    let mut expected = BTreeMap::new();
    for (execution, outcome) in [
        // A restore whose pod is not scheduled yet.
        (Some((None, true)), RowOutcome::HeldByExecution),
        // A restore placed on another node.
        (Some((Some(LIVE_NAME), true)), RowOutcome::HeldByExecution),
        // Controls: only an active execution off the node holds the row.
        (Some((Some(DEAD_NAME), true)), RowOutcome::Lost),
        (Some((Some(LIVE_NAME), false)), RowOutcome::Lost),
        (None, RowOutcome::Lost),
    ] {
        let id = insert_pinned(pool, "restoring", Pin::Target, dead).await?;
        if let Some((node_name, active)) = execution {
            insert_execution(pool, id, node_name, active).await?;
        }
        expected.insert(id, outcome);
    }

    assert_eq!(outcomes_on(pool, dead, DEAD_NAME).await?, expected);
    Ok(())
}

/// `stopped` stays only with its pinned stop artifact durable; an upload from
/// this node makes a row wait only inside the quiet window, by its last
/// attempt, and one from another node or no recorded node only while it is
/// the row's current suspend.
#[tokio::test(flavor = "current_thread")]
async fn stop_artifacts_and_upload_windows_decide_the_edges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let dead = insert_node(pool, DEAD_NAME).await?;
    let live = insert_node(pool, LIVE_NAME).await?;
    let quiet = DEAD_UPLOAD_SETTLE.as_secs().cast_signed() + 60;

    let mut expected = BTreeMap::new();
    for (origin, state, outcome) in [
        ("stop", "ready", RowOutcome::StaysStopped),
        // The pinned artifact is not durable yet.
        ("stop", "creating", RowOutcome::Lost),
        // A durable suspend artifact is not the stopped instance's disk.
        ("suspend", "ready", RowOutcome::LostSnapshotRetained),
    ] {
        let id = insert_pinned(pool, "stopped", Pin::Both, dead).await?;
        let artifact = Uuid::now_v7();
        sqlx::query(sql!(
            "UPDATE sandboxes
                SET current_local_artifact_id = $2,
                    current_local_artifact_kind = 'disk',
                    current_local_artifact_origin = $3
              WHERE id = $1"
        ))
        .bind(id)
        .bind(artifact)
        .bind(origin)
        .execute(pool)
        .await?;
        insert_snapshot(
            pool,
            id,
            SnapshotSeed {
                kind: "disk",
                artifact: Some((artifact, origin)),
                ..SnapshotSeed::full(state, 600)
            },
        )
        .await?;
        expected.insert(id, outcome);
    }

    // `insert_row` places every row two hours ago.
    let before_placement = 3 * 3600;
    for (source, age, attempt_age, outcome) in [
        // Fresh, but from another node: that node may finish it.
        (Some(live), 0, None, RowOutcome::Wait),
        // From no recorded node: it can't be shown to have died with this one.
        (None, 0, None, RowOutcome::Wait),
        // From another node, but before the row's last placement on a row
        // with no artifact marker: a prior cycle's remnant.
        (Some(live), before_placement, None, RowOutcome::Lost),
        // From this node, quiet past the window.
        (Some(dead), quiet, None, RowOutcome::Lost),
        // Created long ago, but attempted inside the window.
        (Some(dead), quiet, Some(0), RowOutcome::Wait),
    ] {
        let id = insert_pinned(pool, "started", Pin::Active, dead).await?;
        insert_snapshot(
            pool,
            id,
            SnapshotSeed {
                source_node: source,
                attempt_age_secs: attempt_age,
                ..SnapshotSeed::full("creating", age)
            },
        )
        .await?;
        expected.insert(id, outcome);
    }

    assert_eq!(outcomes_on(pool, dead, DEAD_NAME).await?, expected);
    Ok(())
}
