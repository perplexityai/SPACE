use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId, SubstrateKind};

use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;
use uuid::Uuid;

use super::{
    DEAD_UPLOAD_SETTLE, DrainedNodeTerminatorSink, NodeDbStore, OrphanDeleteOutcome,
    OrphanDeleteRefusal, PHASE_DRAINED, PHASE_TERMINATING, ResidencyEvidence,
    delete_drained_node_if_orphan_query, local_snapshots_count_query, mark_terminating_query,
    mark_unrecoverable_sandboxes_lost_query,
};

use pplx_space_test_env::TestResult;

/// The `node_k8s.k8s_node_uid` `insert_drained_node` registers for a node —
/// the identity the KubeNodeGone reap's `$2` guard checks against.
fn node_uid(node_id: NodeId) -> String {
    format!("uid-{node_id}")
}

async fn insert_drained_node(pool: &PgPool, node_id: NodeId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.5'::inet, 'worker', 'k8s', 'registered', 'drained', NOW())"
    ))
    .bind(node_id)
    .execute(pool)
    .await?;
    // The reap's identity guard requires a node_k8s row; register one with
    // a deterministic uid so the delete test can bind a matching (or
    // deliberately mismatched) `$2`.
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(node_id)
    .bind(format!("ip-test-{node_id}"))
    .bind(node_uid(node_id))
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_suspended_sandbox_with_creating_snapshot(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult {
    insert_suspended_with_creating_upload_from(pool, node_id, node_id).await?;
    Ok(())
}

/// `None` means either the row is gone (the recovery delete reaped it) or
/// it exists with a NULL `drain_phase`. `fetch_optional` + `flatten` is
/// required: a reaped row returns zero rows, which `fetch_one` would
/// surface as `RowNotFound` rather than `Ok(None)`. The delete tests only
/// ever assert `None` for the reaped-row case, so the two readings don't
/// collide here.
async fn drain_phase(pool: &PgPool, node_id: NodeId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT drain_phase FROM nodes WHERE id = $1"
        ))
        .bind(node_id)
        .fetch_optional(pool)
        .await?
        .flatten(),
    )
}

/// An in-flight upload from the node both counts as a local snapshot and
/// blocks the atomic `drained → terminating` flip; once no upload is in
/// flight the flip proceeds.
#[tokio::test(flavor = "current_thread")]
async fn inflight_upload_blocks_terminate_until_finalized() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    insert_suspended_sandbox_with_creating_snapshot(pool, node).await?;

    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(local_snapshots_count_query())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        count, 1,
        "in-flight upload must count as a blocking snapshot"
    );

    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(flipped, 0, "must not terminate while upload is in flight");
    assert_eq!(
        drain_phase(pool, node).await?.as_deref(),
        Some(PHASE_DRAINED)
    );

    // Finalize the upload (creating → ready + storage location), then the
    // flip proceeds.
    sqlx::query(sql!(
        "UPDATE snapshots SET state = 'ready' WHERE source_node_id = $1"
    ))
    .bind(node)
    .execute(pool)
    .await?;
    let snapshot_id: SnapshotId =
        sqlx::query_scalar(sql!("SELECT id FROM snapshots WHERE source_node_id = $1"))
            .bind(node)
            .fetch_one(pool)
            .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;

    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(
        flipped, 1,
        "terminate proceeds once the snapshot is durable"
    );
    assert_eq!(
        drain_phase(pool, node).await?.as_deref(),
        Some(PHASE_TERMINATING)
    );
    Ok(())
}

async fn insert_restoring_sandbox(
    pool: &PgPool,
    node_id: NodeId,
    evacuation_marker: Option<&str>,
) -> TestResult {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation,
             evacuation_restore_target
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'restoring', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1, $3
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(evacuation_marker)
    .execute(pool)
    .await?;
    Ok(())
}

/// An **evacuation** restore (`restoring` with the marker set, target still
/// equal to the source) must NOT block `drained → terminating`: it reads
/// from the durable S3 snapshot, so terminating the source loses nothing.
/// A non-evacuation `restoring` row (NULL marker) still blocks. Mirrors
/// `node_drain::store`'s `evacuation_restoring_does_not_recouple_source_drain`
/// so the two guards stay in lockstep.
#[tokio::test(flavor = "current_thread")]
async fn evacuation_restoring_does_not_block_terminate() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // Node A: only an evacuation restore in flight → must not block.
    let node_a = NodeId::new();
    insert_drained_node(pool, node_a).await?;
    insert_restoring_sandbox(pool, node_a, Some("started")).await?;
    let count_a: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(local_snapshots_count_query())))
        .bind(node_a)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        count_a, 0,
        "an evacuation restore must not block termination"
    );
    let flipped_a = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node_a)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(
        flipped_a, 1,
        "terminate proceeds past an evacuation restore"
    );

    // Node B: a non-evacuation restore (NULL marker) still blocks.
    let node_b = NodeId::new();
    insert_drained_node(pool, node_b).await?;
    insert_restoring_sandbox(pool, node_b, None).await?;
    let count_b: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(local_snapshots_count_query())))
        .bind(node_b)
        .fetch_one(pool)
        .await?;
    assert_eq!(
        count_b, 1,
        "a non-evacuation restore still blocks termination"
    );
    let flipped_b = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node_b)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(
        flipped_b, 0,
        "must not terminate past a non-evacuation restore"
    );
    Ok(())
}

/// Regression (mirrors `node_drain::store`'s
/// `drain_blocks_marked_sandbox_until_current_upload_ready`): a marked,
/// suspended sandbox whose **current** evacuation upload is still
/// `creating` must keep blocking `drained → terminating` even when an older
/// placement-fresh `ready` snapshot exists — otherwise the node terminates
/// and kills the only in-flight copy, stranding the sandbox (the cohort
/// restore refuses the stale prior snapshot). The shared
/// `snapshot_durable_for_source_release` predicate gates on the newest
/// snapshot being ready for marked rows, so the terminator releases on
/// exactly the condition the restore waits on.
#[tokio::test(flavor = "current_thread")]
async fn terminate_blocks_marked_sandbox_until_current_upload_ready() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_drained_node(pool, node).await?;

    let sandbox = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation,
             evacuation_restore_target
         )
         VALUES (
             $1, '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1, 'started'
         )"
    ))
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    // Older, placement-fresh ready snapshot (would look portable to the old
    // freshness gate) + the current upload still creating (newest full).
    let stale_ready = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW() - make_interval(secs => 1800))"
    ))
    .bind(stale_ready)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/older-ready')"
    ))
    .bind(stale_ready)
    .execute(pool)
    .await?;
    let current = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(current)
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;

    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(local_snapshots_count_query())))
        .bind(node)
        .fetch_one(pool)
        .await?;
    assert_eq!(count, 1, "current upload in flight must block terminate");
    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(
        flipped, 0,
        "must not terminate while the current upload is creating"
    );

    // Finalize the current upload → newest is ready → terminate proceeds.
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(current)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/current-upload')"
    ))
    .bind(current)
    .execute(pool)
    .await?;
    let flipped = sqlx::query(sqlx::AssertSqlSafe(sql!(mark_terminating_query(
        ResidencyEvidence::default()
    ))))
    .bind(node)
    .bind(PHASE_TERMINATING)
    .bind(PHASE_DRAINED)
    .execute(pool)
    .await?
    .rows_affected();
    assert_eq!(
        flipped, 1,
        "terminate proceeds once the current upload is ready"
    );
    assert_eq!(
        drain_phase(pool, node).await?.as_deref(),
        Some(PHASE_TERMINATING)
    );
    Ok(())
}

async fn insert_active_sandbox(pool: &PgPool, node_id: NodeId, lifecycle: &str) -> TestResult {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             active_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             $3, 'placed', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .bind(lifecycle)
    .execute(pool)
    .await?;
    Ok(())
}

/// Insert a `suspended` sandbox pinned to `node_id` via `target_node_id`
/// whose newest full snapshot is `ready` and has an S3 storage location —
/// i.e. durably restorable. Such a sandbox drops out of
/// `snapshot_blocking_predicate`, so it must NOT block termination/reaping.
async fn insert_suspended_with_durable_snapshot(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult<SandboxId> {
    let sandbox_id = insert_suspended_without_snapshots(pool, node_id).await?;
    let snapshot_id = SnapshotId::new();
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'ready', $3, NOW())"
    ))
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(snapshot_id)
    .bind(format!("s3://test-bucket/{snapshot_id}"))
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The KubeNodeGone recovery delete reaps a clean `drained` row, refuses
/// while a genuinely-resident active sandbox or a local-only suspended
/// snapshot still pins the node, lets a durably-portable suspended sandbox
/// through (the regression codex flagged — same source-release rule as the
/// normal drain path), refuses when the row is not in `drained`, and
/// refuses when the observed kube uid no longer matches (a same-name kube
/// Node replacement landed after the EC2 cross-check).
#[tokio::test(flavor = "current_thread")]
async fn delete_drained_orphan_guards() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let query = delete_drained_node_if_orphan_query(ResidencyEvidence::default());

    // Clean drained row, no sandbox references, matching uid → deleted.
    let clean = NodeId::new();
    insert_drained_node(pool, clean).await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(clean)
        .bind(Some(node_uid(clean)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(deleted, 1, "a clean drained orphan must be reaped");
    assert_eq!(drain_phase(pool, clean).await?, None, "row must be gone");

    // Drained row with a genuinely-resident active sandbox (active_node_id,
    // non-suspended) → refused by the resident guard.
    let pinned = NodeId::new();
    insert_drained_node(pool, pinned).await?;
    insert_active_sandbox(pool, pinned, "started").await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(pinned)
        .bind(Some(node_uid(pinned)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        deleted, 0,
        "must not reap a node a resident active sandbox still references"
    );
    assert_eq!(
        drain_phase(pool, pinned).await?.as_deref(),
        Some(PHASE_DRAINED)
    );

    // Drained row with a local-only suspended sandbox (in-flight `creating`
    // snapshot pinned via target_node_id) → refused by the snapshot guard.
    let local_only = NodeId::new();
    insert_drained_node(pool, local_only).await?;
    insert_suspended_sandbox_with_creating_snapshot(pool, local_only).await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(local_only)
        .bind(Some(node_uid(local_only)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        deleted, 0,
        "must not reap a node whose suspended snapshot is still local-only"
    );
    assert_eq!(
        drain_phase(pool, local_only).await?.as_deref(),
        Some(PHASE_DRAINED)
    );

    // Drained row with a durably-portable suspended sandbox (ready + S3
    // snapshot, target_node_id pin lingers) → reaped. This is the codex
    // regression: the prior guard parked such rows forever.
    let durable = NodeId::new();
    insert_drained_node(pool, durable).await?;
    insert_suspended_with_durable_snapshot(pool, durable).await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(durable)
        .bind(Some(node_uid(durable)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        deleted, 1,
        "a durably-portable suspended sandbox must not block the reap"
    );
    assert_eq!(drain_phase(pool, durable).await?, None, "row must be gone");

    // A terminal (deleted) sandbox reference does NOT block the reap.
    let terminal_ref = NodeId::new();
    insert_drained_node(pool, terminal_ref).await?;
    insert_active_sandbox(pool, terminal_ref, "deleted").await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(terminal_ref)
        .bind(Some(node_uid(terminal_ref)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        deleted, 1,
        "a terminal sandbox reference must not block the reap"
    );

    // Identity guard: a same-name kube Node replacement bumped
    // node_k8s.k8s_node_uid since the EC2 cross-check observed it → the
    // reap must refuse (the row now belongs to the fresh node).
    let replaced = NodeId::new();
    insert_drained_node(pool, replaced).await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(replaced)
        .bind(Some("stale-observed-uid".to_string()))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(
        deleted, 0,
        "must not reap when the observed kube uid no longer matches node_k8s"
    );
    assert_eq!(
        drain_phase(pool, replaced).await?.as_deref(),
        Some(PHASE_DRAINED)
    );

    // A row not in `drained` (e.g. already terminating) → refused by the
    // drain_phase guard (node_k8s present + matching uid, so the refusal is
    // unambiguously the phase, not the identity guard).
    let terminating = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.6'::inet, 'worker', 'k8s', 'registered', 'terminating', NOW())"
    ))
    .bind(terminating)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(terminating)
    .bind(format!("ip-test-{terminating}"))
    .bind(node_uid(terminating))
    .execute(pool)
    .await?;
    let deleted = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(terminating)
        .bind(Some(node_uid(terminating)))
        .bind(SubstrateKind::Kubernetes)
        .execute(pool)
        .await?
        .rows_affected();
    assert_eq!(deleted, 0, "only `drained` rows are eligible for this reap");
    assert_eq!(
        drain_phase(pool, terminating).await?.as_deref(),
        Some(PHASE_TERMINATING)
    );

    Ok(())
}

/// Insert a `suspended` sandbox pinned to `node_id` via `target_node_id`
/// with **zero** snapshot rows ever — the legacy shape that motivated the
/// gone-node mark-lost give-up: its only copy was the local artifact on
/// the node, so it can never become portable.
async fn insert_suspended_without_snapshots(
    pool: &PgPool,
    node_id: NodeId,
) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, lifecycle, placement, managed_by,
             target_node_id, placement_changed_at, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'suspended', 'unplaced', 'k8s',
             $2, NOW() - INTERVAL '1 hour', 1
         )"
    ))
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Insert a `suspended` sandbox pinned to `target_node_id` whose suspend
/// upload is still `creating` from `source_node_id` (which may differ
/// from the pin — the in-flight-from-*which*-node distinction the
/// mark-lost veto keys on).
async fn insert_suspended_with_creating_upload_from(
    pool: &PgPool,
    target_node_id: NodeId,
    source_node_id: NodeId,
) -> TestResult<SandboxId> {
    let sandbox_id = insert_suspended_without_snapshots(pool, target_node_id).await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(source_node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// Backdate a sandbox's snapshot heartbeats past [`DEAD_UPLOAD_SETTLE`]
/// so the settle guard treats its `creating` rows as provably dead.
async fn age_snapshots_past_dead_upload_settle(pool: &PgPool, sandbox_id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE snapshots
            SET timestamp = NOW() - make_interval(secs => $2),
                last_attempt_at = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox_id)
    .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64 + 60)
    .execute(pool)
    .await?;
    Ok(())
}

async fn lifecycle(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar::<_, String>(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id)
            .fetch_one(pool)
            .await?,
    )
}

/// The gone-node mark-lost CAS flips exactly the provably-unrecoverable
/// pinned suspended sandboxes and nothing else: a zero-snapshot legacy row
/// and a row whose only `creating` upload is from the (terminated) node
/// itself are marked; a durably-portable row and a row with an upload
/// still in flight from a *different* (live) node are spared; a stale
/// observed kube uid or a non-`drained` phase makes the whole UPDATE
/// match nothing.
#[tokio::test(flavor = "current_thread")]
async fn mark_unrecoverable_sandboxes_lost_guards() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let query = mark_unrecoverable_sandboxes_lost_query();

    // One gone node carrying all four sandbox shapes at once, so the
    // RETURNING set must discriminate between them.
    let node = NodeId::new();
    let live_node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let lost_zero_snapshots = insert_suspended_without_snapshots(pool, node).await?;
    let lost_dead_upload = insert_suspended_with_creating_upload_from(pool, node, node).await?;
    // Quiet past the settle window — provably dead, eligible for the flip.
    age_snapshots_past_dead_upload_settle(pool, lost_dead_upload).await?;
    // Suspended with a ready + S3-registered snapshot: durably portable,
    // so the give-up must spare it.
    let spared_durable = insert_suspended_with_durable_snapshot(pool, node).await?;
    let spared_live_upload =
        insert_suspended_with_creating_upload_from(pool, node, live_node).await?;

    let all_candidates = vec![
        lost_zero_snapshots,
        lost_dead_upload,
        spared_durable,
        spared_live_upload,
    ];

    // Stale observed uid → identity guard refuses the whole flip.
    let marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(Some("stale-observed-uid".to_string()))
        .bind(&all_candidates)
        .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?;
    assert!(
        marked.is_empty(),
        "a stale observed kube uid must refuse the mark-lost flip"
    );

    // Matching uid → exactly the two unrecoverable rows flip.
    let mut marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(Some(node_uid(node)))
        .bind(&all_candidates)
        .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?;
    marked.sort();
    let mut expected = vec![lost_zero_snapshots, lost_dead_upload];
    expected.sort();
    assert_eq!(
        marked, expected,
        "exactly the zero-snapshot row and the dead-upload row are unrecoverable"
    );
    assert_eq!(lifecycle(pool, lost_zero_snapshots).await?, "failed");
    assert_eq!(lifecycle(pool, lost_dead_upload).await?, "failed");
    assert_eq!(
        lifecycle(pool, spared_durable).await?,
        "suspended",
        "a durably-portable suspended sandbox must not be marked lost"
    );
    assert_eq!(
        lifecycle(pool, spared_live_upload).await?,
        "suspended",
        "an upload still in flight from a live node must veto the flip"
    );

    // Claim restriction: a row matching the broad predicate but absent
    // from the caller's `$3` claim set must not flip — under READ
    // COMMITTED the claim only fences rows that matched at its own
    // snapshot, so a late arrival flipping un-fenced would re-open the
    // FK-lock race with a concurrent BeginArtifactBackup insert. It
    // waits for the next tick's claim instead.
    let late_arrival = insert_suspended_without_snapshots(pool, node).await?;
    let marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(Some(node_uid(node)))
        .bind(vec![spared_durable])
        .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?;
    assert!(
        marked.is_empty(),
        "an unclaimed candidate must not flip even when it matches the predicate"
    );
    assert_eq!(
        lifecycle(pool, late_arrival).await?,
        "suspended",
        "the unclaimed late arrival waits for the next tick's claim"
    );

    // A node not in `drained` (e.g. terminating) → phase guard refuses
    // even for an otherwise-unrecoverable sandbox.
    let terminating = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.7'::inet, 'worker', 'k8s', 'registered', 'terminating', NOW())"
    ))
    .bind(terminating)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(terminating)
    .bind(format!("ip-test-{terminating}"))
    .bind(node_uid(terminating))
    .execute(pool)
    .await?;
    let parked = insert_suspended_without_snapshots(pool, terminating).await?;
    let marked: Vec<SandboxId> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(terminating)
        .bind(Some(node_uid(terminating)))
        .bind(vec![parked])
        .bind(DEAD_UPLOAD_SETTLE.as_secs() as i64)
        .bind(SubstrateKind::Kubernetes)
        .fetch_all(pool)
        .await?;
    assert!(
        marked.is_empty(),
        "only `drained` nodes are eligible for the gone-node give-up"
    );
    assert_eq!(lifecycle(pool, parked).await?, "suspended");

    Ok(())
}

/// The post-refusal diagnosis names the guard(s) that refused: each
/// refusal shape produces exactly its own flags/counts through the real
/// [`NodeDbStore`] sink.
#[tokio::test(flavor = "current_thread")]
async fn delete_refusal_diagnostics_classify_guards() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    // Resident guard: a started sandbox still pinned via active_node_id.
    let pinned = NodeId::new();
    insert_drained_node(pool, pinned).await?;
    insert_active_sandbox(pool, pinned, "started").await?;
    let outcome = sink
        .delete_drained_node_if_orphan(pinned, Some(node_uid(pinned)))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            active_residents: 1,
            ..OrphanDeleteRefusal::default()
        })
    );

    // Snapshot guard: a local-only suspended sandbox pinned via
    // target_node_id.
    let local_only = NodeId::new();
    insert_drained_node(pool, local_only).await?;
    insert_suspended_without_snapshots(pool, local_only).await?;
    let outcome = sink
        .delete_drained_node_if_orphan(local_only, Some(node_uid(local_only)))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            snapshot_blockers: 1,
            ..OrphanDeleteRefusal::default()
        })
    );

    // Identity guard: the observed uid no longer matches node_k8s.
    let replaced = NodeId::new();
    insert_drained_node(pool, replaced).await?;
    let outcome = sink
        .delete_drained_node_if_orphan(replaced, Some("stale-observed-uid".to_string()))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            uid_mismatch: true,
            ..OrphanDeleteRefusal::default()
        })
    );

    // Phase guard: a row that drifted out of `drained`.
    let terminating = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state, drain_phase, drain_started_at)
         VALUES ($1, '10.0.0.8'::inet, 'worker', 'k8s', 'registered', 'terminating', NOW())"
    ))
    .bind(terminating)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, $3)"
    ))
    .bind(terminating)
    .bind(format!("ip-test-{terminating}"))
    .bind(node_uid(terminating))
    .execute(pool)
    .await?;
    let outcome = sink
        .delete_drained_node_if_orphan(terminating, Some(node_uid(terminating)))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            phase_drifted: true,
            ..OrphanDeleteRefusal::default()
        })
    );

    // And a clean orphan still reports Deleted through the same method.
    let clean = NodeId::new();
    insert_drained_node(pool, clean).await?;
    let outcome = sink
        .delete_drained_node_if_orphan(clean, Some(node_uid(clean)))
        .await?;
    assert_eq!(outcome, OrphanDeleteOutcome::Deleted);

    Ok(())
}

/// Regression for a codex-flagged interaction with the
/// suspended-execution reaper: a node-side auto-suspended sandbox still
/// carries an `active = TRUE` execution row plus `placement = 'on_node'`
/// / `active_node_id` bookkeeping (the API-driven
/// `finish_suspended_execution` never ran for it). The reaper that
/// retires those scans `lifecycle = 'suspended'` only, so if the
/// mark-lost flip ran first and left the bookkeeping behind, the sandbox
/// would be `failed` with a phantom active execution pinned to a node
/// row the next tick hard-deletes. The flip must therefore retire the
/// stale execution (`'forgotten'`, the pod-tracking write-off state) and
/// unplace the sandbox in the same transaction.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_retires_stale_execution_and_placement() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    // The node-side auto-suspend shape: suspended, but still placed with
    // an active execution (no finish_suspended_execution ever ran).
    let sandbox = insert_suspended_without_snapshots(pool, node).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement = 'on_node', active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox)
    .bind(node)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, active)
         VALUES ($1, 1, $2, 'space-sandboxes', 'running', TRUE)"
    ))
    .bind(sandbox)
    .bind(format!("sbx-test-{sandbox}"))
    .execute(pool)
    .await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    assert_eq!(lifecycle(pool, sandbox).await?, "failed");

    let (active, state, terminated): (bool, String, bool) = sqlx::query_as(sql!(
        "SELECT active, state, terminated_at IS NOT NULL
           FROM sandbox_k8s_executions WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert!(
        !active,
        "the stale execution must be retired in the same transaction"
    );
    assert_eq!(state, "forgotten");
    assert!(terminated, "terminated_at must be stamped");

    let (placement, active_node_id): (String, Option<NodeId>) = sqlx::query_as(sql!(
        "SELECT placement, active_node_id FROM sandboxes WHERE id = $1"
    ))
    .bind(sandbox)
    .fetch_one(pool)
    .await?;
    assert_eq!(
        placement, "unplaced",
        "stale placement must not survive the flip"
    );
    assert_eq!(
        active_node_id, None,
        "active_node_id must not dangle at the about-to-be-deleted node row"
    );
    Ok(())
}

/// Regression for the reviewer-requested settle guard on same-node
/// `creating` uploads: EC2 `terminated` proves the upload *worker* is
/// dead, but a `FinalizeArtifactBackup(READY)` RPC that left the node
/// before it died can still be in the API path. A `creating` row whose
/// heartbeat (`COALESCE(last_attempt_at, timestamp)`) is still fresh
/// must therefore veto the flip — the sandbox stays parked until the
/// row has been quiet for the settle window, after which any in-flight
/// finalize has long since landed and the row is provably dead.
#[tokio::test(flavor = "current_thread")]
async fn fresh_same_node_creating_upload_defers_mark_lost() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    // Same-node creating upload registered moments ago (timestamp NOW,
    // no heartbeat yet): a finalize for it could still be in flight.
    let sandbox = insert_suspended_with_creating_upload_from(pool, node, node).await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(
        marked,
        Vec::<SandboxId>::new(),
        "a fresh same-node creating upload must defer the give-up"
    );
    assert_eq!(lifecycle(pool, sandbox).await?, "suspended");
    let snapshot_state: String =
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE sandbox_id = $1"))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        snapshot_state, "creating",
        "a fresh creating row must not be finalized failed"
    );

    // Once the row has been quiet past the settle window the worker's
    // death is conclusive and the give-up proceeds.
    age_snapshots_past_dead_upload_settle(pool, sandbox).await?;
    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    assert_eq!(lifecycle(pool, sandbox).await?, "failed");
    Ok(())
}

/// Regression for a codex-flagged convergence gap: marking a sandbox
/// lost while its dead `creating` upload row (sourced from the gone
/// node) survives does NOT unblock the reap — the snapshot guard's
/// in-flight arm (`inflight_suspend_upload_from_node_exists`) is
/// lifecycle-agnostic, so the now-`failed` sandbox keeps refusing the
/// delete forever, and nothing else terminalizes the row (the upload
/// worker died with the box). The give-up must finalize those same-node
/// `creating` rows to `'failed'` in the same transaction so the next
/// tick's delete converges.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_dead_upload_converges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_with_creating_upload_from(pool, node, node).await?;
    age_snapshots_past_dead_upload_settle(pool, sandbox).await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    let snapshot_state: String =
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE sandbox_id = $1"))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        snapshot_state, "failed",
        "the dead same-node creating upload must be finalized failed"
    );

    let outcome = sink
        .delete_drained_node_if_orphan(node, Some(node_uid(node)))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Deleted,
        "the reap must converge once the dead upload row is terminal"
    );
    Ok(())
}

/// Regression for a codex-flagged finalize race: the upload may have
/// durably completed with its `FinalizeArtifactBackup(READY)` still in
/// flight when the node's EC2 instance is confirmed terminated. The
/// finalizer only touches `snapshots`, so nothing in the flip's sandbox
/// row locks serializes it — without locking the same-node `creating`
/// rows first, the flip can evaluate against the pre-finalize snapshot
/// and terminally fail a sandbox that just became recoverable. With the
/// locks, a finalize that commits first is seen by the flip's durable
/// check (sandbox spared); one that arrives later blocks and then
/// refuses on its own `state IN ('creating', …)` guard.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_yields_to_concurrent_snapshot_finalize() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_with_creating_upload_from(pool, node, node).await?;
    // Aged past the settle so the quiet-period guard does not veto:
    // the lock serialization is the mechanism under test here.
    age_snapshots_past_dead_upload_settle(pool, sandbox).await?;
    let snapshot_id: SnapshotId =
        sqlx::query_scalar(sql!("SELECT id FROM snapshots WHERE sandbox_id = $1"))
            .bind(sandbox)
            .fetch_one(pool)
            .await?;

    // FinalizeArtifactBackup(READY) in flight: the upload completed and
    // the finalizer's transaction holds the snapshot row, not yet
    // committed.
    let mut finalize_tx = pool.begin().await?;
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(snapshot_id)
        .execute(&mut *finalize_tx)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://test-bucket/late-finalize')"
    ))
    .bind(snapshot_id)
    .execute(&mut *finalize_tx)
    .await?;

    let racing_sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());
    let racer = tokio::spawn(async move {
        racing_sink
            .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    finalize_tx.commit().await?;

    let marked = racer.await.expect("racer must not panic")?;
    assert_eq!(
        marked,
        Vec::<SandboxId>::new(),
        "a finalize that commits before the flip must spare the sandbox"
    );
    assert_eq!(
        lifecycle(pool, sandbox).await?,
        "suspended",
        "the just-became-durable sandbox must survive the raced give-up"
    );
    let snapshot_state: String =
        sqlx::query_scalar(sql!("SELECT state FROM snapshots WHERE id = $1"))
            .bind(snapshot_id)
            .fetch_one(pool)
            .await?;
    assert_eq!(
        snapshot_state, "ready",
        "the finalized snapshot must not be clobbered to failed"
    );
    Ok(())
}

/// Regression for a codex-flagged veto race: a `BeginArtifactBackup`
/// INSERT from a still-live node takes only a `KEY SHARE` FK lock on the
/// sandbox row, which does not conflict with the flip UPDATE's
/// `NO KEY UPDATE` row lock — so without an explicit `FOR UPDATE` claim
/// (which `KEY SHARE` *does* conflict with) the in-flight veto can
/// evaluate before the insert commits and terminally fail a sandbox
/// whose durable copy is about to start uploading. The sink must claim
/// the candidate rows `FOR UPDATE` before evaluating the guard, so the
/// insert either commits first (veto sees it) or blocks until the flip
/// decides.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_refuses_when_concurrent_upload_insert_commits_first() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node = NodeId::new();
    let live_node = Uuid::now_v7();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_without_snapshots(pool, node).await?;

    // BeginArtifactBackup from a live node, transaction still open: the
    // creating row is inserted (FK KEY SHARE held on the sandbox row)
    // but not yet committed.
    let mut upload_tx = pool.begin().await?;
    sqlx::query(sql!(
        "INSERT INTO snapshots (id, sandbox_id, type, state, source_node_id, timestamp)
         VALUES ($1, $2, 'full', 'creating', $3, NOW())"
    ))
    .bind(Uuid::now_v7())
    .bind(sandbox)
    .bind(live_node)
    .execute(&mut *upload_tx)
    .await?;

    let racing_sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());
    let racer = tokio::spawn(async move {
        racing_sink
            .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    upload_tx.commit().await?;

    let marked = racer.await.expect("racer must not panic")?;
    assert_eq!(
        marked,
        Vec::<SandboxId>::new(),
        "an upload insert that commits before the flip must veto the give-up"
    );
    assert_eq!(
        lifecycle(pool, sandbox).await?,
        "suspended",
        "the sandbox must survive the raced give-up"
    );
    Ok(())
}

/// Regression for a codex-flagged guard race: the mark-lost UPDATE's
/// `EXISTS` guards on `nodes` / `node_k8s` read the statement snapshot,
/// so without explicit locks an admin undrain (or reflector uid bump)
/// committing concurrently is not observed — the flip would terminally
/// fail sandboxes in exactly the replacement/undrain cases the guards
/// exist to protect. The sink must lock the guard rows inside its
/// transaction (the suspended-execution reaper's `FOR UPDATE` +
/// re-check pattern), so a concurrent undrain serializes: whichever
/// commits first, the flip never survives a node that is no longer
/// `drained`.
#[tokio::test(flavor = "current_thread")]
async fn mark_lost_refuses_when_concurrent_undrain_commits_first() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_without_snapshots(pool, node).await?;

    // Admin undrain in an open transaction: the row lock is held, the
    // change not yet committed.
    let mut undrain_tx = pool.begin().await?;
    sqlx::query(sql!("UPDATE nodes SET drain_phase = NULL WHERE id = $1"))
        .bind(node)
        .execute(&mut *undrain_tx)
        .await?;

    // The give-up runs concurrently. Locked guards block it behind the
    // undrain's row lock; snapshot-only guards would sail past and flip.
    let racing_sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());
    let racer = tokio::spawn(async move {
        racing_sink
            .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
            .await
    });
    // Give the racer time to reach the guard before the undrain lands.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    undrain_tx.commit().await?;

    let marked = racer.await.expect("racer must not panic")?;
    assert_eq!(
        marked,
        Vec::<SandboxId>::new(),
        "an undrain that commits before the flip must refuse the give-up"
    );
    assert_eq!(
        lifecycle(pool, sandbox).await?,
        "suspended",
        "the sandbox must survive the raced give-up"
    );
    Ok(())
}

/// End-to-end convergence through the real sink: a gone-node `drained` row
/// blocked by a zero-snapshot legacy suspended sandbox is refused, the mark-lost give-up flips the
/// sandbox, and the next delete reaps the row.
#[tokio::test(flavor = "current_thread")]
async fn gone_node_mark_lost_then_reap_converges() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_without_snapshots(pool, node).await?;

    // Tick 1: the snapshot guard refuses the reap...
    let outcome = sink
        .delete_drained_node_if_orphan(node, Some(node_uid(node)))
        .await?;
    assert_eq!(
        outcome,
        OrphanDeleteOutcome::Refused(OrphanDeleteRefusal {
            snapshot_blockers: 1,
            ..OrphanDeleteRefusal::default()
        })
    );
    // ...and the give-up marks the unrecoverable sandbox lost.
    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    assert_eq!(lifecycle(pool, sandbox).await?, "failed");

    // Tick 2: the reap converges.
    let outcome = sink
        .delete_drained_node_if_orphan(node, Some(node_uid(node)))
        .await?;
    assert_eq!(outcome, OrphanDeleteOutcome::Deleted);
    assert_eq!(drain_phase(pool, node).await?, None, "row must be gone");

    Ok(())
}

/// One row whose flip another node's in-flight upload vetoes doesn't keep the
/// give-up from a different casualty on the same node.
///
/// Stated as what one tick does to the node's rows: the vetoed row stays
/// `suspended` and the other undurable row is marked lost. The vetoed row is
/// also queued on this node, so a claim that read the queue first and stopped
/// there would return it alone, defer it, and leave the casualty for a later
/// tick that never comes while the upload runs.
#[tokio::test(flavor = "current_thread")]
async fn a_vetoed_row_does_not_hold_back_another_casualty() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    let other_node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let vetoed = insert_suspended_with_creating_upload_from(pool, node, other_node).await?;
    enqueue_for(pool, node, vetoed).await?;
    let casualty = insert_suspended_without_snapshots(pool, node).await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(
        marked,
        vec![casualty],
        "the casualty must be given up on this tick, whatever the vetoed row does"
    );
    assert_eq!(lifecycle(pool, casualty).await?, "failed");
    assert_eq!(
        lifecycle(pool, vetoed).await?,
        "suspended",
        "control: the other node's live upload still defers its own row"
    );
    Ok(())
}

/// A sandbox whose durable copy stops counting after the node is gone is
/// still given up, on a later tick.
///
/// The claim skips rows that are durable when it runs, so a row that loses
/// durability afterwards isn't claimed on that tick. What keeps that from
/// stranding it is that it now blocks the reap, so the next tick's give-up
/// claims it. Here the storage location of the row's only ready copy is
/// orphaned between two give-ups.
#[tokio::test(flavor = "current_thread")]
async fn a_row_that_loses_durability_is_given_up_on_the_next_tick() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_with_durable_snapshot(pool, node).await?;

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert!(
        marked.is_empty(),
        "premise: a durable row is not given up, got {marked:?}"
    );

    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations ssl
            SET orphaned_at = NOW()
           FROM snapshots sn
          WHERE ssl.snapshot_id = sn.id AND sn.sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let OrphanDeleteOutcome::Refused(refusal) = sink
        .delete_drained_node_if_orphan(node, Some(node_uid(node)))
        .await?
    else {
        panic!("the row with no durable copy left must refuse the reap");
    };
    assert_eq!(
        refusal.snapshot_blockers, 1,
        "the row now blocks the reap, which is what runs the give-up again"
    );

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    assert_eq!(lifecycle(pool, sandbox).await?, "failed");
    Ok(())
}

/// The claim locks the rows the flip could act on and leaves the node's
/// durable history alone, including the snapshot and storage-location rows its
/// durability check reads.
///
/// Asserted by contention rather than by counting, because `FOR UPDATE` row
/// locks are recorded in the tuple header and don't appear in `pg_locks`. A
/// second connection reaches for each row with `NOWAIT`, which succeeds iff
/// the claim left that row unlocked. The historical claim, every pinned
/// `suspended` row, is the control: it does lock the durable row, so the probe
/// can see the lock this test asserts is absent.
#[tokio::test(flavor = "current_thread")]
async fn the_claim_locks_only_rows_without_a_durable_copy() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let durable = insert_suspended_with_durable_snapshot(pool, node).await?;
    let casualty = insert_suspended_without_snapshots(pool, node).await?;

    /// Is the row `probe` selects for `$1 = id` free while `claim` holds its
    /// locks?
    async fn free_under(
        pool: &PgPool,
        claim: &str,
        node: NodeId,
        probe: &str,
        id: SandboxId,
    ) -> TestResult<bool> {
        let mut claim_tx = pool.begin().await?;
        sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(claim)))
            .bind(node)
            .fetch_all(&mut *claim_tx)
            .await?;
        let mut probe_tx = pool.begin().await?;
        // Only `lock_not_available` (55P03) means "locked"; any other error
        // fails the test rather than reading as a lock.
        let free = match sqlx::query_scalar::<_, Uuid>(sqlx::AssertSqlSafe(sql!(probe)))
            .bind(id)
            .fetch_one(&mut *probe_tx)
            .await
        {
            Ok(_) => true,
            Err(sqlx::Error::Database(err)) if err.code().as_deref() == Some("55P03") => false,
            Err(err) => return Err(err.into()),
        };
        probe_tx.rollback().await?;
        claim_tx.rollback().await?;
        Ok(free)
    }
    const SANDBOX_PROBE: &str = "SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE NOWAIT";
    const SNAPSHOT_PROBE: &str = "SELECT id FROM snapshots WHERE sandbox_id = $1 FOR UPDATE NOWAIT";
    const STORAGE_LOCATION_PROBE: &str = "SELECT sn.id
           FROM snapshot_storage_locations ssl
           JOIN snapshots sn ON sn.id = ssl.snapshot_id
          WHERE sn.sandbox_id = $1
            FOR UPDATE OF ssl NOWAIT";

    let historical = "SELECT id FROM sandboxes
                       WHERE lifecycle = 'suspended' AND target_node_id = $1
                       ORDER BY id
                       FOR UPDATE";
    assert!(
        !free_under(pool, historical, node, SANDBOX_PROBE, durable).await?,
        "control: the historical claim locks the durable row, so the probe can \
         tell the two claims apart"
    );

    // A claim that joins the durability tables locks their rows too, so the
    // two probes below can see a lock there when one is taken.
    let joined = "SELECT s.id
                    FROM sandboxes s
                    JOIN snapshots sn ON sn.sandbox_id = s.id
                    JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = sn.id
                   WHERE s.lifecycle = 'suspended' AND s.target_node_id = $1
                     FOR UPDATE";
    for probe in [SNAPSHOT_PROBE, STORAGE_LOCATION_PROBE] {
        assert!(
            !free_under(pool, joined, node, probe, durable).await?,
            "control: a claim joining the durability tables locks their rows, \
             so {probe:?} can see such a lock"
        );
    }

    let claim = super::gone_node_giveup_claim_query();
    assert!(
        free_under(pool, &claim, node, SANDBOX_PROBE, durable).await?,
        "the claim must not lock a row with a durable copy"
    );
    assert!(
        free_under(pool, &claim, node, SNAPSHOT_PROBE, durable).await?,
        "the claim must not lock the snapshot rows its durability check reads"
    );
    assert!(
        free_under(pool, &claim, node, STORAGE_LOCATION_PROBE, durable).await?,
        "...nor the storage-location rows it reads"
    );
    assert!(
        !free_under(pool, &claim, node, SANDBOX_PROBE, casualty).await?,
        "...but it must lock the row it claims, or the flip races the \
         concurrent BeginArtifactBackup insert the lock serializes against"
    );
    Ok(())
}

async fn enqueue_for(pool: &PgPool, node: NodeId, sandbox: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO node_local_backup_pending
             (node_id, sandbox_id, artifact_id, artifact_kind)
         VALUES ($1, $2, $3, 'full')"
    ))
    .bind(node)
    .bind(sandbox)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

/// The give-up drops a given-up sandbox's queue row on this node, and leaves
/// the same sandbox's row on another node, which is that node's inventory.
#[tokio::test(flavor = "current_thread")]
async fn the_give_up_drops_its_queue_row_on_this_node_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let sink = NodeDbStore::new(pool.clone(), ResidencyEvidence::default());

    let node = NodeId::new();
    let other_node = NodeId::new();
    insert_drained_node(pool, node).await?;
    let sandbox = insert_suspended_without_snapshots(pool, node).await?;
    enqueue_for(pool, node, sandbox).await?;
    enqueue_for(pool, other_node, sandbox).await?;
    let queue_rows = |node: NodeId| async move {
        sqlx::query_scalar::<_, i64>(sql!(
            "SELECT COUNT(*) FROM node_local_backup_pending
              WHERE node_id = $1 AND sandbox_id = $2"
        ))
        .bind(node)
        .bind(sandbox)
        .fetch_one(pool)
        .await
    };

    let marked = sink
        .mark_unrecoverable_sandboxes_lost(node, Some(node_uid(node)))
        .await?;
    assert_eq!(marked, vec![sandbox]);
    assert_eq!(
        queue_rows(node).await?,
        0,
        "the given-up sandbox's queue row on this node is dropped"
    );
    assert_eq!(
        queue_rows(other_node).await?,
        1,
        "its row on another node belongs to that node's inventory and survives"
    );
    Ok(())
}
