//! # Test matrix
//!
//! - C1 `c1_default_is_enabled`
//! - F1 `f1_lists_the_wedge_shape_and_defers_everything_else` — the
//!   repairable fixture's snapshot predates `lifecycle_changed_at` /
//!   `placement_changed_at` (the wedge itself); every unsafe variant is
//!   excluded. This is the regression guard for the whole loop: any
//!   lifecycle/placement-anchored eligibility fails it.
//! - F2b `f2_repair_flips_row_and_preserves_marker`
//! - F3 `f3_markerless_repair_readmits_snapshot_for_wake_selection`
//! - F4 `f4_authoritative_state_races_skip`
//! - F4b `f4b_repair_reverts_only_a_row_still_failed` — the apply's
//!   lifecycle guard over every stored lifecycle.
//! - F5 `f5_marker_stale_with_newer_suspend_snapshot_clears_marker`
//! - F6 `f6_unassigned_frozen_failed_projection_repaired_and_purged` — a
//!   `failed`, pid-less observed record on an unassigned sandbox (the fossil
//!   the failed adoption wrote in its brief assignment window) no longer
//!   defers; the repair purges it in the same transaction. Live-looking
//!   projections and still-assigned sandboxes keep deferring.
//! - F7 `f7_direct_rows_are_neither_listed_nor_repaired` — the scan and the
//!   apply both bind the Kubernetes substrate: the repairable fixture with
//!   `managed_by = 'space'` is not listed and its apply refuses.
//! - T2 `t2_live_tick_repairs`

use pplx_space_common_core::types::LocalArtifactId;
use pplx_space_common_core::types::{NodeId, SandboxId, SnapshotId};

use std::time::Duration;

use pplx_space_common_core::types::PositiveDuration;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, pg_test_db, try_pg_test_db};
use pplx_space_placement::test_support::stored_lifecycle_matrix;
use pplx_space_sandbox_portability::fresh_ready_snapshot_exists;
use sqlx::PgPool;
use uuid::Uuid;

use super::store::RepairDbStore;
use super::*;
use crate::lifecycle_writer_test_support::relabel_keeping_lifecycle_age;

use pplx_space_test_env::TestResult;

const MIN_AGE: i64 = MIN_AGE_SECS as i64;

struct FailedSandboxSpec {
    sandbox_id: SandboxId,
    lifecycle: &'static str,
    lifecycle_age_secs: i64,
    execution_active: bool,
    execution_pod_ip: Option<&'static str>,
    observed_status: Option<&'static str>,
    observed_pid: Option<i32>,
    /// Strip the node assignment after insert (`active_node_id` /
    /// `target_node_id` NULL, placement `unplaced`) — the shape the failed
    /// wake's cleanup leaves behind, and the state in which the publish
    /// fence orphans any observed row written during the assignment window.
    unassigned: bool,
    /// Ready full suspend snapshot for the marker's artifact, timestamped
    /// **before** `lifecycle_changed_at` — the wedge shape: the canonical
    /// snapshot predates the failed transition, and `placement_changed_at`
    /// (bumped by the failed restore placement) postdates the snapshot.
    snapshot: bool,
    current_marker: bool,
    /// Ready full `stop`-origin snapshot newer than the suspend snapshot;
    /// the markerless selector would load it, so recovery must refuse.
    newer_nonsuspend_snapshot: bool,
    /// `last_command_at`/`last_access_at` after the snapshot — restoring
    /// would provably roll back user activity.
    activity_after_snapshot: bool,
    /// Additional ready suspend snapshot with a different artifact id,
    /// newer than the marker's (absent) own snapshot.
    newer_distinct_snapshot: bool,
}

impl FailedSandboxSpec {
    fn repairable(sandbox_id: SandboxId) -> Self {
        Self {
            sandbox_id,
            lifecycle: "failed",
            lifecycle_age_secs: MIN_AGE + 300,
            execution_active: false,
            execution_pod_ip: None,
            observed_status: None,
            observed_pid: None,
            unassigned: false,
            snapshot: true,
            current_marker: true,
            newer_nonsuspend_snapshot: false,
            activity_after_snapshot: false,
            newer_distinct_snapshot: false,
        }
    }

    /// Snapshot creation time, strictly before `lifecycle_changed_at`.
    fn snapshot_age_secs(&self) -> i64 {
        self.lifecycle_age_secs + 60
    }
}

async fn insert_failed_sandbox(pool: &PgPool, spec: &FailedSandboxSpec) -> TestResult {
    let artifact_id = Uuid::now_v7();
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO sandboxes
            (id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, network_policy_generation,
             command_ready_pending, current_local_artifact_id,
             current_local_artifact_kind, current_local_artifact_origin,
             restore_lease_expires_at, restore_lease_token)
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', $4, 'on_node',
                 $5, $5, 1, TRUE, $6, 'full', 'suspend',
                 NOW() - INTERVAL '4 minutes', $7)",
    )
    .bind(spec.sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .bind(spec.lifecycle)
    .bind(node_id)
    .bind(artifact_id)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    if !spec.current_marker {
        sqlx::query(
            "UPDATE sandboxes
                SET current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .execute(pool)
        .await?;
    }
    // Both clocks postdate the canonical snapshot (inserted below, 60s older).
    sqlx::query(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - make_interval(secs => $2::BIGINT),
                placement_changed_at = NOW() - make_interval(secs => $2::BIGINT)
          WHERE id = $1",
    )
    .bind(spec.sandbox_id)
    .bind(spec.lifecycle_age_secs)
    .execute(pool)
    .await?;

    sqlx::query(
        "INSERT INTO sandbox_k8s_executions
            (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
             state, active, created_at)
         VALUES ($1, 1, $2, 'sandboxes', 'uid-1', $3::inet,
                 'forgotten', $4, NOW() - make_interval(secs => $5::BIGINT))",
    )
    .bind(spec.sandbox_id)
    .bind(format!(
        "space-sbx-{}-g1",
        spec.sandbox_id.as_uuid().simple()
    ))
    .bind(spec.execution_pod_ip)
    .bind(spec.execution_active)
    .bind(MIN_AGE + 300)
    .execute(pool)
    .await?;

    if let Some(status) = spec.observed_status {
        sqlx::query(
            "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.1.0.1', 'worker', 'k8s', 'ready')",
        )
        .bind(node_id)
        .execute(pool)
        .await?;
        sqlx::query(
            "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status, pid)
             VALUES ($1, $2, $3, $4)",
        )
        .bind(spec.sandbox_id)
        .bind(node_id)
        .bind(status)
        .bind(spec.observed_pid)
        .execute(pool)
        .await?;
    }

    if spec.unassigned {
        sqlx::query(
            "UPDATE sandboxes
                SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced'
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .execute(pool)
        .await?;
    }

    if spec.snapshot {
        insert_ready_snapshot(
            pool,
            spec.sandbox_id,
            artifact_id,
            spec.snapshot_age_secs(),
            "suspend",
        )
        .await?;
    }
    if spec.newer_distinct_snapshot {
        insert_ready_snapshot(
            pool,
            spec.sandbox_id,
            Uuid::now_v7(),
            spec.snapshot_age_secs().saturating_sub(30),
            "suspend",
        )
        .await?;
    }
    if spec.newer_nonsuspend_snapshot {
        insert_ready_snapshot(
            pool,
            spec.sandbox_id,
            Uuid::now_v7(),
            spec.snapshot_age_secs().saturating_sub(45),
            "stop",
        )
        .await?;
    }
    if spec.activity_after_snapshot {
        sqlx::query(
            "UPDATE sandboxes
                SET last_command_at = NOW() - make_interval(secs => $2::BIGINT),
                    last_access_at = NOW() - make_interval(secs => $2::BIGINT)
              WHERE id = $1",
        )
        .bind(spec.sandbox_id)
        .bind(spec.snapshot_age_secs().saturating_sub(30))
        .execute(pool)
        .await?;
    }
    Ok(())
}

async fn insert_ready_snapshot(
    pool: &PgPool,
    sandbox_id: SandboxId,
    artifact_id: Uuid,
    age_secs: i64,
    origin: &str,
) -> TestResult {
    let snapshot_id = SnapshotId::new();
    sqlx::query(
        "INSERT INTO snapshots
            (id, sandbox_id, org_id, type, state, timestamp,
             local_artifact_id, artifact_kind, artifact_origin)
         VALUES ($1, $2, $3::uuid, 'full', 'ready',
                 NOW() - make_interval(secs => $4::BIGINT), $5, 'full', $6)",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(SEED_ORG_ID)
    .bind(age_secs)
    .bind(artifact_id)
    .bind(origin)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string)
         VALUES ($1, 's3://bucket/snapshot')",
    )
    .bind(snapshot_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn min_age() -> PositiveDuration {
    PositiveDuration::new(Duration::from_secs(MIN_AGE_SECS)).unwrap()
}

async fn wake_selector_admits(pool: &PgPool, sandbox_id: SandboxId) -> TestResult<bool> {
    let fresh = fresh_ready_snapshot_exists("s");
    let query = format!("SELECT {fresh} FROM sandboxes s WHERE s.id = $1");
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(query.as_str()))
        .bind(sandbox_id)
        .fetch_one(pool)
        .await?)
}

#[test]
fn c1_default_is_enabled() {
    let config = ReconcilerConfig::default();
    assert!(config.enabled);
}

/// The repair writes, so only an unset or explicitly truthy switch leaves it
/// running; a misspelled one must stop it.
#[test]
fn c2_malformed_kill_switch_disables_the_repair() {
    let env = pplx_space_test_env::TestEnv::default();
    assert!(ReconcilerConfig::from_environment(&env).enabled);
    for value in ["true", "1"] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_FAILED_RESTORABLE_REPAIR_ENABLED", value);
        assert!(ReconcilerConfig::from_environment(&env).enabled, "{value}");
    }
    for value in ["false", "0", "off", " false  ", "fasle", ""] {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_FAILED_RESTORABLE_REPAIR_ENABLED", value);
        assert!(
            !ReconcilerConfig::from_environment(&env).enabled,
            "{value:?}"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn f1_lists_the_wedge_shape_and_defers_everything_else() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let repairable = FailedSandboxSpec::repairable(SandboxId::new());
    insert_failed_sandbox(&db.pool, &repairable).await?;

    let excluded = [
        FailedSandboxSpec {
            lifecycle: "suspended",
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            lifecycle: "quarantined",
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            lifecycle_age_secs: 60,
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            snapshot: false,
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            execution_active: true,
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            execution_pod_ip: Some("10.0.0.9"),
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            observed_status: Some("failed"),
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            observed_status: Some("started"),
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            activity_after_snapshot: true,
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
        FailedSandboxSpec {
            current_marker: false,
            newer_nonsuspend_snapshot: true,
            ..FailedSandboxSpec::repairable(SandboxId::new())
        },
    ];
    for spec in &excluded {
        insert_failed_sandbox(&db.pool, spec).await?;
    }

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sandbox_id, repairable.sandbox_id);
    assert!(!candidates[0].clear_marker);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f2_repair_flips_row_and_preserves_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let spec = FailedSandboxSpec::repairable(SandboxId::new());
    insert_failed_sandbox(&db.pool, &spec).await?;

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 1);
    let outcome = store
        .revert_failed_to_suspended(&candidates[0], min_age())
        .await?;
    assert_eq!(outcome, ApplyOutcome::RevertedToSuspended);

    let (lifecycle, placement, node, placement_cleared, lease_cleared, marker): (
        String,
        String,
        Option<NodeId>,
        bool,
        bool,
        Option<LocalArtifactId>,
    ) = sqlx::query_as(
        "SELECT lifecycle, placement, active_node_id,
                placement_changed_at IS NULL,
                restore_lease_expires_at IS NULL,
                current_local_artifact_id
           FROM sandboxes WHERE id = $1",
    )
    .bind(spec.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("suspended", "unplaced")
    );
    assert_eq!(node, None);
    assert!(placement_cleared, "placement_changed_at must be cleared");
    assert!(lease_cleared, "stale restore lease must be cleared");
    assert_eq!(
        marker, candidates[0].current_local_artifact_id,
        "the exact pre-repair marker must be preserved"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f3_markerless_repair_readmits_snapshot_for_wake_selection() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let spec = FailedSandboxSpec {
        current_marker: false,
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    insert_failed_sandbox(&db.pool, &spec).await?;

    // Before repair, the wake selector's placement-freshness gate rejects the
    // snapshot: the failed restore's placement bump postdates it.
    assert!(!wake_selector_admits(&db.pool, spec.sandbox_id).await?);

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 1);
    assert!(
        !candidates[0].clear_marker,
        "markerless rows have nothing to clear"
    );
    let outcome = store
        .revert_failed_to_suspended(&candidates[0], min_age())
        .await?;
    assert_eq!(outcome, ApplyOutcome::RevertedToSuspended);

    // After repair, placement_changed_at is NULL and the same gate admits the
    // snapshot — the sandbox is wake-eligible end to end.
    assert!(wake_selector_admits(&db.pool, spec.sandbox_id).await?);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f4_authoritative_state_races_skip() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };

    let active_execution = FailedSandboxSpec::repairable(SandboxId::new());
    let execution_with_ip = FailedSandboxSpec::repairable(SandboxId::new());
    let observed_state = FailedSandboxSpec::repairable(SandboxId::new());
    let lifecycle_change = FailedSandboxSpec::repairable(SandboxId::new());
    for spec in [
        &active_execution,
        &execution_with_ip,
        &observed_state,
        &lifecycle_change,
    ] {
        insert_failed_sandbox(&db.pool, spec).await?;
    }

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 4);

    sqlx::query("UPDATE sandbox_k8s_executions SET active = TRUE WHERE sandbox_id = $1")
        .bind(active_execution.sandbox_id)
        .execute(&db.pool)
        .await?;
    sqlx::query("UPDATE sandbox_k8s_executions SET pod_ip = '10.0.0.9' WHERE sandbox_id = $1")
        .bind(execution_with_ip.sandbox_id)
        .execute(&db.pool)
        .await?;
    let observed_node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '10.1.0.200', 'worker', 'k8s', 'ready')",
    )
    .bind(observed_node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(
        "INSERT INTO sandbox_observed_states (sandbox_id, node_id, status)
         VALUES ($1, $2, 'started')",
    )
    .bind(observed_state.sandbox_id)
    .bind(observed_node_id)
    .execute(&db.pool)
    .await?;
    sqlx::query("UPDATE sandboxes SET lifecycle = 'deleting' WHERE id = $1")
        .bind(lifecycle_change.sandbox_id)
        .execute(&db.pool)
        .await?;

    for candidate in &candidates {
        let outcome = store
            .revert_failed_to_suspended(candidate, min_age())
            .await?;
        assert_eq!(
            outcome,
            ApplyOutcome::SkippedRaced,
            "sandbox {} bypassed an apply-time authority guard",
            candidate.sandbox_id
        );
    }
    Ok(())
}

/// The revert moves a row to `suspended` only while it is still `failed`: a
/// candidate listed while `failed` whose row now stores any other lifecycle,
/// with every other guard and its age unchanged, is refused and keeps that
/// lifecycle. `A`: reverted; `.`: refused.
#[tokio::test(flavor = "current_thread")]
async fn f4b_repair_reverts_only_a_row_still_failed() -> TestResult {
    const EXPECTED: &[&str] = &[
        "created           .",
        "creating          .",
        "starting          .",
        "started           .",
        "restoring         .",
        "pausing           .",
        "paused            .",
        "resuming          .",
        "suspending        .",
        "suspended         .",
        "stopping          .",
        "stopped           .",
        "deleting          .",
        "checkpointing     .",
        "quarantined       .",
        "failed            A",
        "deleted           .",
        "cleanup_required  .",
        "bogus             .",
        "_Suspended_       .",
    ];
    let db = pg_test_db().await?;
    let store = RepairDbStore::new(db.pool.clone());

    let mut mismatches = Vec::new();
    for (stored, [(_, reverts)]) in stored_lifecycle_matrix(["revert"], EXPECTED) {
        let reverts = reverts.applies();
        let spec = FailedSandboxSpec::repairable(SandboxId::new());
        insert_failed_sandbox(&db.pool, &spec).await?;
        let candidate = store
            .list_candidates(100, None, min_age())
            .await?
            .into_iter()
            .find(|candidate| candidate.sandbox_id == spec.sandbox_id)
            .expect("the repairable row is listed");
        relabel_keeping_lifecycle_age(&db.pool, spec.sandbox_id, stored).await?;

        let outcome = store
            .revert_failed_to_suspended(&candidate, min_age())
            .await?;

        let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(spec.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
        let (expected_outcome, expected_lifecycle) = if reverts {
            (ApplyOutcome::RevertedToSuspended, "suspended")
        } else {
            (ApplyOutcome::SkippedRaced, stored)
        };
        if outcome != expected_outcome || lifecycle != expected_lifecycle {
            mismatches.push(format!("from {stored}: {outcome:?}, row now {lifecycle}"));
        }
    }
    assert!(mismatches.is_empty(), "{mismatches:#?}");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f5_marker_stale_with_newer_suspend_snapshot_clears_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // Marker present but with no backing snapshot; a distinct newer durable
    // suspend snapshot exists. Recovery clears the marker so the restore
    // selector picks that newest snapshot.
    let spec = FailedSandboxSpec {
        snapshot: false,
        newer_distinct_snapshot: true,
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    insert_failed_sandbox(&db.pool, &spec).await?;

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 1);
    assert!(candidates[0].clear_marker);
    let outcome = store
        .revert_failed_to_suspended(&candidates[0], min_age())
        .await?;
    assert_eq!(outcome, ApplyOutcome::RevertedToSuspendedMarkerCleared);

    let marker: Option<Uuid> =
        sqlx::query_scalar("SELECT current_local_artifact_id FROM sandboxes WHERE id = $1")
            .bind(spec.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(marker, None, "stale marker must be cleared");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f6_unassigned_frozen_failed_projection_repaired_and_purged() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // The frozen-projection wedge: the failed adoption wrote the observed row
    // while the failing restore briefly held the node assignment; the release
    // then closed the publish fence, leaving a `failed`, pid-less record no
    // publisher can update or delete — deferring the repair forever.
    let frozen = FailedSandboxSpec {
        observed_status: Some("failed"),
        unassigned: true,
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    insert_failed_sandbox(&db.pool, &frozen).await?;
    // Boundary shapes that must keep deferring: a live-looking projection on
    // an unassigned sandbox, and a failed record on a still-assigned sandbox
    // (its fence is open — the node's next publish could race the purge).
    let observed_live_unassigned = FailedSandboxSpec {
        observed_status: Some("restoring"),
        unassigned: true,
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    let observed_pid_unassigned = FailedSandboxSpec {
        observed_status: Some("failed"),
        observed_pid: Some(42),
        unassigned: true,
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    let observed_failed_assigned = FailedSandboxSpec {
        observed_status: Some("failed"),
        ..FailedSandboxSpec::repairable(SandboxId::new())
    };
    insert_failed_sandbox(&db.pool, &observed_live_unassigned).await?;
    insert_failed_sandbox(&db.pool, &observed_pid_unassigned).await?;
    insert_failed_sandbox(&db.pool, &observed_failed_assigned).await?;

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(
        candidates.len(),
        1,
        "only the frozen unassigned shape may list"
    );
    assert_eq!(candidates[0].sandbox_id, frozen.sandbox_id);

    let outcome = store
        .revert_failed_to_suspended(&candidates[0], min_age())
        .await?;
    assert_eq!(outcome, ApplyOutcome::RevertedToSuspended);

    let (lifecycle, placement, command_ready_pending): (String, String, bool) = sqlx::query_as(
        "SELECT lifecycle, placement, command_ready_pending
               FROM sandboxes WHERE id = $1",
    )
    .bind(frozen.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(
        (lifecycle.as_str(), placement.as_str()),
        ("suspended", "unplaced")
    );
    assert!(
        !command_ready_pending,
        "suspended finalization must still clear observed readiness"
    );

    let projection_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sandbox_observed_states WHERE sandbox_id = $1")
            .bind(frozen.sandbox_id)
            .fetch_one(&db.pool)
            .await?;
    assert_eq!(
        projection_rows, 0,
        "the fossil projection must be purged with the flip, or the API \
         overlay keeps reporting the sandbox failed"
    );

    // The repaired row left the cohort; the deferred shapes still block.
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert!(candidates.is_empty());
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn t2_live_tick_repairs() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let spec = FailedSandboxSpec::repairable(SandboxId::new());
    insert_failed_sandbox(&db.pool, &spec).await?;

    let mut reconciler = Reconciler::new(RepairDbStore::new(db.pool.clone()));
    let report = reconciler.tick().await;
    assert_eq!((report.reverted, report.raced, report.errors), (1, 0, 0));

    let lifecycle: String = sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
        .bind(spec.sandbox_id)
        .fetch_one(&db.pool)
        .await?;
    assert_eq!(lifecycle, "suspended");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn f7_direct_rows_are_neither_listed_nor_repaired() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let spec = FailedSandboxSpec::repairable(SandboxId::new());
    insert_failed_sandbox(&db.pool, &spec).await?;
    let set_managed_by = |managed_by: &'static str| {
        sqlx::query("UPDATE sandboxes SET managed_by = $2 WHERE id = $1")
            .bind(spec.sandbox_id)
            .bind(managed_by)
            .execute(&db.pool)
    };

    let store = RepairDbStore::new(db.pool.clone());
    let candidates = store.list_candidates(100, None, min_age()).await?;
    assert_eq!(candidates.len(), 1, "the k8s fixture is listed");

    set_managed_by("space").await?;
    assert!(
        store
            .list_candidates(100, None, min_age())
            .await?
            .is_empty(),
        "a direct row is not listed"
    );
    assert_eq!(
        store
            .revert_failed_to_suspended(&candidates[0], min_age())
            .await?,
        ApplyOutcome::SkippedRaced,
        "the apply refuses a direct row"
    );

    set_managed_by("k8s").await?;
    assert_eq!(
        store
            .revert_failed_to_suspended(&candidates[0], min_age())
            .await?,
        ApplyOutcome::RevertedToSuspended
    );
    Ok(())
}
