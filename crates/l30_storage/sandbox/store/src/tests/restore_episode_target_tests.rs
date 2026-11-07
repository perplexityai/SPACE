//! `sandboxes.restore_episode_target` against the node drain's marker,
//! `evacuation_restore_target`. Nothing in production writes the target yet,
//! so these tests write it directly.
//!
//! Every statement here that asks whether a restore is the system's, or which
//! lifecycle it lands in, must give the same answer whichever column holds
//! the value ([`assert_episode_target_reads_as_marker`]), and every statement
//! that ends a restore episode must leave the target NULL. The activation
//! rollback's retirement check is the exception: the rollback clears the
//! target, so only the marker stops it.

use std::time::Duration;

use pplx_space_common_core::types::{
    PositiveDuration, SandboxTransitionReceiptKind, SandboxTransitionTargetState, SubstrateKind,
};
use pplx_space_sandbox_portability::{
    drain_gate_blockers_for_node, snapshot_durable_for_source_release,
};
use serde_json::json;
use sqlx::types::Json;

use super::*;
use crate::action_stamp::{ActionStamp, StartSubstrate};
use crate::ingest::SuspendedFinalizationCandidate;
use crate::sweeper::FailGuard;
use crate::test_support::restore_target::{
    HeldTarget, RESTORE_TARGETS, assert_episode_target_reads_as_marker, hold_restore_target,
    read_restore_episode_target,
};
use crate::{
    action_stamp, create, delete, direct, drain, ingest, lost, pod_tracking, quarantine,
    restore_lease, restore_source, sweeper, wake,
};

/// The sweeper's and the drain's age threshold in these tests; fixtures age
/// their rows well past it.
fn age() -> PositiveDuration {
    PositiveDuration::new_panic(Duration::from_secs(60))
}

/// A row of `managed_by` in `lifecycle`, holding `held`.
async fn sandbox(
    pool: &PgPool,
    managed_by: SubstrateKind,
    lifecycle: &str,
    held: Option<HeldTarget>,
) -> TestResult<SandboxId> {
    let id = SandboxId::from_uuid(insert_sandbox(pool, None, lifecycle).await?);
    sqlx::query(sql!("UPDATE sandboxes SET managed_by = $2 WHERE id = $1"))
        .bind(id)
        .bind(managed_by)
        .execute(pool)
        .await?;
    hold_restore_target(pool, id, held).await?;
    Ok(id)
}

/// Move the row's lifecycle clock (and its placement clock, when set) two
/// hours back, past every threshold these tests use. Leaves the lifecycle
/// alone, so no trigger moves either clock back.
async fn age_row(pool: &PgPool, id: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle_changed_at = NOW() - INTERVAL '2 hours',
                placement_changed_at = CASE
                    WHEN placement_changed_at IS NULL THEN NULL
                    ELSE NOW() - INTERVAL '2 hours'
                END
          WHERE id = $1"
    ))
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn place(
    pool: &PgPool,
    id: SandboxId,
    active: Option<NodeId>,
    target: Option<NodeId>,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = $2,
                target_node_id = $3,
                placement = CASE WHEN $2::UUID IS NULL THEN 'unplaced' ELSE 'on_node' END
          WHERE id = $1"
    ))
    .bind(id)
    .bind(active)
    .bind(target)
    .execute(pool)
    .await?;
    Ok(())
}

/// A Kubernetes node in `drain_phase` (forced when draining), with a
/// `node_k8s` row whose uid is `k8s-uid`.
async fn node(pool: &PgPool, drain_phase: Option<&str>) -> TestResult<NodeId> {
    let id = NodeId::new();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, role, managed_by, drain_phase, drain_mode)
         VALUES ($1, 'worker', 'k8s', $2, CASE WHEN $2 IS NULL THEN NULL ELSE 'forced' END)"
    ))
    .bind(id)
    .bind(drain_phase)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
         VALUES ($1, $2, 'k8s-uid')"
    ))
    .bind(id)
    .bind(format!("node-{id}"))
    .execute(pool)
    .await?;
    Ok(id)
}

/// An active execution of `sandbox` at generation 1 in `state`.
async fn execution(pool: &PgPool, sandbox: SandboxId, state: &str) -> TestResult {
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state)
         VALUES ($1, 1, $2, 'space', $3)"
    ))
    .bind(sandbox)
    .bind(format!("pod-{sandbox}"))
    .bind(state)
    .execute(pool)
    .await?;
    Ok(())
}

/// A ready full snapshot of `id` from before its restore began, which every
/// restore-source and revert proof these tests reach accepts.
async fn pre_restore_snapshot(pool: &PgPool, id: SandboxId) -> TestResult {
    insert_full_snapshot(
        pool,
        id.as_uuid(),
        Uuid::now_v7(),
        "ready",
        "2026-01-01T00:00:00Z",
        false,
    )
    .await
}

async fn lifecycle_of(pool: &PgPool, id: SandboxId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar(sql!("SELECT lifecycle FROM sandboxes WHERE id = $1"))
            .bind(id)
            .fetch_one(pool)
            .await?,
    )
}

/// The sweeper's candidate select and fail CAS spare a restore that is the
/// system's, and its unrestorable-`restoring` fail does too. A `suspending`
/// row is listed either way, carrying the target to the converge decision.
#[tokio::test(flavor = "current_thread")]
async fn the_sweeper_reads_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let space = SubstrateKind::Space.as_str();

    for lifecycle in [SandboxLifecycle::Restoring, SandboxLifecycle::Suspending] {
        assert_episode_target_reads_as_marker(
            &format!("sweeper::list_aged_transient({lifecycle})"),
            async |held| -> TestResult<_> {
                let id = sandbox(pool, SubstrateKind::Space, lifecycle.as_str(), held).await?;
                age_row(pool, id).await?;
                let listed =
                    sweeper::list_aged_transient(pool, space, lifecycle, age(), 1000, None).await?;
                Ok(listed
                    .into_iter()
                    .find(|row| row.id == id)
                    .map(|row| row.restore_target()))
            },
        )
        .await?;
    }

    assert_episode_target_reads_as_marker(
        "sweeper::fail_aged_candidate",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Space, "restoring", held).await?;
            age_row(pool, id).await?;
            let failed = sweeper::fail_aged_candidate(
                pool,
                id,
                space,
                SandboxLifecycle::Restoring,
                age(),
                FailGuard::Aged,
            )
            .await?;
            Ok(failed.is_some())
        },
    )
    .await?;

    assert_episode_target_reads_as_marker(
        "sweeper::fail_unrestorable_restoring",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Space, "restoring", held).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET placement_changed_at = NOW() - INTERVAL '2 hours'
                  WHERE id = $1"
            ))
            .bind(id)
            .execute(pool)
            .await?;
            Ok(sweeper::fail_unrestorable_restoring(pool, id, space, age()).await?)
        },
    )
    .await?;
    Ok(())
}

/// The user restore claim and the markerless lease claim refuse a
/// `restoring` row whose restore is the system's.
#[tokio::test(flavor = "current_thread")]
async fn the_restore_claims_read_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "restore_lease::claim_user_restore(restoring)",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            let claim =
                restore_lease::claim_user_restore(pool, id, 60, Uuid::now_v7(), 60, None).await?;
            Ok(claim.map(|claim| claim.lifecycle))
        },
    )
    .await?;

    assert_episode_target_reads_as_marker(
        "restore_lease::claim_markerless_restoring_lease",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            let claim =
                restore_lease::claim_markerless_restoring_lease(pool, id, 60, Uuid::now_v7(), 60)
                    .await?;
            Ok(claim.map(|claim| claim.lifecycle))
        },
    )
    .await?;
    Ok(())
}

/// The node's `restoring -> started` and `-> stopped` convergences leave the
/// system's restore alone; the stopped one only where it lands in `stopped`.
#[tokio::test(flavor = "current_thread")]
async fn the_published_convergences_read_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "ingest::converge_published_restoring_to_started",
        async |held| -> TestResult<_> {
            let node_id = NodeId::new();
            let id = sandbox(pool, SubstrateKind::Space, "restoring", held).await?;
            place(pool, id, Some(node_id), Some(node_id)).await?;
            let rows = Json(json!([{
                "sandbox_id": id,
                "published_lifecycle": "started",
                "transition_receipt_kind": null,
            }]));
            let converged =
                ingest::converge_published_restoring_to_started(pool, node_id, rows).await?;
            Ok((converged.contains(&id), lifecycle_of(pool, id).await?))
        },
    )
    .await?;

    assert_episode_target_reads_as_marker(
        "ingest::converge_published_stopped",
        async |held| -> TestResult<_> {
            let node_id = NodeId::new();
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            place(pool, id, Some(node_id), Some(node_id)).await?;
            let rows = Json(json!([{ "sandbox_id": id, "published_lifecycle": "stopped" }]));
            let converged = ingest::converge_published_stopped(pool, node_id, rows).await?;
            Ok((converged.contains(&id), lifecycle_of(pool, id).await?))
        },
    )
    .await?;
    Ok(())
}

/// The markerless-restoring recovery leaves the system's restore alone.
#[tokio::test(flavor = "current_thread")]
async fn the_markerless_restoring_recovery_reads_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "pod_tracking::recover_markerless_restoring_after_execution_cleanup",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            pre_restore_snapshot(pool, id).await?;
            let mut tx = pool.begin().await?;
            pod_tracking::recover_markerless_restoring_after_execution_cleanup(&mut tx, id).await?;
            tx.commit().await?;
            lifecycle_of(pool, id).await
        },
    )
    .await?;
    Ok(())
}

/// The activation rollback's retirement check refuses a marked row, and allows
/// one with a restore episode target, which the rollback clears.
#[tokio::test(flavor = "current_thread")]
async fn the_rollback_retirement_check_stops_at_the_marker_only() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    // The rollback clears an episode target, so the target doesn't stop it
    // retiring the execution as the marker does.
    let retirable = async |held| -> TestResult<bool> {
        let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
        pre_restore_snapshot(pool, id).await?;
        age_row(pool, id).await?;
        execution(pool, id, "running").await?;
        Ok(quarantine::rollback_restore_retirable(pool, id, 1).await?)
    };
    assert!(retirable(None).await?, "the control isn't retirable");
    for target in RESTORE_TARGETS {
        assert!(
            !retirable(Some(HeldTarget::marker(target))).await?,
            "a `{target}` marker is retirable"
        );
        assert!(
            retirable(Some(HeldTarget::episode(target))).await?,
            "a `{target}` episode target isn't retirable"
        );
    }
    Ok(())
}

/// The drain's two age-outs of a draining node's residents spare the
/// system's restore.
#[tokio::test(flavor = "current_thread")]
async fn the_drain_age_outs_read_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "drain::abandon_stale_resident_sandboxes",
        async |held| -> TestResult<_> {
            let node_id = node(pool, Some("draining")).await?;
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            place(pool, id, None, Some(node_id)).await?;
            age_row(pool, id).await?;
            let failed =
                drain::abandon_stale_resident_sandboxes(pool, node_id, age(), &["restoring"])
                    .await?;
            Ok(failed.contains(&(id, "restoring".to_owned())))
        },
    )
    .await?;

    assert_episode_target_reads_as_marker(
        "drain::fail_resident_sandboxes_on_gone_node",
        async |held| -> TestResult<_> {
            let node_id = node(pool, Some("draining")).await?;
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            place(pool, id, Some(node_id), Some(node_id)).await?;
            let failed = drain::fail_resident_sandboxes_on_gone_node(
                pool,
                node_id,
                &["restoring"],
                Some("k8s-uid"),
            )
            .await?;
            Ok(failed.contains(&(id, "restoring".to_owned())))
        },
    )
    .await?;
    Ok(())
}

/// A pinned disk artifact of `id` and its ready, located backup.
async fn pin_disk_artifact(pool: &PgPool, id: SandboxId) -> TestResult {
    let artifact = Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(artifact)
    .execute(pool)
    .await?;
    let snapshot = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO snapshots (
             id, sandbox_id, type, state, local_artifact_id, artifact_kind,
             artifact_origin, org_id
         )
         VALUES ($1, $2, 'disk', 'ready', $3, 'disk', 'stop', $4)"
    ))
    .bind(snapshot)
    .bind(id)
    .bind(artifact)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO snapshot_storage_locations (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, 's3://test-bucket/snapshots/stopped?region=us-east-1', $2)"
    ))
    .bind(snapshot)
    .bind(SEED_ORG)
    .execute(pool)
    .await?;
    Ok(())
}

/// The unplaced restore plan and the disk-artifact source treat a `stopped`
/// target as the stopped re-placement, and no other target.
#[tokio::test(flavor = "current_thread")]
async fn the_restore_sources_read_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "restore_source::restore_source_plan",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "stopped", held).await?;
            Ok(restore_source::restore_source_plan(pool, id)
                .await?
                .map(|plan| plan.stopped_evacuation_unplaced))
        },
    )
    .await?;

    assert_episode_target_reads_as_marker(
        "restore_source::stopped_evacuation_restore_source",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            pin_disk_artifact(pool, id).await?;
            Ok(restore_source::stopped_evacuation_restore_source(pool, id)
                .await?
                .is_some())
        },
    )
    .await?;
    Ok(())
}

/// Portability's source-release predicate takes a marked row's newest ready
/// snapshot over placement freshness, and its drain gate doesn't count the
/// system's `restoring` row against the source node.
#[tokio::test(flavor = "current_thread")]
async fn the_portability_fragments_read_the_episode_target_as_the_marker() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;

    assert_episode_target_reads_as_marker(
        "portability::snapshot_durable_for_source_release (durably_restorable)",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "suspended", held).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET placement_changed_at = '2026-05-01T00:00:00Z' WHERE id = $1"
            ))
            .bind(id)
            .execute(pool)
            .await?;
            pre_restore_snapshot(pool, id).await?;
            Ok(durably_restorable(pool, id).await?)
        },
    )
    .await?;

    // Fresh against the placement but not the newest full snapshot: durable
    // only by the unmarked rule.
    let durable = sql!(format!(
        "SELECT {} FROM sandboxes s WHERE s.id = $1",
        snapshot_durable_for_source_release("s")
    ));
    assert_episode_target_reads_as_marker(
        "portability::snapshot_durable_for_source_release (a newer full row)",
        async |held| -> TestResult<_> {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "suspended", held).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET placement_changed_at = '2026-01-15T00:00:00Z' WHERE id = $1"
            ))
            .bind(id)
            .execute(pool)
            .await?;
            for (state, timestamp) in [
                ("ready", "2026-02-01T00:00:00Z"),
                ("creating", "2026-03-01T00:00:00Z"),
            ] {
                insert_full_snapshot(pool, id.as_uuid(), Uuid::now_v7(), state, timestamp, false)
                    .await?;
            }
            Ok(
                sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(durable.as_str()))
                    .bind(id)
                    .fetch_one(pool)
                    .await?,
            )
        },
    )
    .await?;

    let blockers = sql!(format!(
        "SELECT EXISTS (SELECT 1 FROM ({}) blockers WHERE blockers.sandbox_id = $2)",
        drain_gate_blockers_for_node("$1")
    ));
    assert_episode_target_reads_as_marker(
        "portability::drain_gate_blockers_for_node",
        async |held| -> TestResult<_> {
            let node_id = NodeId::new();
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", held).await?;
            place(pool, id, None, Some(node_id)).await?;
            Ok(
                sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(blockers.as_str()))
                    .bind(node_id)
                    .bind(id)
                    .fetch_one(pool)
                    .await?,
            )
        },
    )
    .await?;
    Ok(())
}

/// The restore claim's three reverts land every row in `suspended`, whichever
/// column holds a target, and each clears the restore episode target.
#[tokio::test(flavor = "current_thread")]
async fn the_restore_claim_reverts_clear_the_episode_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let mut cases = vec![None];
    for target in RESTORE_TARGETS {
        cases.push(Some(HeldTarget::marker(target)));
        cases.push(Some(HeldTarget::episode(target)));
    }
    for revert in [
        "retiring_placement",
        "clearing_placement_clock",
        "lifecycle",
    ] {
        for held in &cases {
            let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", *held).await?;
            let mut tx = pool.begin().await?;
            match revert {
                "retiring_placement" => {
                    restore_lease::revert_restore_claim_retiring_placement(&mut tx, id).await?
                }
                "clearing_placement_clock" => {
                    restore_lease::revert_restore_claim_clearing_placement_clock(&mut tx, id)
                        .await?
                }
                _ => restore_lease::revert_restore_claim_lifecycle(&mut tx, id).await?,
            }
            tx.commit().await?;
            assert_eq!(
                lifecycle_of(pool, id).await?,
                "suspended",
                "{revert} {held:?}"
            );
            assert_eq!(
                read_restore_episode_target(pool, id).await?,
                None,
                "{revert}"
            );
        }
    }
    Ok(())
}

/// What one episode-ending statement did to a row that held a `started`
/// restore episode target: whether the statement matched the row, and the
/// target it left.
async fn assert_ends_episode(
    pool: &PgPool,
    site: &str,
    id: SandboxId,
    matched: bool,
) -> TestResult {
    assert!(matched, "{site} didn't take its fixture's row");
    assert_eq!(
        read_restore_episode_target(pool, id).await?,
        None,
        "{site} left the restore episode target"
    );
    Ok(())
}

/// A row in `lifecycle` holding a `started` episode target, read back so a
/// clear can't pass on a row that never held one.
async fn episode_row(
    pool: &PgPool,
    managed_by: SubstrateKind,
    lifecycle: &str,
) -> TestResult<SandboxId> {
    let id = sandbox(
        pool,
        managed_by,
        lifecycle,
        Some(HeldTarget::episode(EvacuationRestoreTarget::Started)),
    )
    .await?;
    assert_eq!(
        read_restore_episode_target(pool, id).await?.as_deref(),
        Some("started")
    );
    Ok(id)
}

/// Every sandbox-store statement that ends a restore episode clears the
/// target in its own `SET`, with no trigger behind it: the commits, the
/// reverts, the sweeper's fail and converge, the delete, the drain's and the
/// lost node's give-ups, the node's projections and the activation rollback.
#[tokio::test(flavor = "current_thread")]
async fn every_episode_ending_statement_clears_the_target() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let k8s = SubstrateKind::Kubernetes;
    let space = SubstrateKind::Space;

    // The sweeper. A target spares a `restoring` row from the fail CAS, so it
    // fails a `suspending` one.
    let id = episode_row(pool, space, "suspending").await?;
    age_row(pool, id).await?;
    let failed = sweeper::fail_aged_candidate(
        pool,
        id,
        space.as_str(),
        SandboxLifecycle::Suspending,
        age(),
        FailGuard::Aged,
    )
    .await?;
    assert_ends_episode(pool, "sweeper::fail_aged_candidate", id, failed.is_some()).await?;

    let id = episode_row(pool, space, "restoring").await?;
    age_row(pool, id).await?;
    let converged =
        sweeper::converge_restoring(pool, id, space.as_str(), SandboxLifecycle::Started, age())
            .await?;
    assert_ends_episode(pool, "sweeper::converge_restoring", id, converged).await?;

    for lifecycle_only in [false, true] {
        let id = episode_row(pool, space, "restoring").await?;
        pre_restore_snapshot(pool, id).await?;
        age_row(pool, id).await?;
        let reverted = if lifecycle_only {
            sweeper::revert_to_suspended_lifecycle_only(
                pool,
                id,
                space.as_str(),
                SandboxLifecycle::Restoring,
                age(),
                None,
            )
            .await?
        } else {
            sweeper::revert_to_suspended(
                pool,
                id,
                space.as_str(),
                SandboxLifecycle::Restoring,
                age(),
                None,
            )
            .await?
        };
        assert_ends_episode(
            pool,
            &format!("sweeper::revert_to_suspended(lifecycle_only = {lifecycle_only})"),
            id,
            reverted.is_some(),
        )
        .await?;
    }

    // The restore lease: a user takeover of a `suspended` row, and the owned
    // completion. The reverts are `the_restore_claim_reverts_clear_the_episode_target`.
    let id = episode_row(pool, k8s, "suspended").await?;
    let claim = restore_lease::claim_user_restore(pool, id, 60, Uuid::now_v7(), 60, None).await?;
    assert_ends_episode(
        pool,
        "restore_lease::claim_user_restore",
        id,
        claim.is_some(),
    )
    .await?;

    let id = episode_row(pool, space, "restoring").await?;
    let token = Uuid::now_v7();
    set_restore_lease(pool, id, token).await?;
    let completed = restore_lease::complete_owned_user_restore(pool, id, token).await?;
    assert_ends_episode(
        pool,
        "restore_lease::complete_owned_user_restore",
        id,
        completed,
    )
    .await?;

    // The wake completions and the post-action stamps.
    for enforce_fence in [false, true] {
        let id = episode_row(pool, k8s, "restoring").await?;
        let completed = wake::complete_wake(pool, id, enforce_fence).await?;
        assert_ends_episode(pool, "wake::complete_wake", id, completed).await?;
    }
    let id = episode_row(pool, space, "restoring").await?;
    let completed = wake::complete_redriven_wake(pool, id).await?;
    assert_ends_episode(pool, "wake::complete_redriven_wake", id, completed).await?;

    for stamp in [
        ActionStamp::Start(StartSubstrate::ClaimsRestoring),
        ActionStamp::Stop,
    ] {
        let id = episode_row(pool, k8s, "restoring").await?;
        let stamped = action_stamp::stamp_action_lifecycle(pool, id, stamp).await?;
        assert_ends_episode(pool, &format!("action_stamp::{stamp:?}"), id, stamped).await?;
    }
    let id = episode_row(pool, k8s, "restoring").await?;
    let stamped =
        action_stamp::stamp_started_after_start(pool, id, StartSubstrate::ClaimsRestoring).await?;
    assert_ends_episode(pool, "action_stamp::stamp_started_after_start", id, stamped).await?;

    // A restoring create's finalize, and the Direct restore lease's release
    // and completion.
    for lifecycle in ["started", "failed"] {
        let id = episode_row(pool, k8s, "restoring").await?;
        let token = Uuid::now_v7();
        set_restore_lease(pool, id, token).await?;
        let finalized = create::finalize_created(
            pool,
            id,
            lifecycle,
            "on_node",
            Some(NodeId::new()),
            true,
            Some(token),
        )
        .await?;
        assert_ends_episode(
            pool,
            &format!("create::finalize_created({lifecycle})"),
            id,
            finalized,
        )
        .await?;
    }

    let id = episode_row(pool, space, "restoring").await?;
    let token = Uuid::now_v7();
    set_restore_lease(pool, id, token).await?;
    let released = direct::release_restore_lease(pool, id, token, false).await?;
    assert_ends_episode(pool, "direct::release_restore_lease", id, released).await?;
    // A release that leaves the row `restoring` keeps its episode.
    let id = episode_row(pool, space, "restoring").await?;
    let token = Uuid::now_v7();
    set_restore_lease(pool, id, token).await?;
    assert!(direct::release_restore_lease(pool, id, token, true).await?);
    assert_eq!(
        read_restore_episode_target(pool, id).await?.as_deref(),
        Some("started"),
        "a retry's release keeps the row restoring, and its target"
    );

    let id = episode_row(pool, space, "restoring").await?;
    let token = Uuid::now_v7();
    set_restore_lease(pool, id, token).await?;
    let completed = direct::complete_restore_lease(pool, id, token).await?;
    assert_ends_episode(pool, "direct::complete_restore_lease", id, completed).await?;

    // The delete.
    let id = episode_row(pool, k8s, "restoring").await?;
    let mut tx = pool.begin().await?;
    delete::mark_deleting(&mut tx, id).await?;
    tx.commit().await?;
    let deleting = lifecycle_of(pool, id).await? == "deleting";
    assert_ends_episode(pool, "delete::mark_deleting", id, deleting).await?;

    // The drain: its restore finalize, its give-ups and its age-outs. A target
    // spares a `restoring` row from the age-outs, so they fail `suspending`.
    let id = episode_row(pool, k8s, "restoring").await?;
    let finalized = drain::finalize_evacuation_restore(pool, id, SandboxLifecycle::Paused).await?;
    assert_ends_episode(pool, "drain::finalize_evacuation_restore", id, finalized).await?;

    let node_id = node(pool, Some("draining")).await?;
    let id = episode_row(pool, k8s, "suspended").await?;
    place(pool, id, None, Some(node_id)).await?;
    let lost = drain::mark_sandbox_lost(pool, id, node_id).await?;
    assert_ends_episode(pool, "drain::mark_sandbox_lost", id, lost == 1).await?;

    let id = episode_row(pool, k8s, "suspended").await?;
    place(pool, id, None, Some(node_id)).await?;
    let lost = drain::mark_sandbox_lost_on_gone_node(pool, id, node_id, Some("k8s-uid")).await?;
    assert_ends_episode(pool, "drain::mark_sandbox_lost_on_gone_node", id, lost == 1).await?;

    let id = episode_row(pool, k8s, "started").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let failed = drain::mark_sandbox_failed_on_node(pool, id, node_id, &["started"]).await?;
    assert_ends_episode(
        pool,
        "drain::mark_sandbox_failed_on_node",
        id,
        !failed.is_empty(),
    )
    .await?;

    let id = episode_row(pool, k8s, "suspending").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    age_row(pool, id).await?;
    let failed =
        drain::abandon_stale_resident_sandboxes(pool, node_id, age(), &["suspending"]).await?;
    let matched = failed.iter().any(|(failed, _)| *failed == id);
    assert_ends_episode(pool, "drain::abandon_stale_resident_sandboxes", id, matched).await?;

    let id = episode_row(pool, k8s, "suspending").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let failed = drain::fail_resident_sandboxes_on_gone_node(
        pool,
        node_id,
        &["suspending"],
        Some("k8s-uid"),
    )
    .await?;
    let matched = failed.iter().any(|(failed, _)| *failed == id);
    assert_ends_episode(
        pool,
        "drain::fail_resident_sandboxes_on_gone_node",
        id,
        matched,
    )
    .await?;

    // The lost node's give-ups.
    let id = episode_row(pool, k8s, "started").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    execution(pool, id, "recovery_failed").await?;
    let mut tx = pool.begin().await?;
    let lost = lost::mark_started_lost(&mut tx, id, node_id, 1, Duration::from_secs(60)).await?;
    tx.commit().await?;
    assert_ends_episode(pool, "lost::mark_started_lost", id, lost).await?;

    let drained = node(pool, Some("drained")).await?;
    let id = episode_row(pool, k8s, "suspended").await?;
    place(pool, id, None, Some(drained)).await?;
    let mut tx = pool.begin().await?;
    let lost = lost::mark_unrecoverable_sandboxes_lost(
        &mut tx,
        drained,
        Some("k8s-uid"),
        &[id],
        Duration::from_secs(60),
    )
    .await?;
    tx.commit().await?;
    assert_ends_episode(
        pool,
        "lost::mark_unrecoverable_sandboxes_lost",
        id,
        lost == [id],
    )
    .await?;

    // The node's projections: quarantine, failure, suspension, the stopped
    // convergence, the activation receipt and the census tombstone.
    let id = episode_row(pool, k8s, "restoring").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let claimed = Json(json!([{ "sandbox_id": id, "from_lifecycle": "restoring" }]));
    let quarantined = ingest::mark_published_quarantined(pool, node_id, claimed).await?;
    assert_ends_episode(
        pool,
        "ingest::mark_published_quarantined",
        id,
        quarantined == [id],
    )
    .await?;

    let id = episode_row(pool, k8s, "restoring").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let rows = Json(json!([{ "sandbox_id": id, "published_lifecycle": "failed" }]));
    let failed = ingest::mark_published_failed(pool, node_id, rows).await?;
    assert_ends_episode(
        pool,
        "ingest::mark_published_failed",
        id,
        !failed.is_empty(),
    )
    .await?;

    let id = episode_row(pool, k8s, "restoring").await?;
    let candidates = [SuspendedFinalizationCandidate {
        sandbox_id: id,
        clear_marker: false,
        published_artifact: None,
        published_execution_identity: None,
    }];
    let finalized = ingest::finalize_suspended(pool, &candidates).await?;
    assert_ends_episode(
        pool,
        "ingest::finalize_suspended",
        id,
        !finalized.is_empty(),
    )
    .await?;

    let id = episode_row(pool, k8s, "restoring").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let rows = Json(json!([{ "sandbox_id": id, "published_lifecycle": "stopped" }]));
    let converged = ingest::converge_published_stopped(pool, node_id, rows).await?;
    assert_ends_episode(
        pool,
        "ingest::converge_published_stopped",
        id,
        converged == [id],
    )
    .await?;

    let id = episode_row(pool, k8s, "restoring").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let attempt = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, state, pod_ip,
              activation_attempt_id, activation_attempt_target_state, route_activation_id)
         VALUES ($1, 1, $2, 'space', 'running', '10.0.0.1', $3, 'started', $3)"
    ))
    .bind(id)
    .bind(format!("pod-{id}"))
    .bind(attempt)
    .execute(pool)
    .await?;
    let rows = Json(json!([{
        "sandbox_id": id,
        "published_lifecycle": "started",
        "transition_receipt_kind": SandboxTransitionReceiptKind::ActivationCompleted.as_str(),
        "transition_receipt_target_state": "started",
        "transition_receipt_execution_generation": 1,
        "transition_receipt_activation_attempt_id": attempt,
    }]));
    let projected = ingest::release_completed_activations(pool, node_id, rows).await?;
    assert_ends_episode(
        pool,
        "ingest::release_completed_activations",
        id,
        projected == [id],
    )
    .await?;

    let census_node = NodeId::new();
    let id = episode_row(pool, space, "restoring").await?;
    place(pool, id, Some(census_node), Some(census_node)).await?;
    ingest::prune_census_absent_sandboxes(pool, census_node, &[], "network_policy").await?;
    let tombstoned = lifecycle_of(pool, id).await? == "deleted";
    assert_ends_episode(
        pool,
        "ingest::prune_census_absent_sandboxes",
        id,
        tombstoned,
    )
    .await?;

    let id = episode_row(pool, k8s, "restoring").await?;
    place(pool, id, Some(node_id), Some(node_id)).await?;
    let rolled_back = quarantine::converge_activation_rollback(
        pool,
        node_id,
        id,
        SandboxTransitionTargetState::Suspended,
        true,
    )
    .await?;
    assert_ends_episode(
        pool,
        "quarantine::converge_activation_rollback",
        id,
        rolled_back,
    )
    .await?;
    Ok(())
}

/// A restore lease `token` that expired a second ago.
async fn set_restore_lease(pool: &PgPool, id: SandboxId, token: Uuid) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET restore_lease_token = $2,
                restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(id)
    .bind(token)
    .execute(pool)
    .await?;
    Ok(())
}

/// Whether `id` can hold `marker` and `target` together, written in a
/// transaction that is rolled back, or which CHECK refused them.
async fn targets_admitted(
    pool: &PgPool,
    id: SandboxId,
    marker: Option<&str>,
    target: Option<&str>,
) -> TestResult<Result<(), String>> {
    let mut tx = pool.begin().await?;
    let written = sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = $2,
                restore_episode_target = $3
          WHERE id = $1"
    ))
    .bind(id)
    .bind(marker)
    .bind(target)
    .execute(&mut *tx)
    .await;
    tx.rollback().await?;
    match written {
        Ok(done) => {
            assert_eq!(done.rows_affected(), 1);
            Ok(Ok(()))
        }
        Err(sqlx::Error::Database(database)) if database.constraint().is_some() => {
            Ok(Err(database.constraint().unwrap_or_default().to_owned()))
        }
        Err(err) => Err(err.into()),
    }
}

/// The marker and the episode target are never both set, whichever values
/// they hold, and either alone is admitted.
#[tokio::test(flavor = "current_thread")]
async fn the_marker_and_the_episode_target_are_never_both_set() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let pool = &db.pool;
    let id = sandbox(pool, SubstrateKind::Kubernetes, "restoring", None).await?;
    let values = || RESTORE_TARGETS.iter().map(|target| target.as_str());
    for marker in values() {
        for target in values() {
            assert_eq!(
                targets_admitted(pool, id, Some(marker), Some(target)).await?,
                Err("sandboxes_restore_target_exclusive_check".to_owned()),
                "marker {marker} with target {target}"
            );
        }
        assert_eq!(
            targets_admitted(pool, id, Some(marker), None).await?,
            Ok(())
        );
        assert_eq!(
            targets_admitted(pool, id, None, Some(marker)).await?,
            Ok(())
        );
    }
    assert_eq!(targets_admitted(pool, id, None, None).await?, Ok(()));
    Ok(())
}
