//! Route test for `GET /runtime-environments/population`: which record
//! binds each sandbox, on each substrate.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn population_counts_each_sandbox_by_the_record_it_runs_from() -> TestResult {
    let Some(h) = harness().await? else {
        return Ok(());
    };
    let (e0, e1) = (environment(0), environment(1));
    h.set_default(e1).await;
    let node = NodeId::new();
    insert_ready_worker_node(&h.db.pool, node).await?;

    // Running on E1, as its node reports.
    let started = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(&h.db.pool, started, node, Some(metadata(Some(e1)))).await?;
    // Legacy: observed, but the record carries no environment.
    let legacy = insert_sandbox(&h.db.pool, "stopped").await?;
    observe_sandbox(&h.db.pool, legacy, node, Some(metadata(None))).await?;
    // Pre-metadata observation: no metadata at all, still a record.
    let pre_metadata = insert_sandbox(&h.db.pool, "paused").await?;
    observe_sandbox(&h.db.pool, pre_metadata, node, None).await?;
    // Not reported by its node yet.
    insert_sandbox(&h.db.pool, "creating").await?;
    // Suspended with no artifact marker: the newest ready snapshot with a
    // storage location decides (E0), not an older one, a newer one with no
    // location, one still uploading, or a stale observation left behind.
    let suspended = insert_sandbox(&h.db.pool, "suspended").await?;
    observe_sandbox(&h.db.pool, suspended, node, Some(metadata(Some(e1)))).await?;
    for (snapshot, environment) in [
        (("ready", Some("suspend"), 300, true), e1),
        (("ready", None, 200, true), e0),
        (("ready", Some("suspend"), 100, false), e1),
        (("creating", Some("suspend"), 10, true), e1),
    ] {
        insert_snapshot(&h.db.pool, suspended, snapshot, metadata(Some(environment))).await?;
    }
    // Marked: restore uses the snapshot holding the current local artifact
    // (E0), even with a newer ready one (E1) beside it.
    let marked = insert_sandbox(&h.db.pool, "suspended").await?;
    let artifact = insert_snapshot(
        &h.db.pool,
        marked,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    insert_snapshot(
        &h.db.pool,
        marked,
        ("ready", Some("suspend"), 10, true),
        metadata(Some(e1)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, marked, artifact).await?;
    // Marked, but the marked snapshot's location is orphaned: restore has no
    // source, so its environment can't be told.
    let orphaned = insert_sandbox(&h.db.pool, "suspended").await?;
    let artifact = insert_snapshot(
        &h.db.pool,
        orphaned,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e1)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, orphaned, artifact).await?;
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW()
         WHERE snapshot_id IN (SELECT id FROM snapshots WHERE sandbox_id = $1)"
    ))
    .bind(orphaned)
    .execute(&h.db.pool)
    .await?;
    // Unmarked, with only a snapshot from before its last placement: a
    // previous cycle's bytes, which restore won't use.
    let stale_cycle = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_snapshot(
        &h.db.pool,
        stale_cycle,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e1)),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET placement_changed_at = NOW() - INTERVAL '1 minute' WHERE id = $1"
    ))
    .bind(stale_cycle)
    .execute(&h.db.pool)
    .await?;
    // Restoring: bound by the snapshot a retry restores from (E0), the newest
    // from before the restore began, not by one a failed branch published
    // since (E1) or by the observation its node published before the suspend.
    let restoring = insert_sandbox(&h.db.pool, "restoring").await?;
    observe_sandbox(&h.db.pool, restoring, node, Some(metadata(Some(e1)))).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '100 seconds' WHERE id = $1"
    ))
    .bind(restoring)
    .execute(&h.db.pool)
    .await?;
    for (snapshot, environment) in [
        (("ready", Some("suspend"), 300, true), e0),
        (("ready", Some("suspend"), 50, true), e1),
    ] {
        insert_snapshot(&h.db.pool, restoring, snapshot, metadata(Some(environment))).await?;
    }
    // Evacuated by a node drain: restore takes the newest full snapshot (E1),
    // even though it predates the last placement.
    let evacuated = insert_sandbox(&h.db.pool, "suspended").await?;
    insert_snapshot(
        &h.db.pool,
        evacuated,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e1)),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'started',
                placement_changed_at = NOW() - INTERVAL '1 minute'
          WHERE id = $1"
    ))
    .bind(evacuated)
    .execute(&h.db.pool)
    .await?;
    // Restoring under an evacuation: the newest full snapshot decides (E0),
    // not the pre-restore source a retry would use (E1).
    let evacuating = insert_sandbox(&h.db.pool, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'started',
                lifecycle_changed_at = NOW() - INTERVAL '100 seconds'
          WHERE id = $1"
    ))
    .bind(evacuating)
    .execute(&h.db.pool)
    .await?;
    for (snapshot, environment) in [
        (("ready", Some("suspend"), 300, true), e1),
        (("ready", Some("suspend"), 50, true), e0),
    ] {
        insert_snapshot(
            &h.db.pool,
            evacuating,
            snapshot,
            metadata(Some(environment)),
        )
        .await?;
    }
    // Direct restores from its source node's artifact: the newest full
    // snapshot in any upload state decides (E0), even over an older ready one.
    let direct = insert_sandbox_on(&h.db.pool, SubstrateKind::Space, "suspended").await?;
    for (snapshot, environment) in [
        (("ready", Some("suspend"), 300, true), e1),
        (("failed", Some("suspend"), 10, false), e0),
    ] {
        insert_snapshot(&h.db.pool, direct, snapshot, metadata(Some(environment))).await?;
    }
    // A Direct retry uses the pre-restore source (E0), not a later attempt.
    let direct_restoring = insert_sandbox_on(&h.db.pool, SubstrateKind::Space, "restoring").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '100 seconds' WHERE id = $1"
    ))
    .bind(direct_restoring)
    .execute(&h.db.pool)
    .await?;
    for (snapshot, environment) in [
        (("creating", Some("suspend"), 300, false), e0),
        (("ready", Some("suspend"), 50, true), e1),
    ] {
        insert_snapshot(
            &h.db.pool,
            direct_restoring,
            snapshot,
            metadata(Some(environment)),
        )
        .await?;
    }
    // Failed with a durable suspend snapshot: the repair loop can revert it,
    // so it can run again, restored from that snapshot (E0) rather than as its
    // last observation (E1) says. Failed with nothing to revert to can't.
    let repairable = insert_sandbox(&h.db.pool, "failed").await?;
    observe_failed(&h.db.pool, repairable, node, Some(metadata(Some(e1)))).await?;
    insert_snapshot(
        &h.db.pool,
        repairable,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    // Its observation predates the failure, so it's no evidence: unrecorded.
    let gone = insert_sandbox(&h.db.pool, "failed").await?;
    observe_failed(&h.db.pool, gone, node, Some(metadata(Some(e1)))).await?;
    // Its node still reports it live (E1): bound by that, and it can run.
    // A durable snapshot (E0) beside it doesn't decide: the VM is running.
    let live = insert_sandbox(&h.db.pool, "failed").await?;
    observe_sandbox(&h.db.pool, live, node, Some(metadata(Some(e1)))).await?;
    insert_snapshot(
        &h.db.pool,
        live,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    // Observed as failed but with a pid: the repair loop reads that as live,
    // so it's bound by the observation (E1) and can run.
    let live_pid = insert_sandbox(&h.db.pool, "failed").await?;
    observe_failed(&h.db.pool, live_pid, node, Some(metadata(Some(e1)))).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_observed_states SET pid = 4242 WHERE sandbox_id = $1"
    ))
    .bind(live_pid)
    .execute(&h.db.pool)
    .await?;
    // No observation, but an execution still active: may be running, on an
    // environment nothing here records, so unrecorded and it can run.
    let live_execution = insert_sandbox(&h.db.pool, "failed").await?;
    insert_snapshot(
        &h.db.pool,
        live_execution,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions (sandbox_id, generation, pod_name, pod_namespace)
         VALUES ($1, 1, 'synthetic-pod', 'synthetic')"
    ))
    .bind(live_execution)
    .execute(&h.db.pool)
    .await?;
    // Direct runs no repair loop, so a durable snapshot doesn't bring it back.
    let direct_failed = insert_sandbox_on(&h.db.pool, SubstrateKind::Space, "failed").await?;
    insert_snapshot(
        &h.db.pool,
        direct_failed,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    // Repairable with a marker no backup holds: repair clears the marker and
    // restores from the eligible snapshot (E0).
    let stale_marker = insert_sandbox(&h.db.pool, "failed").await?;
    insert_snapshot(
        &h.db.pool,
        stale_marker,
        ("ready", Some("suspend"), 300, true),
        metadata(Some(e0)),
    )
    .await?;
    mark_current_artifact(&h.db.pool, stale_marker, Some(Uuid::now_v7())).await?;
    // Repairable, but its snapshot predates recorded metadata: the snapshot
    // still decides, as unbound, not the observation (E0) left beside it.
    let legacy_snapshot = insert_sandbox(&h.db.pool, "failed").await?;
    observe_failed(&h.db.pool, legacy_snapshot, node, Some(metadata(Some(e0)))).await?;
    insert_snapshot(
        &h.db.pool,
        legacy_snapshot,
        ("ready", Some("suspend"), 300, true),
        metadata(None),
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE snapshots SET execution_metadata = NULL WHERE sandbox_id = $1"
    ))
    .bind(legacy_snapshot)
    .execute(&h.db.pool)
    .await?;
    // Quarantined can be recovered, so it can run; it's bound by the
    // observation of the execution that recovery would resume (E1).
    let quarantined = insert_sandbox(&h.db.pool, "quarantined").await?;
    observe_sandbox(&h.db.pool, quarantined, node, Some(metadata(Some(e1)))).await?;
    // Bound for cleanup, or mid-delete: reported, but neither runs again.
    insert_sandbox(&h.db.pool, "cleanup_required").await?;
    insert_sandbox(&h.db.pool, "deleting").await?;
    // Metadata that isn't an object: unreadable, not legacy, though it has
    // no `environment` key to read.
    let malformed = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(
        &h.db.pool,
        malformed,
        node,
        Some(json!(["not", "metadata"])),
    )
    .await?;
    // An explicit null environment is unbound, as an absent one is.
    let null_environment = insert_sandbox(&h.db.pool, "started").await?;
    observe_sandbox(
        &h.db.pool,
        null_environment,
        node,
        Some(json!({"environment": null})),
    )
    .await?;
    // A deletion tombstone isn't work, even with its last observation intact.
    let deleted = insert_sandbox(&h.db.pool, "deleted").await?;
    observe_sandbox(&h.db.pool, deleted, node, Some(metadata(Some(e1)))).await?;

    insert_template_release(&h.db.pool, "bound", Some(metadata(Some(e1)))).await?;
    insert_template_release(&h.db.pool, "bound", Some(metadata(Some(e1)))).await?;
    insert_template_release(&h.db.pool, "legacy", None).await?;
    insert_template_release(&h.db.pool, "unreadable", Some(json!(["not", "metadata"]))).await?;

    let (status, body) = h.get("/admin/api/v1/runtime-environments/population").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body,
        json!({
            "generation": 1,
            "environments": [
                {
                    "environment_ref": e0.to_string(),
                    "state": null,
                    "is_default": false,
                    "sandboxes": 8,
                    "sandboxes_by_lifecycle": {"failed": 2, "restoring": 3, "suspended": 3},
                    "sandboxes_not_runnable": 0,
                    "template_releases": 0, "template_releases_in_use": 0,
                },
                {
                    "environment_ref": e1.to_string(),
                    "state": "allowed",
                    "is_default": true,
                    "sandboxes": 5,
                    "sandboxes_by_lifecycle": {"failed": 2, "quarantined": 1, "started": 1, "suspended": 1},
                    "sandboxes_not_runnable": 0,
                    "template_releases": 2, "template_releases_in_use": 0,
                },
            ],
            "unbound": {
                "sandboxes": 4,
                "sandboxes_by_lifecycle": {"failed": 1, "paused": 1, "started": 1, "stopped": 1},
                    "sandboxes_not_runnable": 0,
                "template_releases": 1, "template_releases_in_use": 0,
            },
            "unrecorded": {
                "sandboxes": 8,
                "sandboxes_by_lifecycle": {"cleanup_required": 1, "creating": 1, "deleting": 1, "failed": 3, "suspended": 2},
                    "sandboxes_not_runnable": 4,
                "template_releases": 0, "template_releases_in_use": 0,
            },
            "invalid": {"sandboxes": 1, "sandboxes_by_lifecycle": {"started": 1},
                    "sandboxes_not_runnable": 0, "template_releases": 1, "template_releases_in_use": 0},
        })
    );
    Ok(())
}
