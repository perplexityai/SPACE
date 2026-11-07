//! Postgres-backed coverage for a stopped evacuation of a sandbox whose
//! stay-stopped runtime upgrade didn't finish.
//!
//! Such an upgrade leaves an active execution created after the stop, with
//! the upgrade recorded on it, and a pod whose create kubelet keeps retrying,
//! each retry a boot of the upgrade. Two things used to stall the drain on
//! it: the teardown refused to retire an execution newer than the stop, and
//! a boot after the evacuation minted its backup dropped the node's pin on
//! it, which the teardown requires. Each case below drives the evacuation
//! tick by tick to the release.

use super::stopped_evacuation_tests::{
    TestResult, enqueued, forced_draining_node, insert_backup, insert_sandbox, marker_of,
};
use super::*;

/// How the seeded upgrade's claim stands.
#[derive(Clone, Copy)]
enum Lease {
    Live,
    Expired,
}

/// Seed the execution an unfinished stay-stopped runtime upgrade leaves on
/// `sandbox`: created after its stop, its upgrade recorded, its claim
/// targeting `stopped`.
async fn unfinished_upgrade_execution(
    pool: &PgPool,
    sandbox: SandboxId,
    lease: Lease,
) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle_changed_at = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    let expires_in_secs: i64 = match lease {
        Lease::Live => 3600,
        Lease::Expired => -60,
    };
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, k8s_node_name, state,
              active, activation_attempt_id, activation_attempt_expires_at,
              activation_attempt_target_state, runtime_upgrade_operation_id,
              runtime_upgrade_target_environment_ref, runtime_upgrade_authorization_id)
         VALUES ($1, 3, $2, 'space', 'upgrade-pod-uid', 'worker-a', 'created', TRUE, $3,
                 NOW() + make_interval(secs => $4), 'stopped', $5, $6,
                 'runtime-environment-test:e1')"
    ))
    .bind(sandbox)
    .bind(format!("space-sbx-{}-g3", sandbox.as_uuid().simple()))
    .bind(uuid::Uuid::now_v7())
    .bind(expires_in_secs as f64)
    .bind(uuid::Uuid::now_v7())
    .bind(format!("sha256:{}", "e1".repeat(32)))
    .execute(pool)
    .await?;
    Ok(())
}

async fn end_the_lease(pool: &PgPool, sandbox: SandboxId) -> TestResult {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(sandbox)
    .execute(pool)
    .await?;
    Ok(())
}

/// The upgrade execution's claim and activity, to show nothing touched it.
async fn upgrade_execution_of(
    pool: &PgPool,
    sandbox: SandboxId,
) -> TestResult<Option<(bool, Option<uuid::Uuid>, bool)>> {
    Ok(sqlx::query_as(sql!(
        "SELECT active, activation_attempt_id, activation_attempt_expires_at > NOW()
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND runtime_upgrade_operation_id IS NOT NULL"
    ))
    .bind(sandbox)
    .fetch_optional(pool)
    .await?)
}

async fn disk_backup_ready(pool: &PgPool, sandbox: SandboxId) -> TestResult<bool> {
    let ready = current_disk_artifact_backup_ready("s");
    let query = format!("SELECT {ready} FROM sandboxes s WHERE s.id = $1");
    Ok(
        sqlx::query_scalar::<_, bool>(sqlx::AssertSqlSafe(sql!(query.as_str())))
            .bind(sandbox)
            .fetch_one(pool)
            .await?,
    )
}

/// Drive the evacuation from its mint to its release: the tick that mints
/// waits on the upload, and once the backup is durable the next releases the
/// node, retiring the upgrade's execution with the pre-stop one.
async fn finish_from_the_mint(
    db_pool: &PgPool,
    sink: &SubstrateSink,
    sandbox: SandboxId,
    node_id: NodeId,
    pin: LocalArtifactId,
) -> TestResult {
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id }
            if local_artifact_id == pin),
        "{outcome:?}"
    );
    insert_backup(
        db_pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Released { local_artifact_id }
            if local_artifact_id == Some(pin)),
        "the teardown must retire the upgrade's execution and release the node; got {outcome:?}"
    );
    assert_eq!(sink.recorded_teardowns(), vec![sandbox]);
    assert!(
        disk_backup_ready(db_pool, sandbox).await?,
        "the evacuation's backup stands in for the one the upgrade owed"
    );
    if let Some((active, _, _)) = upgrade_execution_of(db_pool, sandbox).await? {
        assert!(!active, "the teardown retires the upgrade's execution");
    }
    Ok(())
}

/// A stay-stopped upgrade that finished, on any outcome, retires its
/// execution and leaves the sandbox placed on the node holding its disk
/// (`retire_stopped_upgrade_execution` in the api crate, whose tests check
/// the placement on each outcome). On that shape the drain still offers the
/// sandbox for evacuation: the listing reads `active_node_id`, not the
/// execution.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_upgrade_retired_is_still_offered_for_evacuation() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET active = FALSE, activation_attempt_id = NULL,
                activation_attempt_expires_at = NULL, activation_attempt_target_state = NULL
          WHERE sandbox_id = $1"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;

    let offered =
        list_evacuatable_sandbox_rows(&db.pool, node_id, ResidencyEvidence::LifecycleOnly, true)
            .await?;

    assert_eq!(
        offered,
        vec![(sandbox, SandboxLifecycle::Stopped)],
        "the drain must still evacuate a sandbox whose upgrade retired"
    );
    Ok(())
}

/// The upgrade's lease ended with its pod still there, so kubelet's retries
/// of the pod's create can still boot it. The evacuation takes the execution
/// over: it deletes the pod and mints only once the pod is gone, so no boot
/// can drop the pin on what it minted, and the teardown retires the
/// execution once the backup it minted, the one the upgrade owed, is durable.
#[tokio::test(flavor = "current_thread")]
async fn a_leftover_upgrade_pod_is_gone_before_the_evacuation_mints() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped"),
        "the evacuation opened, which refuses the upgrade from here on"
    );
    assert_eq!(sink.stopped_path_events(), ["delete upgrade pod"]);

    // The pod is gone by the next tick, which mints.
    finish_from_the_mint(&db.pool, &sink, sandbox, node_id, pin).await?;
    assert_eq!(
        sink.stopped_path_events(),
        ["delete upgrade pod", "upgrade pod gone", "publish"],
        "the evacuation mints only once the upgrade's pod is gone"
    );
    assert_eq!(
        upgrade_execution_of(&db.pool, sandbox)
            .await?
            .map(|(active, _, _)| active),
        Some(false),
        "the teardown retired the upgrade's execution"
    );
    Ok(())
}

/// An upgrade claimed just before the drain reaches the sandbox holds a live
/// lease, and may still place or boot its pod. The evacuation doesn't open
/// while it runs, so the upgrade can finish; once the lease ends, the
/// evacuation opens, takes the execution over, waits out the pod and
/// finishes. The marker it sets refuses any new claim of the upgrade
/// (`record_stopped_runtime_upgrade_in`'s own test in the api crate).
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_claimed_before_the_drain_opens_finishes_or_ends_first() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Live).await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert_eq!(marker_of(&db.pool, sandbox).await?, None);
    assert!(sink.stopped_path_events().is_empty());

    end_the_lease(&db.pool, sandbox).await?;
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped")
    );
    finish_from_the_mint(&db.pool, &sink, sandbox, node_id, pin).await?;
    assert_eq!(
        sink.stopped_path_events(),
        ["delete upgrade pod", "upgrade pod gone", "publish"]
    );
    assert_eq!(
        upgrade_execution_of(&db.pool, sandbox)
            .await?
            .map(|(active, _, _)| active),
        Some(false)
    );
    Ok(())
}

/// While an upgrade's lease runs, the evacuation leaves that sandbox alone,
/// claiming nothing and touching neither the execution nor its pod, and
/// moves on: a healthy stopped sandbox on the same node evacuates.
#[tokio::test(flavor = "current_thread")]
async fn a_live_upgrade_defers_only_its_own_sandbox() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let upgrading = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, upgrading, Lease::Live).await?;
    let held = upgrade_execution_of(&db.pool, upgrading).await?;
    let healthy = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    let outcome = sink.evacuate_stopped_sandbox(upgrading, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    finish_from_the_mint(&db.pool, &sink, healthy, node_id, pin).await?;

    assert_eq!(marker_of(&db.pool, upgrading).await?, None);
    assert_eq!(upgrade_execution_of(&db.pool, upgrading).await?, held);
    assert_eq!(held.map(|(_, _, live)| live), Some(true));
    assert_eq!(
        sink.stopped_path_events(),
        ["publish"],
        "only the healthy sandbox's backup, and no upgrade pod deleted"
    );
    Ok(())
}

/// An evacuation resumed with its backup already durable goes straight to
/// its teardown, which retires an expired upgrade's execution. It waits out
/// that upgrade's pod first, as the tick that mints does: the teardown
/// mustn't retire the execution while kubelet can still boot the upgrade.
#[tokio::test(flavor = "current_thread")]
async fn a_resumed_durable_evacuation_waits_out_a_leftover_upgrade_pod() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    // The evacuation mints, and its backup becomes durable.
    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::AwaitingDurableDisk { local_artifact_id }
            if local_artifact_id == pin),
        "{outcome:?}"
    );
    insert_backup(
        &db.pool,
        sandbox,
        pin,
        ArtifactKind::Disk,
        "disk",
        "ready",
        false,
    )
    .await?;
    // An upgrade that predates the marker left its execution and pod.
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert!(sink.recorded_teardowns().is_empty());
    assert_eq!(
        upgrade_execution_of(&db.pool, sandbox)
            .await?
            .map(|(active, _, _)| active),
        Some(true),
        "the upgrade's execution stays while its pod may still boot"
    );

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Released { local_artifact_id }
            if local_artifact_id == Some(pin)),
        "{outcome:?}"
    );
    assert_eq!(
        sink.stopped_path_events(),
        ["publish", "delete upgrade pod", "upgrade pod gone"],
        "the teardown runs only once the upgrade's pod is gone"
    );
    assert_eq!(sink.recorded_teardowns(), vec![sandbox]);
    assert_eq!(
        upgrade_execution_of(&db.pool, sandbox)
            .await?
            .map(|(active, _, _)| active),
        Some(false)
    );
    Ok(())
}
