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
    TestResult, claim, disk_backup_ready, enqueued, forced_draining_node, insert_backup,
    insert_sandbox, marker_of, set_current_artifact,
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
/// of the pod's create can still boot it. Step 0a settles it before the
/// evacuation opens: it deletes the pod and defers, and the evacuation opens
/// and mints only once the pod is gone, so no boot can drop the pin on what it
/// minted. The teardown retires the execution once the backup it minted, the
/// one the upgrade owed, is durable.
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
        marker_of(&db.pool, sandbox).await?,
        None,
        "step 0a settles the upgrade before the evacuation opens"
    );
    assert_eq!(sink.stopped_path_settles(), [sandbox]);
    assert_eq!(sink.stopped_path_events(), ["delete upgrade pod"]);

    // The pod is gone by the next tick, which opens the evacuation and mints.
    finish_from_the_mint(&db.pool, &sink, sandbox, node_id, pin).await?;
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped"),
        "the evacuation opened once the pod was gone"
    );
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

/// Step 0a settles an expired upgrade on an unmarked row, deleting its pod
/// and abandoning its operation, before any claim re-checks the drain. So it
/// runs only for a row that is still a stopped resident of a node this drain
/// is force-draining. Once the node is undrained, the evacuation leaves the
/// upgrade alone for its retry, and the claim refuses the row.
#[tokio::test(flavor = "current_thread")]
async fn an_undrained_node_never_settles_an_expired_upgrade() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let draining = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, draining, Lease::Expired).await?;
    let undrained_node = forced_draining_node(&db.pool).await?;
    let undrained = insert_sandbox(&db.pool, Some(undrained_node), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, undrained, Lease::Expired).await?;
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = NULL, drain_mode = 'gentle' WHERE id = $1"
    ))
    .bind(undrained_node)
    .execute(&db.pool)
    .await?;
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(
        db.pool.clone(),
        enqueued(LocalArtifactId::new()),
    );

    let outcome = sink
        .evacuate_stopped_sandbox(undrained, undrained_node)
        .await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert!(
        sink.stopped_path_settles().is_empty(),
        "an undrained node's expired upgrade is left for its retry"
    );
    assert_eq!(marker_of(&db.pool, undrained).await?, None);
    assert!(sink.stopped_path_events().is_empty());

    sink.evacuate_stopped_sandbox(draining, node_id).await?;
    assert_eq!(
        sink.stopped_path_settles(),
        [draining],
        "a force-draining node's expired upgrade is settled first"
    );
    Ok(())
}

/// A row whose cluster pin names no durable disk backup is refused at 0b on
/// every tick, whatever its upgrade did, so the drain never evacuates it.
/// Step 0a runs after 0b and leaves such a row's expired upgrade, and its pod,
/// for the upgrade's retry.
#[tokio::test(flavor = "current_thread")]
async fn a_row_refused_on_its_pin_keeps_its_expired_upgrade() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;
    let pin = LocalArtifactId::new();
    set_current_artifact(&db.pool, sandbox, pin, ArtifactKind::Disk).await?;
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(
            outcome,
            StoppedEvacuationOutcome::UnattributableArtifact { local_artifact_id }
                if local_artifact_id == Some(pin)
        ),
        "{outcome:?}"
    );
    assert!(
        sink.stopped_path_settles().is_empty(),
        "0b refuses the row before 0a settles its upgrade"
    );
    assert!(sink.stopped_path_events().is_empty());
    Ok(())
}

/// An evacuation already open, as one an older controller claimed without
/// checking for an upgrade, finds an expired upgrade's pod still running.
/// Step 0a doesn't run on a marked row, so step 1c is what deletes the pod,
/// and the evacuation mints only once the pod is gone.
#[tokio::test(flavor = "current_thread")]
async fn an_open_evacuation_deletes_a_leftover_upgrade_pod_before_it_mints() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, sandbox, node_id).await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_upgrade_pod(db.pool.clone(), enqueued(pin));

    let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
    assert!(
        matches!(outcome, StoppedEvacuationOutcome::Deferred { .. }),
        "{outcome:?}"
    );
    assert!(
        sink.stopped_path_settles().is_empty(),
        "step 0a runs only on an unmarked row"
    );
    assert_eq!(sink.stopped_path_events(), ["delete upgrade pod"]);

    finish_from_the_mint(&db.pool, &sink, sandbox, node_id, pin).await?;
    assert_eq!(
        sink.stopped_path_events(),
        ["delete upgrade pod", "upgrade pod gone", "publish"],
        "the evacuation mints only once the upgrade's pod is gone"
    );
    Ok(())
}

/// An upgrade claimed just before the drain reaches the sandbox holds a live
/// lease, and may still place or boot its pod. The evacuation doesn't open
/// while it runs, so the upgrade can finish; once the lease ends, step 0a
/// settles the upgrade, deleting its pod, and the evacuation opens once the
/// pod is gone and finishes. The marker it sets refuses any new claim of the upgrade
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
    assert_eq!(marker_of(&db.pool, sandbox).await?, None);
    assert_eq!(sink.stopped_path_events(), ["delete upgrade pod"]);
    finish_from_the_mint(&db.pool, &sink, sandbox, node_id, pin).await?;
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped")
    );
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
    // An upgrade execution and pod that only a claim opened before the
    // marker, by an older controller, could have let through.
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

/// Move `sandbox` to `lifecycle`, to show which row holds a node's gates.
async fn set_lifecycle(pool: &PgPool, sandbox: SandboxId, lifecycle: &str) -> TestResult {
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(sandbox)
        .bind(lifecycle)
        .execute(pool)
        .await?;
    Ok(())
}

async fn drain_phase_of(pool: &PgPool, node: NodeId) -> TestResult<Option<String>> {
    Ok(
        sqlx::query_scalar(sql!("SELECT drain_phase FROM nodes WHERE id = $1"))
            .bind(node)
            .fetch_one(pool)
            .await?,
    )
}

/// Run the drained-node terminator's own `drained → terminating` write and
/// answer whether it advanced the node.
async fn terminator_would_advance(pool: &PgPool, node: NodeId) -> TestResult<bool> {
    let query = crate::drained_node_terminator::store::mark_terminating_query(
        ResidencyEvidence::NodeCensusAware,
    );
    let updated = sqlx::query(sqlx::AssertSqlSafe(sql!(query.as_str())))
        .bind(node)
        .bind(PHASE_TERMINATING)
        .bind(PHASE_DRAINED)
        .execute(pool)
        .await?;
    Ok(updated.rows_affected() == 1)
}

/// A node that refuses, as invalid, to abandon an expired upgrade's operation
/// leaves a sandbox the evacuation can't capture. The drain reports it as
/// needing an operator, asks the node once, and keeps the node: neither the
/// drain's `draining → drained` write nor the terminator's `drained →
/// terminating` write goes through while the sandbox's only copy is there. A
/// neighbour on the same node still evacuates.
#[tokio::test(flavor = "current_thread")]
async fn a_refused_abandon_needs_an_operator_and_holds_the_node() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let refused = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    unfinished_upgrade_execution(&db.pool, refused, Lease::Expired).await?;
    let neighbour = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    let pin = LocalArtifactId::new();
    let sink = SubstrateSink::with_stopped_path_refused_abandon(db.pool.clone(), enqueued(pin));

    for newly in [true, false] {
        let outcome = sink.evacuate_stopped_sandbox(refused, node_id).await?;
        assert!(
            matches!(
                outcome,
                StoppedEvacuationOutcome::UpgradeAbandonRefused { newly_refused, .. }
                    if newly_refused == newly
            ),
            "{outcome:?}"
        );
    }
    assert_eq!(
        sink.stopped_path_events(),
        ["abandon refused"],
        "the node is asked to abandon the operation once"
    );
    assert_eq!(marker_of(&db.pool, refused).await?, None);

    finish_from_the_mint(&db.pool, &sink, neighbour, node_id, pin).await?;
    assert_eq!(
        sink.stopped_path_events(),
        ["abandon refused", "publish"],
        "the neighbour evacuated, and the refused sandbox's node wasn't asked again"
    );

    // The faked teardown leaves out the placement clear production's does
    // (`StoppedEvacuationOutcome::Released`), so the released neighbour is
    // taken off the node here, leaving the refused sandbox the only resident.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET active_node_id = NULL, target_node_id = NULL, placement = 'unplaced'
          WHERE id = $1"
    ))
    .bind(neighbour)
    .execute(&db.pool)
    .await?;
    assert!(
        !NodeDrainSink::set_drain_phase(&sink, node_id, PHASE_DRAINING, PHASE_DRAINED, None)
            .await?,
        "the refused sandbox's only copy holds the node in draining"
    );
    assert_eq!(
        drain_phase_of(&db.pool, node_id).await?.as_deref(),
        Some(PHASE_DRAINING)
    );
    // It's the refused sandbox that holds both edges: without it the drain
    // completes, and the terminator advances only once it's gone again.
    set_lifecycle(&db.pool, refused, "failed").await?;
    assert!(
        NodeDrainSink::set_drain_phase(&sink, node_id, PHASE_DRAINING, PHASE_DRAINED, None).await?
    );
    set_lifecycle(&db.pool, refused, "stopped").await?;
    assert!(
        !terminator_would_advance(&db.pool, node_id).await?,
        "the refused sandbox's only copy keeps the node from terminating"
    );
    set_lifecycle(&db.pool, refused, "failed").await?;
    assert!(terminator_would_advance(&db.pool, node_id).await?);
    Ok(())
}

/// The same refusal on an evacuation already open, where step 1c meets it:
/// reported as needing an operator, and the node asked once.
#[tokio::test(flavor = "current_thread")]
async fn an_open_evacuations_refused_abandon_is_asked_once() -> TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };
    let node_id = forced_draining_node(&db.pool).await?;
    let sandbox = insert_sandbox(&db.pool, Some(node_id), "stopped").await?;
    claim(&db.pool, sandbox, node_id).await?;
    unfinished_upgrade_execution(&db.pool, sandbox, Lease::Expired).await?;
    let sink = SubstrateSink::with_stopped_path_refused_abandon(
        db.pool.clone(),
        enqueued(LocalArtifactId::new()),
    );

    for newly in [true, false] {
        let outcome = sink.evacuate_stopped_sandbox(sandbox, node_id).await?;
        assert!(
            matches!(
                outcome,
                StoppedEvacuationOutcome::UpgradeAbandonRefused { newly_refused, .. }
                    if newly_refused == newly
            ),
            "{outcome:?}"
        );
    }
    assert_eq!(sink.stopped_path_events(), ["abandon refused"]);
    assert!(sink.stopped_path_settles().is_empty());
    assert_eq!(
        marker_of(&db.pool, sandbox).await?.as_deref(),
        Some("stopped"),
        "the open evacuation stays open, holding its claim"
    );
    Ok(())
}
