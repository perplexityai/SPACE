//! The upgrade's pods and generations: which pod an attempt reuses, replaces or
//! never adopts, and how an attempt interrupted around its pod's create
//! converges.

use super::*;

/// An upgrade that didn't finish keeps its execution and its pod, a
/// placeholder that runs no microVM, for its retry: one whose backup wasn't
/// enqueued, or one that ended between creating its pod and persisting it,
/// which leaves the execution `creating`. A user start that comes first never
/// reuses that pod, whose Wake couldn't launch the sandbox: it replaces the
/// pod under a new execution and Wakes only that one. Nor does a start that
/// renders no upgrade reuse an earlier start's pod carrying one, persisted or
/// not, while the sandbox is still stopped: that pod would boot under the
/// upgrade's target anyway.
#[rstest::rstest]
#[case::backup_unconfirmed(None, true)]
#[case::pod_unpersisted(Some(PodRuntimeUpgradeTargetState::Stopped), false)]
#[case::a_crashed_starts_upgrade(Some(PodRuntimeUpgradeTargetState::Started), true)]
#[case::a_crashed_starts_unpersisted_upgrade(Some(PodRuntimeUpgradeTargetState::Started), false)]
#[tokio::test(flavor = "current_thread")]
async fn a_start_never_reuses_an_unfinished_upgrades_placeholder_pod(
    #[case] left_by: Option<PodRuntimeUpgradeTargetState>,
    #[case] persisted: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    // The upgrade's own enqueue, when it runs here, fails; the start's, of
    // the backup the upgrade left owed, succeeds.
    let mut s = subject_whose_backups(if left_by.is_none() {
        ScriptedReply::Sequence(
            vec![
                ScriptedReply::Status(tonic::Status::internal("backup queue full")),
                backup_enqueued(),
            ]
            .into(),
        )
    } else {
        backup_enqueued()
    })
    .await?;
    if let Some(target_state) = left_by {
        let target = target_state.as_str();
        held_execution(&s, target, -3600).await?;
        // Only a stopped sandbox's upgrade records its operation.
        let operation = match target_state {
            PodRuntimeUpgradeTargetState::Stopped => record_upgrade_on_held_execution(&s).await?,
            PodRuntimeUpgradeTargetState::Started => uuid::Uuid::now_v7(),
        };
        s.pods().annotate(&[
            (
                SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
                &operation.to_string(),
            ),
            (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, target),
        ]);
        if !persisted {
            unpersist_held_execution(&s).await?;
        }
    } else {
        s.upgrade().await.unwrap_err();
        s.assert_kept_for_a_retry("after the failed enqueue")
            .await?;
    }
    let (upgraded_generation, _, _) = s.active_execution().await?.expect("the kept execution");
    s.cache_the_pod();

    // A stopped sandbox's upgrade left its backup owed. Its pod goes first,
    // since kubelet may still retry its create, and the start waits for a
    // retry once it's gone, before capturing anything.
    if !matches!(left_by, Some(PodRuntimeUpgradeTargetState::Started)) {
        let error = start_the_sandbox(&s).await.map(drop).unwrap_err();
        let SubstrateError::Unavailable { message } = &error else {
            panic!("{error:?}");
        };
        assert!(message.contains("gone; retry"), "{message}");
        assert!(!s.pods().exists(), "the upgrade's pod is still there");
        // The replica's reflector sees the pod go.
        s.cache_the_pod();
    }

    // The start doesn't need to finish: only the pod it Wakes matters.
    let _ = start_the_sandbox(&s).await;

    let checkpoints = s
        .pods()
        .created_annotations(pplx_space_common_core::kubernetes::SANDBOX_CHECKPOINT_ANNOTATION);
    // The upgrade's pod, if it created one here, then the start's.
    assert_eq!(
        checkpoints.len(),
        1 + usize::from(left_by.is_none()),
        "{checkpoints:?}"
    );
    assert_ne!(
        checkpoints.last().cloned().flatten().as_deref(),
        Some(pplx_space_common_core::kubernetes::RUNTIME_UPGRADE_CHECKPOINT),
        "the start's pod boots the upgrade"
    );
    let (generation, target, _) = s.active_execution().await?.expect("the start's execution");
    assert!(generation > upgraded_generation, "{generation}");
    assert_eq!(target.as_deref(), Some("started"));
    // A stopped sandbox's upgrade that recorded itself left a backup owed,
    // published or not, which the start enqueued before claiming the
    // execution: after the upgrade's own failed one, or alone. A crashed
    // start's upgrade records nothing.
    assert_eq!(
        s.enqueued_backups().len(),
        match left_by {
            None => 2,
            Some(PodRuntimeUpgradeTargetState::Stopped) => 1,
            Some(PodRuntimeUpgradeTargetState::Started) => 0,
        }
    );
    let wakes = s.node().wakes.lock().unwrap().clone();
    assert!(!wakes.is_empty(), "the start never woke a pod");
    assert!(
        wakes.iter().all(|wake| {
            wake.start_context
                .as_ref()
                .and_then(|context| context.expected_execution_generation)
                != Some(upgraded_generation)
        }),
        "the start woke the upgrade's placeholder: {wakes:?}"
    );
    Ok(())
}

/// A start or resume upgrade's pod stays the sandbox's running pod once the
/// sandbox has left `stopped`. A materialization that renders no upgrade, a
/// start of the started sandbox here, keeps it rather than replacing it as an
/// earlier start's.
#[tokio::test(flavor = "current_thread")]
async fn a_started_sandbox_keeps_the_pod_its_upgrade_booted() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "started", -3600).await?;
    s.pods().annotate(&[
        (
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
            &uuid::Uuid::now_v7().to_string(),
        ),
        (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "started"),
    ]);
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    s.cache_the_pod();

    // The start doesn't need to finish: only what it does with the pod
    // matters.
    let _ = s
        .substrate
        .start_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::RecoveryCharge::exempt_for_tests(),
        )
        .await;

    assert!(s.pods().exists());
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 0);
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 0);
    Ok(())
}

/// An earlier attempt claimed the execution and recorded its upgrade, then
/// ended before its pod's create was sent: the execution is `creating`, with
/// no pod at its name. The retry's takeover and claim refresh the execution,
/// so it never ages into a stale create; the retry creates the pod itself,
/// carrying the recorded upgrade, and converges in one call rather than
/// waiting on a create that will never come.
#[tokio::test(flavor = "current_thread")]
async fn a_recorded_execution_whose_pod_was_never_created_gets_it_on_a_retry()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    *s.pods().pod.lock().unwrap() = None;
    unpersist_held_execution(&s).await?;

    assert_eq!(upgraded(&s.upgrade().await?), operation);

    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    assert_eq!(s.enqueued_backups().len(), 1);
    s.assert_stopped_without_execution("after the retry").await
}

/// A retry whose earlier pod failed replaces the execution, and the new
/// generation records the upgrade its pod carries before that pod exists: a
/// later retry still finds the upgrade to confirm and back up, rather than
/// retiring the execution as one that never carried it. So does one whose
/// execution is still `creating` under a pod carrying another operation.
#[rstest::rstest]
#[case::pod_failed(true)]
#[case::unpersisted_under_another_operation(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_replaced_upgrade_execution_records_the_upgrade_again(
    #[case] persisted: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let (failed_generation, _, _) = s.active_execution().await?.expect("the kept execution");
    let recorded = s.recorded_upgrade().await?;
    if persisted {
        s.pods().fail();
    } else {
        s.pods().annotate(&[(
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
            &uuid::Uuid::now_v7().to_string(),
        )]);
        unpersist_held_execution(&s).await?;
    }
    s.cache_the_pod();

    s.upgrade_unsettled().await;

    let (generation, target, live) = s.active_execution().await?.expect("the replacement");
    assert!(generation > failed_generation, "{generation}");
    assert_eq!((target.as_deref(), live), (Some("stopped"), true));
    assert_eq!(s.recorded_upgrade().await?, recorded);
    s.pods().assert_every_create_upgrades(
        &recorded.0.expect("the recorded operation").to_string(),
        "stopped",
    );
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 2);
    Ok(())
}

/// A pod at the execution's name that doesn't carry the upgrade is never
/// read as its outcome: the node's report for the operation isn't that pod's.
/// The operation replaces it under a new generation whose pod carries the
/// upgrade, then reads and retires that one. That holds for the Running pod
/// a crashed start leaves under its expired claim, which the operation claims
/// as any activation would, and for a pod at an earlier attempt's execution
/// that doesn't carry what it recorded. A start that ended between creating
/// its pod and persisting it, leaving its execution `creating`, is no
/// different.
#[rstest::rstest]
#[case::a_crashed_start("started", false, true)]
#[case::a_start_crashed_before_its_persist("started", false, false)]
#[case::an_earlier_attempt("stopped", true, true)]
#[tokio::test(flavor = "current_thread")]
async fn a_pod_without_the_upgrade_is_replaced_before_its_outcome_is_read(
    #[case] left_by: &str,
    #[case] recorded: bool,
    #[case] persisted: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, left_by, -3600).await?;
    if recorded {
        record_upgrade_on_held_execution(&s).await?;
    }
    if !persisted {
        unpersist_held_execution(&s).await?;
    }
    s.cache_the_pod();

    let operation = upgraded(&s.upgrade().await?);

    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
    // The pod left behind, then the upgrade's own.
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 2);
    assert_eq!(s.enqueued_backups().len(), 1);
    s.assert_stopped_without_execution("after the upgrade")
        .await
}

/// An earlier attempt recorded its upgrade, created its pod, then ended
/// before recording the pod's UID. The retry reuses the pod, which carries
/// the operation, identifies it by its name, which the generation fixes, and
/// retires it.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_whose_pod_uid_went_unrecorded_is_identified_and_retired() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    s.pods().annotate(&[
        (
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
            &operation.to_string(),
        ),
        (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "stopped"),
    ]);
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = NULL WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;

    assert_eq!(upgraded(&s.upgrade().await?), operation);

    // The earlier attempt's pod, reused and then retired.
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 0);
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 1);
    s.assert_stopped_without_execution("after the upgrade")
        .await
}

/// An attempt ended between creating its pod and persisting it: the
/// execution is `creating`, with no pod UID, address or node, and the
/// sandbox is still placed on the node holding its disk. The retry finds the
/// pod carrying the operation it renders and hydrates it, and converges,
/// whether or not its reflector has seen the pod. (A sandbox no node is
/// recorded as holding is refused before anything is claimed:
/// `a_sandbox_whose_disk_node_isnt_its_placements_pin_is_refused`.)
#[rstest::rstest]
#[case::cached(true)]
#[case::uncached(false)]
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_interrupted_before_persisting_its_pod_converges_on_a_retry(
    #[case] cached: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    s.pods().annotate(&[
        (
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
            &operation.to_string(),
        ),
        (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "stopped"),
        (
            pplx_space_common_core::kubernetes::SANDBOX_ID_ANNOTATION,
            &s.sandbox_id.to_string(),
        ),
        (
            pplx_space_common_core::kubernetes::SANDBOX_EXECUTION_GENERATION_ANNOTATION,
            "7",
        ),
    ]);
    unpersist_held_execution(&s).await?;
    if cached {
        s.cache_the_pod();
    }

    assert_eq!(upgraded(&s.upgrade().await?), operation);

    // The earlier attempt's pod, hydrated and retired, its disk backed up.
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 0);
    assert_eq!(s.enqueued_backups().len(), 1);
    s.assert_stopped_without_execution("after the retry").await
}

/// A drain's re-placement that claims the execution an upgrade left after
/// its lease ended, one that never persisted its pod and is still
/// `creating`, never takes the upgrade's placeholder for its re-placed copy:
/// the placeholder runs no microVM. It replaces the pod under a new
/// execution, and nothing Wakes the placeholder.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_never_adopts_an_upgrades_unpersisted_placeholder() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    s.pods().annotate(&[
        (
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
            &operation.to_string(),
        ),
        (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "stopped"),
        // The identity that makes it this execution's pod.
        (
            pplx_space_common_core::kubernetes::SANDBOX_ID_ANNOTATION,
            &s.sandbox_id.to_string(),
        ),
        (
            pplx_space_common_core::kubernetes::SANDBOX_EXECUTION_GENERATION_ANNOTATION,
            "7",
        ),
    ]);
    unpersist_held_execution(&s).await?;
    // Its node placement was persisted (`persist_pod_node_placement`), so
    // the owed backup goes to that node; the pod itself never was. With no
    // node named, the owed backup refuses the re-placement before it places
    // anything (`an_owed_backup_that_can_never_be_enqueued_is_refused_for_good`).
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = $2 WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(WORKER_NODE)
    .execute(&s.db.pool)
    .await?;
    // The shape a stopped evacuation's teardown leaves for the drain's
    // re-placement.
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', placement = 'unplaced',
                active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        &s.db.pool,
        s.sandbox_id,
        pplx_space_common_core::types::LocalArtifactId::new(),
        "s3://space-test/evacuated-disk?region=us-east-1",
    )
    .await?;
    s.cache_the_pod();
    // The backup the upgrade owes waits until its pod, which kubelet may
    // still retry, is gone; the first try removes it.
    let first = re_place_off_the_drained_node(&s).await.map(drop);
    assert!(
        first.is_err(),
        "the re-placement didn't wait for the upgrade's pod"
    );
    assert!(!s.pods().exists(), "the upgrade's pod is still there");
    s.cache_the_pod();

    // The re-placement doesn't need to finish: only the pod it keeps matters.
    let _ = s
        .substrate
        .restore_execution_inner(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("node_drain.evacuate"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::kubernetes::restore_intent::RestoreIntent::drain(
                None,
                pplx_space_common_core::types::SandboxLifecycle::Stopped,
            ),
        )
        .await;

    let checkpoints = s
        .pods()
        .created_annotations(pplx_space_common_core::kubernetes::SANDBOX_CHECKPOINT_ANNOTATION);
    assert_eq!(checkpoints.len(), 1, "{checkpoints:?}");
    assert_ne!(
        checkpoints[0].as_deref(),
        Some(pplx_space_common_core::kubernetes::RUNTIME_UPGRADE_CHECKPOINT),
    );
    assert!(
        s.active_execution()
            .await?
            .is_some_and(|(generation, _, _)| generation > 7),
        "the re-placement kept the placeholder's execution"
    );
    let wakes = s.node().wakes.lock().unwrap().clone();
    assert!(
        wakes.iter().all(|wake| {
            wake.start_context
                .as_ref()
                .and_then(|context| context.expected_execution_generation)
                != Some(7)
        }),
        "the re-placement woke the upgrade's placeholder: {wakes:?}"
    );
    Ok(())
}

/// A create carrying the upgrade whose answer was lost may still commit after
/// the read that confirms it: the API server can still be processing a
/// request a transport error cut off. So no pod at the name after a failed
/// create keeps the claim, its record and its lease, as for an unsettled
/// upgrade, instead of retiring them as a clean failure; a retry once the
/// lease has ended places the upgrade again.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_upgrade_pod_create_keeps_the_claim_until_its_lease_ends() -> Result<(), BoxError>
{
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.pods()
        .creates_fail_uncommitted
        .store(true, Ordering::SeqCst);

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("may still land"), "{message}");
    assert!(!s.pods().exists());
    let (_, target, live) = s.active_execution().await?.expect("the kept execution");
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(live, "the claim's lease ended");
    assert!(s.recorded_upgrade().await?.0.is_some(), "the record went");

    s.pods()
        .creates_fail_uncommitted
        .store(false, Ordering::SeqCst);
    s.end_the_lease().await?;
    upgraded(&s.upgrade().await?);
    Ok(())
}
