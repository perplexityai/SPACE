//! What a stay-stopped runtime upgrade does with each outcome the node reports:
//! the upgrade reached, rolled back or refused, an outcome that can't be read
//! or settled yet, and a read of a pod that isn't the attempt's own.

use super::*;

/// The decision chooses E1, and the operation places one pod in the
/// runtime-upgrade mode targeting `stopped`, under an operation of its own,
/// never a start's. The node reports the upgrade done and publishes the
/// sandbox stopped under E1; the operation backs up the upgraded disk,
/// deletes the pod and retires the execution. Nothing Wakes the sandbox, and
/// its row stays stopped. A node that finds the sandbox already on E1 is
/// treated the same.
#[rstest::rstest]
#[case::upgraded(Report::Upgraded { publish: true })]
#[case::already_on_target(Report::AlreadyOnTarget)]
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_boots_in_its_pod_and_leaves_the_sandbox_stopped(
    #[case] report: Report,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    let decided = s
        .substrate
        .decide_runtime_upgrade(s.sandbox_id, UpgradeActivation::Start, None)
        .await?
        .expect("the start decision");
    let start = decided.pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Started);
    // Its authorization, which every decision of the same move shares.
    let stay_stopped = decided.pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Stopped);

    let operation = upgraded(&s.upgrade().await?);

    // A node refuses an operation id reused for another target state, and
    // each decision to leave the sandbox stopped names its own operation.
    assert_ne!(operation, start.operation_id);
    assert_ne!(operation, stay_stopped.operation_id);
    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
    for (key, expected) in [
        (SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION, E1),
        (SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION, "false"),
        (
            SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION,
            stay_stopped.authorization_id.as_str(),
        ),
    ] {
        assert_eq!(
            s.pods().created_annotation(key).as_deref(),
            Some(expected),
            "{key}"
        );
    }
    assert!(stay_stopped.authorization_id.contains(E0));
    assert!(
        s.node().wakes.lock().unwrap().is_empty(),
        "the upgrade woke the sandbox"
    );
    let backups = s.enqueued_backups();
    assert_eq!(backups.len(), 1, "{backups:?}");
    assert_eq!(backups[0].sandbox_id, s.sandbox_id.to_string());
    s.assert_stopped_without_execution("after the upgrade")
        .await
}

/// A rollback, or a call the node refused before it began, leaves the disk
/// the sandbox stopped with: the operation backs nothing up and retires the
/// execution and its pod. A rollback or refusal the node recorded for the
/// pod settles with the pod still pending in its create, as a failed
/// sandbox create leaves it. A rollback, and an operation that never began,
/// answer retryably (503); a refusal the node recorded answers 412, since the
/// node refuses the upgrade until what it checked changes. Every time, a
/// retry places the operation again in a new pod, which the node takes as a
/// new attempt rather than replaying the outcome.
#[rstest::rstest]
#[case::rolled_back(Report::RolledBack, "rolled back", false)]
#[case::refused(
    Report::Refused,
    "was refused before it began (E0 -> E1 is not a certified",
    true
)]
#[case::not_started(Report::Absent, "refused before it began", false)]
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_that_left_the_old_environment_retires_without_a_backup(
    #[case] report: Report,
    #[case] reason: &str,
    #[case] lasting: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    if matches!(report, Report::Absent) {
        // The shim's create failed before the node recorded anything.
        s.pods().end_container();
    }

    for attempt in 1..=2 {
        let error = s.upgrade().await.unwrap_err();

        let message = match (&error, lasting) {
            (SubstrateError::FailedPrecondition { message }, true)
            | (SubstrateError::Unavailable { message }, false) => message,
            _ => panic!("attempt {attempt}: {error:?}"),
        };
        assert!(message.contains(reason), "{message}");
        assert!(s.enqueued_backups().is_empty());
        assert_eq!(s.pods().creates.load(Ordering::SeqCst), attempt);
        s.assert_stopped_without_execution("after the settled failure")
            .await?;
    }
    let uids = s.pods().created_uids();
    assert_ne!(uids[0], uids[1], "the retry reused the pod");
    Ok(())
}

/// After any stay-stopped upgrade outcome, the drain still sees the stopped
/// sandbox, waits for it, and evacuates its disk. The retire leaves the
/// placement alone: `active_node_id` still names the node holding the disk,
/// which is the only column the drain finds a node's stopped sandboxes by.
/// So the residency gate counts the sandbox on its node, the drain's
/// stopped-evacuation claim takes it, and the placement still pins the disk
/// where a second upgrade's placement check looks for it.
#[rstest::rstest]
#[case::upgraded(Report::Upgraded { publish: true })]
#[case::rolled_back(Report::RolledBack)]
#[case::refused(Report::Refused)]
#[case::not_started(Report::Absent)]
#[tokio::test(flavor = "current_thread")]
async fn every_upgrade_outcome_leaves_the_sandbox_placed_on_its_disk_node(
    #[case] report: Report,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    if matches!(report, Report::Absent) {
        s.pods().end_container();
    }
    let placement = || async {
        sqlx::query_as::<_, (Option<pplx_space_common_core::types::NodeId>, String)>(sql!(
            "SELECT active_node_id, placement FROM sandboxes WHERE id = $1"
        ))
        .bind(s.sandbox_id)
        .fetch_one(&s.db.pool)
        .await
    };
    let before = placement().await?;
    assert_eq!(before.0, Some(s.node_id));

    let outcome = s.upgrade().await;

    assert_eq!(
        matches!(report, Report::Upgraded { .. }),
        outcome.is_ok(),
        "{:?}",
        outcome.map(drop)
    );
    s.assert_stopped_without_execution("after the upgrade's outcome")
        .await?;
    assert_eq!(placement().await?, before, "the retire moved the placement");
    let resident = pplx_space_sandbox_residency::active_resident_predicate(
        "s",
        "$1",
        pplx_space_sandbox_residency::ResidencyEvidence::LifecycleOnly,
    );
    let query = sql!(format!(
        "SELECT COUNT(*) FROM sandboxes s WHERE s.id = $2 AND {resident}"
    ));
    let counted: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
        .bind(s.node_id)
        .bind(s.sandbox_id)
        .fetch_one(&s.db.pool)
        .await?;
    assert_eq!(counted, 1, "the residency gate doesn't count the sandbox");
    assert_eq!(
        s.substrate
            .store
            .disk_pin_node_name(s.sandbox_id)
            .await?
            .as_deref(),
        Some(WORKER_NODE),
        "a second upgrade's placement check would refuse it"
    );
    if !matches!(report, Report::Upgraded { .. })
        && let Err(SubstrateError::FailedPrecondition { message }) = s.upgrade().await
    {
        assert!(!message.contains("pins the disk to"), "{message}");
    }
    sqlx::query(sql!(
        "UPDATE nodes SET drain_phase = 'draining', drain_mode = 'forced' WHERE id = $1"
    ))
    .bind(s.node_id)
    .execute(&s.db.pool)
    .await?;
    let claim = pplx_space_sandbox_store::drain::claim_stopped_evacuation(
        &s.db.pool,
        s.sandbox_id,
        s.node_id,
        true,
        None,
    )
    .await?;
    assert!(claim.holds(), "the drain couldn't claim it: {claim:?}");
    Ok(())
}

/// A retry after a rollback starts over in a new pod, and until that pod's
/// shim reaches the node, the node still reports the earlier pod's rollback.
/// A report that names another pod, or that names none, as a node that
/// predates the pod field answers, is no proof the retry's pod is done, even
/// once its container ended: the operation keeps the pod, the execution and
/// its claim's lease, deletes nothing, and answers its deadline's timeout.
/// Once the lease ends and the node reports the retry's own pod's rollback,
/// a retry settles on it, deletes that pod and retires as the first did.
#[rstest::rstest]
#[case::earlier_pod(Report::EarlierRollback, false)]
#[case::earlier_pod_ended(Report::EarlierRollback, true)]
#[case::old_node(Report::UnnamedRollback, false)]
#[case::old_node_ended(Report::UnnamedRollback, true)]
#[tokio::test(flavor = "current_thread")]
async fn a_rollback_that_doesnt_name_the_pod_never_settles_or_deletes_it(
    #[case] report: Report,
    #[case] container_ends: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(Report::RolledBack);
    let error = s.upgrade().await.unwrap_err();
    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_stopped_without_execution("after the first rollback")
        .await?;
    let deletes = s.pods().deletes.load(Ordering::SeqCst);
    s.pods().keep_creating();
    s.node().report(report);
    if container_ends {
        s.pods().end_container();
    }

    let error = s.upgrade_unsettled().await;

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 2);
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), deletes);
    s.assert_kept_under_its_lease("while the retry's pod is unsettled")
        .await?;

    s.end_the_lease().await?;
    s.node().report(Report::RolledBack);
    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(s.pods().deletes.load(Ordering::SeqCst) > deletes);
    assert!(s.enqueued_backups().is_empty());
    s.assert_stopped_without_execution("after the retry's rollback")
        .await
}

/// A retry after a rollback runs the upgrade again in a new pod, with a new
/// UID, since the rollback deleted the pod it was final for. The rollback
/// retired the execution that recorded the operation, so the retry is a new
/// decision under a new operation. It can succeed: the node rolls the first attempt back,
/// then upgrades the retry's pod and publishes the sandbox stopped under E1,
/// and the retry backs up and retires as any upgrade that reached the target
/// does.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_after_a_rollback_boots_the_upgrade_again_and_can_succeed() -> Result<(), BoxError>
{
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(Report::RolledBack);
    let error = s.upgrade().await.unwrap_err();
    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_stopped_without_execution("after the rollback")
        .await?;
    let first = s
        .pods()
        .created_annotation(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION)
        .expect("the first pod carries the operation");
    s.pods().run_containers();
    s.node().report(Report::Upgraded { publish: true });

    let operation = upgraded(&s.upgrade().await?);

    assert_ne!(operation.to_string(), first);
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 2);
    let uids = s.pods().created_uids();
    assert_ne!(uids[0], uids[1], "the retry reused the rolled-back pod");
    assert_eq!(
        s.pods()
            .created_annotations(SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION),
        [Some(first), Some(operation.to_string())]
    );
    assert_eq!(s.enqueued_backups().len(), 1);
    s.assert_stopped_without_execution("after the retry's upgrade")
        .await
}

/// A rollback the node recorded for the pod the wait read settles only once
/// that pod is deleted under its UID. When the delete finds another pod at
/// the name, the read was of a pod the API has replaced (a reflector still
/// holding a pod a retry recreated, say), and the pod there now may be
/// booting the upgrade: the wait never settles on it, keeps the pod, the
/// execution and its claim's lease, and answers its deadline's timeout.
#[tokio::test(flavor = "current_thread")]
async fn a_rollback_whose_pod_was_replaced_at_its_name_never_settles() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(Report::RolledBack);
    s.pods()
        .precondition_failing_deletes
        .store(usize::MAX, Ordering::SeqCst);

    let error = s.upgrade_unsettled().await;

    assert!(matches!(error, SubstrateError::Timeout { .. }), "{error:?}");
    assert!(s.enqueued_backups().is_empty());
    s.assert_kept_under_its_lease("after the replaced pod's rollback")
        .await
}

/// The node can't say the upgrade settled, or hasn't published its result:
/// it may still be working, so the operation keeps the pod, the execution
/// and its claim's lease. That includes a node that has the sandbox on the
/// target but refused the pod's create, as it does for a sandbox that isn't
/// at rest: only the node's publication says the sandbox ended stopped.
/// While the lease runs, a retry is refused retryably and takes nothing
/// over, and neither does a start. Once it ends, a retry places the same
/// upgrade, and once the node reports it done and published, it converges.
#[rstest::rstest]
#[case::in_progress(Report::InProgress, false)]
#[case::absent_while_the_pod_runs(Report::Absent, false)]
#[case::node_unreachable(Report::Unreachable, false)]
#[case::unpublished(Report::Upgraded { publish: false }, false)]
#[case::on_target_with_the_pod_refused(Report::Upgraded { publish: false }, true)]
#[tokio::test(flavor = "current_thread")]
async fn an_unsettled_upgrade_keeps_its_pod_and_a_retry_converges(
    #[case] report: Report,
    #[case] container_ends: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    if container_ends {
        s.pods().end_container();
    }

    let error = s.upgrade_unsettled().await;

    assert!(
        matches!(
            error,
            SubstrateError::Timeout { .. } | SubstrateError::Unavailable { .. }
        ),
        "{error:?}"
    );
    s.assert_kept_under_its_lease("after the unsettled upgrade")
        .await?;
    assert!(s.enqueued_backups().is_empty());
    let recorded = s.recorded_upgrade().await?;
    let held = s.active_execution().await?;
    s.node().report(Report::Upgraded { publish: true });

    let error = s.upgrade().await.unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("in flight"), "{message}");
    let start = s
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
    assert!(start.is_err(), "a start took the execution mid-upgrade");
    assert_eq!(s.active_execution().await?, held);
    assert_eq!(s.recorded_upgrade().await?, recorded);
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);

    s.end_the_lease().await?;
    let operation = upgraded(&s.upgrade().await?);

    assert_eq!(Some(operation), recorded.0);
    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    assert_eq!(s.enqueued_backups().len(), 1);
    s.assert_stopped_without_execution("after the retry").await
}

/// What never answers in [`a_read_that_hangs_ends_with_the_waits_deadline`].
#[derive(Clone, Copy, Debug)]
enum HungRead {
    /// The node's `GetRuntimeUpgrade`.
    Node,
    /// The API server's uncached read of the pod.
    Pod,
    /// With no pod, the database's read of the execution's node.
    Execution,
    /// The database's read of the node's publication, once the upgrade
    /// settled.
    Publication,
}

/// A read the node, the API server or the database never answers can't hold
/// the wait past its deadline by more than the floor one observation gets.
#[rstest::rstest]
#[case::node_read(HungRead::Node)]
#[case::pod_read(HungRead::Pod)]
#[case::execution_read(HungRead::Execution)]
#[case::publication_read(HungRead::Publication)]
#[tokio::test(flavor = "current_thread")]
async fn a_read_that_hangs_ends_with_the_waits_deadline(
    #[case] hung: HungRead,
) -> Result<(), BoxError> {
    use crate::controllers::substrate::kubernetes::pod_runtime_upgrade::{
        MissingPod, PodUpgradeFailure,
    };
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let upgrade = s
        .substrate
        .decide_runtime_upgrade(s.sandbox_id, UpgradeActivation::Start, None)
        .await?
        .expect("the start decision")
        .pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Stopped);
    let pod_name = format!("space-sbx-{}-g2", s.sandbox_id.as_uuid().simple());
    if !matches!(hung, HungRead::Execution) {
        *s.pods().pod.lock().unwrap() = Some(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": pod_name,
                "namespace": "space",
                "uid": format!("{pod_name}-uid"),
                "annotations": {
                    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION: upgrade.operation_id.to_string(),
                },
            },
            "spec": { "nodeName": "node-a" },
            "status": { "phase": "Running", "podIP": "127.0.0.1" }
        }));
    }
    // A table lock held by another transaction holds every read of it.
    let mut holder = s.db.pool.begin().await?;
    match hung {
        HungRead::Node => s.node().report(Report::Hangs),
        HungRead::Pod => s.pods().pod_reads_hang.store(true, Ordering::SeqCst),
        HungRead::Execution => {
            sqlx::query(sql!(
                "LOCK TABLE sandbox_k8s_executions IN ACCESS EXCLUSIVE MODE"
            ))
            .execute(&mut *holder)
            .await?;
        }
        HungRead::Publication => {
            sqlx::query(sql!(
                "LOCK TABLE sandbox_observed_states IN ACCESS EXCLUSIVE MODE"
            ))
            .execute(&mut *holder)
            .await?;
        }
    }
    let deadline = tokio::time::Instant::now() + Duration::from_millis(200);

    let outcome = tokio::time::timeout(Duration::from_millis(1800), async {
        match hung {
            HungRead::Publication => {
                s.substrate
                    .await_stopped_publication(s.sandbox_id, upgrade.target, deadline)
                    .await
            }
            _ => {
                s.substrate
                    .settle_pod_runtime_upgrade(
                        s.sandbox_id,
                        &pod_name,
                        MissingPod::Gone,
                        &upgrade,
                        deadline,
                    )
                    .await
            }
        }
    })
    .await
    .expect("a hung read held the wait past its deadline");
    holder.rollback().await?;

    assert!(
        matches!(
            outcome,
            Err(PodUpgradeFailure::MayStillRun(
                SubstrateError::Timeout { .. }
            ))
        ),
        "{outcome:?}"
    );
    Ok(())
}

/// What the pod at the execution's name looks like when the wait reads it.
#[derive(Clone, Copy, Debug)]
enum StalePod {
    /// An earlier attempt's pod, carrying the same operation (its id is
    /// derived, so a retry renders it again) under the same name, still in
    /// the cache while the execution records the current attempt's pod.
    Earlier,
    /// The attempt's own pod, on its way out: a later attempt may be
    /// replacing it at the same name.
    Terminating,
}

/// A rollback the node reports for the pod the wait reads settles only when
/// that pod is the attempt's own and not on its way out. An earlier
/// attempt's pod a cache still holds at the name, or a pod being deleted,
/// may stand in front of a pod booting the same operation: the wait keeps
/// waiting, never deletes it, and answers its deadline's timeout.
#[rstest::rstest]
#[case::earlier_pod(StalePod::Earlier)]
#[case::terminating_pod(StalePod::Terminating)]
#[tokio::test(flavor = "current_thread")]
async fn a_rollback_read_from_a_pod_that_isnt_the_attempts_own_never_settles(
    #[case] stale: StalePod,
) -> Result<(), BoxError> {
    use crate::controllers::substrate::kubernetes::pod_runtime_upgrade::{
        MissingPod, PodUpgradeFailure,
    };
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let upgrade = s
        .substrate
        .decide_runtime_upgrade(s.sandbox_id, UpgradeActivation::Start, None)
        .await?
        .expect("the start decision")
        .pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Stopped);
    // The execution records the attempt's own pod.
    held_execution(&s, "stopped", 300).await?;
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    let (uid, deleting) = match stale {
        StalePod::Earlier => ("earlier-attempt-uid".to_string(), false),
        StalePod::Terminating => (format!("{pod_name}-uid"), true),
    };
    let mut metadata = serde_json::json!({
        "name": pod_name,
        "namespace": "space",
        "uid": uid,
        "annotations": {
            SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION: upgrade.operation_id.to_string(),
        },
    });
    if deleting {
        metadata["deletionTimestamp"] = serde_json::json!("2026-11-07T00:00:00Z");
    }
    *s.pods().pod.lock().unwrap() = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": { "nodeName": "node-a" },
        "status": { "phase": "Pending" }
    }));
    // The node reports the rollback for the pod at the name.
    s.node().report(Report::RolledBack);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);

    let outcome = s
        .substrate
        .settle_pod_runtime_upgrade(
            s.sandbox_id,
            &pod_name,
            MissingPod::Gone,
            &upgrade,
            deadline,
        )
        .await;

    assert!(
        matches!(
            outcome,
            Err(PodUpgradeFailure::MayStillRun(
                SubstrateError::Timeout { .. }
            ))
        ),
        "{outcome:?}"
    );
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 0);
    Ok(())
}

/// With the pod gone and no execution left that owns its name (the attempt's
/// execution was retired or replaced before the wait's first read), nothing
/// says which pod was the attempt's or that it never existed: a report of an
/// operation the node never recorded doesn't settle as never started.
#[tokio::test(flavor = "current_thread")]
async fn a_gone_pod_with_no_execution_owning_its_name_never_settles() -> Result<(), BoxError> {
    use crate::controllers::substrate::kubernetes::pod_runtime_upgrade::{
        MissingPod, PodUpgradeFailure,
    };
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let upgrade = s
        .substrate
        .decide_runtime_upgrade(s.sandbox_id, UpgradeActivation::Start, None)
        .await?
        .expect("the start decision")
        .pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Stopped);
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    *s.pods().pod.lock().unwrap() = None;
    s.node().report(Report::Absent);
    let deadline = tokio::time::Instant::now() + Duration::from_millis(400);

    let outcome = s
        .substrate
        .settle_pod_runtime_upgrade(
            s.sandbox_id,
            &pod_name,
            MissingPod::Gone,
            &upgrade,
            deadline,
        )
        .await;

    assert!(
        matches!(
            outcome,
            Err(PodUpgradeFailure::MayStillRun(
                SubstrateError::Timeout { .. }
            ))
        ),
        "{outcome:?}"
    );
    Ok(())
}

/// A stopped sandbox's control-plane disk pin names the artifact captured
/// before the upgrade, say by an earlier drain's re-placement, and nothing
/// projects a stopped sandbox's node pin up. The upgrade's boot may rewrite
/// the disk and a rollback's cleanup drops the node's pin, so the claim
/// clears the control-plane pin, as a start's boot does, and no outcome
/// leaves it set: left set, a drain would refuse the sandbox on every tick
/// and a sourceless restart would select pre-upgrade bytes. That includes an
/// upgrade whose backup enqueue fails and one the node refuses.
#[rstest::rstest]
#[case::upgraded(Report::Upgraded { publish: true }, false)]
#[case::rolled_back(Report::RolledBack, false)]
#[case::refused(Report::Refused, false)]
#[case::backup_enqueue_fails(Report::Upgraded { publish: true }, true)]
#[tokio::test(flavor = "current_thread")]
async fn no_upgrade_outcome_keeps_the_disk_pin_from_before_it(
    #[case] report: Report,
    #[case] enqueue_fails: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = if enqueue_fails {
        subject_whose_backups(ScriptedReply::Status(tonic::Status::internal(
            "backup queue full",
        )))
        .await?
    } else {
        subject().await?
    };
    s.node().report(report);
    let pinned = pplx_space_common_core::types::LocalArtifactId::new();
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'disk',
                current_local_artifact_origin = 'stop'
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .bind(pinned.as_uuid())
    .execute(&s.db.pool)
    .await?;

    let _ = s.upgrade().await;

    let pin: Option<uuid::Uuid> = sqlx::query_scalar(sql!(
        "SELECT current_local_artifact_id FROM sandboxes WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .fetch_one(&s.db.pool)
    .await?;
    assert_eq!(pin, None, "the upgrade left the disk pin from before it");
    Ok(())
}
