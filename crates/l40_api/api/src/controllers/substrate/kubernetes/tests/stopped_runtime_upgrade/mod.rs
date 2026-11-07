//! Upgrading a stopped sandbox's runtime without starting it, against
//! Postgres, a scripted node and a Kubernetes API: the operation places the
//! sandbox's pod in its runtime-upgrade mode, reads the outcome from the
//! node's `GetRuntimeUpgrade`, and backs up and retires what reached the
//! target. Kubernetes only: Direct refuses the operation, under the
//! stopped-sandbox runtime upgrade row of the `direct.rs` gap table.

use std::sync::atomic::Ordering;
use std::time::Duration;

use pplx_space_common_core::kubernetes::{
    PodRuntimeUpgradeTargetState, SANDBOX_RUNTIME_UPGRADE_AUTHORIZATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_MEMORY_LOSS_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_ENVIRONMENT_ANNOTATION,
    SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION,
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::SandboxTransitionTargetState;

use super::runtime_upgrade_dispatch::{E0, E1};
use super::*;
use crate::controllers::substrate::kubernetes::scripted_test_doubles::{
    BoxError, ScriptedReply, WORKER_NODE,
};
use crate::controllers::substrate::runtime_upgrade::{StoppedRuntimeUpgrade, UpgradeActivation};

pub(super) mod fixture;
use fixture::*;

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
    let stay_stopped = decided.pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Stopped);
    let start = decided.pod_upgrade(s.sandbox_id, PodRuntimeUpgradeTargetState::Started);
    // A node refuses an operation id reused for another target state.
    assert_ne!(stay_stopped.operation_id, start.operation_id);

    let operation = upgraded(&s.upgrade().await?);

    assert_eq!(operation, stay_stopped.operation_id);
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

/// The node settles a `stopped` upgrade inside the shim's pod-sandbox
/// create, so it reports it reached the target, and publishes the sandbox
/// stopped, before kubelet posts the pod's address. The operation still
/// finishes in one call: it enqueues the current-instance disk backup
/// through the node holding the disk, by name, then deletes the pod and
/// retires the execution.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_whose_pod_has_no_address_yet_is_backed_up_through_its_node()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.pods().addressless.store(true, Ordering::SeqCst);
    s.pods().keep_creating();

    let operation = upgraded(&s.upgrade().await?);

    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    let backups = s.enqueued_backups();
    assert_eq!(backups.len(), 1, "{backups:?}");
    assert_eq!(backups[0].sandbox_id, s.sandbox_id.to_string());
    assert_eq!(
        backups[0].expected_artifact_kind,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
    );
    assert!(
        matches!(
            backups[0].source,
            Some(
                pplx_space_sbx_mgr_client::proto::enqueue_artifact_backup_request::Source::CurrentInstance(_)
            )
        ),
        "{:?}",
        backups[0].source
    );
    assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 1);
    s.assert_stopped_without_execution("after the upgrade")
        .await
}

/// A rollback, or a call the node refused before it began, leaves the disk
/// the sandbox stopped with: the operation backs nothing up and retires the
/// execution and its pod. A rollback or refusal the node recorded for the
/// pod settles with the pod still pending in its create, as a failed
/// sandbox create leaves it. Either answers retryably (503), every time: a
/// retry places the operation again in a new pod, which the node takes as a
/// new attempt rather than replaying the rollback.
#[rstest::rstest]
#[case::rolled_back(Report::RolledBack, "rolled back")]
#[case::refused(Report::Refused, "was refused before it began (cleanup owns")]
#[case::not_started(Report::Absent, "refused before it began")]
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_that_left_the_old_environment_retires_without_a_backup(
    #[case] report: Report,
    #[case] reason: &str,
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

        let SubstrateError::Unavailable { message } = &error else {
            panic!("attempt {attempt}: {error:?}");
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

/// A retry after a rollback runs the upgrade again under the same operation
/// in a new pod, with a new UID, since the rollback deleted the pod it was
/// final for, and it can succeed: the node rolls the first attempt back,
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

    assert_eq!(operation.to_string(), first);
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 2);
    let uids = s.pods().created_uids();
    assert_ne!(uids[0], uids[1], "the retry reused the rolled-back pod");
    s.pods().assert_every_create_upgrades(&first, "stopped");
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

/// An upgrade whose execution couldn't be retired, because its pod's delete
/// failed or found a pod with another UID at the name (a UID precondition
/// failure), leaves the execution and its pod, which nothing but a retry takes
/// over, so the answer is retryable: for an upgrade that reached the target,
/// whose backup is already enqueued, and for a rollback. The retry retires
/// and only then answers: the upgrade, or a retryable refusal for the
/// rollback, which the node reports again for the retry's pod.
#[rstest::rstest]
#[case::upgraded(Report::Upgraded { publish: true }, "delete", false)]
#[case::rolled_back(Report::RolledBack, "rolled back", false)]
#[case::uid_changed(Report::Upgraded { publish: true }, "no longer the one", true)]
#[tokio::test(flavor = "current_thread")]
async fn an_unretired_execution_answers_retryably_until_a_retry_retires_it(
    #[case] report: Report,
    #[case] reason: &str,
    #[case] uid_precondition_fails: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(report);
    if uid_precondition_fails {
        s.pods()
            .precondition_failing_deletes
            .store(1, Ordering::SeqCst);
    } else {
        // A rollback's pod is deleted by the wait that settles on it, then
        // by the retire: both fail.
        let failing = if matches!(report, Report::RolledBack) {
            2
        } else {
            1
        };
        s.pods().failing_deletes.store(failing, Ordering::SeqCst);
    }

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(reason), "{message}");
    s.assert_kept_for_a_retry("after the unretired execution")
        .await?;

    let outcome = s.upgrade().await;

    if matches!(report, Report::RolledBack) {
        let error = outcome.unwrap_err();
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        assert!(s.enqueued_backups().is_empty());
    } else {
        upgraded(&outcome?);
    }
    s.assert_stopped_without_execution("after the retry").await
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
            crate::controllers::substrate::RecoveryCharge::Exempt,
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

/// The node upgraded the disk, but refused its backup with an error a retry
/// within the call can't clear: the durable copy may still be the old disk,
/// so the execution and pod stay, and a retry backs it up before retiring.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_backup_enqueue_keeps_the_execution_until_a_retry_backs_it_up()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            backup_enqueued(),
        ]
        .into(),
    ))
    .await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("backup"), "{message}");
    s.assert_kept_for_a_retry("after the failed enqueue")
        .await?;

    upgraded(&s.upgrade().await?);

    assert_eq!(s.enqueued_backups().len(), 2);
    s.assert_stopped_without_execution("after the retry").await
}

/// The node settles a stopped upgrade once its cleanup returns, but
/// node-manager's network-slot reset after that cleanup still holds the
/// sandbox for a moment, and the backup's disk snapshot is refused as
/// unavailable while it does. The enqueue asks again until the phase
/// deadline: a refusal that clears finishes the upgrade in one call, and one
/// that outlasts the deadline answers the retryable 503 by then, keeping the
/// execution for a retry.
#[rstest::rstest]
#[case::clears(false)]
#[case::outlasts_the_deadline(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_backup_enqueue_refused_while_the_node_resets_the_sandbox_is_asked_again(
    #[case] persistent: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let refused = ScriptedReply::Status(tonic::Status::unavailable(
        "adopt stopped disk artifact: sandbox host mutation or reset is in progress; retry",
    ));
    let s = subject_whose_backups(if persistent {
        refused
    } else {
        ScriptedReply::Sequence(vec![refused, backup_enqueued()].into())
    })
    .await?;
    let timeout = Duration::from_secs(1);
    let started = std::time::Instant::now();

    let outcome = s
        .substrate
        .upgrade_stopped_runtime(s.sandbox_id, timeout)
        .await;

    let asked = s.enqueued_backups().len();
    if persistent {
        let error = outcome.unwrap_err();
        let SubstrateError::Unavailable { message } = &error else {
            panic!("{error:?}");
        };
        assert!(message.contains("disk backup isn't enqueued"), "{message}");
        assert!(asked > 1, "the enqueue was asked once");
        // The deadline, plus at most one call that began before it.
        assert!(
            started.elapsed() < timeout + Duration::from_millis(500),
            "{:?}",
            started.elapsed()
        );
        s.assert_kept_for_a_retry("after the deadline").await
    } else {
        upgraded(&outcome?);
        assert_eq!(asked, 2);
        s.assert_stopped_without_execution("after the upgrade")
            .await
    }
}

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

    // The start doesn't need to finish: only the pod it Wakes matters.
    let _ = s
        .substrate
        .start_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await;

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
            crate::controllers::substrate::RecoveryCharge::Exempt,
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

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_on_an_allowed_environment_is_left_alone() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("as recorded").await
}

#[tokio::test(flavor = "current_thread")]
async fn a_stranded_sandbox_is_refused_before_anything_is_placed() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE runtime_environments SET upgrade_edges = '{}' WHERE environment_ref = $1"
    ))
    .bind(E0)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    s.assert_stopped_without_execution("stranded").await
}

/// Only a stopped sandbox is upgraded without a start; any other is refused
/// for good (409). A drain's re-placement, which holds a stopped sandbox
/// `restoring` while it moves the disk and leaves it `stopped` again, is
/// refused retryably (503), as a disk still moving off a draining node is. A
/// user restore's `restoring` ends started.
#[rstest::rstest]
#[case::started("started", false)]
#[case::suspended("suspended", false)]
#[case::paused("paused", false)]
#[case::restored_by_a_user("restoring", false)]
#[case::re_placed_by_a_drain("restoring", true)]
#[tokio::test(flavor = "current_thread")]
async fn only_a_stopped_sandbox_is_upgraded_without_a_start(
    #[case] lifecycle: &str,
    #[case] re_placement: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET lifecycle = $2,
                evacuation_restore_target = CASE WHEN $3 THEN 'stopped' END
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .bind(lifecycle)
    .bind(re_placement)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    if re_placement {
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
    } else {
        assert!(
            matches!(error, SubstrateError::Conflict { .. }),
            "{error:?}"
        );
    }
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_is_on_no_node_is_refused() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = NULL, target_node_id = NULL WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET k8s_node_name = NULL WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    // Evacuated: the disk is durable in object storage and on no node.
    pplx_space_db_test_fixtures::publish_ready_disk_artifact(
        &s.db.pool,
        s.sandbox_id,
        pplx_space_common_core::types::LocalArtifactId::new(),
        "s3://space-test/evacuated-disk?region=us-east-1",
    )
    .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name,
        None
    );

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::FailedPrecondition { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// The node the disk is named on has to be a registered node able to take
/// work: a pod pinned to a node that's gone, or can't run it, never runs, and
/// no other node holds the disk. Refused before anything is placed: for good
/// (412) when the node is gone or outside the sandbox node selector, which no
/// retry changes, and retryably (503) when it can't take work now.
#[rstest::rstest]
#[case::gone("gone", true)]
#[case::outside_the_selector("outside the selector", true)]
#[case::not_ready("not ready", false)]
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_node_cant_take_work_is_refused(
    #[case] node: &str,
    #[case] for_good: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    let mut worker = worker_node();
    match node {
        "outside the selector" => worker.metadata.labels = None,
        "not ready" => {
            if let Some(conditions) = worker
                .status
                .as_mut()
                .and_then(|status| status.conditions.as_mut())
            {
                conditions[0].status = "False".to_string();
            }
        }
        _ => {}
    }
    let nodes = if node == "gone" { vec![] } else { vec![worker] };
    s.substrate = s.cluster.substrate_with_nodes(&s.db.pool, nodes);

    let error = s.upgrade().await.unwrap_err();

    if for_good {
        assert!(
            matches!(error, SubstrateError::FailedPrecondition { .. }),
            "{error:?}"
        );
    } else {
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
    }
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// An encrypted sandbox's stopped disk has no artifact the platform can use,
/// so the upgrade's owed backup could never be a real one. It's refused for
/// good (412) before anything is claimed or placed.
#[tokio::test(flavor = "current_thread")]
async fn an_encrypted_sandbox_is_refused_before_anything_is_placed() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET volume_encryption = TRUE WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("encrypted"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// The upgrade's pod goes only where the placement pins the disk to a
/// registered node, the one node an attempt that ends before persisting its
/// pod can still be backed up on. A disk node named only by execution
/// history, an unregistered placement node, or a re-placement's chosen
/// target is refused for good (412) before anything is claimed or placed.
#[rstest::rstest]
#[case::unregistered("unregistered")]
#[case::history_only("history only")]
#[case::replacing("replacing")]
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_whose_disk_node_isnt_its_placements_pin_is_refused(
    #[case] placement: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let pool = &s.db.pool;
    match placement {
        "unregistered" => {
            sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
                .bind(s.node_id)
                .execute(pool)
                .await?;
        }
        "history only" => {
            sqlx::query(sql!(
                "UPDATE sandboxes SET active_node_id = NULL WHERE id = $1"
            ))
            .bind(s.sandbox_id)
            .execute(pool)
            .await?;
        }
        _ => {
            let target = pplx_space_k8s_node_id::space_k8s_node_id("node-target", "worker");
            pplx_space_db_test_fixtures::insert_ready_worker_node(pool, target).await?;
            sqlx::query(sql!(
                "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
            ))
            .bind(s.sandbox_id)
            .bind(target)
            .execute(pool)
            .await?;
        }
    }
    let before = s.active_execution().await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("pins the disk to"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// A retry that will reuse the pod an earlier attempt left on the disk's
/// node, the one carrying the recorded operation, needs no new network slot,
/// so a node full only with that pod's own isn't refused: the retry reuses
/// the pod and converges. A pod that doesn't carry the operation is replaced,
/// which takes a slot the full node doesn't have, so that retry is refused,
/// retryably, before anything is placed.
#[rstest::rstest]
#[case::carrying_the_operation(true)]
#[case::without_the_operation(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_retry_reusing_its_pod_on_a_full_node_needs_no_new_slot(
    #[case] carried: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    if carried {
        s.pods().annotate(&[
            (
                SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
                &operation.to_string(),
            ),
            (SANDBOX_RUNTIME_UPGRADE_TARGET_STATE_ANNOTATION, "stopped"),
        ]);
    }
    s.pods().request_network_slots(1);
    s.cache_the_pod_with_network_slots(1);

    let outcome = s.upgrade().await;

    if carried {
        assert_eq!(upgraded(&outcome?), operation);
        assert_eq!(s.pods().creates.load(Ordering::SeqCst), 0);
        s.assert_stopped_without_execution("after the upgrade")
            .await
    } else {
        let error = outcome.unwrap_err();
        let SubstrateError::Unavailable { message } = &error else {
            panic!("{error:?}");
        };
        assert!(message.contains("network slot"), "{message}");
        s.assert_nothing_placed_or_sent("refused");
        assert!(s.pods().exists());
        Ok(())
    }
}

/// An upgrade whose claim's lease still runs may own the execution, so
/// another is refused whatever it would decide now: a publication that makes
/// it "as recorded" doesn't let it retire that execution.
#[tokio::test(flavor = "current_thread")]
async fn an_upgrade_in_flight_refuses_another() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;

    for publish in [false, true] {
        if publish {
            s.publish_upgraded().await?;
        }
        let error = s.upgrade().await.unwrap_err();

        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        assert!(s.pods().exists());
        assert_eq!(
            s.active_execution().await?,
            Some((7, Some("stopped".to_string()), true))
        );
    }
    Ok(())
}

/// An earlier attempt placed an upgrade the node may have begun. The retry
/// keeps that upgrade although the policy no longer asks for it; it never
/// decides afresh over it.
#[tokio::test(flavor = "current_thread")]
async fn a_retry_keeps_the_upgrade_an_earlier_attempt_recorded() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let recorded = s.recorded_upgrade().await?;
    {
        use pplx_space_runtime_environment_policy_store::{
            PolicyChange, PolicyChangeReason, RuntimeEnvironmentState, set_environment_state,
        };
        let mut tx = s.db.pool.begin().await?;
        set_environment_state(
            &mut tx,
            E0.parse()?,
            RuntimeEnvironmentState::Allowed,
            &PolicyChange {
                actor: None,
                reason: PolicyChangeReason::new("test allow")?,
            },
        )
        .await?;
        tx.commit().await?;
    }
    s.forget_cached_policy();
    s.node().report(Report::Upgraded { publish: true });

    let operation = upgraded(&s.upgrade().await?);

    assert_eq!(Some(operation), recorded.0);
    s.pods()
        .assert_every_create_upgrades(&operation.to_string(), "stopped");
    s.assert_stopped_without_execution("after the retry").await
}

/// A claim targeting `stopped` with nothing recorded isn't an upgrade's: a
/// drain's re-placement leaves one when its best-effort release fails, over
/// the pod holding the sandbox's re-placed copy. While its lease runs, the
/// operation answers a retryable refusal and leaves the execution and its pod
/// to their owner. Once the lease has ended, the operation claims the
/// execution as any activation may claim one whose lease ended, replaces the
/// pod, which doesn't carry the upgrade, and converges.
#[rstest::rstest]
#[case::live(3600)]
#[case::expired(-3600)]
#[tokio::test(flavor = "current_thread")]
async fn a_drains_stopped_claim_is_reclaimed_only_once_its_lease_ended(
    #[case] lease_secs: i64,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    held_execution(&s, "stopped", lease_secs).await?;
    s.cache_the_pod();
    let before = (s.active_execution().await?, left_by(&s).await?);

    let outcome = s.upgrade().await;

    if lease_secs > 0 {
        let error = outcome.unwrap_err();
        assert!(
            matches!(error, SubstrateError::Unavailable { .. }),
            "{error:?}"
        );
        s.assert_nothing_placed_or_sent("refused");
        assert!(s.pods().exists());
        assert_eq!(s.pods().deletes.load(Ordering::SeqCst), 0);
        assert_eq!((s.active_execution().await?, left_by(&s).await?), before);
    } else {
        let operation = upgraded(&outcome?);
        s.pods()
            .assert_every_create_upgrades(&operation.to_string(), "stopped");
        assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
        assert_eq!(s.enqueued_backups().len(), 1);
        s.assert_stopped_without_execution("after the reclaimed upgrade")
            .await?;
    }
    Ok(())
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

/// Retiring an upgrade's execution that changed after it was read, so the
/// fenced update matches no row, isn't done: the execution stays active with
/// what it recorded, and the retire answers a retryable error rather than
/// letting the operation report success.
#[tokio::test(flavor = "current_thread")]
async fn a_retire_whose_fenced_update_misses_is_not_done() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let attempt = left_by(&s).await?;
    // Stands in for a change between the retire's read and its fenced
    // update: the update that would finish the execution matches nothing.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION keep_execution_active() RETURNS trigger AS $$
         BEGIN
             IF OLD.active AND NOT NEW.active THEN RETURN NULL; END IF;
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER keep_execution_active
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION keep_execution_active();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s
        .substrate
        .retire_upgrade_boot(s.sandbox_id, 7, attempt, None, None)
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert!(
        s.active_execution()
            .await?
            .is_some_and(|(generation, _, _)| generation == 7)
    );
    Ok(())
}

/// A retire whose attempt no longer owns the execution deletes the pod at
/// its name only when nothing adopted the execution, since an adopter owns
/// the pod with it, and an upgrade whose backup it can't confirm isn't done
/// either way. A boot that never saw its pod's UID (a rollback's receipt
/// retired the execution before it was recorded) deletes the pod there that
/// carries its operation, and leaves one that doesn't.
#[rstest::rstest]
#[case::adopted(true, false, true, true)]
#[case::adopted_before_its_backup(true, true, true, true)]
#[case::finished_elsewhere(false, false, true, true)]
#[case::finished_before_its_backup(false, true, true, true)]
#[case::finished_before_its_uid_was_recorded(false, false, false, true)]
#[case::finished_with_another_operations_pod(false, false, false, false)]
#[tokio::test(flavor = "current_thread")]
async fn a_retire_that_lost_its_execution_keeps_an_adopted_pod_and_an_unconfirmed_backup(
    #[case] adopted: bool,
    #[case] backup: bool,
    #[case] uid_recorded: bool,
    #[case] pod_carries_the_operation: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;
    if !adopted {
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
        ))
        .bind(s.sandbox_id)
        .execute(&s.db.pool)
        .await?;
    }
    let operation_id = uuid::Uuid::now_v7();
    let carried = if pod_carries_the_operation {
        operation_id
    } else {
        uuid::Uuid::now_v7()
    };
    s.pods().annotate(&[(
        SANDBOX_RUNTIME_UPGRADE_OPERATION_ID_ANNOTATION,
        &carried.to_string(),
    )]);
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    let booted = crate::controllers::substrate::kubernetes::adapter::BootedPod {
        uid: uid_recorded
            .then(|| pplx_space_common_core::types::PodUid::parse(format!("{pod_name}-uid")))
            .transpose()?,
        name: pod_name,
        operation_id,
    };

    let retired = s
        .substrate
        .retire_upgrade_boot(
            s.sandbox_id,
            7,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
            Some(booted),
            backup.then(tokio::time::Instant::now),
        )
        .await;

    assert_eq!(retired.is_err(), backup, "{retired:?}");
    assert_eq!(s.pods().exists(), adopted || !pod_carries_the_operation);
    Ok(())
}

/// A retry that fails to record the earlier attempt's upgrade again ends
/// its claim and keeps the execution and pod, with what it recorded, for the
/// next retry, and places and reads nothing.
#[tokio::test(flavor = "current_thread")]
async fn a_resend_failing_to_record_keeps_the_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let recorded = s.recorded_upgrade().await?;
    let creates = s.pods().creates.load(Ordering::SeqCst);
    let reads = s.cluster.worker.received(GET_RUNTIME_UPGRADE).len();
    // The store refuses to record the upgrade again.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_runtime_upgrade_record() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'recording refused'; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER refuse_runtime_upgrade_record
             BEFORE UPDATE OF runtime_upgrade_authorization_id ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION refuse_runtime_upgrade_record();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(error.to_string().contains("record"), "{error:?}");
    assert_eq!(
        s.active_execution()
            .await?
            .map(|(_, target, live)| (target, live)),
        Some((Some("stopped".to_string()), false))
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    // Nothing new was placed or read.
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), creates);
    assert_eq!(s.cluster.worker.received(GET_RUNTIME_UPGRADE).len(), reads);
    assert!(s.pods().exists());
    Ok(())
}

/// The claim targeting `stopped` and the upgrade's record commit together or
/// not at all. Here the record fails and the attempt ends without ending its
/// claim, as one whose process exits right after the claim would: nothing of
/// the claim is left, which a retry would otherwise refuse as another
/// activation's for good once its lease ran out. The next attempt claims the
/// execution afresh and converges in one call.
#[tokio::test(flavor = "current_thread")]
async fn an_attempt_ending_at_its_claim_leaves_no_unrecorded_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    // The record fails, and so does anything that would end an attempt's
    // claim afterwards.
    sqlx::raw_sql(sql!(
        "CREATE FUNCTION refuse_runtime_upgrade_record() RETURNS trigger AS $$
         BEGIN RAISE EXCEPTION 'recording refused'; END $$ LANGUAGE plpgsql;
         CREATE TRIGGER refuse_runtime_upgrade_record
             BEFORE UPDATE OF runtime_upgrade_authorization_id ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION refuse_runtime_upgrade_record();
         CREATE FUNCTION keep_activation_claims() RETURNS trigger AS $$
         BEGIN
             IF OLD.activation_attempt_id IS NOT NULL
                AND NEW.activation_attempt_id IS DISTINCT FROM OLD.activation_attempt_id
             THEN
                 RETURN NULL;
             END IF;
             RETURN NEW;
         END $$ LANGUAGE plpgsql;
         CREATE TRIGGER keep_activation_claims
             BEFORE UPDATE ON sandbox_k8s_executions
             FOR EACH ROW EXECUTE FUNCTION keep_activation_claims();"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(error.to_string().contains("record"), "{error:?}");
    assert!(left_by(&s).await.is_err(), "a claim was left unrecorded");
    assert_eq!(s.active_execution().await?, None);
    s.assert_nothing_placed_or_sent("refused");

    sqlx::raw_sql(sql!(
        "DROP TRIGGER refuse_runtime_upgrade_record ON sandbox_k8s_executions;
         DROP TRIGGER keep_activation_claims ON sandbox_k8s_executions;"
    ))
    .execute(&s.db.pool)
    .await?;
    // Any claim left would have run out its lease by now.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() - INTERVAL '1 second'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    upgraded(&s.upgrade().await?);
    s.assert_stopped_without_execution("after the retry").await
}

/// Give the sandbox an active execution whose attempt targets `target` under
/// a lease ending `lease_secs` from now, with its pod in place: what a start
/// or an earlier upgrade leaves while it runs, or after it crashed.
async fn held_execution(s: &Subject, target: &str, lease_secs: i64) -> Result<(), BoxError> {
    let pod_name = format!("space-sbx-{}-g7", s.sandbox_id.as_uuid().simple());
    insert_k8s_execution(
        &s.db.pool,
        s.sandbox_id,
        7,
        &pod_name,
        &format!("{pod_name}-uid"),
        Some("127.0.0.1"),
        true,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_id = attempt.id,
                route_activation_id = attempt.id,
                activation_attempt_target_state = $2,
                activation_attempt_expires_at = NOW() + make_interval(secs => $3::BIGINT),
                k8s_node_name = 'node-a'
           FROM (SELECT gen_random_uuid() AS id) attempt
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(target)
    .bind(lease_secs)
    .execute(&s.db.pool)
    .await?;
    *s.pods().pod.lock().unwrap() = Some(serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name, "namespace": "space", "uid": format!("{pod_name}-uid") },
        "spec": { "nodeName": "node-a" },
        "status": { "phase": "Running", "podIP": "127.0.0.1" }
    }));
    Ok(())
}

/// Leave the execution [`held_execution`] made as an activation that ended
/// between creating its pod and persisting it does: `creating`, with no pod
/// UID, address or node recorded.
async fn unpersist_held_execution(s: &Subject) -> Result<(), BoxError> {
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'creating', pod_uid = NULL, pod_ip = NULL, k8s_node_name = NULL
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    Ok(())
}

/// Record an upgrade to E1 on the execution [`held_execution`] left, as an
/// earlier attempt of the operation does before it places its pod. Answers
/// its operation.
async fn record_upgrade_on_held_execution(s: &Subject) -> Result<uuid::Uuid, BoxError> {
    let operation = uuid::Uuid::now_v7();
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET runtime_upgrade_operation_id = $2,
                runtime_upgrade_target_environment_ref = $3,
                runtime_upgrade_authorization_id = $4
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .bind(operation)
    .bind(E1)
    .bind(format!("runtime-environment-blocked:{E0}"))
    .execute(&s.db.pool)
    .await?;
    Ok(operation)
}

#[tokio::test(flavor = "current_thread")]
async fn a_start_in_flight_keeps_its_execution() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", 300).await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    assert!(s.pods().exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), true))
    );
    Ok(())
}

/// A start that crashed leaves an expired claim targeting `started`, maybe
/// over a running guest. It isn't an upgrade's leftover, so when the decision
/// keeps the sandbox's environment, nothing claims it: it stays for the
/// start's own recovery.
#[tokio::test(flavor = "current_thread")]
async fn a_crashed_start_is_left_alone_when_the_decision_keeps_its_environment()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "started", -3600).await?;
    s.publish_upgraded().await?;

    assert_eq!(s.upgrade().await?, StoppedRuntimeUpgrade::AsRecorded);

    assert!(s.pods().exists());
    assert_eq!(
        s.active_execution().await?,
        Some((7, Some("started".to_string()), false))
    );
    Ok(())
}

/// A drain is moving the stopped sandbox's disk off its node: a retryable
/// refusal until the disk is placed again, and nothing is placed meanwhile.
#[tokio::test(flavor = "current_thread")]
async fn a_sandbox_moving_off_a_draining_node_is_refused_for_now() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET evacuation_restore_target = 'stopped', active_node_id = NULL
          WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
}

/// The disk node is checked before the claim, and a drain can move the disk
/// in between while the row stays `stopped`: it releases the node (the
/// evacuation marker set, the placement cleared) or finishes re-placing the
/// sandbox on another node. The claim checks the disk node again under the
/// sandbox row's lock, so either answers a retryable 503 with nothing claimed
/// or placed, rather than a pod pinned to the old node's released or stale
/// copy. Here the change lands inside the claim's transaction, after its lock
/// and before the record, and rolls back with the refused claim; a retry once
/// nothing moves converges.
#[rstest::rstest]
#[case::released(true)]
#[case::re_placed(false)]
#[tokio::test(flavor = "current_thread")]
async fn a_disk_that_moves_before_the_claim_is_refused_for_now(
    #[case] released: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let node_b = pplx_space_k8s_node_id::space_k8s_node_id("node-b", "worker");
    pplx_space_db_test_fixtures::insert_ready_worker_node(&s.db.pool, node_b).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'node-b')"
    ))
    .bind(node_b)
    .execute(&s.db.pool)
    .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name
            .as_deref(),
        Some(WORKER_NODE)
    );
    // The drain's write, run by the claim's own execution insert.
    let drain = if released {
        sql!(
            "CREATE FUNCTION drain_moves_the_disk() RETURNS trigger AS $$
             BEGIN
                 UPDATE sandboxes
                    SET evacuation_restore_target = 'stopped', active_node_id = NULL
                  WHERE id = NEW.sandbox_id;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER drain_moves_the_disk
                 AFTER INSERT ON sandbox_k8s_executions
                 FOR EACH ROW EXECUTE FUNCTION drain_moves_the_disk();"
        )
    } else {
        sql!(
            "CREATE FUNCTION drain_moves_the_disk() RETURNS trigger AS $$
             BEGIN
                 UPDATE sandboxes
                    SET active_node_id =
                        (SELECT node_id FROM node_k8s WHERE k8s_node_name = 'node-b')
                  WHERE id = NEW.sandbox_id;
                 RETURN NEW;
             END $$ LANGUAGE plpgsql;
             CREATE TRIGGER drain_moves_the_disk
                 AFTER INSERT ON sandbox_k8s_executions
                 FOR EACH ROW EXECUTE FUNCTION drain_moves_the_disk();"
        )
    };
    sqlx::raw_sql(drain).execute(&s.db.pool).await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    // A release leaves the evacuation's marker, which refuses first.
    let reason = if released { "evacuating" } else { "disk moved" };
    assert!(message.contains(reason), "{message}");
    s.assert_nothing_placed_or_sent("refused");
    assert_eq!(s.active_execution().await?, None);

    sqlx::raw_sql(sql!(
        "DROP TRIGGER drain_moves_the_disk ON sandbox_k8s_executions"
    ))
    .execute(&s.db.pool)
    .await?;
    upgraded(&s.upgrade().await?);
    s.assert_stopped_without_execution("after the retry").await
}

/// A start that moved the sandbox on owns the execution an earlier upgrade
/// left: the takeover, which checks the lifecycle under the sandbox's lock,
/// refuses it with a retryable error and leaves its attempt alone.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_sandbox_a_start_moved_on() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'started' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let before = s.active_execution().await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            left_by(&s).await?,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    Ok(())
}

/// A drain's stopped evacuation that opened on the sandbox, its marker set
/// while the disk is still on its node, refuses the upgrade retryably under
/// the sandbox's lock, both a new claim and a takeover of the execution an
/// earlier attempt left: a boot after the evacuation minted its backup would
/// drop the node's pin on it, and the teardown would never release the node.
/// Nothing is claimed, placed or changed; the same upgrade succeeds once the
/// marker is gone.
#[tokio::test(flavor = "current_thread")]
async fn an_open_stopped_evacuation_refuses_the_upgrade() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    let mark = |marker: Option<&'static str>| {
        sqlx::query(sql!(
            "UPDATE sandboxes SET evacuation_restore_target = $2 WHERE id = $1"
        ))
        .bind(s.sandbox_id)
        .bind(marker)
        .execute(&s.db.pool)
    };
    mark(Some("stopped")).await?;

    let error = s.upgrade().await.unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("evacuating"), "{message}");
    s.assert_nothing_placed_or_sent("refused");
    assert_eq!(s.active_execution().await?, None);

    held_execution(&s, "stopped", -3600).await?;
    record_upgrade_on_held_execution(&s).await?;
    let before = s.active_execution().await?;
    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            left_by(&s).await?,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("evacuating"), "{message}");
    assert_eq!(s.active_execution().await?, before);

    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET active = FALSE WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    *s.pods().pod.lock().unwrap() = None;
    mark(None).await?;
    upgraded(&s.upgrade().await?);
    Ok(())
}

/// An attempt that replaced the claim the retry read owns the execution, and
/// so does a replacement execution that carried the read attempt to a new
/// generation: the takeover, which checks the attempt and the generation
/// under the sandbox's lock, refuses either with a retryable error and leaves
/// the execution alone.
#[rstest::rstest]
#[case::another_attempt(
    "UPDATE sandbox_k8s_executions SET activation_attempt_id = gen_random_uuid()
      WHERE sandbox_id = $1 AND active"
)]
#[case::another_generation(
    "UPDATE sandbox_k8s_executions SET generation = 8 WHERE sandbox_id = $1 AND active"
)]
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_refuses_a_claim_another_attempt_replaced(
    #[case] replace: &str,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let read = left_by(&s).await?;
    sqlx::query(sqlx::AssertSqlSafe(sql!(replace)))
        .bind(s.sandbox_id)
        .execute(&s.db.pool)
        .await?;
    let before = s.active_execution().await?;
    let replacement = left_by(&s).await?;

    let error = s
        .substrate
        .store
        .take_over_stopped_runtime_upgrade(
            s.sandbox_id,
            7,
            read,
            pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        )
        .await
        .unwrap_err();

    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(left_by(&s).await?, replacement);
    Ok(())
}

/// The attempt holding the claim a retry reads before its takeover.
async fn left_by(
    s: &Subject,
) -> Result<pplx_space_common_core::types::SandboxActivationAttemptId, BoxError> {
    Ok(s.substrate
        .store
        .stopped_runtime_upgrade_claim(s.sandbox_id)
        .await?
        .ok_or("no stopped runtime upgrade claim")?
        .activation_attempt_id)
}

/// The recorded upgrade is what makes a `stopped` claim the upgrade's. Only
/// the upgrade claims that target on a `stopped` row, and it keeps the record
/// across its own claims. Any other activation that claims the execution
/// clears it: a start, including one that reuses an execution without a pod,
/// and a drain's re-placement, which claims `stopped` on a `restoring` row.
#[rstest::rstest]
#[case::the_upgrade(SandboxTransitionTargetState::Stopped, "stopped", true)]
#[case::a_start(SandboxTransitionTargetState::Started, "stopped", false)]
#[case::a_re_placement(SandboxTransitionTargetState::Stopped, "restoring", false)]
#[tokio::test(flavor = "current_thread")]
async fn only_the_upgrades_claim_keeps_its_recorded_upgrade(
    #[case] target: SandboxTransitionTargetState,
    #[case] lifecycle: &str,
    #[case] kept: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    // The execution never got its pod.
    *s.pods().pod.lock().unwrap() = None;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions SET pod_uid = NULL WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!("UPDATE sandboxes SET lifecycle = $2 WHERE id = $1"))
        .bind(s.sandbox_id)
        .bind(lifecycle)
        .execute(&s.db.pool)
        .await?;
    let recorded = s.recorded_upgrade().await?;
    let claim = |owed_backup_enqueued| {
        s.substrate
            .store
            .ensure_active_execution_claim_for_lifecycle(
                s.sandbox_id,
                lifecycle.parse().expect("a lifecycle"),
                None,
                Some((
                    pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                    target,
                )),
                None,
                owed_backup_enqueued,
            )
    };

    if kept {
        claim(None).await?;
        assert_eq!(s.recorded_upgrade().await?, recorded);
        return Ok(());
    }
    // Another activation clears the record only once the backup the upgrade
    // owes is enqueued for its operation.
    let error = claim(None).await.map(drop).unwrap_err();
    assert!(
        matches!(error, SubstrateError::Unavailable { .. }),
        "{error:?}"
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    claim(Some(operation)).await?;
    assert_eq!(s.recorded_upgrade().await?, (None, None, None));
    Ok(())
}

/// A drain's re-placement moving the row to `restoring` while the upgrade
/// claims the execution and records itself either waits for the claim or
/// makes it fail, record and all: here the drain's write is uncommitted when
/// the claim runs, and commits before the claim could. That holds for the
/// first claim and for a replacement generation's. A record that read the
/// lifecycle without locking the row would see `stopped` and succeed, and
/// the drain, which claims after its write commits, would join an attempt
/// whose record it never saw.
#[rstest::rstest]
#[case::first_claim(false)]
#[case::replacement(true)]
#[tokio::test(flavor = "current_thread")]
async fn a_claim_racing_a_drains_restoring_write_fails(
    #[case] replacement: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let before = s.active_execution().await?;
    let mut drain = s.db.pool.begin().await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'restoring' WHERE id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&mut *drain)
    .await?;
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };
    let attempt = Some((
        pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        SandboxTransitionTargetState::Stopped,
    ));
    let store = &s.substrate.store;
    let claim = async {
        if replacement {
            store
                .replace_active_execution_claim_if_generation(
                    s.sandbox_id,
                    7,
                    "replaced",
                    attempt,
                    Some(&upgrade),
                )
                .await
                .map(drop)
        } else {
            store
                .ensure_active_execution_claim_for_lifecycle(
                    s.sandbox_id,
                    pplx_space_common_core::types::SandboxLifecycle::Stopped,
                    None,
                    attempt,
                    Some(&upgrade),
                    None,
                )
                .await
                .map(drop)
        }
    };
    tokio::pin!(claim);
    // Give the claim its chance to run against the uncommitted write.
    let early = tokio::time::timeout(Duration::from_millis(200), &mut claim).await;

    drain.commit().await?;
    let claimed = match early {
        Ok(claimed) => claimed,
        Err(_blocked) => claim.await,
    };

    assert!(
        claimed.is_err(),
        "the claim recorded the upgrade under a drain's restoring row"
    );
    // Rolled back whole: no replacement, no claim, nothing recorded.
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(s.recorded_upgrade().await?.0, None);
    Ok(())
}

/// The claim checks the disk pin again under the sandbox row's lock: a pin
/// that stopped naming the checked node since the precheck (here its node's
/// Kubernetes registration is gone, while execution history still names it)
/// refuses the claim retryably, and nothing is claimed or recorded.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_whose_disk_pin_moved_since_the_check_fails() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let before = s.active_execution().await?;
    sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
        .bind(s.node_id)
        .execute(&s.db.pool)
        .await?;
    assert_eq!(
        s.substrate
            .store
            .restore_source_plan(s.sandbox_id)
            .await?
            .node_name
            .as_deref(),
        Some(WORKER_NODE),
        "only the pin should have moved"
    );
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };

    let claimed = s
        .substrate
        .store
        .ensure_active_execution_claim_for_lifecycle(
            s.sandbox_id,
            pplx_space_common_core::types::SandboxLifecycle::Stopped,
            None,
            Some((
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Stopped,
            )),
            Some(&upgrade),
            None,
        )
        .await;

    let Err(SubstrateError::Unavailable { message }) = &claimed else {
        panic!("{:?}", claimed.map(drop));
    };
    assert!(message.contains("moved off node"), "{message}");
    assert_eq!(s.active_execution().await?, before);
    assert_eq!(s.recorded_upgrade().await?.0, None);
    Ok(())
}

/// An execution whose claim expired still carries an earlier upgrade's
/// record, which may owe a disk backup. A claim recording a different
/// operation never overwrites it: the claim fails retryably, claims nothing,
/// and the earlier record stays for its own retry to take over.
#[tokio::test(flavor = "current_thread")]
async fn a_claim_never_overwrites_another_operations_record() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    let earlier = record_upgrade_on_held_execution(&s).await?;
    let recorded = s.recorded_upgrade().await?;
    let upgrade = super::super::store::StoppedUpgradeRecord {
        upgrade: super::super::store::RecordedRuntimeUpgrade {
            operation_id: uuid::Uuid::now_v7(),
            target: E1.parse()?,
            authorization_id: format!("runtime-environment-blocked:{E0}"),
        },
        disk_node: WORKER_NODE.to_string(),
    };

    let claimed = s
        .substrate
        .store
        .ensure_active_execution_claim_for_lifecycle(
            s.sandbox_id,
            pplx_space_common_core::types::SandboxLifecycle::Stopped,
            None,
            Some((
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Stopped,
            )),
            Some(&upgrade),
            None,
        )
        .await;

    assert!(
        matches!(&claimed, Err(SubstrateError::Unavailable { .. })),
        "{:?}",
        claimed.map(drop)
    );
    assert_eq!(s.recorded_upgrade().await?, recorded);
    assert_eq!(recorded.0, Some(earlier));
    Ok(())
}

/// A drain's re-placement that finds a runtime upgrade's live claim never
/// joins it, though both target `stopped`: the upgrade's pod is a
/// placeholder booting its upgrade, not the re-placement, and joining would
/// release the upgrade's claim as the drain's own. It answers a retryable
/// refusal and leaves the claim alone.
#[tokio::test(flavor = "current_thread")]
async fn a_re_placement_never_joins_a_runtime_upgrades_claim() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", 3600).await?;
    record_upgrade_on_held_execution(&s).await?;
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
    let before = (s.active_execution().await?, left_by(&s).await?);

    let error = s
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
        .await
        .unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("runtime upgrade owns"), "{message}");
    assert_eq!((s.active_execution().await?, left_by(&s).await?), before);
    assert!(s.node().wakes.lock().unwrap().is_empty());
    Ok(())
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

/// A retry that took over an earlier attempt's execution ends that claim
/// however the read after the takeover goes: a claim left live would refuse
/// starts and retries until its lease ran out. Here that read finds nothing
/// recorded, so the claim is given up rather than expired with its target.
#[tokio::test(flavor = "current_thread")]
async fn a_takeover_whose_read_finds_no_upgrade_gives_its_claim_up() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject().await?;
    held_execution(&s, "stopped", -3600).await?;
    record_upgrade_on_held_execution(&s).await?;
    // Only an update that assigns another attempt, the takeover's, clears
    // the record, so only the read after the takeover misses it.
    sqlx::query(sql!(
        "CREATE FUNCTION clear_upgrade_record() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN
             IF NEW.activation_attempt_id IS DISTINCT FROM OLD.activation_attempt_id THEN
                 NEW.runtime_upgrade_operation_id := NULL;
                 NEW.runtime_upgrade_target_environment_ref := NULL;
                 NEW.runtime_upgrade_authorization_id := NULL;
             END IF;
             RETURN NEW;
         END $$"
    ))
    .execute(&s.db.pool)
    .await?;
    sqlx::query(sql!(
        "CREATE TRIGGER clear_upgrade_record BEFORE UPDATE ON sandbox_k8s_executions
         FOR EACH ROW EXECUTE FUNCTION clear_upgrade_record()"
    ))
    .execute(&s.db.pool)
    .await?;

    let error = s.upgrade().await.unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("takeover"), "{message}");
    assert_eq!(s.active_execution().await?, Some((7, None, false)));
    s.assert_nothing_placed_or_sent("refused");
    Ok(())
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

/// A boot that settled without reaching the target, whose execution then
/// couldn't be retired, answers retryably (503): only a retry takes the
/// execution over. Except a retire no retry can get past, an execution whose
/// pod name doesn't parse (`Failed`), which stays `Failed`, as it does for an
/// upgrade that reached the target.
#[test]
fn an_unretired_settled_failure_is_retryable_unless_no_retry_can_retire_it() {
    use crate::controllers::substrate::kubernetes::adapter::unretired_settled_failure;
    let rolled_back = || SubstrateError::Unavailable {
        message: "rolled back".to_string(),
    };
    let unretirable = unretired_settled_failure(
        rolled_back(),
        SubstrateError::Failed {
            message: "invalid pod".to_string(),
        },
    );
    assert!(
        matches!(&unretirable, SubstrateError::Failed { message }
            if message.contains("rolled back") && message.contains("invalid pod")),
        "{unretirable:?}"
    );
    let unretired = unretired_settled_failure(
        rolled_back(),
        SubstrateError::Unavailable {
            message: "delete failed".to_string(),
        },
    );
    assert!(
        matches!(&unretired, SubstrateError::Unavailable { message }
            if message.contains("rolled back") && message.contains("delete failed")),
        "{unretired:?}"
    );
}

/// The start's owed-backup check reads the claim outside the sandbox's lock,
/// so the upgrade's lease can end between the check, which skips a live claim,
/// and the start's claim, which clears the record. The claim decides again
/// under the lock: with the backup owed, published or not, and none enqueued
/// for the recorded operation, it's refused retryably and keeps the record,
/// and the retry, whose check now sees the lease over, enqueues the backup
/// and claims.
#[tokio::test(flavor = "current_thread")]
async fn a_lease_that_ends_after_the_owed_backup_check_doesnt_lose_the_backup()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            backup_enqueued(),
        ]
        .into(),
    ))
    .await?;
    s.upgrade().await.unwrap_err();
    s.assert_kept_for_a_retry("after the failed enqueue")
        .await?;
    let (operation, _, _) = s.recorded_upgrade().await?;
    let store = &s.substrate.store;
    let claim = |owed_backup_enqueued| {
        store.ensure_active_execution_claim_for_lifecycle(
            s.sandbox_id,
            pplx_space_common_core::types::SandboxLifecycle::Stopped,
            None,
            Some((
                pplx_space_common_core::types::SandboxActivationAttemptId::new(),
                SandboxTransitionTargetState::Started,
            )),
            None,
            owed_backup_enqueued,
        )
    };

    // The check runs while the claim's lease still runs, and skips it.
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET activation_attempt_expires_at = NOW() + INTERVAL '1 hour'
          WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    assert_eq!(
        s.substrate
            .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
            .await?,
        None
    );
    // Then the lease ends, before the start claims. The backup is owed
    // whether or not the node's publication of the upgrade is there: one
    // that's late or lost doesn't mean the disk is unchanged.
    s.end_the_lease().await?;
    sqlx::query(sql!(
        "DELETE FROM sandbox_observed_states WHERE sandbox_id = $1"
    ))
    .bind(s.sandbox_id)
    .execute(&s.db.pool)
    .await?;
    let error = claim(None).await.map(drop).unwrap_err();
    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("backup"), "{message}");
    assert_eq!(s.recorded_upgrade().await?.0, operation);
    assert_eq!(s.enqueued_backups().len(), 1, "only the upgrade's own");

    // The retry's check enqueues it, and its claim clears the record.
    let enqueued = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await?;
    assert_eq!(enqueued, operation);
    assert_eq!(s.enqueued_backups().len(), 2);
    claim(enqueued).await?;
    assert_eq!(s.recorded_upgrade().await?, (None, None, None));
    Ok(())
}

/// An upgrade that reached its target and published, but whose backup
/// wasn't enqueued, leaves its execution with the upgrade recorded and its
/// claim's lease over. A start that would take it over enqueues that owed
/// backup first, since its claim clears the record; while that enqueue
/// fails, the start is refused retryably and claims nothing, and the record
/// stays for the next one.
#[tokio::test(flavor = "current_thread")]
async fn a_start_is_refused_while_the_backup_its_takeover_owes_cant_be_enqueued()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject_whose_backups(ScriptedReply::Status(tonic::Status::internal(
        "backup queue full",
    )))
    .await?;
    s.upgrade().await.unwrap_err();
    s.assert_kept_for_a_retry("after the failed enqueue")
        .await?;
    let held = s.active_execution().await?;
    let recorded = s.recorded_upgrade().await?;

    let error = s
        .substrate
        .start_execution(
            crate::metrics::sandbox::api::SandboxApiMetricIdentity::native("start"),
            s.sandbox_id,
            crate::controllers::substrate::ActivationOptions {
                blocking: true,
                ready_timeout: Duration::from_millis(800),
            },
            crate::controllers::substrate::RecoveryCharge::Exempt,
        )
        .await
        .map(drop)
        .unwrap_err();

    let SubstrateError::Unavailable { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains("backup"), "{message}");
    assert_eq!(s.enqueued_backups().len(), 2);
    assert_eq!(s.active_execution().await?, held);
    assert_eq!(s.recorded_upgrade().await?, recorded);
    assert_eq!(s.pods().creates.load(Ordering::SeqCst), 1);
    assert!(s.node().wakes.lock().unwrap().is_empty());
    Ok(())
}

/// Where the owed backup's enqueue is pointed, and what the node answers.
/// The fixture's disk node is registered under its Kubernetes name, and an
/// earlier execution still names it.
#[derive(Clone, Copy, Debug)]
enum OwedBackupNode {
    /// The execution names the disk node.
    Execution,
    /// The execution names a node that's gone.
    Gone,
    /// The execution names none (its attempt ended before persisting the
    /// pod), and the sandbox's placement pins its disk to the registered
    /// disk node.
    DiskPin,
    /// The execution names none, and the placement's node registered no
    /// Kubernetes node name.
    PinUnregistered,
    /// The execution names none, and the placement pins the disk to a
    /// registered node whose Kubernetes node is gone.
    PinGone,
    /// The execution names none, and a re-placement has chosen another
    /// target, registered too, so the placement doesn't pin the disk to one
    /// node.
    PinReplacing,
    /// The execution names none, and the placement holds no node; only
    /// execution history names one.
    HistoryOnly,
}

impl OwedBackupNode {
    /// Point the upgrade's execution and the sandbox's placement as this
    /// case describes.
    async fn arrange(self, s: &Subject) -> Result<(), BoxError> {
        let pool = &s.db.pool;
        let execution_node = match self {
            Self::Execution => Some(WORKER_NODE),
            Self::Gone => Some("node-gone"),
            Self::DiskPin
            | Self::PinUnregistered
            | Self::PinGone
            | Self::PinReplacing
            | Self::HistoryOnly => None,
        };
        sqlx::query(sql!(
            "UPDATE sandbox_k8s_executions SET k8s_node_name = $2 WHERE sandbox_id = $1 AND active"
        ))
        .bind(s.sandbox_id)
        .bind(execution_node)
        .execute(pool)
        .await?;
        match self {
            Self::Execution | Self::Gone | Self::DiskPin => {}
            Self::PinUnregistered => {
                sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
                    .bind(s.node_id)
                    .execute(pool)
                    .await?;
            }
            Self::PinGone => {
                sqlx::query(sql!(
                    "UPDATE node_k8s SET k8s_node_name = 'node-gone' WHERE node_id = $1"
                ))
                .bind(s.node_id)
                .execute(pool)
                .await?;
            }
            Self::PinReplacing => {
                let target = pplx_space_k8s_node_id::space_k8s_node_id("node-target", "worker");
                pplx_space_db_test_fixtures::insert_ready_worker_node(pool, target).await?;
                sqlx::query(sql!(
                    "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, 'node-target')"
                ))
                .bind(target)
                .execute(pool)
                .await?;
                sqlx::query(sql!(
                    "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
                ))
                .bind(s.sandbox_id)
                .bind(target)
                .execute(pool)
                .await?;
            }
            Self::HistoryOnly => {
                sqlx::query(sql!(
                    "UPDATE sandboxes SET active_node_id = NULL WHERE id = $1"
                ))
                .bind(s.sandbox_id)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }
}

/// The backup a stopped upgrade left owed blocks an activation until it's
/// enqueued. A cause no retry changes is answered `FailedPrecondition` with
/// its reason: no registered node holds the disk, the node holding it is gone, or
/// sandbox-manager refuses the enqueue as `NotFound`, `FailedPrecondition`
/// or `InvalidArgument`. A cause that may clear stays retryable, including a
/// sandbox the node hasn't stopped yet, which it answers `Aborted`. Either way
/// the record stays. An execution that names no node has its backup enqueued
/// on the node the sandbox's placement pins its disk to, and only when that
/// node registered a Kubernetes node name; execution history alone, an
/// unregistered node or a re-placement's target is refused for good.
#[rstest::rstest]
#[case::enqueued(OwedBackupNode::Execution, None, None)]
#[case::through_the_disk_pin(OwedBackupNode::DiskPin, None, None)]
#[case::pin_unregistered(OwedBackupNode::PinUnregistered, None, Some((true, "no registered node")))]
#[case::pin_gone(OwedBackupNode::PinGone, None, Some((true, "which is gone")))]
#[case::pin_replacing(OwedBackupNode::PinReplacing, None, Some((true, "no registered node")))]
#[case::history_only(OwedBackupNode::HistoryOnly, None, Some((true, "no registered node")))]
#[case::node_gone(OwedBackupNode::Gone, None, Some((true, "which is gone")))]
#[case::sandbox_unknown(OwedBackupNode::Execution, Some(tonic::Code::NotFound), Some((true, "NotFound")))]
#[case::refused(
    OwedBackupNode::Execution,
    Some(tonic::Code::FailedPrecondition),
    Some((true, "FailedPrecondition"))
)]
#[case::malformed(
    OwedBackupNode::Execution,
    Some(tonic::Code::InvalidArgument),
    Some((true, "InvalidArgument"))
)]
#[case::not_yet_stopped(OwedBackupNode::Execution, Some(tonic::Code::Aborted), Some((false, "retry")))]
#[case::failing(OwedBackupNode::Execution, Some(tonic::Code::Internal), Some((false, "retry")))]
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_that_can_never_be_enqueued_is_refused_for_good(
    #[case] node: OwedBackupNode,
    #[case] refusal: Option<tonic::Code>,
    #[case] expected: Option<(bool, &str)>,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject_whose_backups(match refusal {
        Some(code) => ScriptedReply::Status(tonic::Status::new(code, "refused")),
        None => backup_enqueued(),
    })
    .await?;
    held_execution(&s, "stopped", -3600).await?;
    let operation = record_upgrade_on_held_execution(&s).await?;
    node.arrange(&s).await?;
    let recorded = s.recorded_upgrade().await?;

    let owed = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await;

    match expected {
        None => {
            assert_eq!(owed?, Some(operation));
            assert_eq!(s.enqueued_backups().len(), 1);
        }
        Some((lasting, reason)) => {
            let error = owed.unwrap_err();
            let message = match (&error, lasting) {
                (SubstrateError::FailedPrecondition { message }, true)
                | (SubstrateError::Unavailable { message }, false) => message,
                _ => panic!("{error:?}"),
            };
            assert!(message.contains(reason), "{message}");
            assert_eq!(s.recorded_upgrade().await?, recorded);
        }
    }
    Ok(())
}
