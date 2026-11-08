//! The disk backup an upgrade that reached its target owes: enqueued through
//! its node before the retire, and the backup a later activation enqueues for
//! an upgrade that stopped short of that, only once the upgrade's pod is gone
//! and its node abandoned the operation.

use super::*;

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
    // The upgrade's pod is gone, as the owed backup's fence leaves it
    // (`an_owed_backup_waits_until_the_upgrades_pod_is_gone`).
    *s.pods().pod.lock().unwrap() = None;
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
    // The upgrade's pod is gone, as the owed backup's fence leaves it
    // (`an_owed_backup_waits_until_the_upgrades_pod_is_gone`).
    *s.pods().pod.lock().unwrap() = None;
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
            crate::controllers::substrate::RecoveryCharge::exempt_for_tests(),
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

/// The upgrade's pod may still be pending in its create, which kubelet keeps
/// retrying, and a retry could boot the upgrade after the owed backup
/// captured the disk. So the owed backup never captures while that pod is
/// there: it deletes the pod under its UID and refuses retryably, capturing
/// nothing, until the API server no longer has it. Then it enqueues, and
/// retires the upgrade's execution with the sandbox's placement kept, so a
/// start right after mints a fresh execution and places a pod of its own
/// rather than reusing an execution whose pod is gone.
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_waits_until_the_upgrades_pod_is_gone() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject_whose_backups(ScriptedReply::Sequence(
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
    let (upgraded_generation, _, _) = s.active_execution().await?.expect("the kept execution");
    let (operation, _, _) = s.recorded_upgrade().await?;
    let operation = operation.expect("the recorded upgrade");
    let attempts = s.enqueued_backups().len();
    let placement = || async {
        sqlx::query_as::<_, (Option<pplx_space_common_core::types::NodeId>, String)>(sql!(
            "SELECT active_node_id, placement FROM sandboxes WHERE id = $1"
        ))
        .bind(s.sandbox_id)
        .fetch_one(&s.db.pool)
        .await
    };
    let placed = placement().await?;

    let first = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await;

    let Err(SubstrateError::Unavailable { message }) = &first else {
        panic!("{first:?}");
    };
    assert!(message.contains("gone; retry"), "{message}");
    assert!(!s.pods().exists(), "the upgrade's pod is still there");
    assert_eq!(
        s.enqueued_backups().len(),
        attempts,
        "a backup was captured while the upgrade's pod could still boot"
    );
    assert_eq!(s.recorded_upgrade().await?.0, Some(operation));

    let second = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await?;

    assert_eq!(second, Some(operation));
    assert_eq!(
        s.node().abandons(),
        [(operation.to_string(), true)],
        "the node abandons the operation once its pod is gone, and only then"
    );
    assert_eq!(s.enqueued_backups().len(), attempts + 1);
    assert_eq!(
        s.active_execution().await?,
        None,
        "the upgrade's execution wasn't retired"
    );
    assert_eq!(placement().await?, placed, "the retire moved the placement");

    s.cache_the_pod();
    let _ = start_the_sandbox(&s).await;

    let (generation, _, _) = s.active_execution().await?.expect("the start's execution");
    assert!(
        generation > upgraded_generation,
        "the start reused the upgrade's execution"
    );
    assert!(s.pods().exists(), "the start placed no pod");
    Ok(())
}

/// A pod that's gone doesn't prove its shim's `UpgradeRuntime` call is: one
/// force-deleted mid-create can still have it on its way to the node. So the
/// owed backup captures only once the node has abandoned the operation, and
/// a node that refuses (an operation still fencing the sandbox) or can't
/// serve the call yet (an older sandbox-manager) gets a retryable refusal,
/// with nothing captured and the execution and its record kept.
#[rstest::rstest]
#[case::still_fencing(tonic::Code::Aborted)]
#[case::older_node(tonic::Code::Unimplemented)]
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_captures_only_once_the_node_abandoned_the_upgrade(
    #[case] refusal: tonic::Code,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            backup_enqueued(),
        ]
        .into(),
    ))
    .await?;
    s.upgrade().await.unwrap_err();
    let kept = s.active_execution().await?.expect("the kept execution");
    let (operation, _, _) = s.recorded_upgrade().await?;
    let attempts = s.enqueued_backups().len();
    s.node().refuse_abandons(refusal);
    // The first call removes the upgrade's pod.
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    s.cache_the_pod();

    let refused = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await;

    let Err(SubstrateError::Unavailable { message }) = &refused else {
        panic!("{refused:?}");
    };
    assert!(message.contains("isn't abandoned on its node"), "{message}");
    assert!(!s.node().abandons().is_empty(), "the node was never asked");
    assert_eq!(
        s.enqueued_backups().len(),
        attempts,
        "a backup was captured before the node abandoned the upgrade"
    );
    assert_eq!(s.active_execution().await?, Some(kept));
    assert_eq!(s.recorded_upgrade().await?.0, operation);
    Ok(())
}

/// A node that records the upgrade's operation for another sandbox refuses
/// to abandon it for this one (`INVALID_ARGUMENT`), and no retry changes
/// that. So the owed backup is refused for good (`FailedPrecondition`), not
/// retried forever, with nothing captured and the execution and its record
/// kept for an operator.
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_whose_operation_the_node_records_for_another_sandbox_is_refused_for_good()
-> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            backup_enqueued(),
        ]
        .into(),
    ))
    .await?;
    s.upgrade().await.unwrap_err();
    let kept = s.active_execution().await?.expect("the kept execution");
    let (operation, _, _) = s.recorded_upgrade().await?;
    let recorded = operation.expect("the recorded upgrade");
    let attempts = s.enqueued_backups().len();
    s.node().record_for_another_sandbox(&recorded.to_string());
    // The first call removes the upgrade's pod.
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    s.cache_the_pod();

    let refused = s
        .substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await;

    let Err(SubstrateError::FailedPrecondition { message }) = &refused else {
        panic!("{refused:?}");
    };
    assert!(
        message.contains("can't be abandoned on its node"),
        "{message}"
    );
    assert!(message.contains("names another sandbox"), "{message}");
    assert_eq!(
        s.node().abandons(),
        [(recorded.to_string(), true)],
        "the node was asked once the pod was gone"
    );
    assert_eq!(
        s.enqueued_backups().len(),
        attempts,
        "a backup was captured though the node never abandoned the upgrade"
    );
    assert_eq!(s.active_execution().await?, Some(kept));
    assert_eq!(s.recorded_upgrade().await?.0, operation);
    Ok(())
}

/// An upgrade's attempt ended unsettled (the node hadn't begun it, or
/// reported a rollback naming no pod) and its lease ran out. A start's owed
/// backup abandoned its operation on the node, then its enqueue failed, so
/// the execution still records the operation. A retry
/// of the upgrade takes that execution over and places the same operation
/// again, which the node refuses before it fences. The node reports it
/// abandoned, and its report never changes again: absent, for one that never
/// began, or the rollback it recorded, naming an earlier pod or none. Either
/// way the retry settles it and retires the execution, keeping the
/// placement, instead of running out its deadline and keeping another lease.
/// The next decision names a fresh operation, so the upgrade then succeeds.
#[rstest::rstest]
#[case::never_began(Report::InProgress, Report::Absent)]
#[case::rolled_back_naming_no_pod(Report::UnnamedRollback, Report::UnnamedRollback)]
#[tokio::test(flavor = "current_thread")]
async fn a_retry_of_an_abandoned_upgrade_settles_instead_of_timing_out(
    #[case] first: Report,
    #[case] then: Report,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    // The owed backup's enqueue fails once, then the fresh upgrade's
    // succeeds.
    let mut s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            backup_enqueued(),
        ]
        .into(),
    ))
    .await?;
    // The upgrade never boots: the node hasn't begun it by the deadline, and
    // its lease then runs out.
    s.node().report(first);
    s.upgrade_unsettled().await;
    s.end_the_lease().await?;
    let (operation, _, _) = s.recorded_upgrade().await?;
    let operation = operation.expect("the recorded upgrade");
    // Owned copies, so the closure doesn't hold `s` borrowed across
    // `cache_the_pod`, which needs it mutably.
    let (pool, sandbox_id) = (s.db.pool.clone(), s.sandbox_id);
    let placement = || {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (Option<pplx_space_common_core::types::NodeId>, String)>(sql!(
                "SELECT active_node_id, placement FROM sandboxes WHERE id = $1"
            ))
            .bind(sandbox_id)
            .fetch_one(&pool)
            .await
        }
    };
    let placed = placement().await?;
    // The owed backup removes the pod, then abandons the operation and fails
    // to enqueue.
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    s.cache_the_pod();
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    assert_eq!(s.node().abandons(), [(operation.to_string(), true)]);
    assert_eq!(s.recorded_upgrade().await?.0, Some(operation));
    s.node().report(then);

    let retried = s.upgrade().await;

    assert!(
        matches!(retried, Err(SubstrateError::Unavailable { .. })),
        "{retried:?}"
    );
    assert_eq!(
        s.active_execution().await?,
        None,
        "the retry kept the abandoned upgrade's execution"
    );
    assert_eq!(placement().await?, placed, "the retire moved the placement");

    // A fresh decision names a fresh operation, which the node hasn't
    // abandoned, so once the failure clears the upgrade goes through.
    s.node().report(Report::Upgraded { publish: true });
    let fresh = upgraded(
        &s.upgrade()
            .await
            .expect("a fresh decision isn't refused as the abandoned operation"),
    );
    assert_ne!(fresh, operation);
    Ok(())
}

/// A retry of the upgrade can take its execution over while the owed backup
/// is being enqueued, and boot again after the capture. The retry names the
/// same operation, so the backup proves nothing for it: the helper answers
/// retryably instead of the operation, keeping the retry's execution and its
/// record, and a claim can't clear that record with the older capture.
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_whose_execution_changed_hands_proves_nothing() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            ScriptedReply::Delayed(
                std::time::Duration::from_millis(400),
                std::sync::Arc::new(backup_enqueued()),
            ),
        ]
        .into(),
    ))
    .await?;
    s.upgrade().await.unwrap_err();
    let (generation, _, _) = s.active_execution().await?.expect("the kept execution");
    let (operation, _, _) = s.recorded_upgrade().await?;
    // The first call removes the upgrade's pod and captures nothing.
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    let retry_attempt = uuid::Uuid::now_v7();

    let (owed, taken_over) = tokio::join!(
        s.substrate
            .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            sqlx::query(sql!(
                "UPDATE sandbox_k8s_executions
                    SET activation_attempt_id = $2,
                        activation_attempt_expires_at = NOW() + INTERVAL '5 minutes'
                  WHERE sandbox_id = $1 AND active"
            ))
            .bind(s.sandbox_id)
            .bind(retry_attempt)
            .execute(&s.db.pool)
            .await
        }
    );
    taken_over?;

    let Err(SubstrateError::Unavailable { message }) = &owed else {
        panic!("the older capture was answered for the retry: {owed:?}");
    };
    assert!(message.contains("changed hands"), "{message}");
    let attempt: Option<uuid::Uuid> = sqlx::query_scalar(sql!(
        "SELECT activation_attempt_id FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active AND generation = $2"
    ))
    .bind(s.sandbox_id)
    .bind(generation)
    .fetch_one(&s.db.pool)
    .await?;
    assert_eq!(
        attempt,
        Some(retry_attempt),
        "the retry's execution was retired"
    );
    assert_eq!(s.recorded_upgrade().await?.0, operation);
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
        // The upgrade's pod is gone, as the owed backup's fence leaves it
        // (`an_owed_backup_waits_until_the_upgrades_pod_is_gone`).
        *s.pods().pod.lock().unwrap() = None;

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

/// The owed backup takes the upgrade's expired claim over before it touches
/// the upgrade's pod or operation, which a retry of the upgrade reuses. A
/// retry that comes while it holds the claim is refused as an upgrade in
/// flight and touches nothing, and the backup goes on to enqueue and retire.
/// An owed backup that answers early (the pod isn't gone yet) ends that
/// claim's lease again, so the next attempt retries at once.
#[tokio::test(flavor = "current_thread")]
async fn an_owed_backup_holds_the_upgrades_claim_while_it_cleans_up() -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject_whose_backups(ScriptedReply::Sequence(
        vec![
            ScriptedReply::Status(tonic::Status::internal("backup queue full")),
            ScriptedReply::Delayed(
                std::time::Duration::from_millis(400),
                std::sync::Arc::new(backup_enqueued()),
            ),
        ]
        .into(),
    ))
    .await?;
    s.upgrade().await.unwrap_err();
    let (operation, _, _) = s.recorded_upgrade().await?;
    let operation = operation.expect("the recorded upgrade");

    // The first call removes the pod and answers early: the claim it took
    // over is expired again.
    s.substrate
        .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id)
        .await
        .unwrap_err();
    let (_, target, live) = s.active_execution().await?.expect("the kept execution");
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(!live, "an early answer kept the owed backup's claim");
    s.cache_the_pod();
    let creates = s.pods().creates.load(Ordering::SeqCst);

    let (owed, retried) = tokio::join!(
        s.substrate
            .enqueue_backup_owed_by_stopped_upgrade(s.sandbox_id),
        async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            s.upgrade().await
        }
    );

    assert_eq!(owed?, Some(operation));
    let Err(SubstrateError::Unavailable { message }) = &retried else {
        panic!("the retry ran during the owed backup: {retried:?}");
    };
    assert!(message.contains("in flight"), "{message}");
    assert_eq!(
        s.pods().creates.load(Ordering::SeqCst),
        creates,
        "the retry placed a pod"
    );
    assert_eq!(
        s.active_execution().await?,
        None,
        "the execution wasn't retired"
    );
    Ok(())
}

/// A retry of an upgrade that may owe its disk backup can replace the
/// generation the upgrade ran under and copy its record onto the new claim.
/// That claim may then be abandoned before its pod is created, by the generic
/// cleanup of a create that fails early (a prior-generation pod sweep that
/// runs out of budget, say). The claim is the only trace of the debt, so it
/// isn't retired: it keeps its record, with its lease ended, and the next
/// start enqueues the owed backup through the disk pin before it claims
/// anything, then boots on a new generation. A claim with no record is
/// abandoned as before.
#[rstest::rstest]
#[case::recorded(true)]
#[case::unrecorded(false)]
#[tokio::test(flavor = "current_thread")]
async fn an_abandoned_create_claim_keeps_a_recorded_upgrades_backup_debt(
    #[case] recorded: bool,
) -> Result<(), BoxError> {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.node().report(Report::InProgress);
    s.upgrade_unsettled().await;
    let (operation, _, _) = s.recorded_upgrade().await?;
    let operation = operation.expect("the recorded upgrade");
    // The replacement claim: still creating, no pod or node yet, carrying
    // the record or not.
    let (generation, attempt): (
        i64,
        pplx_space_common_core::types::SandboxActivationAttemptId,
    ) = sqlx::query_as(sql!(
        "UPDATE sandbox_k8s_executions
                SET state = 'creating', pod_uid = NULL, pod_ip = NULL, k8s_node_name = NULL,
                    runtime_upgrade_operation_id =
                        CASE WHEN $2 THEN runtime_upgrade_operation_id END,
                    runtime_upgrade_target_environment_ref =
                        CASE WHEN $2 THEN runtime_upgrade_target_environment_ref END,
                    runtime_upgrade_authorization_id =
                        CASE WHEN $2 THEN runtime_upgrade_authorization_id END
              WHERE sandbox_id = $1 AND active
          RETURNING generation, activation_attempt_id"
    ))
    .bind(s.sandbox_id)
    .bind(recorded)
    .fetch_one(&s.db.pool)
    .await?;
    *s.pods().pod.lock().unwrap() = None;
    s.cache_the_pod();
    let enqueued = s.enqueued_backups().len();

    let abandoned = s
        .substrate
        .store
        .abandon_unmaterialized_create_claim_by_generation(
            s.sandbox_id,
            generation,
            None,
            Some(attempt),
        )
        .await?;

    assert_eq!(abandoned, !recorded);
    if !recorded {
        assert_eq!(s.active_execution().await?, None);
        return Ok(());
    }
    let (kept, target, live) = s.active_execution().await?.expect("the recorded claim");
    assert_eq!(kept, generation);
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(!live, "the kept claim's lease still runs");
    assert_eq!(s.recorded_upgrade().await?.0, Some(operation));

    let _ = start_the_sandbox(&s).await;

    assert_eq!(
        s.enqueued_backups().len(),
        enqueued + 1,
        "the start dropped the backup the upgrade owed"
    );
    let (started, _, _) = s.active_execution().await?.expect("the start's execution");
    assert!(started > generation, "the start reused the recorded claim");
    Ok(())
}

/// The upgrade's pod create commits but answers with a transport error, and
/// the read that follows finds that pod terminal. The stale-pod cleanup
/// deletes it, but the execution records the upgrade, which may already have
/// upgraded the disk and so owes its backup, so the cleanup keeps it: back to
/// `creating` with the deleted pod forgotten, record kept, lease ended, and
/// the sandbox still placed on its disk node. A start then enqueues the owed
/// backup before it claims anything, and boots on a new generation.
#[tokio::test(flavor = "current_thread")]
async fn a_stale_upgrade_pod_after_a_lost_create_answer_keeps_the_backup_debt()
-> Result<(), BoxError> {
    use std::sync::atomic::Ordering;
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let mut s = subject().await?;
    s.pods()
        .creates_fail_after_commit
        .store(true, Ordering::SeqCst);
    s.pods().created_pods_fail.store(true, Ordering::SeqCst);
    let placement = || async {
        sqlx::query_as::<_, (Option<pplx_space_common_core::types::NodeId>, String)>(sql!(
            "SELECT active_node_id, placement FROM sandboxes WHERE id = $1"
        ))
        .bind(s.sandbox_id)
        .fetch_one(&s.db.pool)
        .await
    };
    let placed = placement().await?;

    s.upgrade().await.unwrap_err();

    let (generation, target, live) = s
        .active_execution()
        .await?
        .expect("the cleanup retired the upgrade's execution");
    assert_eq!(target.as_deref(), Some("stopped"));
    assert!(!live, "the kept execution's lease still runs");
    let (operation, _, _) = s.recorded_upgrade().await?;
    assert!(
        operation.is_some(),
        "the cleanup dropped the upgrade's record"
    );
    assert_eq!(
        placement().await?,
        placed,
        "the cleanup moved the placement"
    );
    let pod_uid: Option<String> = sqlx::query_scalar(sql!(
        "SELECT pod_uid FROM sandbox_k8s_executions WHERE sandbox_id = $1 AND active"
    ))
    .bind(s.sandbox_id)
    .fetch_one(&s.db.pool)
    .await?;
    assert_eq!(
        pod_uid, None,
        "the kept execution still records the deleted pod"
    );

    s.pods()
        .creates_fail_after_commit
        .store(false, Ordering::SeqCst);
    s.pods().created_pods_fail.store(false, Ordering::SeqCst);
    *s.pods().pod.lock().unwrap() = None;
    s.cache_the_pod();
    let enqueued = s.enqueued_backups().len();
    let _ = start_the_sandbox(&s).await;

    assert_eq!(
        s.enqueued_backups().len(),
        enqueued + 1,
        "the start dropped the backup the upgrade owed"
    );
    let (started, _, _) = s.active_execution().await?.expect("the start's execution");
    assert!(
        started > generation,
        "the start reused the upgrade's execution"
    );
    Ok(())
}
