use pplx_space_common_core::types::JobId;

use super::*;
use crate::service::cleanup_support::{
    CleanupCompletion, CleanupLockGuard, CleanupLockTryResult, DEFAULT_CLEANUP_CONCURRENCY_CAP,
};
use crate::service::tests::service_tests::{insert_test_sandbox, service_for_teardown_test};
use pplx_space_common_jobs::db as jobs;

fn work(conn: &crate::db::Connection, kill: bool) -> (JobId, CleanupWork) {
    let sandbox_id = SandboxId::new();
    insert_test_sandbox(conn, &sandbox_id.to_string(), SandboxState::Deleting);
    let work = CleanupWork {
        version: 1,
        sandbox_id,
        uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        execution_generation: None,
        cleanup_id: None,
        completion: CleanupCompletion::DeleteRow,
        keep_filesystem: false,
        immediate_kill: kill,
        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
        published_artifact: None,
    };
    let tx = crate::db::Transaction::deferred(conn).unwrap();
    let id = cleanup_jobs::persist(&tx, work.clone()).unwrap();
    tx.commit().unwrap();
    (id, work)
}

#[tokio::test]
async fn blocking_stop_follows_a_retryable_attempt_to_durable_completion() {
    use pplx_space_node_mgr_client::proto::{CleanupSandboxRequest, CleanupSandboxResponse};

    let cleanups = Arc::new(Mutex::new(Vec::new()));
    let node_manager = crate::node_manager_transport::fake::FakeNodeManager::default()
        .unary("CleanupSandbox", {
            let cleanups = Arc::clone(&cleanups);
            move |request: CleanupSandboxRequest| {
                let mut cleanups = cleanups.lock().unwrap();
                cleanups.push(request);
                if cleanups.len() == 1 {
                    Err(Status::unavailable("runtime still exiting"))
                } else {
                    Ok(CleanupSandboxResponse::default())
                }
            }
        })
        .serve();
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let subject = sandbox_id.to_string();
    insert_test_sandbox(&conn, &subject, SandboxState::Stopping);
    let service = crate::service::tests::node_manager_transport_tests::service_with_node_manager(
        conn,
        node_manager.connector(),
    );
    let execution_fence =
        crate::db::node_execution_fence(&service.db.lock().unwrap(), &subject).unwrap();
    assert!(execution_fence.is_some());
    let label = SandboxRuntimeLabel::from_sandbox_id(sandbox_id);
    let request = SynchronousCleanupRequest {
        sandbox_id: &subject,
        uuid8: label.as_str(),
        keep_filesystem: true,
        context: "blocking stop regression",
        completion: CleanupCompletion::MarkStopped,
        immediate_kill: true,
        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
        published_artifact: None,
    };
    tokio::time::timeout(
        Duration::from_secs(5),
        service.run_cleanup_to_completion_using_admission(request, None, None, CleanupWait::Stop),
    )
    .await
    .unwrap()
    .unwrap();
    // Each attempt asks node-manager for the stop the caller requested.
    let requested = CleanupSandboxRequest {
        execution_fence: execution_fence.map(Into::into),
        sandbox_id: subject.clone(),
        force: true,
        keep_filesystem: true,
        return_after_slot_release: true,
        immediate_kill: true,
    };
    assert_eq!(*cleanups.lock().unwrap(), [requested.clone(), requested]);
    assert_eq!(node_manager.calls(), ["CleanupSandbox", "CleanupSandbox"]);
    assert_eq!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &subject)
            .unwrap()
            .unwrap()
            .state,
        SandboxState::Stopped
    );
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

#[tokio::test]
async fn stop_waiter_bounds_retries_without_losing_the_durable_job() {
    use tonic::Code;
    for (state, code, expected) in [
        (
            JobStatus::Pending,
            Code::Unavailable,
            Code::DeadlineExceeded,
        ),
        (
            JobStatus::Pending,
            Code::ResourceExhausted,
            Code::DeadlineExceeded,
        ),
        (
            JobStatus::Pending,
            Code::DeadlineExceeded,
            Code::DeadlineExceeded,
        ),
        (
            JobStatus::Pending,
            Code::PermissionDenied,
            Code::PermissionDenied,
        ),
        (JobStatus::Pending, Code::Internal, Code::Internal),
        (JobStatus::Pending, Code::Unknown, Code::Unavailable),
        (JobStatus::Failed, Code::Unavailable, Code::Unavailable),
        (JobStatus::Cancelled, Code::Unavailable, Code::Aborted),
    ] {
        let conn = crate::db::test_conn();
        let (id, _) = work(&conn, true);
        jobs::mark_job_running(conn.raw(), &id).unwrap();
        let failure = if code == Code::Unknown {
            "legacy cleanup failure".to_owned()
        } else {
            serde_json::to_string(&CleanupFailure {
                code: code as i32,
                message: "test failure".into(),
            })
            .unwrap()
        };
        match state {
            JobStatus::Pending => {
                jobs::retry_running_job(conn.raw(), &id, &failure, 30).unwrap();
            }
            JobStatus::Failed => {
                jobs::fail_job_permanent(conn.raw(), &id, &failure, None).unwrap();
            }
            JobStatus::Cancelled => {
                jobs::request_job_cancellation(conn.raw(), &id).unwrap();
                jobs::mark_job_cancelled(conn.raw(), &id).unwrap();
            }
            _ => unreachable!(),
        }
        let service = service_for_teardown_test(conn);
        let budget = if expected == Code::DeadlineExceeded {
            Duration::from_millis(50)
        } else {
            Duration::from_secs(5)
        };
        let result = service.wait_stop_cleanup_job(id, budget).await;
        let error = result.unwrap_err();
        assert_eq!(error.code(), expected, "{state:?} {code:?}");
        if code == Code::Unknown {
            assert_eq!(error.message(), failure);
            let original = service.wait_cleanup_job(id).await.unwrap_err();
            assert_eq!(original.code(), Code::Unavailable);
            assert_eq!(original.message(), failure);
        }
        assert_eq!(
            jobs::get_job(service.db.lock().unwrap().raw(), &id)
                .unwrap()
                .unwrap()
                .status,
            state.as_str()
        );
        assert!(service.cleanup_executor.waiters.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn burst_is_bounded_fair_coalesced_and_independent_of_waiter_cancellation() {
    let conn = crate::db::test_conn();
    let jobs: Vec<_> = (0..64).map(|n| work(&conn, n % 2 == 0)).collect();
    let service = service_for_teardown_test(conn);
    let capacity = DEFAULT_CLEANUP_CONCURRENCY_CAP;
    let (started, mut starts) = tokio::sync::mpsc::channel(capacity);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
        let started = started.clone();
        Box::pin(async move {
            let (release, wait) = tokio::sync::oneshot::channel();
            started.send((id, kill, release)).await.unwrap();
            wait.await.unwrap()
        })
    }));
    service.ensure_cleanup_executor();
    let mut batch = Vec::new();
    for _ in 0..capacity {
        batch.push(
            tokio::time::timeout(Duration::from_secs(5), starts.recv())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    assert!(
        batch.iter().any(|(_, kill, _)| *kill),
        "foreground must make progress"
    );
    assert!(
        batch.iter().any(|(_, kill, _)| !kill),
        "background must make progress"
    );
    assert_eq!(service.cleanup_executor.active.len(), capacity);
    assert!(
        starts.try_recv().is_err(),
        "no tasks beyond the capacity budget may dispatch"
    );

    let active_id = &batch[0].0;
    let (job_id, original) = jobs
        .iter()
        .find(|(_, work)| work.sandbox_id.to_string() == *active_id)
        .unwrap();
    // Duplicate hints cannot create another runner while this mutation owns its fence.
    for _ in 0..32 {
        service.schedule_async_cleanup(AsyncCleanupRequest {
            sandbox_id: original.sandbox_id,
            uuid8: original.uuid8.clone(),
            keep_filesystem: false,
            context: "duplicate cleanup hint",
            completion: CleanupCompletion::DeleteRow,
            immediate_kill: original.immediate_kill,
            guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
            persist_intent: false,
            cleanup_id: None,
        });
    }
    let waiter_service = service.clone();
    let awaited = *job_id;
    let waiter = tokio::spawn(async move { waiter_service.wait_cleanup_job(awaited).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while service.cleanup_executor.waiters.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(service.cleanup_executor.waiters.lock().unwrap().is_empty());
    assert_eq!(service.cleanup_executor.active.len(), capacity);

    for (_, _, release) in batch {
        release.send(Ok(())).unwrap();
    }
    for _ in capacity..jobs.len() {
        let (_, _, release) = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        release.send(Ok(())).unwrap();
    }
    for (id, _) in jobs {
        service.wait_cleanup_job(id).await.unwrap();
    }
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    assert!(service.cleanup_executor.active.is_empty());
    assert!(service.cleanup_locks.lock().unwrap().is_empty());
}

#[tokio::test]
async fn restarted_claim_retries_without_blocking_other_completions() {
    let conn = crate::db::test_conn();
    let (failed_id, failed) = work(&conn, false);
    let (successful_id, _) = work(&conn, true);
    jobs::mark_job_running(conn.raw(), &failed_id).unwrap();
    let service = service_for_teardown_test(conn);
    let failed_subject = failed.sandbox_id.to_string();
    let (attempted, mut attempts) = tokio::sync::mpsc::channel(2);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, _| {
        let attempted = attempted.clone();
        let fail = id == failed_subject;
        Box::pin(async move {
            attempted.send(()).await.unwrap();
            if fail {
                Err(Status::resource_exhausted("node capacity"))
            } else {
                Ok(())
            }
        })
    }));
    service.ensure_cleanup_executor();
    service.wait_cleanup_job(successful_id).await.unwrap();
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(5), attempts.recv())
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let done = {
                let db = service.db.lock().unwrap();
                jobs::get_job(db.raw(), &failed_id).unwrap().unwrap().status
                    == JobStatus::Pending.as_str()
            };
            if done {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    {
        let db = service.db.lock().unwrap();
        assert!(
            crate::db::cleanup_intent_current(
                &db,
                &failed.sandbox_id.to_string(),
                CleanupCompletion::DeleteRow
            )
            .unwrap()
        );
        assert!(
            cleanup_jobs::due_job_ids(&db, JobPriority::new(0), &[], 16)
                .unwrap()
                .is_empty(),
            "retry must honor durable backoff"
        );
    }
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    assert_eq!(service.active_state_transition_count(), 0);
}

// The cases share no state, so each gets its own `#[tokio::test]` below and
// nextest runs them in parallel; the test name is what identifies a failing
// case. `rstest` is not a dependency of this crate and this split adds none.
async fn lifecycle_and_job_completion_commit_together_and_recover(
    initial: SandboxState,
    completion: CleanupCompletion,
    activation: Option<crate::db::ActivationKind>,
    artifact: bool,
    target: Option<SandboxState>,
) {
    use crate::db::ActivationKind::Restore;
    use crate::service::tests::service_tests::register_full_suspend_artifact;
    use CleanupCompletion::Quarantine;
    use pplx_space_common_core::types::SandboxActivationAttemptId;
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let subject = sandbox_id.to_string();
    insert_test_sandbox(
        &conn,
        &subject,
        if artifact {
            SandboxState::Suspended
        } else {
            initial
        },
    );
    if artifact {
        register_full_suspend_artifact(&conn, &subject);
        conn.execute(
            "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
            [initial.as_str(), &subject],
        )
        .unwrap();
    }
    let episode = activation.map(|kind| {
        let episode = crate::db::begin_activation(
            &conn,
            &subject,
            initial,
            kind,
            Some(ExecutionGeneration::new(1).unwrap()),
            Some(SandboxActivationAttemptId::new()),
        )
        .unwrap()
        .unwrap();
        let reason = if kind == Restore {
            SandboxQuarantineReasonCode::RestoreActivationInterrupted
        } else {
            SandboxQuarantineReasonCode::ColdActivationInterrupted
        };
        crate::db::request_cleanup_episode(
            &conn,
            &subject,
            episode.id,
            reason,
            "activation interrupted",
        )
        .unwrap();
        episode.id
    });
    let work = CleanupWork {
        version: 1,
        sandbox_id,
        uuid8: SandboxRuntimeLabel::from_sandbox_id(sandbox_id),
        execution_generation: crate::db::runtime_execution_generation(&conn, &subject).unwrap(),
        cleanup_id: episode,
        completion,
        keep_filesystem: completion != CleanupCompletion::DeleteRow,
        immediate_kill: false,
        guest_flush: crate::db::cleanup_jobs::GuestFlush::Unrequested,
        published_artifact: None,
    };
    let before = crate::db::get_sandbox(&conn, &subject)
        .unwrap()
        .unwrap()
        .state;
    let tx = crate::db::Transaction::deferred(&conn).unwrap();
    let id = cleanup_jobs::persist(&tx, work).unwrap();
    tx.commit().unwrap();
    jobs::mark_job_running(conn.raw(), &id).unwrap();
    conn.execute_batch("CREATE TRIGGER reject_cleanup_completion BEFORE UPDATE ON jobs
        WHEN NEW.status IN ('completed', 'failed') BEGIN SELECT RAISE(FAIL, 'completion storage unavailable'); END;").unwrap();
    let service = service_for_teardown_test(conn);
    if episode.is_some() {
        service.watchdog.rearm_sandbox_from_db(&subject).await;
        service
            .snapshot_scheduler
            .add_sandbox(subject.clone(), "seed".into())
            .await;
        service.auto_lifecycle.record_wake_completed(&subject);
    }
    service
        .network_policy_reconcile_queue
        .enqueue(subject.clone());
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatched = calls.clone();
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, _| {
        dispatched.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }));
    service
        .run_durable_cleanup_attempt(&id, &subject, false)
        .await
        .unwrap_err();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    if episode.is_some() {
        assert!(service.watchdog.get_health(&subject).await.is_some());
        assert!(service.snapshot_scheduler.is_active(&subject).await);
        assert!(
            service
                .auto_lifecycle
                .since_last_wake_completed(&subject)
                .is_some()
        );
    }
    {
        let db = service.db.lock().unwrap();
        assert_eq!(
            crate::db::get_sandbox(&db, &subject)
                .unwrap()
                .unwrap()
                .state,
            before,
            "failed job commit must roll back lifecycle finalization: {completion:?}"
        );
        assert_eq!(
            crate::db::cleanup_episode(&db, &subject)
                .unwrap()
                .map(|episode| episode.id),
            episode
        );
        assert_eq!(
            jobs::get_job(db.raw(), &id).unwrap().unwrap().status,
            JobStatus::Pending.as_str()
        );
        db.execute_batch("DROP TRIGGER reject_cleanup_completion")
            .unwrap();
    }
    // Keep retirement pending after its terminal commit. Cancelling a
    // caller at this boundary must not abandon the executor-owned tail.
    let replacement_sidecar = service.watchdog.sidecar_info_for_test(&subject).await;
    let retirement_guard = episode.map(|_| service.watchdog.try_retirement(&subject).unwrap());
    // Restart recovery owns the persisted retry even with no caller attached.
    service.ensure_cleanup_executor();
    {
        let db = service.db.lock().unwrap();
        db.execute(
            "UPDATE jobs SET next_attempt_at = NULL WHERE job_id = ?1",
            [&id],
        )
        .unwrap();
    }
    service.cleanup_executor.notify();
    // Legacy failure cleanup intentionally finishes in quarantine; only an
    // unexpected quarantine (missing restore evidence) fails its job.
    let expected_status = if target == Some(SandboxState::Quarantined) && completion != Quarantine {
        JobStatus::Failed
    } else {
        JobStatus::Completed
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if jobs::get_job(service.db.lock().unwrap().raw(), &id)
                .unwrap()
                .unwrap()
                .status
                == expected_status.as_str()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if episode.is_some() {
        let barrier = Arc::new(TestAdmissionBarrier {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        *service.cleanup_executor.waiter_read_barrier.lock().unwrap() = Some(barrier.clone());
        let waiter_service = service.clone();
        let awaited = id;
        let waiter = tokio::spawn(async move { waiter_service.wait_cleanup_job(awaited).await });
        tokio::time::timeout(Duration::from_secs(5), barrier.entered.notified())
            .await
            .unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert!(service.cleanup_executor.waiters.lock().unwrap().is_empty());
        assert!(service.cleanup_executor.active.contains_key(&subject));
        assert!(service.active_state_transition_count() > 0);
        assert!(matches!(
            service.try_cleanup_fence(&subject),
            CleanupLockTryResult::Busy
        ));
    }
    drop(retirement_guard);
    let result = service.wait_cleanup_job(id).await;
    assert_eq!(
        result.err().map_or(tonic::Code::Ok, |error| error.code()),
        if expected_status == JobStatus::Failed {
            tonic::Code::FailedPrecondition
        } else {
            tonic::Code::Ok
        }
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "failed atomic commit retries idempotent host cleanup"
    );
    assert_eq!(
        crate::db::get_sandbox(&service.db.lock().unwrap(), &subject)
            .unwrap()
            .map(|row| row.state),
        target
    );
    assert!(
        crate::db::cleanup_episode(&service.db.lock().unwrap(), &subject)
            .unwrap()
            .is_none()
    );
    // Virtual time from here on. A case that registered an activation episode
    // also registered the sandbox with the snapshot scheduler, whose first tick
    // opens a node-manager connection; that connect does not watch shutdown.
    // The test service's node-manager is an absent socket dialed once, so the
    // connect fails at once, and pausing only keeps any later scheduler timer
    // from holding the drain on the real clock. Every assertion after this
    // point reads state the drain is required to have settled. Everything
    // above runs on the real clock exactly as the executor's own timers need.
    tokio::time::pause();
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    if episode.is_some() {
        assert!(
            service.watchdog.get_health(&subject).await.is_none(),
            "{initial:?} -> {target:?}"
        );
        assert!(!service.snapshot_scheduler.is_active(&subject).await);
        assert!(
            service
                .auto_lifecycle
                .since_last_wake_completed(&subject)
                .is_none()
        );
        let mut replacement = replacement_sidecar.unwrap();
        replacement.network_tx_bytes += 1;
        let replacement_bytes = replacement.network_tx_bytes;
        service.watchdog.add_sandbox(replacement).await;
        assert_eq!(
            service
                .watchdog
                .get_health(&subject)
                .await
                .unwrap()
                .network_tx_bytes,
            replacement_bytes,
            "same-ID registration must not inherit the retired incarnation's health"
        );
        service
            .snapshot_scheduler
            .add_sandbox(subject.clone(), "replacement".into())
            .await;
        assert!(service.snapshot_scheduler.is_active(&subject).await);
    }
    assert_eq!(
        service
            .network_policy_reconcile_queue
            .drain_pending()
            .contains(&subject),
        target.is_some(),
        "surviving rows, including quarantine, retain their policy obligation"
    );
    // Replaying a claim after a successful commit cannot dispatch again.
    service
        .run_durable_cleanup_attempt(&id, &subject, false)
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_deleting_row() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Deleting,
        CleanupCompletion::DeleteRow,
        None,
        false,
        None,
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_stopping_to_stopped() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Stopping,
        CleanupCompletion::MarkStopped,
        None,
        false,
        Some(SandboxState::Stopped),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_suspending_with_artifact() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Suspending,
        CleanupCompletion::MarkSuspended,
        None,
        true,
        Some(SandboxState::Suspended),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_suspending_without_artifact() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Suspending,
        CleanupCompletion::MarkSuspended,
        None,
        false,
        Some(SandboxState::Quarantined),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_interrupted_cold_start_from_created() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Created,
        CleanupCompletion::MarkCreated,
        Some(crate::db::ActivationKind::ColdStart),
        false,
        Some(SandboxState::Created),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_interrupted_cold_start_from_stopped() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Stopped,
        CleanupCompletion::MarkStopped,
        Some(crate::db::ActivationKind::ColdStart),
        false,
        Some(SandboxState::Stopped),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_cleanup_required_quarantine() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::CleanupRequired,
        CleanupCompletion::Quarantine,
        None,
        false,
        Some(SandboxState::Quarantined),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_interrupted_restore_with_artifact() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Suspended,
        CleanupCompletion::MarkSuspended,
        Some(crate::db::ActivationKind::Restore),
        true,
        Some(SandboxState::Suspended),
    )
    .await;
}

#[tokio::test]
async fn lifecycle_commit_and_recovery_interrupted_restore_without_artifact() {
    lifecycle_and_job_completion_commit_together_and_recover(
        SandboxState::Suspended,
        CleanupCompletion::MarkSuspended,
        Some(crate::db::ActivationKind::Restore),
        false,
        Some(SandboxState::Quarantined),
    )
    .await;
}

#[tokio::test]
async fn failed_job_admission_rolls_back_claim_and_preserves_emergency_teardown() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
    conn.execute_batch(
        "CREATE TRIGGER reject_cleanup_job BEFORE INSERT ON jobs
        BEGIN SELECT RAISE(FAIL, 'scheduling storage unavailable'); END;",
    )
    .unwrap();
    let service = service_for_teardown_test(conn);
    let admission = service
        .admit_lifecycle_cleanup(
            sandbox_id,
            SandboxState::Stopping,
            true,
            CleanupCompletion::MarkStopped,
            true,
        )
        .await
        .unwrap();
    assert!(admission.is_unpersisted_for_sandbox(sandbox_id).unwrap());
    assert_eq!(
        service
            .lookup_sandbox(&sandbox_id.to_string())
            .unwrap()
            .state,
        SandboxState::Started
    );
    assert!(
        jobs::find_active_job(
            service.db.lock().unwrap().raw(),
            cleanup_jobs::KIND,
            &sandbox_id.to_string()
        )
        .unwrap()
        .is_none()
    );
    drop(admission);
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
}

#[tokio::test]
async fn promoted_selected_job_requeues_without_stealing_graceful_capacity() {
    let conn = crate::db::test_conn();
    let capacity = DEFAULT_CLEANUP_CONCURRENCY_CAP;
    assert!(capacity > 1);
    let foreground: Vec<_> = (0..capacity - 1).map(|_| work(&conn, true)).collect();
    let (promoted_id, mut promoted) = work(&conn, false);
    let (graceful_id, graceful) = work(&conn, false);
    let service = service_for_teardown_test(conn);
    let subject = promoted.sandbox_id.to_string();
    let fence = CleanupLockGuard::new(
        Arc::clone(&service.cleanup_locks),
        subject.clone(),
        service.cleanup_lock(&subject),
    )
    .lock_owned()
    .await;
    let (started, mut starts) = tokio::sync::mpsc::channel(capacity);
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |id, kill| {
        let started = started.clone();
        Box::pin(async move {
            let (release, wait) = tokio::sync::oneshot::channel();
            started.send((id, kill, release)).await.unwrap();
            wait.await.unwrap()
        })
    }));
    service.ensure_cleanup_executor();
    let mut held = Vec::new();
    for _ in 0..capacity - 1 {
        let event = tokio::time::timeout(Duration::from_secs(5), starts.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(event.1);
        held.push(event);
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while !service.cleanup_executor.active.contains_key(&subject) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!*service.cleanup_executor.active.get(&subject).unwrap());
    promoted.immediate_kill = true;
    {
        let db = service.db.lock().unwrap();
        let tx = crate::db::Transaction::deferred(&db).unwrap();
        assert_eq!(cleanup_jobs::persist(&tx, promoted).unwrap(), promoted_id);
        tx.commit().unwrap();
    }
    let waiter_service = service.clone();
    let awaited = promoted_id;
    let waiter = tokio::spawn(async move { waiter_service.wait_cleanup_job(awaited).await });
    drop(fence);
    let next = tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next.0,
        graceful.sandbox_id.to_string(),
        "promotion must release the graceful reservation before dispatch"
    );
    assert!(!next.1);
    assert!(!waiter.is_finished(), "requeue is not caller completion");
    {
        let db = service.db.lock().unwrap();
        let job = jobs::get_job(db.raw(), &promoted_id).unwrap().unwrap();
        assert_eq!(job.status, JobStatus::Pending.as_str());
        assert_eq!(
            job.attempts, 0,
            "yielding before mutation does not count as an attempt"
        );
        assert!(cleanup_jobs::decode(&job.payload).unwrap().immediate_kill);
    }
    next.2.send(Ok(())).unwrap();
    for (_, _, release) in held {
        release.send(Ok(())).unwrap();
    }
    let last = tokio::time::timeout(Duration::from_secs(5), starts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.0, subject);
    assert!(last.1);
    assert!(*service.cleanup_executor.active.get(&subject).unwrap());
    assert!(!waiter.is_finished());
    last.2.send(Ok(())).unwrap();
    waiter.await.unwrap().unwrap();
    for (id, _) in foreground {
        service.wait_cleanup_job(id).await.unwrap();
    }
    service.wait_cleanup_job(graceful_id).await.unwrap();
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    assert!(service.cleanup_executor.active.is_empty());
}

#[tokio::test]
async fn non_execution_outcomes_preserve_durable_authority() {
    use JobStatus::{Cancelled, Completed, Failed};
    use tonic::Code::{Aborted, FailedPrecondition};
    for (case, expected_status, expected_code) in [
        ("cancelled", Cancelled, Aborted),
        ("replacement_generation", Cancelled, Aborted),
        ("unknown_state", Failed, FailedPrecondition),
        ("quarantined", Failed, FailedPrecondition),
        ("absent_without_receipt", Cancelled, Aborted),
        ("covered_delete", Completed, tonic::Code::Ok),
    ] {
        let conn = crate::db::test_conn();
        let (id, mut work) = work(&conn, false);
        if case == "covered_delete" {
            work.execution_generation = Some(ExecutionGeneration::new(1).unwrap());
            conn.execute(
                "UPDATE sandboxes SET space_api_route_execution_generation = 1 WHERE id = ?1",
                [work.sandbox_id.to_string()],
            )
            .unwrap();
            jobs::update_active_job_payload(
                conn.raw(),
                &id,
                &serde_json::to_string(&work).unwrap(),
                work.priority(),
            )
            .unwrap();
        }
        jobs::mark_job_running(conn.raw(), &id).unwrap();
        let service = service_for_teardown_test(conn);
        let subject = work.sandbox_id.to_string();
        let fence = CleanupLockGuard::new(
            Arc::clone(&service.cleanup_locks),
            subject.clone(),
            service.cleanup_lock(&subject),
        )
        .lock_owned()
        .await;
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(|_, _| {
            panic!("non-current work must not dispatch host cleanup")
        }));
        let runner_service = service.clone();
        let runner_id = id;
        let runner_subject = subject.clone();
        let runner = tokio::spawn(async move {
            runner_service
                .run_durable_cleanup_attempt(&runner_id, &runner_subject, false)
                .await
        });
        {
            let conn = service.db.lock().unwrap();
            match case {
                "cancelled" => {
                    jobs::request_job_cancellation(conn.raw(), &id).unwrap();
                    jobs::mark_job_cancelled(conn.raw(), &id).unwrap();
                }
                "replacement_generation" => {
                    conn.execute("UPDATE sandboxes SET space_api_route_execution_generation = 2 WHERE id = ?1", [&subject]).unwrap();
                }
                "unknown_state" | "quarantined" => {
                    let state = if case == "unknown_state" {
                        "future_state"
                    } else {
                        "quarantined"
                    };
                    let update = |conn: &crate::db::Connection| {
                        conn.execute(
                            "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
                            [state, &subject],
                        )
                        .unwrap();
                    };
                    if case == "unknown_state" {
                        crate::db::seed_historical_lifecycle_rows(&conn, update);
                    } else {
                        update(&conn);
                    }
                }
                "absent_without_receipt" | "covered_delete" => {
                    crate::db::complete_row_intent_cleanup_with_receipt(
                        &conn,
                        &subject,
                        CleanupCompletion::DeleteRow,
                        None,
                        None,
                    )
                    .unwrap();
                }
                _ => unreachable!(),
            }
        }
        drop(fence);
        runner.await.unwrap().unwrap();
        let result = service.wait_cleanup_job(id).await;
        assert_eq!(
            result.err().map_or(tonic::Code::Ok, |error| error.code()),
            expected_code,
            "{case}"
        );
        let status = jobs::get_job(service.db.lock().unwrap().raw(), &id)
            .unwrap()
            .unwrap()
            .status;
        assert_eq!(status, expected_status.as_str(), "{case}");
        // An ended or restarted attempt cannot overwrite terminal authority.
        service
            .run_durable_cleanup_attempt(&id, &subject, false)
            .await
            .unwrap();
        assert_eq!(
            jobs::get_job(service.db.lock().unwrap().raw(), &id)
                .unwrap()
                .unwrap()
                .status,
            expected_status.as_str(),
            "{case}"
        );
    }
}

#[tokio::test]
async fn selected_cancellation_cannot_publish_success_after_a_stale_waiter_read() {
    let conn = crate::db::test_conn();
    let (id, mut work) = work(&conn, false);
    let subject = work.sandbox_id.to_string();
    work.completion = CleanupCompletion::MarkStopped;
    work.keep_filesystem = true;
    conn.execute(
        "UPDATE sandboxes SET state = 'stopping' WHERE id = ?1",
        [&subject],
    )
    .unwrap();
    jobs::update_active_job_payload(
        conn.raw(),
        &id,
        &serde_json::to_string(&work).unwrap(),
        work.priority(),
    )
    .unwrap();
    let service = service_for_teardown_test(conn);
    let fence = CleanupLockGuard::new(
        Arc::clone(&service.cleanup_locks),
        subject.clone(),
        service.cleanup_lock(&subject),
    )
    .lock_owned()
    .await;
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let dispatched = calls.clone();
    *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, _| {
        dispatched.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }));
    let barrier = Arc::new(TestAdmissionBarrier {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    *service.cleanup_executor.waiter_read_barrier.lock().unwrap() = Some(barrier.clone());
    service.ensure_cleanup_executor();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !service.cleanup_executor.active.contains_key(&subject) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let waiter_service = service.clone();
    let awaited = id;
    let waiter = tokio::spawn(async move { waiter_service.wait_cleanup_job(awaited).await });
    tokio::time::timeout(Duration::from_secs(5), barrier.entered.notified())
        .await
        .unwrap();
    let replacement_id = {
        let db = service.db.lock().unwrap();
        let tx = crate::db::Transaction::deferred(&db).unwrap();
        // A different obligation replaces the selected job under its fence.
        let mut replacement = work;
        replacement.completion = CleanupCompletion::DeleteRow;
        replacement.keep_filesystem = false;
        crate::db::mark_cleanup_required(
            &tx,
            &subject,
            false,
            replacement.completion,
            "delete supersedes stop",
        )
        .unwrap();
        let replacement_id = cleanup_jobs::persist(&tx, replacement).unwrap();
        tx.commit().unwrap();
        replacement_id
    };
    drop(fence);
    // Completion of the replacement proves the cancelled attempt released its
    // subject and sent its notification while the old waiter held Running.
    service.wait_cleanup_job(replacement_id).await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only replacement work dispatches"
    );
    barrier.release.add_permits(1);
    let error = tokio::time::timeout(Duration::from_secs(5), waiter)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Aborted);
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    assert_eq!(
        jobs::get_job(service.db.lock().unwrap().raw(), &id)
            .unwrap()
            .unwrap()
            .status,
        JobStatus::Cancelled.as_str()
    );
}
