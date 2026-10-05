use super::service_tests::{
    SandboxEventCapture, add_test_monitoring, insert_test_sandbox, runtime_fields,
    service_for_teardown_test,
};
use super::*;
use crate::service::activation_support::internal_request;
use crate::service::cleanup::{CleanupAdmissionDecision, ReleaseExecutionClaim};
use crate::service::cleanup_support::CleanupCompletion;
use pplx_space_common_core::types::{
    ExecutionGeneration, ExecutionReleaseDetachReason,
    ExecutionReleaseIntent as InternalExecutionReleaseIntent, SandboxCleanupEpisodeId,
};

#[tokio::test]
async fn rowless_execution_release_preserves_the_node_escape_hatch() {
    let conn = crate::db::test_conn();
    let service = service_for_teardown_test(conn);

    let admission = service
        .admit_execution_release(
            SandboxId::new(),
            crate::service::cleanup::ReleaseExecutionClaim::from_intent(
                InternalExecutionReleaseIntent::Delete,
                ExecutionGeneration::new(1).unwrap(),
            ),
        )
        .await;

    assert!(
        matches!(
            admission,
            Ok(CleanupAdmissionDecision::ExecutionReleaseAdmitted {
                admission: crate::service::cleanup::CleanupAdmission::Unpersisted {
                    persistence_error: None,
                    ..
                },
                ..
            })
        ),
        "a rowless execution must still reach node cleanup"
    );
}

#[tokio::test]
async fn execution_release_rpc_accepts_stop_and_publishes_the_decision() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    add_test_monitoring(&service, &sandbox_id_string).await;

    let cleanup_permits = u32::try_from(service.cleanup_sem.available_permits()).unwrap();
    let _cleanup_blocker = service
        .cleanup_sem
        .clone()
        .acquire_many_owned(cleanup_permits)
        .await
        .unwrap();
    let event_capture = SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(event_capture.clone());
    let subscriber_guard = tracing::subscriber::set_default(subscriber);
    let metric_capture = pplx_space_metrics::test_capture::capture_counters_guard();

    let response = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Stop as i32,
        }))
        .await
        .expect("the current execution must be accepted")
        .into_inner();

    let counters = metric_capture.finish();
    drop(subscriber_guard);
    assert!(
        pplx_space_common_jobs::db::find_active_job(
            &service.db.lock().unwrap(),
            crate::db::cleanup_jobs::KIND,
            &sandbox_id_string,
        )
        .unwrap()
        .is_some(),
        "accepted release must durably schedule cleanup even at capacity"
    );
    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::LifecycleMutationAccepted as i32
    );
    // A stop keeps the filesystem, so it flushes the guest first.
    assert_eq!(
        persisted_guest_flush(&service, &sandbox_id_string),
        crate::db::cleanup_jobs::GuestFlush::BeforeTermination
    );
    let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(row.state, SandboxState::Stopping);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
    assert_eq!(row.cleanup_keep_filesystem, Some(true));
    assert!(
        service
            .watchdog
            .get_health(&sandbox_id_string)
            .await
            .is_none()
    );
    assert!(
        !service
            .snapshot_scheduler
            .is_active(&sandbox_id_string)
            .await
    );
    let dirty_refs = {
        let db = service.db.lock().expect("mutex poisoned");
        crate::db::list_node_dirty_refs(&db, 10).unwrap()
    };
    assert!(dirty_refs.iter().any(|reference| {
        reference.element_kind == "sandbox_inventory"
            && reference.element_value == sandbox_id_string
    }));
    assert!(counters.iter().any(|counter| {
        counter.name == "sandbox.worker.execution_release.decision"
            && counter.tags.iter().any(|tag| tag == "intent:stop")
            && counter.tags.iter().any(|tag| tag == "disposition:accepted")
            && counter
                .tags
                .iter()
                .any(|tag| tag == "reason:not_applicable")
    }));
    let events = event_capture.events.lock().unwrap();
    assert!(events.iter().any(|event| {
        event
            .get("event_type")
            .is_some_and(|value| value == "stop_requested")
            && event
                .get("operation")
                .is_some_and(|value| value == "release_execution")
            && event
                .get("execution_generation")
                .is_some_and(|value| value == "1")
    }));
}

#[tokio::test]
async fn execution_release_rpc_accepts_delete_and_retires_auto_lifecycle_state() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    service
        .auto_lifecycle
        .record_wake_completed(&sandbox_id_string);
    assert!(
        service
            .auto_lifecycle
            .since_last_wake_completed(&sandbox_id_string)
            .is_some()
    );
    let cleanup_permits = u32::try_from(service.cleanup_sem.available_permits()).unwrap();
    let _cleanup_blocker = service
        .cleanup_sem
        .clone()
        .acquire_many_owned(cleanup_permits)
        .await
        .unwrap();

    let response = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect("the current execution must be accepted for deletion")
        .into_inner();

    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::LifecycleMutationAccepted as i32
    );
    let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(row.state, SandboxState::Deleting);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::DeleteRow));
    assert_eq!(row.cleanup_keep_filesystem, Some(false));
    assert_eq!(
        persisted_guest_flush(&service, &sandbox_id_string),
        crate::db::cleanup_jobs::GuestFlush::Unrequested,
        "a delete discards the disk and never flushes"
    );
    assert!(
        service
            .auto_lifecycle
            .since_last_wake_completed(&sandbox_id_string)
            .is_none(),
        "accepted deletion must retire per-sandbox auto-lifecycle state"
    );
}

#[tokio::test]
async fn execution_release_delete_defers_failed_suspend_when_the_filesystem_is_the_only_copy() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Failed);
    conn.execute(
        "UPDATE sandboxes SET failure_reason = ?1 WHERE id = ?2",
        (
            crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE,
            &sandbox_id_string,
        ),
    )
    .unwrap();
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();

    let error = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect_err("the only retained filesystem copy must defer execution-release delete");

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(pplx_space_sbx_mgr_client::is_retained_filesystem_only_copy_status(&error));
    let retained = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(retained.state, SandboxState::Failed);
    assert_eq!(
        retained.failure_reason.as_deref(),
        Some(crate::db::SUSPEND_PUBLISH_FAILED_NO_ARTIFACT_RUNTIME_GONE)
    );
}

#[tokio::test]
async fn execution_release_detaches_from_manager_owned_cleanup_episodes() {
    for (state, intent, completion, entered_from_state, activation_state, quarantine_reason_code) in [
        (
            SandboxState::Started,
            ReleaseExecutionIntent::Stop,
            CleanupCompletion::MarkSuspended,
            "quarantined",
            "restoring",
            Some("restore_firecracker_launch_failed"),
        ),
        (
            SandboxState::Failed,
            ReleaseExecutionIntent::Delete,
            CleanupCompletion::Quarantine,
            "created",
            "starting",
            None,
        ),
    ] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, state);
        crate::db::update_runtime_fields(
            &conn,
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
        conn.execute(
            "INSERT INTO sandbox_cleanup_episodes (
                     sandbox_id, cleanup_id, completion, keep_filesystem, phase,
                     entered_from_state, activation_state, requested_at,
                     quarantine_reason_code, created_at
                 ) VALUES (?1, ?2, ?3, TRUE, 'requested', ?4, ?5, 1, ?6, 1)",
            rusqlite::params![
                sandbox_id_string,
                SandboxCleanupEpisodeId::new().to_string(),
                completion.as_str(),
                entered_from_state,
                activation_state,
                quarantine_reason_code,
            ],
        )
        .unwrap();
        let service = service_for_teardown_test(conn);

        let response = service
            .release_execution(internal_request(ReleaseExecutionRequest {
                sandbox_id: sandbox_id_string.clone(),
                execution_generation: 1,
                intent: intent as i32,
            }))
            .await
            .expect("manager-owned cleanup must detach the runtime")
            .into_inner();

        assert_eq!(
            response.disposition,
            ReleaseExecutionDisposition::Detach as i32
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            state,
            "release must not supersede manager-owned cleanup"
        );
        let db = service.db.lock().expect("mutex poisoned");
        assert!(
            crate::db::cleanup_episode(&db, &sandbox_id_string)
                .unwrap()
                .is_some(),
            "release must preserve the active cleanup episode"
        );
    }
}

#[tokio::test]
async fn execution_release_distinguishes_missing_and_unknown_intents() {
    let conn = crate::db::test_conn();
    let service = service_for_teardown_test(conn);
    let sandbox_id = SandboxId::new().to_string();

    let missing = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Unspecified as i32,
        }))
        .await
        .expect_err("an unspecified intent must be rejected");
    assert_eq!(missing.code(), tonic::Code::InvalidArgument);
    assert_eq!(missing.message(), "intent is required");

    let unknown = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id,
            execution_generation: 1,
            intent: 99,
        }))
        .await
        .expect_err("a future unknown intent must be rejected explicitly");
    assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    assert_eq!(unknown.message(), "unknown intent value 99");
}

#[tokio::test]
async fn execution_release_waiting_for_cleanup_lock_detaches_from_new_cleanup_episode() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();

    let cleanup_lock = service.cleanup_lock(&sandbox_id_string);
    let cleanup_blocker = Arc::clone(&cleanup_lock).lock_owned().await;
    let baseline_refs = Arc::strong_count(&cleanup_lock);
    let release_service = service.clone();
    let release_id = sandbox_id_string.clone();
    let release = tokio::spawn(async move {
        release_service
            .release_execution(internal_request(ReleaseExecutionRequest {
                sandbox_id: release_id,
                execution_generation: 1,
                intent: ReleaseExecutionIntent::Delete as i32,
            }))
            .await
    });

    tokio::time::timeout(Duration::from_secs(1), async {
        while Arc::strong_count(&cleanup_lock) <= baseline_refs {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("delete must reach the cleanup claim");
    {
        let db = service.db.lock().expect("mutex poisoned");
        crate::db::mark_cleanup_required(
            &db,
            &sandbox_id_string,
            true,
            CleanupCompletion::MarkCreated,
            "activation rollback owns cleanup",
        )
        .unwrap();
    }
    drop(cleanup_blocker);

    let response = release
        .await
        .unwrap()
        .expect("ownership loss is an expected conditional outcome")
        .into_inner();
    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::Detach as i32
    );
    let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(row.state, SandboxState::CleanupRequired);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkCreated));
}

#[tokio::test]
async fn execution_release_uses_lifecycle_when_optional_fields_are_malformed() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    crate::db::update_runtime_fields(
        &conn,
        sandbox_id,
        runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        [&sandbox_id_string],
    )
    .unwrap();
    let service = SandboxManagerService::new(conn);

    let admission = service
        .admit_execution_release(
            sandbox_id,
            crate::service::cleanup::ReleaseExecutionClaim::from_intent(
                InternalExecutionReleaseIntent::Delete,
                ExecutionGeneration::new(1).unwrap(),
            ),
        )
        .await
        .expect("optional metadata must not hide shim teardown ownership");
    assert!(matches!(
        admission,
        CleanupAdmissionDecision::ExecutionReleaseAdmitted { .. }
    ));

    let state = {
        let db = service.db.lock().expect("mutex poisoned");
        db.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id_string],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
    };
    assert_eq!(state, SandboxState::Deleting.as_str());
}

#[tokio::test]
async fn execution_release_admission_uses_the_persisted_runtime_label() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let allocated_runtime_label = SandboxRuntimeLabel::parse("slot0007").unwrap();
    crate::db::update_uuid8(&conn, &sandbox_id_string, &allocated_runtime_label).unwrap();
    let service = service_for_teardown_test(conn);
    let execution_generation = ExecutionGeneration::new(1).unwrap();
    service
        .update_runtime_fields(sandbox_id, runtime_fields(execution_generation, 7))
        .unwrap();

    let admission = service
        .admit_execution_release(
            sandbox_id,
            ReleaseExecutionClaim::from_intent(
                InternalExecutionReleaseIntent::Stop,
                execution_generation,
            ),
        )
        .await
        .unwrap();

    assert!(matches!(
        admission,
        CleanupAdmissionDecision::ExecutionReleaseAdmitted {
            runtime_label,
            ..
        } if runtime_label == allocated_runtime_label
    ));
}

#[tokio::test]
async fn execution_release_detaches_once_manager_cleanup_owns_the_generation() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    let execution_generation = ExecutionGeneration::new(1).unwrap();
    service
        .update_runtime_fields(sandbox_id, runtime_fields(execution_generation, 7))
        .unwrap();

    let stop_admission = service
        .admit_execution_release(
            sandbox_id,
            crate::service::cleanup::ReleaseExecutionClaim::from_intent(
                InternalExecutionReleaseIntent::Stop,
                execution_generation,
            ),
        )
        .await
        .expect("the current execution must admit the initial stop");
    assert!(matches!(
        stop_admission,
        CleanupAdmissionDecision::ExecutionReleaseAdmitted { .. }
    ));
    drop(stop_admission);

    for intent in [
        InternalExecutionReleaseIntent::Stop,
        InternalExecutionReleaseIntent::Delete,
    ] {
        let repeated_release = service
            .admit_execution_release(
                sandbox_id,
                ReleaseExecutionClaim::from_intent(intent, execution_generation),
            )
            .await;
        assert!(matches!(
            repeated_release,
            Ok(CleanupAdmissionDecision::ExecutionReleaseDetached {
                reason: ExecutionReleaseDetachReason::LifecycleNotReleasable,
            })
        ));
    }
    let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(row.state, SandboxState::Stopping);
    assert_eq!(row.cleanup_completion, Some(CleanupCompletion::MarkStopped));
}

#[tokio::test]
async fn execution_release_detaches_from_unknown_persisted_lifecycle() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
        )
        .unwrap();
    {
        let conn = service.db.lock().expect("mutex poisoned");
        crate::db::seed_historical_lifecycle_rows(&conn, |conn| {
            conn.execute(
                "UPDATE sandboxes SET state = 'state_from_newer_worker' WHERE id = ?1",
                [&sandbox_id_string],
            )
            .unwrap();
        });
    }

    let response = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string,
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect("an unknown lifecycle must fail closed by detaching")
        .into_inner();

    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::Detach as i32
    );
}

#[tokio::test]
async fn execution_release_detaches_from_malformed_manager_owned_row() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &sandbox_id, SandboxState::CleanupRequired);
    conn.execute(
        "UPDATE sandboxes SET process_env_json = 'malformed' WHERE id = ?1",
        [&sandbox_id],
    )
    .unwrap();
    let service = SandboxManagerService::new(conn);

    let response = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect("manager-owned lifecycle is an expected conditional outcome")
        .into_inner();
    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::Detach as i32
    );
    let state = {
        let db = service.db.lock().expect("mutex poisoned");
        db.query_row(
            "SELECT state FROM sandboxes WHERE id = ?1",
            [&sandbox_id],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
    };
    assert_eq!(state, SandboxState::CleanupRequired.as_str());
}

#[tokio::test]
async fn execution_release_detaches_a_stale_execution() {
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    service
        .update_runtime_fields(
            sandbox_id,
            runtime_fields(ExecutionGeneration::new(2).unwrap(), 8),
        )
        .unwrap();

    let response = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect("a stale shim must detach without mutating the current execution")
        .into_inner();

    assert_eq!(
        response.disposition,
        ReleaseExecutionDisposition::Detach as i32
    );
    let row = service.lookup_sandbox(&sandbox_id_string).unwrap();
    assert_eq!(row.state, SandboxState::Started);
    assert_eq!(
        row.space_api_route_identity.execution_generation,
        Some(ExecutionGeneration::new(2).unwrap())
    );
}

#[tokio::test]
async fn execution_release_detaches_from_dormant_rows_without_execution_lineage() {
    for state in [SandboxState::Created, SandboxState::Stopped] {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let sandbox_id_string = sandbox_id.to_string();
        insert_test_sandbox(&conn, &sandbox_id_string, state);
        let service = service_for_teardown_test(conn);

        let response = service
            .release_execution(internal_request(ReleaseExecutionRequest {
                sandbox_id: sandbox_id_string.clone(),
                execution_generation: 1,
                intent: ReleaseExecutionIntent::Delete as i32,
            }))
            .await
            .expect("a dormant manager-owned row must detach the stale runtime adapter")
            .into_inner();

        assert_eq!(
            response.disposition,
            ReleaseExecutionDisposition::Detach as i32,
            "unexpected disposition for {state}"
        );
        assert_eq!(
            service.lookup_sandbox(&sandbox_id_string).unwrap().state,
            state
        );
    }
}

#[tokio::test]
async fn execution_release_fails_closed_when_tracked_generation_is_missing() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    let service = service_for_teardown_test(conn);
    let event_capture = SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(event_capture.clone());
    let subscriber_guard = tracing::subscriber::set_default(subscriber);

    let error = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: 1,
            intent: ReleaseExecutionIntent::Delete as i32,
        }))
        .await
        .expect_err("missing tracked lineage must not authorize local task release");
    drop(subscriber_guard);

    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(
        service.lookup_sandbox(&sandbox_id_string).unwrap().state,
        SandboxState::Started
    );
    assert!(event_capture.events.lock().unwrap().iter().any(|event| {
        event
            .get("event_type")
            .is_some_and(|value| value == "delete_failed")
            && event
                .get("operation")
                .is_some_and(|value| value == "release_execution")
            && event.get("action").is_some_and(|value| value == "delete")
            && event
                .get("execution_generation")
                .is_some_and(|value| value == "1")
            && event
                .get("reason")
                .is_some_and(|value| value == "execution_release_failed")
    }));
}

#[tokio::test]
async fn execution_release_fails_closed_when_cleanup_intent_cannot_be_persisted() {
    use tracing_subscriber::layer::SubscriberExt as _;

    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    let sandbox_id_string = sandbox_id.to_string();
    insert_test_sandbox(&conn, &sandbox_id_string, SandboxState::Started);
    conn.execute_batch(
        "CREATE TRIGGER fail_fenced_cleanup_intent
             BEFORE UPDATE OF cleanup_completion ON sandboxes
             BEGIN
                 SELECT RAISE(FAIL, 'injected fenced cleanup-intent failure');
             END;",
    )
    .unwrap();
    let service = service_for_teardown_test(conn);
    let generation = ExecutionGeneration::new(1).unwrap();
    service
        .update_runtime_fields(sandbox_id, runtime_fields(generation, 7))
        .unwrap();
    let event_capture = SandboxEventCapture::default();
    let subscriber = tracing_subscriber::registry().with(event_capture.clone());
    let subscriber_guard = tracing::subscriber::set_default(subscriber);

    let error = service
        .release_execution(internal_request(ReleaseExecutionRequest {
            sandbox_id: sandbox_id_string.clone(),
            execution_generation: generation.get(),
            intent: ReleaseExecutionIntent::Stop as i32,
        }))
        .await
        .expect_err("execution release must not run unpersisted");
    drop(subscriber_guard);

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(
        service.lookup_sandbox(&sandbox_id_string).unwrap().state,
        SandboxState::Started
    );
    assert!(event_capture.events.lock().unwrap().iter().any(|event| {
        event
            .get("event_type")
            .is_some_and(|value| value == "stop_failed")
            && event
                .get("operation")
                .is_some_and(|value| value == "release_execution")
            && event.get("action").is_some_and(|value| value == "stop")
            && event
                .get("execution_generation")
                .is_some_and(|value| value == "1")
            && event
                .get("reason")
                .is_some_and(|value| value == "execution_release_failed")
    }));
}

#[tokio::test]
async fn cancelled_cleanup_rpcs_preserve_admitted_termination_strength() {
    // Exercise the actual handlers on both sides of the atomic-admission boundary.
    let cases = [
        ("stop", true, false),
        ("stop", true, true),
        ("stop", false, false),
        ("stop", false, true),
        ("kill", true, false),
        ("kill", true, true),
        ("term", true, false),
        ("term", true, true),
        ("release_stop", true, false),
        ("release_delete", false, false),
    ];
    let mut observed = Vec::new();
    for (operation, immediate, blocking) in cases {
        let conn = crate::db::test_conn();
        let sandbox_id = SandboxId::new();
        let id = sandbox_id.to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Started);
        let service = service_for_teardown_test(conn);
        service
            .update_runtime_fields(
                sandbox_id,
                runtime_fields(ExecutionGeneration::new(1).unwrap(), 7),
            )
            .unwrap();
        let run_dir = tempfile::tempdir().unwrap();
        let sandbox_dir = run_dir.path().join(&id);
        std::fs::create_dir(&sandbox_dir).unwrap();
        std::os::unix::fs::symlink(run_dir.path(), sandbox_dir.join("fc.pid")).unwrap();
        *service.cleanup_executor.run_dir.lock().unwrap() = Some(run_dir.path().to_owned());
        let barrier = Arc::new(crate::service::cleanup_executor::TestAdmissionBarrier {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        *service.cleanup_executor.admission_barrier.lock().unwrap() = Some(barrier.clone());
        let (dispatched, mut dispatches) = tokio::sync::mpsc::channel(1);
        *service.cleanup_executor.dispatch.lock().unwrap() = Some(Arc::new(move |_, kill| {
            let dispatched = dispatched.clone();
            Box::pin(async move {
                dispatched.send(kill).await.unwrap();
                Ok(())
            })
        }));
        let rpc_service = service.clone();
        let rpc_id = id.clone();
        let rpc = tokio::spawn(async move {
            match operation {
                "stop" => rpc_service
                    .stop(internal_request(StopRequest {
                        sandbox_id: rpc_id,
                        template_filter: None,
                        blocking,
                        immediate_kill: immediate,
                    }))
                    .await
                    .map(|_| ()),
                "kill" | "term" => rpc_service
                    .kill(internal_request(KillRequest {
                        sandbox_id: rpc_id,
                        signal: if operation == "kill" { 9 } else { 15 },
                        blocking,
                    }))
                    .await
                    .map(|_| ()),
                "release_stop" | "release_delete" => rpc_service
                    .release_execution(internal_request(ReleaseExecutionRequest {
                        sandbox_id: rpc_id,
                        execution_generation: 1,
                        intent: if operation == "release_stop" {
                            ReleaseExecutionIntent::Stop
                        } else {
                            ReleaseExecutionIntent::Delete
                        } as i32,
                    }))
                    .await
                    .map(|_| ()),
                _ => unreachable!(),
            }
        });
        tokio::time::timeout(Duration::from_secs(5), barrier.entered.notified())
            .await
            .unwrap();
        let (job_id, persisted_strength) = {
            let db = service.db.lock().unwrap();
            let job_id = pplx_space_common_jobs::db::find_active_job(
                &db,
                crate::db::cleanup_jobs::KIND,
                &id,
            )
            .unwrap()
            .unwrap();
            let job = pplx_space_common_jobs::db::get_job(&db, &job_id)
                .unwrap()
                .unwrap();
            (
                job_id,
                crate::db::cleanup_jobs::decode(&job.payload)
                    .unwrap()
                    .immediate_kill,
            )
        };
        rpc.abort();
        assert!(rpc.await.unwrap_err().is_cancelled());
        let dispatched_strength = tokio::time::timeout(Duration::from_secs(5), dispatches.recv())
            .await
            .unwrap()
            .unwrap();
        service.wait_cleanup_job(job_id).await.unwrap();
        service.shutdown_token.cancel();
        service.task_tracker.close();
        service.task_tracker.wait().await;
        observed.push((persisted_strength, dispatched_strength));
    }
    assert_eq!(
        observed,
        cases.map(|(_, immediate, _)| (immediate, immediate))
    );
}

/// The guest flush the sandbox's persisted cleanup work asks for.
fn persisted_guest_flush(
    service: &SandboxManagerService,
    sandbox_id: &str,
) -> crate::db::cleanup_jobs::GuestFlush {
    let db = service.db.lock().unwrap();
    let job =
        pplx_space_common_jobs::db::find_active_job(&db, crate::db::cleanup_jobs::KIND, sandbox_id)
            .unwrap()
            .expect("the release's cleanup is persisted");
    let payload = pplx_space_common_jobs::db::get_job(&db, &job)
        .unwrap()
        .unwrap()
        .payload;
    crate::db::cleanup_jobs::decode(&payload)
        .unwrap()
        .guest_flush
}
