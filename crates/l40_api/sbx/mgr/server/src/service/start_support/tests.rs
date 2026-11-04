use super::*;

#[tokio::test]
async fn admission_wait_cleanup_runs_when_wait_is_cancelled() {
    let limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 1,
            refill_rps: 1_000.0,
            burst: 10.0,
        },
    ));
    let held_permit = limiter
        .acquire(ActivationAdmissionKind::Cold, "sandbox-held")
        .await
        .unwrap();
    let task_limiter = Arc::clone(&limiter);
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let mut wait = tokio::spawn(async move {
        let _cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, _, _, _| {
                let _ = cleanup_tx.send(());
            },
        );
        task_limiter
            .acquire(ActivationAdmissionKind::Cold, "sandbox-waiting")
            .await
            .unwrap();
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut wait)
            .await
            .is_err(),
        "activation should remain blocked in admission acquire"
    );
    wait.abort();
    let _ = wait.await;
    drop(held_permit);

    tokio::time::timeout(Duration::from_secs(1), cleanup_rx)
        .await
        .expect("cancelled admission wait should schedule cleanup")
        .expect("cleanup should complete");
}

#[tokio::test]
async fn restore_preparation_cancellation_runs_cleanup_before_admission() {
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let (armed_tx, armed_rx) = tokio::sync::oneshot::channel();
    let preparation = tokio::spawn(async move {
        let _cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, permit, _, _| {
                let _ = cleanup_tx.send(permit.is_none());
            },
        );
        let _ = armed_tx.send(());
        std::future::pending::<()>().await;
    });

    armed_rx.await.expect("restore cleanup should arm");
    preparation.abort();
    let _ = preparation.await;

    assert!(
        cleanup_rx
            .await
            .expect("cancelled restore preparation should schedule cleanup"),
        "restore preparation cleanup should be armed before admission"
    );
}

#[tokio::test]
async fn restore_activation_cleanup_runs_before_admitted_permit_is_released() {
    let limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 1,
            refill_rps: 1_000.0,
            burst: 1.0,
        },
    ));
    let cleanup_limiter = Arc::clone(&limiter);
    let task_limiter = Arc::clone(&limiter);
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let (admitted_tx, admitted_rx) = tokio::sync::oneshot::channel();
    let activation = tokio::spawn(async move {
        let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, _, _, _| {
                let _ = cleanup_tx.send(cleanup_limiter.in_flight());
            },
        );
        let permit = task_limiter
            .acquire(ActivationAdmissionKind::Restore, "sandbox-1")
            .await
            .unwrap();
        cleanup_guard.admit(permit);
        let _ = admitted_tx.send(());
        std::future::pending::<()>().await;
    });
    admitted_rx.await.expect("restore should acquire admission");
    assert_eq!(limiter.in_flight(), 1);

    activation.abort();
    let _ = activation.await;

    assert_eq!(
        cleanup_rx.await.expect("cancellation should run cleanup"),
        1,
        "cleanup must be scheduled before admission capacity is released"
    );
    assert_eq!(limiter.in_flight(), 0);
}

#[tokio::test]
async fn activation_cleanup_remains_armed_after_permit_release() {
    let limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 1,
            refill_rps: 1_000.0,
            burst: 1.0,
        },
    ));
    let task_limiter = Arc::clone(&limiter);
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let (released_tx, released_rx) = tokio::sync::oneshot::channel();
    let activation = tokio::spawn(async move {
        let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            move |_, _, _, _| {
                let _ = cleanup_tx.send(());
            },
        );
        let permit = task_limiter
            .acquire(ActivationAdmissionKind::Restore, "sandbox-1")
            .await
            .unwrap();
        cleanup_guard.admit(permit);
        cleanup_guard.finish_admission(AdmissionPermitOutcome::Succeeded);
        let _ = released_tx.send(());
        std::future::pending::<()>().await;
    });
    released_rx
        .await
        .expect("activation should release admission");
    assert_eq!(limiter.in_flight(), 0);

    activation.abort();
    let _ = activation.await;

    cleanup_rx
        .await
        .expect("cancellation after permit release should schedule cleanup");
}

#[tokio::test]
async fn activation_success_disarms_cleanup() {
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |_, _, _, _| {
            let _ = cleanup_tx.send(());
        },
    );

    cleanup_guard.complete_success();

    assert!(
        cleanup_rx.await.is_err(),
        "durable handoff should disarm cancellation cleanup"
    );
}

#[test]
fn activation_failure_preserves_the_concrete_error_for_cleanup() {
    let (cleanup_tx, cleanup_rx) = std::sync::mpsc::channel();
    let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::ColdActivationInterrupted,
        "activation did not complete",
        move |_, _, _, reason| cleanup_tx.send(reason).unwrap(),
    );
    let status = Status::deadline_exceeded("guest readiness timed out");

    let returned = cleanup_guard.failure(status);
    drop(cleanup_guard);

    assert_eq!(returned.message(), "guest readiness timed out");
    assert_eq!(cleanup_rx.recv().unwrap(), "guest readiness timed out");
}

#[tokio::test]
async fn activation_success_atomically_releases_admission_and_disarms_cleanup() {
    let limiter = ActivationAdmissionLimiter::with_config(ActivationAdmissionConfig {
        max_concurrent_activations: 1,
        refill_rps: 1_000.0,
        burst: 1.0,
    });
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |_, _, _, _| {
            let _ = cleanup_tx.send(());
        },
    );
    let permit = limiter
        .acquire(ActivationAdmissionKind::Restore, "sandbox-1")
        .await
        .unwrap();
    cleanup_guard.admit(permit);

    cleanup_guard.complete_success();

    assert_eq!(limiter.in_flight(), 0);
    assert!(cleanup_rx.await.is_err());
}

#[tokio::test]
async fn activation_cleanup_uses_updated_runtime_label() {
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |runtime_label, _, _, _| {
            let _ = cleanup_tx.send(runtime_label);
        },
    );

    cleanup_guard.update_runtime_label(SandboxRuntimeLabel::parse("runtime2").unwrap());
    drop(cleanup_guard);

    assert_eq!(
        cleanup_rx.await.unwrap(),
        SandboxRuntimeLabel::parse("runtime2").unwrap()
    );
}

#[tokio::test]
async fn allocated_runtime_label_owns_cleanup_before_database_persistence() {
    let db = crate::db::SharedConnection::new(crate::db::Connection::open_in_memory());
    let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel();
    let mut cleanup_guard = ActivationCleanupGuard::with_interruption_reason(
        SandboxRuntimeLabel::parse("runtime1").unwrap(),
        SandboxQuarantineReasonCode::Unspecified,
        "activation did not complete",
        move |runtime_label, _, _, _| {
            let _ = cleanup_tx.send(runtime_label);
        },
    );
    let mut current = SandboxRuntimeLabel::parse("runtime1").unwrap();

    persist_allocated_runtime_label(
        &db,
        "sandbox-1",
        &mut current,
        &mut cleanup_guard,
        "runtime2".to_string(),
    )
    .expect_err("missing sandbox schema should fail persistence");
    drop(cleanup_guard);

    assert_eq!(
        cleanup_rx.await.unwrap(),
        SandboxRuntimeLabel::parse("runtime2").unwrap()
    );
}

#[tokio::test]
async fn stage_helpers_use_stage_as_the_exported_resource_name() {
    use pplx_space_observability::test_capture::RecordingVisitor;
    use tracing_subscriber::layer::SubscriberExt;

    #[derive(Clone, Default)]
    struct StageNames(Arc<std::sync::Mutex<Vec<String>>>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for StageNames {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            _id: &tracing::span::Id,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = RecordingVisitor::default();
            attrs.record(&mut visitor);
            self.0.lock().expect("span capture lock poisoned").extend(
                visitor
                    .into_fields()
                    .into_iter()
                    .filter_map(|(key, value)| (key == "otel.name").then_some(value)),
            );
        }
    }
    let names = StageNames::default();
    let _subscriber =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(names.clone()));
    let stage = "restore.cross_node.pull_metadata";
    start_stage(stage, "sandbox", async {}).await;
    start_stage_timeout(stage, Duration::from_secs(1), "sandbox", async { Ok(()) })
        .await
        .unwrap();
    start_stage_timeout_or(stage, Duration::from_secs(1), "sandbox", (), async {}).await;
    assert_eq!(
        *names.0.lock().expect("span capture lock poisoned"),
        vec![stage; 3]
    );
}
