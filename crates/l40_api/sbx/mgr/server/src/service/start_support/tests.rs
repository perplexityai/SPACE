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
        .acquire(LimitedActivationKind::Cold, "sandbox-held")
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
            .acquire(LimitedActivationKind::Cold, "sandbox-waiting")
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
            .acquire(LimitedActivationKind::Restore, "sandbox-1")
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
            .acquire(LimitedActivationKind::Restore, "sandbox-1")
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
        .acquire(LimitedActivationKind::Restore, "sandbox-1")
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

/// `admit_vm_activation` meters a cold boot and a restore through the limiter
/// under their own kinds, and admits a resident resume on its first poll while
/// the limiter has no free slot.
#[tokio::test]
async fn admit_vm_activation_meters_cold_and_restore_but_not_a_resident_resume() {
    use futures_util::FutureExt;

    let scratch = tempfile::tempdir().unwrap();
    let mut service = crate::service::SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test_in(scratch.path()),
    );
    service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 2,
            refill_rps: 100.0,
            burst: 100.0,
        },
    ));
    let limiter = Arc::clone(&service.activation_admission_limiter);

    let cold = service
        .admit_vm_activation(ActivationAdmissionKind::Cold, "sandbox-cold")
        .await
        .expect("a free slot admits a cold boot");
    assert!(matches!(
        cold.permit(),
        ActivationAdmissionPermit::Limited(_)
    ));
    assert_eq!(limiter.in_flight_for(LimitedActivationKind::Cold), 1);
    assert_eq!(limiter.in_flight_for(LimitedActivationKind::Restore), 0);

    let restore = service
        .admit_vm_activation(ActivationAdmissionKind::Restore, "sandbox-restore")
        .await
        .expect("a free slot admits a restore");
    assert!(matches!(
        restore.permit(),
        ActivationAdmissionPermit::Limited(_)
    ));
    assert_eq!(limiter.in_flight_for(LimitedActivationKind::Restore), 1);
    assert_eq!(limiter.in_flight(), 2);

    // Both slots are held, so a resume the limiter metered could not finish on
    // its first poll.
    let resume = service
        .admit_vm_activation(ActivationAdmissionKind::ResidentResume, "sandbox-resume")
        .now_or_never()
        .expect("a resident resume is admitted on its first poll")
        .expect("a resident resume is never refused");
    assert!(matches!(
        resume.permit(),
        ActivationAdmissionPermit::Unlimited
    ));
    assert_eq!(limiter.in_flight(), 2);

    drop(resume);
    assert_eq!(limiter.in_flight(), 2);
    drop(cold);
    drop(restore);
    assert_eq!(limiter.in_flight(), 0);

    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

fn seal_test_service(scratch: &std::path::Path) -> crate::service::SandboxManagerService {
    let mut service = crate::service::SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        crate::service::SandboxManagerServiceOptions::for_test_in(scratch),
    );
    service.activation_admission_limiter = Arc::new(ActivationAdmissionLimiter::with_config(
        ActivationAdmissionConfig {
            max_concurrent_activations: 1,
            refill_rps: 1_000.0,
            burst: 1_000.0,
        },
    ));
    service
}

async fn shut_down(service: crate::service::SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

fn assert_sealed_refusal(status: &Status, epoch: u64) {
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status:?}");
    assert!(
        status
            .message()
            .contains(pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER)
            && status.message().contains(&format!("epoch {epoch}")),
        "{status:?}"
    );
}

/// While sealed, every activation kind is refused, whether or not it runs
/// under a caller's state-writer lease, without taking limiter capacity or
/// advancing the admitted count. After the unseal each is admitted, counted,
/// and listed in flight under its own kind until its admission is dropped.
#[tokio::test]
async fn sealed_admission_refuses_every_kind_and_unseal_admits_each_again() {
    use futures_util::FutureExt;

    for kind in ActivationAdmissionKind::ALL {
        // A new kind fails to compile until it gets a match arm here, and
        // must also be added to `ALL`.
        match kind {
            ActivationAdmissionKind::Cold
            | ActivationAdmissionKind::Restore
            | ActivationAdmissionKind::ResidentResume => {}
        }
    }
    let scratch = tempfile::tempdir().unwrap();
    let service = seal_test_service(scratch.path());
    service.activation_seal.seal(&service.db, 3).unwrap();
    // The limiter's only slot, held outside the seal's count: an admission
    // that waited for capacity would not resolve on its first poll.
    let holder = service
        .activation_admission_limiter
        .acquire(LimitedActivationKind::Cold, "holder")
        .await
        .unwrap();

    for kind in ActivationAdmissionKind::ALL {
        let refused = service
            .admit_vm_activation(kind, "sandbox-sealed")
            .now_or_never()
            .expect("a sealed node refuses before any limiter wait")
            .expect_err("a sealed node admits nothing");
        assert_sealed_refusal(&refused, 3);
        let lease = service.state_writer_lease("SealTestCaller").unwrap();
        let nested = crate::service::drain::with_state_writer(
            lease,
            service.admit_vm_activation(kind, "sandbox-nested"),
        )
        .now_or_never()
        .expect("a sealed node refuses before any limiter wait")
        .expect_err("work under an inherited lease is refused too");
        assert_sealed_refusal(&nested, 3);
    }
    drop(holder);
    assert_eq!(service.activation_admission_limiter.in_flight(), 0);
    let status = service.activation_seal.status();
    assert!(status.in_flight.is_empty());

    service.activation_seal.unseal(&service.db, 3).unwrap();
    let mut admitted = Vec::new();
    for kind in ActivationAdmissionKind::ALL {
        admitted.push(
            service
                .admit_vm_activation(kind, kind.as_str())
                .await
                .unwrap_or_else(|status| panic!("{kind:?} after the unseal: {status:?}"))
                .into_parts()
                .1,
        );
    }
    let status = service.activation_seal.status();
    assert_eq!(
        status.in_flight,
        ActivationAdmissionKind::ALL.map(ActivationAdmissionKind::as_str)
    );
    drop(admitted);
    service.activation_seal.assert_quiesced();
    shut_down(service).await;
}

/// A cold boot waiting for limiter capacity when the seal lands is refused
/// once the capacity frees, rather than launching under the seal.
#[tokio::test]
async fn an_activation_queued_for_capacity_when_the_seal_lands_is_refused() {
    let scratch = tempfile::tempdir().unwrap();
    let service = seal_test_service(scratch.path());
    let held = service
        .admit_vm_activation(ActivationAdmissionKind::Restore, "sandbox-held")
        .await
        .unwrap();
    let requested = Arc::new(std::sync::Mutex::new(Vec::new()));
    let queued = tokio::spawn(REQUESTED_VM_ACTIVATIONS.scope(Arc::clone(&requested), {
        let service = service.clone();
        async move {
            service
                .admit_vm_activation(ActivationAdmissionKind::Cold, "sandbox-queued")
                .await
                .map(drop)
        }
    }));
    // On this single-threaded runtime the request runs from its entry through
    // the seal's first check to the limiter's retry sleep in one poll.
    tokio::time::timeout(Duration::from_secs(10), async {
        while requested.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the cold boot reaches admission");
    assert!(!queued.is_finished(), "the cold boot waits for capacity");

    service.activation_seal.seal(&service.db, 4).unwrap();
    drop(held);
    let refused = tokio::time::timeout(Duration::from_secs(30), queued)
        .await
        .expect("the queued cold boot answers once capacity frees")
        .unwrap()
        .expect_err("the seal landed while it was queued");
    assert_sealed_refusal(&refused, 4);
    service.activation_seal.assert_quiesced();
    shut_down(service).await;
}

/// An admitted activation stays in flight for as long as its cleanup guard
/// lives, which is how it outlives its request in a detached finalizer: after
/// the permit is released, and until the finalizer succeeds, hands its failure
/// to cleanup, or is dropped.
#[tokio::test]
async fn an_admitted_activation_stays_in_flight_until_its_guard_ends() {
    let scratch = tempfile::tempdir().unwrap();
    let service = seal_test_service(scratch.path());
    let guard = || {
        ActivationCleanupGuard::with_interruption_reason(
            SandboxRuntimeLabel::parse("runtime1").unwrap(),
            SandboxQuarantineReasonCode::Unspecified,
            "activation did not complete",
            |_, _, _, _| {},
        )
    };
    type End = (&'static str, fn(ActivationCleanupGuard));
    let ends: [End; 4] = [
        ("success", ActivationCleanupGuard::complete_success),
        (
            "handled failure",
            ActivationCleanupGuard::complete_handled_failure,
        ),
        ("cleanup", ActivationCleanupGuard::cleanup),
        ("drop", drop),
    ];
    for (end, finish) in ends {
        let mut cleanup_guard = guard();
        cleanup_guard.admit_activation(
            service
                .admit_vm_activation(ActivationAdmissionKind::Restore, "sandbox-finalized")
                .await
                .unwrap(),
        );
        cleanup_guard.finish_admission(AdmissionPermitOutcome::Succeeded);
        assert_eq!(service.activation_admission_limiter.in_flight(), 0, "{end}");
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let finalizer = tokio::spawn(async move {
            let _ = release_rx.await;
            finish(cleanup_guard);
        });
        tokio::task::yield_now().await;
        assert_eq!(
            service.activation_seal.status().in_flight.len(),
            1,
            "{end}: the detached finalizer still holds the activation"
        );
        release_tx.send(()).unwrap();
        finalizer.await.unwrap();
        service.activation_seal.assert_quiesced();
    }
    shut_down(service).await;
}
