use super::*;
use crate::proto::InFlightActivationKind as WireKind;
use crate::service::activation_support::internal_request;
use crate::service::start_support::ActivationAdmissionKind;

fn test_service(scratch: &std::path::Path) -> SandboxManagerService {
    SandboxManagerService::new_with_options(
        crate::db::test_conn(),
        SandboxManagerServiceOptions::for_test_in(scratch),
    )
}

async fn shut_down(service: SandboxManagerService) {
    service.shutdown_token.cancel();
    service.task_tracker.close();
    tokio::time::timeout(Duration::from_secs(30), service.task_tracker.wait())
        .await
        .expect("service tasks stop within 30 seconds of shutdown");
}

/// The seal RPCs answer what the seal holds: `SealActivations` the epoch, its
/// start and the work still in flight under each kind's own wire value;
/// `ActivationSealState` the same without changing it; `UnsealActivations`
/// refuses another epoch and then clears the seal.
#[tokio::test]
async fn seal_rpcs_answer_the_seal_and_the_work_in_flight() {
    let scratch = tempfile::tempdir().unwrap();
    let service = test_service(scratch.path());
    let held = InFlightActivationKind::ALL.map(|kind| {
        if kind.is_activation() {
            service
                .activation_seal
                .admit(kind, &format!("sandbox-{kind}"))
                .unwrap()
        } else {
            service
                .activation_seal
                .continuation(kind, &format!("sandbox-{kind}"))
        }
    });

    let sealed = service
        .seal_activations(internal_request(SealActivationsRequest {
            epoch: 12,
            wait_timeout: None,
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!sealed.quiesced);
    let sealed = sealed.status.expect("the seal answers its status");
    assert_eq!(sealed.sealed_epoch, Some(12));
    assert_eq!(sealed.activations_admitted, 3);
    let kinds = sealed
        .in_flight
        .iter()
        .map(|in_flight| (in_flight.kind(), in_flight.sandbox_id.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            (WireKind::Cold, "sandbox-cold"),
            (WireKind::Restore, "sandbox-restore"),
            (WireKind::ResidentResume, "sandbox-resident_resume"),
            (WireKind::Checkpoint, "sandbox-checkpoint"),
            (WireKind::StopFlush, "sandbox-stop_flush"),
        ]
    );
    assert!(
        sealed
            .in_flight
            .iter()
            .all(|in_flight| in_flight.age.is_some())
    );
    let since = sealed.sealed_since.expect("a seal has a start");
    let since_ms = service.activation_seal.status().sealed.unwrap().since_ms;
    assert_eq!(
        since.seconds * 1_000 + i64::from(since.nanos / 1_000_000),
        i64::try_from(since_ms).unwrap()
    );

    let state = service
        .activation_seal_state(internal_request(ActivationSealStateRequest {}))
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(state.sealed_epoch, Some(12));
    assert_eq!(state.sealed_since, Some(since));
    assert_eq!(state.in_flight.len(), 5);

    let refused = service
        .unseal_activations(internal_request(UnsealActivationsRequest { epoch: 11 }))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert_eq!(service.activation_seal.status().sealed.unwrap().epoch, 12);

    drop(held);
    let unsealed = service
        .unseal_activations(internal_request(UnsealActivationsRequest { epoch: 12 }))
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    assert_eq!(unsealed.sealed_epoch, None);
    assert_eq!(unsealed.sealed_since, None);
    assert!(unsealed.in_flight.is_empty());
    assert_eq!(unsealed.activations_admitted, 3);
    shut_down(service).await;
}

/// Shutdown ends a seal's wait: with work still in flight, cancelling the
/// service's shutdown token answers at once, not quiesced, with the work
/// listed and the seal in place.
#[tokio::test]
async fn shutdown_ends_a_seals_wait_with_the_work_still_in_flight() {
    let scratch = tempfile::tempdir().unwrap();
    let service = test_service(scratch.path());
    let held = service
        .activation_seal
        .admit(InFlightActivationKind::Cold, "sandbox-held")
        .unwrap();
    let waiting = tokio::spawn({
        let service = service.clone();
        async move {
            service
                .seal_activations(internal_request(SealActivationsRequest {
                    epoch: 4,
                    wait_timeout: Some(prost_types::Duration {
                        seconds: i64::try_from(crate::activation_seal::MAX_SEAL_WAIT.as_secs())
                            .unwrap(),
                        nanos: 0,
                    }),
                }))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), async {
        while service.activation_seal.status().sealed.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the seal lands");
    assert!(!waiting.is_finished(), "the seal waits for the cold boot");

    service.shutdown_token.cancel();
    let answered = tokio::time::timeout(Duration::from_secs(10), waiting)
        .await
        .expect("shutdown ends the wait")
        .unwrap()
        .unwrap()
        .into_inner();
    assert!(!answered.quiesced);
    let status = answered.status.unwrap();
    assert_eq!(status.sealed_epoch, Some(4));
    assert_eq!(
        status
            .in_flight
            .iter()
            .map(|in_flight| (in_flight.kind(), in_flight.sandbox_id.as_str()))
            .collect::<Vec<_>>(),
        [(WireKind::Cold, "sandbox-held")]
    );
    drop(held);
    shut_down(service).await;
}

fn unprivileged<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(pplx_space_uds_peer_auth::PeerIdentity::new(
            64_000, 64_000, None,
        ));
    request
}

/// A malformed wait is refused before anything is sealed, and only root or
/// the local admin role may seal or unseal.
#[tokio::test]
async fn seal_rpcs_refuse_a_malformed_wait_and_an_unprivileged_caller() {
    let scratch = tempfile::tempdir().unwrap();
    let service = test_service(scratch.path());
    let malformed = service
        .seal_activations(internal_request(SealActivationsRequest {
            epoch: 1,
            wait_timeout: Some(prost_types::Duration {
                seconds: -1,
                nanos: 0,
            }),
        }))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
    assert_eq!(service.activation_seal.status().sealed, None);

    let denied = service
        .seal_activations(unprivileged(SealActivationsRequest {
            epoch: 1,
            wait_timeout: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied:?}");
    assert_eq!(service.activation_seal.status().sealed, None);
    service.activation_seal.seal(&service.db, 1).unwrap();
    let denied = service
        .unseal_activations(unprivileged(UnsealActivationsRequest { epoch: 1 }))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied:?}");
    assert_eq!(service.activation_seal.status().sealed.unwrap().epoch, 1);
    shut_down(service).await;
}

/// A service started on a sealed node's database is sealed under the same
/// epoch, with the seal's original start, and refuses activations; one
/// started after the unseal is unsealed and admits them.
#[tokio::test]
async fn a_seal_and_an_unseal_survive_a_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sandbox.sqlite");
    crate::db::create_test_schema_on_disk(&path);
    let start = || {
        SandboxManagerService::new_with_options(
            crate::db::Connection::open_at(&path).unwrap(),
            SandboxManagerServiceOptions::for_test_in(directory.path()),
        )
    };
    let state = |service: &SandboxManagerService| {
        let service = service.clone();
        async move {
            service
                .activation_seal_state(internal_request(ActivationSealStateRequest {}))
                .await
                .unwrap()
                .into_inner()
                .status
                .unwrap()
        }
    };

    let service = start();
    let sealed = service
        .seal_activations(internal_request(SealActivationsRequest {
            epoch: 11,
            wait_timeout: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .status
        .unwrap();
    shut_down(service).await;
    tokio::time::sleep(Duration::from_millis(5)).await;

    let service = start();
    assert_eq!(service.activation_seal_load_error(), None);
    let restarted = state(&service).await;
    assert_eq!(restarted.sealed_epoch, Some(11));
    assert_eq!(
        restarted.sealed_since, sealed.sealed_since,
        "the restart keeps the seal's start"
    );
    let refused = service
        .admit_vm_activation(ActivationAdmissionKind::Cold, "sandbox-restarted")
        .await
        .unwrap_err();
    assert!(
        refused
            .message()
            .contains(pplx_space_common_core::activation_seal_signals::ACTIVATIONS_SEALED_MARKER),
        "{refused:?}"
    );
    service
        .unseal_activations(internal_request(UnsealActivationsRequest { epoch: 11 }))
        .await
        .unwrap();
    shut_down(service).await;

    let service = start();
    assert_eq!(state(&service).await.sealed_epoch, None);
    drop(
        service
            .admit_vm_activation(ActivationAdmissionKind::Cold, "sandbox-unsealed")
            .await
            .unwrap(),
    );
    shut_down(service).await;
}
