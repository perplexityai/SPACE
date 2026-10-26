use super::*;
use crate::BridgeGuestMeta;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request};

fn guest() -> SpacedGuestService {
    let service = SpacedGuestService::new(CancellationToken::new()).unwrap();
    service.ready();
    service
}

fn activation(service: &SpacedGuestService) -> ActivateProcessRecoveryRequest {
    ActivateProcessRecoveryRequest {
        expected_barrier_id: service.inner.process_recovery.barrier().to_string(),
        activation_id: ProcessRecoveryToken::new().to_string(),
    }
}

async fn activate(service: &SpacedGuestService, request: ActivateProcessRecoveryRequest) {
    let expected = request.activation_id.clone();
    let response = BridgeGuestMeta::activate_process_recovery(service, Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        response.process_execution.unwrap().activation_id,
        Some(expected)
    );
}

#[tokio::test]
async fn snapshot_preparation_invalidates_delayed_activation_even_when_flush_fails() {
    let service = guest();
    let request = activation(&service);
    activate(&service, request.clone()).await;
    activate(&service, request.clone()).await;
    let mut conflicting = request.clone();
    conflicting.activation_id = ProcessRecoveryToken::new().to_string();
    assert_eq!(
        service
            .activate_recovery(conflicting)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );

    for fail_flush in [false, true] {
        let delayed = ActivateProcessRecoveryRequest {
            expected_barrier_id: service.inner.process_recovery.barrier().to_string(),
            ..request.clone()
        };
        let execution = service.inner.execution.clone();
        let result = service
            .flush_with_recovery_barrier(move || {
                assert!(
                    execution.process_execution_info().activation_id.is_none(),
                    "quiesce before flushing"
                );
                if fail_flush {
                    Err(Status::internal("synthetic flush failure"))
                } else {
                    Ok(())
                }
            })
            .await;
        assert_eq!(result.is_err(), fail_flush);
        assert_eq!(
            service.activate_recovery(delayed).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert!(
            service
                .inner
                .execution
                .process_execution_info()
                .activation_id
                .is_none()
        );
        // Resident resume or rollback uses the same external activation, but a
        // fresh actual restore must choose a new one. Both require the new barrier.
        activate(
            &service,
            ActivateProcessRecoveryRequest {
                expected_barrier_id: service.inner.process_recovery.barrier().to_string(),
                ..request.clone()
            },
        )
        .await;
    }
    service.inner.shutdown.cancel();
    service.inner.execution.drain().await.unwrap();
}

#[tokio::test]
async fn cancelled_flush_keeps_activation_excluded_until_the_worker_settles() {
    let service = guest();
    activate(&service, activation(&service)).await;
    let previous = service.inner.process_recovery.barrier();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let worker = service.clone();
    let flush = tokio::spawn(async move {
        worker
            .flush_with_recovery_barrier(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
    });
    started_rx.await.unwrap();
    flush.abort();
    assert!(flush.await.unwrap_err().is_cancelled());
    let refused = service.activate_recovery(activation(&service)).await;
    // Always release the real blocking worker before asserting.
    release_tx.send(()).unwrap();
    assert_eq!(refused.unwrap_err().code(), Code::ResourceExhausted);
    assert_ne!(service.inner.process_recovery.barrier(), previous);
    assert!(
        service
            .inner
            .execution
            .process_execution_info()
            .activation_id
            .is_none()
    );
    drop(
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            service.inner.process_recovery.transaction.acquire(),
        )
        .await
        .unwrap()
        .unwrap(),
    );
    activate(&service, activation(&service)).await;
    service.inner.shutdown.cancel();
    service.inner.execution.drain().await.unwrap();
}

#[tokio::test]
async fn recovery_activation_rejects_malformed_and_previous_service_barriers() {
    let service = guest();
    let other = guest();
    let valid = activation(&service);
    for (barrier, epoch, code) in [
        (
            String::new(),
            valid.activation_id.clone(),
            Code::InvalidArgument,
        ),
        (
            valid.expected_barrier_id.clone(),
            String::new(),
            Code::InvalidArgument,
        ),
        (
            other.inner.process_recovery.barrier().to_string(),
            valid.activation_id.clone(),
            Code::FailedPrecondition,
        ),
    ] {
        let error = service
            .activate_recovery(ActivateProcessRecoveryRequest {
                expected_barrier_id: barrier,
                activation_id: epoch,
            })
            .await
            .unwrap_err();
        assert_eq!(error.code(), code);
        assert!(
            service
                .inner
                .execution
                .process_execution_info()
                .activation_id
                .is_none()
        );
    }
    activate(&service, valid).await;
    for service in [service, other] {
        service.inner.shutdown.cancel();
        service.inner.execution.drain().await.unwrap();
    }
}
