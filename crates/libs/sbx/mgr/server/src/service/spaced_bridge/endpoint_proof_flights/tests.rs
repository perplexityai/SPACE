use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::service::spaced_bridge::tests::endpoint_proof;

const CALLERS: usize = 8;

/// A node-manager call that is counted when it starts and answers `result`
/// once it takes one of `gate`'s permits.
fn gated_call(
    calls: &Arc<AtomicUsize>,
    gate: &Arc<tokio::sync::Semaphore>,
    result: Result<(), Status>,
) -> impl FnOnce() -> BoxFuture<'static, Result<(), Status>> {
    let calls = Arc::clone(calls);
    let gate = Arc::clone(gate);
    move || {
        calls.fetch_add(1, Ordering::SeqCst);
        async move {
            gate.acquire().await.unwrap().forget();
            result
        }
        .boxed()
    }
}

#[tokio::test]
async fn concurrent_validations_of_one_proof_share_one_call() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let validations = futures_util::future::join_all(
        (0..CALLERS).map(|_| flights.validate(&proof, gated_call(&calls, &gate, Ok(())))),
    );
    tokio::pin!(validations);
    assert!(futures_util::poll!(&mut validations).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    gate.add_permits(1);
    let results = validations.await;

    assert_eq!(results.len(), CALLERS);
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(flights.in_flight(), 0);
}

#[tokio::test]
async fn different_proofs_do_not_share_a_call() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    // Same endpoint, next activation: node-manager is asked about each.
    let next_activation = SpacedEndpointProof {
        activation_attempt_id: pplx_space_common_core::types::SandboxActivationAttemptId::new(),
        ..proof.clone()
    };
    // Same activation, but spaced-host restarted in place.
    let restarted_host = SpacedEndpointProof {
        spaced_host: crate::endpoint_proof::ProcessActivationIdentity {
            start_time_ticks: 2,
            ..proof.spaced_host.clone()
        },
        ..proof.clone()
    };
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let validations = futures_util::future::join3(
        flights.validate(&proof, gated_call(&calls, &gate, Ok(()))),
        flights.validate(
            &next_activation,
            gated_call(&calls, &gate, Err(Status::permission_denied("stale"))),
        ),
        flights.validate(
            &restarted_host,
            gated_call(&calls, &gate, Err(Status::unavailable("restarting"))),
        ),
    );
    tokio::pin!(validations);
    assert!(futures_util::poll!(&mut validations).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    gate.add_permits(3);
    let (current, next, restarted) = validations.await;

    assert!(current.is_ok(), "{current:?}");
    assert_eq!(next.unwrap_err().code(), tonic::Code::PermissionDenied);
    assert_eq!(restarted.unwrap_err().code(), tonic::Code::Unavailable);
}

#[tokio::test]
async fn an_error_reaches_every_waiter_and_is_not_reused() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let validations = futures_util::future::join_all((0..CALLERS).map(|_| {
        flights.validate(
            &proof,
            gated_call(&calls, &gate, Err(Status::unavailable("connection reset"))),
        )
    }));
    tokio::pin!(validations);
    assert!(futures_util::poll!(&mut validations).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    gate.add_permits(1);
    let results = validations.await;

    for result in results {
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert_eq!(error.message(), "connection reset");
    }

    gate.add_permits(1);
    flights
        .validate(&proof, gated_call(&calls, &gate, Ok(())))
        .await
        .expect("a later validation asks node-manager again");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn a_call_outlives_the_caller_that_started_it_but_not_the_last_one() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let mut starter = Box::pin(flights.validate(&proof, gated_call(&calls, &gate, Ok(()))));
    let mut joiner = Box::pin(flights.validate(&proof, gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut starter).is_pending());
    assert!(futures_util::poll!(&mut joiner).is_pending());
    drop(starter);
    gate.add_permits(1);
    joiner.await.expect("the joined call still answers");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut abandoned = Box::pin(flights.validate(&proof, gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut abandoned).is_pending());
    drop(abandoned);
    assert_eq!(flights.in_flight(), 0);

    gate.add_permits(1);
    flights
        .validate(&proof, gated_call(&calls, &gate, Ok(())))
        .await
        .expect("an abandoned call is not joined");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}
