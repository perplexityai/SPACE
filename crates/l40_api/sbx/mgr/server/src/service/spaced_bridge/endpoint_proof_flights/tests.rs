use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::*;
use crate::service::spaced_bridge::tests::endpoint_proof;

const CALLERS: usize = 8;

/// A node-manager call that is counted when it starts and, once it takes one
/// of `gate`'s permits, answers `result` with the instant it was sent: when
/// it was first polled.
fn gated_call(
    calls: &Arc<AtomicUsize>,
    gate: &Arc<tokio::sync::Semaphore>,
    result: Result<(), Status>,
) -> impl FnMut() -> BoxFuture<'static, Result<Instant, Status>> {
    let calls = Arc::clone(calls);
    let gate = Arc::clone(gate);
    move || {
        calls.fetch_add(1, Ordering::SeqCst);
        let gate = Arc::clone(&gate);
        let result = result.clone();
        async move {
            let sent_at = Instant::now();
            gate.acquire().await.unwrap().forget();
            result.map(|()| sent_at)
        }
        .boxed()
    }
}

fn host_replaced() -> Status {
    Status::permission_denied("spaced-host was replaced")
}

#[tokio::test]
async fn concurrent_validations_of_one_proof_share_one_call() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let validations = futures_util::future::join_all(
        (0..CALLERS)
            .map(|_| flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(())))),
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
        flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))),
        flights.validate(
            &next_activation,
            Instant::now(),
            gated_call(&calls, &gate, Err(Status::permission_denied("stale"))),
        ),
        flights.validate(
            &restarted_host,
            Instant::now(),
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
            Instant::now(),
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
        .validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(())))
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

    let mut starter =
        Box::pin(flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))));
    let mut joiner =
        Box::pin(flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut starter).is_pending());
    assert!(futures_util::poll!(&mut joiner).is_pending());
    drop(starter);
    gate.add_permits(1);
    joiner.await.expect("the joined call still answers");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let mut abandoned =
        Box::pin(flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut abandoned).is_pending());
    drop(abandoned);
    assert_eq!(flights.in_flight(), 0);

    gate.add_permits(1);
    flights
        .validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(())))
        .await
        .expect("an abandoned call is not joined");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_connected_after_the_shared_attempt_was_sent_asks_again() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let mut starter =
        Box::pin(flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut starter).is_pending());
    tokio::time::advance(Duration::from_millis(1)).await;
    // This caller connects after the shared attempt was sent, so node-manager
    // may have evaluated the proof before the connect. By the time it is asked
    // again, the attested spaced-host is gone.
    let mut late = Box::pin(flights.validate(
        &proof,
        Instant::now(),
        gated_call(&calls, &gate, Err(host_replaced())),
    ));
    assert!(futures_util::poll!(&mut late).is_pending());
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    gate.add_permits(1);
    starter
        .await
        .expect("the starter connected before its attempt was sent");
    gate.add_permits(1);
    let error = late
        .await
        .expect_err("the shared answer predates this connect");

    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(flights.in_flight(), 0);
}

#[tokio::test(start_paused = true)]
async fn a_caller_that_connected_before_the_shared_attempt_was_sent_shares_it() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));

    let early_connected_at = Instant::now();
    tokio::time::advance(Duration::from_millis(1)).await;
    let mut starter =
        Box::pin(flights.validate(&proof, Instant::now(), gated_call(&calls, &gate, Ok(()))));
    assert!(futures_util::poll!(&mut starter).is_pending());
    let mut early = Box::pin(flights.validate(
        &proof,
        early_connected_at,
        gated_call(&calls, &gate, Err(host_replaced())),
    ));
    assert!(futures_util::poll!(&mut early).is_pending());

    gate.add_permits(1);
    let (starter, early) = futures_util::future::join(starter, early).await;

    assert!(starter.is_ok(), "{starter:?}");
    assert!(early.is_ok(), "{early:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn a_caller_asks_at_most_twice() {
    let flights = EndpointProofFlights::default();
    let proof = endpoint_proof(1);
    let calls = Arc::new(AtomicUsize::new(0));
    // A double whose every answer predates the connect, which real attempts
    // cannot produce: they are sent after the caller connects.
    let stale = Instant::now();
    tokio::time::advance(Duration::from_millis(1)).await;
    let call = {
        let calls = Arc::clone(&calls);
        move || {
            calls.fetch_add(1, Ordering::SeqCst);
            futures_util::future::ready(Ok(stale))
        }
    };

    let error = flights
        .validate(&proof, Instant::now(), call)
        .await
        .expect_err("no answer was sent after the connect");

    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(calls.load(Ordering::SeqCst), MAX_ASKS);
    assert_eq!(flights.in_flight(), 0);
}
