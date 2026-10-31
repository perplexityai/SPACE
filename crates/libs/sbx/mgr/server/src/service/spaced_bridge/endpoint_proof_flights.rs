//! One node-manager validation per endpoint proof at a time.
//!
//! Every guest process start validates its sandbox's endpoint proof, so a
//! burst of starts in one sandbox asks node-manager the same question many
//! times at once. As separate streams on the one HTTP/2 connection to
//! node-manager, a burst can outrun the server's stream acceptance; the
//! client then resets the streams it timed out, and enough resets make the
//! server's rapid-reset guard close the connection. A closed connection fails
//! the calls in flight on it. Concurrent validations of one proof therefore
//! share a single call.
//!
//! An answer reaches only the callers that joined while its call was in
//! flight. The call leaves the map before its answer is delivered, so a later
//! validation always asks node-manager again: an attestation answer must not
//! outlive the request that produced it. A caller that joins accepts an
//! evaluation that may have started before its own connect to spaced-host, and
//! shares the remainder of that call's 15 s retry window.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared, WeakShared};
use tonic::Status;

use crate::endpoint_proof::SpacedEndpointProof;

type Validation = BoxFuture<'static, Result<(), Status>>;

/// The validations in flight, keyed by the whole proof: sandbox id,
/// execution generation, activation attempt and every endpoint identity the
/// node-manager request carries. Two callers share a call only when they
/// would have sent node-manager the same question for the same activation.
#[derive(Clone, Default)]
pub(in crate::service) struct EndpointProofFlights {
    state: Arc<Mutex<FlightState>>,
}

#[derive(Default)]
struct FlightState {
    next_flight: u64,
    in_flight: HashMap<SpacedEndpointProof, InFlight>,
}

struct InFlight {
    flight: u64,
    call: WeakShared<Validation>,
}

impl EndpointProofFlights {
    /// Run `call` to validate `proof`, or join the call already validating it.
    ///
    /// The call runs only while some caller awaits it; when every caller has
    /// gone, it is dropped as an unshared call would be.
    pub(super) async fn validate<F, Fut>(
        &self,
        proof: &SpacedEndpointProof,
        call: F,
    ) -> Result<(), Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        self.join_or_start(proof, call).await
    }

    fn join_or_start<F, Fut>(&self, proof: &SpacedEndpointProof, call: F) -> Shared<Validation>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), Status>> + Send + 'static,
    {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(call) = state
            .in_flight
            .get(proof)
            .and_then(|in_flight| in_flight.call.upgrade())
        {
            return call;
        }
        let flight = state.next_flight;
        state.next_flight += 1;
        let registration = Registration {
            state: Arc::clone(&self.state),
            proof: proof.clone(),
            flight,
        };
        let pending = call();
        let call = async move {
            let result = pending.await;
            drop(registration);
            result
        }
        .boxed()
        .shared();
        // `downgrade` answers `None` only for a completed call, and this one
        // has not been polled yet.
        if let Some(weak) = call.downgrade() {
            state
                .in_flight
                .insert(proof.clone(), InFlight { flight, call: weak });
        }
        call
    }

    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .in_flight
            .len()
    }
}

/// Removes its call's entry when the call completes or is dropped. A newer
/// call for the same proof may already own the entry, after the last caller
/// of this one left and before this drop took the lock; that entry stays.
struct Registration {
    state: Arc<Mutex<FlightState>>,
    proof: SpacedEndpointProof,
    flight: u64,
}

impl Drop for Registration {
    fn drop(&mut self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state
            .in_flight
            .get(&self.proof)
            .is_some_and(|in_flight| in_flight.flight == self.flight)
        {
            state.in_flight.remove(&self.proof);
        }
    }
}

#[cfg(test)]
mod tests;
