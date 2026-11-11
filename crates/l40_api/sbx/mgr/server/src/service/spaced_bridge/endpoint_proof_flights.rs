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
//! outlive the request that produced it.
//!
//! A caller first connects to spaced-host, which checks the peer's pid and
//! uid, and then asks node-manager whether the attested spaced-host is alive.
//! That order is what ties the peer to the proof: if node-manager evaluates
//! the proof at `e` and the connect finished at `c <= e`, the peer at `c` was
//! the attested process, whose pid cannot be reused while it lives. A caller
//! that joins an in-flight call may have connected after node-manager already
//! evaluated it, so a successful call reports when its answering attempt was
//! sent, and a caller accepts it only if it was sent at or after the caller's
//! own connect. Node-manager evaluates a request after it is sent, so
//! `e >= sent_at >= c`. A caller whose shared answer predates its connect asks
//! again; the call it joined has left the map by then, so its next answer is
//! always sent after its connect. A caller therefore asks at most twice, and
//! a re-asking caller waits for the remainder of the joined call's 15 s retry
//! window and then up to a fresh one. A refusal or failure reaches every caller
//! of its call, whenever it was sent: rejecting fails closed.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::FutureExt as _;
use futures_util::future::{BoxFuture, Shared, WeakShared};
use tokio::time::Instant;
use tonic::Status;

use crate::endpoint_proof::SpacedEndpointProof;

/// A successful call answers when its answering attempt was sent to
/// node-manager, read from the monotonic clock before the request went out.
type Validation = BoxFuture<'static, Result<Instant, Status>>;

/// The first ask plus one re-ask, which the module doc shows always answers
/// after the caller's connect.
const MAX_ASKS: usize = 2;

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
    /// Run `call` to validate `proof`, or join the call already validating it,
    /// accepting only an answer sent at or after `connected_at`, the instant
    /// the caller's spaced-host connect returned.
    ///
    /// The call runs only while some caller awaits it; when every caller has
    /// gone, it is dropped as an unshared call would be.
    pub(super) async fn validate<F, Fut>(
        &self,
        proof: &SpacedEndpointProof,
        connected_at: Instant,
        mut call: F,
    ) -> Result<(), Status>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<Instant, Status>> + Send + 'static,
    {
        for _ in 0..MAX_ASKS {
            let sent_at = self.join_or_start(proof, &mut call).await?;
            if sent_at >= connected_at {
                return Ok(());
            }
        }
        // Unreachable unless the monotonic clock ran backwards.
        tracing::warn!(
            sandbox_id = %proof.sandbox_id,
            "every endpoint proof answer predates the spaced-host connect"
        );
        Err(Status::unavailable(
            "spaced endpoint proof is temporarily unavailable",
        ))
    }

    fn join_or_start<F, Fut>(&self, proof: &SpacedEndpointProof, call: F) -> Shared<Validation>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Instant, Status>> + Send + 'static,
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
