//! Bounded admission before Tonic decodes payload-bearing requests.
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use pplx_space_grpc_metrics::payload::{self, Budget, Outcome, Site};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use crate::queued_slots::{QueuedSlots, SlotRequest};
use tonic::{Status, body::Body};
use tower::Service;

/// A handler can retain this request extension in service-owned execution so
/// caller cancellation does not release admission before its payload is freed.
#[derive(Clone, Debug)]
pub struct RequestPermit {
    inner: Arc<PermitInner>,
}

#[derive(Debug)]
struct PermitInner {
    permit: Mutex<Held>,
    semaphore: Arc<Semaphore>,
    slots: usize,
    acquired_at: Instant,
    observation: Option<Budget>,
    cancelled_at: OnceLock<Instant>,
}

/// What a permit holds: its payload slots, or, once
/// [`WeakRequestPermit::exchange`] returned them, a permit from another budget
/// held for the rest of the work.
#[derive(Debug)]
enum Held {
    // Each permit is held only to return its slots when dropped.
    Payload { _slots: OwnedSemaphorePermit },
    Exchanged { _slots: OwnedSemaphorePermit },
}

impl PermitInner {
    fn cancelled(&self) {
        if let Some(budget) = self.observation {
            self.cancelled_at.get_or_init(|| {
                let now = Instant::now();
                payload::cancelled(budget, self.slots);
                now
            });
        }
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Held> {
        self.permit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn holds_slots(&self) -> bool {
        matches!(*self.held(), Held::Payload { .. })
    }

    /// Swap the payload slots for `other`, for every clone, unless they are
    /// already gone; then `other` comes back.
    fn exchange(&self, other: OwnedSemaphorePermit) -> Result<(), OwnedSemaphorePermit> {
        let mut held = self.held();
        if !matches!(*held, Held::Payload { .. }) {
            return Err(other);
        }
        // The payload slots drop here, returning to their budget.
        *held = Held::Exchanged { _slots: other };
        drop(held);
        self.record_cancelled_release();
        Ok(())
    }

    fn record_cancelled_release(&self) {
        if let (Some(budget), Some(started)) = (self.observation, self.cancelled_at.get()) {
            payload::released_after_cancellation(budget, self.slots, started.elapsed());
        }
    }
}

impl Drop for PermitInner {
    fn drop(&mut self) {
        if self.holds_slots() {
            self.record_cancelled_release();
        }
    }
}

/// Observes a dropped request without retaining its admission or spawning work.
/// Completion, including an error response, is not request cancellation.
#[must_use = "observe the request future to avoid recording cancellation immediately"]
#[derive(Debug)]
pub struct RequestLifetime {
    permit: Weak<PermitInner>,
}

impl RequestLifetime {
    pub async fn observe<F: Future>(mut self, future: F) -> F::Output {
        let result = future.await;
        self.permit = Weak::new();
        result
    }
}

impl Drop for RequestLifetime {
    fn drop(&mut self) {
        if let Some(permit) = self.permit.upgrade() {
            permit.cancelled();
        }
    }
}

/// Refers to a [`RequestPermit`] without keeping its slots held.
#[derive(Clone, Debug)]
pub struct WeakRequestPermit {
    permit: Weak<PermitInner>,
}

impl WeakRequestPermit {
    /// How long the permit has held its slots, while it still holds them.
    pub fn held_for(&self) -> Option<std::time::Duration> {
        let permit = self.permit.upgrade()?;
        permit.holds_slots().then(|| permit.acquired_at.elapsed())
    }

    /// Return the payload slots to their budget now, for every clone, and
    /// hold `other` (from a separate, smaller budget) in their place for as
    /// long as the permit lives. For work whose hold time must be bounded
    /// where the payload budget cannot wait for it, such as a start stalled in
    /// the kernel, while `other` still caps how much such work is live.
    /// `other` comes back if every owner dropped the permit or it was already
    /// exchanged.
    pub fn exchange(&self, other: OwnedSemaphorePermit) -> Result<(), OwnedSemaphorePermit> {
        match self.permit.upgrade() {
            Some(permit) => permit.exchange(other),
            None => Err(other),
        }
    }
}

impl RequestPermit {
    fn new(permit: OwnedSemaphorePermit, observation: Option<Budget>) -> Self {
        Self {
            inner: Arc::new(PermitInner {
                semaphore: Arc::clone(permit.semaphore()),
                slots: permit.num_permits(),
                permit: Mutex::new(Held::Payload { _slots: permit }),
                acquired_at: Instant::now(),
                observation,
                cancelled_at: OnceLock::new(),
            }),
        }
    }

    /// The payload slots held, or 0 once exchanged.
    pub fn slots(&self) -> usize {
        if self.inner.holds_slots() {
            self.inner.slots
        } else {
            0
        }
    }

    /// A reference that does not keep the slots held.
    pub fn downgrade(&self) -> WeakRequestPermit {
        WeakRequestPermit {
            permit: Arc::downgrade(&self.inner),
        }
    }

    /// Observe cancellation while waiting for work owned beyond this request.
    pub fn request_lifetime(&self) -> RequestLifetime {
        RequestLifetime {
            permit: Arc::downgrade(&self.inner),
        }
    }

    /// Mark observed execution cancellation. Repeated observations share one
    /// timestamp across all clones; final release emits the retained duration.
    pub fn observe_cancellation(&self) {
        self.inner.cancelled();
    }

    /// Retain admission from this budget, or acquire it for a native call.
    /// A permit from a different budget or with insufficient weight cannot
    /// stand in for this operation's capacity. No queue is added here.
    pub fn try_acquire(
        slots: Arc<Semaphore>,
        weight: u32,
        admitted: Option<Self>,
    ) -> Result<Self, TryAcquireError> {
        Self::acquire(slots, weight, admitted, None)
    }

    /// As [`Self::try_acquire`], recording admission and cancellation retention.
    /// Handler acquisitions do not imply lost transport admission: native calls
    /// legitimately arrive without a request extension.
    pub fn try_acquire_observed(
        slots: Arc<Semaphore>,
        weight: u32,
        admitted: Option<Self>,
        budget: Budget,
    ) -> Result<Self, TryAcquireError> {
        Self::acquire(slots, weight, admitted, Some(budget))
    }

    fn acquire(
        slots: Arc<Semaphore>,
        weight: u32,
        admitted: Option<Self>,
        observation: Option<Budget>,
    ) -> Result<Self, TryAcquireError> {
        let supplied = admitted.is_some();
        if let Some(permit) = admitted
            && Arc::ptr_eq(&permit.inner.semaphore, &slots)
            && permit.slots() >= weight as usize
        {
            if let Some(budget) = observation {
                payload::admission(budget, Site::Handler, Outcome::Reused);
            }
            return Ok(permit);
        }
        let permit = slots.try_acquire_many_owned(weight);
        if let Some(budget) = observation {
            let outcome = match (&permit, supplied) {
                (Err(_), _) => Outcome::Rejected,
                (Ok(_), true) => Outcome::Reacquired,
                (Ok(_), false) => Outcome::Acquired,
            };
            payload::admission(budget, Site::Handler, outcome);
        }
        permit.map(|permit| Self::new(permit, observation))
    }
}

/// Admission for selected request payloads (including uploads).
/// Clones/listeners must share one [`QueuedSlots`]. Unselected methods are
/// unaffected. Overload is rejected, or waits in the slots' bounded queue if
/// they have one, without polling the body; a queued request's deadline
/// includes time before the returned future is first polled.
/// Each selected method specifies its slot cost; larger decoded payloads can
/// consume more of the same budget without creating a second admission queue.
/// Admission ends when response headers open, not when a response stream ends.
/// Work retaining request payloads beyond that point must retain RequestPermit.
#[derive(Clone, Debug)]
pub struct RequestAdmission<S> {
    inner: S,
    slots: QueuedSlots,
    methods: &'static [(&'static str, u32)],
    observation: Option<Budget>,
}

impl<S> RequestAdmission<S> {
    pub fn new(inner: S, slots: QueuedSlots, methods: &'static [(&'static str, u32)]) -> Self {
        Self {
            inner,
            slots,
            methods,
            observation: None,
        }
    }

    /// Record payload admission for this shared budget. Other services remain
    /// uninstrumented unless they opt in.
    pub fn with_observation(mut self, budget: Budget) -> Self {
        self.observation = Some(budget);
        self
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for RequestAdmission<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for RequestAdmission<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let method = request.uri().path().rsplit('/').next().unwrap_or("");
        let permit = match self.methods.iter().find(|(name, _)| *name == method) {
            None => None,
            Some((_, weight)) => match self.slots.request(*weight) {
                SlotRequest::Admitted(permit) => {
                    if let Some(budget) = self.observation {
                        payload::admission(budget, Site::Transport, Outcome::Acquired);
                    }
                    let permit = RequestPermit::new(permit, self.observation);
                    request.extensions_mut().insert(permit.clone());
                    Some(permit)
                }
                SlotRequest::Queued(wait) => {
                    let replacement = self.inner.clone();
                    let mut inner = std::mem::replace(&mut self.inner, replacement);
                    let observation = self.observation;
                    return Box::pin(async move {
                        let Some(permit) = wait.admitted().await else {
                            return Ok(capacity_exhausted(observation));
                        };
                        if let Some(budget) = observation {
                            payload::admission(budget, Site::Transport, Outcome::Acquired);
                        }
                        let permit = RequestPermit::new(permit, observation);
                        request.extensions_mut().insert(permit.clone());
                        let _permit = permit;
                        inner.call(request).await
                    });
                }
                SlotRequest::Rejected => {
                    let response = capacity_exhausted(self.observation);
                    return Box::pin(async { Ok(response) });
                }
            },
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let _permit = permit;
            future.await
        })
    }
}

fn capacity_exhausted(observation: Option<Budget>) -> http::Response<Body> {
    if let Some(budget) = observation {
        payload::admission(budget, Site::Transport, Outcome::Rejected);
    }
    crate::refused_before_start(
        Status::resource_exhausted("payload request capacity exhausted"),
        CAPACITY_RETRY_AFTER,
    )
    .into_http()
}

// Payload slots free as in-flight transfers finish; a second is a fair wait.
const CAPACITY_RETRY_AFTER: std::time::Duration = std::time::Duration::from_secs(1);

#[cfg(test)]
mod tests;
