//! Bounded admission before Tonic decodes payload-bearing requests.
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::OwnedSemaphorePermit;

use crate::queued_slots::{QueuedSlots, SlotRequest};
use tonic::{Status, body::Body};
use tower::Service;

/// A handler can retain this request extension in service-owned execution so
/// caller cancellation does not release admission before its payload is freed.
#[derive(Clone, Debug)]
pub struct RequestPermit {
    _permit: Arc<OwnedSemaphorePermit>,
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
}

impl<S> RequestAdmission<S> {
    pub fn new(inner: S, slots: QueuedSlots, methods: &'static [(&'static str, u32)]) -> Self {
        Self {
            inner,
            slots,
            methods,
        }
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
                    let permit = RequestPermit {
                        _permit: Arc::new(permit),
                    };
                    request.extensions_mut().insert(permit.clone());
                    Some(permit)
                }
                SlotRequest::Queued(wait) => {
                    let replacement = self.inner.clone();
                    let mut inner = std::mem::replace(&mut self.inner, replacement);
                    return Box::pin(async move {
                        let Some(permit) = wait.admitted().await else {
                            return Ok(capacity_exhausted());
                        };
                        let permit = RequestPermit {
                            _permit: Arc::new(permit),
                        };
                        request.extensions_mut().insert(permit.clone());
                        let _permit = permit;
                        inner.call(request).await
                    });
                }
                SlotRequest::Rejected => return Box::pin(async { Ok(capacity_exhausted()) }),
            },
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let _permit = permit;
            future.await
        })
    }
}

fn capacity_exhausted() -> http::Response<Body> {
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
