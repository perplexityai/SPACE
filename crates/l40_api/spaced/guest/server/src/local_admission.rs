use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::Instant;
use tonic::{Status, body::Body, codegen::http};
use tower::Service;

use super::{LocalMetaServer, SpacedGuestService};

/// Local callers share one sandbox trust domain. Reserve health separately
/// from forwarded signals, whose sustained rate matches the host's 5/s policy.
/// Histories are bounded to 16 Ping and 5 signal arrivals, across all peers.
#[derive(Clone)]
pub(super) struct LocalMetaAdmission {
    inner: LocalMetaServer<SpacedGuestService>,
    health: LocalBudget,
    signals: LocalBudget,
}

impl LocalMetaAdmission {
    pub(super) fn new(service: SpacedGuestService) -> Self {
        Self {
            inner: LocalMetaServer::new(service),
            health: LocalBudget::new(1, 16),
            signals: LocalBudget::new(7, 5),
        }
    }
}

#[derive(Clone)]
struct LocalBudget {
    slots: Arc<Semaphore>,
    arrivals: Arc<Mutex<VecDeque<Instant>>>,
    rate: usize,
}

impl LocalBudget {
    fn new(concurrency: usize, rate: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(concurrency)),
            arrivals: Arc::new(Mutex::new(VecDeque::with_capacity(rate))),
            rate,
        }
    }

    fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        let permit = self.slots.clone().try_acquire_owned().map_err(|_| {
            crate::refused(Status::resource_exhausted(
                "local metadata request capacity exhausted",
            ))
        })?;
        let mut arrivals = self
            .arrivals
            .lock()
            .map_err(|_| Status::internal("local metadata rate state poisoned"))?;
        let now = Instant::now();
        while arrivals
            .front()
            .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(1))
        {
            arrivals.pop_front();
        }
        if arrivals.len() >= self.rate {
            return Err(crate::refused(Status::resource_exhausted(
                "local metadata request rate exhausted",
            )));
        }
        arrivals.push_back(now);
        Ok(permit)
    }
}

impl tonic::server::NamedService for LocalMetaAdmission {
    const NAME: &'static str =
        <LocalMetaServer<SpacedGuestService> as tonic::server::NamedService>::NAME;
}

impl Service<http::Request<Body>> for LocalMetaAdmission {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = tonic::codegen::BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        <LocalMetaServer<SpacedGuestService> as Service<http::Request<Body>>>::poll_ready(
            &mut self.inner,
            cx,
        )
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let budget = if request.uri().path().rsplit('/').next() == Some("Ping") {
            &self.health
        } else {
            &self.signals
        };
        let permit = match budget.admit() {
            Ok(permit) => permit,
            Err(status) => return Box::pin(async move { Ok(status.into_http()) }),
        };
        let future = self.inner.call(request);
        Box::pin(async move {
            let _permit = permit;
            future.await
        })
    }
}
