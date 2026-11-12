//! A method may need a longer wait without extending every other RPC's budget.
//! Tonic's outer transport deadline still honors a shorter client deadline.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use http::{Request, Response};
use tower::{Layer, Service};

#[derive(Clone, Copy)]
pub struct MethodTimeoutLayer(pub fn(&str) -> Duration);

impl<S> Layer<S> for MethodTimeoutLayer {
    type Service = MethodTimeout<S>;

    fn layer(&self, inner: S) -> Self::Service {
        MethodTimeout {
            inner,
            timeout: self.0,
        }
    }
}

#[derive(Clone)]
pub struct MethodTimeout<S> {
    inner: S,
    timeout: fn(&str) -> Duration,
}

impl<S, B, R> Service<Request<B>> for MethodTimeout<S>
where
    S: Service<Request<B>, Response = Response<R>>,
    S::Future: Send + 'static,
    S::Error: 'static,
    R: Default + 'static,
{
    type Response = Response<R>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let timeout = (self.timeout)(request.uri().path());
        let response = self.inner.call(request);
        Box::pin(async move {
            match tokio::time::timeout(timeout, response).await {
                Ok(result) => result,
                Err(_) => {
                    Ok(tonic::Status::deadline_exceeded("RPC wait budget expired").into_http())
                }
            }
        })
    }
}

#[cfg(test)]
mod tests;
