//! Preserve interrupted input at Tonic's request-decoding boundary.
use std::pin::Pin;
use std::task::{Context, Poll};

use http_body::{Body as HttpBody, Frame, SizeHint};
use tonic::{Status, body::Body};
use tower::Service;

/// Tonic treats Code::Cancelled from a request body as successful EOF.
/// Client-streaming services that use EOF as a commit boundary need cancellation
/// to remain an error. This adapter is opt-in per service, not a global policy.
#[derive(Clone, Debug)]
pub struct PreserveInputErrors<S> {
    service: S,
    /// The methods it applies to, by name; `None` is every method.
    methods: Option<&'static [&'static str]>,
}

impl<S> PreserveInputErrors<S> {
    pub fn new(service: S) -> Self {
        Self {
            service,
            methods: None,
        }
    }

    /// Only for `methods`, so a service whose other streams treat a
    /// cancelled input as its end keeps doing so.
    pub fn for_methods(service: S, methods: &'static [&'static str]) -> Self {
        Self {
            service,
            methods: Some(methods),
        }
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for PreserveInputErrors<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for PreserveInputErrors<S>
where
    S: Service<http::Request<Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let method = request.uri().path().rsplit('/').next().unwrap_or("");
        if self
            .methods
            .is_some_and(|methods| !methods.contains(&method))
        {
            return self.service.call(request);
        }
        self.service
            .call(request.map(|body| Body::new(InputBody(body))))
    }
}

struct InputBody(Body);

impl HttpBody for InputBody {
    type Data = bytes::Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Status>>> {
        match Pin::new(&mut self.0).poll_frame(cx) {
            Poll::Ready(Some(Err(status))) if status.code() == tonic::Code::Cancelled => {
                Poll::Ready(Some(Err(Status::aborted("request input stream cancelled"))))
            }
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }
    fn size_hint(&self) -> SizeHint {
        self.0.size_hint()
    }
}

#[cfg(test)]
mod tests;
