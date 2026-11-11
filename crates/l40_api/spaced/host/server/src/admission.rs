//! Bounded maintenance waiting after authentication and request admission.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::codegen::{Bytes, StdError, http};
use tower::{Layer, Service};

use super::SpacedHostService;

#[derive(Clone)]
pub(super) struct HostAdmissionLayer(pub(super) SpacedHostService);

impl<S> Layer<S> for HostAdmissionLayer {
    type Service = AdmittedHost<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AdmittedHost {
            inner,
            host: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct AdmittedHost<S> {
    inner: S,
    host: SpacedHostService,
}

impl<S, B, R> Service<http::Request<B>> for AdmittedHost<S>
where
    S: Service<http::Request<B>, Response = http::Response<R>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
    R: http_body::Body<Data = Bytes> + Send + 'static,
    R::Error: Into<StdError> + Send + 'static,
    S::Error: 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let path = request.uri().path();
        let customer = path.starts_with("/space.spaced.host.v1.Process/")
            || path.starts_with("/space.spaced.host.v1.Filesystem/")
            || path.starts_with("/space.spaced.host.v1.Networking/");
        let mut budget = pplx_space_common_grpc::GrpcRetryContext::none();
        if customer && let Some(timeout) = request.headers().get("grpc-timeout") {
            let mut headers = http::HeaderMap::new();
            headers.insert("grpc-timeout", timeout.clone());
            budget = budget
                .with_grpc_timeout_metadata(&tonic::metadata::MetadataMap::from_headers(headers));
        }
        let deadline = budget
            .remaining()
            .map(|left| tokio::time::Instant::now() + left);
        // Call the instance whose poll_ready completed, not its fresh clone.
        let replacement = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, replacement);
        let host = self.host.clone();
        Box::pin(async move {
            let guard = if customer {
                match host
                    .inner
                    .update
                    .admit_when_ready(deadline, &host.inner.shutdown)
                    .await
                {
                    Ok(guard) => Some(guard),
                    Err(status) => return Ok(status.into_http()),
                }
            } else {
                None
            };
            if customer && let Some(left) = budget.remaining() {
                // Tonic sees the remaining deadline, not a fresh copy of the
                // caller's original timeout after maintenance has consumed it.
                let mut remaining = tonic::Request::new(());
                remaining.set_timeout(left);
                let headers = remaining.into_parts().0.into_headers();
                if let Some(timeout) = headers.get("grpc-timeout") {
                    request
                        .headers_mut()
                        .insert("grpc-timeout", timeout.clone());
                }
            }
            inner.call(request).await.map(|response| {
                response.map(|body| {
                    Body::new(AdmittedBody {
                        body: Box::pin(body),
                        _guard: guard,
                    })
                })
            })
        })
    }
}

struct AdmittedBody<B> {
    body: Pin<Box<B>>,
    _guard: Option<super::update::DispatchGuard>,
}

impl<B: http_body::Body> http_body::Body for AdmittedBody<B> {
    type Data = B::Data;
    type Error = B::Error;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.get_mut().body.as_mut().poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}
