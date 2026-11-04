use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use tonic::body::Body;
use tonic::codegen::{Bytes, StdError, http};
use tower::{Layer, Service};

use super::{ALLOW_ROOT_OR_SBXMGR, SpacedHostService, Status};

#[derive(Clone)]
pub(super) struct HostAuthorizationLayer(pub(super) SpacedHostService);

impl<S> Layer<S> for HostAuthorizationLayer {
    type Service = AuthorizedHost<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthorizedHost {
            inner,
            host: self.0.clone(),
        }
    }
}

#[derive(Clone)]
pub(super) struct AuthorizedHost<S> {
    inner: S,
    host: SpacedHostService,
}

impl SpacedHostService {
    fn authorize_http_request<B>(&self, request: &http::Request<B>) -> Result<(), Status> {
        #[cfg(any(test, feature = "test-support"))]
        if self.inner.host_peer_authorization == super::SpacedHostPeerAuthorization::Disabled {
            return Ok(());
        }
        let credentials = request
            .extensions()
            .get::<tonic::transport::server::UdsConnectInfo>()
            .and_then(|info| info.peer_cred)
            .ok_or_else(|| Status::unauthenticated("UDS peer credentials missing"))?;
        let identity = pplx_space_common_svc::PeerIdentity::new(
            credentials.uid(),
            credentials.gid(),
            credentials.pid(),
        );
        self.inner
            .host_peer_authorizer
            .authorize_identity(identity, request.uri().path(), ALLOW_ROOT_OR_SBXMGR)
            .map(|_| ())
    }
}

impl<S, B, R> Service<http::Request<B>> for AuthorizedHost<S>
where
    S: Service<http::Request<B>, Response = http::Response<R>>,
    S::Future: Send + 'static,
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

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        if let Err(status) = self.host.authorize_http_request(&request) {
            return Box::pin(async move { Ok(status.into_http()) });
        }
        let response = self.inner.call(request);
        Box::pin(async move { response.await.map(|response| response.map(Body::new)) })
    }
}

#[cfg(test)]
mod tests;
