use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use http::{Request, Response};
use http_body::Body as HttpBody;
use pplx_space_node_api_client::NODE_API_DRAINING_MESSAGE;
use tonic::Status;
use tower::{Layer, Service};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Clone)]
pub struct NodeApiDrainLayer {
    drain_signal: pplx_space_common_svc::DrainSignal,
}

impl NodeApiDrainLayer {
    pub fn new(drain_signal: pplx_space_common_svc::DrainSignal) -> Self {
        Self { drain_signal }
    }
}

impl<S> Layer<S> for NodeApiDrainLayer {
    type Service = NodeApiDrainService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NodeApiDrainService {
            inner,
            drain_signal: self.drain_signal.clone(),
        }
    }
}

#[derive(Clone)]
pub struct NodeApiDrainService<S> {
    inner: S,
    drain_signal: pplx_space_common_svc::DrainSignal,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for NodeApiDrainService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
    ReqBody: Send + 'static,
    ResBody: HttpBody<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        if self.drain_signal.is_draining() && is_node_api_rpc_path(req.uri().path()) {
            return Box::pin(async {
                let error: BoxError = Box::new(Status::unavailable(NODE_API_DRAINING_MESSAGE));
                Err(error)
            });
        }

        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            let response = inner.call(req).await.map_err(Into::into)?;
            Ok(response.map(tonic::body::Body::new))
        })
    }
}

fn is_node_api_rpc_path(path: &str) -> bool {
    path.starts_with("/space.node.api.v1.")
}

#[cfg(test)]
mod tests;
