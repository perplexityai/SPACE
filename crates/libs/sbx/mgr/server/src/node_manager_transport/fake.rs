//! An in-process node-manager for tests.
//!
//! The server listens on a socket in a test-owned temporary directory and
//! answers only the RPCs a test scripts. Every other call is refused with
//! `Unimplemented`, so code that regresses into an unexpected node-manager
//! call stops at the fake instead of reaching the host's node-manager.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::{ConnectError, NodeManagerConnector, NodeManagerGrpcClient, NodeManagerTransport};

const SERVICE_PATH: &str = "/space.node.v1.NodeManager/";

type Route = Arc<
    dyn Fn(
            http::Request<tonic::body::Body>,
        ) -> BoxFuture<'static, http::Response<tonic::body::Body>>
        + Send
        + Sync,
>;

/// The RPCs a [`FakeNodeManager`] answers, keyed by method name.
#[derive(Default)]
pub(crate) struct FakeNodeManager {
    routes: HashMap<&'static str, Route>,
}

impl FakeNodeManager {
    /// Answer `method` (for example `"StopProcess"`) with `handler`.
    pub(crate) fn unary<Req, Resp, F>(mut self, method: &'static str, handler: F) -> Self
    where
        Req: prost::Message + Default + Send + 'static,
        Resp: prost::Message + Send + 'static,
        F: Fn(Req) -> Result<Resp, Status> + Send + Sync + 'static,
    {
        let handler = Arc::new(handler);
        let route: Route = Arc::new(move |request| {
            let handler = Arc::clone(&handler);
            Box::pin(async move {
                tonic::server::Grpc::new(tonic_prost::ProstCodec::<Resp, Req>::default())
                    .unary(Unary(handler, PhantomData), request)
                    .await
            })
        });
        self.routes.insert(method, route);
        self
    }

    /// Start serving. The server stops when the returned handle drops.
    pub(crate) fn serve(self) -> ServedNodeManager {
        let sockets = tempfile::tempdir().unwrap();
        let socket = sockets.path().join("node-manager.sock");
        let listener = tokio::net::UnixListener::bind(&socket).unwrap();
        let routes = Arc::new(self.routes);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let recorded = Arc::clone(&calls);
        let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
            let method = request
                .uri()
                .path()
                .strip_prefix(SERVICE_PATH)
                .unwrap_or(request.uri().path())
                .to_owned();
            recorded.lock().unwrap().push(method.clone());
            let route = routes.get(method.as_str()).cloned();
            async move {
                Ok::<_, std::convert::Infallible>(match route {
                    Some(route) => route(request).await,
                    None => {
                        Status::unimplemented(format!("fake node-manager does not answer {method}"))
                            .into_http()
                    }
                })
            }
        });
        let stop = CancellationToken::new();
        tokio::spawn(
            tonic::transport::Server::builder().serve_with_incoming_shutdown(
                rpc,
                tokio_stream::wrappers::UnixListenerStream::new(listener),
                stop.clone().cancelled_owned(),
            ),
        );
        ServedNodeManager {
            connector: fixed_client(lazy_client(&socket)),
            calls,
            stop,
            _sockets: sockets,
        }
    }
}

/// A running [`FakeNodeManager`].
pub(crate) struct ServedNodeManager {
    connector: NodeManagerConnector,
    calls: Arc<Mutex<Vec<String>>>,
    stop: CancellationToken,
    _sockets: tempfile::TempDir,
}

impl ServedNodeManager {
    /// A connector whose clients all reach this server.
    pub(crate) fn connector(&self) -> NodeManagerConnector {
        self.connector.clone()
    }

    /// Method names of every call received, answered or refused, in arrival
    /// order.
    pub(crate) fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl Drop for ServedNodeManager {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

/// A connector whose clients are all `client`, for tests that already serve a
/// node-manager of their own.
pub(crate) fn fixed_client(client: NodeManagerGrpcClient) -> NodeManagerConnector {
    NodeManagerConnector::new(Arc::new(InProcess(client)))
}

/// A connector whose clients all dial `socket`, a test-owned path nothing
/// binds, so every node-manager call fails before reaching any server. As on a
/// node whose node-manager is down, `connect` and `connect_once` fail to dial
/// and a lazy client's calls fail with `Unavailable`. `connect` dials once
/// rather than waiting out the host connector's retry budget.
pub(crate) fn unbound(socket: &Path) -> NodeManagerConnector {
    NodeManagerConnector::new(Arc::new(Unbound(socket.to_owned())))
}

/// [`unbound`] on a socket in a fresh directory that nothing creates, for
/// tests that build a subsystem context rather than a service.
pub(crate) fn unbound_in_absent_dir() -> NodeManagerConnector {
    unbound(
        &std::env::temp_dir()
            .join(format!("space-sbx-mgr-test-{}", uuid::Uuid::now_v7()))
            .join("absent-node-manager.sock"),
    )
}

fn lazy_client(socket: &Path) -> NodeManagerGrpcClient {
    let channel = pplx_space_common_grpc::connect_uds_channel_lazy(socket, endpoint());
    client(channel)
}

fn endpoint() -> tonic::transport::Endpoint {
    tonic::transport::Endpoint::from_static("http://[::]:50051")
}

fn client(channel: tonic::transport::Channel) -> NodeManagerGrpcClient {
    NodeManagerGrpcClient::new(pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(
        channel,
    ))
}

struct Unbound(PathBuf);

impl Unbound {
    async fn dial(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        let channel = pplx_space_common_grpc::connect_uds_channel_with_endpoint(
            "node-manager",
            &self.0,
            endpoint(),
        )
        .await?;
        Ok(client(channel))
    }
}

#[async_trait::async_trait]
impl NodeManagerTransport for Unbound {
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.dial().await
    }

    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        self.dial().await
    }

    fn lazy(&self) -> NodeManagerGrpcClient {
        lazy_client(&self.0)
    }
}

struct InProcess(NodeManagerGrpcClient);

#[async_trait::async_trait]
impl NodeManagerTransport for InProcess {
    async fn connect(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        Ok(self.0.clone())
    }

    async fn connect_once(&self) -> Result<NodeManagerGrpcClient, ConnectError> {
        Ok(self.0.clone())
    }

    fn lazy(&self) -> NodeManagerGrpcClient {
        self.0.clone()
    }
}

struct Unary<F, Resp>(Arc<F>, PhantomData<fn() -> Resp>);

impl<Req, Resp, F> tonic::server::UnaryService<Req> for Unary<F, Resp>
where
    F: Fn(Req) -> Result<Resp, Status>,
{
    type Response = Resp;
    type Future = std::future::Ready<Result<tonic::Response<Resp>, Status>>;

    fn call(&mut self, request: tonic::Request<Req>) -> Self::Future {
        std::future::ready((self.0)(request.into_inner()).map(tonic::Response::new))
    }
}
