use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use pplx_space_node_mgr_client::proto::{InspectSandboxRequest, InspectSandboxResponse};
use tokio_stream::wrappers::UnixListenerStream;

use pplx_space_node_mgr_client::{ConnectError, NodeManagerGrpcClient};
use tonic::{Request, Status};

pub(super) type Reply = Arc<dyn Fn(usize) -> Result<InspectSandboxResponse, Status> + Send + Sync>;

/// Node-manager stand-in answering only the execution inspect RPC.
pub(super) struct Receiver {
    calls: Arc<AtomicUsize>,
    pub(super) entered: Arc<tokio::sync::Notify>,
    server: tokio::task::JoinHandle<()>,
}

impl Receiver {
    pub(super) fn bind(path: &Path, reply: Reply) -> Self {
        Self::bind_async(path, move |call| std::future::ready(reply(call)))
    }

    pub(super) fn bind_async<F, Fut>(path: &Path, reply: F) -> Self
    where
        F: Fn(usize) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<InspectSandboxResponse, Status>> + Send + 'static,
    {
        let listener = tokio::net::UnixListener::bind(path).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let inspect = tower::service_fn({
            let calls = calls.clone();
            let entered = entered.clone();
            move |request: Request<InspectSandboxRequest>| {
                assert!(request.get_ref().execution_only);
                let call = calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                let reply = reply(call);
                async move { reply.await.map(tonic::Response::new) }
            }
        });
        let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
            let inspect = inspect.clone();
            async move {
                Ok::<_, std::convert::Infallible>(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(inspect, request)
                        .await,
                )
            }
        });
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(rpc, UnixListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            calls,
            entered,
            server,
        }
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.server.abort();
    }
}

pub(super) fn always(status: fn() -> Status) -> Reply {
    Arc::new(move |_| Err(status()))
}

pub(super) async fn dial(path: PathBuf) -> Result<NodeManagerGrpcClient, ConnectError> {
    let channel = pplx_space_common_grpc::connect_uds_channel("node-manager-test", &path).await?;
    Ok(NodeManagerGrpcClient::new(
        pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
    ))
}
