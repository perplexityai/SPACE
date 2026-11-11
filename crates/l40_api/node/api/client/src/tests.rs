use super::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::{Request, Response, Status};

#[test]
fn missing_health_capability_covers_unknown_and_legacy_health_servers() {
    assert!(health_capability_unsupported(tonic::Code::NotFound));
    assert!(health_capability_unsupported(tonic::Code::Unimplemented));
    assert!(!health_capability_unsupported(tonic::Code::Unavailable));
}

#[tokio::test]
async fn node_api_client_bounds_initial_connect_attempt_below_rpc_timeout() {
    let connect: NodeApiConnectFn = Box::new(|| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_secs(30)).await;
            panic!("connect attempt should time out before the future completes");
        })
    });
    let mut client = NodeApiClient::from_connect_fn("node-api", connect)
        .with_retry_policy(GrpcRetryPolicy::none());
    let started = std::time::Instant::now();

    let status = client
        .get_node_info(
            proto::GetNodeInfoRequest::default(),
            Duration::from_secs(30),
        )
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert!(
        status
            .message()
            .contains("connect attempt timed out after 1s"),
        "{status}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "connect attempt took {:?}",
        started.elapsed()
    );
}

#[test]
fn recognizes_node_api_draining_status() {
    let status = tonic::Status::unavailable(NODE_API_DRAINING_MESSAGE);

    let draining = is_node_api_draining_status(&status);

    assert!(draining);
}

#[test]
fn ignores_unavailable_without_node_api_draining_message() {
    let status = tonic::Status::unavailable("node-api restarting");

    let draining = is_node_api_draining_status(&status);

    assert!(!draining);
}

#[tokio::test]
async fn node_ctl_lazy_reconnects_after_retryable_rpc_status() {
    let (_temp_dir, socket_path) = test_socket_path();
    let calls = Arc::new(AtomicUsize::new(0));
    let connect_count = Arc::new(AtomicUsize::new(0));
    let server = spawn_node_ctl_server(
        &socket_path,
        TestNodeCtl::fail_first_mark_facet_dirty(calls.clone()),
    )
    .await;
    let mut client = node_ctl_test_client(socket_path.clone(), connect_count.clone());

    let response = client
        .mark_facet_dirty(Request::new(proto::MarkFacetDirtyRequest::default()))
        .await
        .unwrap();

    assert_eq!(response.into_inner(), proto::MarkFacetDirtyResponse {});
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(connect_count.load(Ordering::SeqCst), 2);
    stop_node_ctl_server(server).await;
}

#[tokio::test]
async fn node_ctl_lazy_allows_concurrent_requests() {
    let (_temp_dir, socket_path) = test_socket_path();
    let blocked_call = Arc::new(BlockedCall::default());
    let server = spawn_node_ctl_server(
        &socket_path,
        TestNodeCtl::block_mark_facet_dirty(blocked_call.clone()),
    )
    .await;
    let mut blocked_client = node_ctl_test_client(socket_path, Arc::new(AtomicUsize::new(0)));
    let mut concurrent_client = blocked_client.clone();

    let blocked_request = tokio::spawn(async move {
        blocked_client
            .mark_facet_dirty(Request::new(proto::MarkFacetDirtyRequest::default()))
            .await
    });
    tokio::time::timeout(Duration::from_secs(1), blocked_call.started.notified())
        .await
        .expect("blocked request did not reach node-ctl");
    let concurrent_response = tokio::time::timeout(
        Duration::from_secs(1),
        concurrent_client
            .get_controller_state(Request::new(proto::GetControllerStateRequest::default())),
    )
    .await;
    blocked_call.release.notify_one();
    blocked_request.await.unwrap().unwrap();

    assert!(
        concurrent_response.is_ok(),
        "independent request was serialized behind a blocked RPC"
    );
    concurrent_response.unwrap().unwrap();
    stop_node_ctl_server(server).await;
}

async fn spawn_node_ctl_server<T>(socket_path: &Path, service: T) -> tokio::task::JoinHandle<()>
where
    T: proto::node_ctl_server::NodeCtl,
{
    let listener = tokio::net::UnixListener::bind(socket_path).unwrap();
    let incoming = UnixListenerStream::new(listener);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(proto::node_ctl_server::NodeCtlServer::new(service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    })
}

async fn stop_node_ctl_server(server: tokio::task::JoinHandle<()>) {
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}

fn node_ctl_test_client(
    socket_path: PathBuf,
    connect_count: Arc<AtomicUsize>,
) -> NodeCtlLazyClient {
    NodeCtlLazyClient::new(Box::new(move || {
        let socket_path = socket_path.clone();
        let connect_count = connect_count.clone();
        Box::pin(async move {
            connect_count.fetch_add(1, Ordering::SeqCst);
            let endpoint = configure_node_ctl_endpoint(
                Endpoint::try_from(LOCAL_ENDPOINT_URI).expect("static endpoint URI is valid"),
            );
            let channel = endpoint
                .connect_with_connector(tower::service_fn(move |_| {
                    let socket_path = socket_path.clone();
                    async move {
                        let stream = tokio::net::UnixStream::connect(&socket_path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }))
                .await?;
            Ok(proto::node_ctl_client::NodeCtlClient::new(
                wrap_grpc_client_channel(channel),
            ))
        })
    }))
}

fn test_socket_path() -> (tempfile::TempDir, PathBuf) {
    let temp_dir = tempfile::Builder::new()
        .prefix("nctl-")
        .tempdir_in("/tmp")
        .unwrap();
    let socket_path = temp_dir.path().join("node-ctl.sock");
    (temp_dir, socket_path)
}

#[derive(Clone)]
struct TestNodeCtl {
    mark_facet_dirty: MarkFacetDirtyBehavior,
}

impl TestNodeCtl {
    fn fail_first_mark_facet_dirty(calls: Arc<AtomicUsize>) -> Self {
        Self {
            mark_facet_dirty: MarkFacetDirtyBehavior::FailFirst(calls),
        }
    }

    fn block_mark_facet_dirty(blocked_call: Arc<BlockedCall>) -> Self {
        Self {
            mark_facet_dirty: MarkFacetDirtyBehavior::Block(blocked_call),
        }
    }
}

#[derive(Clone)]
enum MarkFacetDirtyBehavior {
    FailFirst(Arc<AtomicUsize>),
    Block(Arc<BlockedCall>),
}

#[derive(Default)]
struct BlockedCall {
    started: Notify,
    release: Notify,
}

#[tonic::async_trait]
impl proto::node_ctl_server::NodeCtl for TestNodeCtl {
    async fn get_reconciliation_status(
        &self,
        _request: Request<proto::GetReconciliationStatusRequest>,
    ) -> Result<Response<proto::GetReconciliationStatusResponse>, Status> {
        Ok(Response::new(
            proto::GetReconciliationStatusResponse::default(),
        ))
    }

    async fn get_controller_state(
        &self,
        _request: Request<proto::GetControllerStateRequest>,
    ) -> Result<Response<proto::GetControllerStateResponse>, Status> {
        Ok(Response::new(proto::GetControllerStateResponse::default()))
    }

    async fn refresh_facet_state(
        &self,
        _request: Request<proto::RefreshFacetStateRequest>,
    ) -> Result<Response<proto::RefreshFacetStateResponse>, Status> {
        Ok(Response::new(proto::RefreshFacetStateResponse::default()))
    }

    async fn trigger_state_reconciliation(
        &self,
        _request: Request<proto::TriggerStateReconciliationRequest>,
    ) -> Result<Response<proto::TriggerStateReconciliationResponse>, Status> {
        Ok(Response::new(
            proto::TriggerStateReconciliationResponse::default(),
        ))
    }

    async fn mark_facet_dirty(
        &self,
        _request: Request<proto::MarkFacetDirtyRequest>,
    ) -> Result<Response<proto::MarkFacetDirtyResponse>, Status> {
        match &self.mark_facet_dirty {
            MarkFacetDirtyBehavior::FailFirst(calls)
                if calls.fetch_add(1, Ordering::SeqCst) == 0 =>
            {
                return Err(Status::unavailable("stale transport"));
            }
            MarkFacetDirtyBehavior::Block(blocked_call) => {
                blocked_call.started.notify_one();
                blocked_call.release.notified().await;
            }
            MarkFacetDirtyBehavior::FailFirst(_) => {}
        }
        Ok(Response::new(proto::MarkFacetDirtyResponse::default()))
    }

    async fn set_facet_sync_disabled(
        &self,
        _request: Request<proto::SetFacetSyncDisabledRequest>,
    ) -> Result<Response<proto::SetFacetSyncDisabledResponse>, Status> {
        Ok(Response::new(proto::SetFacetSyncDisabledResponse::default()))
    }
}
