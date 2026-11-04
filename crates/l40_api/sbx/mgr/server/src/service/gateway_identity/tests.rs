use super::*;

pub(crate) struct GatewayNode {
    _directory: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
    pub(crate) authorized: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl GatewayNode {
    pub(crate) fn attach(service: &mut super::super::SandboxManagerService) -> Self {
        use pplx_space_node_mgr_client::proto::{
            ValidateGatewayCallerRequest, ValidateGatewayCallerResponse,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("node.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        let authorized = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let authorization = authorized.clone();
        let method = tower::service_fn(
            move |request: tonic::Request<ValidateGatewayCallerRequest>| {
                let authorized = authorization.clone();
                async move {
                    if !authorized.load(std::sync::atomic::Ordering::SeqCst)
                        || request.get_ref().launch_capability != vec![b'a'; 64]
                    {
                        return Err(Status::failed_precondition(
                            "gateway launch is no longer current",
                        ));
                    }
                    Ok(tonic::Response::new(ValidateGatewayCallerResponse {}))
                }
            },
        );
        let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
            let method = method.clone();
            async move {
                Ok::<_, std::convert::Infallible>(
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(method, request)
                        .await,
                )
            }
        });
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .serve_with_incoming(
                    rpc,
                    tokio_stream::wrappers::UnixListenerStream::new(listener),
                )
                .await
                .unwrap();
        });
        let channel = pplx_space_common_grpc::connect_uds_channel_lazy(
            &path,
            tonic::transport::Endpoint::from_static("http://gateway-node-test"),
        );
        service.gateway_caller_client =
            pplx_space_node_mgr_client::proto::node_manager_client::NodeManagerClient::new(
                pplx_space_common_svc::bootstrap::wrap_grpc_client_channel(channel),
            );
        Self {
            _directory: directory,
            task,
            authorized,
        }
    }
}

impl Drop for GatewayNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn launches_require_isolated_gateway_identity() {
    // Slot mapping and range boundaries are covered in common-core's gateway tests.
    assert_eq!(launch_uid(10_042).unwrap(), 2_000_042);
    assert_eq!(
        launch_uid(501).unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
}
