use super::*;
use pplx_space_template_mgr_client::proto::{
    GetCapabilitiesRequest, template_manager_client::TemplateManagerClient,
    template_manager_server::TemplateManagerServer,
};
use pplx_space_test_env::TestResult;
use tokio_stream::wrappers::UnixListenerStream;

struct CapabilitiesServer(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);

impl Drop for CapabilitiesServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Over the gRPC socket, node-ctl's account reads that this template-manager
/// reads a twin's own metadata: node-ctl reports it from this answer.
#[tokio::test]
async fn node_ctl_reads_that_twins_metadata_is_read() -> TestResult {
    let CachedBaseFixture {
        root: _root,
        mut service,
        ..
    } = cached_base_fixture();
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "capabilities-rpc-test",
        [UdsPeerAccount::with_fallback_uid(
            SPACE_NODECTL_ACCOUNT,
            nix::unistd::geteuid().as_raw(),
        )],
    );
    let socket_dir = tempfile::tempdir()?;
    let socket = socket_dir.path().join("manager.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let _server = CapabilitiesServer(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TemplateManagerServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    ));
    let channel = pplx_space_common_grpc::connect_uds_channel("capabilities-test", &socket).await?;
    let mut client = TemplateManagerClient::new(channel);
    let capabilities = client
        .get_capabilities(GetCapabilitiesRequest {})
        .await?
        .into_inner();
    assert!(capabilities.reads_twin_metadata);
    Ok(())
}
