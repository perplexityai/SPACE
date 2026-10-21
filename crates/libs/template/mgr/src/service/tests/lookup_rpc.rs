use super::*;
use pplx_space_common_svc::template_lookup::TemplateLookupScope;
use pplx_space_template_mgr_client::proto::{
    GetTemplateRequest, TemplateRef, template_manager_client::TemplateManagerClient,
    template_manager_server::TemplateManagerServer,
};
use pplx_space_test_env::TestResult;
use tokio_stream::wrappers::UnixListenerStream;

struct LookupServer(tokio::task::JoinHandle<Result<(), tonic::transport::Error>>);

impl Drop for LookupServer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[rstest::rstest]
#[case::legacy(None)]
#[case::local(Some(TemplateLookupScope::LocalExecution))]
#[case::forwarded(Some(TemplateLookupScope::Node("127.0.0.2".parse().unwrap())))]
#[case::sandbox(Some(TemplateLookupScope::Sandbox {
    id: pplx_space_common_core::types::SandboxId::new(), caller_node: None
}))]
#[tokio::test]
async fn cached_lookup_preserves_offline_behavior_during_rollout(
    #[case] scope: Option<TemplateLookupScope>,
) -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        ("SPACE_INTERNAL_API_ENDPOINT", None),
        ("SPACE_CLUSTER_NODE_API_ENDPOINT", None),
    ]) {
        return Ok(());
    }
    let CachedBaseFixture {
        root,
        mut service,
        hash_a,
        ..
    } = cached_base_fixture();
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "lookup-rpc-test",
        [UdsPeerAccount::with_fallback_uid(
            SPACE_SBXMGR_ACCOUNT,
            nix::unistd::geteuid().as_raw(),
        )],
    );
    let socket_dir = tempfile::tempdir()?;
    let socket = socket_dir.path().join("manager.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let _server = LookupServer(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TemplateManagerServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    ));
    let channel = pplx_space_common_grpc::connect_uds_channel("lookup-test", &socket).await?;
    let mut client = TemplateManagerClient::new(channel);
    let mut request = Request::new(GetTemplateRequest {
        template: Some(TemplateRef {
            name: "base".into(),
            content_hash: Some(hash_a.clone()),
            tag: None,
        }),
        ..Default::default()
    });
    if let Some(scope) = scope {
        scope.attach(&mut request);
    }

    let response = client.get_template(request).await?;

    assert_eq!(
        response.get_ref().template.as_ref().unwrap().content_hash,
        Some(hash_a.clone())
    );
    assert_eq!(
        response.get_ref().template_dir,
        root.path().join("base").join(&hash_a).to_string_lossy()
    );
    if let Some(scope) = scope {
        scope.observe_response(&response)?;
        assert!(
            response
                .metadata()
                .contains_key("x-space-template-lookup-scope-received")
        );
    } else {
        assert!(
            !response
                .metadata()
                .contains_key("x-space-template-lookup-scope-received")
        );
    }
    assert!(
        !response
            .metadata()
            .contains_key("x-space-template-lookup-scope")
    );
    Ok(())
}
