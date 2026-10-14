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
#[case::local(TemplateLookupScope::LocalExecution, true)]
#[case::cluster(TemplateLookupScope::Cluster, false)]
#[case::node(TemplateLookupScope::Node("127.0.0.2".parse().unwrap()), false)]
#[case::sandbox(TemplateLookupScope::Sandbox {
    id: pplx_space_common_core::types::SandboxId::new(), caller_node: None
}, false)]
#[tokio::test]
async fn cached_lookup_over_uds_requires_catalog_unless_execution_is_local(
    #[case] scope: TemplateLookupScope,
    #[case] allowed_offline: bool,
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
    // Authenticate the real socket peer as the sandbox-manager test principal.
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
    scope.attach(&mut request);

    let response = client.get_template(request).await;

    if allowed_offline {
        let response = response?;
        scope.require_acknowledgement(&response)?;
        assert_eq!(
            response.get_ref().template.as_ref().unwrap().content_hash,
            Some(hash_a.clone())
        );
        assert_eq!(
            response.get_ref().template_dir,
            root.path().join("base").join(&hash_a).to_string_lossy()
        );
    } else {
        assert_eq!(response.unwrap_err().code(), tonic::Code::Unavailable);
    }
    Ok(())
}

#[rstest::rstest]
#[case::default_tag("")]
#[case::explicit_tag(":stable")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_tagged_base_resolves_through_catalog(#[case] suffix: &str) -> TestResult {
    use crate::volume::tests::catalog::{Catalog, TEMPLATE};
    use pplx_space_node_api_client::proto::TemplateVisibility;
    use pplx_space_test_env::enter_test_process;
    use std::ffi::OsStr;

    const CHILD: &str = "SPACE_TEST_LOCAL_TAGGED_BASE";
    if std::env::var_os(CHILD).is_none() {
        let catalog = Catalog::start(TemplateVisibility::Generic).await?;
        assert!(!enter_test_process(&[
            (CHILD, Some(OsStr::new("1"))),
            (
                "SPACE_INTERNAL_API_ENDPOINT",
                Some(OsStr::new(&catalog.endpoint))
            ),
            (
                "SPACE_CLUSTER_API_TOKEN_PATH",
                Some(catalog.token_file.path().as_os_str())
            ),
            (
                "SPACE_CLUSTER_API_ENDPOINT_DISCOVERY",
                Some(OsStr::new("off"))
            ),
            ("SPACE_INTERNAL_RPC_INSECURE_DEV", Some(OsStr::new("true"))),
        ]));
        return Ok(());
    }
    assert!(enter_test_process(&[]));
    let root = tempfile::tempdir()?;
    let conn = crate::db::test_conn();
    let digest = format!("sha256:{}", "a".repeat(64));
    insert_ready_build(&conn, TEMPLATE, &digest);
    let mut service = TemplateManagerService::new_with_template_root(conn, root.path())?;
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "local-build-test",
        [UdsPeerAccount::with_fallback_uid(
            SPACE_SBXMGR_ACCOUNT,
            nix::unistd::geteuid().as_raw(),
        )],
    );
    let socket = root.path().join("manager.sock");
    let listener = tokio::net::UnixListener::bind(&socket)?;
    let _server = LookupServer(tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(TemplateManagerServer::new(service))
            .serve_with_incoming(UnixListenerStream::new(listener)),
    ));
    let channel = pplx_space_common_grpc::connect_uds_channel("local-build-test", &socket).await?;
    let mut client = TemplateManagerClient::new(channel);
    let reference: pplx_space_common_core::types::TemplateReference =
        format!("{TEMPLATE}{suffix}").parse()?;
    let mut request = Request::new(GetTemplateRequest {
        template: Some(TemplateRef::try_from(&reference)?),
        ..Default::default()
    });
    TemplateLookupScope::LocalExecution.attach(&mut request);

    let response = client.get_template(request).await?;

    TemplateLookupScope::LocalExecution.require_acknowledgement(&response)?;
    assert_eq!(
        response
            .get_ref()
            .template
            .as_ref()
            .unwrap()
            .content_hash
            .as_deref(),
        Some(digest.as_str())
    );
    assert_eq!(response.get_ref().authorized_org_id, None);
    Ok(())
}
