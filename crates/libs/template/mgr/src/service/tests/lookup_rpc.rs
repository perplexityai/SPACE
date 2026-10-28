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
#[case::cluster(Some(TemplateLookupScope::Cluster))]
#[case::forwarded(Some(TemplateLookupScope::Node("127.0.0.2".parse().unwrap())))]
#[case::sandbox(Some(TemplateLookupScope::Sandbox {
    id: pplx_space_common_core::types::SandboxId::new(), caller_node: None
}))]
#[tokio::test]
async fn cached_lookup_preserves_offline_behavior_during_rollout(
    #[case] scope: Option<TemplateLookupScope>,
    #[values(false, true)] enforced: bool,
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
    service = service.with_lookup_enforcement(enforced);
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
            environment: None,
        }),
        ..Default::default()
    });
    if let Some(scope) = scope {
        scope.attach(&mut request);
    }

    let response = client.get_template(request).await;

    if enforced && scope != Some(TemplateLookupScope::LocalExecution) {
        let expected = if scope.is_none() {
            tonic::Code::PermissionDenied
        } else {
            tonic::Code::Unavailable
        };
        assert_eq!(response.unwrap_err().code(), expected);
        return Ok(());
    }
    let response = response?;
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
        if enforced {
            scope.require_authorization(&response)?;
        } else {
            assert!(scope.require_authorization(&response).is_err());
        }
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
    let mut service = TemplateManagerService::new_with_template_root(conn, root.path())?
        .with_lookup_enforcement(true);
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

    TemplateLookupScope::LocalExecution.require_authorization(&response)?;
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

#[rstest::rstest]
#[case::legacy(None)]
#[case::local(Some("local"))]
#[case::cluster(Some("cluster"))]
#[case::node(Some("node:127.0.0.2"))]
#[case::sandbox(Some("sandbox:00000000-0000-0000-0000-000000000005"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_lookup_preserves_scope_at_catalog(
    #[case] scope: Option<&str>,
    #[values(false, true)] by_hash: bool,
) -> TestResult {
    use crate::volume::tests::catalog::{Catalog, TEMPLATE};
    use pplx_space_node_api_client::proto::TemplateVisibility;
    use pplx_space_test_env::enter_test_process;
    use std::ffi::OsStr;

    const CHILD: &str = "SPACE_TEST_COLD_LOOKUP_SCOPE";
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
        assert_eq!(
            *catalog.lookup_scopes.lock().unwrap(),
            vec![scope.map(str::parse::<TemplateLookupScope>).transpose()?],
        );
        return Ok(());
    }
    assert!(enter_test_process(&[]));
    let root = tempfile::tempdir()?;
    let mut service =
        TemplateManagerService::new_with_template_root(crate::db::test_conn(), root.path())?;
    service.peer_authorizer = UdsPeerAuthorizer::new(
        "cold-lookup-test",
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
    let channel = pplx_space_common_grpc::connect_uds_channel("cold-lookup-test", &socket).await?;
    let mut client = TemplateManagerClient::new(channel);
    let mut request = Request::new(GetTemplateRequest {
        template: Some(TemplateRef {
            name: TEMPLATE.into(),
            content_hash: by_hash.then(|| format!("sha256:{}", "a".repeat(64))),
            tag: None,
            environment: None,
        }),
        vcpu: Some(1),
        memory_mib: Some(512),
        ..Default::default()
    });
    if let Some(scope) = scope {
        scope.parse::<TemplateLookupScope>()?.attach(&mut request);
    }

    let status = client.get_template(request).await.unwrap_err();

    // The catalog has a release but no locations, so a cold pull reaches
    // materialization without a volume-manager or writes to the host cache.
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("no remote storage locations"),
        "{status}"
    );
    Ok(())
}
