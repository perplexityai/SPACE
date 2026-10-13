use pplx_space_build_mgr_client::proto::build_manager_server::BuildManagerServer;
use pplx_space_common_svc::auth::build::BuildAuthLayer;
use pplx_space_common_svc::auth::test_support::{
    TokenReviewStatus, kubeconfig_for_endpoint, projected_token, service_account_status,
    token_review_service,
};
use rstest::{fixture, rstest};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::{CancellationToken, DropGuard};

use super::*;

#[rstest]
#[case::tls(true)]
#[case::plaintext_development(false)]
fn authenticated_rpc_enqueues_only_authorized_owners(
    #[case] tls: bool,
    #[future] authenticated_rpc: TestResult<AuthenticatedRpc>,
) -> TestResult {
    let env = [
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            if tls { "false" } else { "true" },
        ),
        ("SPACE_K8S_TOKEN_AUTH_ENABLED", "true"),
        ("SPACE_K8S_TOKEN_AUTH_STATIC_FALLBACK_ENABLED", "true"),
        ("KUBERNETES_SERVICE_HOST", "127.0.0.1"),
        ("SPACE_K8S_TOKEN_AUTH_NAMESPACE", "space"),
        (
            "SPACE_BUILD_MANAGER_AUTH_ALLOWED_SERVICE_ACCOUNTS",
            "tenant,system",
        ),
        ("SPACE_BUILD_AUTH_SYSTEM_SERVICE_ACCOUNTS", "system"),
        ("SPACE_BUILD_AUTH_TENANT_SERVICE_ACCOUNTS", "tenant,system"),
    ];
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let directory = tempfile::tempdir()?;
    let identity = rcgen::generate_simple_self_signed(vec!["builder.example".into()])?;
    let cert = directory.path().join("server.pem");
    let key = directory.path().join("server.key");
    std::fs::write(&cert, identity.cert.pem())?;
    std::fs::write(&key, identity.signing_key.serialize_pem())?;
    let kubeconfig = directory.path().join("kubeconfig");
    let token_path = directory.path().join("build-manager.token");
    let mut vars: Vec<_> = env
        .iter()
        .map(|(key, value)| (*key, Some(std::ffi::OsStr::new(value))))
        .collect();
    vars.extend([
        (
            "SPACE_INTERNAL_RPC_TLS_CERT",
            tls.then_some(cert.as_os_str()),
        ),
        ("SPACE_INTERNAL_RPC_TLS_KEY", tls.then_some(key.as_os_str())),
        ("SPACE_INTERNAL_RPC_TLS_CA", tls.then_some(cert.as_os_str())),
        (
            "SPACE_BUILD_MANAGER_TLS_SERVER_NAME",
            Some(std::ffi::OsStr::new("builder.example")),
        ),
        ("KUBECONFIG", Some(kubeconfig.as_os_str())),
        (
            "SPACE_BUILD_MANAGER_TOKEN_PATH",
            Some(token_path.as_os_str()),
        ),
    ]);
    if !pplx_space_test_env::enter_test_process(&vars) {
        return Ok(());
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                run_authenticated_rpc(authenticated_rpc.await?).await
            })
            .await??;
            Ok(())
        })
}

struct ProjectedTokens {
    tenant: String,
    system: String,
    worker: String,
}

fn projected_tokens() -> ProjectedTokens {
    ProjectedTokens {
        tenant: projected_token("tenant"),
        system: projected_token("system"),
        worker: projected_token("worker"),
    }
}

struct TokenReviewer {
    tempdir: TempDir,
    tokens: ProjectedTokens,
    shutdown: CancellationToken,
    _shutdown_guard: DropGuard,
    task: JoinHandle<std::io::Result<()>>,
}

async fn token_reviewer() -> TestResult<TokenReviewer> {
    let projected_tokens = projected_tokens();
    let tempdir = tempfile::tempdir()?;
    let identities = [
        (projected_tokens.tenant.clone(), "tenant"),
        (projected_tokens.system.clone(), "system"),
        (projected_tokens.worker.clone(), "worker"),
    ];
    let review_app = token_review_service(move |request| {
        assert_eq!(
            request.audiences,
            Some(vec!["space-build-manager".to_string()])
        );
        let account = identities
            .iter()
            .find(|(token, _)| request.token.as_deref() == Some(token.as_str()));
        match account {
            Some((_, name)) => service_account_status(
                "space-build-manager",
                &format!("system:serviceaccount:space:{name}"),
            ),
            None => TokenReviewStatus {
                authenticated: Some(false),
                ..Default::default()
            },
        }
    });
    let review_listener = TcpListener::bind("127.0.0.1:0").await?;
    let kubeconfig = std::path::PathBuf::from(
        std::env::var_os("KUBECONFIG").expect("test child kubeconfig path"),
    );
    std::fs::write(
        &kubeconfig,
        kubeconfig_for_endpoint(review_listener.local_addr()?),
    )?;
    let shutdown = CancellationToken::new();
    let _shutdown_guard = shutdown.clone().drop_guard();
    let review_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        axum::serve(review_listener, review_app)
            .with_graceful_shutdown(review_shutdown.cancelled_owned())
            .await
    });
    Ok(TokenReviewer {
        tempdir,
        tokens: projected_tokens,
        shutdown,
        _shutdown_guard,
        task,
    })
}

struct AuthenticatedRpc {
    reviewer: TokenReviewer,
    backend: Arc<MockBuildBackend>,
    service: BuildManagerDaemonService,
    token_path: std::path::PathBuf,
    client: pplx_space_build_mgr_client::BuildManagerGrpcClient,
    legacy_client: pplx_space_build_mgr_client::BuildManagerGrpcClient,
    server: JoinHandle<Result<(), tonic::transport::Error>>,
}

#[fixture]
async fn authenticated_rpc() -> TestResult<AuthenticatedRpc> {
    let reviewer = token_reviewer().await?;
    let layer = BuildAuthLayer::for_build_manager("shared-token".to_string()).await?;
    let backend = Arc::new(MockBuildBackend::default());
    let service = BuildManagerDaemonService::with_backend(1, backend.clone())?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = pplx_space_common_grpc::internal_transport::qualify_endpoint(
        &listener.local_addr()?.to_string(),
    );
    let server_shutdown = reviewer.shutdown.clone();
    let server_service = service.clone();
    let server_builder = pplx_space_common_grpc::internal_transport::server_builder()?;
    let server = tokio::spawn(async move {
        server_builder
            .layer(layer)
            .add_service(BuildManagerServer::new(server_service))
            .serve_with_incoming_shutdown(
                pplx_space_common_grpc::internal_transport::Incoming::new(
                    TcpListenerStream::new(listener),
                    server_shutdown.clone(),
                ),
                server_shutdown.cancelled_owned(),
            )
            .await
    });
    let token_path = std::path::PathBuf::from(
        std::env::var_os("SPACE_BUILD_MANAGER_TOKEN_PATH").expect("test child token path"),
    );
    let channel = pplx_space_common_grpc::internal_transport::client_connect_endpoint(
        pplx_space_common_grpc::internal_transport::Service::BuildManager,
        tonic::transport::Endpoint::from_shared(endpoint.clone())?,
    )
    .await?
    .connect_lazy();
    let client = pplx_space_build_mgr_client::test_client_from_env(channel)?;
    let legacy_client =
        pplx_space_build_mgr_client::connect_with_token(endpoint, "shared-token").await?;
    Ok(AuthenticatedRpc {
        reviewer,
        backend,
        service,
        token_path,
        client,
        legacy_client,
        server,
    })
}

async fn run_authenticated_rpc(fixture: AuthenticatedRpc) -> TestResult {
    let AuthenticatedRpc {
        reviewer,
        backend,
        service,
        token_path,
        mut client,
        mut legacy_client,
        server,
    } = fixture;
    let ProjectedTokens {
        tenant,
        system,
        worker,
    } = &reviewer.tokens;
    let org = "018f0000-0000-7000-8000-000000000002";
    let grant = SystemPublicationGrantId::new().to_string();
    for (rotation, (token, owner, proof, expected)) in [
        (
            Some(worker.as_str()),
            Some(org),
            None,
            Err(Code::PermissionDenied),
        ),
        (
            Some(tenant.as_str()),
            None,
            None,
            Err(Code::PermissionDenied),
        ),
        (None, Some(org), None, Err(Code::Unauthenticated)),
        (None, None, None, Err(Code::Unauthenticated)),
        (
            Some(system.as_str()),
            None,
            Some("invalid-grant"),
            Err(Code::InvalidArgument),
        ),
        (
            Some(tenant.as_str()),
            Some(org),
            Some(grant.as_str()),
            Err(Code::InvalidArgument),
        ),
        (Some(tenant.as_str()), Some(org), None, Ok(())),
        (Some(system.as_str()), None, None, Ok(())),
        (Some(system.as_str()), None, Some(grant.as_str()), Ok(())),
    ]
    .into_iter()
    .enumerate()
    {
        if let Some(token) = token {
            let next_token = reviewer.tempdir.path().join("next-token");
            std::fs::write(&next_token, token)?;
            // Filesystems may assign the same mtime to rapid token rotations.
            std::fs::File::open(&next_token)?.set_modified(
                std::time::SystemTime::UNIX_EPOCH
                    + Duration::from_secs(u64::try_from(rotation)? + 1),
            )?;
            std::fs::rename(next_token, &token_path)?;
        }
        let hash = format!("sha256:{}", "b".repeat(64));
        if expected.is_ok() {
            backend
                .build_results
                .lock()
                .unwrap()
                .push_back(Ok(BuildOutcome {
                    content_hash: Some(hash.clone()),
                }));
        }
        let mut request = start_build_request(
            Some(test_recipe()),
            "rpc-template",
            vec!["latest".to_string()],
        );
        request.org_id = owner.map(str::to_string);
        request.system_publication_grant_id = proof.map(str::to_string);

        let result = match token {
            Some(_) => client.start_build(request).await,
            None => legacy_client.start_build(request).await,
        };

        match expected {
            Err(code) => {
                assert_eq!(result.unwrap_err().code(), code);
                assert!(service.worker.list_jobs(None, None, 1)?.is_empty());
                assert!(backend.build_calls.lock().unwrap().is_empty());
                assert!(backend.publish_calls.lock().unwrap().is_empty());
            }
            Ok(()) => {
                let job_id = result?.into_inner().job_id;
                let job = service
                    .worker
                    .get_job(&job_id.parse().unwrap())?
                    .expect("queued build");
                let expected_owner = owner.map(TemplateOrg::parse).transpose()?;
                let expected_publication = expected_owner.map_or(
                    TemplatePublication::System(proof.map(str::parse).transpose()?),
                    TemplatePublication::Tenant,
                );
                assert_eq!(
                    job.payload_as::<BuildJobPayload>()?.template_org,
                    expected_owner
                );
                let status = wait_for_terminal_status(&service, &job_id).await?;
                assert_eq!(BuildState::try_from(status.state)?, BuildState::Succeeded);
                assert_eq!(
                    backend.cluster_template_calls.lock().unwrap().last(),
                    Some(&("rpc-template".to_string(), expected_publication))
                );
                assert_eq!(
                    backend.publish_calls.lock().unwrap().last(),
                    Some(&(
                        "rpc-template".to_string(),
                        hash,
                        vec!["latest".to_string()],
                        expected_publication
                    ))
                );
            }
        }
    }
    drop(client);
    drop(legacy_client);
    reviewer.shutdown.cancel();
    server.await??;
    reviewer.task.await??;
    Ok(())
}
