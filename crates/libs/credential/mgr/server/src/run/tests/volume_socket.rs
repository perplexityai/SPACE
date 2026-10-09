use super::*;
use std::convert::Infallible;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Mutex;

use hyper_util::rt::TokioIo;
use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use pplx_space_common_svc::core::host::linux::identities::users;
use pplx_space_credmgr_client::{CredentialManagerClient, CredmgrBearerInterceptor};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Code, Request, Status};

use crate::node_ca::NodeCa;
use crate::node_service::NodeCredentialManager;
use crate::proto::{
    ExchangeTokenRequest, ExchangeTokenResponse, IssueSandboxArtifactDekRequest,
    UnwrapSandboxArtifactDekRequest,
};

/// Re-execution changes only the child's identity. User namespaces let an
/// unprivileged Linux developer exercise the same kernel credentials as CI.
#[test]
fn real_peers_obey_volume_listener_policy() {
    const CHILD_UID: &str = "SPACE_TEST_VOLUME_SOCKET_UID";
    if let Ok(uid) = std::env::var(CHILD_UID) {
        let uid: u32 = uid.parse().unwrap();
        assert_eq!(nix::unistd::geteuid().as_raw(), uid);
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(exercise_peer(uid));
        println!("verified kernel uid={uid}");
        return;
    }
    let name = "run::tests::volume_socket::real_peers_obey_volume_listener_policy";
    for uid in [
        users::SPACE_VOLMGR.uid,
        users::SPACE_SBXMGR.uid,
        users::SPACE_TMPLMGR.uid,
        0,
        65534,
    ] {
        let exe = std::env::current_exe().unwrap();
        let mut command = if nix::unistd::geteuid().is_root() {
            let mut command = Command::new(&exe);
            command.uid(uid).gid(uid);
            command
        } else {
            let mut command = Command::new("unshare");
            command.args([
                "--user",
                &format!("--map-user={uid}"),
                &format!("--map-group={uid}"),
                "--",
            ]);
            command.arg(&exe);
            command
        };
        command.args(["--exact", name, "--nocapture"]);
        command.env(CHILD_UID, uid.to_string());
        let (_, output) = pplx_space_test_env::run_command(&mut command, Duration::from_secs(30))
            .expect("real-peer tests require root or Linux user namespaces (unshare)");
        assert!(
            output.status.success(),
            "uid={uid}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains(&format!("verified kernel uid={uid}")),
            "child must execute its assertions"
        );
    }
}

async fn uds_channel(path: PathBuf) -> Channel {
    Endpoint::from_static("http://localhost")
        .connect_with_connector(tower::service_fn(move |_| {
            let path = path.clone();
            async move {
                tokio::net::UnixStream::connect(path)
                    .await
                    .map(TokioIo::new)
            }
        }))
        .await
        .unwrap()
}

async fn uds_client(path: PathBuf) -> tonic::client::Grpc<Channel> {
    tonic::client::Grpc::new(uds_channel(path).await)
}

async fn call(client: &mut tonic::client::Grpc<Channel>, method: &str) -> Code {
    // Use valid scopes so allowed DEK calls exercise forwarding, rather than
    // stopping at node-local malformed-input validation.
    match method {
        "IssueSandboxArtifactDek" => {
            call_request(
                client,
                method,
                IssueSandboxArtifactDekRequest {
                    sandbox_id: "01980000-0000-7000-8000-000000000001".into(),
                    ..Default::default()
                },
            )
            .await
        }
        "UnwrapSandboxArtifactDek" => {
            call_request(
                client,
                method,
                UnwrapSandboxArtifactDekRequest {
                    sandbox_id: "01980000-0000-7000-8000-000000000001".into(),
                    ..Default::default()
                },
            )
            .await
        }
        _ => call_request(client, method, ExchangeTokenRequest::default()).await,
    }
}

async fn call_request<T: prost::Message + Default + Send + Sync + 'static>(
    client: &mut tonic::client::Grpc<Channel>,
    method: &str,
    message: T,
) -> Code {
    client.ready().await.unwrap();
    let path = format!("/space.credential.manager.v1.CredentialManager/{method}")
        .parse::<http::uri::PathAndQuery>()
        .unwrap();
    let result: Result<tonic::Response<ExchangeTokenResponse>, Status> = client
        .unary(
            Request::new(message),
            path,
            tonic_prost::ProstCodec::default(),
        )
        .await;
    result.unwrap_err().code()
}

async fn exercise_peer(uid: u32) {
    let dir = tempdir().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_endpoint = Endpoint::from_shared(format!(
        "http://{}",
        upstream_listener.local_addr().unwrap()
    ))
    .unwrap();
    let shutdown = CancellationToken::new();
    let upstream_shutdown = shutdown.clone();
    let upstream_server = tokio::spawn(async move {
        Server::builder()
            .serve_with_incoming_shutdown(
                tower::service_fn(move |request: http::Request<tonic::body::Body>| {
                    recorded
                        .lock()
                        .unwrap()
                        .push(request.uri().path().to_string());
                    async {
                        Ok::<_, Infallible>(
                            Status::aborted("recorded upstream call")
                                .into_http::<tonic::body::Body>(),
                        )
                    }
                }),
                TcpListenerStream::new(upstream_listener),
                upstream_shutdown.cancelled_owned(),
            )
            .await
            .unwrap();
    });
    let upstream = CredentialManagerClient::new(wrap_grpc_client_channel(InterceptedService::new(
        upstream_endpoint.connect().await.unwrap(),
        CredmgrBearerInterceptor::None,
    )));
    let service = NodeCredentialManager::new(
        upstream.clone(),
        Arc::new(NodeCa::new(upstream).unwrap()),
        PeerProcessCheckMode::Off,
        shutdown.clone(),
    );
    let standard_path = dir.path().join("standard.sock");
    let volume_path = dir.path().join("volume.sock");
    let listeners = CredentialListeners {
        standard: tokio::net::UnixListener::bind(&standard_path).unwrap(),
        volume: Some(tokio::net::UnixListener::bind(&volume_path).unwrap()),
    };
    let mut servers = JoinSet::new();
    listeners.spawn(&mut servers, service, &shutdown);
    let mut volume = uds_client(volume_path).await;
    let mut forwarded = 0;
    for (method, transfer) in [
        ("ExchangeToken", true),
        ("IssueSandboxArtifactDek", true),
        ("UnwrapSandboxArtifactDek", true),
        ("IssueIntermediateCertificate", false),
        ("Encrypt", false),
        ("Decrypt", false),
        ("GetOrIssueSandboxVolumeDek", false),
        ("SandboxVolumeEncryptionEnabled", false),
        ("ResolveNetworkCredentials", false),
        ("FutureMethod", false),
    ] {
        let allowed = uid == users::SPACE_VOLMGR.uid && transfer;
        assert_eq!(
            call(&mut volume, method).await,
            if allowed {
                Code::Aborted
            } else {
                Code::PermissionDenied
            },
            "volume listener: uid={uid}, method={method}"
        );
        forwarded += usize::from(allowed);
        assert_eq!(
            calls.lock().unwrap().len(),
            forwarded,
            "denials must not reach the authority"
        );
    }
    let malformed_code = if uid == users::SPACE_VOLMGR.uid {
        Code::InvalidArgument
    } else {
        Code::PermissionDenied
    };
    assert_eq!(
        call_request(
            &mut volume,
            "IssueSandboxArtifactDek",
            IssueSandboxArtifactDekRequest::default()
        )
        .await,
        malformed_code
    );
    assert_eq!(
        call_request(
            &mut volume,
            "UnwrapSandboxArtifactDek",
            UnwrapSandboxArtifactDekRequest::default()
        )
        .await,
        malformed_code
    );
    assert_eq!(
        calls.lock().unwrap().len(),
        forwarded,
        "malformed probes must not contact the failing authority"
    );
    let mut standard = uds_client(standard_path).await;
    if uid == users::SPACE_VOLMGR.uid {
        for method in [
            "ExchangeToken",
            "IssueSandboxArtifactDek",
            "UnwrapSandboxArtifactDek",
            "IssueIntermediateCertificate",
        ] {
            assert_eq!(
                call(&mut standard, method).await,
                Code::PermissionDenied,
                "standard listener: {method}"
            );
        }
        assert_eq!(calls.lock().unwrap().len(), forwarded);
    } else if uid == users::SPACE_SBXMGR.uid {
        for method in [
            "ExchangeToken",
            "IssueSandboxArtifactDek",
            "UnwrapSandboxArtifactDek",
        ] {
            assert_eq!(
                call(&mut standard, method).await,
                Code::Aborted,
                "legacy volume peer: {method}"
            );
        }
        assert_eq!(
            call(&mut standard, "IssueIntermediateCertificate").await,
            Code::InvalidArgument,
            "sandbox-manager must still reach CSR validation"
        );
    } else if uid == users::SPACE_TMPLMGR.uid {
        assert_eq!(
            call(&mut standard, "ExchangeToken").await,
            Code::Aborted,
            "template-manager must retain standard-listener storage access"
        );
        assert_eq!(calls.lock().unwrap().len(), forwarded + 1);
        for method in ["IssueSandboxArtifactDek", "UnwrapSandboxArtifactDek"] {
            assert_eq!(
                call(&mut standard, method).await,
                Code::PermissionDenied,
                "template-manager must not reach artifact keys: {method}"
            );
        }
        assert_eq!(calls.lock().unwrap().len(), forwarded + 1);
    }
    drop((standard, volume));
    shutdown.cancel();
    while let Some(result) = servers.join_next().await {
        result.unwrap().1.unwrap();
    }
    upstream_server.await.unwrap();
    if uid == users::SPACE_VOLMGR.uid || uid == users::SPACE_SBXMGR.uid {
        authority_admits_artifact_dek_peer(uid).await;
    }
}

/// The authority's own listeners admit the volume peer to the artifact-DEK
/// handlers. This authority has no database, so reaching the handler surfaces
/// its FAILED_PRECONDITION; a listener or allow-list denial would be
/// PERMISSION_DENIED. Key semantics are covered by the service tests.
async fn authority_admits_artifact_dek_peer(uid: u32) {
    let dir = tempdir().unwrap();
    let standard_path = dir.path().join("standard.sock");
    let volume_path = dir.path().join("volume.sock");
    let listeners = CredentialListeners {
        standard: tokio::net::UnixListener::bind(&standard_path).unwrap(),
        volume: Some(tokio::net::UnixListener::bind(&volume_path).unwrap()),
    };
    let shutdown = CancellationToken::new();
    let mut servers = JoinSet::new();
    listeners.spawn(
        &mut servers,
        CredentialManagerService::new(
            Arc::new(InMemoryKeyWrapper::new()),
            PeerProcessCheckMode::Off,
        ),
        &shutdown,
    );
    let path = if uid == users::SPACE_VOLMGR.uid {
        volume_path
    } else {
        standard_path
    };
    let mut client = CredentialManagerClient::new(uds_channel(path).await);
    let sandbox_id = uuid::Uuid::now_v7().to_string();
    let issue = client
        .issue_sandbox_artifact_dek(IssueSandboxArtifactDekRequest {
            sandbox_id: sandbox_id.clone(),
            expected_org_id: String::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(issue.code(), Code::FailedPrecondition, "{issue:?}");
    let unwrap = client
        .unwrap_sandbox_artifact_dek(UnwrapSandboxArtifactDekRequest {
            sandbox_id,
            expected_org_id: String::new(),
            wrapped_dek: Vec::new(),
        })
        .await
        .unwrap_err();
    assert_eq!(unwrap.code(), Code::FailedPrecondition, "{unwrap:?}");
    drop(client);
    shutdown.cancel();
    while let Some(result) = servers.join_next().await {
        result.unwrap().1.unwrap();
    }
}
