use std::convert::Infallible;
use std::future::Future;

use pplx_space_credmgr_client::proto::{
    ResolveNetworkCredentialsRequest as NetworkResolveRequest,
    ResolveNetworkCredentialsResponse as NetworkResolveResponse,
};
use pplx_space_spaced_common::proto::host_control::{
    GetNetworkPolicyRequest, host_control_client::HostControlClient,
    host_control_server::HostControlServer,
};
use tokio::sync::Notify;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Code;
use tonic::transport::server::UdsConnectInfo;

use super::*;
use crate::auto_lifecycle_delete_receipts::RouteIdentityProvenance;
use crate::service::tests::service_tests::insert_test_sandbox;

const GENERATION: i64 = 23;

#[derive(Default)]
struct Agent {
    network_requests: Mutex<Vec<NetworkResolveRequest>>,
    entered: Notify,
    resume: Notify,
}

const NETWORK_REFERENCE_ID: &str = "018f1e9d-7b7a-7d3a-8000-000000000001";

fn resolved_network() -> NetworkResolveResponse {
    NetworkResolveResponse {
        effective_network_policy_ref: String::new(),
        credentials: vec![ResolvedNetworkCredential {
            credential_reference_id: NETWORK_REFERENCE_ID.into(),
            value: b"Bearer host-control-test-canary".to_vec(),
        }],
    }
}

fn credential_policy() -> NetworkPolicyV1 {
    serde_json::from_value(serde_json::json!({
        "version": "v1",
        "egress": {
            "default_action": "deny",
            "allow": [{"domain": "api.example.com"}]
        },
        "tls_interception": "on",
        "network_credential_rules": [{
            "rule_id": "network-credential-test",
            "destination": {
                "exact_host": "api.example.com",
                "path_prefix": "/v1"
            },
            "credential_header_bindings": [{
                "header_name": "authorization",
                "credential_reference_id": NETWORK_REFERENCE_ID
            }],
            "supported_http_methods": ["GET", "POST"]
        }]
    }))
    .unwrap()
}

fn install_policy(
    service: &SandboxManagerService,
    sandbox_id: SandboxId,
    policy: &NetworkPolicyV1,
) {
    let conn = service.db_conn().unwrap();
    crate::db::set_network_policy_fields(
        &conn,
        &sandbox_id.to_string(),
        Some(policy),
        1,
        crate::db::PolicyAuthority::Cluster,
    )
    .unwrap();
}

fn policy_request() -> Request<GetNetworkPolicyRequest> {
    Request::new(GetNetworkPolicyRequest {})
}

fn set_generation(conn: &crate::db::Connection, sandbox_id: SandboxId, generation: Option<i64>) {
    crate::db::update_space_api_route_identity(
        conn,
        sandbox_id,
        &SpaceApiRouteIdentity {
            execution_generation: generation.map(|value| ExecutionGeneration::new(value).unwrap()),
            ..Default::default()
        },
        RouteIdentityProvenance::Unfenced,
    )
    .unwrap();
}

async fn with_agent<F, Fut>(reply: Result<NetworkResolveResponse, Status>, pending: bool, test: F)
where
    F: FnOnce(
        SandboxManagerService,
        SandboxId,
        UdsConnectInfo,
        Arc<Agent>,
        HostControlClient<tonic::transport::Channel>,
        super::super::gateway_identity::tests::GatewayNode,
    ) -> Fut,
    Fut: Future<Output = ()>,
{
    with_agent_gate(reply, pending, true, test).await;
}

async fn with_agent_gate<F, Fut>(
    reply: Result<NetworkResolveResponse, Status>,
    pending: bool,
    network_credentials_enabled: bool,
    test: F,
) where
    F: FnOnce(
        SandboxManagerService,
        SandboxId,
        UdsConnectInfo,
        Arc<Agent>,
        HostControlClient<tonic::transport::Channel>,
        super::super::gateway_identity::tests::GatewayNode,
    ) -> Fut,
    Fut: Future<Output = ()>,
{
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("credmgr.sock");
    let listener = tokio::net::UnixListener::bind(&socket).unwrap();
    let agent = Arc::new(Agent::default());
    let recorded = agent.clone();
    let resolve_network = tower::service_fn(move |request: Request<NetworkResolveRequest>| {
        let agent = recorded.clone();
        let reply = reply.clone();
        async move {
            let request = request.into_inner();
            agent.network_requests.lock().unwrap().push(request.clone());
            agent.entered.notify_one();
            if pending {
                agent.resume.notified().await;
            }
            reply.map(|mut response| {
                response.effective_network_policy_ref =
                    request.effective_network_policy_ref.clone();
                Response::new(response)
            })
        }
    });
    let rpc = tower::service_fn(move |request: http::Request<tonic::body::Body>| {
        let resolve_network = resolve_network.clone();
        async move {
            let response = match request.uri().path() {
                "/space.credential.manager.v1.CredentialManager/ResolveNetworkCredentials" => {
                    tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                        .unary(resolve_network, request)
                        .await
                }
                path => panic!("unexpected credential-manager RPC: {path}"),
            };
            Ok::<_, Infallible>(response)
        }
    });
    let server = tonic::transport::Server::builder()
        .serve_with_incoming(rpc, UnixListenerStream::new(listener));

    let (peer, _other) = tokio::net::UnixStream::pair().unwrap();
    let credentials = peer.peer_cred().unwrap();
    let peer = UdsConnectInfo {
        peer_addr: None,
        peer_cred: Some(credentials),
    };
    let conn = crate::db::test_conn();
    let sandbox_id = SandboxId::new();
    insert_test_sandbox(&conn, &sandbox_id.to_string(), SandboxState::Started);
    set_generation(&conn, sandbox_id, Some(GENERATION));
    crate::db::update_netgtw_pid(
        &conn,
        &sandbox_id.to_string(),
        credentials.uid(),
        u32::try_from(credentials.pid().unwrap()).unwrap(),
    )
    .unwrap();
    let mut service = SandboxManagerService::new_with_options(
        conn,
        SandboxManagerServiceOptions {
            credential_manager_socket_path: socket,
            ..SandboxManagerServiceOptions::for_test()
        },
    );
    service.network_credentials_enabled = network_credentials_enabled;
    let gateway = super::super::gateway_identity::tests::GatewayNode::attach(&mut service);
    // These RPC tests do not need background health/snapshot tasks. Drain them
    // before DB contention can block their synchronous lookups on Tokio workers.
    service.shutdown_token.cancel();
    service.task_tracker.close();
    service.task_tracker.wait().await;
    service.shutdown_token = CancellationToken::new();
    let host_socket = directory.path().join("host-control.sock");
    let host_listener = tokio::net::UnixListener::bind(&host_socket).unwrap();
    let host_server = pplx_space_common_svc::grpc_server_builder()
        .add_service(HostControlServer::new(service.clone()))
        .serve_with_incoming(UnixListenerStream::new(host_listener));
    let exercise = async {
        let channel =
            pplx_space_common_grpc::connect_uds_channel("host-control-test", &host_socket)
                .await
                .unwrap();
        test(
            service,
            sandbox_id,
            peer,
            agent,
            HostControlClient::new(channel),
            gateway,
        )
        .await;
    };
    // The listener and pending RPC futures live only as long as this exercise.
    tokio::select! {
        result = server => panic!("test credential agent stopped: {result:?}"),
        result = host_server => panic!("test HostControl server stopped: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(60), exercise) => {
            result.expect("HostControl exercise timed out");
        }
    }
}

#[tokio::test]
async fn network_policy_deadline_covers_authorization_lookup_under_contention() {
    with_agent(
        Ok(resolved_network()),
        false,
        async move |service, sandbox_id, _, agent, mut client, _gateway| {
            install_policy(&service, sandbox_id, &credential_policy());
            let mut request = policy_request();
            request.set_timeout(Duration::from_millis(250));
            let (locked_tx, locked_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
            let db = service.db.clone();
            let holder = tokio::task::spawn_blocking(move || {
                let _guard = db.lock().unwrap();
                let _ = locked_tx.send(());
                // The OS-thread watchdog also releases it if the Tokio timer driver blocks.
                release_rx.recv_timeout(Duration::from_secs(3))
            });
            locked_rx.await.unwrap();
            let result =
                tokio::time::timeout(Duration::from_secs(2), client.get_network_policy(request))
                    .await;
            drop(release_tx);
            assert!(
                matches!(
                    holder.await.unwrap(),
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
                ),
                "the Tokio worker blocked until the OS watchdog released the DB"
            );
            assert_eq!(
                result
                    .expect("deadline must fire while the DB is locked")
                    .unwrap_err()
                    .code(),
                Code::Cancelled
            );
            assert!(agent.network_requests.lock().unwrap().is_empty());
        },
    )
    .await;
}

#[tokio::test]
async fn network_policy_without_credential_rules_never_calls_the_authority() {
    with_agent_gate(
        Ok(resolved_network()),
        false,
        false,
        async |service, sandbox_id, _, agent, mut client, _gateway| {
            install_policy(&service, sandbox_id, &NetworkPolicyV1::deny_all());
            let response = client
                .get_network_policy(policy_request())
                .await
                .unwrap()
                .into_inner();
            assert!(response.network_credentials.is_empty());
            assert!(agent.network_requests.lock().unwrap().is_empty());
        },
    )
    .await;
}

#[tokio::test]
async fn disabled_network_credentials_return_policy_without_material() {
    with_agent_gate(
        Ok(resolved_network()),
        false,
        false,
        async move |service, sandbox_id, _, agent, mut client, _gateway| {
            install_policy(&service, sandbox_id, &credential_policy());
            let response = client
                .get_network_policy(policy_request())
                .await
                .unwrap()
                .into_inner();
            assert!(response.effective_policy.is_some());
            assert!(!response.effective_policy_ref.is_empty());
            assert!(response.network_credentials.is_empty());
            assert!(agent.network_requests.lock().unwrap().is_empty());
        },
    )
    .await;
}

#[tokio::test]
async fn network_policy_and_resolved_credentials_return_in_one_response() {
    with_agent(
        Ok(resolved_network()),
        false,
        async |service, sandbox_id, _, agent, mut client, _gateway| {
            install_policy(&service, sandbox_id, &credential_policy());
            let response = client
                .get_network_policy(policy_request())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.network_credentials.len(), 1);
            assert_eq!(
                response.network_credentials[0].credential_reference_id,
                NETWORK_REFERENCE_ID
            );
            let requests = agent.network_requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].sandbox_id, sandbox_id.to_string());
            assert_eq!(requests[0].execution_generation, GENERATION);
            assert_eq!(
                requests[0].effective_network_policy_ref,
                response.effective_policy_ref
            );
        },
    )
    .await;
}

#[tokio::test]
async fn network_policy_resolution_can_outlive_the_local_retry_window() {
    with_agent(
        Ok(resolved_network()),
        true,
        async |service, sandbox_id, _, agent, mut client, _gateway| {
            install_policy(&service, sandbox_id, &credential_policy());
            let call = client.get_network_policy(policy_request());
            tokio::pin!(call);
            tokio::select! {
                _ = agent.entered.notified() => {},
                result = &mut call => panic!("resolution returned before the authority gate: {result:?}"),
            }

            tokio::time::pause();
            tokio::time::advance(DEFAULT_LOCAL_GRPC_RETRY_POLICY.max_elapsed + Duration::from_millis(1)).await;
            agent.resume.notify_one();
            let response = call.await.unwrap().into_inner();
            tokio::time::resume();

            assert_eq!(response.network_credentials.len(), 1);
            assert_eq!(agent.network_requests.lock().unwrap().len(), 1);
        },
    )
    .await;
}

#[tokio::test]
async fn network_policy_survives_credential_resolution_failure() {
    for code in [Code::Unavailable, Code::PermissionDenied] {
        with_agent(
            Err(Status::new(code, "credential resolution failed")),
            false,
            async move |service, sandbox_id, _, agent, mut client, _gateway| {
                install_policy(&service, sandbox_id, &credential_policy());
                let response = client
                    .get_network_policy(policy_request())
                    .await
                    .unwrap()
                    .into_inner();
                assert!(response.effective_policy.is_some());
                assert!(!response.effective_policy_ref.is_empty());
                assert!(response.network_credentials.is_empty());
                let attempts = agent.network_requests.lock().unwrap().len();
                if code == Code::Unavailable {
                    assert!(attempts > 1, "transient failures should be retried");
                } else {
                    assert_eq!(attempts, 1, "permanent failures should not be retried");
                }
            },
        )
        .await;
    }
}

#[tokio::test]
async fn policy_or_execution_change_during_resolution_keeps_the_captured_snapshot() {
    #[derive(Clone, Copy)]
    enum Change {
        Policy,
        Execution,
    }

    for change in [Change::Policy, Change::Execution] {
        with_agent(
            Ok(resolved_network()),
            true,
            async move |service, sandbox_id, _, agent, mut client, _gateway| {
                install_policy(&service, sandbox_id, &credential_policy());
                let response = {
                    let call = client.get_network_policy(policy_request());
                    tokio::pin!(call);
                    tokio::select! {
                        _ = agent.entered.notified() => {},
                        result = &mut call => panic!("resolution returned before state mutation: {result:?}"),
                    }
                    assert_eq!(
                        service.active_state_transition_count(),
                        0,
                        "provider I/O must not hold a state-writer lease"
                    );
                    match change {
                        Change::Policy => {
                            install_policy(&service, sandbox_id, &NetworkPolicyV1::deny_all())
                        }
                        Change::Execution => set_generation(
                            &service.db_conn().unwrap(),
                            sandbox_id,
                            Some(GENERATION + 1),
                        ),
                    }
                    agent.resume.notify_one();
                    call.await.unwrap().into_inner()
                };
                assert_eq!(response.network_credentials.len(), 1);
                assert_eq!(
                    response.effective_policy_ref,
                    agent.network_requests.lock().unwrap()[0].effective_network_policy_ref
                );
                match change {
                    Change::Policy => {
                        let refreshed = client
                            .get_network_policy(policy_request())
                            .await
                            .unwrap()
                            .into_inner();
                        assert!(refreshed.network_credentials.is_empty());
                        assert_ne!(refreshed.effective_policy_ref, response.effective_policy_ref);
                        assert_eq!(agent.network_requests.lock().unwrap().len(), 1);
                    }
                    Change::Execution => {
                        agent.resume.notify_one();
                        let refreshed = client
                            .get_network_policy(policy_request())
                            .await
                            .unwrap()
                            .into_inner();
                        assert_eq!(refreshed.network_credentials.len(), 1);
                        let requests = agent.network_requests.lock().unwrap();
                        assert_eq!(requests.len(), 2);
                        assert_eq!(requests[1].execution_generation, GENERATION + 1);
                    }
                }
            },
        )
        .await;
    }
}

#[tokio::test]
async fn network_policy_resolution_honors_caller_and_service_deadlines() {
    for caller_timeout in [Some(Duration::from_millis(250)), None] {
        with_agent(
            Ok(resolved_network()),
            true,
            async move |service, sandbox_id, _, agent, mut client, _gateway| {
                install_policy(&service, sandbox_id, &credential_policy());
                let mut request = policy_request();
                if let Some(timeout) = caller_timeout {
                    request.set_timeout(timeout);
                }
                let call = client.get_network_policy(request);
                tokio::pin!(call);
                tokio::select! {
                    _ = agent.entered.notified() => {},
                    result = &mut call => panic!("request should reach the agent: {result:?}"),
                }
                if caller_timeout.is_none() {
                    tokio::time::pause();
                    tokio::time::advance(CREDENTIAL_INJECTION_TIMEOUT + Duration::from_secs(1))
                        .await;
                    tokio::time::resume();
                }
                assert_eq!(
                    call.await.unwrap_err().code(),
                    if caller_timeout.is_some() {
                        Code::Cancelled
                    } else {
                        Code::DeadlineExceeded
                    }
                );
            },
        )
        .await;
    }
}
