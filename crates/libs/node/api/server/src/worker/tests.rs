use super::*;
use pplx_space_sbx_mgr_client::proto;
#[test]
fn wait_ready_policy_requires_authenticated_authority_and_lifecycle_purpose() {
    for (authority, purpose, expected) in [
        (
            Some(WaitReadyLifecycleAuthority::Granted),
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::TrustedLifecycle,
        ),
        (
            Some(WaitReadyLifecycleAuthority::Granted),
            WaitReadyCallPurpose::PublicBridge,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
        (
            Some(WaitReadyLifecycleAuthority::Denied),
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
        (
            None,
            WaitReadyCallPurpose::LifecycleProbe,
            SpaceApiBridgeCallerPolicy::RouteTokenRequired,
        ),
    ] {
        assert_eq!(classify_wait_ready_policy(authority, purpose), expected);
    }
}

#[test]
fn wait_ready_forward_replaces_caller_supplied_policy() {
    let mut request = pplx_space_common_svc::space_api_route::wait_ready_lifecycle_request(());
    set_space_api_bridge_caller_policy(
        request.metadata_mut(),
        SpaceApiBridgeCallerPolicy::TrustedLifecycle,
    );

    prepare_wait_ready_forward(&mut request);

    assert_eq!(
        pplx_space_common_svc::space_api_route::space_api_bridge_caller_policy(request.metadata())
            .unwrap(),
        Some(SpaceApiBridgeCallerPolicy::RouteTokenRequired)
    );
    assert_eq!(
        wait_ready_call_purpose(request.metadata()),
        WaitReadyCallPurpose::PublicBridge
    );
}

#[test]
fn public_worker_api_rejects_explicit_netns_path() {
    let netns_path = Some("/run/netns/not-public".to_string());
    let error = reject_public_netns_path("StartSandbox", &netns_path).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("internal-only"));
}

#[test]
fn public_worker_api_rejects_explicit_suspend_restore_storage_uri() {
    let storage_uri = Some("s3://space/snapshot?region=us-east-1".to_string());
    let error =
        reject_public_suspend_restore_context("StartSandbox", &storage_uri, &None).unwrap_err();

    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("internal-only"));
}

#[test]
fn public_worker_api_rejects_explicit_suspend_restore_source_node() {
    let source_node = Some("space-worker-a".to_string());
    assert!(reject_public_suspend_restore_context("StartSandbox", &None, &source_node).is_err());
}

#[test]
fn public_worker_api_accepts_absent_netns_path() {
    reject_public_netns_path("StartSandbox", &None).unwrap();
}

#[test]
fn public_worker_api_accepts_absent_suspend_restore_context() {
    reject_public_suspend_restore_context("StartSandbox", &None, &None).unwrap();
}

#[test]
fn public_worker_api_never_accepts_an_explicit_operation_owner() {
    for operation in [
        None,
        Some(String::new()),
        Some(uuid::Uuid::now_v7().to_string()),
    ] {
        let result = reject_public_operation_activation_attempt(&operation);
        match operation {
            None => assert!(result.is_ok()),
            Some(_) => assert_eq!(result.unwrap_err().code(), tonic::Code::InvalidArgument),
        }
    }
}

#[tokio::test]
async fn service_port_ingress_first_frame_wait_is_bounded() {
    let error = read_service_port_ingress_first_frame(
        std::future::pending::<Result<Option<ServicePortIngressFrame>, Status>>(),
        Duration::from_millis(1),
    )
    .await
    .expect_err("idle service-port ingress stream should time out");

    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    assert!(error.message().contains("timed out"));
}

#[tokio::test]
async fn service_port_ingress_guarded_response_holds_permit_until_stream_dropped() {
    let limiter = UdsPerSandboxLimiter::service_port_ingress_for_tests(1, 1);
    let sandbox_id = SandboxId::new().to_string();
    let permit = limiter
        .acquire("ServicePortIngress", &sandbox_id)
        .await
        .expect("first ingress stream acquires permit");
    let response_stream = tokio_stream::iter([Err::<ServicePortIngressFrame, Status>(
        Status::deadline_exceeded("service port ingress stream idle timed out"),
    )]);
    let mut guarded = guard_stream(response_stream, permit);

    let rejected_while_guarded = limiter.acquire("ServicePortIngress", &sandbox_id).await;
    let Err(error) = rejected_while_guarded else {
        panic!("guarded stream should hold permit");
    };
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let error = guarded
        .next()
        .await
        .expect("guarded stream yields idle timeout")
        .expect_err("idle timeout propagates through guarded stream");
    assert_eq!(error.code(), tonic::Code::DeadlineExceeded);
    drop(guarded);

    let _permit = limiter
        .acquire("ServicePortIngress", &sandbox_id)
        .await
        .expect("dropping closed guarded stream releases permit");
}

#[test]
fn policy_mutation_requires_control_plane_authority() {
    // Only a caller node-api classified as control-plane (Granted) may reach the
    // sbx-mgr policy-mutation RPCs. A data-plane token (Denied) is refused; an
    // unauthenticated request (auth layer off, local dev) forwards as before.
    check_policy_mutation_authority(Some(NetworkPolicyMutationAuthority::Granted))
        .expect("granted caller may mutate policy");
    check_policy_mutation_authority(None).expect("no auth layer forwards as before");
    let denied = check_policy_mutation_authority(Some(NetworkPolicyMutationAuthority::Denied))
        .expect_err("denied caller is refused");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
}

/// A sandbox-manager that answers every call with `Unimplemented`, so a call
/// node-api forwarded is told apart from one it refused at admission.
async fn unimplemented_sandbox_manager() -> crate::clients::BackendClients {
    let backend = axum::Router::new().fallback(|| async {
        http::Response::builder()
            .header("content-type", "application/grpc")
            .header(
                "grpc-status",
                (tonic::Code::Unimplemented as i32).to_string(),
            )
            .body(axum::body::Body::empty())
            .unwrap()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, backend).await.unwrap() });
    let channel = tonic::transport::Endpoint::from_shared(endpoint)
        .unwrap()
        .connect()
        .await
        .unwrap();
    crate::clients::BackendClients::with_channel(channel)
}

async fn service_with_limiter(limiter: UdsPerSandboxLimiter) -> WorkerApiService {
    WorkerApiService {
        clients: unimplemented_sandbox_manager().await,
        uds_per_sandbox_limiter: limiter,
        service_port_ingress_limiter: shared_service_port_ingress_limiter(),
        resource_status_reader: NodeResourceStatusReader::new(),
        shutdown_token: CancellationToken::new(),
    }
}

#[tokio::test]
async fn process_control_is_admitted_while_streams_hold_every_shared_permit() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8);
    let service = service_with_limiter(limiter.clone()).await;
    let sandbox_id = SandboxId::new().to_string();
    let _stream = limiter
        .acquire("SpacedConnectProcess", &sandbox_id)
        .await
        .unwrap();

    let list = service
        .spaced_list_processes(Request::new(proto::SpacedListProcessesRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    let signal = service
        .spaced_send_signal(Request::new(proto::SpacedSendSignalRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    let kill = service
        .spaced_kill_process(Request::new(proto::SpacedKillProcessRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    for (method, code) in [
        ("SpacedListProcesses", list.unwrap_err().code()),
        ("SpacedSendSignal", signal.unwrap_err().code()),
        ("SpacedKillProcess", kill.unwrap_err().code()),
    ] {
        assert_eq!(
            code,
            tonic::Code::Unimplemented,
            "{method} was not forwarded"
        );
    }

    let stat = service
        .spaced_stat(Request::new(proto::SpacedStatRequest {
            sandbox_id,
            ..Default::default()
        }))
        .await;
    assert_eq!(stat.unwrap_err().code(), tonic::Code::ResourceExhausted);
}

// Sandbox kill stops the VM from the host and never waits on the guest, so it
// must not queue behind process calls a hung guest holds.
#[tokio::test]
async fn sandbox_kill_is_admitted_while_every_sandbox_permit_is_held() {
    let limiter = UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8);
    let service = service_with_limiter(limiter.clone()).await;
    let sandbox_id = SandboxId::new().to_string();
    let _held = [
        limiter
            .acquire("SpacedConnectProcess", &sandbox_id)
            .await
            .unwrap(),
        limiter
            .acquire_in(SandboxBudget::Reserved, "SpacedListProcesses", &sandbox_id)
            .await
            .unwrap(),
    ];

    let process_kill = service
        .spaced_kill_process(Request::new(proto::SpacedKillProcessRequest {
            sandbox_id: sandbox_id.clone(),
            ..Default::default()
        }))
        .await;
    assert_eq!(
        process_kill.unwrap_err().code(),
        tonic::Code::ResourceExhausted
    );

    let sandbox_kill = service
        .kill_sandbox(Request::new(proto::KillRequest {
            sandbox_id,
            signal: 9,
            ..Default::default()
        }))
        .await;
    assert_eq!(sandbox_kill.unwrap_err().code(), tonic::Code::Unimplemented);
}

/// Sandbox-manager authorizes an upgrade by its caller, which is node-api, so
/// node-api's internal Wake forwards one only for the control plane.
#[tokio::test]
async fn the_internal_wake_forwards_a_runtime_upgrade_only_for_the_control_plane() {
    use pplx_space_node_api_client::proto::worker_api_internal_server::WorkerApiInternal;
    let service =
        service_with_limiter(UdsPerSandboxLimiter::for_tests_with_reserved(1, 1, 8)).await;
    let wake = || {
        Request::new(proto::WakeRequest {
            sandbox_id: SandboxId::new().to_string(),
            blocking: true,
            runtime_upgrade: Some(proto::WakeRuntimeUpgrade::default()),
            ..Default::default()
        })
    };

    let mut unprivileged = wake();
    unprivileged.extensions_mut().insert(
        pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test(),
    );
    let denied = WorkerApiInternal::wake_sandbox(&service, unprivileged)
        .await
        .unwrap_err();
    assert_eq!(denied.code(), tonic::Code::PermissionDenied, "{denied}");

    // The fake sandbox-manager answers whatever reaches it with Unimplemented.
    let forwarded = WorkerApiInternal::wake_sandbox(&service, wake())
        .await
        .unwrap_err();
    assert_eq!(forwarded.code(), tonic::Code::Unimplemented, "{forwarded}");
}
