use super::*;
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
fn public_worker_api_rejects_wake_start_context() {
    let start_context = Some(pplx_space_sbx_mgr_client::proto::WakeStartContext::default());
    let error = reject_public_wake_start_context("WakeSandbox", &start_context).unwrap_err();

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

/// Method names passed to each `invocation` in `source`: the first string
/// literal after it, which rustfmt may move to a later line.
fn admitted_methods<'a>(source: &'a str, invocation: &str) -> std::collections::BTreeSet<&'a str> {
    source
        .split(invocation)
        .skip(1)
        .map(|rest| rest.split('"').nth(1).expect("method literal"))
        .collect()
}

// The control reserve exists so these calls stay available while streams and
// long unary calls hold the shared permits; a call routed there by accident
// would take that guarantee away.
#[test]
fn only_process_and_sandbox_control_rpcs_take_the_control_budget() {
    let source = include_str!("../worker.rs");
    assert_eq!(
        source.matches("SandboxBudget::Control").count(),
        1,
        "only fwd_sbx_control! may request the control budget"
    );
    let control = admitted_methods(source, "fwd_sbx_control!(");
    assert_eq!(
        control,
        [
            "KillSandbox",
            "SpacedKillProcess",
            "SpacedListProcesses",
            "SpacedSendSignal",
        ]
        .into_iter()
        .collect()
    );

    let mut shared = admitted_methods(source, "fwd_sbx!(");
    shared.extend(admitted_methods(source, "fwd_sbx_stream!("));
    shared.extend(admitted_methods(source, "limited_sbx_operation("));
    assert!(shared.contains("SpacedWaitReady") && shared.contains("SpacedStartProcess"));
    assert!(
        control.is_disjoint(&shared),
        "{:?}",
        control.intersection(&shared)
    );
}
