use super::*;

#[test]
fn ingress_dnat_namespace_policy_rejects_root_namespace() {
    let err = validate_ingress_dnat_namespace(
        None,
        "tcp",
        9012,
        crate::slot_pool::ServicePortIngressIsolation::SharedHost,
    )
    .expect_err("root namespace ingress DNAT must be rejected");

    assert!(err.contains("tcp/9012"));
    validate_ingress_dnat_namespace(
        Some("pod-netns"),
        "tcp",
        9012,
        crate::slot_pool::ServicePortIngressIsolation::SharedHost,
    )
    .unwrap();
}

#[test]
fn ingress_dnat_namespace_policy_allows_root_namespace_for_exclusive_host() {
    validate_ingress_dnat_namespace(
        None,
        "tcp",
        9012,
        crate::slot_pool::ServicePortIngressIsolation::ExclusiveHost,
    )
    .unwrap();
}

#[test]
fn missing_pod_netns_maps_to_terminal_failed_precondition() {
    let error = crate::net::DefaultIfaceError::NetnsMissing {
        path: "/run/netns/cni-0123".to_string(),
        source: std::io::Error::from(std::io::ErrorKind::NotFound),
    };
    let status = ingress_iface_error_status(&error);
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(status.message().contains("/run/netns/cni-0123"));
    assert_eq!(ingress_iface_error_reason(&error), "pod_netns_missing");
    // The terminal contract is the metadata marker, not the bare code:
    // node-ctl keys on this so the root-namespace ingress rejection (same
    // code, no marker) stays retryable.
    assert_eq!(
        status
            .metadata()
            .get(pplx_space_common_core::port_exposure_signals::EXPOSE_TERMINAL_REASON_METADATA_KEY)
            .and_then(|value| value.to_str().ok()),
        Some(pplx_space_common_core::port_exposure_signals::EXPOSE_TERMINAL_REASON_STALE_POD_NETNS)
    );
}

#[test]
fn root_ns_ingress_rejection_carries_no_terminal_marker() {
    // The root-namespace ingress-safety rejection returns the same
    // FailedPrecondition code but must NOT be tagged terminal.
    let message = validate_ingress_dnat_namespace(
        None,
        "tcp",
        9012,
        crate::slot_pool::ServicePortIngressIsolation::SharedHost,
    )
    .expect_err("root namespace ingress DNAT must be rejected");
    let status = Status::failed_precondition(message);
    assert!(
        status
            .metadata()
            .get(pplx_space_common_core::port_exposure_signals::EXPOSE_TERMINAL_REASON_METADATA_KEY)
            .is_none()
    );
}

#[test]
fn other_ingress_iface_failures_stay_retryable_internal() {
    let error =
        crate::net::DefaultIfaceError::Other("netlink operation timed out after 5s".to_string());
    let status = ingress_iface_error_status(&error);
    assert_eq!(status.code(), tonic::Code::Internal);
    assert_eq!(
        ingress_iface_error_reason(&error),
        "resolve_ingress_iface_failed"
    );
}

/// UpdateCgroup forwards the sandbox's current execution and the requested
/// limits, with memory clamped to the node's per-sandbox cap. A stale
/// execution is refused before the runtime supervisor sees it.
#[tokio::test]
async fn update_cgroup_forwards_current_execution_with_clamped_memory() {
    use crate::runtime_supervisor_client::tests::{SupervisorScript, scripted_in_process_client};
    use crate::service::tests::service_tests::{
        bind_test_execution, root_request, service_for_unit_tests, stale_test_fence,
    };
    use pplx_space_node_runtime_core::proto as supervisor;

    let forwarded = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let recorder = std::sync::Arc::clone(&forwarded);
    let mut service = service_for_unit_tests().await;
    service.sandbox_cgroup_config = SandboxCgroupConfig {
        vcpu_count: Some(2),
        memory_mib: Some(4096),
    };
    service.runtime_supervisor = scripted_in_process_client(SupervisorScript {
        update_cgroup: Some(std::sync::Arc::new(move |request| {
            recorder.lock().unwrap().push(request);
            Ok(supervisor::UpdateCgroupResponse {})
        })),
        ..Default::default()
    });
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    let fence = bind_test_execution(&service, &sandbox_id);
    let request = |execution_fence| UpdateCgroupRequest {
        sandbox_id: sandbox_id.clone(),
        vcpu_count: Some(1),
        memory_mib: Some(8192),
        pids_max: Some(64),
        execution_fence: Some(execution_fence),
    };

    let stale = service
        .update_cgroup(root_request(request(stale_test_fence())))
        .await
        .unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition, "{stale}");
    assert!(forwarded.lock().unwrap().is_empty());

    service
        .update_cgroup(root_request(request(fence.clone())))
        .await
        .unwrap();
    assert_eq!(
        *forwarded.lock().unwrap(),
        [supervisor::UpdateCgroupRequest {
            execution_id: fence.execution_id,
            sandbox_id: sandbox_id.clone(),
            cgroup_limits: Some(supervisor::CgroupLimits {
                vcpu_count: Some(1),
                memory_mib: Some(4096),
                pids_max: Some(64),
            }),
        }]
    );
}

/// A supervisor refusal keeps its code; the message names the operation.
#[tokio::test]
async fn update_cgroup_reports_a_supervisor_refusal_with_its_code() {
    use crate::service::tests::service_tests::{
        bind_test_execution, root_request, service_for_unit_tests,
    };

    // The unit-test service's own supervisor refuses every RPC.
    let service = service_for_unit_tests().await;
    let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
    let fence = bind_test_execution(&service, &sandbox_id);

    let error = service
        .update_cgroup(root_request(UpdateCgroupRequest {
            sandbox_id,
            execution_fence: Some(fence),
            ..Default::default()
        }))
        .await
        .unwrap_err();

    assert_eq!(error.code(), tonic::Code::Unimplemented, "{error}");
    assert!(
        error
            .message()
            .starts_with("runtime-supervisor update cgroup failed: "),
        "{error}"
    );
}
