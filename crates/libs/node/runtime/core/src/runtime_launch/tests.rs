use super::*;
use crate::proto::{CgroupLimits, VerifiedRuntimeLaunch};

#[test]
fn launch_conversion_preserves_verified_identity_isolation_and_limits() {
    let id = "018e390d-4dd8-7000-8000-000000000001";
    let reference = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let limits = CgroupLimits {
        vcpu_count: Some(2),
        memory_mib: Some(512),
        pids_max: Some(64),
    };
    let request = LaunchRuntimeRequest {
        device_execution: None,
        sandbox_id: id.into(),
        execution_id: "execution".into(),
        runtime: RuntimeKind::Firecracker as i32,
        launcher_binary: Some("/runtime/bin/jailer".into()),
        runtime_binary: "/runtime/bin/firecracker".into(),
        workspace_root: "/runtime/workspaces".into(),
        network_namespace: "/run/netns/sandbox".into(),
        uid: 1000,
        gid: 1001,
        configuration_path: "config.json".into(),
        log_path: "runtime.log".into(),
        console_path: "console.log".into(),
        metrics_path: "metrics".into(),
        resource_limits: Some(limits),
        verified_runtime: Some(VerifiedRuntimeLaunch {
            sandbox_id: id.into(),
            environment_reference: reference.into(),
            integrity_requirement: ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                as i32,
        }),
    };
    assert_eq!(
        FirecrackerLaunch::try_from(request.clone()).unwrap(),
        FirecrackerLaunch {
            sandbox_id: id.into(),
            execution_id: "execution".into(),
            jailer_bin: "/runtime/bin/jailer".into(),
            firecracker_bin: "/runtime/bin/firecracker".into(),
            chroot_base_dir: "/runtime/workspaces".into(),
            ns_path: "/run/netns/sandbox".into(),
            uid: 1000,
            gid: 1001,
            fc_config_path: "config.json".into(),
            fc_log_path: "runtime.log".into(),
            fc_console_path: "console.log".into(),
            fc_metrics_path: "metrics".into(),
            cgroup_limits: Some(limits),
            environment_reference: Some(reference.parse().unwrap()),
        }
    );
    let mut device = request.clone();
    device.device_execution =
        Some(pplx_space_execution_common::proto::DeviceExecutionInput::default());
    assert!(matches!(
        FirecrackerLaunch::try_from(device),
        Err(LaunchContractError::UnsupportedDevices)
    ));
    for sandbox_id in ["", "another-sandbox"] {
        let mut conflict = request.clone();
        conflict.sandbox_id = sandbox_id.into();
        assert!(matches!(
            FirecrackerLaunch::try_from(conflict),
            Err(LaunchContractError::IdentityMismatch)
        ));
    }
    let mut unbound = request;
    unbound.verified_runtime = None;
    let unbound = FirecrackerLaunch::try_from(unbound).unwrap();
    assert_eq!(unbound.sandbox_id, id);
    assert_eq!(unbound.environment_reference, None);
}
