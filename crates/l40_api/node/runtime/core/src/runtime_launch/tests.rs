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
        sandbox_id: id.into(),
        execution_id: "018e390d-4dd8-7000-8000-000000000002".into(),
        runtime: RuntimeKind::Firecracker as i32,
        launcher_binary: Some("/runtime/bin/jailer".into()),
        runtime_binary: "/runtime/bin/firecracker".into(),
        workspace_root: "/runtime/workspaces".into(),
        network_namespace: "/run/netns/sandbox".into(),
        uid: 1000,
        gid: 1001,
        configuration_path: "/config.json".into(),
        log_path: "/runtime.log".into(),
        console_path: "/console.log".into(),
        metrics_path: "/metrics".into(),
        resource_limits: Some(limits),
        verified_runtime: Some(VerifiedRuntimeLaunch {
            sandbox_id: id.into(),
            environment_reference: reference.into(),
            integrity_requirement: ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                as i32,
        }),
    };
    let parsed = FirecrackerLaunch::try_from(request.clone()).unwrap();
    assert_eq!(parsed.sandbox_id, id.parse().unwrap());
    assert_eq!(parsed.execution_id.to_string(), request.execution_id);
    assert_eq!(
        parsed.environment_reference,
        Some(reference.parse().unwrap())
    );
    assert_eq!(parsed.jailer_bin.as_str(), "/runtime/bin/jailer");
    assert_eq!(
        parsed.firecracker_bin.as_path(),
        std::path::Path::new("/runtime/bin/firecracker")
    );
    assert_eq!(parsed.cgroup_limits, Some(limits));
    for invalid in ["relative", "/runtime/../jailer", "/runtime/ja\0iler"] {
        let mut invalid_request = request.clone();
        invalid_request.launcher_binary = Some(invalid.into());
        assert!(matches!(
            FirecrackerLaunch::try_from(invalid_request),
            Err(LaunchContractError::Path { .. } | LaunchContractError::NulPath(_))
        ));
    }
    for (sandbox, execution) in [
        ("bad-id", request.execution_id.as_str()),
        (id, "bad-execution"),
    ] {
        let mut invalid = request.clone();
        invalid.verified_runtime = None;
        invalid.sandbox_id = sandbox.into();
        invalid.execution_id = execution.into();
        assert!(matches!(
            FirecrackerLaunch::try_from(invalid),
            Err(LaunchContractError::Sandbox(_) | LaunchContractError::Execution(_))
        ));
    }
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
    assert_eq!(unbound.sandbox_id, id.parse().unwrap());
    assert_eq!(unbound.environment_reference, None);
}
