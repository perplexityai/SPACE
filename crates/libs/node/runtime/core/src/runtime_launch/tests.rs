use super::*;
use crate::proto::{ArtifactIntegrityRequirement, CgroupLimits, VerifiedRuntimeLaunch};

#[test]
fn launch_conversion_preserves_verified_identity_isolation_and_limits() {
    let original = LaunchFirecrackerRequest {
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
        cgroup_limits: Some(CgroupLimits {
            vcpu_count: Some(2),
            memory_mib: Some(512),
            pids_max: Some(64),
        }),
        verified_runtime: Some(VerifiedRuntimeLaunch {
            sandbox_id: "018e390d-4dd8-7000-8000-000000000001".into(),
            environment_reference:
                "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
            integrity_requirement: ArtifactIntegrityRequirement::AuthenticatedImmutableSources
                as i32,
        }),
        ..Default::default()
    };
    let generic = LaunchRuntimeRequest::try_from(original.clone()).unwrap();
    assert_eq!(
        generic.sandbox_id,
        original.verified_runtime.as_ref().unwrap().sandbox_id
    );
    assert_eq!(
        LaunchFirecrackerRequest::try_from(generic.clone()).unwrap(),
        original
    );
    for sandbox_id in ["", "another-sandbox"] {
        let mut conflict = generic.clone();
        conflict.sandbox_id = sandbox_id.into();
        assert!(matches!(
            LaunchFirecrackerRequest::try_from(conflict),
            Err(LaunchContractError::IdentityMismatch)
        ));
    }
}

#[test]
fn legacy_launch_rejects_both_identity_fields_even_when_equal() {
    let verified_id = "018e390d-4dd8-7000-8000-000000000001";
    for legacy_id in [verified_id, "018e390d-4dd8-7000-8000-000000000002"] {
        let request = LaunchFirecrackerRequest {
            sandbox_id: legacy_id.into(),
            verified_runtime: Some(VerifiedRuntimeLaunch {
                sandbox_id: verified_id.into(),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert!(matches!(
            LaunchRuntimeRequest::try_from(request),
            Err(LaunchContractError::DualSandboxIdentities)
        ));
    }
}
