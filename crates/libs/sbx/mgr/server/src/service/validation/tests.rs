use super::require_environment_acknowledgement;
use super::{normalize_device_input, normalize_environment_input};
use pplx_space_common_core::types::{Arch, EnvironmentReference, ExecutionMetadata, SandboxId};

#[test]
fn bound_execution_requires_positive_peer_acknowledgement() {
    let reference = |digit: &str| -> EnvironmentReference {
        format!("sha256:{}", digit.repeat(64)).parse().unwrap()
    };
    let legacy = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
    let bound = legacy.clone().with_environment(reference("a"));
    let other = legacy.clone().with_environment(reference("b"));
    for (requested, resolved, accepted) in [
        (&legacy, &legacy, true),
        (&legacy, &bound, true),
        (&bound, &bound, true),
        (&bound, &legacy, false),
        (&bound, &other, false),
    ] {
        let result = require_environment_acknowledgement(requested, resolved);
        assert_eq!(result.is_ok(), accepted);
        if let Err(error) = result {
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }
    }
}

#[test]
fn environment_selection_inherits_template_but_refuses_mismatch_and_nonlocal_memory_paths() {
    use crate::proto::{BootMode, CreateRequest};
    use pplx_space_common_core::types::{Digest, RuntimeIdentity, RuntimeKind};
    let reference = EnvironmentReference::new(Digest::sha256([8; 32]));
    let execution = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: "1.17.0".parse().unwrap(),
        },
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]).guest(),
        None,
        vec![],
    )
    .unwrap()
    .with_environment(reference);
    let base = CreateRequest {
        boot_mode: BootMode::Cold as i32,
        ..Default::default()
    };
    for selector in [None, Some(reference)] {
        assert!(super::validate_environment_create(&base, selector, &execution).unwrap());
    }
    super::validate_environment_create(
        &base,
        Some(EnvironmentReference::new(Digest::sha256([9; 32]))),
        &execution,
    )
    .unwrap_err();
    for request in [
        CreateRequest {
            boot_mode: BootMode::Pool as i32,
            ..base.clone()
        },
        CreateRequest {
            boot_mode: BootMode::BaseSnapshot as i32,
            ..base.clone()
        },
        CreateRequest {
            pool_template: Some("candidate".into()),
            ..base.clone()
        },
        CreateRequest {
            snapshots_enabled: true,
            ..base.clone()
        },
        CreateRequest {
            sandbox_id: Some("cluster-owned".into()),
            ..base.clone()
        },
    ] {
        super::validate_environment_create(&request, None, &execution).unwrap_err();
    }
}

#[test]
fn selected_create_has_one_authority_and_old_wire_readers_reject_it() {
    use prost::Message;
    #[derive(prost::Message)]
    struct OldCreate {
        #[prost(string, tag = "1")]
        template: String,
        #[prost(string, tag = "3")]
        runtime: String,
    }
    let reference = "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
        .parse()
        .unwrap();
    let mut request = crate::proto::CreateRequest {
        template: "selected-template".into(),
        runtime: "fc".into(),
        boot_mode: crate::proto::BootMode::Cold as i32,
        ..Default::default()
    };
    pplx_space_sbx_mgr_client::environment::bind(&mut request, reference);
    let old = OldCreate::decode(request.encode_to_vec().as_slice()).unwrap();
    let old_request = crate::proto::CreateRequest {
        template: old.template,
        runtime: old.runtime,
        ..Default::default()
    };
    assert_eq!(
        super::super::create_support::validate_create_request(&old_request)
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    for legacy_runtime in ["", "fc"] {
        let mut mixed = request.clone();
        mixed.template = "conflicting-template".into();
        mixed.runtime = legacy_runtime.into();
        assert_eq!(
            normalize_environment_input(&mut mixed).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
    assert_eq!(
        normalize_environment_input(&mut request).unwrap(),
        Some(reference)
    );
    assert_eq!(request.template, "selected-template");
    assert!(
        request.runtime.is_empty(),
        "runtime comes from verified environment evidence, never the caller"
    );
}

#[test]
fn memory_state_support_follows_complete_environment_identity() {
    use pplx_space_common_core::types::Digest;
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = environments[0]
        .definition()
        .execution_requirements()
        .unwrap();
    let other = environments[1]
        .definition()
        .execution_requirements()
        .unwrap();
    let same_kernel = environments[2]
        .definition()
        .execution_requirements()
        .unwrap();
    let unknown_with_same_runtime = e0
        .clone()
        .with_environment(EnvironmentReference::new(Digest::sha256([9; 32])));
    for (execution, cold_only) in [
        (
            ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
            false,
        ),
        (e0, false),
        (other, false),
        (same_kernel, false),
        (unknown_with_same_runtime, true),
    ] {
        assert_eq!(super::requires_cold_creation(&execution), cold_only);
    }
}

#[test]
fn automatic_lifecycle_admission_respects_runtime_capabilities() {
    use pplx_space_common_core::types::{
        AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleStage, RuntimeKind,
    };
    for action in [
        AutoLifecycleIdleAction::None,
        AutoLifecycleIdleAction::Pause,
        AutoLifecycleIdleAction::Suspend,
        AutoLifecycleIdleAction::Stop,
        AutoLifecycleIdleAction::Delete,
    ] {
        let policy = AutoLifecyclePolicy {
            stages: vec![AutoLifecycleStage {
                condition_set:
                    pplx_space_common_core::types::AutoLifecycleConditionSetId::AccessIdle,
                at: 60,
                action,
            }],
            ..AutoLifecyclePolicy::disabled()
        };
        super::validate_runtime_lifecycle_policy(RuntimeKind::Firecracker, &policy).unwrap();
        let result =
            super::validate_runtime_lifecycle_policy(RuntimeKind::CloudHypervisor, &policy);
        match action {
            AutoLifecycleIdleAction::Pause | AutoLifecycleIdleAction::Suspend => {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            }
            _ => result.unwrap(),
        }
    }
}

#[test]
fn device_create_keeps_retry_identity_and_rejects_mixed_or_memory_inputs() {
    use crate::proto::{CreateRequest, DeviceCreateInput, EnvironmentCreateInput};
    use pplx_space_execution_common::proto::{DeviceRequirement, DeviceRequirements};
    let reference =
        EnvironmentReference::new(pplx_space_common_core::types::Digest::sha256([8; 32]));
    let original = CreateRequest {
        sandbox_id: Some(SandboxId::new().to_string()),
        boot_mode: crate::proto::BootMode::Cold as i32,
        device_create: Some(DeviceCreateInput {
            environment: Some(EnvironmentCreateInput {
                reference: reference.to_string(),
                template: "compute-seed".into(),
            }),
            requirements: Some(DeviceRequirements {
                entries: vec![DeviceRequirement {
                    profile: "compute".into(),
                    count: 2,
                    compatible_profiles: vec![],
                }],
            }),
        }),
        ..Default::default()
    };
    let mut request = original.clone();
    let parsed = normalize_device_input(&mut request).unwrap().unwrap();
    assert_eq!(parsed.entries()[0].count(), 2);
    assert_eq!(
        normalize_environment_input(&mut request).unwrap(),
        Some(reference)
    );
    assert_eq!(request.template, "compute-seed");
    assert_eq!(request.sandbox_id, original.sandbox_id);
    assert!(request.runtime.is_empty());
    for case in 0..9 {
        let mut request = original.clone();
        match case {
            0 => request.runtime = "fc".into(),
            1 => request.template = "other".into(),
            2 => request.environment = request.device_create.as_ref().unwrap().environment.clone(),
            3 => request.device_create.as_mut().unwrap().environment = None,
            4 => request.device_create.as_mut().unwrap().requirements = None,
            5 => {
                request
                    .device_create
                    .as_mut()
                    .unwrap()
                    .requirements
                    .as_mut()
                    .unwrap()
                    .entries[0]
                    .count = 0
            }
            6 => request.snapshots_enabled = true,
            7 => request.pool_template = Some("pool".into()),
            8 => request.boot_mode = crate::proto::BootMode::BaseSnapshot as i32,
            _ => unreachable!(),
        }
        normalize_device_input(&mut request).unwrap_err();
    }
}
