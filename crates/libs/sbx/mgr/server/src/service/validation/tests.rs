use super::normalize_environment_input;
use super::require_environment_acknowledgement;
use pplx_space_common_core::types::{Arch, EnvironmentReference, ExecutionMetadata};

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
