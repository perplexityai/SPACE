use super::*;
use pplx_space_common_core::types::ExecutionMetadata;
use pplx_space_sbx_mgr_client::proto::{AutoLifecyclePolicy, AutoLifecycleWakePolicy};
use tonic::Code;

fn template() -> PublishedTemplateState {
    PublishedTemplateState {
        template: "synthetic-template".to_string(),
        present: true,
        ..PublishedTemplateState::default()
    }
}

#[test]
fn template_rejections_preserve_status_and_message() {
    let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, Vec::new());
    let cases = [
        (
            PublishedTemplateState {
                template: " \t".to_string(),
                ..template()
            },
            "template must not be empty",
        ),
        (
            PublishedTemplateState {
                execution: Some((&execution).into()),
                runtime: Some("gvisor".to_string()),
                ..template()
            },
            "published template runtime disagrees with execution metadata",
        ),
        (
            PublishedTemplateState {
                execution: Some((&execution).into()),
                runtime: Some("future-runtime".to_string()),
                ..template()
            },
            "unsupported runtime 'future-runtime'",
        ),
        (
            PublishedTemplateState {
                execution: Some((&execution).into()),
                arch: Some("aarch64".to_string()),
                ..template()
            },
            "published template architecture disagrees with execution metadata",
        ),
        (
            PublishedTemplateState {
                execution: Some((&execution).into()),
                arch: Some("future-architecture".to_string()),
                ..template()
            },
            "unsupported CPU architecture 'future-architecture'",
        ),
        (
            PublishedTemplateState {
                auto_lifecycle_authority: 123,
                ..template()
            },
            "policy authority proto value 123 is unknown",
        ),
        (
            PublishedTemplateState {
                network_policy_authority: 123,
                ..template()
            },
            "policy authority proto value 123 is unknown",
        ),
        (
            PublishedTemplateState {
                network_policy: Some(pplx_space_net_common::proto::NetworkPolicyV1::default()),
                ..template()
            },
            "network_policy.version must be v1",
        ),
    ];
    for (state, message) in cases {
        for include_metadata in [false, true] {
            let error = template_state_row(&state, include_metadata).unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert_eq!(error.message(), message);
        }
    }
}

#[test]
fn legacy_metadata_without_execution_keeps_its_existing_acceptance() {
    // Publication does not use the stricter template-creation name grammar.
    let state = PublishedTemplateState {
        template: " ../synthetic-template ".to_string(),
        runtime: Some(" future-runtime ".to_string()),
        arch: Some(" future-architecture ".to_string()),
        default_auto_lifecycle: Some(AutoLifecyclePolicy {
            wake: Some(AutoLifecycleWakePolicy { on: vec![123] }),
            ..AutoLifecyclePolicy::default()
        }),
        ..template()
    };
    let row = template_state_row(&state, true).unwrap();
    assert_eq!(row["template_name"], "../synthetic-template");
    assert_eq!(row["runtime"], "future-runtime");
    assert_eq!(row["arch"], "future-architecture");
    assert!(row["default_auto_lifecycle_json"].is_null());
}

#[test]
fn builder_only_numeric_fields_are_not_validated_for_workers() {
    let state = PublishedTemplateState {
        artifact_size_bytes: Some(u64::MAX),
        snapshot_vcpu: Some(u32::MAX),
        snapshot_memory_mib: Some(u32::MAX),
        vcpu: Some(u32::MAX),
        memory_mib: Some(u32::MAX),
        disk_size_mb: Some(u32::MAX),
        ..template()
    };
    assert_eq!(
        template_state_row(&state, false).unwrap(),
        template_state_row(&template(), false).unwrap()
    );
    let error = template_state_row(&state, true).unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        error.message(),
        "artifact_size_bytes exceeds i64::MAX: number too large to fit in target type"
    );
}

#[test]
fn execution_metadata_accepts_matching_legacy_runtime_aliases() {
    let execution = ExecutionMetadata::initial_firecracker(Arch::X86_64, Vec::new());
    for runtime in ["fc", "firecracker"] {
        let state = PublishedTemplateState {
            runtime: Some(runtime.to_string()),
            arch: Some("x86_64".to_string()),
            execution: Some((&execution).into()),
            ..template()
        };
        template_state_row(&state, true).unwrap();
        let row = template_state_row(&state, false).unwrap();
        assert_eq!(row["runtime"], "fc");
        assert_eq!(row["arch"], "x86_64");
    }
}
