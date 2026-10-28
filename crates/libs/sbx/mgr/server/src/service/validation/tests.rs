use super::normalize_environment_input;
use super::require_environment_acknowledgement;
use super::template_base_snapshot_dir;
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

/// Every create path a cluster, pool or warm boot uses, beside node-local cold.
fn create_paths() -> Vec<(&'static str, crate::proto::CreateRequest, bool)> {
    use crate::proto::{BootMode, CreateRequest};
    let cold = CreateRequest {
        runtime: "fc".into(),
        boot_mode: BootMode::Cold as i32,
        ..Default::default()
    };
    // (name, request, whether the path captures or loads memory)
    vec![
        ("node-local cold", cold.clone(), false),
        (
            "cluster-owned sandbox id",
            CreateRequest {
                sandbox_id: Some("cluster-owned".into()),
                ..cold.clone()
            },
            false,
        ),
        (
            "cluster-owned route identity",
            CreateRequest {
                route_identity: Some(Default::default()),
                ..cold.clone()
            },
            false,
        ),
        (
            "pool member",
            CreateRequest {
                pool_template: Some("candidate".into()),
                ..cold.clone()
            },
            true,
        ),
        (
            "pool adoption",
            CreateRequest {
                boot_mode: BootMode::Pool as i32,
                ..cold.clone()
            },
            true,
        ),
        (
            "base-snapshot boot",
            CreateRequest {
                boot_mode: BootMode::BaseSnapshot as i32,
                ..cold.clone()
            },
            true,
        ),
        (
            "snapshots at create",
            CreateRequest {
                snapshots_enabled: true,
                ..cold
            },
            true,
        ),
    ]
}

#[test]
fn compiled_environments_admit_the_create_paths_they_qualify_for_and_never_relabel_a_template() {
    use pplx_space_common_core::types::Digest;
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    for environment in CompiledEnvironment::for_architecture(Arch::X86_64).unwrap() {
        let reference = environment.definition().reference();
        let execution = environment.definition().execution_requirements().unwrap();
        let non_default = !environment.is_initial();
        for (name, mut request, memory_state) in create_paths() {
            request.runtime = execution.runtime().kind.alias().into();
            for selector in [None, Some(reference)] {
                let result = super::validate_environment_create(&request, selector, &execution);
                // A selected create boots cold, so only its snapshots load memory.
                let needs_memory_state = if selector.is_some() {
                    request.snapshots_enabled
                } else {
                    memory_state
                };
                let context = format!(
                    "{} {name} selected={}",
                    environment.name(),
                    selector.is_some()
                );
                let unqualified_cluster = execution.runtime().kind
                    == pplx_space_common_core::types::RuntimeKind::CloudHypervisor
                    && (request.sandbox_id.is_some() || request.route_identity.is_some());
                if unqualified_cluster
                    || (needs_memory_state && !environment.supports_memory_state())
                {
                    assert_eq!(
                        result.unwrap_err().code(),
                        tonic::Code::FailedPrecondition,
                        "{context}"
                    );
                    continue;
                }
                assert_eq!(
                    result.unwrap_or_else(|error| panic!("{context}: {error}")),
                    non_default,
                    "{context}"
                );
            }
            let error = super::validate_environment_create(
                &request,
                Some(EnvironmentReference::new(Digest::sha256([9; 32]))),
                &execution,
            )
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{name}");
        }
    }
}

#[test]
fn an_environment_without_memory_state_support_is_refused_on_memory_paths() {
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
    assert!(super::requires_cold_creation(&execution));
    // A bound template's own paths; selected creates are covered separately.
    for (name, request, memory_state) in create_paths() {
        let result = super::validate_environment_create(&request, None, &execution);
        if memory_state {
            assert_eq!(
                result.unwrap_err().code(),
                tonic::Code::FailedPrecondition,
                "{name}"
            );
        } else {
            assert!(result.unwrap(), "{name}");
        }
    }
    // Legacy unbound work keeps its existing contract on every path.
    let legacy = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
    for (name, request, _) in create_paths() {
        assert!(
            !super::validate_environment_create(&request, None, &legacy).unwrap(),
            "{name}"
        );
    }
}

#[test]
fn a_selected_create_cannot_be_a_pool_member_but_an_empty_pool_names_none() {
    for (name, request, _) in create_paths() {
        let result = super::validate_selected_create_fields(&request);
        assert_eq!(result.is_err(), name == "pool member", "{name}");
    }
    let empty_pool = crate::proto::CreateRequest {
        boot_mode: crate::proto::BootMode::Cold as i32,
        pool_template: Some(String::new()),
        ..Default::default()
    };
    super::validate_selected_create_fields(&empty_pool).unwrap();
    assert!(super::allows_node_local_environment_selection(&empty_pool));
    let empty_id = crate::proto::CreateRequest {
        sandbox_id: Some(String::new()),
        ..empty_pool
    };
    assert!(super::allows_node_local_environment_selection(&empty_id));
}

#[test]
fn a_selected_create_boots_cold_so_only_its_snapshots_need_memory_state() {
    use pplx_space_common_core::types::{Digest, RuntimeIdentity, RuntimeKind};
    let reference = EnvironmentReference::new(Digest::sha256([8; 32]));
    let cold_only = ExecutionMetadata::new(
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
    for (name, request, _) in create_paths() {
        if name == "pool member" {
            continue; // refused from the request before this check
        }
        let result = super::validate_environment_create(&request, Some(reference), &cold_only);
        assert_eq!(result.is_err(), name == "snapshots at create", "{name}");
    }
}

#[test]
fn only_node_local_cold_creation_selects_a_compatible_consumer() {
    for (name, request, _) in create_paths() {
        assert_eq!(
            super::allows_node_local_environment_selection(&request),
            name == "node-local cold",
            "{name}"
        );
    }
}

#[test]
fn a_non_default_base_snapshot_boots_only_under_its_producer_environment() {
    use pplx_space_common_core::types::SnapshotExecutionMetadata;
    use pplx_space_node_runtime_core::catalog::CompiledEnvironment;
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let execution = |name| {
        catalog
            .iter()
            .find(|entry| entry.name() == name)
            .unwrap()
            .definition()
            .execution_requirements()
            .unwrap()
    };
    // The upgrade changes only the runtime; the 6.18 guest kernel is a separate environment.
    let (e0, upgrade, other) = (
        execution("e0"),
        execution("fc-1.17.0-linux-6.1.155"),
        execution("fc-1.17.0-linux-6.18.44"),
    );
    let legacy = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
    enum Producer<'a> {
        Missing,
        Corrupt,
        Captured(&'a ExecutionMetadata),
    }
    for (name, consumer, producer, usable) in [
        (
            "legacy snapshot, legacy consumer",
            &legacy,
            Producer::Missing,
            true,
        ),
        ("legacy snapshot, E0 consumer", &e0, Producer::Missing, true),
        (
            "E0 snapshot, E0 consumer",
            &e0,
            Producer::Captured(&e0),
            true,
        ),
        (
            "corrupt provenance, E0 consumer",
            &e0,
            Producer::Corrupt,
            true,
        ),
        (
            "UPGRADE snapshot, E0 consumer",
            &e0,
            Producer::Captured(&upgrade),
            false,
        ),
        (
            "UPGRADE snapshot, legacy consumer",
            &legacy,
            Producer::Captured(&upgrade),
            false,
        ),
        (
            "UPGRADE snapshot, UPGRADE consumer",
            &upgrade,
            Producer::Captured(&upgrade),
            true,
        ),
        (
            "E0 snapshot, UPGRADE consumer",
            &upgrade,
            Producer::Captured(&e0),
            false,
        ),
        (
            "legacy snapshot, UPGRADE consumer",
            &upgrade,
            Producer::Captured(&legacy),
            false,
        ),
        (
            "other snapshot, UPGRADE consumer",
            &upgrade,
            Producer::Captured(&other),
            false,
        ),
        (
            "missing provenance, UPGRADE consumer",
            &upgrade,
            Producer::Missing,
            false,
        ),
        (
            "corrupt provenance, UPGRADE consumer",
            &upgrade,
            Producer::Corrupt,
            false,
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        match producer {
            Producer::Missing => {}
            Producer::Corrupt => std::fs::write(dir.path().join("metadata.json"), b"{").unwrap(),
            Producer::Captured(producer) => std::fs::write(
                dir.path().join("metadata.json"),
                serde_json::to_vec(&SnapshotExecutionMetadata::new(producer.clone()).unwrap())
                    .unwrap(),
            )
            .unwrap(),
        }
        assert_eq!(
            super::base_snapshot_matches_execution(dir.path().to_str().unwrap(), consumer),
            usable,
            "{name}"
        );
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
fn a_capture_publishes_under_its_own_environment_s_cache_partition() {
    let hash = format!("sha256:{}", "a".repeat(64));
    let initial = pplx_space_node_runtime_core::catalog::initial_environment_reference().unwrap();
    let twin = EnvironmentReference::new(pplx_space_common_core::types::Digest::sha256([2; 32]));
    let bound = |environment| {
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]).with_environment(environment)
    };
    let dir = |execution: Option<&ExecutionMetadata>| {
        template_base_snapshot_dir("seed", &hash, execution, 2, 1024)
    };
    let legacy = dir(None);
    assert!(
        legacy.contains(&format!("/seed/snapshots/{hash}/")),
        "{legacy}"
    );
    assert_eq!(dir(Some(&bound(initial))), legacy);
    let twin_dir = dir(Some(&bound(twin)));
    assert!(
        twin_dir.contains(&format!("/seed/snapshots/{hash}@{twin}/")),
        "{twin_dir}"
    );
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
        super::validate_runtime_lifecycle_policy(RuntimeKind::CloudHypervisor, &policy).unwrap();
        for entry in pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(
            pplx_space_common_core::types::Arch::X86_64,
        )
        .unwrap()
        {
            let execution = entry.definition().execution_requirements().unwrap();
            let result = super::validate_execution_lifecycle_policy(Some(&execution), &policy);
            assert_eq!(
                result.is_ok(),
                action != AutoLifecycleIdleAction::Suspend || entry.supports_memory_state()
            );
        }
        let result = super::validate_runtime_lifecycle_policy(RuntimeKind::Gvisor, &policy);
        match action {
            AutoLifecycleIdleAction::Pause | AutoLifecycleIdleAction::Suspend => {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            }
            _ => result.unwrap(),
        }
    }
}

#[test]
fn qualified_template_supports_local_snapshot_and_pool_creation_without_runtime_relabeling() {
    use crate::proto::{BootMode, CreateRequest};
    use pplx_space_common_core::types::RuntimeKind;
    for environment in [Arch::X86_64, Arch::Aarch64]
        .into_iter()
        .flat_map(|arch| {
            pplx_space_node_runtime_core::catalog::CompiledEnvironment::for_architecture(arch)
                .unwrap()
        })
        .filter(|entry| entry.supports_memory_state())
    {
        let runtime = environment.release().release.kind;
        let execution = environment.definition().execution_requirements().unwrap();
        for mode in [BootMode::Cold, BootMode::BaseSnapshot, BootMode::Pool] {
            let request = CreateRequest {
                runtime: runtime.alias().into(),
                boot_mode: mode as i32,
                pool_template: Some("seed".into()),
                ..Default::default()
            };
            super::validate_environment_create(&request, None, &execution).unwrap();
            super::validate_environment_create(
                &CreateRequest {
                    runtime: if runtime == RuntimeKind::Firecracker {
                        "ch"
                    } else {
                        "fc"
                    }
                    .into(),
                    ..request
                },
                None,
                &execution,
            )
            .unwrap_err();
        }
    }
}
