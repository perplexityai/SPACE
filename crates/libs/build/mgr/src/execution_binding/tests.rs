use super::*;
use pplx_space_common_core::types::*;

fn fixture() -> ResolveExecutionEnvironmentResponse {
    let definition = EnvironmentDefinition::from_document(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../../../proto/execution/v1/fixtures/e0.json"
    )))
    .unwrap();
    let spec = definition.descriptor().spec();
    let RuntimeBinding::ManagedRelease(runtime) = &spec.runtime;
    let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
    let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } = &spec.compatibility;
    let execution = definition
        .bind_execution(
            ExecutionMetadata::new(
                runtime.runtime.clone(),
                spec.guest,
                Some(state_format.clone()),
                vec![ArtifactComponentMetadata {
                    kind: kernel.kind,
                    role: Some("boot".parse().unwrap()),
                    version: Some(kernel.version.clone()),
                    digest: Some(kernel.digest),
                    format: None,
                }],
            )
            .unwrap(),
        )
        .unwrap();
    ResolveExecutionEnvironmentResponse {
        definition: Some((&definition).into()),
        execution: Some((&execution).into()),
    }
}

#[test]
fn resolution_requires_complete_matching_support_evidence() {
    let binding = BuildExecutionBinding::from_response(None, fixture()).unwrap();
    BuildExecutionBinding::from_response(Some(binding.definition.reference()), fixture()).unwrap();
    assert!(matches!(
        BuildExecutionBinding::from_response(
            Some(EnvironmentReference::new(Digest::sha256([0; 32]))),
            fixture()
        ),
        Err(BuildExecutionBindingError::Reference)
    ));
    type Mutation = fn(&mut ResolveExecutionEnvironmentResponse);
    for mutate in [
        (|r: &mut ResolveExecutionEnvironmentResponse| r.definition = None) as Mutation,
        |r| r.execution = None,
        |r| r.execution.as_mut().unwrap().environment = None,
        |r| {
            r.execution
                .as_mut()
                .unwrap()
                .runtime
                .as_mut()
                .unwrap()
                .version = "1.17.0".into()
        },
        |r| r.execution.as_mut().unwrap().components.clear(),
    ] {
        let mut response = fixture();
        mutate(&mut response);
        BuildExecutionBinding::from_response(None, response).unwrap_err();
    }
}

#[test]
fn scratch_inputs_replace_inherited_selectors_and_reject_conflicting_recipe_pins() {
    let binding = BuildExecutionBinding::from_response(None, fixture()).unwrap();
    let mut command = tokio::process::Command::new("true");
    for key in [
        "SPACE_SEED_RUNTIME",
        "SPACE_SEED_RUNTIME_VERSION",
        "SPACE_SEED_STATE_FORMAT_VERSION",
        "SPACE_SEED_KERNEL_VERSION",
        "SPACE_SEED_KERNEL_DIGEST",
    ] {
        command.env(key, "untrusted-inherited-selector");
    }
    binding.configure_scratch(&mut command, None).unwrap();
    let env: std::collections::HashMap<_, _> = command.as_std().get_envs().collect();
    for (key, expected) in [
        ("SPACE_SEED_RUNTIME", "firecracker"),
        ("SPACE_SEED_RUNTIME_VERSION", "1.15.1"),
        ("SPACE_SEED_STATE_FORMAT_VERSION", "9.0.0"),
        ("SPACE_SEED_KERNEL_VERSION", "6.1.155"),
    ] {
        assert_eq!(
            env[std::ffi::OsStr::new(key)],
            Some(std::ffi::OsStr::new(expected))
        );
    }
    let BootProfile::LinuxDirectV1 { kernel } = &binding.definition.descriptor().spec().boot;
    let mut pin = super::super::GuestKernelPin {
        version: kernel.version.clone(),
        digest: kernel.digest,
    };
    binding.configure_scratch(&mut command, Some(&pin)).unwrap();
    pin.digest = Digest::sha256([0; 32]);
    assert!(matches!(
        binding.configure_scratch(&mut command, Some(&pin)),
        Err(BuildExecutionBindingError::KernelPin)
    ));
}

#[test]
fn seed_metadata_requires_evidence_and_becomes_a_versioned_bound_document() {
    let binding = BuildExecutionBinding::from_response(None, fixture()).unwrap();
    let mut metadata = TemplateMetadata::default();
    binding.bind_template(&mut metadata).unwrap_err();
    let mut wire = fixture().execution.unwrap();
    wire.environment = None;
    metadata.set_execution_metadata(ExecutionMetadata::try_from(&wire).unwrap());
    assert_eq!(
        metadata.schema_version,
        Some(TEMPLATE_METADATA_SCHEMA_VERSION)
    );
    binding.bind_template(&mut metadata).unwrap();
    assert_eq!(
        metadata.schema_version,
        Some(BOUND_TEMPLATE_METADATA_SCHEMA_VERSION)
    );
    let restored: TemplateMetadata =
        serde_json::from_str(&serde_json::to_string(&metadata).unwrap()).unwrap();
    assert_eq!(
        restored
            .execution_metadata()
            .unwrap()
            .unwrap()
            .environment(),
        Some(binding.definition.reference())
    );
}

#[test]
fn binding_preserves_seed_provenance_and_extensions() {
    let binding = BuildExecutionBinding::from_response(None, fixture()).unwrap();
    let mut execution = fixture().execution.unwrap();
    execution.environment = None;
    let mut metadata = TemplateMetadata::default();
    metadata.set_execution_metadata(ExecutionMetadata::try_from(&execution).unwrap());
    let mut document = serde_json::to_value(metadata).unwrap();
    document["base_image"] = "https://example.com/ubuntu.squashfs".into();
    document["image_recipe"] = serde_json::json!({"distribution": "ubuntu", "release": "22.04"});
    let bytes = binding
        .bind_document(&serde_json::to_vec(&document).unwrap())
        .unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(actual["base_image"], document["base_image"]);
    assert_eq!(actual["image_recipe"], document["image_recipe"]);
    assert_eq!(actual["schema_version"], 3);
}

#[tokio::test]
async fn unbound_bases_and_custom_or_arm_scratch_builds_do_not_require_a_resolver() {
    for arch in [Arch::X86_64, Arch::Aarch64] {
        let legacy = ExecutionMetadata::initial_firecracker(arch, vec![]);
        assert!(
            BuildExecutionBinding::for_base(None, &legacy)
                .await
                .unwrap()
                .is_none()
        );
        let pin = super::super::GuestKernelPin {
            version: "6.1.155-custom".parse().unwrap(),
            digest: Digest::sha256([3; 32]),
        };
        assert!(
            BuildExecutionBinding::for_scratch(None, Some(&pin), arch)
                .await
                .unwrap()
                .is_none()
        );
        let mut command = tokio::process::Command::new("true");
        configure_legacy_scratch(&mut command, Some(&pin));
        let env: std::collections::HashMap<_, _> = command.as_std().get_envs().collect();
        assert_eq!(
            env[std::ffi::OsStr::new("SPACE_SEED_KERNEL_VERSION")],
            Some(std::ffi::OsStr::new("6.1.155-custom"))
        );
        configure_legacy_scratch(&mut command, None);
        let env: std::collections::HashMap<_, _> = command.as_std().get_envs().collect();
        assert_eq!(env[std::ffi::OsStr::new("SPACE_SEED_KERNEL_DIGEST")], None);
    }
    assert!(
        BuildExecutionBinding::for_scratch(None, None, Arch::Aarch64)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn derived_vm_requests_keep_unbound_releases_on_the_legacy_protocol() {
    for version in ["1.15.1", "1.16.0"] {
        let base = ExecutionMetadata::new(
            RuntimeIdentity {
                kind: RuntimeKind::Firecracker,
                version: version.parse().unwrap(),
            },
            GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: Arch::X86_64,
            },
            None,
            vec![],
        )
        .unwrap();
        let binding = BuildExecutionBinding::for_base(None, &base).await.unwrap();
        let request = super::super::build_vm_create_request(
            "seed".into(),
            "sha256:legacy-base".into(),
            None,
            binding.as_ref().map(BuildExecutionBinding::reference),
            &pplx_space_net_common::NetworkPolicyV1::allow_all(),
        );
        assert!(request.environment.is_none(), "unselected {version}");
        assert_eq!(request.runtime, "fc");
        assert_eq!(request.template, "seed");
    }

    let binding = BuildExecutionBinding::from_response(None, fixture()).unwrap();
    let inherited_e0 = super::super::build_vm_create_request(
        "seed".into(),
        "sha256:bound-base".into(),
        None,
        binding.create_environment(false),
        &pplx_space_net_common::NetworkPolicyV1::allow_all(),
    );
    assert!(
        inherited_e0.environment.is_none(),
        "ordinary inherited E0 builds remain rolling-compatible"
    );
    assert_eq!(inherited_e0.runtime, "fc");
    let explicit_e0 = super::super::build_vm_create_request(
        "seed".into(),
        "sha256:bound-base".into(),
        None,
        binding.create_environment(true),
        &pplx_space_net_common::NetworkPolicyV1::allow_all(),
    );
    assert_eq!(
        explicit_e0.environment.unwrap().reference,
        binding.reference().to_string()
    );
    assert!(explicit_e0.runtime.is_empty());
    assert!(explicit_e0.template.is_empty());
    // A different boot kernel is a different binding, even with the same VMM.
    let mut spec = binding.definition.descriptor().spec().clone();
    let BootProfile::LinuxDirectV1 { kernel } = &mut spec.boot;
    kernel.digest = Digest::sha256([7; 32]);
    let definition = EnvironmentDefinition::new(EnvironmentDescriptor::try_from(spec).unwrap());
    let requirements = definition.execution_requirements().unwrap();
    let other = BuildExecutionBinding::from_response(
        None,
        ResolveExecutionEnvironmentResponse {
            definition: Some((&definition).into()),
            execution: Some((&requirements).into()),
        },
    )
    .unwrap();
    assert_eq!(
        other.create_environment(false),
        Some(definition.reference())
    );
}
