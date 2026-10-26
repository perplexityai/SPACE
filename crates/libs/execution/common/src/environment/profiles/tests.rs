use super::*;
use prost::Message;

fn portable_spec(kind: RuntimeKind) -> EnvironmentSpec {
    let mut spec = super::super::tests::e0().descriptor().spec().clone();
    spec.host.architecture = Arch::Aarch64;
    spec.guest.architecture = Arch::Aarch64;
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.kind = kind;
    release.runtime.version = "1.0.0".parse().unwrap();
    release.digest = Digest::sha256([7; 32]);
    release.target = spec.host;
    release.layout = RuntimePayloadLayout::CatalogReleaseV1;
    spec.boot = BootProfile::ArtifactsV1 {
        contract: "test-direct-boot-v1".parse().unwrap(),
        artifacts: vec![BootArtifact::new(
            "boot".parse().unwrap(),
            ComponentIdentity {
                kind: ArtifactComponentKind::Kernel,
                version: "1.0.0".parse().unwrap(),
                digest: Digest::sha256([8; 32]),
            },
            "linux-image".parse().unwrap(),
        )],
    };
    spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
        contract: "test-machine-v1".parse().unwrap(),
        state_format: None,
    };
    spec
}

#[test]
fn runtimes_share_one_wire_shape_with_optional_memory_state() {
    // These are synthetic structural definitions, not qualified catalog entries.
    for runtime in [
        RuntimeKind::Firecracker,
        RuntimeKind::CloudHypervisor,
        RuntimeKind::Gvisor,
    ] {
        for state in [
            None,
            Some(StateFormatIdentity {
                format: "test-vmstate".parse().unwrap(),
                version: "2.0.0".parse().unwrap(),
            }),
        ] {
            let mut spec = portable_spec(runtime);
            spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
                contract: "test-machine-v1".parse().unwrap(),
                state_format: state.clone(),
            };
            let definition = EnvironmentDefinition::new(spec.try_into().unwrap());
            let wire = proto::EnvironmentDefinition::from(&definition);
            // The deployed schema-1 decoder checks this before decoding profiles.
            assert_eq!(wire.schema_version, 2);
            let bytes = wire.encode_to_vec();
            let decoded = proto::EnvironmentDefinition::decode(bytes.as_slice()).unwrap();
            assert_eq!(
                EnvironmentDefinition::try_from(&decoded).unwrap(),
                definition
            );
            let execution = definition.execution_requirements().unwrap();
            assert_eq!(execution.state_format(), state.as_ref());
            definition.check_execution(&execution).unwrap();
            #[cfg(feature = "serde")]
            assert_eq!(
                EnvironmentDefinition::from_document(&definition.to_document().unwrap()).unwrap(),
                definition
            );
        }
    }
}

#[test]
fn runtime_neutral_golden_and_schema_boundary() {
    let definition = EnvironmentDefinition::new(
        portable_spec(RuntimeKind::CloudHypervisor)
            .try_into()
            .unwrap(),
    );
    assert_eq!(
        super::canonical_descriptor(definition.descriptor()),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../proto/execution/v1/fixtures/runtime-neutral.canonical"
        ))
    );
    assert_eq!(
        definition.reference().to_string(),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../proto/execution/v1/fixtures/runtime-neutral.reference"
        ))
        .trim()
    );
    for definition in [definition, super::super::tests::e0()] {
        let mut wire = proto::EnvironmentDefinition::from(&definition);
        wire.schema_version = if wire.schema_version == 1 { 2 } else { 1 };
        assert!(matches!(
            EnvironmentDefinition::try_from(&wire),
            Err(EnvironmentDecodeError::SchemaMismatch { .. })
        ));
    }
}

#[test]
fn boot_inputs_do_not_require_a_linux_guest_or_kernel() {
    let mut spec = portable_spec(RuntimeKind::CloudHypervisor);
    spec.guest.os = GuestOperatingSystem::Windows;
    spec.boot = BootProfile::ArtifactsV1 {
        contract: "test-firmware-boot-v1".parse().unwrap(),
        artifacts: vec![BootArtifact::new(
            "firmware".parse().unwrap(),
            ComponentIdentity {
                kind: ArtifactComponentKind::Firmware,
                version: "1.0.0".parse().unwrap(),
                digest: Digest::sha256([9; 32]),
            },
            "test-rom".parse().unwrap(),
        )],
    };
    let definition = EnvironmentDefinition::new(spec.try_into().unwrap());
    let wire = proto::EnvironmentDefinition::from(&definition);
    assert_eq!(EnvironmentDefinition::try_from(&wire).unwrap(), definition);
    let requirements = definition.execution_requirements().unwrap();
    definition.check_execution(&requirements).unwrap();
    assert!(
        requirements
            .components()
            .iter()
            .all(|component| component.kind != ArtifactComponentKind::Kernel)
    );

    let mut spec = portable_spec(RuntimeKind::Gvisor);
    let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
        panic!("portable fixture")
    };
    artifacts.clear();
    let definition = EnvironmentDefinition::new(spec.try_into().unwrap());
    let wire = proto::EnvironmentDefinition::from(&definition);
    assert_eq!(EnvironmentDefinition::try_from(&wire).unwrap(), definition);
    assert!(
        definition
            .execution_requirements()
            .unwrap()
            .components()
            .is_empty()
    );
}

#[test]
fn incomplete_or_mixed_portable_profiles_are_refused() {
    for mut spec in [
        super::super::tests::e0().descriptor().spec().clone(),
        portable_spec(RuntimeKind::CloudHypervisor),
    ] {
        spec.guest.architecture = match spec.host.architecture {
            Arch::X86_64 => Arch::Aarch64,
            Arch::Aarch64 => Arch::X86_64,
        };
        assert!(matches!(
            EnvironmentDescriptor::try_from(spec),
            Err(EnvironmentDescriptorError::KvmArchitecture { .. })
        ));
    }
    for template in [
        super::super::tests::e0().descriptor().spec().clone(),
        portable_spec(RuntimeKind::CloudHypervisor),
    ] {
        for host_os in [HostOperatingSystem::Macos, HostOperatingSystem::Windows] {
            let mut spec = template.clone();
            spec.host.os = host_os;
            let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
            release.target = spec.host;
            assert!(matches!(
                EnvironmentDescriptor::try_from(spec),
                Err(EnvironmentDescriptorError::KvmHost(os)) if os == host_os
            ));
        }
    }
    type Mutation = fn(&mut proto::ArtifactBootProfileV1);
    let mutations: &[Mutation] = &[
        |boot| boot.contract.clear(),
        |boot| boot.contract = "not\na\ncontract".into(),
        |boot| boot.artifacts[0].role.clear(),
        |boot| boot.artifacts[0].format.clear(),
        |boot| boot.artifacts[0].component = None,
        |boot| boot.artifacts[0].component.as_mut().unwrap().digest.clear(),
        |boot| boot.artifacts[0].component.as_mut().unwrap().kind = 0,
        |boot| boot.artifacts.push(boot.artifacts[0].clone()),
    ];
    let definition = EnvironmentDefinition::new(
        portable_spec(RuntimeKind::CloudHypervisor)
            .try_into()
            .unwrap(),
    );
    for mutate in mutations {
        let mut wire = proto::EnvironmentDefinition::from(&definition);
        let proto::boot_profile::Profile::ArtifactsV1(boot) = wire
            .descriptor
            .as_mut()
            .unwrap()
            .boot
            .as_mut()
            .unwrap()
            .profile
            .as_mut()
            .unwrap()
        else {
            panic!("portable fixture")
        };
        mutate(boot);
        EnvironmentDefinition::try_from(&wire).unwrap_err();
    }
    let mut wire = proto::EnvironmentDefinition::from(&definition);
    wire.descriptor.as_mut().unwrap().compatibility =
        proto::EnvironmentDefinition::from(&super::super::tests::e0())
            .descriptor
            .unwrap()
            .compatibility;
    assert!(matches!(
        EnvironmentDefinition::try_from(&wire),
        Err(EnvironmentDecodeError::Descriptor(
            EnvironmentDescriptorError::MixedProfiles
        ))
    ));
}

#[test]
fn boot_roles_are_unique_and_order_does_not_change_identity() {
    let mut spec = portable_spec(RuntimeKind::CloudHypervisor);
    let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
        panic!("portable fixture")
    };
    artifacts.push(BootArtifact::new(
        "firmware".parse().unwrap(),
        ComponentIdentity {
            kind: ArtifactComponentKind::Firmware,
            version: "1.0.0".parse().unwrap(),
            digest: Digest::sha256([9; 32]),
        },
        "test-rom".parse().unwrap(),
    ));
    let first = EnvironmentDefinition::new(spec.clone().try_into().unwrap());
    let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
        panic!("portable fixture")
    };
    artifacts.reverse();
    assert_eq!(
        EnvironmentDefinition::new(spec.clone().try_into().unwrap()),
        first
    );
    let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
        panic!("portable fixture")
    };
    artifacts[1] = BootArtifact::new(
        artifacts[0].role().clone(),
        artifacts[1].component().clone(),
        artifacts[1].format().clone(),
    );
    assert!(matches!(
        EnvironmentDescriptor::try_from(spec),
        Err(EnvironmentDescriptorError::DuplicateBootRole(_))
    ));
}

#[test]
fn portable_identity_and_binding_include_contracts_roles_and_formats() {
    type Mutation = fn(&mut EnvironmentSpec);
    let cases: &[(&str, Mutation)] = &[
        ("boot contract", |spec| {
            if let BootProfile::ArtifactsV1 { contract, .. } = &mut spec.boot {
                *contract = "test-other-boot-v1".parse().unwrap();
            }
        }),
        ("machine contract", |spec| {
            if let ExecutionCompatibilityProfile::RuntimeV1 { contract, .. } =
                &mut spec.compatibility
            {
                *contract = "test-other-machine-v1".parse().unwrap();
            }
        }),
        ("role", |spec| {
            if let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot {
                artifacts[0] = BootArtifact::new(
                    "system-disk".parse().unwrap(),
                    artifacts[0].component().clone(),
                    artifacts[0].format().clone(),
                );
            }
        }),
        ("format", |spec| {
            if let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot {
                artifacts[0] = BootArtifact::new(
                    artifacts[0].role().clone(),
                    artifacts[0].component().clone(),
                    "other-image".parse().unwrap(),
                );
            }
        }),
    ];
    let original = EnvironmentDefinition::new(
        portable_spec(RuntimeKind::CloudHypervisor)
            .try_into()
            .unwrap(),
    );
    for (name, mutate) in cases {
        let mut spec = original.descriptor().spec().clone();
        mutate(&mut spec);
        let changed = EnvironmentDefinition::new(spec.try_into().unwrap());
        assert_ne!(changed.reference(), original.reference(), "{name}");
        let execution = original.execution_requirements().unwrap();
        assert!(
            matches!(
                changed.check_execution(&execution),
                Err(EnvironmentBindingError::Reference { .. })
            ),
            "{name}"
        );
    }
    let mut execution = original.execution_requirements().unwrap();
    let mut components = execution.components().to_vec();
    components[0].format = Some("wrong-format".parse().unwrap());
    execution = ExecutionMetadata::new(
        execution.runtime().clone(),
        execution.guest(),
        None,
        components,
    )
    .unwrap();
    assert!(matches!(
        original.check_cold_boot_source(&execution),
        Err(EnvironmentBindingError::BootComponents)
    ));
}
