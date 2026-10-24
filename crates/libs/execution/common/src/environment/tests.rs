use prost::Message;

use super::*;

const E0_REFERENCE: &str =
    "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b";
const E0_CANONICAL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../proto/execution/v1/fixtures/e0.canonical"
));

pub(super) fn e0() -> EnvironmentDefinition {
    let host = HostPlatform {
        os: HostOperatingSystem::Linux,
        architecture: Arch::X86_64,
    };
    EnvironmentDefinition::new(
        EnvironmentSpec {
            host,
            guest: GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: Arch::X86_64,
            },
            runtime: RuntimeBinding::ManagedRelease(ManagedRuntimeRelease {
                runtime: RuntimeIdentity {
                    kind: RuntimeKind::Firecracker,
                    version: "1.15.1".parse().unwrap(),
                },
                digest: "sha256:7225701e6d651bb351c6858e3587994fb75a9932fb16d744dfec30fb8bfdd027"
                    .parse()
                    .unwrap(),
                target: host,
                layout: RuntimePayloadLayout::FirecrackerReleaseV1,
            }),
            backend: HypervisorBackend::Kvm,
            boot: BootProfile::LinuxDirectV1 {
                kernel: ComponentIdentity {
                    kind: ArtifactComponentKind::Kernel,
                    version: "6.1.155".parse().unwrap(),
                    digest:
                        "sha256:e20e46d0c36c55c0d1014eb20576171b3f3d922260d9f792017aeff53af3d4f2"
                            .parse()
                            .unwrap(),
                },
            },
            isolation: IsolationClass::HardwareVm,
            compatibility: ExecutionCompatibilityProfile::FirecrackerSnapshotV1 {
                state_format: StateFormatIdentity {
                    format: "firecracker-snapshot".parse().unwrap(),
                    version: "9.0.0".parse().unwrap(),
                },
            },
        }
        .try_into()
        .unwrap(),
    )
}

#[test]
fn golden_identity_survives_protobuf_round_trip() {
    let definition = e0();
    assert_eq!(canonical_descriptor(definition.descriptor()), E0_CANONICAL);
    assert_eq!(definition.reference().to_string(), E0_REFERENCE);
    let bytes = proto::EnvironmentDefinition::from(&definition).encode_to_vec();
    let wire = proto::EnvironmentDefinition::decode(bytes.as_slice()).unwrap();
    assert_eq!(EnvironmentDefinition::try_from(&wire).unwrap(), definition);
}

#[test]
fn incomplete_or_unknown_wire_values_never_default_to_e0() {
    type Mutation = fn(&mut proto::EnvironmentDescriptor);
    let cases: &[(&str, Mutation)] = &[
        ("host", |s| s.host = None),
        ("guest", |s| s.guest = None),
        ("runtime", |s| s.runtime = None),
        ("provider", |s| s.runtime.as_mut().unwrap().provider = None),
        ("boot", |s| s.boot = None),
        ("boot profile", |s| s.boot.as_mut().unwrap().profile = None),
        ("compatibility", |s| s.compatibility = None),
        ("compatibility profile", |s| {
            s.compatibility.as_mut().unwrap().profile = None
        }),
        ("host OS", |s| s.host.as_mut().unwrap().os = 99),
        ("host architecture", |s| {
            s.host.as_mut().unwrap().architecture = 99
        }),
        ("guest OS", |s| s.guest.as_mut().unwrap().os = 99),
        ("guest architecture", |s| {
            s.guest.as_mut().unwrap().architecture = 99
        }),
        ("backend", |s| s.backend = 99),
        ("isolation", |s| s.isolation = 99),
        ("unspecified isolation", |s| s.isolation = 0),
        ("unspecified backend", |s| s.backend = 0),
    ];
    for (name, mutate) in cases {
        let mut wire = proto::EnvironmentDefinition::from(&e0());
        mutate(wire.descriptor.as_mut().unwrap());
        assert!(EnvironmentDefinition::try_from(&wire).is_err(), "{name}");
    }
    for schema in [0, 3, u32::MAX] {
        let mut wire = proto::EnvironmentDefinition::from(&e0());
        wire.schema_version = schema;
        assert!(
            matches!(EnvironmentDefinition::try_from(&wire), Err(EnvironmentDecodeError::UnsupportedSchema(v)) if v == schema)
        );
    }
    // Field 2 would be a future provider arm. Prost drops it; the domain
    // conversion must reject the resulting absent provider instead of defaulting.
    let mut wire = proto::EnvironmentDefinition::from(&e0());
    wire.descriptor.as_mut().unwrap().runtime =
        Some(proto::RuntimeBinding::decode(&b"\x12\x00"[..]).unwrap());
    assert!(matches!(
        EnvironmentDefinition::try_from(&wire),
        Err(EnvironmentDecodeError::Missing("runtime provider"))
    ));
}

#[test]
fn managed_release_requires_all_identity_and_target_fields() {
    type Mutation = fn(&mut proto::ManagedRuntimeRelease);
    let cases: &[(&str, Mutation)] = &[
        ("runtime", |r| r.runtime = None),
        ("runtime kind", |r| r.runtime.as_mut().unwrap().kind = 99),
        ("runtime version", |r| {
            r.runtime.as_mut().unwrap().version.clear()
        }),
        ("digest", |r| r.digest.clear()),
        ("target", |r| r.target = None),
        ("layout", |r| r.layout = 99),
        ("unspecified layout", |r| r.layout = 0),
    ];
    for (name, mutate) in cases {
        let mut wire = proto::EnvironmentDefinition::from(&e0());
        let proto::runtime_binding::Provider::ManagedRelease(release) = wire
            .descriptor
            .as_mut()
            .unwrap()
            .runtime
            .as_mut()
            .unwrap()
            .provider
            .as_mut()
            .unwrap();
        mutate(release);
        assert!(EnvironmentDefinition::try_from(&wire).is_err(), "{name}");
    }
}

#[test]
fn semantic_changes_cannot_retain_the_old_reference() {
    type Mutation = fn(&mut EnvironmentSpec);
    let cases: &[(&str, Mutation)] = &[
        ("runtime version", |s| {
            let RuntimeBinding::ManagedRelease(r) = &mut s.runtime;
            r.runtime.version = "1.17.0".parse().unwrap();
        }),
        ("runtime digest", |s| {
            let RuntimeBinding::ManagedRelease(r) = &mut s.runtime;
            r.digest = Digest::sha256([1; 32]);
        }),
        ("kernel version", |s| {
            let BootProfile::LinuxDirectV1 { kernel } = &mut s.boot else {
                panic!("expected schema-1 fixture")
            };
            kernel.version = "6.18.44".parse().unwrap();
        }),
        ("kernel digest", |s| {
            let BootProfile::LinuxDirectV1 { kernel } = &mut s.boot else {
                panic!("expected schema-1 fixture")
            };
            kernel.digest = Digest::sha256([2; 32]);
        }),
        ("state version", |s| {
            let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
                &mut s.compatibility
            else {
                panic!("expected schema-1 fixture")
            };
            state_format.version = "10.0.0".parse().unwrap();
        }),
        ("architecture", |s| {
            s.host.architecture = Arch::Aarch64;
            s.guest.architecture = Arch::Aarch64;
            let RuntimeBinding::ManagedRelease(r) = &mut s.runtime;
            r.target = s.host;
        }),
    ];
    for (name, mutate) in cases {
        let mut spec = e0().descriptor().spec().clone();
        mutate(&mut spec);
        let changed = EnvironmentDefinition::new(spec.try_into().unwrap());
        assert_ne!(changed.reference(), e0().reference(), "{name}");
        let mut wire = proto::EnvironmentDefinition::from(&changed);
        wire.reference = E0_REFERENCE.to_owned();
        assert!(
            matches!(
                EnvironmentDefinition::try_from(&wire),
                Err(EnvironmentDecodeError::ReferenceMismatch { .. })
            ),
            "{name}"
        );
    }
}

#[test]
fn host_identity_is_independent_but_unimplemented_combinations_are_rejected() {
    for os in [
        HostOperatingSystem::Linux,
        HostOperatingSystem::Windows,
        HostOperatingSystem::Macos,
    ] {
        let host = HostPlatform {
            os,
            architecture: Arch::X86_64,
        };
        assert_eq!(
            HostPlatform::try_from(&proto::HostPlatform::from(host)).unwrap(),
            host
        );
        let mut spec = e0().descriptor().spec().clone();
        spec.host = host;
        let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
        release.target = host;
        assert_eq!(
            EnvironmentDescriptor::try_from(spec).is_ok(),
            os == HostOperatingSystem::Linux
        );
    }
    for os in [GuestOperatingSystem::Windows, GuestOperatingSystem::Macos] {
        let mut spec = e0().descriptor().spec().clone();
        spec.guest.os = os;
        assert!(matches!(
            EnvironmentDescriptor::try_from(spec),
            Err(EnvironmentDescriptorError::UnsupportedCombination)
        ));
    }
    let mut spec = e0().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.target.os = HostOperatingSystem::Macos;
    assert!(matches!(
        EnvironmentDescriptor::try_from(spec),
        Err(EnvironmentDescriptorError::RuntimeTarget { .. })
    ));
}

#[test]
fn legacy_execution_metadata_retains_its_wire_meaning() {
    // Frozen pre-environment encoding: Firecracker 1.15.1, Linux/x86_64,
    // absent state format/components. It carries no environment approval.
    let bytes = b"\x0a\x0a\x08\x01\x12\x061.15.1\x12\x04\x08\x01\x10\x01";
    let wire = proto::ExecutionMetadata::decode(&bytes[..]).unwrap();
    let value = ExecutionMetadata::try_from(&wire).unwrap();
    assert_eq!(value.runtime().kind, RuntimeKind::Firecracker);
    assert_eq!(value.guest().architecture, Arch::X86_64);
    assert_eq!(
        proto::ExecutionMetadata::from(&value).encode_to_vec(),
        bytes
    );
    assert!(matches!(
        EnvironmentDefinition::try_from(&proto::EnvironmentDefinition::default()),
        Err(EnvironmentDecodeError::UnsupportedSchema(0))
    ));
}
