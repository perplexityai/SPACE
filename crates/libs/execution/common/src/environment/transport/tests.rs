use super::*;
use prost::Message;

#[test]
fn generic_firecracker_preserves_identity_and_refuses_retired_wire_fields() {
    let legacy = super::super::tests::e0();
    let modern = legacy.clone().into_runtime_neutral().unwrap();
    assert_eq!(
        legacy.firecracker_linux_contract(),
        modern.firecracker_linux_contract()
    );
    assert_eq!(modern.reference(), legacy.reference());
    assert_eq!(
        canonical_descriptor(modern.legacy_descriptor.as_ref().unwrap()),
        canonical_descriptor(legacy.descriptor())
    );
    assert!(matches!(
        modern.descriptor().spec().boot,
        BootProfile::ArtifactsV1 { .. }
    ));
    let response = proto::EnvironmentDefinition::from(&modern);
    assert_eq!(response.schema_version, 2);
    assert_eq!(response.identity_schema_version, 1);
    assert_eq!(response, proto::EnvironmentDefinition::from(&legacy));
    assert_eq!(EnvironmentDefinition::try_from(&response).unwrap(), modern);
    let mut old = response.clone();
    old.schema_version = 1;
    assert!(matches!(
        EnvironmentDefinition::try_from(&old),
        Err(EnvironmentDecodeError::UnsupportedSchema(1))
    ));
    // Retired field 1 is unknown, never reinterpreted as the generic profile.
    for boot in [true, false] {
        let mut old = response.clone();
        let descriptor = old.descriptor.as_mut().unwrap();
        if boot {
            descriptor.boot = Some(proto::BootProfile::decode(&b"\x0a\x00"[..]).unwrap());
        } else {
            descriptor.compatibility =
                Some(proto::ExecutionCompatibilityProfile::decode(&b"\x0a\x00"[..]).unwrap());
        }
        EnvironmentDefinition::try_from(&old).unwrap_err();
    }
    // Encoding migration must not tighten admission for historical metadata.
    let mut components = legacy
        .execution_requirements()
        .unwrap()
        .components()
        .to_vec();
    components[0].format = None;
    let required = legacy.execution_requirements().unwrap();
    let observed = ExecutionMetadata::new(
        required.runtime().clone(),
        required.guest(),
        required.state_format().cloned(),
        components,
    )
    .unwrap()
    .with_environment(legacy.reference());
    modern.check_execution(&observed).unwrap();
    #[cfg(feature = "serde")]
    assert_eq!(modern.to_document().unwrap(), legacy.to_document().unwrap());
}

#[test]
fn historical_identity_never_drops_generic_requirements() {
    let legacy = super::super::tests::e0();
    let modern = legacy.into_runtime_neutral().unwrap();
    type Mutation = fn(&mut proto::EnvironmentDefinition);
    let cases: &[Mutation] = &[
        |wire| wire.identity_schema_version = 99,
        |wire| wire.identity_schema_version = 0,
        |wire| {
            let proto::boot_profile::Profile::ArtifactsV1(boot) = wire
                .descriptor
                .as_mut()
                .unwrap()
                .boot
                .as_mut()
                .unwrap()
                .profile
                .as_mut()
                .unwrap();
            boot.contract = "different-boot-v1".into();
        },
        |wire| {
            let proto::execution_compatibility_profile::Profile::RuntimeV1(profile) = wire
                .descriptor
                .as_mut()
                .unwrap()
                .compatibility
                .as_mut()
                .unwrap()
                .profile
                .as_mut()
                .unwrap();
            profile.state_format = None;
        },
        |wire| {
            let proto::boot_profile::Profile::ArtifactsV1(boot) = wire
                .descriptor
                .as_mut()
                .unwrap()
                .boot
                .as_mut()
                .unwrap()
                .profile
                .as_mut()
                .unwrap();
            let mut extra = boot.artifacts[0].clone();
            extra.role = "extra".into();
            boot.artifacts.push(extra);
        },
    ];
    for mutate in cases {
        let mut wire = proto::EnvironmentDefinition::from(&modern);
        mutate(&mut wire);
        EnvironmentDefinition::try_from(&wire).unwrap_err();
    }
    // Native generic definitions cannot silently downgrade to a legacy identity.
    let native = EnvironmentDefinition::new(modern.descriptor().clone());
    assert_ne!(native.reference(), modern.reference());
    let wire = proto::EnvironmentDefinition::from(&native);
    assert_eq!(wire.schema_version, 2);
    assert_eq!(wire.identity_schema_version, 0);
}
