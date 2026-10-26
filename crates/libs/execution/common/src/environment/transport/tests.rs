use super::*;
use prost::Message;

#[test]
fn generic_firecracker_preserves_identity_and_negotiates_both_deployment_orders() {
    let legacy = super::super::tests::e0();
    let old_wire = proto::EnvironmentDefinition::from(&legacy);
    let modern = legacy.clone().into_runtime_neutral().unwrap();
    assert_eq!(modern.reference(), legacy.reference());
    assert_eq!(modern.canonical_identity(), legacy.canonical_identity());
    assert!(matches!(
        modern.descriptor().spec().boot,
        BootProfile::ArtifactsV1 { .. }
    ));
    for capability in [0, 1, 2] {
        let response = modern.for_peer(capability).unwrap();
        assert_eq!(response.schema_version, if capability < 2 { 1 } else { 2 });
        if capability < 2 {
            assert_eq!(response.encode_to_vec(), old_wire.encode_to_vec());
        } else {
            assert_eq!(response.identity_schema_version, 1);
        }
        assert_eq!(EnvironmentDefinition::try_from(&response).unwrap(), modern);
    }
    // A new caller must also accept the old node's response unchanged.
    assert_eq!(EnvironmentDefinition::try_from(&old_wire).unwrap(), modern);
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
                .unwrap()
            else {
                panic!("generic fixture")
            };
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
                .unwrap()
            else {
                panic!("generic fixture")
            };
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
                .unwrap()
            else {
                panic!("generic fixture")
            };
            let mut extra = boot.artifacts[0].clone();
            extra.role = "extra".into();
            boot.artifacts.push(extra);
        },
    ];
    for mutate in cases {
        let mut wire = modern.for_peer(2).unwrap();
        mutate(&mut wire);
        EnvironmentDefinition::try_from(&wire).unwrap_err();
    }
    // Native generic definitions cannot silently downgrade to a legacy identity.
    let native = EnvironmentDefinition::new(modern.descriptor().clone());
    assert_ne!(native.reference(), modern.reference());
    assert!(matches!(
        native.for_peer(0),
        Err(EnvironmentDecodeError::UnsupportedPeer)
    ));
}
