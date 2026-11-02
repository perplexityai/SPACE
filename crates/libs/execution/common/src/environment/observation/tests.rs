use pplx_space_common_core::types::*;
use prost::Message;

use super::*;

/// Decoded definitions are runtime-neutral, so fixtures start that way.
fn e0() -> EnvironmentDefinition {
    super::super::tests::e0().into_runtime_neutral().unwrap()
}

/// A second Firecracker release over E0's guest and kernel, as E1 is, built
/// directly in the generic representation.
fn runtime_variant(version: &str, state_version: &str) -> EnvironmentDefinition {
    let mut spec = e0().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    release.runtime.version = version.parse().unwrap();
    release.digest = Digest::sha256([version.len() as u8; 32]);
    spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
        contract: crate::environment::FIRECRACKER_MACHINE_CONTRACT
            .parse()
            .unwrap(),
        state_format: Some(StateFormatIdentity {
            format: FIRECRACKER_STATE_FORMAT.parse().unwrap(),
            version: state_version.parse().unwrap(),
        }),
    };
    let definition = EnvironmentDefinition::new(spec.try_into().unwrap());
    assert_eq!(
        definition.schema_version(),
        crate::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
    );
    definition
}

fn key(components: &[u32]) -> Option<VersionSortKey> {
    Some(VersionSortKey::new(components.to_vec()).unwrap())
}

fn sample() -> EnvironmentObservation {
    let e0 = e0();
    let e1 = runtime_variant("1.17.0", "12.0.0");
    let patch = runtime_variant("1.17.10", "12.0.0");
    EnvironmentObservation::new(
        [
            KnownEnvironment::new(e0.clone(), key(&[1, 15, 1]), [e1.reference()], []).unwrap(),
            KnownEnvironment::new(
                e1.clone(),
                key(&[1, 17, 0]),
                [e0.reference(), patch.reference()],
                [patch.reference()],
            )
            .unwrap(),
            KnownEnvironment::new(patch, None, [], []).unwrap(),
        ],
        [e1.reference(), e0.reference()],
    )
    .unwrap()
}

#[test]
fn observation_survives_the_wire_in_canonical_order() {
    let observation = sample();
    let wire = proto::EnvironmentObservation::from(&observation);
    assert_eq!(wire.schema_version, ENVIRONMENT_OBSERVATION_SCHEMA_VERSION);
    let known: Vec<_> = wire
        .known
        .iter()
        .map(|entry| entry.definition.as_ref().unwrap().reference.clone())
        .collect();
    let mut sorted = known.clone();
    sorted.sort();
    assert_eq!(known, sorted, "known entries are ordered by reference");
    let mut usable = wire.usable_environments.clone();
    usable.sort();
    assert_eq!(usable, wire.usable_environments);
    let e1 = runtime_variant("1.17.0", "12.0.0").reference().to_string();
    let e1_wire = wire
        .known
        .iter()
        .find(|entry| entry.definition.as_ref().unwrap().reference == e1)
        .unwrap();
    assert_eq!(e1_wire.runtime_version_sort_key, [1, 17, 0]);
    assert_eq!(e1_wire.upgrade_edges.len(), 2);
    assert_eq!(e1_wire.memory_edges.len(), 1);
    let bytes = wire.encode_to_vec();
    let decoded = proto::EnvironmentObservation::decode(bytes.as_slice()).unwrap();
    assert_eq!(
        EnvironmentObservation::try_from(&decoded).unwrap(),
        observation
    );
    // A release without a driver-defined order stays unordered, not zero.
    let patch = runtime_variant("1.17.10", "12.0.0").reference();
    assert_eq!(
        EnvironmentObservation::try_from(&decoded)
            .unwrap()
            .get(patch)
            .unwrap()
            .runtime_version_sort_key(),
        None
    );
}

#[test]
fn invalid_observations_are_rejected_whole() {
    type Mutation = fn(&mut proto::EnvironmentObservation);
    let e0_ref = || e0().reference().to_string();
    let e1_ref = || runtime_variant("1.17.0", "12.0.0").reference().to_string();
    let unknown = || runtime_variant("9.9.9", "12.0.0").reference().to_string();
    let entry = |observation: &proto::EnvironmentObservation, reference: String| {
        observation
            .known
            .iter()
            .position(|entry| entry.definition.as_ref().unwrap().reference == reference)
            .unwrap()
    };
    let cases: Vec<(&str, Mutation)> = vec![
        ("schema 0", |o| o.schema_version = 0),
        ("schema 2", |o| o.schema_version = 2),
        ("missing definition", |o| o.known[0].definition = None),
        ("tampered definition", |o| {
            let definition = o.known[0].definition.as_mut().unwrap();
            definition.reference = format!("sha256:{}", "22".repeat(32));
        }),
        ("duplicate known", |o| {
            let first = o.known[0].clone();
            o.known.push(first);
        }),
        ("sort key overflow", |o| {
            o.known[0].runtime_version_sort_key = vec![u32::MAX]
        }),
        ("malformed usable", |o| {
            o.usable_environments.push("e1".to_owned())
        }),
        ("malformed edge", |o| {
            o.known[0].upgrade_edges.push("e1".to_owned())
        }),
        ("duplicate usable", |o| {
            let first = o.usable_environments[0].clone();
            o.usable_environments.push(first);
        }),
    ];
    for (name, mutate) in cases {
        let mut wire = proto::EnvironmentObservation::from(&sample());
        mutate(&mut wire);
        assert!(EnvironmentObservation::try_from(&wire).is_err(), "{name}");
    }
    // Cases that need references computed at run time.
    let mut wire = proto::EnvironmentObservation::from(&sample());
    wire.usable_environments.push(unknown());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::UsableNotKnown(_))
    ));
    let mut wire = proto::EnvironmentObservation::from(&sample());
    let index = entry(&wire, e0_ref());
    wire.known[index].upgrade_edges.push(unknown());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::EdgeNotKnown { .. })
    ));
    let mut wire = proto::EnvironmentObservation::from(&sample());
    let index = entry(&wire, e0_ref());
    wire.known[index].upgrade_edges.push(e0_ref());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::SelfEdge(_))
    ));
    let mut wire = proto::EnvironmentObservation::from(&sample());
    let index = entry(&wire, e0_ref());
    wire.known[index].upgrade_edges.push(e1_ref());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::DuplicateEdge { .. })
    ));
    let mut wire = proto::EnvironmentObservation::from(&sample());
    let index = entry(&wire, e1_ref());
    wire.known[index].memory_edges.push(e1_ref());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::SelfEdge(_))
    ));
    // E0 -> E1 changes the snapshot format: an upgrade edge, never a memory one.
    let mut wire = proto::EnvironmentObservation::from(&sample());
    let index = entry(&wire, e0_ref());
    wire.known[index].upgrade_edges.clear();
    wire.known[index].memory_edges.push(e1_ref());
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::MemoryEdgeWithoutUpgrade { .. })
    ));
}

#[test]
fn empty_observation_is_a_report_not_an_absence() {
    let empty = EnvironmentObservation::new([], []).unwrap();
    let inventory = proto::NodeExecutionInventory {
        environments: Some((&empty).into()),
        ..Default::default()
    };
    let decoded = EnvironmentObservation::from_inventory(&inventory)
        .unwrap()
        .expect("an empty catalog is still an observation");
    assert_eq!(decoded.known().len(), 0);
    assert!(decoded.usable().is_empty());
}

/// Field-for-field copies of the messages as nodes and cluster-api built them
/// before environment observations and sort keys existed.
mod before {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct RuntimeInstallation {
        #[prost(message, optional, tag = "1")]
        pub runtime: Option<crate::proto::RuntimeIdentity>,
        #[prost(message, repeated, tag = "2")]
        pub supported_guests: Vec<crate::proto::GuestPlatform>,
        #[prost(message, repeated, tag = "3")]
        pub state_formats: Vec<crate::proto::StateFormatCapability>,
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct NodeExecutionInventory {
        #[prost(message, repeated, tag = "1")]
        pub runtimes: Vec<RuntimeInstallation>,
        #[prost(message, repeated, tag = "2")]
        pub components: Vec<crate::proto::ArtifactComponentInstallation>,
        #[prost(message, optional, tag = "3")]
        pub policy: Option<crate::proto::ExecutionPolicyObservation>,
    }
}

fn runtime_installation(version: &str, sort_key: Vec<u32>) -> proto::RuntimeInstallation {
    proto::RuntimeInstallation {
        runtime: Some(proto::RuntimeIdentity {
            kind: proto::RuntimeKind::Firecracker as i32,
            version: version.to_owned(),
        }),
        supported_guests: vec![proto::GuestPlatform {
            os: proto::GuestOperatingSystem::Linux as i32,
            architecture: proto::CpuArchitecture::X8664 as i32,
        }],
        state_formats: vec![proto::StateFormatCapability {
            identity: Some(proto::StateFormatIdentity {
                format: FIRECRACKER_STATE_FORMAT.to_owned(),
                version: "9.0.0".to_owned(),
            }),
            can_create: true,
            can_load: true,
        }],
        version_sort_key: sort_key,
    }
}

fn policy() -> proto::ExecutionPolicyObservation {
    proto::ExecutionPolicyObservation {
        schema_version: EXECUTION_POLICY_SCHEMA_VERSION,
        allowed_environment_references: vec![e0().reference().to_string()],
        default_environment_reference: Some(e0().reference().to_string()),
        legacy_unbound_allowed: true,
        ..Default::default()
    }
}

#[test]
fn readers_built_before_observations_ignore_them_and_keep_the_rest() {
    let current = proto::NodeExecutionInventory {
        runtimes: vec![runtime_installation("1.15.1", vec![1, 15, 1])],
        components: Vec::new(),
        policy: Some(policy()),
        environments: Some((&sample()).into()),
    };
    let bytes = current.encode_to_vec();
    let old = before::NodeExecutionInventory::decode(bytes.as_slice()).unwrap();
    assert_eq!(old.policy, current.policy);
    assert_eq!(old.runtimes.len(), 1);
    assert_eq!(old.runtimes[0].runtime, current.runtimes[0].runtime);
    assert_eq!(
        old.runtimes[0].state_formats,
        current.runtimes[0].state_formats
    );
}

#[test]
fn inventories_from_nodes_before_observations_read_as_unknown() {
    let old = before::NodeExecutionInventory {
        runtimes: vec![before::RuntimeInstallation {
            runtime: Some(proto::RuntimeIdentity {
                kind: proto::RuntimeKind::Firecracker as i32,
                version: "1.15.1".to_owned(),
            }),
            supported_guests: Vec::new(),
            state_formats: Vec::new(),
        }],
        components: Vec::new(),
        policy: Some(policy()),
    };
    let current = proto::NodeExecutionInventory::decode(old.encode_to_vec().as_slice()).unwrap();
    assert_eq!(current.policy, old.policy);
    assert!(current.environments.is_none());
    assert!(current.runtimes[0].version_sort_key.is_empty());
    assert_eq!(
        EnvironmentObservation::from_inventory(&current).unwrap(),
        None,
        "an old node reports no environment evidence, not an empty catalog"
    );
}

#[test]
fn observations_carry_generic_definitions_only() {
    let historical = super::super::tests::e0();
    assert_eq!(historical.schema_version(), 1);
    assert!(matches!(
        KnownEnvironment::new(historical.clone(), None, [], []),
        Err(EnvironmentObservationError::LegacyDefinition(_))
    ));
    // The same identity in the generic representation is accepted.
    let generic = historical.into_runtime_neutral().unwrap();
    assert_eq!(generic.reference(), e0().reference());
    KnownEnvironment::new(generic, None, [], []).unwrap();
    // A reader rejects a schema-1 definition on the wire before decoding it.
    let mut wire = proto::EnvironmentObservation::from(&sample());
    wire.known[0].definition.as_mut().unwrap().schema_version = 1;
    assert!(matches!(
        EnvironmentObservation::try_from(&wire),
        Err(EnvironmentObservationError::LegacyDefinition(_))
    ));
}
