use super::*;

fn observed(definition: &EnvironmentDefinition) -> ExecutionMetadata {
    let spec = definition.descriptor().spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
    let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } = &spec.compatibility;
    ExecutionMetadata::new(
        release.runtime.clone(),
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
    .unwrap()
}

#[test]
fn binding_checks_observations_and_survives_wire_and_snapshot_round_trips() {
    let definition = super::super::tests::e0();
    let execution = definition.bind_execution(observed(&definition)).unwrap();
    assert_eq!(execution.environment(), Some(definition.reference()));
    assert_eq!(
        ExecutionMetadata::try_from(&proto::ExecutionMetadata::from(&execution)).unwrap(),
        execution
    );
    #[cfg(feature = "serde")]
    {
        let snapshot = SnapshotExecutionMetadata::new(execution.clone()).unwrap();
        let round_trip: SnapshotExecutionMetadata =
            serde_json::from_str(&serde_json::to_string(&snapshot).unwrap()).unwrap();
        assert_eq!(round_trip.execution(), &execution);
    }
    type Mutation = fn(&mut proto::ExecutionMetadata);
    for (name, mutate) in [
        (
            "runtime",
            (|e: &mut proto::ExecutionMetadata| {
                e.runtime.as_mut().unwrap().version = "1.17.0".into()
            }) as Mutation,
        ),
        ("guest", |e| {
            e.guest.as_mut().unwrap().os = proto::GuestOperatingSystem::Windows as i32
        }),
        ("state format", |e| e.state_format = None),
        ("kernel digest", |e| {
            e.components[0].digest = Some(Digest::sha256([0; 32]).to_string())
        }),
        ("kernel role", |e| e.components[0].role = None),
        ("missing kernel", |e| e.components.clear()),
        ("reference", |e| {
            e.environment = Some(EnvironmentReference::new(Digest::sha256([0; 32])).to_string())
        }),
    ] {
        let mut wire = proto::ExecutionMetadata::from(&execution);
        mutate(&mut wire);
        assert!(
            definition
                .bind_execution(ExecutionMetadata::try_from(&wire).unwrap())
                .is_err(),
            "{name}"
        );
    }
    let mut legacy = proto::ExecutionMetadata::from(&execution);
    legacy.components[0].version = Some("legacy-e20e46d0c36c".into());
    definition
        .bind_execution(ExecutionMetadata::try_from(&legacy).unwrap())
        .unwrap();
}

#[test]
fn cold_boot_source_ignores_producer_version_and_format_but_not_platform_or_kernel() {
    let definition = super::super::tests::e0();
    let source = proto::ExecutionMetadata::from(&observed(&definition));
    type Mutation = fn(&mut proto::ExecutionMetadata);
    let check = |mutate: Mutation| {
        let mut wire = source.clone();
        mutate(&mut wire);
        definition.check_cold_boot_source(&ExecutionMetadata::try_from(&wire).unwrap())
    };
    // The producer's runtime version and state format describe the producer,
    // not the disk, so a cold boot may change them.
    check(|e| e.runtime.as_mut().unwrap().version = "1.17.0".into()).unwrap();
    check(|e| e.state_format.as_mut().unwrap().version = "12.0.0".into()).unwrap();
    for (name, mutate, expected) in [
        (
            "runtime kind",
            (|e: &mut proto::ExecutionMetadata| {
                e.runtime.as_mut().unwrap().kind = proto::RuntimeKind::CloudHypervisor as i32
            }) as Mutation,
            "RuntimeOrGuest",
        ),
        (
            "guest",
            |e| e.guest.as_mut().unwrap().os = proto::GuestOperatingSystem::Windows as i32,
            "RuntimeOrGuest",
        ),
        (
            "kernel digest",
            |e| e.components[0].digest = Some(Digest::sha256([0; 32]).to_string()),
            "BootComponents",
        ),
        (
            "kernel role",
            |e| e.components[0].role = None,
            "BootComponents",
        ),
    ] {
        let found = match check(mutate) {
            Err(EnvironmentBindingError::RuntimeOrGuest) => "RuntimeOrGuest",
            Err(EnvironmentBindingError::BootComponents) => "BootComponents",
            other => panic!("{name}: {other:?}"),
        };
        assert_eq!(found, expected, "{name}");
    }
}
