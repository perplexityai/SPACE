use super::*;

fn observed(definition: &EnvironmentDefinition) -> ExecutionMetadata {
    let spec = definition.descriptor().spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let BootProfile::LinuxDirectV1 { kernel } = &spec.boot else {
        panic!("expected schema-1 fixture")
    };
    let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } = &spec.compatibility
    else {
        panic!("expected schema-1 fixture")
    };
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
