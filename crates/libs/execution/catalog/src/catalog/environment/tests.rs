use super::*;

#[test]
fn compiled_pins_produce_the_e0_golden_definition() {
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let definition = initial.definition().unwrap();
    // execution-common checks canonical bytes; the catalog pins their reference.
    assert_eq!(
        definition.reference().to_string(),
        "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b"
    );
    assert_eq!(
        initial.resolve_definition(definition.reference()).unwrap(),
        definition
    );
    for digest in [
        initial.release().digest(),
        initial.release().archive_sha256,
        initial.kernel().sha256,
    ] {
        assert!(matches!(
            initial.resolve_definition(EnvironmentReference::new(digest)),
            Err(ResolveEnvironmentError::UnknownReference(_))
        ));
    }
}

#[test]
fn valid_but_unqualified_candidates_cannot_resolve() {
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let mut spec = initial.definition().unwrap().descriptor().spec().clone();
    let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
    let catalog = crate::catalog::RuntimeCatalog::compiled().unwrap();
    let candidate = catalog
        .releases(RuntimeKind::Firecracker, Arch::X86_64)
        .find(|r| r.release.version.as_str() == "1.17.0")
        .unwrap();
    release.runtime = candidate.release.clone();
    release.digest = candidate.digest();
    let definition = EnvironmentDefinition::new(spec.try_into().unwrap());
    assert!(matches!(
        initial.resolve_definition(definition.reference()),
        Err(ResolveEnvironmentError::UnknownReference(_))
    ));
    // The old aarch64 pins still exist; they do not implicitly qualify a new
    // descriptor on an architecture outside the qualified platforms.
    let arm = InitialEnvironment::for_architecture(Arch::Aarch64).unwrap();
    assert!(matches!(
        arm.definition(),
        Err(CompiledEnvironmentDefinitionError::UnsupportedArchitecture(
            Arch::Aarch64
        ))
    ));
}

#[test]
fn compiled_environments_have_complete_distinct_bindings_and_operation_support() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let initial = InitialEnvironment::for_architecture(Arch::X86_64)
        .unwrap()
        .definition()
        .unwrap();
    let references: std::collections::BTreeSet<_> = environments
        .iter()
        .map(|entry| entry.definition().reference())
        .collect();
    assert_eq!(references.len(), environments.len());
    assert_eq!(environments.len(), 6);
    for (environment, (runtime, kernel, state, memory_state)) in environments.iter().zip([
        ("1.15.1", "6.1.155", "9.0.0", true),
        ("1.17.0", "6.18.44", "12.0.0", true),
        ("1.17.0", "6.1.155", "12.0.0", true),
    ]) {
        let definition = environment.definition();
        let requirements = definition.execution_requirements().unwrap();
        assert_eq!(requirements.runtime().version.as_str(), runtime);
        assert_eq!(requirements.state_format().unwrap().version.as_str(), state);
        assert_eq!(
            requirements.components()[0]
                .version
                .as_ref()
                .unwrap()
                .as_str(),
            kernel
        );
        assert_eq!(
            requirements.components()[0].digest,
            Some(environment.kernel().unwrap().sha256)
        );
        assert_eq!(environment.supports_memory_state(), memory_state);
    }
    assert_eq!(environments[0].definition(), &initial);
    assert_eq!(
        environments[1].definition().reference().to_string(),
        "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
    );
    let arm = CompiledEnvironment::for_architecture(Arch::Aarch64).unwrap();
    assert!(arm.is_empty(), "unqualified ARM64 must not be admitted");
    let ch = CompiledEnvironment::cloud_hypervisor(
        &RuntimeCatalog::compiled().unwrap(),
        Arch::Aarch64,
        false,
    )
    .unwrap();
    assert!(!ch.supports_memory_state());
    let execution = ch.definition().execution_requirements().unwrap();
    assert!(execution.state_format().is_none());
    assert_eq!(
        execution.components()[0].digest,
        Some(ch.kernel().unwrap().sha256)
    );
    let x86_ch = &environments[3];
    assert_eq!(x86_ch.release().release.kind, RuntimeKind::CloudHypervisor);
    assert!(!x86_ch.supports_memory_state());
    let x86_execution = x86_ch.definition().execution_requirements().unwrap();
    assert_eq!(x86_execution.guest().architecture, Arch::X86_64);
    assert!(x86_execution.state_format().is_none());
    assert_ne!(x86_ch.definition().reference(), ch.definition().reference());
    assert_eq!(
        x86_ch.definition().reference().to_string(),
        "sha256:002536d84a16cadef74e47a9e59072b4eba0c4032cca8a4b2c37354fbe082fad"
    );
    let state_ch = &environments[4];
    assert!(state_ch.supports_memory_state());
    assert_ne!(
        state_ch.definition().reference(),
        x86_ch.definition().reference()
    );
}

#[test]
fn firmware_environment_has_no_kernel_and_is_not_a_default_or_snapshot_loader() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let entry = catalog
        .iter()
        .find(|e| e.name() == "ch-54.0-windows-uefi")
        .unwrap();
    let execution = entry.definition().execution_requirements().unwrap();
    assert!(entry.kernel().is_none());
    assert!(!entry.supports_memory_state());
    assert_eq!(execution.guest().os, GuestOperatingSystem::Windows);
    assert!(execution.state_format().is_none());
    assert_eq!(execution.components().len(), 1);
    assert_eq!(
        execution.components()[0].kind,
        ArtifactComponentKind::Firmware
    );
    assert_eq!(
        crate::guest::BootSource::from_execution(&execution).unwrap(),
        crate::guest::BootSource::Uefi
    );
    let wire = pplx_space_execution_common::proto::EnvironmentDefinition::from(entry.definition());
    assert_eq!(
        EnvironmentDefinition::try_from(&wire).unwrap(),
        *entry.definition()
    );
}

const E1: &str = "fc-1.17.0-linux-6.1.155";
const E2: &str = "fc-1.17.0-linux-6.18.44";
fn find<'a>(environments: &'a [CompiledEnvironment], name: &str) -> &'a CompiledEnvironment {
    environments
        .iter()
        .find(|entry| entry.name() == name)
        .unwrap()
}
#[test]
fn compiled_upgrade_edges_are_e0_to_e1_and_back() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let edges: std::collections::BTreeSet<_> = environments
        .iter()
        .flat_map(|entry| {
            entry.upgrade_targets().iter().map(|target| {
                let target = environments
                    .iter()
                    .find(|candidate| candidate.definition().reference() == *target)
                    .unwrap();
                (entry.name(), target.name())
            })
        })
        .collect();
    // E0 -> E1 changes the state format (9.0.0 to 12.0.0); a cold boot carries
    // no memory. The 6.18 kernel differs from E0's and E1's, so no edge reaches or
    // leaves it, and no environment is an edge to itself.
    assert_eq!(edges, [("e0", E1), (E1, "e0")].into_iter().collect());
    // The set hides repeats; each edge is recorded once, on its source.
    let recorded: usize = environments
        .iter()
        .map(|entry| entry.upgrade_targets().len())
        .sum();
    assert_eq!(recorded, edges.len());
    // An entry built outside the catalog carries no edges.
    assert!(
        InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .compiled_environment()
            .unwrap()
            .upgrade_targets()
            .is_empty()
    );
}
#[test]
fn upgrade_edges_that_break_the_cold_boot_rules_are_rejected() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let entries = || {
        ["e0", E1, E2].map(|name| {
            let mut entry = find(&environments, name).clone();
            entry.upgrade_targets.clear();
            entry
        })
    };
    for (edges, expected) in [
        (&[("e0", "missing")][..], "UnknownEnvironment"),
        (&[("missing", "e0")][..], "UnknownEnvironment"),
        (&[("e0", "e0")][..], "RuntimeUnchanged"),
        (&[("e0", E2)][..], "BootComponents"),
        (&[(E2, "e0")][..], "BootComponents"),
        // Same runtime and a different kernel: no runtime change to certify.
        (&[(E2, E1)][..], "RuntimeUnchanged"),
        (&[("e0", E1), ("e0", E1)][..], "Duplicate"),
    ] {
        let error = certify_upgrade_edges(&mut entries(), edges).unwrap_err();
        let matched = match &error.problem {
            UpgradeEdgeProblem::UnknownEnvironment => "UnknownEnvironment",
            UpgradeEdgeProblem::RuntimeUnchanged => "RuntimeUnchanged",
            UpgradeEdgeProblem::Duplicate => "Duplicate",
            UpgradeEdgeProblem::NotColdBootCompatible(EnvironmentBindingError::BootComponents) => {
                "BootComponents"
            }
            other => panic!("{edges:?}: {other}"),
        };
        assert_eq!(matched, expected, "{edges:?}");
    }
}
#[test]
fn an_invalid_edge_table_withdraws_every_edge_but_no_environment() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let mut entries = ["e0", E1].map(|name| {
        let mut entry = find(&environments, name).clone();
        entry.upgrade_targets.clear();
        entry
    });
    // The first edge is valid and is recorded before the second fails.
    let table = [("e0", E1), (E1, "missing")];
    let mut partial = entries.clone();
    certify_upgrade_edges(&mut partial, &table).unwrap_err();
    assert_eq!(partial[0].upgrade_targets().len(), 1);
    record_upgrade_edges(&mut entries, &table);
    assert!(
        entries
            .iter()
            .all(|entry| entry.upgrade_targets().is_empty())
    );
    record_upgrade_edges(&mut entries, &[("e0", E1)]);
    assert_eq!(
        entries[0].upgrade_targets(),
        [entries[1].definition().reference()]
    );
}
#[test]
fn memory_state_requires_qualification_and_a_declared_format() {
    for (qualified, has_format, supported) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, true),
    ] {
        let mut entry = InitialEnvironment::for_architecture(Arch::X86_64)
            .unwrap()
            .compiled_environment()
            .unwrap();
        let mut spec = entry.definition.descriptor().spec().clone();
        let state_format = has_format.then(|| spec.compatibility.state_format().unwrap().clone());
        let RuntimeBinding::ManagedRelease(release) = &mut spec.runtime;
        release.layout = RuntimePayloadLayout::CatalogReleaseV1;
        spec.boot = BootProfile::ArtifactsV1 {
            contract: "test-boot-v1".parse().unwrap(),
            artifacts: vec![],
        };
        spec.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
            contract: "test-machine-v1".parse().unwrap(),
            state_format,
        };
        entry.definition = EnvironmentDefinition::new(spec.try_into().unwrap());
        entry.memory_state = qualified;
        assert_eq!(
            entry.supports_memory_state(),
            supported,
            "qualified={qualified}, has_format={has_format}"
        );
    }
}

#[test]
fn an_execution_is_attributed_to_the_environment_it_ran_under() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = find(&environments, "e0");
    let e1 = find(&environments, E1);
    let attributed = |execution: &ExecutionMetadata| {
        CompiledEnvironment::of_execution(&environments, execution).map(CompiledEnvironment::name)
    };

    // Bound metadata names its environment.
    let bound = e1.definition().execution_requirements().unwrap();
    assert_eq!(attributed(&bound), Some(E1));

    // Unbound metadata that satisfies a definition belongs to it.
    let bound_e0 = e0.definition().execution_requirements().unwrap();
    let unbound = ExecutionMetadata::new(
        bound_e0.runtime().clone(),
        bound_e0.guest(),
        bound_e0.state_format().cloned(),
        bound_e0.components().to_vec(),
    )
    .unwrap();
    assert_eq!(unbound.environment(), None);
    assert_eq!(attributed(&unbound), Some("e0"));

    // A stock E0 snapshot from before boot components were recorded.
    let stock = ExecutionMetadata::new(
        bound_e0.runtime().clone(),
        bound_e0.guest(),
        bound_e0.state_format().cloned(),
        vec![],
    )
    .unwrap();
    assert_eq!(attributed(&stock), Some("e0"));

    // A historical contract no compiled environment describes.
    let custom = ExecutionMetadata::new(
        RuntimeIdentity {
            kind: RuntimeKind::Firecracker,
            version: "1.14.0".parse().unwrap(),
        },
        bound_e0.guest(),
        bound_e0.state_format().cloned(),
        vec![],
    )
    .unwrap();
    assert_eq!(attributed(&custom), None);
}
