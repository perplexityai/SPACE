use pplx_space_execution_common::environment::canonical_descriptor;

use super::*;

#[test]
fn compiled_pins_produce_the_e0_golden_definition() {
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let definition = initial.definition().unwrap();
    assert_eq!(
        canonical_descriptor(definition.descriptor()),
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../../../../proto/execution/v1/fixtures/e0.canonical"
        ))
    );
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
    assert_eq!(environments.len(), 3);
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
            Some(environment.kernel().sha256)
        );
        assert_eq!(environment.supports_memory_state(), memory_state);
    }
    assert_eq!(environments[0].definition(), &initial);
    assert_eq!(
        environments[1].definition().reference().to_string(),
        "sha256:959f1181ac69f0dde7125fbc81be6fcd75b64e9e06e7a393f067b96eb6fb2ac4"
    );
    assert!(
        CompiledEnvironment::for_architecture(Arch::Aarch64)
            .unwrap()
            .is_empty()
    );
}
