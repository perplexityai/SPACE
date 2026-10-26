use super::*;

#[test]
fn compiled_pins_produce_the_e0_golden_definition() {
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let definition = initial.definition().unwrap();
    assert_eq!(
        definition.canonical_identity(),
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
    assert_eq!(environments.len(), 5);
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
    let arm = CompiledEnvironment::for_architecture(Arch::Aarch64).unwrap();
    assert_eq!(arm.len(), 1);
    let ch = &arm[0];
    assert_eq!(ch.release().release.kind, RuntimeKind::CloudHypervisor);
    assert!(!ch.is_initial());
    assert!(!ch.supports_memory_state());
    let execution = ch.definition().execution_requirements().unwrap();
    assert!(execution.state_format().is_none());
    assert_eq!(execution.components()[0].digest, Some(ch.kernel().sha256));
    let policy = crate::selection::ExecutionPolicy::parse(None, None, &arm).unwrap();
    assert!(policy.default_reference().is_none());
    assert!(policy.legacy_unbound_allowed());
    assert!(!policy.allows(ch.definition().reference()));
    let enabled =
        crate::selection::ExecutionPolicy::parse(Some(ch.name()), Some(ch.name()), &arm).unwrap();
    enabled
        .check_execution(
            &execution,
            crate::runtime_inventory::ExecutionSelectionMode::NewColdBoot,
        )
        .unwrap();
    let x86_ch = &environments[3];
    assert_eq!(x86_ch.release().release.kind, RuntimeKind::CloudHypervisor);
    assert!(!x86_ch.supports_memory_state());
    let x86_execution = x86_ch.definition().execution_requirements().unwrap();
    assert_eq!(x86_execution.guest().architecture, Arch::X86_64);
    assert!(x86_execution.state_format().is_none());
    assert_ne!(x86_ch.definition().reference(), ch.definition().reference());
    let defaults = crate::selection::ExecutionPolicy::parse(None, None, &environments).unwrap();
    assert!(!defaults.allows(x86_ch.definition().reference()));
    let signed_boot = &environments[4];
    assert!(!defaults.allows(signed_boot.definition().reference()));
    assert!(!signed_boot.supports_memory_state());
    assert!(signed_boot.exclusive_device_profiles().is_empty());
    assert_ne!(signed_boot.release().digest(), x86_ch.release().digest());
    let execution = signed_boot.definition().execution_requirements().unwrap();
    let initramfs = execution
        .components()
        .iter()
        .find(|component| component.kind == ArtifactComponentKind::Initrd)
        .unwrap();
    assert_eq!(initramfs.role.as_ref().unwrap().as_str(), "initramfs");
    // The signed modules and kernel are one boot contract. Retaining the
    // environment reference cannot excuse omission of its initramfs.
    let incomplete = ExecutionMetadata::new(
        execution.runtime().clone(),
        execution.guest(),
        None,
        execution
            .components()
            .iter()
            .filter(|component| component.kind != ArtifactComponentKind::Initrd)
            .cloned()
            .collect(),
    )
    .unwrap()
    .with_environment(signed_boot.definition().reference());
    assert!(matches!(
        signed_boot.definition().check_execution(&incomplete),
        Err(pplx_space_execution_common::environment::EnvironmentBindingError::BootComponents)
    ));
}

#[test]
fn initial_identity_and_legacy_restore_do_not_depend_on_display_name() {
    use crate::runtime_inventory::ExecutionSelectionMode;
    use crate::selection::ExecutionPolicy;
    let initial = InitialEnvironment::for_architecture(Arch::X86_64).unwrap();
    let mut entry = initial.compiled_environment().unwrap();
    entry.name = "renamed-initial";
    let bound = entry.definition().execution_requirements().unwrap();
    let historical = ExecutionMetadata::new(
        bound.runtime().clone(),
        bound.guest(),
        bound.state_format().cloned(),
        vec![],
    )
    .unwrap();
    let policy = ExecutionPolicy::parse(Some("renamed-initial"), None, &[entry]).unwrap();
    assert!(!policy.legacy_unbound_allowed());
    assert_eq!(policy.default_reference(), bound.environment());
    policy
        .check_execution(&historical, ExecutionSelectionMode::PersistedStateRestore)
        .unwrap();
}

#[cfg(feature = "artifact-store")]
impl CompiledEnvironment {
    pub(crate) fn with_boot_artifact_for_test(
        mut self,
        pin: crate::catalog::PinnedBootArtifact,
        role: ArtifactComponentRole,
        format: ArtifactFormat,
    ) -> Self {
        let mut spec = self.definition.descriptor().spec().clone();
        let BootProfile::ArtifactsV1 { artifacts, .. } = &mut spec.boot else {
            panic!("test extension requires a runtime-neutral descriptor");
        };
        artifacts.push(BootArtifact::new(role, pin.identity().clone(), format));
        self.definition = EnvironmentDefinition::new(spec.try_into().unwrap());
        self.boot_artifacts.push(pin);
        self
    }
}

#[test]
fn device_development_profile_is_confined_to_the_hardened_boot_environment() {
    let environments = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let mut profiled = 0;
    for environment in environments {
        for profile in environment.exclusive_device_profiles() {
            profiled += 1;
            assert_eq!(
                environment.name(),
                "ch-54.0-linux-6.1.155-nvidia-580.178.04"
            );
            assert_eq!(profile.hardware().guest_visible_devices(), 1);
            assert!(
                matches!(&environment.definition().descriptor().spec().boot, BootProfile::ArtifactsV1 { artifacts, .. } if artifacts.len() == 2)
            );
        }
    }
    assert_eq!(profiled, usize::from(cfg!(feature = "gpu-development")));
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
