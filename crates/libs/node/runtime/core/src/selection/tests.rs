use super::*;
use crate::catalog::CompiledEnvironment;

#[test]
fn policy_defaults_to_initial_and_rejects_ambiguous_configuration() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    for (allowed, default, valid) in [
        (None, None, true),
        (Some("e0,fc-1.17.0-linux-6.1.155"), None, true),
        (
            Some("fc-1.17.0-linux-6.1.155"),
            Some("fc-1.17.0-linux-6.1.155"),
            true,
        ),
        (Some(""), None, false),
        (Some("e0,e0"), None, false),
        (Some("e0,legacy"), None, true),
        (Some(" e0 , legacy "), Some(" e0 "), true),
        (Some("e0, e0"), None, false),
        (Some("e0,  "), None, false),
        (Some("e0,legacy,legacy"), None, false),
        (Some("e0,legacy"), Some("legacy"), false),
        (Some("e0,"), None, false),
        (Some("unknown"), None, false),
        (Some("e0"), Some("fc-1.17.0-linux-6.1.155"), false),
        (None, Some(""), false),
    ] {
        let result = ExecutionPolicy::parse(allowed, default, &catalog);
        if valid {
            result.unwrap();
        } else {
            result.unwrap_err();
        }
    }
    let policy = ExecutionPolicy::parse(None, None, &catalog).unwrap();
    assert_eq!(
        policy.default_reference(),
        Some(catalog[0].definition().reference())
    );
    assert_eq!(policy.allowed_references().count(), 1);
    let arm = CompiledEnvironment::for_architecture(Arch::Aarch64).unwrap();
    let unsupported = ExecutionPolicy::parse(None, None, &arm).unwrap();
    assert_eq!(unsupported.default_reference(), None);
    assert_eq!(unsupported.allowed_references().count(), 0);
    assert!(unsupported.legacy_unbound_allowed());
    ExecutionPolicy::parse(Some("e0"), None, &arm).unwrap_err();
}

#[test]
fn policy_applies_to_exact_and_legacy_execution_without_rewriting_it() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let policy = ExecutionPolicy::parse(None, None, &catalog).unwrap();
    for entry in &catalog {
        let bound = entry.definition().execution_requirements().unwrap();
        let legacy = ExecutionMetadata::new(
            bound.runtime().clone(),
            bound.guest(),
            bound.state_format().cloned(),
            bound.components().to_vec(),
        )
        .unwrap();
        for execution in [&bound, &legacy] {
            for mode in [
                ExecutionSelectionMode::NewColdBoot,
                ExecutionSelectionMode::ExistingColdBoot,
                ExecutionSelectionMode::PersistedStateRestore,
            ] {
                let result = policy.check_execution(execution, mode);
                if entry.name() == "e0" {
                    result.unwrap();
                } else {
                    result.unwrap_err();
                }
            }
        }
    }
    let source = catalog[0].definition().execution_requirements().unwrap();
    let missing_kernel = ExecutionMetadata::new(
        source.runtime().clone(),
        source.guest(),
        source.state_format().cloned(),
        vec![],
    )
    .unwrap();
    policy
        .check_execution(
            &missing_kernel,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .unwrap();
    ExecutionPolicy::parse(Some("e0"), None, &catalog)
        .unwrap()
        .check_execution(&missing_kernel, ExecutionSelectionMode::NewColdBoot)
        .unwrap_err();
    let revoked = ExecutionPolicy::parse(
        Some("fc-1.17.0-linux-6.1.155"),
        Some("fc-1.17.0-linux-6.1.155"),
        &catalog,
    )
    .unwrap();
    revoked
        .check_execution(
            &missing_kernel,
            ExecutionSelectionMode::PersistedStateRestore,
        )
        .unwrap_err();
}

#[test]
fn historical_contracts_survive_default_policy_but_can_be_disabled() {
    for arch in [Arch::X86_64, Arch::Aarch64] {
        let catalog = CompiledEnvironment::for_architecture(arch).unwrap();
        let policy = ExecutionPolicy::parse(None, None, &catalog).unwrap();
        let initial = crate::catalog::InitialEnvironment::for_architecture(arch).unwrap();
        let historical = ExecutionMetadata::new(
            initial.release().release.clone(),
            GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: arch,
            },
            Some(StateFormatIdentity {
                format: "firecracker-snapshot".parse().unwrap(),
                version: "9.0.0".parse().unwrap(),
            }),
            vec![],
        )
        .unwrap();
        for mode in [
            ExecutionSelectionMode::NewColdBoot,
            ExecutionSelectionMode::ExistingColdBoot,
            ExecutionSelectionMode::PersistedStateRestore,
        ] {
            policy.check_execution(&historical, mode).unwrap();
        }
        // No environment is fabricated for ARM or historical custom inputs.
        assert!(historical.environment().is_none());
        if arch == Arch::X86_64 {
            let restricted = ExecutionPolicy::parse(Some("e0"), None, &catalog).unwrap();
            restricted
                .check_execution(&historical, ExecutionSelectionMode::ExistingColdBoot)
                .unwrap_err();
        }
    }
}
