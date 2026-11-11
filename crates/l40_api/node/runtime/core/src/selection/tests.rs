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

fn reference(catalog: &[CompiledEnvironment], name: &str) -> EnvironmentReference {
    catalog
        .iter()
        .find(|entry| entry.name() == name)
        .unwrap()
        .definition()
        .reference()
}

fn cluster(
    default: Option<EnvironmentReference>,
    executing: &[EnvironmentReference],
    legacy_unbound_allowed: Option<bool>,
) -> ClusterExecutionPolicy {
    ClusterExecutionPolicy {
        generation: 7,
        default: default.map(ClusterDefault::Environment),
        executing: executing.iter().copied().collect(),
        legacy_unbound_allowed,
    }
}

#[test]
fn cluster_policy_narrows_startup_settings_and_never_widens_them() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = reference(&catalog, "e0");
    let e1 = reference(&catalog, "fc-1.17.0-linux-6.1.155");
    let e2 = reference(&catalog, "fc-1.17.0-linux-6.18.44");
    let foreign: EnvironmentReference = format!("sha256:{}", "ab".repeat(32)).parse().unwrap();
    let startup =
        ExecutionPolicy::parse(Some("e0,fc-1.17.0-linux-6.1.155,legacy"), None, &catalog).unwrap();
    let usable = true;
    let allowed = |policy: &ExecutionPolicy| policy.allowed_references().collect::<Vec<_>>();

    // A default narrows to the intersection and moves the default.
    let narrowed = startup.narrow(&cluster(Some(e1), &[e1, e2, foreign], None), usable);
    assert_eq!(narrowed.default, DefaultOutcome::Adopted);
    assert_eq!(allowed(&narrowed.policy), vec![e1]);
    assert_eq!(narrowed.policy.default_reference(), Some(e1));
    assert_eq!(narrowed.policy.applied_generation(), Some(7));
    assert!(narrowed.policy.legacy_unbound_allowed());
    assert!(!narrowed.policy.allows(e2), "the cluster can't add E2");
    assert!(!narrowed.policy.allows(foreign));

    // Without a cluster default, the executing set narrows nothing.
    let unset = startup.narrow(&cluster(None, &[e1], None), usable);
    assert_eq!(unset.default, DefaultOutcome::Unset);
    assert_eq!(allowed(&unset.policy), allowed(&startup));
    assert_eq!(unset.policy.default_reference(), Some(e0));
    assert_eq!(unset.policy.applied_generation(), Some(7));

    // The cluster revokes legacy, and can't grant it.
    let revoked = startup.narrow(&cluster(None, &[], Some(false)), usable);
    assert!(!revoked.policy.legacy_unbound_allowed());
    let strict = ExecutionPolicy::parse(Some("e0"), None, &catalog).unwrap();
    let granted = strict.narrow(&cluster(Some(e0), &[e0], Some(true)), usable);
    assert!(!granted.policy.legacy_unbound_allowed());

    // A default the node doesn't allow is refused, never added.
    let e0_only = ExecutionPolicy::parse(Some("e0,legacy"), None, &catalog).unwrap();
    let refused = e0_only.narrow(&cluster(Some(e1), &[e0, e1], None), usable);
    assert_eq!(refused.default, DefaultOutcome::NotAllowed);
    assert!(refused.default.diverges());
    assert_eq!(allowed(&refused.policy), vec![e0]);
    assert_eq!(refused.policy.default_reference(), Some(e0));
}

#[test]
fn cluster_default_that_is_not_usable_is_refused() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = reference(&catalog, "e0");
    let e1 = reference(&catalog, "fc-1.17.0-linux-6.1.155");
    let startup =
        ExecutionPolicy::parse(Some("e0,fc-1.17.0-linux-6.1.155"), None, &catalog).unwrap();

    // The configured default still executes, so it stays.
    let kept = startup.narrow(&cluster(Some(e1), &[e0, e1], None), false);
    assert_eq!(kept.default, DefaultOutcome::Unusable);
    assert_eq!(kept.policy.default_reference(), Some(e0));
    assert!(!kept.policy.default_withdrawn());
    assert!(
        kept.policy.allows(e1),
        "bound E1 work still runs where installed"
    );

    // The cluster removed the configured default: no default at all, so
    // unbound new work is refused instead of running on a removed environment.
    let none = startup.narrow(&cluster(Some(e1), &[e1], None), false);
    assert_eq!(none.default, DefaultOutcome::Unusable);
    assert_eq!(none.policy.default_reference(), None);
    assert!(none.policy.default_withdrawn());
    assert_eq!(
        none.policy.allowed_references().collect::<Vec<_>>(),
        vec![e1]
    );
}

#[test]
fn narrowed_policy_refuses_unbound_work_matching_a_removed_environment() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = catalog.iter().find(|entry| entry.is_initial()).unwrap();
    let e1 = reference(&catalog, "fc-1.17.0-linux-6.1.155");
    let startup =
        ExecutionPolicy::parse(Some("e0,fc-1.17.0-linux-6.1.155,legacy"), None, &catalog).unwrap();
    let bound = e0.definition().execution_requirements().unwrap();
    let unbound = ExecutionMetadata::new(
        bound.runtime().clone(),
        bound.guest(),
        bound.state_format().cloned(),
        bound.components().to_vec(),
    )
    .unwrap();
    for metadata in [&bound, &unbound] {
        startup
            .check_execution(metadata, ExecutionSelectionMode::NewColdBoot)
            .unwrap();
    }
    // Blocking E0 stops bound and unbound E0 work alike, even with legacy on.
    let blocked = startup.narrow(&cluster(Some(e1), &[e1], None), true);
    assert!(blocked.policy.legacy_unbound_allowed());
    for metadata in [&bound, &unbound] {
        blocked
            .policy
            .check_execution(metadata, ExecutionSelectionMode::NewColdBoot)
            .unwrap_err();
    }
}

#[test]
fn cluster_policy_only_revokes_legacy_on_architectures_without_environments() {
    let arm = CompiledEnvironment::for_architecture(Arch::Aarch64).unwrap();
    let startup = ExecutionPolicy::parse(None, None, &arm).unwrap();
    let foreign: EnvironmentReference = format!("sha256:{}", "cd".repeat(32)).parse().unwrap();
    let kept = startup.narrow(&cluster(Some(foreign), &[foreign], None), true);
    assert_eq!(kept.default, DefaultOutcome::NotAllowed);
    assert_eq!(kept.policy.default_reference(), None);
    assert!(
        !kept.policy.default_withdrawn(),
        "the startup settings had no default to withdraw"
    );
    assert_eq!(kept.policy.allowed_references().count(), 0);
    assert!(kept.policy.legacy_unbound_allowed());
    let revoked = startup.narrow(&cluster(None, &[], Some(false)), true);
    assert!(!revoked.policy.legacy_unbound_allowed());
}

#[test]
fn an_unrecognized_cluster_default_still_narrows_and_is_refused() {
    let catalog = CompiledEnvironment::for_architecture(Arch::X86_64).unwrap();
    let e0 = reference(&catalog, "e0");
    let e1 = reference(&catalog, "fc-1.17.0-linux-6.1.155");
    let startup =
        ExecutionPolicy::parse(Some("e0,fc-1.17.0-linux-6.1.155"), None, &catalog).unwrap();
    let narrowed = startup.narrow(
        &ClusterExecutionPolicy {
            generation: 1,
            default: Some(ClusterDefault::Unrecognized),
            executing: [e0].into(),
            legacy_unbound_allowed: None,
        },
        true,
    );
    assert_eq!(narrowed.default, DefaultOutcome::NotAllowed);
    assert_eq!(
        narrowed.policy.allowed_references().collect::<Vec<_>>(),
        vec![e0]
    );
    assert!(!narrowed.policy.allows(e1), "the block still lands");
    assert_eq!(narrowed.policy.default_reference(), Some(e0));
}
