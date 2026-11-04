use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;

fn reference(byte: char) -> EnvironmentReference {
    format!("sha256:{}", byte.to_string().repeat(64))
        .parse()
        .unwrap()
}

fn e0() -> EnvironmentReference {
    reference('0')
}
fn e1() -> EnvironmentReference {
    reference('1')
}
/// Same runtime as E1, another kernel.
fn e2() -> EnvironmentReference {
    reference('2')
}

fn environment(
    reference: EnvironmentReference,
    runtime_version: &str,
    kernel_version: &str,
    state_format_version: &str,
) -> CatalogEnvironment {
    CatalogEnvironment {
        reference,
        runtime: "firecracker".into(),
        runtime_version: runtime_version.into(),
        kernel_version: kernel_version.into(),
        state_format_name: "firecracker-snapshot".into(),
        state_format_version: state_format_version.into(),
        upgrade_edges: BTreeSet::new(),
        memory_edges: BTreeSet::new(),
    }
}

fn catalog() -> Vec<CatalogEnvironment> {
    vec![
        environment(e0(), "1.15.1", "6.1.155", "9.0.0"),
        environment(e1(), "1.17.0", "6.1.155", "12.0.0"),
        environment(e2(), "1.17.0", "6.18.44", "12.0.0"),
    ]
}

fn with_edges(
    mut environment: CatalogEnvironment,
    upgrade: &[EnvironmentReference],
    memory: &[EnvironmentReference],
) -> CatalogEnvironment {
    environment.upgrade_edges = upgrade.iter().copied().collect();
    environment.memory_edges = memory.iter().copied().collect();
    environment
}

fn policy(
    default: Option<EnvironmentReference>,
    states: &[(EnvironmentReference, RuntimeEnvironmentState)],
) -> RuntimeEnvironmentPolicy {
    RuntimeEnvironmentPolicy {
        generation: 7,
        default_environment: default,
        legacy_unbound_allowed: None,
        environments: states.iter().copied().collect::<BTreeMap<_, _>>(),
    }
}

fn unbound(
    runtime_version: Option<&str>,
    kernel_version: Option<&str>,
    state_format_version: Option<&str>,
) -> RecordedExecution {
    RecordedExecution {
        environment: None,
        runtime: runtime_version.map(|_| "firecracker".into()),
        runtime_version: runtime_version.map(Into::into),
        kernel_version: kernel_version.map(Into::into),
        state_format_name: state_format_version.map(|_| "firecracker-snapshot".into()),
        state_format_version: state_format_version.map(Into::into),
    }
}

/// Legacy unbound work is permitted unless the cluster disables it, which is
/// node-manager's default too.
const LEGACY_PERMITTED: Option<bool> = None;
const LEGACY_DISABLED: Option<bool> = Some(false);

#[test]
fn a_bound_record_is_its_environment_whatever_the_catalog_holds() {
    let bound = RecordedExecution {
        environment: Some(e2().to_string()),
        ..unbound(Some("1.15.1"), None, Some("9.0.0"))
    };
    assert_eq!(attribute(&bound, &catalog(), LEGACY_PERMITTED), Some(e2()));
    assert_eq!(attribute(&bound, &[], LEGACY_DISABLED), Some(e2()));
    // A binding that doesn't parse attributes to nothing rather than to the
    // record's legacy columns.
    let garbled = RecordedExecution {
        environment: Some("sha256:nope".into()),
        ..unbound(Some("1.15.1"), Some("6.1.155"), Some("9.0.0"))
    };
    assert_eq!(attribute(&garbled, &catalog(), LEGACY_DISABLED), None);
}

#[test]
fn an_unbound_record_counts_only_as_an_exact_match() {
    let catalog = catalog();
    for legacy in [LEGACY_PERMITTED, Some(true), LEGACY_DISABLED] {
        // Every identity field, matching one environment.
        assert_eq!(
            attribute(
                &unbound(Some("1.15.1"), Some("6.1.155"), Some("9.0.0")),
                &catalog,
                legacy
            ),
            Some(e0())
        );
        assert_eq!(
            attribute(
                &unbound(Some("1.17.0"), Some("6.18.44"), Some("12.0.0")),
                &catalog,
                legacy
            ),
            Some(e2())
        );
        // A record missing a field is no exact match, even where the rest
        // matches one environment: node-manager would run it as a historical
        // contract, or refuse it, without consulting that environment.
        for partial in [
            unbound(Some("1.15.1"), None, Some("9.0.0")),
            unbound(Some("1.17.0"), Some("6.1.155"), None),
        ] {
            assert_eq!(attribute(&partial, &catalog, legacy), None, "{partial:?}");
        }
        // Nothing matches.
        assert_eq!(
            attribute(
                &unbound(Some("1.15.1"), Some("6.18.44"), Some("9.0.0")),
                &catalog,
                legacy
            ),
            None
        );
    }
}

#[test]
fn a_record_from_before_execution_metadata_is_e0_only_without_legacy_work() {
    let pre_metadata = RecordedExecution::default();
    // With legacy unbound work permitted, node-manager restores it as a
    // historical contract whatever E0's state, so the control plane leaves
    // it to the node.
    for legacy in [LEGACY_PERMITTED, Some(true)] {
        assert_eq!(attribute(&pre_metadata, &catalog(), legacy), None);
    }
    // Without it, node-manager admits such a checkpoint only as E0.
    assert_eq!(
        attribute(&pre_metadata, &catalog(), LEGACY_DISABLED),
        Some(e0())
    );
    assert_eq!(
        attribute(&pre_metadata, &catalog()[1..], LEGACY_DISABLED),
        None
    );
    // A kernel the record names isn't replaced by E0's.
    let custom_kernel = RecordedExecution {
        kernel_version: Some("6.18.44".into()),
        ..RecordedExecution::default()
    };
    assert_eq!(attribute(&custom_kernel, &catalog(), LEGACY_DISABLED), None);
}

fn plans(
    policy: &RuntimeEnvironmentPolicy,
    from: EnvironmentReference,
    from_catalog: Option<&CatalogEnvironment>,
) -> (RuntimePlan, RuntimePlan) {
    (
        decide(policy, UpgradeActivation::Start, from, from_catalog),
        decide(policy, UpgradeActivation::Restore, from, from_catalog),
    )
}

#[test]
fn without_a_default_or_on_an_executing_environment_nothing_moves() {
    use RuntimeEnvironmentState::{Allowed, Blocked};
    let e0_with_edges = with_edges(catalog().remove(0), &[e1()], &[e1()]);
    let as_recorded = (RuntimePlan::AsRecorded, RuntimePlan::AsRecorded);
    // Bootstrap: no default, so even a blocked row narrows nothing.
    assert_eq!(
        plans(
            &policy(None, &[(e0(), Blocked)]),
            e0(),
            Some(&e0_with_edges)
        ),
        as_recorded
    );
    // On the default.
    assert_eq!(
        plans(
            &policy(Some(e0()), &[(e0(), Allowed)]),
            e0(),
            Some(&e0_with_edges)
        ),
        as_recorded
    );
    // On another allowed environment.
    assert_eq!(
        plans(
            &policy(Some(e1()), &[(e0(), Allowed), (e1(), Allowed)]),
            e0(),
            Some(&e0_with_edges)
        ),
        as_recorded
    );
}

#[test]
fn a_deprecated_environment_moves_only_stopped_sandboxes_and_only_along_an_edge() {
    use RuntimeEnvironmentState::{Allowed, Deprecated};
    let policy = policy(Some(e1()), &[(e0(), Deprecated), (e1(), Allowed)]);
    let to_default = EnvironmentMove {
        from: e0(),
        to: e1(),
        cause: UpgradeCause::Deprecated,
    };
    let e0_with_edges = with_edges(catalog().remove(0), &[e1()], &[e1()]);
    assert_eq!(
        plans(&policy, e0(), Some(&e0_with_edges)),
        (
            RuntimePlan::ColdBootUpgrade(to_default),
            // A suspended sandbox keeps its memory on a deprecated
            // environment, memory edge or not.
            RuntimePlan::AsRecorded
        )
    );
    // Without an edge it still executes where it is.
    assert_eq!(
        plans(&policy, e0(), Some(&catalog()[0])),
        (RuntimePlan::AsRecorded, RuntimePlan::AsRecorded)
    );
    assert_eq!(
        plans(&policy, e0(), None),
        (RuntimePlan::AsRecorded, RuntimePlan::AsRecorded)
    );
}

#[test]
fn a_blocked_environment_moves_every_activation_or_strands_it() {
    use RuntimeEnvironmentState::{Allowed, Blocked};
    let policy = policy(Some(e1()), &[(e0(), Blocked), (e1(), Allowed)]);
    let to_default = EnvironmentMove {
        from: e0(),
        to: e1(),
        cause: UpgradeCause::Blocked,
    };
    // A memory edge restores onto the default; a start, which loads no
    // memory, cold-boots along the same edge.
    let memory = with_edges(catalog().remove(0), &[e1()], &[e1()]);
    assert_eq!(
        plans(&policy, e0(), Some(&memory)),
        (
            RuntimePlan::ColdBootUpgrade(to_default),
            RuntimePlan::RestoreOnDefault(to_default)
        )
    );
    // An upgrade edge alone cold-boots both.
    let upgrade = with_edges(catalog().remove(0), &[e1()], &[]);
    assert_eq!(
        plans(&policy, e0(), Some(&upgrade)),
        (
            RuntimePlan::ColdBootUpgrade(to_default),
            RuntimePlan::ColdBootUpgrade(to_default)
        )
    );
    // Edges that don't reach the default don't count.
    let elsewhere = with_edges(catalog().remove(0), &[e2()], &[e2()]);
    for from_catalog in [Some(&elsewhere), Some(&catalog()[0]), None] {
        assert_eq!(
            plans(&policy, e0(), from_catalog),
            (
                RuntimePlan::Stranded(to_default),
                RuntimePlan::Stranded(to_default)
            )
        );
    }
}

#[test]
fn an_environment_without_a_state_doesnt_execute_once_there_is_a_default() {
    use RuntimeEnvironmentState::Allowed;
    let policy = policy(Some(e1()), &[(e1(), Allowed)]);
    let to_default = EnvironmentMove {
        from: e0(),
        to: e1(),
        cause: UpgradeCause::Unlisted,
    };
    // A stopped sandbox loses nothing by moving, so an upgrade edge moves it.
    let upgrade = with_edges(catalog().remove(0), &[e1()], &[]);
    assert_eq!(
        plans(&policy, e0(), Some(&upgrade)).0,
        RuntimePlan::ColdBootUpgrade(to_default)
    );
    // A suspended one keeps its memory along a memory edge.
    let memory = with_edges(catalog().remove(0), &[e1()], &[e1()]);
    assert_eq!(
        plans(&policy, e0(), Some(&memory)).1,
        RuntimePlan::RestoreOnDefault(to_default)
    );
    assert_eq!(
        plans(&policy, e0(), None),
        (
            RuntimePlan::Stranded(to_default),
            RuntimePlan::Stranded(to_default)
        )
    );
}

#[test]
fn discarding_memory_needs_an_operator_block_naming_the_environment() {
    // Setting the first default leaves every other environment without a
    // state, which no operator wrote about them. A suspended sandbox there is
    // stranded, which allowing its environment undoes, rather than cold-booted
    // with its memory discarded, which nothing undoes.
    use RuntimeEnvironmentState::{Allowed, Blocked};
    let upgrade = with_edges(catalog().remove(0), &[e1()], &[]);
    let unlisted = policy(Some(e1()), &[(e1(), Allowed)]);
    assert_eq!(
        decide(&unlisted, UpgradeActivation::Restore, e0(), Some(&upgrade)),
        RuntimePlan::Stranded(EnvironmentMove {
            from: e0(),
            to: e1(),
            cause: UpgradeCause::Unlisted,
        })
    );
    // An explicit block is the operator's consent, under an authorization
    // that names the blocked environment.
    let blocked = policy(Some(e1()), &[(e0(), Blocked), (e1(), Allowed)]);
    let RuntimePlan::ColdBootUpgrade(to_default) =
        decide(&blocked, UpgradeActivation::Restore, e0(), Some(&upgrade))
    else {
        panic!("a blocked environment's resume moves along its upgrade edge");
    };
    let authorization = to_default.authorization(UpgradeActivation::Restore);
    assert!(authorization.memory_loss);
    assert_eq!(
        authorization.authorization_id,
        format!("runtime-environment-blocked:{}", e0())
    );
}

#[test]
fn only_a_suspended_sandbox_moved_off_a_blocked_environment_loses_memory() {
    let to = |cause| EnvironmentMove {
        from: e0(),
        to: e1(),
        cause,
    };
    for (cause, activation, memory_loss) in [
        (UpgradeCause::Blocked, UpgradeActivation::Restore, true),
        // Never decided, but never consent either: no operator wrote it.
        (UpgradeCause::Unlisted, UpgradeActivation::Restore, false),
        (UpgradeCause::Blocked, UpgradeActivation::Start, false),
        (UpgradeCause::Unlisted, UpgradeActivation::Start, false),
        (UpgradeCause::Deprecated, UpgradeActivation::Start, false),
        // Never decided, but never consent either.
        (UpgradeCause::Deprecated, UpgradeActivation::Restore, false),
    ] {
        let authorization = to(cause).authorization(activation);
        assert_eq!(
            authorization.memory_loss, memory_loss,
            "{cause} {activation:?}"
        );
        assert_eq!(
            authorization.authorization_id,
            format!("runtime-environment-{cause}:{}", e0())
        );
        assert!(authorization.reason.contains(&e1().to_string()));
        // sandbox-manager's bounds.
        assert!(authorization.authorization_id.len() <= 256);
        assert!(authorization.reason.len() <= 1024);
    }
}

#[test]
fn a_retry_names_the_same_operation_and_a_new_subject_a_new_one() {
    let sandbox = SandboxId::new();
    let to_default = EnvironmentMove {
        from: e0(),
        to: e1(),
        cause: UpgradeCause::Blocked,
    };
    let operation = to_default.operation_id(sandbox, UpgradeActivation::Restore, "snapshot-a");
    assert_eq!(
        operation,
        to_default.operation_id(sandbox, UpgradeActivation::Restore, "snapshot-a")
    );
    for other in [
        to_default.operation_id(sandbox, UpgradeActivation::Restore, "snapshot-b"),
        to_default.operation_id(sandbox, UpgradeActivation::Start, "snapshot-a"),
        to_default.operation_id(SandboxId::new(), UpgradeActivation::Restore, "snapshot-a"),
        EnvironmentMove {
            to: e2(),
            ..to_default
        }
        .operation_id(sandbox, UpgradeActivation::Restore, "snapshot-a"),
        // A node keeps a settled upgrade's record, and refuses a retry under
        // another authorization, so a changed cause names a new upgrade.
        EnvironmentMove {
            cause: UpgradeCause::Unlisted,
            ..to_default
        }
        .operation_id(sandbox, UpgradeActivation::Restore, "snapshot-a"),
    ] {
        assert_ne!(operation, other);
    }
    // The authorization names the environment's state, not a policy
    // generation, so a retry after an unrelated policy write names the same
    // upgrade under the same authorization.
    let checkpoint = pplx_space_common_core::types::LocalArtifactId::new();
    let upgrade = to_default.wake_upgrade(
        sandbox,
        UpgradeActivation::Restore,
        "snapshot-a",
        Some(checkpoint),
    );
    assert_eq!(
        upgrade.checkpoint_local_artifact_id,
        Some(checkpoint.to_string())
    );
    assert_eq!(upgrade.operation_id, operation.to_string());
    assert_eq!(upgrade.target_environment, e1().to_string());
    assert!(upgrade.authorization.unwrap().memory_loss);
}

#[test]
fn a_stranded_refusal_is_a_failed_precondition_naming_both_environments() {
    let sandbox = SandboxId::new();
    let error = stranded_error(
        sandbox,
        EnvironmentMove {
            from: e0(),
            to: e1(),
            cause: UpgradeCause::Blocked,
        },
    );
    let super::super::SubstrateError::FailedPrecondition { message } = &error else {
        panic!("{error:?}");
    };
    assert!(message.contains(&e0().to_string()), "{message}");
    assert!(message.contains(&e1().to_string()), "{message}");
    assert!(message.contains(&sandbox.to_string()), "{message}");
    // Not a retryable answer: no Retry-After.
    let controller = crate::controllers::ControllerError::from(error);
    assert_eq!(
        controller.status,
        axum::http::StatusCode::PRECONDITION_FAILED
    );
    assert!(controller.retry_after().is_none());
}

#[test]
fn the_switch_is_off_unless_set() {
    assert!(!RuntimeUpgradeAtActivation::default().is_enabled());
    assert!(RuntimeUpgradeAtActivation::from_enabled(true).is_enabled());
    assert!(!RuntimeUpgradeAtActivation::from_enabled(false).is_enabled());
}

#[tokio::test(start_paused = true)]
async fn the_policy_is_reused_until_it_ages_and_a_failed_read_is_not_cached() {
    let cache = RuntimePolicyCache::default();
    let reads = AtomicUsize::new(0);
    let read = |generation: u64| {
        reads.fetch_add(1, Ordering::SeqCst);
        async move {
            let mut read = policy(None, &[]);
            read.generation = generation;
            Ok::<_, ()>(read)
        }
    };
    assert_eq!(cache.get(|| read(1)).await.unwrap().generation, 1);
    tokio::time::advance(POLICY_CACHE_TTL - Duration::from_millis(1)).await;
    assert_eq!(cache.get(|| read(2)).await.unwrap().generation, 1);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(cache.get(|| read(3)).await.unwrap().generation, 3);
    assert_eq!(reads.load(Ordering::SeqCst), 2);

    let cache = RuntimePolicyCache::default();
    cache
        .get(|| async { Err::<RuntimeEnvironmentPolicy, _>("down") })
        .await
        .unwrap_err();
    assert_eq!(cache.get(|| read(4)).await.unwrap().generation, 4);
}
