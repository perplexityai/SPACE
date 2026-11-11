use pplx_space_test_env::TestEnv;

use super::*;

#[test]
fn the_gate_is_off_unless_set() {
    assert_eq!(
        EnvironmentPlacementGate::from_environment(&TestEnv::default()),
        Ok(EnvironmentPlacementGate::Off)
    );
    for (value, expected) in [
        ("off", EnvironmentPlacementGate::Off),
        ("shadow", EnvironmentPlacementGate::Shadow),
        ("on", EnvironmentPlacementGate::On),
        (" Shadow ", EnvironmentPlacementGate::Shadow),
    ] {
        let mut env = TestEnv::default();
        env.set(EnvironmentPlacementGate::ENV, value);
        assert_eq!(
            EnvironmentPlacementGate::from_environment(&env),
            Ok(expected),
            "{value:?}"
        );
    }
}

#[test]
fn an_unknown_gate_value_is_refused_not_read_as_off() {
    for value in ["", "true", "1", "enabled", "dry-run"] {
        let mut env = TestEnv::default();
        env.set(EnvironmentPlacementGate::ENV, value);
        assert_eq!(
            EnvironmentPlacementGate::from_environment(&env),
            Err(EnvironmentPlacementGateParseError::Unknown(
                value.to_string()
            )),
            "{value:?}"
        );
    }
}

#[test]
fn the_gate_round_trips_through_its_wire_value() {
    for gate in EnvironmentPlacementGate::ALL {
        assert_eq!(gate.as_str().parse(), Ok(gate));
        assert_eq!(gate.to_string(), gate.as_str());
    }
}

#[test]
fn only_on_refuses_and_only_shadow_records() {
    use EnvironmentPlacementGate::{Off, On, Shadow};
    for (gate, refuses, evaluates, records) in [
        (Off, false, false, false),
        (Shadow, false, true, true),
        (On, true, true, false),
    ] {
        assert_eq!(gate.refuses(), refuses, "{gate}");
        assert_eq!(gate.evaluates(), evaluates, "{gate}");
        assert_eq!(gate.records_refusals(), records, "{gate}");
    }
}

#[test]
fn a_restore_refused_for_want_of_cold_boot_evidence_has_its_own_label() {
    use crate::metrics::environment_placement::EnvironmentPlacementPath as Path;
    use pplx_space_node_status_store::EnvironmentRefusal as Refusal;
    for path in [Path::RestoreSource, Path::RestoreRemote] {
        assert_eq!(
            refusal_label(path, Refusal::EnvironmentNotUsable),
            RESTORE_NOT_COLD_BOOTABLE
        );
        for refusal in [Refusal::NoEvidence, Refusal::Incomplete, Refusal::Stale] {
            assert_eq!(refusal_label(path, refusal), refusal.as_str());
        }
    }
    for path in [
        Path::CachedCreate,
        Path::LastKnownCreate,
        Path::PullCreate,
        Path::PoolAdoption,
    ] {
        for refusal in Refusal::ALL {
            assert_eq!(refusal_label(path, refusal), refusal.as_str());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shadow_reads_a_failed_binding_read_as_unbound_and_on_fails() {
    // Nothing listens on port 1, so every read fails.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(100))
        .connect_lazy("postgres://space@127.0.0.1:1/space")
        .expect("a lazy pool doesn't connect");
    assert_eq!(
        work_environment(
            EnvironmentPlacementGate::Shadow,
            &pool,
            "template",
            Some("hash"),
            BindingScope::ReleaseOrHolders,
        )
        .await
        .expect("shadow places as off"),
        None
    );
    work_environment(
        EnvironmentPlacementGate::On,
        &pool,
        "template",
        Some("hash"),
        BindingScope::ReleaseOrHolders,
    )
    .await
    .expect_err("on doesn't place bound work as unbound");
    assert_eq!(
        work_environment(
            EnvironmentPlacementGate::Off,
            &pool,
            "template",
            Some("hash"),
            BindingScope::ReleaseOrHolders,
        )
        .await
        .expect("off reads nothing"),
        None
    );
}

/// Kubernetes runs off and shadow, but refuses on at startup until the worker
/// boots the environment a pod requires.
#[test]
fn kubernetes_refuses_on_until_its_worker_boots_the_pods_environment() {
    use EnvironmentPlacementGate::{Off, On, Shadow};
    Off.check_kubernetes_support()
        .expect("off is supported everywhere");
    Shadow
        .check_kubernetes_support()
        .expect("shadow places as off");
    assert!(matches!(
        On.check_kubernetes_support(),
        Err(crate::controllers::substrate::SubstrateError::Unsupported { message })
            if message.contains(EnvironmentPlacementGate::ENV)
    ));
}
