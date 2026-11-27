//! Namespace rule installation against nftables in a disposable network
//! namespace. Needs root, iproute2 and nftables; listed in the nextest
//! privileged group.
use super::super::*;
use crate::netns_tests::TestNamespace;

const VPEER: &str = "vpeer-000001";
const VPEER_IP: &str = "10.12.0.1";

fn sandbox_table(namespace: &TestNamespace) -> serde_json::Value {
    let output = nft_output(
        Some(namespace.name()),
        &["-j", "list", "table", "ip", SANDBOX_TABLE],
    )
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn reinstall_over_a_leftover_sandbox_table_fails_whole() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    let ns = namespace.name();
    let rules = namespace_rules_json(
        VPEER,
        VPEER_IP,
        crate::net::GUEST_IP,
        super::packaged_pre_tproxy_guardrails_policy(),
    );
    apply_nft_json(&rules, Some(ns)).unwrap();
    let installed = sandbox_table(&namespace);

    // As after a teardown that failed to delete the table: appending would
    // duplicate every rule, so the transaction is refused and nothing changes.
    let error = apply_nft_json(&rules, Some(ns)).unwrap_err();
    assert!(matches!(error, FirewallError::Apply { .. }), "{error}");
    assert_eq!(sandbox_table(&namespace), installed);
}

fn has_table(namespace: &TestNamespace, family: &str, name: &str) -> bool {
    nft_output(Some(namespace.name()), &["list", "table", family, name])
        .unwrap()
        .status
        .success()
}

// The in-place slot reset removes the firewall-rules table with the other
// owned tables, so the next occupant starts like a fresh slot. Catches a
// reset that leaves an installed or half-installed table behind, and one that
// removes more than SPACE's tables or fails on an absent one.
#[test]
fn scrub_removes_every_owned_table_including_firewall_rules() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    let ns = Some(namespace.name());
    apply_nft_json(
        &namespace_rules_json(
            VPEER,
            VPEER_IP,
            crate::net::GUEST_IP,
            super::packaged_pre_tproxy_guardrails_policy(),
        ),
        ns,
    )
    .unwrap();
    apply_nft_script(&guest_l2_ingress_policy_script(), ns).unwrap();
    apply_nft_script(
        "add table ip proxy
add table ip space_firewall_rules
add chain ip space_firewall_rules guest_drop { type filter hook prerouting priority -176; }
add table ip unrelated
",
        ns,
    )
    .unwrap();
    for (family, table) in [
        ("ip", SANDBOX_TABLE),
        ("netdev", SANDBOX_L2_TABLE),
        ("ip", "proxy"),
        ("ip", "space_firewall_rules"),
        ("ip", "unrelated"),
    ] {
        assert!(has_table(&namespace, family, table), "{family} {table}");
    }

    scrub_namespace_tables(namespace.name()).unwrap();

    for (family, table) in [
        ("ip", SANDBOX_TABLE),
        ("netdev", SANDBOX_L2_TABLE),
        ("ip", "proxy"),
        ("ip", "space_firewall_rules"),
    ] {
        assert!(!has_table(&namespace, family, table), "{family} {table}");
    }
    assert!(has_table(&namespace, "ip", "unrelated"));

    // A slot whose tables are already gone scrubs cleanly.
    scrub_namespace_tables(namespace.name()).unwrap();
}
