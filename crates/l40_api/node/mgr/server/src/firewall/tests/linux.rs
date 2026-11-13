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
