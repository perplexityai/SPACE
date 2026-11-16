//! Full installations against nftables in a disposable network namespace.
//! Needs root, iproute2 and nftables; listed in the nextest privileged group.
use super::super::config_version::{ConfigPart, PartHash, read_marker};
use super::super::*;
use crate::netns_tests::TestNamespace;

const VPEER: &str = "vpeer-000001";
const VPEER_IP: &str = "10.12.0.1";
const GATEWAY_UID: u32 = 100_001;

fn ruleset(namespace: &TestNamespace) -> serde_json::Value {
    let output = nft_output(Some(namespace.name()), &["-j", "list", "ruleset"]).unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn sandbox_table(namespace: &TestNamespace) -> serde_json::Value {
    let output = nft_output(
        Some(namespace.name()),
        &["-j", "list", "table", "ip", SANDBOX_TABLE],
    )
    .unwrap();
    assert!(output.status.success(), "{output:?}");
    serde_json::from_slice(&output.stdout).unwrap()
}

fn rules(listing: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    listing["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get("rule"))
}

fn expected(part: ConfigPart) -> PartHash {
    part.builtin_hash().unwrap_or_else(|| {
        PartHash::sandbox_base(super::packaged_pre_tproxy_guardrails_policy()).unwrap()
    })
}

fn marker(namespace: &TestNamespace, part: ConfigPart) -> Option<PartHash> {
    read_marker(part, &ruleset(namespace)).unwrap()
}

fn markers(namespace: &TestNamespace) -> [Option<PartHash>; ConfigPart::ALL.len()] {
    let listing = ruleset(namespace);
    ConfigPart::ALL.map(|part| read_marker(part, &listing).unwrap())
}

fn install(namespace: &TestNamespace) {
    install_namespace_rules(
        namespace.name(),
        VPEER,
        VPEER_IP,
        crate::net::GUEST_IP,
        super::packaged_pre_tproxy_guardrails_policy(),
    )
    .unwrap();
}

/// A namespace as a leased slot's gateway leaves it: every part installed.
fn install_with_gateway(namespace: &TestNamespace) {
    install(namespace);
    setup_netgtw_tproxy_rules(namespace.name(), GATEWAY_UID, VPEER, true).unwrap();
}

#[test]
fn reinstall_over_a_leftover_sandbox_table_fails_whole() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install(&namespace);
    let installed = sandbox_table(&namespace);

    // As after a teardown that failed to delete the table: appending would
    // duplicate every rule, so the transaction is refused and nothing changes.
    let error = install_namespace_rules(
        namespace.name(),
        VPEER,
        VPEER_IP,
        crate::net::GUEST_IP,
        super::packaged_pre_tproxy_guardrails_policy(),
    )
    .unwrap_err();
    assert!(matches!(error, FirewallError::Apply { .. }), "{error}");
    assert_eq!(sandbox_table(&namespace), installed);
}

#[test]
fn namespace_installation_marks_every_namespace_part() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install(&namespace);
    let expected = ConfigPart::ALL.map(|part| (part != ConfigPart::Proxy).then(|| expected(part)));
    assert_eq!(markers(&namespace), expected);
}

#[test]
fn startup_l2_reinstall_keeps_one_l2_marker() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install(&namespace);
    apply_guest_l2_ingress_policy(namespace.name()).unwrap();
    assert_eq!(
        marker(&namespace, ConfigPart::L2Ingress),
        Some(expected(ConfigPart::L2Ingress))
    );
}

#[test]
fn every_gateway_launch_leaves_one_trailing_proxy_marker() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install(&namespace);
    for dhcp in [false, true, true] {
        setup_netgtw_tproxy_rules(namespace.name(), GATEWAY_UID, VPEER, dhcp).unwrap();
        let listing = ruleset(&namespace);
        assert_eq!(
            read_marker(ConfigPart::Proxy, &listing),
            Ok(Some(expected(ConfigPart::Proxy))),
            "dhcp {dhcp}"
        );
        let last_prerouting = rules(&listing)
            .filter(|rule| rule["table"] == PROXY_TABLE && rule["chain"] == "prerouting")
            .last()
            .unwrap();
        assert_eq!(
            last_prerouting["comment"],
            expected(ConfigPart::Proxy).marker(ConfigPart::Proxy),
            "dhcp {dhcp}"
        );
    }
}

#[test]
fn markers_are_unreachable_from_any_hook() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install_with_gateway(&namespace);
    let listing = ruleset(&namespace);
    for chain in listing["nftables"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry.get("chain"))
        .filter(|chain| chain["name"].as_str().unwrap().starts_with("space_cfg_"))
    {
        assert!(chain.get("hook").is_none(), "{chain}");
    }
    for rule in rules(&listing) {
        assert!(
            rule["expr"].as_array().unwrap().iter().all(|statement| {
                ["jump", "goto"].iter().all(|verdict| {
                    !statement[verdict]["target"]
                        .as_str()
                        .is_some_and(|target| target.starts_with("space_cfg_"))
                })
            }),
            "{rule}"
        );
    }
}

#[test]
fn incremental_writers_leave_markers_alone() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    let ns = namespace.name();
    install_with_gateway(&namespace);
    let stamped = markers(&namespace);
    ensure_guest_dhcp_bootstrap(ns).unwrap();
    allow_guest_port(ns, VPEER, 8080, "tcp").unwrap();
    constrain_direct_inbound_reply_path(ns, None).unwrap();
    deny_guest_port(ns, PortSelector::All, "tcp").unwrap();
    assert_eq!(markers(&namespace), stamped);
}

#[test]
fn forward_repair_is_not_a_full_installation() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    let ns = namespace.name();
    install(&namespace);
    // A forward chain from before markers existed, missing one drop.
    let egress_drop = rules(&sandbox_table(&namespace))
        .find(|rule| rule["comment"] == "fail-closed-guest-egress")
        .unwrap()["handle"]
        .to_string();
    for args in [
        &["delete", "chain", "ip", SANDBOX_TABLE, "space_cfg_forward"][..],
        &[
            "delete",
            "rule",
            "ip",
            SANDBOX_TABLE,
            "forward",
            "handle",
            &egress_drop,
        ],
    ] {
        assert!(nft_status(Some(ns), args).unwrap().success(), "{args:?}");
    }

    ensure_namespace_forward_drop_rules(ns, VPEER).unwrap();
    let repaired = nft_chain_json(Some(ns), "ip", SANDBOX_TABLE, "forward")
        .unwrap()
        .unwrap();
    assert_eq!(validate_forward_drop_chain(&repaired, VPEER), Ok(()));
    assert_eq!(marker(&namespace, ConfigPart::Forward), None);
}

#[test]
fn teardown_removes_every_marker() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    install_with_gateway(&namespace);
    teardown_proxy_rules(namespace.name());
    teardown_namespace_rules(namespace.name());
    assert_eq!(markers(&namespace), [None; ConfigPart::ALL.len()]);
}

#[test]
fn rejected_installation_leaves_no_marker() {
    if !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables") {
        return;
    }
    let namespace = TestNamespace::create();
    // Valid JSON that nft refuses: a rule in a chain the table lacks.
    let policy = r#"{ "add": { "rule": { "family": "ip", "table": "sandbox", "chain": "missing",
        "expr": [{ "drop": null }] } } }"#;
    let error = install_namespace_rules(
        namespace.name(),
        VPEER,
        VPEER_IP,
        crate::net::GUEST_IP,
        policy,
    )
    .unwrap_err();
    assert!(matches!(error, FirewallError::Apply { .. }), "{error}");
    // Only the L2 policy, its own earlier transaction, committed.
    let expected =
        ConfigPart::ALL.map(|part| (part == ConfigPart::L2Ingress).then(|| expected(part)));
    assert_eq!(markers(&namespace), expected);
    let output = nft_output(
        Some(namespace.name()),
        &["list", "table", "ip", SANDBOX_TABLE],
    )
    .unwrap();
    assert!(!output.status.success(), "{output:?}");
}

/// Prints the listing the `stamped_namespace_nft_*.json` fixtures record; see
/// `stamped_listings` in the configuration version tests. A tool rather than a
/// test, so it runs only with `SPACE_PRINT_NFT_FIXTURE=1`: CI runs ignored
/// tests, and the installation tests above already cover this setup.
#[test]
#[ignore = "prints a fixture; run on demand under each nftables version"]
fn print_stamped_namespace_listing() {
    if std::env::var("SPACE_PRINT_NFT_FIXTURE").as_deref() != Ok("1")
        || !pplx_space_test_env::root_or_skip("create network namespaces and nftables tables")
    {
        return;
    }
    let namespace = TestNamespace::create();
    install_with_gateway(&namespace);
    let output = nft_output(Some(namespace.name()), &["-j", "list", "ruleset"]).unwrap();
    assert!(output.status.success(), "{output:?}");
    println!("{}", String::from_utf8(output.stdout).unwrap());
}
