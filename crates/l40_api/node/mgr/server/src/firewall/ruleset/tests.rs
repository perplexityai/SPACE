use super::*;

// Each fixture is verbatim output from one nft version: `nft -j list ruleset`,
// and `nft -j list chain` for every chain that listing contains. The rules
// were installed in a fresh network namespace with a dummy tap0, in order:
// guest_l2_ingress_policy_script(), fixtures/namespace_rules.json,
// forward_build_json("vpeer-000000") and netgtw_tproxy_rules_json(12345,
// false), so every part carries its stamp.
const FIXTURES: [&str; 3] = [
    include_str!("../../fixtures/namespace_ruleset_nft_1_0_4.json"),
    include_str!("../../fixtures/namespace_ruleset_nft_1_0_9.json"),
    include_str!("../../fixtures/namespace_ruleset_nft_1_1_3.json"),
];

#[test]
fn chain_view_matches_nft_chain_listing() {
    for fixture in FIXTURES {
        let fixture: Value = serde_json::from_str(fixture).unwrap();
        let ruleset = NamespaceRuleset::parse(fixture["ruleset"].to_string().as_bytes()).unwrap();
        for chain in fixture["chains"].as_array().unwrap() {
            let [family, table, name] =
                ["family", "table", "name"].map(|key| chain[key].as_str().unwrap());
            assert_eq!(
                ruleset.chain(family, table, name).as_ref(),
                Some(&chain["listing"]),
                "{family} {table} {name}"
            );
        }
        // Each differs from a listed chain only in family, table or name.
        for (family, table, name) in [
            ("inet", "sandbox", "forward"),
            ("ip", "sandbox", "divert"),
            ("netdev", "sandbox_l2", "egress"),
        ] {
            assert_eq!(ruleset.chain(family, table, name), None);
        }
    }
}

// Reading a malformed listing as an empty ruleset would skip the reply path
// repair without an error.
#[test]
fn malformed_listing_is_an_error() {
    for stdout in [&b"{}"[..], br#"{"nftables": {}}"#] {
        assert!(matches!(
            NamespaceRuleset::parse(stdout),
            Err(FirewallError::ListJson(_))
        ));
    }
}
