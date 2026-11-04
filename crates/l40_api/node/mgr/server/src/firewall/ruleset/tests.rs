use super::*;
use crate::firewall::Part;

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

#[test]
fn stamps_are_read_from_each_parts_chain() {
    for fixture in FIXTURES {
        let text = fixture;
        let fixture: Value = serde_json::from_str(fixture).unwrap();
        let ruleset = NamespaceRuleset::parse(fixture["ruleset"].to_string().as_bytes()).unwrap();
        let stamps = ruleset.stamps();
        for (part, reading) in stamps.iter() {
            let version = reading
                .version()
                .unwrap_or_else(|| panic!("{part} has no stamp"));
            assert!(text.contains(&format!("\"space-cfg:{part}={version}\"")));
        }

        // A stamp outside its part's chain is ignored. A second stamp for a
        // part, or a malformed one, leaves the part unstamped.
        let mut entries = fixture["ruleset"]["nftables"].as_array().unwrap().clone();
        let mut stamp = |part: &str, chain: &str, version: &str| {
            entries.push(serde_json::json!({"rule": {
                "family": "ip", "table": "sandbox", "chain": chain, "handle": 900,
                "comment": format!("space-cfg:{part}={version}"),
                "expr": [{"counter": {"packets": 0, "bytes": 0}}]
            }}));
        };
        stamp("l2_ingress", "forward", "0123abcd");
        stamp("nat", "postrouting", "0123abcd");
        let malformed = entries
            .iter_mut()
            .find(|entry| {
                entry["rule"]["comment"]
                    .as_str()
                    .is_some_and(|comment| comment.starts_with("space-cfg:forward="))
            })
            .unwrap();
        malformed["rule"]["comment"] = "space-cfg:forward=FFFFFFFF".into();
        let edited = NamespaceRuleset { entries }.stamps();
        for (part, reading) in edited.iter() {
            let expected = match part {
                Part::Nat | Part::Forward => StampReading::NoStamp,
                _ => *stamps.get(part),
            };
            assert_eq!(*reading, expected, "{part}");
        }
    }
}

// The proxy rules exist only while net-gateway runs, so a part can be absent
// rather than unstamped. Only a missing table shows that its rules are gone.
#[test]
fn a_part_reads_absent_only_without_its_table() {
    for fixture in FIXTURES {
        let fixture: Value = serde_json::from_str(fixture).unwrap();
        let entries = fixture["ruleset"]["nftables"].as_array().unwrap();
        let without = |drop: &dyn Fn(&Value) -> bool| {
            let entries = entries
                .iter()
                .filter(|entry| !drop(entry))
                .cloned()
                .collect();
            NamespaceRuleset { entries }.stamps()
        };
        let in_proxy = |object: &Value| object["family"] == "ip" && object["table"] == "proxy";
        let proxy_stamp = |entry: &Value| {
            entry["rule"]["comment"]
                .as_str()
                .is_some_and(|comment| comment.starts_with("space-cfg:proxy="))
        };
        let proxy_table = |entry: &Value| {
            entry.get("rule").is_some_and(in_proxy)
                || entry.get("chain").is_some_and(in_proxy)
                || entry
                    .get("table")
                    .is_some_and(|table| table["family"] == "ip" && table["name"] == "proxy")
        };
        let stamps = without(&|_| false);
        for (dropped, proxy) in [
            (without(&proxy_stamp), StampReading::NoStamp),
            (without(&proxy_table), StampReading::NoTable),
        ] {
            for (part, reading) in dropped.iter() {
                let expected = if part == Part::Proxy {
                    proxy
                } else {
                    *stamps.get(part)
                };
                assert_eq!(*reading, expected, "{part}");
            }
        }
    }
}
