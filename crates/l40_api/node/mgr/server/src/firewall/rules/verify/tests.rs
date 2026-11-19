use std::io;
use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};

use pplx_space_firewall_rules_model::FirewallRuleRevision;
use pplx_space_firewall_rules_protocol::decode_revision;
use serde_json::{Value, json};

use super::{Verification, observed};

mod linux;

fn revision() -> FirewallRuleRevision {
    decode_revision(
        r#"{"schema_version":1,"rules":[
        {"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]},
        {"id":"web","all_of":[
            {"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":"192.0.2.1"},
            {"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]}]}"#,
    )
    .unwrap()
}

/// The table nft 1.1.3 lists for [`revision`], written by hand.
fn listing() -> Value {
    let guard =
        json!({"match": {"op": "==", "left": {"meta": {"key": "iifname"}}, "right": "tap0"}});
    json!({"nftables": [
        {"metainfo": {"version": "1.1.3", "release_name": "Commodore Bullmoose #4", "json_schema_version": 1}},
        {"table": {"family": "ip", "name": "space_firewall_rules", "handle": 1}},
        {"chain": {"family": "ip", "table": "space_firewall_rules", "name": "guest_drop", "handle": 1,
                   "type": "filter", "hook": "prerouting", "prio": -176, "policy": "accept"}},
        {"rule": {"family": "ip", "table": "space_firewall_rules", "chain": "guest_drop", "handle": 2,
                  "comment": "space-firewall-rule:dns", "expr": [
            guard,
            {"match": {"op": "==", "left": {"payload": {"protocol": "udp", "field": "dport"}}, "right": 53}},
            {"drop": null},
        ]}},
        {"rule": {"family": "ip", "table": "space_firewall_rules", "chain": "guest_drop", "handle": 3,
                  "comment": "space-firewall-rule:web", "expr": [
            guard,
            {"match": {"op": "==", "left": {"payload": {"protocol": "ip", "field": "daddr"}}, "right": "192.0.2.1"}},
            {"match": {"op": "==", "left": {"payload": {"protocol": "tcp", "field": "dport"}}, "right": 80}},
            {"drop": null},
        ]}},
    ]})
}

fn objects(listing: &mut Value) -> &mut Vec<Value> {
    listing["nftables"].as_array_mut().unwrap()
}

fn rule(listing: &mut Value, index: usize) -> &mut Value {
    &mut listing["nftables"][3 + index]["rule"]
}

fn exprs(listing: &mut Value, index: usize) -> &mut Vec<Value> {
    rule(listing, index)["expr"].as_array_mut().unwrap()
}

/// The tables nft lists in the family while the owned table exists.
fn tables() -> Value {
    json!({"nftables": [
        {"metainfo": {"version": "1.1.3", "release_name": "Commodore Bullmoose #4", "json_schema_version": 1}},
        {"table": {"family": "ip", "name": "sandbox", "handle": 1}},
        {"table": {"family": "ip", "name": "space_firewall_rules", "handle": 2}},
    ]})
}

/// An edit to the hand-written [`listing`].
type Change = fn(&mut Value);

fn changed(change: Change) -> Vec<u8> {
    let mut listing = listing();
    change(&mut listing);
    listing.to_string().into_bytes()
}

fn listed(stdout: &[u8]) -> io::Result<Output> {
    Ok(Output {
        status: ExitStatus::from_raw(0),
        stdout: stdout.to_vec(),
        stderr: Vec::new(),
    })
}

// Catches a verifier that accepts an absent table or one differing from the
// revision or SPACE's placement, or that rejects a table differing only in
// handles.
#[test]
fn every_semantic_drift_is_a_mismatch() {
    let tables_text = tables().to_string();
    let judged = |stdout: &[u8]| {
        observed(&revision(), listed(tables_text.as_bytes()), || {
            listed(stdout)
        })
    };
    assert_eq!(
        judged(listing().to_string().as_bytes()),
        Verification::Match
    );
    for absent in [
        json!([]),
        json!([{"table": {"family": "ip", "name": "sandbox"}}]),
    ] {
        let mut tables = tables();
        objects(&mut tables).splice(1.., absent.as_array().unwrap().clone());
        let verification = observed(&revision(), listed(tables.to_string().as_bytes()), || {
            panic!("listed a table the tables listing did not hold")
        });
        assert!(
            matches!(verification, Verification::Mismatch(_)),
            "{tables}: {verification:?}"
        );
    }
    let renumbered = changed(|listing| {
        for (index, kind, handle) in [
            (1, "table", 7),
            (2, "chain", 8),
            (3, "rule", 40),
            (4, "rule", 9),
        ] {
            listing["nftables"][index][kind]["handle"] = json!(handle);
        }
    });
    assert_eq!(judged(&renumbered), Verification::Match);

    let drifts: [(&str, Change); 17] = [
        ("missing rule", |l| drop(objects(l).pop())),
        ("extra rule", |l| {
            let mut extra = objects(l)[3].clone();
            extra["rule"]["comment"] = json!("space-firewall-rule:extra");
            objects(l).push(extra);
        }),
        ("reordered rules", |l| objects(l).swap(3, 4)),
        ("reordered comparisons", |l| exprs(l, 1).swap(1, 2)),
        ("wrong comparison", |l| {
            exprs(l, 0)[1]["match"]["right"] = json!(54)
        }),
        ("wrong rule ID", |l| {
            rule(l, 0)["comment"] = json!("space-firewall-rule:web")
        }),
        ("wrong guard", |l| {
            exprs(l, 0)[0]["match"]["right"] = json!("tap1")
        }),
        ("missing guard", |l| drop(exprs(l, 0).remove(0))),
        ("wrong table", |l| {
            l["nftables"][1]["table"]["name"] = json!("sandbox")
        }),
        ("wrong chain type", |l| {
            l["nftables"][2]["chain"]["type"] = json!("nat")
        }),
        ("wrong chain hook", |l| {
            l["nftables"][2]["chain"]["hook"] = json!("input")
        }),
        ("wrong chain priority", |l| {
            l["nftables"][2]["chain"]["prio"] = json!(-175)
        }),
        ("wrong chain policy", |l| {
            l["nftables"][2]["chain"]["policy"] = json!("drop")
        }),
        ("extra chain", |l| {
            let mut extra = objects(l)[2].clone();
            extra["chain"]["name"] = json!("other");
            objects(l).push(extra);
        }),
        ("wrong verdict", |l| {
            exprs(l, 0)[2] = json!({"accept": null})
        }),
        ("extra expression", |l| {
            exprs(l, 0).insert(2, json!({"counter": {"packets": 0, "bytes": 0}}));
        }),
        ("unexpected object", |l| {
            objects(l).push(
                json!({"set": {"family": "ip", "table": "space_firewall_rules",
                "name": "extra", "type": "ipv4_addr", "handle": 5}}),
            );
        }),
    ];
    for (drift, change) in drifts {
        let verification = judged(&changed(change));
        assert!(
            matches!(verification, Verification::Mismatch(_)),
            "{drift}: {verification:?}"
        );
    }
}

// Catches a verifier that reports a match, or a definite mismatch, for a
// listing it could not read or does not understand.
#[test]
fn unreadable_or_unsupported_listings_are_inconclusive() {
    let failed = |code: i32, stderr: &str| -> io::Result<Output> {
        Ok(Output {
            status: ExitStatus::from_raw(code << 8),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        })
    };
    let text = listing().to_string();
    let tables_text = tables().to_string();
    let present = || listed(tables_text.as_bytes());
    let unlisted =
        || -> io::Result<Output> { panic!("listed the table after a failed tables listing") };
    let tables_cases = [
        (
            "tables command failure",
            Err(io::Error::from(io::ErrorKind::NotFound)),
        ),
        ("tables timeout", Err(io::Error::from(io::ErrorKind::TimedOut))),
        ("tables exit", failed(1, "Error: Operation not permitted")),
        ("malformed tables JSON", listed(b"table ip space_firewall_rules")),
        (
            "non-table object",
            listed(&changed(|l| objects(l).truncate(3))),
        ),
        (
            "nameless table",
            listed(br#"{"nftables":[{"metainfo":{"json_schema_version":1}},{"table":{"family":"ip"}}]}"#),
        ),
    ];
    for (case, tables) in tables_cases {
        let verification = observed(&revision(), tables, unlisted);
        assert!(
            matches!(verification, Verification::Inconclusive(_)),
            "{case}: {verification:?}"
        );
    }

    let cases = [
        (
            "table removed after the tables listing",
            failed(
                1,
                "Error: No such file or directory\nlist table ip space_firewall_rules\n",
            ),
        ),
        (
            "command failure",
            Err(io::Error::from(io::ErrorKind::NotFound)),
        ),
        ("timeout", Err(io::Error::from(io::ErrorKind::TimedOut))),
        ("malformed JSON", listed(b"table ip space_firewall_rules {")),
        ("empty listing", listed(br#"{"nftables":[]}"#)),
        ("truncated JSON", listed(&text.as_bytes()[..text.len() / 2])),
        (
            "newer JSON schema",
            listed(&changed(|l| {
                l["nftables"][0]["metainfo"]["json_schema_version"] = json!(2);
            })),
        ),
        (
            "metainfo beside another object",
            listed(&changed(|l| {
                let mut extra = objects(l)[2]["chain"].clone();
                extra["name"] = json!("other");
                objects(l)[0]["chain"] = extra;
            })),
        ),
        (
            "unknown rule key",
            listed(&changed(|l| rule(l, 0)["flags"] = json!(1))),
        ),
        (
            "unsupported comparison",
            listed(&changed(|l| {
                exprs(l, 0)[1]["match"]["left"] = json!({"&": [
                    {"payload": {"protocol": "tcp", "field": "flags"}}, {"|": ["syn", "ack"]},
                ]});
            })),
        ),
    ];
    for (case, listed) in cases {
        let verification = observed(&revision(), present(), || listed);
        assert!(
            matches!(verification, Verification::Inconclusive(_)),
            "{case}: {verification:?}"
        );
    }
}
