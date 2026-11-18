//! Decoding, editing and encoding together, as the CLI and the Admin API use
//! the two crates.

use pplx_space_firewall_rules::{Edit, FirewallRuleRevision, Rule};

use crate::test_fixtures::{
    ACCEPTANCE_ADMIN, ACCEPTANCE_BASE, ACCEPTANCE_PROBE, ACCEPTANCE_WEB_PRIME, id, rule_json,
};
use crate::{decode_revision, encode_revision};

fn acceptance_edits() -> Vec<Edit> {
    vec![
        Edit::Replace(rule_json(ACCEPTANCE_WEB_PRIME)),
        Edit::Remove(id("dns")),
        Edit::Add(rule_json(ACCEPTANCE_ADMIN)),
        Edit::Add(rule_json(ACCEPTANCE_PROBE)),
    ]
}

/// Writes one rule in a canonical document and reads it back.
fn through_canonical_form(rule: Rule) -> Rule {
    let text = encode_revision(&FirewallRuleRevision::new(vec![rule]).unwrap());
    decode_revision(&text).unwrap().into_rules().remove(0)
}

#[test]
fn equivalent_inputs_give_identical_candidates_and_diffs() {
    let base = decode_revision(ACCEPTANCE_BASE).unwrap();
    let expected = base.apply(&acceptance_edits()).unwrap();
    let ids: Vec<&str> = expected
        .candidate()
        .rules()
        .iter()
        .map(|rule| rule.id().as_str())
        .collect();
    assert_eq!(ids, ["web", "metrics", "admin", "probe"]);

    // Same base and edits with other key order and whitespace.
    let reformatted_base = decode_revision(
        r#"{"rules":[{"all_of":[{"op":"==","right":53,"left":{"payload":{"field":"dport","protocol":"udp"}}}],"id":"dns"},
        {"all_of":[{"right":{"prefix":{"len":24,"addr":"198.51.100.0"}},"op":"==","left":{"payload":{"field":"daddr","protocol":"ip"}}},
                   {"op":"==","left":{"payload":{"field":"dport","protocol":"tcp"}},"right":80}],"id":"web"},
        {"id":"metrics","all_of":[{"right":{"range":[9100,9199]},"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"=="}]}],
        "schema_version":1}"#,
    )
    .unwrap();
    let reformatted_edits = vec![
        Edit::Replace(rule_json(
            r#"{"all_of":[{"op":"==","left":{"payload":{"field":"daddr","protocol":"ip"}},"right":{"prefix":{"len":24,"addr":"198.51.100.0"}}},
            {"right":{"range":[80,443]},"op":"==","left":{"payload":{"field":"dport","protocol":"tcp"}}}],"id":"web"}"#,
        )),
        Edit::Remove(id("dns")),
        Edit::Add(through_canonical_form(rule_json(ACCEPTANCE_ADMIN))),
        Edit::Add(through_canonical_form(rule_json(ACCEPTANCE_PROBE))),
    ];
    assert_eq!(reformatted_base, base);
    assert_eq!(
        reformatted_base.apply(&reformatted_edits),
        Ok(expected.clone())
    );

    // Repeated runs agree.
    for _ in 0..3 {
        assert_eq!(base.apply(&acceptance_edits()), Ok(expected.clone()));
    }

    // Encoding both sides and decoding them again changes nothing.
    let base_again = decode_revision(&encode_revision(&base)).unwrap();
    let candidate_again = decode_revision(&encode_revision(expected.candidate())).unwrap();
    assert_eq!(&candidate_again, expected.candidate());
    assert_eq!(base_again.apply(&acceptance_edits()), Ok(expected));
}
