use std::collections::{HashMap, HashSet};

use super::*;
use crate::test_fixtures::{
    ACCEPTANCE_ADMIN, ACCEPTANCE_BASE, ACCEPTANCE_PROBE, ACCEPTANCE_WEB_PRIME, ids, payload_eq,
    port_rule, revision,
};
use crate::{DecodeErrorKind, Match, Value};

fn rule_json(text: &str) -> Rule {
    Rule::from_json_str(text).unwrap()
}

fn id(value: &str) -> RuleId {
    RuleId::new(value)
}

fn acceptance_edits() -> Vec<Edit> {
    vec![
        Edit::Replace(rule_json(ACCEPTANCE_WEB_PRIME)),
        Edit::Remove(id("dns")),
        Edit::Add(rule_json(ACCEPTANCE_ADMIN)),
        Edit::Add(rule_json(ACCEPTANCE_PROBE)),
    ]
}

#[test]
fn acceptance_batch_replaces_in_place_removes_and_appends() {
    let base = FirewallRuleRevision::from_json_str(ACCEPTANCE_BASE).unwrap();
    let original = base.clone();

    let change = base.apply(&acceptance_edits()).unwrap();

    assert_eq!(base, original);
    assert_eq!(
        ids(change.candidate()),
        ["web", "metrics", "admin", "probe"]
    );
    let web = base.rule(&id("web")).unwrap();
    let metrics = base.rule(&id("metrics")).unwrap();
    assert_eq!(
        change.candidate().rules(),
        [
            rule_json(ACCEPTANCE_WEB_PRIME),
            metrics.clone(),
            rule_json(ACCEPTANCE_ADMIN),
            rule_json(ACCEPTANCE_PROBE),
        ]
    );
    // Base order first (dns, web), then additions; metrics is unchanged and
    // absent.
    assert_eq!(
        change.diff().changes(),
        [
            RuleChange::Removed(base.rule(&id("dns")).unwrap().clone()),
            RuleChange::Replaced {
                before: web.clone(),
                after: rule_json(ACCEPTANCE_WEB_PRIME),
            },
            RuleChange::Added(rule_json(ACCEPTANCE_ADMIN)),
            RuleChange::Added(rule_json(ACCEPTANCE_PROBE)),
        ]
    );
}

#[test]
fn acceptance_batch_with_an_invalid_fifth_edit_fails_whole() {
    let base = FirewallRuleRevision::from_json_str(ACCEPTANCE_BASE).unwrap();
    let original = base.clone();

    for (fifth, expected) in [
        (
            Edit::Remove(id("absent")),
            EditError::RemoveMissing {
                id: id("absent"),
                index: 4,
            },
        ),
        (
            Edit::Add(port_rule("metrics", "tcp", 9100)),
            EditError::AddExisting {
                id: id("metrics"),
                index: 4,
            },
        ),
        (
            Edit::Remove(id("web")),
            EditError::ConflictingEdits {
                id: id("web"),
                first_index: 0,
                first_kind: EditKind::Replace,
                index: 4,
                kind: EditKind::Remove,
            },
        ),
    ] {
        let mut edits = acceptance_edits();
        edits.push(fifth);
        let error = base.apply(&edits).unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(base, original);
    }
    let mut edits = acceptance_edits();
    edits.push(Edit::Remove(id("web")));
    assert_eq!(
        base.apply(&edits).unwrap_err().to_string(),
        r#"edit 4 (remove) names rule "web", which edit 0 (replace) already names"#
    );
}

#[test]
fn single_edits_need_the_right_presence() {
    let base = revision(vec![port_rule("a", "tcp", 1), port_rule("b", "tcp", 2)]);
    let original = base.clone();
    let cases: Vec<(Edit, Result<Vec<Rule>, EditError>)> = vec![
        (
            Edit::Add(port_rule("c", "tcp", 3)),
            Ok(vec![
                port_rule("a", "tcp", 1),
                port_rule("b", "tcp", 2),
                port_rule("c", "tcp", 3),
            ]),
        ),
        (
            Edit::Add(port_rule("a", "tcp", 3)),
            Err(EditError::AddExisting {
                id: id("a"),
                index: 0,
            }),
        ),
        (
            Edit::Replace(port_rule("b", "udp", 2)),
            Ok(vec![port_rule("a", "tcp", 1), port_rule("b", "udp", 2)]),
        ),
        (
            Edit::Replace(port_rule("c", "udp", 2)),
            Err(EditError::ReplaceMissing {
                id: id("c"),
                index: 0,
            }),
        ),
        (Edit::Remove(id("a")), Ok(vec![port_rule("b", "tcp", 2)])),
        (
            Edit::Remove(id("c")),
            Err(EditError::RemoveMissing {
                id: id("c"),
                index: 0,
            }),
        ),
    ];
    for (edit, expected) in cases {
        let result = base.apply(std::slice::from_ref(&edit));
        match expected {
            Ok(rules) => assert_eq!(result.unwrap().candidate().rules(), rules, "{edit:?}"),
            Err(error) => assert_eq!(result, Err(error), "{edit:?}"),
        }
        assert_eq!(base, original);
    }
}

#[test]
fn edits_naming_one_id_twice_reject_the_batch() {
    // `x` is absent and `p` present, so each edit would pass alone where its
    // presence rule allows.
    let base = revision(vec![port_rule("p", "tcp", 1), port_rule("q", "tcp", 2)]);
    let original = base.clone();
    let add = |name: &str| Edit::Add(port_rule(name, "udp", 9));
    let replace = |name: &str| Edit::Replace(port_rule(name, "udp", 9));
    let remove = |name: &str| Edit::Remove(id(name));
    let cases = [
        (add("x"), add("x")),
        (add("x"), remove("x")),
        (remove("p"), add("p")),
        (replace("p"), remove("p")),
        (replace("p"), replace("p")),
        (remove("p"), remove("p")),
    ];
    for (first, second) in cases {
        let edits = [first.clone(), Edit::Remove(id("q")), second.clone()];
        assert_eq!(
            base.apply(&edits),
            Err(EditError::ConflictingEdits {
                id: second.id().clone(),
                first_index: 0,
                first_kind: first.kind(),
                index: 2,
                kind: second.kind(),
            }),
            "{first:?} then {second:?}"
        );
        assert_eq!(base, original);
    }
}

#[test]
fn an_invalid_edit_anywhere_rejects_the_batch() {
    let base = revision(vec![port_rule("a", "tcp", 1), port_rule("b", "tcp", 2)]);
    let original = base.clone();
    let valid = [
        Edit::Replace(port_rule("a", "udp", 1)),
        Edit::Add(port_rule("c", "tcp", 3)),
    ];
    for index in 0..=valid.len() {
        let mut edits = valid.to_vec();
        edits.insert(index, Edit::Remove(id("missing")));
        assert_eq!(
            base.apply(&edits),
            Err(EditError::RemoveMissing {
                id: id("missing"),
                index,
            }),
            "invalid edit at {index}"
        );
        assert_eq!(base, original);
    }
}

#[test]
fn ordering_follows_the_documented_convention() {
    let base = revision(vec![
        port_rule("a", "tcp", 1),
        port_rule("b", "tcp", 2),
        port_rule("c", "tcp", 3),
    ]);
    let cases: Vec<(Vec<Edit>, Vec<&str>)> = vec![
        (
            vec![Edit::Replace(port_rule("a", "udp", 1))],
            vec!["a", "b", "c"],
        ),
        (
            vec![Edit::Replace(port_rule("b", "udp", 1))],
            vec!["a", "b", "c"],
        ),
        (
            vec![Edit::Replace(port_rule("c", "udp", 1))],
            vec!["a", "b", "c"],
        ),
        (vec![Edit::Remove(id("a"))], vec!["b", "c"]),
        (vec![Edit::Remove(id("b"))], vec!["a", "c"]),
        (vec![Edit::Remove(id("c"))], vec!["a", "b"]),
        (
            vec![
                Edit::Add(port_rule("z", "tcp", 26)),
                Edit::Add(port_rule("y", "tcp", 25)),
            ],
            vec!["a", "b", "c", "z", "y"],
        ),
        (
            // Additions append even when listed before a replacement.
            vec![
                Edit::Add(port_rule("d", "tcp", 4)),
                Edit::Replace(port_rule("a", "udp", 1)),
                Edit::Remove(id("b")),
            ],
            vec!["a", "c", "d"],
        ),
    ];
    for (edits, expected) in cases {
        let change = base.apply(&edits).unwrap();
        assert_eq!(ids(change.candidate()), expected, "{edits:?}");
        for edit in &edits {
            if let Edit::Add(rule) | Edit::Replace(rule) = edit {
                assert_eq!(change.candidate().rule(rule.id()), Some(rule));
            }
        }
    }
}

#[test]
fn match_order_is_preserved_through_edits() {
    let matches = vec![
        payload_eq("tcp", "dport", Value::Integer(3)),
        payload_eq("tcp", "dport", Value::Integer(1)),
        payload_eq("tcp", "dport", Value::Integer(2)),
    ];
    let ordered = Rule::new(id("m"), matches.clone()).unwrap();
    let base = revision(vec![ordered.clone(), port_rule("n", "tcp", 1)]);

    let mut reversed = matches;
    reversed.reverse();
    let replacement = Rule::new(id("n"), reversed.clone()).unwrap();
    let change = base.apply(&[Edit::Replace(replacement)]).unwrap();

    let all_of = |rule: &str| -> Vec<Match> {
        change
            .candidate()
            .rule(&id(rule))
            .unwrap()
            .all_of()
            .to_vec()
    };
    assert_eq!(all_of("m"), ordered.all_of());
    assert_eq!(all_of("n"), reversed);
}

#[test]
fn reordering_matches_is_a_replacement() {
    let first = payload_eq("tcp", "dport", Value::Integer(1));
    let second = payload_eq("tcp", "sport", Value::Integer(2));
    let before = Rule::new(id("r"), vec![first.clone(), second.clone()]).unwrap();
    let after = Rule::new(id("r"), vec![second, first]).unwrap();
    let base = revision(vec![before.clone()]);

    let change = base.apply(&[Edit::Replace(after.clone())]).unwrap();
    assert_eq!(
        change.diff().changes(),
        [RuleChange::Replaced { before, after }]
    );
}

#[test]
fn complete_revisions_are_validated_at_every_boundary() {
    // A base with a repeated ID cannot be built or decoded, so it can never
    // reach `apply` with untouched invalid content.
    assert_eq!(
        FirewallRuleRevision::new(vec![
            port_rule("a", "tcp", 1),
            port_rule("b", "tcp", 2),
            port_rule("a", "tcp", 3),
        ]),
        Err(RuleRevisionError::DuplicateRuleId {
            id: id("a"),
            first_index: 0,
            duplicate_index: 2,
        })
    );
    let untouched_duplicate = r#"{"schema_version":1,"rules":[
        {"id":"a","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":1}]},
        {"id":"b","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":2}]},
        {"id":"b","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":3}]}
    ]}"#;
    let error = FirewallRuleRevision::from_json_str(untouched_duplicate).unwrap_err();
    assert_eq!(error.path().to_string(), "$.rules[2].id");
    assert!(matches!(
        error.kind(),
        DecodeErrorKind::DuplicateRuleId { first_index: 1, .. }
    ));
    let untouched_invalid = r#"{"schema_version":1,"rules":[
        {"id":"a","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":1}]},
        {"id":"b","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":{"range":[9,1]}}]}
    ]}"#;
    let error = FirewallRuleRevision::from_json_str(untouched_invalid).unwrap_err();
    assert_eq!(error.path().to_string(), "$.rules[1].all_of[0].right.range");
    assert!(matches!(
        error.kind(),
        DecodeErrorKind::ReversedRange { start: 9, end: 1 }
    ));
}

#[test]
fn removing_the_last_rule_yields_an_explicit_empty_snapshot() {
    let only = port_rule("only", "tcp", 1);
    let base = revision(vec![only.clone()]);
    let change = base.apply(&[Edit::Remove(id("only"))]).unwrap();

    assert!(change.candidate().rules().is_empty());
    assert_eq!(
        change.candidate().to_json_string(),
        r#"{"schema_version":1,"rules":[]}"#
    );
    assert_eq!(change.diff().changes(), [RuleChange::Removed(only)]);
}

#[test]
fn identical_replacement_and_empty_batch_show_no_change() {
    let base = FirewallRuleRevision::from_json_str(ACCEPTANCE_BASE).unwrap();
    let web = base.rule(&id("web")).unwrap().clone();

    let replaced = base.apply(&[Edit::Replace(web)]).unwrap();
    assert_eq!(replaced.candidate(), &base);
    assert!(replaced.diff().is_empty());

    let empty = base.apply(&[]).unwrap();
    assert_eq!(empty.candidate(), &base);
    assert!(empty.diff().is_empty());
}

#[test]
fn equivalent_inputs_give_identical_candidates_and_diffs() {
    let base = FirewallRuleRevision::from_json_str(ACCEPTANCE_BASE).unwrap();
    let expected = base.apply(&acceptance_edits()).unwrap();

    // Same base and edits with other key order and whitespace.
    let reformatted_base = FirewallRuleRevision::from_json_str(
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
        Edit::Add(Rule::from_json_str(&rule_json(ACCEPTANCE_ADMIN).to_json_string()).unwrap()),
        Edit::Add(Rule::from_json_str(&rule_json(ACCEPTANCE_PROBE).to_json_string()).unwrap()),
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

    // Serializing both sides and decoding them again changes nothing.
    let base_again = FirewallRuleRevision::from_json_str(&base.to_json_string()).unwrap();
    let candidate_again =
        FirewallRuleRevision::from_json_str(&expected.candidate().to_json_string()).unwrap();
    assert_eq!(&candidate_again, expected.candidate());
    assert_eq!(base_again.apply(&acceptance_edits()), Ok(expected));
}

/// A small deterministic generator (xorshift64*), so the generated cases are
/// reproducible without a property-testing dependency.
struct Generator(u64);

impl Generator {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d) % bound
    }

    fn rule(&mut self, id: &str) -> Rule {
        let matches = (0..=self.below(2))
            .map(|_| payload_eq("tcp", "dport", Value::Integer(self.below(4))))
            .collect();
        Rule::new(RuleId::new(id), matches).unwrap()
    }
}

const ID_POOL: [&str; 8] = ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"];

/// The first problem a batch has, computed independently of `apply`: for each
/// edit in order, a repeated ID, then the presence its kind requires.
fn expected_failure(base: &FirewallRuleRevision, edits: &[Edit]) -> Option<(usize, EditKind)> {
    let present: HashSet<&str> = base.rules().iter().map(|r| r.id().as_str()).collect();
    let mut seen = HashSet::new();
    edits.iter().enumerate().find_map(|(index, edit)| {
        let name = edit.id().as_str();
        let ok = seen.insert(name)
            && match edit {
                Edit::Add(_) => !present.contains(name),
                Edit::Replace(_) | Edit::Remove(_) => present.contains(name),
            };
        (!ok).then_some((index, edit.kind()))
    })
}

#[test]
fn generated_batches_keep_the_edit_and_diff_invariants() {
    let mut generator = Generator(0x5eed_f5ac);
    let (mut successes, mut failures) = (0, 0);
    for _ in 0..2000 {
        let base_ids: Vec<&str> = ID_POOL
            .iter()
            .copied()
            .filter(|_| generator.below(2) == 0)
            .collect();
        let base = revision(base_ids.iter().map(|name| generator.rule(name)).collect());
        let original = base.clone();
        let edits: Vec<Edit> = (0..generator.below(6))
            .map(|_| {
                let name = ID_POOL[generator.below(ID_POOL.len() as u64) as usize];
                match generator.below(3) {
                    0 => Edit::Add(generator.rule(name)),
                    1 => Edit::Replace(generator.rule(name)),
                    _ => Edit::Remove(RuleId::new(name)),
                }
            })
            .collect();

        let result = base.apply(&edits);
        assert_eq!(base, original, "apply changed its base");

        match (expected_failure(&base, &edits), result) {
            (Some((index, kind)), Err(error)) => {
                failures += 1;
                let reported = match error {
                    EditError::ConflictingEdits { index, kind, .. } => (index, kind),
                    EditError::AddExisting { index, .. } => (index, EditKind::Add),
                    EditError::ReplaceMissing { index, .. } => (index, EditKind::Replace),
                    EditError::RemoveMissing { index, .. } => (index, EditKind::Remove),
                    EditError::InvalidCandidate(error) => panic!("unexpected {error}"),
                };
                assert_eq!(reported, (index, kind), "{edits:?}");
            }
            (None, Ok(change)) => {
                successes += 1;
                assert_successful_change(&base, &edits, &change);
            }
            (expected, result) => panic!("expected failure {expected:?}, got {result:?}"),
        }
    }
    // Both outcomes are exercised in quantity.
    assert!(
        successes > 300 && failures > 300,
        "{successes} / {failures}"
    );
}

fn assert_successful_change(
    base: &FirewallRuleRevision,
    edits: &[Edit],
    change: &RuleRevisionChange,
) {
    let candidate = change.candidate();
    let named: HashSet<&RuleId> = edits.iter().map(Edit::id).collect();

    let unique: HashSet<&RuleId> = candidate.rules().iter().map(Rule::id).collect();
    assert_eq!(
        unique.len(),
        candidate.rules().len(),
        "duplicate candidate IDs"
    );

    // Untouched rules keep their content and relative order.
    let untouched = |snapshot: &FirewallRuleRevision| -> Vec<Rule> {
        snapshot
            .rules()
            .iter()
            .filter(|rule| !named.contains(rule.id()))
            .cloned()
            .collect()
    };
    assert_eq!(untouched(candidate), untouched(base));

    // Survivors keep base order; additions follow in batch order.
    let added: Vec<&RuleId> = edits
        .iter()
        .filter_map(|edit| match edit {
            Edit::Add(rule) => Some(rule.id()),
            _ => None,
        })
        .collect();
    let removed: HashSet<&RuleId> = edits
        .iter()
        .filter_map(|edit| match edit {
            Edit::Remove(id) => Some(id),
            _ => None,
        })
        .collect();
    let expected_order: Vec<&RuleId> = base
        .rules()
        .iter()
        .map(Rule::id)
        .filter(|id| !removed.contains(id))
        .chain(added)
        .collect();
    assert_eq!(
        candidate.rules().iter().map(Rule::id).collect::<Vec<_>>(),
        expected_order
    );

    // The diff lists exactly the rules whose content differs, each with its
    // real before and after.
    let before: HashMap<&RuleId, &Rule> = base.rules().iter().map(|r| (r.id(), r)).collect();
    let after: HashMap<&RuleId, &Rule> = candidate.rules().iter().map(|r| (r.id(), r)).collect();
    let mut differing: HashSet<&RuleId> = before
        .keys()
        .chain(after.keys())
        .copied()
        .filter(|id| before.get(id) != after.get(id))
        .collect();
    for entry in change.diff().changes() {
        assert!(
            differing.remove(entry.id()),
            "spurious or repeated {entry:?}"
        );
        match entry {
            RuleChange::Added(rule) => {
                assert!(!before.contains_key(rule.id()));
                assert_eq!(after.get(rule.id()), Some(&rule));
            }
            RuleChange::Removed(rule) => {
                assert_eq!(before.get(rule.id()), Some(&rule));
                assert!(!after.contains_key(rule.id()));
            }
            RuleChange::Replaced {
                before: old,
                after: new,
            } => {
                assert_ne!(old, new);
                assert_eq!(old.id(), new.id());
                assert_eq!(before.get(old.id()), Some(&old));
                assert_eq!(after.get(new.id()), Some(&new));
            }
        }
    }
    assert!(differing.is_empty(), "diff misses {differing:?}");

    // Canonical order: base positions first, then additions in candidate order.
    let position = |id: &RuleId| -> usize {
        base.rules()
            .iter()
            .position(|rule| rule.id() == id)
            .or_else(|| {
                candidate
                    .rules()
                    .iter()
                    .position(|rule| rule.id() == id)
                    .map(|index| base.rules().len() + index)
            })
            .unwrap()
    };
    let positions: Vec<usize> = change
        .diff()
        .changes()
        .iter()
        .map(|c| position(c.id()))
        .collect();
    assert!(positions.is_sorted(), "{:?}", change.diff());
}
