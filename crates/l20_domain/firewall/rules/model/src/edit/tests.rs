use std::collections::{HashMap, HashSet};
use std::mem::{Discriminant, discriminant};
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use super::*;
use crate::test_fixtures::{
    acceptance_admin, acceptance_base, acceptance_probe, acceptance_web_prime, header, id, ids,
    payload_eq, port_rule, revision,
};
use crate::{IntegerRange, Ipv4Prefix, Match, Read, TcpOptionName, Value};

fn acceptance_edits() -> Vec<Edit> {
    vec![
        Edit::Replace(acceptance_web_prime()),
        Edit::Remove(id("dns")),
        Edit::Add(acceptance_admin()),
        Edit::Add(acceptance_probe()),
    ]
}

#[test]
fn acceptance_batch_replaces_in_place_removes_and_appends() {
    let base = acceptance_base();
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
            acceptance_web_prime(),
            metrics.clone(),
            acceptance_admin(),
            acceptance_probe(),
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
                after: acceptance_web_prime(),
            },
            RuleChange::Added(acceptance_admin()),
            RuleChange::Added(acceptance_probe()),
        ]
    );
}

#[test]
fn acceptance_batch_with_an_invalid_fifth_edit_fails_whole() {
    let base = acceptance_base();
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
        (add("x"), replace("x")),
        (add("x"), remove("x")),
        (replace("p"), add("p")),
        (replace("p"), replace("p")),
        (replace("p"), remove("p")),
        (remove("p"), add("p")),
        (remove("p"), replace("p")),
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

    // The diff follows base order, not ID order.
    let b = port_rule("b", "tcp", 1);
    let a = port_rule("a", "tcp", 2);
    let b_prime = port_rule("b", "udp", 1);
    let base = revision(vec![b.clone(), a.clone()]);
    let change = base
        .apply(&[Edit::Replace(b_prime.clone()), Edit::Remove(id("a"))])
        .unwrap();
    assert_eq!(
        change.diff().changes(),
        [
            RuleChange::Replaced {
                before: b,
                after: b_prime,
            },
            RuleChange::Removed(a),
        ]
    );
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
fn removing_the_last_rule_yields_an_explicit_empty_snapshot() {
    let only = port_rule("only", "tcp", 1);
    let base = revision(vec![only.clone()]);
    let change = base.apply(&[Edit::Remove(id("only"))]).unwrap();

    assert!(change.candidate().rules().is_empty());
    assert_eq!(change.diff().changes(), [RuleChange::Removed(only)]);
}

#[test]
fn identical_replacement_and_empty_batch_show_no_change() {
    let base = acceptance_base();
    let web = base.rule(&id("web")).unwrap().clone();

    let replaced = base.apply(&[Edit::Replace(web)]).unwrap();
    assert_eq!(replaced.candidate(), &base);
    assert!(replaced.diff().is_empty());

    let empty = base.apply(&[]).unwrap();
    assert_eq!(empty.candidate(), &base);
    assert!(empty.diff().is_empty());
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

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }

    /// A read of any kind, from small pools.
    fn read(&mut self) -> Read {
        let header = header(self.pick(&["ip", "tcp"]), self.pick(&["daddr", "dport"]));
        match self.below(3) {
            0 => Read::Payload(header),
            1 => Read::TcpOption(TcpOptionName::new(self.pick(&["mptcp", "sack-perm"])).unwrap()),
            _ => Read::MaskedPayload {
                header,
                mask: NonZeroU64::new(1 + self.below(2)).unwrap(),
            },
        }
    }

    /// A value of any kind, from small pools.
    fn value(&mut self) -> Value {
        match self.below(5) {
            0 => Value::Integer(self.below(3)),
            1 => Value::Ipv4(Ipv4Addr::new(192, 0, 2, self.pick(&[1, 2]))),
            2 => Value::True,
            3 => Value::Ipv4Prefix(
                Ipv4Prefix::new(Ipv4Addr::new(198, 51, 100, 0), self.pick(&[24, 32])).unwrap(),
            ),
            _ => {
                let start = self.below(3);
                Value::Range(IntegerRange::new(start, start + self.below(2)).unwrap())
            }
        }
    }

    fn rule(&mut self, name: &str) -> Rule {
        let matches = (0..=self.below(2))
            .map(|_| Match::new(self.read(), self.value()))
            .collect();
        Rule::new(id(name), matches).unwrap()
    }
}

/// Records the kind of every read and value in `rules`.
fn record_kinds<'a>(
    rules: impl IntoIterator<Item = &'a Rule>,
    reads: &mut HashSet<Discriminant<Read>>,
    values: &mut HashSet<Discriminant<Value>>,
) {
    for comparison in rules.into_iter().flat_map(Rule::all_of) {
        reads.insert(discriminant(comparison.left()));
        values.insert(discriminant(comparison.right()));
    }
}

const ID_POOL: [&str; 8] = ["r0", "r1", "r2", "r3", "r4", "r5", "r6", "r7"];

/// The error for the first problem a batch has, computed independently of
/// `apply`: for each edit in order, a repeated ID, then the presence its kind
/// requires.
fn expected_failure(base: &FirewallRuleRevision, edits: &[Edit]) -> Option<EditError> {
    let present: HashSet<&str> = base.rules().iter().map(|r| r.id().as_str()).collect();
    let mut seen: HashMap<&str, (usize, EditKind)> = HashMap::new();
    edits.iter().enumerate().find_map(|(index, edit)| {
        let name = edit.id().as_str();
        let (id, kind) = (edit.id().clone(), edit.kind());
        if let Some(&(first_index, first_kind)) = seen.get(name) {
            return Some(EditError::ConflictingEdits {
                id,
                first_index,
                first_kind,
                index,
                kind,
            });
        }
        seen.insert(name, (index, kind));
        match (edit, present.contains(name)) {
            (Edit::Add(_), true) => Some(EditError::AddExisting { id, index }),
            (Edit::Replace(_), false) => Some(EditError::ReplaceMissing { id, index }),
            (Edit::Remove(_), false) => Some(EditError::RemoveMissing { id, index }),
            _ => None,
        }
    })
}

#[test]
fn generated_batches_keep_the_edit_and_diff_invariants() {
    let mut generator = Generator(0x5eed_f5ac);
    let (mut successes, mut failures, mut identical) = (0, 0, 0);
    let (mut reads, mut values) = (HashSet::new(), HashSet::new());
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
                    // Half of the replacements of a present rule reuse
                    // it unchanged, which the diff must leave out.
                    1 => match base.rule(&id(name)) {
                        Some(rule) if generator.below(2) == 0 => Edit::Replace(rule.clone()),
                        _ => Edit::Replace(generator.rule(name)),
                    },
                    _ => Edit::Remove(id(name)),
                }
            })
            .collect();
        record_kinds(base.rules(), &mut reads, &mut values);
        let edited = edits.iter().filter_map(|edit| match edit {
            Edit::Add(rule) | Edit::Replace(rule) => Some(rule),
            Edit::Remove(_) => None,
        });
        record_kinds(edited, &mut reads, &mut values);

        let result = base.apply(&edits);
        assert_eq!(base, original, "apply changed its base");

        match (expected_failure(&base, &edits), result) {
            (Some(expected), Err(error)) => {
                failures += 1;
                assert_eq!(error, expected, "{edits:?}");
            }
            (None, Ok(change)) => {
                successes += 1;
                identical += edits
                    .iter()
                    .filter(|edit| match edit {
                        Edit::Replace(rule) => base.rule(rule.id()) == Some(rule),
                        _ => false,
                    })
                    .count();
                assert_successful_change(&base, &edits, &change);
            }
            (expected, result) => panic!("expected failure {expected:?}, got {result:?}"),
        }
    }
    // Every read and value kind is generated, and both outcomes and
    // identical replacements are exercised in quantity.
    assert_eq!((reads.len(), values.len()), (3, 5));
    assert!(identical > 50, "{identical} identical replacements");
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

    // Each added or replacing rule is in the candidate exactly as written.
    for edit in edits {
        if let Edit::Add(rule) | Edit::Replace(rule) = edit {
            assert_eq!(candidate.rule(rule.id()), Some(rule), "{edit:?}");
        }
    }

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
