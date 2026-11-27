use super::*;
use crate::test_fixtures::{
    acceptance_admin, acceptance_base, acceptance_probe, acceptance_web_prime, id, payload_eq,
    port_rule, revision,
};
use crate::{Rule, Value};

/// Catches a wrong canonical order or an unchanged rule in the diff: dns is
/// removed before web is replaced, as in the base, additions follow in
/// candidate order, and the identical metrics rule is absent.
#[test]
fn acceptance_pair_lists_base_changes_then_additions() {
    let base = acceptance_base();
    let metrics = base.rule(&id("metrics")).unwrap().clone();
    let candidate = revision(vec![
        acceptance_web_prime(),
        metrics,
        acceptance_admin(),
        acceptance_probe(),
    ]);
    assert_eq!(
        diff(&base, &candidate).changes(),
        [
            RuleChange::Removed(base.rule(&id("dns")).unwrap().clone()),
            RuleChange::Replaced {
                before: base.rule(&id("web")).unwrap().clone(),
                after: acceptance_web_prime(),
            },
            RuleChange::Added(acceptance_admin()),
            RuleChange::Added(acceptance_probe()),
        ]
    );
}

/// Catches sorting changes by ID: base changes keep base order and additions
/// keep candidate order, even where that is not ID order.
#[test]
fn changes_follow_revision_order_not_id_order() {
    let (b, a) = (port_rule("b", "tcp", 1), port_rule("a", "tcp", 2));
    let b_prime = port_rule("b", "udp", 1);
    let (z, y) = (port_rule("z", "tcp", 26), port_rule("y", "tcp", 25));
    let base = revision(vec![b.clone(), a.clone()]);
    let candidate = revision(vec![z.clone(), b_prime.clone(), y.clone()]);
    assert_eq!(
        diff(&base, &candidate).changes(),
        [
            RuleChange::Replaced {
                before: b,
                after: b_prime,
            },
            RuleChange::Removed(a),
            RuleChange::Added(z),
            RuleChange::Added(y),
        ]
    );
}

/// Catches comparing `all_of` as a set: the same comparisons in another order
/// are a different rule.
#[test]
fn reordering_comparisons_is_a_replacement() {
    let first = payload_eq("tcp", "dport", Value::Integer(1));
    let second = payload_eq("tcp", "sport", Value::Integer(2));
    let before = Rule::new(id("r"), vec![first.clone(), second.clone()]).unwrap();
    let after = Rule::new(id("r"), vec![second, first]).unwrap();
    assert_eq!(
        diff(
            &revision(vec![before.clone()]),
            &revision(vec![after.clone()])
        )
        .changes(),
        [RuleChange::Replaced { before, after }]
    );
}

/// Catches reporting a change where there is none: identical snapshots give
/// an empty diff that is not reordered.
#[test]
fn identical_snapshots_give_an_empty_diff() {
    let base = acceptance_base();
    let unchanged = diff(&base, &base.clone());
    assert!(unchanged.is_empty());
    assert!(!unchanged.reordered());
}

/// Catches dropping rule order from the comparison, or from `is_empty`: the
/// same rules in another order change no rule but are a reorder.
#[test]
fn a_pure_reorder_is_reported() {
    let base = acceptance_base();
    let mut rules = base.rules().to_vec();
    rules.reverse();
    let reordered = diff(&base, &revision(rules));
    assert_eq!(reordered.changes(), []);
    assert!(reordered.reordered());
    assert!(!reordered.is_empty());

    // Only the middle two swap, so the first and last positions alone would
    // not show it.
    let (a, b, c, d) = (
        port_rule("a", "tcp", 1),
        port_rule("b", "tcp", 2),
        port_rule("c", "tcp", 3),
        port_rule("d", "tcp", 4),
    );
    let swapped = diff(
        &revision(vec![a.clone(), b.clone(), c.clone(), d.clone()]),
        &revision(vec![a, c, b, d]),
    );
    assert!(swapped.reordered());
}

/// Catches comparing positions instead of the order of the shared rules, and
/// an `is_empty` that only looks at order: adding rules before, between and
/// after them, and removing one, keeps the shared rules in order but is not
/// empty.
#[test]
fn additions_and_removals_alone_are_not_a_reorder() {
    let (a, b, c) = (
        port_rule("a", "tcp", 1),
        port_rule("b", "tcp", 2),
        port_rule("c", "tcp", 3),
    );
    let (x, y, z) = (
        port_rule("x", "tcp", 24),
        port_rule("y", "tcp", 25),
        port_rule("z", "tcp", 26),
    );
    let base = revision(vec![a.clone(), b.clone(), c.clone()]);
    let candidate = revision(vec![x.clone(), a, y.clone(), c, z.clone()]);
    let changed = diff(&base, &candidate);
    assert_eq!(
        changed.changes(),
        [
            RuleChange::Removed(b),
            RuleChange::Added(x),
            RuleChange::Added(y),
            RuleChange::Added(z),
        ]
    );
    assert!(!changed.reordered());
    assert!(!changed.is_empty());
}

/// Catches a replacement hiding a reorder or the reverse: a replaced rule that
/// also moves sets both.
#[test]
fn a_reorder_with_a_replacement_reports_both() {
    let (a, b) = (port_rule("a", "tcp", 1), port_rule("b", "tcp", 2));
    let b_prime = port_rule("b", "udp", 2);
    let changed = diff(
        &revision(vec![a.clone(), b.clone()]),
        &revision(vec![b_prime.clone(), a]),
    );
    assert_eq!(
        changed.changes(),
        [RuleChange::Replaced {
            before: b,
            after: b_prime,
        }]
    );
    assert!(changed.reordered());
}

/// Catches treating an empty candidate as no change: every base rule is
/// removed, in base order.
#[test]
fn an_emptied_snapshot_removes_every_rule() {
    let base = acceptance_base();
    let removed: Vec<RuleChange> = base
        .rules()
        .iter()
        .cloned()
        .map(RuleChange::Removed)
        .collect();
    assert_eq!(diff(&base, &revision(Vec::new())).changes(), removed);
}
