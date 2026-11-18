use super::*;
use crate::test_fixtures::port_rule;

#[test]
fn a_revision_rejects_a_repeated_rule_id() {
    // So a base with a repeated ID can never reach `apply`.
    assert_eq!(
        FirewallRuleRevision::new(vec![
            port_rule("a", "tcp", 1),
            port_rule("b", "tcp", 2),
            port_rule("a", "tcp", 3),
        ]),
        Err(RuleRevisionError::DuplicateRuleId {
            id: RuleId::new("a"),
            first_index: 0,
            duplicate_index: 2,
        })
    );
    assert_eq!(
        FirewallRuleRevision::new(vec![
            port_rule("a", "tcp", 1),
            port_rule("b", "tcp", 2),
            port_rule("b", "tcp", 3),
        ]),
        Err(RuleRevisionError::DuplicateRuleId {
            id: RuleId::new("b"),
            first_index: 1,
            duplicate_index: 2,
        })
    );
}
