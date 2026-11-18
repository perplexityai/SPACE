use super::*;
use crate::test_fixtures::{id, port_rule};

#[test]
fn a_revision_rejects_a_repeated_rule_id() {
    // So the diff's match by ID is never ambiguous.
    assert_eq!(
        FirewallRuleRevision::new(vec![
            port_rule("a", "tcp", 1),
            port_rule("b", "tcp", 2),
            port_rule("a", "tcp", 3),
        ]),
        Err(RuleRevisionError::DuplicateRuleId { id: id("a") })
    );
}

#[test]
fn names_follow_their_syntax_rules() {
    let longest = "a".repeat(64);
    let too_long = "a".repeat(65);
    type Check = fn(&str) -> Result<(), NameError>;
    let checks: [(&str, Check); 2] = [
        ("rule ID", |value| RuleId::new(value).map(drop)),
        ("name", |value| Name::new(value).map(drop)),
    ];
    for (kind, check) in checks {
        assert_eq!(check("!~"), Ok(()), "{kind}");
        assert_eq!(check(&longest), Ok(()), "{kind}");
        assert_eq!(check(""), Err(NameError::Empty), "{kind}");
        assert_eq!(
            check(&too_long),
            Err(NameError::TooLong { len: 65 }),
            "{kind}"
        );
        for (value, index, found) in [
            ("a\nb", 1, '\n'),
            ("\u{1b}[31m", 0, '\u{1b}'),
            ("web\u{200b}", 3, '\u{200b}'),
            ("w\u{e9}b", 1, '\u{e9}'),
            ("del\u{7f}", 3, '\u{7f}'),
        ] {
            assert_eq!(
                check(value),
                Err(NameError::InvalidCharacter { index, found }),
                "{kind} {value:?}"
            );
        }
    }

    // Only a rule ID refuses the space.
    assert_eq!(
        RuleId::new("a b"),
        Err(NameError::InvalidCharacter {
            index: 1,
            found: ' '
        })
    );
    assert_eq!(Name::new(" a b ").unwrap().as_str(), " a b ");
    assert_eq!(RuleId::new("web").unwrap().to_string(), "web");
}
