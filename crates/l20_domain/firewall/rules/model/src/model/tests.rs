use super::*;
use crate::test_fixtures::{id, port_rule};

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
            id: id("a"),
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
            id: id("b"),
            first_index: 1,
            duplicate_index: 2,
        })
    );
}

#[test]
fn names_follow_their_syntax_rules() {
    let longest = "a".repeat(MAX_NAME_LEN);
    let too_long = "a".repeat(MAX_NAME_LEN + 1);
    for kind in [
        NameKind::RuleId,
        NameKind::HeaderProtocol,
        NameKind::HeaderField,
        NameKind::TcpOptionName,
    ] {
        let check = |value: &str| kind.validate(value);
        assert_eq!(check("!~"), Ok(()), "{kind}");
        assert_eq!(check(&longest), Ok(()), "{kind}");
        assert_eq!(check(""), Err(NameError::Empty { kind }));
        assert_eq!(
            check(&too_long),
            Err(NameError::TooLong {
                kind,
                len: MAX_NAME_LEN + 1
            })
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
                Err(NameError::InvalidCharacter { kind, index, found }),
                "{kind} {value:?}"
            );
        }
    }

    // Only a rule ID refuses the space.
    assert_eq!(
        RuleId::new("a b"),
        Err(NameError::InvalidCharacter {
            kind: NameKind::RuleId,
            index: 1,
            found: ' '
        })
    );
    assert_eq!(HeaderProtocol::new(" a b ").unwrap().as_str(), " a b ");
    assert_eq!(HeaderField::new(" a b ").unwrap().as_str(), " a b ");
    assert_eq!(TcpOptionName::new(" a b ").unwrap().as_str(), " a b ");
    assert_eq!(RuleId::try_from("web").unwrap().to_string(), "web");
}
