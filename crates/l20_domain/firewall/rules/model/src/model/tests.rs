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
    let longest_id = "A".repeat(64);
    for valid in ["a", "Z", "0", "Web.Edge-1_a", longest_id.as_str()] {
        assert_eq!(RuleId::new(valid).unwrap().as_str(), valid);
    }
    let longest_name = "a".repeat(64);
    for valid in [
        "tcp",
        "ip6",
        "sack-perm",
        "frag-off",
        "a_b",
        "9",
        longest_name.as_str(),
    ] {
        assert_eq!(Name::new(valid).unwrap().as_str(), valid);
    }

    type Check = fn(&str) -> Result<(), NameError>;
    let checks: [(&str, Check); 2] = [
        ("rule ID", |value| RuleId::new(value).map(drop)),
        ("name", |value| Name::new(value).map(drop)),
    ];
    for (kind, check) in checks {
        assert_eq!(check(""), Err(NameError::Empty), "{kind}");
        assert_eq!(
            check(&"a".repeat(65)),
            Err(NameError::TooLong { len: 65 }),
            "{kind}"
        );
        for (value, index, found) in [
            ("-a", 0, '-'),
            ("_a", 0, '_'),
            (".a", 0, '.'),
            (" a", 0, ' '),
            ("a ", 1, ' '),
            ("a b", 1, ' '),
            ("a\"} {", 1, '"'),
            ("tcp; flush ruleset", 3, ';'),
            ("space-cfg:x", 9, ':'),
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

    // Only a rule ID takes uppercase letters and `.`.
    assert_eq!(
        Name::new("Tcp"),
        Err(NameError::InvalidCharacter {
            index: 0,
            found: 'T'
        })
    );
    assert_eq!(
        Name::new("a.b"),
        Err(NameError::InvalidCharacter {
            index: 1,
            found: '.'
        })
    );
    assert_eq!(RuleId::new("web").unwrap().to_string(), "web");
}
