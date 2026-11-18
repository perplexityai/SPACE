use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use super::*;
use crate::test_fixtures::{
    document_with_left, document_with_match, document_with_right, header, payload_eq,
};
use crate::{IntegerRange, Ipv4Prefix, TcpOptionName};

type KindCheck = fn(&DecodeErrorKind) -> bool;

/// Asserts that every document is rejected at the given path with a matching
/// kind, and that none decodes to a policy.
fn assert_rejections(cases: Vec<(String, &str, KindCheck)>) {
    for (document, path, check) in cases {
        let error =
            PolicyRevision::from_json_str(&document).expect_err(&format!("accepted {document}"));
        assert_eq!(error.path().to_string(), path, "{document}: {error}");
        assert!(check(error.kind()), "{document}: unexpected {error:?}");
    }
}

fn decode(document: &str) -> PolicyRevision {
    PolicyRevision::from_json_str(document).unwrap_or_else(|error| panic!("{document}: {error}"))
}

/// Decodes a document, writes it back canonically, and decodes that again.
fn assert_round_trips(policy: &PolicyRevision) {
    let text = policy.to_json_string();
    assert_eq!(
        &PolicyRevision::from_json_str(&text).unwrap(),
        policy,
        "{text}"
    );
}

const ALL_VARIANTS: &str = r#"{
  "schema_version": 1,
  "rules": [
    {"id": "first", "all_of": [
      {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": "192.0.2.1"},
      {"left": {"tcp option": {"name": "mptcp"}}, "op": "==", "right": true},
      {"left": {"&": [{"payload": {"protocol": "tcp", "field": "flags"}}, 18]}, "op": "==", "right": 2}
    ]},
    {"id": "second", "all_of": [
      {"left": {"payload": {"protocol": "ip", "field": "saddr"}}, "op": "==", "right": {"prefix": {"addr": "198.51.100.0", "len": 24}}},
      {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": {"range": [1024, 65535]}}
    ]}
  ]
}"#;

#[test]
fn valid_document_decodes_every_read_and_value_in_order() {
    let policy = decode(ALL_VARIANTS);

    let expected = PolicyRevision::new(vec![
        Rule::new(
            RuleId::new("first"),
            vec![
                payload_eq("ip", "daddr", Value::Ipv4(Ipv4Addr::new(192, 0, 2, 1))),
                Match::new(Read::TcpOption(TcpOptionName::new("mptcp")), Value::True),
                Match::new(
                    Read::MaskedPayload {
                        header: header("tcp", "flags"),
                        mask: NonZeroU64::new(18).unwrap(),
                    },
                    Value::Integer(2),
                ),
            ],
        )
        .unwrap(),
        Rule::new(
            RuleId::new("second"),
            vec![
                payload_eq(
                    "ip",
                    "saddr",
                    Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(198, 51, 100, 0), 24).unwrap()),
                ),
                payload_eq(
                    "tcp",
                    "dport",
                    Value::Range(IntegerRange::new(1024, 65535).unwrap()),
                ),
            ],
        )
        .unwrap(),
    ])
    .unwrap();
    assert_eq!(policy, expected);
    assert_round_trips(&policy);
}

#[test]
fn canonical_form_is_stable_and_keeps_rule_and_match_order() {
    let policy = decode(ALL_VARIANTS);
    assert_eq!(
        policy.to_json_string(),
        concat!(
            r#"{"schema_version":1,"rules":["#,
            r#"{"id":"first","all_of":["#,
            r#"{"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":"192.0.2.1"},"#,
            r#"{"left":{"tcp option":{"name":"mptcp"}},"op":"==","right":true},"#,
            r#"{"left":{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18]},"op":"==","right":2}]},"#,
            r#"{"id":"second","all_of":["#,
            r#"{"left":{"payload":{"protocol":"ip","field":"saddr"}},"op":"==","right":{"prefix":{"addr":"198.51.100.0","len":24}}},"#,
            r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":{"range":[1024,65535]}}]}"#,
            r#"]}"#,
        )
    );
}

#[test]
fn key_order_and_whitespace_do_not_change_the_policy() {
    let reordered = r#"{"rules":[
        {"all_of":[
            {"right":"192.0.2.1","op":"==","left":{"payload":{"field":"daddr","protocol":"ip"}}},
            {"op":"==","right":true,"left":{"tcp option":{"name":"mptcp"}}},
            {"right":2,"left":{"&":[{"payload":{"field":"flags","protocol":"tcp"}},18]},"op":"=="}
        ],"id":"first"},
        {"all_of":[
            {"right":{"prefix":{"len":24,"addr":"198.51.100.0"}},"left":{"payload":{"field":"saddr","protocol":"ip"}},"op":"=="},
            {"left":{"payload":{"field":"dport","protocol":"tcp"}},"right":{"range":[1024,65535]},"op":"=="}
        ],"id":"second"}
    ],"schema_version":1}"#;
    let policy = decode(reordered);
    assert_eq!(policy, decode(ALL_VARIANTS));
    assert_eq!(
        policy.to_json_string(),
        decode(ALL_VARIANTS).to_json_string()
    );
}

#[test]
fn explicit_empty_rule_list_is_a_snapshot() {
    let policy = decode(r#"{"schema_version":1,"rules":[]}"#);
    assert!(policy.rules().is_empty());
    assert_eq!(
        policy.to_json_string(),
        r#"{"schema_version":1,"rules":[]}"#
    );
}

#[test]
fn boundary_values_keep_their_meaning() {
    let cases: Vec<(&str, Value)> = vec![
        ("0", Value::Integer(0)),
        ("18446744073709551615", Value::Integer(u64::MAX)),
        (r#""0.0.0.0""#, Value::Ipv4(Ipv4Addr::UNSPECIFIED)),
        (r#""255.255.255.255""#, Value::Ipv4(Ipv4Addr::BROADCAST)),
        ("true", Value::True),
        (
            r#"{"prefix":{"addr":"0.0.0.0","len":0}}"#,
            Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::UNSPECIFIED, 0).unwrap()),
        ),
        (
            r#"{"prefix":{"addr":"192.0.2.7","len":32}}"#,
            Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(192, 0, 2, 7), 32).unwrap()),
        ),
        (
            // Host bits are kept as written, not masked off.
            r#"{"prefix":{"addr":"192.0.2.7","len":24}}"#,
            Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(192, 0, 2, 7), 24).unwrap()),
        ),
        (
            r#"{"range":[5,5]}"#,
            Value::Range(IntegerRange::new(5, 5).unwrap()),
        ),
        (
            r#"{"range":[0,18446744073709551615]}"#,
            Value::Range(IntegerRange::new(0, u64::MAX).unwrap()),
        ),
    ];
    for (right, expected) in cases {
        let policy = decode(&document_with_right(right));
        assert_eq!(policy.rules()[0].all_of()[0].right(), &expected, "{right}");
        assert_round_trips(&policy);
    }

    let masked = decode(&document_with_left(
        r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18446744073709551615]}"#,
    ));
    assert_eq!(
        masked.rules()[0].all_of()[0].left(),
        &Read::MaskedPayload {
            header: header("tcp", "flags"),
            mask: NonZeroU64::MAX,
        }
    );
}

#[test]
fn names_are_kept_verbatim_without_qualification() {
    // The decoder has no field or option catalog: an unknown name is
    // structurally valid and kept byte for byte.
    let policy = decode(&document_with_match(
        r#"{"left":{"payload":{"protocol":"Not A Protocol","field":" spaced "}},"op":"==","right":1},
           {"left":{"tcp option":{"name":"option-that-does-not-exist"}},"op":"==","right":true}"#,
    ));
    let all_of = policy.rules()[0].all_of();
    assert_eq!(
        all_of[0].left(),
        &Read::Payload(header("Not A Protocol", " spaced "))
    );
    assert_eq!(
        all_of[1].left(),
        &Read::TcpOption(TcpOptionName::new("option-that-does-not-exist"))
    );
}

#[test]
fn rejects_documents_that_are_not_v1_snapshots() {
    assert_rejections(vec![
        (String::new(), "$", |k| {
            matches!(k, DecodeErrorKind::Syntax(_))
        }),
        ("null".into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::Null,
                    ..
                }
            )
        }),
        ("[]".into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::Array,
                    ..
                }
            )
        }),
        (r#""add rule ip filter output drop""#.into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::String,
                    ..
                }
            )
        }),
        (r#"{"schema_version":1,"rules":[]} {}"#.into(), "$", |k| {
            matches!(k, DecodeErrorKind::Syntax(_))
        }),
        (r#"{"rules":[]}"#.into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::MissingField {
                    field: "schema_version"
                }
            )
        }),
        (
            r#"{"schema_version":null,"rules":[]}"#.into(),
            "$.schema_version",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":"1","rules":[]}"#.into(),
            "$.schema_version",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":1.0,"rules":[]}"#.into(),
            "$.schema_version",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::OtherNumber,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":true,"rules":[]}"#.into(),
            "$.schema_version",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::True,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":2,"rules":[]}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::UnsupportedSchemaVersion { found: 2 }),
        ),
        (
            r#"{"schema_version":0,"rules":[]}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::UnsupportedSchemaVersion { found: 0 }),
        ),
        (
            r#"{"schema_version":-1,"rules":[]}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::UnsupportedSchemaVersion { found: -1 }),
        ),
        (r#"{"schema_version":1}"#.into(), "$", |k| {
            matches!(k, DecodeErrorKind::MissingField { field: "rules" })
        }),
        (
            r#"{"schema_version":1,"rules":null}"#.into(),
            "$.rules",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":1,"rules":{}}"#.into(),
            "$.rules",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Object,
                        ..
                    }
                )
            },
        ),
        (
            r#"{"schema_version":1,"rules":"[]"}"#.into(),
            "$.rules",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (r#"{}"#.into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::MissingField {
                    field: "schema_version"
                }
            )
        }),
    ]);
}

const GOOD_MATCH: &str =
    r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":1}"#;

fn document_with_rules(rules: &[String]) -> String {
    format!(r#"{{"schema_version":1,"rules":[{}]}}"#, rules.join(","))
}

fn good_rule(id: &str) -> String {
    format!(r#"{{"id":"{id}","all_of":[{GOOD_MATCH}]}}"#)
}

#[test]
fn decodes_one_or_several_rules_and_matches() {
    let policy = decode(&document_with_rules(&[
        good_rule("a"),
        good_rule("b"),
        format!(r#"{{"id":"c","all_of":[{GOOD_MATCH},{GOOD_MATCH}]}}"#),
    ]));
    assert_eq!(policy.rules().len(), 3);
    assert_eq!(policy.rules()[2].all_of().len(), 2);
    assert_eq!(
        policy.rule(&RuleId::new("b")).unwrap().id(),
        &RuleId::new("b")
    );
}

#[test]
fn rejects_malformed_rules_at_the_bad_rule() {
    assert_rejections(vec![
        (
            document_with_rules(&[good_rule("a"), format!(r#"{{"all_of":[{GOOD_MATCH}]}}"#)]),
            "$.rules[1]",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "id" }),
        ),
        (
            document_with_rules(&[format!(r#"{{"id":null,"all_of":[{GOOD_MATCH}]}}"#)]),
            "$.rules[0].id",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            document_with_rules(&[format!(r#"{{"id":7,"all_of":[{GOOD_MATCH}]}}"#)]),
            "$.rules[0].id",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::UnsignedInteger,
                        ..
                    }
                )
            },
        ),
        (
            document_with_rules(&[good_rule("a"), r#"{"id":"b"}"#.into()]),
            "$.rules[1]",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "all_of" }),
        ),
        (
            document_with_rules(&[r#"{"id":"a","all_of":null}"#.into()]),
            "$.rules[0].all_of",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            document_with_rules(&[good_rule("a"), r#"{"id":"b","all_of":[]}"#.into()]),
            "$.rules[1].all_of",
            |k| matches!(k, DecodeErrorKind::EmptyAllOf),
        ),
        (
            document_with_rules(&[r#"{"id":"a","all_of":["tcp dport 1"]}"#.into()]),
            "$.rules[0].all_of[0]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_rules(&[good_rule("a"), good_rule("b"), good_rule("a")]),
            "$.rules[2].id",
            |k| matches!(k, DecodeErrorKind::DuplicateRuleId { id, first_index: 0 } if id.as_str() == "a"),
        ),
        (
            document_with_rules(&[format!(
                r#"{{"id":"a","all_of":[{GOOD_MATCH}],"comment":"x"}}"#
            )]),
            "$.rules[0].comment",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "comment"),
        ),
        (
            document_with_rules(&[r#""nft flush ruleset""#.into()]),
            "$.rules[0]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
    ]);
}

#[test]
fn rejects_malformed_reads() {
    let at = "$.rules[0].all_of[0].left";
    assert_rejections(vec![
        (
            document_with_left(r#"{"payload":{"field":"dport"}}"#),
            "$.rules[0].all_of[0].left.payload",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "protocol" }),
        ),
        (
            document_with_left(r#"{"payload":{"protocol":"tcp"}}"#),
            "$.rules[0].all_of[0].left.payload",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "field" }),
        ),
        (
            document_with_left(r#"{"payload":{"protocol":null,"field":"dport"}}"#),
            "$.rules[0].all_of[0].left.payload.protocol",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"tcp option":{}}"#),
            r#"$.rules[0].all_of[0].left["tcp option"]"#,
            |k| matches!(k, DecodeErrorKind::MissingField { field: "name" }),
        ),
        (document_with_left(r#""tcp dport""#), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::String,
                    ..
                }
            )
        }),
        (
            document_with_left(r#"{"payload":"tcp dport"}"#),
            "$.rules[0].all_of[0].left.payload",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"tcp option":"mptcp"}"#),
            r#"$.rules[0].all_of[0].left["tcp option"]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (document_with_left(r#"{}"#), at, |k| {
            matches!(k, DecodeErrorKind::MissingVariant { .. })
        }),
        (
            document_with_left(
                r#"{"payload":{"protocol":"tcp","field":"dport"},"tcp option":{"name":"mptcp"}}"#,
            ),
            at,
            |k| matches!(k, DecodeErrorKind::CompetingVariants { first, second } if first == "payload" && second == "tcp option"),
        ),
        (
            document_with_left(r#"{"&":{"payload":{"protocol":"tcp","field":"flags"}}}"#),
            r#"$.rules[0].all_of[0].left["&"]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Object,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}}]}"#),
            r#"$.rules[0].all_of[0].left["&"]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::TupleLength {
                        expected: 2,
                        found: 1
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},2,3]}"#),
            r#"$.rules[0].all_of[0].left["&"]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::TupleLength {
                        expected: 2,
                        found: 3
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[2,{"payload":{"protocol":"tcp","field":"flags"}}]}"#),
            r#"$.rules[0].all_of[0].left["&"][0]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::UnsignedInteger,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},"0x02"]}"#),
            r#"$.rules[0].all_of[0].left["&"][1]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},0]}"#),
            r#"$.rules[0].all_of[0].left["&"][1]"#,
            |k| matches!(k, DecodeErrorKind::ZeroMask),
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},-1]}"#),
            r#"$.rules[0].all_of[0].left["&"][1]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::NegativeInteger,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(
                r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18446744073709551616]}"#,
            ),
            r#"$.rules[0].all_of[0].left["&"][1]"#,
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::OtherNumber,
                        ..
                    }
                )
            },
        ),
        (
            document_with_left(r#"{"&":[{"tcp option":{"name":"mptcp"}},1]}"#),
            r#"$.rules[0].all_of[0].left["&"][0]["tcp option"]"#,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "tcp option"),
        ),
        (
            document_with_left(
                r#"{"&":[{"&":[{"payload":{"protocol":"tcp","field":"flags"}},3]},1]}"#,
            ),
            r#"$.rules[0].all_of[0].left["&"][0]["&"]"#,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "&"),
        ),
        (
            document_with_left(r#"{"&":[{},1]}"#),
            r#"$.rules[0].all_of[0].left["&"][0]"#,
            |k| matches!(k, DecodeErrorKind::MissingVariant { .. }),
        ),
        (
            document_with_left(r#"{"meta":{"key":"mark"}}"#),
            "$.rules[0].all_of[0].left.meta",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "meta"),
        ),
        (
            document_with_left(r#"{"|":[{"payload":{"protocol":"tcp","field":"flags"}},1]}"#),
            r#"$.rules[0].all_of[0].left["|"]"#,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "|"),
        ),
    ]);
}

#[test]
fn rejects_malformed_values_without_coercion() {
    let at = "$.rules[0].all_of[0].right";
    assert_rejections(vec![
        (document_with_right("false"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::False,
                    ..
                }
            )
        }),
        (document_with_right("null"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::Null,
                    ..
                }
            )
        }),
        (document_with_right("1.5"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::OtherNumber,
                    ..
                }
            )
        }),
        (document_with_right("1.0"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::OtherNumber,
                    ..
                }
            )
        }),
        (document_with_right("1e3"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::OtherNumber,
                    ..
                }
            )
        }),
        (document_with_right("18446744073709551616"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::OtherNumber,
                    ..
                }
            )
        }),
        (document_with_right("-1"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::NegativeInteger,
                    ..
                }
            )
        }),
        (document_with_right("[1,2]"), at, |k| {
            matches!(
                k,
                DecodeErrorKind::WrongType {
                    found: JsonKind::Array,
                    ..
                }
            )
        }),
        (document_with_right(r#""1.2.3""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#""256.0.0.1""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#""01.2.3.4""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#"" 192.0.2.1""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#""192.0.2.0/24""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#""::1""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#""2001:db8::1""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (
            document_with_right(r#""53""#),
            at,
            |k| matches!(k, DecodeErrorKind::InvalidIpv4 { text } if text == "53"),
        ),
        (document_with_right(r#""""#), at, |k| {
            matches!(k, DecodeErrorKind::InvalidIpv4 { .. })
        }),
        (document_with_right(r#"{}"#), at, |k| {
            matches!(k, DecodeErrorKind::MissingVariant { .. })
        }),
        (
            document_with_right(r#"{"set":[1,2]}"#),
            "$.rules[0].all_of[0].right.set",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "set"),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":24},"range":[1,2]}"#),
            at,
            |k| matches!(k, DecodeErrorKind::CompetingVariants { .. }),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":33}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| matches!(k, DecodeErrorKind::PrefixLength { len: 33 }),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":256}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| matches!(k, DecodeErrorKind::PrefixLength { len: 256 }),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":4294967320}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| matches!(k, DecodeErrorKind::PrefixLength { len: 4_294_967_320 }),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":-1}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::NegativeInteger,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":"24"}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"2001:db8::","len":32}}"#),
            "$.rules[0].all_of[0].right.prefix.addr",
            |k| matches!(k, DecodeErrorKind::InvalidIpv4 { .. }),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0"}}"#),
            "$.rules[0].all_of[0].right.prefix",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "len" }),
        ),
        (
            document_with_right(r#"{"prefix":"192.0.2.0/24"}"#),
            "$.rules[0].all_of[0].right.prefix",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[5,4]}"#),
            "$.rules[0].all_of[0].right.range",
            |k| matches!(k, DecodeErrorKind::ReversedRange { start: 5, end: 4 }),
        ),
        (
            document_with_right(r#"{"range":["1",2]}"#),
            "$.rules[0].all_of[0].right.range[0]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[1,2.5]}"#),
            "$.rules[0].all_of[0].right.range[1]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::OtherNumber,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[-1,2]}"#),
            "$.rules[0].all_of[0].right.range[0]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::NegativeInteger,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[1,18446744073709551616]}"#),
            "$.rules[0].all_of[0].right.range[1]",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::OtherNumber,
                        ..
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[]}"#),
            "$.rules[0].all_of[0].right.range",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::TupleLength {
                        expected: 2,
                        found: 0
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[1]}"#),
            "$.rules[0].all_of[0].right.range",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::TupleLength {
                        expected: 2,
                        found: 1
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":[1,2,3]}"#),
            "$.rules[0].all_of[0].right.range",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::TupleLength {
                        expected: 2,
                        found: 3
                    }
                )
            },
        ),
        (
            document_with_right(r#"{"range":"1-2"}"#),
            "$.rules[0].all_of[0].right.range",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::String,
                        ..
                    }
                )
            },
        ),
    ]);
}

#[test]
fn rejects_unsupported_operators() {
    let read = r#""left":{"payload":{"protocol":"tcp","field":"dport"}}"#;
    let cases = ["!=", "eq", "in", "<", "", " =="];
    for op in cases {
        let document = document_with_match(&format!(r#"{{{read},"op":"{op}","right":1}}"#));
        let error = PolicyRevision::from_json_str(&document).unwrap_err();
        assert_eq!(error.path().to_string(), "$.rules[0].all_of[0].op");
        assert!(
            matches!(error.kind(), DecodeErrorKind::UnsupportedOperator { op: found } if found == op),
            "{error:?}"
        );
    }
    assert_rejections(vec![
        (
            document_with_match(&format!(r#"{{{read},"right":1}}"#)),
            "$.rules[0].all_of[0]",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "op" }),
        ),
        (
            document_with_match(&format!(r#"{{{read},"op":null,"right":1}}"#)),
            "$.rules[0].all_of[0].op",
            |k| {
                matches!(
                    k,
                    DecodeErrorKind::WrongType {
                        found: JsonKind::Null,
                        ..
                    }
                )
            },
        ),
        (
            document_with_match(r#"{"op":"==","right":1}"#),
            "$.rules[0].all_of[0]",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "left" }),
        ),
        (
            document_with_match(&format!(r#"{{{read},"op":"=="}}"#)),
            "$.rules[0].all_of[0]",
            |k| matches!(k, DecodeErrorKind::MissingField { field: "right" }),
        ),
    ]);
}

#[test]
fn rejects_caller_placement_and_verdict_at_every_level() {
    let mut cases: Vec<(String, &str, KindCheck)> = Vec::new();
    // Revision level.
    for (field, path) in [
        ("table", "$.table"),
        ("chain", "$.chain"),
        ("verdict", "$.verdict"),
        ("interface", "$.interface"),
        ("priority", "$.priority"),
        ("command", "$.command"),
    ] {
        cases.push((
            format!(r#"{{"schema_version":1,"rules":[],"{field}":"x"}}"#),
            path,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { .. }),
        ));
    }

    // Rule level.
    for (field, path) in [
        ("verdict", "$.rules[0].verdict"),
        ("interface", "$.rules[0].interface"),
        ("table", "$.rules[0].table"),
        ("chain", "$.rules[0].chain"),
        ("priority", "$.rules[0].priority"),
        ("command", "$.rules[0].command"),
    ] {
        cases.push((
            document_with_rules(&[format!(
                r#"{{"id":"a","all_of":[{GOOD_MATCH}],"{field}":"accept"}}"#
            )]),
            path,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { .. }),
        ));
    }

    // Match, read and value levels.
    cases.extend([
        (
            document_with_match(
                r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":1,"verdict":"accept"}"#,
            ),
            "$.rules[0].all_of[0].verdict",
            (|k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "verdict"))
                as KindCheck,
        ),
        (
            document_with_left(r#"{"payload":{"protocol":"tcp","field":"dport","interface":"eth0"}}"#),
            "$.rules[0].all_of[0].left.payload.interface",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "interface"),
        ),
        (
            document_with_left(r#"{"tcp option":{"name":"mptcp","field":"kind"}}"#),
            r#"$.rules[0].all_of[0].left["tcp option"].field"#,
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "field"),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":24,"table":"filter"}}"#),
            "$.rules[0].all_of[0].right.prefix.table",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "table"),
        ),
        (
            document_with_match(r#""tcp dport 22 drop""#),
            "$.rules[0].all_of[0]",
            |k| matches!(k, DecodeErrorKind::WrongType { found: JsonKind::String, .. }),
        ),
    ]);
    assert_rejections(cases);
}

#[test]
fn rejects_repeated_keys_instead_of_keeping_the_last() {
    // Raw text: building these with a map would lose the first value.
    assert_rejections(vec![
        (
            r#"{"schema_version":2,"schema_version":1,"rules":[]}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "schema_version"),
        ),
        (
            r#"{"schema_version":1,"rules":[],"rules":[]}"#.into(),
            "$.rules",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "rules"),
        ),
        (
            document_with_rules(&[format!(r#"{{"id":"a","id":"b","all_of":[{GOOD_MATCH}]}}"#)]),
            "$.rules[0].id",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "id"),
        ),
        (
            document_with_rules(&[format!(
                r#"{{"id":"a","all_of":[],"all_of":[{GOOD_MATCH}]}}"#
            )]),
            "$.rules[0].all_of",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "all_of"),
        ),
        (
            document_with_match(
                r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"!=","op":"==","right":1}"#,
            ),
            "$.rules[0].all_of[0].op",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "op"),
        ),
        (
            document_with_match(
                r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":1,"right":2}"#,
            ),
            "$.rules[0].all_of[0].right",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "right"),
        ),
        (
            document_with_left(
                r#"{"payload":{"protocol":"tcp","field":"dport"},"payload":{"protocol":"udp","field":"dport"}}"#,
            ),
            "$.rules[0].all_of[0].left.payload",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "payload"),
        ),
        (
            document_with_left(
                r#"{"payload":{"protocol":"tcp","protocol":"udp","field":"dport"}}"#,
            ),
            "$.rules[0].all_of[0].left.payload.protocol",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "protocol"),
        ),
        (
            document_with_left(r#"{"tcp option":{"name":"mptcp","name":"sack"}}"#),
            r#"$.rules[0].all_of[0].left["tcp option"].name"#,
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "name"),
        ),
        (
            document_with_right(r#"{"prefix":{"addr":"192.0.2.0","len":33,"len":24}}"#),
            "$.rules[0].all_of[0].right.prefix.len",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "len"),
        ),
        (
            document_with_right(r#"{"range":[1,2],"range":[3,4]}"#),
            "$.rules[0].all_of[0].right.range",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "range"),
        ),
    ]);
}

#[test]
fn rule_decoder_applies_the_same_grammar() {
    let rule = Rule::from_json_str(&good_rule("a")).unwrap();
    assert_eq!(rule.id().as_str(), "a");
    assert_eq!(Rule::from_json_str(&rule.to_json_string()).unwrap(), rule);

    for (text, path) in [
        (
            format!(r#"{{"id":"a","id":"b","all_of":[{GOOD_MATCH}]}}"#),
            "$.id",
        ),
        (
            format!(r#"{{"id":"a","all_of":[{GOOD_MATCH}],"verdict":"accept"}}"#),
            "$.verdict",
        ),
        (r#"{"id":"a","all_of":[]}"#.to_owned(), "$.all_of"),
        ("null".to_owned(), "$"),
    ] {
        let error = Rule::from_json_str(&text).unwrap_err();
        assert_eq!(error.path().to_string(), path, "{text}: {error}");
    }
}

#[test]
fn error_display_names_the_location() {
    let error = PolicyRevision::from_json_str(&document_with_left(
        r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},0]}"#,
    ))
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        r#"$.rules[0].all_of[0].left["&"][1]: mask must be nonzero"#
    );
}

#[test]
fn from_slice_matches_from_str() {
    assert_eq!(
        PolicyRevision::from_json_slice(ALL_VARIANTS.as_bytes()).unwrap(),
        decode(ALL_VARIANTS)
    );
    assert!(matches!(
        PolicyRevision::from_json_slice(b"\xff").unwrap_err().kind(),
        DecodeErrorKind::Syntax(_)
    ));
}
