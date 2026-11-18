use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use pplx_space_firewall_rules::{
    IntegerRange, Ipv4Prefix, MAX_NAME_LEN, NameError, NameKind, TcpOptionName,
};

use super::*;
use crate::encode_revision;
use crate::test_fixtures::{
    document_with_left, document_with_match, document_with_right, header, id, payload_eq,
};

type KindCheck = fn(&DecodeErrorKind) -> bool;

/// Asserts that every document is rejected at the given path with a matching
/// kind, and that none decodes to a revision.
fn assert_rejections(cases: Vec<(String, &str, KindCheck)>) {
    for (document, path, check) in cases {
        let error = decode_revision(&document).expect_err(&format!("accepted {document}"));
        assert_eq!(error.path().to_string(), path, "{document}: {error}");
        assert!(check(error.kind()), "{document}: unexpected {error:?}");
    }
}

fn decode(document: &str) -> FirewallRuleRevision {
    decode_revision(document).unwrap_or_else(|error| panic!("{document}: {error}"))
}

/// Decodes a document, writes it back canonically, and decodes that again.
fn assert_round_trips(revision: &FirewallRuleRevision) {
    let text = encode_revision(revision);
    assert_eq!(&decode_revision(&text).unwrap(), revision, "{text}");
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
    let revision = decode(ALL_VARIANTS);

    let expected = FirewallRuleRevision::new(vec![
        Rule::new(
            id("first"),
            vec![
                payload_eq("ip", "daddr", Value::Ipv4(Ipv4Addr::new(192, 0, 2, 1))),
                Match::new(
                    Read::TcpOption(TcpOptionName::new("mptcp").unwrap()),
                    Value::True,
                ),
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
            id("second"),
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
    assert_eq!(revision, expected);
    assert_round_trips(&revision);
}

#[test]
fn canonical_form_is_stable_and_keeps_rule_and_match_order() {
    let revision = decode(ALL_VARIANTS);
    assert_eq!(
        encode_revision(&revision),
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
fn key_order_and_whitespace_do_not_change_the_revision() {
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
    let revision = decode(reordered);
    assert_eq!(revision, decode(ALL_VARIANTS));
    assert_eq!(
        encode_revision(&revision),
        encode_revision(&decode(ALL_VARIANTS))
    );
}

#[test]
fn explicit_empty_rule_list_is_a_snapshot() {
    let revision = decode(r#"{"schema_version":1,"rules":[]}"#);
    assert!(revision.rules().is_empty());
    assert_eq!(
        encode_revision(&revision),
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
            r#"{"range":[5,5]}"#,
            Value::Range(IntegerRange::new(5, 5).unwrap()),
        ),
        (
            r#"{"range":[0,18446744073709551615]}"#,
            Value::Range(IntegerRange::new(0, u64::MAX).unwrap()),
        ),
    ];
    for (right, expected) in cases {
        let revision = decode(&document_with_right(right));
        assert_eq!(
            revision.rules()[0].all_of()[0].right(),
            &expected,
            "{right}"
        );
        assert_round_trips(&revision);
    }

    // Host bits are kept as written, not masked off.
    let host_bits = decode(&document_with_right(
        r#"{"prefix":{"addr":"192.0.2.7","len":24}}"#,
    ));
    let Value::Ipv4Prefix(prefix) = host_bits.rules()[0].all_of()[0].right() else {
        panic!("not a prefix: {host_bits:?}");
    };
    assert_eq!(prefix.addr(), Ipv4Addr::new(192, 0, 2, 7));
    assert_eq!(prefix.prefix_len(), 24);
    assert_round_trips(&host_bits);

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
    let revision = decode(&document_with_match(
        r#"{"left":{"payload":{"protocol":"Not A Protocol","field":" spaced "}},"op":"==","right":1},
           {"left":{"tcp option":{"name":"Option-That-Does-Not-Exist"}},"op":"==","right":true}"#,
    ));
    let all_of = revision.rules()[0].all_of();
    let Read::Payload(payload) = all_of[0].left() else {
        panic!("not a payload read: {:?}", all_of[0]);
    };
    assert_eq!(payload.protocol().as_str(), "Not A Protocol");
    assert_eq!(payload.field().as_str(), " spaced ");
    let Read::TcpOption(option) = all_of[1].left() else {
        panic!("not a TCP option read: {:?}", all_of[1]);
    };
    assert_eq!(option.as_str(), "Option-That-Does-Not-Exist");
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
        // The version is read first, so a newer document is reported as one
        // whatever else it holds.
        (
            r#"{"schema_version":2,"rules":[],"x":{}}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::UnsupportedSchemaVersion { found: 2 }),
        ),
        (
            r#"{"x":{},"rules":"none","rules":1,"schema_version":2}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::UnsupportedSchemaVersion { found: 2 }),
        ),
        (
            r#"{"schema_version":1,"rules":[],"x":{}}"#.into(),
            "$.x",
            |k| matches!(k, DecodeErrorKind::UnexpectedField { field } if field == "x"),
        ),
        (r#"{"rules":[],"x":{}}"#.into(), "$", |k| {
            matches!(
                k,
                DecodeErrorKind::MissingField {
                    field: "schema_version"
                }
            )
        }),
        (
            r#"{"schema_version":1,"rules":[],"schema_version":1}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "schema_version"),
        ),
        (
            r#"{"schema_version":2,"schema_version":1,"rules":[]}"#.into(),
            "$.schema_version",
            |k| matches!(k, DecodeErrorKind::DuplicateField { field } if field == "schema_version"),
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
    let revision = decode(&document_with_rules(&[
        good_rule("a"),
        good_rule("b"),
        format!(r#"{{"id":"c","all_of":[{GOOD_MATCH},{GOOD_MATCH}]}}"#),
    ]));
    assert_eq!(revision.rules().len(), 3);
    assert_eq!(revision.rules()[2].all_of().len(), 2);
    assert_eq!(revision.rule(&id("b")).unwrap().id(), &id("b"));
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
            document_with_rules(&[good_rule("a"), good_rule("b"), good_rule("b")]),
            "$.rules[2].id",
            |k| matches!(k, DecodeErrorKind::DuplicateRuleId { id, first_index: 1 } if id.as_str() == "b"),
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
        // Too large for a 64-bit float, so tokenizing fails: the error is at
        // the root and carries the line and column of the number's last digit.
        (
            document_with_right("1e400"),
            "$",
            |k| matches!(k, DecodeErrorKind::Syntax(error) if (error.line(), error.column()) == (1, 127)),
        ),
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
        let error = decode_revision(&document).unwrap_err();
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
fn error_display_names_the_location() {
    let error = decode_revision(&document_with_left(
        r#"{"&":[{"payload":{"protocol":"tcp","field":"flags"}},0]}"#,
    ))
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        r#"$.rules[0].all_of[0].left["&"][1]: mask must be nonzero"#
    );

    let duplicate =
        decode_revision(&document_with_rules(&[good_rule("a"), good_rule("a")])).unwrap_err();
    assert_eq!(
        duplicate.to_string(),
        r#"$.rules[1].id: rule ID "a" repeats the rule at index 0"#
    );

    let overflow = decode_revision(&document_with_right("1e400")).unwrap_err();
    assert_eq!(
        overflow.to_string(),
        "$: malformed JSON at line 1, column 127"
    );

    // A refused character is written escaped, never raw.
    let escape = decode_revision(&document_with_rules(&[good_rule(r"\u001b[31mweb")])).unwrap_err();
    assert_eq!(
        escape.to_string(),
        r"$.rules[0].id: a rule ID may hold only printable ASCII other than space, found '\u{1b}' at byte 0"
    );
}

#[test]
fn rejects_names_that_break_their_syntax_rule() {
    let long = "a".repeat(MAX_NAME_LEN + 1);
    let id_at = "$.rules[0].id";
    let invalid = |kind, index, found| NameError::InvalidCharacter { kind, index, found };
    let cases: Vec<(String, &str, NameError)> = vec![
        (
            document_with_rules(&[good_rule("")]),
            id_at,
            NameError::Empty {
                kind: NameKind::RuleId,
            },
        ),
        (
            document_with_rules(&[good_rule(&long)]),
            id_at,
            NameError::TooLong {
                kind: NameKind::RuleId,
                len: MAX_NAME_LEN + 1,
            },
        ),
        (
            document_with_rules(&[good_rule("a b")]),
            id_at,
            invalid(NameKind::RuleId, 1, ' '),
        ),
        (
            document_with_rules(&[good_rule(r"a\nb")]),
            id_at,
            invalid(NameKind::RuleId, 1, '\n'),
        ),
        (
            document_with_rules(&[good_rule(r"\u001b[31mweb")]),
            id_at,
            invalid(NameKind::RuleId, 0, '\u{1b}'),
        ),
        (
            document_with_rules(&[good_rule(r"web\u200b")]),
            id_at,
            invalid(NameKind::RuleId, 3, '\u{200b}'),
        ),
        (
            document_with_rules(&[good_rule("w\u{e9}b")]),
            id_at,
            invalid(NameKind::RuleId, 1, '\u{e9}'),
        ),
        (
            document_with_left(r#"{"payload":{"protocol":"","field":"dport"}}"#),
            "$.rules[0].all_of[0].left.payload.protocol",
            NameError::Empty {
                kind: NameKind::HeaderProtocol,
            },
        ),
        (
            document_with_left(r#"{"payload":{"protocol":"tcp","field":"d\tport"}}"#),
            "$.rules[0].all_of[0].left.payload.field",
            invalid(NameKind::HeaderField, 1, '\t'),
        ),
        (
            document_with_left(&format!(r#"{{"tcp option":{{"name":"{long}"}}}}"#)),
            r#"$.rules[0].all_of[0].left["tcp option"].name"#,
            NameError::TooLong {
                kind: NameKind::TcpOptionName,
                len: MAX_NAME_LEN + 1,
            },
        ),
        (
            document_with_left(r#"{"&":[{"payload":{"protocol":"\u001b[0m","field":"flags"}},1]}"#),
            r#"$.rules[0].all_of[0].left["&"][0].payload.protocol"#,
            invalid(NameKind::HeaderProtocol, 0, '\u{1b}'),
        ),
    ];
    for (document, path, expected) in cases {
        let error = decode_revision(&document).expect_err(&format!("accepted {document}"));
        assert_eq!(error.path().to_string(), path, "{document}: {error}");
        assert!(
            matches!(error.kind(), DecodeErrorKind::InvalidName(found) if *found == expected),
            "{document}: {error:?}"
        );
    }

    // The limits themselves are accepted.
    let longest = "a".repeat(MAX_NAME_LEN);
    let revision = decode(&document_with_rules(&[good_rule(&longest)]));
    assert_eq!(revision.rules()[0].id().as_str(), longest);
    // Every printable character other than space, split into IDs that fit.
    // Quote and backslash are left out only because `good_rule` writes the ID
    // into the JSON unescaped; the encoder tests cover them.
    let printable: String = (0x21u8..=0x7e)
        .map(char::from)
        .filter(|c| !matches!(c, '"' | '\\'))
        .collect();
    let ids: Vec<&str> = printable
        .as_bytes()
        .chunks(MAX_NAME_LEN)
        .map(|chunk| std::str::from_utf8(chunk).unwrap())
        .collect();
    let rules: Vec<String> = ids.iter().map(|id| good_rule(id)).collect();
    let revision = decode(&document_with_rules(&rules));
    let decoded: Vec<&str> = revision.rules().iter().map(|r| r.id().as_str()).collect();
    assert_eq!(decoded, ids);
}

#[test]
fn deep_nesting_is_rejected_while_tokenizing() {
    // Far beyond serde_json's recursion limit of 128: rejected as malformed
    // JSON at the root, without overflowing the stack.
    let depth = 100_000;
    for right in [
        format!("{}{}", "[".repeat(depth), "]".repeat(depth)),
        format!("{}1{}", r#"{"a":"#.repeat(depth), "}".repeat(depth)),
    ] {
        let error = decode_revision(&document_with_right(&right)).unwrap_err();
        assert_eq!(error.path().to_string(), "$");
        assert!(
            matches!(error.kind(), DecodeErrorKind::Syntax(inner) if inner.is_syntax()),
            "{error:?}"
        );
    }
}

#[test]
fn paths_bracket_keys_that_dot_shorthand_cannot_name() {
    // RFC 9535 dot shorthand cannot start with a digit, and a bracketed name
    // is a JSON string, so a control character is written as a \u escape.
    for (key, path) in [
        (r#""123""#, r#"$["123"]"#),
        (r#""a\u0000b""#, r#"$["a\u0000b"]"#),
        (r#""_x9""#, "$._x9"),
    ] {
        let document = format!(r#"{{"schema_version":1,"rules":[],{key}:1}}"#);
        let error = decode_revision(&document).unwrap_err();
        assert_eq!(error.path().to_string(), path, "{document}: {error}");
        assert!(
            matches!(error.kind(), DecodeErrorKind::UnexpectedField { .. }),
            "{error:?}"
        );
    }
}

#[test]
fn from_slice_matches_from_str() {
    assert_eq!(
        decode_revision_bytes(ALL_VARIANTS.as_bytes()).unwrap(),
        decode(ALL_VARIANTS)
    );
    for bytes in [&b"\xff"[..], br#"{"schema_version":1,"rules":[]} x"#] {
        assert!(matches!(
            decode_revision_bytes(bytes).unwrap_err().kind(),
            DecodeErrorKind::Syntax(_)
        ));
    }
}
