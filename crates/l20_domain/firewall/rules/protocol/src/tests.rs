use std::error::Error as _;
use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use pplx_space_firewall_rules::{
    FirewallRuleRevision, HeaderField, HeaderProtocol, IntegerRange, Ipv4Prefix, Match,
    PayloadHeader, Read, Rule, RuleId, TcpOptionName, Value,
};

use crate::{DecodeError, decode_revision, decode_revision_bytes, encode_revision};

/// Every read and value kind, in canonical form, as the previous hand-written
/// encoder wrote it.
const ALL_VARIANTS: &str = concat!(
    r#"{"schema_version":1,"rules":["#,
    r#"{"id":"first","all_of":["#,
    r#"{"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":"192.0.2.1"},"#,
    r#"{"left":{"tcp option":{"name":"mptcp"}},"op":"==","right":true},"#,
    r#"{"left":{"&":[{"payload":{"protocol":"tcp","field":"flags"}},18]},"op":"==","right":2}]},"#,
    r#"{"id":"second","all_of":["#,
    r#"{"left":{"payload":{"protocol":"ip","field":"saddr"}},"op":"==","right":{"prefix":{"addr":"198.51.100.0","len":24}}},"#,
    r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":{"range":[1024,65535]}}]}"#,
    r#"]}"#,
);

const DPORT: &str = r#"{"payload":{"protocol":"tcp","field":"dport"}}"#;

fn header(protocol: &str, field: &str) -> PayloadHeader {
    PayloadHeader::new(
        HeaderProtocol::new(protocol).unwrap(),
        HeaderField::new(field).unwrap(),
    )
}

fn rule(id: &str, all_of: Vec<Match>) -> Rule {
    Rule::new(RuleId::new(id).unwrap(), all_of).unwrap()
}

fn doc(rules: &str) -> String {
    format!(r#"{{"schema_version":1,"rules":[{rules}]}}"#)
}

fn with_match(comparison: &str) -> String {
    doc(&format!(r#"{{"id":"r","all_of":[{comparison}]}}"#))
}

fn with_op(op: &str) -> String {
    with_match(&format!(r#"{{"left":{DPORT},"op":{op},"right":1}}"#))
}

fn with_left(left: &str) -> String {
    with_match(&format!(r#"{{"left":{left},"op":"==","right":1}}"#))
}

fn with_right(right: &str) -> String {
    with_match(&format!(r#"{{"left":{DPORT},"op":"==","right":{right}}}"#))
}

/// Expands a table of `<level> <json>` lines. The level places the JSON in an
/// otherwise valid document: as the whole `document`, the `rules` array, one
/// `match`, or that match's `left`, `op` or `right`. `GOOD` is a valid
/// comparison.
fn documents(table: &str) -> Vec<String> {
    let good = format!(r#"{{"left":{DPORT},"op":"==","right":1}}"#);
    table
        .lines()
        .skip(1)
        .map(|line| {
            let (level, json) = line.split_once(' ').unwrap_or((line, ""));
            let json = json.replace("GOOD", &good);
            match level {
                "document" => json,
                "rules" => doc(&json),
                "match" => with_match(&json),
                "left" => with_left(&json),
                "op" => with_op(&json),
                "right" => with_right(&json),
                _ => panic!("unknown level in {line:?}"),
            }
        })
        .collect()
}

#[test]
fn every_variant_decodes_to_the_model_and_encodes_to_the_golden_bytes() {
    let expected = FirewallRuleRevision::new(vec![
        rule(
            "first",
            vec![
                Match::new(
                    Read::Payload(header("ip", "daddr")),
                    Value::Ipv4(Ipv4Addr::new(192, 0, 2, 1)),
                ),
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
        ),
        rule(
            "second",
            vec![
                Match::new(
                    Read::Payload(header("ip", "saddr")),
                    Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(198, 51, 100, 0), 24).unwrap()),
                ),
                Match::new(
                    Read::Payload(header("tcp", "dport")),
                    Value::Range(IntegerRange::new(1024, 65535).unwrap()),
                ),
            ],
        ),
    ])
    .unwrap();
    assert_eq!(encode_revision(&expected), ALL_VARIANTS);
    assert_eq!(decode_revision(ALL_VARIANTS).unwrap(), expected);
    assert_eq!(
        decode_revision_bytes(ALL_VARIANTS.as_bytes()).unwrap(),
        expected
    );

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
    assert_eq!(decode_revision(reordered).unwrap(), expected);
}

/// The acceptance example as a caller writes it, and its canonical bytes as the
/// previous hand-written encoder wrote them.
#[test]
fn the_acceptance_example_encodes_to_the_golden_bytes() {
    let written = r#"{
  "schema_version": 1,
  "rules": [
    {"id": "dns", "all_of": [
      {"left": {"payload": {"protocol": "udp", "field": "dport"}}, "op": "==", "right": 53}
    ]},
    {"id": "web", "all_of": [
      {"left": {"payload": {"protocol": "ip", "field": "daddr"}}, "op": "==", "right": {"prefix": {"addr": "198.51.100.0", "len": 24}}},
      {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": 80}
    ]},
    {"id": "metrics", "all_of": [
      {"left": {"payload": {"protocol": "tcp", "field": "dport"}}, "op": "==", "right": {"range": [9100, 9199]}}
    ]}
  ]
}"#;
    let golden = concat!(
        r#"{"schema_version":1,"rules":["#,
        r#"{"id":"dns","all_of":[{"left":{"payload":{"protocol":"udp","field":"dport"}},"op":"==","right":53}]},"#,
        r#"{"id":"web","all_of":[{"left":{"payload":{"protocol":"ip","field":"daddr"}},"op":"==","right":{"prefix":{"addr":"198.51.100.0","len":24}}},"#,
        r#"{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":80}]},"#,
        r#"{"id":"metrics","all_of":[{"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"==","right":{"range":[9100,9199]}}]}"#,
        r#"]}"#,
    );
    let revision = decode_revision(written).unwrap();
    assert_eq!(encode_revision(&revision), golden);
    assert_eq!(decode_revision(golden).unwrap(), revision);
}

#[test]
fn names_are_kept_verbatim_and_escaped_as_json_strings() {
    let adversarial = || header(r#"say "hi" \ bye"#, r#" {"field":1} "#);
    let revision = FirewallRuleRevision::new(vec![
        rule(
            r#"q"\/x"#,
            vec![Match::new(Read::Payload(adversarial()), Value::True)],
        ),
        rule(
            r"\\",
            vec![Match::new(
                Read::TcpOption(TcpOptionName::new(r#"opt "a" \b"#).unwrap()),
                Value::True,
            )],
        ),
    ])
    .unwrap();
    let golden = concat!(
        r#"{"schema_version":1,"rules":["#,
        r#"{"id":"q\"\\/x","all_of":[{"left":{"payload":{"protocol":"say \"hi\" \\ bye","field":" {\"field\":1} "}},"op":"==","right":true}]},"#,
        r#"{"id":"\\\\","all_of":[{"left":{"tcp option":{"name":"opt \"a\" \\b"}},"op":"==","right":true}]}"#,
        r#"]}"#,
    );
    assert_eq!(encode_revision(&revision), golden);
    assert_eq!(decode_revision(golden).unwrap(), revision);
}

/// Boundary documents already in canonical form, one per line as in
/// [`REJECTED`]: each decodes and encodes back to the same bytes.
const CANONICAL: &str = r##"
document {"schema_version":1,"rules":[]}
right 0
right 18446744073709551615
right "0.0.0.0"
right "255.255.255.255"
right {"prefix":{"addr":"0.0.0.0","len":0}}
right {"prefix":{"addr":"192.0.2.7","len":24}}
right {"prefix":{"addr":"192.0.2.7","len":32}}
right {"range":[5,5]}
right {"range":[0,18446744073709551615]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},18446744073709551615]}
left {"payload":{"protocol":"Not A Protocol","field":" spaced "}}
rules {"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","all_of":[GOOD]}
rules {"id":"!#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[]^_`ab","all_of":[GOOD]}
rules {"id":"cdefghijklmnopqrstuvwxyz{|}~","all_of":[GOOD]}
"##;

#[test]
fn boundary_values_are_canonical_fixed_points() {
    for document in documents(CANONICAL) {
        let revision = decode_revision(&document).unwrap_or_else(|e| panic!("{document}: {e}"));
        assert_eq!(encode_revision(&revision), document);
    }
}

#[test]
fn the_version_is_read_before_the_grammar() {
    let errors = |document: &str| {
        [
            decode_revision(document).unwrap_err(),
            decode_revision_bytes(document.as_bytes()).unwrap_err(),
        ]
    };
    for (document, version) in [
        (r#"{"schema_version":2,"rules":[]}"#, 2),
        (r#"{"schema_version":2,"rules":[],"x":{}}"#, 2),
        (r#"{"x":{},"rules":"none","rules":1,"schema_version":2}"#, 2),
        (r#"{"schema_version":0,"rules":[]}"#, 0),
    ] {
        for error in errors(document) {
            assert!(
                matches!(error, DecodeError::UnsupportedSchemaVersion(found) if found == version),
                "{document}: {error:?}"
            );
        }
    }
    for document in [
        r#"{"rules":[]}"#,
        r#"{"rules":[],"x":{}}"#,
        r#"{"schema_version":null,"rules":[]}"#,
        r#"{"schema_version":1.0,"rules":[]}"#,
        r#"{"schema_version":-1,"rules":[]}"#,
        r#"{"schema_version":"1","rules":[]}"#,
        r#"{"schema_version":true,"rules":[]}"#,
        r#"{"schema_version":2,"schema_version":1,"rules":[]}"#,
        r#"{"schema_version":1,"rules":[],"schema_version":1}"#,
        r#"[1,[]]"#,
    ] {
        for error in errors(document) {
            assert!(
                matches!(error, DecodeError::Json(_)),
                "{document}: {error:?}"
            );
        }
    }
}

#[test]
fn errors_locate_the_problem() {
    let error = decode_revision("{\n  \"schema_version\": 1,\n  \"rules\": [] x").unwrap_err();
    let DecodeError::Json(json) = &error else {
        panic!("{error:?}");
    };
    assert_eq!((json.line(), json.column()), (3, 15));
    assert!(error.source().is_some());

    let rule_json = |id: &str, all_of: &str| format!(r#"{{"id":"{id}","all_of":[{all_of}]}}"#);
    let good = format!(r#"{{"left":{DPORT},"op":"==","right":1}}"#);
    let error = decode_revision(&doc(
        &[rule_json("a", &good), rule_json("web", "")].join(",")
    ))
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        r#"invalid firewall rules: rules[1] ("web"): rule "web" has an empty all_of"#
    );
}

/// Every document the previous decoder rejected, plus the forms a derived
/// `Deserialize` would otherwise accept: arrays for objects, `{"==":null}` for
/// the operator, and a unit-variant string for a read. Lines are expanded by
/// [`documents`].
const REJECTED: &str = r##"
document
document null
document []
document "add rule ip filter output drop"
document {}
document {"schema_version":1,"rules":[]} x
document {"schema_version":1,"rules":[]} {}
document {"schema_version":1}
document {"schema_version":1,"rules":"[]"}
document {"schema_version":1,"rules":null}
document {"schema_version":1,"rules":{}}
rules "nft flush ruleset"
document {"schema_version":1,"rules":[],"rules":[]}
document {"schema_version":1,"rules":[],"x":{}}
document {"schema_version":1,"rules":[],"123":1}
document {"schema_version":1,"rules":[],"_x9":1}
document {"schema_version":1,"rules":[],"a\u0000b":1}
document {"schema_version":1,"rules":[],"verdict":"x"}
document {"schema_version":1,"rules":[],"table":"x"}
document {"schema_version":1,"rules":[],"chain":"x"}
document {"schema_version":1,"rules":[],"priority":"x"}
document {"schema_version":1,"rules":[],"interface":"x"}
document {"schema_version":1,"rules":[],"command":"x"}
rules {"id":"a","all_of":[GOOD],"comment":"x"}
rules {"id":"a","all_of":[GOOD],"verdict":"accept"}
rules {"id":"a","all_of":[GOOD],"table":"accept"}
rules {"id":"a","all_of":[GOOD],"chain":"accept"}
rules {"id":"a","all_of":[GOOD],"priority":"accept"}
rules {"id":"a","all_of":[GOOD],"interface":"accept"}
rules {"id":"a","all_of":[GOOD],"command":"accept"}
rules {"id":"","all_of":[GOOD]}
rules {"id":"a b","all_of":[GOOD]}
rules {"id":"\u001b[31mweb","all_of":[GOOD]}
rules {"id":"a\nb","all_of":[GOOD]}
rules {"id":"web\u200b","all_of":[GOOD]}
rules {"id":"wéb","all_of":[GOOD]}
rules {"id":7,"all_of":[GOOD]}
rules {"id":null,"all_of":[GOOD]}
rules {"id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"id":"a","all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"id":"b","all_of":[GOOD]},{"id":"a","all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"id":"b","all_of":[GOOD]},{"id":"b","all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"id":"b"}
rules {"id":"a","all_of":[GOOD]},{"id":"b","all_of":[]}
rules {"id":"a","all_of":null}
rules {"id":"a","all_of":["tcp dport 1"]}
rules {"id":"a","id":"b","all_of":[GOOD]}
rules {"id":"a","all_of":[],"all_of":[GOOD]}
rules ["a",[GOOD]]
match "tcp dport 22 drop"
match {"op":"==","right":1}
match {"left":{"payload":{"protocol":"tcp","field":"dport"}},"right":1}
match {"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"=="}
right 1,"right":2
right 1,"verdict":"accept"
left {"payload":{"protocol":"tcp","field":"dport"}},"op":"!="
match [{"payload":{"protocol":"tcp","field":"dport"}},"==",1]
op "!="
op "<"
op "in"
op "eq"
op ""
op " =="
op null
op {"==":null}
left "tcp dport"
left "payload"
left {}
left {"meta":{"key":"mark"}}
left {"|":[{"payload":{"protocol":"tcp","field":"flags"}},1]}
left {"payload":{"protocol":"tcp","field":"dport"},"tcp option":{"name":"mptcp"}}
left {"payload":{"protocol":"tcp","field":"dport"},"payload":{"protocol":"udp","field":"dport"}}
left {"payload":"tcp dport"}
left {"payload":["tcp","dport"]}
left {"payload":{"field":"dport"}}
left {"payload":{"protocol":"tcp"}}
left {"payload":{"protocol":null,"field":"dport"}}
left {"payload":{"protocol":"","field":"dport"}}
left {"payload":{"protocol":"tcp","field":"d\tport"}}
left {"payload":{"protocol":"tcp","protocol":"udp","field":"dport"}}
left {"payload":{"protocol":"tcp","field":"dport","interface":"eth0"}}
left {"tcp option":"mptcp"}
left {"tcp option":{}}
left {"tcp option":["mptcp"]}
left {"tcp option":{"name":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
left {"tcp option":{"name":"mptcp","name":"sack"}}
left {"tcp option":{"name":"mptcp","field":"kind"}}
left {"&":{"payload":{"protocol":"tcp","field":"flags"}}}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}}]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},2,3]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},0]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},-1]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},18446744073709551616]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},"0x02"]}
left {"&":[2,{"payload":{"protocol":"tcp","field":"flags"}}]}
left {"&":[{},1]}
left {"&":[{"tcp option":{"name":"mptcp"}},1]}
left {"&":[{"&":[{"payload":{"protocol":"tcp","field":"flags"}},3]},1]}
left {"&":[[{"protocol":"tcp","field":"flags"}],1]}
left {"&":[{"payload":{"protocol":"\u001b[0m","field":"flags"}},1]}
right false
right null
right -1
right 1.0
right 1.5
right 1e3
right 18446744073709551616
right 1e400
right [1,2]
right ""
right " 192.0.2.1"
right "01.2.3.4"
right "1.2.3"
right "256.0.0.1"
right "192.0.2.0/24"
right "2001:db8::1"
right "::1"
right "53"
right [192,0,2,1]
right {}
right {"set":[1,2]}
right {"prefix":{"addr":"192.0.2.0","len":24},"range":[1,2]}
right {"prefix":{"addr":"192.0.2.0","len":24},"x":1}
right {"range":[1,2],"range":[3,4]}
right {"prefix":"192.0.2.0/24"}
right {"prefix":{"addr":"192.0.2.0","len":"24"}}
right {"prefix":{"addr":"192.0.2.0","len":-1}}
right {"prefix":{"addr":"192.0.2.0","len":33}}
right {"prefix":{"addr":"192.0.2.0","len":256}}
right {"prefix":{"addr":"192.0.2.0","len":4294967320}}
right {"prefix":{"addr":"192.0.2.0"}}
right {"prefix":{"addr":"2001:db8::","len":32}}
right {"prefix":{"addr":"192.0.2.0","len":24,"table":"filter"}}
right {"prefix":{"addr":"192.0.2.0","len":33,"len":24}}
right {"prefix":["192.0.2.0",24]}
right {"range":"1-2"}
right {"range":[]}
right {"range":[1]}
right {"range":[1,2,3]}
right {"range":[5,4]}
right {"range":["1",2]}
right {"range":[-1,2]}
right {"range":[1,2.5]}
right {"range":[1,18446744073709551616]}
right {"range":{"start":1,"end":2}}
"##;

#[test]
fn rejects_every_document_outside_the_grammar() {
    let mut documents = documents(REJECTED);
    let depth = 100_000;
    documents.push(with_right(&format!(
        "{}{}",
        "[".repeat(depth),
        "]".repeat(depth)
    )));
    documents.push(with_right(&format!(
        "{}1{}",
        r#"{"a":"#.repeat(depth),
        "}".repeat(depth)
    )));

    for document in &documents {
        assert!(decode_revision(document).is_err(), "accepted {document}");
        assert!(
            decode_revision_bytes(document.as_bytes()).is_err(),
            "accepted {document}"
        );
    }
    decode_revision_bytes(b"\xff").unwrap_err();
}
