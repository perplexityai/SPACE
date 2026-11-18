use std::net::Ipv4Addr;
use std::num::NonZeroU64;

use pplx_space_firewall_rules::{
    FirewallRuleRevision, IntegerRange, Ipv4Prefix, Match, Name, PayloadHeader, Read, Rule, RuleId,
    Value,
};

use crate::{DecodeError, decode_revision, encode_revision};

/// Every read and value kind in canonical form: keys in grammar order, rules
/// and comparisons in their own order, no whitespace, and names as written.
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
    PayloadHeader {
        protocol: Name::new(protocol).unwrap(),
        field: Name::new(field).unwrap(),
    }
}

fn eq(left: Read, right: Value) -> Match {
    Match { left, right }
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
/// comparison, and lines starting with `#` are comments.
fn documents(table: &str) -> Vec<String> {
    let good = format!(r#"{{"left":{DPORT},"op":"==","right":1}}"#);
    table
        .lines()
        .skip(1)
        .filter(|line| !line.starts_with('#'))
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

/// Golden bytes for every read and value kind, decoding regardless of key order
/// and whitespace, names kept verbatim and escaped, and boundary documents that
/// encode back to themselves.
#[test]
fn documents_round_trip_through_the_canonical_form() {
    let expected = FirewallRuleRevision::new(vec![
        rule(
            "first",
            vec![
                eq(
                    Read::Payload(header("ip", "daddr")),
                    Value::Ipv4(Ipv4Addr::new(192, 0, 2, 1)),
                ),
                eq(Read::TcpOption(Name::new("mptcp").unwrap()), Value::True),
                eq(
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
                eq(
                    Read::Payload(header("ip", "saddr")),
                    Value::Ipv4Prefix(Ipv4Prefix::new(Ipv4Addr::new(198, 51, 100, 0), 24).unwrap()),
                ),
                eq(
                    Read::Payload(header("tcp", "dport")),
                    Value::Range(IntegerRange::new(1024, 65535).unwrap()),
                ),
            ],
        ),
    ])
    .unwrap();
    assert_eq!(encode_revision(&expected), ALL_VARIANTS);
    assert_eq!(decode_revision(ALL_VARIANTS).unwrap(), expected);

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

    let escaped = FirewallRuleRevision::new(vec![
        rule(
            r#"q"\/x"#,
            vec![eq(
                Read::Payload(header(r#"say "hi" \ bye"#, r#" {"field":1} "#)),
                Value::True,
            )],
        ),
        rule(
            r"\\",
            vec![eq(
                Read::TcpOption(Name::new(r#"opt "a" \b"#).unwrap()),
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
    assert_eq!(encode_revision(&escaped), golden);
    assert_eq!(decode_revision(golden).unwrap(), escaped);

    for document in documents(CANONICAL) {
        let revision = decode_revision(&document).unwrap_or_else(|e| panic!("{document}: {e}"));
        assert_eq!(encode_revision(&revision), document);
    }
}

/// The acceptance example as a caller writes it, with spaces and line breaks,
/// and its canonical bytes: keys in grammar order and no whitespace.
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

/// Boundary documents already in canonical form, one per line as in
/// [`REJECTED`].
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
fn the_version_is_read_before_the_grammar() {
    for (document, version) in [
        (r#"{"schema_version":2,"rules":[]}"#, 2),
        (r#"{"schema_version":2,"rules":[],"x":{}}"#, 2),
        (r#"{"x":{},"rules":"none","rules":1,"schema_version":2}"#, 2),
        (r#"{"schema_version":0,"rules":[]}"#, 0),
    ] {
        let error = decode_revision(document).unwrap_err();
        assert!(
            matches!(error, DecodeError::UnsupportedSchemaVersion(found) if found == version),
            "{document}: {error:?}"
        );
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
        let error = decode_revision(document).unwrap_err();
        assert!(
            matches!(error, DecodeError::Json(_)),
            "{document}: {error:?}"
        );
    }
}

/// One row for each rule the decoder enforces, in the format [`documents`]
/// reads. The last group holds forms a plain derived `Deserialize` would
/// accept.
const REJECTED: &str = r##"
# Not a revision object, trailing data, a number JSON cannot hold
document "add rule ip filter output drop"
document {"schema_version":1,"rules":[]} x
right 1e400
# Missing required field
document {"schema_version":1}
rules {"id":"a","all_of":[GOOD]},{"all_of":[GOOD]}
rules {"id":"a","all_of":[GOOD]},{"id":"b"}
match {"op":"==","right":1}
match {"left":{"payload":{"protocol":"tcp","field":"dport"}},"right":1}
match {"left":{"payload":{"protocol":"tcp","field":"dport"}},"op":"=="}
left {"payload":{"field":"dport"}}
left {"payload":{"protocol":"tcp"}}
left {"tcp option":{}}
right {"prefix":{"addr":"192.0.2.0"}}
# Wrong type at each position
document {"schema_version":1,"rules":{}}
rules "nft flush ruleset"
rules {"id":7,"all_of":[GOOD]}
rules {"id":"a","all_of":null}
rules {"id":"a","all_of":["tcp dport 1"]}
op null
left {"payload":"tcp dport"}
left {"payload":{"protocol":null,"field":"dport"}}
left {"tcp option":"mptcp"}
left {"&":{"payload":{"protocol":"tcp","field":"flags"}}}
left {"&":[2,{"payload":{"protocol":"tcp","field":"flags"}}]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},"0x02"]}
right null
right [1,2]
right {"prefix":"192.0.2.0/24"}
right {"prefix":{"addr":"192.0.2.0","len":"24"}}
right {"range":"1-2"}
right {"range":["1",2]}
# Unknown key at each level, including a caller-supplied verdict, table or chain
document {"schema_version":1,"rules":[],"verdict":"x"}
document {"schema_version":1,"rules":[],"chain":"x"}
rules {"id":"a","all_of":[GOOD],"table":"accept"}
right 1,"verdict":"accept"
left {"payload":{"protocol":"tcp","field":"dport","interface":"eth0"}}
left {"tcp option":{"name":"mptcp","field":"kind"}}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"},"x":1},1]}
right {"prefix":{"addr":"192.0.2.0","len":24,"table":"filter"}}
# Missing or unknown variant
left {}
left {"meta":{"key":"mark"}}
left {"&":[{},1]}
left {"&":[{"tcp option":{"name":"mptcp"}},1]}
right {}
right {"set":[1,2]}
# Duplicate key at each level, inside the tagged enums and the untagged value
document {"schema_version":1,"rules":[],"rules":[]}
rules {"id":"a","id":"b","all_of":[GOOD]}
right 1,"right":2
left {"payload":{"protocol":"tcp","field":"dport"},"payload":{"protocol":"udp","field":"dport"}}
left {"payload":{"protocol":"tcp","protocol":"udp","field":"dport"}}
left {"tcp option":{"name":"mptcp","name":"sack"}}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"},"payload":{"protocol":"udp","field":"flags"}},1]}
right {"range":[1,2],"range":[3,4]}
right {"prefix":{"addr":"192.0.2.0","len":33,"len":24}}
# Competing variants
left {"payload":{"protocol":"tcp","field":"dport"},"tcp option":{"name":"mptcp"}}
right {"prefix":{"addr":"192.0.2.0","len":24},"range":[1,2]}
# false, negative, float, overflow, zero mask
right false
right -1
right 1.5
right 18446744073709551616
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}},0]}
# Malformed or IPv6 address
right "01.2.3.4"
right "2001:db8::1"
# Prefix length, reversed range, tuple length
right {"prefix":{"addr":"192.0.2.0","len":33}}
right {"prefix":{"addr":"192.0.2.0","len":256}}
right {"range":[5,4]}
right {"range":[1,2,3]}
left {"&":[{"payload":{"protocol":"tcp","field":"flags"}}]}
# Operator other than ==
op "!="
# Empty all_of, invalid names, duplicate rule IDs
rules {"id":"a","all_of":[GOOD]},{"id":"b","all_of":[]}
rules {"id":"a b","all_of":[GOOD]}
left {"payload":{"protocol":"","field":"dport"}}
left {"payload":{"protocol":"tcp","field":"d\tport"}}
left {"tcp option":{"name":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}
rules {"id":"a","all_of":[GOOD]},{"id":"a","all_of":[GOOD]}
# Accepted by a plain derive: arrays for objects, {"==":null}, a unit-variant
# read, and values that could fall back to another untagged variant
rules ["a",[GOOD]]
match [{"payload":{"protocol":"tcp","field":"dport"}},"==",1]
left {"payload":["tcp","dport"]}
left {"tcp option":["mptcp"]}
left {"&":[[{"protocol":"tcp","field":"flags"}],1]}
right {"prefix":["192.0.2.0",24]}
op {"==":null}
left "payload"
right [192,0,2,1]
right {"prefix":{"addr":"192.0.2.0","len":24},"x":1}
"##;

#[test]
fn rejects_every_document_outside_the_grammar() {
    let depth = 100_000;
    let deep = with_right(&format!("{}{}", "[".repeat(depth), "]".repeat(depth)));
    for document in documents(REJECTED).iter().chain([&deep]) {
        assert!(decode_revision(document).is_err(), "accepted {document}");
    }
}
