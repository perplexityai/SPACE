use super::*;

// Listed by nft 1.1.3 for `add rule ... counter comment "space-cfg:..."`
// through `nft -j list ruleset`.
fn listed_stamp() -> Value {
    serde_json::json!({
        "family": "ip", "table": "sandbox", "chain": "forward", "handle": 2,
        "comment": "space-cfg:forward=89abcdef",
        "expr": [{"counter": {"packets": 0, "bytes": 0}}]
    })
}

#[test]
fn stamp_rule_is_a_prefixed_counter_and_nothing_else() {
    assert!(is_stamp_rule(&listed_stamp()));
    let mut input_form = listed_stamp();
    input_form["expr"] = serde_json::json!([{"counter": null}]);
    assert!(is_stamp_rule(&input_form));

    for (path, value) in [
        ("/comment", serde_json::json!("forward=89abcdef")),
        ("/comment", serde_json::json!(null)),
        ("/expr", serde_json::json!([])),
        ("/expr", serde_json::json!([{"drop": null}])),
        (
            "/expr",
            serde_json::json!([{"counter": null}, {"accept": null}]),
        ),
        (
            "/expr/0",
            serde_json::json!({"counter": null, "accept": null}),
        ),
    ] {
        let mut rule = listed_stamp();
        *rule.pointer_mut(path).unwrap() = value;
        assert!(!is_stamp_rule(&rule), "{path}");
    }
}

#[test]
fn versions_ignore_formatting_but_not_rules() {
    let rule = |port: u16| {
        format!(
            r#"{{ "add": {{ "rule": {{ "family": "ip", "table": "sandbox", "chain": "forward",
                "expr": [{{ "match": {{ "left": {{ "payload": {{ "protocol": "tcp", "field": "dport" }} }},
                "right": {port}, "op": "==" }} }}, {{ "drop": null }}] }} }} }}"#
        )
    };
    let version =
        |json: &str| PartVersion::of_json(&serde_json::from_str::<Vec<Value>>(json).unwrap());
    let original = version(&format!("[{}]", rule(22)));
    let reordered = version(
        r#"[{"add":{"rule":{"expr":[{"match":{"op":"==","right":22,
            "left":{"payload":{"field":"dport","protocol":"tcp"}}}},{"drop":null}],
            "chain":"forward","table":"sandbox","family":"ip"}}}]"#,
    );
    assert_eq!(original, reordered);
    assert_ne!(original, version(&format!("[{}]", rule(23))));
    assert_ne!(original, version(&format!("[{}, {}]", rule(22), rule(23))));

    let script = "add table netdev t\nadd rule netdev t c counter drop\n";
    assert_eq!(
        PartVersion::of_script(script),
        PartVersion::of_script("\n  add table   netdev t\n\nadd rule netdev\tt c counter drop")
    );
    assert_ne!(
        PartVersion::of_script(script),
        PartVersion::of_script("add table netdev t\nadd rule netdev t c counter accept\n")
    );
}
