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
