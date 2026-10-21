use super::*;
use serde_json::{Value, json};

type SchemaMutation = fn(&mut Value);

// This is the M1 compatibility floor, not an updateable snapshot. Additive
// changes belong only in the generated document; a future API version should
// establish its own baseline rather than replacing this one.
const M1_OPENAPI_BASELINE: &str = include_str!("m1_openapi_baseline.json");

fn contract() -> Value {
    json!({
        "servers": [{ "url": "/v1" }], "security": [{ "apiKey": [] }],
        "paths": { "/widgets": { "post": {
            "operationId": "createWidget",
            "parameters": [{ "name": "trace", "in": "header", "required": false }],
            "requestBody": { "required": false, "content": { "application/json": {
                "schema": { "$ref": "#/components/schemas/CreateWidget" }
            } } },
            "responses": { "200": {
                "headers": { "x-request-id": { "schema": { "type": "string" } } },
                "content": {
                    "application/json": { "schema": { "$ref": "#/components/schemas/Widget" } },
                    "text/event-stream": { "x-space-sse-event-data-schema": {
                        "$ref": "#/components/schemas/Event"
                    } }
                }
            } }
        } } },
        "components": {
            "securitySchemes": { "apiKey": { "type": "apiKey", "in": "header", "name": "Authorization" } },
            "schemas": {
                "CreateWidget": { "required": ["name"], "properties": {
                    "name": { "type": "string", "maxLength": 32 },
                    "kind": { "type": "string", "enum": ["small", "large"] }
                } },
                "Widget": { "required": ["id", "state"], "properties": {
                    "id": { "type": "string" },
                    "state": { "type": "string", "enum": ["queued", "ready"] }
                } },
                "Event": { "oneOf": [
                    { "properties": { "type": { "const": "created" } } },
                    { "properties": { "type": { "type": "string", "not": { "enum": ["created"] } } } }
                ] }
            }
        }
    })
}

fn rejected(baseline: &Value, current: &Value, detail: &str) {
    let error = check_openapi_compatibility(baseline, current).unwrap_err();
    assert!(error.detail.contains(detail), "unexpected error: {error}");
}

fn request_schema<'a>(old: &'a Value, new: &'a Value) -> SchemaComparator<'a> {
    SchemaComparator::new(old, new, Direction::Request)
}

fn response_schema<'a>(old: &'a Value, new: &'a Value) -> SchemaComparator<'a> {
    SchemaComparator::new(new, old, Direction::Response)
}

#[test]
fn current_m1_contract_is_compatible_with_baseline() {
    let baseline = serde_json::from_str(M1_OPENAPI_BASELINE)
        .expect("M1 OpenAPI compatibility baseline must be valid JSON");
    let current = super::super::m1_sdk_openapi_value();

    check_openapi_compatibility(&baseline, &current)
        .expect("current M1 OpenAPI contract must remain compatible with the baseline");
}

// The baseline gate above only walks the operations and schemas the baseline
// already names, so an operation added after M1 never reaches `validate_schema`
// or reference resolution. Comparing the document to itself keeps the whole
// served surface inside the subset the checker models - `allOf` and the other
// unmodeled keywords a generator upgrade can start emitting would otherwise
// enter the document as surface the gate silently cannot compare.
#[test]
fn current_m1_contract_stays_within_the_modeled_subset() {
    let spec = super::super::m1_sdk_openapi_value();
    check_openapi_compatibility(&spec, &spec)
        .expect("M1 contract must be expressible in the modeled schema subset");
}

#[test]
#[rustfmt::skip]
fn rejects_breaking_contract_mutations_with_the_expected_reason() {
    let baseline = contract();
    let cases: [(&str, SchemaMutation); 15] = [
        ("path was removed", |v| { v["paths"].as_object_mut().unwrap().remove("/widgets"); }),
        ("operation was removed", |v| { v["paths"]["/widgets"].as_object_mut().unwrap().remove("post"); }),
        ("parameter trace was removed", |v| v["paths"]["/widgets"]["post"]["parameters"] = json!([])),
        ("request body was removed", |v| { v["paths"]["/widgets"]["post"].as_object_mut().unwrap().remove("requestBody"); }),
        ("request body became required", |v| v["paths"]["/widgets"]["post"]["requestBody"]["required"] = json!(true)),
        ("media type was removed", |v| v["paths"]["/widgets"]["post"]["requestBody"]["content"] = json!({})),
        ("request maxLength", |v| v["components"]["schemas"]["CreateWidget"]["properties"]["name"]["maxLength"] = json!(8)),
        ("request required fields", |v| v["components"]["schemas"]["CreateWidget"]["required"] = json!(["name", "kind"])),
        ("response schema types", |v| v["components"]["schemas"]["Widget"]["properties"]["id"]["type"] = json!(["string", "null"])),
        ("response required fields", |v| v["components"]["schemas"]["Widget"]["required"] = json!(["id"])),
        ("response enum", |v| v["components"]["schemas"]["Widget"]["properties"]["state"]["enum"] = json!(["queued", "ready", "failed"])),
        ("response status was removed", |v| { v.pointer_mut("/paths/~1widgets/post/responses").unwrap().as_object_mut().unwrap().remove("200"); }),
        ("response header x-request-id was removed", |v| { v.pointer_mut("/paths/~1widgets/post/responses/200/headers").unwrap().as_object_mut().unwrap().remove("x-request-id"); }),
        ("media type was removed", |v| { v.pointer_mut("/paths/~1widgets/post/responses/200/content").unwrap().as_object_mut().unwrap().remove("application/json"); }),
        ("not accepted by any union variant", |v| *v.pointer_mut("/paths/~1widgets/post/responses/200/content/text~1event-stream/x-space-sse-event-data-schema").unwrap() = json!({})),
    ];
    for (detail, mutate) in cases {
        let mut current = baseline.clone();
        mutate(&mut current);
        rejected(&baseline, &current, detail);
    }
}

#[test]
#[rustfmt::skip]
fn checks_security_servers_references_and_response_header_schemas() {
    let baseline = contract();
    let mut current = baseline.clone();
    current["security"] = json!([{ "other": [] }]);
    rejected(&baseline, &current, "root security changed");
    current = baseline.clone();
    current["servers"] = json!([{ "url": "/v2" }]);
    rejected(&baseline, &current, "servers changed");
    current = baseline.clone();
    current["components"]["securitySchemes"]["apiKey"]["name"] = json!("X-Key");
    rejected(&baseline, &current, "security scheme apiKey changed");
    let mut without_body = baseline.clone();
    without_body.pointer_mut("/paths/~1widgets/post").unwrap().as_object_mut().unwrap().remove("requestBody");
    let mut with_body = baseline.clone(); with_body["paths"]["/widgets"]["post"]["requestBody"]["required"] = json!(true);
    rejected(&without_body, &with_body, "required request body was added");
    current = baseline.clone();
    *current
        .pointer_mut("/paths/~1widgets/post/responses/200/headers/x-request-id/schema/type")
        .unwrap() = json!(["string", "null"]);
    rejected(&baseline, &current, "response schema types");

    let mut referenced = baseline.clone();
    referenced["components"]["parameters"] = json!({ "Trace": referenced.pointer_mut("/paths/~1widgets/post/parameters/0").unwrap().take() });
    referenced["paths"]["/widgets"]["post"]["parameters"] =
        json!([{ "$ref": "#/components/parameters/Trace" }]);
    referenced["components"]["requestBodies"] = json!({ "Create": referenced.pointer_mut("/paths/~1widgets/post/requestBody").unwrap().take() });
    referenced["paths"]["/widgets"]["post"]["requestBody"] =
        json!({ "$ref": "#/components/requestBodies/Create" });
    referenced["components"]["responses"] = json!({ "Widget": referenced.pointer_mut("/paths/~1widgets/post/responses/200").unwrap().take() });
    referenced["paths"]["/widgets"]["post"]["responses"]["200"] = json!({ "$ref": "#/components/responses/Widget" });
    check_openapi_compatibility(&referenced, &referenced).unwrap();
    let mut changed = referenced.clone();
    changed["components"]["requestBodies"]["Create"]["required"] = json!(true);
    rejected(&referenced, &changed, "request body became required");
}

#[test]
#[rustfmt::skip]
fn accepts_parameter_hoisting_numeric_subtypes_and_response_additions() {
    let baseline = contract();
    let mut current = baseline.clone();
    current["paths"]["/widgets"]["parameters"] = current["paths"]["/widgets"]["post"]["parameters"].take();
    current["paths"]["/widgets"]["post"]["responses"]["408"] = json!({ "description": "timeout" });
    current
        .pointer_mut("/paths/~1widgets/post/responses/200/content")
        .unwrap()
        .as_object_mut()
        .unwrap()
        .insert("text/plain".into(), json!({ "schema": { "type": "string" } }));
    check_openapi_compatibility(&baseline, &current).unwrap();

    let integer = json!({ "type": "integer" });
    let number = json!({ "type": "number" });
    request_schema(&Value::Null, &Value::Null).optional(Some(&integer), Some(&number), "request").unwrap();
    response_schema(&Value::Null, &Value::Null).optional(Some(&integer), Some(&number), "response").unwrap();
    request_schema(&Value::Null, &Value::Null).optional(Some(&json!({ "pattern": "^a+$" })), Some(&json!({ "pattern": "^.*$" })), "request").unwrap();
}

/// A pattern matches when any top-level alternative does, so a request may add
/// alternatives that keep every baseline one verbatim, and a response may drop
/// them. A `|` escaped or inside a group or class does not separate
/// alternatives; splitting there would accept a pattern that matches less.
#[test]
#[rustfmt::skip]
fn patterns_widen_only_by_keeping_every_alternative() {
    for (baseline, current, request_compatible, response_compatible) in [
        ("^a$", "^a$|^b$", true, false),
        ("^a$", "^b$|^a$", true, false),
        ("^a$|^b$", "^a$", false, true),
        ("^a$", "^(a)$|^b$", false, false),
        ("b$", r"^a\|b$", false, false),
        ("b]$", "^[a|b]$", false, false),
        ("b)$", "^(a|b)$", false, false),
        ("b$", "^(a|b$", false, false),
    ] {
        let baseline = json!({ "pattern": baseline });
        let current = json!({ "pattern": current });
        assert_eq!(
            request_schema(&Value::Null, &Value::Null).optional(Some(&baseline), Some(&current), "request").is_ok(),
            request_compatible,
            "request {baseline} -> {current}"
        );
        assert_eq!(
            response_schema(&Value::Null, &Value::Null).optional(Some(&current), Some(&baseline), "response").is_ok(),
            response_compatible,
            "response {baseline} -> {current}"
        );
    }
}

/// An open request object accepted unknown keys and ignored them, so
/// documenting one later constrains a value that never reached behaviour.
/// Responses get no such allowance: there the same shape means the contract
/// has stopped promising a property a reader was told to expect.
#[test]
#[rustfmt::skip]
fn a_request_may_document_a_property_the_baseline_left_open() {
    let mut baseline = contract();
    baseline["components"]["schemas"]["CreateWidget"]["properties"]
        .as_object_mut()
        .unwrap()
        .remove("kind");

    check_openapi_compatibility(&baseline, &contract()).unwrap();

    let open = json!({ "type": "object", "properties": { "name": { "type": "string" } } });
    let documented = json!({
        "type": "object",
        "properties": { "name": { "type": "string" }, "kind": { "type": "string", "maxLength": 8 } }
    });
    request_schema(&Value::Null, &Value::Null).optional(Some(&open), Some(&documented), "request").unwrap();
    assert!(
        response_schema(&Value::Null, &Value::Null)
            .optional(Some(&open), Some(&documented), "response")
            .is_err()
    );
}

/// The allowance above is only for properties the baseline never named. A
/// property it did declare still has to keep accepting what it accepted.
#[test]
#[rustfmt::skip]
fn a_documented_request_property_may_not_narrow() {
    let mut current = contract();
    current["components"]["schemas"]["CreateWidget"]["properties"]["name"] =
        json!({ "type": "string", "maxLength": 8 });

    rejected(&contract(), &current, "more restrictive");
}

/// A response enum may open into an extensible string, but only while the
/// previously closed values stay documented as examples; requests never get
/// this allowance because an input enum is a promise about what is accepted.
#[test]
#[rustfmt::skip]
fn response_enum_may_open_into_a_documented_string() {
    let baseline = contract();
    let opened = json!({ "type": "string", "examples": ["queued", "ready"] });
    let mut current = baseline.clone();
    current["components"]["schemas"]["Widget"]["properties"]["state"] = opened.clone();
    check_openapi_compatibility(&baseline, &current).unwrap();

    let mut extended = baseline.clone();
    extended["components"]["schemas"]["Widget"]["properties"]["state"] = json!({ "type": "string", "examples": ["queued", "ready", "failed"] });
    check_openapi_compatibility(&baseline, &extended).unwrap();

    let mut undocumented = baseline.clone();
    undocumented["components"]["schemas"]["Widget"]["properties"]["state"] = json!({ "type": "string", "examples": ["queued"] });
    rejected(&baseline, &undocumented, "response enum or const became less restrictive");

    let mut untyped = baseline.clone();
    untyped["components"]["schemas"]["Widget"]["properties"]["state"] = json!({ "examples": ["queued", "ready"] });
    rejected(&baseline, &untyped, "response enum or const became less restrictive");

    let closed_enum = json!({ "type": "string", "enum": ["small", "large"] });
    request_schema(&Value::Null, &Value::Null).optional(Some(&closed_enum), Some(&opened), "request").unwrap();
    response_schema(&Value::Null, &Value::Null).optional(Some(&closed_enum), Some(&opened), "response").unwrap();
}

#[test]
fn handles_negation_unions_exclusive_bounds_and_unknown_keywords() {
    let schema = json!({ "not": { "$ref": "#/components/schemas/Excluded" } });
    let baseline =
        json!({ "schema": schema, "components": { "schemas": { "Excluded": { "enum": ["a"] } } } });
    let current = json!({ "schema": schema, "components": { "schemas": { "Excluded": { "enum": ["a", "b"] } } } });
    response_schema(&baseline, &current)
        .optional(current.get("schema"), baseline.get("schema"), "response")
        .unwrap();
    assert!(
        response_schema(&current, &baseline)
            .optional(baseline.get("schema"), current.get("schema"), "response")
            .is_err()
    );

    let union_baseline = json!({ "anyOf": [{ "type": "object" }], "required": ["type"] });
    let union_current = json!({ "anyOf": [{ "type": "object" }] });
    assert!(
        response_schema(&Value::Null, &Value::Null)
            .optional(Some(&union_current), Some(&union_baseline), "response")
            .is_err()
    );
    let unknown = json!({ "allOf": [{ "type": "string" }] });
    assert!(
        request_schema(&Value::Null, &Value::Null)
            .optional(Some(&unknown), Some(&unknown), "request")
            .is_err()
    );
    let inclusive = json!({ "minimum": 1 });
    let exclusive = json!({ "exclusiveMinimum": 1 });
    assert!(
        request_schema(&Value::Null, &Value::Null)
            .optional(Some(&inclusive), Some(&exclusive), "request")
            .is_err()
    );
}

#[test]
fn recursive_untyped_schema_comparison_terminates() {
    let document = json!({ "components": { "schemas": { "Node": {
        "properties": { "child": { "$ref": "#/components/schemas/Node" } }
    } } } });
    let schema = json!({ "$ref": "#/components/schemas/Node" });
    response_schema(&document, &Value::Null)
        .optional(None, Some(&schema), "response")
        .unwrap();
}
