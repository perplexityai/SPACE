use super::schema_contract::collect_component_schema_references;
use super::{OPENAPI_HTTP_METHODS, m1_sdk_openapi_value, public_openapi_value};
use axum::response::IntoResponse;
use http_body_util::BodyExt;
use jsonschema::{Draft, Validator};
use pplx_space_common_core::types::{
    GuestUserName, ProcessCommand, validate_linux_guest_path_input,
};
use pplx_space_public_api::{
    CreateCommandRequest, CreateProcessRequest, CreateSandboxRequest, SandboxLifecycleState,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[test]
fn public_openapi_stub_is_public_only() {
    let spec = public_openapi_value();

    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(spec["servers"][0]["url"], "/v1");
    let paths = spec["paths"]
        .as_object()
        .expect("paths should be an object");

    // The public mount never republishes the admin-API-prefixed path.
    assert!(!paths.contains_key("/admin/api/v1/nodes"));

    // Pin the full sorted path -> HTTP-method set. A superset of every
    // `contains_key`/`get(method).is_some()` check the old ladder ran: an
    // unexpected route (or a route missing an expected method) shows up as a
    // snapshot diff instead of silently compiling.
    let paths_by_method: BTreeMap<&str, Vec<&str>> = paths
        .iter()
        .map(|(path, item)| {
            let mut methods: Vec<&str> = item
                .as_object()
                .expect("path item should be an object")
                .keys()
                .map(String::as_str)
                .filter(|key| OPENAPI_HTTP_METHODS.contains(key))
                .collect();
            methods.sort_unstable();
            (path.as_str(), methods)
        })
        .collect();
    insta::assert_json_snapshot!(paths_by_method);

    assert_eq!(
        spec["components"]["headers"]["RequestId"]["schema"]["type"],
        "string"
    );
    let error_schema = &spec["components"]["schemas"]["PublicErrorEnvelope"];
    assert_eq!(
        error_schema["properties"]["code"]["$ref"],
        "#/components/schemas/PublicErrorCode"
    );
    // Error codes are an extensible response vocabulary: a plain string whose
    // known values are documented as examples, never a closed enum.
    let error_code_schema = &spec["components"]["schemas"]["PublicErrorCode"];
    assert_eq!(error_code_schema["type"], "string");
    assert!(error_code_schema.get("enum").is_none());
    let error_codes = error_code_schema["examples"]
        .as_array()
        .expect("public error code examples");
    for code in [
        "invalid_request",
        "unauthorized",
        "forbidden",
        "not_found",
        "conflict",
        "failed_precondition",
        "capacity_unavailable",
        "rate_limited",
        "idempotency_conflict",
        "operation_failed",
        "service_unavailable",
        "internal",
    ] {
        assert!(
            error_codes
                .iter()
                .any(|value| value.as_str().is_some_and(|candidate| candidate == code)),
            "missing public error code {code}"
        );
    }
    assert_eq!(error_schema["properties"]["retryable"]["type"], "boolean");
    assert_eq!(
        error_schema["properties"]["request_id"]["type"],
        json!(["string", "null"])
    );
    assert!(
        error_schema["required"]
            .as_array()
            .expect("public error required fields")
            .contains(&json!("request_id"))
    );
    let sandbox_schema = &spec["components"]["schemas"]["PublicSandbox"];
    assert_eq!(
        sandbox_schema["properties"]["state"]["$ref"],
        "#/components/schemas/SandboxLifecycleState"
    );
    let sandbox_state_schema = &spec["components"]["schemas"]["SandboxLifecycleState"];
    assert_eq!(sandbox_state_schema["type"], "string");
    assert!(sandbox_state_schema.get("enum").is_none());
    assert_eq!(
        sandbox_state_schema["examples"],
        serde_json::json!(SandboxLifecycleState::KNOWN_VALUES)
    );
    assert!(
        sandbox_schema["required"]
            .as_array()
            .expect("public sandbox required fields")
            .contains(&serde_json::json!("failure_code"))
    );
    assert_eq!(
        sandbox_schema["properties"]["failure_code"]["anyOf"],
        serde_json::json!([
            { "$ref": "#/components/schemas/SandboxFailureCode" },
            { "type": "null" }
        ])
    );
    let failure_code_schema = &spec["components"]["schemas"]["SandboxFailureCode"];
    assert_eq!(failure_code_schema["type"], "string");
    assert!(failure_code_schema.get("enum").is_none());
    assert_eq!(
        failure_code_schema["examples"],
        serde_json::json!(["runtime_failure"])
    );
    assert_eq!(
        spec["paths"]["/sandboxes"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["$ref"],
        "#/components/schemas/PublicSandboxPage"
    );
    assert_eq!(
        spec["paths"]["/sandboxes"]["get"]["parameters"],
        serde_json::json!([
            {
                "name": "cursor",
                "in": "query",
                "required": false,
                "schema": {
                    "type": "string",
                    "maxLength": 4096,
                    "pattern": "^pc1_[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$"
                }
            },
            {
                "name": "limit",
                "in": "query",
                "required": false,
                "description": "Maximum number of sandboxes. Omitted or zero uses the server default of 100; values above 1000 are capped at 1000.",
                "schema": { "type": "integer", "minimum": 0, "maximum": 65535 }
            }
        ])
    );
    assert_eq!(
        spec["paths"]["/sandboxes/{sandbox_id}"]["get"]["responses"]["200"]["content"]["application/json"]
            ["schema"]["$ref"],
        "#/components/schemas/PublicSandbox"
    );
    let frames = spec["components"]["schemas"]["DataPlaneServerFrame"]["oneOf"]
        .as_array()
        .expect("server frame variants");
    assert!(frames.iter().any(|frame| {
        frame["properties"]["type"]["const"]
            .as_str()
            .is_some_and(|value| value == "input_accepted")
    }));
    assert!(frames.iter().any(|frame| {
        frame["properties"]["type"]["const"]
            .as_str()
            .is_some_and(|value| value == "control_accepted")
    }));
    assert!(frames.iter().any(|frame| {
        frame["properties"]["type"]["const"]
            .as_str()
            .is_some_and(|value| value == "resync_required")
    }));
}

#[test]
fn m1_contract_is_frozen() {
    let spec = m1_sdk_openapi_value();
    let actual = spec["paths"]
        .as_object()
        .expect("M1 paths should be an object")
        .iter()
        .flat_map(|(path, item)| {
            item.as_object()
                .expect("path item should be an object")
                .keys()
                .filter(|method| OPENAPI_HTTP_METHODS.contains(&method.as_str()))
                .map(move |method| (path.as_str(), method.as_str()))
        })
        .collect::<BTreeSet<_>>();
    // Derived from the manifest rather than restated: the snapshot below is the
    // freeze, and `m1_lifecycle_surface_is_pause_suspend_and_wake` pins the
    // lifecycle subset, so a literal list here would be a third copy of the
    // same fact. What this assertion is actually for is the projection filter -
    // that `m1_sdk_openapi_value` keeps exactly the supported operations.
    let expected = pplx_space_public_api::routes::NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| operation.status().is_m1_supported())
        .map(|operation| (operation.relative_path(), operation.method().openapi_name()))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
    insta::assert_snapshot!(
        serde_json::to_string(&m1_contract(&spec)).expect("M1 contract should serialize")
    );
}

#[test]
fn timeout_contract_is_synchronous_and_excluded_from_the_release_projection() {
    let spec = public_openapi_value();
    let path = pplx_space_public_api::routes::SET_SANDBOX_TIMEOUT.relative_path();
    let operation = &spec["paths"][path]["post"];
    assert_eq!(
        operation[pplx_space_public_api::routes::M1_STATUS_EXTENSION],
        "incubating"
    );
    assert_eq!(
        operation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/SetSandboxTimeoutRequest"
    );
    assert_eq!(
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
        "#/components/schemas/SetSandboxTimeoutResponse"
    );
    assert!(operation["responses"].get("202").is_none());
    assert!(
        operation["parameters"]
            .as_array()
            .unwrap()
            .iter()
            .any(
                |parameter| parameter["name"] == "Idempotency-Key" && parameter["required"] == true
            )
    );
    let schema = &spec["components"]["schemas"]["SetSandboxTimeoutResponse"];
    let validator = Validator::options()
        .with_draft(Draft::Draft202012)
        .build(schema)
        .unwrap();
    let mut response = json!({
        "sandbox_id": pplx_space_public_api::PublicSandboxId::new().to_string(),
        "idle_timeout": "90s",
        "expires_at": null
    });
    assert!(validator.is_valid(&response));
    response["expires_at"] = json!("2026-10-06T18:00:00Z");
    assert!(validator.is_valid(&response));
    response["expires_at"] = json!(90);
    assert!(!validator.is_valid(&response));
    let release = m1_sdk_openapi_value();
    assert!(release["paths"].get(path).is_none());
    assert!(
        release["components"]["schemas"]
            .get("SetSandboxTimeoutRequest")
            .is_none()
    );
    assert!(
        release["components"]["schemas"]
            .get("SetSandboxTimeoutResponse")
            .is_none()
    );
}

#[test]
fn reference_examples_match_their_schemas() {
    let spec = m1_sdk_openapi_value();
    for (path, schema) in [
        ("/sandboxes", "CreateSandboxRequest"),
        ("/sandboxes/{sandbox_id}/commands", "CreateCommandRequest"),
    ] {
        let description = spec["paths"][path]["post"]["description"]
            .as_str()
            .expect("operation description");
        let example = description
            .split_once("```json\n")
            .expect("JSON example")
            .1
            .split_once("\n```")
            .expect("closed JSON example")
            .0;
        let value: Value = serde_json::from_str(example).expect("valid example JSON");
        component_validator(&spec, schema)
            .validate(&value)
            .expect("request example matches its schema");
    }
    let description =
        spec["paths"]["/sessions/{session_id}/events"]["get"]["responses"]["200"]["description"]
            .as_str()
            .expect("stream description");
    let validator = component_validator(&spec, "DataPlaneServerFrame");
    let frames: Vec<_> = description
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .collect();
    assert!(!frames.is_empty(), "stream has example frames");
    for frame in frames {
        let value: Value = serde_json::from_str(frame).expect("valid frame JSON");
        validator
            .validate(&value)
            .expect("event data matches its schema");
    }
}

#[test]
fn sandbox_create_schema_tracks_public_wire_parsing() {
    let spec = public_openapi_value();
    let validator = component_validator(&spec, "CreateSandboxRequest");
    let digest = "0".repeat(64);

    // Fields the schema is authoritative for. Both the schema and the wire must
    // reach the pinned verdict: asserting only that they agree would pass if
    // both were wrong the same way.
    for (request, valid) in [
        (json!({ "template": "seed" }), true),
        (json!({ "template": "seed:latest" }), true),
        (json!({ "template": "seed:v1.2.3" }), true),
        (json!({ "template": "seed_1.2-3" }), true),
        (json!({ "template": format!("seed@sha256:{digest}") }), true),
        (
            json!({ "template": format!("seed@sha256:{}", "0".repeat(12)) }),
            true,
        ),
        (
            json!({ "template": format!("seed@sha256:{}", "0".repeat(63)) }),
            true,
        ),
        // Digest hex is lowercase only, sha256 only, and 12..=64 long.
        (
            json!({ "template": format!("seed@sha256:{}", "A".repeat(64)) }),
            false,
        ),
        (
            json!({ "template": format!("seed@sha256:{}", "0".repeat(11)) }),
            false,
        ),
        (
            json!({ "template": format!("seed@sha256:{}", "0".repeat(65)) }),
            false,
        ),
        (
            json!({ "template": format!("seed@md5:{}", "0".repeat(32)) }),
            false,
        ),
        (
            json!({ "template": format!("seed@SHA256:{digest}") }),
            false,
        ),
        // `.` and `..` are valid name characters but not valid names.
        (json!({ "template": "." }), false),
        (json!({ "template": ".." }), false),
        (json!({ "template": "..." }), true),
        (json!({ "template": "" }), false),
        (json!({ "template": "../seed" }), false),
        (json!({ "template": "seed:" }), false),
        (json!({ "template": ":latest" }), false),
        (json!({ "template": "seed:bad tag" }), false),
        (json!({ "template": "seed:-leading-dash" }), false),
        (
            json!({ "template": format!("seed:{}", "b".repeat(129)) }),
            false,
        ),
        (json!({ "template": "a".repeat(256) }), false),
        (json!({ "runtime": "firecracker" }), false),
        // Every canonical runtime and every alias the deserializer accepts.
        (
            json!({ "template": "seed", "runtime": "firecracker" }),
            true,
        ),
        (
            json!({ "template": "seed", "runtime": "cloud-hypervisor" }),
            true,
        ),
        (json!({ "template": "seed", "runtime": "gvisor" }), true),
        (json!({ "template": "seed", "runtime": "fc" }), true),
        (json!({ "template": "seed", "runtime": "ch" }), true),
        (json!({ "template": "seed", "runtime": "gv" }), true),
        (json!({ "template": "seed", "runtime": "FC" }), false),
        (json!({ "template": "seed", "runtime": "unknown" }), false),
        (json!({ "template": "seed", "resources": {} }), true),
        (
            json!({ "template": "seed", "resources": { "vcpu": 2 } }),
            true,
        ),
        (
            json!({ "template": "seed", "env": { "SPACE_MODE": "test" } }),
            true,
        ),
        // Create does not run `env` through `EnvMap`, so the command path's key
        // rules do not apply here and the schema must not claim they do.
        (
            json!({ "template": "seed", "env": { "BAD-NAME": "value" } }),
            true,
        ),
        (
            json!({ "template": "seed", "metadata": { "owner": "team", "count": 3, "on": true } }),
            true,
        ),
    ] {
        let schema_valid = validator.is_valid(&request);
        let wire_valid = serde_json::from_value::<CreateSandboxRequest>(request.clone()).is_ok();
        assert_eq!(schema_valid, valid, "schema result for {request}");
        assert_eq!(wire_valid, valid, "wire result for {request}");
    }

    // Incubating fields are deliberately open objects, so the wire is the only
    // authority. Pinning that here keeps the looseness a stated decision: if
    // one of these is ever described precisely, this assertion fails and the
    // case moves to the table above.
    for (request, wire_accepts) in [
        (
            json!({
                "template": "seed",
                "network_policy": {
                    "version": "v1",
                    "egress": { "default_action": "deny", "allow": [{ "domain": "example.com" }] }
                }
            }),
            true,
        ),
        (
            json!({
                "template": "seed",
                "network_policy": { "egress": { "default_action": "sometimes" } }
            }),
            false,
        ),
        (
            json!({ "template": "seed", "network_policy": { "egress": { "default_action": "deny", "allow": [{ "ip": "not-an-ip" }] } } }),
            false,
        ),
        (
            json!({
                "template": "seed",
                "auto_lifecycle": {
                    "stages": [
                        { "condition_set": "access_idle", "at": "6h", "action": "suspend" }
                    ]
                }
            }),
            true,
        ),
        (
            json!({
                "template": "seed",
                "auto_lifecycle": {
                    "stages": [
                        { "condition_set": "access_idle", "at": "0s", "action": "suspend" }
                    ]
                }
            }),
            false,
        ),
    ] {
        assert!(
            validator.is_valid(&request),
            "incubating fields are open objects, so the schema should accept {request}"
        );
        let wire_valid = serde_json::from_value::<CreateSandboxRequest>(request.clone()).is_ok();
        assert_eq!(wire_valid, wire_accepts, "wire result for {request}");
    }
}

fn m1_contract(spec: &Value) -> Value {
    // The projection prunes unreferenced components, so the served document is
    // itself the frozen schema set. Recomputing the reference closure here would
    // only restate `retain_referenced_schemas`.
    let mut contract = serde_json::json!({
        "paths": spec["paths"],
        "schemas": spec["components"]["schemas"],
    });
    // Response Media Type examples are documentation, not wire constraints.
    // Do not remove schema.examples: those pin the known open-enum vocabulary.
    for item in contract["paths"].as_object_mut().unwrap().values_mut() {
        for method in OPENAPI_HTTP_METHODS {
            let Some(responses) = item
                .get_mut(method)
                .and_then(|operation| operation.get_mut("responses"))
                .and_then(Value::as_object_mut)
            else {
                continue;
            };
            for response in responses.values_mut() {
                let Some(content) = response.get_mut("content").and_then(Value::as_object_mut)
                else {
                    continue;
                };
                for media in content.values_mut().filter_map(Value::as_object_mut) {
                    media.remove("example");
                    media.remove("examples");
                }
            }
        }
    }
    remove_openapi_documentation(&mut contract);
    canonicalize_key_order(&contract)
}

#[test]
fn contract_freeze_ignores_response_samples_but_keeps_schema_constraints() {
    let spec = m1_sdk_openapi_value();
    let contract = m1_contract(&spec);
    let response =
        "/paths/~1operations~1{operation_id}/get/responses/400/content/application~1json";
    let mut documented = spec.clone();
    let media = documented.pointer_mut(response).unwrap();
    media["examples"] = json!({"another_sample": {"value": {"code": "invalid_request"}}});
    media["example"] = json!({"code": "invalid_request"});
    assert_eq!(m1_contract(&documented), contract);

    let mut changed_response = spec.clone();
    changed_response.pointer_mut(response).unwrap()["schema"] = json!({"type": "string"});
    assert_ne!(m1_contract(&changed_response), contract);

    let mut changed_vocabulary = spec.clone();
    changed_vocabulary["components"]["schemas"]["PublicErrorCode"]["examples"] =
        json!(["different_code"]);
    assert_ne!(m1_contract(&changed_vocabulary), contract);

    let mut changed_property = spec.clone();
    changed_property["components"]["schemas"]["PublicErrorEnvelope"]["properties"]["examples"] =
        json!({"type": "string"});
    assert_ne!(m1_contract(&changed_property), contract);
}

/// Canonicalizes map order so the freeze records the contract rather than the
/// order it was built in.
///
/// `serde_json` keeps insertion order when `preserve_order` is on and key order
/// when it is off, and workspace feature unification turns it on under
/// `--all-features`. Without this the same document freezes two different ways,
/// so a snapshot accepted from a plain `cargo test` fails the CI lane and the
/// regenerated snapshot then fails locally.
fn canonicalize_key_order(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .map(|(key, value)| (key.clone(), canonicalize_key_order(value)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_key_order).collect()),
        scalar => scalar.clone(),
    }
}

/// The M1 document is what SDKs generate from, so it has to be self-contained
/// and free of incubating surface: every `$ref` must resolve inside it, and no
/// schema may survive that nothing reaches.
#[test]
fn m1_openapi_components_are_exactly_the_reachable_closure() {
    let spec = m1_sdk_openapi_value();
    let schemas = spec["components"]["schemas"]
        .as_object()
        .expect("component schemas should be an object");

    let mut reachable = BTreeSet::new();
    collect_component_schema_references(&spec["paths"], &mut reachable);
    collect_component_schema_references(&spec["components"], &mut reachable);

    let present = schemas.keys().cloned().collect::<BTreeSet<_>>();
    assert_eq!(
        reachable, present,
        "M1 components must contain exactly the schemas the document reaches"
    );
}

#[test]
fn sandbox_idle_response_fields_are_nullable_incubating_and_extensible() {
    let spec = public_openapi_value();
    let schema = &spec["components"]["schemas"]["PublicSandbox"];
    for field in ["expires_at", "idle_timeout", "idle_action"] {
        assert_eq!(
            schema["properties"][field][pplx_space_public_api::routes::M1_STATUS_EXTENSION],
            "incubating",
        );
    }
    let validator = component_validator(&spec, "PublicSandbox");
    let mut example = spec["paths"]["/sandboxes/{sandbox_id}"]["get"]["responses"]["200"]
        ["content"]["application/json"]["examples"]["illustrative"]["value"].clone();
    validator
        .validate(&example)
        .expect("idle configuration example conforms");
    example["idle_action"] = json!("hibernate");
    validator
        .validate(&example)
        .expect("future idle actions remain valid responses");
    for field in ["expires_at", "idle_timeout", "idle_action"] {
        example[field] = Value::Null;
    }
    validator
        .validate(&example)
        .expect("idle configuration may be null");
}

#[test]
fn sandbox_idle_create_fields_are_optional_and_actions_are_closed_on_input() {
    let spec = public_openapi_value();
    let schema = &spec["components"]["schemas"]["CreateSandboxRequest"];
    for field in ["idle_timeout", "idle_action"] {
        assert_eq!(
            schema["properties"][field][pplx_space_public_api::routes::M1_STATUS_EXTENSION],
            "incubating"
        );
        assert!(
            !schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!(field))
        );
    }
    let validator = component_validator(&spec, "CreateSandboxRequest");
    for (action, accepted) in [
        (Value::Null, true),
        (json!("delete"), true),
        (json!("suspend"), true),
        (json!("pause"), false),
        (json!("hibernate"), false),
    ] {
        let request = json!({ "template": "seed", "idle_timeout": "600s", "idle_action": action });
        assert_eq!(validator.is_valid(&request), accepted, "{request}");
        assert_eq!(
            serde_json::from_value::<CreateSandboxRequest>(request.clone()).is_ok(),
            accepted,
            "{request}"
        );
    }
}

#[test]
fn m1_operation_results_are_generator_safe_tagged_variants() {
    let spec = m1_sdk_openapi_value();
    let variants = spec["components"]["schemas"]["PublicOperationResult"]["oneOf"]
        .as_array()
        .expect("PublicOperationResult should be a tagged union");

    assert_eq!(variants.len(), 4);
    for variant in variants {
        assert!(
            variant.get("$ref").is_none(),
            "OpenAPI generators do not consistently merge $ref siblings"
        );
        assert_eq!(variant["type"], "object");
        assert!(variant["properties"]["type"]["const"].is_string());
        assert!(
            variant["required"]
                .as_array()
                .expect("required fields should be an array")
                .contains(&json!("type"))
        );
    }
}

fn remove_openapi_documentation(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            object.remove("description");
            object.remove("summary");
            object.values_mut().for_each(remove_openapi_documentation);
        }
        serde_json::Value::Array(values) => {
            values.iter_mut().for_each(remove_openapi_documentation);
        }
        _ => {}
    }
}

#[test]
fn public_openapi_is_valid_31() {
    let spec = public_openapi_value();
    // Pinned from https://spec.openapis.org/oas/3.1/schema/2025-09-15 so
    // validation remains available offline and independent of registry crates.
    let schema: Value = serde_json::from_str(include_str!("openapi-3.1-schema.json"))
        .expect("pinned OpenAPI 3.1 schema should parse");
    let validator = jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
        .expect("pinned OpenAPI 3.1 schema should compile");
    validator
        .validate(&spec)
        .unwrap_or_else(|error| panic!("public OpenAPI document is invalid: {error}"));
    let mut invalid = spec.clone();
    invalid["info"]["title"] = Value::Null;
    assert!(
        !validator.is_valid(&invalid),
        "schema must reject invalid info"
    );
    assert_eq!(
        spec["jsonSchemaDialect"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert!(!contains_key(&spec, "nullable"));
}

#[test]
fn public_openapi_local_references_resolve() {
    let spec = public_openapi_value();
    assert_local_references_resolve(&spec, &spec);
}

#[test]
fn rollback_contract_only_advertises_reachable_responses() {
    let spec = public_openapi_value();
    let operation = &spec["paths"]["/sandboxes/{sandbox_id}:rollback"]["post"];
    let responses = operation["responses"]
        .as_object()
        .expect("rollback responses should be an object")
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();

    assert_eq!(
        responses,
        BTreeSet::from(["202", "400", "401", "404", "409", "413", "500", "503"]),
        "rollback responses must match errors reachable from the raw-Bytes handler"
    );
}

#[test]
fn public_openapi_declares_every_templated_path_parameter() {
    let spec = public_openapi_value();
    for (path, item) in spec["paths"]
        .as_object()
        .expect("paths should be an object")
    {
        let declared = item["parameters"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|parameter| parameter["in"] == "path")
            .map(|parameter| {
                assert_eq!(parameter["required"], true, "path parameter in {path}");
                parameter["name"].as_str().expect("path parameter name")
            })
            .collect::<std::collections::BTreeSet<_>>();
        let templated = super::path_contract::path_parameter_names(path)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(declared, templated, "path parameters for {path}");
    }
}

#[test]
fn command_request_schema_matches_runtime_boundaries() {
    let spec = public_openapi_value();
    let validator = component_validator(&spec, "CreateCommandRequest");
    for (request, valid) in [
        (
            json!({
                "command": ["sh"],
                "cwd": "/workspace",
                "env": { "SPACE_MODE": "test" },
                "pty": { "cols": 80, "rows": 24 },
                "stdin": true,
                "timeout": "1.5m"
            }),
            true,
        ),
        (
            json!({ "command": ["true"], "env": { "BAD-NAME": "value" } }),
            false,
        ),
        (
            json!({
                "command": ["true"],
                "env": {
                    "VALUE": "é".repeat(
                        pplx_space_common_core::types::MAX_ENV_VALUE_BYTES / 2 + 1
                    )
                }
            }),
            false,
        ),
        (
            json!({ "command": ["true"], "pty": { "cols": 0, "rows": 24 } }),
            false,
        ),
        (json!({ "command": ["true"], "cwd": "" }), false),
        (json!({ "command": ["true"], "user": "root" }), true),
        (json!({ "command": ["true"], "user": "" }), false),
        (
            json!({
                "command": ["true"],
                "user": "u".repeat(
                    pplx_space_common_core::types::MAX_GUEST_USER_NAME_BYTES + 1
                )
            }),
            false,
        ),
        (
            json!({ "command": ["true"], "cwd": "C:\\Users\\me" }),
            false,
        ),
        (
            json!({ "command": ["true"], "cwd": "\\\\server\\share" }),
            false,
        ),
        (json!({ "command": ["true"], "timeout": "500ms" }), false),
        (json!({ "command": ["true"], "timeout": "0s" }), false),
        (json!({ "command": ["true"], "timeout": ".5m" }), false),
        (
            json!({ "command": ["true"], "timeout": "0.0000000001s" }),
            false,
        ),
        (
            json!({ "command": ["true"], "timeout": "18446744073709551616s" }),
            false,
        ),
        (json!({ "command": ["true"], "timeout": " 1s" }), false),
        (
            json!({ "command": ["true"], "timeout": "1000000000s" }),
            false,
        ),
        (
            json!({ "command": ["true"], "timeout": "0.000000001s" }),
            true,
        ),
        (
            json!({ "command": ["true"], "timeout": "0.0000000001m" }),
            true,
        ),
        (
            json!({ "command": ["true"], "timeout": "0.00000000001h" }),
            true,
        ),
    ] {
        let schema_valid = validator.is_valid(&request)
            && command_environment_extensions_are_valid(&spec, &request);
        assert_eq!(
            schema_valid,
            valid,
            "schema result for {request}: {:?}",
            validator
                .iter_errors(&request)
                .map(|error| error.to_string())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            command_request_api_boundaries_are_runtime_valid(request),
            valid
        );
    }
}

fn command_environment_extensions_are_valid(spec: &Value, request: &Value) -> bool {
    let schema = &spec["components"]["schemas"]["CreateCommandRequest"];
    let env_schema = &schema["properties"]["env"];
    let Some(env) = request.get("env").and_then(Value::as_object) else {
        return true;
    };
    let max_value_bytes = extension_limit(
        &env_schema["additionalProperties"],
        "x-space-max-utf8-bytes",
    );
    let env_bytes = env.iter().fold(0_usize, |total, (key, value)| {
        total.saturating_add(key.len()).saturating_add(
            value
                .as_str()
                .expect("standard schema validation accepted a non-string env value")
                .len(),
        )
    });
    env_bytes <= extension_limit(env_schema, "x-space-max-aggregate-utf8-bytes")
        && env.values().all(|value| {
            value
                .as_str()
                .expect("standard schema validation accepted a non-string env value")
                .len()
                <= max_value_bytes
        })
}

fn extension_limit(schema: &Value, keyword: &str) -> usize {
    schema[keyword]
        .as_u64()
        .and_then(|limit| usize::try_from(limit).ok())
        .unwrap_or_else(|| panic!("{keyword} should be a usize-compatible integer"))
}

fn command_request_api_boundaries_are_runtime_valid(value: Value) -> bool {
    let Ok(request) = serde_json::from_value::<CreateCommandRequest>(value) else {
        return false;
    };
    let request = CreateProcessRequest::from(request);
    ProcessCommand::new(request.command).is_ok()
        && request
            .cwd
            .as_deref()
            .is_none_or(|cwd| !cwd.is_empty() && validate_linux_guest_path_input(cwd).is_ok())
        && request
            .user
            .is_none_or(|user| GuestUserName::parse(user).is_ok())
        && request
            .pty
            .is_none_or(|size| size.cols != 0 && size.rows != 0)
}

#[test]
fn unknown_sse_frames_remain_forward_compatible() {
    let spec = public_openapi_value();
    let future_frame = json!({
        "type": "resource_pressure",
        "memory_bytes": 1048576,
        "advice": "checkpoint"
    });
    component_validator(&spec, "DataPlaneServerFrame")
        .validate(&future_frame)
        .expect("the published SSE schema should accept unknown future frame types");
    assert_eq!(
        serde_json::from_value::<pplx_space_public_api::DataPlaneServerFrame>(future_frame)
            .expect("runtime should accept an unknown future frame"),
        pplx_space_public_api::DataPlaneServerFrame::Unknown
    );
}

#[tokio::test]
async fn real_public_error_response_conforms_to_openapi() {
    let response = crate::routes::public::error::PublicApiError::bad_request("invalid command")
        .into_response();
    let body = response
        .into_body()
        .collect()
        .await
        .expect("error body should collect")
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).expect("error body should be JSON");
    component_validator(&public_openapi_value(), "PublicErrorEnvelope")
        .validate(&body)
        .unwrap_or_else(|error| panic!("published error schema rejected {body}: {error}"));
}

fn component_validator(spec: &Value, name: &str) -> Validator {
    let schema = json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$ref": format!("#/components/schemas/{name}"),
        "components": spec["components"]
    });
    jsonschema::options()
        .with_draft(Draft::Draft202012)
        .build(&schema)
        .unwrap_or_else(|error| panic!("invalid schema for {name}: {error}"))
}

#[test]
fn supported_success_examples_and_semantics_are_complete() {
    use pplx_space_public_api::routes::{NATIVE_V1_OPERATIONS, STREAM_SESSION_EVENTS};

    let spec = m1_sdk_openapi_value();
    let mut json_responses = 0;
    for route in NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|route| route.status().is_m1_supported())
    {
        let operation = &spec["paths"][route.relative_path()][route.method().openapi_name()];
        assert!(!operation["description"].as_str().unwrap().is_empty());
        for (status, response) in operation["responses"].as_object().unwrap() {
            if !status.starts_with('2') {
                continue;
            }
            if route == STREAM_SESSION_EVENTS {
                assert_eq!(status, "200");
                let description = response["description"].as_str().unwrap();
                let excerpt = description
                    .split("```text\n")
                    .nth(1)
                    .unwrap()
                    .split("```")
                    .next()
                    .unwrap();
                let validator = component_validator(&spec, "DataPlaneServerFrame");
                let mut frames = 0;
                for line in excerpt
                    .lines()
                    .filter_map(|line| line.strip_prefix("data: "))
                {
                    validator
                        .validate(&serde_json::from_str::<Value>(line).unwrap())
                        .unwrap();
                    frames += 1;
                }
                assert_eq!(frames, 2);
                continue;
            }
            let media = &response["content"]["application/json"];
            let component = media["schema"]["$ref"]
                .as_str()
                .unwrap()
                .strip_prefix("#/components/schemas/")
                .unwrap();
            let example = &media["examples"]["illustrative"]["value"];
            component_validator(&spec, component)
                .validate(example)
                .unwrap_or_else(|error| panic!("{} {status}: {error}", route.operation_id()));
            json_responses += 1;
        }
    }
    assert_eq!(json_responses, 13);
    for (component, field) in [
        ("PublicCommand", "session_id"),
        ("PublicCommand", "process_id"),
        ("PublicCommand", "expires_at"),
        ("PublicProcess", "pid"),
        ("DataPlaneAttachInfo", "url"),
        ("DataPlaneAttachInfo", "events_url"),
        ("DataPlaneAttachInfo", "token"),
        ("DataPlaneAttachInfo", "expires_at"),
        ("PublicSandbox", "resources"),
        ("PublicSandbox", "conditions"),
        ("PublicOperationRecord", "state"),
        ("PublicOperationRecord", "result"),
        ("PublicOperationRecord", "terminal_error_code"),
        ("PublicSandboxPage", "next_cursor"),
        ("PublicProcessPage", "next_cursor"),
        ("ProcessMutationResponse", "ok"),
    ] {
        let description =
            spec["components"]["schemas"][component]["properties"][field]["description"]
                .as_str()
                .unwrap_or_else(|| panic!("{component}.{field} needs documentation"));
        assert!(!description.is_empty());
        assert!(!description.contains("[`process_id_for_session`]"));
    }
    assert!(
        spec["info"]["description"]
            .as_str()
            .unwrap()
            .contains("Process kill and signal")
    );
}

#[test]
fn supported_error_examples_match_envelopes_status_and_retryability() {
    use crate::routes::public::error::error_contract;
    use pplx_space_public_api::{
        DataPlaneHttpErrorEnvelope, PublicErrorCode, PublicErrorEnvelope,
        routes::{NATIVE_V1_OPERATIONS, STREAM_SESSION_EVENTS},
    };

    let spec = m1_sdk_openapi_value();
    let public_validator = component_validator(&spec, "PublicErrorEnvelope");
    let data_validator = component_validator(&spec, "DataPlaneHttpErrorEnvelope");
    for operation in NATIVE_V1_OPERATIONS
        .iter()
        .copied()
        .filter(|operation| operation.status().is_m1_supported())
    {
        let responses = spec["paths"][operation.relative_path()][operation.method().openapi_name()]
            ["responses"]
            .as_object()
            .unwrap();
        for (status, response) in responses {
            let status = status.parse::<u16>().unwrap();
            if status < 400 {
                continue;
            }
            let examples = response["content"]["application/json"]["examples"]
                .as_object()
                .expect("every supported error status has explicit samples");
            assert!(!examples.is_empty());
            for (name, example) in examples {
                assert!(
                    example["description"]
                        .as_str()
                        .unwrap()
                        .contains("Use the returned retryable flag")
                );
                let value = &example["value"];
                if operation == STREAM_SESSION_EVENTS {
                    data_validator.validate(value).unwrap();
                    let envelope: DataPlaneHttpErrorEnvelope =
                        serde_json::from_value(value.clone()).unwrap();
                    assert_eq!(name, envelope.error.code.as_str());
                    let (codes, retryable): (&[&str], bool) = match status {
                        400 => (&["invalid_request"], false),
                        401 => (&["unauthorized"], false),
                        403 => (&["forbidden"], false),
                        404 => (&["not_found"], false),
                        409 => (&["conflict"], true),
                        429 => (&["rate_limited"], true),
                        500 => (&["internal"], false),
                        503 => (
                            &["not_ready", "service_unavailable", "stream_unavailable"],
                            true,
                        ),
                        _ => panic!("unexpected stream status {status}"),
                    };
                    assert!(codes.contains(&envelope.error.code.as_str()));
                    assert_eq!(envelope.error.retryable, retryable);
                } else {
                    public_validator.validate(value).unwrap();
                    let envelope: PublicErrorEnvelope =
                        serde_json::from_value(value.clone()).unwrap();
                    assert_eq!(name, envelope.code.as_str());
                    let (expected_status, retryable) = error_contract(&envelope.code);
                    if matches!(status, 413 | 415 | 422) {
                        assert_eq!(envelope.code, PublicErrorCode::InvalidRequest);
                    } else {
                        assert_eq!(expected_status.as_u16(), status);
                    }
                    assert_eq!(envelope.retryable, retryable);
                }
            }
        }
    }
    let examples = |path: &str, method: &str, status: &str| {
        spec["paths"][path][method]["responses"][status]["content"]["application/json"]["examples"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        examples("/sandboxes", "post", "409"),
        ["conflict", "idempotency_conflict"]
    );
    assert_eq!(
        examples("/sandboxes/{sandbox_id}/commands", "post", "409"),
        ["idempotency_conflict"]
    );
    assert_eq!(
        examples("/sandboxes/{sandbox_id}/processes", "get", "409"),
        ["conflict"]
    );
    assert_eq!(
        examples("/sandboxes", "post", "503"),
        ["capacity_unavailable", "service_unavailable"]
    );
    assert_eq!(
        examples("/sandboxes/{sandbox_id}/processes", "get", "503"),
        ["service_unavailable"]
    );
}

fn contains_key(value: &Value, key: &str) -> bool {
    match value {
        Value::Object(object) => {
            object.contains_key(key) || object.values().any(|value| contains_key(value, key))
        }
        Value::Array(values) => values.iter().any(|value| contains_key(value, key)),
        _ => false,
    }
}

fn assert_local_references_resolve(document: &Value, value: &Value) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                let pointer = reference.strip_prefix('#').unwrap_or_else(|| {
                    panic!("public OpenAPI reference must be local: {reference}")
                });
                assert!(
                    document.pointer(pointer).is_some(),
                    "public OpenAPI reference does not resolve: {reference}"
                );
            }
            object
                .values()
                .for_each(|value| assert_local_references_resolve(document, value));
        }
        Value::Array(values) => values
            .iter()
            .for_each(|value| assert_local_references_resolve(document, value)),
        _ => {}
    }
}

/// An accepted-but-incomplete mutation is only usable if the caller can learn
/// how it ended, and `getOperation` is the only mechanism for that. It is
/// filtered out of the M1 document while incubating, so promoting a `202`
/// operation without it would ship an SDK that can start work it cannot
/// observe. Enforced here rather than remembered, because the lifecycle actions
/// promoted later are all `202` too.
#[test]
fn m1_accepted_operations_have_a_supported_completion_path() {
    let spec = m1_sdk_openapi_value();
    let accepted = spec["paths"]
        .as_object()
        .expect("paths should be an object")
        .iter()
        .flat_map(|(path, item)| {
            item.as_object()
                .expect("path item should be an object")
                .iter()
                .filter(|(method, _)| OPENAPI_HTTP_METHODS.contains(&method.as_str()))
                .filter(|(_, operation)| {
                    operation["responses"]
                        .as_object()
                        .is_some_and(|responses| responses.contains_key("202"))
                })
                .map(move |(method, _)| format!("{method} {path}"))
        })
        .collect::<Vec<_>>();

    assert!(
        accepted.is_empty()
            || pplx_space_public_api::routes::GET_OPERATION
                .status()
                .is_m1_supported(),
        "M1 exposes {accepted:?}, which are accepted asynchronously, but getOperation is not M1-supported"
    );
}

/// `lifecycle_action` is a `match`, so a new `SandboxLifecycleAction` variant
/// cannot compile without being declared. This checks the other half: that each
/// variant is wired to the route named after it, and that the one action served
/// as an HTTP method rather than an `:action` suffix stays that way.
#[test]
fn every_lifecycle_action_has_a_documented_route() {
    let spec = public_openapi_value();
    for action in pplx_space_public_api::SandboxLifecycleAction::ALL {
        if *action == pplx_space_public_api::SandboxLifecycleAction::Delete {
            continue;
        }
        let path = format!("/sandboxes/{{sandbox_id}}:{}", action.as_str());
        assert!(
            spec["paths"][&path]["post"].is_object(),
            "{path} has no documented post operation"
        );
    }
    assert!(
        spec["paths"]["/sandboxes/{sandbox_id}"]["delete"].is_object(),
        "delete is served as DELETE /sandboxes/{{sandbox_id}}, not an :action route"
    );
}

/// M1 supports the three recovery verbs and nothing else from the lifecycle
/// surface. Pinned because promoting an action is a one-line change whose blast
/// radius is the frozen contract.
#[test]
fn m1_lifecycle_surface_is_pause_suspend_and_wake() {
    let promoted = pplx_space_public_api::routes::NATIVE_V1_OPERATIONS
        .iter()
        .filter(|operation| operation.status().is_m1_supported())
        .filter(|operation| operation.path().contains(":"))
        .filter(|operation| operation.path().starts_with("/v1/sandboxes/{sandbox_id}:"))
        .map(|operation| operation.operation_id())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        promoted,
        BTreeSet::from(["pauseSandbox", "suspendSandbox", "wakeSandbox"])
    );
}
