use super::*;

fn declaration() -> serde_json::Value {
    serde_json::json!({
        "schema_version": 1, "apps": [{"name": "app", "command": ["/bin/sh", "-lc", "exec app"],
        "user": "root", "working_directory": "/", "env": {"TOKEN": "secret"},
        "readiness": {"type": "exec", "command": ["/bin/true"]},
        "startup_timeout_secs": 30}]
    })
}

#[test]
fn round_trip_preserves_intent_without_exposing_command_or_environment() {
    let runtime: TemplateRuntime = serde_json::from_value(declaration()).unwrap();
    assert_eq!(
        TemplateRuntime::from_bytes(&runtime.canonical_bytes().unwrap()).unwrap(),
        runtime
    );
    let debug = format!("{runtime:?}");
    assert!(!debug.contains("secret"));
    assert!(!debug.contains("exec app"));
}

#[test]
fn rejects_ambiguous_or_unusable_declarations() {
    for (field, value) in [
        ("name", serde_json::json!("../app")),
        ("name", serde_json::json!("")),
        ("name", serde_json::json!("UPPER")),
        ("name", serde_json::json!("a".repeat(65))),
        ("name", serde_json::json!("bad/name")),
        ("command", serde_json::json!([])),
        ("command", serde_json::json!(["a\u{0}b"])),
        ("working_directory", serde_json::json!("relative")),
        ("startup_timeout_secs", serde_json::json!(0)),
        ("startup_timeout_secs", serde_json::json!(61)),
        ("env", serde_json::json!({"BAD-KEY": "value"})),
        (
            "readiness",
            serde_json::json!({"type": "exec", "command": []}),
        ),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut input = declaration();
        input["apps"][0][field] = value;
        assert!(
            serde_json::from_value::<TemplateRuntime>(input).is_err(),
            "{field}"
        );
    }
}

#[test]
fn multiple_apps_are_independent_and_names_and_schema_are_unambiguous() {
    let mut input = declaration();
    let mut worker = input["apps"][0].clone();
    worker["name"] = serde_json::json!("worker");
    worker["command"] = serde_json::json!(["/bin/sleep", "infinity"]);
    input["apps"].as_array_mut().unwrap().push(worker);
    let runtime: TemplateRuntime = serde_json::from_value(input.clone()).unwrap();
    let bytes = runtime.canonical_bytes().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("[[apps]]"));
    assert_eq!(TemplateRuntime::from_bytes(&bytes).unwrap(), runtime);
    assert_eq!(
        runtime.apps()[1].name().unit(),
        "space-runtime@worker.service"
    );
    assert_ne!(runtime.apps()[0].command(), runtime.apps()[1].command());
    for (field, value) in [
        ("schema_version", serde_json::json!(3)),
        ("apps", serde_json::json!([])),
        ("apps", serde_json::json!(vec![input["apps"][0].clone(); 2])),
        (
            "apps",
            serde_json::json!(
                (0..17)
                    .map(|i| {
                        let mut app = input["apps"][0].clone();
                        app["name"] = serde_json::json!(format!("app{i}"));
                        app
                    })
                    .collect::<Vec<_>>()
            ),
        ),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut invalid = input.clone();
        invalid[field] = value;
        assert!(
            serde_json::from_value::<TemplateRuntime>(invalid).is_err(),
            "{field}"
        );
    }
    TemplateRuntime::from_bytes(b"schema_version = 1\napps = [").unwrap_err();
}

#[test]
fn startup_policy_defaults_and_required_version_are_explicit() {
    let runtime: TemplateRuntime = serde_json::from_value(declaration()).unwrap();
    assert_eq!(
        runtime.apps()[0].startup_policy(),
        RuntimeStartupPolicy::BestEffort
    );
    let mut input = declaration();
    input["apps"][0]["startup_policy"] = serde_json::json!("required");
    let runtime: TemplateRuntime = serde_json::from_value(input.clone()).unwrap();
    let bytes = runtime.canonical_bytes().unwrap();
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .contains("schema_version = 2")
    );
    assert_eq!(TemplateRuntime::from_bytes(&bytes).unwrap(), runtime);
    input["apps"][0]["startup_policy"] = serde_json::json!("requred");
    serde_json::from_value::<TemplateRuntime>(input).unwrap_err();
}
