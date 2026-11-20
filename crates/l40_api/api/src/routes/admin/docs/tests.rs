use super::{ADMIN_LLMS_TXT, admin_openapi_value};
use std::collections::BTreeMap;

#[test]
fn admin_openapi_stub_is_admin_only() {
    let spec = admin_openapi_value();

    assert_eq!(spec["openapi"], "3.1.0");
    assert_eq!(spec["servers"][0]["url"], "/v1");
    let paths = spec["paths"]
        .as_object()
        .expect("paths should be an object");

    // The admin mount never republishes the public-API-prefixed path.
    assert!(!paths.contains_key("/v1/sandboxes"));

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
                .collect();
            methods.sort_unstable();
            (path.as_str(), methods)
        })
        .collect();
    insta::assert_json_snapshot!(paths_by_method);
}

#[test]
fn admin_llms_txt_advertises_admin_roots() {
    assert!(ADMIN_LLMS_TXT.contains("SPACE Admin API"));
    assert!(ADMIN_LLMS_TXT.contains("/openapi.json on admin.api.space hosts"));
    assert!(ADMIN_LLMS_TXT.contains("/admin/api/openapi.json on shared hosts"));
    assert!(ADMIN_LLMS_TXT.contains("/v1 on admin.api.space hosts"));
    assert!(ADMIN_LLMS_TXT.contains("/admin/api/v1 on shared hosts"));
    assert!(ADMIN_LLMS_TXT.contains("/e2b/api on shared hosts"));
}

/// The `:drain` stub carries operator guidance an operator acts on, and the
/// path→method snapshot does not look at descriptions at all — so before this
/// test the wording was unpinned.
///
/// Asserts the *facts*, not the wording: that the destructive consequence of
/// `forced` is stated as unrecoverable, that its lack of a grace period is
/// stated, that both pre-flight gauges are named (neither is a superset of what
/// the sweep destroys, so naming one is worse than naming none), and that the
/// four ways `:drain` can 409 are all reachable from the response description.
/// Phrase-level assertions rather than a snapshot, so rewording is free and
/// dropping a fact is not.
#[test]
fn the_drain_stub_states_what_forced_mode_destroys() {
    let spec = admin_openapi_value();
    let drain = &spec["paths"]["/nodes/{node_id}:drain"]["post"];

    let mode = drain["requestBody"]["content"]["application/json"]["schema"]["properties"]["mode"]
        ["description"]
        .as_str()
        .expect("mode description");
    for fact in [
        "unrecoverabl",
        "no grace period",
        "node_drain.sandboxes_remaining",
        "node_drain.drain_blockers",
    ] {
        assert!(
            mode.contains(fact),
            "the mode description must state {fact:?}; got {mode:?}"
        );
    }

    let conflict = drain["responses"]["409"]["description"]
        .as_str()
        .expect("409 description");
    for cause in [
        "drain_phase",
        "downgrade",
        "replace_after_terminate",
        "require_undrained",
    ] {
        assert!(
            conflict.contains(cause),
            "the 409 must name the {cause:?} cause; got {conflict:?}"
        );
    }

    // The in-flight edge's contract, which is what makes an omitted field
    // different from an explicit one.
    let body = drain["requestBody"]["description"]
        .as_str()
        .expect("body description");
    assert!(
        body.contains("already draining"),
        "the body description must cover the already-draining edge; got {body:?}"
    );
}

/// The `:drain` stub documents the v2 drain: its `engine` and
/// `hold_seconds` fields, that a hold is required to start one and how a
/// repeat moves the deadline, that a running sandbox is held until the
/// deadline, that a v2 drain takes no mode, and the refusals' statuses and
/// codes. Facts, not wording.
#[test]
fn the_drain_stub_states_the_v2_contract() {
    let spec = admin_openapi_value();
    let drain = &spec["paths"]["/nodes/{node_id}:drain"]["post"];
    let properties = &drain["requestBody"]["content"]["application/json"]["schema"]["properties"];
    assert_eq!(
        properties["engine"]["enum"],
        serde_json::json!(["v1", "v2"])
    );
    assert!(properties.get("deadline").is_none());
    let hold = &properties["hold_seconds"];
    assert_eq!(hold["type"], "integer");
    assert_eq!(hold["minimum"], 0);
    assert_eq!(hold["maximum"], 31_536_000);
    let hold = hold["description"]
        .as_str()
        .expect("hold_seconds description");
    for fact in [
        "Required",
        "no default",
        "drain_status.deadline",
        "shorter",
        "409",
        "400",
    ] {
        assert!(hold.contains(fact), "{fact:?}: {hold:?}");
    }
    let engine = properties["engine"]["description"]
        .as_str()
        .expect("engine description");
    for fact in [
        "SPACE_DRAIN_V2_ENABLED",
        "drain_v2_node_heartbeat_stale",
        "drain_v2_evacuation_cap",
        "Retry-After",
        "hold_seconds 0",
        "undrain",
    ] {
        assert!(engine.contains(fact), "{fact:?}: {engine:?}");
    }
    let mode = properties["mode"]["description"]
        .as_str()
        .expect("mode description");
    assert!(mode.contains("takes no mode"), "{mode:?}");
    let body = drain["requestBody"]["description"]
        .as_str()
        .expect("body description");
    for fact in ["hold", "deadline", "503", "Retry-After", "409"] {
        assert!(body.contains(fact), "{fact:?}: {body:?}");
    }
    for (status, facts) in [
        (
            "400",
            &["SPACE_DRAIN_V2_ENABLED", "hold_seconds", "mode forced"][..],
        ),
        (
            "409",
            &[
                "drain_v2_node_heartbeat_stale",
                "drain_v2_evacuation_cap",
                "Retry-After",
                "hold_seconds",
            ][..],
        ),
    ] {
        let description = drain["responses"][status]["description"]
            .as_str()
            .expect("a response description");
        for fact in facts {
            assert!(
                description.contains(fact),
                "{status} {fact:?}: {description:?}"
            );
        }
    }
    for path in ["/nodes", "/nodes/{node_id}"] {
        let description = spec["paths"][path]["get"]["description"]
            .as_str()
            .expect("a node read description");
        for fact in ["drain_engine", "drain"] {
            assert!(
                description.contains(fact),
                "{path} {fact:?}: {description:?}"
            );
        }
    }
}
