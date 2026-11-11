use super::*;

#[test]
fn startup_policy_round_trip_and_fail_closed_version() {
    for (policy, version) in [("best-effort", 1), ("required", 2)] {
        let runtime: TemplateRuntime = serde_json::from_value(serde_json::json!({
            "schema_version": 1, "apps": [{"name": "app", "command": ["/bin/sleep", "infinity"],
            "user": "root", "working_directory": "/", "startup_timeout_secs": 1,
            "readiness": {"type": "launch"}, "startup_policy": policy}]
        }))
        .unwrap();
        let mut encoded = encode(&runtime);
        assert_eq!(encoded.schema_version, version);
        assert_eq!(decode(encoded.clone()).unwrap(), runtime);
        encoded.apps[0].startup_policy = 99;
        decode(encoded).unwrap_err();
    }
}
