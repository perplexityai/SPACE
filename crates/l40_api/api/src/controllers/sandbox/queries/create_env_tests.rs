use super::*;

#[test]
fn sanitize_create_env_keeps_valid_entries_and_drops_invalid_entries() {
    let env = BTreeMap::from([
        ("VALID".to_string(), "ok".to_string()),
        ("MULTILINE".to_string(), "line 1\nline 2".to_string()),
        ("BAD-NAME".to_string(), "drop".to_string()),
        ("BAD_VALUE".to_string(), "drop\0me".to_string()),
    ]);

    let sanitized = sanitize_create_env(&env);

    assert_eq!(sanitized.get("VALID").map(String::as_str), Some("ok"));
    assert_eq!(
        sanitized.get("MULTILINE").map(String::as_str),
        Some("line 1\nline 2")
    );
    assert!(!sanitized.contains_key("BAD-NAME"));
    assert!(!sanitized.contains_key("BAD_VALUE"));
}

#[test]
fn stored_resource_rejects_values_past_the_integer_column() {
    assert_eq!(stored_resource(None, "vcpu").unwrap(), None);
    assert_eq!(
        stored_resource(Some(i32::MAX as u32), "vcpu").unwrap(),
        Some(i32::MAX)
    );
    let error = stored_resource(Some(3_000_000_000), "vcpu").unwrap_err();
    assert_eq!(error.status(), StatusCode::BAD_REQUEST);
}
