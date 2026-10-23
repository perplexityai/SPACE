use super::*;

#[test]
fn env_key_rejects_shell_unsafe_names() {
    for (key, expected) in [
        ("FOO", Ok(())),
        ("_aZ09", Ok(())),
        ("", Err("env key must not be empty")),
        ("1BAD", Err("env key must start with ASCII letter or '_'")),
        ("é", Err("env key must start with ASCII letter or '_'")),
        (
            "BAD-NAME",
            Err("env key must contain only ASCII letters, digits, or '_'"),
        ),
        (
            "Aé",
            Err("env key must contain only ASCII letters, digits, or '_'"),
        ),
        ("A=", Err("env key contains invalid character")),
        ("A\0", Err("env key contains invalid character")),
    ] {
        assert_eq!(
            EnvKey::parse(key)
                .map(|_| ())
                .map_err(EnvParseError::as_str),
            expected,
            "{key:?}"
        );
    }
    // Exercise both grammar positions independently of the shared byte sets.
    for ch in (0..=255).map(char::from) {
        let first = ch == '_' || ch.is_ascii_alphabetic();
        let rest = first || ch.is_ascii_digit();
        assert_eq!(
            EnvKey::parse(format!("{ch}A")).is_ok(),
            first,
            "first {ch:?}"
        );
        assert_eq!(EnvKey::parse(format!("A{ch}")).is_ok(), rest, "rest {ch:?}");
    }
    for (length, expected) in [(128, Ok(())), (129, Err("env key is too long"))] {
        assert_eq!(
            EnvKey::parse("A".repeat(length))
                .map(|_| ())
                .map_err(EnvParseError::as_str),
            expected
        );
    }
}

#[test]
fn env_value_debug_is_redacted() {
    let value = EnvValue::parse("secret").unwrap();
    assert_eq!(format!("{value:?}"), "EnvValue(<redacted>)");
}

#[test]
fn env_value_accepts_newlines() {
    let value = EnvValue::parse("line 1\nline 2\r\n").unwrap();
    assert_eq!(value.expose(), "line 1\nline 2\r\n");
}

#[test]
fn env_value_rejects_nul() {
    EnvValue::parse("bad\0value").unwrap_err();
}

#[test]
fn env_map_converts_from_raw_proto_map() {
    let raw = BTreeMap::from([("PATH".to_string(), "/bin".to_string())]);
    let env = EnvMap::try_from(raw).unwrap();
    assert_eq!(env.as_map().len(), 1);

    let raw = HashMap::from([("SANDBOX_TYPE".to_string(), "example-workload".to_string())]);
    let env = EnvMap::try_from(raw).unwrap();
    let raw = HashMap::<String, String>::from(env);
    assert_eq!(
        raw.get("SANDBOX_TYPE").map(String::as_str),
        Some("example-workload")
    );

    let invalid = BTreeMap::from([("BAD-NAME".to_string(), "value".to_string())]);
    EnvMap::try_from(invalid).unwrap_err();
}

#[cfg(feature = "serde")]
#[test]
fn env_map_serializes_as_validated_json_object() {
    let env = EnvMap::try_from(BTreeMap::from([(
        "PATH".to_string(),
        "/usr/bin".to_string(),
    )]))
    .unwrap();

    let value = serde_json::to_value(&env).unwrap();
    assert_eq!(value, serde_json::json!({ "PATH": "/usr/bin" }));

    let parsed: EnvMap = serde_json::from_value(value).unwrap();
    assert_eq!(
        parsed
            .as_map()
            .get(&EnvKey::parse("PATH").unwrap())
            .map(EnvValue::expose),
        Some("/usr/bin")
    );

    let invalid = serde_json::json!({ "BAD-NAME": "value" });
    serde_json::from_value::<EnvMap>(invalid).unwrap_err();
}

#[test]
fn env_map_sanitize_drops_invalid_entries() {
    let raw = BTreeMap::from([
        ("VALID".to_string(), "ok".to_string()),
        ("MULTILINE".to_string(), "line 1\nline 2".to_string()),
        ("BAD-NAME".to_string(), "drop".to_string()),
        ("BAD_VALUE".to_string(), "drop\0me".to_string()),
    ]);

    let (env, issues) = EnvMap::sanitize(raw);

    assert_eq!(env.as_map().len(), 2);
    assert_eq!(
        env.as_map()
            .get(&EnvKey::parse("VALID").unwrap())
            .map(EnvValue::expose),
        Some("ok")
    );
    assert_eq!(
        env.as_map()
            .get(&EnvKey::parse("MULTILINE").unwrap())
            .map(EnvValue::expose),
        Some("line 1\nline 2")
    );
    assert_eq!(
        issues,
        vec![
            EnvSanitizationIssue::RejectedEntry {
                key: "BAD-NAME".to_string(),
                reason: EnvParseError("env key must contain only ASCII letters, digits, or '_'"),
            },
            EnvSanitizationIssue::RejectedEntry {
                key: "BAD_VALUE".to_string(),
                reason: EnvParseError("env value contains invalid character"),
            },
        ]
    );
}

#[test]
fn env_map_sanitize_accepts_string_like_iterators() {
    let (env, issues) =
        EnvMap::sanitize([("DUPLICATE", "old"), ("VALID", "ok"), ("DUPLICATE", "new")]);

    assert!(issues.is_empty());
    assert_eq!(
        env.as_map()
            .get(&EnvKey::parse("DUPLICATE").unwrap())
            .map(EnvValue::expose),
        Some("new")
    );
    assert_eq!(
        env.as_map()
            .get(&EnvKey::parse("VALID").unwrap())
            .map(EnvValue::expose),
        Some("ok")
    );
}

#[test]
fn env_map_sanitize_does_not_retain_rejected_values_in_debug() {
    let raw = BTreeMap::from([("BAD".to_string(), "secret\0value".to_string())]);

    let (_env, issues) = EnvMap::sanitize(raw);
    let debug = format!("{issues:?}");

    assert!(debug.contains("BAD"));
    assert!(debug.contains("env value contains invalid character"));
    assert!(!debug.contains("secret"));
}

#[test]
fn env_map_sanitize_reports_too_long_value_by_key_without_value() {
    let raw = BTreeMap::from([(
        "BIG_SECRET".to_string(),
        format!("secret-{}", "x".repeat(MAX_ENV_VALUE_BYTES)),
    )]);

    let (_env, issues) = EnvMap::sanitize(raw);
    let debug = format!("{issues:?}");

    assert_eq!(
        issues,
        vec![EnvSanitizationIssue::RejectedEntry {
            key: "BIG_SECRET".to_string(),
            reason: EnvParseError("env value is too long"),
        }]
    );
    assert!(!debug.contains("secret-"));
}

#[test]
fn env_map_sanitize_caps_entry_count() {
    let raw = (0..=MAX_ENV_ENTRIES).map(|idx| (format!("KEY_{idx:03}"), "value".to_string()));

    let (env, issues) = EnvMap::sanitize(raw);

    assert_eq!(env.as_map().len(), MAX_ENV_ENTRIES);
    assert_eq!(
        issues,
        vec![EnvSanitizationIssue::MaxEntriesExceeded {
            max: MAX_ENV_ENTRIES,
            dropped: vec![format!("KEY_{MAX_ENV_ENTRIES:03}")],
        }]
    );
}

#[test]
fn env_map_sanitize_caps_total_bytes() {
    let mut raw = (0..8)
        .map(|idx| (format!("K{idx:02}"), "x".repeat(MAX_ENV_VALUE_BYTES - 3)))
        .collect::<BTreeMap<_, _>>();
    raw.insert("ZZZ".to_string(), "would overflow".to_string());

    let (env, issues) = EnvMap::sanitize(raw);

    assert_eq!(env.as_map().len(), 8);
    assert!(env.as_map().contains_key(&EnvKey::parse("K00").unwrap()));
    assert!(!env.as_map().contains_key(&EnvKey::parse("ZZZ").unwrap()));
    assert_eq!(
        issues,
        vec![EnvSanitizationIssue::TotalBytesExceeded {
            max: MAX_ENV_TOTAL_BYTES,
            dropped: vec!["ZZZ".to_string()],
        }]
    );
}
