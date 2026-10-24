use super::*;

const E0_DOCUMENT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../../proto/execution/v1/fixtures/e0.json"
));

#[test]
fn golden_document_reloads_with_the_same_identity_regardless_of_object_order() {
    let definition = EnvironmentDefinition::from_document(E0_DOCUMENT).unwrap();
    assert_eq!(
        definition.reference().to_string(),
        "sha256:3168c96b5719ce62543ae53e6641a4eb80416a9f01e5585f6f76c3c70111fa0b"
    );
    assert_eq!(
        EnvironmentDefinition::from_document(&definition.to_document().unwrap()).unwrap(),
        definition
    );
    // Reorder every object explicitly, regardless of serde_json's map backend.
    fn reversed_objects(value: &serde_json::Value) -> String {
        if let Some(object) = value.as_object() {
            let fields = object
                .iter()
                .rev()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap(),
                        reversed_objects(value)
                    )
                })
                .collect::<Vec<_>>();
            format!("{{{}}}", fields.join(","))
        } else {
            value.to_string()
        }
    }
    let document: serde_json::Value = serde_json::from_str(E0_DOCUMENT).unwrap();
    let reordered = reversed_objects(&document);
    assert_ne!(reordered, document.to_string());
    assert_eq!(
        EnvironmentDefinition::from_document(&reordered).unwrap(),
        definition
    );
}

#[test]
fn dropped_nested_fields_and_duplicate_keys_are_not_accepted() {
    let base: serde_json::Value = serde_json::from_str(E0_DOCUMENT).unwrap();
    for pointer in [
        "",
        "/descriptor",
        "/descriptor/guest",
        "/descriptor/runtime/release/runtime",
        "/descriptor/boot/kernel",
        "/descriptor/compatibility/state_format",
    ] {
        let mut document = base.clone();
        document
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("future_semantics".to_owned(), true.into());
        assert!(
            EnvironmentDefinition::from_document(&document.to_string()).is_err(),
            "{pointer}"
        );
    }
    let source = serde_json::to_string(&base).unwrap();
    for key in [
        "schema_version",
        "reference",
        "descriptor",
        "host",
        "guest",
        "os",
        "architecture",
        "runtime",
        "provider",
        "release",
        "kind",
        "version",
        "digest",
        "target",
        "layout",
        "backend",
        "boot",
        "profile",
        "kernel",
        "isolation",
        "compatibility",
        "state_format",
        "format",
    ] {
        // Duplicate each occurrence separately, with the same value: ambiguity
        // is an error even if last-value-wins would produce the same tree.
        let needle = format!("\"{key}\":");
        assert!(source.contains(&needle), "{key}");
        for (position, _) in source.match_indices(&needle) {
            let start = position + needle.len();
            let mut stream = serde_json::Deserializer::from_str(source.get(start..).unwrap())
                .into_iter::<serde_json::Value>();
            let value = stream.next().unwrap().unwrap();
            let duplicate = format!("\"{key}\":{value},");
            let mut text = source.clone();
            text.insert_str(position, &duplicate);
            assert!(
                EnvironmentDefinition::from_document(&text).is_err(),
                "{key} at {position}"
            );
        }
    }
}

#[test]
fn unsupported_profiles_versions_and_changed_identities_fail_closed() {
    let base: serde_json::Value = serde_json::from_str(E0_DOCUMENT).unwrap();
    let cases: &[(&str, serde_json::Value)] = &[
        ("/schema_version", 2.into()),
        ("/descriptor/runtime/provider", "host_facility".into()),
        ("/descriptor/boot/profile", "firmware_disk_v1".into()),
        ("/descriptor/compatibility/profile", "future_v1".into()),
        ("/descriptor/isolation", "future_isolation".into()),
        (
            "/descriptor/runtime/release/runtime/version",
            "1.17.0".into(),
        ),
        ("/descriptor/runtime/release/runtime/kind", "fc".into()),
        ("/reference", format!("sha256:{}", "0".repeat(64)).into()),
    ];
    for (pointer, replacement) in cases {
        let mut document = base.clone();
        *document.pointer_mut(pointer).unwrap() = replacement.clone();
        assert!(
            EnvironmentDefinition::from_document(&document.to_string()).is_err(),
            "{pointer}"
        );
    }
}

#[test]
fn schema_dispatch_precedes_descriptor_decoding() {
    let base: serde_json::Value = serde_json::from_str(E0_DOCUMENT).unwrap();
    for schema in [0, 3, u32::MAX] {
        for descriptor in [
            base["descriptor"].clone(),
            serde_json::json!({"future_semantics": true}),
        ] {
            let mut document = base.clone();
            document["schema_version"] = schema.into();
            document["descriptor"] = descriptor;
            assert!(matches!(
                EnvironmentDefinition::from_document(&document.to_string()),
                Err(EnvironmentDocumentError::Definition(EnvironmentDecodeError::UnsupportedSchema(version))) if version == schema
            ));
        }
    }
    // Version dispatch must not accept an ambiguous or malformed envelope.
    for text in [
        r#"{"schema_version":1,"schema_version":2}"#,
        r#"{"schema_version":2,"schema_version":1}"#,
        r#"{"schema_version":2,"schema_version":2}"#,
        r#"{"schema_version":"2"}"#,
        r#"{"descriptor":{}}"#,
        r#"{"schema_version":2,"descriptor":"#,
    ] {
        assert!(
            matches!(
                EnvironmentDefinition::from_document(text),
                Err(EnvironmentDocumentError::Json(_))
            ),
            "{text}"
        );
    }
}
