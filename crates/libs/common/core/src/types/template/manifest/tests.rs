use super::*;

#[test]
fn round_trip_preserves_shape() {
    let mut m = TemplateManifest {
        guest_image: None,
        schema_version: None,
        execution: None,
        name: "seed".to_string(),
        content_hash: "sha256:abc".to_string(),
        build_date: "2026-05-19T12:34:56Z".to_string(),
        runtime: "fc".to_string(),
        arch: "aarch64".to_string(),
        config: serde_json::Value::Object(serde_json::Map::new()),
        bootable_artifact: "rootfs.ext4".to_string(),
        files: BTreeMap::new(),
    };
    m.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:abc".to_string(),
            size: Some(1024),
            ..Default::default()
        },
    );
    m.files.insert(
        "rootfs.ext4.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:def".to_string(),
            size: Some(256),
            compression: Some(TemplateManifestCompression {
                codec: "zstd".to_string(),
                level: 3,
            }),
        },
    );
    let json = serde_json::to_string(&m).unwrap();
    let parsed: TemplateManifest = serde_json::from_str(&json).unwrap();
    assert_eq!(m, parsed);
}

#[test]
fn parses_minimal_legacy_manifest_with_only_required_fields() {
    // The seed builder's build.sh writes this shape today before
    // template-manager normalizes it on first GetTemplate.
    let raw = r#"{
            "name": "seed",
            "content_hash": "sha256:abc",
            "build_date": "2026-05-19T12:34:56Z",
            "runtime": "fc",
            "arch": "aarch64",
            "config": {},
            "bootable_artifact": "rootfs.ext4",
            "files": {
                "rootfs.ext4": { "digest": "sha256:abc", "size": 1024 }
            }
        }"#;
    let m: TemplateManifest =
        serde_json::from_str(raw).expect("legacy minimal manifest must parse");
    assert_eq!(m.name, "seed");
    assert_eq!(m.bootable_artifact, "rootfs.ext4");
    assert_eq!(m.files["rootfs.ext4"].size, Some(1024));
    assert!(m.files["rootfs.ext4"].compression.is_none());
}

#[test]
fn parses_empty_manifest_via_serde_defaults() {
    // An empty object exercises the `#[serde(default)]` on every
    // field — guards against a missing-field-fails-the-parse
    // regression that would break disk recovery's
    // "manifest exists but is partial" path.
    let m: TemplateManifest =
        serde_json::from_str("{}").expect("empty manifest must parse via defaults");
    assert_eq!(m.name, "");
    assert_eq!(m.bootable_artifact, "");
    assert!(m.files.is_empty());
    assert!(matches!(m.config, serde_json::Value::Object(ref o) if o.is_empty()));
}

#[test]
fn unknown_fields_are_dropped() {
    // Forward-compat note: we intentionally do NOT use
    // `#[serde(deny_unknown_fields)]`. A future writer that adds a
    // field MUST not break older readers; instead the field gets
    // dropped on parse → re-serialize. If a consumer ever needs to
    // preserve unknown fields, switch to a `#[serde(flatten)] extra`
    // map. This test pins the current "drop on parse" behavior so
    // a future change to deny_unknown_fields is a deliberate one.
    let raw = r#"{ "name": "seed", "unexpected_field": 42 }"#;
    let m: TemplateManifest = serde_json::from_str(raw).unwrap();
    assert_eq!(m.name, "seed");
    let round_tripped = serde_json::to_string(&m).unwrap();
    assert!(
        !round_tripped.contains("unexpected_field"),
        "round-trip currently drops unknown fields: {round_tripped}"
    );
}

#[test]
fn size_missing_vs_zero_survives_round_trip() {
    // The pre-typing `serde_json::Value` view distinguished a missing
    // `size` (signal to consumers like `rootfs_artifact_size_from_manifest`
    // to fall back to `fs::metadata().len()`) from `size: 0`
    // (accepted as a literal zero). The typed `Option<u64>` field +
    // `skip_serializing_if = "Option::is_none"` preserves that
    // distinction. Without this, a missing `size` would collapse to
    // a written `0` and downstream consumers would treat torn /
    // half-written manifests as if they declared a 0-byte file.
    let missing = r#"{ "name": "x", "files": { "rootfs.ext4": { "digest": "sha256:abc" } } }"#;
    let m: TemplateManifest = serde_json::from_str(missing).unwrap();
    assert_eq!(m.files["rootfs.ext4"].size, None);
    let round_tripped = serde_json::to_string(&m.files["rootfs.ext4"]).unwrap();
    assert!(
        !round_tripped.contains("\"size\""),
        "missing size must stay absent on re-serialize: {round_tripped}"
    );

    let zero =
        r#"{ "name": "x", "files": { "rootfs.ext4": { "digest": "sha256:abc", "size": 0 } } }"#;
    let m: TemplateManifest = serde_json::from_str(zero).unwrap();
    assert_eq!(m.files["rootfs.ext4"].size, Some(0));
    let round_tripped = serde_json::to_string(&m.files["rootfs.ext4"]).unwrap();
    assert!(
        round_tripped.contains("\"size\":0"),
        "explicit zero must survive re-serialize: {round_tripped}"
    );
}

// Tests for the public API surface (typed accessors + the
// filesystem-safety validator) live in the integration test file
// `tests/template_manifest_api.rs` so they only see what
// downstream crates can see.
