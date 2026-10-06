use super::*;
use crate::types::template::{Digest, TemplateName};

fn fixture_hash() -> ContentHash {
    ContentHash::new(Digest::sha256([0xab; 32]))
}

#[test]
fn key_layout_matches_on_disk_cache() {
    let template = TemplateName::parse("seed").unwrap();
    let hash = fixture_hash();
    assert_eq!(
        template_object_key("", &template, hash, TemplateArtifact::RootfsZstd),
        "templates/seed/sha256:abababababababababababababababababababababababababababababababab/rootfs.ext4.zst",
    );
    assert_eq!(
        template_object_key("", &template, hash, TemplateArtifact::Metadata),
        "templates/seed/sha256:abababababababababababababababababababababababababababababababab/metadata.json",
    );
    assert_eq!(
        template_object_key("", &template, hash, TemplateArtifact::Manifest),
        "templates/seed/sha256:abababababababababababababababababababababababababababababababab/manifest.json",
    );
}

#[test]
fn key_prefix_is_joined_with_single_slash() {
    let template = TemplateName::parse("seed").unwrap();
    let hash = fixture_hash();
    for prefix in ["clusters/abc", "/clusters/abc/", "clusters/abc/"] {
        let key = template_object_key(prefix, &template, hash, TemplateArtifact::RootfsZstd);
        assert!(
            key.starts_with("clusters/abc/templates/seed/sha256:"),
            "prefix={prefix} key={key}",
        );
        assert!(!key.contains("//"), "prefix={prefix} key={key}");
    }
}

/// Non-vacuous by construction: it enumerates the enum rather than a
/// hand-written copy of the constant, so a fourth `TemplateArtifact` fails
/// this test until it is either published or deliberately excluded here.
#[test]
fn every_template_artifact_is_published() {
    let published = TemplateArtifact::PUBLISHED;
    for artifact in [
        TemplateArtifact::RootfsZstd,
        TemplateArtifact::Metadata,
        TemplateArtifact::Manifest,
    ] {
        assert!(
            published.contains(&artifact),
            "{artifact:?} is not in TemplateArtifact::PUBLISHED; a publisher that \
                 iterates the constant would silently reproduce an incomplete build",
        );
    }
    assert_eq!(published.len(), 3, "PUBLISHED gained or lost an entry");
}

/// The manifest describes the other artifacts, so it must be written after
/// them — see the constant's own docs.
#[test]
fn the_manifest_is_published_last() {
    assert_eq!(
        TemplateArtifact::PUBLISHED.last(),
        Some(&TemplateArtifact::Manifest),
    );
}

#[test]
fn template_prefix_is_the_key_without_the_file_name() {
    let template = TemplateName::parse("seed").unwrap();
    let hash = fixture_hash();
    for prefix in ["", "clusters/abc", "/clusters/abc/"] {
        let dir = template_object_prefix(prefix, &template, hash);
        assert!(!dir.ends_with('/'), "prefix={prefix} dir={dir}");
        for artifact in [
            TemplateArtifact::RootfsZstd,
            TemplateArtifact::Metadata,
            TemplateArtifact::Manifest,
        ] {
            assert_eq!(
                template_object_key(prefix, &template, hash, artifact),
                format!("{dir}/{}", artifact.file_name()),
                "prefix={prefix} artifact={artifact:?}",
            );
        }
    }
}

/// The durable URI shape for S3-backed template releases.
/// Pinned as text because the value is durable state read back by
/// template-manager, not something a producer may re-spell at will.
#[test]
fn storage_uri_renders_real_s3_without_endpoint_or_path_style() {
    let template = TemplateName::parse("seed").unwrap();
    let uri = StorageUri {
        bucket: "example-template-artifacts".to_string(),
        key_prefix: template_object_prefix("", &template, fixture_hash()),
        region: "us-east-1".to_string(),
        endpoint: None,
        path_style: false,
    };

    assert_eq!(
        uri.to_string(),
        "s3://example-template-artifacts/templates/seed/sha256:abababababababababababababababababababababababababababababababab?region=us-east-1",
    );
}

#[test]
fn storage_uri_display_round_trips_every_parseable_shape() {
    for wire in [
        "s3://bucket?region=us-east-1",
        "s3://bucket/templates/seed/sha256:ab?region=us-east-1",
        "s3://bucket/p?region=us-east-1&path_style=true",
        "s3://bucket/p?region=us-east-1&endpoint=http://object-store:8333",
        "s3://bucket/p?region=us-east-1&endpoint=http://object-store:8333&path_style=false",
        // Redundant relative to the endpoint-implied default, so Display
        // drops it; the parsed value is what has to survive.
        "s3://bucket/p?region=us-east-1&endpoint=http://object-store:8333&path_style=true",
    ] {
        let parsed = StorageUri::parse(wire).expect("fixture should parse");
        let rendered = parsed.to_string();
        assert_eq!(
            StorageUri::parse(&rendered).expect("rendered uri should parse"),
            parsed,
            "wire={wire} rendered={rendered}",
        );
    }
}

#[test]
fn snapshot_key_layout_uses_named_artifacts() {
    let id = "01234567-89ab-7def-8123-456789abcdef";
    let snap = "019253ab-cdef-7000-8123-000000000001";
    let shard = snapshot_shard(id);
    assert_eq!(
        snapshot_object_key("", id, snap, SnapshotArtifact::Rootfs),
        format!("suspend_snapshots/{shard:02x}/{id}/{snap}/rootfs"),
    );
    assert_eq!(
        snapshot_object_key("", id, snap, SnapshotArtifact::Memory),
        format!("suspend_snapshots/{shard:02x}/{id}/{snap}/memfile"),
    );
    assert_eq!(
        snapshot_object_key("", id, snap, SnapshotArtifact::VmState),
        format!("suspend_snapshots/{shard:02x}/{id}/{snap}/binfile"),
    );
    assert_eq!(
        snapshot_object_key("", id, snap, SnapshotArtifact::Metadata),
        format!("suspend_snapshots/{shard:02x}/{id}/{snap}/metadata.json"),
    );
}

#[test]
fn snapshot_key_prefix_is_joined_with_single_slash() {
    let id = "abc123";
    let snap = "snap-1";
    let shard = snapshot_shard(id);
    let expected = format!("clusters/xyz/suspend_snapshots/{shard:02x}/{id}/{snap}/memfile");
    for prefix in ["clusters/xyz", "/clusters/xyz/", "clusters/xyz/"] {
        let key = snapshot_object_key(prefix, id, snap, SnapshotArtifact::Memory);
        assert_eq!(key, expected, "prefix={prefix}");
        assert!(!key.contains("//"), "prefix={prefix} key={key}");
    }
}

#[test]
fn snapshot_key_parser_round_trips_the_canonical_layout() {
    let id = "01234567-89ab-7def-8123-456789abcdef";
    let snap = "019253ab-cdef-7000-8123-000000000001";
    let key = snapshot_object_key("clusters/xyz", id, snap, SnapshotArtifact::RootfsBtrfsSend);

    assert_eq!(
        parse_snapshot_object_key(&key),
        Some(SnapshotObjectKeyParts {
            sandbox_id: id,
            snapshot_id: snap,
            file_name: SnapshotArtifact::RootfsBtrfsSend.file_name(),
        })
    );

    let wrong_shard = key.replacen(&format!("/{:02x}/", snapshot_shard(id)), "/ff/", 1);
    assert_eq!(parse_snapshot_object_key(&wrong_shard), None);
    assert_eq!(parse_snapshot_object_key(&format!("{key}/extra")), None);
}

#[test]
fn snapshot_keys_for_distinct_attempts_never_collide() {
    // Two attempts on the same sandbox land under distinct nested
    // directories, so the second never overwrites the first at the byte
    // level. This is the structural invariant the immutable-snapshot
    // design relies on for the read-during-upload race.
    let id = "019253ab-cdef-7000-8123-000000000000";
    let snap_a = "019253ab-cdef-7000-8123-000000000001";
    let snap_b = "019253ab-cdef-7000-8123-000000000002";
    for artifact in [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::Metadata,
    ] {
        let key_a = snapshot_object_key("", id, snap_a, artifact);
        let key_b = snapshot_object_key("", id, snap_b, artifact);
        assert_ne!(key_a, key_b);
    }
}

#[test]
fn snapshot_shard_is_in_range_and_distributes_uuid_ids() {
    // All shards must be < SNAPSHOT_SHARD_COUNT.
    for i in 0..1024 {
        let id = format!("019e3e2d-7038-7c40-b8fa-{i:012x}");
        assert!((snapshot_shard(&id) as u32) < SNAPSHOT_SHARD_COUNT);
    }

    // Uniform-enough: across 4096 synthetic UUIDv7-shaped ids the
    // hottest shard should not exceed 3× the mean. This catches
    // hashing accidents (e.g. only-the-first-byte sensitivity) while
    // staying well above the natural variance of FNV-1a.
    let mut counts = [0u32; SNAPSHOT_SHARD_COUNT as usize];
    for i in 0..4096_u32 {
        let id = format!("019e3e2d-7038-7c40-b8fa-{i:012x}");
        counts[snapshot_shard(&id) as usize] += 1;
    }
    let mean = 4096 / SNAPSHOT_SHARD_COUNT;
    let max = counts.iter().copied().max().unwrap();
    assert!(max < mean * 3, "shard distribution skewed: {counts:?}");
}

#[test]
fn snapshot_shard_handles_short_ids() {
    // Non-UUID identifiers used in tests and CLI examples must still
    // hash; the fn is total over arbitrary strings.
    assert!((snapshot_shard("sbx-1") as u32) < SNAPSHOT_SHARD_COUNT);
    assert!((snapshot_shard("") as u32) < SNAPSHOT_SHARD_COUNT);
    // Deterministic across calls.
    assert_eq!(snapshot_shard("hello"), snapshot_shard("hello"));
}

#[test]
fn storage_uri_handles_bucket_only() {
    let parts = StorageUri::parse("s3://my-bucket?region=us-east-2").unwrap();
    assert_eq!(parts.bucket, "my-bucket");
    assert_eq!(parts.key_prefix, "");
    assert_eq!(parts.region, "us-east-2");
    assert_eq!(parts.endpoint, None);
    assert!(!parts.path_style);
}

#[test]
fn exact_storage_uri_preserves_validated_wire_representation() {
    let wire = "s3://my-bucket/path?endpoint=http://minio:9000&region=us-east-2&path_style=false";
    let exact = ExactStorageUri::parse(wire).unwrap();

    assert_eq!(exact.as_wire(), wire);
    assert_eq!(exact.parsed().bucket, "my-bucket");
    assert_eq!(
        exact,
        ExactStorageUri::parse(
            "s3://my-bucket/path?region=us-east-2&path_style=false&endpoint=http://minio:9000"
        )
        .unwrap()
    );
}

#[test]
fn storage_uri_handles_bucket_and_prefix() {
    let parts =
        StorageUri::parse("s3://my-bucket/templates/seed/sha256:abc?region=us-east-2").unwrap();
    assert_eq!(parts.bucket, "my-bucket");
    assert_eq!(parts.key_prefix, "templates/seed/sha256:abc");
    assert_eq!(parts.region, "us-east-2");
}

#[test]
fn storage_uri_strips_trailing_slash() {
    let parts = StorageUri::parse("s3://my-bucket/path/?region=us-east-2").unwrap();
    assert_eq!(parts.key_prefix, "path");
}

#[test]
fn storage_uri_extracts_endpoint_and_defaults_path_style() {
    let parts =
        StorageUri::parse("s3://my-bucket/path?region=us-east-2&endpoint=http://minio:9000")
            .unwrap();
    assert_eq!(parts.endpoint.as_deref(), Some("http://minio:9000"));
    assert!(parts.path_style, "endpoint should default path_style=true");
}

#[test]
fn storage_uri_explicit_path_style_overrides_default() {
    let parts = StorageUri::parse(
        "s3://my-bucket/path?region=us-east-2&endpoint=http://minio:9000&path_style=false",
    )
    .unwrap();
    assert!(!parts.path_style);

    let parts = StorageUri::parse("s3://my-bucket/path?region=us-east-2&path_style=true").unwrap();
    assert!(parts.path_style);
}

#[test]
fn storage_uri_requires_region() {
    assert!(matches!(
        StorageUri::parse("s3://my-bucket/path"),
        Err(StorageUriParseError::MissingRegion(_)),
    ));
    assert!(matches!(
        StorageUri::parse("s3://my-bucket/path?endpoint=http://minio:9000"),
        Err(StorageUriParseError::MissingRegion(_)),
    ));
}

#[test]
fn storage_uri_rejects_non_s3_scheme() {
    assert!(matches!(
        StorageUri::parse("https://example.com/foo"),
        Err(StorageUriParseError::NotS3(_)),
    ));
    assert!(matches!(
        StorageUri::parse(""),
        Err(StorageUriParseError::NotS3(_)),
    ));
    assert!(matches!(
        StorageUri::parse("s3://"),
        Err(StorageUriParseError::Empty(_)),
    ));
    assert!(matches!(
        StorageUri::parse("s3:///path?region=us-east-2"),
        Err(StorageUriParseError::MissingBucket(_)),
    ));
}

#[test]
fn storage_uri_rejects_empty_bucket_with_query_only() {
    // Regression: the None arm of split_once on an empty path used
    // to fall through with bucket = "", silently accepting a URI
    // with no bucket at all.
    assert!(matches!(
        StorageUri::parse("s3://?region=us-east-2"),
        Err(StorageUriParseError::MissingBucket(_)),
    ));
    assert!(matches!(
        StorageUri::parse("s3://?region=us-east-2&endpoint=http://minio:9000"),
        Err(StorageUriParseError::MissingBucket(_)),
    ));
}

#[test]
fn storage_uri_rejects_unknown_query_params() {
    assert!(matches!(
        StorageUri::parse("s3://my-bucket/path?region=us-east-2&extra=oops"),
        Err(StorageUriParseError::UnsupportedQuery { .. }),
    ));
    assert!(matches!(
        StorageUri::parse("s3://my-bucket/path?region=us-east-2&path_style=maybe"),
        Err(StorageUriParseError::InvalidPathStyle { .. }),
    ));
}
