//! Integration tests for the public API surface of
//! [`pplx_space_template_manifest::TemplateManifest`] — the typed
//! accessors and the filesystem-safety validator that template-manager
//! depends on at the manifest parse boundary.
//!
//! These are integration tests rather than inline `#[cfg(test)] mod
//! tests` so the test code only sees what downstream crates can see;
//! the test file itself acts as a guard against accidentally relying
//! on a private helper.

use pplx_space_template_manifest::{
    TemplateManifest, TemplateManifestCompression, TemplateManifestFile, ZSTD_CODEC,
};

#[test]
fn is_zstd_compressed_predicate_accepts_well_formed_entry() {
    let entry = TemplateManifestFile {
        digest: "sha256:abc".to_string(),
        size: Some(1024),
        compression: Some(TemplateManifestCompression {
            codec: ZSTD_CODEC.to_string(),
            level: 3,
        }),
    };
    assert!(entry.is_zstd_compressed());
}

#[test]
fn is_zstd_compressed_rejects_partial_entries() {
    // Predicate guards against every field that the pre-typing
    // template-manager `manifest_has_compressed_rootfs_entry` check
    // validated before declaring the entry usable.
    let codec_missing = TemplateManifestFile {
        digest: "sha256:abc".to_string(),
        size: Some(1024),
        ..Default::default()
    };
    assert!(!codec_missing.is_zstd_compressed());

    let codec_wrong = TemplateManifestFile {
        digest: "sha256:abc".to_string(),
        size: Some(1024),
        compression: Some(TemplateManifestCompression {
            codec: "lz4".to_string(),
            level: 3,
        }),
    };
    assert!(!codec_wrong.is_zstd_compressed());

    let level_unset = TemplateManifestFile {
        digest: "sha256:abc".to_string(),
        size: Some(1024),
        compression: Some(TemplateManifestCompression {
            codec: ZSTD_CODEC.to_string(),
            level: 0,
        }),
    };
    assert!(!level_unset.is_zstd_compressed());

    let digest_empty = TemplateManifestFile {
        digest: String::new(),
        size: Some(1024),
        compression: Some(TemplateManifestCompression {
            codec: ZSTD_CODEC.to_string(),
            level: 3,
        }),
    };
    assert!(!digest_empty.is_zstd_compressed());

    let size_missing = TemplateManifestFile {
        digest: "sha256:abc".to_string(),
        size: None,
        compression: Some(TemplateManifestCompression {
            codec: ZSTD_CODEC.to_string(),
            level: 3,
        }),
    };
    assert!(!size_missing.is_zstd_compressed());
}

#[test]
fn compressed_rootfs_follows_bootable_artifact() {
    let mut m = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    // No files yet → no compressed entry.
    assert!(m.compressed_rootfs().is_none());
    assert!(!m.has_compressed_rootfs());

    // Only uncompressed → still no compressed entry.
    m.files.insert(
        "rootfs.ext4".to_string(),
        TemplateManifestFile {
            digest: "sha256:abc".to_string(),
            size: Some(1024),
            ..Default::default()
        },
    );
    assert!(m.compressed_rootfs().is_none());

    // Add the .zst sibling → now the accessor finds it.
    m.files.insert(
        "rootfs.ext4.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:def".to_string(),
            size: Some(256),
            compression: Some(TemplateManifestCompression {
                codec: ZSTD_CODEC.to_string(),
                level: 3,
            }),
        },
    );
    let zstd_entry = m.compressed_rootfs().expect("zstd entry must be found");
    assert_eq!(zstd_entry.digest, "sha256:def");
    assert!(m.has_compressed_rootfs());

    // Bootable artifact empty → accessor declines even if a `.zst`
    // file happens to be listed under some other key.
    m.bootable_artifact = String::new();
    assert!(m.compressed_rootfs().is_none());
}

#[test]
fn validate_filenames_accepts_typical_manifests() {
    let mut m = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
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
                codec: ZSTD_CODEC.to_string(),
                level: 3,
            }),
        },
    );
    // Standalone non-rootfs file with one dot in the name — common
    // shape for kernels, configs, etc.
    m.files.insert(
        "kernel.img".to_string(),
        TemplateManifestFile {
            digest: "sha256:ghi".to_string(),
            size: Some(2048),
            ..Default::default()
        },
    );
    m.validate_filenames()
        .expect("typical manifest must validate");
}

#[test]
fn validate_filenames_rejects_path_traversal_and_friends() {
    let cases: &[(&str, &str)] = &[
        ("../etc/passwd", "path_separator"),
        ("foo/bar", "path_separator"),
        ("..", "hidden_or_dot"),
        (".", "hidden_or_dot"),
        (".hidden", "hidden_or_dot"),
        ("with\0nul", "nul_byte"),
        ("", "empty"),
    ];
    for (name, expected_reason) in cases {
        let mut m = TemplateManifest {
            bootable_artifact: "rootfs.ext4".to_string(),
            ..Default::default()
        };
        m.files.insert(
            name.to_string(),
            TemplateManifestFile {
                digest: "sha256:abc".to_string(),
                size: Some(1024),
                ..Default::default()
            },
        );
        let err = m
            .validate_filenames()
            .expect_err(&format!("expected reject for {name:?}"));
        assert_eq!(
            err.reason, *expected_reason,
            "wrong reason for {name:?}: got {err:?}"
        );
        assert_eq!(err.name, *name);
    }
}

#[test]
fn validate_filenames_rejects_chained_zst_suffix() {
    let mut m = TemplateManifest {
        bootable_artifact: "rootfs.ext4".to_string(),
        ..Default::default()
    };
    m.files.insert(
        "rootfs.ext4.zst.zst".to_string(),
        TemplateManifestFile {
            digest: "sha256:abc".to_string(),
            size: Some(1024),
            compression: Some(TemplateManifestCompression {
                codec: ZSTD_CODEC.to_string(),
                level: 3,
            }),
        },
    );
    let err = m
        .validate_filenames()
        .expect_err("chained .zst.zst must be rejected");
    assert_eq!(err.reason, "chained_zst");
}

#[test]
fn validate_filenames_also_checks_bootable_artifact() {
    let m = TemplateManifest {
        bootable_artifact: "../escape".to_string(),
        ..Default::default()
    };
    let err = m
        .validate_filenames()
        .expect_err("unsafe bootable_artifact must be rejected");
    assert_eq!(err.reason, "path_separator");
    assert_eq!(err.name, "../escape");
}
