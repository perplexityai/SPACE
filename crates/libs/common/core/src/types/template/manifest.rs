//! Typed `manifest.json` shape for template builds.
//!
//! The manifest is the artifact-side description of a template build —
//! the "table of contents" companion to [`super::TemplateMetadata`]'s
//! "blueprint". It travels with the build (on disk in the digest dir,
//! in S3 alongside the artifacts, and as the `manifest_json` column in
//! template-manager's SQLite), and downstream code uses it to answer
//! "what's in this build, where's the bootable image, what are the
//! digests of the files that ship with it."
//!
//! # On-disk shape
//!
//! Originally produced by the template builder (today: build-manager
//! and the seed `build.sh`), then re-normalized by template-manager's
//! `normalize_template_manifest` after the rootfs is decompressed
//! locally so the in-process and on-disk views agree.
//!
//! ```json
//! {
//!   "name": "seed",
//!   "content_hash": "sha256:...",
//!   "build_date": "2026-05-19T12:34:56Z",
//!   "runtime": "fc",
//!   "arch": "aarch64",
//!   "config": {},
//!   "bootable_artifact": "rootfs.ext4",
//!   "files": {
//!     "rootfs.ext4":     { "digest": "sha256:...", "size": 1073741824 },
//!     "rootfs.ext4.zst": {
//!       "digest": "sha256:...",
//!       "size": 200000000,
//!       "compression": { "codec": "zstd", "level": 3 }
//!     }
//!   }
//! }
//! ```
//!
//! All fields are `#[serde(default)]` so an older or partial manifest
//! still parses; the writer normalizes them before commit. The map
//! storing `files` is a [`BTreeMap`] so the serialized order is
//! deterministic — useful for hashing the manifest, diffing across
//! builds, and snapshot tests.
//!
//! `config` is intentionally opaque (`serde_json::Value`) — it carries
//! template-specific runtime config that today's template-manager
//! never inspects. If a future caller needs to look inside, give it its
//! own typed view rather than reaching through this struct.
//!
//! # Wire format
//!
//! Deliberately matches the pre-typing `serde_json::Value` output bit
//! for bit. No fields are `skip_serializing_if`'d on String emptiness,
//! and `TemplateManifestFile::size` is `Option<u64>` so a missing
//! `size` key in the input survives the round-trip as `None` (and re-
//! serializes as absent) instead of collapsing to a written `0`.

use std::collections::BTreeMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Unbound template `manifest.json` schema version.
pub const TEMPLATE_MANIFEST_SCHEMA_VERSION: u32 = 2;
/// Environment-bound documents require readers that preserve the binding.
pub const BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION: u32 = 3;
/// Non-default image handling requires readers that understand the guest contract.
pub const GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION: u32 = 4;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TemplateManifestSchemaError {
    #[error("unsupported template manifest schema version {0}")]
    UnsupportedVersion(u32),
    #[error("versioned template manifest requires execution metadata")]
    MissingExecution,
    #[error("legacy template manifest must not contain versioned execution metadata")]
    LegacyExecution,
    #[error("template schema and execution environment binding disagree")]
    EnvironmentBinding,
    #[error(
        "guest image contract must match schema version 4, a bound environment and its disk requirements"
    )]
    GuestImageContract,
}

/// Typed view of `manifest.json` carried with every template build.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateManifest {
    /// Absent for the legacy version-1 shape.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub schema_version: Option<u32>,

    /// Exact execution environment selected for this template build.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub execution: Option<crate::types::ExecutionMetadata>,

    /// Omitted for legacy Linux images. Version 4 prevents old readers from
    /// applying ext4 tools or waiting for an absent guest agent.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub guest_image: Option<pplx_space_execution_spec::guest_image::GuestImageContract>,

    /// Template name (matches the directory under `/space/cache/templates/`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: String,

    /// Content-addressed digest of the build (matches the digest dir name).
    /// Empty when the manifest was just written by the upstream builder
    /// and template-manager hasn't normalized it yet.
    #[cfg_attr(feature = "serde", serde(default))]
    pub content_hash: String,

    /// Timestamp the build was completed (ISO-8601). Sourced from the
    /// `metadata.json`'s `built_at` during build, preserved through
    /// normalize.
    #[cfg_attr(feature = "serde", serde(default))]
    pub build_date: String,

    /// VM runtime kind alias (`"fc"` for Firecracker today). Defaults
    /// to `"fc"` if absent on the wire — see
    /// `pplx_space_common_core::types::RUNTIME_FIRECRACKER_ALIAS`.
    #[cfg_attr(feature = "serde", serde(default))]
    pub runtime: String,

    /// CPU architecture identifier (`"aarch64"`, `"x86_64"`). Empty
    /// when the producer didn't write it; consumers fall back to
    /// metadata's `arch` and ultimately to `"aarch64"` (see the
    /// `template_arch` helper in template-manager).
    #[cfg_attr(feature = "serde", serde(default))]
    pub arch: String,

    /// Template-specific runtime configuration carried opaquely. Today
    /// template-manager never inspects this; it's serialized through
    /// to consumers (e.g. sandbox creation) that have their own
    /// strongly-typed view of what goes in here.
    #[cfg_attr(feature = "serde", serde(default = "default_config"))]
    pub config: serde_json::Value,

    /// Relative filename of the file that should be booted as the VM
    /// rootfs. In practice always `"rootfs.ext4"` today.
    #[cfg_attr(feature = "serde", serde(default))]
    pub bootable_artifact: String,

    /// Files that travel with the build, keyed by filename relative to
    /// the digest dir. `BTreeMap` keeps the serialized order stable so
    /// the manifest is bit-identical across writers that use the same
    /// inputs.
    #[cfg_attr(feature = "serde", serde(default))]
    pub files: BTreeMap<String, TemplateManifestFile>,
}

/// Per-file entry inside [`TemplateManifest::files`].
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateManifestFile {
    /// Content-addressed digest of the file's bytes as they sit on
    /// disk (post-decompress for the rootfs; raw bytes for the
    /// compressed sibling).
    #[cfg_attr(feature = "serde", serde(default))]
    pub digest: String,

    /// Size in bytes as the file sits on disk. `Option<u64>` so the
    /// pre-typing distinction between "size is missing" (`None`) and
    /// "size is zero" (`Some(0)`) survives the round-trip — some
    /// readers fall back to `fs::metadata().len()` for the former
    /// while accepting the latter literally.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub size: Option<u64>,

    /// Compression descriptor for sibling artifacts that ship in a
    /// compressed form (e.g. `rootfs.ext4.zst`). `None` when the file
    /// is stored as-is.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub compression: Option<TemplateManifestCompression>,
}

/// Compression metadata for a manifest file entry.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateManifestCompression {
    /// Codec name (`"zstd"` today).
    pub codec: String,

    /// Codec-specific compression level (zstd: 1-22). Type is `i32`
    /// to match the zstd crate's `i32` parameter; producers typically
    /// write a small positive integer.
    pub level: i32,
}

/// Filename suffix that indicates a zstd-compressed sibling artifact.
/// Producers append this to the uncompressed name (`rootfs.ext4` →
/// `rootfs.ext4.zst`) so consumers can identify the pair without
/// reading the compression descriptor.
pub const ZSTD_FILENAME_SUFFIX: &str = ".zst";

/// Wire-level codec name for zstd-compressed manifest entries.
pub const ZSTD_CODEC: &str = "zstd";

impl TemplateManifestFile {
    /// `true` when this entry is described as a zstd-compressed
    /// artifact with a well-formed compression descriptor: codec
    /// `"zstd"`, level set, digest and size present. Defends against
    /// partial wire shapes where one of the fields was dropped — the
    /// pre-typing check in template-manager validated all three.
    pub fn is_zstd_compressed(&self) -> bool {
        if self.digest.is_empty() || self.size.is_none() {
            return false;
        }
        self.compression
            .as_ref()
            .is_some_and(|c| c.codec == ZSTD_CODEC && c.level > 0)
    }
}

impl TemplateManifest {
    pub fn set_execution_metadata(&mut self, execution: crate::types::ExecutionMetadata) {
        self.execution = Some(execution);
        self.refresh_schema_version();
    }
    pub fn set_guest_image(
        &mut self,
        image: Option<pplx_space_execution_spec::guest_image::GuestImageContract>,
    ) {
        self.guest_image = image;
        self.refresh_schema_version();
    }
    fn refresh_schema_version(&mut self) {
        self.schema_version = Some(if self.guest_image.is_some() {
            GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION
        } else if self
            .execution
            .as_ref()
            .is_some_and(|execution| execution.environment().is_some())
        {
            BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION
        } else {
            TEMPLATE_MANIFEST_SCHEMA_VERSION
        });
    }

    pub fn execution_metadata(
        &self,
    ) -> Result<Option<&crate::types::ExecutionMetadata>, TemplateManifestSchemaError> {
        if self.guest_image.is_some()
            != (self.schema_version == Some(GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION))
        {
            return Err(TemplateManifestSchemaError::GuestImageContract);
        }
        match (self.schema_version, self.execution.as_ref()) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(TemplateManifestSchemaError::LegacyExecution),
            (Some(version), Some(execution))
                if version == TEMPLATE_MANIFEST_SCHEMA_VERSION
                    || version == BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION
                    || version == GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION =>
            {
                if execution.environment().is_some()
                    != (version == BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION
                        || version == GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION)
                {
                    return Err(TemplateManifestSchemaError::EnvironmentBinding);
                }
                Ok(Some(execution))
            }
            (
                Some(
                    TEMPLATE_MANIFEST_SCHEMA_VERSION
                    | BOUND_TEMPLATE_MANIFEST_SCHEMA_VERSION
                    | GUEST_TEMPLATE_MANIFEST_SCHEMA_VERSION,
                ),
                None,
            ) => Err(TemplateManifestSchemaError::MissingExecution),
            (Some(version), _) => Err(TemplateManifestSchemaError::UnsupportedVersion(version)),
        }
    }

    /// The compressed sibling for the bootable artifact, when one is
    /// listed and well-formed. Returns `None` for legacy single-file
    /// builds that only ship `rootfs.ext4`.
    ///
    /// The key is constructed as
    /// `format!("{bootable_artifact}{ZSTD_FILENAME_SUFFIX}")` so this
    /// follows whatever the manifest declared as its bootable artifact
    /// rather than hard-coding `rootfs.ext4.zst`.
    pub fn compressed_rootfs(&self) -> Option<&TemplateManifestFile> {
        if self.bootable_artifact.is_empty() {
            return None;
        }
        let key = format!("{}{ZSTD_FILENAME_SUFFIX}", self.bootable_artifact);
        self.files
            .get(&key)
            .filter(|entry| entry.is_zstd_compressed())
    }

    /// Convenience: `true` when [`Self::compressed_rootfs`] returns
    /// `Some`. Used at dispatch sites that just need the boolean.
    pub fn has_compressed_rootfs(&self) -> bool {
        self.compressed_rootfs().is_some()
    }

    /// Reject manifest filenames that aren't safe to use as path
    /// components in the digest dir layout. Run this once at the parse
    /// boundary in any consumer that turns `manifest.files` keys (and
    /// [`Self::bootable_artifact`]) into filesystem paths — a tampered
    /// or corrupted `manifest.json` could otherwise steer writes out of
    /// the digest dir.
    ///
    /// Rejection rules per name:
    /// - empty string
    /// - contains a path separator (`/`) or NUL byte
    /// - starts with `.` (catches `.`, `..`, and hidden files)
    /// - ends in `.zst.zst` (chained zstd compression — opens an
    ///   ambiguity where a `.zst` entry could be both a download target
    ///   and a decompression target, racing on the same on-disk path)
    pub fn validate_filenames(&self) -> Result<(), TemplateManifestFilenameError> {
        if !self.bootable_artifact.is_empty() {
            validate_manifest_filename(&self.bootable_artifact)?;
        }
        for key in self.files.keys() {
            validate_manifest_filename(key)?;
        }
        Ok(())
    }
}

/// Reason a [`TemplateManifest::validate_filenames`] call rejected the
/// manifest. Carries the offending name and a short categorical
/// reason; consumers typically log the full error and translate to
/// their own error type at the call site.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error("manifest filename {name:?} is unsafe: {reason}")]
pub struct TemplateManifestFilenameError {
    pub name: String,
    pub reason: &'static str,
}

fn validate_manifest_filename(name: &str) -> Result<(), TemplateManifestFilenameError> {
    let reason = if name.is_empty() {
        "empty"
    } else if name.contains('/') {
        "path_separator"
    } else if name.contains('\0') {
        "nul_byte"
    } else if name.starts_with('.') {
        // Catches `.`, `..`, hidden files. We never legitimately need
        // any of these in a template manifest.
        "hidden_or_dot"
    } else if name.ends_with(".zst.zst") {
        // Chained zstd compression. The pull plan's dedup rule and
        // decompress-target builder both pivot on a single `.zst`
        // suffix; allowing two would let one entry be both a download
        // and a decompression target, racing on the same path.
        "chained_zst"
    } else {
        return Ok(());
    };
    Err(TemplateManifestFilenameError {
        name: name.to_string(),
        reason,
    })
}

#[cfg(feature = "serde")]
fn default_config() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

#[cfg(all(test, feature = "serde"))]
mod tests;
