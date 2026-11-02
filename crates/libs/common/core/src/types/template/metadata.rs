//! Typed `metadata.json` shape for template builds.
//!
//! The metadata document is the "blueprint" companion to
//! [`super::TemplateManifest`]'s "table of contents". Where the
//! manifest describes *what's in this build* (artifact digests, sizes,
//! compression, bootable file), the metadata describes *how the build
//! was constructed* — its base image, recipe, sizing defaults, the
//! version of `spaced` and execution environment that built it. It is written once by the build
//! pipeline and never re-normalized; downstream code reads it for
//! runtime defaults (vcpu / memory_mib / disk_size_mb), compatibility
//! checks, and operator-facing lineage.
//!
//! # On-disk shape
//!
//! Today there are two writers — build-manager's recipe builds and the
//! seed `build.sh`. They produce a permissive superset; this struct is
//! that superset, with every field `#[serde(default)]` so a doc from
//! either source parses cleanly.
//!
//! ```json
//! {
//!   "id": "seed",
//!   "name": "seed",
//!   "description": "...",
//!   "base": "scratch",
//!   "base_resolved": "sha256:...",
//!   "built_at": "2026-05-19T12:34:56Z",
//!   "spaced_version": "0.1.0",
//!   "spaced_compatibility_family": "0.1",
//!   "schema_version": 2,
//!   "execution": {
//!     "runtime": { "kind": "firecracker", "version": "1.15.1" },
//!     "guest": { "os": "linux", "architecture": "aarch64" },
//!     "state_format": { "format": "firecracker-snapshot", "version": "9.0.0" },
//!     "components": []
//!   },
//!   "recipe": "seed",
//!   "steps_count": 0,
//!   "arch": "aarch64",
//!   "vcpu": 2,
//!   "memory_mib": 1024,
//!   "disk_size_mb": 4096,
//!   "balloon": { "mode": "report" }
//! }
//! ```
//!
//! # Wire format
//!
//! Preserves the pre-typing build-manager-local `TemplateMetadata` wire shape while adding
//! an independently versioned execution metadata document:
//!
//! - `id`, `name`, `base`, `built_at`, `recipe`, `steps_count` always
//!   serialize (no `skip_serializing_if` — matches the old struct).
//! - `description` always serializes (writes `null` when `None` —
//!   matches the old struct, which lacked `skip_serializing_if` on
//!   this field even though the others around it had it).
//! - `base_resolved`, `spaced_version`, `spaced_compatibility_family`,
//!   `disk_size_mb` skip when `None` — matches the old struct.
//! - Missing `schema_version` means the legacy version-1 shape. Version 2 requires
//!   unbound `execution`; version 3 additionally requires its environment reference.
//!   Unsupported or partial versioned documents are rejected by
//!   [`TemplateMetadata::execution_metadata`].
//! - `arch` is a new field added by the typing pass (build-mgr never
//!   wrote it; the seed builder did). It skips when `None` so
//!   recipe-build output remains unchanged.
//! - `vcpu` and `memory_mib` move from non-`Option` to `Option` for
//!   read-side permissiveness (legacy seed metadata.json may omit
//!   them). They skip when `None`. The recipe-build writer always
//!   wraps `Some(...)`, so wire output is unchanged.
//! - `balloon` is optional and skips when `None`; see
//!   [`crate::types::BalloonConfig`] for its strictly validated shape.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Unbound template `metadata.json` schema version.
pub const TEMPLATE_METADATA_SCHEMA_VERSION: u32 = 2;
/// Environment-bound documents require readers that preserve the binding.
pub const BOUND_TEMPLATE_METADATA_SCHEMA_VERSION: u32 = 3;
/// Non-default image handling requires readers that understand the guest contract.
pub const GUEST_TEMPLATE_METADATA_SCHEMA_VERSION: u32 = 4;

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum TemplateMetadataSchemaError {
    #[error("unsupported template metadata schema version {0}")]
    UnsupportedVersion(u32),
    #[error("versioned template metadata requires execution metadata")]
    MissingExecution,
    #[error("legacy template metadata must not contain versioned execution metadata")]
    LegacyExecution,
    #[error("template schema and execution environment binding disagree")]
    EnvironmentBinding,
    #[error(
        "guest image contract must match schema version 4, a bound environment and its disk requirements"
    )]
    GuestImageContract,
}

/// Typed view of `metadata.json` carried with every template build.
///
/// All fields are optional on the wire (`#[serde(default)]`) because
/// different producers populate different subsets:
/// - Build-manager (recipe builds) writes `id`, `name`, `base`,
///   `built_at`, `recipe`, `steps_count`, `vcpu`, `memory_mib`,
///   `disk_size_mb`, and the optional `description`, `base_resolved`,
///   `spaced_version`, `spaced_compatibility_family`, and runtime provenance.
/// - The seed `build.sh` writes `arch`, `built_at`, and runtime provenance (and a few
///   others depending on the recipe).
///
/// Consumers should treat any field they need as potentially absent —
/// see [`TemplateMetadata::runtime_defaults`] for the canonical pattern.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TemplateMetadata {
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

    /// Logical template identifier (matches the directory under
    /// `/space/cache/templates/`).
    #[cfg_attr(feature = "serde", serde(default))]
    pub id: String,

    /// Human-facing template name. Often equal to `id` in current
    /// build flows; kept separate so a future rename doesn't churn
    /// the durable id.
    #[cfg_attr(feature = "serde", serde(default))]
    pub name: String,

    /// Optional operator-facing description from the recipe.
    ///
    /// Note: deliberately does *not* carry `skip_serializing_if` —
    /// the pre-typing build-mgr struct serialized `None` as JSON
    /// `null`, and downstream JSON consumers such as
    /// `grpc_core::v["description"].as_str().unwrap_or("")` read
    /// `null` and absent identically. Preserved for wire parity.
    #[cfg_attr(feature = "serde", serde(default))]
    pub description: Option<String>,

    /// Base template reference *as written in the recipe*
    /// (e.g. `"base:latest"`). Not necessarily the resolved digest —
    /// see [`Self::base_resolved`] for that.
    #[cfg_attr(feature = "serde", serde(default))]
    pub base: String,

    /// Resolved digest of the base template at build time
    /// (e.g. `"sha256:..."`), recorded so a future rebuild can verify
    /// it descended from the same parent.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub base_resolved: Option<String>,

    /// Build completion timestamp (ISO-8601 UTC). Propagated into the
    /// manifest's `build_date` and the DB row's `build_date` column.
    #[cfg_attr(feature = "serde", serde(default))]
    pub built_at: String,

    /// Version of `spaced` that produced this build (e.g. crate
    /// version). Used by compatibility checks at restore time.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub spaced_version: Option<String>,

    /// Compatibility family (e.g. `"0.1"`) — coarser than
    /// `spaced_version`, lets restore accept patch-level differences.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub spaced_compatibility_family: Option<String>,

    /// Recipe identifier that produced this build (for builder-flow
    /// builds; empty for seed/imported templates).
    #[cfg_attr(feature = "serde", serde(default))]
    pub recipe: String,

    /// Number of recipe steps executed. Zero for non-recipe builds.
    #[cfg_attr(feature = "serde", serde(default))]
    pub steps_count: usize,

    /// CPU architecture identifier (`"aarch64"`, `"x86_64"`). Seed
    /// builds write this; recipe builds may not. Consumers fall back
    /// to the manifest's `arch` and ultimately to `"aarch64"`. New
    /// field in the typed view — build-mgr never wrote it, so
    /// `skip_serializing_if` keeps recipe-build output unchanged.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub arch: Option<String>,

    /// Default vCPU count for sandboxes created from this template.
    /// `None` (or `Some(0)`) is treated as "no default" by readers.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub vcpu: Option<u8>,

    /// Default memory (MiB). `None` / `Some(0)` is treated as
    /// "no default".
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub memory_mib: Option<u32>,

    /// Sized rootfs disk capacity (MiB). Recipe builds use this to
    /// expand the rootfs to a sandbox-runtime size; seed templates
    /// may omit it. `None` / `Some(0)` is treated as "no default".
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub disk_size_mb: Option<u32>,

    /// Opt-in virtio balloon device for VMs that cold boot from this
    /// template. Absent means no balloon device. Snapshots keep the device
    /// they were taken with, so a change applies after the template's base
    /// snapshot is rebuilt. Skips when `None` so existing documents are
    /// unchanged.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub balloon: Option<crate::types::BalloonConfig>,
}

impl TemplateMetadata {
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
            GUEST_TEMPLATE_METADATA_SCHEMA_VERSION
        } else if self
            .execution
            .as_ref()
            .is_some_and(|execution| execution.environment().is_some())
        {
            BOUND_TEMPLATE_METADATA_SCHEMA_VERSION
        } else {
            TEMPLATE_METADATA_SCHEMA_VERSION
        });
    }

    pub fn execution_metadata(
        &self,
    ) -> Result<Option<&crate::types::ExecutionMetadata>, TemplateMetadataSchemaError> {
        if self.guest_image.is_some()
            != (self.schema_version == Some(GUEST_TEMPLATE_METADATA_SCHEMA_VERSION))
        {
            return Err(TemplateMetadataSchemaError::GuestImageContract);
        }
        match (self.schema_version, self.execution.as_ref()) {
            (None, None) => Ok(None),
            (None, Some(_)) => Err(TemplateMetadataSchemaError::LegacyExecution),
            (Some(version), Some(execution))
                if version == TEMPLATE_METADATA_SCHEMA_VERSION
                    || version == BOUND_TEMPLATE_METADATA_SCHEMA_VERSION
                    || version == GUEST_TEMPLATE_METADATA_SCHEMA_VERSION =>
            {
                if execution.environment().is_some()
                    != (version == BOUND_TEMPLATE_METADATA_SCHEMA_VERSION
                        || version == GUEST_TEMPLATE_METADATA_SCHEMA_VERSION)
                {
                    return Err(TemplateMetadataSchemaError::EnvironmentBinding);
                }
                Ok(Some(execution))
            }
            (
                Some(
                    TEMPLATE_METADATA_SCHEMA_VERSION
                    | BOUND_TEMPLATE_METADATA_SCHEMA_VERSION
                    | GUEST_TEMPLATE_METADATA_SCHEMA_VERSION,
                ),
                None,
            ) => Err(TemplateMetadataSchemaError::MissingExecution),
            (Some(version), _) => Err(TemplateMetadataSchemaError::UnsupportedVersion(version)),
        }
    }

    /// Filter `Option<u32>` / `Option<u8>` fields to drop `Some(0)`
    /// values — both build-manager and the seed builder occasionally
    /// emit zeros that mean "no default" rather than "default of
    /// zero". Returns a `(vcpu, memory_mib, disk_size_mb)` tuple of
    /// `u32` values where every `Some` is strictly positive.
    pub fn runtime_defaults(&self) -> (Option<u32>, Option<u32>, Option<u32>) {
        let positive_u32 = |v: Option<u32>| v.filter(|x| *x > 0);
        let vcpu = self.vcpu.filter(|v| *v > 0).map(u32::from);
        let memory_mib = positive_u32(self.memory_mib);
        let disk_size_mb = positive_u32(self.disk_size_mb);
        (vcpu, memory_mib, disk_size_mb)
    }
}

#[cfg(all(test, feature = "serde"))]
mod tests;
