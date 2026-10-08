use pplx_space_common_core::types::SandboxId;

use super::*;

pub(super) fn snapshot_type_from_proto(value: i32) -> Result<crate::proto::SnapshotType, Status> {
    use crate::proto::SnapshotType;
    let snapshot_type = SnapshotType::try_from(value)
        .map_err(|_| Status::invalid_argument(format!("unknown snapshot type value {value}")))?;
    Ok(match snapshot_type {
        SnapshotType::Unspecified | SnapshotType::Full => SnapshotType::Full,
        SnapshotType::Disk => SnapshotType::Disk,
    })
}

pub(super) fn optional_positive_or_default(
    field: &'static str,
    requested: Option<u32>,
    default: u32,
) -> Result<u32, Status> {
    match requested {
        Some(0) => Err(Status::invalid_argument(format!(
            "{field} must be greater than 0 when set"
        ))),
        Some(value) => Ok(value),
        None => Ok(default),
    }
}

pub(super) fn canonical_batched_sandbox_ids(ids: &[String]) -> Result<Vec<String>, Status> {
    use pplx_space_common_core::inventory::{InventoryBudget, MAX_ENCODED_BYTES, MAX_ITEMS};
    let mut budget = InventoryBudget::new(MAX_ITEMS, MAX_ENCODED_BYTES);
    for id in ids {
        budget
            .charge(id.len())
            .map_err(crate::inventory_budget::exceeded)?;
    }
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for id in ids.iter().map(|id| id.trim()).filter(|id| !id.is_empty()) {
        let parsed = id.parse::<SandboxId>().map_err(|_| {
            Status::invalid_argument(format!(
                "sandbox_ids must contain canonical sandbox UUIDs; invalid id '{id}'"
            ))
        })?;
        let canonical = parsed.to_string();
        if id != canonical {
            return Err(Status::invalid_argument(format!(
                "sandbox_ids must contain canonical sandbox UUIDs; got non-canonical id '{id}'"
            )));
        }
        if seen.insert(canonical.clone()) {
            out.push(canonical);
        }
    }
    Ok(out)
}

pub(super) fn duration_ceil_secs(duration: Duration) -> u64 {
    duration
        .as_secs()
        .saturating_add(u64::from(duration.subsec_nanos() > 0))
}

pub(super) fn normalize_arch(arch: &str) -> &str {
    match arch {
        "amd64" => "x86_64",
        "arm64" => "aarch64",
        other => other,
    }
}

pub(super) fn reject_incompatible_template_arch(
    template: &str,
    content_hash: &str,
    template_arch: &str,
) -> Result<(), tonic::Status> {
    let node_arch = normalize_arch(std::env::consts::ARCH);
    let template_arch = normalize_arch(template_arch);
    if template_arch == node_arch {
        return Ok(());
    }

    tracing::error!(
        template = %template,
        content_hash = %content_hash,
        template_arch = %template_arch,
        node_arch = %node_arch,
        "rejecting sandbox create from template built for different architecture"
    );
    Err(tonic::Status::failed_precondition(format!(
        "template '{template}' ({content_hash}) was built for {template_arch}, but this node is {node_arch}"
    )))
}

pub(super) fn template_metadata_arch(template_dir: &str) -> Option<String> {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let metadata_json = std::fs::read_to_string(metadata_path).ok()?;
    let metadata: serde_json::Value = serde_json::from_str(&metadata_json).ok()?;
    metadata
        .get("arch")
        .and_then(serde_json::Value::as_str)
        .map(|arch| normalize_arch(arch).to_string())
}

pub(super) fn template_execution_metadata(
    template_dir: &str,
    fallback_arch: &str,
) -> Result<pplx_space_common_core::types::ExecutionMetadata, Status> {
    use pplx_space_common_core::types::{Arch, ExecutionMetadata, TemplateMetadata};

    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let metadata = match std::fs::read(&metadata_path) {
        Ok(bytes) => serde_json::from_slice::<TemplateMetadata>(&bytes).map_err(|error| {
            Status::failed_precondition(format!(
                "parse template execution metadata {}: {error}",
                metadata_path.display()
            ))
        })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => TemplateMetadata::default(),
        Err(error) => {
            return Err(Status::internal(format!(
                "read template execution metadata {}: {error}",
                metadata_path.display()
            )));
        }
    };
    match metadata.execution_metadata().map_err(|error| {
        Status::failed_precondition(format!(
            "invalid template execution metadata {}: {error}",
            metadata_path.display()
        ))
    })? {
        Some(execution) => Ok(execution.clone()),
        None => {
            let architecture = normalize_arch(metadata.arch.as_deref().unwrap_or(fallback_arch))
                .parse::<Arch>()
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            Ok(ExecutionMetadata::initial_firecracker(architecture, vec![]))
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct TemplateRuntimeDefaults {
    pub(super) vcpu: Option<u32>,
    pub(super) memory_mib: Option<u32>,
    pub(super) balloon: TemplateBalloon,
}

/// The template's opt-in balloon device. Unlike the sizing defaults, a
/// present but invalid setting is kept so a cold boot can reject it rather
/// than silently boot without the device the template asked for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) enum TemplateBalloon {
    #[default]
    Absent,
    Configured(pplx_space_common_core::types::BalloonConfig),
    Invalid(String),
}

impl TemplateBalloon {
    fn from_metadata(value: Option<serde_json::Value>) -> Self {
        match value {
            None => Self::Absent,
            Some(value) => match serde_json::from_value(value) {
                Ok(config) => Self::Configured(config),
                Err(error) => Self::Invalid(error.to_string()),
            },
        }
    }

    /// The device a cold boot with `memory_mib` of guest memory renders.
    pub(super) fn for_memory_mib(
        &self,
        memory_mib: u32,
    ) -> Result<Option<pplx_space_common_core::types::BalloonConfig>, Status> {
        match self {
            Self::Absent => Ok(None),
            Self::Configured(config) => config
                .validate_for_memory_mib(memory_mib)
                .map(|()| Some(*config))
                .map_err(|error| {
                    Status::failed_precondition(format!("template metadata balloon: {error}"))
                }),
            Self::Invalid(error) => Err(Status::failed_precondition(format!(
                "template metadata balloon: {error}"
            ))),
        }
    }
}

#[derive(Debug, Default, serde::Deserialize)]
struct TemplateMetadataReader {
    vcpu: Option<u8>,
    memory_mib: Option<u32>,
    balloon: Option<serde_json::Value>,
}

impl TemplateMetadataReader {
    fn runtime_defaults(self) -> TemplateRuntimeDefaults {
        let positive = |value: Option<u32>| value.filter(|value| *value > 0);
        TemplateRuntimeDefaults {
            vcpu: self.vcpu.filter(|value| *value > 0).map(u32::from),
            memory_mib: positive(self.memory_mib),
            balloon: TemplateBalloon::from_metadata(self.balloon),
        }
    }
}

pub(super) fn template_runtime_defaults(template_dir: &str) -> TemplateRuntimeDefaults {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let Ok(metadata_json) = std::fs::read_to_string(metadata_path) else {
        return TemplateRuntimeDefaults::default();
    };
    let Ok(metadata) = serde_json::from_str::<TemplateMetadataReader>(&metadata_json) else {
        return TemplateRuntimeDefaults::default();
    };
    metadata.runtime_defaults()
}

#[derive(Debug, Default)]
pub(super) struct TemplateSpacedCompatibility {
    pub(super) version: Option<String>,
    pub(super) compatibility_family: Option<String>,
}

pub(super) fn template_metadata_spaced_compatibility(
    template_dir: &str,
) -> TemplateSpacedCompatibility {
    let metadata_path = std::path::Path::new(template_dir).join("metadata.json");
    let Ok(metadata_json) = std::fs::read_to_string(metadata_path) else {
        return TemplateSpacedCompatibility::default();
    };
    let Ok(metadata) = serde_json::from_str::<serde_json::Value>(&metadata_json) else {
        return TemplateSpacedCompatibility::default();
    };
    TemplateSpacedCompatibility {
        version: metadata
            .get("spaced_version")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
        compatibility_family: metadata
            .get("spaced_compatibility_family")
            .and_then(serde_json::Value::as_str)
            .map(ToOwned::to_owned),
    }
}

pub(super) fn reject_incompatible_template_spaced_compatibility(
    template: &str,
    content_hash: &str,
    template_version: Option<&str>,
    template_compatibility_family: Option<&str>,
) -> Result<(), tonic::Status> {
    pplx_space_spaced_common::validate_spaced_guest_compatibility(
        template_version,
        template_compatibility_family,
    )
    .map_err(|error| {
        tracing::error!(
            template,
            content_hash,
            %error,
            "rejecting template with unsupported spaced guest identity"
        );
        tonic::Status::failed_precondition(format!(
            "template '{template}' ({content_hash}): {error}"
        ))
    })
}
