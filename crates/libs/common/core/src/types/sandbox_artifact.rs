//! Durable metadata stored beside a sandbox's node-local files.

use super::{ContentHash, ExecutionMetadata, MemorySize, SandboxId, TemplateName, VcpuCount};
use pplx_space_execution_spec::guest_image::GuestImageContract;
use serde::{Deserialize, Deserializer, Serialize, de};

/// Current sandbox `metadata.json` schema version.
pub const SANDBOX_ARTIFACT_METADATA_VERSION: u32 = 2;
const GUEST_ARTIFACT_METADATA_VERSION: u32 = 4;

/// Metadata stored at the sandbox cache root beside `instance/` and `snapshots/`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct SandboxArtifactMetadata {
    schema_version: u32,
    pub sandbox_id: SandboxId,
    pub execution: ExecutionMetadata,
    pub template: TemplateName,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<ContentHash>,
    /// Admitted CPU allocation, preserved across snapshots and restores.
    /// Absence on older artifacts does not establish a default allocation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_budget: Option<VcpuCount>,
    /// Admitted guest RAM, independent of the host cgroup overhead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guest_memory: Option<MemorySize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    guest_image: Option<GuestImageContract>,
}

impl SandboxArtifactMetadata {
    pub fn new(
        sandbox_id: SandboxId,
        execution: ExecutionMetadata,
        template: TemplateName,
        content_hash: Option<ContentHash>,
    ) -> Self {
        Self {
            schema_version: SANDBOX_ARTIFACT_METADATA_VERSION,
            sandbox_id,
            execution,
            template,
            content_hash,
            cpu_budget: None,
            guest_memory: None,
            guest_image: None,
        }
    }

    pub fn with_cpu_budget(mut self, cpu_budget: Option<VcpuCount>) -> Self {
        self.cpu_budget = cpu_budget;
        self
    }

    pub fn with_guest_memory(mut self, memory: Option<MemorySize>) -> Self {
        self.guest_memory = memory;
        self
    }

    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }
    pub fn with_guest_image(mut self, image: GuestImageContract) -> Self {
        self.schema_version = GUEST_ARTIFACT_METADATA_VERSION;
        self.guest_image = Some(image);
        self
    }

    pub fn guest_image(&self) -> GuestImageContract {
        self.guest_image.unwrap_or_default()
    }
}

#[derive(Deserialize)]
struct SandboxArtifactMetadataWire {
    schema_version: u32,
    sandbox_id: SandboxId,
    execution: ExecutionMetadata,
    template: TemplateName,
    #[serde(default)]
    content_hash: Option<ContentHash>,
    #[serde(default)]
    cpu_budget: Option<VcpuCount>,
    #[serde(default)]
    guest_memory: Option<MemorySize>,
    #[serde(default)]
    guest_image: Option<GuestImageContract>,
    #[serde(flatten)]
    unknown: std::collections::BTreeMap<String, serde::de::IgnoredAny>,
}

impl<'de> Deserialize<'de> for SandboxArtifactMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SandboxArtifactMetadataWire::deserialize(deserializer)?;
        let supported = match wire.schema_version {
            SANDBOX_ARTIFACT_METADATA_VERSION => wire.guest_image.is_none(),
            GUEST_ARTIFACT_METADATA_VERSION => wire.unknown.is_empty(),
            _ => false,
        };
        if !supported {
            return Err(de::Error::custom(format!(
                "unsupported sandbox metadata schema version {}",
                wire.schema_version
            )));
        }
        Ok(Self {
            schema_version: wire.schema_version,
            sandbox_id: wire.sandbox_id,
            execution: wire.execution,
            template: wire.template,
            content_hash: wire.content_hash,
            cpu_budget: wire.cpu_budget,
            guest_memory: wire.guest_memory,
            guest_image: wire.guest_image,
        })
    }
}

#[cfg(test)]
mod tests;
