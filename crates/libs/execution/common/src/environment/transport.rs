//! Translate transport representations without changing persisted identities.
use super::*;

pub const FIRECRACKER_BOOT_CONTRACT: &str = "firecracker-linux-direct-v1";
pub const FIRECRACKER_MACHINE_CONTRACT: &str = "firecracker-machine-v1";

impl EnvironmentDefinition {
    /// Normalize the historical representation once at the catalog/codec boundary.
    pub fn into_runtime_neutral(mut self) -> Result<Self, EnvironmentDecodeError> {
        let spec = self.descriptor.spec();
        let BootProfile::LinuxDirectV1 { kernel } = &spec.boot else {
            return Ok(self);
        };
        let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
            &spec.compatibility
        else {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        };
        let mut generic = spec.clone();
        let RuntimeBinding::ManagedRelease(release) = &mut generic.runtime;
        release.layout = RuntimePayloadLayout::CatalogReleaseV1;
        generic.boot = BootProfile::ArtifactsV1 {
            contract: FIRECRACKER_BOOT_CONTRACT.parse()?,
            artifacts: vec![BootArtifact::new(
                "boot".parse()?,
                kernel.clone(),
                "vmlinux".parse()?,
            )],
        };
        generic.compatibility = ExecutionCompatibilityProfile::RuntimeV1 {
            contract: FIRECRACKER_MACHINE_CONTRACT.parse()?,
            state_format: Some(state_format.clone()),
        };
        self.legacy_descriptor = Some(self.descriptor);
        self.descriptor = generic.try_into()?;
        Ok(self)
    }

    pub fn identity_schema_version(&self) -> u32 {
        if self.legacy_descriptor.is_some() {
            ENVIRONMENT_SCHEMA_VERSION
        } else {
            self.schema_version()
        }
    }

    /// Historical canonical bytes remain necessary while persisted references exist.
    pub fn canonical_identity(&self) -> String {
        canonical_descriptor(self.legacy_descriptor.as_ref().unwrap_or(&self.descriptor))
    }

    /// Serve old readers only when this identity has an exact old representation.
    pub fn for_peer(
        &self,
        supported_schema: u32,
    ) -> Result<proto::EnvironmentDefinition, EnvironmentDecodeError> {
        if supported_schema >= RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION {
            return Ok(self.into());
        }
        if self.schema_version() == ENVIRONMENT_SCHEMA_VERSION {
            return Ok(self.into());
        }
        let legacy = self
            .legacy_descriptor
            .as_ref()
            .ok_or(EnvironmentDecodeError::UnsupportedPeer)?;
        Ok((&Self::new(legacy.clone())).into())
    }

    pub(super) fn from_representation(
        descriptor: EnvironmentDescriptor,
        schema: u32,
        identity: u32,
    ) -> Result<Self, EnvironmentDecodeError> {
        let definition = Self::new(descriptor).check_schema(schema)?;
        if identity == schema {
            return Ok(definition);
        }
        if schema != RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
            || identity != ENVIRONMENT_SCHEMA_VERSION
        {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        }
        let spec = definition.descriptor.spec();
        let BootProfile::ArtifactsV1 {
            contract,
            artifacts,
        } = &spec.boot
        else {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        };
        let [artifact] = artifacts.as_slice() else {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        };
        let ExecutionCompatibilityProfile::RuntimeV1 {
            contract: machine,
            state_format: Some(state),
        } = &spec.compatibility
        else {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        };
        if contract.as_str() != FIRECRACKER_BOOT_CONTRACT
            || machine.as_str() != FIRECRACKER_MACHINE_CONTRACT
            || artifact.role().as_str() != "boot"
            || artifact.format().as_str() != "vmlinux"
        {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        }
        let mut legacy = spec.clone();
        let RuntimeBinding::ManagedRelease(release) = &mut legacy.runtime;
        release.layout = RuntimePayloadLayout::FirecrackerReleaseV1;
        legacy.boot = BootProfile::LinuxDirectV1 {
            kernel: artifact.component().clone(),
        };
        legacy.compatibility = ExecutionCompatibilityProfile::FirecrackerSnapshotV1 {
            state_format: state.clone(),
        };
        // The legacy validator also rejects non-Firecracker, non-Linux and
        // mismatched architectures. No generic requirement may be dropped.
        let normalized = Self::new(legacy.try_into()?).into_runtime_neutral()?;
        if normalized.descriptor != definition.descriptor {
            return Err(EnvironmentDecodeError::IdentityEncoding);
        }
        Ok(normalized)
    }
}

#[cfg(test)]
mod tests;
