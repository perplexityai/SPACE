//! Builder-side use of node-manager's verified environment, separate from guest
//! process environment variables and from downloadable artifact catalog entries.
use pplx_space_execution_common::environment::RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION as DEFINITION_SCHEMA;
use std::path::Path;

use pplx_space_common_core::types::{EnvironmentReference, ExecutionMetadata, TemplateMetadata};
use pplx_space_execution_common::{
    ExecutionProtoError,
    environment::{EnvironmentBindingError, EnvironmentDecodeError, EnvironmentDefinition},
};
use pplx_space_node_mgr_client::proto::{
    ResolveExecutionEnvironmentRequest, ResolveExecutionEnvironmentResponse,
};

#[derive(Debug, thiserror::Error)]
pub enum BuildExecutionBindingError {
    #[error(transparent)]
    Architecture(#[from] pplx_space_common_core::types::ArchParseError),
    #[error("cannot connect to environment resolver: {0}")]
    Connect(#[from] pplx_space_node_mgr_client::ConnectError),
    #[error("environment resolution failed: {0}")]
    Resolve(#[from] tonic::Status),
    #[error("environment resolution timed out: {0}")]
    Timeout(#[from] tokio::time::error::Elapsed),
    #[error("scratch builder does not implement this boot/compatibility profile")]
    ScratchProfile,
    #[error("execution binding lacks required {0}")]
    Missing(&'static str),
    #[error("invalid environment definition: {0}")]
    Definition(#[from] EnvironmentDecodeError),
    #[error("invalid execution evidence: {0}")]
    Execution(#[from] ExecutionProtoError),
    #[error("execution binding mismatch: {0}")]
    Binding(#[from] EnvironmentBindingError),
    #[error("environment resolver returned a different requested binding")]
    Reference,
    #[error("compatible environment resolution: {0}")]
    Selection(#[from] pplx_space_node_mgr_client::environment_selection::SelectionResponseError),
    #[error("scratch build requires Firecracker/Linux/{architecture}, got {runtime:?}/{guest:?}")]
    ScratchRequirements {
        architecture: pplx_space_common_core::types::Arch,
        runtime: pplx_space_common_core::types::RuntimeKind,
        guest: pplx_space_common_core::types::GuestPlatform,
    },
    #[error("recipe kernel pin differs from the resolved environment")]
    KernelPin,
    #[error("cannot read template metadata {path}: {source}")]
    Read {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("invalid template metadata JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid template metadata schema: {0}")]
    Schema(#[from] pplx_space_common_core::types::TemplateMetadataSchemaError),
}

#[derive(Debug)]
pub(super) struct BuildExecutionBinding {
    definition: EnvironmentDefinition,
    execution: ExecutionMetadata,
}

impl BuildExecutionBinding {
    pub(super) fn reference(&self) -> EnvironmentReference {
        self.definition.reference()
    }

    /// Freeze even E0 before Create; inventory changes must not cause a second
    /// selection between builder resolution and the sandbox's persisted intent.
    pub(super) fn create_environment(&self) -> EnvironmentReference {
        self.reference()
    }

    pub(super) async fn for_base(
        reference: Option<EnvironmentReference>,
        execution: &ExecutionMetadata,
    ) -> Result<Option<Self>, BuildExecutionBindingError> {
        if let Some(reference) = reference {
            let binding = Self::resolve(Some(reference)).await?;
            binding.definition.check_cold_boot_source(execution)?;
            return Ok(Some(binding));
        }
        let Some(reference) = execution.environment() else {
            return Ok(None);
        };
        let request = pplx_space_node_mgr_client::proto::resolve_execution_environment_request::Compatible::ColdBootSource(execution.into());
        let binding = Self::resolve_compatible(request, reference).await?;
        binding.definition.check_cold_boot_source(execution)?;
        Ok(Some(binding))
    }

    pub(super) async fn for_scratch(
        reference: Option<EnvironmentReference>,
        pin: Option<&super::GuestKernelPin>,
        architecture: pplx_space_common_core::types::Arch,
    ) -> Result<Option<Self>, BuildExecutionBindingError> {
        // A custom recipe pin or an unqualified host retains the legacy seed
        // contract. This is chosen before resolution, never on resolver failure.
        if reference.is_none()
            && (pin.is_some() || architecture != pplx_space_common_core::types::Arch::X86_64)
        {
            return Ok(None);
        }
        let binding = Self::resolve(reference).await?;
        binding.check_scratch_requirements(architecture)?;
        Ok(Some(binding))
    }

    fn check_scratch_requirements(
        &self,
        architecture: pplx_space_common_core::types::Arch,
    ) -> Result<(), BuildExecutionBindingError> {
        use pplx_space_common_core::types::{GuestOperatingSystem, GuestPlatform, RuntimeKind};
        let guest = GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture,
        };
        if self.execution.runtime().kind != RuntimeKind::Firecracker
            || self.execution.guest() != guest
        {
            return Err(BuildExecutionBindingError::ScratchRequirements {
                architecture,
                runtime: self.execution.runtime().kind,
                guest: self.execution.guest(),
            });
        }
        Ok(())
    }

    /// Resolution, including an old-peer exact fallback, shares one deadline.
    async fn resolve_compatible(
        request: pplx_space_node_mgr_client::proto::resolve_execution_environment_request::Compatible,
        fallback_reference: EnvironmentReference,
    ) -> Result<Self, BuildExecutionBindingError> {
        use pplx_space_node_mgr_client::environment_selection::{SelectionResponseError, decode};
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut node = pplx_space_node_mgr_client::connect().await?;
            let response = node
                .resolve_execution_environment(ResolveExecutionEnvironmentRequest {
                    definition_schema_version: DEFINITION_SCHEMA,
                    reference: None,
                    compatible: Some(request.clone()),
                })
                .await?
                .into_inner();
            match decode(&request, response) {
                Ok(selected) => Ok(Self {
                    definition: selected.definition,
                    execution: selected.execution,
                }),
                Err(SelectionResponseError::Unsupported) => {
                    // Mixed rollout retains the pre-existing exact source,
                    // never an unacknowledged upgrade.
                    let response = node
                        .resolve_execution_environment(ResolveExecutionEnvironmentRequest {
                            definition_schema_version: DEFINITION_SCHEMA,
                            reference: Some(fallback_reference.to_string()),
                            compatible: None,
                        })
                        .await?
                        .into_inner();
                    Self::from_response(Some(fallback_reference), response)
                }
                Err(error) => Err(error.into()),
            }
        })
        .await?
    }

    /// Resolve within 30 seconds, including connection setup.
    pub(super) async fn resolve(
        reference: Option<EnvironmentReference>,
    ) -> Result<Self, BuildExecutionBindingError> {
        let response = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut node = pplx_space_node_mgr_client::connect().await?;
            Ok::<_, BuildExecutionBindingError>(
                node.resolve_execution_environment(ResolveExecutionEnvironmentRequest {
                    definition_schema_version: DEFINITION_SCHEMA,
                    compatible: None,
                    reference: reference.map(|value| value.to_string()),
                })
                .await?
                .into_inner(),
            )
        })
        .await??;
        Self::from_response(reference, response)
    }

    fn from_response(
        reference: Option<EnvironmentReference>,
        response: ResolveExecutionEnvironmentResponse,
    ) -> Result<Self, BuildExecutionBindingError> {
        let definition = EnvironmentDefinition::try_from(
            response
                .definition
                .as_ref()
                .ok_or(BuildExecutionBindingError::Missing("definition"))?,
        )?;
        if reference.is_some_and(|reference| reference != definition.reference()) {
            return Err(BuildExecutionBindingError::Reference);
        }
        let execution = ExecutionMetadata::try_from(
            response
                .execution
                .as_ref()
                .ok_or(BuildExecutionBindingError::Missing("execution"))?,
        )?;
        // A successful legacy RPC or a dropped field is not a support signal.
        if execution.environment() != Some(definition.reference()) {
            return Err(BuildExecutionBindingError::Missing(
                "verified environment reference",
            ));
        }
        let execution = definition.bind_execution(execution)?;
        Ok(Self {
            definition,
            execution,
        })
    }

    pub(super) fn bind(
        &self,
        execution: ExecutionMetadata,
    ) -> Result<ExecutionMetadata, BuildExecutionBindingError> {
        Ok(self.definition.bind_execution(execution)?)
    }

    pub(super) fn configure_scratch(
        &self,
        command: &mut tokio::process::Command,
        pin: Option<&super::GuestKernelPin>,
    ) -> Result<(), BuildExecutionBindingError> {
        use pplx_space_common_core::types::{BootProfile, ExecutionCompatibilityProfile};
        use pplx_space_execution_common::environment::{
            FIRECRACKER_BOOT_CONTRACT, FIRECRACKER_MACHINE_CONTRACT,
        };
        let spec = self.definition.descriptor().spec();
        let BootProfile::ArtifactsV1 {
            contract,
            artifacts,
        } = &spec.boot
        else {
            return Err(BuildExecutionBindingError::ScratchProfile);
        };
        let [boot] = artifacts.as_slice() else {
            return Err(BuildExecutionBindingError::ScratchProfile);
        };
        let ExecutionCompatibilityProfile::RuntimeV1 {
            contract: machine,
            state_format: Some(state_format),
        } = &spec.compatibility
        else {
            return Err(BuildExecutionBindingError::ScratchProfile);
        };
        if contract.as_str() != FIRECRACKER_BOOT_CONTRACT
            || machine.as_str() != FIRECRACKER_MACHINE_CONTRACT
            || boot.role().as_str() != "boot"
            || boot.format().as_str() != "vmlinux"
            || boot.component().kind != pplx_space_common_core::types::ArtifactComponentKind::Kernel
        {
            return Err(BuildExecutionBindingError::ScratchProfile);
        }
        let kernel = boot.component();
        if pin.is_some_and(|pin| pin.version != kernel.version || pin.digest != kernel.digest) {
            return Err(BuildExecutionBindingError::KernelPin);
        }
        // Values are resolved observations, never inherited selector ENV values.
        command.env("SPACE_SEED_RUNTIME", self.execution.runtime().kind.as_str());
        command.env(
            "SPACE_SEED_RUNTIME_VERSION",
            self.execution.runtime().version.as_str(),
        );
        command.env(
            "SPACE_SEED_STATE_FORMAT_VERSION",
            state_format.version.as_str(),
        );
        command.env("SPACE_SEED_KERNEL_VERSION", kernel.version.as_str());
        command.env("SPACE_SEED_KERNEL_DIGEST", kernel.digest.to_string());
        Ok(())
    }

    pub(super) fn bind_template(
        &self,
        metadata: &mut TemplateMetadata,
    ) -> Result<(), BuildExecutionBindingError> {
        let observed =
            metadata
                .execution_metadata()?
                .ok_or(BuildExecutionBindingError::Missing(
                    "template execution metadata",
                ))?;
        let bound = self.bind(observed.clone())?;
        metadata.set_execution_metadata(bound);
        Ok(())
    }

    pub(super) fn bind_document(
        &self,
        bytes: &[u8],
    ) -> Result<Vec<u8>, BuildExecutionBindingError> {
        let mut metadata: TemplateMetadata = serde_json::from_slice(bytes)?;
        self.bind_template(&mut metadata)?;
        let mut document: serde_json::Map<String, serde_json::Value> =
            serde_json::from_slice(bytes)?;
        document.insert(
            "schema_version".into(),
            serde_json::to_value(metadata.schema_version)?,
        );
        document.insert(
            "execution".into(),
            serde_json::to_value(metadata.execution_metadata()?)?,
        );
        Ok(serde_json::to_vec_pretty(&document)?)
    }
}

/// Unbound scratch recipes keep the established runtime/state format and their
/// own optional kernel pin. Inherited process variables cannot select artifacts.
pub(super) fn configure_legacy_scratch(
    command: &mut tokio::process::Command,
    pin: Option<&super::GuestKernelPin>,
) {
    use pplx_space_common_core::types::{
        INITIAL_FIRECRACKER_RUNTIME_VERSION, INITIAL_FIRECRACKER_STATE_FORMAT_VERSION,
    };
    command.env("SPACE_SEED_RUNTIME", "firecracker");
    command.env(
        "SPACE_SEED_RUNTIME_VERSION",
        INITIAL_FIRECRACKER_RUNTIME_VERSION,
    );
    command.env(
        "SPACE_SEED_STATE_FORMAT_VERSION",
        INITIAL_FIRECRACKER_STATE_FORMAT_VERSION,
    );
    command.env_remove("SPACE_SEED_KERNEL_VERSION");
    command.env_remove("SPACE_SEED_KERNEL_DIGEST");
    if let Some(pin) = pin {
        command.env("SPACE_SEED_KERNEL_VERSION", pin.version.as_str());
        command.env("SPACE_SEED_KERNEL_DIGEST", pin.digest.to_string());
    }
}

pub(super) fn read_metadata(path: &Path) -> Result<TemplateMetadata, BuildExecutionBindingError> {
    let bytes = std::fs::read(path).map_err(|source| BuildExecutionBindingError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(serde_json::from_slice(&bytes)?)
}

#[cfg(test)]
mod tests;
