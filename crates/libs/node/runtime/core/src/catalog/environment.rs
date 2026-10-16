//! Compiled environment definitions. Artifact installation approval alone does
//! not approve an environment for execution or its memory-state operations.

use pplx_space_common_core::types::*;
use pplx_space_execution_common::environment::EnvironmentDefinition;

use super::{CatalogError, InitialEnvironment, PinnedKernel, PinnedRelease, RuntimeCatalog};

#[derive(Debug, thiserror::Error)]
pub enum CompiledEnvironmentDefinitionError {
    #[error("no supported compiled environment descriptor for architecture {0}")]
    UnsupportedArchitecture(Arch),
    #[error("invalid compiled environment descriptor: {0}")]
    Descriptor(#[from] EnvironmentDescriptorError),
    #[error("invalid compiled state format: {0}")]
    StateFormat(#[from] StateFormatNameParseError),
    #[error("invalid compiled state format version: {0}")]
    StateFormatVersion(#[from] StateFormatVersionParseError),
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveEnvironmentError {
    #[error(transparent)]
    Definition(#[from] CompiledEnvironmentDefinitionError),
    #[error("environment {0} is not the compiled initial environment")]
    UnknownReference(EnvironmentReference),
}

impl InitialEnvironment {
    /// Production qualification covers Linux/x86_64 E0 only. Existing artifact
    /// pins for other architectures retain their installation meaning.
    pub fn definition(&self) -> Result<EnvironmentDefinition, CompiledEnvironmentDefinitionError> {
        if self.release.architecture != Arch::X86_64 {
            return Err(CompiledEnvironmentDefinitionError::UnsupportedArchitecture(
                self.release.architecture,
            ));
        }
        definition_from_pins(
            &self.release,
            &self.kernel,
            INITIAL_FIRECRACKER_STATE_FORMAT_VERSION,
        )
    }

    /// Resolve by complete environment identity, never by an artifact version
    /// or by whatever happens to be installed. This does not select a default.
    pub fn resolve_definition(
        &self,
        reference: EnvironmentReference,
    ) -> Result<EnvironmentDefinition, ResolveEnvironmentError> {
        let definition = self.definition()?;
        if definition.reference() != reference {
            return Err(ResolveEnvironmentError::UnknownReference(reference));
        }
        Ok(definition)
    }
}

fn definition_from_pins(
    release: &PinnedRelease,
    kernel: &PinnedKernel,
    state_version: &str,
) -> Result<EnvironmentDefinition, CompiledEnvironmentDefinitionError> {
    let host = HostPlatform {
        os: HostOperatingSystem::Linux,
        architecture: release.architecture,
    };
    let descriptor = EnvironmentSpec {
        host,
        guest: GuestPlatform {
            os: GuestOperatingSystem::Linux,
            architecture: kernel.architecture,
        },
        runtime: RuntimeBinding::ManagedRelease(ManagedRuntimeRelease {
            runtime: release.release.clone(),
            digest: release.digest(),
            target: host,
            layout: RuntimePayloadLayout::FirecrackerReleaseV1,
        }),
        backend: HypervisorBackend::Kvm,
        boot: BootProfile::LinuxDirectV1 {
            kernel: ComponentIdentity {
                kind: ArtifactComponentKind::Kernel,
                version: kernel.version.clone(),
                digest: kernel.sha256,
            },
        },
        isolation: IsolationClass::HardwareVm,
        compatibility: ExecutionCompatibilityProfile::FirecrackerSnapshotV1 {
            state_format: StateFormatIdentity {
                format: FIRECRACKER_STATE_FORMAT.parse()?,
                version: state_version.parse()?,
            },
        },
    }
    .try_into()?;
    Ok(EnvironmentDefinition::new(descriptor))
}

/// A complete approved binding, including the operations qualified for it.
/// Installing an artifact alone never adds an entry to this catalog.
#[derive(Clone, Debug)]
pub struct CompiledEnvironment {
    release: PinnedRelease,
    kernel: PinnedKernel,
    definition: EnvironmentDefinition,
    memory_state: bool,
}

impl CompiledEnvironment {
    #[cfg(test)]
    pub(crate) fn for_test(
        release: PinnedRelease,
        kernel: PinnedKernel,
        state_version: &'static str,
        memory_state: bool,
    ) -> Self {
        let definition = definition_from_pins(&release, &kernel, state_version).unwrap();
        Self {
            release,
            kernel,
            definition,
            memory_state,
        }
    }

    pub fn for_architecture(architecture: Arch) -> Result<Vec<Self>, CatalogError> {
        if architecture != Arch::X86_64 {
            return Ok(Vec::new());
        }
        let catalog = RuntimeCatalog::compiled()?;
        // Complete bindings are approved independently of downloadable release pins.
        // Memory-state compatibility must be qualified before it is advertised.
        [
            (
                "1.15.1",
                "6.1.155",
                INITIAL_FIRECRACKER_STATE_FORMAT_VERSION,
                true,
            ),
            ("1.17.0", "6.18.44", "12.0.0", true),
            ("1.17.0", "6.1.155", "12.0.0", true),
        ]
        .into_iter()
        .map(|(runtime, kernel, state_version, memory_state)| {
            let release = catalog
                .releases(RuntimeKind::Firecracker, architecture)
                .find(|pin| pin.release.version.as_str() == runtime)
                .ok_or(CatalogError::MissingEnvironmentPins { runtime, kernel })?;
            let kernel_version = kernel
                .parse()
                .map_err(|_| CatalogError::MissingEnvironmentPins { runtime, kernel })?;
            let kernel_pin = catalog
                .kernel(&kernel_version, architecture)
                .ok_or(CatalogError::MissingEnvironmentPins { runtime, kernel })?;
            let definition = definition_from_pins(release, kernel_pin, state_version)
                .map_err(CatalogError::EnvironmentDefinition)?;
            Ok(Self {
                release: release.clone(),
                kernel: kernel_pin.clone(),
                definition,
                memory_state,
            })
        })
        .collect()
    }

    pub fn release(&self) -> &PinnedRelease {
        &self.release
    }
    pub fn kernel(&self) -> &PinnedKernel {
        &self.kernel
    }
    pub fn supports_memory_state(&self) -> bool {
        self.memory_state
    }
    pub fn definition(&self) -> &EnvironmentDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests;
