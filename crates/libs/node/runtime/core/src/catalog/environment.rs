//! Compiled environment definitions. Artifact installation approval alone does
//! not approve an environment for execution or its memory-state operations.

use pplx_space_common_core::types::*;
use pplx_space_execution_common::environment::EnvironmentDefinition;

use super::{CatalogError, InitialEnvironment, PinnedKernel, PinnedRelease, RuntimeCatalog};

#[derive(Debug, thiserror::Error)]
pub enum CompiledEnvironmentDefinitionError {
    #[error(transparent)]
    Contract(#[from] ExecutionContractIdParseError),
    #[error(transparent)]
    Role(#[from] ArtifactComponentRoleParseError),
    #[error(transparent)]
    Format(#[from] ArtifactFormatParseError),
    #[error("no supported compiled environment descriptor for architecture {0}")]
    UnsupportedArchitecture(Arch),
    #[error("invalid compiled environment descriptor: {0}")]
    Descriptor(#[from] EnvironmentDescriptorError),
    #[error("invalid compiled environment encoding: {0}")]
    Encoding(#[from] pplx_space_execution_common::environment::EnvironmentDecodeError),
    #[error("invalid compiled runtime ordering version: {0}")]
    RuntimeVersion(#[from] semver::Error),
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
    /// Resolve E0 independently of additional compiled bindings. An invalid
    /// optional qualification must not withdraw the production default.
    pub fn compiled_environment(
        &self,
    ) -> Result<CompiledEnvironment, CompiledEnvironmentDefinitionError> {
        let definition = self.definition()?;
        Ok(CompiledEnvironment {
            runtime_ordering_key: semver::Version::parse(self.release.release.version.as_str())?,
            initial_reference: Some(definition.reference()),
            release: self.release.clone(),
            kernel: Some(self.kernel.clone()),
            boot_artifacts: Vec::new(),
            exclusive_device_profiles: Vec::new(),
            definition,
            memory_state: true,
            name: "e0",
        })
    }

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
    Ok(EnvironmentDefinition::new(descriptor).into_runtime_neutral()?)
}

/// A complete approved binding, including the operations qualified for it.
/// Installing an artifact alone never adds an entry to this catalog.
#[derive(Clone, Debug)]
pub struct CompiledEnvironment {
    release: PinnedRelease,
    kernel: Option<PinnedKernel>,
    boot_artifacts: Vec<super::PinnedBootArtifact>,
    exclusive_device_profiles: Vec<super::ExclusiveDeviceProfile>,
    definition: EnvironmentDefinition,
    memory_state: bool,
    name: &'static str,
    runtime_ordering_key: semver::Version,
    initial_reference: Option<EnvironmentReference>,
}

impl CompiledEnvironment {
    #[cfg(test)]
    pub(crate) fn for_test(
        release: PinnedRelease,
        kernel: PinnedKernel,
        state_version: &'static str,
        memory_state: bool,
    ) -> Self {
        let definition = if release.release.kind == RuntimeKind::CloudHypervisor {
            standalone_definition(&release, &kernel).unwrap()
        } else {
            definition_from_pins(&release, &kernel, state_version).unwrap()
        };
        Self {
            runtime_ordering_key: semver::Version::parse(release.release.version.as_str()).unwrap(),
            initial_reference: Some(
                InitialEnvironment::for_architecture(release.architecture)
                    .unwrap()
                    .definition()
                    .unwrap()
                    .reference(),
            ),
            release,
            kernel: Some(kernel),
            boot_artifacts: Vec::new(),
            exclusive_device_profiles: Vec::new(),
            definition,
            memory_state,
            name: "synthetic",
        }
    }

    #[cfg(test)]
    pub(crate) fn named_for_test(mut self, name: &'static str) -> Self {
        self.name = name;
        self
    }

    /// Shared immutable catalog for execution consumers in this process.
    pub fn cached_for_architecture(
        architecture: Arch,
    ) -> Result<&'static [Self], &'static CatalogError> {
        static X86: std::sync::OnceLock<Result<Vec<CompiledEnvironment>, CatalogError>> =
            std::sync::OnceLock::new();
        static ARM: std::sync::OnceLock<Result<Vec<CompiledEnvironment>, CatalogError>> =
            std::sync::OnceLock::new();
        let cache = match architecture {
            Arch::X86_64 => &X86,
            Arch::Aarch64 => &ARM,
        };
        cache
            .get_or_init(|| Self::for_architecture(architecture))
            .as_deref()
    }

    /// Build an owned catalog for startup validation and qualification tests.
    /// Repeated execution lookups use `cached_for_architecture` instead.
    pub fn for_architecture(architecture: Arch) -> Result<Vec<Self>, CatalogError> {
        if architecture == Arch::Aarch64 {
            return Ok(vec![Self::cloud_hypervisor(architecture)?]);
        }
        let catalog = RuntimeCatalog::compiled()?;
        let initial_reference = InitialEnvironment::for_architecture(architecture)?
            .definition()
            .map_err(CatalogError::EnvironmentDefinition)?
            .reference();
        // Complete bindings are approved independently of downloadable release pins.
        // Memory-state compatibility must be qualified before it is advertised.
        let mut environments: Vec<Self> = [
            (
                "e0",
                "1.15.1",
                "6.1.155",
                INITIAL_FIRECRACKER_STATE_FORMAT_VERSION,
                true,
            ),
            (
                "fc-1.17.0-linux-6.18.44",
                "1.17.0",
                "6.18.44",
                "12.0.0",
                true,
            ),
            (
                "fc-1.17.0-linux-6.1.155",
                "1.17.0",
                "6.1.155",
                "12.0.0",
                true,
            ),
        ]
        .into_iter()
        .map(|(name, runtime, kernel, state_version, memory_state)| {
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
            let runtime_ordering_key = semver::Version::parse(release.release.version.as_str())
                .map_err(CompiledEnvironmentDefinitionError::RuntimeVersion)
                .map_err(CatalogError::EnvironmentDefinition)?;
            Ok(Self {
                runtime_ordering_key,
                initial_reference: Some(initial_reference),
                release: release.clone(),
                kernel: Some(kernel_pin.clone()),
                boot_artifacts: Vec::new(),
                exclusive_device_profiles: Vec::new(),
                definition,
                memory_state,
                name,
            })
        })
        .collect::<Result<_, CatalogError>>()?;
        environments.push(Self::cloud_hypervisor(architecture)?);
        environments.push(Self::cloud_hypervisor_signed_boot()?);
        environments.push(Self::cloud_hypervisor_windows()?);
        Ok(environments)
    }

    fn cloud_hypervisor_windows() -> Result<Self, CatalogError> {
        let catalog = RuntimeCatalog::compiled()?;
        let release = catalog
            .releases(RuntimeKind::CloudHypervisor, Arch::X86_64)
            .find(|pin| pin.release.version.as_str() == "54.0")
            .ok_or_else(|| CatalogError::Compiled {
                entry: "CH Windows UEFI".into(),
                field: "runtime pin",
                value: "54.0",
            })?
            .clone();
        let firmware = super::uefi::firmware()?;
        let definition = (|| -> Result<_, CompiledEnvironmentDefinitionError> {
            let host = HostPlatform {
                os: HostOperatingSystem::Linux,
                architecture: Arch::X86_64,
            };
            let boot_contract =
                pplx_space_execution_common::environment::CLOUD_HYPERVISOR_UEFI_BOOT_CONTRACT
                    .parse()?;
            let machine_contract =
                pplx_space_execution_common::environment::CLOUD_HYPERVISOR_WINDOWS_MACHINE_CONTRACT
                    .parse()?;
            let spec = EnvironmentSpec {
                host,
                guest: GuestPlatform {
                    os: GuestOperatingSystem::Windows,
                    architecture: Arch::X86_64,
                },
                runtime: RuntimeBinding::ManagedRelease(ManagedRuntimeRelease {
                    runtime: release.release.clone(),
                    digest: release.digest(),
                    target: host,
                    layout: RuntimePayloadLayout::CatalogReleaseV1,
                }),
                backend: HypervisorBackend::Kvm,
                boot: BootProfile::ArtifactsV1 {
                    contract: boot_contract,
                    artifacts: vec![BootArtifact::new(
                        "firmware".parse()?,
                        firmware.identity().clone(),
                        "edk2-fd".parse()?,
                    )],
                },
                isolation: IsolationClass::HardwareVm,
                compatibility: ExecutionCompatibilityProfile::RuntimeV1 {
                    contract: machine_contract,
                    state_format: None,
                },
            };
            Ok(EnvironmentDefinition::new(spec.try_into()?))
        })()?;
        Ok(Self {
            runtime_ordering_key: semver::Version::new(54, 0, 0),
            initial_reference: None,
            release,
            kernel: None,
            boot_artifacts: vec![firmware],
            exclusive_device_profiles: Vec::new(),
            definition,
            memory_state: false,
            name: "ch-54.0-windows-uefi",
        })
    }

    fn cloud_hypervisor_signed_boot() -> Result<Self, CatalogError> {
        use super::gpu_boot;
        let catalog = RuntimeCatalog::compiled()?;
        let missing = || CatalogError::MissingEnvironmentPins {
            runtime: gpu_boot::RUNTIME_VERSION,
            kernel: gpu_boot::KERNEL_VERSION,
        };
        let release = catalog
            .releases(RuntimeKind::CloudHypervisor, Arch::X86_64)
            .find(|pin| pin.release.version.as_str() == gpu_boot::RUNTIME_VERSION)
            .ok_or_else(missing)?
            .clone();
        let kernel = catalog
            .all_kernels(Arch::X86_64)
            .find(|pin| pin.version.as_str() == gpu_boot::KERNEL_VERSION)
            .ok_or_else(missing)?
            .clone();
        let initramfs = gpu_boot::initramfs()?;
        let definition = (|| -> Result<_, CompiledEnvironmentDefinitionError> {
            let base = standalone_definition(&release, &kernel)?;
            let mut spec = base.descriptor().spec().clone();
            spec.boot = BootProfile::ArtifactsV1 {
                contract: CLOUD_HYPERVISOR_BOOT_CONTRACT.parse()?,
                artifacts: vec![
                    BootArtifact::new(
                        "boot".parse()?,
                        ComponentIdentity {
                            kind: ArtifactComponentKind::Kernel,
                            version: kernel.version.clone(),
                            digest: kernel.sha256,
                        },
                        "linux-bzimage".parse()?,
                    ),
                    BootArtifact::new(
                        "initramfs".parse()?,
                        initramfs.identity().clone(),
                        "linux-initramfs".parse()?,
                    ),
                ],
            };
            Ok(EnvironmentDefinition::new(spec.try_into()?))
        })()?;
        Ok(Self {
            runtime_ordering_key: semver::Version::new(54, 0, 0),
            initial_reference: None,
            release,
            kernel: Some(kernel),
            boot_artifacts: vec![initramfs],
            // Qualification never changes the immutable boot-artifact identity.
            exclusive_device_profiles: {
                #[cfg(feature = "gpu-development")]
                {
                    vec![super::development_t4_profile()?]
                }
                #[cfg(not(feature = "gpu-development"))]
                {
                    Vec::new()
                }
            },
            definition,
            memory_state: false,
            name: "ch-54.0-linux-6.1.155-nvidia-580.178.04",
        })
    }

    fn cloud_hypervisor(architecture: Arch) -> Result<Self, CatalogError> {
        let catalog = RuntimeCatalog::compiled()?;
        let missing = || CatalogError::MissingEnvironmentPins {
            runtime: "54.0",
            kernel: "7.2.8+",
        };
        let release = catalog
            .releases(RuntimeKind::CloudHypervisor, architecture)
            .find(|pin| pin.release.version.as_str() == "54.0")
            .ok_or_else(missing)?
            .clone();
        let kernel = catalog
            .all_kernels(architecture)
            .find(|pin| {
                pin.runtime == RuntimeKind::CloudHypervisor && pin.version.as_str() == "7.2.8+"
            })
            .ok_or_else(missing)?
            .clone();
        let definition = standalone_definition(&release, &kernel)
            .map_err(CatalogError::EnvironmentDefinition)?;
        Ok(Self {
            runtime_ordering_key: semver::Version::new(54, 0, 0),
            initial_reference: None,
            release,
            kernel: Some(kernel),
            boot_artifacts: Vec::new(),
            exclusive_device_profiles: Vec::new(),
            definition,
            memory_state: false,
            name: "ch-54.0-linux-7.2.8",
        })
    }

    pub fn exclusive_device_profiles(&self) -> &[super::ExclusiveDeviceProfile] {
        &self.exclusive_device_profiles
    }

    pub fn exclusive_device_resources(
        &self,
        execution: &pplx_space_execution_common::devices::DeviceExecution,
    ) -> Result<super::DeviceResourceLimits, super::DeviceProfileError> {
        super::device_profile::resources(&self.exclusive_device_profiles, execution)
    }

    pub fn exclusive_device_requirement_resources(
        &self,
        requirements: &pplx_space_execution_spec::devices::DeviceRequirements,
    ) -> Result<super::DeviceResourceLimits, super::DeviceProfileError> {
        super::device_profile::requirement_resources(&self.exclusive_device_profiles, requirements)
    }

    pub fn is_initial(&self) -> bool {
        Some(self.definition.reference()) == self.initial_reference
    }

    pub(crate) fn runtime_ordering_key(&self) -> &semver::Version {
        &self.runtime_ordering_key
    }

    pub fn name(&self) -> &'static str {
        self.name
    }

    pub fn release(&self) -> &PinnedRelease {
        &self.release
    }
    pub fn kernel(&self) -> Option<&PinnedKernel> {
        self.kernel.as_ref()
    }
    /// File pins supplement the descriptor's identities with publication sizes.
    /// Roles and formats come from the descriptor, never from installation paths.
    pub fn boot_artifact(
        &self,
        identity: &ComponentIdentity,
    ) -> Option<&super::PinnedBootArtifact> {
        self.boot_artifacts.iter().find(|pin| {
            pin.identity() == identity && pin.architecture() == self.release.architecture
        })
    }

    pub fn supports_memory_state(&self) -> bool {
        self.memory_state
            && self
                .definition
                .descriptor()
                .spec()
                .compatibility
                .state_format()
                .is_some()
    }
    pub fn definition(&self) -> &EnvironmentDefinition {
        &self.definition
    }
}

#[cfg(test)]
mod tests;

use pplx_space_execution_common::environment::{
    CLOUD_HYPERVISOR_BOOT_CONTRACT, CLOUD_HYPERVISOR_MACHINE_CONTRACT,
};

fn standalone_definition(
    release: &PinnedRelease,
    kernel: &PinnedKernel,
) -> Result<EnvironmentDefinition, CompiledEnvironmentDefinitionError> {
    let host = HostPlatform {
        os: HostOperatingSystem::Linux,
        architecture: release.architecture,
    };
    Ok(EnvironmentDefinition::new(
        EnvironmentSpec {
            host,
            guest: GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: kernel.architecture,
            },
            runtime: RuntimeBinding::ManagedRelease(ManagedRuntimeRelease {
                runtime: release.release.clone(),
                digest: release.digest(),
                target: host,
                layout: RuntimePayloadLayout::CatalogReleaseV1,
            }),
            backend: HypervisorBackend::Kvm,
            boot: BootProfile::ArtifactsV1 {
                contract: CLOUD_HYPERVISOR_BOOT_CONTRACT.parse()?,
                artifacts: vec![BootArtifact::new(
                    "boot".parse()?,
                    ComponentIdentity {
                        kind: ArtifactComponentKind::Kernel,
                        version: kernel.version.clone(),
                        digest: kernel.sha256,
                    },
                    match kernel.architecture {
                        Arch::Aarch64 => "linux-image",
                        Arch::X86_64 => "vmlinux",
                    }
                    .parse()?,
                )],
            },
            isolation: IsolationClass::HardwareVm,
            compatibility: ExecutionCompatibilityProfile::RuntimeV1 {
                contract: CLOUD_HYPERVISOR_MACHINE_CONTRACT.parse()?,
                state_format: None,
            },
        }
        .try_into()?,
    ))
}
