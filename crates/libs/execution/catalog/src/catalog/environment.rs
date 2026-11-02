//! Compiled environment definitions. Artifact installation approval alone does
//! not approve an environment for execution or its memory-state operations.

use pplx_space_common_core::types::*;
use pplx_space_execution_common::environment::{
    EnvironmentBindingError, EnvironmentDefinition, EnvironmentRequirementsError,
};

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
#[error("compiled upgrade edge {from} -> {to} is invalid")]
struct UpgradeEdgeError {
    from: &'static str,
    to: &'static str,
    #[source]
    problem: UpgradeEdgeProblem,
}

/// Why a compiled upgrade edge can't be certified.
#[derive(Debug, thiserror::Error)]
enum UpgradeEdgeProblem {
    #[error("an endpoint is not a compiled environment")]
    UnknownEnvironment,
    #[error("the edge is listed more than once")]
    Duplicate,
    #[error("the runtime is unchanged")]
    RuntimeUnchanged,
    #[error("the source's execution requirements are invalid")]
    SourceRequirements(#[source] EnvironmentRequirementsError),
    #[error("the target fails the cold-boot source check")]
    NotColdBootCompatible(#[source] EnvironmentBindingError),
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
            definition,
            memory_state: true,
            guest_image: QualifiedGuestImage::LinuxDirect,
            name: "e0",
            upgrade_targets: Vec::new(),
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

/// Certified cold-boot upgrade edges between compiled environments, by name.
/// Listing a pair here is the certification; if any listed pair breaks these
/// rules, the catalog withdraws every edge. An edge changes the runtime, and
/// its target must pass the environment-level cold-boot source check: the same
/// runtime kind, guest platform and kernel bytes. The state format may differ,
/// because a cold boot carries no memory. Keeping memory needs a separate
/// memory-edge certification.
const CERTIFIED_UPGRADE_EDGES: &[(&str, &str)] = &[
    ("e0", "fc-1.17.0-linux-6.1.155"),
    // The reverse lets the upgrade drill exercise both directions.
    ("fc-1.17.0-linux-6.1.155", "e0"),
];

/// A complete approved binding, including the operations qualified for it.
/// Installing an artifact alone never adds an entry to this catalog.
#[derive(Clone, Debug)]
pub struct CompiledEnvironment {
    release: PinnedRelease,
    kernel: Option<PinnedKernel>,
    boot_artifacts: Vec<super::PinnedBootArtifact>,
    definition: EnvironmentDefinition,
    memory_state: bool,
    guest_image: QualifiedGuestImage,
    name: &'static str,
    runtime_ordering_key: semver::Version,
    initial_reference: Option<EnvironmentReference>,
    upgrade_targets: Vec<EnvironmentReference>,
}

#[derive(Clone, Copy, Debug)]
enum QualifiedGuestImage {
    LinuxDirect,
    AgentlessDhcpDisk,
}

impl CompiledEnvironment {
    pub fn guest_shutdown(&self) -> crate::guest::GuestShutdown {
        match self.guest_image {
            QualifiedGuestImage::LinuxDirect => crate::guest::GuestShutdown::AgentFlush,
            QualifiedGuestImage::AgentlessDhcpDisk => crate::guest::GuestShutdown::Acpi,
        }
    }

    /// Image qualification is attached to the environment, not inferred from
    /// the guest OS or from firmware versus direct boot at each caller.
    pub fn supports_guest_image(
        &self,
        image: pplx_space_common_core::types::guest_image::GuestImageContract,
    ) -> bool {
        use pplx_space_common_core::types::guest_image::{
            GuestDiskLayout, GuestNetwork, GuestReadiness,
        };
        match self.guest_image {
            QualifiedGuestImage::LinuxDirect => image.legacy_transport(),
            QualifiedGuestImage::AgentlessDhcpDisk => {
                image.disk() == GuestDiskLayout::OpaqueRawDisk
                    && image.network() == GuestNetwork::SlotDhcpV1
                    && !image.expects_agent()
                    && matches!(image.readiness(), GuestReadiness::TcpService { .. })
            }
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn for_test(
        release: PinnedRelease,
        kernel: PinnedKernel,
        state_version: &'static str,
        memory_state: bool,
    ) -> Self {
        let definition = definition_from_pins(&release, &kernel, state_version)
            .expect("test pins define a valid environment");
        Self {
            runtime_ordering_key: semver::Version::parse(release.release.version.as_str())
                .expect("test release version is valid"),
            initial_reference: Some(
                InitialEnvironment::for_architecture(release.architecture)
                    .expect("test architecture has initial pins")
                    .definition()
                    .expect("test initial environment is valid")
                    .reference(),
            ),
            release,
            kernel: Some(kernel),
            boot_artifacts: Vec::new(),
            definition,
            memory_state,
            guest_image: QualifiedGuestImage::LinuxDirect,
            name: "synthetic",
            upgrade_targets: Vec::new(),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn named_for_test(mut self, name: &'static str) -> Self {
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
            // Install pins are not execution qualification. Keep ARM64 out of
            // admission until cold boot, guest traffic, recovery and cleanup pass.
            return Ok(Vec::new());
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
                definition,
                memory_state,
                guest_image: QualifiedGuestImage::LinuxDirect,
                name,
                upgrade_targets: Vec::new(),
            })
        })
        .collect::<Result<_, CatalogError>>()?;
        environments.push(Self::cloud_hypervisor(&catalog, architecture, false)?);
        environments.push(Self::cloud_hypervisor(&catalog, architecture, true)?);
        environments.push(Self::cloud_hypervisor_windows(&catalog, architecture)?);
        record_upgrade_edges(&mut environments, CERTIFIED_UPGRADE_EDGES);
        Ok(environments)
    }

    fn cloud_hypervisor_windows(
        catalog: &RuntimeCatalog,
        architecture: Arch,
    ) -> Result<Self, CatalogError> {
        let release = catalog
            .releases(RuntimeKind::CloudHypervisor, architecture)
            .find(|pin| pin.release.version.as_str() == "54.0")
            .ok_or_else(|| CatalogError::Compiled {
                entry: "CH Windows UEFI".into(),
                field: "runtime pin",
                value: "54.0",
            })?
            .clone();
        let firmware = catalog
            .boot_artifacts(architecture)
            .find(|pin| {
                pin.identity().kind == ArtifactComponentKind::Firmware
                    && pin.identity().version.as_str() == super::uefi::VERSION
            })
            .ok_or_else(|| CatalogError::Compiled {
                entry: "CH Windows UEFI".into(),
                field: "firmware pin",
                value: super::uefi::VERSION,
            })?
            .clone();
        let definition = (|| -> Result<_, CompiledEnvironmentDefinitionError> {
            let host = HostPlatform {
                os: HostOperatingSystem::Linux,
                architecture,
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
                    architecture,
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
            definition,
            memory_state: false,
            guest_image: QualifiedGuestImage::AgentlessDhcpDisk,
            name: "ch-54.0-windows-uefi",
            upgrade_targets: Vec::new(),
        })
    }

    fn cloud_hypervisor(
        catalog: &RuntimeCatalog,
        architecture: Arch,
        memory_state: bool,
    ) -> Result<Self, CatalogError> {
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
        let definition = standalone_definition(&release, &kernel, memory_state)
            .map_err(CatalogError::EnvironmentDefinition)?;
        Ok(Self {
            runtime_ordering_key: semver::Version::new(54, 0, 0),
            initial_reference: None,
            release,
            kernel: Some(kernel),
            boot_artifacts: Vec::new(),
            definition,
            memory_state,
            guest_image: QualifiedGuestImage::LinuxDirect,
            name: if memory_state {
                "ch-54.0-linux-7.2.8-state-v1"
            } else {
                "ch-54.0-linux-7.2.8"
            },
            upgrade_targets: Vec::new(),
        })
    }

    pub fn is_initial(&self) -> bool {
        Some(self.definition.reference()) == self.initial_reference
    }

    pub fn runtime_ordering_key(&self) -> &semver::Version {
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
    /// Environments this one can be cold-boot upgraded to, in certification
    /// order. Filled only in a catalog from `for_architecture` (and so
    /// `cached_for_architecture`); an entry built
    /// alone, such as `InitialEnvironment::compiled_environment`, has none.
    pub fn upgrade_targets(&self) -> &[EnvironmentReference] {
        &self.upgrade_targets
    }

    /// The compiled environment `execution` ran under: its binding, or for
    /// unbound metadata the environment whose definition it satisfies. A stock
    /// initial-environment snapshot written before boot components were
    /// recorded counts as the initial environment, as policy admission
    /// treats it. Anything else is no compiled environment.
    pub fn of_execution<'a>(
        catalog: &'a [Self],
        execution: &ExecutionMetadata,
    ) -> Option<&'a Self> {
        if let Some(reference) = execution.environment() {
            return catalog
                .iter()
                .find(|entry| entry.definition.reference() == reference);
        }
        catalog
            .iter()
            .find(|entry| entry.definition.check_execution(execution).is_ok())
            .or_else(|| {
                catalog.iter().find(|entry| {
                    entry.is_initial()
                        && execution.components().is_empty()
                        && entry
                            .definition
                            .execution_requirements()
                            .is_ok_and(|required| {
                                execution.runtime() == required.runtime()
                                    && execution.guest() == required.guest()
                                    && required.state_format().is_some()
                                    && execution.state_format() == required.state_format()
                            })
                })
            })
    }
    /// Upgrade targets also certified to restore this one's checkpoints. None
    /// is certified yet: keeping memory across a runtime change needs a
    /// qualified loader pair first.
    pub fn memory_targets(&self) -> &[EnvironmentReference] {
        &[]
    }
}

/// Edges are optional metadata, so an invalid table withdraws every edge and
/// never an environment: no node publishes part of a table. Tests reject an
/// invalid compiled table before it ships.
fn record_upgrade_edges(
    environments: &mut [CompiledEnvironment],
    edges: &[(&'static str, &'static str)],
) {
    if let Err(error) = certify_upgrade_edges(environments, edges) {
        tracing::warn!(target: "pplx_space_node_runtime_core::catalog::environment",
            error = %error,
            error.debug = ?error,
            "withdrawing every compiled upgrade edge"
        );
        for entry in environments {
            entry.upgrade_targets.clear();
        }
    }
}

/// Check every edge against the cold-boot rules and record it on its source.
fn certify_upgrade_edges(
    environments: &mut [CompiledEnvironment],
    edges: &[(&'static str, &'static str)],
) -> Result<(), UpgradeEdgeError> {
    for &(from, to) in edges {
        let invalid = |problem| UpgradeEdgeError { from, to, problem };
        let find = |name| {
            environments
                .iter()
                .find(|entry: &&CompiledEnvironment| entry.name == name)
                .ok_or_else(|| invalid(UpgradeEdgeProblem::UnknownEnvironment))
        };
        let (source, target) = (find(from)?, find(to)?);
        if source.definition.descriptor().spec().runtime
            == target.definition.descriptor().spec().runtime
        {
            return Err(invalid(UpgradeEdgeProblem::RuntimeUnchanged));
        }
        let source_execution = source
            .definition
            .execution_requirements()
            .map_err(|error| invalid(UpgradeEdgeProblem::SourceRequirements(error)))?;
        target
            .definition
            .check_cold_boot_source(&source_execution)
            .map_err(|error| invalid(UpgradeEdgeProblem::NotColdBootCompatible(error)))?;
        let target_reference = target.definition.reference();
        let source = environments
            .iter_mut()
            .find(|entry| entry.name == from)
            .ok_or_else(|| invalid(UpgradeEdgeProblem::UnknownEnvironment))?;
        let targets = &mut source.upgrade_targets;
        if targets.contains(&target_reference) {
            return Err(invalid(UpgradeEdgeProblem::Duplicate));
        }
        targets.push(target_reference);
    }
    Ok(())
}

#[cfg(test)]
mod tests;

use pplx_space_execution_common::environment::{
    CLOUD_HYPERVISOR_BOOT_CONTRACT, CLOUD_HYPERVISOR_MACHINE_CONTRACT,
};

fn standalone_definition(
    release: &PinnedRelease,
    kernel: &PinnedKernel,
    memory_state: bool,
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
                    pplx_space_execution_common::environment::cloud_hypervisor_linux_format(
                        kernel.architecture,
                    )
                    .parse()?,
                )],
            },
            isolation: IsolationClass::HardwareVm,
            compatibility: ExecutionCompatibilityProfile::RuntimeV1 {
                contract: CLOUD_HYPERVISOR_MACHINE_CONTRACT.parse()?,
                state_format: if memory_state {
                    Some(StateFormatIdentity {
                        format: "cloud-hypervisor-snapshot".parse()?,
                        version: "54.0.1".parse()?,
                    })
                } else {
                    None
                },
            },
        }
        .try_into()?,
    ))
}
