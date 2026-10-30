use pplx_space_common_core::types::{ExecutionMetadata, GuestOperatingSystem, RuntimeKind};

/// The launch boundary derives this from verified execution metadata, not from
/// caller-provided runtime JSON.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootSource {
    LinuxDirect,
    Uefi,
}

impl BootSource {
    pub fn from_execution(execution: &ExecutionMetadata) -> Result<Self, GuestProfileError> {
        Self::resolve_execution(execution).map(|(source, _)| source)
    }

    fn resolve_execution(
        execution: &ExecutionMetadata,
    ) -> Result<(Self, Option<&'static crate::catalog::CompiledEnvironment>), GuestProfileError>
    {
        // Historical Firecracker disks predate bound environment descriptors;
        // their wire contract already specifies Linux direct boot.
        if execution.environment().is_none()
            && execution.runtime().kind == RuntimeKind::Firecracker
            && execution.guest().os == GuestOperatingSystem::Linux
            && execution.components().iter().all(|component| {
                component.kind == pplx_space_common_core::types::ArtifactComponentKind::Kernel
            })
        {
            return Ok((Self::LinuxDirect, None));
        }
        use pplx_space_common_core::types::BootProfile;
        use pplx_space_execution_common::environment::{
            CLOUD_HYPERVISOR_BOOT_CONTRACT, CLOUD_HYPERVISOR_UEFI_BOOT_CONTRACT,
            FIRECRACKER_BOOT_CONTRACT,
        };
        let catalog = crate::catalog::CompiledEnvironment::cached_for_architecture(
            execution.guest().architecture,
        )
        .map_err(|_| GuestProfileError("compiled environment catalog unavailable"))?;
        let environment = crate::catalog::CompiledEnvironment::of_execution(catalog, execution)
            .ok_or(GuestProfileError("unknown execution boot contract"))?;
        if execution.environment().is_some() {
            environment
                .definition()
                .check_execution(execution)
                .map_err(|_| GuestProfileError("execution differs from its bound environment"))?;
        }
        let source = match &environment.definition().descriptor().spec().boot {
            BootProfile::LinuxDirectV1 { .. } => Ok(Self::LinuxDirect),
            BootProfile::ArtifactsV1 { contract, .. } => match contract.as_str() {
                FIRECRACKER_BOOT_CONTRACT | CLOUD_HYPERVISOR_BOOT_CONTRACT => Ok(Self::LinuxDirect),
                CLOUD_HYPERVISOR_UEFI_BOOT_CONTRACT => Ok(Self::Uefi),
                _ => Err(GuestProfileError("unsupported execution boot contract")),
            },
        }?;
        Ok((source, Some(environment)))
    }
}

/// Shutdown protocol explicitly qualified by the execution environment.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestShutdown {
    AgentFlush,
    Acpi,
}

/// Image behavior qualified against the compiled execution environment.
/// Resolve at each process boundary before side effects.
#[derive(Clone, Copy, Debug)]
pub struct GuestProfile {
    shutdown: GuestShutdown,
    image: pplx_space_common_core::types::guest_image::GuestImageContract,
}
impl GuestProfile {
    pub fn resolve(
        execution: &ExecutionMetadata,
        image: pplx_space_common_core::types::guest_image::GuestImageContract,
    ) -> Result<Self, GuestProfileError> {
        image
            .validate()
            .map_err(|_| GuestProfileError("inconsistent guest agent/readiness contract"))?;
        let (_, environment) = BootSource::resolve_execution(execution)?;
        let qualified = match environment {
            Some(environment) => environment.supports_guest_image(image),
            // Only the historical unbound Firecracker path can reach here after
            // BootSource admission; retain its legacy image semantics on ARM too.
            None => execution.environment().is_none() && image.legacy_transport(),
        };
        if !qualified {
            return Err(GuestProfileError(
                "image is incompatible with the qualified boot protocol",
            ));
        }
        let shutdown = match environment {
            Some(environment) => environment.guest_shutdown(),
            None => GuestShutdown::AgentFlush,
        };
        Ok(Self { image, shutdown })
    }
    pub fn shutdown(self) -> GuestShutdown {
        self.shutdown
    }

    pub fn uses_dhcp(self) -> bool {
        self.image.network() == pplx_space_common_core::types::guest_image::GuestNetwork::SlotDhcpV1
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid guest profile: {0}")]
pub struct GuestProfileError(&'static str);
