//! Versioned environment codec and content identity. This is not an execution
//! allowlist: consumers must resolve the reference against their approved catalog.

use aws_lc_rs::digest::{SHA256, digest};
use pplx_space_common_core::types::*;

use crate::{ExecutionProtoError, proto};

mod binding;
mod observation;
mod profiles;
mod transport;
pub use binding::{EnvironmentBindingError, EnvironmentRequirementsError};
pub use observation::{
    ENVIRONMENT_OBSERVATION_SCHEMA_VERSION, EnvironmentObservation, EnvironmentObservationError,
    KnownEnvironment,
};
pub use transport::{FIRECRACKER_BOOT_CONTRACT, FIRECRACKER_MACHINE_CONTRACT};
pub const CLOUD_HYPERVISOR_BOOT_CONTRACT: &str = "cloud-hypervisor-linux-direct-v1";
/// UEFI raw-disk boot; no Linux kernel, command line, or initramfs.
pub const CLOUD_HYPERVISOR_UEFI_BOOT_CONTRACT: &str = "cloud-hypervisor-uefi-v1";
/// x86 PCI/ACPI, KVM Hyper-V enlightenments, UTC RTC; cold boot only.
pub const CLOUD_HYPERVISOR_WINDOWS_MACHINE_CONTRACT: &str = "cloud-hypervisor-windows-pci-v1";
pub const CLOUD_HYPERVISOR_MACHINE_CONTRACT: &str = "cloud-hypervisor-pci-v1";

/// The direct-boot image format is part of the compiled CH boot contract.
pub const fn cloud_hypervisor_linux_format(architecture: Arch) -> &'static str {
    match architecture {
        Arch::Aarch64 => "linux-image",
        Arch::X86_64 => "vmlinux",
    }
}

/// Original encoding; existing definitions retain these bytes and references.
pub const ENVIRONMENT_SCHEMA_VERSION: u32 = 1;
pub const RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentDefinition {
    reference: EnvironmentReference,
    descriptor: EnvironmentDescriptor,
    // Immutable historical representation used only for identity and old peers.
    legacy_descriptor: Option<EnvironmentDescriptor>,
}

#[derive(Debug, thiserror::Error)]
pub enum EnvironmentDecodeError {
    #[error("unsupported execution environment schema {0}")]
    UnsupportedSchema(u32),
    #[error("execution environment is missing {0}")]
    Missing(&'static str),
    #[error("unsupported execution environment {field} value {value}")]
    UnsupportedValue { field: &'static str, value: i32 },
    #[error("schema {claimed} does not describe these environment profiles (expected {expected})")]
    SchemaMismatch { claimed: u32, expected: u32 },
    #[error("generic profile cannot represent the requested historical identity")]
    IdentityEncoding,
    #[error("invalid execution contract: {0}")]
    Contract(#[from] ExecutionContractIdParseError),
    #[error("invalid boot artifact role: {0}")]
    Role(#[from] ArtifactComponentRoleParseError),
    #[error("invalid boot artifact format: {0}")]
    Format(#[from] ArtifactFormatParseError),
    #[error("invalid execution environment field: {0}")]
    Field(#[from] ExecutionProtoError),
    #[error("invalid execution environment: {0}")]
    Descriptor(#[from] EnvironmentDescriptorError),
    #[error("invalid environment digest: {0}")]
    Digest(#[from] DigestParseError),
    #[error("environment reference {claimed} does not match descriptor {computed}")]
    ReferenceMismatch {
        claimed: EnvironmentReference,
        computed: EnvironmentReference,
    },
}

impl EnvironmentDefinition {
    pub fn new(descriptor: EnvironmentDescriptor) -> Self {
        let hashed = digest(&SHA256, canonical_descriptor(&descriptor).as_bytes());
        let mut bytes = [0; 32];
        bytes.copy_from_slice(hashed.as_ref());
        Self {
            reference: EnvironmentReference::new(Digest::sha256(bytes)),
            descriptor,
            legacy_descriptor: None,
        }
    }

    pub fn schema_version(&self) -> u32 {
        let RuntimeBinding::ManagedRelease(release) = &self.descriptor.spec().runtime;
        match release.layout {
            RuntimePayloadLayout::FirecrackerReleaseV1 => ENVIRONMENT_SCHEMA_VERSION,
            RuntimePayloadLayout::CatalogReleaseV1 => RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION,
        }
    }

    pub fn reference(&self) -> EnvironmentReference {
        self.reference
    }
    pub fn descriptor(&self) -> &EnvironmentDescriptor {
        &self.descriptor
    }

    fn check_schema(self, claimed: u32) -> Result<Self, EnvironmentDecodeError> {
        let expected = self.schema_version();
        if claimed != expected {
            return Err(EnvironmentDecodeError::SchemaMismatch { claimed, expected });
        }
        Ok(self)
    }

    fn check_reference(
        self,
        claimed: EnvironmentReference,
    ) -> Result<Self, EnvironmentDecodeError> {
        if claimed != self.reference {
            return Err(EnvironmentDecodeError::ReferenceMismatch {
                claimed,
                computed: self.reference,
            });
        }
        Ok(self)
    }
}

/// Fixed field order, ASCII keys and canonical scalar spellings, final newline.
/// Identifiers cannot contain newlines or '='. No wire/JSON bytes, observations,
/// labels, source URLs, or node-local paths participate. See docs/execution-environments.md.
fn canonical_descriptor(descriptor: &EnvironmentDescriptor) -> String {
    let spec = descriptor.spec();
    let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
    let (
        BootProfile::LinuxDirectV1 { kernel },
        ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format },
    ) = (&spec.boot, &spec.compatibility)
    else {
        return profiles::canonical_descriptor(descriptor);
    };
    format!(
        concat!(
            "space.execution.environment\nschema={}\n",
            "host.os={}\nhost.arch={}\nbackend={}\nguest.os={}\nguest.arch={}\n",
            "runtime.provider=managed_release\nruntime.kind={}\nruntime.version={}\nruntime.digest={}\n",
            "runtime.target.os={}\nruntime.target.arch={}\nruntime.layout={}\n",
            "boot.profile=linux_direct_v1\nboot.kernel.kind={}\nboot.kernel.version={}\nboot.kernel.digest={}\n",
            "isolation={}\ncompatibility.profile=firecracker_snapshot_v1\n",
            "compatibility.state.format={}\ncompatibility.state.version={}\n"
        ),
        ENVIRONMENT_SCHEMA_VERSION,
        spec.host.os,
        spec.host.architecture,
        spec.backend,
        spec.guest.os,
        spec.guest.architecture,
        release.runtime.kind,
        release.runtime.version,
        release.digest,
        release.target.os,
        release.target.architecture,
        release.layout,
        kernel.kind,
        kernel.version,
        kernel.digest,
        spec.isolation,
        state_format.format,
        state_format.version
    )
}

fn required<T>(value: Option<T>, field: &'static str) -> Result<T, EnvironmentDecodeError> {
    value.ok_or(EnvironmentDecodeError::Missing(field))
}
fn unsupported(field: &'static str, value: i32) -> EnvironmentDecodeError {
    EnvironmentDecodeError::UnsupportedValue { field, value }
}

impl From<HostPlatform> for proto::HostPlatform {
    fn from(value: HostPlatform) -> Self {
        Self {
            os: match value.os {
                HostOperatingSystem::Linux => proto::HostOperatingSystem::Linux,
                HostOperatingSystem::Windows => proto::HostOperatingSystem::Windows,
                HostOperatingSystem::Macos => proto::HostOperatingSystem::Macos,
            } as i32,
            architecture: proto::CpuArchitecture::from(value.architecture) as i32,
        }
    }
}
impl TryFrom<&proto::HostPlatform> for HostPlatform {
    type Error = EnvironmentDecodeError;
    fn try_from(value: &proto::HostPlatform) -> Result<Self, Self::Error> {
        Ok(Self {
            os: match proto::HostOperatingSystem::try_from(value.os) {
                Ok(proto::HostOperatingSystem::Linux) => HostOperatingSystem::Linux,
                Ok(proto::HostOperatingSystem::Windows) => HostOperatingSystem::Windows,
                Ok(proto::HostOperatingSystem::Macos) => HostOperatingSystem::Macos,
                _ => return Err(unsupported("host OS", value.os)),
            },
            architecture: proto::CpuArchitecture::try_from(value.architecture)
                .map_err(|_| unsupported("host architecture", value.architecture))?
                .try_into()?,
        })
    }
}

impl From<&EnvironmentDefinition> for proto::EnvironmentDefinition {
    fn from(value: &EnvironmentDefinition) -> Self {
        let spec = value.descriptor.spec();
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        Self {
            schema_version: RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION,
            identity_schema_version: if value.identity_schema_version()
                == RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION
            {
                0
            } else {
                value.identity_schema_version()
            },
            reference: value.reference.to_string(),
            descriptor: Some(proto::EnvironmentDescriptor {
                host: Some(spec.host.into()),
                guest: Some(spec.guest.into()),
                runtime: Some(proto::RuntimeBinding {
                    provider: Some(proto::runtime_binding::Provider::ManagedRelease(
                        proto::ManagedRuntimeRelease {
                            runtime: Some((&release.runtime).into()),
                            digest: release.digest.to_string(),
                            target: Some(release.target.into()),
                            layout: proto::RuntimePayloadLayout::CatalogReleaseV1 as i32,
                        },
                    )),
                }),
                backend: match spec.backend {
                    HypervisorBackend::Kvm => proto::HypervisorBackend::Kvm,
                } as i32,
                isolation: match spec.isolation {
                    IsolationClass::HardwareVm => proto::IsolationClass::HardwareVm,
                } as i32,
                boot: Some((&spec.boot).into()),
                compatibility: Some((&spec.compatibility).into()),
            }),
        }
    }
}

impl TryFrom<&proto::EnvironmentDefinition> for EnvironmentDefinition {
    type Error = EnvironmentDecodeError;
    fn try_from(value: &proto::EnvironmentDefinition) -> Result<Self, Self::Error> {
        if value.schema_version != RUNTIME_NEUTRAL_ENVIRONMENT_SCHEMA_VERSION {
            return Err(EnvironmentDecodeError::UnsupportedSchema(
                value.schema_version,
            ));
        }
        let spec = required(value.descriptor.as_ref(), "descriptor")?;
        let proto::runtime_binding::Provider::ManagedRelease(release) = required(
            required(spec.runtime.as_ref(), "runtime")?
                .provider
                .as_ref(),
            "runtime provider",
        )?;
        let descriptor = EnvironmentSpec {
            host: required(spec.host.as_ref(), "host")?.try_into()?,
            guest: required(spec.guest.as_ref(), "guest")?.try_into()?,
            backend: match proto::HypervisorBackend::try_from(spec.backend) {
                Ok(proto::HypervisorBackend::Kvm) => HypervisorBackend::Kvm,
                _ => return Err(unsupported("backend", spec.backend)),
            },
            isolation: match proto::IsolationClass::try_from(spec.isolation) {
                Ok(proto::IsolationClass::HardwareVm) => IsolationClass::HardwareVm,
                _ => return Err(unsupported("isolation", spec.isolation)),
            },
            runtime: RuntimeBinding::ManagedRelease(ManagedRuntimeRelease {
                runtime: required(release.runtime.as_ref(), "release runtime")?.try_into()?,
                digest: release.digest.parse()?,
                target: required(release.target.as_ref(), "release target")?.try_into()?,
                layout: match proto::RuntimePayloadLayout::try_from(release.layout) {
                    Ok(proto::RuntimePayloadLayout::CatalogReleaseV1) => {
                        RuntimePayloadLayout::CatalogReleaseV1
                    }
                    _ => return Err(unsupported("release layout", release.layout)),
                },
            }),
            boot: required(spec.boot.as_ref(), "boot")?.try_into()?,
            compatibility: required(spec.compatibility.as_ref(), "compatibility")?.try_into()?,
        }
        .try_into()?;
        let identity = if value.identity_schema_version == 0 {
            value.schema_version
        } else {
            value.identity_schema_version
        };
        Self::from_representation(descriptor, value.schema_version, identity)?
            .check_reference(value.reference.parse()?)?
            .into_runtime_neutral()
    }
}

#[cfg(feature = "serde")]
mod document;
#[cfg(feature = "serde")]
pub use document::EnvironmentDocumentError;

#[cfg(test)]
mod tests;
