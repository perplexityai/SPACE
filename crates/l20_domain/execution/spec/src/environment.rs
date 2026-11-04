//! Immutable execution definitions, independent of node observations and selection policy.
//!
//! A descriptor identifies required bytes and execution semantics. Parsing one
//! does not qualify it for execution; the node's approved catalog does that.

use std::{fmt, str::FromStr};

use super::{
    Arch, ArtifactComponentRole, ArtifactFormat, ComponentIdentity, Digest, DigestParseError,
    ExecutionContractId, GuestPlatform, RuntimeIdentity, StateFormatIdentity,
};

macro_rules! vocabulary {
    ($(#[$attribute:meta])* $name:ident, $error:ident, {$($variant:ident => $text:literal),+ $(,)?}) => {
        #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
        $(#[$attribute])*
        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        pub enum $name {
            $(#[cfg_attr(feature = "serde", serde(rename = $text))] $variant),+
        }
        impl $name {
            pub const fn as_str(self) -> &'static str {
                match self { $(Self::$variant => $text),+ }
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(self.as_str()) }
        }
        #[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
        #[error("unsupported {name} {0:?}", name = stringify!($name))]
        pub struct $error(String);
        impl FromStr for $name {
            type Err = $error;
            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value { $($text => Ok(Self::$variant),)+ other => Err($error(other.to_owned())) }
            }
        }
    };
}

vocabulary!(#[derive(Ord, PartialOrd)] HostOperatingSystem, HostOperatingSystemParseError, {
    Linux => "linux", Windows => "windows", Macos => "macos",
});
vocabulary!(#[derive(Ord, PartialOrd)] HypervisorBackend, HypervisorBackendParseError, { Kvm => "kvm" });
vocabulary!(IsolationClass, IsolationClassParseError, { HardwareVm => "hardware_vm" });
vocabulary!(#[derive(Ord, PartialOrd)] RuntimePayloadLayout, RuntimePayloadLayoutParseError, {
    FirecrackerReleaseV1 => "firecracker_release_v1",
    CatalogReleaseV1 => "catalog_release_v1",
});

/// OS and architecture of the machine executing host-side runtime code.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HostPlatform {
    pub os: HostOperatingSystem,
    pub architecture: Arch,
}

/// Binary digest, not the source archive checksum. The target describes host code.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedRuntimeRelease {
    pub runtime: RuntimeIdentity,
    pub digest: Digest,
    pub target: HostPlatform,
    pub layout: RuntimePayloadLayout,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(
        tag = "provider",
        content = "release",
        rename_all = "snake_case",
        deny_unknown_fields
    )
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeBinding {
    ManagedRelease(ManagedRuntimeRelease),
}

/// Versioned boot semantics. Guest userspace and mutable per-VM state belong to
/// the template/sandbox, not this shared artifact definition.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BootProfile {
    LinuxDirectV1 {
        kernel: ComponentIdentity,
    },
    ArtifactsV1 {
        contract: ExecutionContractId,
        /// May be empty when the contract needs no separate boot files. The
        /// catalog and runtime adapter validate the contract's required inputs.
        artifacts: Vec<BootArtifact>,
    },
}

/// One immutable boot input. Its role, format and bytes are independent of
/// download packaging and the adapter's host-local paths.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootArtifact {
    role: ArtifactComponentRole,
    component: ComponentIdentity,
    format: ArtifactFormat,
}

impl BootArtifact {
    pub fn new(
        role: ArtifactComponentRole,
        component: ComponentIdentity,
        format: ArtifactFormat,
    ) -> Self {
        Self {
            role,
            component,
            format,
        }
    }
    pub fn role(&self) -> &ArtifactComponentRole {
        &self.role
    }
    pub fn component(&self) -> &ComponentIdentity {
        &self.component
    }
    pub fn format(&self) -> &ArtifactFormat {
        &self.format
    }
}

impl BootProfile {
    pub const LINUX_DIRECT_ROLE: &'static str = "boot";
    pub const LINUX_DIRECT_FORMAT: &'static str = "vmlinux";

    pub fn components(&self) -> impl Iterator<Item = (&str, &ComponentIdentity, &str)> {
        let (direct, artifacts): (_, &[BootArtifact]) = match self {
            Self::LinuxDirectV1 { kernel } => (
                Some((Self::LINUX_DIRECT_ROLE, kernel, Self::LINUX_DIRECT_FORMAT)),
                &[],
            ),
            Self::ArtifactsV1 { artifacts, .. } => (None, artifacts),
        };
        direct.into_iter().chain(artifacts.iter().map(|artifact| {
            (
                artifact.role.as_str(),
                &artifact.component,
                artifact.format.as_str(),
            )
        }))
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionCompatibilityProfile {
    FirecrackerSnapshotV1 {
        state_format: StateFormatIdentity,
    },
    RuntimeV1 {
        contract: ExecutionContractId,
        state_format: Option<StateFormatIdentity>,
    },
}

impl ExecutionCompatibilityProfile {
    pub fn state_format(&self) -> Option<&StateFormatIdentity> {
        match self {
            Self::FirecrackerSnapshotV1 { state_format } => Some(state_format),
            Self::RuntimeV1 { state_format, .. } => state_format.as_ref(),
        }
    }
}

/// Inputs to a descriptor. All fields are required; no absent OS or runtime
/// binding acquires a default during decoding.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(deny_unknown_fields))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentSpec {
    pub host: HostPlatform,
    pub guest: GuestPlatform,
    pub runtime: RuntimeBinding,
    pub backend: HypervisorBackend,
    pub boot: BootProfile,
    pub isolation: IsolationClass,
    pub compatibility: ExecutionCompatibilityProfile,
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum EnvironmentDescriptorError {
    #[error("managed runtime target {target:?} differs from required host {host:?}")]
    RuntimeTarget {
        target: HostPlatform,
        host: HostPlatform,
    },
    #[error("KVM requires a Linux host, got {0}")]
    KvmHost(HostOperatingSystem),
    #[error("KVM requires the guest architecture {guest} to match the host {host}")]
    KvmArchitecture { host: Arch, guest: Arch },
    #[error(
        "the implemented boot/backend/compatibility profiles require Firecracker on Linux with a same-architecture Linux guest"
    )]
    UnsupportedCombination,
    #[error("direct Linux boot requires a kernel component")]
    BootComponent,
    #[error("the Firecracker compatibility profile requires firecracker-snapshot state")]
    StateFormat,
    #[error("legacy and runtime-neutral environment profiles cannot be mixed")]
    MixedProfiles,
    #[error("duplicate boot artifact role {0}")]
    DuplicateBootRole(ArtifactComponentRole),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnvironmentDescriptor(EnvironmentSpec);

impl EnvironmentDescriptor {
    pub fn spec(&self) -> &EnvironmentSpec {
        &self.0
    }
}

impl TryFrom<EnvironmentSpec> for EnvironmentDescriptor {
    type Error = EnvironmentDescriptorError;

    fn try_from(mut spec: EnvironmentSpec) -> Result<Self, Self::Error> {
        use super::{
            ArtifactComponentKind, FIRECRACKER_STATE_FORMAT, GuestOperatingSystem, RuntimeKind,
        };
        let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
        if release.target != spec.host {
            return Err(EnvironmentDescriptorError::RuntimeTarget {
                target: release.target,
                host: spec.host,
            });
        }
        match spec.backend {
            HypervisorBackend::Kvm if spec.host.os != HostOperatingSystem::Linux => {
                return Err(EnvironmentDescriptorError::KvmHost(spec.host.os));
            }
            HypervisorBackend::Kvm if spec.guest.architecture != spec.host.architecture => {
                return Err(EnvironmentDescriptorError::KvmArchitecture {
                    host: spec.host.architecture,
                    guest: spec.guest.architecture,
                });
            }
            HypervisorBackend::Kvm => {}
        }
        match (&release.layout, &mut spec.boot, &spec.compatibility) {
            (
                RuntimePayloadLayout::CatalogReleaseV1,
                BootProfile::ArtifactsV1 { artifacts, .. },
                ExecutionCompatibilityProfile::RuntimeV1 { .. },
            ) => {
                artifacts.sort_by(|a, b| a.role.cmp(&b.role));
                for (left, right) in artifacts.iter().zip(artifacts.iter().skip(1)) {
                    if left.role == right.role {
                        return Err(EnvironmentDescriptorError::DuplicateBootRole(
                            left.role.clone(),
                        ));
                    }
                }
                // Structural validation is not qualification. Only the trusted
                // catalog and an implementing adapter may authorize a contract.
                return Ok(Self(spec));
            }
            (
                RuntimePayloadLayout::FirecrackerReleaseV1,
                BootProfile::LinuxDirectV1 { kernel },
                ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format },
            ) => {
                if kernel.kind != ArtifactComponentKind::Kernel {
                    return Err(EnvironmentDescriptorError::BootComponent);
                }
                if state_format.format.as_str() != FIRECRACKER_STATE_FORMAT {
                    return Err(EnvironmentDescriptorError::StateFormat);
                }
            }
            _ => return Err(EnvironmentDescriptorError::MixedProfiles),
        }
        if release.runtime.kind != RuntimeKind::Firecracker
            || spec.guest.os != GuestOperatingSystem::Linux
        {
            return Err(EnvironmentDescriptorError::UnsupportedCombination);
        }
        Ok(Self(spec))
    }
}

/// Digest of the full canonical environment, distinct from an artifact digest.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EnvironmentReference(Digest);

impl EnvironmentReference {
    pub const fn new(digest: Digest) -> Self {
        Self(digest)
    }
    pub const fn digest(self) -> Digest {
        self.0
    }
}
impl fmt::Display for EnvironmentReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for EnvironmentReference {
    type Err = DigestParseError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}
impl TryFrom<String> for EnvironmentReference {
    type Error = DigestParseError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}
impl From<EnvironmentReference> for String {
    fn from(value: EnvironmentReference) -> Self {
        value.to_string()
    }
}

mod policy;
pub use policy::{
    EXECUTION_POLICY_APPLIED_SCHEMA_VERSION, EXECUTION_POLICY_SCHEMA_VERSION,
    ExecutionPolicyObservation, ExecutionPolicyObservationError,
};
