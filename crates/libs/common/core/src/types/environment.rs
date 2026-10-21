//! Immutable execution definitions, independent of node observations and selection policy.
//!
//! A descriptor identifies required bytes and execution semantics. Parsing one
//! does not qualify it for execution; the node's approved catalog does that.

use std::{fmt, str::FromStr};

use super::{
    Arch, ComponentIdentity, Digest, DigestParseError, GuestPlatform, RuntimeIdentity,
    StateFormatIdentity,
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
vocabulary!(#[derive(Ord, PartialOrd)] RuntimePayloadLayout, RuntimePayloadLayoutParseError, { FirecrackerReleaseV1 => "firecracker_release_v1" });

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
    LinuxDirectV1 { kernel: ComponentIdentity },
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionCompatibilityProfile {
    FirecrackerSnapshotV1 { state_format: StateFormatIdentity },
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
    #[error(
        "the implemented boot/backend/compatibility profiles require Firecracker on Linux with a same-architecture Linux guest"
    )]
    UnsupportedCombination,
    #[error("direct Linux boot requires a kernel component")]
    BootComponent,
    #[error("the Firecracker compatibility profile requires firecracker-snapshot state")]
    StateFormat,
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

    fn try_from(spec: EnvironmentSpec) -> Result<Self, Self::Error> {
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
        if release.runtime.kind != RuntimeKind::Firecracker
            || spec.host.os != HostOperatingSystem::Linux
            || spec.guest.os != GuestOperatingSystem::Linux
            || spec.guest.architecture != spec.host.architecture
        {
            return Err(EnvironmentDescriptorError::UnsupportedCombination);
        }
        let BootProfile::LinuxDirectV1 { kernel } = &spec.boot;
        if kernel.kind != ArtifactComponentKind::Kernel {
            return Err(EnvironmentDescriptorError::BootComponent);
        }
        let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
            &spec.compatibility;
        if state_format.format.as_str() != FIRECRACKER_STATE_FORMAT {
            return Err(EnvironmentDescriptorError::StateFormat);
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
    EXECUTION_POLICY_SCHEMA_VERSION, ExecutionPolicyObservation, ExecutionPolicyObservationError,
};
