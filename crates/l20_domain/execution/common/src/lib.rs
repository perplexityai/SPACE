//! Shared protobuf conversions for portable execution metadata.

use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole, ArtifactFormat,
    ArtifactVersion, Digest, ExecutionMetadata, GuestOperatingSystem, GuestPlatform,
    RuntimeIdentity, RuntimeKind, RuntimeVersion, StateFormatIdentity, StateFormatName,
    StateFormatVersion,
};

pub mod environment;
pub mod policy;

pub mod proto {
    tonic::include_proto!("space.execution.v1");
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutionProtoError {
    #[error("execution metadata is missing runtime identity")]
    MissingRuntime,
    #[error("execution metadata is missing guest platform")]
    MissingGuest,
    #[error("runtime kind is unspecified")]
    UnspecifiedRuntimeKind,
    #[error("runtime kind value {0} is unknown to this binary")]
    UnknownRuntimeKind(i32),
    #[error("guest operating system is unspecified")]
    UnspecifiedGuestOperatingSystem,
    #[error("guest operating system value {0} is unknown to this binary")]
    UnknownGuestOperatingSystem(i32),
    #[error("CPU architecture is unspecified")]
    UnspecifiedCpuArchitecture,
    #[error("CPU architecture value {0} is unknown to this binary")]
    UnknownCpuArchitecture(i32),
    #[error("artifact component kind is unspecified")]
    UnspecifiedArtifactComponentKind,
    #[error("artifact component kind value {0} is unknown to this binary")]
    UnknownArtifactComponentKind(i32),
    #[error("state format capability is missing identity")]
    MissingStateFormatIdentity,
    #[error("invalid execution metadata: {0}")]
    InvalidMetadata(#[from] pplx_space_common_core::types::ExecutionMetadataError),
    #[error("invalid runtime version: {0}")]
    RuntimeVersion(#[from] pplx_space_common_core::types::RuntimeVersionParseError),
    #[error("invalid state format name: {0}")]
    StateFormatName(#[from] pplx_space_common_core::types::StateFormatNameParseError),
    #[error("invalid state format version: {0}")]
    StateFormatVersion(#[from] pplx_space_common_core::types::StateFormatVersionParseError),
    #[error("invalid artifact component role: {0}")]
    ComponentRole(#[from] pplx_space_common_core::types::ArtifactComponentRoleParseError),
    #[error("invalid artifact version: {0}")]
    ArtifactVersion(#[from] pplx_space_common_core::types::ArtifactVersionParseError),
    #[error("invalid artifact digest: {0}")]
    Digest(#[from] pplx_space_common_core::types::DigestParseError),
    #[error("invalid artifact format: {0}")]
    ArtifactFormat(#[from] pplx_space_common_core::types::ArtifactFormatParseError),
}

impl From<RuntimeKind> for proto::RuntimeKind {
    fn from(value: RuntimeKind) -> Self {
        match value {
            RuntimeKind::Firecracker => Self::Firecracker,
            RuntimeKind::CloudHypervisor => Self::CloudHypervisor,
            RuntimeKind::Gvisor => Self::Gvisor,
        }
    }
}

impl TryFrom<proto::RuntimeKind> for RuntimeKind {
    type Error = ExecutionProtoError;

    fn try_from(value: proto::RuntimeKind) -> Result<Self, Self::Error> {
        match value {
            proto::RuntimeKind::Firecracker => Ok(Self::Firecracker),
            proto::RuntimeKind::CloudHypervisor => Ok(Self::CloudHypervisor),
            proto::RuntimeKind::Gvisor => Ok(Self::Gvisor),
            proto::RuntimeKind::Unspecified => Err(ExecutionProtoError::UnspecifiedRuntimeKind),
        }
    }
}

impl From<GuestOperatingSystem> for proto::GuestOperatingSystem {
    fn from(value: GuestOperatingSystem) -> Self {
        match value {
            GuestOperatingSystem::Linux => Self::Linux,
            GuestOperatingSystem::Windows => Self::Windows,
            GuestOperatingSystem::Macos => Self::Macos,
        }
    }
}

impl TryFrom<proto::GuestOperatingSystem> for GuestOperatingSystem {
    type Error = ExecutionProtoError;

    fn try_from(value: proto::GuestOperatingSystem) -> Result<Self, Self::Error> {
        match value {
            proto::GuestOperatingSystem::Linux => Ok(Self::Linux),
            proto::GuestOperatingSystem::Windows => Ok(Self::Windows),
            proto::GuestOperatingSystem::Macos => Ok(Self::Macos),
            proto::GuestOperatingSystem::Unspecified => {
                Err(ExecutionProtoError::UnspecifiedGuestOperatingSystem)
            }
        }
    }
}

impl From<Arch> for proto::CpuArchitecture {
    fn from(value: Arch) -> Self {
        match value {
            Arch::X86_64 => Self::X8664,
            Arch::Aarch64 => Self::Aarch64,
        }
    }
}

impl TryFrom<proto::CpuArchitecture> for Arch {
    type Error = ExecutionProtoError;

    fn try_from(value: proto::CpuArchitecture) -> Result<Self, Self::Error> {
        match value {
            proto::CpuArchitecture::X8664 => Ok(Self::X86_64),
            proto::CpuArchitecture::Aarch64 => Ok(Self::Aarch64),
            proto::CpuArchitecture::Unspecified => {
                Err(ExecutionProtoError::UnspecifiedCpuArchitecture)
            }
        }
    }
}

impl From<ArtifactComponentKind> for proto::ArtifactComponentKind {
    fn from(value: ArtifactComponentKind) -> Self {
        match value {
            ArtifactComponentKind::Kernel => Self::Kernel,
            ArtifactComponentKind::Initrd => Self::Initrd,
            ArtifactComponentKind::Firmware => Self::Firmware,
            ArtifactComponentKind::Rootfs => Self::Rootfs,
            ArtifactComponentKind::BootDisk => Self::BootDisk,
            ArtifactComponentKind::GuestTools => Self::GuestTools,
        }
    }
}

impl TryFrom<proto::ArtifactComponentKind> for ArtifactComponentKind {
    type Error = ExecutionProtoError;

    fn try_from(value: proto::ArtifactComponentKind) -> Result<Self, Self::Error> {
        match value {
            proto::ArtifactComponentKind::Kernel => Ok(Self::Kernel),
            proto::ArtifactComponentKind::Initrd => Ok(Self::Initrd),
            proto::ArtifactComponentKind::Firmware => Ok(Self::Firmware),
            proto::ArtifactComponentKind::Rootfs => Ok(Self::Rootfs),
            proto::ArtifactComponentKind::BootDisk => Ok(Self::BootDisk),
            proto::ArtifactComponentKind::GuestTools => Ok(Self::GuestTools),
            proto::ArtifactComponentKind::Unspecified => {
                Err(ExecutionProtoError::UnspecifiedArtifactComponentKind)
            }
        }
    }
}

impl From<&RuntimeIdentity> for proto::RuntimeIdentity {
    fn from(value: &RuntimeIdentity) -> Self {
        Self {
            kind: proto::RuntimeKind::from(value.kind) as i32,
            version: value.version.to_string(),
        }
    }
}

impl TryFrom<&proto::RuntimeIdentity> for RuntimeIdentity {
    type Error = ExecutionProtoError;

    fn try_from(value: &proto::RuntimeIdentity) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: proto::RuntimeKind::try_from(value.kind)
                .map_err(|_| ExecutionProtoError::UnknownRuntimeKind(value.kind))?
                .try_into()?,
            version: RuntimeVersion::parse(&value.version)?,
        })
    }
}

impl From<GuestPlatform> for proto::GuestPlatform {
    fn from(value: GuestPlatform) -> Self {
        Self {
            os: proto::GuestOperatingSystem::from(value.os) as i32,
            architecture: proto::CpuArchitecture::from(value.architecture) as i32,
        }
    }
}

impl TryFrom<&proto::GuestPlatform> for GuestPlatform {
    type Error = ExecutionProtoError;

    fn try_from(value: &proto::GuestPlatform) -> Result<Self, Self::Error> {
        Ok(Self {
            os: proto::GuestOperatingSystem::try_from(value.os)
                .map_err(|_| ExecutionProtoError::UnknownGuestOperatingSystem(value.os))?
                .try_into()?,
            architecture: proto::CpuArchitecture::try_from(value.architecture)
                .map_err(|_| ExecutionProtoError::UnknownCpuArchitecture(value.architecture))?
                .try_into()?,
        })
    }
}

impl From<&StateFormatIdentity> for proto::StateFormatIdentity {
    fn from(value: &StateFormatIdentity) -> Self {
        Self {
            format: value.format.to_string(),
            version: value.version.to_string(),
        }
    }
}

impl TryFrom<&proto::StateFormatIdentity> for StateFormatIdentity {
    type Error = ExecutionProtoError;

    fn try_from(value: &proto::StateFormatIdentity) -> Result<Self, Self::Error> {
        Ok(Self {
            format: StateFormatName::parse(&value.format)?,
            version: StateFormatVersion::parse(&value.version)?,
        })
    }
}

impl From<&ArtifactComponentMetadata> for proto::ArtifactComponent {
    fn from(value: &ArtifactComponentMetadata) -> Self {
        Self {
            kind: proto::ArtifactComponentKind::from(value.kind) as i32,
            role: value.role.as_ref().map(ToString::to_string),
            version: value.version.as_ref().map(ToString::to_string),
            digest: value.digest.as_ref().map(ToString::to_string),
            format: value.format.as_ref().map(ToString::to_string),
        }
    }
}

impl TryFrom<&proto::ArtifactComponent> for ArtifactComponentMetadata {
    type Error = ExecutionProtoError;

    fn try_from(value: &proto::ArtifactComponent) -> Result<Self, Self::Error> {
        Ok(Self {
            kind: proto::ArtifactComponentKind::try_from(value.kind)
                .map_err(|_| ExecutionProtoError::UnknownArtifactComponentKind(value.kind))?
                .try_into()?,
            role: value
                .role
                .as_deref()
                .map(ArtifactComponentRole::parse)
                .transpose()?,
            version: value
                .version
                .as_deref()
                .map(ArtifactVersion::parse)
                .transpose()?,
            digest: value
                .digest
                .as_deref()
                .map(str::parse::<Digest>)
                .transpose()?,
            format: value
                .format
                .as_deref()
                .map(ArtifactFormat::parse)
                .transpose()?,
        })
    }
}

impl From<&ExecutionMetadata> for proto::ExecutionMetadata {
    fn from(value: &ExecutionMetadata) -> Self {
        Self {
            runtime: Some(value.runtime().into()),
            guest: Some(value.guest().into()),
            state_format: value.state_format().map(Into::into),
            components: value.components().iter().map(Into::into).collect(),
            environment: value.environment().map(|reference| reference.to_string()),
        }
    }
}

impl TryFrom<&proto::ExecutionMetadata> for ExecutionMetadata {
    type Error = ExecutionProtoError;

    fn try_from(value: &proto::ExecutionMetadata) -> Result<Self, Self::Error> {
        let runtime = value
            .runtime
            .as_ref()
            .ok_or(ExecutionProtoError::MissingRuntime)?
            .try_into()?;
        let guest = value
            .guest
            .as_ref()
            .ok_or(ExecutionProtoError::MissingGuest)?
            .try_into()?;
        let state_format = value
            .state_format
            .as_ref()
            .map(TryInto::try_into)
            .transpose()?;
        let components = value
            .components
            .iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        let metadata = ExecutionMetadata::new(runtime, guest, state_format, components)?;
        Ok(match value.environment.as_deref() {
            Some(reference) => metadata.with_environment(reference.parse()?),
            None => metadata,
        })
    }
}

#[cfg(test)]
mod tests;
