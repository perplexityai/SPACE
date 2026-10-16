//! Durable node-local ownership for sandbox restore operations.
//!
//! These types are shared by the sandbox-manager persistence and service
//! layers, but remain local to sandbox-manager because the control plane does
//! not participate in node-local artifact staging.

use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::types::ExactStorageUri;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreOrigin {
    Local,
    Remote,
}

impl RestoreOrigin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Remote => "remote",
        }
    }
}

impl fmt::Display for RestoreOrigin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown restore origin {0:?}")]
pub struct RestoreOriginParseError(String);

impl FromStr for RestoreOrigin {
    type Err = RestoreOriginParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "local" => Ok(Self::Local),
            "remote" => Ok(Self::Remote),
            other => Err(RestoreOriginParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestorePhase {
    MetadataStaging,
    ArtifactStaging,
    Activation,
}

impl RestorePhase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MetadataStaging => "metadata_staging",
            Self::ArtifactStaging => "artifact_staging",
            Self::Activation => "activation",
        }
    }

    pub const fn is_remote_staging(self) -> bool {
        matches!(self, Self::MetadataStaging | Self::ArtifactStaging)
    }
}

impl fmt::Display for RestorePhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown restore phase {0:?}")]
pub struct RestorePhaseParseError(String);

impl FromStr for RestorePhase {
    type Err = RestorePhaseParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "metadata_staging" => Ok(Self::MetadataStaging),
            "artifact_staging" => Ok(Self::ArtifactStaging),
            "activation" => Ok(Self::Activation),
            other => Err(RestorePhaseParseError(other.to_string())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteRestoreOperation {
    phase: RestorePhase,
    source: ExactStorageUri,
}

impl RemoteRestoreOperation {
    fn new(phase: RestorePhase, source: ExactStorageUri) -> Option<Self> {
        phase.is_remote_staging().then_some(Self { phase, source })
    }

    pub fn metadata_staging(source: ExactStorageUri) -> Self {
        Self {
            phase: RestorePhase::MetadataStaging,
            source,
        }
    }

    pub fn artifact_staging(source: ExactStorageUri) -> Self {
        Self {
            phase: RestorePhase::ArtifactStaging,
            source,
        }
    }

    pub const fn phase(&self) -> RestorePhase {
        self.phase
    }

    pub const fn source(&self) -> &ExactStorageUri {
        &self.source
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error(
    "restore origin {origin:?}, phase {phase:?}, and storage source presence {has_source} form an invalid operation"
)]
pub struct RestoreOperationShapeError {
    origin: Option<RestoreOrigin>,
    phase: Option<RestorePhase>,
    has_source: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreOperation {
    LocalActivation,
    Remote(RemoteRestoreOperation),
}

impl RestoreOperation {
    pub(crate) fn from_persisted(
        origin: Option<RestoreOrigin>,
        phase: Option<RestorePhase>,
        source: Option<ExactStorageUri>,
    ) -> Result<Option<Self>, RestoreOperationShapeError> {
        let has_source = source.is_some();
        match (origin, phase, source) {
            (None, None, None) => Ok(None),
            (Some(RestoreOrigin::Local), Some(RestorePhase::Activation), None) => {
                Ok(Some(Self::LocalActivation))
            }
            (Some(RestoreOrigin::Remote), Some(phase), Some(source)) => {
                match RemoteRestoreOperation::new(phase, source) {
                    Some(operation) => Ok(Some(Self::Remote(operation))),
                    None => Err(RestoreOperationShapeError {
                        origin: Some(RestoreOrigin::Remote),
                        phase: Some(phase),
                        has_source,
                    }),
                }
            }
            (origin, phase, _) => Err(RestoreOperationShapeError {
                origin,
                phase,
                has_source,
            }),
        }
    }

    pub const fn local_activation() -> Self {
        Self::LocalActivation
    }

    pub fn origin(&self) -> RestoreOrigin {
        match self {
            Self::LocalActivation => RestoreOrigin::Local,
            Self::Remote(_) => RestoreOrigin::Remote,
        }
    }

    pub fn phase(&self) -> RestorePhase {
        match self {
            Self::LocalActivation => RestorePhase::Activation,
            Self::Remote(operation) => operation.phase(),
        }
    }

    pub fn remote_source(&self) -> Option<&ExactStorageUri> {
        match self {
            Self::LocalActivation => None,
            Self::Remote(operation) => Some(operation.source()),
        }
    }
}
