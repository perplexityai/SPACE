//! Immutable auxiliary boot inputs. Qualification and boot roles belong to the
//! environment; installing a pinned file does not approve it for execution.
pub const BOOT_ARTIFACT_IMAGE: &str = "image";

use pplx_space_common_core::types::{Arch, ArtifactComponentKind, ComponentIdentity};

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "BootArtifactPin", into = "BootArtifactPin")]
pub struct PinnedBootArtifact {
    identity: ComponentIdentity,
    architecture: Arch,
    size: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum BootArtifactPinError {
    #[error("no auxiliary boot-file adapter for component kind {0}")]
    Kind(ArtifactComponentKind),
    #[error("boot artifact size must be positive")]
    Empty,
    #[error("boot artifact version must be a non-hidden file name")]
    Version,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BootArtifactPin {
    identity: ComponentIdentity,
    architecture: Arch,
    size: u64,
}

impl PinnedBootArtifact {
    pub fn new(
        identity: ComponentIdentity,
        architecture: Arch,
        size: u64,
    ) -> Result<Self, BootArtifactPinError> {
        if !matches!(identity.kind, ArtifactComponentKind::Firmware) {
            return Err(BootArtifactPinError::Kind(identity.kind));
        }
        if identity.version.as_str().starts_with('.')
            || !matches!(
                std::path::Path::new(identity.version.as_str())
                    .components()
                    .collect::<Vec<_>>()
                    .as_slice(),
                [std::path::Component::Normal(_)]
            )
        {
            return Err(BootArtifactPinError::Version);
        }
        if size == 0 {
            return Err(BootArtifactPinError::Empty);
        }
        Ok(Self {
            identity,
            architecture,
            size,
        })
    }
    pub fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }
    pub fn architecture(&self) -> Arch {
        self.architecture
    }
    pub fn size(&self) -> u64 {
        self.size
    }
}

impl TryFrom<BootArtifactPin> for PinnedBootArtifact {
    type Error = BootArtifactPinError;
    fn try_from(pin: BootArtifactPin) -> Result<Self, Self::Error> {
        Self::new(pin.identity, pin.architecture, pin.size)
    }
}
impl From<PinnedBootArtifact> for BootArtifactPin {
    fn from(pin: PinnedBootArtifact) -> Self {
        Self {
            identity: pin.identity,
            architecture: pin.architecture,
            size: pin.size,
        }
    }
}
