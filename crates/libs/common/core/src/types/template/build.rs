use std::fmt;
use std::str::FromStr;

use uuid::Uuid;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::digest::TemplateArtifactDigest;
use super::hash::ContentHash;
use super::identity::TemplateName;

pub use pplx_space_cpu_arch::{Arch, ArchParseError};

/// Error returned when a build run UUID cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::BuildRunId;
/// assert!(BuildRunId::from_str("not-a-uuid").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum BuildRunIdParseError {
    #[error("invalid build run UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// Composite key for a built runtime-specific template artifact.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::{ContentHash, Digest, TemplateBuildId, TemplateName};
/// let template = TemplateName::parse("seed").unwrap();
/// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
/// let build = TemplateBuildId::new(template, hash);
/// assert_eq!(build.content_hash(), hash);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateBuildId {
    template: TemplateName,
    artifact_digest: TemplateArtifactDigest,
}

impl TemplateBuildId {
    /// Creates an artifact key from a template name and content hash.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::{ContentHash, Digest, TemplateBuildId, TemplateName};
    /// let template = TemplateName::parse("seed").unwrap();
    /// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
    /// let build = TemplateBuildId::new(template, hash);
    /// assert_eq!(build.content_hash(), hash);
    /// ```
    pub const fn new(template: TemplateName, content_hash: ContentHash) -> Self {
        Self::from_artifact_digest(template, TemplateArtifactDigest::new(content_hash))
    }

    /// Creates an artifact key from a template name and artifact digest.
    pub const fn from_artifact_digest(
        template: TemplateName,
        artifact_digest: TemplateArtifactDigest,
    ) -> Self {
        Self {
            template,
            artifact_digest,
        }
    }

    /// Returns the human-facing template name part of this artifact key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::{ContentHash, Digest, TemplateBuildId, TemplateName};
    /// let template = TemplateName::parse("seed").unwrap();
    /// let build = TemplateBuildId::new(template, ContentHash::new(Digest::sha256([0xab; 32])));
    /// assert_eq!(build.template().as_str(), "seed");
    /// ```
    pub const fn template(&self) -> &TemplateName {
        &self.template
    }

    /// Returns the runtime-specific artifact digest part of this key.
    pub const fn artifact_digest(&self) -> TemplateArtifactDigest {
        self.artifact_digest
    }

    /// Returns the runtime-specific artifact digest as a generic content hash.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::{ContentHash, Digest, TemplateBuildId, TemplateName};
    /// let hash = ContentHash::new(Digest::sha256([0xab; 32]));
    /// let build = TemplateBuildId::new(TemplateName::parse("seed").unwrap(), hash);
    /// assert_eq!(build.content_hash(), hash);
    /// ```
    pub const fn content_hash(&self) -> ContentHash {
        self.artifact_digest.content_hash()
    }
}

/// Identity for one concrete template build execution.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::BuildRunId;
/// let run = BuildRunId::new();
/// assert_eq!(run.to_string().len(), 36);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BuildRunId(Uuid);

impl BuildRunId {
    /// Creates a fresh v7 UUID-backed build run ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::BuildRunId;
    /// let run = BuildRunId::new();
    /// assert_eq!(run.as_uuid().get_version_num(), 7);
    /// ```
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an already parsed UUID as a build run ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::BuildRunId;
    /// # use uuid::Uuid;
    /// let uuid = Uuid::nil();
    /// assert_eq!(BuildRunId::from_uuid(uuid).as_uuid(), uuid);
    /// ```
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the underlying UUID for storage or wire encoding.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::BuildRunId;
    /// let run = BuildRunId::new();
    /// assert_eq!(run.as_uuid(), run.as_uuid());
    /// ```
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for BuildRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for BuildRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BuildRunId")
            .field(&self.to_string())
            .finish()
    }
}

impl fmt::Display for BuildRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for BuildRunId {
    type Err = BuildRunIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for BuildRunId {
    type Error = BuildRunIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<BuildRunId> for String {
    fn from(value: BuildRunId) -> Self {
        value.to_string()
    }
}

/// Template build lifecycle state.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::TemplateBuildState;
/// let state = TemplateBuildState::Queued;
/// assert!(matches!(state, TemplateBuildState::Queued));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TemplateBuildState {
    Queued,
    Building,
    Ready,
    Failed,
}

/// Owner category for template artifacts.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::OwnerType;
/// assert!(matches!(OwnerType::System, OwnerType::System));
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OwnerType {
    User,
    Org,
    System,
}

/// Execution and publication state of a build-manager job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum TemplateBuildJobState {
    Pending,
    Running,
    Succeeded,
    Failed,
    PublishFailed,
    Unspecified,
}

impl TemplateBuildJobState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::PublishFailed => "publish_failed",
            Self::Unspecified => "unspecified",
        }
    }
}

impl fmt::Display for TemplateBuildJobState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid template build job state {0:?}")]
pub struct TemplateBuildJobStateParseError(String);

impl FromStr for TemplateBuildJobState {
    type Err = TemplateBuildJobStateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "publish_failed" => Ok(Self::PublishFailed),
            "unspecified" => Ok(Self::Unspecified),
            _ => Err(TemplateBuildJobStateParseError(value.to_string())),
        }
    }
}

#[cfg(test)]
mod tests;
