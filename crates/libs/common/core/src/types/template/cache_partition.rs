use std::fmt;

use crate::types::{DigestParseError, EnvironmentReference};

/// Separates a rootfs digest from its environment in a partitioned build
/// directory name. Digests and environment references never contain it.
const ENVIRONMENT_SEPARATOR: char = '@';

/// Node-local cache partition that holds one template build.
///
/// A content hash names rootfs bytes, not an execution environment, so one
/// rootfs bound to two environments needs two cache entries: each has its own
/// manifest, base snapshots and btrfs subvolume.
///
/// Builds that predate environment keys stay in [`Self::Legacy`]. Those are
/// unbound builds and builds bound to the initial environment, the only one
/// nodes executed before keys existed. Their database keys, directory names
/// and btrfs `received_uuid` values are unchanged, so existing caches and
/// in-flight transfers stay valid. Every other environment gets its own
/// partition.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::*;
/// let initial = EnvironmentReference::new(Digest::sha256([1; 32]));
/// let other = EnvironmentReference::new(Digest::sha256([2; 32]));
///
/// assert_eq!(
///     TemplateCachePartition::for_environment(Some(initial), Some(initial)),
///     TemplateCachePartition::Legacy,
/// );
/// assert_eq!(
///     TemplateCachePartition::for_environment(None, Some(initial)),
///     TemplateCachePartition::Legacy,
/// );
///
/// let partition = TemplateCachePartition::for_environment(Some(other), Some(initial));
/// assert_eq!(partition.environment(), Some(other));
/// let hash = Digest::blake3([3; 32]).to_string();
/// let dir = partition.build_dir_name(&hash);
/// assert_eq!(
///     TemplateCachePartition::parse_build_dir_name(&dir),
///     Some((hash.as_str(), partition)),
/// );
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TemplateCachePartition {
    /// Unbound builds and builds bound to the initial environment.
    #[default]
    Legacy,
    /// Builds bound to any other environment.
    Environment(EnvironmentReference),
}

impl TemplateCachePartition {
    /// Selects the partition for a build bound to `environment`.
    ///
    /// `initial` is the initial environment's reference. It's a parameter
    /// because the reference is derived from the compiled runtime catalog,
    /// which this crate doesn't depend on.
    pub fn for_environment(
        environment: Option<EnvironmentReference>,
        initial: Option<EnvironmentReference>,
    ) -> Self {
        match environment {
            Some(environment) if Some(environment) != initial => Self::Environment(environment),
            _ => Self::Legacy,
        }
    }

    /// The environment this partition is dedicated to, if any.
    pub const fn environment(self) -> Option<EnvironmentReference> {
        match self {
            Self::Legacy => None,
            Self::Environment(environment) => Some(environment),
        }
    }

    /// Directory name of a build under its template directory: the content
    /// hash alone for [`Self::Legacy`], `{content_hash}@{environment}`
    /// otherwise.
    pub fn build_dir_name(self, content_hash: &str) -> String {
        match self {
            Self::Legacy => content_hash.to_string(),
            Self::Environment(environment) => {
                format!("{content_hash}{ENVIRONMENT_SEPARATOR}{environment}")
            }
        }
    }

    /// Splits a build directory name into its content hash and partition.
    /// Returns `None` for names that aren't build directories, such as
    /// staging and replacement siblings.
    pub fn parse_build_dir_name(name: &str) -> Option<(&str, Self)> {
        let (content_hash, partition) = match name.split_once(ENVIRONMENT_SEPARATOR) {
            None => (name, Self::Legacy),
            Some((content_hash, environment)) => {
                (content_hash, Self::Environment(environment.parse().ok()?))
            }
        };
        content_hash
            .parse::<crate::types::Digest>()
            .ok()
            .map(|_| (content_hash, partition))
    }

    /// Value stored in node-local SQLite key columns. The empty string is the
    /// legacy partition so rows written before environment keys keep their
    /// identity.
    pub fn key(self) -> String {
        self.environment()
            .map(|environment| environment.to_string())
            .unwrap_or_default()
    }

    /// Parses a value written by [`Self::key`].
    pub fn from_key(key: &str) -> Result<Self, DigestParseError> {
        if key.is_empty() {
            return Ok(Self::Legacy);
        }
        key.parse().map(Self::Environment)
    }
}

impl fmt::Display for TemplateCachePartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Legacy => f.write_str("legacy"),
            Self::Environment(environment) => environment.fmt(f),
        }
    }
}

#[cfg(test)]
mod tests;
