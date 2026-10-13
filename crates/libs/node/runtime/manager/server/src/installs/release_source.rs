//! Where release archives come from, and the vocabulary for them.
//!
//! A release source serves one runtime's release archives. Firecracker's,
//! the one the runtime manager ships, is its GitHub releases:
//! `<base>/v<version>/firecracker-v<version>-<arch>.tgz`. Callers name a
//! release; the runtime manager downloads it, checks the archive against the
//! catalog's pinned SHA-256 (see
//! [`pplx_space_node_runtime_core::catalog`]), and unpacks only the binaries
//! the runtime's release layout names. The checksum a source publishes
//! beside an archive comes from the same origin as the bytes, so it is not
//! what an install trusts.

use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use futures_util::Stream;
use pplx_space_common_core::types::{Arch, Digest, RuntimeIdentity, RuntimeKind};

use super::source::BoxError;

/// Firecracker's GitHub release downloads.
pub const FIRECRACKER_RELEASE_SOURCE: &str =
    "https://github.com/firecracker-microvm/firecracker/releases/download";

/// An `https://` URL with a host and no query: a release source's download
/// base or an archive under it. The path can be extended with
/// [`ReleaseUrl::join`] without re-parsing.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReleaseUrl(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid release location '{0}'; expected an https:// URL with a host and no query")]
pub struct ReleaseUrlParseError(String);

impl ReleaseUrl {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// This location with `/segment` appended. Segments come from validated
    /// identifiers (versions, architectures), which hold no whitespace, so
    /// the result is a valid location too.
    pub fn join(&self, segment: &str) -> Self {
        Self(format!("{}/{segment}", self.0.trim_end_matches('/')))
    }
}

impl FromStr for ReleaseUrl {
    type Err = ReleaseUrlParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || ReleaseUrlParseError(value.to_string());
        let uri: http::Uri = value.parse().map_err(|_| invalid())?;
        if uri.scheme_str() != Some("https")
            || uri.host().is_none_or(str::is_empty)
            || uri.query().is_some()
        {
            return Err(invalid());
        }
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for ReleaseUrl {
    type Error = ReleaseUrlParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ReleaseUrl> for String {
    fn from(archive: ReleaseUrl) -> Self {
        archive.0
    }
}

impl std::fmt::Display for ReleaseUrl {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A release archive and the checksum its source published: everything an
/// install needs, and the request a release install job carries.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerifiableRelease {
    pub release: RuntimeIdentity,
    pub architecture: Arch,
    pub archive: ReleaseUrl,
    /// The archive's SHA-256, as pinned.
    pub sha256: Digest,
}

/// Why the source could not serve a release.
#[derive(Debug, thiserror::Error)]
pub enum ReleaseSourceError {
    #[error("fetching {location} failed")]
    Fetch {
        location: ReleaseUrl,
        #[source]
        source: BoxError,
    },
    #[error("{location} did not answer within {}s", after.as_secs())]
    TimedOut {
        location: ReleaseUrl,
        after: Duration,
    },
    #[error("{location} is not served by this source")]
    Foreign { location: ReleaseUrl },
}

pub type ReleaseChunks =
    Pin<Box<dyn Stream<Item = Result<bytes::Bytes, ReleaseSourceError>> + Send>>;

/// Where release archives come from. The runtime manager ships Firecracker's
/// GitHub releases behind the `github` feature; a deployment can supply its
/// own source. Calls that wait on the network must be bounded by the source.
#[async_trait::async_trait]
pub trait ReleaseSource: Send + Sync {
    /// The runtime whose releases the source publishes.
    fn runtime(&self) -> RuntimeKind;

    /// The source's root, for listings and records.
    fn root(&self) -> &ReleaseUrl;

    /// Where `release`'s archive for `architecture` is.
    fn archive(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleaseUrl;

    /// The path inside the archive of the binary the release layout names
    /// `binary`, such as `firecracker`.
    fn archive_member(&self, release: &RuntimeIdentity, architecture: Arch, binary: &str)
    -> String;

    /// The archive's bytes, in order. The stream is not bounded as a whole:
    /// the caller bounds how long it waits for each chunk.
    async fn open(&self, archive: &ReleaseUrl) -> Result<ReleaseChunks, ReleaseSourceError>;
}

#[cfg(test)]
mod tests;
