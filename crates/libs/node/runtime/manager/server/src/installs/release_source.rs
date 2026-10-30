//! Release download sources and delivery packaging.
//!
//! Each source serves one runtime's payloads. The installer authenticates
//! downloads against the catalog's pinned SHA-256 and materializes only the
//! runtime layout's executables. Archive checksums published beside downloads
//! share their origin and are not the install's trust root.

use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use futures_util::Stream;
use pplx_space_common_core::types::{Arch, Digest, RuntimeIdentity, RuntimeKind};

use super::source::BoxError;

/// Firecracker's GitHub release downloads.
pub const FIRECRACKER_RELEASE_SOURCE: &str =
    "https://github.com/firecracker-microvm/firecracker/releases/download";

/// Cloud Hypervisor's static executable release downloads.
pub const CLOUD_HYPERVISOR_RELEASE_SOURCE: &str =
    "https://github.com/cloud-hypervisor/cloud-hypervisor/releases/download";

/// An `https://` URL with a host and no userinfo, query or fragment: a release
/// source's download base or an archive under it. The path can be extended with
/// [`ReleaseUrl::join`] without re-parsing.
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ReleaseUrl(String);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error(
    "invalid release location; expected an https:// URL with a host and no userinfo, query or fragment"
)]
pub struct ReleaseUrlParseError;

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
        let invalid = || ReleaseUrlParseError;
        let uri: http::Uri = value.parse().map_err(|_| invalid())?;
        if uri.scheme_str() != Some("https")
            || uri.host().is_none_or(str::is_empty)
            || uri.query().is_some()
            || uri
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || value.contains('#')
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

/// A release payload and its pinned checksum: everything an
/// install needs, and the request a release install job carries.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerifiableRelease {
    pub release: RuntimeIdentity,
    pub architecture: Arch,
    pub archive: ReleaseUrl,
    /// The downloaded payload's SHA-256, as pinned.
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

/// Delivery packaging, independent of the installed executable layout.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReleasePayload {
    TarGz {
        members: Vec<ReleaseArchiveMember>,
    },
    /// One executable, with no archive wrapper. The runtime layout must name one binary.
    Executable,
}

/// A mapping from a catalog layout path to a member in the download archive.
/// The installer checks `binary` against the runtime layout before using it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReleaseArchiveMember {
    pub binary: String,
    pub member: String,
}

/// Where release payloads come from. The runtime manager ships Firecracker's
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

    /// Packaging and required members. Paths written on the node come only
    /// from the runtime's catalog layout, never from archive contents.
    fn payload(&self, release: &RuntimeIdentity, architecture: Arch) -> ReleasePayload;

    /// The payload's bytes, in order. The stream is not bounded as a whole:
    /// the caller bounds how long it waits for each chunk.
    async fn open(&self, archive: &ReleaseUrl) -> Result<ReleaseChunks, ReleaseSourceError>;
}

/// Map the compiled Firecracker layout to the upstream archive's member names.
#[cfg(any(test, feature = "github"))]
pub(super) fn firecracker_payload(release: &RuntimeIdentity, architecture: Arch) -> ReleasePayload {
    use pplx_space_node_runtime_core::runtime_inventory::diagnostic::FIRECRACKER_RELEASE_LAYOUT;
    let version = release.version.as_str();
    ReleasePayload::TarGz {
        members: FIRECRACKER_RELEASE_LAYOUT
            .binaries
            .iter()
            .map(|binary| {
                let name = binary.rsplit_once('/').map_or(*binary, |(_, name)| name);
                ReleaseArchiveMember {
                    binary: (*binary).to_owned(),
                    member: format!(
                        "release-v{version}-{architecture}/{name}-v{version}-{architecture}"
                    ),
                }
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests;
