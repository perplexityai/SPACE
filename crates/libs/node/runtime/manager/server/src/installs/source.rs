//! What the kernel source publishes, and the vocabulary for it.
//!
//! A kernel source publishes guest kernels for one runtime, keyed by the
//! runtime's release line and architecture. Firecracker's, the one the
//! runtime manager ships, keys them
//! `firecracker-ci/v<major>.<minor>/<arch>/vmlinux-<kernel version>` in a
//! public bucket. Callers name a runtime line and a kernel version; the
//! runtime manager lists, downloads, and verifies.
//!
//! Kernels are verified against the catalog's size and SHA-256 pins. An S3
//! single-part entity tag additionally supplies an MD5 transfer checksum;
//! multipart entity tags are not content digests and leave MD5 unset.

use std::pin::Pin;
use std::str::FromStr;
use std::time::Duration;

use futures_util::Stream;
use pplx_space_common_core::types::{
    Arch, ArtifactVersion, Digest, RuntimeIdentity, RuntimeKind, RuntimeVersion,
    RuntimeVersionParseError, StorageUri,
};

/// Firecracker's public CI artifact bucket. Path-style, because the bucket's
/// name contains dots, which virtual-hosted TLS cannot serve.
pub const FIRECRACKER_CI_KERNEL_SOURCE: &str =
    "s3://spec.ccfc.min/firecracker-ci?region=us-east-1&path_style=true";

/// A release line of a runtime, `1.15`: all patch releases in a line use
/// the same catalog of guest kernels.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct ReleaseLine {
    major: u64,
    minor: u64,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid release '{0}'; expected a line such as 1.15 or a release such as 1.15.1")]
pub struct ReleaseLineParseError(String);

impl ReleaseLine {
    pub const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }
}

impl FromStr for ReleaseLine {
    type Err = ReleaseLineParseError;

    /// Accepts a line (`1.15`) or a release (`1.15.1`), with or without `v`.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || ReleaseLineParseError(value.to_string());
        let digits =
            |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
        let unprefixed = value.strip_prefix('v').unwrap_or(value);
        let mut parts = unprefixed.split('.');
        let (Some(major), Some(minor)) = (parts.next(), parts.next()) else {
            return Err(invalid());
        };
        let patch_ok = match parts.next() {
            None => true,
            Some(patch) => digits(patch) && parts.next().is_none(),
        };
        if !digits(major) || !digits(minor) || !patch_ok {
            return Err(invalid());
        }
        Ok(Self {
            major: major.parse().map_err(|_| invalid())?,
            minor: minor.parse().map_err(|_| invalid())?,
        })
    }
}

impl TryFrom<&str> for ReleaseLine {
    type Error = ReleaseLineParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for ReleaseLine {
    type Error = ReleaseLineParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ReleaseLine> for String {
    fn from(line: ReleaseLine) -> Self {
        line.to_string()
    }
}

impl std::fmt::Display for ReleaseLine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

/// A runtime and one of its release lines, `firecracker 1.15`: what a
/// kernel is published for.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct RuntimeLine {
    pub runtime: RuntimeKind,
    pub release: ReleaseLine,
}

impl RuntimeLine {
    pub const fn new(runtime: RuntimeKind, release: ReleaseLine) -> Self {
        Self { runtime, release }
    }

    /// The line as a runtime identity whose version is the line, `1.15`.
    pub fn identity(self) -> Result<RuntimeIdentity, RuntimeVersionParseError> {
        Ok(RuntimeIdentity {
            kind: self.runtime,
            version: RuntimeVersion::parse(self.release.to_string())?,
        })
    }
}

impl std::fmt::Display for RuntimeLine {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{} {}", self.runtime, self.release)
    }
}

/// A kernel version as the source names it (`6.1.155`, `5.10.245-no-acpi`),
/// which is also the kernel's install directory name and the version the
/// execution inventory reports for it.
#[derive(
    Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(try_from = "String", into = "String")]
pub struct KernelVersion(ArtifactVersion);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid kernel version '{0}'")]
pub struct KernelVersionParseError(String);

impl FromStr for KernelVersion {
    type Err = KernelVersionParseError;

    /// Dotted numbers, then optionally a lowercase variant suffix:
    /// `6.1.155`, `5.10.245-no-acpi`. Names the source keeps beside kernels,
    /// such as `<version>.config`, do not parse.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || KernelVersionParseError(value.to_string());
        let (release, variant) = match value.split_once('-') {
            Some((release, variant)) => (release, Some(variant)),
            None => (value, None),
        };
        let numeric_parts = release
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit()));
        let plain_variant = variant.is_none_or(|variant| {
            variant.split('-').all(|part| {
                !part.is_empty()
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
            })
        });
        if !numeric_parts || !plain_variant {
            return Err(invalid());
        }
        ArtifactVersion::parse(value)
            .map(Self)
            .map_err(|_| invalid())
    }
}

impl TryFrom<&str> for KernelVersion {
    type Error = KernelVersionParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for KernelVersion {
    type Error = KernelVersionParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<KernelVersion> for String {
    fn from(version: KernelVersion) -> Self {
        version.0.into()
    }
}

impl AsRef<str> for KernelVersion {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl KernelVersion {
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    /// The version as the shared artifact vocabulary, and the catalog, name it.
    pub fn artifact_version(&self) -> &ArtifactVersion {
        &self.0
    }

    /// The kernel release without the variant: `5.10.245` for
    /// `5.10.245-no-acpi`. What the image's own banner names.
    pub fn release(&self) -> &str {
        let version = self.as_str();
        version
            .split_once('-')
            .map_or(version, |(release, _)| release)
    }
}

impl std::fmt::Display for KernelVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// The MD5 of a kernel image, as the source publishes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct ContentMd5([u8; 16]);

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("invalid MD5 '{0}'; expected 32 lowercase hexadecimal digits")]
pub struct ContentMd5ParseError(String);

impl ContentMd5 {
    pub const fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The MD5 an S3 entity tag carries, when it is one: 32 hex digits,
    /// quoted. A multipart upload's tag (`<hex>-<parts>`) is not a content
    /// digest.
    pub fn from_etag(etag: &str) -> Option<Self> {
        etag.trim_matches('"').parse().ok()
    }
}

impl FromStr for ContentMd5 {
    type Err = ContentMd5ParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = || ContentMd5ParseError(value.to_string());
        if value.len() != 32
            || !value
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(invalid());
        }
        let mut digest = [0u8; 16];
        for (index, byte) in digest.iter_mut().enumerate() {
            let pair = value.get(index * 2..index * 2 + 2).ok_or_else(invalid)?;
            *byte = u8::from_str_radix(pair, 16).map_err(|_| invalid())?;
        }
        Ok(Self(digest))
    }
}

impl TryFrom<String> for ContentMd5 {
    type Error = ContentMd5ParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<ContentMd5> for String {
    fn from(md5: ContentMd5) -> Self {
        md5.to_string()
    }
}

impl std::fmt::Display for ContentMd5 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One kernel the source lists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceKernel {
    pub version: KernelVersion,
    pub line: RuntimeLine,
    pub architecture: Arch,
    /// The object itself: its key is the URI's key prefix.
    pub location: StorageUri,
    pub size: u64,
    /// The published MD5, when the entity tag is one.
    pub md5: Option<ContentMd5>,
}

impl SourceKernel {
    /// Capture the authorized digest in the download request. Source transfer
    /// metadata cannot change that identity on a retry.
    pub(crate) fn download_request(&self, digest: Digest) -> VerifiableKernel {
        VerifiableKernel {
            digest,
            version: self.version.clone(),
            line: self.line,
            architecture: self.architecture,
            location: self.location.clone(),
            size: self.size,
            md5: self.md5,
        }
    }
}

/// A listed kernel whose bytes can be checked: everything an install needs,
/// and the request a kernel install job carries.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerifiableKernel {
    /// Immutable catalog identity captured when the job is accepted.
    pub digest: Digest,
    pub version: KernelVersion,
    pub line: RuntimeLine,
    pub architecture: Arch,
    #[serde(with = "storage_uri_text")]
    pub location: StorageUri,
    pub size: u64,
    pub md5: Option<ContentMd5>,
}

/// `StorageUri` in its `s3://` text form, which round-trips through parsing.
pub(super) mod storage_uri_text {
    use pplx_space_common_core::types::StorageUri;

    pub(in crate::installs) fn serialize<S: serde::Serializer>(
        uri: &StorageUri,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_str(uri)
    }

    pub(in crate::installs) fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<StorageUri, D::Error> {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Why the source could not list or serve kernels. Sources wrap their
/// transport's own error as the cause.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("listing {location} failed")]
    List {
        location: Box<StorageUri>,
        #[source]
        source: BoxError,
    },
    #[error("downloading {location} failed")]
    Download {
        location: Box<StorageUri>,
        #[source]
        source: BoxError,
    },
    #[error("{location} did not answer within {}s", after.as_secs())]
    TimedOut {
        location: Box<StorageUri>,
        after: Duration,
    },
    #[error("{location} is not an object in this source")]
    Foreign { location: Box<StorageUri> },
    /// The listing ran past what a kernel line holds.
    #[error("listing {location} passed {limit} entries; refusing a listing that large")]
    ListingTooLarge {
        location: Box<StorageUri>,
        limit: usize,
    },
}

pub type ByteChunks = Pin<Box<dyn Stream<Item = Result<bytes::Bytes, SourceError>> + Send>>;

/// Where kernels come from. The runtime manager ships an S3 source behind
/// the `s3` feature; a deployment can supply its own through
/// `KernelSourceConfig::Custom`. Calls that wait on the network must be
/// bounded by the source.
#[async_trait::async_trait]
pub trait KernelSource: Send + Sync {
    /// The source's root, for listings and records.
    fn root(&self) -> &StorageUri;

    /// The runtime whose kernels the source publishes. Callers ask only
    /// for its lines.
    fn runtime(&self) -> RuntimeKind;

    /// Every kernel the source lists for `line`, a line of
    /// [`KernelSource::runtime`], on `architecture`.
    async fn list(
        &self,
        line: RuntimeLine,
        architecture: Arch,
    ) -> Result<Vec<SourceKernel>, SourceError>;

    /// The bytes of `kernel`, in order. The stream is not bounded as a
    /// whole: the caller bounds how long it waits for each chunk.
    async fn open(&self, kernel: &VerifiableKernel) -> Result<ByteChunks, SourceError>;
}

/// The architecture this build runs on.
pub(crate) fn host_arch() -> Option<Arch> {
    match std::env::consts::ARCH {
        "x86_64" => Some(Arch::X86_64),
        "aarch64" => Some(Arch::Aarch64),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
