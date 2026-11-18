use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::hash::{
    ContentHash, Digest, DigestParseError, DigestPrefix, DigestPrefixParseError,
    DigestPrefixResolveError,
};

/// Immutable digest of a logical template release index.
///
/// A template index can contain multiple runtime-specific artifacts. For
/// example, `seed@index:sha256:...` points to the exact logical `seed` release;
/// a runtime selector is still needed to choose the Firecracker, Cloud
/// Hypervisor, or gVisor artifact inside it.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateIndexDigest(ContentHash);

impl TemplateIndexDigest {
    /// Wraps a generic content hash as a template index digest.
    pub const fn new(value: ContentHash) -> Self {
        Self(value)
    }

    /// Returns the underlying generic content hash.
    pub const fn content_hash(self) -> ContentHash {
        self.0
    }

    /// Returns the underlying typed digest.
    pub const fn digest(self) -> Digest {
        self.0.digest()
    }

    /// Returns the default short digest prefix for operator-facing display.
    pub fn short_prefix(self) -> TemplateIndexDigestPrefix {
        TemplateIndexDigestPrefix::new(self.0.short_prefix())
    }

    /// Returns a short digest string such as `sha256:0123456789ab`.
    pub fn short_string(self) -> String {
        self.short_prefix().to_string()
    }
}

impl fmt::Display for TemplateIndexDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateIndexDigest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ContentHash::from_str(value).map(Self)
    }
}

impl TryFrom<String> for TemplateIndexDigest {
    type Error = DigestParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateIndexDigest> for String {
    fn from(value: TemplateIndexDigest) -> Self {
        value.to_string()
    }
}

impl From<ContentHash> for TemplateIndexDigest {
    fn from(value: ContentHash) -> Self {
        Self(value)
    }
}

impl From<TemplateIndexDigest> for ContentHash {
    fn from(value: TemplateIndexDigest) -> Self {
        value.0
    }
}

/// Short, non-canonical prefix of a logical template index digest.
///
/// A prefix is an operator convenience, not an immutable content address. It
/// must be resolved against a repository/index and must match exactly one full
/// digest before it can become a [`TemplateIndexDigest`].
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateIndexDigestPrefix(DigestPrefix);

impl TemplateIndexDigestPrefix {
    /// Wraps a generic digest prefix as a template index digest prefix.
    pub const fn new(value: DigestPrefix) -> Self {
        Self(value)
    }

    /// Returns the underlying generic digest prefix.
    pub const fn digest_prefix(&self) -> &DigestPrefix {
        &self.0
    }

    /// Returns whether this prefix matches the given full template index digest.
    pub fn matches(&self, digest: TemplateIndexDigest) -> bool {
        self.0.matches(digest.digest())
    }

    /// Resolves this prefix against candidate full digests.
    ///
    /// The caller should pass candidates already scoped to the template name and
    /// repository/index being queried. A successful match must be unique.
    pub fn resolve_unique<I>(
        &self,
        candidates: I,
    ) -> Result<TemplateIndexDigest, DigestPrefixResolveError>
    where
        I: IntoIterator<Item = TemplateIndexDigest>,
    {
        self.0
            .resolve_unique(candidates, |candidate| candidate.digest())
    }
}

impl fmt::Display for TemplateIndexDigestPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateIndexDigestPrefix {
    type Err = DigestPrefixParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        DigestPrefix::from_str(value).map(Self)
    }
}

impl TryFrom<String> for TemplateIndexDigestPrefix {
    type Error = DigestPrefixParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateIndexDigestPrefix> for String {
    fn from(value: TemplateIndexDigestPrefix) -> Self {
        value.to_string()
    }
}

/// Immutable digest of a runtime-specific template artifact.
///
/// This is the digest used after selecting a runtime variant from a logical
/// template release. For example,
/// `runtime=firecracker template=seed artifact=sha256:...` identifies one
/// concrete Firecracker artifact.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateArtifactDigest(ContentHash);

impl TemplateArtifactDigest {
    /// Wraps a generic content hash as a runtime-specific template artifact digest.
    pub const fn new(value: ContentHash) -> Self {
        Self(value)
    }

    /// Returns the underlying generic content hash.
    pub const fn content_hash(self) -> ContentHash {
        self.0
    }

    /// Returns the underlying typed digest.
    pub const fn digest(self) -> Digest {
        self.0.digest()
    }

    /// Returns the default short digest prefix for operator-facing display.
    pub fn short_prefix(self) -> TemplateArtifactDigestPrefix {
        TemplateArtifactDigestPrefix::new(self.0.short_prefix())
    }

    /// Returns a short digest string such as `sha256:0123456789ab`.
    pub fn short_string(self) -> String {
        self.short_prefix().to_string()
    }
}

impl fmt::Display for TemplateArtifactDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateArtifactDigest {
    type Err = DigestParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        ContentHash::from_str(value).map(Self)
    }
}

impl TryFrom<String> for TemplateArtifactDigest {
    type Error = DigestParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateArtifactDigest> for String {
    fn from(value: TemplateArtifactDigest) -> Self {
        value.to_string()
    }
}

impl From<ContentHash> for TemplateArtifactDigest {
    fn from(value: ContentHash) -> Self {
        Self(value)
    }
}

impl From<TemplateArtifactDigest> for ContentHash {
    fn from(value: TemplateArtifactDigest) -> Self {
        value.0
    }
}

/// Short, non-canonical prefix of a runtime-specific template artifact digest.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateArtifactDigestPrefix(DigestPrefix);

impl TemplateArtifactDigestPrefix {
    /// Wraps a generic digest prefix as a template artifact digest prefix.
    pub const fn new(value: DigestPrefix) -> Self {
        Self(value)
    }

    /// Returns the underlying generic digest prefix.
    pub const fn digest_prefix(&self) -> &DigestPrefix {
        &self.0
    }

    /// Returns whether this prefix matches the given full template artifact digest.
    pub fn matches(&self, digest: TemplateArtifactDigest) -> bool {
        self.0.matches(digest.digest())
    }

    /// Resolves this prefix against candidate full digests.
    pub fn resolve_unique<I>(
        &self,
        candidates: I,
    ) -> Result<TemplateArtifactDigest, DigestPrefixResolveError>
    where
        I: IntoIterator<Item = TemplateArtifactDigest>,
    {
        self.0
            .resolve_unique(candidates, |candidate| candidate.digest())
    }
}

impl fmt::Display for TemplateArtifactDigestPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateArtifactDigestPrefix {
    type Err = DigestPrefixParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        DigestPrefix::from_str(value).map(Self)
    }
}

impl TryFrom<String> for TemplateArtifactDigestPrefix {
    type Error = DigestPrefixParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateArtifactDigestPrefix> for String {
    fn from(value: TemplateArtifactDigestPrefix) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests;
