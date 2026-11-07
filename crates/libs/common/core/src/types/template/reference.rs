use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use pplx_space_execution_spec::runtime::RuntimeKind;

use super::build::TemplateBuildId;
use super::digest::{TemplateArtifactDigest, TemplateArtifactDigestPrefix, TemplateIndexDigest};
use super::hash::{
    ContentHash, DigestParseError, DigestPrefixParseError, DigestPrefixResolveError, HashAlg,
};
use super::identity::{
    DEFAULT_TEMPLATE_TAG, TemplateName, TemplateParseError, TemplateTag, TemplateTagParseError,
};

/// Error returned when parsing a template reference string.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::TemplateReference;
/// assert!(TemplateReference::from_str("seed:latest").is_ok());
/// assert!(TemplateReference::from_str("seed:v1@sha256:abc").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum TemplateReferenceParseError {
    #[error("template reference must not be empty")]
    Empty,
    #[error("template reference must not combine a tag selector and digest selector")]
    TagAndDigest,
    #[error("resolved template reference must use an index digest selector")]
    ExpectedIndexDigest,
    #[error("invalid template name: {0}")]
    InvalidName(#[source] TemplateParseError),
    #[error("invalid template tag: {0}")]
    InvalidTag(#[source] TemplateTagParseError),
    #[error("invalid template index digest: {0}")]
    InvalidIndexDigest(#[source] DigestParseError),
    #[error("invalid template artifact digest: {0}")]
    InvalidArtifactDigest(#[source] DigestParseError),
    #[error("invalid template artifact digest prefix: {0}")]
    InvalidArtifactDigestPrefix(#[source] DigestPrefixParseError),
}

/// Error returned when resolving a template reference digest selector.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum TemplateReferenceResolveError {
    #[error("template reference must use an artifact digest or artifact digest prefix selector")]
    ExpectedDigestSelector,
    #[error(transparent)]
    DigestPrefix(#[from] DigestPrefixResolveError),
}

/// Selector portion of a template reference.
///
/// A selector is either a mutable cross-runtime tag (`:latest`) or a
/// runtime-specific artifact digest (`@sha256:...`). Digest selectors still need
/// a [`RuntimeKind`] to identify the concrete runtime variant.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::{TemplateSelector, TemplateTag};
/// let selector = TemplateSelector::Tag(TemplateTag::latest());
/// assert_eq!(selector.to_string(), "latest");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TemplateSelector {
    /// Mutable alias resolved at lookup time.
    Tag(TemplateTag),
    /// Immutable runtime-specific template artifact digest.
    ArtifactDigest(TemplateArtifactDigest),
    /// Short prefix of an immutable runtime-specific template artifact digest.
    ArtifactDigestPrefix(TemplateArtifactDigestPrefix),
}

impl TemplateSelector {
    /// Returns the default `latest` tag selector.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::TemplateSelector;
    /// assert_eq!(TemplateSelector::latest().to_string(), "latest");
    /// ```
    pub fn latest() -> Self {
        Self::Tag(TemplateTag::latest())
    }

    /// Returns the tag selector, when this selector is tag-based.
    pub fn tag(&self) -> Option<&TemplateTag> {
        match self {
            Self::Tag(tag) => Some(tag),
            Self::ArtifactDigest(_) | Self::ArtifactDigestPrefix(_) => None,
        }
    }

    /// Returns the artifact digest selector, when this selector is full-digest based.
    pub fn artifact_digest(&self) -> Option<TemplateArtifactDigest> {
        match self {
            Self::Tag(_) | Self::ArtifactDigestPrefix(_) => None,
            Self::ArtifactDigest(hash) => Some(*hash),
        }
    }

    /// Returns the artifact digest prefix selector, when this selector is prefix-based.
    pub fn artifact_digest_prefix(&self) -> Option<&TemplateArtifactDigestPrefix> {
        match self {
            Self::Tag(_) | Self::ArtifactDigest(_) => None,
            Self::ArtifactDigestPrefix(prefix) => Some(prefix),
        }
    }
}

impl Default for TemplateSelector {
    fn default() -> Self {
        Self::latest()
    }
}

impl fmt::Display for TemplateSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tag(tag) => tag.fmt(f),
            Self::ArtifactDigest(hash) => hash.fmt(f),
            Self::ArtifactDigestPrefix(prefix) => prefix.fmt(f),
        }
    }
}

impl FromStr for TemplateSelector {
    type Err = TemplateReferenceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.contains(':') {
            return parse_template_artifact_digest_selector(value);
        }

        TemplateTag::parse(value)
            .map(Self::Tag)
            .map_err(TemplateReferenceParseError::InvalidTag)
    }
}

fn parse_template_artifact_digest_selector(
    value: &str,
) -> Result<TemplateSelector, TemplateReferenceParseError> {
    let (algorithm, hex) =
        value
            .split_once(':')
            .ok_or(TemplateReferenceParseError::InvalidArtifactDigest(
                DigestParseError::MissingAlgorithmSeparator,
            ))?;
    let algorithm = HashAlg::parse(algorithm).map_err(|_| {
        TemplateReferenceParseError::InvalidArtifactDigestPrefix(
            DigestPrefixParseError::UnsupportedAlgorithm(algorithm.to_string()),
        )
    })?;

    if hex.len() == algorithm.digest_hex_len() {
        TemplateArtifactDigest::from_str(value)
            .map(TemplateSelector::ArtifactDigest)
            .map_err(TemplateReferenceParseError::InvalidArtifactDigest)
    } else {
        TemplateArtifactDigestPrefix::from_str(value)
            .map(TemplateSelector::ArtifactDigestPrefix)
            .map_err(TemplateReferenceParseError::InvalidArtifactDigestPrefix)
    }
}

impl TryFrom<String> for TemplateSelector {
    type Error = TemplateReferenceParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateSelector> for String {
    fn from(value: TemplateSelector) -> Self {
        value.to_string()
    }
}

/// Runtime-independent input reference for a template lookup.
///
/// This is the string users type for the template axis only. Runtime selection
/// stays separate from this reference. Tags are cross-runtime logical template
/// aliases; digest selectors are runtime-specific artifact digests and must be
/// resolved with a [`RuntimeKind`].
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::TemplateReference;
/// assert_eq!(
///     TemplateReference::from_str("seed").unwrap().to_string(),
///     "seed:latest",
/// );
/// assert!(TemplateReference::from_str("seed@sha256:0000000000000000000000000000000000000000000000000000000000000000").is_ok());
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateReference {
    name: TemplateName,
    selector: TemplateSelector,
}

impl TemplateReference {
    /// Creates a template reference from a name and selector.
    pub const fn new(name: TemplateName, selector: TemplateSelector) -> Self {
        Self { name, selector }
    }

    /// Creates a `name:latest` reference.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::{TemplateName, TemplateReference};
    /// let reference = TemplateReference::latest(TemplateName::parse("seed").unwrap());
    /// assert_eq!(reference.to_string(), "seed:latest");
    /// ```
    pub fn latest(name: TemplateName) -> Self {
        Self::new(name, TemplateSelector::latest())
    }

    /// Creates a template reference from a name and tag selector.
    pub const fn with_tag(name: TemplateName, tag: TemplateTag) -> Self {
        Self::new(name, TemplateSelector::Tag(tag))
    }

    /// Creates a template reference from a name and artifact digest selector.
    pub const fn with_artifact_digest(name: TemplateName, digest: TemplateArtifactDigest) -> Self {
        Self::new(name, TemplateSelector::ArtifactDigest(digest))
    }

    /// Creates a template reference from a name and artifact digest prefix selector.
    pub const fn with_artifact_digest_prefix(
        name: TemplateName,
        prefix: TemplateArtifactDigestPrefix,
    ) -> Self {
        Self::new(name, TemplateSelector::ArtifactDigestPrefix(prefix))
    }

    /// Returns the template name.
    pub const fn name(&self) -> &TemplateName {
        &self.name
    }

    /// Returns the selector.
    pub const fn selector(&self) -> &TemplateSelector {
        &self.selector
    }

    /// Returns the tag selector, when this reference is tag-based.
    pub fn tag(&self) -> Option<&TemplateTag> {
        self.selector.tag()
    }

    /// Returns true when this reference uses the default `latest` selector.
    pub fn uses_default_selector(&self) -> bool {
        self.tag()
            .map(|tag| tag.as_str() == DEFAULT_TEMPLATE_TAG)
            .unwrap_or(false)
    }

    /// Returns the artifact digest selector, when this reference is digest-based.
    pub fn artifact_digest(&self) -> Option<TemplateArtifactDigest> {
        self.selector.artifact_digest()
    }

    /// Returns the artifact digest prefix selector, when this reference is prefix-based.
    pub fn artifact_digest_prefix(&self) -> Option<&TemplateArtifactDigestPrefix> {
        self.selector.artifact_digest_prefix()
    }

    /// Resolves a full artifact digest or artifact-prefix selector to an exact variant.
    ///
    /// Full digest selectors resolve without consulting `candidates`. Prefix
    /// selectors must match exactly one candidate artifact digest from the
    /// current repository/index scope. Tag selectors are intentionally rejected
    /// because tag lookup is a separate mutable-name resolution step.
    pub fn resolve_artifact_selector<I>(
        self,
        runtime: RuntimeKind,
        candidates: I,
    ) -> Result<ResolvedTemplateVariant, TemplateReferenceResolveError>
    where
        I: IntoIterator<Item = TemplateArtifactDigest>,
    {
        match self.selector {
            TemplateSelector::ArtifactDigest(digest) => {
                Ok(ResolvedTemplateVariant::new(runtime, self.name, digest))
            }
            TemplateSelector::ArtifactDigestPrefix(prefix) => prefix
                .resolve_unique(candidates)
                .map(|digest| ResolvedTemplateVariant::new(runtime, self.name, digest))
                .map_err(Into::into),
            TemplateSelector::Tag(_) => Err(TemplateReferenceResolveError::ExpectedDigestSelector),
        }
    }
}

impl fmt::Display for TemplateReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.selector {
            TemplateSelector::Tag(tag) => write!(f, "{}:{tag}", self.name),
            TemplateSelector::ArtifactDigest(digest) => write!(f, "{}@{digest}", self.name),
            TemplateSelector::ArtifactDigestPrefix(prefix) => write!(f, "{}@{prefix}", self.name),
        }
    }
}

impl FromStr for TemplateReference {
    type Err = TemplateReferenceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            return Err(TemplateReferenceParseError::Empty);
        }

        if let Some((name, digest)) = value.split_once('@') {
            if name.contains(':') {
                return Err(TemplateReferenceParseError::TagAndDigest);
            }
            let name =
                TemplateName::parse(name).map_err(TemplateReferenceParseError::InvalidName)?;
            let selector = parse_template_artifact_digest_selector(digest)?;
            return Ok(Self::new(name, selector));
        }

        if let Some((name, tag)) = value.rsplit_once(':') {
            let name =
                TemplateName::parse(name).map_err(TemplateReferenceParseError::InvalidName)?;
            let tag = TemplateTag::parse(tag).map_err(TemplateReferenceParseError::InvalidTag)?;
            return Ok(Self::new(name, TemplateSelector::Tag(tag)));
        }

        TemplateName::parse(value)
            .map(Self::latest)
            .map_err(TemplateReferenceParseError::InvalidName)
    }
}

impl TryFrom<String> for TemplateReference {
    type Error = TemplateReferenceParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<TemplateReference> for String {
    fn from(value: TemplateReference) -> Self {
        value.to_string()
    }
}

/// Runtime plus template reference parameter bundle.
///
/// This intentionally has no combined string grammar. CLI/API boundaries should
/// parse runtime and template as separate fields, then pass them together with
/// this type when a call needs both axes.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeScopedTemplateReference {
    runtime: RuntimeKind,
    reference: TemplateReference,
}

impl RuntimeScopedTemplateReference {
    /// Creates a runtime-scoped template reference from separate parsed values.
    pub const fn new(runtime: RuntimeKind, reference: TemplateReference) -> Self {
        Self { runtime, reference }
    }

    /// Returns the runtime axis.
    pub const fn runtime(&self) -> RuntimeKind {
        self.runtime
    }

    /// Returns the template reference axis.
    pub const fn reference(&self) -> &TemplateReference {
        &self.reference
    }

    /// Consumes the bundle into its parts.
    pub fn into_parts(self) -> (RuntimeKind, TemplateReference) {
        (self.runtime, self.reference)
    }
}

/// Resolved runtime-independent logical template reference.
///
/// Resolution replaces a tag selector with the immutable template index digest it
/// points to. Runtime is still a separate axis; selecting a concrete runtime
/// variant from this index produces a [`ResolvedTemplateVariant`]. Its string
/// form is explicit (`seed@index:sha256:...`) so it cannot be confused with a
/// runtime-specific artifact digest input (`seed@sha256:...`).
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResolvedTemplateReference {
    name: TemplateName,
    index_digest: TemplateIndexDigest,
}

impl ResolvedTemplateReference {
    /// Creates a resolved template reference.
    pub const fn new(name: TemplateName, index_digest: TemplateIndexDigest) -> Self {
        Self { name, index_digest }
    }

    /// Returns the resolved template name.
    pub const fn name(&self) -> &TemplateName {
        &self.name
    }

    /// Returns the immutable template index digest.
    pub const fn index_digest(&self) -> TemplateIndexDigest {
        self.index_digest
    }

    /// Returns an operator-facing short reference string.
    ///
    /// This is not canonical serialization. Use [`Display`] / [`ToString`] when
    /// an exact immutable reference must round-trip without repository lookup.
    pub fn short_string(&self) -> String {
        format!("{}@index:{}", self.name, self.index_digest.short_string())
    }
}

impl fmt::Display for ResolvedTemplateReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@index:{}", self.name, self.index_digest)
    }
}

impl FromStr for ResolvedTemplateReference {
    type Err = TemplateReferenceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some((name, digest)) = value.split_once("@index:") else {
            return Err(TemplateReferenceParseError::ExpectedIndexDigest);
        };
        if name.contains(':') {
            return Err(TemplateReferenceParseError::TagAndDigest);
        }
        let name = TemplateName::parse(name).map_err(TemplateReferenceParseError::InvalidName)?;
        let index_digest = TemplateIndexDigest::from_str(digest)
            .map_err(TemplateReferenceParseError::InvalidIndexDigest)?;
        Ok(Self::new(name, index_digest))
    }
}

impl TryFrom<String> for ResolvedTemplateReference {
    type Error = TemplateReferenceParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<ResolvedTemplateReference> for String {
    fn from(value: ResolvedTemplateReference) -> Self {
        value.to_string()
    }
}

/// Resolved runtime-specific variant of a logical template.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ResolvedTemplateVariant {
    runtime: RuntimeKind,
    name: TemplateName,
    artifact_digest: TemplateArtifactDigest,
}

impl ResolvedTemplateVariant {
    /// Creates a resolved runtime-specific template variant.
    pub const fn new(
        runtime: RuntimeKind,
        name: TemplateName,
        artifact_digest: TemplateArtifactDigest,
    ) -> Self {
        Self {
            runtime,
            name,
            artifact_digest,
        }
    }

    /// Returns the selected runtime.
    pub const fn runtime(&self) -> RuntimeKind {
        self.runtime
    }

    /// Returns the logical template name.
    pub const fn name(&self) -> &TemplateName {
        &self.name
    }

    /// Returns the runtime-specific artifact digest.
    pub const fn artifact_digest(&self) -> TemplateArtifactDigest {
        self.artifact_digest
    }

    /// Returns an operator-facing string with the artifact digest shortened.
    pub fn short_string(&self) -> String {
        format!(
            "runtime={} template={} artifact={}",
            self.runtime,
            self.name,
            self.artifact_digest.short_string()
        )
    }

    /// Returns the runtime-specific artifact digest as a generic content hash.
    pub const fn content_hash(&self) -> ContentHash {
        self.artifact_digest.content_hash()
    }

    /// Converts to the existing local build key type.
    pub fn into_build_id(self) -> TemplateBuildId {
        TemplateBuildId::from_artifact_digest(self.name, self.artifact_digest)
    }

    /// Consumes the pair into its parts.
    pub fn into_parts(self) -> (RuntimeKind, TemplateName, TemplateArtifactDigest) {
        (self.runtime, self.name, self.artifact_digest)
    }
}

impl fmt::Display for ResolvedTemplateVariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Human-readable diagnostics, not a stable parseable wire format.
        write!(
            f,
            "runtime={} template={} artifact={}",
            self.runtime, self.name, self.artifact_digest
        )
    }
}

#[cfg(test)]
mod tests;
