use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Default tag used when a template reference omits an explicit selector.
pub const DEFAULT_TEMPLATE_TAG: &str = "latest";

/// Template label used for node-local sandbox artifacts recovered without metadata.
pub const ORPHANED_SANDBOX_TEMPLATE: &str = "unknown";

/// Stable database identity for a template entity.
///
/// Template names are human-facing handles and may participate in user-facing
/// references. Template IDs are app-owned UUIDv7 values used for entity age and
/// retention semantics.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateId(Uuid);

impl TemplateId {
    /// Creates a fresh UUIDv7-backed template ID.
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an already parsed UUID as a template ID.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    /// Returns the underlying UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl Default for TemplateId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TemplateId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse::<Uuid>().map(Self)
    }
}

impl From<TemplateId> for Uuid {
    fn from(value: TemplateId) -> Self {
        value.0
    }
}

impl From<TemplateId> for String {
    fn from(value: TemplateId) -> Self {
        value.to_string()
    }
}

impl TryFrom<String> for TemplateId {
    type Error = uuid::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

/// Error returned by template and artifact name parsers.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_template_model::TemplateName;
/// assert!(TemplateName::parse("../seed").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum TemplateParseError {
    #[error("path component must not be empty")]
    Empty,
    #[error("path component must be at most {max} bytes, got {actual}")]
    TooLong { max: usize, actual: usize },
    #[error("path component must contain only ASCII alphanumeric characters, '.', '_', or '-'")]
    InvalidComponent,
}

/// Error returned when parsing an OCI-style template tag.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_template_model::TemplateTag;
/// assert!(TemplateTag::parse("release-2026.05").is_ok());
/// assert!(TemplateTag::parse(":latest").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct TemplateTagParseError(&'static str);

/// Human-facing template identifier that is safe to use as a path component.
///
/// Template names contain only ASCII alphanumeric characters plus `.`, `_`,
/// and `-`, and are no longer than 255 bytes.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_template_model::TemplateName;
/// let template = TemplateName::parse("seed").unwrap();
/// assert_eq!(template.as_str(), "seed");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateName(String);

impl TemplateName {
    /// Parses a non-empty, allowlisted template name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_template_model::TemplateName;
    /// assert!(TemplateName::parse("seed").is_ok());
    /// assert!(TemplateName::parse("..").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, TemplateParseError> {
        let value = value.into();
        validate_component(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated template name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_template_model::TemplateName;
    /// let template = TemplateName::parse("seed").unwrap();
    /// assert_eq!(template.as_str(), "seed");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TemplateName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateName {
    type Err = TemplateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for TemplateName {
    type Error = TemplateParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<TemplateName> for String {
    fn from(value: TemplateName) -> Self {
        value.0
    }
}

/// Mutable alias for a template build, following OCI tag character rules.
///
/// Tags are human-facing pointers such as `latest`, `prod`, or
/// `py312-2026.05`. They are distinct from immutable content hashes.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_template_model::TemplateTag;
/// let tag = TemplateTag::parse("latest").unwrap();
/// assert_eq!(tag.as_str(), "latest");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct TemplateTag(String);

impl TemplateTag {
    /// Parses a tag using OCI's conservative tag grammar.
    ///
    /// Tags are 1-128 ASCII bytes, start with `[A-Za-z0-9_]`, and then
    /// contain only `[A-Za-z0-9_.-]`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_template_model::TemplateTag;
    /// assert!(TemplateTag::parse("release_1.2-rc1").is_ok());
    /// assert!(TemplateTag::parse("-bad").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, TemplateTagParseError> {
        let value = value.into();
        validate_tag(&value)?;
        Ok(Self(value))
    }

    /// Returns the default `latest` tag.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_template_model::{DEFAULT_TEMPLATE_TAG, TemplateTag};
    /// assert_eq!(TemplateTag::latest().as_str(), DEFAULT_TEMPLATE_TAG);
    /// ```
    pub fn latest() -> Self {
        Self(DEFAULT_TEMPLATE_TAG.to_string())
    }

    /// Returns the validated tag string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_template_model::TemplateTag;
    /// let tag = TemplateTag::parse("prod").unwrap();
    /// assert_eq!(tag.as_str(), "prod");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TemplateTag {
    fn default() -> Self {
        Self::latest()
    }
}

impl fmt::Display for TemplateTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for TemplateTag {
    type Err = TemplateTagParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for TemplateTag {
    type Error = TemplateTagParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<TemplateTag> for String {
    fn from(value: TemplateTag) -> Self {
        value.0
    }
}

fn validate_component(value: &str) -> Result<(), TemplateParseError> {
    if value.is_empty() {
        return Err(TemplateParseError::Empty);
    }
    if value.len() > 255 {
        return Err(TemplateParseError::TooLong {
            max: 255,
            actual: value.len(),
        });
    }
    if value == "." || value == ".." || !value.bytes().all(is_template_name_byte) {
        return Err(TemplateParseError::InvalidComponent);
    }
    Ok(())
}

fn is_template_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

fn validate_tag(value: &str) -> Result<(), TemplateTagParseError> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(TemplateTagParseError("template tag must not be empty"));
    }
    if bytes.len() > 128 {
        return Err(TemplateTagParseError(
            "template tag must be at most 128 bytes",
        ));
    }
    let Some((first, rest)) = bytes.split_first() else {
        return Err(TemplateTagParseError("template tag must not be empty"));
    };
    if !first.is_ascii_alphanumeric() && *first != b'_' {
        return Err(TemplateTagParseError(
            "template tag must start with ASCII alphanumeric or '_'",
        ));
    }
    if !rest
        .iter()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'_' | b'.' | b'-'))
    {
        return Err(TemplateTagParseError(
            "template tag may contain only ASCII alphanumeric, '_', '.', or '-'",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
