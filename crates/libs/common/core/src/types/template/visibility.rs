use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use pplx_space_accounts::OrgId;

/// Wire spelling for [`TemplateVisibility::Generic`].
const TEMPLATE_VISIBILITY_GENERIC: &str = "generic";
/// Wire spelling for [`TemplateVisibility::OrgSpecific`].
const TEMPLATE_VISIBILITY_ORG_SPECIFIC: &str = "org_specific";

/// Error returned when a template visibility identifier cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_common_core::types::TemplateVisibility;
/// assert!(TemplateVisibility::from_str("public").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported template visibility '{0}'")]
pub struct TemplateVisibilityParseError(String);

/// Who may request a template at sandbox-create time.
///
/// Visibility is orthogonal to ownership: `org_id IS NULL` on a template means
/// *system-owned*, not *shared*. An org can author a template and publish it as
/// generic, so the sharing intent is carried explicitly by this value instead
/// of being inferred from the presence of an owner.
///
/// There is intentionally no `Default` impl: defaulting a missed read to
/// [`TemplateVisibility::Generic`] would silently grant cross-org access, so
/// every consumer must read the stored value explicitly.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_common_core::types::TemplateVisibility;
/// let visibility = TemplateVisibility::parse("org_specific").unwrap();
/// assert_eq!(visibility, TemplateVisibility::OrgSpecific);
/// assert_eq!(visibility.to_string(), "org_specific");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum TemplateVisibility {
    /// Any org may request the template.
    Generic,
    /// Only members of the owning org may request the template.
    OrgSpecific,
}

impl TemplateVisibility {
    /// Returns the canonical wire spelling of this visibility.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::TemplateVisibility;
    /// assert_eq!(TemplateVisibility::Generic.as_str(), "generic");
    /// assert_eq!(TemplateVisibility::OrgSpecific.as_str(), "org_specific");
    /// ```
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generic => TEMPLATE_VISIBILITY_GENERIC,
            Self::OrgSpecific => TEMPLATE_VISIBILITY_ORG_SPECIFIC,
        }
    }

    /// Parses a visibility identifier from its wire spelling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_common_core::types::TemplateVisibility;
    /// assert_eq!(
    ///     TemplateVisibility::parse("generic").unwrap(),
    ///     TemplateVisibility::Generic,
    /// );
    /// assert!(TemplateVisibility::parse("shared").is_err());
    /// ```
    pub fn parse(value: &str) -> Result<Self, TemplateVisibilityParseError> {
        match value {
            TEMPLATE_VISIBILITY_GENERIC => Ok(Self::Generic),
            TEMPLATE_VISIBILITY_ORG_SPECIFIC => Ok(Self::OrgSpecific),
            other => Err(TemplateVisibilityParseError(other.to_string())),
        }
    }

    /// Returns whether a caller from `caller_org` may use a template with
    /// this visibility owned by `owner_org`.
    ///
    /// Generic templates are usable by any org. Org-specific templates are
    /// usable only by the owning org; an org-specific template without an
    /// owner permits no one (the schema's `org_specific_requires_owner`
    /// CHECK keeps that state out of the database, so this arm is pure
    /// defense in depth).
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_accounts::OrgId;
    /// # use pplx_space_common_core::types::TemplateVisibility;
    /// let owner = OrgId::new();
    /// let stranger = OrgId::new();
    /// assert!(TemplateVisibility::Generic.permits(None, stranger));
    /// assert!(TemplateVisibility::OrgSpecific.permits(Some(owner), owner));
    /// assert!(!TemplateVisibility::OrgSpecific.permits(Some(owner), stranger));
    /// assert!(!TemplateVisibility::OrgSpecific.permits(None, stranger));
    /// ```
    pub fn permits(self, owner_org: Option<OrgId>, caller_org: OrgId) -> bool {
        match self {
            Self::Generic => true,
            Self::OrgSpecific => owner_org == Some(caller_org),
        }
    }
}

impl fmt::Display for TemplateVisibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TemplateVisibility {
    type Err = TemplateVisibilityParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for TemplateVisibility {
    type Error = TemplateVisibilityParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<TemplateVisibility> for String {
    fn from(value: TemplateVisibility) -> Self {
        value.to_string()
    }
}

// sqlx codecs: the operator node-group surface decodes `templates.visibility`
// into this enum (`CompatTemplateRow` / `TemplateVisibilityRow` in
// `routes/admin/`), so the type binds directly against the TEXT column.
#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for TemplateVisibility {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for TemplateVisibility {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for TemplateVisibility {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::parse(raw).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
