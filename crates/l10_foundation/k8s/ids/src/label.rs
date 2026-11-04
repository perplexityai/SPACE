//! Kubernetes label-value vocabulary.
//!
//! The one owner of the Kubernetes label-value grammar. `pplx-space-common-svc`
//! re-exports [`LabelValue`] from here (through `pplx-space-common-core`) so
//! services keep a single validator (and a single grammar) rather than forking
//! their own.

use std::fmt;
use std::str::FromStr;

/// Error returned when a Kubernetes label value violates the Kubernetes grammar.
///
/// ```
/// # use pplx_space_k8s_ids::LabelValue;
/// let err = LabelValue::parse("-bad").unwrap_err();
/// assert!(err.to_string().contains("kubernetes label value"));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error(
    "kubernetes label value must be empty or <= 63 chars, contain only [A-Za-z0-9._-], and begin/end with [A-Za-z0-9]"
)]
pub struct LabelValueParseError;

/// Validated Kubernetes label value.
///
/// Kubernetes allows an empty label value. Non-empty values must be at most 63
/// characters, use only `[A-Za-z0-9._-]`, and begin and end with an
/// alphanumeric character.
///
/// ```
/// # use pplx_space_k8s_ids::LabelValue;
/// let value = LabelValue::parse("sandbox-01").unwrap();
/// assert_eq!(value.as_str(), "sandbox-01");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LabelValue(String);

impl LabelValue {
    /// Parses a Kubernetes label value.
    ///
    /// ```
    /// # use pplx_space_k8s_ids::LabelValue;
    /// assert!(LabelValue::parse("").is_ok());
    /// assert!(LabelValue::parse("node.1").is_ok());
    /// assert!(LabelValue::parse("node-").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, LabelValueParseError> {
        let value = value.into();
        if value.len() > 63
            || !value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-'))
            || !has_valid_label_value_edges(&value)
        {
            return Err(LabelValueParseError);
        }
        Ok(Self(value))
    }

    /// Returns the validated label value.
    ///
    /// ```
    /// # use pplx_space_k8s_ids::LabelValue;
    /// let value = LabelValue::parse("pool_a").unwrap();
    /// assert_eq!(value.as_str(), "pool_a");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

fn has_valid_label_value_edges(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }

    value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

impl fmt::Display for LabelValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for LabelValue {
    type Err = LabelValueParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

#[cfg(test)]
mod tests;
