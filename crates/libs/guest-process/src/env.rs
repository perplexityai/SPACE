//! Environment variable boundary types.
//!
//! These wrappers validate shell-safe keys, bound environment size, and redact
//! values that may contain credentials or customer data.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

mod contract;
pub use contract::{MAX_ENV_ENTRIES, MAX_ENV_KEY_BYTES, MAX_ENV_TOTAL_BYTES, MAX_ENV_VALUE_BYTES};

/// Error returned when an environment key, value, or map fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// assert!(EnvKey::parse("1BAD").is_err());
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct EnvParseError(&'static str);

impl EnvParseError {
    /// Returns the static validation reason.
    pub fn as_str(self) -> &'static str {
        self.0
    }
}

/// Non-fatal issue found while sanitizing a raw environment map.
///
/// Values are intentionally not retained because environment variables often
/// carry secrets or tenant data.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum EnvSanitizationIssue {
    /// A key or value failed per-entry validation.
    RejectedEntry { key: String, reason: EnvParseError },
    /// Otherwise valid entries were dropped after the accepted map reached
    /// [`MAX_ENV_ENTRIES`].
    MaxEntriesExceeded { max: usize, dropped: Vec<String> },
    /// Otherwise valid entries were dropped because accepting them would exceed
    /// [`MAX_ENV_TOTAL_BYTES`].
    TotalBytesExceeded { max: usize, dropped: Vec<String> },
}

/// Validated environment variable name.
///
/// Keys follow a conservative shell-safe grammar: ASCII letter or `_` first,
/// then ASCII letters, digits, or `_`.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let key = EnvKey::parse("EXAMPLE_KEY").unwrap();
/// assert_eq!(key.as_str(), "EXAMPLE_KEY");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EnvKey(String);

impl EnvKey {
    /// Parses and validates an environment key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// assert!(EnvKey::parse("_OK1").is_ok());
    /// assert!(EnvKey::parse("BAD-NAME").is_err());
    /// ```
    pub fn parse(value: impl AsRef<str>) -> Result<Self, EnvParseError> {
        let value = value.as_ref();
        validate_key(value)?;
        Ok(Self(value.to_string()))
    }

    /// Returns the validated key string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let key = EnvKey::parse("PATH").unwrap();
    /// assert_eq!(key.as_str(), "PATH");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EnvKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for EnvKey {
    type Err = EnvParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for EnvKey {
    type Error = EnvParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_key(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for EnvKey {
    type Error = EnvParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<EnvKey> for String {
    fn from(value: EnvKey) -> Self {
        value.0
    }
}

/// Environment variable value.
///
/// Values are NUL-free and are redacted in `Debug` output because they often
/// carry secrets or tenant data.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_guest_process::*;
/// let value = EnvValue::parse("token").unwrap();
/// assert_eq!(format!("{value:?}"), "EnvValue(<redacted>)");
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EnvValue(String);

impl EnvValue {
    /// Parses and validates an environment value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let value = EnvValue::parse("/usr/bin").unwrap();
    /// assert_eq!(value.expose(), "/usr/bin");
    /// ```
    pub fn parse(value: impl AsRef<str>) -> Result<Self, EnvParseError> {
        let value = value.as_ref();
        validate_value(value)?;
        Ok(Self(value.to_string()))
    }

    /// Exposes the raw value at an intentional boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let value = EnvValue::parse("secret").unwrap();
    /// assert_eq!(value.expose(), "secret");
    /// ```
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for EnvValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EnvValue(<redacted>)")
    }
}

impl FromStr for EnvValue {
    type Err = EnvParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for EnvValue {
    type Error = EnvParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_value(&value)?;
        Ok(Self(value))
    }
}

impl TryFrom<&str> for EnvValue {
    type Error = EnvParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

/// Validated environment map with count and aggregate byte limits.
///
/// Use this before handing environment variables to a guest process or service
/// boundary.
///
/// # Examples
///
/// ```rust
/// # use std::collections::BTreeMap;
/// # use pplx_space_guest_process::*;
/// let mut values = BTreeMap::new();
/// values.insert(EnvKey::parse("PATH").unwrap(), EnvValue::parse("/bin").unwrap());
/// let env = EnvMap::new(values).unwrap();
/// assert_eq!(env.as_map().len(), 1);
/// ```
#[derive(Clone, Eq, PartialEq)]
pub struct EnvMap(BTreeMap<EnvKey, EnvValue>);

impl EnvMap {
    /// Returns an empty environment map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let env = EnvMap::empty();
    /// assert!(env.as_map().is_empty());
    /// ```
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    /// Validates and wraps an environment map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::collections::BTreeMap;
    /// # use pplx_space_guest_process::*;
    /// let values = BTreeMap::new();
    /// let env = EnvMap::new(values).unwrap();
    /// assert!(env.as_map().is_empty());
    /// ```
    pub fn new(values: BTreeMap<EnvKey, EnvValue>) -> Result<Self, EnvParseError> {
        if values.len() > MAX_ENV_ENTRIES {
            return Err(EnvParseError("env has too many entries"));
        }
        let total = values.iter().fold(0usize, |total, (key, value)| {
            total
                .saturating_add(key.as_str().len())
                .saturating_add(value.expose().len())
        });
        if total > MAX_ENV_TOTAL_BYTES {
            return Err(EnvParseError("env has too many bytes"));
        }
        Ok(Self(values))
    }

    /// Drops invalid entries from a raw environment map and returns the
    /// accepted environment plus redacted issue metadata.
    ///
    /// Aggregate count and size limits are enforced by dropping otherwise valid
    /// entries after the accepted map reaches a limit.
    ///
    /// Duplicate keys are normalized before validation; the last value for a
    /// key wins.
    pub fn sanitize<I, K, V>(values: I) -> (Self, Vec<EnvSanitizationIssue>)
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let values = values
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_string(), value.as_ref().to_string()))
            .collect::<BTreeMap<_, _>>();
        let mut accepted = BTreeMap::new();
        let mut issues = Vec::new();
        let mut max_entries_dropped = Vec::new();
        let mut total_bytes_dropped = Vec::new();
        let mut total_bytes = 0usize;

        for (raw_key, raw_value) in values {
            let key = match EnvKey::parse(raw_key.clone()) {
                Ok(key) => key,
                Err(reason) => {
                    issues.push(EnvSanitizationIssue::RejectedEntry {
                        key: raw_key,
                        reason,
                    });
                    continue;
                }
            };
            let value = match EnvValue::parse(raw_value) {
                Ok(value) => value,
                Err(reason) => {
                    issues.push(EnvSanitizationIssue::RejectedEntry {
                        key: key.as_str().to_string(),
                        reason,
                    });
                    continue;
                }
            };
            if accepted.len() >= MAX_ENV_ENTRIES {
                max_entries_dropped.push(key.into());
                continue;
            }
            let entry_bytes = key.as_str().len().saturating_add(value.expose().len());
            if total_bytes.saturating_add(entry_bytes) > MAX_ENV_TOTAL_BYTES {
                total_bytes_dropped.push(key.into());
                continue;
            }
            total_bytes += entry_bytes;
            accepted.insert(key, value);
        }

        if !max_entries_dropped.is_empty() {
            issues.push(EnvSanitizationIssue::MaxEntriesExceeded {
                max: MAX_ENV_ENTRIES,
                dropped: max_entries_dropped,
            });
        }
        if !total_bytes_dropped.is_empty() {
            issues.push(EnvSanitizationIssue::TotalBytesExceeded {
                max: MAX_ENV_TOTAL_BYTES,
                dropped: total_bytes_dropped,
            });
        }

        (Self(accepted), issues)
    }

    /// Returns the validated environment map.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_guest_process::*;
    /// let env = EnvMap::default();
    /// assert_eq!(env.as_map().len(), 0);
    /// ```
    pub fn as_map(&self) -> &BTreeMap<EnvKey, EnvValue> {
        &self.0
    }
}

impl Default for EnvMap {
    fn default() -> Self {
        Self::empty()
    }
}

impl TryFrom<BTreeMap<String, String>> for EnvMap {
    type Error = EnvParseError;

    fn try_from(values: BTreeMap<String, String>) -> Result<Self, Self::Error> {
        let values = values
            .into_iter()
            .map(|(key, value)| Ok((EnvKey::parse(key)?, EnvValue::parse(value)?)))
            .collect::<Result<_, EnvParseError>>()?;
        Self::new(values)
    }
}

impl TryFrom<HashMap<String, String>> for EnvMap {
    type Error = EnvParseError;

    fn try_from(values: HashMap<String, String>) -> Result<Self, Self::Error> {
        let values: BTreeMap<String, String> = values.into_iter().collect();
        Self::try_from(values)
    }
}

impl From<EnvMap> for BTreeMap<String, String> {
    fn from(value: EnvMap) -> Self {
        value
            .0
            .into_iter()
            .map(|(key, value)| (key.into(), value.0))
            .collect()
    }
}

impl From<EnvMap> for HashMap<String, String> {
    fn from(value: EnvMap) -> Self {
        BTreeMap::<String, String>::from(value)
            .into_iter()
            .collect()
    }
}

#[cfg(feature = "serde")]
impl Serialize for EnvMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let values = self
            .0
            .iter()
            .map(|(key, value)| (key.as_str(), value.expose()))
            .collect::<BTreeMap<_, _>>();
        values.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for EnvMap {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let values = BTreeMap::<String, String>::deserialize(deserializer)?;
        Self::try_from(values).map_err(de::Error::custom)
    }
}

impl fmt::Debug for EnvMap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvMap")
            .field("len", &self.0.len())
            .field("values", &"<redacted>")
            .finish()
    }
}

fn validate_key(key: &str) -> Result<(), EnvParseError> {
    if key.is_empty() {
        return Err(EnvParseError("env key must not be empty"));
    }
    if key.len() > MAX_ENV_KEY_BYTES {
        return Err(EnvParseError("env key is too long"));
    }
    if key.contains('\0') || key.contains('=') {
        return Err(EnvParseError("env key contains invalid character"));
    }
    let Some(first) = key.as_bytes().first() else {
        return Err(EnvParseError("env key must not be empty"));
    };
    if !contract::ENV_KEY_FIRST.contains(first) {
        return Err(EnvParseError("env key must start with ASCII letter or '_'"));
    }
    if !key
        .as_bytes()
        .iter()
        .skip(1)
        .all(|ch| contract::ENV_KEY_REST.contains(ch))
    {
        return Err(EnvParseError(
            "env key must contain only ASCII letters, digits, or '_'",
        ));
    }
    Ok(())
}

fn validate_value(value: &str) -> Result<(), EnvParseError> {
    if value.len() > MAX_ENV_VALUE_BYTES {
        return Err(EnvParseError("env value is too long"));
    }
    if value.contains('\0') {
        return Err(EnvParseError("env value contains invalid character"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
