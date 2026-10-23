//! Cluster identity boundary type.
//!
//! [`ClusterId`] is the first-class identity of a SPACE cluster stamp
//! (`prod-1`, `prod-2`, `testing`, …). It is deliberately separate from the
//! deployment *environment* (`prod`, `testing`): clusters can share an
//! environment while retaining distinct IDs. The value is rendered from
//! the Helm `clusterId` / `global.clusterId` value into every workload as the
//! `SPACE_CLUSTER_ID` environment variable, and is carried as a `cluster_id`
//! tag on every metric and a `cluster_id` field on every sandbox event so a
//! single Datadog org can slice telemetry per cluster.
//!
//! It is its own crate so that code can name the cluster identity without
//! depending on `pplx-space-common-core`. `pplx-space-common-core` re-exports
//! it from `types::cluster`.

use std::fmt;
use std::str::FromStr;
use std::sync::OnceLock;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Environment variable that carries the process's cluster id.
pub const CLUSTER_ID_ENV: &str = "SPACE_CLUSTER_ID";

/// Maximum UTF-8 byte length of a cluster id (one RFC-1123 label).
pub const MAX_CLUSTER_ID_BYTES: usize = 63;

/// Error returned when a cluster id fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_cluster_id::ClusterId;
/// assert!(ClusterId::parse("Prod-2").is_err()); // uppercase
/// assert!(ClusterId::parse("-prod").is_err()); // leading hyphen
/// ```
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
pub enum ClusterIdParseError {
    /// The value was empty.
    #[error("cluster id must not be empty")]
    Empty,
    /// The value exceeded [`MAX_CLUSTER_ID_BYTES`].
    #[error("cluster id is too long (max {max} bytes, got {actual})")]
    TooLong {
        /// The maximum allowed byte length.
        max: usize,
        /// The rejected value's byte length.
        actual: usize,
    },
    /// The value was not a lowercase RFC-1123 label.
    #[error(
        "cluster id must be a lowercase RFC-1123 label: lowercase ASCII letters, digits, and '-', \
         starting and ending with an alphanumeric"
    )]
    InvalidLabel,
}

/// Validated cluster identity.
///
/// A cluster id is a single lowercase RFC-1123 label: `[a-z0-9]([-a-z0-9]*[a-z0-9])?`,
/// at most [`MAX_CLUSTER_ID_BYTES`] bytes. This matches the charset used for
/// Kubernetes/DNS labels so the id can safely appear in resource names,
/// hostnames, metric tags, and event fields.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_cluster_id::ClusterId;
/// let id = ClusterId::parse("prod-2").unwrap();
/// assert_eq!(id.as_str(), "prod-2");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ClusterId(String);

impl ClusterId {
    /// Parses and validates a cluster id.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_cluster_id::ClusterId;
    /// assert!(ClusterId::parse("testing").is_ok());
    /// assert!(ClusterId::parse("prod-2").is_ok());
    /// assert!(ClusterId::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, ClusterIdParseError> {
        let value = value.into();
        validate(&value)?;
        Ok(Self(value))
    }

    /// Returns the validated cluster id string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Reads and parses the cluster id from the [`CLUSTER_ID_ENV`] environment
    /// variable.
    ///
    /// Returns `Ok(None)` when the variable is unset or empty (the id is
    /// optional so services run in local/dev contexts without one), and an
    /// error only when it is set to a non-empty but invalid value.
    pub fn from_env() -> Result<Option<Self>, ClusterIdParseError> {
        Self::from_environment(&pplx_space_env_vars::ProcessEnvironment)
    }

    pub fn from_environment(
        env: &dyn pplx_space_env_vars::Environment,
    ) -> Result<Option<Self>, ClusterIdParseError> {
        Self::from_env_value(env.var(CLUSTER_ID_ENV).ok())
    }

    /// Parses an already-read env value. Split out from [`Self::from_env`] so the
    /// unset / empty / invalid branches are unit-testable without mutating the
    /// process environment. `None` (unset or non-UTF-8) and empty both map to
    /// `Ok(None)`; a non-empty value is validated.
    fn from_env_value(raw: Option<String>) -> Result<Option<Self>, ClusterIdParseError> {
        match raw {
            Some(raw) if !raw.is_empty() => Self::parse(raw).map(Some),
            _ => Ok(None),
        }
    }
}

/// The process's cluster identity: parsed once from [`CLUSTER_ID_ENV`] and
/// cached for the process lifetime (unset or invalid → `None`; an invalid value
/// is logged once, then treated as absent).
///
/// This is the **single owner** of the process cluster id. Both the metrics sink
/// (`cluster_id:` constant tag) and the sandbox-event stream (`cluster_id`
/// field) read it, so they never disagree about the identity or its validation,
/// and it is captured once rather than separately per sink.
pub fn process_cluster_id() -> Option<&'static ClusterId> {
    static PROCESS_CLUSTER_ID: OnceLock<Option<ClusterId>> = OnceLock::new();
    PROCESS_CLUSTER_ID
        .get_or_init(|| match ClusterId::from_env() {
            Ok(cluster_id) => cluster_id,
            Err(error) => {
                tracing::warn!(%error, "ignoring invalid SPACE_CLUSTER_ID");
                None
            }
        })
        .as_ref()
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for ClusterId {
    type Err = ClusterIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for ClusterId {
    type Error = ClusterIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for ClusterId {
    type Error = ClusterIdParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<ClusterId> for String {
    fn from(value: ClusterId) -> Self {
        value.0
    }
}

fn validate(value: &str) -> Result<(), ClusterIdParseError> {
    if value.is_empty() {
        return Err(ClusterIdParseError::Empty);
    }
    if value.len() > MAX_CLUSTER_ID_BYTES {
        return Err(ClusterIdParseError::TooLong {
            max: MAX_CLUSTER_ID_BYTES,
            actual: value.len(),
        });
    }
    let bytes = value.as_bytes();
    let is_label_byte = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-';
    if !bytes.iter().copied().all(is_label_byte) {
        return Err(ClusterIdParseError::InvalidLabel);
    }
    let is_alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    // Non-emptiness is established above, so first/last are always `Some`.
    if !bytes.first().copied().is_some_and(is_alnum) || !bytes.last().copied().is_some_and(is_alnum)
    {
        return Err(ClusterIdParseError::InvalidLabel);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
