//! The runtime manager's service name type.

use std::fmt;

/// Stable service name used in routing, discovery, and telemetry.
///
/// ```
/// # use pplx_space_node_runtime_manager::service_name::ServiceName;
/// let name = ServiceName::parse("space-tmplmgr").unwrap();
/// assert_eq!(name.as_str(), "space-tmplmgr");
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ServiceName(String);

impl ServiceName {
    /// Parses a bounded, NUL-free service name.
    ///
    /// ```
    /// # use pplx_space_node_runtime_manager::service_name::ServiceName;
    /// assert!(ServiceName::parse("space-api").is_ok());
    /// assert!(ServiceName::parse("").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, ServiceNameParseError> {
        let value = value.into();
        if value.is_empty() || value.len() > 128 || value.contains('\0') {
            return Err(ServiceNameParseError);
        }
        Ok(Self(value))
    }

    /// Returns the validated service name as a string slice.
    ///
    /// ```
    /// # use pplx_space_node_runtime_manager::service_name::ServiceName;
    /// let name = ServiceName::parse("space-node").unwrap();
    /// assert_eq!(name.as_str(), "space-node");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Error returned when a service name is empty, too long, or contains NUL.
///
/// ```
/// # use pplx_space_node_runtime_manager::service_name::ServiceName;
/// let err = ServiceName::parse("").unwrap_err();
/// assert_eq!(err.to_string(), "service name must be non-empty and bounded");
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("service name must be non-empty and bounded")]
pub struct ServiceNameParseError;
