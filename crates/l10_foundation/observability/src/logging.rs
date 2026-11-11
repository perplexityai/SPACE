//! Log-facing views of the shared process identity.

use crate::identity::Identity;

pub const SPACE_SERVICE_NAME: &str = "SPACE_SERVICE_NAME";
pub const SPACE_ENV: &str = "SPACE_ENV";
pub const SPACE_VERSION: &str = "SPACE_VERSION";

/// Resolve log service identity while accepting legacy Datadog deployments.
pub fn service_name(default: &str) -> String {
    Identity::from_env(default).log_service_name().to_string()
}

/// Resolve the deployment environment used by log context and disclosure gates.
/// A neutral setting takes precedence over the legacy Datadog setting.
pub fn environment() -> Option<String> {
    Identity::from_env("").log_environment().map(str::to_string)
}

pub(crate) fn version() -> Option<String> {
    Identity::from_env("").log_version().map(str::to_string)
}
