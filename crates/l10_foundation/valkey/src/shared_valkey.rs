//! Shared-Valkey configuration for shared transient control-plane state.

use ::redis::cluster::ClusterClient;

use crate::redis::cluster_client_from_url;

pub const SHARED_VALKEY_URL_ENV: &str = "SPACE_SHARED_VALKEY_URL";

#[derive(Debug, thiserror::Error)]
pub enum SharedValkeyConfigError {
    #[error("{SHARED_VALKEY_URL_ENV} is not valid Unicode")]
    InvalidEnvironment,
    #[error("invalid shared Valkey URL: {0}")]
    InvalidUrl(#[from] ::redis::RedisError),
}

/// Validated bootstrap configuration for the shared cluster-mode-enabled
/// Valkey deployment. The URL is intentionally not exposed through `Debug`
/// because it contains the cluster authentication token.
#[derive(Clone)]
pub struct SharedValkeyConfig {
    url: String,
}

impl SharedValkeyConfig {
    /// Loads the optional app contract. An unset or blank value leaves a
    /// consumer disabled; malformed configured values remain startup errors.
    pub fn from_env() -> Result<Option<Self>, SharedValkeyConfigError> {
        match std::env::var(SHARED_VALKEY_URL_ENV) {
            Ok(url) => Self::from_optional_url(Some(&url)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(SharedValkeyConfigError::InvalidEnvironment)
            }
        }
    }

    pub fn from_optional_url(url: Option<&str>) -> Result<Option<Self>, SharedValkeyConfigError> {
        let Some(url) = url.map(str::trim).filter(|url| !url.is_empty()) else {
            return Ok(None);
        };
        let config = Self {
            url: url.to_string(),
        };
        config.client()?;
        Ok(Some(config))
    }

    /// Builds a lazy cluster-aware client. No network I/O happens until a
    /// caller requests a connection; the redis crate owns topology discovery,
    /// MOVED/ASK routing, and reconnects after that point.
    pub fn client(&self) -> Result<ClusterClient, SharedValkeyConfigError> {
        cluster_client_from_url(&self.url).map_err(SharedValkeyConfigError::InvalidUrl)
    }
}

#[cfg(test)]
mod tests;
