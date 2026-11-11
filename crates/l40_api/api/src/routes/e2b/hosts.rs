use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, header, request::Parts},
};
use serde::Deserialize;

use super::E2bApiError;
use crate::routes::e2b::constants::SPACE_ECW_HOSTS_ENV;

#[derive(Debug, Clone, Default)]
pub(crate) struct EcwHostConfig {
    pairs: Vec<EcwHostPair>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct EcwHostPair {
    pub(super) api_host: String,
    pub(super) host: String,
}

#[derive(Debug, Clone)]
pub(super) struct MatchedEcwHost(pub(super) EcwHostPair);

#[derive(Debug, Deserialize)]
struct EcwHostEnv {
    #[serde(rename = "apiHost", alias = "api_host")]
    api_host: String,
    host: String,
}

impl EcwHostConfig {
    pub(crate) fn from_env() -> Self {
        let raw_hosts = env_value(SPACE_ECW_HOSTS_ENV);
        Self::from_env_value(raw_hosts.as_deref())
    }

    pub(crate) fn from_env_value(raw_hosts: Option<&str>) -> Self {
        let mut pairs = Vec::new();

        if let Some(raw_hosts) = raw_hosts {
            match serde_json::from_str::<Vec<EcwHostEnv>>(raw_hosts) {
                Ok(hosts) => {
                    pairs.extend(hosts.into_iter().filter_map(|host_pair| {
                        let api_host = host_pair.api_host.trim();
                        let host = host_pair.host.trim();
                        if api_host.is_empty() || host.is_empty() {
                            return None;
                        }
                        Some(EcwHostPair {
                            api_host: api_host.to_string(),
                            host: host.to_string(),
                        })
                    }));
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        env = SPACE_ECW_HOSTS_ENV,
                        "failed to parse ECW hosts",
                    );
                }
            }
        }

        Self { pairs }
    }

    pub(super) fn api_pair_for_headers(&self, headers: &HeaderMap) -> Option<EcwHostPair> {
        let host = headers
            .get(header::HOST)
            .and_then(|host| host.to_str().ok())
            .map(host_without_port)?;

        self.pairs
            .iter()
            .find(|pair| host.eq_ignore_ascii_case(&pair.api_host))
            .cloned()
    }
}

impl<S> FromRequestParts<S> for MatchedEcwHost
where
    S: Send + Sync,
{
    type Rejection = E2bApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let pair = parts
            .extensions
            .get::<EcwHostConfig>()
            .and_then(|ecw_hosts| ecw_hosts.api_pair_for_headers(&parts.headers))
            .ok_or_else(|| E2bApiError::not_found("Endpoint"))?;

        Ok(Self(pair))
    }
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

pub(super) fn host_without_port(host: &str) -> &str {
    host.split_once(':').map_or(host, |(host, _)| host)
}
