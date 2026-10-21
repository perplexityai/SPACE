//! Per-organization cap on live sandboxes, enforced when `POST /v1/sandboxes`
//! admits a create.
//!
//! Disabled unless configured: with neither variable set every org is
//! unlimited, which is the behavior before the cap existed. An org's limit is
//! its entry in `SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES` if it has one, else
//! `SPACE_NATIVE_ORG_SANDBOX_LIMIT`. Malformed values fail startup rather than
//! silently leaving an org uncapped.

use std::collections::BTreeMap;
use std::sync::Arc;

use pplx_space_common_core::types::OrgId;

const DEFAULT_LIMIT_ENV: &str = "SPACE_NATIVE_ORG_SANDBOX_LIMIT";
const OVERRIDES_ENV: &str = "SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES";

#[derive(Clone, Debug, Default)]
pub struct OrgSandboxQuota {
    default_limit: Option<u32>,
    overrides: Arc<BTreeMap<OrgId, u32>>,
}

#[derive(Debug, thiserror::Error)]
pub enum OrgSandboxQuotaConfigError {
    #[error("{0} is not valid Unicode")]
    NonUnicode(&'static str),
    #[error("invalid {DEFAULT_LIMIT_ENV}: {0}")]
    DefaultLimit(#[source] std::num::ParseIntError),
    #[error("invalid {OVERRIDES_ENV}: {0}")]
    Overrides(#[source] serde_json::Error),
}

impl OrgSandboxQuota {
    pub fn from_env() -> Result<Self, OrgSandboxQuotaConfigError> {
        Self::parse(
            env_var(DEFAULT_LIMIT_ENV)?.as_deref(),
            env_var(OVERRIDES_ENV)?.as_deref(),
        )
    }

    fn parse(
        default_limit: Option<&str>,
        overrides: Option<&str>,
    ) -> Result<Self, OrgSandboxQuotaConfigError> {
        let default_limit = default_limit
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(str::parse)
            .transpose()
            .map_err(OrgSandboxQuotaConfigError::DefaultLimit)?;
        let overrides = overrides
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(serde_json::from_str)
            .transpose()
            .map_err(OrgSandboxQuotaConfigError::Overrides)?
            .unwrap_or_default();
        Ok(Self {
            default_limit,
            overrides: Arc::new(overrides),
        })
    }

    /// The most sandboxes `org_id` may hold, counting creates still in
    /// flight, or `None` when the org is uncapped.
    pub fn limit_for(&self, org_id: OrgId) -> Option<u32> {
        self.overrides.get(&org_id).copied().or(self.default_limit)
    }
}

fn env_var(name: &'static str) -> Result<Option<String>, OrgSandboxQuotaConfigError> {
    match std::env::var(name) {
        Ok(raw) => Ok(Some(raw)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(OrgSandboxQuotaConfigError::NonUnicode(name)),
    }
}

#[cfg(test)]
mod tests;
