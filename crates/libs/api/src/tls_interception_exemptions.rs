use pplx_space_net_common::{EgressDestinationV1, NetworkPolicyV1};

const ENV: &str = "SPACE_TLS_INTERCEPTION_EXEMPTIONS";

#[derive(Clone, Debug)]
pub struct TlsInterceptionExemptions {
    destinations: Vec<EgressDestinationV1>,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsInterceptionExemptionsConfigError {
    #[error("{ENV} is not valid Unicode")]
    Environment(#[source] std::env::VarError),
    #[error("invalid {ENV}: {0}")]
    Json(#[source] serde_json::Error),
    #[error("{ENV} must contain at least one destination; unset it to disable exemptions")]
    Empty,
    #[error("{ENV} accepts only domain and wildcard_domain destinations")]
    UnsupportedDestination,
}

impl TlsInterceptionExemptions {
    pub fn from_env() -> Result<Option<Self>, TlsInterceptionExemptionsConfigError> {
        Self::from_environment(std::env::var(ENV))
    }

    fn from_environment(
        value: Result<String, std::env::VarError>,
    ) -> Result<Option<Self>, TlsInterceptionExemptionsConfigError> {
        match value {
            Ok(raw) => Self::from_config(&raw).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(TlsInterceptionExemptionsConfigError::Environment(error)),
        }
    }

    pub fn from_config(raw: &str) -> Result<Self, TlsInterceptionExemptionsConfigError> {
        let destinations: Vec<EgressDestinationV1> =
            serde_json::from_str(raw).map_err(TlsInterceptionExemptionsConfigError::Json)?;
        if destinations.is_empty() {
            return Err(TlsInterceptionExemptionsConfigError::Empty);
        }
        if destinations.iter().any(|destination| {
            !matches!(
                destination,
                EgressDestinationV1::Domain(_) | EgressDestinationV1::WildcardDomain(_)
            )
        }) {
            return Err(TlsInterceptionExemptionsConfigError::UnsupportedDestination);
        }
        Ok(Self { destinations })
    }
}

pub(crate) fn compose(
    configured: Option<&TlsInterceptionExemptions>,
    policy_override: Option<NetworkPolicyV1>,
    template_default: Option<&NetworkPolicyV1>,
) -> Option<NetworkPolicyV1> {
    let Some(configured) = configured else {
        return policy_override;
    };
    let Some(base) = policy_override.as_ref().or(template_default) else {
        return policy_override;
    };
    if base.tls_interception.is_off()
        || configured
            .destinations
            .iter()
            .all(|destination| base.tls_interception_exemptions.contains(destination))
    {
        return policy_override;
    }
    let mut policy = base.clone();
    for destination in &configured.destinations {
        if !policy.tls_interception_exemptions.contains(destination) {
            policy.tls_interception_exemptions.push(destination.clone());
        }
    }
    Some(policy)
}

#[cfg(test)]
#[path = "tls_interception_exemptions/tests.rs"]
mod tests;
