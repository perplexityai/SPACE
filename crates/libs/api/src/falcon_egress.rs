//! Deployment-owned destinations for the existing Falcon create header.
//!
//! `SPACE_FALCON_US2_EGRESS_DESTINATIONS` is a JSON array of `ip`, `cidr`, and
//! exact `domain` destinations, parsed once at API startup. There is no built-in list.
//! The `us2` header, environment variable, and type names remain for compatibility;
//! they do not restrict the configured regions. No renaming is included here.
//! TODO: remove US-2-specific names in a separate coordinated caller/config migration.
//! `SPACE_FALCON_TLS_INTERCEPTION_EXEMPTIONS_ENABLED` defaults to false and
//! controls whether composition adds TLS exemptions for these destinations.
//! Composition produces an ordinary [`NetworkPolicyV1`]; the gateway, shim,
//! and node manager need no vendor awareness.

use pplx_space_net_common::{EgressDefaultAction, EgressDestinationV1, NetworkPolicyV1};

const ENV: &str = "SPACE_FALCON_US2_EGRESS_DESTINATIONS";
const EXEMPTIONS_ENV: &str = "SPACE_FALCON_TLS_INTERCEPTION_EXEMPTIONS_ENABLED";

#[derive(Clone, Debug)]
pub(crate) struct FalconUs2Egress {
    destinations: Vec<EgressDestinationV1>,
    tls_interception_exemptions_enabled: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum FalconEgressConfigError {
    #[error("{ENV} is not valid Unicode")]
    Environment(#[source] std::env::VarError),
    #[error("{EXEMPTIONS_ENV} is not valid Unicode")]
    ExemptionsEnvironment(#[source] std::env::VarError),
    #[error("{EXEMPTIONS_ENV} must be true or false")]
    ExemptionsActivation(#[source] std::str::ParseBoolError),
    #[error("invalid {ENV}: {0}")]
    Json(#[source] serde_json::Error),
    #[error("{ENV} must contain at least one destination; unset it to disable Falcon egress")]
    Empty,
    #[error("{ENV} accepts only IP addresses, CIDRs, and exact domain destinations")]
    UnsupportedDestination,
}

impl FalconUs2Egress {
    pub(crate) fn from_env() -> Result<Option<Self>, FalconEgressConfigError> {
        let activation = match std::env::var(EXEMPTIONS_ENV) {
            Ok(raw) => Some(raw),
            Err(std::env::VarError::NotPresent) => None,
            Err(error) => return Err(FalconEgressConfigError::ExemptionsEnvironment(error)),
        };
        match std::env::var(ENV) {
            Ok(raw) => Self::from_config(&raw, activation.as_deref()).map(Some),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(error) => Err(FalconEgressConfigError::Environment(error)),
        }
    }

    fn from_config(raw: &str, activation: Option<&str>) -> Result<Self, FalconEgressConfigError> {
        let tls_interception_exemptions_enabled = activation
            .unwrap_or("false")
            .parse()
            .map_err(FalconEgressConfigError::ExemptionsActivation)?;
        let destinations: Vec<EgressDestinationV1> =
            serde_json::from_str(raw).map_err(FalconEgressConfigError::Json)?;
        if destinations.is_empty() {
            return Err(FalconEgressConfigError::Empty);
        }
        if destinations.iter().any(|destination| {
            !matches!(
                destination,
                EgressDestinationV1::Ip(_)
                    | EgressDestinationV1::Cidr(_)
                    | EgressDestinationV1::Domain(_)
            )
        }) {
            return Err(FalconEgressConfigError::UnsupportedDestination);
        }
        Ok(Self {
            destinations,
            tls_interception_exemptions_enabled,
        })
    }

    /// Allow configured destinations and add TLS exemptions when enabled.
    /// Explicit and platform denies still take precedence.
    pub(crate) fn compose(&self, base: &NetworkPolicyV1) -> NetworkPolicyV1 {
        let mut policy = base.clone();
        for destination in &self.destinations {
            if self.tls_interception_exemptions_enabled
                && !policy.tls_interception_exemptions.contains(destination)
            {
                policy.tls_interception_exemptions.push(destination.clone());
            }
            if !policy.egress.allow.contains(destination) {
                policy.egress.allow.push(destination.clone());
            }
        }
        // Falcon's destinations are mostly hostnames, which only resolve while the
        // static resolvers are reachable. The ECW translation layer owns that
        // injection, but it runs only when the caller's own policy already carried a
        // domain allow. Two Falcon bases miss it: a template default policy, which
        // never passes through translation at all, and a default-deny request policy
        // whose allows are all addresses. Reusing the shared helper here covers both,
        // and it skips resolvers an existing destination already matches, so
        // overlapping with translation adds nothing.
        if policy.default_action() == EgressDefaultAction::Deny {
            policy.egress.allow = crate::e2b::with_ecw_dns_resolver_allows(policy.egress.allow);
        }
        policy
    }
}

/// A missing optional sensor overlay must not block sandbox creation.
pub(crate) fn for_create(
    requested: bool,
    configured: Option<&FalconUs2Egress>,
) -> Option<&FalconUs2Egress> {
    use pplx_space_metrics::sandbox::admission::{FalconCreateOutcome, falcon_create};

    if !requested {
        return None;
    }
    if configured.is_some() {
        falcon_create(FalconCreateOutcome::Ready);
    } else {
        tracing::warn!("Falcon create admission falling back to Noop: egress is not configured");
        falcon_create(FalconCreateOutcome::Unconfigured);
    }
    configured
}

#[cfg(test)]
mod tests;
