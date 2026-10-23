//! Operator-owned additions selected only by authenticated federated creates.
//! Selected names are durable; fresh policy composition uses current definitions.

use std::collections::{BTreeMap, BTreeSet};

use pplx_space_metrics::sandbox::admission::{EgressProfileOutcome, egress_profile};
use pplx_space_net_common::{
    EgressDefaultAction, EgressDestinationV1, NetworkPolicyV1, TlsInterceptionExemptionV1,
};
use serde::{Deserialize, Deserializer, Serialize, de};

const ENV: &str = "SPACE_EGRESS_PROFILES";
pub(crate) const HEADER: &str = "x-space-egress-profiles";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct EgressProfiles(BTreeMap<ProfileName, EgressProfile>);

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct EgressProfileSelection {
    names: BTreeSet<ProfileName>,
}

impl EgressProfileSelection {
    pub(crate) fn as_names(&self) -> Vec<&str> {
        self.names.iter().map(|name| name.0.as_str()).collect()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.names.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "String")]
struct ProfileName(String);

#[derive(Debug, thiserror::Error)]
#[error("profile names must contain only lowercase letters, digits, and hyphens")]
struct InvalidProfileName;

impl TryFrom<String> for ProfileName {
    type Error = InvalidProfileName;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.is_empty()
            || !value
                .bytes()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'-')
        {
            return Err(InvalidProfileName);
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EgressProfile {
    #[serde(default)]
    default: bool,
    allow: Vec<EgressDestinationV1>,
    #[serde(default)]
    tls_interception_exemptions: Vec<EgressDestinationV1>,
}

// Deserialize entries directly: a JSON Value or map would erase duplicate names.
impl<'de> Deserialize<'de> for EgressProfiles {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ProfilesVisitor;
        impl<'de> de::Visitor<'de> for ProfilesVisitor {
            type Value = EgressProfiles;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an object of named egress profiles")
            }

            fn visit_map<M: de::MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut profiles = BTreeMap::new();
                while let Some((name, profile)) = map.next_entry::<ProfileName, EgressProfile>()? {
                    if profile.allow.is_empty() {
                        return Err(de::Error::custom("profile allow must not be empty"));
                    }
                    if profile
                        .allow
                        .iter()
                        .chain(&profile.tls_interception_exemptions)
                        .any(|d| {
                            !matches!(
                                d,
                                EgressDestinationV1::Ip(_)
                                    | EgressDestinationV1::Cidr(_)
                                    | EgressDestinationV1::Domain(_)
                            )
                        })
                    {
                        return Err(de::Error::custom(
                            "profiles accept only IP, CIDR, and exact domain destinations",
                        ));
                    }
                    if profiles.insert(name, profile).is_some() {
                        return Err(de::Error::custom("duplicate profile name"));
                    }
                }
                Ok(EgressProfiles(profiles))
            }
        }
        deserializer.deserialize_map(ProfilesVisitor)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum EgressProfilesConfigError {
    #[error("{ENV} is not valid Unicode")]
    Environment(#[source] std::env::VarError),
    #[error("invalid {ENV}: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum EgressProfileSelectionError {
    #[error("X-Space-Egress-Profiles must be a space-separated list of +name or -name tokens")]
    InvalidRequest,
    #[error("unknown egress profile: {0}")]
    Unconfigured(String),
}

impl EgressProfiles {
    pub fn from_env() -> Result<Self, EgressProfilesConfigError> {
        match std::env::var(ENV) {
            Ok(raw) => Self::from_config(&raw),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(error) => Err(EgressProfilesConfigError::Environment(error)),
        }
    }

    pub(crate) fn from_config(raw: &str) -> Result<Self, EgressProfilesConfigError> {
        Ok(serde_json::from_str(raw)?)
    }

    pub(crate) fn select(
        &self,
        header: Option<&str>,
        federated: bool,
    ) -> Result<EgressProfileSelection, EgressProfileSelectionError> {
        if !federated {
            if header.is_some() {
                invalid_request();
            }
            return Ok(EgressProfileSelection::default());
        }
        let mut selected: BTreeSet<_> = self
            .0
            .iter()
            .filter(|(_, profile)| profile.default)
            .map(|(name, _)| name.clone())
            .collect();
        if let Some(header) = header {
            let mut seen = BTreeSet::new();
            for token in header.split(' ') {
                let (add, raw_name) = if let Some(name) = token.strip_prefix('+') {
                    (true, name)
                } else if let Some(name) = token.strip_prefix('-') {
                    (false, name)
                } else {
                    return Err(invalid_request());
                };
                let name =
                    ProfileName::try_from(raw_name.to_owned()).map_err(|_| invalid_request())?;
                if !seen.insert(name.clone()) {
                    return Err(invalid_request());
                }
                self.0.get(&name).ok_or_else(|| {
                    // Unknown names are untrusted and unbounded; do not use them as metric tags.
                    tracing::warn!("unconfigured egress profile requested");
                    egress_profile("unknown", EgressProfileOutcome::Unconfigured);
                    EgressProfileSelectionError::Unconfigured(raw_name.to_owned())
                })?;
                if add {
                    selected.insert(name);
                } else {
                    selected.remove(&name);
                }
            }
        }
        for name in &selected {
            egress_profile(&name.0, EgressProfileOutcome::Ready);
        }
        Ok(EgressProfileSelection { names: selected })
    }

    pub(crate) fn resolve(
        &self,
        selection: &EgressProfileSelection,
    ) -> Result<Self, crate::controllers::ControllerError> {
        selection
            .names
            .iter()
            .map(|name| {
                self.0
                    .get(name)
                    .cloned()
                    .map(|profile| (name.clone(), profile))
                    .ok_or_else(|| {
                        egress_profile(&name.0, EgressProfileOutcome::Unconfigured);
                        crate::controllers::ControllerError::service_unavailable(format!(
                            "saved egress profile is not configured: {}",
                            name.0
                        ))
                    })
            })
            .collect::<Result<_, _>>()
            .map(Self)
    }

    pub(crate) fn compose(&self, base: &NetworkPolicyV1) -> NetworkPolicyV1 {
        let mut policy = base.clone();
        for profile in self.0.values() {
            for destination in &profile.allow {
                if !policy.egress.allow.contains(destination) {
                    policy.egress.allow.push(destination.clone());
                }
            }
            for destination in &profile.tls_interception_exemptions {
                let exemption = TlsInterceptionExemptionV1::all_ports(destination.clone());
                if !policy.tls_interception_exemptions.contains(&exemption) {
                    policy.tls_interception_exemptions.push(exemption);
                }
            }
        }
        // Template policies bypass ECW translation; profile hostnames still need DNS.
        if !self.0.is_empty() && policy.default_action() == EgressDefaultAction::Deny {
            policy.egress.allow = crate::e2b::with_ecw_dns_resolver_allows(policy.egress.allow);
        }
        policy
    }

    pub(crate) fn compose_override(
        &self,
        policy: Option<NetworkPolicyV1>,
        template: Option<&NetworkPolicyV1>,
    ) -> Result<Option<NetworkPolicyV1>, crate::controllers::ControllerError> {
        if self.0.is_empty() {
            return Ok(policy);
        }
        // An explicit tenant replacement takes precedence over the template base.
        let base = policy.as_ref().or(template).ok_or_else(|| {
            crate::controllers::ControllerError::bad_request(
                "cannot inherit network policy: sandbox has no template policy",
            )
        })?;
        Ok(Some(self.compose(base)))
    }
}

pub(crate) fn invalid_request() -> EgressProfileSelectionError {
    tracing::warn!("invalid egress profile request");
    egress_profile("unknown", EgressProfileOutcome::InvalidRequest);
    EgressProfileSelectionError::InvalidRequest
}

#[cfg(test)]
mod tests;

pub(crate) async fn load(
    pool: &sqlx::PgPool,
    sandbox_id: pplx_space_common_core::types::SandboxId,
) -> Result<EgressProfileSelection, crate::controllers::ControllerError> {
    use crate::controllers::ControllerError;
    let value = pplx_space_sandbox_store::egress_profiles(pool, sandbox_id)
        .await
        .map_err(|error| {
            crate::controllers::db_contention_error(
                "failed to read sandbox egress profiles",
                &sqlx::Error::from(error),
            )
        })?
        .ok_or_else(|| ControllerError::not_found("sandbox egress profiles not found"))?;
    let names = value
        .into_iter()
        .map(ProfileName::try_from)
        .collect::<Result<_, _>>()
        .map_err(|error| {
            ControllerError::internal(format!("invalid persisted egress profiles: {error}"))
        })?;
    Ok(EgressProfileSelection { names })
}

impl crate::controllers::sandbox::SandboxController {
    pub(crate) fn select_egress_profiles(
        &self,
        headers: &axum::http::HeaderMap,
        federated: bool,
    ) -> Result<EgressProfileSelection, EgressProfileSelectionError> {
        let mut values = headers.get_all(HEADER).iter();
        let header = values.next();
        if !federated {
            if header.is_some() {
                invalid_request();
            }
            return Ok(EgressProfileSelection::default());
        }
        if values.next().is_some() {
            return Err(invalid_request());
        }
        let header = header
            .map(|value| value.to_str().map_err(|_| invalid_request()))
            .transpose()?;
        self.egress_profiles.select(header, true)
    }
}
