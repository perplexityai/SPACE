//! Live-sandbox limits enforced when `POST /v1/sandboxes` admits a create
//! (see [`crate::public_operations::SandboxQuota`]).
//!
//! Disabled unless configured: with no variable set every caller is
//! unlimited, which is the behavior before the limits existed. An org's limit
//! is its entry in `SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES` if it has one,
//! else `SPACE_NATIVE_ORG_SANDBOX_LIMIT`. `SPACE_NATIVE_SUBJECT_SANDBOX_LIMIT`
//! additionally limits each federated-identity subject. Malformed values fail
//! startup rather than silently leaving a caller uncapped: a non-integer or
//! negative limit, `0` for the default or per-user limit (an override of `0`
//! is allowed and refuses every native create of that org), and overrides
//! that are not a JSON object of organization ids, or that repeat one.

use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;

use pplx_space_common_core::types::{OrgId, UserId};

use crate::public_operations::{SandboxQuota, SubjectSandboxQuota};

const DEFAULT_LIMIT_ENV: &str = "SPACE_NATIVE_ORG_SANDBOX_LIMIT";
const OVERRIDES_ENV: &str = "SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES";
const SUBJECT_LIMIT_ENV: &str = "SPACE_NATIVE_SUBJECT_SANDBOX_LIMIT";

#[derive(Clone, Debug, Default)]
pub struct NativeSandboxLimits {
    default_limit: Option<NonZeroU32>,
    overrides: Arc<BTreeMap<OrgId, u32>>,
    subject_limit: Option<NonZeroU32>,
}

#[derive(Debug, thiserror::Error)]
pub enum NativeSandboxLimitsConfigError {
    #[error("{0} is not valid Unicode")]
    NonUnicode(&'static str),
    #[error("invalid {DEFAULT_LIMIT_ENV}: {0}")]
    DefaultLimit(#[source] std::num::ParseIntError),
    #[error("invalid {OVERRIDES_ENV}: {0}")]
    Overrides(#[source] serde_json::Error),
    #[error("invalid {SUBJECT_LIMIT_ENV}: {0}")]
    SubjectLimit(#[source] std::num::ParseIntError),
}

impl NativeSandboxLimits {
    pub fn from_env() -> Result<Self, NativeSandboxLimitsConfigError> {
        let limits = Self::parse(
            env_var(DEFAULT_LIMIT_ENV)?.as_deref(),
            env_var(OVERRIDES_ENV)?.as_deref(),
            env_var(SUBJECT_LIMIT_ENV)?.as_deref(),
        )?;
        tracing::info!(
            default_org_limit = ?limits.default_limit,
            org_overrides = limits.overrides.len(),
            subject_limit = ?limits.subject_limit,
            "native create sandbox limits configured"
        );
        Ok(limits)
    }

    fn parse(
        default_limit: Option<&str>,
        overrides: Option<&str>,
        subject_limit: Option<&str>,
    ) -> Result<Self, NativeSandboxLimitsConfigError> {
        let default_limit =
            parse_limit(default_limit).map_err(NativeSandboxLimitsConfigError::DefaultLimit)?;
        let subject_limit =
            parse_limit(subject_limit).map_err(NativeSandboxLimitsConfigError::SubjectLimit)?;
        let overrides = overrides
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .map(serde_json::from_str::<Overrides>)
            .transpose()
            .map_err(NativeSandboxLimitsConfigError::Overrides)?
            .unwrap_or_default();
        Ok(Self {
            default_limit,
            overrides: Arc::new(overrides.0),
            subject_limit,
        })
    }

    /// The limits a native create in `org_id` is held to, or `None` when it
    /// has none. `federated_subject` is the caller's own user when it is a
    /// federated-identity caller; only such a caller has a subject limit.
    pub fn for_create(
        &self,
        org_id: OrgId,
        federated_subject: Option<UserId>,
    ) -> Option<SandboxQuota> {
        let quota = SandboxQuota {
            org_id,
            org_limit: self.limit_for(org_id),
            subject: federated_subject
                .zip(self.subject_limit)
                .map(|(user_id, limit)| SubjectSandboxQuota {
                    user_id,
                    limit: limit.get(),
                }),
        };
        (quota.org_limit.is_some() || quota.subject.is_some()).then_some(quota)
    }

    /// The most sandboxes `org_id` may hold, counting creates still in
    /// flight, or `None` when the org is uncapped.
    fn limit_for(&self, org_id: OrgId) -> Option<u32> {
        self.overrides
            .get(&org_id)
            .copied()
            .or(self.default_limit.map(NonZeroU32::get))
    }
}

/// `SPACE_NATIVE_ORG_SANDBOX_LIMIT_OVERRIDES`: a map deserialized by hand so
/// that a repeated organization id fails instead of keeping the last value.
#[derive(Default)]
struct Overrides(BTreeMap<OrgId, u32>);

impl<'de> serde::Deserialize<'de> for Overrides {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visitor;

        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Overrides;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object mapping organization ids to sandbox limits")
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                mut map: A,
            ) -> Result<Overrides, A::Error> {
                let mut overrides = BTreeMap::new();
                while let Some((org_id, limit)) = map.next_entry::<OrgId, u32>()? {
                    if overrides.insert(org_id, limit).is_some() {
                        return Err(serde::de::Error::custom(format!(
                            "organization {org_id} is listed more than once"
                        )));
                    }
                }
                Ok(Overrides(overrides))
            }
        }

        deserializer.deserialize_map(Visitor)
    }
}

fn parse_limit(raw: Option<&str>) -> Result<Option<NonZeroU32>, std::num::ParseIntError> {
    raw.map(str::trim)
        .filter(|raw| !raw.is_empty())
        .map(str::parse)
        .transpose()
}

fn env_var(name: &'static str) -> Result<Option<String>, NativeSandboxLimitsConfigError> {
    match std::env::var(name) {
        Ok(raw) => Ok(Some(raw)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(NativeSandboxLimitsConfigError::NonUnicode(name))
        }
    }
}

#[cfg(test)]
mod tests;
