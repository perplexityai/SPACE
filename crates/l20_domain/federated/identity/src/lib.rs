//! Deployment policy shared by federated grant writers and consumers.
//!
//! It is its own crate so that code can read the policy without depending on
//! `pplx-space-common-svc`, which re-exports it as `federated_identity`.
use std::collections::BTreeSet;

use pplx_space_common_core::types::{FederatedGrantAudience, UpstreamIssuer, UpstreamSubjectId};

pub const DEFAULT_FEDERATED_IDENTITY_ISSUER: &str = "space";
pub const DEFAULT_FEDERATED_GRANT_AUDIENCE: &str = "credential-provider";
pub const FEDERATED_OWNER_GRANT_PURPOSE: &str = "federated_owner";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FederatedIdentityPolicy {
    pub issuer: UpstreamIssuer,
    pub grant_audience: FederatedGrantAudience,
    pub rejected_subjects: BTreeSet<UpstreamSubjectId>,
}

#[derive(Debug, thiserror::Error)]
#[error("invalid {setting}")]
pub struct FederatedIdentityPolicyError {
    setting: &'static str,
}

impl Default for FederatedIdentityPolicy {
    fn default() -> Self {
        Self::parse(None, None, None).expect("generic federated identity defaults are valid")
    }
}

impl FederatedIdentityPolicy {
    pub fn from_env() -> Result<Self, FederatedIdentityPolicyError> {
        let read = |setting| match std::env::var(setting) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(std::env::VarError::NotUnicode(_)) => Err(FederatedIdentityPolicyError { setting }),
        };
        Self::parse(
            read("SPACE_FEDERATED_IDENTITY_ISSUER")?.as_deref(),
            read("SPACE_FEDERATED_GRANT_AUDIENCE")?.as_deref(),
            read("SPACE_FEDERATED_REJECTED_SUBJECTS")?.as_deref(),
        )
    }

    pub fn parse(
        issuer: Option<&str>,
        grant_audience: Option<&str>,
        rejected_subjects: Option<&str>,
    ) -> Result<Self, FederatedIdentityPolicyError> {
        let issuer = UpstreamIssuer::parse(issuer.unwrap_or(DEFAULT_FEDERATED_IDENTITY_ISSUER))
            .map_err(|_| FederatedIdentityPolicyError {
                setting: "SPACE_FEDERATED_IDENTITY_ISSUER",
            })?;
        let grant_audience = FederatedGrantAudience::parse(
            grant_audience.unwrap_or(DEFAULT_FEDERATED_GRANT_AUDIENCE),
        )
        .map_err(|_| FederatedIdentityPolicyError {
            setting: "SPACE_FEDERATED_GRANT_AUDIENCE",
        })?;
        let rejected_subjects =
            serde_json::from_str(rejected_subjects.unwrap_or("[]")).map_err(|_| {
                FederatedIdentityPolicyError {
                    setting: "SPACE_FEDERATED_REJECTED_SUBJECTS",
                }
            })?;
        Ok(Self {
            issuer,
            grant_audience,
            rejected_subjects,
        })
    }
}

#[cfg(test)]
mod tests;
