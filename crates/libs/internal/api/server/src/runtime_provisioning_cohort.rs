//! Which nodes `GetNodeRuntimeDesiredState` assigns a provisioning recipe.
//!
//! node-ctl installs a recipe on every tick with no gate of its own, so the
//! moment a recipe is served every admitted node installs it. The cohort
//! bounds the blast radius of an install-path bug, a checksum mismatch, an
//! upstream outage or a disk surprise while a cluster is brought on: canary
//! nodes first, then a growing percentage. A node that leaves the cohort only
//! stops receiving new installs, because a recipe never removes bytes.
//!
//! The setting is separate from `SPACE_INTERNAL_API_RUNTIME_DESIRED_STATE_ENABLED`,
//! so installing environments and enforcing the execution policy are enabled
//! and rolled back independently. Each replica reads it at startup.

use std::collections::BTreeSet;

use pplx_space_common_core::types::NodeId;
use sha2::{Digest as _, Sha256};

/// Serves recipes when truthy. Off by default; an unrecognized value reads as
/// off, as for the desired-state setting.
const ENABLED_ENV: &str = "SPACE_INTERNAL_API_RUNTIME_PROVISIONING_RECIPE_ENABLED";
/// Comma-separated nodes admitted whatever the percentage, each by the name the
/// node reports (its Kubernetes node name on Kubernetes) or its node ID.
const CANARY_NODES_ENV: &str = "SPACE_INTERNAL_API_RUNTIME_PROVISIONING_CANARY_NODES";
/// Whole percentage, 0 to 100, of nodes admitted by a stable hash of their
/// node ID. Unset means 0.
const PERCENT_ENV: &str = "SPACE_INTERNAL_API_RUNTIME_PROVISIONING_PERCENT";

/// Separates this cohort's hash from any other use of the node ID's digest.
const HASH_DOMAIN: &[u8] = b"space runtime provisioning cohort v1\0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeProvisioningCohort {
    /// No node gets a recipe.
    Disabled,
    Enabled {
        canary_nodes: BTreeSet<String>,
        percent: u8,
    },
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum CohortConfigError {
    #[error("{PERCENT_ENV} must be a whole number from 0 to 100, got {0:?}")]
    Percent(String),
}

impl RuntimeProvisioningCohort {
    pub(crate) fn from_env() -> Result<Self, CohortConfigError> {
        let enabled = std::env::var(ENABLED_ENV).ok();
        let canary_nodes = std::env::var(CANARY_NODES_ENV).ok();
        let percent = std::env::var(PERCENT_ENV).ok();
        let cohort = Self::from_values(
            enabled.as_deref(),
            canary_nodes.as_deref(),
            percent.as_deref(),
        );
        // Replicas decide independently, so record each replica's decision
        // next to what it was given.
        match &cohort {
            Ok(cohort) => {
                tracing::info!(
                    setting = ENABLED_ENV,
                    raw = enabled.as_deref().unwrap_or("<unset>"),
                    ?cohort,
                    "runtime provisioning recipe cohort"
                );
                if matches!(cohort, Self::Enabled { canary_nodes, percent: 0 } if canary_nodes.is_empty())
                {
                    tracing::warn!(
                        "runtime provisioning recipes are enabled, but no canary node is listed \
                         and the percentage is 0, so no node is admitted"
                    );
                }
            }
            Err(error) => tracing::error!(%error, "runtime provisioning recipe cohort"),
        }
        cohort
    }

    /// The cohort settings are read only while enabled, so a stale value left
    /// behind with the setting off can't stop a replica from starting.
    pub(crate) fn from_values(
        enabled: Option<&str>,
        canary_nodes: Option<&str>,
        percent: Option<&str>,
    ) -> Result<Self, CohortConfigError> {
        if !pplx_space_env_vars::env_flag(enabled, false) {
            return Ok(Self::Disabled);
        }
        let canary_nodes = canary_nodes
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(str::to_owned)
            .collect();
        let percent = match percent.map(str::trim).filter(|value| !value.is_empty()) {
            None => 0,
            Some(value) => value
                .parse::<u8>()
                .ok()
                .filter(|percent| *percent <= 100)
                .ok_or_else(|| CohortConfigError::Percent(value.to_owned()))?,
        };
        Ok(Self::Enabled {
            canary_nodes,
            percent,
        })
    }

    /// Whether the node gets a recipe. `reported` is the identifier the node
    /// sent and `node_id` its registration, which the caller has already
    /// authenticated.
    pub(crate) fn admits(&self, reported: &str, node_id: NodeId) -> bool {
        match self {
            Self::Disabled => false,
            Self::Enabled {
                canary_nodes,
                percent,
            } => {
                canary_nodes.contains(reported)
                    || canary_nodes.contains(&node_id.to_string())
                    || hash_bucket(node_id) < *percent
            }
        }
    }
}

/// The node's place in 0..100, the same in every replica and binary. Raising
/// the percentage admits every node it admitted before, plus more.
pub(crate) fn hash_bucket(node_id: NodeId) -> u8 {
    let digest = Sha256::new()
        .chain_update(HASH_DOMAIN)
        .chain_update(node_id.as_uuid().as_bytes())
        .finalize();
    let prefix = digest
        .first_chunk::<8>()
        .copied()
        .expect("a SHA-256 digest is 32 bytes");
    u8::try_from(u64::from_be_bytes(prefix) % 100).expect("a remainder of 100 fits in u8")
}

#[cfg(test)]
mod tests;
