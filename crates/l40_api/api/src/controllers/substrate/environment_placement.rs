//! The environment requirement gate on placement.
//!
//! Work is *bound* when its record names an execution environment: the
//! template build's `execution_metadata.environment` for a create or a pool
//! adoption or a pull (a holder's own build's, else its release's; for a
//! pull candidate that holds nothing, the release's, else the one every
//! bound holder agrees on), and the checkpoint's for a restore. The gate asks of each
//! candidate node whether its snapshot is evidence
//! (`pplx_space_node_status_store::environment_evidence`) that lists the
//! work's environment in `usable_environment_refs`, and if not, why not
//! (`pplx_space_node_status_store::environment_refusal`). Unbound work carries
//! no environment and is never refused.
//!
//! [`EnvironmentPlacementGate`] picks what the answer does:
//!
//! * off: nothing. Each candidate statement takes the mode as a bound
//!   parameter, and with it off each clause the gate adds is either TRUE or
//!   reduces to the check it extends, so placement is exactly the gate's
//!   absence.
//! * shadow: placement is exactly off's, and each candidate the gate would
//!   refuse is counted (`environment_placement.would_refuse`, by path and
//!   reason) and logged.
//! * on: bound work goes only to a node with evidence listing its
//!   environment, E0 included. For bound work that match replaces the
//!   state-format major-version comparison, the legacy runtime and guest
//!   projections and, for a create or a pool adoption, the legacy kernel
//!   check; every other recorded component is still checked.
//!
//! The gate reads only what nodes publish. It doesn't apply the cluster's
//! execution policy, and it doesn't exclude a node that lags a narrowing
//! policy change.

use std::fmt;
use std::str::FromStr;

use pplx_space_common_core::env::Environment;
use pplx_space_common_core::types::NodeId;
use pplx_space_node_status_store::EnvironmentRefusal;
use sqlx::PgPool;

use crate::metrics::environment_placement::EnvironmentPlacementPath;

#[cfg(test)]
mod tests;

/// What placement does with a node's environment evidence for bound work.
///
/// Set by [`Self::ENV`]. Placement runs in each process that creates or
/// restores sandboxes, the API and the cluster controller, so both read it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EnvironmentPlacementGate {
    /// Today's placement; the gate isn't evaluated.
    #[default]
    Off,
    /// Today's placement, counting and logging what on would refuse.
    Shadow,
    /// Bound work goes only to nodes with evidence they can run it.
    On,
}

impl EnvironmentPlacementGate {
    /// `off`, `shadow` or `on`; unset is `off`. Anything else fails startup
    /// rather than guessing which placement the operator meant.
    pub const ENV: &'static str = "SPACE_ENVIRONMENT_PLACEMENT_GATE";

    pub const ALL: [Self; 3] = [Self::Off, Self::Shadow, Self::On];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::On => "on",
        }
    }

    /// Whether candidates the gate refuses are left out.
    pub const fn refuses(self) -> bool {
        matches!(self, Self::On)
    }

    /// Whether placement reads the work's binding for the gate: while the gate
    /// is off, the release and holder reads that exist only for it are
    /// skipped. The candidate statements still compute each candidate's
    /// refusal, which off ignores.
    pub const fn evaluates(self) -> bool {
        matches!(self, Self::Shadow | Self::On)
    }

    /// Whether each refusal the statements report is counted and logged.
    /// Only shadow sees any: on leaves the refused candidates out.
    pub const fn records_refusals(self) -> bool {
        matches!(self, Self::Shadow)
    }

    /// Kubernetes pods aren't gated yet (the gap table in
    /// `kubernetes/mod.rs`): `on` would claim enforcement nothing provides,
    /// so it's refused at startup, and `shadow`, which places as `off`
    /// anyway, only logs that it counts nothing.
    pub fn check_kubernetes_support(self) -> Result<(), super::SubstrateError> {
        match self {
            Self::Off => Ok(()),
            Self::Shadow => {
                tracing::warn!(
                    "the environment placement gate's shadow mode counts nothing on the \
                     Kubernetes substrate yet"
                );
                Ok(())
            }
            Self::On => Err(super::SubstrateError::Unsupported {
                message: format!(
                    "{}=on isn't supported on the Kubernetes substrate yet",
                    Self::ENV
                ),
            }),
        }
    }

    pub fn from_environment(
        env: &dyn Environment,
    ) -> Result<Self, EnvironmentPlacementGateParseError> {
        match env.var(Self::ENV) {
            Ok(value) => value.parse(),
            Err(std::env::VarError::NotPresent) => Ok(Self::default()),
            Err(std::env::VarError::NotUnicode(_)) => {
                Err(EnvironmentPlacementGateParseError::NotUnicode)
            }
        }
    }
}

impl fmt::Display for EnvironmentPlacementGate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for EnvironmentPlacementGate {
    type Err = EnvironmentPlacementGateParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        Self::ALL
            .into_iter()
            .find(|gate| gate.as_str() == normalized)
            .ok_or_else(|| EnvironmentPlacementGateParseError::Unknown(value.to_string()))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvironmentPlacementGateParseError {
    #[error(
        "{env} must be `off`, `shadow` or `on`, not {0:?}",
        env = EnvironmentPlacementGate::ENV
    )]
    Unknown(String),
    #[error("{env} is not valid Unicode", env = EnvironmentPlacementGate::ENV)]
    NotUnicode,
}

/// The reason label shadow counts a restore refused only for want of
/// cold-boot evidence under. A node lists an environment as usable when it
/// can cold boot it, which a restore doesn't need, so these are the restores
/// `on` holds back that the node itself might have loaded.
pub const RESTORE_NOT_COLD_BOOTABLE: &str = "restore_not_cold_bootable";

/// The label shadow counts `refusal` on `path` under: the refusal's own wire
/// value, except [`RESTORE_NOT_COLD_BOOTABLE`] for a restore candidate whose
/// evidence doesn't list the checkpoint's environment.
pub fn refusal_label(path: EnvironmentPlacementPath, refusal: EnvironmentRefusal) -> &'static str {
    match (path, refusal) {
        (
            EnvironmentPlacementPath::RestoreSource | EnvironmentPlacementPath::RestoreRemote,
            EnvironmentRefusal::EnvironmentNotUsable,
        ) => RESTORE_NOT_COLD_BOOTABLE,
        _ => refusal.as_str(),
    }
}

/// Count and log, in shadow mode, the candidates `path`'s statement returned
/// that the gate would refuse, each with its refusal wire value: one counter
/// increment and one log line per label for the statement, not per node.
/// Returns what it recorded, as (label, node). Off and on record nothing; on
/// has left refused candidates out of the statement. Recording never fails a
/// placement, so shadow places exactly as off.
pub(crate) fn record_refusals<'a>(
    gate: EnvironmentPlacementGate,
    path: EnvironmentPlacementPath,
    candidates: impl IntoIterator<Item = (NodeId, Option<&'a str>)>,
) -> Vec<(&'static str, NodeId)> {
    if !gate.records_refusals() {
        return Vec::new();
    }
    let mut refused: Vec<(&'static str, Vec<NodeId>)> = Vec::new();
    for (node_id, refusal) in candidates {
        let Some(refusal) = refusal else {
            continue;
        };
        match refusal.parse::<EnvironmentRefusal>() {
            Ok(refusal) => {
                let label = refusal_label(path, refusal);
                match refused.iter_mut().find(|(each, _)| *each == label) {
                    Some((_, nodes)) => nodes.push(node_id),
                    None => refused.push((label, vec![node_id])),
                }
            }
            Err(error) => tracing::warn!(
                node_id = %node_id,
                path = path.as_str(),
                %error,
                "environment placement gate returned an unknown refusal"
            ),
        }
    }
    let mut recorded = Vec::new();
    for (label, nodes) in refused {
        crate::metrics::environment_placement::would_refuse(path, label, nodes.len());
        tracing::info!(
            path = path.as_str(),
            reason = label,
            count = nodes.len(),
            nodes = ?nodes,
            "environment placement gate would refuse these candidates"
        );
        recorded.extend(nodes.into_iter().map(|node| (label, node)));
    }
    recorded
}

/// Where placement reads the binding of the build it places when a
/// candidate's own inventory row names none.
///
/// A content hash names rootfs bytes, not an environment: nodes can cache one
/// hash bound to different environments ("Template caches per environment" in
/// `docs/execution-environments.md`). A candidate holding the build is gated
/// by its own row's binding: node-ctl publishes the legacy-partition build
/// when there is one, which is the one a create with no environment
/// preference resolves. The release records at most one binding, which
/// stands in for a row that names none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BindingScope {
    /// The release's binding only, for candidates that hold the build.
    Release,
    /// The release's binding, else the one every holder whose row names a
    /// binding reports, when they all agree. For a pull candidate that holds
    /// no row of the build: the build a pull fetches is the one those nodes
    /// fetched.
    ReleaseOrHolders,
}

/// The environment work from `template` at `content_hash` runs under `gate`,
/// read from `scope`.
///
/// `None` when nothing in `scope` records a binding for the build, when
/// holders disagree, when the work names no build, and while the gate is off,
/// which skips the reads. In shadow a failed read is logged and reads as
/// `None`, so shadow places exactly as off; on, it fails the placement rather
/// than place bound work as unbound.
pub(crate) async fn work_environment(
    gate: EnvironmentPlacementGate,
    pool: &PgPool,
    template: &str,
    content_hash: Option<&str>,
    scope: BindingScope,
) -> Result<Option<String>, sqlx::Error> {
    let Some(content_hash) = content_hash.filter(|_| gate.evaluates()) else {
        return Ok(None);
    };
    match build_binding(pool, template, content_hash, scope).await {
        Err(error) if !gate.refuses() => {
            tracing::warn!(
                template,
                content_hash,
                %error,
                "environment placement gate couldn't read the build's environment; shadow \
                 places as off"
            );
            Ok(None)
        }
        result => result,
    }
}

async fn build_binding(
    pool: &PgPool,
    template: &str,
    content_hash: &str,
    scope: BindingScope,
) -> Result<Option<String>, sqlx::Error> {
    let release =
        crate::template_releases_store::release_environment(pool, template, content_hash).await?;
    match (release, scope) {
        (Some(environment), _) => Ok(Some(environment)),
        (None, BindingScope::Release) => Ok(None),
        (None, BindingScope::ReleaseOrHolders) => {
            crate::template_inventory_store::agreed_build_environment(pool, template, content_hash)
                .await
        }
    }
}
