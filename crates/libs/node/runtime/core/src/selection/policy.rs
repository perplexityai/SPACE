//! Execution eligibility, independent of discovery and artifact retention: the
//! startup settings, optionally narrowed by the cluster's desired policy.
use crate::catalog::CompiledEnvironment;
use crate::runtime_inventory::ExecutionSelectionMode;
use pplx_space_common_core::types::*;
use std::collections::BTreeSet;

pub const ALLOWED_ENVIRONMENTS: &str = "SPACE_ALLOWED_EXECUTION_ENVIRONMENTS";
pub const DEFAULT_ENVIRONMENT: &str = "SPACE_DEFAULT_EXECUTION_ENVIRONMENT";

#[derive(Clone, Debug)]
pub struct ExecutionPolicy {
    catalog: Vec<CompiledEnvironment>,
    allowed: BTreeSet<EnvironmentReference>,
    default: Option<EnvironmentReference>,
    legacy_unbound_allowed: bool,
    applied_generation: Option<u64>,
    /// Narrowing left no default although the startup settings had one.
    default_withdrawn: bool,
    /// The persisted cluster policy couldn't be read; nothing new executes.
    cluster_policy_unreadable: bool,
}

/// The cluster's desired execution policy as served to this node. Unset fields
/// leave the node's configured values in place.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterExecutionPolicy {
    pub generation: u64,
    pub default: Option<ClusterDefault>,
    /// Allowed and deprecated environments, the default included. It narrows
    /// only while `default` is set.
    pub executing: BTreeSet<EnvironmentReference>,
    pub legacy_unbound_allowed: Option<bool>,
}

/// The default the cluster names.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ClusterDefault {
    Environment(EnvironmentReference),
    /// A default this build can't parse. It still narrows, as any default
    /// does, and is refused as not allowed.
    Unrecognized,
}

impl ClusterExecutionPolicy {
    /// The cluster default, when this build can name it.
    pub fn default_reference(&self) -> Option<EnvironmentReference> {
        match self.default {
            Some(ClusterDefault::Environment(reference)) => Some(reference),
            Some(ClusterDefault::Unrecognized) | None => None,
        }
    }
}

/// Why the narrowed policy has the default it has.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DefaultOutcome {
    /// The cluster sets no default, so the configured one applies.
    Unset,
    Adopted,
    /// The node doesn't compile or allow the cluster default.
    NotAllowed,
    /// The cluster default is allowed but isn't installed and verified here.
    Unusable,
}

impl DefaultOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unset => "unset",
            Self::Adopted => "adopted",
            Self::NotAllowed => "not_allowed",
            Self::Unusable => "unusable",
        }
    }

    /// The node enforces a default other than the one the cluster asked for.
    pub fn diverges(self) -> bool {
        matches!(self, Self::NotAllowed | Self::Unusable)
    }
}

impl std::fmt::Display for DefaultOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct NarrowedPolicy {
    pub policy: ExecutionPolicy,
    pub default: DefaultOutcome,
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("unknown compiled execution environment name: {0}")]
    UnknownName(String),
    #[error("execution environment list contains an empty entry at position {0}")]
    EmptyEntry(usize),
    #[error("execution environment list repeats {0:?}")]
    DuplicateEntry(String),
    #[error("default execution environment must belong to the allowlist")]
    DefaultNotAllowed,
    #[error("execution environment is not allowed on this node")]
    NotAllowed,
    #[error("host architecture has no qualified initial execution environment")]
    UnsupportedArchitecture,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum AllowedEntry {
    Environment(EnvironmentReference),
    LegacyUnbound,
}

impl AllowedEntry {
    fn parse(name: &str, catalog: &[CompiledEnvironment]) -> Result<Self, PolicyError> {
        match name {
            "legacy" => Ok(Self::LegacyUnbound),
            _ => catalog
                .iter()
                .find(|entry| entry.name() == name)
                .map(|entry| Self::Environment(entry.definition().reference()))
                .ok_or_else(|| PolicyError::UnknownName(name.to_owned())),
        }
    }
}

impl ExecutionPolicy {
    pub fn parse(
        allowed: Option<&str>,
        default: Option<&str>,
        catalog: &[CompiledEnvironment],
    ) -> Result<Self, PolicyError> {
        // Defaults are identities, not a second copy of the ENV syntax.
        let initial = catalog
            .iter()
            .find(|entry| entry.is_initial())
            .map(|entry| AllowedEntry::Environment(entry.definition().reference()));
        let mut entries = BTreeSet::new();
        if let Some(allowed) = allowed {
            for (index, name) in allowed.split(',').enumerate() {
                let name = name.trim();
                if name.is_empty() {
                    return Err(PolicyError::EmptyEntry(index + 1));
                }
                if !entries.insert(AllowedEntry::parse(name, catalog)?) {
                    return Err(PolicyError::DuplicateEntry(name.to_owned()));
                }
            }
        } else {
            entries.insert(AllowedEntry::LegacyUnbound);
            entries.extend(initial);
        }
        let default = match default {
            Some(name) => AllowedEntry::parse(name.trim(), catalog)?,
            None => initial.unwrap_or(AllowedEntry::LegacyUnbound),
        };
        if !entries.contains(&default) {
            return Err(PolicyError::DefaultNotAllowed);
        }
        let default = match default {
            AllowedEntry::Environment(reference) => Some(reference),
            // Architectures without qualified descriptors retain their existing
            // unbound contract, without fabricating a descriptor.
            AllowedEntry::LegacyUnbound if catalog.is_empty() => None,
            AllowedEntry::LegacyUnbound => return Err(PolicyError::DefaultNotAllowed),
        };
        let legacy_unbound_allowed = entries.contains(&AllowedEntry::LegacyUnbound);
        let allowed = entries
            .into_iter()
            .filter_map(|entry| match entry {
                AllowedEntry::Environment(reference) => Some(reference),
                AllowedEntry::LegacyUnbound => None,
            })
            .collect();
        Ok(Self {
            catalog: catalog.to_vec(),
            allowed,
            default,
            legacy_unbound_allowed,
            applied_generation: None,
            default_withdrawn: false,
            cluster_policy_unreadable: false,
        })
    }

    /// What a node enforces when it can't read the cluster policy it
    /// persisted: nothing new executes, so a damaged record can never lift a
    /// cluster block. Running VMs are untouched; the next application of the
    /// cluster policy replaces this.
    pub fn refuse_all(&self) -> Self {
        Self {
            catalog: self.catalog.clone(),
            allowed: BTreeSet::new(),
            default: None,
            legacy_unbound_allowed: false,
            applied_generation: None,
            default_withdrawn: false,
            cluster_policy_unreadable: true,
        }
    }

    /// Narrow these startup settings by the cluster policy, never widening
    /// them: the cluster can remove environments and revoke `legacy`, and pick
    /// the default among the environments left. `default_usable` says whether
    /// the cluster default is installed and verified; one that isn't is
    /// refused. A refused default falls back to the configured one while that
    /// still executes, and otherwise leaves no compiled default, so unbound new
    /// work is refused rather than run on an environment the cluster removed.
    pub fn narrow(&self, cluster: &ClusterExecutionPolicy, default_usable: bool) -> NarrowedPolicy {
        debug_assert!(
            self.applied_generation.is_none() && !self.cluster_policy_unreadable,
            "narrow the startup settings, not an already narrowed policy"
        );
        let allowed: BTreeSet<_> = match cluster.default {
            Some(_) => self
                .allowed
                .intersection(&cluster.executing)
                .copied()
                .collect(),
            None => self.allowed.clone(),
        };
        let configured = self.default.filter(|reference| allowed.contains(reference));
        let (default, outcome) = match cluster.default {
            None => (configured, DefaultOutcome::Unset),
            Some(ClusterDefault::Unrecognized) => (configured, DefaultOutcome::NotAllowed),
            Some(ClusterDefault::Environment(reference)) if !allowed.contains(&reference) => {
                (configured, DefaultOutcome::NotAllowed)
            }
            Some(_) if !default_usable => (configured, DefaultOutcome::Unusable),
            Some(ClusterDefault::Environment(reference)) => {
                (Some(reference), DefaultOutcome::Adopted)
            }
        };
        NarrowedPolicy {
            policy: Self {
                catalog: self.catalog.clone(),
                allowed,
                default,
                legacy_unbound_allowed: self.legacy_unbound_allowed
                    && cluster.legacy_unbound_allowed.unwrap_or(true),
                applied_generation: Some(cluster.generation),
                default_withdrawn: self.default.is_some() && default.is_none(),
                cluster_policy_unreadable: false,
            },
            default: outcome,
        }
    }

    /// The cluster policy generation narrowing these settings, if any.
    pub fn applied_generation(&self) -> Option<u64> {
        self.applied_generation
    }

    /// The cluster policy removed the configured default and its own default
    /// can't run here, so unbound new work has no environment.
    pub fn default_withdrawn(&self) -> bool {
        self.default_withdrawn
    }

    /// Built by [`Self::refuse_all`].
    pub fn cluster_policy_unreadable(&self) -> bool {
        self.cluster_policy_unreadable
    }

    pub fn catalog(&self) -> &[CompiledEnvironment] {
        &self.catalog
    }
    pub fn allows(&self, reference: EnvironmentReference) -> bool {
        self.allowed.contains(&reference)
    }
    pub fn allowed_references(&self) -> impl Iterator<Item = EnvironmentReference> + '_ {
        self.allowed.iter().copied()
    }
    pub fn default_reference(&self) -> Option<EnvironmentReference> {
        self.default
    }

    pub fn legacy_unbound_allowed(&self) -> bool {
        self.legacy_unbound_allowed
    }

    /// Unbound metadata matching a compiled definition obeys that definition's
    /// eligibility too. Other historical contracts require the legacy permission;
    /// their file verification and compatibility remain owned by inventory.
    /// Bound metadata can never enter that legacy path.
    /// With legacy permission disabled, stock E0 snapshots may still authenticate
    /// the allowed E0 loader without inventing missing boot provenance.
    pub fn check_execution(
        &self,
        execution: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<(), PolicyError> {
        let known = self
            .catalog
            .iter()
            .any(|entry| entry.definition().check_execution(execution).is_ok());
        if execution.environment().is_none() && !known && self.legacy_unbound_allowed {
            return Ok(());
        }
        for entry in self
            .catalog
            .iter()
            .filter(|entry| self.allows(entry.definition().reference()))
        {
            if execution
                .environment()
                .is_some_and(|r| r != entry.definition().reference())
            {
                continue;
            }
            if mode == ExecutionSelectionMode::PersistedStateRestore
                && !entry.supports_memory_state()
            {
                continue;
            }
            if entry.definition().check_execution(execution).is_ok() {
                return Ok(());
            }
            let spec = entry.definition().descriptor().spec();
            let RuntimeBinding::ManagedRelease(release) = &spec.runtime;
            if entry.is_initial()
                && execution.environment().is_none()
                && mode == ExecutionSelectionMode::PersistedStateRestore
                && execution.components().is_empty()
                && execution.runtime() == &release.runtime
                && execution.guest() == spec.guest
                && spec.compatibility.state_format().is_some()
                && execution.state_format() == spec.compatibility.state_format()
            {
                return Ok(());
            }
        }
        Err(PolicyError::NotAllowed)
    }
}
