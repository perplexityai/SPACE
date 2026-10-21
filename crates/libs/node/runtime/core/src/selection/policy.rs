//! Startup execution eligibility, independent of discovery and artifact retention.
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
        })
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
            let ExecutionCompatibilityProfile::FirecrackerSnapshotV1 { state_format } =
                &spec.compatibility;
            if entry.is_initial()
                && execution.environment().is_none()
                && mode == ExecutionSelectionMode::PersistedStateRestore
                && execution.components().is_empty()
                && execution.runtime() == &release.runtime
                && execution.guest() == spec.guest
                && execution.state_format() == Some(state_format)
            {
                return Ok(());
            }
        }
        Err(PolicyError::NotAllowed)
    }
}
