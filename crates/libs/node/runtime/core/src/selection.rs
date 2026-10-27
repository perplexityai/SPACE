//! Selection among complete, approved execution bindings. Download approval,
//! installation, and execution qualification remain separate decisions.

use crate::runtime_inventory::{
    ExecutionSelectionMode, ResolvedExecutionEnvironment, RuntimeInventory,
};
use pplx_space_common_core::types::*;
use pplx_space_execution_common::environment::{
    EnvironmentBindingError, EnvironmentDefinition, EnvironmentRequirementsError,
};

mod policy;
pub use policy::{
    ALLOWED_ENVIRONMENTS, ClusterDefault, ClusterExecutionPolicy, DEFAULT_ENVIRONMENT,
    DefaultOutcome, ExecutionPolicy, NarrowedPolicy, PolicyError,
};

/// Callers must persist the returned exact binding before effectful work. Exact
/// requests and retries continue to use `RuntimeInventory::resolve` directly.
#[derive(Clone, Copy, Debug)]
pub enum CompatibleSelection<'a> {
    ColdBoot { source: &'a ExecutionMetadata },
    Restore { source: &'a ExecutionMetadata },
}

impl<'a> CompatibleSelection<'a> {
    pub fn source(self) -> &'a ExecutionMetadata {
        match self {
            Self::ColdBoot { source } | Self::Restore { source } => source,
        }
    }

    pub fn mode(self) -> ExecutionSelectionMode {
        match self {
            Self::ColdBoot { .. } => ExecutionSelectionMode::NewColdBoot,
            Self::Restore { .. } => ExecutionSelectionMode::PersistedStateRestore,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SelectionError {
    #[error("compatible selection requires a known, exact source environment")]
    UnknownSource,
    #[error("source execution does not match its environment: {0}")]
    Source(#[from] EnvironmentBindingError),
    #[error("invalid compiled execution metadata: {0}")]
    Metadata(#[from] EnvironmentRequirementsError),
    #[error("no approved, usable environment satisfies the selection and execution policy")]
    NoEligibleEnvironment,
    #[error("equally ranked environments have different identities")]
    Ambiguous,
    #[error("compatible environment is unavailable: {0}")]
    Unavailable(#[source] crate::runtime_inventory::RuntimeInventoryError),
}

/// Eligibility comes from the effective execution policy, not download approval.
/// Availability is evaluated locally against already verified inventory.
pub fn select<'a>(
    inventory: &RuntimeInventory,
    policy: &'a ExecutionPolicy,
    intent: CompatibleSelection<'_>,
) -> Result<(&'a EnvironmentDefinition, ResolvedExecutionEnvironment), SelectionError> {
    let catalog = policy.catalog();
    let execution = intent.source();
    let source_reference = execution
        .environment()
        .ok_or(SelectionError::UnknownSource)?;
    let source_entry = catalog
        .iter()
        .find(|entry| entry.definition().reference() == source_reference)
        .ok_or(SelectionError::UnknownSource)?;
    if matches!(intent, CompatibleSelection::Restore { .. })
        && !source_entry.supports_memory_state()
    {
        return Err(SelectionError::NoEligibleEnvironment);
    }
    source_entry.definition().check_execution(execution)?;
    let source = source_entry.definition().descriptor().spec();
    let mut candidates = Vec::new();
    let mut unavailable = None;
    for entry in catalog
        .iter()
        .filter(|entry| policy.allows(entry.definition().reference()))
    {
        let spec = entry.definition().descriptor().spec();
        // Runtime adapters own ordering. Do not assign semantic-version ordering
        // to future host-provided facilities or other runtime families.
        if entry.release().release.kind != RuntimeKind::Firecracker {
            continue;
        }
        let RuntimeBinding::ManagedRelease(source_runtime) = &source.runtime;
        if source_runtime.runtime.kind != entry.release().release.kind {
            continue;
        }
        if source.host != spec.host
            || source.guest != spec.guest
            || source.backend != spec.backend
            || source.isolation != spec.isolation
            || source.boot != spec.boot
        {
            continue;
        }
        if matches!(intent, CompatibleSelection::Restore { .. })
            && (!entry.supports_memory_state() || source.compatibility != spec.compatibility)
        {
            continue;
        }
        let requested = entry.definition().execution_requirements()?;
        let mut resolved =
            match inventory.resolve(Some(&requested), requested.runtime().kind, intent.mode()) {
                Ok(resolved) => resolved,
                Err(error) => {
                    // A temporary inability to verify the exact source is not
                    // permission to change the consumer persisted by callers.
                    if error.is_transient() && source_reference == entry.definition().reference() {
                        return Err(SelectionError::Unavailable(error));
                    }
                    if unavailable.as_ref().is_none_or(
                        |previous: &crate::runtime_inventory::RuntimeInventoryError| {
                            !previous.is_transient()
                        },
                    ) {
                        unavailable = Some(error);
                    }
                    continue;
                }
            };
        resolved.execution = entry.definition().bind_execution(resolved.execution)?;
        // Exact-first keeps adding an allowed candidate from upgrading existing
        // work. Only an unavailable or revoked source permits another consumer.
        if source_reference == entry.definition().reference() {
            return Ok((entry.definition(), resolved));
        }
        candidates.push((entry.runtime_ordering_key(), entry.definition(), resolved));
    }
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));
    if let [first, second, ..] = candidates.as_slice()
        && first.0 == second.0
        && first.1.reference() != second.1.reference()
    {
        return Err(SelectionError::Ambiguous);
    }
    candidates
        .into_iter()
        .next()
        .map(|(_, definition, resolved)| (definition, resolved))
        .ok_or_else(|| {
            unavailable
                .map(SelectionError::Unavailable)
                .unwrap_or(SelectionError::NoEligibleEnvironment)
        })
}

#[cfg(test)]
mod tests;
