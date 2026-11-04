//! What this node can publish about its environments: the compiled catalog it
//! knows and the bindings it can cold boot. Neither one admits execution.

use pplx_space_execution_common::environment::{
    EnvironmentObservation, EnvironmentObservationError, KnownEnvironment,
};

use super::*;
use crate::catalog::CompiledEnvironment;
use crate::selection::ExecutionPolicy;

impl RuntimeInventory {
    /// Whether this node can cold boot `environment` within its startup
    /// ceiling: the startup policy allows it, and a new cold boot resolves to
    /// it. Cluster policy only narrows that ceiling, so it's the placement
    /// layer's job, as is freshness. Restore readiness isn't covered.
    pub(crate) fn is_cold_boot_usable(
        &self,
        environment: &CompiledEnvironment,
        startup: &ExecutionPolicy,
    ) -> bool {
        let reference = environment.definition().reference();
        if !startup.allows(reference) {
            return false;
        }
        let Ok(requested) = environment.definition().execution_requirements() else {
            return false;
        };
        self.resolve(
            Some(&requested),
            requested.runtime().kind,
            ExecutionSelectionMode::NewColdBoot,
        )
        // Only E0 and included bindings resolve with their own reference.
        // Unverified external copies can satisfy the same identities but
        // resolve without one, and must not count.
        .is_ok_and(|resolved| resolved.environment == Some(reference))
    }

    /// The catalog this build certifies, with each entry's ordering key and
    /// edges, and the subset this snapshot can cold boot under `startup`.
    pub fn environment_observation(
        &self,
        catalog: &[CompiledEnvironment],
        startup: &ExecutionPolicy,
    ) -> Result<EnvironmentObservation, EnvironmentObservationError> {
        let known = catalog
            .iter()
            .map(|entry| {
                KnownEnvironment::new(
                    entry.definition().clone(),
                    version_sort_key(&entry.release().release),
                    entry.upgrade_targets().iter().copied(),
                    entry.memory_targets().iter().copied(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let usable = catalog
            .iter()
            .filter(|entry| self.is_cold_boot_usable(entry, startup))
            .map(|entry| entry.definition().reference());
        EnvironmentObservation::new(known, usable)
    }
}

#[cfg(test)]
mod tests;
