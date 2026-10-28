use pplx_space_common_core::types::{
    SandboxId, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
};

use crate::node_publish::diagnostics::SandboxDiagnostic;
use crate::node_publish::validate::{
    failure_episode::FailureEpisodeClaim,
    quarantine::QuarantineRecoveryProjectionState,
    quarantine_claim::QuarantineClaim,
    sandbox_inventory::{DegradedSandboxClaim, ValidatedSandboxInventory},
    sandbox_observation::ValidatedSandboxObservation,
    snapshots::SnapshotClaim,
};

/// An observation and its optional fencing evidence, not a lifecycle decision.
#[derive(Debug)]
pub(crate) struct ObservedClaim {
    pub(crate) observation: ValidatedSandboxObservation,
    pub(crate) receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    pub(crate) recovery: QuarantineRecoveryProjectionState,
}

/// Per-concern evidence in the canonical order chosen by validation.
/// Absent and degraded observations remain distinct from rich observations.
#[derive(Debug)]
pub(crate) struct SandboxPlan {
    pub(crate) absent_ids: Vec<SandboxId>,
    pub(crate) observations: Vec<ObservedClaim>,
    pub(crate) state_only: Vec<DegradedSandboxClaim>,
    pub(crate) failure_episodes: Vec<FailureEpisodeClaim>,
    pub(crate) snapshots: Vec<SnapshotClaim>,
    pub(crate) quarantine: Vec<QuarantineClaim>,
    pub(crate) diagnostics: Vec<SandboxDiagnostic>,
    pub(crate) publishes_network_policy: bool,
}

impl From<ValidatedSandboxInventory> for SandboxPlan {
    fn from(inventory: ValidatedSandboxInventory) -> Self {
        let mut plan = Self {
            absent_ids: inventory.absent_ids,
            observations: Vec::with_capacity(inventory.observations.len()),
            state_only: inventory.state_only.into_iter().map(Into::into).collect(),
            failure_episodes: inventory.failure_episodes,
            snapshots: Vec::new(),
            quarantine: Vec::new(),
            diagnostics: inventory.diagnostics,
            publishes_network_policy: false,
        };
        for claim in inventory.observations {
            plan.publishes_network_policy |= claim.observation.publishes_network_policy();
            plan.observations.push(ObservedClaim {
                observation: claim.observation,
                receipt: claim.receipt,
                recovery: claim.recovery,
            });
            plan.snapshots.extend(claim.snapshots);
            plan.quarantine.extend(claim.quarantine);
        }
        plan
    }
}
