use pplx_space_common_core::types::SandboxId;

use crate::node_publish::plan::sandboxes::{ObservedClaim, SandboxPlan};
use crate::node_publish::validate::{
    quarantine::QuarantineRecoveryProjectionState, quarantine_claim::QuarantineClaim,
    sandbox_inventory::DegradedSandboxClaim, snapshots::SnapshotClaim,
};

/// Compatibility encoding for existing statements. Typed claim slices stay
/// borrowed from the plan; only the rich-observation JSON is materialized.
#[derive(Debug)]
pub(crate) struct LegacySandboxInput<'a> {
    pub(crate) absent_ids: &'a [SandboxId],
    pub(crate) rows: Vec<serde_json::Value>,
    pub(crate) state_only: &'a [DegradedSandboxClaim],
    pub(crate) snapshot_rows: &'a [SnapshotClaim],
    pub(crate) quarantine_inputs: &'a [QuarantineClaim],
}

impl<'a> From<&'a SandboxPlan> for LegacySandboxInput<'a> {
    fn from(plan: &'a SandboxPlan) -> Self {
        Self {
            absent_ids: &plan.absent_ids,
            rows: plan.observations.iter().map(observed_row).collect(),
            state_only: &plan.state_only,
            snapshot_rows: &plan.snapshots,
            quarantine_inputs: &plan.quarantine,
        }
    }
}

fn observed_row(claim: &ObservedClaim) -> serde_json::Value {
    let (recovery, recovery_degraded) = match &claim.recovery {
        QuarantineRecoveryProjectionState::Absent => (None, false),
        QuarantineRecoveryProjectionState::Degraded => (None, true),
        QuarantineRecoveryProjectionState::Observed(observation) => (Some(observation), false),
    };
    claim
        .observation
        .legacy_row(claim.receipt.as_ref(), recovery, recovery_degraded)
}
