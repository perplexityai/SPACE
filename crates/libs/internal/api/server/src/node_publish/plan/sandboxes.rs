use pplx_space_common_core::types::{
    SandboxId, SandboxLifecycle, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
    SandboxTransitionReceiptKind,
};

use super::activation::{ActivationCompletionClaim, ActivationRollbackClaim};
use super::activity::ActivityClaim;
use super::lifecycle::{FailedClaim, PauseFailedRecoveryClaim, RestoreClaim, StoppedClaim};
use super::recovery_observation::{ClearRecoveryObservationClaim, RecoveryObservationClaim};
use super::suspend_handoff::SuspendHandoffClaim;
use super::suspended::SuspendedClaim;
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
    pub(crate) activities: Vec<ActivityClaim>,
    pub(crate) suspend_handoffs: Vec<SuspendHandoffClaim>,
    pub(crate) restored: Vec<RestoreClaim>,
    pub(crate) suspended: Vec<SuspendedClaim>,
    pub(crate) pause_failed_recoveries: Vec<PauseFailedRecoveryClaim>,
    pub(crate) stopped: Vec<StoppedClaim>,
    pub(crate) failed: Vec<FailedClaim>,
    pub(crate) activation_completions: Vec<ActivationCompletionClaim>,
    pub(crate) activation_rollbacks: Vec<ActivationRollbackClaim>,
    pub(crate) recovery_observations: Vec<RecoveryObservationClaim>,
    pub(crate) clear_recovery_observations: Vec<ClearRecoveryObservationClaim>,
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
            activities: Vec::new(),
            suspend_handoffs: Vec::new(),
            restored: Vec::new(),
            suspended: Vec::new(),
            pause_failed_recoveries: Vec::new(),
            stopped: Vec::new(),
            failed: Vec::new(),
            activation_completions: Vec::new(),
            activation_rollbacks: Vec::new(),
            recovery_observations: Vec::new(),
            clear_recovery_observations: Vec::new(),
            state_only: inventory.state_only.into_iter().map(Into::into).collect(),
            failure_episodes: inventory.failure_episodes,
            snapshots: Vec::new(),
            quarantine: Vec::new(),
            diagnostics: inventory.diagnostics,
            publishes_network_policy: false,
        };
        for claim in inventory.observations {
            plan.activities
                .extend(ActivityClaim::from_observation(&claim.observation));
            plan.suspend_handoffs
                .extend(SuspendHandoffClaim::from_observation(&claim.observation));
            match &claim.recovery {
                QuarantineRecoveryProjectionState::Observed(observation) => {
                    plan.recovery_observations.push(RecoveryObservationClaim {
                        sandbox_id: claim.observation.sandbox_id(),
                        observation: observation.clone(),
                    });
                }
                QuarantineRecoveryProjectionState::Absent => {
                    plan.clear_recovery_observations
                        .push(ClearRecoveryObservationClaim {
                            sandbox_id: claim.observation.sandbox_id(),
                        });
                }
                QuarantineRecoveryProjectionState::Degraded => {}
            }
            if let Some(receipt) = claim.receipt.as_ref() {
                match receipt.kind() {
                    SandboxTransitionReceiptKind::ActivationCompleted => {
                        plan.activation_completions
                            .extend(ActivationCompletionClaim::new(&claim.observation, receipt));
                    }
                    SandboxTransitionReceiptKind::ActivationRollback => {
                        plan.activation_rollbacks
                            .extend(ActivationRollbackClaim::new(&claim.observation, receipt));
                    }
                    SandboxTransitionReceiptKind::QuarantineRecovery => {}
                }
            }
            if let Some(status @ (SandboxLifecycle::Started | SandboxLifecycle::Paused)) =
                claim.observation.reported_lifecycle()
            {
                plan.pause_failed_recoveries.push(PauseFailedRecoveryClaim {
                    sandbox_id: claim.observation.sandbox_id(),
                    status,
                });
            }
            match claim.observation.reported_lifecycle() {
                Some(SandboxLifecycle::Started) => {
                    let receipt_kind = claim.receipt.as_ref().map(|receipt| receipt.kind());
                    if receipt_kind != Some(SandboxTransitionReceiptKind::ActivationCompleted) {
                        plan.restored.push(RestoreClaim {
                            sandbox_id: claim.observation.sandbox_id(),
                            transition_receipt_kind: receipt_kind,
                        });
                    }
                }
                Some(SandboxLifecycle::Suspended) => {
                    plan.suspended
                        .push(SuspendedClaim::new(&claim.observation, claim.receipt));
                }
                Some(SandboxLifecycle::Stopped) => plan.stopped.push(StoppedClaim {
                    sandbox_id: claim.observation.sandbox_id(),
                }),
                Some(SandboxLifecycle::Failed) => plan.failed.push(FailedClaim {
                    sandbox_id: claim.observation.sandbox_id(),
                }),
                _ => {}
            }
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
