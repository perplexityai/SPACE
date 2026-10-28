use std::collections::BTreeMap;

use pplx_space_common_core::types::{
    SandboxId, SandboxQuarantineReasonCode, SandboxTransitionReceipt,
};
use tonic::Status;

use super::failure_episode::FailureEpisodeClaim;
use super::quarantine::{QuarantineRecoveryProjectionState, quarantine_recovery_projection};
use super::quarantine_claim::QuarantineClaim;
use super::receipts;
use super::sandbox_observation::ValidatedSandboxObservation;
use super::sandbox_status::PublishedSandboxStatus;
use super::snapshots::SnapshotClaim;
use crate::metadata::parse_sandbox_id;
use crate::node_publish::diagnostics::SandboxDiagnostic;
use crate::proto::PublishedSandboxState;

#[derive(Debug)]
pub(in crate::node_publish) struct ObservedSandboxClaim {
    pub(in crate::node_publish) observation: ValidatedSandboxObservation,
    pub(in crate::node_publish) receipt:
        Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    pub(in crate::node_publish) recovery: QuarantineRecoveryProjectionState,
    pub(in crate::node_publish) quarantine: Option<QuarantineClaim>,
    pub(in crate::node_publish) snapshots: Vec<SnapshotClaim>,
}

/// A batch of node claims, without database decisions or generated identifiers.
#[derive(Debug, Default)]
pub(crate) struct ValidatedSandboxInventory {
    pub(in crate::node_publish) absent_ids: Vec<SandboxId>,
    pub(in crate::node_publish) observations: Vec<ObservedSandboxClaim>,
    pub(in crate::node_publish) state_only: BTreeMap<SandboxId, Option<PublishedSandboxStatus>>,
    pub(in crate::node_publish) failure_episodes: Vec<FailureEpisodeClaim>,
    pub(in crate::node_publish) diagnostics: Vec<SandboxDiagnostic>,
}

/// Presence and optional status survive when a rich observation is unusable.
#[derive(Debug)]
pub(crate) struct DegradedSandboxClaim {
    sandbox_id: SandboxId,
    status: Option<PublishedSandboxStatus>,
}

impl DegradedSandboxClaim {
    pub(crate) fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub(crate) fn status(&self) -> Option<&PublishedSandboxStatus> {
        self.status.as_ref()
    }
}

impl From<(SandboxId, Option<PublishedSandboxStatus>)> for DegradedSandboxClaim {
    fn from((sandbox_id, status): (SandboxId, Option<PublishedSandboxStatus>)) -> Self {
        Self { sandbox_id, status }
    }
}

impl ValidatedSandboxInventory {
    pub(crate) fn from_proto(states: &[PublishedSandboxState]) -> Self {
        let mut inventory = Self::default();
        let mut states_by_id = BTreeMap::new();
        for state in states {
            match parse_sandbox_id(&state.sandbox_id) {
                Ok(sandbox_id) => {
                    // The last declaration wins; process surviving identities
                    // in canonical order, not in wire order.
                    states_by_id.insert(sandbox_id, state);
                }
                Err(error) => inventory.diagnostics.push(SandboxDiagnostic::OmitSandbox {
                    raw_id: state.sandbox_id.clone(),
                    error,
                }),
            }
        }
        for (sandbox_id, state) in states_by_id {
            if !state.present {
                inventory.absent_ids.push(sandbox_id);
                continue;
            }
            let observed = if state.state_degraded {
                None
            } else {
                match parse_observed(sandbox_id, state, &mut inventory.diagnostics) {
                    Ok(observed) => Some(observed),
                    Err(error) => {
                        inventory
                            .diagnostics
                            .push(SandboxDiagnostic::StateOnly { sandbox_id, error });
                        None
                    }
                }
            };
            inventory
                .failure_episodes
                .push(FailureEpisodeClaim::from_proto(
                    sandbox_id,
                    state,
                    observed.is_none(),
                ));
            if let Some(observed) = observed {
                inventory.observations.push(observed);
            } else {
                // One keyed collection feeds both the state-only SQL input and
                // the pending-backup decision, so neither can omit an identity.
                inventory.state_only.insert(
                    sandbox_id,
                    PublishedSandboxStatus::from_proto(state.status.as_deref()),
                );
            }
        }
        inventory
    }
}

fn parse_observed(
    sandbox_id: SandboxId,
    state: &PublishedSandboxState,
    diagnostics: &mut Vec<SandboxDiagnostic>,
) -> Result<ObservedSandboxClaim, Status> {
    let parsed = receipts::parse(state);
    if let Some(fallback) = parsed.fallback {
        diagnostics.push(SandboxDiagnostic::FallbackTransitionReason {
            raw_id: state.sandbox_id.clone(),
            fallback,
        });
    }
    let receipt = match parsed.receipt {
        Ok(receipt) => receipt,
        Err(error) => {
            diagnostics.push(SandboxDiagnostic::OmitTransitionReceipt { sandbox_id, error });
            None
        }
    };
    let recovery = match quarantine_recovery_projection(state) {
        Ok(recovery) => recovery,
        Err(error) => {
            diagnostics.push(SandboxDiagnostic::OmitQuarantineRecovery { sandbox_id, error });
            QuarantineRecoveryProjectionState::Degraded
        }
    };
    let quarantine = QuarantineClaim::from_proto(sandbox_id, state);
    if let Some((label, error)) = quarantine
        .as_ref()
        .and_then(QuarantineClaim::unrecognized_reason)
    {
        diagnostics.push(SandboxDiagnostic::UnknownQuarantineReason {
            sandbox_id,
            label: label.to_owned(),
            error: error.clone(),
        });
    }
    let validation = ValidatedSandboxObservation::parse(sandbox_id, state, receipt.as_ref());
    if let Some(warning) = validation.version_warning {
        diagnostics.push(SandboxDiagnostic::InvalidSpacedVersion {
            sandbox_id,
            warning,
        });
    }
    let observation = validation.observation?;
    let snapshots = state
        .snapshots
        .iter()
        .map(|snapshot| SnapshotClaim::try_from_proto(sandbox_id, snapshot))
        .collect::<Result<_, _>>()?;
    // No part of a rich observation survives a malformed snapshot. Keep all
    // fields local until the whole observation can be accepted atomically.
    Ok(ObservedSandboxClaim {
        observation,
        receipt,
        recovery,
        quarantine,
        snapshots,
    })
}

#[cfg(test)]
mod tests;
