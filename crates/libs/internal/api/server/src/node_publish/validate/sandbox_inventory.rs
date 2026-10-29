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
use super::snapshots::snapshot_json;
use crate::metadata::parse_sandbox_id;
use crate::node_publish::diagnostics::SandboxDiagnostic;
use crate::proto::PublishedSandboxState;

#[derive(Debug)]
struct ObservedSandboxClaim {
    observation: ValidatedSandboxObservation,
    receipt: Option<SandboxTransitionReceipt<SandboxQuarantineReasonCode>>,
    recovery: QuarantineRecoveryProjectionState,
    quarantine: Option<QuarantineClaim>,
    // Snapshot IDs and descriptive labels are open legacy metadata. Only their
    // numeric bounds are rejected; do not reinterpret them as central UUIDs.
    snapshots: Vec<serde_json::Value>,
}

/// A batch of node claims, without database decisions or generated identifiers.
#[derive(Debug, Default)]
pub(crate) struct ValidatedSandboxInventory {
    absent_ids: Vec<SandboxId>,
    observations: Vec<ObservedSandboxClaim>,
    state_only: BTreeMap<SandboxId, Option<PublishedSandboxStatus>>,
    failure_episodes: Vec<FailureEpisodeClaim>,
    diagnostics: Vec<SandboxDiagnostic>,
}

/// Persistence encoding for the existing statements, not guarded row changes.
#[derive(Debug)]
pub(crate) struct LegacySandboxInput {
    pub(crate) absent_ids: Vec<SandboxId>,
    pub(crate) rows: Vec<serde_json::Value>,
    pub(crate) state_only: BTreeMap<SandboxId, serde_json::Value>,
    pub(crate) failure_episode_rows: Vec<FailureEpisodeClaim>,
    pub(crate) snapshot_rows: Vec<serde_json::Value>,
    pub(crate) quarantine_inputs: Vec<QuarantineClaim>,
    pub(crate) publishes_network_policy: bool,
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

    pub(crate) fn diagnostics(&self) -> &[SandboxDiagnostic] {
        &self.diagnostics
    }

    pub(crate) fn into_legacy_input(self) -> LegacySandboxInput {
        let mut input = LegacySandboxInput {
            absent_ids: self.absent_ids,
            rows: Vec::with_capacity(self.observations.len()),
            state_only: self
                .state_only
                .into_iter()
                .map(|(sandbox_id, status)| {
                    (
                        sandbox_id,
                        serde_json::json!({
                            "sandbox_id": sandbox_id,
                            "status": status.as_ref().map(PublishedSandboxStatus::as_str),
                        }),
                    )
                })
                .collect(),
            failure_episode_rows: self.failure_episodes,
            snapshot_rows: Vec::new(),
            quarantine_inputs: Vec::new(),
            publishes_network_policy: false,
        };
        for claim in self.observations {
            let (recovery, recovery_degraded) = match &claim.recovery {
                QuarantineRecoveryProjectionState::Absent => (None, false),
                QuarantineRecoveryProjectionState::Degraded => (None, true),
                QuarantineRecoveryProjectionState::Observed(observation) => {
                    (Some(observation), false)
                }
            };
            input.publishes_network_policy |= claim.observation.publishes_network_policy();
            input.rows.push(claim.observation.legacy_row(
                claim.receipt.as_ref(),
                recovery,
                recovery_degraded,
            ));
            input.snapshot_rows.extend(claim.snapshots);
            input.quarantine_inputs.extend(claim.quarantine);
        }
        input
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
        .map(|snapshot| snapshot_json(sandbox_id, snapshot))
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
