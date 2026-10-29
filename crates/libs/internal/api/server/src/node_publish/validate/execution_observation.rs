//! Decode collection outcomes without turning absence into positive evidence.

use pplx_space_common_core::types::ExecutionPolicyObservation;
use pplx_space_execution_common::{policy::PolicyObservationError, proto::NodeExecutionInventory};
use tonic::Status;

use crate::proto::{ExecutionInventoryObservation, execution_inventory_observation::Outcome};

use super::execution_inventory::ValidatedExecutionInventory;

#[derive(Debug)]
pub(crate) enum Report<'a> {
    Complete(&'a NodeExecutionInventory),
    Legacy(&'a NodeExecutionInventory),
    Unavailable,
}

#[derive(Debug)]
pub(crate) struct ExecutionInventoryEvidence {
    pub(crate) inventory: ValidatedExecutionInventory,
    // An unusable policy is withheld, not a rejection of the inventory. Keep
    // its typed diagnostic until persistence reaches the policy-report step.
    pub(crate) policy: Result<Option<ExecutionPolicyObservation>, PolicyObservationError>,
}

#[derive(Debug)]
pub(crate) enum ValidatedReport {
    Complete(Result<ExecutionInventoryEvidence, Status>),
    Legacy(Result<ExecutionInventoryEvidence, Status>),
    Unavailable,
}

impl ValidatedReport {
    pub(crate) fn from_report(report: Report<'_>) -> Self {
        match report {
            Report::Complete(inventory) => Self::Complete(parse_inventory(inventory)),
            Report::Legacy(inventory) => Self::Legacy(parse_inventory(inventory)),
            Report::Unavailable => Self::Unavailable,
        }
    }
}

fn parse_inventory(
    inventory: &NodeExecutionInventory,
) -> Result<ExecutionInventoryEvidence, Status> {
    Ok(ExecutionInventoryEvidence {
        inventory: ValidatedExecutionInventory::try_from_proto(inventory)?,
        policy: inventory
            .policy
            .as_ref()
            .map(ExecutionPolicyObservation::try_from)
            .transpose(),
    })
}

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum ObservationError {
    #[error("execution observation is missing a supported outcome")]
    MissingOutcome,
    #[error("complete execution observation disagrees with legacy inventory")]
    ConflictingInventory,
    #[error("unavailable execution observation cannot include legacy inventory")]
    UnavailableWithInventory,
}

impl From<ObservationError> for Status {
    fn from(error: ObservationError) -> Self {
        Self::invalid_argument(error.to_string())
    }
}

pub(crate) fn decode<'a>(
    observation: Option<&'a ExecutionInventoryObservation>,
    legacy: Option<&'a NodeExecutionInventory>,
) -> Result<Option<Report<'a>>, ObservationError> {
    let Some(observation) = observation else {
        return Ok(legacy.map(Report::Legacy));
    };
    match observation.outcome.as_ref() {
        Some(Outcome::Complete(inventory)) => {
            if legacy.is_some_and(|legacy| legacy != inventory) {
                return Err(ObservationError::ConflictingInventory);
            }
            Ok(Some(Report::Complete(inventory)))
        }
        Some(Outcome::Unavailable(_)) => {
            if legacy.is_some() {
                return Err(ObservationError::UnavailableWithInventory);
            }
            Ok(Some(Report::Unavailable))
        }
        None => Err(ObservationError::MissingOutcome),
    }
}
