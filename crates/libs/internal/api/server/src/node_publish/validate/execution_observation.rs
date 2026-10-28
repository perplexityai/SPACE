//! Decode collection outcomes without turning absence into positive evidence.

use pplx_space_execution_common::proto::NodeExecutionInventory;
use tonic::Status;

use crate::proto::{ExecutionInventoryObservation, execution_inventory_observation::Outcome};

#[derive(Debug)]
pub(crate) enum Report<'a> {
    Complete(&'a NodeExecutionInventory),
    Legacy(&'a NodeExecutionInventory),
    Unavailable,
}

#[derive(Debug, thiserror::Error)]
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
