use pplx_space_sbx_mgr_client::proto::QuarantineRecoveryObservedOwner;
use tonic::Status;
use uuid::Uuid;

use super::fields::proto_timestamp_to_rfc3339;
use crate::metadata::optional_trimmed;
use crate::proto::PublishedSandboxState;

#[derive(Clone, Debug)]
pub(crate) struct QuarantineRecoveryProjection {
    pub(crate) schema_version: u32,
    pub(crate) local_quarantine_id: Uuid,
    pub(crate) observed_owner: QuarantineRecoveryObservedOwner,
    pub(crate) attempt_count: i64,
    pub(crate) next_attempt_at: Option<String>,
}

pub(crate) enum QuarantineRecoveryProjectionState {
    Absent,
    Degraded,
    Observed(QuarantineRecoveryProjection),
}

impl QuarantineRecoveryProjection {
    pub(crate) fn observed_owner_name(&self) -> &'static str {
        match self.observed_owner {
            QuarantineRecoveryObservedOwner::Unspecified => "unspecified",
            QuarantineRecoveryObservedOwner::LegacyRecovery => "legacy_recovery",
        }
    }
}

pub(crate) fn quarantine_recovery_projection(
    state: &PublishedSandboxState,
) -> Result<QuarantineRecoveryProjectionState, Status> {
    let Some(observation) = state.quarantine_recovery.as_ref() else {
        return Ok(QuarantineRecoveryProjectionState::Absent);
    };
    if observation.degraded {
        return Ok(QuarantineRecoveryProjectionState::Degraded);
    };
    if observation.schema_version == 0 {
        return Err(Status::invalid_argument(format!(
            "unsupported quarantine recovery observation schema version {}",
            observation.schema_version
        )));
    }
    let local_quarantine_id = optional_trimmed(Some(&observation.local_quarantine_id))
        .ok_or_else(|| Status::invalid_argument("local_quarantine_id is required"))?
        .parse::<Uuid>()
        .map_err(|error| {
            Status::invalid_argument(format!("invalid local_quarantine_id: {error}"))
        })?;
    // Owner is descriptive evidence, not authority. Preserve the episode and
    // scheduling facts when a newer publisher sends an owner this release does
    // not know; rollout safety is better served by an explicit unspecified
    // owner than by freezing the entire prior observation as degraded.
    let observed_owner = QuarantineRecoveryObservedOwner::try_from(observation.observed_owner)
        .unwrap_or(QuarantineRecoveryObservedOwner::Unspecified);
    let next_attempt_at = observation
        .next_attempt_at
        .as_ref()
        .map(|timestamp| {
            if timestamp.seconds < 0 {
                return Err(Status::invalid_argument(
                    "quarantine_next_attempt_at is negative",
                ));
            }
            proto_timestamp_to_rfc3339(timestamp, "quarantine_next_attempt_at")
        })
        .transpose()?;
    Ok(QuarantineRecoveryProjectionState::Observed(
        QuarantineRecoveryProjection {
            schema_version: observation.schema_version,
            local_quarantine_id,
            observed_owner,
            attempt_count: i64::from(observation.attempt_count),
            next_attempt_at,
        },
    ))
}

#[cfg(test)]
mod tests;
