use pplx_space_common_core::types::SandboxId;
use serde::ser::SerializeStruct;

use crate::node_publish::validate::quarantine::QuarantineRecoveryProjection;

/// Validated node-local recovery evidence, separate from canonical quarantine.
#[derive(Debug, serde::Serialize)]
pub(crate) struct RecoveryObservationClaim {
    pub(crate) sandbox_id: SandboxId,
    #[serde(
        rename = "quarantine_recovery_payload",
        serialize_with = "serialize_projection"
    )]
    pub(crate) observation: QuarantineRecoveryProjection,
}

/// A non-degraded report explicitly omitted node-local recovery evidence.
#[derive(Debug, serde::Serialize)]
pub(crate) struct ClearRecoveryObservationClaim {
    pub(crate) sandbox_id: SandboxId,
}

fn serialize_projection<S: serde::Serializer>(
    observation: &QuarantineRecoveryProjection,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let mut payload = serializer.serialize_struct("QuarantineRecoveryProjection", 5)?;
    payload.serialize_field("schema_version", &observation.schema_version)?;
    payload.serialize_field("local_quarantine_id", &observation.local_quarantine_id)?;
    payload.serialize_field("observed_owner", &observation.observed_owner_name())?;
    payload.serialize_field("attempt_count", &observation.attempt_count)?;
    payload.serialize_field("next_attempt_at", &observation.next_attempt_at.as_deref())?;
    payload.end()
}
