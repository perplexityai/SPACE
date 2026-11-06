//! Typed node-local lifecycle observations; absent or unknown values fail closed.

use crate::proto::{
    SandboxCleanup, SandboxCleanupState, SandboxInfo, SandboxLifecycleState, SandboxListElement,
    WaitResponse,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleObservationError {
    #[error("unknown or unspecified sandbox lifecycle value {0}")]
    UnknownValue(i32),
    #[error("sandbox lifecycle observation is missing")]
    Missing,
    #[error("unrecognized persisted sandbox lifecycle {0:?}")]
    UnknownLabel(String),
    #[error("unknown sandbox cleanup state value {0}")]
    UnknownCleanupState(i32),
}

impl SandboxLifecycleState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Creating => "creating",
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Started => "started",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Deleting => "deleting",
            Self::Pausing => "pausing",
            Self::Paused => "paused",
            Self::Resuming => "resuming",
            Self::Checkpointing => "checkpointing",
            Self::Suspending => "suspending",
            Self::Suspended => "suspended",
            Self::Restoring => "restoring",
            Self::CleanupRequired => "cleanup_required",
            Self::CleanupFailed => "cleanup_failed",
            Self::Quarantined => "quarantined",
            Self::Failed => "failed",
        }
    }

    pub fn from_label(label: &str) -> Result<Self, LifecycleObservationError> {
        match label {
            "creating" => Ok(Self::Creating),
            "created" => Ok(Self::Created),
            "starting" => Ok(Self::Starting),
            "started" => Ok(Self::Started),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            "deleting" => Ok(Self::Deleting),
            "pausing" => Ok(Self::Pausing),
            "paused" => Ok(Self::Paused),
            "resuming" => Ok(Self::Resuming),
            "checkpointing" => Ok(Self::Checkpointing),
            "suspending" => Ok(Self::Suspending),
            "suspended" => Ok(Self::Suspended),
            "restoring" => Ok(Self::Restoring),
            "cleanup_required" => Ok(Self::CleanupRequired),
            "cleanup_failed" => Ok(Self::CleanupFailed),
            "quarantined" => Ok(Self::Quarantined),
            "failed" => Ok(Self::Failed),
            other => Err(LifecycleObservationError::UnknownLabel(other.to_string())),
        }
    }
}

pub fn decode_lifecycle(
    value: Option<i32>,
) -> Result<SandboxLifecycleState, LifecycleObservationError> {
    match value {
        None => Err(LifecycleObservationError::Missing),
        Some(raw) => SandboxLifecycleState::try_from(raw)
            .ok()
            .filter(|state| *state != SandboxLifecycleState::Unspecified)
            .ok_or(LifecycleObservationError::UnknownValue(raw)),
    }
}

/// The status a reader decides from. A node publishes a cleanup that owns the
/// row in `cleanup.state`, apart from its lifecycle, and may report the row
/// under the teardown transition it is in rather than as `CleanupRequired` or
/// `CleanupFailed`; either form decodes to the same status here, so a reader
/// answers both alike. A cleanup state this build doesn't know fails closed.
fn decode_status(
    lifecycle: Option<i32>,
    cleanup: Option<&SandboxCleanup>,
) -> Result<SandboxLifecycleState, LifecycleObservationError> {
    let lifecycle = decode_lifecycle(lifecycle)?;
    let Some(raw) = cleanup.map(|cleanup| cleanup.state) else {
        return Ok(lifecycle);
    };
    match SandboxCleanupState::try_from(raw) {
        Ok(SandboxCleanupState::Unspecified) => Ok(lifecycle),
        Ok(SandboxCleanupState::Required) => Ok(SandboxLifecycleState::CleanupRequired),
        Ok(SandboxCleanupState::Failed) => Ok(SandboxLifecycleState::CleanupFailed),
        Err(_) => Err(LifecycleObservationError::UnknownCleanupState(raw)),
    }
}

impl SandboxInfo {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl SandboxListElement {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl WaitResponse {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

#[cfg(test)]
mod tests;
