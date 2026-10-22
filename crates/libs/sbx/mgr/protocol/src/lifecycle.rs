//! Typed node-local lifecycle observations; absent or unknown values fail closed.

use crate::proto::{SandboxInfo, SandboxLifecycleState, SandboxListElement, WaitResponse};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LifecycleObservationError {
    #[error("unknown or unspecified sandbox lifecycle value {0}")]
    UnknownValue(i32),
    #[error("sandbox lifecycle observation is missing")]
    Missing,
    #[error("unrecognized persisted sandbox lifecycle {0:?}")]
    UnknownLabel(String),
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

impl SandboxInfo {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_lifecycle(self.lifecycle)
    }
}

impl SandboxListElement {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_lifecycle(self.lifecycle)
    }
}

impl WaitResponse {
    pub fn observed_lifecycle(&self) -> Result<SandboxLifecycleState, LifecycleObservationError> {
        decode_lifecycle(self.lifecycle)
    }
}

#[cfg(test)]
mod tests;
