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
    #[error("sandbox cleanup over lifecycle {0:?}, which no cleanup can be over")]
    CleanupOutsideTeardown(SandboxLifecycleState),
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

/// A cleanup that owns a sandbox row. A node publishes it in `cleanup.state`,
/// apart from the lifecycle the cleanup is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservedCleanup {
    /// A forced teardown owns the row and is owed.
    Required,
    /// A teardown owns the row and its last attempt failed; it is retried.
    Failed,
}

/// A teardown transition a cleanup can own a row over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Teardown {
    Stopping,
    Deleting,
    Suspending,
}

impl Teardown {
    pub const fn lifecycle(self) -> SandboxLifecycleState {
        match self {
            Self::Stopping => SandboxLifecycleState::Stopping,
            Self::Deleting => SandboxLifecycleState::Deleting,
            Self::Suspending => SandboxLifecycleState::Suspending,
        }
    }

    /// The teardown transition `lifecycle` is, if it is one.
    pub const fn of(lifecycle: SandboxLifecycleState) -> Option<Self> {
        match lifecycle {
            SandboxLifecycleState::Stopping => Some(Self::Stopping),
            SandboxLifecycleState::Deleting => Some(Self::Deleting),
            SandboxLifecycleState::Suspending => Some(Self::Suspending),
            _ => None,
        }
    }
}

/// What a reader decides from: the lifecycle a node publishes for a row no
/// cleanup owns, or the cleanup that owns it and the teardown transition it is
/// over. A status is not a lifecycle and doesn't compare equal to one: ask
/// [`Self::is_unowned`] or [`Self::unowned`] whether a row is in a lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ObservedStatus {
    /// No cleanup owns the row.
    Unowned(SandboxLifecycleState),
    /// `cleanup` owns the row over `teardown`.
    Owned {
        teardown: Teardown,
        cleanup: ObservedCleanup,
    },
}

impl ObservedStatus {
    /// The published lifecycle: a cleanup's teardown transition while one
    /// owns the row. To decide whether a row is in a lifecycle, ask
    /// [`Self::is_unowned`].
    pub const fn lifecycle(self) -> SandboxLifecycleState {
        match self {
            Self::Unowned(lifecycle) => lifecycle,
            Self::Owned { teardown, .. } => teardown.lifecycle(),
        }
    }

    /// The cleanup that owns the row, if one does.
    pub const fn cleanup(self) -> Option<ObservedCleanup> {
        match self {
            Self::Unowned(_) => None,
            Self::Owned { cleanup, .. } => Some(cleanup),
        }
    }

    /// The lifecycle, unless a cleanup owns the row.
    pub const fn unowned(self) -> Option<SandboxLifecycleState> {
        match self {
            Self::Unowned(lifecycle) => Some(lifecycle),
            Self::Owned { .. } => None,
        }
    }

    /// Whether the row is in `lifecycle` and no cleanup owns it.
    pub fn is_unowned(self, lifecycle: SandboxLifecycleState) -> bool {
        self.unowned() == Some(lifecycle)
    }

    /// The status label node-ctl publishes to the control plane:
    /// `cleanup_required` or `cleanup_failed` for a cleanup-owned row,
    /// otherwise the lifecycle's label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Unowned(lifecycle) => lifecycle.label(),
            Self::Owned {
                cleanup: ObservedCleanup::Required,
                ..
            } => "cleanup_required",
            Self::Owned {
                cleanup: ObservedCleanup::Failed,
                ..
            } => "cleanup_failed",
        }
    }
}

/// A cleanup state this build doesn't know, or a cleanup over a lifecycle no
/// cleanup can be over, fails closed.
fn decode_status(
    lifecycle: Option<i32>,
    cleanup: Option<&SandboxCleanup>,
) -> Result<ObservedStatus, LifecycleObservationError> {
    let lifecycle = decode_lifecycle(lifecycle)?;
    let raw = cleanup.map_or(SandboxCleanupState::Unspecified as i32, |cleanup| {
        cleanup.state
    });
    let cleanup = match SandboxCleanupState::try_from(raw) {
        Ok(SandboxCleanupState::Unspecified) => return Ok(ObservedStatus::Unowned(lifecycle)),
        Ok(SandboxCleanupState::Required) => ObservedCleanup::Required,
        Ok(SandboxCleanupState::Failed) => ObservedCleanup::Failed,
        Err(_) => return Err(LifecycleObservationError::UnknownCleanupState(raw)),
    };
    let teardown = Teardown::of(lifecycle)
        .ok_or(LifecycleObservationError::CleanupOutsideTeardown(lifecycle))?;
    Ok(ObservedStatus::Owned { teardown, cleanup })
}

impl SandboxInfo {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl SandboxListElement {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

impl WaitResponse {
    pub fn observed_status(&self) -> Result<ObservedStatus, LifecycleObservationError> {
        decode_status(self.lifecycle, self.cleanup.as_ref())
    }
}

#[cfg(test)]
mod tests;
