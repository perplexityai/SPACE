//! Why a sandbox stopped when no one asked it to.

use super::*;

/// The cause of a stop no caller requested, recorded on the row the stop
/// admission newly moves to `stopping`. It lasts until the sandbox runs
/// again: the `clear_stop_cause_when_running` trigger drops it when any writer
/// moves the row to `started` or `paused`, and nothing else does, so the
/// stop's cleanup states, a failed start back to `stopped` and a delete keep
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopCause {
    /// The shim tore the sandbox's Kubernetes pod down while it ran.
    PodTerminated,
    /// The sandbox's VM process exited on its own.
    VmExited,
}

impl StopCause {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PodTerminated => "pod_terminated",
            Self::VmExited => "vm_exited",
        }
    }
}

impl std::fmt::Display for StopCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StopCause {
    type Err = StopCauseParseError;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "pod_terminated" => Ok(Self::PodTerminated),
            "vm_exited" => Ok(Self::VmExited),
            _ => Err(StopCauseParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("stop cause must be pod_terminated or vm_exited")]
pub struct StopCauseParseError;

/// Record the attribution of the stop that just moved the row to
/// `stopping`: `cause`, or none for a requested stop, replacing whatever an
/// earlier stop left. Returns whether the row was `stopping`. The caller
/// decides that this stop newly owns the row; a stop that re-admits a row
/// another stop owns must not call it.
pub fn record_stop_cause(conn: &Connection, id: &str, cause: Option<StopCause>) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET stop_cause = ?1,
             updated_at = ?2
         WHERE id = ?3
           AND state = 'stopping'",
        params![cause.map(StopCause::as_str), now()?, id],
    )?;
    Ok(changed == 1)
}
