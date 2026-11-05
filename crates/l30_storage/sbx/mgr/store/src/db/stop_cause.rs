//! The node's attribution of a sandbox's last stop.
//!
//! Every stop admission that newly owns a row (moves it to `stopping` from a
//! state no stop owns) records an attribution: the stop's cause, or none for
//! a requested stop, with this node's time of that stop. A stop that
//! re-admits a row another stop already owns keeps that stop's. The
//! `clear_stop_attribution_when_running` trigger drops it when any writer
//! moves the row to `started` or `paused`. A `stopped` row publishes it. The
//! control plane applies a newer one to its row for the sandbox when that row
//! is on the publishing node and isn't `started` or `paused`, whatever else
//! its lifecycle says; it ignores the attribution otherwise.

use super::*;

/// Why a stop no caller requested happened.
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

/// The node's attribution of a row's last stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StopAttribution {
    /// Why the stop happened when no one asked for it; none for a requested
    /// stop, and for a label a newer sbx-manager wrote.
    pub cause: Option<StopCause>,
    /// When this node admitted the stop, in epoch milliseconds of its clock.
    /// Unlike `state_changed_at`, a later failed start doesn't move it.
    pub stopped_at_ms: i64,
}

/// Record the attribution of the stop that just moved the row to
/// `stopping`: `cause`, or none for a requested stop, at this node's current
/// time, replacing whatever an earlier stop left. Returns whether the row was
/// `stopping`. The caller decides that this stop newly owns the row; a stop
/// that re-admits a row another stop owns must not call it.
pub fn record_stop_attribution(
    conn: &Connection,
    id: &str,
    cause: Option<StopCause>,
) -> Result<bool> {
    let changed = conn.execute(
        "UPDATE sandboxes
         SET stop_cause = ?1,
             stop_cause_at = ?2,
             updated_at = ?3
         WHERE id = ?4
           AND state = 'stopping'",
        params![
            cause.map(StopCause::as_str),
            jiff::Timestamp::now().as_millisecond(),
            now()?,
            id
        ],
    )?;
    Ok(changed == 1)
}
