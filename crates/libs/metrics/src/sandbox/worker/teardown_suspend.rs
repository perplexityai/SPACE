//! Teardown suspend: prepare outcomes, driver capacity, and refused-backup
//! replays.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TeardownSuspendPrepareOutcome {
    Pending,
    Durable,
    Superseded,
    Deleted,
}

impl TeardownSuspendPrepareOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Durable => "durable",
            Self::Superseded => "superseded",
            Self::Deleted => "deleted",
        }
    }
}

impl fmt::Display for TeardownSuspendPrepareOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn teardown_suspend_prepare(outcome: TeardownSuspendPrepareOutcome, started_at: Instant) {
    let tags = [tag("outcome", outcome.as_str())];
    counter("sandbox.worker.teardown_suspend.prepare", 1, &tags);
    timing_ms(
        "sandbox.worker.teardown_suspend.prepare.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Configured node-wide capacity for planned-teardown suspend drivers.
pub fn teardown_suspend_driver_capacity(value: usize) {
    gauge(
        "sandbox.worker.teardown_suspend.driver_capacity",
        value as f64,
        &[],
    );
}

/// A planned-teardown poll could not start its durable-suspend driver because
/// all node-wide driver permits were occupied.
pub fn teardown_suspend_driver_saturated() {
    counter("sandbox.worker.teardown_suspend.driver_saturated", 1, &[]);
}

/// Why a teardown drive rejoined a refused backup upload without starting a
/// new attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TeardownBackupRefusalReplay {
    /// The cluster still refuses the registration as superseded.
    Superseded,
    /// The cluster admits the registration now, but the drive didn't start a
    /// new attempt: the marker couldn't be cleared, the row no longer holds the
    /// capture, or the attempt would register without a generation.
    AdmittedNotCleared,
    /// The cluster's answer says nothing about a new attempt.
    NotSuperseded,
    /// The cluster couldn't be asked.
    Unanswered,
}

impl TeardownBackupRefusalReplay {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Superseded => "superseded",
            Self::AdmittedNotCleared => "admitted_not_cleared",
            Self::NotSuperseded => "not_superseded",
            Self::Unanswered => "unanswered",
        }
    }
}

impl fmt::Display for TeardownBackupRefusalReplay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A teardown drive replayed a refused backup upload's failure instead of
/// starting a new attempt, for `reason`.
pub fn teardown_suspend_backup_refusal_replayed(reason: TeardownBackupRefusalReplay) {
    counter(
        "sandbox.worker.teardown_suspend.backup_refusal_replayed",
        1,
        &[tag("reason", reason.as_str())],
    );
}
