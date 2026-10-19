//! Teardown suspend: prepare outcomes and driver capacity.

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
