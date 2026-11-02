//! Runtime launches the node's runtime supervisor performs.
//!
//! - `runtime_supervisor.jailer.duration_ms` (timing): one sample per
//!   Firecracker jailer run, from spawn until the jailer exited or was killed
//!   at its start timeout. The jailer sets up the chroot, cgroups and
//!   namespaces and then daemonizes Firecracker, so this excludes the wait for
//!   Firecracker's pidfile and API socket that follows. Tag `outcome`, from
//!   [`JailerOutcome`]; `timed_out` samples sit at or just above the
//!   configured limit, since they include the kill and reap.

use std::time::Duration;

use crate::emit::{tag, timing_ms};

metric_vocabulary!(
    /// How one jailer run ended.
    JailerOutcome {
        /// The jailer exited with status zero.
        Succeeded => "succeeded",
        /// The jailer exited non-zero, or could not be waited on.
        Failed => "failed",
        /// The jailer was still running at its start timeout and was killed.
        TimedOut => "timed_out",
    }
);

pub fn jailer_finished(outcome: JailerOutcome, elapsed: Duration) {
    timing_ms(
        "runtime_supervisor.jailer.duration_ms",
        elapsed.as_secs_f64() * 1_000.0,
        &[tag("outcome", outcome)],
    );
}
