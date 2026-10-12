use std::fmt;
use std::time::{Duration, Instant};

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchedulerState {
    Running,
    Waiting,
}

impl SchedulerState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Waiting => "waiting",
        }
    }
}

impl fmt::Display for SchedulerState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CancellationOutcome {
    Cancelled,
    CleanupFailed,
}

impl CancellationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::CleanupFailed => "cleanup_failed",
        }
    }
}

impl fmt::Display for CancellationOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn cancellation_requested(kind: &str) {
    counter("jobs.cancellation.requested", 1, &[tag("kind", kind)]);
}

pub fn cancellation_completed(kind: &str, outcome: CancellationOutcome) {
    counter(
        "jobs.cancellation.completed",
        1,
        &[tag("kind", kind), tag("outcome", outcome)],
    );
}

pub fn submitted(kind: &str, dedup: &str) {
    counter(
        "jobs.submitted",
        1,
        &[tag("kind", kind), tag("dedup", dedup)],
    );
}

pub fn admission_rejected(kind: &str, priority_band: &str) {
    counter(
        "jobs.admission_rejected",
        1,
        &[
            tag("kind", kind),
            tag("reason", "active_limit"),
            tag("priority_band", priority_band),
        ],
    );
}

pub fn started(kind: &str, attempt: i32) {
    counter(
        "jobs.started",
        1,
        &[tag("kind", kind), tag("attempt", attempt)],
    );
}

metric_vocabulary!(
    /// Whether a job its executor finished reached the job's goal: the
    /// `result` tag on `jobs.completed` and its `jobs.duration_ms` series.
    /// Failures the executor returns are `jobs.failed` instead.
    JobCompletedResult {
        Success => "success",
        /// The executor completed the job but recorded a domain failure for
        /// its caller to read, as a template build that failed does.
        Failed => "failed",
    }
);

pub fn completed(kind: &str, result: JobCompletedResult, bytes: u64, started_at: Instant) {
    let tags = [tag("kind", kind), tag("result", result)];
    counter("jobs.completed", 1, &tags);
    counter("jobs.bytes", bytes, &[tag("kind", kind)]);
    timing_ms("jobs.duration_ms", elapsed_ms(started_at), &tags);
}

pub fn failed(kind: &str, error_kind: &str, started_at: Instant) {
    let tags = [tag("kind", kind), tag("error_kind", error_kind)];
    counter("jobs.failed", 1, &tags);
    timing_ms(
        "jobs.duration_ms",
        elapsed_ms(started_at),
        &[tag("kind", kind), tag("result", error_kind)],
    );
}

pub fn attempts(kind: &str, terminal: &str, attempts: i32) {
    distribution(
        "jobs.attempts",
        attempts as f64,
        &[tag("kind", kind), tag("terminal", terminal)],
    );
}

pub fn recovered(kind: &str) {
    counter("jobs.recovered", 1, &[tag("kind", kind)]);
}

pub fn completion_observation_delay(kind: &str, delay: Duration) {
    timing_ms(
        "jobs.completion_observation_delay_ms",
        delay.as_secs_f64() * 1_000.0,
        &[tag("kind", kind)],
    );
}

pub fn queue_depth(kind: &str, status: &str, count: u64) {
    gauge(
        "jobs.queue.depth",
        count as f64,
        &[tag("kind", kind), tag("status", status)],
    );
}

pub fn scheduler_depth(kind: &str, state: SchedulerState, priority_band: &str, count: usize) {
    gauge(
        "jobs.scheduler.depth",
        count as f64,
        &[
            tag("kind", kind),
            tag("state", state),
            tag("priority_band", priority_band),
        ],
    );
}

pub fn queue_oldest_pending_age_seconds(kind: &str, age_seconds: f64) {
    gauge(
        "jobs.queue.oldest_pending_age_seconds",
        age_seconds.max(0.0),
        &[tag("kind", kind)],
    );
}

pub fn permanent_failures(kind: &str, count: u64) {
    gauge(
        "jobs.permanent_failures",
        count as f64,
        &[tag("kind", kind)],
    );
}

pub fn drain_outstanding(count: u64) {
    gauge("jobs.drain.outstanding", count as f64, &[]);
}
