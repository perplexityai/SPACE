use std::time::Instant;

use crate::emit::{counter, elapsed_ms, tag, timing_ms};

metric_vocabulary!(ForkPhase {
    InvalidPayload => "invalid_payload",
    SourceFreeze => "source_freeze",
    ArtifactMaterialization => "artifact_materialization",
    TargetRestore => "target_restore",
    Completed => "completed",
});

metric_vocabulary!(ForkSourceLifecycle {
    Unfrozen => "unfrozen",
    Started => "started",
    Paused => "paused",
    Suspended => "suspended",
});

metric_vocabulary!(ForkAttemptOutcome {
    Succeeded => "succeeded",
    Failed => "failed",
    TransientRetry => "transient_retry",
    WorkflowWait => "workflow_wait",
});

metric_vocabulary!(ForkRetryKind {
    TransientFailure => "transient_failure",
    WorkflowProgress => "workflow_progress",
    ActivationBackpressure => "activation_backpressure",
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForkOutcome {
    Completed,
    Failed,
    DeadlineExceeded,
}

impl ForkOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl std::fmt::Display for ForkOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn completed(outcome: ForkOutcome) {
    counter(
        "sandbox.fork.completed",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

/// Record one executor attempt in the persisted workflow phase. The latency is
/// attempt latency rather than end-to-end phase latency because a phase may be
/// resumed by another controller process after a durable requeue. A completed
/// intermediate phase records success even when the same claim immediately
/// continues into the next phase; only a real requeue records a retry.
pub fn phase_attempt(
    phase: ForkPhase,
    source_lifecycle: ForkSourceLifecycle,
    outcome: ForkAttemptOutcome,
    started_at: Instant,
) {
    let tags = [
        tag("phase", phase.as_str()),
        tag("source_lifecycle", source_lifecycle.as_str()),
        tag("outcome", outcome.as_str()),
    ];
    counter("sandbox.fork.phase.attempted", 1, &tags);
    timing_ms(
        "sandbox.fork.phase.duration_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

pub fn retry(phase: ForkPhase, source_lifecycle: ForkSourceLifecycle, kind: ForkRetryKind) {
    counter(
        "sandbox.fork.retry",
        1,
        &[
            tag("phase", phase.as_str()),
            tag("source_lifecycle", source_lifecycle.as_str()),
            tag("kind", kind.as_str()),
        ],
    );
}
