//! Template build metrics: each build, its steps and the drain that ends a
//! build-manager.

use std::time::Instant;

use pplx_space_metrics_emit::{elapsed_ms, metric_vocabulary, tag, timing_ms};

/// Terminal result of one build-manager operation; the `result` tag on
/// `template.build.latency_ms` for `op:build`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildResult {
    Success,
    Failed,
    PublishFailed,
}

impl TemplateBuildResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::PublishFailed => "publish_failed",
        }
    }
}

impl std::fmt::Display for TemplateBuildResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Result of one named build step; the `result` tag on
/// `template.build.step.latency_ms`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildStepResult {
    Success,
    Failed,
}

impl TemplateBuildStepResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
        }
    }

    pub fn from_ok(ok: bool) -> Self {
        if ok { Self::Success } else { Self::Failed }
    }
}

impl std::fmt::Display for TemplateBuildStepResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Outcome of the build-manager SIGTERM drain; the `result` tag on
/// `template.build.latency_ms` for `op:drain`. `Timeout` means the
/// process exited with builds still running and is alertable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildDrainResult {
    Completed,
    Timeout,
}

impl TemplateBuildDrainResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Timeout => "timeout",
        }
    }
}

impl std::fmt::Display for TemplateBuildDrainResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

metric_vocabulary!(
    /// Why a build or one of its steps failed: the `error_kind` tag on the
    /// failed `template.build.latency_ms` and `template.build.step.latency_ms`
    /// series, absent on successes. Build-manager's logs and traces are kept
    /// out of the log pipelines because they carry recipe and image content,
    /// so this is the cause that reaches a dashboard. It is classified from
    /// the typed error, never from its message, and carries no user content.
    TemplateBuildErrorKind {
        /// Node disk admission refused the build's rootfs, or the image pull
        /// outgrew the builder's storage budget.
        StorageAdmission => "storage_admission",
        /// The registry rejected the image reference, or pulling it failed.
        ImagePull => "image_pull",
        /// Turning the pulled image into the build rootfs failed.
        Unpack => "unpack",
        /// The recipe was invalid, or one of its steps failed in the guest.
        Recipe => "recipe",
        /// A bounded wait expired: guest SSH readiness, a bounded guest
        /// operation, or the compressed rootfs a publish waits for.
        Timeout => "timeout",
        /// Registering or publishing the built template failed.
        Publish => "publish",
        /// Any other failure: an internal RPC, the builder host, or a panic.
        Other => "other",
    }
);

/// End-to-end latency and count for one terminal build-manager job
/// (recipe execution plus publish/register when enabled). Count
/// operations from this metric; there is no separate completion
/// counter. A failed build adds its [`TemplateBuildErrorKind`].
pub fn build_completed(
    template: &str,
    result: TemplateBuildResult,
    error_kind: Option<TemplateBuildErrorKind>,
    started_at: Instant,
) {
    let mut tags = vec![
        tag("op", "build"),
        tag("template", template),
        tag("result", result),
    ];
    tags.extend(error_kind.map(|kind| tag("error_kind", kind)));
    timing_ms("template.build.latency_ms", elapsed_ms(started_at), &tags);
}

/// Latency for one named step within a build. `step` is the bounded
/// build-manager `BuildPhase` vocabulary (`resolve_base`,
/// `start_build_vm`, `recipe_step`, `compact_rootfs`, `publish`, …);
/// individual recipe step labels stay on traces/logs, not metric tags.
/// A failed step adds its [`TemplateBuildErrorKind`].
pub fn build_step_completed(
    template: &str,
    step: &str,
    result: TemplateBuildStepResult,
    error_kind: Option<TemplateBuildErrorKind>,
    started_at: Instant,
) {
    let mut tags = vec![
        tag("op", "build"),
        tag("template", template),
        tag("step", step),
        tag("result", result),
    ];
    tags.extend(error_kind.map(|kind| tag("error_kind", kind)));
    timing_ms(
        "template.build.step.latency_ms",
        elapsed_ms(started_at),
        &tags,
    );
}

/// Duration and outcome of the build-manager SIGTERM drain, emitted on
/// `template.build.latency_ms` as `op:drain` (no `template` tag — the
/// drain covers every running build on the pod).
pub fn build_drain_completed(result: TemplateBuildDrainResult, started_at: Instant) {
    let tags = [tag("op", "drain"), tag("result", result)];
    timing_ms("template.build.latency_ms", elapsed_ms(started_at), &tags);
}
