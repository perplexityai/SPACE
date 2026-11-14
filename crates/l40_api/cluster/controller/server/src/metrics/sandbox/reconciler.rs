//! Metrics emitted by the orphaned-sandbox reconciler running in the
//! `space-cluster-controller` singleton binary.

use std::{fmt, time::Instant};

use pplx_space_common_core::types::SandboxLifecycle;

use pplx_space_metrics_emit::{counter, elapsed_ms, tag, timing_ms};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassificationReason {
    Normal,
    Forgotten,
    LegacyDeleting,
}

impl ClassificationReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Forgotten => "forgotten",
            Self::LegacyDeleting => "legacy_deleting",
        }
    }
}

impl fmt::Display for ClassificationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClassificationOutcome {
    Deleted,
    Suspended,
    Quarantined,
    QuarantineBlocked,
    Deferred,
}

impl ClassificationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deleted => "deleted",
            Self::Suspended => "suspended",
            Self::Quarantined => "quarantined",
            Self::QuarantineBlocked => "quarantine_blocked",
            Self::Deferred => "deferred",
        }
    }
}

impl fmt::Display for ClassificationOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Mode tag distinguishing dry-run observation from live writes. Both
/// modes increment the same metric; dashboards filter by `mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    DryRun,
    Live,
}

impl Mode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Mode::DryRun => "dry_run",
            Mode::Live => "live",
        }
    }
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodEvidence {
    TerminalSucceeded,
    TerminalFailed,
    Unready,
    Missing,
    NotApplicable,
}

impl PodEvidence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TerminalSucceeded => "terminal_succeeded",
            Self::TerminalFailed => "terminal_failed",
            Self::Unready => "unready",
            Self::Missing => "missing",
            Self::NotApplicable => "not_applicable",
        }
    }
}

impl fmt::Display for PodEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn rows_scanned(count: u32) {
    counter("sandbox.api.reconciler.rows_scanned", u64::from(count), &[]);
}

/// `reason` is bounded to the orphan-kind tag set: `normal`, `forgotten`,
/// `legacy_deleting` (matching the active `OrphanKind` variants in the
/// reconciler).
/// `outcome` is bounded to the shared planned/committed disposition set:
/// `deleted`, `suspended`, `quarantined`, or `deferred`. `mode`
/// distinguishes dry-run planning from a committed live result.
/// `from_lifecycle` is the closed sandbox lifecycle vocabulary and
/// `pod_evidence` is bounded by the reconciler's presence enum plus
/// `not_applicable` for non-pod classifier paths.
pub fn rows_classified(
    reason: ClassificationReason,
    outcome: ClassificationOutcome,
    mode: Mode,
    from_lifecycle: SandboxLifecycle,
    pod_evidence: PodEvidence,
) {
    counter(
        "sandbox.api.reconciler.rows_classified",
        1,
        &[
            tag("reason", reason),
            tag("outcome", outcome),
            tag("mode", mode.as_str()),
            tag("from_lifecycle", from_lifecycle),
            tag("pod_evidence", pod_evidence),
        ],
    );
}

/// One row the reconciler deferred, by why. `reason` is bounded:
/// `in_flight_snapshot`, `pod_cleanup_unconfirmed`, `node_reports_local_copy`.
/// `node_reports_local_copy` counts a plan that saw a node's report land
/// after the sweep listed the row; the next listing skips that row while the
/// node keeps reporting it, so it usually counts once.
pub fn rows_deferred(reason: &str, mode: Mode) {
    counter(
        "sandbox.api.reconciler.rows_deferred",
        1,
        &[tag("reason", reason), tag("mode", mode.as_str())],
    );
}

/// `reason` is bounded: `pod_present`, `within_grace`, `rate_limited`,
/// `pod_presence_unknown`.
pub fn rows_skipped(reason: &str, count: u32) {
    counter(
        "sandbox.api.reconciler.rows_skipped",
        u64::from(count),
        &[tag("reason", reason)],
    );
}

/// End-to-end duration of one reconcile tick.
pub fn loop_latency(started_at: Instant) {
    timing_ms(
        "sandbox.api.reconciler.loop.latency_ms",
        elapsed_ms(started_at),
        &[],
    );
}
