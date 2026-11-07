//! Warm-pool acquisition metrics the API server records.

use std::fmt;
use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, tag, timing_ms};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AcquireOutcome {
    Hit,
    Miss,
    Error,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CreatePathOutcome {
    WarmHit,
    OrdinaryMiss,
    SameClaimRetry,
    LaterWarmRecovery,
    ColdFallback,
    ColdDirect,
    Error,
}

/// What admitting a native create did about a warm-pool slot.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum AdmissionClaimOutcome {
    /// A ready slot was claimed and the create took its reserved ID.
    Hit,
    /// The create was eligible, but no ready slot (or no template release)
    /// was available; it was admitted cold.
    Miss,
    /// The request cannot adopt a slot, or admission could not establish
    /// that it can; it was admitted cold.
    Ineligible,
    /// Admission-time claiming is turned off.
    Disabled,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ReservationOutcome {
    Reserved,
    Miss,
    Discarded,
    Error,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClaimLifecycleOutcome {
    Reserved,
    Adopted,
    Released,
    CleanupPending,
    StaleClaimed,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClaimLifecycleReason {
    None,
    RequestCancelled,
    CleanupIntentFailed,
    AdoptionFailed,
    PodReadFailed,
    PodUnusable,
    PrepareFailed,
}

impl AcquireOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Error => "error",
        }
    }
}

impl CreatePathOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WarmHit => "warm_hit",
            Self::OrdinaryMiss => "ordinary_miss",
            Self::SameClaimRetry => "same_claim_retry",
            Self::LaterWarmRecovery => "later_warm_recovery",
            Self::ColdFallback => "cold_fallback",
            Self::ColdDirect => "cold_direct",
            Self::Error => "error",
        }
    }
}

impl AdmissionClaimOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Ineligible => "ineligible",
            Self::Disabled => "disabled",
        }
    }
}

impl ReservationOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Miss => "miss",
            Self::Discarded => "discarded",
            Self::Error => "error",
        }
    }
}

impl ClaimLifecycleOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Adopted => "adopted",
            Self::Released => "released",
            Self::CleanupPending => "cleanup_pending",
            Self::StaleClaimed => "stale_claimed",
        }
    }
}

impl ClaimLifecycleReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::RequestCancelled => "request_cancelled",
            Self::CleanupIntentFailed => "cleanup_intent_failed",
            Self::AdoptionFailed => "adoption_failed",
            Self::PodReadFailed => "pod_read_failed",
            Self::PodUnusable => "pod_unusable",
            Self::PrepareFailed => "prepare_failed",
        }
    }
}

impl fmt::Display for AcquireOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for CreatePathOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ReservationOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ClaimLifecycleOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ClaimLifecycleReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn acquire_duration(template: &str, outcome: AcquireOutcome, started_at: Instant) {
    timing_ms(
        "warm_pool.acquire.duration_ms",
        elapsed_ms(started_at),
        &acquire_tags(template, outcome),
    );
}

pub struct CreatePath<'a> {
    pub template: &'a str,
    pub content_hash: &'a str,
    pub surface: &'a str,
    pub outcome: CreatePathOutcome,
    pub started_at: Instant,
    pub claim_duration_ms: f64,
}

pub fn create_path(sample: CreatePath<'_>) {
    let tags = [
        tag("template_len", sample.template.len()),
        tag("content_hash", sample.content_hash),
        tag("surface", sample.surface),
        tag("outcome", sample.outcome.as_str()),
    ];
    let total_duration_ms = elapsed_ms(sample.started_at);
    timing_ms(
        "warm_pool.create_path.duration_ms",
        total_duration_ms,
        &tags,
    );
    timing_ms(
        "warm_pool.create_path.claim.duration_ms",
        sample.claim_duration_ms,
        &tags,
    );
    timing_ms(
        "warm_pool.create_path.post_claim.duration_ms",
        (total_duration_ms - sample.claim_duration_ms).max(0.0),
        &tags,
    );
}

/// One native create admitted, by what admission did about a warm-pool slot.
/// Tagged by outcome only: no template, sandbox, or org identifiers.
pub fn admission_claim(outcome: AdmissionClaimOutcome) {
    counter(
        "warm_pool.admission_claim",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

pub fn reservation_duration(template: &str, outcome: ReservationOutcome, started_at: Instant) {
    timing_ms(
        "warm_pool.reservation.duration_ms",
        elapsed_ms(started_at),
        &[tag("template", template), tag("outcome", outcome.as_str())],
    );
}

pub fn claim_lifecycle(
    template: &str,
    outcome: ClaimLifecycleOutcome,
    reason: ClaimLifecycleReason,
) {
    counter(
        "warm_pool.claim.lifecycle",
        1,
        &[
            tag("template", template),
            tag("outcome", outcome.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

fn acquire_tags(template: &str, outcome: AcquireOutcome) -> Vec<String> {
    vec![tag("template", template), tag("outcome", outcome.as_str())]
}
