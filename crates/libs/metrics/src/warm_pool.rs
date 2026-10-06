use std::{fmt, time::Instant};

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};

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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProbeOutcome {
    Ready,
    Timeout,
    NotStarted,
    Error,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProbeStep {
    Connect,
    WaitReady,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ProbeStepOutcome {
    Success,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PromoteOutcome {
    Promoted,
    PodNotReady,
    ProbeFailed,
}

/// Why the warm-pool reconciler deleted a slot. One tag stream per reason on
/// `warm_pool.reconcile.slots_deleted`, so steady-state churn (excess,
/// mismatched) is separable from the crash/cleanup backstops:
/// `stale_claimed` is the stale-unbound sweep (claims that never bound —
/// process death between claim and bind, or invalidated claims parked for
/// the reaper), `cleanup_pending` is the deferred cleanup of failed pooled
/// prepares, and `dead_claimed` is the suspended-sandbox fossil drain.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SlotDeleteReason {
    MissingPod,
    UnreadyPod,
    Excess,
    Mismatched,
    StaleClaimed,
    DeadClaimed,
    OrphanedReceipt,
    CleanupPending,
    Cordoned,
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

impl ProbeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Timeout => "timeout",
            Self::NotStarted => "not_started",
            Self::Error => "error",
        }
    }
}

impl ProbeStep {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::WaitReady => "wait_ready",
        }
    }
}

impl ProbeStepOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Error => "error",
        }
    }
}

impl PromoteOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Promoted => "promoted",
            Self::PodNotReady => "pod_not_ready",
            Self::ProbeFailed => "probe_failed",
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

impl fmt::Display for ProbeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ProbeStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for ProbeStepOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl SlotDeleteReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MissingPod => "missing_pod",
            Self::UnreadyPod => "unready_pod",
            Self::Excess => "excess",
            Self::Mismatched => "mismatched",
            Self::StaleClaimed => "stale_claimed",
            Self::DeadClaimed => "dead_claimed",
            Self::OrphanedReceipt => "orphaned_receipt",
            Self::CleanupPending => "cleanup_pending",
            Self::Cordoned => "cordoned",
        }
    }
}

impl fmt::Display for PromoteOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SlotDeleteReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn reconcile_summary(created_slots: usize, marked_ready_slots: usize) {
    for (name, count) in [
        ("warm_pool.reconcile.slots_created", created_slots),
        ("warm_pool.reconcile.slots_ready", marked_ready_slots),
    ] {
        if count > 0 {
            counter(name, count as u64, &[]);
        }
    }
}

/// Slots deleted during reconcile, one counter stream per [`SlotDeleteReason`].
/// The reason streams partition the metric: summing over all `reason` tags
/// gives total slot deletions, so the untagged aggregate is not emitted
/// separately (it would double-count).
pub fn slots_deleted(reason: SlotDeleteReason, count: usize) {
    if count > 0 {
        counter(
            "warm_pool.reconcile.slots_deleted",
            count as u64,
            &[tag("reason", reason.as_str())],
        );
    }
}

/// Warm-pool slot pods deleted because they were sitting on a cordoned
/// (draining) node. Unclaimed warm capacity on a node that's being drained
/// is dead weight — the node is about to terminate — so the reconciler
/// evicts it and rebuilds the slot on a schedulable node. These are also
/// counted in `slots_deleted`; this series isolates the cordon-driven
/// subset so a drain-correlated spike is distinguishable from steady-state
/// churn.
pub fn slots_evicted_cordoned(count: usize) {
    if count > 0 {
        counter(
            "warm_pool.reconcile.slots_evicted_cordoned",
            count as u64,
            &[],
        );
    }
}

/// Slots deleted by the dead-claimed-slot drain: `bound` (and legacy
/// pre-stamp `claimed`) receipts whose sandbox is suspended. Bound receipts
/// are excluded from the stale-claimed sweep by state; this drain backstops
/// the inline receipt delete in suspend finalization. Subset of
/// `warm_pool.reconcile.slots_deleted`.
pub fn dead_claimed_slots_drained(count: usize) {
    if count > 0 {
        counter(
            "warm_pool.reconcile.dead_claimed_slots_drained",
            count as u64,
            &[],
        );
    }
}

pub fn reconcile_stage_duration(
    stage: &str,
    content_hash: &str,
    template: &str,
    vcpu: u32,
    memory_mib: u64,
    started_at: Instant,
) {
    timing_ms(
        "warm_pool.reconcile.stage.duration_ms",
        elapsed_ms(started_at),
        &pool_tags(
            template,
            vcpu,
            memory_mib,
            &[tag("content_hash", content_hash), tag("stage", stage)],
        ),
    );
}

pub fn reconcile_tick_duration(started_at: Instant) {
    timing_ms(
        "warm_pool.reconcile.tick.duration_ms",
        elapsed_ms(started_at),
        &[],
    );
}

pub fn reconcile_tick(result_ok: bool) {
    counter(
        "warm_pool.reconcile.tick",
        1,
        &[tag("result", if result_ok { "ok" } else { "error" })],
    );
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

pub fn slots(
    template: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u64,
    managed_by: &str,
    state: &str,
    count: i64,
) {
    gauge(
        "warm_pool.slots",
        count.max(0) as f64,
        &pool_tags(
            template,
            vcpu,
            memory_mib,
            &[
                tag("content_hash", content_hash),
                tag("managed_by", managed_by),
                tag("state", state),
            ],
        ),
    );
}

pub fn reconcile_config_state(
    template: &str,
    content_hash: &str,
    vcpu: u32,
    memory_mib: u64,
    managed_by: &str,
    state: &str,
    value: bool,
) {
    gauge(
        "warm_pool.reconcile.config_state",
        u8::from(value).into(),
        &pool_tags(
            template,
            vcpu,
            memory_mib,
            &[
                tag("content_hash", content_hash),
                tag("managed_by", managed_by),
                tag("state", state),
            ],
        ),
    );
}

pub fn promote_probe_duration(content_hash: &str, outcome: ProbeOutcome, started_at: Instant) {
    timing_ms(
        "warm_pool.promote.probe.duration_ms",
        elapsed_ms(started_at),
        &[
            tag("content_hash", content_hash),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn promote_probe(content_hash: &str, outcome: ProbeOutcome) {
    counter(
        "warm_pool.promote.probe",
        1,
        &[
            tag("content_hash", content_hash),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn promote_probe_step_duration(
    content_hash: &str,
    step: ProbeStep,
    outcome: ProbeStepOutcome,
    started_at: Instant,
) {
    timing_ms(
        "warm_pool.promote.probe.step.duration_ms",
        elapsed_ms(started_at),
        &[
            tag("content_hash", content_hash),
            tag("step", step.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn promote_probe_step(content_hash: &str, step: ProbeStep, outcome: ProbeStepOutcome) {
    counter(
        "warm_pool.promote.probe.step",
        1,
        &[
            tag("content_hash", content_hash),
            tag("step", step.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn promote_attempt(content_hash: &str, outcome: PromoteOutcome) {
    counter(
        "warm_pool.promote.attempt",
        1,
        &[
            tag("content_hash", content_hash),
            tag("outcome", outcome.as_str()),
        ],
    );
}

fn pool_tags(template: &str, vcpu: u32, memory_mib: u64, extra: &[String]) -> Vec<String> {
    let mut tags = Vec::with_capacity(3 + extra.len());
    tags.push(tag("template", template));
    tags.push(tag("pool_vcpu", vcpu));
    tags.push(tag("pool_memory_mib", memory_mib));
    tags.extend_from_slice(extra);
    tags
}

fn acquire_tags(template: &str, outcome: AcquireOutcome) -> Vec<String> {
    vec![tag("template", template), tag("outcome", outcome.as_str())]
}
