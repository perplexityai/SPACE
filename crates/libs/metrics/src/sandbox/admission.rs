use crate::emit::{counter, gauge, tag, timing_ms};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionOperationClass {
    ColdActivation,
    RestoreActivation,
}

impl AdmissionOperationClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ColdActivation => "cold_create",
            Self::RestoreActivation => "restore",
        }
    }
}

impl std::fmt::Display for AdmissionOperationClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDecision {
    Accepted,
    Rejected,
}

impl AdmissionDecision {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

impl std::fmt::Display for AdmissionDecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionRejectReason {
    None,
    ConcurrencyFull,
    RateLimited,
    Disabled,
    EmptyCluster,
    /// The shared token-bucket store (Redis) was reachable but a runtime op
    /// (pool checkout / `EVALSHA`) failed. The cluster limiter fails open, so
    /// this accompanies an `Accepted` decision: admitted despite the store
    /// being down. Alert on a sustained rate.
    StoreUnavailable,
    /// The limiter is enabled but has no usable store (missing or invalid
    /// `SPACE_REDIS_URL`), so it admits every activation for the process's
    /// lifetime. Accompanies an `Accepted` decision and is a steady-state
    /// misconfiguration signal — page on it: the guardrail is silently absent.
    Misconfigured,
    /// The substrate capacity probe (eligible-node count) errored, so the
    /// cluster's start capacity couldn't be assessed. The limiter fails open —
    /// this accompanies an `Accepted` decision — so a transient DB/substrate
    /// blip never rejects a create. Alert on a sustained rate.
    CapacityUnavailable,
}

impl AdmissionRejectReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ConcurrencyFull => "concurrency_full",
            Self::RateLimited => "rate_limited",
            Self::Disabled => "disabled",
            Self::EmptyCluster => "empty_cluster",
            Self::StoreUnavailable => "store_unavailable",
            Self::Misconfigured => "misconfigured",
            Self::CapacityUnavailable => "capacity_unavailable",
        }
    }
}

impl std::fmt::Display for AdmissionRejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionQueueOutcome {
    Accepted,
    TimedOut,
    Cancelled,
}

impl AdmissionQueueOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for AdmissionQueueOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionPermitOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

impl AdmissionPermitOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl std::fmt::Display for AdmissionPermitOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn node_decision(
    operation_class: AdmissionOperationClass,
    decision: AdmissionDecision,
    reason: AdmissionRejectReason,
) {
    counter(
        "node.sandbox_admission.rate.decision",
        1,
        &[
            tag("bucket", "placement"),
            tag("operation_class", operation_class.as_str()),
            tag("decision", decision.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

pub fn node_in_flight(value: usize) {
    gauge(
        "node.sandbox_admission.rate.in_flight",
        value as f64,
        &[tag("bucket", "placement")],
    );
}

pub fn node_operation_in_flight(operation_class: AdmissionOperationClass, value: usize) {
    gauge(
        "node.sandbox_admission.in_flight",
        value as f64,
        &[tag("operation_class", operation_class.as_str())],
    );
}

pub fn node_max_concurrent(value: usize) {
    gauge("node.sandbox_admission.max_concurrent", value as f64, &[]);
}

pub fn node_queue_wait(
    operation_class: AdmissionOperationClass,
    outcome: AdmissionQueueOutcome,
    reason: AdmissionRejectReason,
    value_ms: f64,
) {
    timing_ms(
        "node.sandbox_admission.queue_wait_ms",
        value_ms,
        &[
            tag("operation_class", operation_class.as_str()),
            tag("outcome", outcome.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

pub fn node_permit_hold(
    operation_class: AdmissionOperationClass,
    outcome: AdmissionPermitOutcome,
    value_ms: f64,
) {
    timing_ms(
        "node.sandbox_admission.permit_hold_ms",
        value_ms,
        &[
            tag("operation_class", operation_class.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn node_refill_rps(value: f64) {
    gauge(
        "node.sandbox_admission.rate.refill_rps",
        value,
        &[tag("bucket", "placement")],
    );
}

pub fn cluster_decision(
    operation_class: AdmissionOperationClass,
    decision: AdmissionDecision,
    reason: AdmissionRejectReason,
) {
    counter(
        "cluster.sandbox_admission.rate.decision",
        1,
        &[
            tag("bucket", "sandbox_activation"),
            tag("operation_class", operation_class.as_str()),
            tag("decision", decision.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

/// Liveness tick for the cluster activation limiter, written on a timer rather
/// than on an admission.
///
/// [`cluster_decision`] is written only when an activation is admitted or
/// rejected, so its silence reads the same whether the emitter died or the
/// cluster was idle. Being written on a timer is what breaks that tie; going
/// through the same `counter` function as [`cluster_decision`] — same process,
/// same sink, same DogStatsD socket, same metric kind — is what makes its
/// absence implicate the transport the two share. It carries no limiter state
/// and no call-site tags, so a query of it reads presence, never behavior.
pub fn cluster_heartbeat() {
    counter("cluster.sandbox_admission.rate.heartbeat", 1, &[]);
}

pub fn cluster_tokens(value: f64) {
    gauge(
        "cluster.sandbox_admission.rate.tokens",
        value,
        &[tag("bucket", "sandbox_activation")],
    );
}

pub fn cluster_refill_rps(value: f64) {
    gauge(
        "cluster.sandbox_admission.rate.refill_rps",
        value,
        &[tag("bucket", "sandbox_activation")],
    );
}

pub fn cluster_eligible_nodes(value: u64) {
    gauge(
        "cluster.sandbox_admission.rate.eligible_nodes",
        value as f64,
        &[tag("bucket", "sandbox_activation")],
    );
}
