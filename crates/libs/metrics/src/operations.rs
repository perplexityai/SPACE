use crate::emit::{counter, distribution, gauge, tag};

pub fn created(api_product: &str, kind: &str, target_type: &str) {
    counter(
        "operations.created",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("target_type", target_type),
        ],
    );
}

pub fn claimed(api_product: &str, kind: &str) {
    counter(
        "operations.claimed",
        1,
        &[tag("api_product", api_product), tag("kind", kind)],
    );
}

/// Which process ran a claimed operation: the cluster-controller dispatcher,
/// or the API request that created it (a native create that won an
/// admission-time warm-pool claim).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationExecutor {
    Dispatcher,
    Inline,
}

impl OperationExecutor {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dispatcher => "dispatcher",
            Self::Inline => "inline",
        }
    }
}

/// [`claimed`], tagged with the executor that took the claim.
pub fn claimed_by(api_product: &str, kind: &str, executor: &str) {
    counter(
        "operations.claimed",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("executor", executor),
        ],
    );
}

/// [`completed`], tagged with the executor that recorded the result.
pub fn completed_by(api_product: &str, kind: &str, state: &str, executor: &str) {
    counter(
        "operations.completed",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("state", state),
            tag("executor", executor),
        ],
    );
}

pub fn completed(api_product: &str, kind: &str, state: &str) {
    counter(
        "operations.completed",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("state", state),
        ],
    );
}

pub fn failed(api_product: &str, kind: &str, error_code: &str) {
    counter(
        "operations.failed",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("error_code", error_code),
        ],
    );
}

pub fn requeued(api_product: &str, kind: &str, reason: &str) {
    counter(
        "operations.requeued",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("reason", reason),
        ],
    );
}

pub fn deadline_exceeded(api_product: &str, kind: &str, last_requeue_reason: &str) {
    counter(
        "operations.deadline_exceeded",
        1,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("last_requeue_reason", last_requeue_reason),
        ],
    );
}

pub fn stale_claim(api_product: &str, kind: &str) {
    counter(
        "operations.stale_claim",
        1,
        &[tag("api_product", api_product), tag("kind", kind)],
    );
}

/// Whether a dispatcher claim is an operation's first attempt or a retry after
/// a requeue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimAttempt {
    First,
    Retry,
}

impl ClaimAttempt {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::First => "first",
            Self::Retry => "retry",
        }
    }
}

/// How long a queued operation waited between becoming claimable and being
/// claimed by the cluster-controller dispatcher.
pub fn queue_wait(api_product: &str, kind: &str, attempt: ClaimAttempt, wait_ms: f64) {
    distribution(
        "operations.queue_wait_ms",
        wait_ms,
        &[
            tag("api_product", api_product),
            tag("kind", kind),
            tag("attempt", attempt.as_str()),
        ],
    );
}

/// Executions the cluster-controller dispatcher is running, against its
/// `execution_concurrency` slot limit; and of those, the executions other
/// than teardowns, against the slots they may hold, every slot but the
/// teardown reservation. A backlog of other work saturates the second pair
/// while the first still shows the reserved slots as headroom.
pub fn dispatcher_slots(
    in_flight: usize,
    capacity: usize,
    other_in_flight: usize,
    other_capacity: usize,
) {
    gauge("operations.dispatcher.in_flight", in_flight as f64, &[]);
    gauge("operations.dispatcher.capacity", capacity as f64, &[]);
    gauge(
        "operations.dispatcher.other_in_flight",
        other_in_flight as f64,
        &[],
    );
    gauge(
        "operations.dispatcher.other_capacity",
        other_capacity as f64,
        &[],
    );
}

/// A step that would have slept before retrying returned to the
/// dispatcher instead, freeing its execution slot. `site` names the step.
pub fn retry_wait_deferred(site: &str) {
    counter("operations.retry_wait_deferred", 1, &[tag("site", site)]);
}

metric_vocabulary!(
    /// How a native create's live-sandbox limits answered it: the `outcome`
    /// tag on `operations.sandbox_quota`.
    SandboxQuotaOutcome {
        Admit => "admit",
        /// The organization's limit refused it.
        RefuseOrg => "refuse_org",
        /// The caller's per-user limit refused it.
        RefuseSubject => "refuse_subject",
        /// Its count moved into the headroom while it waited for the lock, or
        /// the lock wait timed out; it answered a retryable 503.
        Contended => "contended",
    }
);

metric_vocabulary!(
    /// The strongest quota lock a checked create took: the `lock_mode` tag on
    /// `operations.sandbox_quota`.
    SandboxQuotaLockMode {
        /// Refused at the lock-free count, before taking any lock.
        None => "none",
        Shared => "shared",
        Exclusive => "exclusive",
    }
);

/// A genuinely new native create checked against its live-sandbox limits.
/// Creates without a limit, replays and key conflicts are not recorded.
pub fn sandbox_quota(outcome: SandboxQuotaOutcome, lock_mode: SandboxQuotaLockMode) {
    counter(
        "operations.sandbox_quota",
        1,
        &[tag("outcome", outcome), tag("lock_mode", lock_mode)],
    );
}
