use crate::emit::{counter, tag};

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
