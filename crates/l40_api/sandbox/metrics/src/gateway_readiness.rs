//! Gateway startup and acquisition measurements. IDs belong in traces,
//! not metric tags. Keep explicit node/build dimensions for distributions.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use pplx_space_k8s_ids::K8sNodeName;

use pplx_space_metrics_emit::{distribution, elapsed_ms, tag};

fn scoped_tags<const N: usize>(labels: [String; N]) -> Vec<String> {
    // DogStatsD distributions do not retain implicit host attribution. Use
    // the deployment's node identity, never a sandbox or guest-supplied name.
    static NODE: OnceLock<Option<K8sNodeName>> = OnceLock::new();
    let node = NODE.get_or_init(|| {
        std::env::var("SPACE_NODE_ID")
            .ok()
            .and_then(|value| K8sNodeName::parse(value).ok())
    });
    let mut tags = Vec::from(labels);
    tags.push(tag(
        "node_name",
        node.as_ref().map_or("unknown", K8sNodeName::as_str),
    ));
    tags.push(tag("git_sha", pplx_space_observability::git_sha()));
    tags
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Outcome {
    Ready,
    Failed,
    TimedOut,
    Cancelled,
    Abandoned,
    Invalidated,
}

impl Outcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Failed => "failed",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Abandoned => "abandoned",
            Self::Invalidated => "invalidated",
        }
    }
}

pub fn startup(
    mode: &str,
    outcome: Outcome,
    total: Duration,
    interception_dependency_wait: Duration,
) {
    let tags = scoped_tags([tag("mode", mode), tag("outcome", outcome.as_str())]);
    distribution(
        "gateway.readiness.duration_ms",
        total.as_secs_f64() * 1000.0,
        &tags,
    );
    distribution(
        "gateway.readiness.interception_dependency_wait_ms",
        interception_dependency_wait.as_secs_f64() * 1000.0,
        &tags,
    );
}

/// One terminal sample, including cancellation when an async future is dropped.
pub struct Wait {
    name: &'static str,
    started: Instant,
    tags: Vec<String>,
    pub outcome: Outcome,
}

impl Wait {
    pub(crate) fn new<const N: usize>(name: &'static str, labels: [String; N]) -> Self {
        Self {
            name,
            started: Instant::now(),
            tags: scoped_tags(labels),
            outcome: Outcome::Cancelled,
        }
    }
}

impl Drop for Wait {
    fn drop(&mut self) {
        self.tags.push(tag("outcome", self.outcome.as_str()));
        distribution(self.name, elapsed_ms(self.started), &self.tags);
    }
}

pub fn ca_acquisition(renewal: bool) -> Wait {
    Wait::new(
        "gateway.ca.acquisition.duration_ms",
        [tag("phase", if renewal { "renewal" } else { "initial" })],
    )
}

pub fn manager_wait(reason: &str) -> Wait {
    let reason = match reason {
        "start" => "start",
        "snapshot-restore" => "restore",
        "resume" => "resume",
        "pool-adoption" | "pool_adoption" => "pool_adoption",
        "pool-adoption-rollback" | "pool_adoption_rollback" => "pool_rollback",
        "reconcile" => "reconcile",
        "set-policy-sync" => "policy_update",
        _ => "other",
    };
    Wait::new(
        "sandbox.worker.net_gateway.readiness_wait.duration_ms",
        [tag("reason", reason)],
    )
}

pub fn after_guest_wait() -> Wait {
    Wait::new(
        "sandbox.worker.net_gateway.after_guest_wait.duration_ms",
        [],
    )
}

/// Manager-observed guest readiness, including connection setup and RPC retries.
/// Shares node/build attribution and cancellation handling with gateway waits.
pub fn spaced_guest_readiness_wait(reason: &str) -> Wait {
    let reason = match reason {
        "start" => "start",
        "resume" => "resume",
        _ => "other",
    };
    Wait::new(
        "sandbox.worker.spaced_guest.readiness_wait.duration_ms",
        [tag("reason", reason)],
    )
}
