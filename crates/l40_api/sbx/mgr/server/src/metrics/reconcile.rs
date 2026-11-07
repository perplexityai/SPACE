//! sbx-mgr's reconcile metrics: applying a facet and its network-policy and
//! credential pulls. The facet-keyed queue and run metrics every reconciling
//! service shares are in `pplx-space-reconcile-metrics`.

use std::time::Instant;

use pplx_space_metrics_emit::{counter, elapsed_ms, metric_vocabulary, tag, timing_ms};

pub fn apply_skipped(facet: &str, reason: &str) {
    counter(
        "reconcile.apply.skipped",
        1,
        &[tag("facet", facet), tag("reason", reason)],
    );
}

pub fn apply_applied(facet: &str, outcome: &str) {
    counter(
        "reconcile.apply.applied",
        1,
        &[tag("facet", facet), tag("outcome", outcome)],
    );
}

pub fn apply_failed(facet: &str, error_class: &str) {
    counter(
        "reconcile.apply.failed",
        1,
        &[tag("facet", facet), tag("error_class", error_class)],
    );
}

pub fn apply_conflict(facet: &str) {
    counter("reconcile.apply.conflict", 1, &[tag("facet", facet)]);
}

pub fn drift_observed(facet: &str) {
    counter("reconcile.drift.observed", 1, &[tag("facet", facet)]);
}

pub enum NetworkPolicyFetchOutcome {
    Success,
    Error,
    Timeout,
    Notification,
    Shutdown,
}

metric_vocabulary! {
    /// Outcome of Sandbox Manager resolving the credential material attached
    /// to one effective network-policy pull.
    NetworkCredentialPullOutcome {
        Success => "ok",
        Error => "error",
    }
}

/// One enabled credential-material pull from Sandbox Manager to Credential
/// Manager. Dropping an unfinished observation records an error, including
/// cancellation by the owning `GetNetworkPolicy` deadline.
#[derive(Debug)]
#[must_use]
pub struct NetworkCredentialPull {
    started: Instant,
    outcome: NetworkCredentialPullOutcome,
}

pub fn network_credential_pull() -> NetworkCredentialPull {
    NetworkCredentialPull {
        started: Instant::now(),
        outcome: NetworkCredentialPullOutcome::Error,
    }
}

impl NetworkCredentialPull {
    pub fn finish(mut self, outcome: NetworkCredentialPullOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for NetworkCredentialPull {
    fn drop(&mut self) {
        let tags = [tag("result", self.outcome)];
        counter("reconcile.network_policy.credential_pull", 1, &tags);
        timing_ms(
            "reconcile.network_policy.credential_pull.duration_ms",
            elapsed_ms(self.started),
            &tags,
        );
    }
}

/// One shared background resolution, independent of the number of waiting callers.
pub fn network_policy_fetch(outcome: NetworkPolicyFetchOutcome, started: Instant) {
    let result = match outcome {
        NetworkPolicyFetchOutcome::Success => "ok",
        NetworkPolicyFetchOutcome::Error => "error",
        NetworkPolicyFetchOutcome::Timeout => "timeout",
        NetworkPolicyFetchOutcome::Notification => "notification",
        NetworkPolicyFetchOutcome::Shutdown => "shutdown",
    };
    let tags = [tag("result", result)];
    counter("reconcile.network_policy.fetch", 1, &tags);
    timing_ms(
        "reconcile.network_policy.fetch.duration_ms",
        elapsed_ms(started),
        &tags,
    );
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkPolicyWaitOutcome {
    Success,
    Error,
    Timeout,
    Cancelled,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkPolicyWaitPath {
    Admission,
    Cached,
    Fetch,
}

/// One foreground cache resolution, including admission and cache hits.
/// Dropping an unfinished observation records caller cancellation, not timeout.
#[derive(Debug)]
#[must_use]
pub struct NetworkPolicyWait {
    started: Instant,
    outcome: NetworkPolicyWaitOutcome,
    path: NetworkPolicyWaitPath,
}

pub fn network_policy_wait() -> NetworkPolicyWait {
    NetworkPolicyWait {
        started: Instant::now(),
        outcome: NetworkPolicyWaitOutcome::Cancelled,
        path: NetworkPolicyWaitPath::Admission,
    }
}

impl NetworkPolicyWait {
    pub fn set_path(&mut self, path: NetworkPolicyWaitPath) {
        self.path = path;
    }

    pub fn finish(mut self, outcome: NetworkPolicyWaitOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for NetworkPolicyWait {
    fn drop(&mut self) {
        let result = match self.outcome {
            NetworkPolicyWaitOutcome::Success => "ok",
            NetworkPolicyWaitOutcome::Error => "error",
            NetworkPolicyWaitOutcome::Timeout => {
                counter("reconcile.network_policy.wait_timeout", 1, &[]);
                "timeout"
            }
            NetworkPolicyWaitOutcome::Cancelled => "cancelled",
        };
        let path = match self.path {
            NetworkPolicyWaitPath::Admission => "admission",
            NetworkPolicyWaitPath::Cached => "cached",
            NetworkPolicyWaitPath::Fetch => "fetch",
        };
        let tags = [tag("result", result), tag("path", path)];
        counter("reconcile.network_policy.wait", 1, &tags);
        timing_ms(
            "reconcile.network_policy.wait.duration_ms",
            elapsed_ms(self.started),
            &tags,
        );
    }
}

pub fn network_policy_push(result: &str) {
    counter("reconcile.network_policy.push", 1, &[tag("result", result)]);
}

/// A network policy refresh converged (Ready + matching ref) while the
/// gateway reported its TLS-interception dependencies as not ready. The
/// sandbox starts anyway; guest egress fails closed per connection until the
/// CA arrives. `reason` is the refresh trigger (`start`, `reconcile`, ...).
pub fn network_policy_interception_not_ready(reason: &str) {
    counter(
        "reconcile.network_policy.interception_not_ready",
        1,
        &[tag("reason", reason)],
    );
}
