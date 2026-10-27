use std::fmt;
use std::str::FromStr;
use std::time::Instant;

use crate::emit::{counter, distribution, elapsed_ms, gauge, tag, timing_ms};

pub fn queued(facet: &str, result: &str) {
    queued_count(facet, result, 1);
}

pub fn queued_count(facet: &str, result: &str, count: u64) {
    counter(
        "reconcile.queue.request",
        count,
        &[tag("facet", facet), tag("result", result)],
    );
}

pub fn claimed(count: u64) {
    counter("reconcile.queue.claimed", count, &[]);
}

pub fn queue_depth(facet: &str, state: &str, count: u64) {
    gauge(
        "reconcile.queue.depth",
        count as f64,
        &[tag("facet", facet), tag("state", state)],
    );
}

pub fn queue_stale_claims(facet: &str, count: u64) {
    gauge(
        "reconcile.queue.stale_claims",
        count as f64,
        &[tag("facet", facet)],
    );
}

pub fn queue_dead_letter(facet: &str) {
    counter("reconcile.queue.dead_letter", 1, &[tag("facet", facet)]);
}

pub fn trigger_depth(state: &str, count: u64) {
    gauge(
        "reconcile.trigger.depth",
        count as f64,
        &[tag("state", state)],
    );
}

pub fn trigger_dead_letter() {
    counter("reconcile.trigger.dead_letter", 1, &[]);
}

pub fn trigger_fanout(result: &str, error_class: &str) {
    counter(
        "reconcile.trigger.fanout",
        1,
        &[tag("result", result), tag("error_class", error_class)],
    );
}

/// One processed trigger-fanout claim batch: how many triggers the claim held
/// and the wall-clock time from claiming them to writing their completions —
/// target load, live-node filtering, endpoint resolution, the concurrent
/// node-api pokes, and the claim completion/dead-letter writes. A batch that
/// fails to load its targets still emits, so a degraded Postgres shows up
/// here rather than as a silent gap.
pub fn trigger_fanout_batch(claimed: u64, started_at: Instant) {
    distribution(
        "reconcile.trigger.fanout.batch.claimed",
        claimed as f64,
        &[],
    );
    timing_ms(
        "reconcile.trigger.fanout.batch.duration_ms",
        elapsed_ms(started_at),
        &[],
    );
}

/// Which clock a `reconcile.queue.lag_seconds` series runs. The age of a queue
/// ref answers two different questions — "is anything picking work up?" and "is
/// this element able to converge?" — and one number for both misreports queue
/// health, because a single unconvergeable element ages past the entire retry
/// backoff schedule while claim throughput is perfectly healthy.
///
/// The two series are not a partition of the rows; they are two clocks, and a
/// row can appear on both.
///
/// Neither is the same partition as `reconcile.queue.depth{state:…}`, whose
/// `claimed` means "a node currently holds the claim token".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueLagStage {
    /// How long the oldest *claimable* ref has been waiting for a node to pick
    /// it up, measured from when it became claimable — `requested_at` for a ref
    /// that has never failed, or the expiry of its backoff window for one that
    /// has. Rows still inside a backoff window are excluded: nothing is
    /// supposed to claim them yet.
    ///
    /// This is the queue-health signal, and it holds for a ref at any `attempts`
    /// count. A ref that failed once and then sat past an expired backoff
    /// because its node's node-ctl wedged is a claim stall, not a retry, and it
    /// surfaces here — the case a discriminator of `attempts = 0` would hide
    /// forever, since `attempts` only resets on a completion that never comes.
    ///
    /// Bounded by the node-ctl poll interval in a healthy fleet.
    Claim,
    /// Total lifetime of the oldest ref that has failed at least one apply
    /// (`attempts > 0`), measured from `requested_at`, which survives every
    /// re-drive.
    ///
    /// Dominated by the capped-exponential-backoff schedule, not by how fast
    /// nodes claim work: a ref that retries to the attempts cap ages past the
    /// whole backoff sum before it dead-letters, regardless of queue health.
    /// Read it as "some element cannot converge", never as latency.
    Retry,
}

impl QueueLagStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Claim => "claim",
            Self::Retry => "retry",
        }
    }
}

impl fmt::Display for QueueLagStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn queue_lag_seconds(facet: &str, stage: QueueLagStage, lag_seconds: f64) {
    gauge(
        "reconcile.queue.lag_seconds",
        lag_seconds.max(0.0),
        &[tag("facet", facet), tag("stage", stage.as_str())],
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcClass {
    Dead,
    GoneLive,
}

impl QueueGcClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Dead => "dead",
            Self::GoneLive => "gone_live",
        }
    }
}

impl fmt::Display for QueueGcClass {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue GC class must be dead or gone_live")]
pub struct QueueGcClassParseError;

impl FromStr for QueueGcClass {
    type Err = QueueGcClassParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "dead" => Ok(Self::Dead),
            "gone_live" => Ok(Self::GoneLive),
            _ => Err(QueueGcClassParseError),
        }
    }
}

/// Which "is this node gone?" detector a queue-GC instance runs. Both share the
/// same store/tick; the tag lets the EC2 (backing-instance) path and the
/// Kubernetes-node-presence path chart and alert separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcDetector {
    Ec2,
    KubePresence,
}

impl QueueGcDetector {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ec2 => "ec2",
            Self::KubePresence => "kube_presence",
        }
    }
}

impl fmt::Display for QueueGcDetector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Whether a queue-GC tick pruned rows or only observed what it would prune.
/// The observe-first rollout runs `dry_run` before an operator enables writes,
/// so both phases chart separately on the same series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcMode {
    Live,
    DryRun,
}

impl QueueGcMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Live => "live",
            Self::DryRun => "dry_run",
        }
    }
}

impl fmt::Display for QueueGcMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueGcResult {
    Success,
    /// The detector could not establish cluster membership this tick (e.g. a
    /// cold or empty node reflector); the tick failed closed and pruned
    /// nothing.
    CacheCold,
    Error,
}

impl QueueGcResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::CacheCold => "cache_cold",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for QueueGcResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("queue GC result must be success, cache_cold, or error")]
pub struct QueueGcResultParseError;

impl FromStr for QueueGcResult {
    type Err = QueueGcResultParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "success" => Ok(Self::Success),
            "cache_cold" => Ok(Self::CacheCold),
            "error" => Ok(Self::Error),
            _ => Err(QueueGcResultParseError),
        }
    }
}

/// Queue or trigger records deleted by a queue-GC instance. `class` is the
/// record class (`dead` letters vs `gone_live` refs for a gone node);
/// `detector` identifies the gone-signal (EC2 backing-instance vs k8s node
/// presence); `mode` is `live`/`dry_run`. In dry-run the count is always zero.
pub fn queue_gc_pruned(
    class: QueueGcClass,
    detector: QueueGcDetector,
    mode: QueueGcMode,
    count: u64,
) {
    counter(
        "reconcile.queue.gc.pruned",
        count,
        &[
            tag("class", class.as_str()),
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

/// Queue and trigger records that pass the final gone-node eligibility guards
/// within the per-tick budget. Dry-run and live emit the same unit so operators
/// can compare the preview population with the eventual delete population.
pub fn queue_gc_eligible(detector: QueueGcDetector, mode: QueueGcMode, count: u64) {
    counter(
        "reconcile.queue.gc.eligible",
        count,
        &[
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

/// Nodes classified gone with at least one row passing the final database
/// eligibility guards within the tick budget. Same unit in dry-run and live,
/// so the observe-first series is comparable across the cutover.
pub fn queue_gc_gone_nodes(detector: QueueGcDetector, mode: QueueGcMode, count: u64) {
    counter(
        "reconcile.queue.gc.gone_nodes",
        count,
        &[
            tag("detector", detector.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

pub fn queue_gc_tick(result: QueueGcResult, detector: QueueGcDetector) {
    counter(
        "reconcile.queue.gc.tick",
        1,
        &[
            tag("result", result.as_str()),
            tag("detector", detector.as_str()),
        ],
    );
}

pub fn dirty_mark(facet: &str, result: &str) {
    counter(
        "reconcile.dirty.mark",
        1,
        &[tag("facet", facet), tag("result", result)],
    );
}

pub fn run_duration(
    facet: &str,
    direction: &str,
    result: &str,
    trigger: &str,
    started_at: Instant,
) {
    let tags = [
        tag("facet", facet),
        tag("direction", direction),
        tag("result", result),
        tag("trigger", trigger),
    ];
    counter("reconcile.run", 1, &tags);
    timing_ms("reconcile.run.duration_ms", elapsed_ms(started_at), &tags);
}

pub fn run_items(facet: &str, direction: &str, result: &str, count: u64) {
    counter(
        "reconcile.run.items",
        count,
        &[
            tag("facet", facet),
            tag("direction", direction),
            tag("result", result),
        ],
    );
}

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
/// sandbox starts anyway; the gateway holds guest egress until the CA arrives
/// and fails closed if it does not. Routine on the refresh that installs a
/// TLS-on policy. `reason` is the refresh trigger (`start`, `reconcile`, ...).
pub fn network_policy_interception_not_ready(reason: &str) {
    counter(
        "reconcile.network_policy.interception_not_ready",
        1,
        &[tag("reason", reason)],
    );
}

/// Per desired-state request: how many requested sandbox ids had no
/// cluster row. A distribution so both the fleet-wide backlog drain and
/// per-node outliers stay observable while cluster-deleted eviction runs.
pub fn sandbox_desired_state_deleted_reported(node: &str, count: u64) {
    distribution(
        "reconcile.sandbox_desired_state.deleted_reported",
        count as f64,
        &[tag("node", node)],
    );
}

/// The deleted partition exceeded the plausibility threshold and was
/// suppressed instead of returned. Any occurrence warrants immediate
/// investigation of cluster database staleness or misrouting.
pub fn sandbox_desired_state_deleted_suppressed() {
    counter("reconcile.sandbox_desired_state.deleted_suppressed", 1, &[]);
}

/// Outcome vocabulary for [`cluster_deleted_evict`]. Closed so a typo'd
/// literal cannot mint a new metric series silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterDeletedEvictResult {
    Evicted,
    Deferred,
    Failed,
}

impl ClusterDeletedEvictResult {
    pub fn as_str(self) -> &'static str {
        match self {
            ClusterDeletedEvictResult::Evicted => "evicted",
            ClusterDeletedEvictResult::Deferred => "deferred",
            ClusterDeletedEvictResult::Failed => "failed",
        }
    }
}

impl std::fmt::Display for ClusterDeletedEvictResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per cluster-deleted eviction attempt from node-ctl's sandbox
/// desired-state facet sync. `result:evicted|deferred|failed`.
pub fn cluster_deleted_evict(result: ClusterDeletedEvictResult) {
    counter(
        "reconcile.cluster_deleted_evict",
        1,
        &[tag("result", result.as_str())],
    );
}

/// Disposition of a failed sandbox desired-state apply sub-op (port exposure,
/// loopback-bridge open, snapshot, auto-lifecycle) from node-ctl. Closed so a
/// typo'd literal cannot mint a new metric series silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DesiredStateApplyResult {
    /// Transient failure (transport, `Unavailable`, `Internal`, timeouts,
    /// rollout-skew `Unimplemented`): an element error was recorded so the
    /// queue re-drives the ref.
    Retryable,
    /// Terminal failure (the guest is unreachable because the sandbox is
    /// failed/suspending/suspended/stopped, or the local allocation/row is
    /// gone): the ref completes without an element error. The owning lifecycle
    /// or cluster reconciler repairs state; periodic desired-state sync
    /// re-applies after the failure becomes actionable.
    Terminal,
}

impl DesiredStateApplyResult {
    pub fn as_str(self) -> &'static str {
        match self {
            DesiredStateApplyResult::Retryable => "retryable",
            DesiredStateApplyResult::Terminal => "terminal",
        }
    }
}

impl std::fmt::Display for DesiredStateApplyResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One per failed sandbox desired-state apply sub-op from node-ctl's sandbox
/// desired-state facet sync. `result:retryable|terminal`. Terminal completions
/// are the signal that a poison ref was retired instead of retried to the
/// backoff cap.
pub fn desired_state_apply(result: DesiredStateApplyResult) {
    counter(
        "reconcile.desired_state_apply",
        1,
        &[tag("result", result.as_str())],
    );
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoLifecycleDeleteDeliveryOperation {
    List,
    Acknowledge,
}

impl AutoLifecycleDeleteDeliveryOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Acknowledge => "acknowledge",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteDeliveryOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete delivery operation {0:?}")]
pub struct AutoLifecycleDeleteDeliveryOperationParseError(String);

impl FromStr for AutoLifecycleDeleteDeliveryOperation {
    type Err = AutoLifecycleDeleteDeliveryOperationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "list" => Ok(Self::List),
            "acknowledge" => Ok(Self::Acknowledge),
            other => Err(AutoLifecycleDeleteDeliveryOperationParseError(
                other.to_string(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoLifecycleDeleteDeliveryOutcome {
    Succeeded,
    Failed,
    Unimplemented,
}

impl AutoLifecycleDeleteDeliveryOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Unimplemented => "unimplemented",
        }
    }
}

impl fmt::Display for AutoLifecycleDeleteDeliveryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
#[error("unknown auto-lifecycle delete delivery outcome {0:?}")]
pub struct AutoLifecycleDeleteDeliveryOutcomeParseError(String);

impl FromStr for AutoLifecycleDeleteDeliveryOutcome {
    type Err = AutoLifecycleDeleteDeliveryOutcomeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "unimplemented" => Ok(Self::Unimplemented),
            other => Err(AutoLifecycleDeleteDeliveryOutcomeParseError(
                other.to_string(),
            )),
        }
    }
}

pub fn auto_lifecycle_delete_delivery(
    operation: AutoLifecycleDeleteDeliveryOperation,
    outcome: AutoLifecycleDeleteDeliveryOutcome,
) {
    counter(
        "reconcile.auto_lifecycle_delete_delivery",
        1,
        &[tag("operation", operation), tag("outcome", outcome)],
    );
}
