//! Cluster-singleton writer of `controller.kubernetes.io/pod-deletion-cost` on
//! data-plane pods, so a scale-in removes the replicas holding the fewest
//! attached streams first.
//!
//! The ReplicaSet controller ranks pods for deletion by, among other things,
//! that annotation: lower cost goes first. Without it, the replica holding
//! hundreds of attached streams is as likely to be picked as one holding none.
//! This loop reads each Ready data-plane pod's internal stream-load report
//! (served on a listener that is not part of the public Service), buckets its
//! count of attached streams, and patches the annotation.
//!
//! Every attached stream counts, whatever its age: a scale-in cuts a day-old
//! terminal or port-forward just as surely as a new one. The cost only orders
//! deletion (lower first, best effort); it can never block or delay a scale-in,
//! so a replica that keeps a high cost because its streams are long-lived is
//! simply removed last, which is where it belongs. "Attached stream" means a
//! connection holding the data plane's attached-stream permit; plain HTTP
//! requests through ECW service ingress, including long responses such as
//! server-sent events, hold none and don't count (see
//! `DataPlaneStreamLoad::active_streams`).
//!
//! The data plane does not write its own annotation: that would grant the
//! internet-facing pod `patch` on pods in its namespace. This process is
//! leader-elected and needs `list`, `watch` and `patch` on pods in the
//! data-plane namespace. RBAC cannot narrow that grant to one annotation: pod
//! `patch` also allows changing labels and container images. A deployment that
//! wants it narrower can add a ValidatingAdmissionPolicy limiting this service
//! account's pod updates to that annotation.
//!
//! Properties worth keeping if this is refactored:
//!
//! - **Bucketed, rate-limited writes.** The value is one of four buckets and is
//!   written only when the bucket changes. Lowering a pod's cost waits at least
//!   [`ReconcilerConfig::min_write_interval`] since its last write; raising it
//!   doesn't, because every moment a busy pod still reads low is a moment a
//!   scale-in removes it first. Every write attempt, raise or lowering, resets
//!   the interval, so a pod gets at most one lowering per interval plus one
//!   raise per bucket step (at most three), and a load oscillating across one
//!   bucket boundary is written about twice per 40s. Pod updates fan out to
//!   every watcher of the namespace, so a raw count would be a write per stream.
//! - **Unknown load ranks as busy.** One failed or timed-out probe writes
//!   nothing; after [`ReconcilerConfig::unknown_as_busy_after`] consecutive
//!   failures the pod is written the top bucket. Keeping the last value would
//!   keep a stale low one too: an idle pod whose probes start failing while it
//!   takes streams would stay first in line. Ranking unknown as busy only ever
//!   errs toward removing that pod last. That holds per pod; if the controller
//!   loses most of the fleet at once, those pods tie at the top and any pod it
//!   can still read moves to the front, which is what the `unknown` alert is
//!   for. Such pods count in the `unknown` bucket of
//!   `data_plane_deletion_cost.pods`; a Ready pod that stays unreadable is a
//!   dead internal listener (the data plane logs it and keeps serving), a
//!   network problem, or overload, and worth alerting on.
//! - **Disabling stops the writes, not the ranking.** Turning the reconciler off
//!   leaves each pod's last value in place until the pod is replaced.
//! - **Best effort.** This orders deletion; it guarantees nothing. When every
//!   replica is busy they tie at the top bucket, and the data plane's own drain
//!   bound is what protects the streams on the pod that goes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_metrics::data_plane_deletion_cost::{CostBucket, ProbeOutcome, WriteOutcome};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::env_config;

mod backend;
#[cfg(test)]
mod tests;

use backend::{DeletionCostBackend, KubernetesDeletionCostBackend};

/// The annotation the ReplicaSet controller reads when choosing which pods to
/// remove on scale-down.
pub const POD_DELETION_COST_ANNOTATION: &str = "controller.kubernetes.io/pod-deletion-cost";

const ENABLED_ENV: &str = "SPACE_DATA_PLANE_DELETION_COST_ENABLED";
const NAMESPACE_ENV: &str = "SPACE_DATA_PLANE_DELETION_COST_NAMESPACE";
const POD_SELECTOR_ENV: &str = "SPACE_DATA_PLANE_DELETION_COST_POD_SELECTOR";

const DEFAULT_NAMESPACE: &str = "space";
const DEFAULT_POD_SELECTOR: &str = "app.kubernetes.io/name=space-data-plane";
const DEFAULT_TICK: Duration = Duration::from_secs(10);
const DEFAULT_MIN_WRITE_INTERVAL: Duration = Duration::from_secs(30);
/// Ceiling on one stream-load read. The report is one atomic load on the pod,
/// so anything slower is a pod that is not answering.
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_secs(2);
/// Ceiling on one annotation patch, so a wedged apiserver call cannot hold the
/// tick indefinitely.
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(5);
/// Probes and patches in flight at once. Worst case a tick takes
/// `ceil(pods / 8) * DEFAULT_PROBE_TIMEOUT + ceil(writes / 8) * DEFAULT_WRITE_TIMEOUT`:
/// 7s for 8 pods whose probes take just under the timeout and whose patches
/// all hang, which fits in one tick; a larger fleet in that state stretches
/// the tick, and `MissedTickBehavior::Delay` pushes the next one back rather
/// than stacking them. Healthy probes answer in milliseconds.
const DEFAULT_CONCURRENCY: usize = 8;
/// Consecutive failed probes before a pod ranks as busy. At the default 10s
/// tick the third failure comes 20s after the first (30s after the last good
/// probe), so one dropped request changes nothing.
const DEFAULT_UNKNOWN_AS_BUSY_AFTER: u32 = 3;

/// One of the four values the writer publishes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeletionCost {
    Idle,
    Low,
    Medium,
    High,
}

impl DeletionCost {
    /// Bucket a count of attached streams on an order-of-magnitude scale: 0,
    /// 1–9, 10–99, 100+. Idle is the bucket that matters most — it is what puts
    /// a replica with nothing to cut first in line.
    pub fn from_active_streams(active_streams: u64) -> Self {
        match active_streams {
            0 => Self::Idle,
            1..=9 => Self::Low,
            10..=99 => Self::Medium,
            _ => Self::High,
        }
    }

    /// Parse an annotation value this writer could have written; `None` for
    /// anything else (a hand-set value, an older writer's).
    fn from_annotation(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|cost| cost.as_str() == value)
    }

    /// The annotation value: the cost the ReplicaSet controller ranks by.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "0",
            Self::Low => "10",
            Self::Medium => "100",
            Self::High => "1000",
        }
    }

    fn metric(self) -> CostBucket {
        match self {
            Self::Idle => CostBucket::Idle,
            Self::Low => CostBucket::Low,
            Self::Medium => CostBucket::Medium,
            Self::High => CostBucket::High,
        }
    }

    const ALL: [Self; 4] = [Self::Idle, Self::Low, Self::Medium, Self::High];
}

impl std::fmt::Display for DeletionCost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The data-plane pods one tick sees.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DataPlanePods {
    /// Pods to probe and annotate: Ready, not terminating, addressable.
    pub candidates: Vec<DataPlanePod>,
    /// Every non-terminating pod, Ready or not. Per-pod history is kept for
    /// these, so it survives a pod dropping out of `candidates` for a while.
    pub live_uids: HashSet<String>,
}

/// A data-plane pod the writer may annotate: Ready, not terminating, and
/// addressable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPlanePod {
    pub name: String,
    /// Sent as a patch precondition, so a write planned against one pod never
    /// lands on a replacement that reused its name.
    pub uid: String,
    pub pod_ip: String,
    /// The pod's `internal` container port, where its stream-load listener is.
    pub port: u16,
    /// The raw annotation value, deliberately unparsed: anything can be on it
    /// (a hand-run `kubectl annotate`, a value from an older writer), and
    /// comparing the raw string means every value that is not exactly the
    /// desired one is rewritten.
    pub current: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub namespace: String,
    pub pod_selector: String,
    pub tick: Duration,
    /// Minimum time before a pod's cost is lowered again, measured from the
    /// start of its last write attempt to the tick that plans the lowering.
    /// With a 10s tick that is at least 30s, usually 30–40s; slow ticks stretch
    /// it further. Raises aren't held back.
    pub min_write_interval: Duration,
    /// Consecutive failed probes after which a pod's load is treated as the top
    /// bucket. See the module docs for why unknown ranks as busy.
    pub unknown_as_busy_after: u32,
    pub probe_timeout: Duration,
    pub write_timeout: Duration,
    pub concurrency: usize,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: DEFAULT_NAMESPACE.to_string(),
            pod_selector: DEFAULT_POD_SELECTOR.to_string(),
            tick: DEFAULT_TICK,
            min_write_interval: DEFAULT_MIN_WRITE_INTERVAL,
            unknown_as_busy_after: DEFAULT_UNKNOWN_AS_BUSY_AFTER,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            write_timeout: DEFAULT_WRITE_TIMEOUT,
            concurrency: DEFAULT_CONCURRENCY,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(env: &dyn pplx_space_common_core::env::Environment) -> Self {
        let default = Self::default();
        let config = env_config::EnvConfig(env);
        let non_empty = |var: &str| {
            env.var(var)
                .ok()
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        Self {
            enabled: config.truthy_default(ENABLED_ENV, default.enabled),
            // An explicit namespace wins; the controller's own namespace is the
            // fallback, since the chart deploys both into the release namespace.
            namespace: non_empty(NAMESPACE_ENV)
                .or_else(|| non_empty("POD_NAMESPACE"))
                .unwrap_or(default.namespace),
            pod_selector: non_empty(POD_SELECTOR_ENV).unwrap_or(default.pod_selector),
            ..default
        }
    }
}

/// What to do with one probed pod's annotation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteDecision {
    Unchanged,
    RateLimited,
    Write(DeletionCost),
}

/// Decide one pod's write. An absent annotation already means cost 0 to the
/// ReplicaSet controller, so an idle pod with no annotation needs no write.
/// Only a lowering is rate-limited: a raise, or a value this writer doesn't
/// recognize, is written at once.
fn decide(
    current: Option<&str>,
    desired: DeletionCost,
    last_write: Option<Instant>,
    now: Instant,
    min_write_interval: Duration,
) -> WriteDecision {
    let matches = match current {
        Some(value) => value == desired.as_str(),
        None => desired == DeletionCost::Idle,
    };
    if matches {
        return WriteDecision::Unchanged;
    }
    let current_cost = match current {
        Some(value) => DeletionCost::from_annotation(value),
        None => Some(DeletionCost::Idle),
    };
    let lowering = current_cost.is_some_and(|current| desired < current);
    if !lowering {
        return WriteDecision::Write(desired);
    }
    match last_write {
        Some(at) if now.saturating_duration_since(at) < min_write_interval => {
            WriteDecision::RateLimited
        }
        _ => WriteDecision::Write(desired),
    }
}

/// Per-pod-UID memory across ticks: the last write attempt, and how many
/// probes in a row have failed. Entries are dropped once the pod is gone or
/// terminating, not when it is merely NotReady, so both are bounded by the
/// selector's non-terminating pods (the Deployment plus any Failed pods not yet
/// garbage-collected) and a readiness flap resets neither.
#[derive(Debug, Default)]
struct WriteHistory {
    last_write: HashMap<String, Instant>,
    failing: HashMap<String, u32>,
}

impl WriteHistory {
    fn get(&self, uid: &str) -> Option<Instant> {
        self.last_write.get(uid).copied()
    }

    fn record(&mut self, uid: String, at: Instant) {
        self.last_write.insert(uid, at);
    }

    /// Mark a failed probe; returns how many in a row have now failed.
    fn probe_failed(&mut self, uid: &str) -> u32 {
        let count = self.failing.entry(uid.to_string()).or_default();
        *count = count.saturating_add(1);
        *count
    }

    /// Mark a successful probe; true when it ends a run of failures.
    fn probe_succeeded(&mut self, uid: &str) -> bool {
        self.failing.remove(uid).is_some()
    }

    fn retain_live(&mut self, live_uids: &HashSet<String>) {
        self.last_write.retain(|uid, _| live_uids.contains(uid));
        self.failing.retain(|uid, _| live_uids.contains(uid));
    }
}

pub async fn run(
    substrate: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "data-plane deletion-cost reconciler is disabled ({ENABLED_ENV} unset); skipping"
        );
        shutdown_token.cancelled().await;
        return;
    }
    let backend = match KubernetesDeletionCostBackend::spawn(substrate, &config) {
        Ok(backend) => backend,
        Err(error) => {
            tracing::error!(
                %error,
                "data-plane deletion-cost reconciler could not build its probe client; \
                 not running"
            );
            shutdown_token.cancelled().await;
            return;
        }
    };
    run_with_backend(backend, config, shutdown_token).await
}

pub async fn run_with_backend<B: DeletionCostBackend>(
    backend: B,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    tracing::info!(
        namespace = %config.namespace,
        pod_selector = %config.pod_selector,
        tick_secs = config.tick.as_secs(),
        min_write_interval_secs = config.min_write_interval.as_secs(),
        "starting data-plane deletion-cost reconciler"
    );
    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut history = WriteHistory::default();
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                tracing::info!("data-plane deletion-cost reconciler shutting down");
                return;
            }
            _ = interval.tick() => {
                reconcile_once(&backend, &config, &mut history).await;
            }
        }
    }
}

async fn reconcile_once<B: DeletionCostBackend>(
    backend: &B,
    config: &ReconcilerConfig,
    history: &mut WriteHistory,
) {
    let Some(DataPlanePods {
        candidates: pods,
        live_uids,
    }) = backend.pods()
    else {
        tracing::debug!("data-plane pod reflector is cold; skipping deletion-cost tick");
        return;
    };
    history.retain_live(&live_uids);

    // Probes fan out: they are independent reads of independent pods, and a
    // serial loop would let one slow pod delay every other pod's update.
    let probed: Vec<(DataPlanePod, Result<DeletionCost, ProbeFailure>)> =
        futures_util::stream::iter(pods)
            .map(|pod| async move {
                let cost = probe_one(backend, config, &pod).await;
                (pod, cost)
            })
            .buffer_unordered(config.concurrency)
            .collect()
            .await;

    let mut known = Vec::new();
    let mut unknown = 0usize;
    let mut unchanged = 0u64;
    let mut rate_limited = 0u64;
    let mut writes = Vec::new();
    let now = Instant::now();
    for (pod, cost) in probed {
        let cost = match cost {
            Ok(cost) => {
                if history.probe_succeeded(&pod.uid) {
                    tracing::info!(
                        pod = %pod.name,
                        "data-plane stream-load probe recovered"
                    );
                }
                cost
            }
            Err(failure) => {
                unknown += 1;
                let failures = history.probe_failed(&pod.uid);
                // Logged at the start of a run; the switch to busy is logged
                // with its write below. An unreachable pod is two lines, not
                // one every tick.
                if failures == 1 {
                    tracing::warn!(
                        pod = %pod.name,
                        failure = %failure,
                        unknown_as_busy_after = config.unknown_as_busy_after,
                        "data-plane stream-load probe failed; the pod ranks as busy once \
                         this many probes in a row have failed"
                    );
                } else {
                    tracing::debug!(
                        pod = %pod.name,
                        failures,
                        failure = %failure,
                        "data-plane stream-load probe still failing"
                    );
                }
                if failures < config.unknown_as_busy_after {
                    continue;
                }
                match decide(
                    pod.current.as_deref(),
                    DeletionCost::High,
                    history.get(&pod.uid),
                    now,
                    config.min_write_interval,
                ) {
                    WriteDecision::Unchanged => unchanged += 1,
                    WriteDecision::RateLimited => rate_limited += 1,
                    WriteDecision::Write(desired) => {
                        tracing::warn!(
                            pod = %pod.name,
                            failures,
                            "data-plane stream-load probe keeps failing; ranking the pod \
                             as busy until a probe succeeds"
                        );
                        writes.push((pod, desired));
                    }
                }
                continue;
            }
        };
        known.push(cost);
        match decide(
            pod.current.as_deref(),
            cost,
            history.get(&pod.uid),
            now,
            config.min_write_interval,
        ) {
            WriteDecision::Unchanged => unchanged += 1,
            WriteDecision::RateLimited => rate_limited += 1,
            WriteDecision::Write(desired) => writes.push((pod, desired)),
        }
    }
    for bucket in DeletionCost::ALL {
        pplx_space_metrics::data_plane_deletion_cost::pods(
            bucket.metric(),
            known.iter().filter(|cost| **cost == bucket).count(),
        );
    }
    pplx_space_metrics::data_plane_deletion_cost::pods(CostBucket::Unknown, unknown);
    pplx_space_metrics::data_plane_deletion_cost::writes(WriteOutcome::Unchanged, unchanged);
    pplx_space_metrics::data_plane_deletion_cost::writes(WriteOutcome::RateLimited, rate_limited);

    // An attempt counts against the interval whether or not it lands, so a
    // lowering whose patch keeps failing is retried at the write rate; a raise
    // is retried every tick until it lands.
    // Stamped when the attempt starts, not when it was queued, so a slow
    // apiserver that holds writes behind the concurrency limit can't shorten
    // a pod's interval.
    let attempts: Vec<(String, Instant, WriteOutcome)> = futures_util::stream::iter(writes)
        .map(|(pod, desired)| async move {
            let started = Instant::now();
            let outcome = write_one(backend, config, &pod, desired).await;
            (pod.uid, started, outcome)
        })
        .buffer_unordered(config.concurrency)
        .collect()
        .await;
    let mut outcomes = Vec::with_capacity(attempts.len());
    for (uid, started, outcome) in attempts {
        history.record(uid, started);
        outcomes.push(outcome);
    }
    let applied = outcomes
        .iter()
        .filter(|outcome| **outcome == WriteOutcome::Applied)
        .count();
    pplx_space_metrics::data_plane_deletion_cost::writes(WriteOutcome::Applied, applied as u64);
    pplx_space_metrics::data_plane_deletion_cost::writes(
        WriteOutcome::Error,
        (outcomes.len() - applied) as u64,
    );
}

/// Why a pod's load is unknown this tick.
#[derive(Debug)]
enum ProbeFailure {
    Error(backend::ProbeError),
    Timeout(Duration),
}

impl std::fmt::Display for ProbeFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error(error) => write!(f, "{error}"),
            Self::Timeout(after) => write!(f, "timed out after {}ms", after.as_millis()),
        }
    }
}

/// Read one pod's load and bucket it.
async fn probe_one<B: DeletionCostBackend>(
    backend: &B,
    config: &ReconcilerConfig,
    pod: &DataPlanePod,
) -> Result<DeletionCost, ProbeFailure> {
    match tokio::time::timeout(config.probe_timeout, backend.probe(pod)).await {
        Ok(Ok(load)) => {
            pplx_space_metrics::data_plane_deletion_cost::probe(ProbeOutcome::Ok);
            Ok(DeletionCost::from_active_streams(load.active_streams))
        }
        Ok(Err(error)) => {
            pplx_space_metrics::data_plane_deletion_cost::probe(ProbeOutcome::Error);
            Err(ProbeFailure::Error(error))
        }
        Err(_) => {
            pplx_space_metrics::data_plane_deletion_cost::probe(ProbeOutcome::Timeout);
            Err(ProbeFailure::Timeout(config.probe_timeout))
        }
    }
}

async fn write_one<B: DeletionCostBackend>(
    backend: &B,
    config: &ReconcilerConfig,
    pod: &DataPlanePod,
    desired: DeletionCost,
) -> WriteOutcome {
    match tokio::time::timeout(config.write_timeout, backend.set_cost(pod, desired)).await {
        Ok(Ok(())) => {
            tracing::info!(
                pod = %pod.name,
                previous = pod.current.as_deref().unwrap_or("<unset>"),
                desired = %desired,
                "data-plane pod deletion cost updated"
            );
            WriteOutcome::Applied
        }
        Ok(Err(error)) => {
            tracing::warn!(
                pod = %pod.name,
                desired = %desired,
                %error,
                "failed to update data-plane pod deletion cost"
            );
            WriteOutcome::Error
        }
        Err(_) => {
            tracing::warn!(
                pod = %pod.name,
                desired = %desired,
                timeout_secs = config.write_timeout.as_secs(),
                "data-plane pod deletion-cost write timed out"
            );
            WriteOutcome::Error
        }
    }
}
