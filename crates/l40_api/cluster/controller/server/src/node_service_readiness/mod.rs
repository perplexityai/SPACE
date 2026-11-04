//! Cluster-singleton publisher of node-local service readiness for sandbox
//! placement.
//!
//! Every SPACE worker runs a set of node-local daemons as DaemonSets —
//! node-ctl, volume-manager, and friends. A sandbox pod that lands on a node
//! whose daemons are mid-restart does not fail: the create *waits* for them.
//! During a worker DaemonSet rollout, nodes can still look schedulable while
//! creates placed on them wait for the daemon restart.
//!
//! The node cannot advertise this itself — node-ctl, which owns the node's
//! other placement labels, is one of the daemons that goes away. So this loop
//! watches the node-local DaemonSet pods cluster-wide and publishes the verdict
//! onto each worker Node as
//! [`NODE_SERVICES_READY_LABEL`](pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL).
//! The sandbox pod builder turns that label into a soft weighted preference and
//! the API drops not-ready nodes from the restore fallback shortlist, so new
//! sandboxes drain off a rolling node and return to it once it is serviced.
//!
//! Properties worth keeping if this is refactored:
//!
//! - **Soft, never hard.** Nothing here makes a node unschedulable. If the only
//!   capacity left is mid-rollout, sandboxes still land there — slow, which is
//!   the pre-existing worst case, rather than rejected.
//! - **Absence of a pod is not-ready.** A DaemonSet rolling update deletes the
//!   old pod before creating the replacement, so for part of the window the
//!   node has no pod for that component at all. That is exactly the interval
//!   the bias must cover.
//! - **Terminating counts as not-ready.** A pod under deletion still reports
//!   `Ready` until its container stops, so the `deletionTimestamp` check is
//!   what makes the signal lead the outage instead of trailing it.
//! - **Unknown components fail open.** Only nodes carrying the sandbox
//!   `nodeSelector` are evaluated, and readers ignore the labels entirely when
//!   too much of the fleet reads as degraded, so a component name that no
//!   longer matches any DaemonSet degrades to "no bias", not to "no placement".
//!
//! Known limitation: a serving pod is matched on `(node, component)` only, not
//! on the release it belongs to. Worker and builder releases both ship a
//! `node-ctl` DaemonSet, so in a dev cluster where the two land on the *same*
//! node the builder's pod can satisfy the worker's verdict. Disjoint
//! `nodeSelector`s avoid that ambiguity. Without them, matching only the
//! component can produce a false ready verdict.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;
use pplx_space_metrics::node_service_readiness::LabelWriteOutcome;
use tokio_util::sync::CancellationToken;

use crate::env_config;

mod backend;
#[cfg(test)]
mod tests;

pub use backend::{KubernetesReadinessBackend, ReadinessBackend};

const DEFAULT_TICK: Duration = Duration::from_secs(5);
/// Bound on label patches issued in one tick. A whole-fleet flip (writer
/// restart against a stale-labelled fleet, or a component list change) is
/// spread over several ticks instead of arriving as one burst of node writes
/// against the apiserver; the remainder converges on the next tick.
const DEFAULT_MAX_WRITES_PER_TICK: usize = 50;
/// Patches in flight at once. A Node label apply is a small, independent write,
/// and the repo's async guidance is to fan these out rather than `await` them in
/// a loop; 8 keeps a full-fleet wave inside the tick budget without becoming an
/// apiserver burst in its own right.
const DEFAULT_WRITE_CONCURRENCY: usize = 8;

/// Ceiling on one Node label write.
///
/// Latency contract for a tick: at most `max_writes_per_tick` patches, each
/// bounded by `min(LABEL_WRITE_TIMEOUT, time left in the tick)`, so the whole
/// action phase fits inside one tick period and ticks cannot pile up behind a
/// slow apiserver — the remainder simply defers. Without a bound at all the
/// kube client's own (multi-minute) default deadline applies, and one wedged
/// patch stalls the loop while every other node's label goes stale.
const LABEL_WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// A node-local SPACE service, named by the `app.kubernetes.io/component`
/// label its DaemonSet pods carry.
///
/// The set is exactly the worker DaemonSets a sandbox create can block on:
/// node-ctl (template presence and the node's placement labels),
/// volume-manager (rootfs and snapshot volumes), sandbox-manager and
/// node-manager (the shim's create path), node-api (readiness and
/// command-ready polling), and template-manager (template pulls). The worker
/// release's other DaemonSets — build-manager, credential-manager, and any an
/// operator adds — are deliberately absent: a create does not wait on them, so a node
/// missing one is not slow to place on.
///
/// Adding a variant is a deliberate act: every node that does not run the new
/// component immediately reads as not-ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeLocalComponent {
    NodeCtl,
    VolumeManager,
    SandboxManager,
    NodeManager,
    NodeApi,
    TemplateManager,
}

impl NodeLocalComponent {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NodeCtl => "node-ctl",
            Self::VolumeManager => "volume-manager",
            Self::SandboxManager => "sandbox-manager",
            Self::NodeManager => "node-manager",
            Self::NodeApi => "node-api",
            Self::TemplateManager => "template-manager",
        }
    }
}

impl std::fmt::Display for NodeLocalComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "unknown node-local component {value:?} (expected one of: node-ctl, volume-manager, \
     sandbox-manager, node-manager, node-api, template-manager)"
)]
pub struct UnknownNodeLocalComponent {
    value: String,
}

impl std::str::FromStr for NodeLocalComponent {
    type Err = UnknownNodeLocalComponent;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "node-ctl" => Ok(Self::NodeCtl),
            "volume-manager" => Ok(Self::VolumeManager),
            "sandbox-manager" => Ok(Self::SandboxManager),
            "node-manager" => Ok(Self::NodeManager),
            "node-api" => Ok(Self::NodeApi),
            "template-manager" => Ok(Self::TemplateManager),
            other => Err(UnknownNodeLocalComponent {
                value: other.to_string(),
            }),
        }
    }
}

/// Published readiness of one node's node-local services.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeServicesReady {
    Ready,
    NotReady,
}

impl NodeServicesReady {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => pplx_space_common_core::kubernetes::NODE_SERVICES_READY_VALUE,
            Self::NotReady => pplx_space_common_core::kubernetes::NODE_SERVICES_NOT_READY_VALUE,
        }
    }
}

impl std::fmt::Display for NodeServicesReady {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One worker Node as the reconciler sees it: its name plus the label value it
/// currently carries (`None` when unlabeled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLabelState {
    pub name: String,
    /// The raw label value, deliberately left unparsed rather than lifted into
    /// [`NodeServicesReady`]. The values *we* write are a closed set, but the
    /// key is a plain Node label: anything can be on it (a hand-run `kubectl
    /// label`, a typo, a value from a future variant). Comparing the raw string
    /// means every value that is not exactly the desired one is rewritten, and
    /// the unexpected value survives into the log line.
    pub current: Option<String>,
}

/// A node-local service pod that is serving right now: `Ready`, not
/// terminating, and bound to a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePod {
    pub node_name: String,
    pub component: NodeLocalComponent,
}

/// A label the reconciler wants to change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLabelChange {
    pub node_name: String,
    pub desired: NodeServicesReady,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReconcilerConfig {
    pub enabled: bool,
    pub tick: Duration,
    pub max_writes_per_tick: usize,
    /// How many patches may be in flight at once. Sequential patches cap the
    /// writer at `max_writes_per_tick / tick`; a full-fleet DaemonSet wave
    /// produces up to two transitions per node per watched component, which at
    /// fleet scale outruns a serial writer and leaves labels trailing the
    /// rollout they are supposed to describe.
    pub write_concurrency: usize,
    /// Share of the evaluated fleet that may read not-ready before the verdicts
    /// are discarded and `Ready` is published everywhere.
    ///
    /// **Coupled to [`Self::components`].** Helm does not serialize DaemonSet
    /// updates, so concurrent waves stack: N watched components each rolling at
    /// the charts' `maxUnavailable: 10%` can leave up to ~10·N% of the fleet
    /// not-ready at once when their unavailable sets are disjoint. The default
    /// is derived from the component count for that reason
    /// ([`default_max_not_ready_fraction`]), and `run_with_backend` refuses to
    /// start if a configured value sits below the union bound its component list
    /// can legitimately produce — that combination would make every routine
    /// rollout fail open, i.e. silently disable the feature.
    pub max_not_ready_fraction: f64,
    /// Components whose pods must all be `Ready` for a node to count as
    /// serviced. Empty is rejected in [`Self::from_env`] — it would mark the
    /// whole fleet ready and silently disable the signal.
    pub components: Vec<NodeLocalComponent>,
}

/// Expected `maxUnavailable` on the worker node-local DaemonSets (`"10%"`).
/// Not read from the live
/// DaemonSet specs: doing so would need a DaemonSet lister plus new read RBAC,
/// and the charts pin the value. If a chart's budget changes, this constant and
/// the derived fail-open threshold have to move with it.
const DAEMONSET_MAX_UNAVAILABLE_FRACTION: f64 = 0.10;

/// Headroom above the rollout union bound for nodes that are genuinely
/// unhealthy (a wedged daemon, a node mid-registration) while a wave is in
/// flight, so ordinary background breakage doesn't fail the signal open.
const NOT_READY_FRACTION_HEADROOM: f64 = 0.15;

/// Smallest fleet the coverage guard applies to.
///
/// A fraction only means something with enough nodes behind it: Kubernetes
/// rounds `maxUnavailable: 10%` *up* to at least one node, so on a handful of
/// nodes a single legitimately-rolling node is already a large share and the
/// guard would fire on every rollout. Below this the guard is skipped entirely —
/// safe, because with that few nodes the bias has nowhere to concentrate load
/// anyway (a soft preference on a 3-node cluster still places the pod).
const MIN_FLEET_FOR_FAIL_OPEN: usize = 10;

/// Hard ceiling on the fail-open threshold. Past half the fleet the signal has
/// stopped describing a rollout no matter how many components are watched.
const MAX_NOT_READY_FRACTION_CEILING: f64 = 0.50;

/// Fail-open threshold implied by `component_count` concurrent DaemonSet waves:
/// the disjoint-union bound (`10%` each) plus headroom, capped at half the
/// fleet.
fn default_max_not_ready_fraction(component_count: usize) -> f64 {
    let union_bound = DAEMONSET_MAX_UNAVAILABLE_FRACTION * component_count as f64;
    (union_bound + NOT_READY_FRACTION_HEADROOM).min(MAX_NOT_READY_FRACTION_CEILING)
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        // The two daemons a sandbox create actually blocks on: node-ctl
        // (template presence / node placement state) and volume-manager
        // (rootfs and snapshot volumes).
        let components = vec![
            NodeLocalComponent::NodeCtl,
            NodeLocalComponent::VolumeManager,
        ];
        Self {
            max_not_ready_fraction: default_max_not_ready_fraction(components.len()),
            components,
            enabled: false,
            tick: DEFAULT_TICK,
            max_writes_per_tick: DEFAULT_MAX_WRITES_PER_TICK,
            write_concurrency: DEFAULT_WRITE_CONCURRENCY,
        }
    }
}

impl ReconcilerConfig {
    pub fn from_env() -> Self {
        let default = Self::default();
        let components = default.components;
        Self {
            enabled: env_config::truthy_default(
                "SPACE_NODE_SERVICE_READINESS_ENABLED",
                default.enabled,
            ),
            tick: default.tick,
            max_writes_per_tick: default.max_writes_per_tick,
            write_concurrency: default.write_concurrency,
            max_not_ready_fraction: default_max_not_ready_fraction(components.len()),
            components,
        }
    }
}

/// One node's verdict: ready only when every configured component has a
/// serving pod on it.
fn node_verdict(
    node: &str,
    serving: &HashSet<(&str, NodeLocalComponent)>,
    components: &[NodeLocalComponent],
) -> NodeServicesReady {
    if components
        .iter()
        .all(|component| serving.contains(&(node, *component)))
    {
        NodeServicesReady::Ready
    } else {
        NodeServicesReady::NotReady
    }
}

/// One node's verdict against a `ServicePod` list. Used by the pre-patch
/// re-check and by [`ReadinessBackend::verdict_now`]'s default body.
pub(super) fn verdict_from_pods(
    node: &str,
    pods: &[ServicePod],
    components: &[NodeLocalComponent],
) -> NodeServicesReady {
    let serving: HashSet<(&str, NodeLocalComponent)> = pods
        .iter()
        .map(|pod| (pod.node_name.as_str(), pod.component))
        .collect();
    node_verdict(node, &serving, components)
}

/// What one tick decided.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LabelPlan {
    changes: Vec<NodeLabelChange>,
    /// Nodes whose current verdict is not-ready, over the whole evaluated set
    /// (including nodes already labelled correctly).
    not_ready: usize,
    /// True when the not-ready set was too large to be a DaemonSet rollout, so
    /// the plan was rewritten to publish `Ready` everywhere. See
    /// [`ReconcilerConfig::max_not_ready_fraction`].
    failed_open: bool,
}

/// Decide this tick's label writes.
///
/// Above [`ReconcilerConfig::max_not_ready_fraction`] the verdicts are
/// discarded and the plan becomes "publish `Ready` everywhere", actively
/// *clearing* `false` labels rather than leaving them for a reader-side valve
/// to ignore. Two reasons the clearing has to happen here:
///
/// * The soft affinity term has no reader-side valve — it can't have one, since
///   the pod spec is rendered before the scheduler ever looks at a node. Only
///   the label going back to `true` un-does it.
/// * Since the template-cache preference is ANDed with readiness, a wide band of
///   stale `false` labels doesn't merely apply a uniform readiness penalty: it
///   strips cache locality from those nodes too, so creates both concentrate on
///   the remainder *and* lose their template affinity. Failing open at the
///   writer removes both effects at once.
fn plan_label_changes(
    nodes: &[NodeLabelState],
    pods: &[ServicePod],
    components: &[NodeLocalComponent],
    max_not_ready_fraction: f64,
) -> LabelPlan {
    let serving: HashSet<(&str, NodeLocalComponent)> = pods
        .iter()
        .map(|pod| (pod.node_name.as_str(), pod.component))
        .collect();

    let verdicts: Vec<(&NodeLabelState, NodeServicesReady)> = nodes
        .iter()
        .map(|node| (node, node_verdict(&node.name, &serving, components)))
        .collect();
    let not_ready = verdicts
        .iter()
        .filter(|(_, verdict)| *verdict == NodeServicesReady::NotReady)
        .count();

    let failed_open = nodes.len() >= MIN_FLEET_FOR_FAIL_OPEN
        && (not_ready as f64) > max_not_ready_fraction * nodes.len() as f64;
    let changes = verdicts
        .into_iter()
        .filter_map(|(node, verdict)| {
            let desired = if failed_open {
                NodeServicesReady::Ready
            } else {
                verdict
            };
            (node.current.as_deref() != Some(desired.as_str())).then(|| NodeLabelChange {
                node_name: node.name.clone(),
                desired,
            })
        })
        .collect();

    LabelPlan {
        changes,
        not_ready,
        failed_open,
    }
}

pub async fn run(
    substrate: Arc<KubernetesSubstrate>,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    if !config.enabled {
        tracing::info!(
            "node-service-readiness reconciler is disabled \
             (SPACE_NODE_SERVICE_READINESS_ENABLED unset); skipping"
        );
        shutdown_token.cancelled().await;
        return;
    }

    let backend = KubernetesReadinessBackend::spawn(substrate, &config);
    run_with_backend(backend, config, shutdown_token).await
}

pub async fn run_with_backend<B: ReadinessBackend>(
    backend: B,
    config: ReconcilerConfig,
    shutdown_token: CancellationToken,
) {
    // An empty component list would make every node read as serviced (the
    // all-of-nothing verdict), silently publishing `true` fleet-wide instead of
    // publishing nothing. `from_env` cannot produce it; refuse to run on the
    // one shape that could — a hand-built config — rather than emit a signal
    // that says every node is fine.
    if config.components.is_empty() {
        tracing::error!(
            "node-service-readiness reconciler has no components configured; \
             refusing to publish a fleet-wide ready verdict"
        );
        shutdown_token.cancelled().await;
        return;
    }

    // The fail-open threshold has to sit above what the watched components'
    // concurrent rollouts can legitimately produce, or every routine deploy
    // trips it and the feature is silently off. Helm doesn't serialize DaemonSet
    // updates, so the bound is the disjoint union of their budgets.
    let union_bound = DAEMONSET_MAX_UNAVAILABLE_FRACTION * config.components.len() as f64;
    if config.max_not_ready_fraction <= union_bound {
        tracing::error!(
            max_not_ready_fraction = config.max_not_ready_fraction,
            union_bound,
            components = config.components.len(),
            "node-service-readiness fail-open threshold is at or below the not-ready \
             share {} concurrent DaemonSet rollouts can produce; every routine deploy \
             would fail open.",
            config.components.len()
        );
        shutdown_token.cancelled().await;
        return;
    }

    tracing::info!(
        tick_secs = config.tick.as_secs(),
        max_writes_per_tick = config.max_writes_per_tick,
        write_concurrency = config.write_concurrency,
        max_not_ready_fraction = config.max_not_ready_fraction,
        components = %config
            .components
            .iter()
            .map(|component| component.as_str())
            .collect::<Vec<_>>()
            .join(","),
        "starting node-service-readiness reconciler"
    );

    let mut interval = tokio::time::interval(config.tick);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut cursor = FairnessCursor::default();
    loop {
        tokio::select! {
            _ = shutdown_token.cancelled() => {
                tracing::info!("node-service-readiness reconciler shutting down");
                return;
            }
            _ = interval.tick() => {
                reconcile_once(&backend, &config, &mut cursor).await;
            }
        }
    }
}

/// Round-robin start point across ticks.
///
/// The change list is rebuilt from the reflector every tick in a stable order,
/// so without a cursor the same node is always attempted first. One node whose
/// patch reliably eats the tick budget — a node-specific admission webhook, an
/// apiserver hang on that object — would then starve every node behind it
/// indefinitely: they would stay stale while the wedged one is retried forever.
/// Advancing past everything attempted this tick means the next tick starts at
/// the first node that didn't get a turn. Mirrors the stuck-ContainerCreating
/// GC's `scan_offset`.
#[derive(Debug, Default)]
struct FairnessCursor {
    offset: usize,
}

impl FairnessCursor {
    /// Rotate the change list so this tick begins where the last one stopped.
    fn apply(&self, changes: &mut [NodeLabelChange]) {
        if changes.is_empty() {
            return;
        }
        changes.rotate_left(self.offset % changes.len());
    }

    /// Advance past the `attempted` changes this tick handled. Wrapping is fine
    /// — `apply` takes it modulo the current list length, so the offset never
    /// needs to track a changing fleet size.
    fn advance(&mut self, attempted: usize) {
        self.offset = self.offset.wrapping_add(attempted);
    }
}

async fn reconcile_once<B: ReadinessBackend>(
    backend: &B,
    config: &ReconcilerConfig,
    cursor: &mut FairnessCursor,
) {
    if !backend.caches_ready() {
        tracing::debug!("node-service-readiness caches are cold; skipping tick");
        return;
    }
    let Some(nodes) = backend.placement_nodes() else {
        tracing::debug!("node reflector is cold; skipping node-service-readiness tick");
        return;
    };
    let pods = backend.serving_pods();
    let plan = plan_label_changes(
        &nodes,
        &pods,
        &config.components,
        config.max_not_ready_fraction,
    );
    let mut changes = plan.changes;
    cursor.apply(&mut changes);

    pplx_space_metrics::node_service_readiness::evaluated_nodes(nodes.len());
    pplx_space_metrics::node_service_readiness::not_ready_nodes(plan.not_ready);
    // Nodes whose published label disagrees with the verdict computed *this*
    // tick. In steady state this is the size of the current rollout wave; if it
    // stays elevated across ticks the writer is not keeping up with the
    // transition rate and labels are trailing the rollout they describe.
    pplx_space_metrics::node_service_readiness::pending_label_changes(changes.len());
    pplx_space_metrics::node_service_readiness::fail_open(plan.failed_open);
    if plan.failed_open {
        tracing::warn!(
            not_ready = plan.not_ready,
            evaluated = nodes.len(),
            max_not_ready_fraction = config.max_not_ready_fraction,
            "node-service-readiness: too much of the fleet reads not-ready to be a \
             DaemonSet rollout; publishing Ready everywhere and clearing stale labels"
        );
    }
    let unchanged = nodes.len().saturating_sub(changes.len());
    pplx_space_metrics::node_service_readiness::label_writes(
        LabelWriteOutcome::Unchanged,
        unchanged as u64,
    );

    let deferred = changes.len().saturating_sub(config.max_writes_per_tick);
    if deferred > 0 {
        tracing::info!(
            deferred,
            max_writes_per_tick = config.max_writes_per_tick,
            "node-service-readiness label changes exceed the per-tick write bound; \
             deferring the remainder to the next tick"
        );
    }

    // Stop issuing patches once the tick's own period is gone, so a slow
    // apiserver defers the remainder instead of stretching one tick across
    // many intervals. See `LABEL_WRITE_TIMEOUT` for the full latency contract.
    let action_deadline = tokio::time::Instant::now() + config.tick;
    let tick_started = tokio::time::Instant::now();
    // Patches fan out rather than running in a loop: they are independent
    // single-object writes, and a serial writer caps the fleet at
    // `max_writes_per_tick / tick` transitions — below what a full-fleet
    // DaemonSet wave produces (two transitions per node per watched component).
    let attempted: usize = futures_util::stream::iter(
        changes
            .iter()
            .take(config.max_writes_per_tick)
            .cloned()
            .collect::<Vec<_>>(),
    )
    .map(|change| async move {
        apply_one_label(backend, config, change, plan.failed_open, action_deadline).await
    })
    .buffer_unordered(config.write_concurrency)
    .filter(|attempted| std::future::ready(*attempted))
    .count()
    .await;

    pplx_space_metrics::node_service_readiness::tick_duration_ms(
        tick_started.elapsed().as_millis() as u64,
    );
    cursor.advance(attempted);
}

/// Publish one node's label. Returns whether the patch was *attempted* — the
/// fairness cursor advances by that count, so a node skipped for a spent budget
/// keeps its place at the front of the next tick.
async fn apply_one_label<B: ReadinessBackend>(
    backend: &B,
    config: &ReconcilerConfig,
    change: NodeLabelChange,
    failed_open: bool,
    action_deadline: tokio::time::Instant,
) -> bool {
    // Clamp to whatever is left of the tick, not just "is there time left".
    // A write that starts a moment before the deadline with the full
    // per-write timeout would run past it — a slow-but-successful patch
    // followed by a wedged one would otherwise cost
    // `patch + LABEL_WRITE_TIMEOUT` and push every other node's next
    // readiness update out behind this tick.
    let remaining = action_deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        tracing::info!(
            node = %change.node_name,
            "node-service-readiness tick budget elapsed; deferring this label write \
             to the next tick"
        );
        return false;
    }

    // Re-check the verdict against the *current* cache before writing. The plan
    // was computed once at the top of the tick, and a rollout moves underneath
    // it: by the time a patch reaches this node its pod may already be back
    // (or newly gone). Publishing the planned value anyway writes a verdict we
    // know to be stale, which then has to be undone on a later tick — and while
    // it stands it steers placement on evidence that no longer holds. Skipped
    // under fail-open, where the desired value is "Ready everywhere" by policy
    // rather than a per-node observation.
    if !failed_open {
        let current = backend.verdict_now(&change.node_name, &config.components);
        if current != change.desired {
            tracing::debug!(
                node = %change.node_name,
                planned = %change.desired,
                current = %current,
                "node-services-ready verdict changed under the plan; skipping the \
                 stale write and re-planning next tick"
            );
            pplx_space_metrics::node_service_readiness::stale_verdict_skipped();
            return true;
        }
    }

    match tokio::time::timeout(
        LABEL_WRITE_TIMEOUT.min(remaining),
        backend.set_label(&change.node_name, change.desired),
    )
    .await
    {
        Err(_) => {
            tracing::warn!(
                node = %change.node_name,
                desired = %change.desired,
                timeout_secs = LABEL_WRITE_TIMEOUT.as_secs(),
                "node-services-ready label write timed out; retrying next tick"
            );
            pplx_space_metrics::node_service_readiness::label_write(LabelWriteOutcome::Error);
        }
        Ok(Ok(())) => {
            tracing::info!(
                node = %change.node_name,
                desired = %change.desired,
                "node-services-ready label reconciled"
            );
            pplx_space_metrics::node_service_readiness::label_write(LabelWriteOutcome::Applied);
        }
        Ok(Err(error)) => {
            tracing::warn!(
                node = %change.node_name,
                desired = %change.desired,
                %error,
                "failed to set node-services-ready label"
            );
            pplx_space_metrics::node_service_readiness::label_write(LabelWriteOutcome::Error);
        }
    }
    true
}

/// Server-side-apply body for one node's label. A dedicated field manager owns
/// exactly this key, so it never fights node-ctl's placement labels.
fn node_label_apply(node_name: &str, desired: NodeServicesReady) -> serde_json::Value {
    let labels: BTreeMap<&str, &str> = BTreeMap::from([(
        pplx_space_common_core::kubernetes::NODE_SERVICES_READY_LABEL,
        desired.as_str(),
    )]);
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "Node",
        "metadata": {
            "name": node_name,
            "labels": labels,
        }
    })
}
