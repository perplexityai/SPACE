//! Pluggable "is this Kubernetes node gone?" detectors for the queue GC.
//!
//! The reconciler is generic over a [`GoneDetector`]; the store, scan, prune,
//! budget, and metrics are shared. Two production detectors differ only in the
//! signal they read:
//!
//! - [`Ec2Detector`] — asks EC2 whether the node's backing instance is gone
//!   (the original signal).
//! - [`KubePresenceDetector`] — requires sustained absence from the
//!   watcher-backed node reflector, then confirms absence with a direct Node
//!   GET. This catches nodes the EC2 signal misses without trusting a stalled
//!   reflector to authorize deletion.
//!
//! Both operate only on nodes the shared scan already found non-`ready`: a node
//! still marked `ready` in SPACE is a lifecycle-tracking failure, not queue
//! debt to reap — correcting its state is a separate concern, and deleting its
//! refs would paper over the bug while the node stays eligible as `ready`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use pplx_space_metrics::reconcile::QueueGcDetector;
use tokio::time::{Instant, timeout};

use super::GoneCandidate;
use crate::orphaned_nodes::{InstanceLookup, InstanceLookupOutcome};

#[cfg(feature = "kubernetes")]
use pplx_space_api::controllers::substrate::kubernetes::KubernetesSubstrate;

/// A node must be absent from the reflector for at least this long before the
/// presence detector calls it gone. Bridges transient absence during Node
/// deletion / re-registration (the reflector is watch-backed and eventually
/// consistent), so a node that flaps and returns is never reaped on a single
/// snapshot. Well above the reflector's resync latency; slower reaping is fine
/// for a retention-cleanup path.
pub const DEFAULT_ABSENCE_GRACE: Duration = Duration::from_secs(600);
pub(crate) const DIRECT_NODE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// A detector's verdict for one candidate node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GoneVerdict {
    /// The node is confirmed gone; its stale refs may be pruned.
    Gone,
    /// The node is present/live; preserve its refs.
    Live,
    /// The detector could not decide (e.g. an EC2 lookup error). Fail closed:
    /// preserve the refs and retry next tick.
    Unknown,
}

pub trait GoneDetector: Send + Sync + 'static {
    /// Which detector this is — becomes the `detector` metric tag.
    fn label(&self) -> QueueGcDetector;

    /// Establish membership for this tick. Returns `false` to fail closed: the
    /// reconciler skips the whole tick (prunes nothing) and reports
    /// `cache_cold`. A per-candidate detector (EC2) always returns `true`; a
    /// snapshot detector (reflector) returns `false` when the snapshot is not
    /// credible (cold or empty cache).
    fn begin_tick(&mut self) -> impl std::future::Future<Output = bool> + Send;

    /// Classify one candidate using the membership established in
    /// [`GoneDetector::begin_tick`]. Takes `&mut self` so a detector can carry
    /// per-node state across ticks (e.g. how long a node has been absent).
    fn classify(
        &mut self,
        candidate: &GoneCandidate,
    ) -> impl std::future::Future<Output = GoneVerdict> + Send;
}

/// EC2-backed detector: the node is gone when its backing instance is
/// `terminated` / absent. Mirrors the original reconcile-queue-GC signal.
pub struct Ec2Detector<L: InstanceLookup> {
    lookup: Arc<L>,
}

impl<L: InstanceLookup> Ec2Detector<L> {
    pub fn new(lookup: L) -> Self {
        Self {
            lookup: Arc::new(lookup),
        }
    }
}

impl<L: InstanceLookup> GoneDetector for Ec2Detector<L> {
    fn label(&self) -> QueueGcDetector {
        QueueGcDetector::Ec2
    }

    async fn begin_tick(&mut self) -> bool {
        // EC2 classifies per-candidate with a live API call; no per-tick
        // snapshot to establish.
        true
    }

    async fn classify(&mut self, candidate: &GoneCandidate) -> GoneVerdict {
        match self
            .lookup
            .lookup_by_private_dns(candidate.k8s_node_name.clone())
            .await
        {
            Ok(InstanceLookupOutcome::Gone) => GoneVerdict::Gone,
            Ok(InstanceLookupOutcome::Live { instance_id, state }) => {
                tracing::debug!(
                    node_id = %candidate.node_id,
                    k8s_node_name = %candidate.k8s_node_name,
                    instance_id,
                    state,
                    "reconcile_queue_gc: backing instance is live; preserving refs"
                );
                GoneVerdict::Live
            }
            Err(error) => {
                tracing::warn!(
                    node_id = %candidate.node_id,
                    k8s_node_name = %candidate.k8s_node_name,
                    %error,
                    "reconcile_queue_gc: EC2 lookup failed; preserving refs"
                );
                GoneVerdict::Unknown
            }
        }
    }
}

/// Live Kubernetes node membership. Production impl:
/// [`SubstrateKubeNodePresence`].
pub trait KubeNodePresence: Send + Sync + 'static {
    type LookupError: std::error::Error + Send + Sync + 'static;

    /// The set of live k8s node names, or `None` if the reflector cache has not
    /// completed its initial `LIST`. `None` means "membership unknown".
    fn live_node_names(&self) -> Option<HashSet<String>>;

    /// Confirm one Node's current API-server presence after sustained absence
    /// from the reflector. Errors mean membership is unknown and fail closed.
    fn node_exists(
        &self,
        node_name: String,
    ) -> impl std::future::Future<Output = Result<bool, Self::LookupError>> + Send;
}

/// Reflector-backed detector: the node is gone when its name has been absent
/// from the live k8s Node-name set for at least `grace`. Keys on the node name
/// (always present on a registered Node) rather than heartbeat (unreliable for
/// k8s nodes) or EC2, and requires *sustained* absence so a node that flaps and
/// returns is never reaped on a single snapshot.
///
/// Once absence exceeds the grace period, the detector performs a bounded
/// direct Node GET. Only a Kubernetes `NotFound` result confirms gone-ness; a
/// present Node or lookup error preserves refs. This prevents a stale but
/// non-empty reflector from authorizing deletion.
pub struct KubePresenceDetector<P: KubeNodePresence> {
    presence: Arc<P>,
    live: Option<HashSet<String>>,
    /// How long a node must stay absent before it is judged gone.
    grace: Duration,
    /// When each currently-absent node name was first observed absent, keyed by
    /// `k8s_node_name`. Reset in `begin_tick` for every name present in the
    /// snapshot — not just scanned candidates — so a node that reappears while
    /// pagination skips it still restarts its grace on the next disappearance,
    /// and cleared entirely on an unknown (cold/empty) snapshot. Bounded by the
    /// count of distinct gone-without-drain node names seen over the process
    /// lifetime (small, and reset on every deploy).
    absent_since: HashMap<String, Instant>,
}

impl<P: KubeNodePresence> KubePresenceDetector<P> {
    /// Construct with the default [`DEFAULT_ABSENCE_GRACE`].
    pub fn new(presence: P) -> Self {
        Self::with_grace(presence, DEFAULT_ABSENCE_GRACE)
    }

    pub fn with_grace(presence: P, grace: Duration) -> Self {
        Self {
            presence: Arc::new(presence),
            live: None,
            grace,
            absent_since: HashMap::new(),
        }
    }
}

impl<P: KubeNodePresence> GoneDetector for KubePresenceDetector<P> {
    fn label(&self) -> QueueGcDetector {
        QueueGcDetector::KubePresence
    }

    async fn begin_tick(&mut self) -> bool {
        // Snapshot the live name set once per tick. Fail closed on a cold cache
        // (`None`) or an empty warm set: a real SPACE cluster always has nodes,
        // so empty almost certainly means the reflector froze rather than that
        // every node left — trusting it would classify every node gone.
        self.live = self.presence.live_node_names();
        match &self.live {
            Some(names) if !names.is_empty() => {
                // Drop timers for nodes present again, so a node that reappeared
                // (even one pagination skipped this tick) measures its *current*
                // contiguous absence on the next disappearance, not the first
                // one ever seen. Timers for still-absent nodes persist so their
                // grace keeps accruing across ticks. Eviction keyed purely on
                // absence duration is deliberately avoided: it would restart the
                // grace on a continuously-absent node and make the observe
                // signal wobble.
                self.absent_since.retain(|name, _| !names.contains(name));
                true
            }
            _ => {
                // Membership is unknown (cold cache or a frozen-empty
                // reflector). We can't attribute absence over this interval, so
                // drop all timers — a recovered snapshot restarts each node's
                // grace from scratch rather than counting the unknown interval
                // as sustained absence.
                self.absent_since.clear();
                false
            }
        }
    }

    async fn classify(&mut self, candidate: &GoneCandidate) -> GoneVerdict {
        let Some(live) = &self.live else {
            // begin_tick returned true, so this should be unreachable; fail
            // closed anyway rather than treating an absent snapshot as "gone".
            return GoneVerdict::Unknown;
        };
        if live.contains(&candidate.k8s_node_name) {
            // Present; begin_tick already reset any grace timer for this name.
            return GoneVerdict::Live;
        }
        // Absent: gone only once it has stayed absent for the grace window.
        let now = Instant::now();
        let first_absent = *self
            .absent_since
            .entry(candidate.k8s_node_name.clone())
            .or_insert(now);
        if now.duration_since(first_absent) < self.grace {
            // Absent but not yet sustained — preserve until it is (or returns).
            return GoneVerdict::Live;
        }

        match timeout(
            DIRECT_NODE_LOOKUP_TIMEOUT,
            self.presence.node_exists(candidate.k8s_node_name.clone()),
        )
        .await
        {
            Ok(Ok(false)) => GoneVerdict::Gone,
            Ok(Ok(true)) => {
                self.absent_since.remove(&candidate.k8s_node_name);
                GoneVerdict::Live
            }
            Ok(Err(error)) => {
                tracing::warn!(
                    node_id = %candidate.node_id,
                    k8s_node_name = %candidate.k8s_node_name,
                    %error,
                    "reconcile_queue_gc: direct Kubernetes node lookup failed; preserving refs"
                );
                GoneVerdict::Unknown
            }
            Err(error) => {
                tracing::warn!(
                    node_id = %candidate.node_id,
                    k8s_node_name = %candidate.k8s_node_name,
                    timeout_secs = DIRECT_NODE_LOOKUP_TIMEOUT.as_secs(),
                    %error,
                    "reconcile_queue_gc: direct Kubernetes node lookup timed out; preserving refs"
                );
                GoneVerdict::Unknown
            }
        }
    }
}

/// Production [`KubeNodePresence`] backed by the substrate's watcher-backed node
/// reflector.
#[cfg(feature = "kubernetes")]
pub struct SubstrateKubeNodePresence {
    substrate: Arc<KubernetesSubstrate>,
}

#[cfg(feature = "kubernetes")]
impl SubstrateKubeNodePresence {
    pub fn new(substrate: Arc<KubernetesSubstrate>) -> Self {
        Self { substrate }
    }
}

#[cfg(feature = "kubernetes")]
impl KubeNodePresence for SubstrateKubeNodePresence {
    type LookupError = kube::Error;

    fn live_node_names(&self) -> Option<HashSet<String>> {
        self.substrate.live_node_names()
    }

    async fn node_exists(&self, node_name: String) -> Result<bool, Self::LookupError> {
        self.substrate.kube_node_exists(&node_name).await
    }
}
