//! The environment placement gate's signals, in every service that places
//! sandboxes (`space-api` and `space-cluster-controller`).
//!
//! On Direct, `would_refuse` is emitted only while
//! `SPACE_ENVIRONMENT_PLACEMENT_GATE` is `shadow`: each increment is the
//! candidate nodes one placement statement returned that the gate, on, would
//! have left out. Zero across a soak is the signal that turning it on refuses
//! nothing today's placement uses. On Kubernetes, the cluster controller's
//! census emits `running_pods` and `pending_pods` while the gate is shadow or
//! on.

use pplx_space_metrics_emit::{counter, gauge, metric_vocabulary, tag};

metric_vocabulary!(
    /// The placement statement that returned the candidate.
    EnvironmentPlacementPath {
        /// A create from a template the node holds, through a fresh snapshot.
        CachedCreate => "cached_create",
        /// The create fallback that tolerates a missing or stale snapshot.
        LastKnownCreate => "last_known_create",
        /// A create that pulls the template onto the node.
        PullCreate => "pull_create",
        /// A warm-pool member to adopt.
        PoolAdoption => "pool_adoption",
        /// A restore on the node holding the sandbox's local state.
        RestoreSource => "restore_source",
        /// A restore from durable storage on another node.
        RestoreRemote => "restore_remote",
    }
);

/// `count` candidates one placement statement returned that the gate would
/// refuse for `reason`, the refusal's wire value
/// (`pplx_space_node_status_store::EnvironmentRefusal::as_str`).
pub fn would_refuse(path: EnvironmentPlacementPath, reason: &'static str, count: usize) {
    counter(
        "environment_placement.would_refuse",
        u64::try_from(count).unwrap_or(u64::MAX),
        &[tag("path", path.as_str()), tag("reason", reason)],
    );
}

metric_vocabulary!(
    /// How a bound sandbox pod's node compares with the
    /// environment it records, on Kubernetes.
    RunningPodPlacement {
        /// The node is labeled with the pod's environment.
        Admitted => "admitted",
        /// The node carries no environment label at all: a node-ctl build
        /// that predates them, or a node-manager with no valid observation.
        NoEvidence => "no_evidence",
        /// The node is labeled with other environments, not this one.
        EnvironmentNotUsable => "environment_not_usable",
    }
);

/// How many bound sandbox pods already on a node, running or still starting,
/// fall in each [`RunningPodPlacement`], from the cluster controller's census while the
/// gate is shadow or on. Anything but `admitted` is a pod the gate, on, would
/// not have placed there.
pub fn running_pods(placement: RunningPodPlacement, count: usize) {
    gauge(
        "environment_placement.running_pods",
        count as f64,
        &[tag("placement", placement.as_str())],
    );
}

metric_vocabulary!(
    /// Why a bound sandbox pod the gate, on, built is Pending: no node in
    /// the cluster carries its environment's label.
    PendingPodReason {
        /// No node carries any environment label: the whole fleet lacks
        /// evidence, as on a new cluster or after a node-manager rollout whose
        /// observation node-ctl can't decode.
        NoEvidence => "no_evidence",
        /// Nodes carry environment labels, none of them this pod's.
        EnvironmentNotUsable => "environment_not_usable",
    }
);

/// How many bound sandbox pods are Pending, ruled unschedulable, because no
/// node carries the environment label they require, by
/// [`PendingPodReason`], from the cluster controller's census while the gate
/// is shadow or on. Non-zero is work the gate, on, is holding back.
pub fn pending_pods(reason: PendingPodReason, count: usize) {
    gauge(
        "environment_placement.pending_pods",
        count as f64,
        &[tag("reason", reason.as_str())],
    );
}
