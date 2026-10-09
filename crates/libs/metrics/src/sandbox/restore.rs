//! Per-restore placement metrics for source-aware restore placement.
//!
//! [`placement_directed`] fires once per restore pod the API substrate
//! creates, capturing where the pod was directed relative to its snapshot
//! source node and how the source was pinned. A `cross_node` sample with
//! `pin=none` is emitted both when no source was known and when a
//! source-pinned pod was re-driven cross-node after the source left it
//! `Unschedulable`.
//!
//! Emission is request-local, not durably deduplicated: a request that crashes
//! between creating a pod and emitting drops that sample, and a concurrent
//! re-drive race can record one extra. Both distortions are bounded by the
//! crash/concurrency rate of restore recovery — noise well inside the
//! cross-node-share signal this counter feeds — and are deliberately not worth
//! a durable per-execution emission marker.
//!
//! [`cross_node_redrive`] fires once per in-request cross-node re-drive
//! attempt on a `required_affinity` pod the scheduler ruled `Unschedulable`.
//! A re-drive that fails, is dropped, or loses ownership returns before its
//! request emits a placement sample, so this counter is that request's only
//! record of the re-drive.
//!
//! End-to-end restore duration stays on the existing
//! `snapshot.restore.duration_ms` distribution (node side); this module owns
//! only the placement decision and its re-drive.

use crate::emit::{counter, tag};

/// Where a restore pod landed relative to its snapshot's source node. How
/// faithfully the value reports the landing depends on the pin (see
/// [`SourcePin`]): `node_name` bypasses the scheduler (landing = source by
/// construction); `required_affinity` is outcome-corrected — the create path
/// watches the scheduler's decision and records the bound node, so
/// `CrossNode`/`required_affinity` means the pod landed on a shortlist
/// fallback (and a re-driven pod re-records as `pin:none`); `none` carries no
/// source targeting. The legacy `preferred_affinity` is a scoring preference
/// the scheduler may ignore, so a `SameNode`/`preferred_affinity` sample can
/// land cross-node — that gap is the very bug source-aware placement fixes.
/// Share queries and alerts must therefore scope to
/// `pin:required_affinity|none` **and** `placement:same_node|cross_node`
/// (excluding `placement:unknown`, the undecided-at-emit case), where the
/// value reports the landing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestorePlacement {
    /// Directed at the source node (via `spec.nodeName`, a required
    /// `matchFields` pin, or a preferred affinity term) so the source's local
    /// suspend bytes can be reused.
    SameNode,
    /// Directed at any eligible node through the portable S3 snapshot, carrying
    /// no source pin — either no source node was known, or the source-pinned
    /// pod was re-driven cross-node in-request after it went `Unschedulable`.
    CrossNode,
    /// A `required_affinity` pod whose scheduler decision was still undecided
    /// when the synchronous watch budget expired — the landing node is unknown
    /// at emit time, and the pod may still bind to a shortlist fallback. This
    /// is neither a confirmed same-node nor cross-node landing; share/alert
    /// queries must exclude `placement:unknown`.
    Unknown,
}

impl RestorePlacement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SameNode => "same_node",
            Self::CrossNode => "cross_node",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for RestorePlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a restore pod's source node was pinned at create time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourcePin {
    /// `spec.nodeName` direct assignment — no portable S3 fallback existed, so
    /// the pod can only restore from the source node's local bytes.
    NodeName,
    /// Required `nodeAffinity.matchFields: metadata.name` term (source-aware
    /// hard pin). Portable fallback exists; an `Unschedulable` source triggers
    /// an in-request cross-node re-drive.
    RequiredAffinity,
    /// Weight-100 preferred `nodeAffinity` term (legacy soft preference).
    PreferredAffinity,
    /// No source pin — the pod is free to schedule anywhere via S3.
    None,
}

impl SourcePin {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeName => "node_name",
            Self::RequiredAffinity => "required_affinity",
            Self::PreferredAffinity => "preferred_affinity",
            Self::None => "none",
        }
    }
}

impl std::fmt::Display for SourcePin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One restore pod's placement decision, captured at create time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlacementSample {
    pub placement: RestorePlacement,
    /// The source node still holds trusted local suspend bytes (a same-node
    /// restore is a local-copy option, not just an S3 pull).
    pub local_snapshot_present: bool,
    pub pin: SourcePin,
}

/// Emit the per-restore placement decision. Called once per created restore
/// pod on the winning materialization; see the module docs for the bounded
/// crash/concurrency distortions this request-local emission accepts.
pub fn placement_directed(sample: PlacementSample) {
    let tags = [
        tag("placement", sample.placement.as_str()),
        tag("local_snapshot_present", sample.local_snapshot_present),
        tag("pin", sample.pin.as_str()),
    ];
    counter("sandbox.restore.placement", 1, &tags);
}

/// Which request path ran a cross-node re-drive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedrivePath {
    /// The create path's scheduling watch, in the attempt that created the
    /// pinned pod, saw it ruled `Unschedulable`.
    FreshCreate,
    /// The existing-pod path found an already-created pinned pod ruled
    /// `Unschedulable`.
    Retry,
}

impl RedrivePath {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FreshCreate => "fresh_create",
            Self::Retry => "retry",
        }
    }
}

impl std::fmt::Display for RedrivePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How one cross-node re-drive attempt ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedriveOutcome {
    /// A successor without the source pin was created and persisted under the
    /// same generation.
    Redriven,
    /// This attempt created no successor: it found the pod bound, found a pod
    /// without the source pin at the name, or lost the successor POST to a
    /// concurrent re-drive.
    Retained,
    /// This attempt stepped aside without a successor of its own: its
    /// create-claim reset or its successor's persist matched no row it owns.
    /// That doesn't prove another attempt is driving the restore.
    OwnershipLost,
    /// The re-drive returned an error, which the caller propagates. It may
    /// have failed before or after deleting the pinned pod.
    Failed,
    /// The re-drive's future was dropped or unwound before it returned, for
    /// example when a caller's wait budget expired around it, including
    /// during cleanup after an outcome was already reached. It produced no
    /// result, so failure ratios should count it with `Failed`.
    Dropped,
}

impl RedriveOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Redriven => "redriven",
            Self::Retained => "retained",
            Self::OwnershipLost => "ownership_lost",
            Self::Failed => "failed",
            Self::Dropped => "dropped",
        }
    }
}

impl std::fmt::Display for RedriveOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Emit one cross-node re-drive attempt's outcome. A retry that re-drives the
/// same pinned pod again emits again, so this counts attempts, not sandboxes.
pub fn cross_node_redrive(path: RedrivePath, outcome: RedriveOutcome) {
    let tags = [tag("path", path.as_str()), tag("outcome", outcome.as_str())];
    counter("sandbox.restore.redrive", 1, &tags);
}
