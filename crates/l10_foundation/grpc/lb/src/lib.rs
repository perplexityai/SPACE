//! Client-side gRPC load balancing over a dynamically discovered endpoint set.
//!
//! A `tonic::transport::Channel` is one HTTP/2 connection and a Kubernetes
//! ClusterIP picks its backend pod once, at dial time, so a long-lived client
//! sends every RPC to whichever replica it first reached. This crate replaces
//! that single connection with one per ready pod, balanced per request, by
//! watching the target Service's EndpointSlices.
//!
//! The balanced channel is never worse than the address it replaces. A fallback
//! endpoint — the Service address itself — stays in the balancer whenever
//! discovery has nothing to offer: before the first resolution, when it resolves
//! to no ready pods, and once a failing watch's last set goes stale. So a missing
//! RBAC rule or an unreachable apiserver degrades to today's behaviour instead of
//! failing requests.
//!
//! What tonic's balancer does, and does not, do: it selects uniformly at random
//! among ready endpoints. Its per-endpoint load metric is hard-coded to zero, so
//! the power-of-two-choices comparison always ties and nothing steers away from a
//! slow-but-responding pod. A pod that saturates its HTTP/2 stream limit does
//! drop out of the ready set until it clears.

use std::collections::BTreeSet;
use std::net::SocketAddr;

mod channel;
#[cfg(feature = "k8s")]
mod kubernetes;

/// The ready endpoints of one Service, at one point in time.
pub type EndpointSet = BTreeSet<SocketAddr>;

/// One resolver observation: the Service's current ready endpoints, or notice
/// that resolving them is currently failing.
///
/// A healthy watch is silent between endpoint changes — bookmarks and routine
/// reconnects produce nothing — so the consumer cannot infer failure from
/// silence. A resolver states it, and that is what starts the staleness clock
/// on the last delivered set; a later `Endpoints` stops it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Endpoints(EndpointSet),
    Failed,
}

pub use channel::balanced_channel;
#[cfg(feature = "k8s")]
pub use kubernetes::{KubernetesConnectError, KubernetesEndpoints, PortSelector};
