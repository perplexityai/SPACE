//! Cluster-wide network-slot headroom, as activation admission reads it.

use pplx_space_metrics::sandbox::admission::ClusterSlotHeadroom;

/// Whether any start-eligible node has a free `space.pplx.net/network-slots`
/// slot, read from the substrate's caches with no apiserver or database call.
///
/// Every sandbox pod requests one slot, so the scheduler already enforces each
/// node's slot pool. Reading the headroom before a pod exists lets admission
/// refuse the activation instead of leaving a pod `Pending`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkSlotHeadroom {
    /// At least one eligible node holds fewer slots than it advertises.
    /// `slotless_nodes`: some other eligible node advertises no slots.
    Available { slotless_nodes: bool },
    /// No eligible node has a free slot: each has all its slots held, or
    /// advertises none (`slotless_nodes`), so a sandbox pod can't land on it.
    Full { slotless_nodes: bool },
    /// The headroom can't be read, so admission must not act on it.
    Unknown(NetworkSlotHeadroomUnknown),
}

/// Why [`NetworkSlotHeadroom::Unknown`] was returned. Each reason is its own
/// metric value, so a guard that never fires can be told apart from one that
/// can't see.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkSlotHeadroomUnknown {
    /// The substrate has no slot pool to read (Direct runs no pods).
    Unsupported,
    /// The node reflector hasn't finished its initial `LIST`.
    NodeCacheCold,
    /// The sandbox-pod reflector hasn't finished its initial `LIST`, so held
    /// slots would read low.
    PodCacheCold,
}

impl NetworkSlotHeadroom {
    pub const fn metric(self) -> ClusterSlotHeadroom {
        match self {
            Self::Available {
                slotless_nodes: false,
            } => ClusterSlotHeadroom::Available,
            Self::Available {
                slotless_nodes: true,
            } => ClusterSlotHeadroom::AvailableWithSlotlessNodes,
            Self::Full {
                slotless_nodes: false,
            } => ClusterSlotHeadroom::Full,
            Self::Full {
                slotless_nodes: true,
            } => ClusterSlotHeadroom::FullWithSlotlessNodes,
            Self::Unknown(NetworkSlotHeadroomUnknown::Unsupported) => {
                ClusterSlotHeadroom::Unsupported
            }
            Self::Unknown(NetworkSlotHeadroomUnknown::NodeCacheCold) => {
                ClusterSlotHeadroom::NodeCacheCold
            }
            Self::Unknown(NetworkSlotHeadroomUnknown::PodCacheCold) => {
                ClusterSlotHeadroom::PodCacheCold
            }
        }
    }
}
