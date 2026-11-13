//! The one count interface the per-client live cap reads: how many sandboxes
//! with a live runtime a client holds, as each substrate can see them.

use std::collections::HashSet;

use pplx_space_common_core::types::{ClientId, SandboxId};

/// Which client ids one cap check counts. Admission builds it once per check,
/// from one key-set snapshot, and passes the same value to the pending set,
/// so the substrate count and the pending count select the same sandboxes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CountedIds {
    /// A listed key's own id.
    Exactly(Vec<ClientId>),
    /// The `unknown` key: every id except the listed ones and `native`.
    AllExcept(Vec<ClientId>),
}

impl CountedIds {
    /// Whether a sandbox labeled `id` counts toward this check.
    pub fn counts(&self, id: &ClientId) -> bool {
        match self {
            Self::Exactly(ids) => ids.contains(id),
            Self::AllExcept(excluded) => *id != ClientId::native() && !excluded.contains(id),
        }
    }
}

/// A client's live sandboxes as the substrate sees them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveSandboxCount {
    Known(u64),
    /// The substrate can't count; admission admits with this reason.
    Unknown(LiveSandboxCountUnknown),
}

/// Why a substrate couldn't count. Each is its own metric reason, so a cap
/// that never fires can be told apart from one that can't see.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveSandboxCountUnknown {
    /// No per-client inventory on this substrate.
    Unsupported,
    /// The pod reflector hasn't finished its initial list.
    PodCacheCold,
    /// The pod reflector can't be trusted: its watch failed or is
    /// re-listing.
    PodCacheStale,
}

impl LiveSandboxCountUnknown {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::PodCacheCold => "pod_cache_cold",
            Self::PodCacheStale => "pod_cache_stale",
        }
    }
}

/// The inputs of one count, besides the ids: the sandbox being activated,
/// which never counts against its own activation, and the sandboxes the
/// pending set already counts.
pub struct LiveCountExclusions<'a> {
    pub activating: Option<SandboxId>,
    pub already_pending: &'a HashSet<SandboxId>,
}

impl LiveCountExclusions<'_> {
    pub fn excludes(&self, sandbox: SandboxId) -> bool {
        self.activating == Some(sandbox) || self.already_pending.contains(&sandbox)
    }
}
