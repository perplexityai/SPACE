//! The complete stored-lifecycle vocabulary for `sandboxes.lifecycle`.
//!
//! [`SandboxLifecycle`] is the shared *live-state* vocabulary: every state a
//! running control-plane path may act on. The Postgres column can also hold
//! two terminal tombstone values — `deleted` and `cleanup_required` — that the
//! shared enum deliberately omits because no live-state consumer may act on
//! them. [`StoredSandboxLifecycle`] closes that gap at the database boundary:
//! it is the shared boundary type for consumers that must handle both live
//! states and tombstones, so raw lifecycle strings do not travel past those
//! SQL boundaries.

use std::str::FromStr;

use pplx_space_common_core::types::{SandboxLifecycle, SandboxLifecycleParseError};

/// A `sandboxes.lifecycle` value as stored in Postgres: a live
/// [`SandboxLifecycle`] state or one of the two terminal tombstones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoredSandboxLifecycle {
    Live(SandboxLifecycle),
    /// Terminal tombstone: the sandbox was deleted; the row survives only for
    /// idempotent delete replies and tombstone retention.
    Deleted,
    /// Terminal tombstone: delete-side cleanup is still owed for the sandbox.
    CleanupRequired,
}

impl StoredSandboxLifecycle {
    /// The stable string form stored in `sandboxes.lifecycle`. Round-trips
    /// losslessly through [`FromStr`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live(lifecycle) => lifecycle.as_str(),
            Self::Deleted => "deleted",
            Self::CleanupRequired => "cleanup_required",
        }
    }

    /// The live-state view: `Some` for shared-vocabulary states, `None` for
    /// the terminal tombstones.
    pub const fn live(self) -> Option<SandboxLifecycle> {
        match self {
            Self::Live(lifecycle) => Some(lifecycle),
            Self::Deleted | Self::CleanupRequired => None,
        }
    }
}

impl std::fmt::Display for StoredSandboxLifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StoredSandboxLifecycle {
    type Err = SandboxLifecycleParseError;

    /// Parses a stored lifecycle string. The column is CHECK-less TEXT, so
    /// the parse normalizes case and whitespace defensively; this is the
    /// single ingest point for that normalization.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "deleted" => Ok(Self::Deleted),
            "cleanup_required" => Ok(Self::CleanupRequired),
            other => other.parse().map(Self::Live),
        }
    }
}

#[cfg(test)]
mod tests;
