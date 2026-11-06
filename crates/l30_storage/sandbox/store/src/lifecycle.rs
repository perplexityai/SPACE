//! The complete stored-lifecycle vocabulary for `sandboxes.lifecycle`.
//!
//! [`SandboxLifecycle`] is the shared *live-state* vocabulary: every state a
//! running control-plane path may act on. The Postgres column can also hold
//! the terminal tombstone `deleted`, which the shared enum deliberately omits
//! because no live-state consumer may act on it. [`StoredSandboxLifecycle`]
//! closes that gap at the database boundary: it is the shared boundary type
//! for consumers that must handle both live states and the tombstone, so raw
//! lifecycle strings do not travel past those SQL boundaries.
//!
//! `cleanup_required` is not a stored value. It is a node-local
//! sandbox-manager cleanup status that no control-plane writer has stored, so a
//! row holding it parses as an unknown lifecycle like any other stray text.

use std::str::FromStr;

use pplx_space_common_core::types::{SandboxLifecycle, SandboxLifecycleParseError};

/// A `sandboxes.lifecycle` value as stored in Postgres: a live
/// [`SandboxLifecycle`] state or the terminal tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StoredSandboxLifecycle {
    Live(SandboxLifecycle),
    /// Terminal tombstone: the sandbox was deleted; the row survives only for
    /// idempotent delete replies and tombstone retention.
    Deleted,
}

impl StoredSandboxLifecycle {
    /// The stable string form stored in `sandboxes.lifecycle`. Round-trips
    /// losslessly through [`FromStr`].
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Live(lifecycle) => lifecycle.as_str(),
            Self::Deleted => "deleted",
        }
    }

    /// The live-state view: `Some` for shared-vocabulary states, `None` for
    /// the terminal tombstone.
    pub const fn live(self) -> Option<SandboxLifecycle> {
        match self {
            Self::Live(lifecycle) => Some(lifecycle),
            Self::Deleted => None,
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
            other => other.parse().map(Self::Live),
        }
    }
}

#[cfg(test)]
mod tests;
