use pplx_space_common_core::types::SandboxLifecycle;
use pplx_space_placement::StoredSandboxLifecycle;
use serde::Serialize;

/// The `status` a node published for a sandbox, parsed once where the
/// publication enters. Every ingest decision on the status reads this, and
/// the ingest SQL compares only its canonical lifecycle, so the database
/// collation does not decide what ingest reads from a node's status.
/// `sandbox_observed_states.status` keeps the node's trimmed spelling, and
/// readers outside ingest compare it themselves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum PublishedStatus {
    /// A stored `sandboxes.lifecycle` value, spelled with any ASCII case and
    /// surrounding whitespace. This is the stored vocabulary, not the
    /// node's: a node's `cleanup_failed` is `Unrecognized`, and its
    /// `cleanup_required` reads as the stored tombstone. No ingest decision
    /// compares either.
    Known(StoredSandboxLifecycle),
    /// Any other text, trimmed. It names no lifecycle, so every ingest
    /// decision on the lifecycle treats it like any status other than the one
    /// it looks for. That includes text outside ASCII that a database
    /// collation would lower-case to a lifecycle (`FAİLED`).
    Unrecognized(String),
}

impl PublishedStatus {
    /// `None` for an absent or blank status.
    pub(super) fn parse(raw: Option<&str>) -> Option<Self> {
        let trimmed = raw?.trim();
        if trimmed.is_empty() {
            return None;
        }
        Some(match trimmed.parse() {
            Ok(lifecycle) => Self::Known(lifecycle),
            Err(_) => Self::Unrecognized(trimmed.to_owned()),
        })
    }

    pub(super) fn lifecycle(&self) -> Option<StoredSandboxLifecycle> {
        match self {
            Self::Known(lifecycle) => Some(*lifecycle),
            Self::Unrecognized(_) => None,
        }
    }

    pub(super) fn is(&self, lifecycle: SandboxLifecycle) -> bool {
        self.lifecycle() == Some(StoredSandboxLifecycle::Live(lifecycle))
    }

    /// The canonical lifecycle, or the trimmed text when it is unrecognized.
    pub(super) fn as_str(&self) -> &str {
        match self {
            Self::Known(lifecycle) => lifecycle.as_str(),
            Self::Unrecognized(text) => text,
        }
    }
}

/// Serializes a parsed status as the `published_lifecycle` the ingest SQL
/// compares: the canonical lifecycle, or `null` when the status is absent or
/// unrecognized.
pub(super) fn serialize_published_lifecycle<S: serde::Serializer>(
    lifecycle: &Option<StoredSandboxLifecycle>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    lifecycle
        .map(StoredSandboxLifecycle::as_str)
        .serialize(serializer)
}

#[cfg(test)]
#[path = "published_status_tests.rs"]
mod tests;
