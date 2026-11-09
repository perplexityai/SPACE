//! Immutable agent bundles and the crash-recovery contract shared by both slots.
//!
//! Artifact identity is independent of promotion order. The journal decision,
//! not service readiness or either filesystem selector, determines commitment.

pub mod artifact;
pub mod journal;
pub mod policy;

pub use artifact::{ArtifactDigest, Binary, BinaryIdentity, BundleDescriptor, DescriptorError};
pub use journal::{Attempt, Journal, JournalState, Selection, Slot, TrialProgress};

/// Installed unit/selector ABI; updating binaries cannot change this contract.
pub const SEED_CONTRACT: &str = "linux-systemd-selector-v1";
/// Bound descriptor and journal reads independently of untrusted file metadata.
pub const MAX_METADATA_BYTES: usize = 64 * 1024;
/// Bound each agent payload before transfer or hashing begins.
pub const MAX_BINARY_BYTES: u64 = 256 * 1024 * 1024;

#[cfg(test)]
mod tests;

/// Candidate confirmation window for selector-v1, starting at selection.
/// The seed's TimeoutStartSec must cover this window and selector work; changing
/// the installed unit timeout requires qualifying a new seed contract.
pub const TRIAL_CONFIRMATION_WINDOW_MS: u64 = 30_000;
