//! Pure conversions of node-supplied publication evidence.
//!
//! Validation uses no database state, clocks, or generated identifiers. Callers
//! retain the ordering of database guards, rejections, and diagnostics.

pub(crate) mod delete_completions;
pub(crate) mod execution_inventory;
pub(crate) mod execution_observation;
pub(crate) mod failure_episode;
pub(crate) mod fields;
pub(crate) mod node_role;
pub(crate) mod node_status;
pub(crate) mod pending_backup;
pub(crate) mod quarantine;
pub(crate) mod quarantine_claim;
pub(crate) mod receipts;
pub(crate) mod sandbox_identity;
pub(crate) mod sandbox_inventory;
pub(crate) mod sandbox_metadata;
pub(crate) mod sandbox_observation;
pub(crate) mod sandbox_status;
pub(crate) mod snapshots;
pub(crate) mod templates;
