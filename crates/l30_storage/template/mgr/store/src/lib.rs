//! template-manager's node-local SQLite store.
//!
//! It owns `node_dirty_ref_revision`, the node-wide counter whose value
//! template-manager's `node_dirty_refs` triggers stamp onto each enqueued row,
//! so that acknowledging a sent reference deletes only the obligation that was
//! sent. template-manager's other tables have no owning store yet.
//!
//! The crate has no statements yet. Its first ones arrive with their first
//! caller, the outbox's revision-guarded acknowledgement, in the same change as
//! the migration that creates the counter.

#![forbid(unsafe_code)]
