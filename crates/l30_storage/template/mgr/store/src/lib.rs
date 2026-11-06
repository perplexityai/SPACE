//! template-manager's node-local SQLite store.
//!
//! It owns `node_dirty_ref_revision`, the node-wide counter whose value
//! template-manager's `node_dirty_refs` triggers stamp onto each enqueued row,
//! so that acknowledging a sent reference deletes only the obligation that was
//! sent. template-manager's other tables have no owning store yet.
//!
//! [`node_dirty_refs`] holds the statements that read and acknowledge those
//! revisions. Each function takes a `rusqlite::Connection`, so a caller can
//! pass its own open transaction, and returns rusqlite's error unchanged.

#![forbid(unsafe_code)]

pub mod node_dirty_refs;
