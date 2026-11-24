//! template-manager's node-local SQLite store.
//!
//! It owns `node_dirty_ref_revision`, the node-wide counter whose value
//! template-manager's `node_dirty_refs` triggers stamp onto each enqueued row,
//! so that acknowledging a sent reference deletes only the obligation that was
//! sent. The store owns template-manager's other tables too, but
//! template-manager's server still writes some of them directly until that
//! code moves behind the store.
//!
//! [`node_dirty_refs`] holds the statements that enqueue, read and acknowledge
//! those references, [`network_policy_notifications`] the ones that enqueue
//! and read the sandbox network-policy outbox, and [`pins`] those of the
//! digest pins. [`builds`] reads, touches, records and deletes template
//! builds with their child rows, [`template_local_state`] reads and deletes a
//! template's rows across tables, and [`eviction`] holds the cache eviction's
//! pin check. [`snapshots`], [`pools`] and [`template_settings`] hold the
//! statements for those tables. [`database`] migrates and opens the
//! database, which template-manager does once at startup.
//!
//! Every function outside [`database`] takes a `rusqlite::Connection`, so a
//! caller can pass its own open transaction, or a `rusqlite::Transaction`
//! where its statements are only correct together. None of them opens a
//! connection or a transaction: callers keep both, with their transaction
//! behavior, and their checks between statements. Each returns rusqlite's
//! error, except [`template_settings::try_update_template_network_policy_tx`],
//! whose error also reports a generation conflict.

#![forbid(unsafe_code)]

pub mod builds;
mod codec;
pub mod database;
pub mod eviction;
pub mod network_policy_notifications;
pub mod node_dirty_refs;
pub mod pins;
pub mod pools;
pub mod snapshots;
pub mod template_local_state;
pub mod template_settings;

/// The current time in template-manager's stored timestamp format: whole
/// seconds since the Unix epoch, as text.
pub fn now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    format!("{now}")
}

fn limit_to_i64(limit: usize) -> i64 {
    i64::try_from(limit).unwrap_or(i64::MAX)
}
