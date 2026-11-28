//! template-manager's node-local SQLite store.
//!
//! It owns `node_dirty_ref_revision`, the node-wide counter whose value
//! template-manager's `node_dirty_refs` triggers stamp onto each enqueued row,
//! so that acknowledging a sent reference deletes only the obligation that was
//! sent. It owns template-manager's other tables too, and holds every statement
//! template-manager's server issues against them outside its tests, except the
//! `pplx-space-common-jobs` helper's own statements, which its worker runs on
//! the same connection through [`jobs`].
//!
//! [`node_dirty_refs`] holds the statements that enqueue, read and acknowledge
//! those references, [`network_policy_notifications`] the ones that enqueue,
//! read and acknowledge the sandbox network-policy outbox, and [`pins`] those
//! of the digest pins, including the prunes that bind one placeholder per
//! listed template and two per kept key and so build their text per call. [`builds`] reads, touches,
//! records and deletes template builds with their child rows,
//! [`template_local_state`] reads and deletes a template's rows across
//! tables, and [`eviction`] holds the cache eviction's pin check, its
//! `ready → deleting` swap, the reaper's resolutions of a build left
//! `deleting` and startup recovery's reset of one. [`snapshots`],
//! [`pools`] and [`template_settings`] hold the statements for those tables.
//! [`database`] migrates and opens the database, which template-manager does
//! once at startup.
//!
//! The service shares the one [`Connection`] [`database::open`] returns
//! through a [`SharedConnection`]. Every function outside [`database`] and
//! [`jobs`] that runs a statement takes a [`Connection`], so a caller can
//! pass the connection its held [`ConnectionGuard`] views or its own open
//! [`Transaction`], or takes a [`Transaction`] where its statements are only
//! correct together. None of them opens a connection or a transaction:
//! callers keep both, with their transaction behavior, and their checks
//! between statements. Each fails with [`Error`], which wraps rusqlite's,
//! except [`template_settings::try_update_template_network_policy_tx`],
//! whose error also reports a generation conflict. `AGENTS.md` lists the
//! rules for this crate's public surface.
//!
//! [`jobs`] is the exception. [`SharedConnection::background_worker`] builds
//! template-manager's job worker over the connection template-manager shares
//! with it. The worker locks that connection itself to run its statements,
//! so a caller must not hold that lock while it calls a [`jobs::JobWorker`]
//! method or awaits one's future, and [`jobs::JobWorker::submit_json`] fails
//! with `tonic::Status`. template-manager reaches the job worker only through
//! [`jobs::JobWorker`]; `BackgroundWorker` is never re-exported.
//!
//! A guarded write, whose `WHERE` re-checks the state or witness its caller
//! decided on, moves here verbatim with that guard, binds only the caller's
//! values, and returns whether or how many rows it matched. Matching none,
//! because another writer won or nothing is left to do, is a normal outcome,
//! not an error; the caller decides what each outcome means.

#![forbid(unsafe_code)]

pub mod builds;
mod codec;
pub mod database;
pub mod eviction;
mod handle;
pub mod jobs;
pub mod network_policy_notifications;
pub mod node_dirty_refs;
pub mod pins;
pub mod pools;
pub mod snapshots;
pub mod template_local_state;
pub mod template_settings;
#[cfg(test)]
mod test_db;

pub use handle::{Connection, ConnectionGuard, Error, Result, SharedConnection, Transaction};

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
