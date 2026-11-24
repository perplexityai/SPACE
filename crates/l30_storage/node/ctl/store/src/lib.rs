//! node-ctl's node-local SQLite store.
//!
//! It owns node-ctl's database: the `dirty_facets` queue of facets waiting to
//! sync, the `facet_sync_overrides` that pause a facet's sync in one
//! direction, the `reconciliation_status` row each reconciler reports into,
//! and `dirty_node_state`, which no code reads or writes today.
//!
//! [`database`] migrates and opens the database, which node-ctl does at
//! startup. Every other function takes a `rusqlite::Connection`, so a caller
//! passes its own connection or open transaction, and returns rusqlite's
//! error unchanged. node-ctl keeps the transactions whose reads guard its
//! writes: run each verb whose doc asks for it inside the caller's
//! transaction.
//!
//! [`dirty_facets`]' claim, clear and failure writes are guarded transitions:
//! each re-checks the row's claim in its `WHERE`. The claim takes only
//! unclaimed rows or rows whose claim went stale, and the other writes match
//! the batch's claim token. The clear's delete and the two updates that record
//! a failed attempt also match the claimed generation. Each binds the claim
//! token, generation and times the caller decided on and returns whether it
//! matched (the claim, how many rows it took); matching nothing is a normal
//! outcome, another claim or a newer mark having won. node-ctl keeps the
//! transaction around a batch's clear or failure and decides what each
//! outcome means.

#![forbid(unsafe_code)]

pub mod database;
pub mod dirty_facets;
pub mod facet_sync_overrides;
pub mod reconciliation_status;

#[cfg(test)]
mod tests;
