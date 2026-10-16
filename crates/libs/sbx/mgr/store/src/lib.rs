//! sbx-mgr's node-local SQLite store: the `sandboxes` table and every other
//! sbx-mgr table, not the control-plane Postgres.
//!
//! This crate is the only code that opens, migrates or queries that
//! database. [`db`] holds its statements, its handle, transaction and error
//! types, and its migration entry point; the other modules are the domain
//! types its rows decode into. Callers hold only the store's own types: outside
//! the `test-support` feature, no public item returns, exposes or re-exports a
//! rusqlite connection, transaction, statement or row, so a caller cannot run
//! SQL of its own. The rusqlite types left in the public API are trait impls:
//! the `From<rusqlite::Error>` conversions into the store's errors, which take
//! a rusqlite error in, and `FromSql` for a timestamp column type, whose
//! `column_result` returns rusqlite's `FromSqlError` to a caller that already
//! holds a rusqlite `ValueRef`: an error type going out, not a database
//! capability.

#![forbid(unsafe_code)]
#![cfg_attr(not(test), deny(clippy::expect_used))]

pub mod auto_lifecycle;
pub mod auto_lifecycle_delete_receipts;
pub mod db;
pub mod endpoint_proof;
pub mod restore_operation;
pub mod state;
