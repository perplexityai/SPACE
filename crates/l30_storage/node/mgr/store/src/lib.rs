//! node-manager's node-local SQLite store.
//!
//! It owns `firewall_rules_assignment`. node-manager's network slot tables are
//! in the same database but have no owner yet, and node-manager still writes
//! them directly.

#![forbid(unsafe_code)]

pub mod firewall_assignment;
