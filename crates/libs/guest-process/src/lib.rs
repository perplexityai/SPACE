//! Guest process, path, environment and lifecycle-timing boundary types.
//!
//! These newtypes describe what a sandbox guest runs and where: process IDs,
//! commands, signals, guest user names and idempotency keys ([`process`]),
//! guest paths and watch roots ([`path`]), environment keys, values and maps with their size
//! contract shared with the Go shim ([`env`]), and the positive durations
//! that time sandbox readiness, retries, uptime and snapshots ([`time`]).
//!
//! It is its own crate so that code can name these values without depending
//! on `pplx-space-common-core`. `pplx-space-common-core` re-exports each
//! module from `types::process`, `types::path`, `types::env` and
//! `types::time`.

pub mod env;
pub mod path;
pub mod process;
pub mod status;
pub mod time;

/// Maximum entries in one guest filesystem write batch across host and guest backends.
pub const MAX_WRITE_BATCH_ENTRIES: usize = 256;

pub use env::*;
pub use path::*;
pub use process::*;
pub use time::{GuestUptime, ReadyTimeout, RetryDelay, SnapshotInterval};
