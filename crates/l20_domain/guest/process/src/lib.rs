//! Guest process, path, environment and lifecycle-timing boundary types.
//!
//! These newtypes describe what a sandbox guest runs and where: process IDs,
//! commands, signals, guest user names and idempotency keys ([`process`]),
//! guest paths and watch roots ([`path`]), environment keys, values and maps with their size
//! contract shared with the Go shim ([`env`]), the positive durations that
//! time sandbox readiness, retries, uptime and snapshots ([`time`]), and the
//! spaced guest socket paths ([`linux::paths`]).
//!
//! It is its own crate so that code can name these values without depending
//! on `pplx-space-common-core`: import them from here.

pub mod env;
pub mod linux;
pub mod path;
pub mod process;
pub use pplx_space_guest_status as status;
pub mod time;

/// Maximum entries in one guest filesystem write batch across host and guest backends.
pub const MAX_WRITE_BATCH_ENTRIES: usize = 256;
/// Largest gRPC message the sandbox manager and the guest filesystem
/// decode; the node API's worker server enforces the same 4 MiB through
/// tonic's default. A write batch travels as one message, so it must fit;
/// streamed writes are split into smaller messages.
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

pub use env::*;
pub use path::*;
pub use process::*;
pub use time::{GuestUptime, ReadyTimeout, RetryDelay, SnapshotInterval};
