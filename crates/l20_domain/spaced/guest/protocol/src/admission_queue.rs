//! The bounded wait spaced-host and the guest give a request that finds every
//! slot of its budget busy, so a short burst queues instead of failing.
//!
//! Queued requests are not decoded, so the slot budgets stay the
//! decode-memory bound. The waiter count matches node-api's default
//! per-sandbox concurrency limit; the wait drains a full queue at the slot
//! hold times of a CPU-starved guest while staying well inside the host's
//! stream-open budget. A file operation can wait at both hops, so at most
//! twice [`MAX_WAIT`] before its work starts.

use std::time::Duration;

pub const WAITERS: usize = 64;
pub const MAX_WAIT: Duration = Duration::from_secs(2);
