//! Lets a queued-work caller take back the waits a failing step would
//! otherwise sleep through.
//!
//! A wake whose restore claim stays fenced by a stale owner paces its own
//! retries inside the caller's readiness budget. For an HTTP caller that
//! wait is the request. For the public-operation dispatcher it holds one of
//! a few shared execution slots while doing nothing, so the dispatcher runs
//! executions inside [`scope`] with a [`RetryDeferral`]: after a short grace,
//! a step that would keep sleeping before retrying instead returns its
//! retryable error, records the deferral, and the dispatcher hands the slot
//! back and requeues the operation. Waits on work another caller is actively
//! doing (joining a live restore lease or a running coalesced wake) are not
//! retries and are unchanged.
//!
//! The deferral is carried in a task-local, like [`crate::wake_trace`], so
//! the HTTP paths that share the substrate code take no extra argument.
//! Work handed to a detached task re-enters it with [`current`] and
//! [`scope`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// One execution's request to take back retry waits, and whether any step
/// did.
#[derive(Clone, Debug, Default)]
pub struct RetryDeferral {
    deferred: Arc<AtomicBool>,
}

impl RetryDeferral {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether a step under this deferral returned instead of waiting.
    pub fn deferred(&self) -> bool {
        self.deferred.load(Ordering::Acquire)
    }
}

tokio::task_local! {
    static CURRENT: Option<RetryDeferral>;
}

/// The deferral the current task runs under, if any.
pub fn current() -> Option<RetryDeferral> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

/// Runs `future` under `deferral`; `None` runs it with retry waits kept.
pub fn scope<T>(
    deferral: Option<RetryDeferral>,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    CURRENT.scope(deferral, future)
}

/// Whether the current task's caller asked to take retry waits back.
pub fn retry_waits_deferred() -> bool {
    current().is_some()
}

/// Called by a step that returns instead of waiting: records the deferral
/// for the caller and counts it under `site`. Returns `false`, recording
/// nothing, when the caller keeps its waits.
pub fn defer_retry_wait(site: &str) -> bool {
    let Some(deferral) = current() else {
        return false;
    };
    deferral.deferred.store(true, Ordering::Release);
    pplx_space_metrics::operations::retry_wait_deferred(site);
    true
}

#[cfg(test)]
mod tests;
