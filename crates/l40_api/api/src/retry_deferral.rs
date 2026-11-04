//! Lets the public-operation dispatcher take back retry waits that would
//! otherwise hold one of its shared execution slots.
//!
//! Dispatcher executions run inside [`scope`] with a [`RetryDeferral`]. A
//! step that would keep sleeping before retrying (the fenced restore-claim
//! lap in the Kubernetes wake path) calls [`defer_retry_wait`]
//! instead, returns its retryable error, and the dispatcher requeues the
//! operation without spending an attempt. HTTP callers never set a scope and
//! keep their in-budget waits. It is a task-local, like
//! [`crate::wake_trace`], because the request and its record are per
//! execution, several controller calls above the step that reads them;
//! detached work re-enters it with [`current`] and [`scope`].

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// One execution's request to take back retry waits, and whether a step did.
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
