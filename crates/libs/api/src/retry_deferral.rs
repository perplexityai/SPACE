//! Lets a queued-work caller take back the waits a failing step would
//! otherwise sleep through.
//!
//! A wake that keeps failing (a coalesced run that re-fails, a restore claim
//! fenced by a stale owner) paces its own retries inside the caller's
//! readiness budget. For an HTTP caller that wait is the request. For the
//! public-operation dispatcher it holds one of a few shared execution slots
//! while doing nothing, so the dispatcher runs executions inside
//! [`defer_retry_waits`]: a step that would sleep before retrying instead
//! returns its retryable error at once, and the dispatcher hands the slot
//! back and requeues the operation with backoff. Waits on work another
//! caller is actively doing (joining a live restore lease or a running
//! coalesced wake) are not retries and are unchanged.

use std::future::Future;

tokio::task_local! {
    static DEFER_RETRY_WAITS: ();
}

/// Runs `future` with retry waits deferred to the caller.
pub async fn defer_retry_waits<F: Future>(future: F) -> F::Output {
    DEFER_RETRY_WAITS.scope((), future).await
}

/// Whether the current task's caller asked to take retry waits back.
pub fn retry_waits_deferred() -> bool {
    DEFER_RETRY_WAITS.try_with(|()| ()).is_ok()
}

/// Re-enters the deferral of the task that captured `deferred`, for work
/// that task spawns or hands to a detached run.
pub async fn scope<F: Future>(deferred: bool, future: F) -> F::Output {
    if deferred {
        defer_retry_waits(future).await
    } else {
        future.await
    }
}

#[cfg(test)]
mod tests;
