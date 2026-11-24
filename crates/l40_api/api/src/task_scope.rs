//! Keep background tasks visible to callers that need to confirm shutdown.

use std::future::Future;

use futures_util::future::Either;
use tokio::task::JoinHandle;
use tokio_util::task::TaskTracker;

tokio::task_local! {
    static TRACKER: TaskTracker;
}

/// Scope a task so its background work is counted by the caller's tracker.
pub async fn scope<T>(tracker: TaskTracker, future: impl Future<Output = T>) -> T {
    TRACKER.scope(tracker, future).await
}

/// Count a child with the current tracker, including any background tasks the
/// child starts when it is dropped. Outside a scope, run it unchanged.
pub fn track<T>(future: impl Future<Output = T>) -> impl Future<Output = T> {
    match TRACKER.try_with(Clone::clone) {
        Ok(tracker) => Either::Left(tracker.track_future(scope(tracker.clone(), future))),
        Err(_) => Either::Right(future),
    }
}

/// Start background work. API requests need no scope; a reconciler's scope
/// must observe the work even if its parent drops the returned handle.
pub(crate) fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(track(future))
}

/// Preserve a subsystem's lifetime tracker and also count its task in the
/// enclosing scope, if any.
#[cfg(feature = "kubernetes")]
pub(crate) fn spawn_in<F>(owner: &TaskTracker, future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    owner.spawn(track(future))
}
