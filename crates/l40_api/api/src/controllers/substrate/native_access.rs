//! Marks work done for a Native public data-plane request.
//!
//! Some worker-connection behavior applies only to these requests: see
//! `KubernetesSubstrate::access_kept_worker_client`. A task-local keeps the
//! choice out of every shared signature, so every other caller (ECW, lifecycle,
//! background work, and anything a Native request spawns onto another task)
//! keeps the default behavior without passing anything.

use std::future::Future;

tokio::task_local! {
    static NATIVE_DATA_PLANE: ();
}

/// Run `work` as part of a Native public data-plane request.
pub(crate) fn scope<F: Future>(work: F) -> impl Future<Output = F::Output> {
    NATIVE_DATA_PLANE.scope((), work)
}

/// Whether the current task is running a Native public data-plane request.
// Only the Kubernetes substrate reads it.
#[cfg_attr(not(feature = "kubernetes"), allow(dead_code))]
pub(crate) fn active() -> bool {
    NATIVE_DATA_PLANE.try_with(|_| ()).is_ok()
}

#[cfg(test)]
#[path = "native_access_tests.rs"]
mod tests;
