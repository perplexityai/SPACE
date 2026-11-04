//! Request diagnostics shared with detached wake work. Scope is explicit at
//! spawn boundaries; the process-wide restore poller must never inherit it.
use std::future::Future;
#[cfg(feature = "kubernetes")]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[cfg(feature = "kubernetes")]
use tokio::time::Instant;

#[derive(Clone)]
pub(crate) struct WakeTrace {
    span: tracing::Span,
    #[cfg(feature = "kubernetes")]
    waiter_ms: Arc<AtomicU64>,
}

tokio::task_local! {
    static CURRENT: Option<WakeTrace>;
}

impl WakeTrace {
    pub(crate) fn new() -> Self {
        let span = tracing::Span::current();
        span.record("wake_required", false);
        span.record("wake_outcome", "not_attempted");
        span.record("restore_kind", "none");
        span.record("restore_waiter_wait_ms", 0_u64);
        Self {
            span,
            #[cfg(feature = "kubernetes")]
            waiter_ms: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn wake_required(&self) {
        self.span.record("wake_required", true);
        self.span.record("wake_outcome", "pending");
        // The runtime, not the API, chooses the restore method. Do not leave
        // a misleading "none" on a failed or cancelled wake.
        self.span.record("restore_kind", "unknown");
        self.span.record("restore_kind_basis", "unknown");
    }

    pub(crate) fn wake_completed(&self) {
        self.span.record("wake_outcome", "completed");
    }

    pub(crate) fn completed(&self, target: Option<&str>, woke: bool) {
        if !woke {
            self.span.record("wake_outcome", "not_required");
        }
        if let Some(target) = target {
            self.span.record("target_node", target);
        }
    }

    #[cfg(feature = "kubernetes")]
    pub(crate) fn restore_placement(&self, source: Option<&str>, target: Option<&str>) {
        if let Some(target) = target {
            self.span.record("target_node", target);
        }
        let kind = match (source, target) {
            (Some(source), Some(target)) if source == target => "in_place",
            (Some(_), Some(_)) => "cross_node",
            _ => "unknown",
        };
        self.span.record("restore_kind", kind);
        self.span.record("restore_kind_basis", "placement");
    }

    #[cfg(feature = "kubernetes")]
    pub(crate) fn no_restore(&self) {
        self.span.record("restore_kind", "none");
        self.span.record("restore_kind_basis", "plan");
    }

    pub(crate) fn failed(&self) {
        self.span.record("wake_outcome", "failed");
    }
}

pub(crate) fn current() -> Option<WakeTrace> {
    CURRENT.try_with(Clone::clone).ok().flatten()
}

pub(crate) fn scope<T>(
    trace: Option<WakeTrace>,
    future: impl Future<Output = T>,
) -> impl Future<Output = T> {
    CURRENT.scope(trace, future)
}

#[cfg(feature = "kubernetes")]
pub(crate) struct RestoreWait {
    trace: Option<WakeTrace>,
    started: Instant,
}

#[cfg(feature = "kubernetes")]
impl RestoreWait {
    pub(crate) fn start() -> Self {
        Self {
            trace: current(),
            started: Instant::now(),
        }
    }
}

#[cfg(feature = "kubernetes")]
impl Drop for RestoreWait {
    fn drop(&mut self) {
        if let Some(trace) = &self.trace {
            let elapsed = self.started.elapsed().as_millis() as u64;
            let total = trace.waiter_ms.fetch_add(elapsed, Ordering::Relaxed) + elapsed;
            trace.span.record("restore_waiter_wait_ms", total);
        }
    }
}

#[cfg(all(test, feature = "kubernetes"))]
mod tests;
