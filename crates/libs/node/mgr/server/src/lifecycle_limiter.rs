//! Per-method bounded concurrency for node-manager lifecycle RPCs.
//!
//! Background: the on-node edge rate limiter caps total in-flight
//! requests at 512 global / 128 per peer with no awareness of which method is
//! being served. Lifecycle RPCs (Allocate, PrepareSandbox, Launch,
//! LaunchSandbox, LoadSnapshot, CleanupSandbox, Release) do far more host work
//! per call (btrfs subvolumes, recursive chown, mount/umount, jailer launch)
//! than the light read-only RPCs they share quota with. Without explicit per-method
//! caps, a flood of expensive lifecycle calls can monopolize the runtime and
//! the blocking pool while leaving the global counter well below 512.
//!
//! This module makes the per-method concurrency *explicit*: each lifecycle
//! method has its own permit count plus a soft cap on additional waiters.
//! Callers acquire a permit before doing real work; if the queue is already
//! full, they get `ResourceExhausted` immediately rather than piling onto a
//! saturated runtime.
//!
//! Metrics:
//!   * `node_mgr.lifecycle.in_flight{method=…}` — gauge, currently holding a permit.
//!   * `node_mgr.lifecycle.queue_depth{method=…}` — gauge, waiting for a permit.
//!   * `node_mgr.lifecycle.acquire_wait_ms{method=…}` — distribution.
//!   * `node_mgr.lifecycle.handler_ms{method=…}` — distribution (acquire→release).
//!   * `node_mgr.lifecycle.rejected{method=…,reason=…}` — counter.
//!
//! Defaults are tunable via environment variables of the form
//! `SPACE_NODEMGR_LIMIT_<METHOD>=<permits>:<max_queue>`, e.g.
//! `SPACE_NODEMGR_LIMIT_PREPARE_SANDBOX=8:24`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use tonic::Status;

/// Lifecycle method identifier. Kept as a small enum so that metric tag values
/// and env-var names cannot drift from the limiter's internal table.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LifecycleMethod {
    Allocate,
    PrepareSandbox,
    Launch,
    LaunchSandbox,
    LoadSnapshot,
    CleanupSandbox,
    Release,
    Filesystem,
}

impl LifecycleMethod {
    pub const ALL: &'static [LifecycleMethod] = &[
        LifecycleMethod::Allocate,
        LifecycleMethod::PrepareSandbox,
        LifecycleMethod::Launch,
        LifecycleMethod::LaunchSandbox,
        LifecycleMethod::LoadSnapshot,
        LifecycleMethod::CleanupSandbox,
        LifecycleMethod::Release,
        LifecycleMethod::Filesystem,
    ];

    pub const fn as_metric_tag(self) -> &'static str {
        match self {
            LifecycleMethod::Allocate => "Allocate",
            LifecycleMethod::PrepareSandbox => "PrepareSandbox",
            LifecycleMethod::Launch => "Launch",
            LifecycleMethod::LaunchSandbox => "LaunchSandbox",
            LifecycleMethod::LoadSnapshot => "LoadSnapshot",
            LifecycleMethod::CleanupSandbox => "CleanupSandbox",
            LifecycleMethod::Release => "Release",
            LifecycleMethod::Filesystem => "Filesystem",
        }
    }

    const fn env_suffix(self) -> &'static str {
        match self {
            LifecycleMethod::Allocate => "ALLOCATE",
            LifecycleMethod::PrepareSandbox => "PREPARE_SANDBOX",
            LifecycleMethod::Launch => "LAUNCH",
            LifecycleMethod::LaunchSandbox => "LAUNCH_SANDBOX",
            LifecycleMethod::LoadSnapshot => "LOAD_SNAPSHOT",
            LifecycleMethod::CleanupSandbox => "CLEANUP_SANDBOX",
            LifecycleMethod::Release => "RELEASE",
            LifecycleMethod::Filesystem => "FILESYSTEM",
        }
    }

    /// (permits, max_queue) defaults. Heavy host-side methods get tight caps;
    /// in-memory pool ops get generous ones. Tuned conservatively for a
    /// 4-vCPU worker; production deployments can override via env.
    const fn default_limits(self) -> (usize, usize) {
        match self {
            LifecycleMethod::Allocate => (64, 192),
            LifecycleMethod::PrepareSandbox => (8, 24),
            LifecycleMethod::Launch => (8, 24),
            LifecycleMethod::LaunchSandbox => (8, 24),
            // Snapshot loading is resource-intensive during restored-VM
            // bring-up; cap active work while allowing larger restore bursts
            // to wait rather than failing admission.
            LifecycleMethod::LoadSnapshot => (8, 56),
            LifecycleMethod::CleanupSandbox => (8, 24),
            LifecycleMethod::Release => (32, 96),
            LifecycleMethod::Filesystem => (4, 16),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LifecycleLimits {
    pub permits: usize,
    pub max_queue: usize,
}

impl LifecycleLimits {
    /// Total arrivals beyond which we reject (in-flight + waiting).
    fn reject_threshold(self) -> usize {
        self.permits.saturating_add(self.max_queue)
    }
}

#[derive(Debug)]
struct MethodSlot {
    method: LifecycleMethod,
    limits: LifecycleLimits,
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Arrived but not yet completed (in-flight + waiting).
    enqueued: Arc<AtomicUsize>,
    /// Currently holding a permit.
    in_flight: Arc<AtomicUsize>,
}

#[derive(Debug)]
pub struct LifecycleLimiter {
    task_tracker: tokio_util::task::TaskTracker,
    slots: [MethodSlot; 8],
}

impl Default for LifecycleLimiter {
    fn default() -> Self {
        Self::from_env()
    }
}

impl LifecycleLimiter {
    pub fn from_env() -> Self {
        Self::with_resolver(read_env_limits)
    }

    fn with_resolver(resolver: impl Fn(LifecycleMethod) -> LifecycleLimits) -> Self {
        let mut slots = Vec::with_capacity(LifecycleMethod::ALL.len());
        for &method in LifecycleMethod::ALL {
            let limits = resolver(method);
            tracing::info!(
                method = method.as_metric_tag(),
                permits = limits.permits,
                max_queue = limits.max_queue,
                "resolved startup-only lifecycle admission limits"
            );
            slots.push(MethodSlot {
                method,
                limits,
                semaphore: Arc::new(tokio::sync::Semaphore::new(limits.permits)),
                enqueued: Arc::new(AtomicUsize::new(0)),
                in_flight: Arc::new(AtomicUsize::new(0)),
            });
        }
        let slots: [MethodSlot; 8] = slots
            .try_into()
            .expect("LifecycleMethod::ALL length must match");
        Self {
            slots,
            task_tracker: tokio_util::task::TaskTracker::new(),
        }
    }

    pub(crate) fn with_task_tracker(mut self, tracker: tokio_util::task::TaskTracker) -> Self {
        self.task_tracker = tracker;
        self
    }

    pub(crate) fn close(&self) {
        for slot in &self.slots {
            slot.semaphore.close();
        }
    }

    fn slot(&self, method: LifecycleMethod) -> &MethodSlot {
        self.slots
            .iter()
            .find(|s| s.method == method)
            .expect("every LifecycleMethod has a slot")
    }

    /// Reserve a permit for `method`. Returns `Status::resource_exhausted` if
    /// arrivals already exceed permits + max_queue. Latency from arrival to
    /// permit acquisition is reported as `acquire_wait_ms`. The returned
    /// guard, when dropped, releases the permit and reports `handler_ms`.
    pub async fn acquire(&self, method: LifecycleMethod) -> Result<LifecyclePermit, Status> {
        let slot = self.slot(method);
        let prev = slot.enqueued.fetch_add(1, Ordering::SeqCst);
        // RAII guard so that any early return (reject, semaphore-closed) and —
        // critically — async cancellation while parked at `acquire_owned().await`
        // decrements `enqueued`. Tonic drops handler futures on client
        // disconnect/timeout, so without this the counter would leak by one per
        // cancelled call and eventually pin the method permanently above its
        // reject threshold.
        let mut enqueue_guard = EnqueueGuard {
            counter: slot.enqueued.clone(),
            armed: true,
        };
        if prev >= slot.limits.reject_threshold() {
            pplx_space_metrics::node_manager::lifecycle_rejected(
                method.as_metric_tag(),
                "queue_full",
            );
            drop(enqueue_guard);
            self.report_gauges(slot);
            return Err(Status::resource_exhausted(format!(
                "{} queue full ({} arrivals; permits={}, max_queue={})",
                method.as_metric_tag(),
                prev + 1,
                slot.limits.permits,
                slot.limits.max_queue,
            )));
        }
        self.report_gauges(slot);
        let acquire_started = Instant::now();
        let permit = slot
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|error| {
                pplx_space_metrics::node_manager::lifecycle_rejected(
                    method.as_metric_tag(),
                    "closed",
                );
                Status::unavailable(format!("lifecycle limiter closed: {error}"))
            })?;
        let in_flight = slot.in_flight.clone();
        in_flight.fetch_add(1, Ordering::SeqCst);
        let mut in_flight_guard = InFlightGuard {
            counter: in_flight.clone(),
            armed: true,
        };
        self.report_gauges(slot);
        pplx_space_metrics::node_manager::lifecycle_acquired(
            method.as_metric_tag(),
            acquire_started,
        );
        let lifecycle_permit = LifecyclePermit {
            _inner: Arc::new(LifecyclePermitInner {
                _task: self.task_tracker.token(),
                method,
                held_permit: Some(permit),
                in_flight,
                enqueued: slot.enqueued.clone(),
                acquired_at: Instant::now(),
            }),
        };
        // Permit owns the enqueued/in-flight slots from here; its Drop will decrement.
        enqueue_guard.armed = false;
        in_flight_guard.armed = false;
        Ok(lifecycle_permit)
    }

    fn report_gauges(&self, slot: &MethodSlot) {
        pplx_space_metrics::node_manager::lifecycle_depth(
            slot.method.as_metric_tag(),
            slot.in_flight.load(Ordering::SeqCst),
            slot.enqueued.load(Ordering::SeqCst),
        );
    }

    /// Snapshot for tests / debugging.
    #[cfg(test)]
    pub(crate) fn snapshot(&self, method: LifecycleMethod) -> (usize, usize) {
        let slot = self.slot(method);
        (
            slot.in_flight.load(Ordering::SeqCst),
            slot.enqueued
                .load(Ordering::SeqCst)
                .saturating_sub(slot.in_flight.load(Ordering::SeqCst)),
        )
    }
}

/// Disarmable RAII guard that decrements an `enqueued` counter on drop.
/// Used inside `LifecycleLimiter::acquire` to make the arrival increment
/// cancel-safe: if the future is dropped at `acquire_owned().await`, the
/// guard's `Drop` runs and the counter is restored. On the success path the
/// guard is disarmed before returning so the permit owns the slot.
struct EnqueueGuard {
    counter: Arc<AtomicUsize>,
    armed: bool,
}

impl Drop for EnqueueGuard {
    fn drop(&mut self) {
        if self.armed {
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// Disarmable RAII guard that restores `in_flight` if acquire panics after
/// incrementing it but before the returned permit owns that accounting.
struct InFlightGuard {
    counter: Arc<AtomicUsize>,
    armed: bool,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if self.armed {
            self.counter.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

/// RAII guard returned from `LifecycleLimiter::acquire`. Holding any clone
/// keeps the permit reserved; dropping the last clone releases the permit and
/// reports the handler latency histogram. Lifecycle handlers clone this into
/// non-abortable blocking tasks so cancellation cannot release capacity while
/// host work is still running.
#[derive(Clone)]
pub struct LifecyclePermit {
    _inner: Arc<LifecyclePermitInner>,
}

struct LifecyclePermitInner {
    _task: tokio_util::task::task_tracker::TaskTrackerToken,
    method: LifecycleMethod,
    held_permit: Option<tokio::sync::OwnedSemaphorePermit>,
    in_flight: Arc<AtomicUsize>,
    enqueued: Arc<AtomicUsize>,
    acquired_at: Instant,
}

impl Drop for LifecyclePermitInner {
    fn drop(&mut self) {
        let _ = self.held_permit.take(); // release semaphore
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.enqueued.fetch_sub(1, Ordering::SeqCst);
        pplx_space_metrics::node_manager::lifecycle_released(
            self.method.as_metric_tag(),
            self.acquired_at,
        );
        pplx_space_metrics::node_manager::lifecycle_depth(
            self.method.as_metric_tag(),
            self.in_flight.load(Ordering::SeqCst),
            self.enqueued.load(Ordering::SeqCst),
        );
    }
}

fn read_env_limits(method: LifecycleMethod) -> LifecycleLimits {
    let (default_permits, default_queue) = method.default_limits();
    let var = format!("SPACE_NODEMGR_LIMIT_{}", method.env_suffix());
    let raw = match std::env::var(&var) {
        Ok(v) if !v.is_empty() => v,
        _ => {
            return LifecycleLimits {
                permits: default_permits,
                max_queue: default_queue,
            };
        }
    };
    match parse_limits(&raw) {
        Some(limits) => limits,
        None => {
            tracing::warn!(
                env_var = %var,
                value = %raw,
                "invalid lifecycle limit override; expected '<permits>:<max_queue>'; using defaults"
            );
            LifecycleLimits {
                permits: default_permits,
                max_queue: default_queue,
            }
        }
    }
}

fn parse_limits(raw: &str) -> Option<LifecycleLimits> {
    let (permits_s, queue_s) = raw.split_once(':')?;
    let permits: usize = permits_s.trim().parse().ok()?;
    let max_queue: usize = queue_s.trim().parse().ok()?;
    // Reject `permits == 0` (semaphore would never admit anyone) and any value
    // above tokio's hard semaphore cap (would panic in `Semaphore::new`). A
    // fat-fingered SPACE_NODEMGR_LIMIT_* with an absurd permit count should
    // fall back to defaults like every other invalid override, not crash
    // node-manager at startup.
    if permits == 0 || permits > tokio::sync::Semaphore::MAX_PERMITS {
        return None;
    }
    Some(LifecycleLimits { permits, max_queue })
}

#[cfg(test)]
pub(crate) mod tests;
