use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use pplx_space_common_core::types::SandboxId;
use pplx_space_common_grpc::uds_per_sandbox_limit_exceeded_status;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::timeout;
use tonic::{Code, Status};

/// Per-sandbox permits for calls on the shared budget, streams included;
/// `SPACE_NODE_API_UDS_PER_SANDBOX` overrides it.
const UDS_PER_SANDBOX_PERMITS: usize = 64;
/// Per-sandbox permits, on top of the shared ones, for calls on the reserved
/// budget. Streams and long unary calls can fill the shared permits; these keep
/// process control available.
const UDS_PER_SANDBOX_RESERVED_PERMITS: usize = 16;
const UDS_PER_SANDBOX_ENV: &str = "SPACE_NODE_API_UDS_PER_SANDBOX";
const DEFAULT_PER_SANDBOX_PERMITS: usize = 64;
const ACQUIRE_TIMEOUT: Duration = Duration::from_millis(500);
const NODE_PERMITS: usize = 2048;

#[derive(Clone, Debug)]
pub struct UdsPerSandboxLimiter {
    inner: Arc<UdsPerSandboxLimiterInner>,
}

#[derive(Debug)]
struct UdsPerSandboxLimiterInner {
    node: Arc<LimitSlot>,
    sandboxes: SandboxSlots,
    /// Absent where no call takes the reserved budget (service-port ingress).
    reserved: Option<SandboxSlots>,
    acquire_timeout: Duration,
    metric_scope: LimiterMetricScope,
}

impl UdsPerSandboxLimiterInner {
    /// The per-sandbox slots `budget` admits from, for acquisition and for
    /// reclamation alike. A limiter without a reserved budget uses the shared one.
    fn sandbox_slots(&self, budget: SandboxBudget) -> &SandboxSlots {
        match budget {
            SandboxBudget::Shared => &self.sandboxes,
            SandboxBudget::Reserved => self.reserved.as_ref().unwrap_or(&self.sandboxes),
        }
    }
}

#[derive(Debug)]
struct SandboxSlots {
    scope: SlotScope,
    limits: SlotLimits,
    slots: DashMap<SandboxId, Arc<LimitSlot>>,
}

impl SandboxSlots {
    fn new(scope: SlotScope, limits: SlotLimits) -> Self {
        Self {
            scope,
            limits,
            slots: DashMap::new(),
        }
    }

    fn slot(&self, sandbox_id: SandboxId, metric_scope: LimiterMetricScope) -> Arc<LimitSlot> {
        self.slots
            .entry(sandbox_id)
            .or_insert_with(|| Arc::new(LimitSlot::new(self.scope, self.limits, metric_scope)))
            .clone()
    }
}

/// Which per-sandbox permits a call takes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxBudget {
    /// Streams and every unary call not on the reserved budget.
    Shared,
    /// Calls that stop or inspect a sandbox's processes, routed through
    /// `fwd_sbx_reserved!` in `worker.rs`.
    Reserved,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SlotLimits {
    permits: usize,
    waiters: usize,
}

impl UdsPerSandboxLimiter {
    pub fn control() -> Self {
        Self::control_with_shared_permits(uds_per_sandbox_permits())
    }

    fn control_with_shared_permits(shared_permits: usize) -> Self {
        Self::new(
            per_sandbox_limits(shared_permits),
            Some(per_sandbox_limits(UDS_PER_SANDBOX_RESERVED_PERMITS)),
            SlotLimits {
                permits: NODE_PERMITS,
                waiters: NODE_PERMITS,
            },
            ACQUIRE_TIMEOUT,
            LimiterMetricScope::Control,
        )
    }

    pub fn service_port_ingress() -> Self {
        let (per_sandbox_permits, _) = service_port_ingress_per_sandbox_bounds(NODE_PERMITS);
        Self::new(
            per_sandbox_limits(per_sandbox_permits),
            None,
            SlotLimits {
                permits: NODE_PERMITS,
                waiters: NODE_PERMITS,
            },
            ACQUIRE_TIMEOUT,
            LimiterMetricScope::ServicePortIngress,
        )
    }

    fn new(
        per_sandbox_limits: SlotLimits,
        per_sandbox_reserved_limits: Option<SlotLimits>,
        node_limits: SlotLimits,
        acquire_timeout: Duration,
        metric_scope: LimiterMetricScope,
    ) -> Self {
        Self {
            inner: Arc::new(UdsPerSandboxLimiterInner {
                node: Arc::new(LimitSlot::new(SlotScope::Node, node_limits, metric_scope)),
                sandboxes: SandboxSlots::new(SlotScope::Sandbox, per_sandbox_limits),
                reserved: per_sandbox_reserved_limits
                    .map(|limits| SandboxSlots::new(SlotScope::SandboxReserved, limits)),
                acquire_timeout,
                metric_scope,
            }),
        }
    }

    pub async fn acquire(
        &self,
        method: &'static str,
        sandbox_id: &str,
    ) -> Result<UdsPerSandboxPermit, Status> {
        self.acquire_in(SandboxBudget::Shared, method, sandbox_id)
            .await
    }

    pub async fn acquire_in(
        &self,
        budget: SandboxBudget,
        method: &'static str,
        sandbox_id: &str,
    ) -> Result<UdsPerSandboxPermit, Status> {
        let sandbox_id = parse_sandbox_id(method, sandbox_id)?;
        self.acquire_with_optional_node(method, sandbox_id, None, budget)
            .await
    }

    pub(crate) async fn acquire_node(&self, method: &'static str) -> Result<UdsNodePermit, Status> {
        self.inner
            .node
            .acquire(None, self.inner.acquire_timeout)
            .await
            .map(UdsNodePermit)
            .map_err(|error| {
                self.record_rejection(method, None, &error);
                error.status(method)
            })
    }

    pub(crate) async fn acquire_with_node(
        &self,
        method: &'static str,
        sandbox_id: &str,
        node: UdsNodePermit,
    ) -> Result<UdsPerSandboxPermit, Status> {
        let sandbox_id = parse_sandbox_id(method, sandbox_id)?;
        self.acquire_with_optional_node(method, sandbox_id, Some(node), SandboxBudget::Shared)
            .await
    }

    async fn acquire_with_optional_node(
        &self,
        method: &'static str,
        sandbox_id: SandboxId,
        node: Option<UdsNodePermit>,
        budget: SandboxBudget,
    ) -> Result<UdsPerSandboxPermit, Status> {
        // Match client-stream admission ordering: never hold a sandbox slot
        // while waiting for a node slot held by a pre-frame request.
        let node_permit = match node {
            Some(node) => node.0,
            None => self.acquire_node(method).await?.0,
        };
        let sandbox_slot = self
            .inner
            .sandbox_slots(budget)
            .slot(sandbox_id, self.inner.metric_scope);
        let sandbox_cleanup = SlotCleanup {
            inner: Arc::clone(&self.inner),
            sandbox_id,
            budget,
        };
        let sandbox_permit = match sandbox_slot
            .acquire(Some(sandbox_cleanup), self.inner.acquire_timeout)
            .await
        {
            Ok(permit) => permit,
            Err(error) => {
                self.record_rejection(method, Some(sandbox_id), &error);
                return Err(error.status(method));
            }
        };

        Ok(UdsPerSandboxPermit {
            _sandbox: sandbox_permit,
            _node: node_permit,
            acquired_at: Instant::now(),
            metric_scope: self.inner.metric_scope,
        })
    }

    fn record_rejection(
        &self,
        method: &'static str,
        sandbox_id: Option<SandboxId>,
        error: &SlotAcquireError,
    ) {
        self.inner.metric_scope.record_rejected(error.scope);
        tracing::warn!(
            method,
            sandbox_id = sandbox_id.map(|id| id.to_string()).as_deref().unwrap_or(""),
            limiter = %self.inner.metric_scope,
            scope = error.scope.as_str(),
            reason = error.reason.as_str(),
            in_flight = error.in_flight,
            waiters = error.waiters,
            permits = error.limits.permits,
            waiter_limit = error.limits.waiters,
            "node-api UDS per-sandbox limiter exceeded"
        );
    }

    #[cfg(test)]
    fn for_tests(per_sandbox_limits: SlotLimits, node_limits: SlotLimits) -> Self {
        Self::new(
            per_sandbox_limits,
            None,
            node_limits,
            Duration::ZERO,
            LimiterMetricScope::Control,
        )
    }

    #[cfg(test)]
    pub(crate) fn service_port_ingress_for_tests(
        per_sandbox_permits: usize,
        node_permits: usize,
    ) -> Self {
        Self::new(
            per_sandbox_limits(per_sandbox_permits),
            None,
            per_sandbox_limits(node_permits),
            Duration::ZERO,
            LimiterMetricScope::ServicePortIngress,
        )
    }

    #[cfg(test)]
    fn for_tests_with_timeout(
        per_sandbox_limits: SlotLimits,
        node_limits: SlotLimits,
        acquire_timeout: Duration,
    ) -> Self {
        Self::new(
            per_sandbox_limits,
            None,
            node_limits,
            acquire_timeout,
            LimiterMetricScope::Control,
        )
    }

    #[cfg(test)]
    pub(crate) fn for_tests_with_reserved(
        per_sandbox_permits: usize,
        reserved_permits: usize,
        node_permits: usize,
    ) -> Self {
        Self::new(
            per_sandbox_limits(per_sandbox_permits),
            Some(per_sandbox_limits(reserved_permits)),
            per_sandbox_limits(node_permits),
            Duration::ZERO,
            LimiterMetricScope::Control,
        )
    }

    #[cfg(test)]
    fn sandbox_snapshot(&self, sandbox_id: &str) -> Option<usize> {
        self.inner
            .sandboxes
            .slots
            .get(&sandbox_id.parse::<SandboxId>().ok()?)
            .map(|slot| slot.snapshot())
    }

    #[cfg(test)]
    fn sandbox_waiters_snapshot(&self, sandbox_id: &str) -> Option<usize> {
        self.inner
            .sandboxes
            .slots
            .get(&sandbox_id.parse::<SandboxId>().ok()?)
            .map(|slot| slot.waiters_snapshot())
    }

    #[cfg(test)]
    fn reserved_snapshot(&self, sandbox_id: &str) -> Option<usize> {
        self.inner
            .reserved
            .as_ref()?
            .slots
            .get(&sandbox_id.parse::<SandboxId>().ok()?)
            .map(|slot| slot.snapshot())
    }

    #[cfg(test)]
    fn node_snapshot(&self) -> usize {
        self.inner.node.snapshot()
    }

    #[cfg(test)]
    fn node_waiters_snapshot(&self) -> usize {
        self.inner.node.waiters_snapshot()
    }
}

// Network RPCs require full IDs. Human-facing selectors belong at the
// resolving API boundary, before requests reach node admission.
fn parse_sandbox_id(method: &str, sandbox_id: &str) -> Result<SandboxId, Status> {
    sandbox_id
        .parse()
        .map_err(|_| Status::invalid_argument(format!("{method}.sandbox_id must be a full UUID")))
}

pub(crate) struct UdsNodePermit(SlotPermit);

pub struct UdsPerSandboxPermit {
    _sandbox: SlotPermit,
    _node: SlotPermit,
    acquired_at: Instant,
    metric_scope: LimiterMetricScope,
}

impl Drop for UdsPerSandboxPermit {
    fn drop(&mut self) {
        self.metric_scope
            .record_handler_ms(self.acquired_at.elapsed().as_secs_f64() * 1000.0);
    }
}

#[derive(Debug)]
struct LimitSlot {
    scope: SlotScope,
    limits: SlotLimits,
    semaphore: Arc<Semaphore>,
    waiter_semaphore: Arc<Semaphore>,
    in_flight: Arc<AtomicUsize>,
    waiters: Arc<AtomicUsize>,
    metric_scope: LimiterMetricScope,
}

impl LimitSlot {
    fn new(scope: SlotScope, limits: SlotLimits, metric_scope: LimiterMetricScope) -> Self {
        Self {
            scope,
            limits,
            semaphore: Arc::new(Semaphore::new(limits.permits)),
            waiter_semaphore: Arc::new(Semaphore::new(limits.waiters)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            waiters: Arc::new(AtomicUsize::new(0)),
            metric_scope,
        }
    }

    async fn acquire(
        self: &Arc<Self>,
        cleanup: Option<SlotCleanup>,
        acquire_timeout: Duration,
    ) -> Result<SlotPermit, SlotAcquireError> {
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(TryAcquireError::NoPermits) if !acquire_timeout.is_zero() => {
                let _waiter = match self.waiter_semaphore.clone().try_acquire_owned() {
                    Ok(permit) => SlotWaiter {
                        slot: Arc::clone(self),
                        permit: Some(permit),
                    },
                    Err(TryAcquireError::NoPermits) => {
                        return Err(self.acquire_error(RejectReason::WaitQueueFull));
                    }
                    Err(TryAcquireError::Closed) => {
                        return Err(self.acquire_error(RejectReason::SemaphoreClosed));
                    }
                };
                self.waiters.fetch_add(1, Ordering::SeqCst);
                match timeout(acquire_timeout, self.semaphore.clone().acquire_owned()).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_closed)) => {
                        return Err(self.acquire_error(RejectReason::SemaphoreClosed));
                    }
                    Err(_elapsed) => return Err(self.acquire_error(RejectReason::Saturated)),
                }
            }
            Err(TryAcquireError::NoPermits) => {
                return Err(self.acquire_error(RejectReason::Saturated));
            }
            Err(TryAcquireError::Closed) => {
                return Err(self.acquire_error(RejectReason::SemaphoreClosed));
            }
        };

        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.report_node_in_flight();
        Ok(SlotPermit {
            slot: Arc::clone(self),
            permit: Some(permit),
            cleanup,
        })
    }

    fn acquire_error(&self, reason: RejectReason) -> SlotAcquireError {
        let in_flight = self.in_flight.load(Ordering::SeqCst);
        self.report_node_in_flight();
        SlotAcquireError {
            scope: self.scope,
            reason,
            limits: self.limits,
            in_flight,
            waiters: self.waiters.load(Ordering::SeqCst),
        }
    }

    fn report_node_in_flight(&self) {
        if self.scope == SlotScope::Node {
            self.metric_scope.record_in_flight(self.snapshot());
        }
    }

    fn snapshot(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    fn waiters_snapshot(&self) -> usize {
        self.waiters.load(Ordering::SeqCst)
    }
}

struct SlotWaiter {
    slot: Arc<LimitSlot>,
    permit: Option<OwnedSemaphorePermit>,
}

impl Drop for SlotWaiter {
    fn drop(&mut self) {
        let permit = self.permit.take();
        drop(permit);
        self.slot.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

struct SlotPermit {
    slot: Arc<LimitSlot>,
    permit: Option<OwnedSemaphorePermit>,
    cleanup: Option<SlotCleanup>,
}

impl Drop for SlotPermit {
    fn drop(&mut self) {
        let permit = self.permit.take();
        drop(permit);
        let previous = self.slot.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.slot.report_node_in_flight();
        if previous == 1
            && let Some(cleanup) = &self.cleanup
        {
            cleanup.reclaim_if_idle(&self.slot);
        }
    }
}

struct SlotCleanup {
    inner: Arc<UdsPerSandboxLimiterInner>,
    sandbox_id: SandboxId,
    budget: SandboxBudget,
}

impl SlotCleanup {
    fn reclaim_if_idle(&self, slot: &Arc<LimitSlot>) {
        let slots = self.inner.sandbox_slots(self.budget);
        slots.slots.remove_if(&self.sandbox_id, |_, current| {
            Arc::ptr_eq(current, slot)
                && current.in_flight.load(Ordering::SeqCst) == 0
                && current.waiters.load(Ordering::SeqCst) == 0
                && Arc::strong_count(current) == 2
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotScope {
    Node,
    Sandbox,
    SandboxReserved,
}

impl SlotScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Node => "node",
            Self::Sandbox => "sandbox",
            Self::SandboxReserved => "sandbox_reserved",
        }
    }
}

impl fmt::Display for SlotScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RejectReason {
    Saturated,
    SemaphoreClosed,
    WaitQueueFull,
}

impl RejectReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Saturated => "saturated",
            Self::SemaphoreClosed => "semaphore_closed",
            Self::WaitQueueFull => "wait_queue_full",
        }
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LimiterMetricScope {
    Control,
    ServicePortIngress,
}

impl LimiterMetricScope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Control => "control",
            Self::ServicePortIngress => "service_port_ingress",
        }
    }

    fn record_in_flight(self, value: usize) {
        match self {
            Self::Control => pplx_space_metrics::node_api::uds_per_sandbox_in_flight(value),
            Self::ServicePortIngress => {
                pplx_space_metrics::node_api::service_port_ingress_in_flight(value);
            }
        }
    }

    fn record_rejected(self, scope: SlotScope) {
        match self {
            Self::Control => pplx_space_metrics::node_api::uds_per_sandbox_rejected(scope.as_str()),
            Self::ServicePortIngress => {
                pplx_space_metrics::node_api::service_port_ingress_rejected()
            }
        }
    }

    fn record_handler_ms(self, value_ms: f64) {
        match self {
            Self::Control => pplx_space_metrics::node_api::uds_per_sandbox_handler_ms(value_ms),
            Self::ServicePortIngress => {
                pplx_space_metrics::node_api::service_port_ingress_handler_ms(value_ms);
            }
        }
    }
}

impl fmt::Display for LimiterMetricScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug)]
struct SlotAcquireError {
    scope: SlotScope,
    reason: RejectReason,
    limits: SlotLimits,
    in_flight: usize,
    waiters: usize,
}

impl SlotAcquireError {
    fn status(&self, method: &'static str) -> Status {
        let message = format!(
            "{method} UDS per-sandbox limiter exceeded: scope={}, reason={}, in_flight={}, permits={}, waiters={}, waiter_limit={}",
            self.scope,
            self.reason,
            self.in_flight,
            self.limits.permits,
            self.waiters,
            self.limits.waiters
        );
        match self.reason {
            RejectReason::SemaphoreClosed => Status::new(Code::Internal, message),
            RejectReason::Saturated | RejectReason::WaitQueueFull => {
                uds_per_sandbox_limit_exceeded_status(message)
            }
        }
    }
}

fn service_port_ingress_per_sandbox_bounds(node_permits: usize) -> (usize, usize) {
    let max =
        node_permits.min(pplx_space_net_gateway_client::MAX_ACTIVE_SERVICE_PORT_INGRESS_STREAMS);
    (DEFAULT_PER_SANDBOX_PERMITS.min(max), max)
}

/// The per-sandbox UDS permit count: `SPACE_NODE_API_UDS_PER_SANDBOX` when it is
/// an integer within `1..=NODE_PERMITS`, otherwise the built-in default.
fn uds_per_sandbox_permits() -> usize {
    match std::env::var(UDS_PER_SANDBOX_ENV) {
        Ok(raw) if !raw.is_empty() => match raw.parse::<usize>() {
            Ok(value) if (1..=NODE_PERMITS).contains(&value) => value,
            _ => {
                tracing::warn!(
                    env_var = UDS_PER_SANDBOX_ENV,
                    value = %raw,
                    default = UDS_PER_SANDBOX_PERMITS,
                    "invalid per-sandbox UDS permit override; using the default"
                );
                UDS_PER_SANDBOX_PERMITS
            }
        },
        _ => UDS_PER_SANDBOX_PERMITS,
    }
}

fn per_sandbox_limits(permits: usize) -> SlotLimits {
    SlotLimits {
        permits,
        waiters: permits,
    }
}

#[cfg(test)]
mod tests;
