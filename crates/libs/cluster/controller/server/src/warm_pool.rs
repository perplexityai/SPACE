use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{
    FutureExt,
    stream::{self, StreamExt},
};
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::{
    Api, Client, Error as KubeError,
    api::{DeleteParams, ListParams, Patch, PatchParams, PostParams},
    runtime::reflector::Store,
};
use pplx_space_api::controllers::substrate::{
    ReflectorReady, Substrate, SubstrateAdapter, connect_worker,
};
use pplx_space_api::node_group_placement::{
    NodeGroupPlacementResolveError, NodeGroupPlacementResolver, PlacementSubject,
    ResolvePath as NodeGroupResolvePath, ResolvedPlacement,
};
use pplx_space_cluster_pool::{
    NewPoolSlot, PoolConfig, PoolSlot, PoolSlotCounts, PoolSlotState, WarmPoolStore,
};
use pplx_space_common_core::kubernetes::NodeGroupPlacement;
use pplx_space_common_core::{
    kubernetes::{
        DEFAULT_SANDBOX_PROXY_PORT, SANDBOX_POOL_CONFIG_ID_KEY, SANDBOX_POOL_ROLE_KEY,
        SANDBOX_POOL_ROLE_WARM, SandboxPodConfigValues, TEMPLATE_PRESENT_LABEL_VALUE,
        template_build_presence_label_key, template_write_annotation_key_for,
    },
    types::{SandboxId, TemplateArtifactDigest, TemplateReference},
};
use pplx_space_common_svc::space_api_route::wait_ready_lifecycle_request;
use pplx_space_metrics::warm_pool::{
    ProbeOutcome, ProbeStep, ProbeStepOutcome, PromoteOutcome, SlotDeleteReason,
};
use pplx_space_sandbox_teardown::finalize_sandbox_db_delete_tx;
use pplx_space_sbx_mgr_client::proto::SpacedWaitReadyRequest;
use time::{Duration as TimeDuration, OffsetDateTime};
use tokio::{
    sync::Semaphore,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tonic::{Code, transport::Endpoint};
use uuid::Uuid;

use crate::pod::{build_pool_pod, pool_pod_name, ready_pod_metadata};

/// Why a warm-pool tick, stage, or startup read failed. `Io` is every
/// Postgres, Kubernetes, and substrate failure with its type erased: the tick
/// loop only logs and counts them, and a per-source variant for each of the
/// dozen sources would be an enum nobody matches on.
#[derive(Debug, thiserror::Error)]
pub enum WarmPoolError {
    #[error(transparent)]
    PodConfig(#[from] pplx_space_common_core::kubernetes::SandboxPodConfigError),
    #[error("warm slot create semaphore closed")]
    CreatePermitsClosed(#[source] tokio::sync::AcquireError),
    #[error("warm pool config reconcile worker panicked: {0}")]
    WorkerPanicked(String),
    #[error(transparent)]
    Io(Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl WarmPoolError {
    fn io(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Io(Box::new(source))
    }
}

impl<E> From<TimedIoError<E>> for WarmPoolError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(source: TimedIoError<E>) -> Self {
        Self::io(source)
    }
}

pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_secs(5);
pub const DEFAULT_WARMING_SLOT_TIMEOUT: Duration = Duration::from_secs(10 * 60);
pub const DEFAULT_CLAIMED_SLOT_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_RECONCILE_K8S_IO_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT: Duration = Duration::from_secs(120);
pub const DEFAULT_MAX_CONCURRENT_CONFIGS: usize = 8;
pub const MAX_RECONCILE_CONFIG_CONCURRENCY: usize = 16;
const RECONCILE_CONFIG_RETIREMENT_GRACE: Duration = Duration::from_secs(10 * 60);
const CONFIG_INVENTORY_HEARTBEAT: Duration = Duration::from_secs(5 * 60);
const WARM_SLOT_STARTED_PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const WARM_SLOT_STARTED_CONNECT_TIMEOUT: Duration = Duration::from_millis(150);
const WARM_SLOT_STARTED_PROBE_MAX_RETRIES: usize = 2;
const WARM_SLOT_STARTED_PROBE_RETRY_BASE_DELAY: Duration = Duration::from_millis(40);
const WARM_SLOT_STARTED_PROBE_RETRY_JITTER: Duration = Duration::from_millis(40);

/// Hard ceiling on warm-slot creations kept in flight across all concurrently
/// reconciled pool configs.
///
/// Each create is a DB insert plus a Kubernetes pod create (hundreds of ms
/// of awaited work), so the previous sequential loop realized only ~2
/// creates/s regardless of `refill_per_tick`. The shared semaphore in
/// `Reconciler` lifts the realized rate while preventing parallel pool-config
/// workers from multiplying Kubernetes and node-manager pressure.
pub const MAX_WARM_SLOT_CREATE_CONCURRENCY: usize = 16;

const SANDBOX_NAMESPACE_ENV: &str = "SPACE_SANDBOX_NAMESPACE";
const POD_NAMESPACE_ENV: &str = "POD_NAMESPACE";
const STALE_CLAIMED_SLOT_SWEEP_LIMIT: i64 = 100;
const CLEANUP_PENDING_SLOT_SWEEP_LIMIT: i64 = 100;
/// Generous relative to any in-flight sandbox-delete transaction: the sweep
/// only backstops receipts the delete paths' inline retire missed, so there
/// is no urgency, and staying far behind the deleting transaction removes any
/// chance of sampling its uncommitted absence.
const ORPHANED_RECEIPT_DRAIN_GRACE: Duration = Duration::from_secs(60 * 60);
/// The receipt backstops (dead-claimed and orphaned-bound sweeps) catch
/// receipts the inline retires in suspend finalization and sandbox delete
/// missed — steady-state matches are ~zero, so they run at a slow cadence
/// with a bounded scan window per run instead of rescanning the table every
/// reconcile tick. At 10k rows per 5 minutes a 700k-row table is fully
/// revisited in ~6 hours, which bounds how long a missed receipt can linger;
/// per-run Postgres cost stays constant as the table grows.
const RECEIPT_BACKSTOP_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RECEIPT_BACKSTOP_SCAN_WINDOW: i64 = 10_000;
/// Windows per backstop per gated run: 100k rows every 5 minutes sweeps a
/// 700k-row table in ~35 minutes while each individual query stays bounded
/// at [`RECEIPT_BACKSTOP_SCAN_WINDOW`] rows.
const RECEIPT_BACKSTOP_WINDOWS_PER_RUN: usize = 10;

#[derive(Debug, Clone)]
pub struct Config {
    pub namespace: String,
    pub reconcile_interval: Duration,
    pub warming_slot_timeout: Duration,
    pub max_concurrent_configs: usize,
    io_timeouts: ReconcileIoTimeouts,
    pub pod: SandboxPodConfigValues,
    /// Orphan-pod sweep stage knobs (default-off, dry-run-first).
    orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig,
}

impl Config {
    pub fn from_env() -> Result<Self, WarmPoolError> {
        Ok(Self {
            namespace: sandbox_namespace_from_env(),
            reconcile_interval: DEFAULT_RECONCILE_INTERVAL,
            warming_slot_timeout: DEFAULT_WARMING_SLOT_TIMEOUT,
            max_concurrent_configs: DEFAULT_MAX_CONCURRENT_CONFIGS,
            io_timeouts: ReconcileIoTimeouts {
                postgres: DEFAULT_RECONCILE_POSTGRES_IO_TIMEOUT,
                k8s: DEFAULT_RECONCILE_K8S_IO_TIMEOUT,
                grpc_substrate: DEFAULT_RECONCILE_GRPC_SUBSTRATE_IO_TIMEOUT,
            },
            pod: pool_pod_config_from_env()?,
            orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig::from_env(),
        })
    }
}

pub(crate) async fn run(
    kube: Client,
    cache: WarmPoolKubeCache,
    store: WarmPoolStore,
    substrate: Arc<Substrate>,
    shutdown_token: CancellationToken,
) -> Result<(), WarmPoolError> {
    let config = Config::from_env()?;
    let reconciler = Reconciler::new(
        kube,
        cache,
        store,
        substrate,
        config.namespace,
        config.pod,
        ReconcilerTuning {
            warming_slot_timeout: config.warming_slot_timeout,
            claimed_slot_timeout: DEFAULT_CLAIMED_SLOT_TIMEOUT,
            max_concurrent_configs: config.max_concurrent_configs,
            io_timeouts: config.io_timeouts,
            orphan_sweep: config.orphan_sweep,
        },
    );
    let mut reconcile_tick = interval(config.reconcile_interval);
    reconcile_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

    tracing::info!(
        reconcile_interval_ms = config.reconcile_interval.as_millis(),
        warming_slot_timeout_ms = config.warming_slot_timeout.as_millis(),
        max_concurrent_configs = config.max_concurrent_configs,
        "warm pool reconciler starting"
    );

    loop {
        tokio::select! {
            () = shutdown_token.cancelled() => {
                tracing::info!("warm pool reconciler shutting down");
                return Ok(());
            }
            _ = reconcile_tick.tick() => {
                let started_at = Instant::now();
                let result = reconciler.reconcile_once().await;
                let result_ok = result.is_ok();
                pplx_space_metrics::warm_pool::reconcile_tick_duration(started_at);
                pplx_space_metrics::warm_pool::reconcile_tick(result_ok);
                let total_duration_ms = started_at.elapsed().as_millis();

                match result {
                    Ok(summary) => {
                        summary.emit_metrics();
                        tracing::info!(
                            total_duration_ms,
                            enabled_pool_configs = summary.enabled_pool_configs,
                            template_write_annotations = summary.template_write_annotations,
                            created_slots = summary.created_slots,
                            marked_ready_slots = summary.marked_ready_slots,
                            deleted_slots = summary.deleted_slots(),
                            deleted_missing_pods = summary.deleted_missing_pods,
                            deleted_unready_pods = summary.deleted_unready_pods,
                            deleted_excess_slots = summary.deleted_excess_slots,
                            deleted_mismatched_slots = summary.deleted_mismatched_slots,
                            deleted_stale_claimed_slots = summary.deleted_stale_claimed_slots,
                            deleted_dead_claimed_slots = summary.deleted_dead_claimed_slots,
                            deleted_orphaned_receipts = summary.deleted_orphaned_receipts,
                            deleted_cleanup_pending_slots = summary.deleted_cleanup_pending_slots,
                            deleted_cordoned_slots = summary.deleted_cordoned_slots,
                            "warm pool reconcile completed"
                        );
                    }
                    Err(err) => {
                        tracing::warn!(
                            total_duration_ms,
                            error = %err,
                            "warm pool reconcile failed"
                        );
                    }
                }
            }
        }
    }
}

fn reconcile_config_concurrency(value: usize) -> usize {
    value.clamp(1, MAX_RECONCILE_CONFIG_CONCURRENCY)
}

#[derive(Debug, Clone, Copy)]
struct ReconcileIoTimeouts {
    postgres: Duration,
    k8s: Duration,
    grpc_substrate: Duration,
}

impl ReconcileIoTimeouts {
    fn postgres(self) -> Duration {
        self.postgres
    }

    fn k8s(self) -> Duration {
        self.k8s
    }

    fn grpc_substrate(self) -> Duration {
        self.grpc_substrate
    }
}

#[derive(Debug, thiserror::Error)]
#[error("warm pool reconcile I/O timed out after {timeout_ms}ms: operation={operation}")]
struct ReconcileIoTimeout {
    operation: &'static str,
    timeout_ms: u128,
}

#[derive(Debug, thiserror::Error)]
enum TimedIoError<E> {
    #[error(transparent)]
    Operation(#[from] E),
    #[error(transparent)]
    Timeout(ReconcileIoTimeout),
}

async fn timed_io<T, E, F>(
    operation: &'static str,
    timeout_duration: Duration,
    future: F,
) -> Result<T, TimedIoError<E>>
where
    E: std::error::Error + 'static,
    F: Future<Output = Result<T, E>>,
{
    let result = tokio::time::timeout(timeout_duration, future).await;
    match result {
        Ok(result) => result.map_err(TimedIoError::Operation),
        Err(_) => Err(TimedIoError::Timeout(ReconcileIoTimeout {
            operation,
            timeout_ms: timeout_duration.as_millis(),
        })),
    }
}

async fn with_create_permit<T, F>(create_permits: &Semaphore, future: F) -> Result<T, WarmPoolError>
where
    F: Future<Output = Result<T, WarmPoolError>>,
{
    let _permit = create_permits
        .acquire()
        .await
        .map_err(WarmPoolError::CreatePermitsClosed)?;
    future.await
}

/// Per-config refill fan-out derived from that config's `refill_per_tick`
/// budget. `Reconciler::create_permits` enforces the aggregate cap across
/// concurrently reconciled configs.
fn warm_slot_create_concurrency(config: &PoolConfig) -> usize {
    usize::try_from(config.refill_per_tick)
        .unwrap_or(0)
        .clamp(1, MAX_WARM_SLOT_CREATE_CONCURRENCY)
}

fn pool_pod_config_from_env() -> Result<SandboxPodConfigValues, WarmPoolError> {
    Ok(SandboxPodConfigValues::from_env()?)
}

fn sandbox_namespace_from_env() -> String {
    sandbox_namespace_from_environment(&pplx_space_common_core::env::ProcessEnvironment)
}

pub(crate) fn sandbox_namespace_from_environment(
    env: &dyn pplx_space_common_core::env::Environment,
) -> String {
    env.var(SANDBOX_NAMESPACE_ENV)
        .or_else(|_| env.var(POD_NAMESPACE_ENV))
        .unwrap_or_else(|_| "default".to_string())
}

struct Reconciler {
    kube: Client,
    pod_cache: PodCache,
    node_store: Store<Node>,
    store: WarmPoolStore,
    substrate: Arc<Substrate>,
    namespace: String,
    pod_config: SandboxPodConfigValues,
    warming_slot_timeout: Duration,
    claimed_slot_timeout: Duration,
    max_concurrent_configs: usize,
    create_permits: Arc<Semaphore>,
    io_timeouts: ReconcileIoTimeouts,
    ready_probe: SbxManagerWarmSlotReadyProbe,
    inventory_publications: Mutex<HashMap<Uuid, ConfigInventoryPublication>>,
    orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig,
    /// Persisted fairness cursor for the orphan sweep, so successive ticks
    /// rotate across the whole backlog instead of re-processing the same prefix.
    orphan_sweep_cursor: AtomicUsize,
    /// Cross-tick keyset cursors and cadence gate for the two receipt
    /// backstops; see [`RECEIPT_BACKSTOP_INTERVAL`]. Reset to the table
    /// start on process restart, which only shortens the current lap.
    receipt_backstops: Mutex<ReceiptBackstopState>,
    /// Resolves each config's node-group candidate set, once per config per
    /// tick. Gated by `pod_config.node_group_placement` — the same
    /// `SPACE_SANDBOX_NODE_GROUP_PLACEMENT` the API reads, from the same chart
    /// key — in which case a warm pod renders no node-group constraints and no
    /// query is issued.
    node_group_placement: NodeGroupPlacementResolver,
}

#[derive(Default)]
struct ReceiptBackstopState {
    last_run: Option<Instant>,
    dead_claimed_lap: Option<BackstopLap>,
    orphaned_lap: Option<BackstopLap>,
}

/// One in-progress sweep lap: resume position plus the high-water id captured
/// when the lap started. Slot ids are UUIDv7, so pinning the lap to the ids
/// that existed at its start keeps lap length independent of the insert rate;
/// rows born mid-lap wait for the next lap. `None` in
/// [`ReceiptBackstopState`] means the next run starts a fresh lap.
#[derive(Clone, Copy)]
struct BackstopLap {
    cursor: Uuid,
    high_water: Uuid,
}

#[derive(Clone, Copy)]
enum ReceiptBackstop {
    DeadClaimed,
    Orphaned,
}

/// Slot-lifecycle tuning knobs for the reconciler. Grouped so
/// `Reconciler::new` stays under clippy's `too_many_arguments` ceiling as
/// knobs are added.
struct ReconcilerTuning {
    warming_slot_timeout: Duration,
    claimed_slot_timeout: Duration,
    max_concurrent_configs: usize,
    io_timeouts: ReconcileIoTimeouts,
    orphan_sweep: crate::warm_pool_orphan_sweep::SweepConfig,
}

pub(crate) struct PodCache {
    pub(crate) store: Store<Pod>,
    pub(crate) ready: ReflectorReady,
}

pub(crate) struct WarmPoolKubeCache {
    pub(crate) pods: PodCache,
    pub(crate) nodes: Store<Node>,
}

trait WarmSlotReadyProbe {
    async fn slot_started(
        &self,
        slot: &PoolSlot,
        metadata: &crate::pod::ReadyPodMetadata,
        content_hash: &str,
    ) -> bool;
}

#[derive(Debug, Clone, Copy)]
struct SbxManagerWarmSlotReadyProbe {
    sandbox_proxy_port: u16,
    probe_timeout: Duration,
}

#[derive(Debug, thiserror::Error)]
enum WarmSlotProbeError {
    #[error(transparent)]
    InvalidEndpoint(#[from] WarmSlotRuntimeEndpointError),
    #[error("warm slot runtime is not reachable: {0}")]
    RuntimeNotReachable(#[source] pplx_space_api::controllers::ControllerError),
    #[error("warm slot sandbox readiness check returned status: {0}")]
    WaitReadyStatus(#[source] tonic::Status),
}

#[derive(Debug, thiserror::Error)]
enum WarmSlotProbeAttemptError {
    #[error(transparent)]
    Probe(#[from] WarmSlotProbeError),
    #[error(transparent)]
    Timeout(#[from] ReconcileIoTimeout),
}

#[derive(Debug)]
struct WarmSlotProbeProgress {
    current: Mutex<WarmSlotProbeCurrentStep>,
}

#[derive(Debug, Clone, Copy)]
struct WarmSlotProbeCurrentStep {
    step: ProbeStep,
    started_at: Instant,
}

impl WarmSlotProbeProgress {
    fn new() -> Self {
        Self {
            current: Mutex::new(WarmSlotProbeCurrentStep {
                step: ProbeStep::Connect,
                started_at: Instant::now(),
            }),
        }
    }

    fn start_step(&self, step: ProbeStep) -> Instant {
        let started_at = Instant::now();
        *self.current.lock().expect("probe progress mutex poisoned") =
            WarmSlotProbeCurrentStep { step, started_at };
        started_at
    }

    fn current_step(&self) -> WarmSlotProbeCurrentStep {
        *self.current.lock().expect("probe progress mutex poisoned")
    }
}

impl SbxManagerWarmSlotReadyProbe {
    fn new(sandbox_proxy_port: u16) -> Self {
        Self {
            sandbox_proxy_port,
            probe_timeout: WARM_SLOT_STARTED_PROBE_TIMEOUT,
        }
    }

    async fn probe_once(
        &self,
        slot: &PoolSlot,
        metadata: &crate::pod::ReadyPodMetadata,
        content_hash: &str,
        progress: &WarmSlotProbeProgress,
    ) -> Result<(), WarmSlotProbeError> {
        let endpoint = warm_slot_runtime_endpoint(
            &metadata.pod_ip,
            self.sandbox_proxy_port,
            WARM_SLOT_STARTED_CONNECT_TIMEOUT,
        )
        .map_err(WarmSlotProbeError::InvalidEndpoint)?;
        let connect_started_at = progress.start_step(ProbeStep::Connect);
        let mut client = match connect_worker(endpoint).await {
            Ok(client) => {
                emit_promote_probe_step(
                    content_hash,
                    ProbeStep::Connect,
                    ProbeStepOutcome::Success,
                    connect_started_at,
                );
                client
            }
            Err(err) => {
                emit_promote_probe_step(
                    content_hash,
                    ProbeStep::Connect,
                    ProbeStepOutcome::Error,
                    connect_started_at,
                );
                return Err(WarmSlotProbeError::RuntimeNotReachable(err));
            }
        };

        let wait_ready_started_at = progress.start_step(ProbeStep::WaitReady);
        match client
            .spaced_wait_ready(wait_ready_lifecycle_request(SpacedWaitReadyRequest {
                sandbox_id: slot.reserved_sandbox_id.to_string(),
                timeout: prost_types::Duration::try_from(self.probe_timeout).ok(),
            }))
            .await
        {
            Ok(_) => {
                emit_promote_probe_step(
                    content_hash,
                    ProbeStep::WaitReady,
                    ProbeStepOutcome::Success,
                    wait_ready_started_at,
                );
                Ok(())
            }
            Err(status) => {
                emit_promote_probe_step(
                    content_hash,
                    ProbeStep::WaitReady,
                    ProbeStepOutcome::Error,
                    wait_ready_started_at,
                );
                Err(WarmSlotProbeError::WaitReadyStatus(status))
            }
        }
    }
}

impl WarmSlotReadyProbe for SbxManagerWarmSlotReadyProbe {
    async fn slot_started(
        &self,
        slot: &PoolSlot,
        metadata: &crate::pod::ReadyPodMetadata,
        content_hash: &str,
    ) -> bool {
        let max_attempts = WARM_SLOT_STARTED_PROBE_MAX_RETRIES + 1;
        for retry_index in 0..=WARM_SLOT_STARTED_PROBE_MAX_RETRIES {
            let attempt = retry_index + 1;
            let started_at = Instant::now();
            let progress = WarmSlotProbeProgress::new();
            let result = match tokio::time::timeout(
                self.probe_timeout,
                self.probe_once(slot, metadata, content_hash, &progress),
            )
            .await
            {
                Ok(result) => result.map_err(WarmSlotProbeAttemptError::Probe),
                Err(_) => Err(WarmSlotProbeAttemptError::Timeout(ReconcileIoTimeout {
                    operation: "warm-slot-started-probe",
                    timeout_ms: self.probe_timeout.as_millis(),
                })),
            };

            match result {
                Ok(()) => {
                    emit_promote_probe(content_hash, ProbeOutcome::Ready, started_at);
                    tracing::debug!(
                        slot_id = %slot.id,
                        sandbox_id = %slot.reserved_sandbox_id,
                        pod = %slot.pod_name,
                        pod_ip = %metadata.pod_ip,
                        node_name = %metadata.node_name,
                        attempt,
                        max_attempts,
                        duration_ms = started_at.elapsed().as_millis(),
                        "warm pool: sandbox start readiness probe succeeded"
                    );
                    return true;
                }
                Err(err) => {
                    let outcome = warm_slot_probe_outcome(&err);
                    emit_promote_probe(content_hash, outcome, started_at);
                    if outcome == ProbeOutcome::Timeout {
                        let step = progress.current_step();
                        emit_promote_probe_step(
                            content_hash,
                            step.step,
                            ProbeStepOutcome::Timeout,
                            step.started_at,
                        );
                    }

                    if retry_index < WARM_SLOT_STARTED_PROBE_MAX_RETRIES
                        && is_retryable_warm_slot_probe_error(&err)
                    {
                        let retry_delay = warm_slot_probe_retry_delay(slot.id, retry_index);
                        let step = progress.current_step();
                        tracing::warn!(
                            slot_id = %slot.id,
                            sandbox_id = %slot.reserved_sandbox_id,
                            pod = %slot.pod_name,
                            pod_ip = %metadata.pod_ip,
                            node_name = %metadata.node_name,
                            probe_step = %step.step,
                            attempt,
                            max_attempts,
                            num_retries = retry_index,
                            max_retries = WARM_SLOT_STARTED_PROBE_MAX_RETRIES,
                            retry_in_ms = retry_delay.as_millis(),
                            reason = warm_slot_probe_failure_reason(&err),
                            error = %err,
                            "warm pool: sandbox start readiness probe failed, retrying"
                        );
                        tokio::time::sleep(retry_delay).await;
                        continue;
                    }

                    log_terminal_warm_slot_probe_failure(
                        slot,
                        metadata,
                        WarmSlotProbeFailureLogContext {
                            err: &err,
                            outcome,
                            attempt,
                            max_attempts,
                            probe_timeout: self.probe_timeout,
                            progress: &progress,
                        },
                    );
                    return false;
                }
            }
        }

        false
    }
}

fn warm_slot_probe_outcome(err: &WarmSlotProbeAttemptError) -> ProbeOutcome {
    match err {
        WarmSlotProbeAttemptError::Timeout(_) => ProbeOutcome::Timeout,
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(status))
            if is_warm_slot_not_started_status(status) =>
        {
            ProbeOutcome::NotStarted
        }
        WarmSlotProbeAttemptError::Probe(_) => ProbeOutcome::Error,
    }
}

fn is_retryable_warm_slot_probe_error(err: &WarmSlotProbeAttemptError) -> bool {
    match err {
        WarmSlotProbeAttemptError::Timeout(_) => true,
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::RuntimeNotReachable(_)) => true,
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(status)) => {
            is_retryable_warm_slot_wait_ready_status(status)
        }
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::InvalidEndpoint(_)) => false,
    }
}

fn warm_slot_probe_failure_reason(err: &WarmSlotProbeAttemptError) -> &'static str {
    match err {
        WarmSlotProbeAttemptError::Timeout(_) => "probe_timeout",
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::RuntimeNotReachable(_)) => {
            "runtime_unreachable"
        }
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::WaitReadyStatus(status)) => {
            match status.code() {
                Code::Unavailable => "wait_ready_unavailable",
                Code::DeadlineExceeded => "wait_ready_deadline_exceeded",
                Code::Unknown => "wait_ready_unknown",
                Code::Cancelled => "wait_ready_cancelled",
                Code::FailedPrecondition => "sandbox_not_started",
                _ => "wait_ready_status",
            }
        }
        WarmSlotProbeAttemptError::Probe(WarmSlotProbeError::InvalidEndpoint(_)) => {
            "invalid_endpoint"
        }
    }
}

fn is_retryable_warm_slot_wait_ready_status(status: &tonic::Status) -> bool {
    matches!(
        status.code(),
        Code::Unavailable
            | Code::DeadlineExceeded
            | Code::Unknown
            | Code::Cancelled
            | Code::FailedPrecondition
    )
}

fn is_warm_slot_not_started_status(status: &tonic::Status) -> bool {
    status.code() == Code::FailedPrecondition
}

fn warm_slot_probe_retry_delay(slot_id: Uuid, retry_index: usize) -> Duration {
    let base_ms =
        WARM_SLOT_STARTED_PROBE_RETRY_BASE_DELAY.as_millis() as u64 * (retry_index as u64 + 1);
    let jitter_bound_ms = WARM_SLOT_STARTED_PROBE_RETRY_JITTER.as_millis() as u64;
    let jitter_ms = if jitter_bound_ms == 0 {
        0
    } else {
        let rotated = slot_id
            .as_u128()
            .rotate_right(((retry_index % 8) * 16) as u32);
        (rotated % (jitter_bound_ms as u128 + 1)) as u64
    };
    Duration::from_millis(base_ms + jitter_ms)
}

struct WarmSlotProbeFailureLogContext<'a> {
    err: &'a WarmSlotProbeAttemptError,
    outcome: ProbeOutcome,
    attempt: usize,
    max_attempts: usize,
    probe_timeout: Duration,
    progress: &'a WarmSlotProbeProgress,
}

impl WarmSlotProbeFailureLogContext<'_> {
    fn num_retries(&self) -> usize {
        self.attempt.saturating_sub(1)
    }

    fn retries_exhausted(&self) -> bool {
        self.attempt == self.max_attempts && is_retryable_warm_slot_probe_error(self.err)
    }
}

fn log_terminal_warm_slot_probe_failure(
    slot: &PoolSlot,
    metadata: &crate::pod::ReadyPodMetadata,
    context: WarmSlotProbeFailureLogContext<'_>,
) {
    match context.outcome {
        ProbeOutcome::Timeout => {
            let step = context.progress.current_step();
            tracing::warn!(
                slot_id = %slot.id,
                sandbox_id = %slot.reserved_sandbox_id,
                pod = %slot.pod_name,
                pod_ip = %metadata.pod_ip,
                node_name = %metadata.node_name,
                probe_step = %step.step,
                attempt = context.attempt,
                max_attempts = context.max_attempts,
                num_retries = context.num_retries(),
                max_retries = WARM_SLOT_STARTED_PROBE_MAX_RETRIES,
                retries_exhausted = context.retries_exhausted(),
                reason = warm_slot_probe_failure_reason(context.err),
                timeout_ms = context.probe_timeout.as_millis(),
                error = %context.err,
                "warm pool: timed out probing sandbox start readiness"
            );
        }
        ProbeOutcome::NotStarted => {
            let step = context.progress.current_step();
            tracing::warn!(
                slot_id = %slot.id,
                sandbox_id = %slot.reserved_sandbox_id,
                pod = %slot.pod_name,
                pod_ip = %metadata.pod_ip,
                node_name = %metadata.node_name,
                probe_step = %step.step,
                attempt = context.attempt,
                max_attempts = context.max_attempts,
                num_retries = context.num_retries(),
                max_retries = WARM_SLOT_STARTED_PROBE_MAX_RETRIES,
                retries_exhausted = context.retries_exhausted(),
                reason = warm_slot_probe_failure_reason(context.err),
                error = %context.err,
                "warm pool: slot pod is running but sandbox is not started"
            );
        }
        ProbeOutcome::Error => {
            let step = context.progress.current_step();
            tracing::warn!(
                slot_id = %slot.id,
                sandbox_id = %slot.reserved_sandbox_id,
                pod = %slot.pod_name,
                pod_ip = %metadata.pod_ip,
                node_name = %metadata.node_name,
                probe_step = %step.step,
                attempt = context.attempt,
                max_attempts = context.max_attempts,
                num_retries = context.num_retries(),
                max_retries = WARM_SLOT_STARTED_PROBE_MAX_RETRIES,
                retries_exhausted = context.retries_exhausted(),
                reason = warm_slot_probe_failure_reason(context.err),
                error = %context.err,
                "warm pool: sandbox start readiness probe failed"
            );
        }
        ProbeOutcome::Ready => {}
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileSummary {
    pub enabled_pool_configs: usize,
    pub template_write_annotations: usize,
    pub created_slots: usize,
    pub marked_ready_slots: usize,
    pub deleted_missing_pods: usize,
    pub deleted_unready_pods: usize,
    pub deleted_excess_slots: usize,
    pub deleted_mismatched_slots: usize,
    pub deleted_stale_claimed_slots: usize,
    pub deleted_dead_claimed_slots: usize,
    pub deleted_orphaned_receipts: usize,
    pub deleted_cleanup_pending_slots: usize,
    pub deleted_cordoned_slots: usize,
    reconciled_slot_counts: Option<PoolSlotCounts>,
    failed_required_operations: usize,
}

impl Reconciler {
    fn new(
        kube: Client,
        cache: WarmPoolKubeCache,
        store: WarmPoolStore,
        substrate: Arc<Substrate>,
        namespace: String,
        pod_config: SandboxPodConfigValues,
        tuning: ReconcilerTuning,
    ) -> Self {
        let node_group_placement =
            NodeGroupPlacementResolver::new(store.pool().clone(), pod_config.node_group_placement);
        Self {
            kube,
            pod_cache: cache.pods,
            node_store: cache.nodes,
            store,
            substrate,
            namespace,
            pod_config,
            warming_slot_timeout: tuning.warming_slot_timeout,
            claimed_slot_timeout: tuning.claimed_slot_timeout,
            max_concurrent_configs: tuning.max_concurrent_configs,
            create_permits: Arc::new(Semaphore::new(MAX_WARM_SLOT_CREATE_CONCURRENCY)),
            io_timeouts: tuning.io_timeouts,
            ready_probe: SbxManagerWarmSlotReadyProbe::new(DEFAULT_SANDBOX_PROXY_PORT),
            inventory_publications: Mutex::new(HashMap::new()),
            orphan_sweep: tuning.orphan_sweep,
            orphan_sweep_cursor: AtomicUsize::new(0),
            receipt_backstops: Mutex::new(ReceiptBackstopState::default()),
            node_group_placement,
        }
    }

    async fn timed_postgres<T, E, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, WarmPoolError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
    {
        Ok(timed_io(operation, self.io_timeouts.postgres(), future).await?)
    }

    async fn timed_k8s<T, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, TimedIoError<KubeError>>
    where
        F: Future<Output = Result<T, KubeError>>,
    {
        timed_io(operation, self.io_timeouts.k8s(), future).await
    }

    async fn timed_grpc_substrate<T, E, F>(
        &self,
        operation: &'static str,
        future: F,
    ) -> Result<T, WarmPoolError>
    where
        E: std::error::Error + Send + Sync + 'static,
        F: Future<Output = Result<T, E>>,
    {
        Ok(timed_io(operation, self.io_timeouts.grpc_substrate(), future).await?)
    }

    async fn reconcile_once(&self) -> Result<ReconcileSummary, WarmPoolError> {
        let deleted_configs = self
            .timed_postgres(
                "autoscale-configs",
                self.store.autoscale_configs(&self.namespace),
            )
            .await?;
        let mut configs = self
            .timed_postgres("reconcilable-configs", self.store.reconcilable_configs())
            .await?;
        let recently_disabled_configs = self
            .timed_postgres(
                "recently-disabled-configs",
                self.store
                    .recently_disabled_configs(RECONCILE_CONFIG_RETIREMENT_GRACE),
            )
            .await?;
        let inventory_config_ids = configs
            .iter()
            .chain(&recently_disabled_configs)
            .map(|config| config.id)
            .collect::<HashSet<_>>();
        let reconcilable_config_ids = configs
            .iter()
            .map(|config| config.id)
            .collect::<HashSet<_>>();
        self.emit_retired_config_states(retired_configs_to_emit(
            deleted_configs,
            recently_disabled_configs,
            &reconcilable_config_ids,
        ));
        self.inventory_publications
            .lock()
            .expect("config inventory publication mutex poisoned")
            .retain(|config_id, _| inventory_config_ids.contains(config_id));
        configs.sort_by(|left, right| {
            left.template_name
                .as_str()
                .cmp(right.template_name.as_str())
                .then_with(|| right.created_at.cmp(&left.created_at))
                .then_with(|| right.id.cmp(&left.id))
        });
        let mut summary = ReconcileSummary {
            enabled_pool_configs: configs.iter().filter(|config| config.enabled).count(),
            ..Default::default()
        };
        summary += self.delete_cleanup_pending_slots().await?;
        summary += self.delete_stale_claimed_slots().await?;
        summary += self.sweep_receipt_backstops().await?;

        // Build the cordoned-node set once per tick from the shared Node
        // reflector. Missing or stale observations are tolerated by retrying
        // next tick, matching the old LIST-failure fallback of treating the
        // cluster as having no cordoned nodes for this pass.
        let cordoned = self.cordoned_node_names();

        let config_results = reconcile_configs(configs, self.max_concurrent_configs, |config| {
            let cordoned = &cordoned;
            async move { self.reconcile_config(&config, cordoned).await }
        })
        .await;
        for config_result in config_results {
            self.emit_config_result(&config_result, Instant::now());
            let (config, result) = config_result.into_parts();
            match result {
                Ok(config_summary) => summary += config_summary,
                Err(err) => tracing::warn!(
                    config_id = %config.id,
                    template_release_id = %config.template_release_id,
                    template = %config.template_name,
                    error = %err,
                    "warm pool: config reconcile failed; continuing with remaining configs"
                ),
            }
        }

        // Reverse (actual→desired) orphan-pod sweep: reclaim terminal warm pods
        // the pool no longer tracks. A bounded stage reusing this reconciler's
        // reflector cache, store, kube client, and cadence. Runs independently
        // of pool configs (it scans all warm pods); no-op unless enabled.
        self.reconcile_orphan_pods().await;

        Ok(summary)
    }

    fn emit_config_result(&self, result: &ConfigReconcileResult, now: Instant) {
        let succeeded = result.succeeded();
        let counts = result.slot_counts();
        if self.should_emit_config_inventory(&result.config, succeeded, now) {
            emit_config_inventory(&result.config, counts, succeeded);
        }
        emit_reconcile_config_states(&result.config, succeeded);
    }

    fn should_emit_config_inventory(
        &self,
        config: &PoolConfig,
        succeeded: bool,
        now: Instant,
    ) -> bool {
        let mut publications = self
            .inventory_publications
            .lock()
            .expect("config inventory publication mutex poisoned");
        let config_state = ConfigInventoryState::from_config(config);
        let should_emit = publications.get(&config.id).is_none_or(|previous| {
            previous.config != config_state
                || previous.succeeded != succeeded
                || now.duration_since(previous.emitted_at) >= CONFIG_INVENTORY_HEARTBEAT
        });
        if should_emit {
            publications.insert(
                config.id,
                ConfigInventoryPublication {
                    config: config_state,
                    succeeded,
                    emitted_at: now,
                },
            );
        }
        should_emit
    }

    fn emit_retired_config_states(&self, configs: impl IntoIterator<Item = PoolConfig>) {
        for config in configs {
            if self.should_emit_config_inventory(&config, false, Instant::now()) {
                emit_config_inventory(&config, Some(PoolSlotCounts::default()), false);
            }
            emit_reconcile_config_states(&config, false);
        }
    }

    /// Invoke the orphan-pod sweep stage with this reconciler's handles and
    /// log its summary. Kept as a thin method so `reconcile_once` reads as a
    /// list of stages; the logic lives in [`crate::warm_pool_orphan_sweep`].
    async fn reconcile_orphan_pods(&self) {
        if !self.orphan_sweep.enabled {
            return;
        }
        let started = Instant::now();
        let report = crate::warm_pool_orphan_sweep::sweep(
            &self.kube,
            &self.pod_cache,
            &self.store,
            &self.namespace,
            &self.orphan_sweep,
            &self.orphan_sweep_cursor,
            crate::warm_pool_orphan_sweep::StageTimeouts {
                k8s: self.io_timeouts.k8s(),
                postgres: self.io_timeouts.postgres(),
            },
        )
        .await;
        pplx_space_metrics::cluster_reconciler::loop_latency(
            pplx_space_metrics::cluster_reconciler::Loop::WarmPoolOrphanGc,
            started,
        );
        tracing::info!(
            scanned_warm_pods = report.scanned_warm_pods,
            candidates = report.candidates,
            too_young = report.too_young,
            deleted_or_would_delete = report.deleted,
            sandbox_bound = report.sandbox_bound,
            slot_row_live = report.slot_row_live,
            deferred = report.deferred,
            errors = report.errors,
            db_error = report.db_error,
            dry_run = self.orphan_sweep.dry_run,
            "warm pool: orphan sweep stage complete"
        );
    }

    async fn reconcile_config_stage<T, F>(
        &self,
        config: &PoolConfig,
        stage: &'static str,
        future: F,
    ) -> Result<T, WarmPoolError>
    where
        F: Future<Output = Result<T, WarmPoolError>>,
    {
        let content_hash = config.content_hash.to_string();
        let template = config.template_name.as_str();
        tracing::info!(
            config_id = %config.id,
            content_hash = %content_hash,
            template = %template,
            stage,
            duration_ms = 0_u128,
            result = "started",
            "warm pool: stage start"
        );

        let started_at = Instant::now();
        let result = future.await;
        let duration_ms = started_at.elapsed().as_millis();
        pplx_space_metrics::warm_pool::reconcile_stage_duration(
            stage,
            &content_hash,
            template,
            config.vcpu.get().get(),
            config.memory.as_mib_floor(),
            started_at,
        );

        match result {
            Ok(value) => {
                tracing::info!(
                    config_id = %config.id,
                    content_hash = %content_hash,
                    template = %template,
                    stage,
                    duration_ms,
                    result = "ok",
                    "warm pool: stage done"
                );
                Ok(value)
            }
            Err(err) => {
                tracing::warn!(
                    config_id = %config.id,
                    content_hash = %content_hash,
                    template = %template,
                    stage,
                    duration_ms,
                    result = "err",
                    error = %err,
                    "warm pool: stage done"
                );
                Err(err)
            }
        }
    }

    async fn reconcile_config(
        &self,
        config: &PoolConfig,
        cordoned: &HashSet<String>,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let pods: Api<Pod> = Api::namespaced(self.kube.clone(), &self.namespace);
        // Resolved ahead of every stage that *adds* capacity, because the
        // candidate set gates this config's growth and not merely its refill.
        //
        // `ensure_template_write_annotations` below patches an exact-template
        // pull onto every worker matching the sandbox selector, and node-ctl
        // acts on it. Resolving after that stage would mean an incompatible
        // config — an org-specific template, which an org-less pooled pod can
        // never legally host — had already fanned its private rootfs across
        // shared capacity by the time the resolver refused to fill it: the pod
        // would fail closed and the *data* would not.
        //
        // A refusal blocks the annotation write and the refill — the two
        // stages that *add* — and leaves every stage that removes alone. It
        // deliberately does not abort the tick: those stages are each
        // error-isolated (`reconcile_config_stage_or_continue`) because a
        // config that cannot grow still has to be able to shrink. Propagating
        // here would take out `evict_slots_on_cordoned_nodes` — which node
        // drain waits on — and `delete_excess_open_slots`, which is how an
        // operator drains a config to zero by disabling it. A template row that
        // is absent or org-specific is a standing condition, so that would be a
        // permanent freeze, not a skipped tick.
        //
        // It is not *only* those two stages, though: booking
        // `failed_required_operations` also publishes this config as failing
        // (`ConfigReconcileResult::succeeded`), so its inventory and state
        // gauges report unhealthy for as long as the condition stands. That is
        // reporting rather than capacity, and it is the intended signal.
        //
        // The `Err` arm is not only the standing conditions. A Postgres
        // timeout or connection failure lands here too, and is treated the
        // same on purpose: under an unknown template visibility, not fanning a
        // pull is the conservative reading, and the cost is one tick's delay
        // (the reconcile interval) rather than a permanent freeze. If DB blips
        // ever show up as warm-capacity lag, splitting the transient arm to
        // skip only the refill is the change to make.
        //
        // Once per config per tick, not once per slot: every slot shares this
        // config's template and the org-less warm-pool subject, so the answer
        // is identical across the fan-out, and resolving per slot would also
        // hold a create permit — a budget that exists to bound Kubernetes
        // create concurrency — while waiting on Postgres.
        //
        // Wrapped unconditionally rather than behind a gate check, which keeps
        // one reader of the gate rather than two: while the gate is closed the
        // resolver returns without querying at all.
        //
        // `timed_io` rather than `timed_postgres`, whose `Io` return erases
        // the variant: the log level below turns on whether the refusal is
        // standing or fresh, and recovering that through a type-erased chain
        // would mean downcasting past the wrapper itself. The two are otherwise
        // the same — `timed_postgres` is `timed_io` plus that conversion, and
        // neither emits a metric, so nothing observable changes by switching.
        let placement = timed_io(
            "resolve-node-group-placement",
            self.io_timeouts.postgres(),
            self.node_group_placement.resolve(
                NodeGroupResolvePath::WarmPool,
                PlacementSubject::WarmPool,
                config.template_name.as_str(),
            ),
        )
        .await;
        let mut summary = ReconcileSummary::default();
        let node_group_placement = match placement {
            Ok(resolved) => {
                summary += template_write_annotation_result(
                    config,
                    self.ensure_template_write_annotations(config).await,
                );
                Some(resolved)
            }
            Err(error) => {
                // The disposition is the same for every variant — see above —
                // but the log level is not, because the two classes need
                // different attention.
                //
                // A standing condition (an org-specific template on a pool
                // config, a reaped `templates` row) recurs every tick until an
                // operator acts, so a warn per config per tick is an unbounded
                // stream saying one thing. `info` there, with the durable
                // signals carrying it:
                // `warm_pool.reconcile.config_state{state:succeeded}`, which
                // the `failed_required_operations` booking below drives to 0
                // for this config, and
                // `node_group_placement.resolved{path:warm_pool}`.
                //
                // A catalog failure or a timeout is not standing — a Postgres
                // blip is news, and one tick later it is usually gone — so
                // those get `warn` rather than joining the stream a
                // misconfiguration creates.
                //
                // `Invariant` is grouped with them for a different reason. It
                // is not transient at all: the resolver and
                // `NodeGroupPlacement::for_caller` disagreeing about the same
                // candidate is deterministic and would recur every tick. It is
                // also unreachable by construction — the constructor re-checks
                // the predicate the resolver filtered with — so a warn stream
                // is the correct outcome if it ever appears, because the
                // premise that it cannot has been broken.
                match &error {
                    TimedIoError::Operation(
                        NodeGroupPlacementResolveError::NoCompatibleNodeGroup { .. }
                        | NodeGroupPlacementResolveError::TemplateAbsent { .. },
                    ) => {
                        tracing::info!(
                            config_id = %config.id,
                            template = %config.template_name,
                            %error,
                            "warm pool: no node-group candidate set for this config, so it \
                             neither pulls its template nor fills; teardown stages still run"
                        );
                    }
                    TimedIoError::Operation(
                        NodeGroupPlacementResolveError::Catalog(_)
                        | NodeGroupPlacementResolveError::Invariant(_),
                    )
                    | TimedIoError::Timeout(_) => {
                        tracing::warn!(
                            config_id = %config.id,
                            template = %config.template_name,
                            %error,
                            "warm pool: could not resolve this config's node-group candidate \
                             set, so it neither pulls its template nor fills this tick; \
                             teardown stages still run"
                        );
                    }
                }
                summary.failed_required_operations += 1;
                None
            }
        };
        summary += self
            .reconcile_config_stage_or_continue(
                config,
                "delete_mismatched_open_slots",
                self.delete_mismatched_open_slots(config),
            )
            .await;
        summary += self
            .reconcile_config_stage_or_continue(
                config,
                "observe_ready_slots",
                self.observe_ready_slots(config),
            )
            .await;
        summary += self
            .reconcile_config_stage_or_continue(
                config,
                "observe_warming_slots",
                self.observe_warming_slots(config),
            )
            .await;
        // Evict unclaimed slots whose pod is on a cordoned node before the
        // refill math runs, so the freed deficit is rebuilt on a schedulable
        // node within the same tick.
        summary += self
            .reconcile_config_stage_or_continue(
                config,
                "evict_slots_on_cordoned_nodes",
                self.evict_slots_on_cordoned_nodes(config, cordoned),
            )
            .await;
        summary += self
            .reconcile_config_stage_or_continue(
                config,
                "delete_excess_open_slots",
                self.delete_excess_open_slots(config),
            )
            .await;
        // Refill stays fail-closed when its authoritative count is unavailable.
        let counts = self
            .reconcile_config_stage(
                config,
                "slot_counts",
                self.timed_postgres("slot-counts", self.store.slot_counts(config.id)),
            )
            .await?;
        // Zero when the candidate set could not be resolved: this config may
        // shrink but not grow until an operator fixes the template row or the
        // node groups.
        let create_count = match node_group_placement.as_ref() {
            Some(_) => {
                usize::try_from(slots_to_create(config, counts.warming, counts.open())).unwrap_or(0)
            }
            None => 0,
        };

        // Fan the per-tick refill out concurrently instead of awaiting each
        // create serially. `create_count` already bounds the work to the
        // configured budget (the min of the open deficit, `refill_per_tick`,
        // and the remaining `max_warming_slots`); the concurrency derived from
        // this config's `refill_per_tick` only caps how many of those creates
        // are in flight at once. A single slot failure is logged and skipped —
        // it neither aborts the tick nor blocks the other creates, where the
        // prior sequential loop returned on the first error and abandoned the
        // rest of the refill.
        let concurrency = warm_slot_create_concurrency(config);
        let results = create_warming_slots(create_count, concurrency, || {
            self.create_warming_slot(
                config,
                &pods,
                node_group_placement
                    .as_ref()
                    .and_then(ResolvedPlacement::placement),
            )
        })
        .await;
        summary += create_warming_slot_result(config, results);

        let counts = counts_after_create(counts, summary.created_slots);
        emit_slot_gauges(config, counts);
        summary.reconciled_slot_counts = Some(counts);

        Ok(summary)
    }

    async fn reconcile_config_stage_or_continue<F>(
        &self,
        config: &PoolConfig,
        stage: &'static str,
        future: F,
    ) -> ReconcileSummary
    where
        F: Future<Output = Result<ReconcileSummary, WarmPoolError>>,
    {
        match self.reconcile_config_stage(config, stage, future).await {
            Ok(summary) => summary,
            Err(_) => ReconcileSummary {
                failed_required_operations: 1,
                ..Default::default()
            },
        }
    }

    async fn delete_stale_claimed_slots(&self) -> Result<ReconcileSummary, WarmPoolError> {
        let slots = self
            .timed_postgres(
                "stale-unbound-claimed-slots",
                self.store.stale_unbound_claimed_slots(
                    self.claimed_slot_timeout,
                    STALE_CLAIMED_SLOT_SWEEP_LIMIT,
                ),
            )
            .await?;

        Ok(
            Self::delete_stale_claimed_slot_batch(slots, |slot| async move {
                self.delete_claimed_slot_pod(&slot).await
            })
            .await,
        )
    }

    async fn delete_stale_claimed_slot_batch<F, Fut>(
        slots: Vec<PoolSlot>,
        mut delete_slot: F,
    ) -> ReconcileSummary
    where
        F: FnMut(PoolSlot) -> Fut,
        Fut: Future<Output = Result<bool, WarmPoolError>>,
    {
        let mut summary = ReconcileSummary::default();

        for slot in slots {
            match delete_slot(slot.clone()).await {
                Ok(true) => summary.deleted_stale_claimed_slots += 1,
                Ok(false) => {}
                Err(err) => {
                    tracing::warn!(
                        pool_slot_id = %slot.id,
                        sandbox_id = %slot.reserved_sandbox_id,
                        pod_name = %slot.pod_name,
                        pod_namespace = %slot.pod_namespace,
                        error = %err,
                        "warm pool: failed to delete stale claimed slot; continuing reconcile"
                    );
                }
            }
        }

        summary
    }

    /// Run up to [`RECEIPT_BACKSTOP_WINDOWS_PER_RUN`] bounded windows of each
    /// receipt backstop, at most once per [`RECEIPT_BACKSTOP_INTERVAL`].
    /// Ticks inside the interval are no-ops. Each backstop commits its cursor
    /// after every successful window and the two run independently, so a
    /// failure in one neither rewinds its own completed windows nor the other
    /// backstop's progress.
    async fn sweep_receipt_backstops(&self) -> Result<ReconcileSummary, WarmPoolError> {
        {
            let mut state = self
                .receipt_backstops
                .lock()
                .expect("receipt backstop state mutex poisoned");
            if state
                .last_run
                .is_some_and(|at| at.elapsed() < RECEIPT_BACKSTOP_INTERVAL)
            {
                return Ok(ReconcileSummary::default());
            }
            state.last_run = Some(Instant::now());
        }

        let dead = self
            .run_backstop_lap_windows(ReceiptBackstop::DeadClaimed)
            .await;
        let orphaned = self
            .run_backstop_lap_windows(ReceiptBackstop::Orphaned)
            .await;

        let summary = ReconcileSummary {
            deleted_dead_claimed_slots: usize::try_from(*dead.as_ref().unwrap_or(&0))
                .expect("sweep bounded by scan window"),
            deleted_orphaned_receipts: usize::try_from(*orphaned.as_ref().unwrap_or(&0))
                .expect("sweep bounded by scan window"),
            ..Default::default()
        };
        dead?;
        orphaned?;
        Ok(summary)
    }

    /// Run one gated batch of windows for a single backstop, starting a fresh
    /// lap when none is in progress and committing the cursor to
    /// [`Reconciler::receipt_backstops`] after every window. Stops early when
    /// the lap completes; the next gated run starts the next lap.
    async fn run_backstop_lap_windows(
        &self,
        backstop: ReceiptBackstop,
    ) -> Result<u64, WarmPoolError> {
        let mut deleted = 0;
        for _ in 0..RECEIPT_BACKSTOP_WINDOWS_PER_RUN {
            let lap = match self.backstop_lap(backstop) {
                Some(lap) => lap,
                None => {
                    let Some(high_water) = self
                        .timed_postgres("receipt-backstop-high-water", self.store.newest_slot_id())
                        .await?
                    else {
                        return Ok(deleted);
                    };
                    let lap = BackstopLap {
                        cursor: Uuid::nil(),
                        high_water,
                    };
                    self.store_backstop_lap(backstop, Some(lap));
                    lap
                }
            };

            let outcome = match backstop {
                ReceiptBackstop::DeadClaimed => {
                    self.timed_postgres(
                        "dead-claimed-slot-drain",
                        self.store.sweep_dead_claimed_slots_window(
                            self.claimed_slot_timeout,
                            lap.cursor,
                            lap.high_water,
                            RECEIPT_BACKSTOP_SCAN_WINDOW,
                        ),
                    )
                    .await?
                }
                ReceiptBackstop::Orphaned => {
                    self.timed_postgres(
                        "orphaned-receipt-drain",
                        self.store.sweep_orphaned_bound_receipts_window(
                            ORPHANED_RECEIPT_DRAIN_GRACE,
                            lap.cursor,
                            lap.high_water,
                            RECEIPT_BACKSTOP_SCAN_WINDOW,
                        ),
                    )
                    .await?
                }
            };
            deleted += outcome.deleted;
            match outcome.next_cursor {
                Some(cursor) => self.store_backstop_lap(
                    backstop,
                    Some(BackstopLap {
                        cursor,
                        high_water: lap.high_water,
                    }),
                ),
                None => {
                    self.store_backstop_lap(backstop, None);
                    return Ok(deleted);
                }
            }
        }
        Ok(deleted)
    }

    fn backstop_lap(&self, backstop: ReceiptBackstop) -> Option<BackstopLap> {
        let state = self
            .receipt_backstops
            .lock()
            .expect("receipt backstop state mutex poisoned");
        match backstop {
            ReceiptBackstop::DeadClaimed => state.dead_claimed_lap,
            ReceiptBackstop::Orphaned => state.orphaned_lap,
        }
    }

    fn store_backstop_lap(&self, backstop: ReceiptBackstop, lap: Option<BackstopLap>) {
        let mut state = self
            .receipt_backstops
            .lock()
            .expect("receipt backstop state mutex poisoned");
        match backstop {
            ReceiptBackstop::DeadClaimed => state.dead_claimed_lap = lap,
            ReceiptBackstop::Orphaned => state.orphaned_lap = lap,
        }
    }

    async fn delete_cleanup_pending_slots(&self) -> Result<ReconcileSummary, WarmPoolError> {
        let slots = self
            .timed_postgres(
                "cleanup-pending-slots",
                self.store
                    .cleanup_pending_slots(CLEANUP_PENDING_SLOT_SWEEP_LIMIT),
            )
            .await?;

        Ok(
            Self::delete_cleanup_pending_slot_batch(slots, |slot| async move {
                self.delete_cleanup_pending_slot(&slot).await
            })
            .await,
        )
    }

    async fn delete_cleanup_pending_slot_batch<F, Fut>(
        slots: Vec<PoolSlot>,
        mut delete_slot: F,
    ) -> ReconcileSummary
    where
        F: FnMut(PoolSlot) -> Fut,
        Fut: Future<Output = Result<bool, WarmPoolError>>,
    {
        let mut summary = ReconcileSummary::default();

        for slot in slots {
            let result = delete_slot(slot.clone()).await;
            Self::record_cleanup_pending_slot_delete_result(&mut summary, &slot, result);
        }

        summary
    }

    fn record_cleanup_pending_slot_delete_result(
        summary: &mut ReconcileSummary,
        slot: &PoolSlot,
        result: Result<bool, WarmPoolError>,
    ) {
        match result {
            Ok(true) => {
                summary.deleted_cleanup_pending_slots += 1;
            }
            Ok(false) => {}
            Err(err) => {
                tracing::warn!(
                    pool_slot_id = %slot.id,
                    sandbox_id = %slot.reserved_sandbox_id,
                    pod_name = %slot.pod_name,
                    pod_namespace = %slot.pod_namespace,
                    error = %err,
                    "warm pool: failed to delete cleanup-pending slot; continuing reconcile"
                );
            }
        }
    }

    /// Names of all Nodes currently cordoned (`spec.unschedulable == true`).
    /// The SPACE node-drain path cordons a node as the first step of draining
    /// it, so this set is exactly "nodes we should pull unclaimed warm
    /// capacity off of."
    fn cordoned_node_names(&self) -> HashSet<String> {
        cached_cordoned_node_names(&self.node_store)
    }

    /// Evict this config's unclaimed (`warming`/`ready`) slots whose pod is
    /// known to be on a cordoned node, deleting the pod and freeing the
    /// slot. The slot's `k8s_node_name` is recorded when the slot is marked
    /// ready, so this catches every `ready` slot — the unclaimed warm
    /// capacity that matters. A `warming` slot has no recorded node yet;
    /// since a cordoned node still runs its already-scheduled pods, the
    /// warming pod can still go ready, and is then evicted on the following
    /// tick. `delete_slot_pod` takes the open-slot delete lock, so a slot
    /// that is claimed concurrently (between this listing and the lock) is
    /// left untouched — we never evict a slot a caller just took.
    async fn evict_slots_on_cordoned_nodes(
        &self,
        config: &PoolConfig,
        cordoned: &HashSet<String>,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let mut summary = ReconcileSummary::default();
        if cordoned.is_empty() {
            return Ok(summary);
        }

        for slot in self
            .timed_postgres("cordoned-open-slots", self.store.open_slots(config.id))
            .await?
        {
            let Some(node) = slot.k8s_node_name.as_deref() else {
                continue;
            };
            if !cordoned.contains(node) {
                continue;
            }
            if self.delete_slot_pod(&slot).await? {
                summary.deleted_cordoned_slots += 1;
                tracing::info!(
                    slot_id = %slot.id,
                    pod = %slot.pod_name,
                    node = %node,
                    reason = "warm_pool_evict_cordoned",
                    "warm pool: evicted unclaimed slot from cordoned node"
                );
            }
        }

        Ok(summary)
    }

    async fn delete_mismatched_open_slots(
        &self,
        config: &PoolConfig,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let mut summary = ReconcileSummary::default();

        for slot in self
            .timed_postgres("mismatched-open-slots", self.store.open_slots(config.id))
            .await?
        {
            if slot_matches_config(&slot, config, &self.namespace) {
                continue;
            }

            if self.delete_slot_pod(&slot).await? {
                summary.deleted_mismatched_slots += 1;
            }
        }

        Ok(summary)
    }

    async fn delete_excess_open_slots(
        &self,
        config: &PoolConfig,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let target = target_open_slots(config);
        let open_slots = self
            .timed_postgres("excess-open-slots", self.store.open_slots(config.id))
            .await?;
        let delete_count = open_slots
            .len()
            .saturating_sub(usize::try_from(target).unwrap_or(0));
        let mut summary = ReconcileSummary::default();

        for slot in open_slots.into_iter().rev().take(delete_count) {
            if self.delete_slot_pod(&slot).await? {
                summary.deleted_excess_slots += 1;
            }
        }

        Ok(summary)
    }

    async fn observe_warming_slots(
        &self,
        config: &PoolConfig,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let mut summary = ReconcileSummary::default();
        let content_hash = config.content_hash.to_string();

        if !self.pod_cache.ready.is_ready() {
            tracing::debug!(
                config_id = %config.id,
                "warm pool: skipping warming slot observation until pod reflector cache is ready"
            );
            return Ok(summary);
        }

        let warming_slots = self
            .timed_postgres("warming-slots", self.store.warming_slots(config.id))
            .await?;
        if warming_slots.is_empty() {
            return Ok(summary);
        }
        let warm_pods_by_name = cached_warm_pods_by_name(&self.pod_cache.store, config);

        for slot in warming_slots {
            let Some(pod) = warm_pods_by_name.get(&slot.pod_name) else {
                if warming_slot_timed_out(slot.created_at, self.warming_slot_timeout)
                    && self.delete_slot_pod(&slot).await?
                {
                    summary.deleted_missing_pods += 1;
                }
                continue;
            };
            let pod = pod.as_ref();

            match claimable_slot_metadata(&slot, pod, &self.ready_probe, &content_hash).await {
                ClaimableSlotMetadata::Ready(metadata) => {
                    self.timed_postgres(
                        "mark-slot-ready",
                        self.store.mark_slot_ready(
                            slot.id,
                            &metadata.uid,
                            &metadata.pod_ip,
                            &metadata.node_name,
                        ),
                    )
                    .await?;
                    emit_promote_attempt(&content_hash, PromoteOutcome::Promoted);
                    tracing::info!(
                        config_id = %config.id,
                        content_hash = %content_hash,
                        slot = %slot.id,
                        pod = %slot.pod_name,
                        pod_ip = %metadata.pod_ip,
                        "warm pool: slot promoted"
                    );
                    summary.marked_ready_slots += 1;
                    continue;
                }
                ClaimableSlotMetadata::PodNotReady => {
                    emit_promote_attempt(&content_hash, PromoteOutcome::PodNotReady);
                    tracing::info!(
                        config_id = %config.id,
                        content_hash = %content_hash,
                        slot = %slot.id,
                        pod = %slot.pod_name,
                        pod_ip = %pod_ip_for_log(pod),
                        "warm pool: slot promotion skipped because pod is not ready"
                    );
                }
                ClaimableSlotMetadata::ProbeFailed(metadata) => {
                    emit_promote_attempt(&content_hash, PromoteOutcome::ProbeFailed);
                    tracing::info!(
                        config_id = %config.id,
                        content_hash = %content_hash,
                        slot = %slot.id,
                        pod = %slot.pod_name,
                        pod_ip = %metadata.pod_ip,
                        "warm pool: slot promotion skipped because readiness probe failed"
                    );
                }
            }

            if should_delete_unready_warming_pod(pod, &slot, self.warming_slot_timeout)
                && self.delete_slot_pod(&slot).await?
            {
                summary.deleted_unready_pods += 1;
            }
        }

        Ok(summary)
    }

    async fn observe_ready_slots(
        &self,
        config: &PoolConfig,
    ) -> Result<ReconcileSummary, WarmPoolError> {
        let mut summary = ReconcileSummary::default();
        if !self.pod_cache.ready.is_ready() {
            tracing::debug!(
                config_id = %config.id,
                "warm pool: skipping ready slot validation until pod reflector cache is ready"
            );
            return Ok(summary);
        }

        let ready_slots: Vec<_> = self
            .timed_postgres("ready-open-slots", self.store.open_slots(config.id))
            .await?
            .into_iter()
            .filter(|slot| slot.state == PoolSlotState::Ready)
            .collect();
        if ready_slots.is_empty() {
            return Ok(summary);
        }
        let warm_pods_by_name = cached_warm_pods_by_name(&self.pod_cache.store, config);

        for slot in ready_slots {
            let Some(pod) = warm_pods_by_name.get(&slot.pod_name) else {
                if self.delete_slot_pod(&slot).await? {
                    summary.deleted_missing_pods += 1;
                }
                continue;
            };

            if ready_slot_matches_pod(&slot, pod) {
                continue;
            }

            if self.delete_slot_pod(&slot).await? {
                summary.deleted_unready_pods += 1;
            }
        }

        Ok(summary)
    }

    async fn delete_slot_pod(&self, slot: &PoolSlot) -> Result<bool, WarmPoolError> {
        let mut tx = self
            .timed_postgres("delete-slot-txn-begin", self.store.pool().begin())
            .await?;
        if !self
            .timed_postgres(
                "delete-slot-lock-open-slot",
                WarmPoolStore::lock_open_slot_for_delete(&mut tx, slot.id),
            )
            .await?
        {
            return Ok(false);
        }

        let pods: Api<Pod> = Api::namespaced(self.kube.clone(), &slot.pod_namespace);
        if let Err(err) = self
            .timed_k8s(
                "delete-slot-pod",
                pods.delete(&slot.pod_name, &DeleteParams::default()),
            )
            .await
            && !is_not_found_error(&err)
        {
            return Err(err.into());
        }
        self.timed_postgres(
            "delete-slot-row",
            WarmPoolStore::delete_locked_open_slot(&mut tx, slot.id),
        )
        .await?;
        self.timed_postgres("delete-slot-txn-commit", tx.commit())
            .await?;
        Ok(true)
    }

    /// Reap one stale unbound claimed slot: recheck the reap predicate,
    /// delete the backing Kubernetes pod, then remove the slot row — in that
    /// order, with the pod delete outside any Postgres transaction. A slow
    /// Kubernetes API call must not pin a row lock and a pooled connection.
    ///
    /// Crash-safety comes from retry plus idempotence rather than atomicity:
    /// if anything fails after the pod delete, the slot row survives, the
    /// next sweep re-finds it, and the repeated pod delete lands on NotFound
    /// (tolerated). The row is only removed after the locked re-verification
    /// inside `finalize_slot_delete` confirms the slot is still stale and
    /// unbound. A bind that is still uncommitted during the lock-free recheck
    /// can commit after this function deletes the pod; repair loops own that
    /// residual failure mode.
    async fn reap_stale_claimed_slot<RFut, PFut, FFut>(
        recheck_stale_unbound: impl FnOnce() -> RFut,
        delete_pod: impl FnOnce() -> PFut,
        finalize_slot_delete: impl FnOnce() -> FFut,
    ) -> Result<bool, WarmPoolError>
    where
        RFut: Future<Output = Result<bool, WarmPoolError>>,
        PFut: Future<Output = Result<(), WarmPoolError>>,
        FFut: Future<Output = Result<bool, WarmPoolError>>,
    {
        // The batch scan that produced this candidate may be many seconds
        // stale by the time the slot is processed; re-verify right before the
        // irreversible pod delete.
        if !recheck_stale_unbound().await? {
            return Ok(false);
        }
        delete_pod().await?;
        finalize_slot_delete().await
    }

    async fn delete_claimed_slot_pod(&self, slot: &PoolSlot) -> Result<bool, WarmPoolError> {
        Self::reap_stale_claimed_slot(
            || async {
                self.timed_postgres(
                    "delete-claimed-slot-recheck",
                    self.store
                        .stale_unbound_claimed_slot_exists(slot.id, self.claimed_slot_timeout),
                )
                .await
            },
            || async {
                let pods: Api<Pod> = Api::namespaced(self.kube.clone(), &slot.pod_namespace);
                match self
                    .timed_k8s(
                        "delete-claimed-slot-pod",
                        pods.delete(&slot.pod_name, &DeleteParams::default()),
                    )
                    .await
                {
                    Ok(_) => Ok(()),
                    Err(err) if is_not_found_error(&err) => Ok(()),
                    Err(err) => Err(err.into()),
                }
            },
            || async {
                let mut tx = self
                    .timed_postgres("delete-claimed-slot-txn-begin", self.store.pool().begin())
                    .await?;
                if !self
                    .timed_postgres(
                        "delete-claimed-slot-lock",
                        WarmPoolStore::lock_stale_unbound_claimed_slot_for_delete(
                            &mut tx,
                            slot.id,
                            self.claimed_slot_timeout,
                        ),
                    )
                    .await?
                {
                    return Ok(false);
                }
                self.timed_postgres(
                    "delete-claimed-slot-row",
                    WarmPoolStore::delete_locked_claimed_slot(&mut tx, slot.id),
                )
                .await?;
                self.timed_postgres("delete-claimed-slot-txn-commit", tx.commit())
                    .await?;
                Ok(true)
            },
        )
        .await
    }

    async fn delete_cleanup_pending_slot(&self, slot: &PoolSlot) -> Result<bool, WarmPoolError> {
        let sandbox_id = slot.reserved_sandbox_id;
        self.timed_grpc_substrate(
            "cleanup-pending-delete-execution",
            self.substrate.delete_execution(sandbox_id),
        )
        .await?;

        let mut tx = self
            .timed_postgres("cleanup-pending-txn-begin", self.store.pool().begin())
            .await?;
        if !self
            .timed_postgres(
                "cleanup-pending-lock-slot",
                WarmPoolStore::lock_cleanup_pending_slot_for_delete(&mut tx, slot.id),
            )
            .await?
        {
            return Ok(false);
        }
        self.timed_postgres(
            "cleanup-pending-finalize-sandbox-delete",
            finalize_sandbox_db_delete_tx(&mut tx, slot.reserved_sandbox_id),
        )
        .await?;
        self.timed_postgres(
            "cleanup-pending-delete-slot-row",
            WarmPoolStore::delete_locked_cleanup_pending_slot(&mut tx, slot.id),
        )
        .await?;
        self.timed_postgres("cleanup-pending-txn-commit", tx.commit())
            .await?;

        tracing::info!(
            sandbox_id = %sandbox_id,
            pool_slot_id = %slot.id,
            pod_name = %slot.pod_name,
            pod_namespace = %slot.pod_namespace,
            "warm pool: deleted cleanup-pending claimed slot"
        );

        Ok(true)
    }

    async fn create_warming_slot(
        &self,
        config: &PoolConfig,
        pods: &Api<Pod>,
        node_group_placement: Option<&NodeGroupPlacement>,
    ) -> Result<PoolSlot, WarmPoolError> {
        with_create_permit(&self.create_permits, async {
            let reserved_sandbox_id = SandboxId::new();
            let pod_name = pool_pod_name(reserved_sandbox_id);
            let slot = self
                .timed_postgres(
                    "insert-warming-slot",
                    self.store.insert_warming_slot(NewPoolSlot {
                        config_id: config.id,
                        template_release_id: config.template_release_id,
                        reserved_sandbox_id,
                        template_name: &config.template_name,
                        content_hash: config.content_hash,
                        vcpu: config.vcpu,
                        memory: config.memory,
                        pod_name: &pod_name,
                        pod_namespace: &self.namespace,
                    }),
                )
                .await?;

            let pod = build_pool_pod(&self.pod_config, &slot, node_group_placement);
            if let Err(err) = self
                .timed_k8s(
                    "create-warming-slot-pod",
                    pods.create(&PostParams::default(), &pod),
                )
                .await
            {
                if should_cleanup_warming_slot_after_create_error(&err) {
                    self.timed_postgres(
                        "create-warming-slot-cleanup-row",
                        self.store.delete_open_slot(slot.id),
                    )
                    .await?;
                }
                return Err(err.into());
            }

            Ok(slot)
        })
        .await
    }

    async fn ensure_template_write_annotations(
        &self,
        config: &PoolConfig,
    ) -> Result<TemplateWriteAnnotationSummary, WarmPoolError> {
        let nodes: Api<Node> = Api::all(self.kube.clone());
        let list_params = template_write_node_list_params(&self.pod_config);
        let node_list = nodes.list(&list_params);
        let node_list = self
            .timed_k8s("template-write-node-list", node_list)
            .await?;
        let request = template_write_request(config);
        let annotation_key = template_write_annotation_key_for(&request);
        let annotation_value = request.to_string();
        let presence_label = template_build_presence_label_key(
            config.template_name.as_str(),
            &config.content_hash.to_string(),
        );
        let mut patched = 0;
        let mut failed = 0;

        for node in node_list {
            let Some(node_name) = node.metadata.name.as_deref() else {
                continue;
            };
            if !needs_template_write_annotation(
                &node,
                &self.pod_config,
                &presence_label,
                &annotation_key,
                &annotation_value,
            ) {
                continue;
            }

            match self
                .timed_k8s(
                    "template-write-node-patch",
                    nodes.patch(
                        node_name,
                        &template_write_annotation_patch_params(),
                        &Patch::Merge(template_write_annotation_patch(
                            &annotation_key,
                            &annotation_value,
                        )),
                    ),
                )
                .await
            {
                Ok(_) => patched += 1,
                Err(err) => {
                    failed += 1;
                    tracing::warn!(
                        node = %node_name,
                        annotation = %annotation_key,
                        template = %config.template_name,
                        template_release_id = %config.template_release_id,
                        error = %err,
                        "template write annotation patch failed"
                    );
                }
            }
        }

        Ok(TemplateWriteAnnotationSummary { patched, failed })
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TemplateWriteAnnotationSummary {
    patched: usize,
    failed: usize,
}

fn template_write_annotation_result(
    config: &PoolConfig,
    result: Result<TemplateWriteAnnotationSummary, WarmPoolError>,
) -> ReconcileSummary {
    match result {
        Ok(annotation_summary) => ReconcileSummary {
            template_write_annotations: annotation_summary.patched,
            failed_required_operations: annotation_summary.failed,
            ..Default::default()
        },
        Err(err) => {
            tracing::warn!(
                config_id = %config.id,
                template_release_id = %config.template_release_id,
                template = %config.template_name,
                error = %err,
                "failed to ensure warm pool template write annotations"
            );
            ReconcileSummary {
                failed_required_operations: 1,
                ..Default::default()
            }
        }
    }
}

fn create_warming_slot_result<T>(
    config: &PoolConfig,
    results: Vec<Result<T, WarmPoolError>>,
) -> ReconcileSummary {
    let mut summary = ReconcileSummary::default();
    for result in results {
        match result {
            Ok(_) => summary.created_slots += 1,
            Err(err) => {
                summary.failed_required_operations += 1;
                tracing::warn!(
                    config_id = %config.id,
                    template_release_id = %config.template_release_id,
                    template = %config.template_name,
                    error = %err,
                    "warm pool: failed to create warming slot; continuing with remaining slots"
                );
            }
        }
    }
    summary
}

impl ReconcileSummary {
    fn completed_required_operations(&self) -> bool {
        self.failed_required_operations == 0
    }

    /// Single source of truth mapping delete-counter fields to their
    /// [`SlotDeleteReason`]: both the `deleted_slots` log field and the
    /// per-reason metric emission consume this. The exhaustive destructure
    /// forces a decision here when a `ReconcileSummary` field is added, and
    /// the fixed array length forces the new delete reason into every
    /// consumer at once.
    fn delete_counts(&self) -> [(SlotDeleteReason, usize); 9] {
        let Self {
            enabled_pool_configs: _,
            template_write_annotations: _,
            created_slots: _,
            marked_ready_slots: _,
            deleted_missing_pods,
            deleted_unready_pods,
            deleted_excess_slots,
            deleted_mismatched_slots,
            deleted_stale_claimed_slots,
            deleted_dead_claimed_slots,
            deleted_orphaned_receipts,
            deleted_cleanup_pending_slots,
            deleted_cordoned_slots,
            reconciled_slot_counts: _,
            failed_required_operations: _,
        } = *self;
        [
            (SlotDeleteReason::MissingPod, deleted_missing_pods),
            (SlotDeleteReason::UnreadyPod, deleted_unready_pods),
            (SlotDeleteReason::Excess, deleted_excess_slots),
            (SlotDeleteReason::Mismatched, deleted_mismatched_slots),
            (SlotDeleteReason::StaleClaimed, deleted_stale_claimed_slots),
            (SlotDeleteReason::DeadClaimed, deleted_dead_claimed_slots),
            (SlotDeleteReason::OrphanedReceipt, deleted_orphaned_receipts),
            (
                SlotDeleteReason::CleanupPending,
                deleted_cleanup_pending_slots,
            ),
            (SlotDeleteReason::Cordoned, deleted_cordoned_slots),
        ]
    }

    fn deleted_slots(&self) -> usize {
        self.delete_counts().iter().map(|(_, count)| count).sum()
    }

    fn emit_metrics(&self) {
        pplx_space_metrics::warm_pool::reconcile_summary(
            self.created_slots,
            self.marked_ready_slots,
        );
        for (reason, count) in self.delete_counts() {
            pplx_space_metrics::warm_pool::slots_deleted(reason, count);
        }
        pplx_space_metrics::warm_pool::slots_evicted_cordoned(self.deleted_cordoned_slots);
        pplx_space_metrics::warm_pool::dead_claimed_slots_drained(self.deleted_dead_claimed_slots);
    }
}

impl std::ops::AddAssign for ReconcileSummary {
    fn add_assign(&mut self, rhs: Self) {
        self.enabled_pool_configs += rhs.enabled_pool_configs;
        self.template_write_annotations += rhs.template_write_annotations;
        self.created_slots += rhs.created_slots;
        self.marked_ready_slots += rhs.marked_ready_slots;
        self.deleted_missing_pods += rhs.deleted_missing_pods;
        self.deleted_unready_pods += rhs.deleted_unready_pods;
        self.deleted_excess_slots += rhs.deleted_excess_slots;
        self.deleted_mismatched_slots += rhs.deleted_mismatched_slots;
        self.deleted_stale_claimed_slots += rhs.deleted_stale_claimed_slots;
        self.deleted_dead_claimed_slots += rhs.deleted_dead_claimed_slots;
        self.deleted_orphaned_receipts += rhs.deleted_orphaned_receipts;
        self.deleted_cleanup_pending_slots += rhs.deleted_cleanup_pending_slots;
        self.deleted_cordoned_slots += rhs.deleted_cordoned_slots;
        self.reconciled_slot_counts = rhs.reconciled_slot_counts.or(self.reconciled_slot_counts);
        self.failed_required_operations += rhs.failed_required_operations;
    }
}

fn template_write_request(config: &PoolConfig) -> TemplateReference {
    TemplateReference::with_artifact_digest(
        config.template_name.clone(),
        TemplateArtifactDigest::from(config.content_hash),
    )
}

fn template_write_node_list_params(config: &SandboxPodConfigValues) -> ListParams {
    ListParams::default().labels(&format!(
        "{}={}",
        config.sandbox_node_selector_key, config.sandbox_node_selector_value
    ))
}

fn cached_warm_pods_by_name(
    pod_store: &Store<Pod>,
    config: &PoolConfig,
) -> HashMap<String, Arc<Pod>> {
    let config_id = config.id.to_string();
    pod_store
        .state()
        .into_iter()
        .filter(|pod| warm_pool_pod_matches_config(pod, &config_id))
        .filter_map(|pod| pod.metadata.name.clone().map(|name| (name, pod)))
        .collect()
}

fn warm_pool_pod_matches_config(pod: &Pod, config_id: &str) -> bool {
    let labels = pod.metadata.labels.as_ref();
    labels
        .and_then(|labels| labels.get(SANDBOX_POOL_ROLE_KEY))
        .map(String::as_str)
        == Some(SANDBOX_POOL_ROLE_WARM)
        && labels
            .and_then(|labels| labels.get(SANDBOX_POOL_CONFIG_ID_KEY))
            .map(String::as_str)
            == Some(config_id)
}

fn needs_template_write_annotation(
    node: &Node,
    config: &SandboxPodConfigValues,
    presence_label: &str,
    annotation_key: &str,
    annotation_value: &str,
) -> bool {
    let labels = node.metadata.labels.as_ref();
    if labels
        .and_then(|labels| labels.get(&config.sandbox_node_selector_key))
        .map(String::as_str)
        != Some(config.sandbox_node_selector_value.as_str())
    {
        return false;
    }
    if labels
        .and_then(|labels| labels.get(presence_label))
        .map(String::as_str)
        == Some(TEMPLATE_PRESENT_LABEL_VALUE)
    {
        return false;
    }

    node.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(annotation_key))
        .map(String::as_str)
        != Some(annotation_value)
}

fn template_write_annotation_patch(
    annotation_key: &str,
    annotation_value: &str,
) -> serde_json::Value {
    let annotations = std::collections::BTreeMap::from([(
        annotation_key.to_string(),
        annotation_value.to_string(),
    )]);
    serde_json::json!({
        "metadata": {
            "annotations": annotations,
        },
    })
}

fn template_write_annotation_patch_params() -> PatchParams {
    PatchParams::default()
}

/// Run up to `create_count` warm-slot creations produced by `make`, keeping
/// at most `concurrency` in flight at a time, and return every result in
/// completion order. Errors are returned rather than short-circuited so the
/// caller can count successes and log failures without aborting the tick.
async fn create_warming_slots<F, Fut, T>(
    create_count: usize,
    concurrency: usize,
    make: F,
) -> Vec<Result<T, WarmPoolError>>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T, WarmPoolError>>,
{
    stream::iter(0..create_count)
        .map(|_| make())
        .buffer_unordered(concurrency.max(1))
        .collect()
        .await
}

async fn reconcile_configs<F, Fut>(
    configs: Vec<PoolConfig>,
    concurrency: usize,
    reconcile_config: F,
) -> Vec<ConfigReconcileResult>
where
    F: Fn(PoolConfig) -> Fut,
    Fut: Future<Output = Result<ReconcileSummary, WarmPoolError>>,
{
    stream::iter(configs)
        .map(|config| {
            let result_config = config.clone();
            let future = reconcile_config(config);
            async move {
                let result = match AssertUnwindSafe(future).catch_unwind().await {
                    Ok(result) => result,
                    Err(panic) => Err(WarmPoolError::WorkerPanicked(panic_payload_message(&panic))),
                };

                ConfigReconcileResult {
                    config: result_config,
                    result,
                }
            }
        })
        .buffer_unordered(reconcile_config_concurrency(concurrency))
        .collect()
        .await
}

struct ConfigReconcileResult {
    config: PoolConfig,
    result: Result<ReconcileSummary, WarmPoolError>,
}

impl ConfigReconcileResult {
    fn succeeded(&self) -> bool {
        self.result
            .as_ref()
            .is_ok_and(ReconcileSummary::completed_required_operations)
    }

    fn slot_counts(&self) -> Option<PoolSlotCounts> {
        self.result
            .as_ref()
            .ok()
            .and_then(|summary| summary.reconciled_slot_counts)
    }

    fn into_parts(self) -> (PoolConfig, Result<ReconcileSummary, WarmPoolError>) {
        (self.config, self.result)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConfigInventoryState {
    template_release_id: Uuid,
    template_name: String,
    content_hash: String,
    vcpu: u32,
    memory_mib: u64,
    target_ready_slots: i32,
    refill_per_tick: i32,
    max_warming_slots: i32,
    namespace: String,
    enabled: bool,
    managed_by: pplx_space_cluster_pool::PoolConfigOwner,
}

impl ConfigInventoryState {
    fn from_config(config: &PoolConfig) -> Self {
        Self {
            template_release_id: config.template_release_id,
            template_name: config.template_name.to_string(),
            content_hash: config.content_hash.to_string(),
            vcpu: config.vcpu.get().get(),
            memory_mib: config.memory.as_mib_floor(),
            target_ready_slots: config.target_ready_slots,
            refill_per_tick: config.refill_per_tick,
            max_warming_slots: config.max_warming_slots,
            namespace: config.namespace.clone(),
            enabled: config.enabled,
            managed_by: config.managed_by,
        }
    }
}

struct ConfigInventoryPublication {
    config: ConfigInventoryState,
    succeeded: bool,
    emitted_at: Instant,
}

fn panic_payload_message(payload: &(dyn Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "unknown panic payload".to_string())
}

fn slots_to_create(config: &PoolConfig, warming_slots: i64, open_slots: i64) -> i64 {
    if !config.enabled {
        return 0;
    }

    let target = target_open_slots(config);
    let refill = i64::from(config.refill_per_tick).max(0);
    let max_warming = i64::from(config.max_warming_slots).max(0);
    let open_deficit = target.saturating_sub(open_slots);
    let warming_budget = max_warming.saturating_sub(warming_slots);

    open_deficit.min(refill).min(warming_budget)
}

fn slot_matches_config(slot: &PoolSlot, config: &PoolConfig, namespace: &str) -> bool {
    slot.template_release_id == config.template_release_id
        && slot.vcpu == config.vcpu
        && slot.memory == config.memory
        && slot.disk_mib == config.disk_mib
        && slot.disk_io_limits == config.disk_io_limits
        && slot.pod_namespace == namespace
}

#[derive(Debug, PartialEq, Eq)]
enum ClaimableSlotMetadata {
    Ready(crate::pod::ReadyPodMetadata),
    PodNotReady,
    ProbeFailed(crate::pod::ReadyPodMetadata),
}

async fn claimable_slot_metadata<P>(
    slot: &PoolSlot,
    pod: &Pod,
    ready_probe: &P,
    content_hash: &str,
) -> ClaimableSlotMetadata
where
    P: WarmSlotReadyProbe + Sync,
{
    let Some(metadata) = ready_pod_metadata(pod) else {
        return ClaimableSlotMetadata::PodNotReady;
    };
    if ready_probe
        .slot_started(slot, &metadata, content_hash)
        .await
    {
        return ClaimableSlotMetadata::Ready(metadata);
    }
    ClaimableSlotMetadata::ProbeFailed(metadata)
}

fn emit_promote_probe(content_hash: &str, outcome: ProbeOutcome, started_at: Instant) {
    pplx_space_metrics::warm_pool::promote_probe_duration(content_hash, outcome, started_at);
    pplx_space_metrics::warm_pool::promote_probe(content_hash, outcome);
}

fn emit_promote_probe_step(
    content_hash: &str,
    step: ProbeStep,
    outcome: ProbeStepOutcome,
    started_at: Instant,
) {
    pplx_space_metrics::warm_pool::promote_probe_step_duration(
        content_hash,
        step,
        outcome,
        started_at,
    );
    pplx_space_metrics::warm_pool::promote_probe_step(content_hash, step, outcome);
}

fn emit_promote_attempt(content_hash: &str, outcome: PromoteOutcome) {
    pplx_space_metrics::warm_pool::promote_attempt(content_hash, outcome);
}

fn pod_ip_for_log(pod: &Pod) -> &str {
    pod.status
        .as_ref()
        .and_then(|status| status.pod_ip.as_deref())
        .unwrap_or("<none>")
}

#[derive(Debug, thiserror::Error)]
enum WarmSlotRuntimeEndpointError {
    #[error("invalid warm slot pod IP {pod_ip}: {source}")]
    InvalidIp {
        pod_ip: String,
        #[source]
        source: std::net::AddrParseError,
    },
    #[error("invalid warm slot runtime endpoint: {0}")]
    InvalidEndpoint(#[source] tonic::transport::Error),
}

fn warm_slot_runtime_endpoint(
    pod_ip: &str,
    port: u16,
    connect_timeout: Duration,
) -> Result<Endpoint, WarmSlotRuntimeEndpointError> {
    let ip =
        pod_ip
            .parse::<IpAddr>()
            .map_err(|source| WarmSlotRuntimeEndpointError::InvalidIp {
                pod_ip: pod_ip.to_owned(),
                source,
            })?;
    let addr = SocketAddr::new(ip, port);
    Endpoint::from_shared(
        pplx_space_common_grpc::internal_transport::qualify_endpoint(&addr.to_string()),
    )
    .map(|endpoint| {
        endpoint
            .tcp_nodelay(true)
            .connect_timeout(connect_timeout)
            .timeout(WARM_SLOT_STARTED_PROBE_TIMEOUT)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .keep_alive_timeout(Duration::from_secs(60))
    })
    .map_err(WarmSlotRuntimeEndpointError::InvalidEndpoint)
}

fn ready_slot_matches_pod(slot: &PoolSlot, pod: &Pod) -> bool {
    ready_pod_metadata(pod).is_some_and(|metadata| {
        slot.pod_uid.as_deref() == Some(metadata.uid.as_str())
            && slot.pod_ip.as_deref() == Some(metadata.pod_ip.as_str())
            && slot.k8s_node_name.as_deref() == Some(metadata.node_name.as_str())
    })
}

fn target_open_slots(config: &PoolConfig) -> i64 {
    // An encryptable pool must stay memberless until provisioning carries the
    // encryptable member shape: filling it would strand plaintext members that
    // only encrypted creates could claim. Target zero even if a row is enabled
    // out of band; the drain path still reclaims any slots it acquired.
    if config.enabled && !config.encryptable {
        i64::from(config.target_ready_slots).max(0)
    } else {
        0
    }
}

fn counts_after_create(counts: PoolSlotCounts, created_slots: usize) -> PoolSlotCounts {
    PoolSlotCounts {
        warming: counts
            .warming
            .saturating_add(i64::try_from(created_slots).unwrap_or(i64::MAX)),
        ready: counts.ready,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReconcileConfigState {
    active: bool,
    succeeded: bool,
}

fn reconcile_config_state(config: &PoolConfig, succeeded: bool) -> ReconcileConfigState {
    ReconcileConfigState {
        active: config.enabled,
        succeeded: config.enabled && succeeded,
    }
}

fn emit_reconcile_config_states(config: &PoolConfig, succeeded: bool) {
    let template = config.template_name.as_str();
    let content_hash = config.content_hash.to_string();
    let vcpu = config.vcpu.get().get();
    let memory_mib = config.memory.as_mib_floor();
    let state = reconcile_config_state(config, succeeded);

    pplx_space_metrics::warm_pool::reconcile_config_state(
        template,
        &content_hash,
        vcpu,
        memory_mib,
        config.managed_by.as_str(),
        "active",
        state.active,
    );
    pplx_space_metrics::warm_pool::reconcile_config_state(
        template,
        &content_hash,
        vcpu,
        memory_mib,
        config.managed_by.as_str(),
        "succeeded",
        state.succeeded,
    );
}

fn retired_configs_to_emit(
    deleted_configs: Vec<PoolConfig>,
    recently_disabled_configs: Vec<PoolConfig>,
    reconcilable_config_ids: &HashSet<Uuid>,
) -> Vec<PoolConfig> {
    deleted_configs
        .into_iter()
        .chain(
            recently_disabled_configs
                .into_iter()
                .filter(|config| !reconcilable_config_ids.contains(&config.id)),
        )
        .collect()
}

fn emit_slot_gauges(config: &PoolConfig, counts: PoolSlotCounts) {
    let template = config.template_name.as_str();
    let content_hash = config.content_hash.to_string();
    let vcpu = config.vcpu.get().get();
    let memory_mib = config.memory.as_mib_floor();

    pplx_space_metrics::warm_pool::slots(
        template,
        &content_hash,
        vcpu,
        memory_mib,
        config.managed_by.as_str(),
        "ready",
        counts.ready,
    );
    pplx_space_metrics::warm_pool::slots(
        template,
        &content_hash,
        vcpu,
        memory_mib,
        config.managed_by.as_str(),
        "warming",
        counts.warming,
    );
    pplx_space_metrics::warm_pool::slots(
        template,
        &content_hash,
        vcpu,
        memory_mib,
        config.managed_by.as_str(),
        "desired",
        // Reuse the reconciler's effective target so a config it refuses to
        // fill (disabled, encryptable) also reports zero desired capacity.
        target_open_slots(config),
    );
}

fn emit_config_inventory(config: &PoolConfig, counts: Option<PoolSlotCounts>, succeeded: bool) {
    let (warming_slots, ready_slots) = counts
        .map(|counts| (Some(counts.warming), Some(counts.ready)))
        .unwrap_or((None, None));

    tracing::info!(
        config_id = %config.id,
        template_release_id = %config.template_release_id,
        template = %config.template_name,
        content_hash = %config.content_hash,
        pool_vcpu = config.vcpu.get().get(),
        pool_memory_mib = config.memory.as_mib_floor(),
        target_ready_slots = config.target_ready_slots,
        desired_ready_slots = target_open_slots(config),
        refill_per_tick = config.refill_per_tick,
        max_warming_slots = config.max_warming_slots,
        namespace = %config.namespace,
        enabled = config.enabled,
        managed_by = %config.managed_by,
        warming_slots,
        ready_slots,
        config_created_at = ?config.created_at,
        config_updated_at = ?config.updated_at,
        reconcile_succeeded = succeeded,
        "warm pool: config inventory"
    );
}

fn is_not_found(err: &KubeError) -> bool {
    matches!(err, KubeError::Api(response) if response.code == 404)
}

fn is_not_found_error(err: &TimedIoError<KubeError>) -> bool {
    matches!(err, TimedIoError::Operation(error) if is_not_found(error))
}

fn should_cleanup_warming_slot_after_create_error(err: &TimedIoError<KubeError>) -> bool {
    !matches!(err, TimedIoError::Timeout(_))
}

fn cached_cordoned_node_names(node_store: &Store<Node>) -> HashSet<String> {
    node_store
        .state()
        .into_iter()
        .filter(|node| node_is_cordoned(node.as_ref()))
        .filter_map(|node| node.metadata.name.clone())
        .collect()
}

/// A Node counts as cordoned when `spec.unschedulable` is explicitly `true`.
/// A missing `spec` or absent/`false` `unschedulable` field means
/// schedulable — never evict warm capacity off it.
fn node_is_cordoned(node: &Node) -> bool {
    node.spec
        .as_ref()
        .and_then(|spec| spec.unschedulable)
        .unwrap_or(false)
}

fn should_delete_unready_warming_pod(pod: &Pod, slot: &PoolSlot, timeout: Duration) -> bool {
    if matches!(pod_phase(pod), Some("Failed" | "Succeeded")) {
        return true;
    }

    warming_slot_timed_out(slot.created_at, timeout)
}

fn pod_phase(pod: &Pod) -> Option<&str> {
    pod.status.as_ref()?.phase.as_deref()
}

fn warming_slot_timed_out(created_at: OffsetDateTime, timeout: Duration) -> bool {
    let Ok(timeout) = TimeDuration::try_from(timeout) else {
        return false;
    };

    created_at + timeout <= OffsetDateTime::now_utc()
}

#[cfg(test)]
mod tests;
