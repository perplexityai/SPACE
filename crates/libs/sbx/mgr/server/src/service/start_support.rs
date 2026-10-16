use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::*;
use pplx_space_common_core::env::valid_positive_f64;
use pplx_space_metrics::sandbox::admission::{
    AdmissionDecision, AdmissionOperationClass, AdmissionPermitOutcome, AdmissionQueueOutcome,
    AdmissionRejectReason,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const DEFAULT_MAX_CONCURRENT_ACTIVATIONS: usize = 5;
const DEFAULT_REFILL_RPS: f64 = 5.0;
const DEFAULT_BURST: f64 = 10.0;
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const CONCURRENCY_RETRY_AFTER: Duration = Duration::from_millis(200);
const MIN_CONSUMING_BURST: f64 = 1.0;
const MAX_CONCURRENT_ENV: &str = "SPACE_SBXMGR_ACTIVATION_ADMISSION_MAX_CONCURRENT";

type ActivationCleanup = Box<
    dyn FnOnce(
            SandboxRuntimeLabel,
            Option<ActivationAdmissionPermit>,
            SandboxQuarantineReasonCode,
            String,
        ) + Send
        + 'static,
>;

pub(super) struct ActivationCleanupGuard {
    runtime_label: SandboxRuntimeLabel,
    cleanup: Option<ActivationCleanup>,
    permit: Option<ActivationAdmissionPermit>,
    interrupted_reason_code: SandboxQuarantineReasonCode,
    interrupted_reason: &'static str,
    failure_reason: Option<String>,
}

impl ActivationCleanupGuard {
    pub(super) fn with_interruption_reason(
        runtime_label: SandboxRuntimeLabel,
        interrupted_reason_code: SandboxQuarantineReasonCode,
        interrupted_reason: &'static str,
        cleanup: impl FnOnce(
            SandboxRuntimeLabel,
            Option<ActivationAdmissionPermit>,
            SandboxQuarantineReasonCode,
            String,
        ) + Send
        + 'static,
    ) -> Self {
        Self {
            runtime_label,
            cleanup: Some(Box::new(cleanup)),
            permit: None,
            interrupted_reason_code,
            interrupted_reason,
            failure_reason: None,
        }
    }

    pub(super) fn update_runtime_label(&mut self, runtime_label: SandboxRuntimeLabel) {
        self.runtime_label = runtime_label;
    }

    pub(super) fn admit(&mut self, permit: ActivationAdmissionPermit) {
        self.permit = Some(permit);
    }

    pub(super) fn cleanup(mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup(
                self.runtime_label.clone(),
                self.permit.take(),
                self.interrupted_reason_code,
                self.interrupted_reason.to_string(),
            );
        }
    }

    pub(super) fn cleanup_after_failure(
        mut self,
        reason_code: SandboxQuarantineReasonCode,
        reason: impl Into<String>,
    ) {
        self.mark_permit_failed();
        if let Some(cleanup) = self.cleanup.take() {
            cleanup(
                self.runtime_label.clone(),
                self.permit.take(),
                reason_code,
                reason.into(),
            );
        }
    }

    pub(super) fn finish_admission(&mut self, outcome: AdmissionPermitOutcome) {
        if let Some(permit) = self.permit.take() {
            permit.finish(outcome);
        }
    }

    pub(super) fn failure(&mut self, error: Status) -> Status {
        self.record_failure(&error);
        error
    }

    pub(super) fn record_failure(&mut self, error: &Status) {
        self.mark_permit_failed();
        self.failure_reason
            .get_or_insert_with(|| error.message().to_string());
    }

    pub(super) fn mark_permit_failed(&mut self) {
        if let Some(permit) = self.permit.as_mut() {
            permit.outcome = AdmissionPermitOutcome::Failed;
        }
    }

    pub(super) fn complete_success(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.finish(AdmissionPermitOutcome::Succeeded);
        }
        self.cleanup.take();
    }

    pub(super) fn complete_handled_failure(mut self) {
        if let Some(permit) = self.permit.take() {
            permit.finish(AdmissionPermitOutcome::Failed);
        }
        self.cleanup.take();
    }
}

impl Drop for ActivationCleanupGuard {
    fn drop(&mut self) {
        // Persist cleanup before Rust drops the owned permit and admits another
        // activation onto a node that still has this activation's runtime.
        if let Some(cleanup) = self.cleanup.take() {
            let reason = self
                .failure_reason
                .take()
                .unwrap_or_else(|| self.interrupted_reason.to_string());
            cleanup(
                self.runtime_label.clone(),
                self.permit.take(),
                self.interrupted_reason_code,
                reason,
            );
        }
    }
}

pub(super) fn persist_allocated_runtime_label(
    db: &crate::db::SharedConnection,
    sandbox_id: &str,
    current: &mut SandboxRuntimeLabel,
    cleanup: &mut ActivationCleanupGuard,
    allocated: String,
) -> Result<(), Status> {
    if allocated.is_empty() || allocated == current.as_str() {
        return Ok(());
    }
    let allocated = SandboxRuntimeLabel::parse(allocated)
        .map_err(|error| Status::internal(format!("invalid node-manager slot label: {error}")))?;
    cleanup.update_runtime_label(allocated.clone());
    let db = db
        .lock()
        .map_err(|_| Status::internal("sandbox database mutex poisoned"))?;
    crate::db::update_uuid8(&db, sandbox_id, &allocated)
        .map_err(|error| Status::internal(format!("db error: {error}")))?;
    *current = allocated;
    Ok(())
}

pub(super) const START_TEMPLATE_RESOLUTION_TIMEOUT: Duration = Duration::from_secs(10);
pub(super) const START_NODE_METADATA_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) struct ActivationAdmissionConfig {
    pub(super) max_concurrent_activations: usize,
    pub(super) refill_rps: f64,
    pub(super) burst: f64,
}

impl Default for ActivationAdmissionConfig {
    fn default() -> Self {
        Self {
            max_concurrent_activations: DEFAULT_MAX_CONCURRENT_ACTIVATIONS,
            refill_rps: DEFAULT_REFILL_RPS,
            burst: DEFAULT_BURST,
        }
    }
}

impl ActivationAdmissionConfig {
    pub(super) fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            max_concurrent_activations: parse_positive_usize_env(
                MAX_CONCURRENT_ENV,
                defaults.max_concurrent_activations,
            ),
            refill_rps: defaults.refill_rps,
            burst: defaults.burst,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxStartPath {
    FreshCold,
    WarmSnapshot,
    CheckpointRestore,
}

impl SandboxStartPath {
    pub(super) fn from_request(req: &StartRequest) -> Self {
        if req.checkpoint.as_deref().unwrap_or("").is_empty() {
            if req.from_warm {
                Self::WarmSnapshot
            } else {
                Self::FreshCold
            }
        } else {
            Self::CheckpointRestore
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::FreshCold => "fresh_cold",
            Self::WarmSnapshot => "warm_snapshot",
            Self::CheckpointRestore => "checkpoint_restore",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SandboxStartExecutionPath {
    FreshCold,
    WarmSnapshot,
    CheckpointRestore,
    CheckpointReattach,
}

impl SandboxStartExecutionPath {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::FreshCold => "fresh_cold",
            Self::WarmSnapshot => "warm_snapshot",
            Self::CheckpointRestore => "checkpoint_restore",
            Self::CheckpointReattach => "checkpoint_reattach",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ActivationAdmissionKind {
    Cold,
    Restore,
}

impl ActivationAdmissionKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cold => "cold_activation",
            Self::Restore => "restore_activation",
        }
    }

    fn stage(self) -> &'static str {
        match self {
            Self::Cold => "cold_activation.admission_wait",
            Self::Restore => "restore_activation.admission_wait",
        }
    }

    const fn metric_class(self) -> AdmissionOperationClass {
        match self {
            Self::Cold => AdmissionOperationClass::ColdActivation,
            Self::Restore => AdmissionOperationClass::RestoreActivation,
        }
    }
}

#[derive(Debug)]
struct ActivationInFlight {
    cold: AtomicUsize,
    restore: AtomicUsize,
}

impl ActivationInFlight {
    fn counter(&self, kind: ActivationAdmissionKind) -> &AtomicUsize {
        match kind {
            ActivationAdmissionKind::Cold => &self.cold,
            ActivationAdmissionKind::Restore => &self.restore,
        }
    }
}

#[derive(Debug)]
struct TokenBucket {
    tokens: f64,
    updated_at: Instant,
}

#[derive(Debug)]
pub(super) struct ActivationAdmissionLimiter {
    config: ActivationAdmissionConfig,
    semaphore: Arc<Semaphore>,
    in_flight: Arc<AtomicUsize>,
    in_flight_by_kind: Arc<ActivationInFlight>,
    bucket: std::sync::Mutex<TokenBucket>,
}

struct ActivationAdmissionRejection {
    status: Status,
    reason: AdmissionRejectReason,
}

struct AdmissionQueueMetricGuard {
    kind: ActivationAdmissionKind,
    started_at: Instant,
    outcome: AdmissionQueueOutcome,
    reason: AdmissionRejectReason,
}

impl AdmissionQueueMetricGuard {
    fn new(kind: ActivationAdmissionKind, started_at: Instant) -> Self {
        Self {
            kind,
            started_at,
            outcome: AdmissionQueueOutcome::Cancelled,
            reason: AdmissionRejectReason::None,
        }
    }

    fn finish(mut self, outcome: AdmissionQueueOutcome, reason: AdmissionRejectReason) -> f64 {
        self.outcome = outcome;
        self.reason = reason;
        elapsed_ms(self.started_at)
    }
}

impl Drop for AdmissionQueueMetricGuard {
    fn drop(&mut self) {
        pplx_space_metrics::sandbox::admission::node_queue_wait(
            self.kind.metric_class(),
            self.outcome,
            self.reason,
            elapsed_ms(self.started_at),
        );
    }
}

impl Default for ActivationAdmissionLimiter {
    fn default() -> Self {
        Self::from_env()
    }
}

impl ActivationAdmissionLimiter {
    pub(super) fn from_env() -> Self {
        Self::with_config(ActivationAdmissionConfig::from_env())
    }

    pub(super) fn with_config(config: ActivationAdmissionConfig) -> Self {
        let config = sanitize_config(config);
        pplx_space_metrics::sandbox::admission::node_refill_rps(config.refill_rps);
        pplx_space_metrics::sandbox::admission::node_max_concurrent(
            config.max_concurrent_activations,
        );
        Self {
            config,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_activations)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            in_flight_by_kind: Arc::new(ActivationInFlight {
                cold: AtomicUsize::new(0),
                restore: AtomicUsize::new(0),
            }),
            bucket: std::sync::Mutex::new(TokenBucket {
                tokens: config.burst,
                updated_at: Instant::now(),
            }),
        }
    }

    pub(super) fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub(super) fn in_flight_for(&self, kind: ActivationAdmissionKind) -> usize {
        self.in_flight_by_kind.counter(kind).load(Ordering::SeqCst)
    }

    pub(super) async fn acquire(
        &self,
        kind: ActivationAdmissionKind,
        sandbox_id: &str,
    ) -> Result<ActivationAdmissionPermit, Status> {
        self.acquire_with_timeout(kind, sandbox_id, DEFAULT_WAIT_TIMEOUT)
            .await
    }

    async fn acquire_with_timeout(
        &self,
        kind: ActivationAdmissionKind,
        sandbox_id: &str,
        wait_timeout: Duration,
    ) -> Result<ActivationAdmissionPermit, Status> {
        pplx_space_metrics::sandbox::admission::node_refill_rps(self.config.refill_rps);
        pplx_space_metrics::sandbox::admission::node_max_concurrent(
            self.config.max_concurrent_activations,
        );
        let queued_at = Instant::now();
        let queue_metric = AdmissionQueueMetricGuard::new(kind, queued_at);
        let span = tracing::info_span!(
            "sbxmgr.activation_admission.wait",
            sandbox_id,
            stage = kind.stage(),
            admission_kind = kind.as_str(),
            max_concurrent_activations = self.config.max_concurrent_activations as u64,
            refill_rps = self.config.refill_rps,
            burst = self.config.burst,
            queue_ms = tracing::field::Empty,
        );
        loop {
            match self.try_acquire_inner(kind) {
                Ok(permit) => {
                    let queue_ms = queue_metric
                        .finish(AdmissionQueueOutcome::Accepted, AdmissionRejectReason::None);
                    span.record("queue_ms", queue_ms);
                    tracing::debug!(
                        sandbox_id,
                        admission_kind = kind.as_str(),
                        queue_ms,
                        "acquired sbxmgr activation admission permit"
                    );
                    return Ok(permit);
                }
                Err(rejection) => {
                    let elapsed = queued_at.elapsed();
                    if elapsed >= wait_timeout {
                        let queue_ms =
                            queue_metric.finish(AdmissionQueueOutcome::TimedOut, rejection.reason);
                        span.record("queue_ms", queue_ms);
                        self.emit_rejection(kind, rejection.reason);
                        return Err(rejection.status);
                    }
                    let retry_after = pplx_space_common_grpc::retry_after(&rejection.status)
                        .unwrap_or(CONCURRENCY_RETRY_AFTER);
                    let remaining = wait_timeout.saturating_sub(elapsed);
                    if retry_after >= remaining {
                        let queue_ms =
                            queue_metric.finish(AdmissionQueueOutcome::TimedOut, rejection.reason);
                        span.record("queue_ms", queue_ms);
                        self.emit_rejection(kind, rejection.reason);
                        return Err(rejection.status);
                    }
                    tokio::time::sleep(retry_after)
                        .instrument(tracing::info_span!(parent: &span, "wait"))
                        .await;
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) fn try_acquire(
        &self,
        kind: ActivationAdmissionKind,
    ) -> Result<ActivationAdmissionPermit, Status> {
        match self.try_acquire_inner(kind) {
            Ok(permit) => Ok(permit),
            Err(rejection) => {
                self.emit_rejection(kind, rejection.reason);
                Err(rejection.status)
            }
        }
    }

    fn try_acquire_inner(
        &self,
        kind: ActivationAdmissionKind,
    ) -> Result<ActivationAdmissionPermit, ActivationAdmissionRejection> {
        let metric_class = kind.metric_class();
        let permit = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                return Err(ActivationAdmissionRejection {
                    status: pplx_space_common_grpc::rate_limited_status(
                        format!(
                            "sandbox activation admission concurrency full (max_concurrent_activations={})",
                            self.config.max_concurrent_activations
                        ),
                        Some(CONCURRENCY_RETRY_AFTER),
                    ),
                    reason: AdmissionRejectReason::ConcurrencyFull,
                });
            }
        };

        if let Err(retry_after) = self.consume_token() {
            drop(permit);
            return Err(ActivationAdmissionRejection {
                status: pplx_space_common_grpc::rate_limited_status(
                    format!(
                        "sandbox activation admission rate limited (refill_rps={}, burst={}; retry_after_ms={})",
                        self.config.refill_rps,
                        self.config.burst,
                        retry_after.as_millis()
                    ),
                    Some(retry_after),
                ),
                reason: AdmissionRejectReason::RateLimited,
            });
        }

        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        let operation_in_flight = self
            .in_flight_by_kind
            .counter(kind)
            .fetch_add(1, Ordering::SeqCst)
            + 1;
        pplx_space_metrics::sandbox::admission::node_decision(
            metric_class,
            AdmissionDecision::Accepted,
            AdmissionRejectReason::None,
        );
        pplx_space_metrics::sandbox::admission::node_in_flight(in_flight);
        pplx_space_metrics::sandbox::admission::node_operation_in_flight(
            metric_class,
            operation_in_flight,
        );
        Ok(ActivationAdmissionPermit {
            _permit: permit,
            in_flight: self.in_flight.clone(),
            in_flight_by_kind: Arc::clone(&self.in_flight_by_kind),
            kind,
            started_at: Instant::now(),
            outcome: AdmissionPermitOutcome::Cancelled,
        })
    }

    fn emit_rejection(&self, kind: ActivationAdmissionKind, reason: AdmissionRejectReason) {
        pplx_space_metrics::sandbox::admission::node_refill_rps(self.config.refill_rps);
        pplx_space_metrics::sandbox::admission::node_decision(
            kind.metric_class(),
            AdmissionDecision::Rejected,
            reason,
        );
        pplx_space_metrics::sandbox::admission::node_in_flight(self.in_flight());
    }

    fn consume_token(&self) -> Result<(), Duration> {
        let now = Instant::now();
        let mut bucket = match self.bucket.lock() {
            Ok(bucket) => bucket,
            Err(poisoned) => {
                tracing::error!("activation admission token bucket mutex was poisoned; recovering");
                poisoned.into_inner()
            }
        };
        refill_bucket(&mut bucket, self.config, now);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            return Ok(());
        }
        let missing = 1.0 - bucket.tokens;
        let retry_after = token_retry_after(missing, self.config.refill_rps);
        Err(retry_after)
    }
}

#[derive(Debug)]
pub(super) struct ActivationAdmissionPermit {
    _permit: OwnedSemaphorePermit,
    in_flight: Arc<AtomicUsize>,
    in_flight_by_kind: Arc<ActivationInFlight>,
    kind: ActivationAdmissionKind,
    started_at: Instant,
    outcome: AdmissionPermitOutcome,
}

impl ActivationAdmissionPermit {
    pub(super) fn finish(mut self, outcome: AdmissionPermitOutcome) {
        self.outcome = outcome;
    }
}

impl Drop for ActivationAdmissionPermit {
    fn drop(&mut self) {
        let in_flight = self
            .in_flight
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        let operation_in_flight = self
            .in_flight_by_kind
            .counter(self.kind)
            .fetch_sub(1, Ordering::SeqCst)
            .saturating_sub(1);
        pplx_space_metrics::sandbox::admission::node_in_flight(in_flight);
        pplx_space_metrics::sandbox::admission::node_operation_in_flight(
            self.kind.metric_class(),
            operation_in_flight,
        );
        pplx_space_metrics::sandbox::admission::node_permit_hold(
            self.kind.metric_class(),
            self.outcome,
            elapsed_ms(self.started_at),
        );
        tracing::debug!(
            admission_kind = self.kind.as_str(),
            "released sbxmgr activation admission permit"
        );
    }
}

fn elapsed_ms(started_at: Instant) -> f64 {
    started_at.elapsed().as_secs_f64() * 1_000.0
}

pub(super) fn sanitize_config(config: ActivationAdmissionConfig) -> ActivationAdmissionConfig {
    let defaults = ActivationAdmissionConfig::default();
    ActivationAdmissionConfig {
        max_concurrent_activations: valid_max_concurrent(config.max_concurrent_activations)
            .unwrap_or(defaults.max_concurrent_activations),
        refill_rps: valid_positive_f64(config.refill_rps).unwrap_or(defaults.refill_rps),
        burst: valid_burst(config.burst).unwrap_or(defaults.burst),
    }
}

fn refill_bucket(bucket: &mut TokenBucket, config: ActivationAdmissionConfig, now: Instant) {
    let elapsed = now.saturating_duration_since(bucket.updated_at);
    bucket.tokens = (bucket.tokens + elapsed.as_secs_f64() * config.refill_rps).min(config.burst);
    bucket.updated_at = now;
}

fn token_retry_after(missing_tokens: f64, refill_rps: f64) -> Duration {
    let seconds = (missing_tokens / refill_rps).max(0.001);
    if !seconds.is_finite() {
        return DEFAULT_WAIT_TIMEOUT;
    }
    Duration::from_secs_f64(seconds.min(DEFAULT_WAIT_TIMEOUT.as_secs_f64()))
}

fn parse_positive_usize_env(var: &str, default: usize) -> usize {
    match std::env::var(var) {
        Ok(raw) => raw
            .trim()
            .parse::<usize>()
            .ok()
            .and_then(valid_max_concurrent)
            .unwrap_or_else(|| {
                tracing::warn!(
                    var,
                    raw = %raw,
                    default,
                    "env var is not a positive activation-admission integer; using default"
                );
                default
            }),
        Err(_) => default,
    }
}

fn valid_max_concurrent(value: usize) -> Option<usize> {
    (value > 0 && value <= Semaphore::MAX_PERMITS).then_some(value)
}

fn valid_burst(value: f64) -> Option<f64> {
    valid_positive_f64(value).filter(|value| *value >= MIN_CONSUMING_BURST)
}

pub(super) async fn start_stage<T, F>(stage: &'static str, sandbox_id: &str, fut: F) -> T
where
    F: Future<Output = T>,
{
    fut.instrument(tracing::info_span!(
        "sbxmgr.start.stage",
        sandbox_id,
        stage,
        otel.name = stage
    ))
    .await
}

pub(super) async fn start_stage_timeout<T, F>(
    stage: &'static str,
    timeout: Duration,
    sandbox_id: &str,
    fut: F,
) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    match tokio::time::timeout(
        timeout,
        fut.instrument(tracing::info_span!(
            "sbxmgr.start.stage",
            sandbox_id,
            stage,
            otel.name = stage,
            timeout_ms = timeout.as_millis() as u64
        )),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(
                sandbox_id,
                stage,
                timeout_ms = timeout.as_millis() as u64,
                "sbxmgr start stage timed out"
            );
            Err(Status::deadline_exceeded(format!(
                "{stage} timed out after {}ms",
                timeout.as_millis()
            )))
        }
    }
}

pub(super) async fn start_stage_timeout_or<T, F>(
    stage: &'static str,
    timeout: Duration,
    sandbox_id: &str,
    fallback: T,
    fut: F,
) -> T
where
    F: Future<Output = T>,
{
    match tokio::time::timeout(
        timeout,
        fut.instrument(tracing::info_span!(
            "sbxmgr.start.stage",
            sandbox_id,
            stage,
            otel.name = stage,
            timeout_ms = timeout.as_millis() as u64
        )),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(
                sandbox_id,
                stage,
                timeout_ms = timeout.as_millis() as u64,
                "sbxmgr start stage timed out; using fallback"
            );
            fallback
        }
    }
}

#[cfg(test)]
mod tests;
