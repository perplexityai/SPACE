//! Shared gRPC rate limiting for local SPACE services.
//!
//! This module is intentionally transport-aware at the edge: for UDS gRPC it
//! extracts peer identity from `SO_PEERCRED` via tonic's `UdsConnectInfo`.
//! Policy is keyed by stable peer identity, not by PID.
//!
//! It is its own crate so a service can rate-limit gRPC requests without
//! depending on the rest of `pplx-space-common-svc`.

use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use http::{Request, Response};
use http_body::{Body as HttpBody, Frame, SizeHint};
use pin_project_lite::pin_project;
use tonic::Status;
use tower::{Layer, Service};

use pplx_space_uds_peer_auth::{PeerIdentity, peer_identity_from_http_request};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub const GRPC_HEALTH_CHECK_METHOD: &str = "/grpc.health.v1.Health/Check";
const OTHER_GRPC_METHOD_TAG: &str = "other";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GrpcRateLimiterMode {
    Observe,
    Enforce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnknownPeerPolicy {
    Reject,
    SharedBucket,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum GrpcRateLimitBucket {
    Business,
    Health,
}

impl GrpcRateLimitBucket {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Business => "business",
            Self::Health => "health",
        }
    }
}

impl fmt::Display for GrpcRateLimitBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct GrpcRateLimiterConfig {
    service_name: Arc<str>,
    mode: GrpcRateLimiterMode,
    unknown_peer_policy: UnknownPeerPolicy,
    default_limits: GrpcRateLimiterLimits,
    bucket_limits: HashMap<GrpcRateLimitBucket, GrpcRateLimiterLimits>,
    method_buckets: HashMap<Arc<str>, GrpcRateLimitBucket>,
    method_limits: HashMap<Arc<str>, GrpcRateLimiterLimits>,
}

impl GrpcRateLimiterConfig {
    pub fn enforce(service_name: impl Into<Arc<str>>) -> Self {
        Self {
            service_name: service_name.into(),
            mode: GrpcRateLimiterMode::Enforce,
            unknown_peer_policy: UnknownPeerPolicy::Reject,
            default_limits: GrpcRateLimiterLimits::disabled(),
            bucket_limits: HashMap::new(),
            method_buckets: HashMap::new(),
            method_limits: HashMap::new(),
        }
    }

    pub fn observe_only(service_name: impl Into<Arc<str>>) -> Self {
        Self {
            mode: GrpcRateLimiterMode::Observe,
            ..Self::enforce(service_name)
        }
    }

    pub fn with_unknown_peer_policy(mut self, policy: UnknownPeerPolicy) -> Self {
        self.unknown_peer_policy = policy;
        self
    }

    pub fn with_default_limits(mut self, limits: GrpcRateLimiterLimits) -> Self {
        self.default_limits = limits;
        self
    }

    pub fn with_method_limits(
        mut self,
        grpc_method_path: impl Into<Arc<str>>,
        limits: GrpcRateLimiterLimits,
    ) -> Self {
        self.method_limits.insert(grpc_method_path.into(), limits);
        self
    }

    pub fn with_bucket_limits(
        mut self,
        bucket: GrpcRateLimitBucket,
        limits: GrpcRateLimiterLimits,
    ) -> Self {
        self.bucket_limits.insert(bucket, limits);
        self
    }

    pub fn with_method_bucket(
        mut self,
        grpc_method_path: impl Into<Arc<str>>,
        bucket: GrpcRateLimitBucket,
    ) -> Self {
        self.method_buckets.insert(grpc_method_path.into(), bucket);
        self
    }

    pub fn with_health_check_limits(self, limits: GrpcRateLimiterLimits) -> Self {
        self.with_bucket_limits(GrpcRateLimitBucket::Health, limits)
            .with_method_bucket(GRPC_HEALTH_CHECK_METHOD, GrpcRateLimitBucket::Health)
    }

    fn bucket_for_method(&self, method: &str) -> GrpcRateLimitBucket {
        self.method_buckets
            .get(method)
            .copied()
            .unwrap_or(GrpcRateLimitBucket::Business)
    }

    fn bucket_limits(&self, bucket: GrpcRateLimitBucket) -> GrpcRateLimiterLimits {
        match bucket {
            GrpcRateLimitBucket::Business => self.default_limits,
            GrpcRateLimitBucket::Health => self
                .bucket_limits
                .get(&bucket)
                .copied()
                .unwrap_or(self.default_limits),
        }
    }

    fn metric_grpc_method<'a>(&self, method: &'a str) -> &'a str {
        if method == GRPC_HEALTH_CHECK_METHOD
            || self.method_buckets.contains_key(method)
            || self.method_limits.contains_key(method)
        {
            method
        } else {
            OTHER_GRPC_METHOD_TAG
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GrpcRateLimiterLimits {
    pub max_global_in_flight: Option<usize>,
    pub max_in_flight_per_peer: Option<usize>,
    pub max_requests_per_peer: Option<u32>,
    /// Time window used to derive the GCRA refill interval for
    /// `max_requests_per_peer`.
    pub per_peer_request_window: Duration,
}

impl GrpcRateLimiterLimits {
    pub fn disabled() -> Self {
        Self {
            max_global_in_flight: None,
            max_in_flight_per_peer: None,
            max_requests_per_peer: None,
            per_peer_request_window: Duration::from_secs(1),
        }
    }

    pub fn conservative_worker_default() -> Self {
        Self {
            max_global_in_flight: Some(4096),
            max_in_flight_per_peer: Some(2048),
            max_requests_per_peer: Some(20_000),
            per_peer_request_window: Duration::from_secs(1),
        }
    }

    pub fn conservative_health_default() -> Self {
        Self {
            max_global_in_flight: Some(128),
            max_in_flight_per_peer: Some(32),
            max_requests_per_peer: Some(5),
            per_peer_request_window: Duration::from_secs(1),
        }
    }

    pub fn with_max_global_in_flight(mut self, max: usize) -> Self {
        self.max_global_in_flight = Some(max);
        self
    }

    pub fn with_max_in_flight_per_peer(mut self, max: usize) -> Self {
        self.max_in_flight_per_peer = Some(max);
        self
    }

    pub fn with_max_requests_per_peer(mut self, max: u32, window: Duration) -> Self {
        self.max_requests_per_peer = Some(max);
        self.per_peer_request_window = window;
        self
    }
}

#[derive(Clone)]
pub struct GrpcRateLimiterLayer {
    config: Arc<GrpcRateLimiterConfig>,
    state: Arc<Mutex<RateLimiterState>>,
}

impl GrpcRateLimiterLayer {
    pub fn new(config: GrpcRateLimiterConfig) -> Self {
        Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(RateLimiterState::default())),
        }
    }
}

pub fn grpc_rate_limiter_layer(config: GrpcRateLimiterConfig) -> GrpcRateLimiterLayer {
    GrpcRateLimiterLayer::new(config)
}

pub fn conservative_uds_grpc_rate_limiter_layer(
    service_name: impl Into<Arc<str>>,
) -> GrpcRateLimiterLayer {
    GrpcRateLimiterLayer::new(
        GrpcRateLimiterConfig::enforce(service_name)
            .with_default_limits(GrpcRateLimiterLimits::conservative_worker_default())
            .with_health_check_limits(GrpcRateLimiterLimits::conservative_health_default()),
    )
}

impl<S> Layer<S> for GrpcRateLimiterLayer {
    type Service = GrpcRateLimiterService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRateLimiterService {
            inner,
            config: Arc::clone(&self.config),
            state: Arc::clone(&self.state),
        }
    }
}

#[derive(Clone)]
pub struct GrpcRateLimiterService<S> {
    inner: S,
    config: Arc<GrpcRateLimiterConfig>,
    state: Arc<Mutex<RateLimiterState>>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for GrpcRateLimiterService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send,
    ResBody: HttpBody<Data = bytes::Bytes> + Send + 'static,
    ResBody::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<ReqBody>) -> Self::Future {
        let method = req.uri().path().to_owned();
        let peer = peer_identity_from_http_request(&req);

        let rate_limit = rate_limit_request(&self.config, &self.state, peer.as_ref(), &method);
        match rate_limit {
            RateLimiterResult::Accepted(guard) | RateLimiterResult::ObservedWouldReject(guard) => {
                let future = self.inner.call(req);
                Box::pin(async move {
                    let response = future.await.map_err(Into::into)?;
                    Ok(response.map(|body| tonic::body::Body::new(GuardedBody::new(body, guard))))
                })
            }
            RateLimiterResult::Rejected(status) => Box::pin(async move {
                let error: BoxError = Box::new(status);
                Err(error)
            }),
        }
    }
}

pin_project! {
    struct GuardedBody<B> {
        #[pin]
        inner: B,
        guard: Option<RateLimiterGuard>,
    }
}

impl<B> GuardedBody<B> {
    fn new(inner: B, guard: RateLimiterGuard) -> Self {
        Self {
            inner,
            guard: Some(guard),
        }
    }
}

impl<B> HttpBody for GuardedBody<B>
where
    B: HttpBody<Data = bytes::Bytes>,
{
    type Data = bytes::Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.project();
        let poll = this.inner.poll_frame(cx);
        if matches!(poll, Poll::Ready(None)) {
            this.guard.take();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn rate_limit_request(
    config: &GrpcRateLimiterConfig,
    state: &Arc<Mutex<RateLimiterState>>,
    peer: Option<&PeerIdentity>,
    method: &str,
) -> RateLimiterResult {
    let bucket = config.bucket_for_method(method);
    let peer_key = match peer {
        Some(peer) => peer_key(peer),
        None => match config.unknown_peer_policy {
            UnknownPeerPolicy::Reject => {
                return reject_or_observe(
                    config,
                    state,
                    bucket,
                    PeerKey::Unknown,
                    method,
                    RejectReason::MissingPeerIdentity,
                );
            }
            UnknownPeerPolicy::SharedBucket => PeerKey::Unknown,
        },
    };

    rate_limit_with_key(config, state, bucket, peer_key, method)
}

fn peer_key(peer: &PeerIdentity) -> PeerKey {
    PeerKey::UidGid {
        uid: peer.uid(),
        gid: peer.gid(),
    }
}

fn rate_limit_with_key(
    config: &GrpcRateLimiterConfig,
    state: &Arc<Mutex<RateLimiterState>>,
    bucket: GrpcRateLimitBucket,
    peer_key: PeerKey,
    method: &str,
) -> RateLimiterResult {
    let default_limits = config.bucket_limits(bucket);
    let method_limits = config.method_limits.get(method).copied();
    let limits = merge_limits(default_limits, method_limits);
    let now = Instant::now();
    let mut state_guard = state.lock().expect("rate limiter state mutex poisoned");
    if let Some(max) = limits.max_global_in_flight
        && global_in_flight(&state_guard, bucket) >= max
    {
        drop(state_guard);
        return reject_or_observe(
            config,
            state,
            bucket,
            peer_key,
            method,
            RejectReason::GlobalInFlight { max },
        );
    }

    {
        let peer_state = state_guard
            .peers
            .entry(BucketPeerKey::new(bucket, peer_key.clone()))
            .or_default();

        if let Some(max) = limits.max_in_flight_per_peer
            && peer_state.in_flight >= max
        {
            drop(state_guard);
            return reject_or_observe(
                config,
                state,
                bucket,
                peer_key,
                method,
                RejectReason::PeerInFlight { max },
            );
        }
        peer_state.in_flight += 1;
    }

    if let Err(reason) = check_gcra_limit(
        &mut state_guard,
        RateKey::Peer {
            bucket,
            peer: peer_key.clone(),
        },
        default_limits,
        now,
    ) {
        rollback_in_flight(&mut state_guard, bucket, &peer_key);
        drop(state_guard);
        return reject_or_observe(config, state, bucket, peer_key, method, reason);
    }

    if let Some(method_limits) = method_limits {
        let rate_key = RateKey::PeerMethod {
            bucket,
            peer: peer_key.clone(),
            method: Arc::from(method),
        };
        if let Err(reason) = check_gcra_limit(&mut state_guard, rate_key, method_limits, now) {
            rollback_in_flight(&mut state_guard, bucket, &peer_key);
            drop(state_guard);
            return reject_or_observe(config, state, bucket, peer_key, method, reason);
        }
    }

    let global_in_flight = increment_global_in_flight(&mut state_guard, bucket);
    drop(state_guard);
    record_rate_limiter_decision(config, bucket, method, "accepted", "none");
    emit_in_flight_gauge(config.service_name.as_ref(), bucket, global_in_flight);

    RateLimiterResult::Accepted(RateLimiterGuard::new(
        Arc::clone(state),
        peer_key,
        bucket,
        Arc::clone(&config.service_name),
    ))
}

fn reject_or_observe(
    config: &GrpcRateLimiterConfig,
    state: &Arc<Mutex<RateLimiterState>>,
    bucket: GrpcRateLimitBucket,
    peer_key: PeerKey,
    method: &str,
    reason: RejectReason,
) -> RateLimiterResult {
    match config.mode {
        GrpcRateLimiterMode::Enforce => {
            log_rate_limit_decision(config, bucket, &peer_key, method, reason, "rejected");
            record_rate_limiter_decision(config, bucket, method, "rejected", reason.kind());
            RateLimiterResult::Rejected(reason.status())
        }
        GrpcRateLimiterMode::Observe => {
            log_rate_limit_decision(config, bucket, &peer_key, method, reason, "would_reject");
            let mut state_guard = state.lock().expect("rate limiter state mutex poisoned");
            let global_in_flight = increment_global_in_flight(&mut state_guard, bucket);
            state_guard
                .peers
                .entry(BucketPeerKey::new(bucket, peer_key.clone()))
                .or_default()
                .in_flight += 1;
            drop(state_guard);
            record_rate_limiter_decision(config, bucket, method, "would_reject", reason.kind());
            emit_in_flight_gauge(config.service_name.as_ref(), bucket, global_in_flight);
            RateLimiterResult::ObservedWouldReject(RateLimiterGuard::new(
                Arc::clone(state),
                peer_key,
                bucket,
                Arc::clone(&config.service_name),
            ))
        }
    }
}

fn log_rate_limit_decision(
    config: &GrpcRateLimiterConfig,
    bucket: GrpcRateLimitBucket,
    peer_key: &PeerKey,
    method: &str,
    reason: RejectReason,
    decision: &'static str,
) {
    tracing::warn!(
        service = %config.service_name,
        bucket = bucket.as_str(),
        grpc_method = method,
        decision,
        peer_kind = peer_key.kind(),
        peer_uid = ?peer_key.uid(),
        peer_gid = ?peer_key.gid(),
        reason = reason.kind(),
        limit_max = ?reason.limit_max(),
        window_ms = ?reason.window_ms(),
        retry_after_ms = ?reason.retry_after_ms(),
        "UDS gRPC rate limiter exceeded"
    );
}

fn record_rate_limiter_decision(
    config: &GrpcRateLimiterConfig,
    bucket: GrpcRateLimitBucket,
    method: &str,
    decision: &'static str,
    reason: &'static str,
) {
    pplx_space_metrics::grpc::server_rate_limiter_request(
        config.service_name.as_ref(),
        bucket.as_str(),
        config.metric_grpc_method(method),
        decision,
        reason,
    );
}

fn emit_in_flight_gauge(service_name: &str, bucket: GrpcRateLimitBucket, value: usize) {
    pplx_space_metrics::grpc::server_rate_limiter_in_flight(
        service_name,
        bucket.as_str(),
        value as f64,
    );
}

fn merge_limits(
    default_limits: GrpcRateLimiterLimits,
    override_limits: Option<GrpcRateLimiterLimits>,
) -> GrpcRateLimiterLimits {
    let Some(override_limits) = override_limits else {
        return default_limits;
    };

    GrpcRateLimiterLimits {
        max_global_in_flight: override_limits
            .max_global_in_flight
            .or(default_limits.max_global_in_flight),
        max_in_flight_per_peer: override_limits
            .max_in_flight_per_peer
            .or(default_limits.max_in_flight_per_peer),
        max_requests_per_peer: override_limits
            .max_requests_per_peer
            .or(default_limits.max_requests_per_peer),
        per_peer_request_window: if override_limits.max_requests_per_peer.is_some() {
            override_limits.per_peer_request_window
        } else {
            default_limits.per_peer_request_window
        },
    }
}

fn check_gcra_limit(
    state: &mut RateLimiterState,
    key: RateKey,
    limits: GrpcRateLimiterLimits,
    now: Instant,
) -> Result<(), RejectReason> {
    let Some(rate) = GcraRate::from_limits(limits) else {
        return Ok(());
    };

    let bucket = state.rate_buckets.entry(key).or_insert_with(|| GcraBucket {
        theoretical_arrival_time: now,
    });
    check_gcra_bucket(bucket, rate, now).map_err(|retry_after| RejectReason::PeerRequestRate {
        max: rate.capacity,
        window: rate.window,
        retry_after,
    })
}

fn check_gcra_bucket(
    bucket: &mut GcraBucket,
    rate: GcraRate,
    now: Instant,
) -> Result<(), Duration> {
    if rate.capacity == 0 {
        return Err(rate.window);
    }

    let interval_secs = rate.window.as_secs_f64() / f64::from(rate.capacity);
    if !interval_secs.is_finite() || interval_secs <= 0.0 {
        return Err(rate.window);
    }

    let interval = Duration::from_secs_f64(interval_secs);
    let burst_tolerance = interval.mul_f64(f64::from(rate.capacity.saturating_sub(1)));

    let allowed_at = bucket
        .theoretical_arrival_time
        .checked_sub(burst_tolerance)
        .unwrap_or(bucket.theoretical_arrival_time);
    if now < allowed_at {
        return Err(allowed_at.duration_since(now));
    }

    bucket.theoretical_arrival_time = std::cmp::max(now, bucket.theoretical_arrival_time)
        .checked_add(interval)
        .unwrap_or(bucket.theoretical_arrival_time);
    Ok(())
}

fn global_in_flight(state: &RateLimiterState, bucket: GrpcRateLimitBucket) -> usize {
    state
        .global_in_flight
        .get(&bucket)
        .copied()
        .unwrap_or_default()
}

fn increment_global_in_flight(state: &mut RateLimiterState, bucket: GrpcRateLimitBucket) -> usize {
    let in_flight = state.global_in_flight.entry(bucket).or_default();
    *in_flight += 1;
    *in_flight
}

fn decrement_global_in_flight(state: &mut RateLimiterState, bucket: GrpcRateLimitBucket) -> usize {
    let in_flight = state.global_in_flight.entry(bucket).or_default();
    *in_flight = in_flight.saturating_sub(1);
    *in_flight
}

fn rollback_in_flight(
    state: &mut RateLimiterState,
    bucket: GrpcRateLimitBucket,
    peer_key: &PeerKey,
) {
    if let Some(peer) = state
        .peers
        .get_mut(&BucketPeerKey::new(bucket, peer_key.clone()))
    {
        peer.in_flight = peer.in_flight.saturating_sub(1);
    }
}

#[derive(Default)]
struct RateLimiterState {
    global_in_flight: HashMap<GrpcRateLimitBucket, usize>,
    peers: HashMap<BucketPeerKey, PeerState>,
    rate_buckets: HashMap<RateKey, GcraBucket>,
}

#[derive(Clone, Debug, Default)]
struct PeerState {
    in_flight: usize,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum PeerKey {
    UidGid { uid: u32, gid: u32 },
    Unknown,
}

impl PeerKey {
    fn kind(&self) -> &'static str {
        match self {
            Self::UidGid { .. } => "uid_gid",
            Self::Unknown => "unknown",
        }
    }

    fn uid(&self) -> Option<u32> {
        match self {
            Self::UidGid { uid, .. } => Some(*uid),
            Self::Unknown => None,
        }
    }

    fn gid(&self) -> Option<u32> {
        match self {
            Self::UidGid { gid, .. } => Some(*gid),
            Self::Unknown => None,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct BucketPeerKey {
    bucket: GrpcRateLimitBucket,
    peer: PeerKey,
}

impl BucketPeerKey {
    fn new(bucket: GrpcRateLimitBucket, peer: PeerKey) -> Self {
        Self { bucket, peer }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum RateKey {
    Peer {
        bucket: GrpcRateLimitBucket,
        peer: PeerKey,
    },
    PeerMethod {
        bucket: GrpcRateLimitBucket,
        peer: PeerKey,
        method: Arc<str>,
    },
}

#[derive(Clone, Copy, Debug)]
struct GcraBucket {
    theoretical_arrival_time: Instant,
}

#[derive(Clone, Copy, Debug)]
struct GcraRate {
    capacity: u32,
    window: Duration,
}

impl GcraRate {
    fn from_limits(limits: GrpcRateLimiterLimits) -> Option<Self> {
        limits.max_requests_per_peer.map(|capacity| Self {
            capacity,
            window: limits.per_peer_request_window,
        })
    }
}

enum RateLimiterResult {
    Accepted(RateLimiterGuard),
    ObservedWouldReject(RateLimiterGuard),
    Rejected(Status),
}

struct RateLimiterGuard {
    state: Arc<Mutex<RateLimiterState>>,
    peer_key: PeerKey,
    bucket: GrpcRateLimitBucket,
    service_name: Arc<str>,
}

impl RateLimiterGuard {
    fn new(
        state: Arc<Mutex<RateLimiterState>>,
        peer_key: PeerKey,
        bucket: GrpcRateLimitBucket,
        service_name: Arc<str>,
    ) -> Self {
        Self {
            state,
            peer_key,
            bucket,
            service_name,
        }
    }
}

impl Drop for RateLimiterGuard {
    fn drop(&mut self) {
        let global_in_flight = {
            let mut state = self
                .state
                .lock()
                .expect("rate limiter state mutex poisoned");
            let global_in_flight = decrement_global_in_flight(&mut state, self.bucket);
            if let Some(peer) = state
                .peers
                .get_mut(&BucketPeerKey::new(self.bucket, self.peer_key.clone()))
            {
                peer.in_flight = peer.in_flight.saturating_sub(1);
            }
            global_in_flight
        };
        emit_in_flight_gauge(self.service_name.as_ref(), self.bucket, global_in_flight);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RejectReason {
    MissingPeerIdentity,
    GlobalInFlight {
        max: usize,
    },
    PeerInFlight {
        max: usize,
    },
    PeerRequestRate {
        max: u32,
        window: Duration,
        retry_after: Duration,
    },
}

impl RejectReason {
    fn kind(self) -> &'static str {
        match self {
            Self::MissingPeerIdentity => "missing_peer_identity",
            Self::GlobalInFlight { .. } => "global_in_flight",
            Self::PeerInFlight { .. } => "peer_in_flight",
            Self::PeerRequestRate { .. } => "peer_request_rate",
        }
    }

    fn limit_max(self) -> Option<u64> {
        match self {
            Self::GlobalInFlight { max } | Self::PeerInFlight { max } => Some(max as u64),
            Self::PeerRequestRate { max, .. } => Some(u64::from(max)),
            Self::MissingPeerIdentity => None,
        }
    }

    fn window_ms(self) -> Option<u128> {
        match self {
            Self::PeerRequestRate { window, .. } => Some(window.as_millis()),
            Self::MissingPeerIdentity | Self::GlobalInFlight { .. } | Self::PeerInFlight { .. } => {
                None
            }
        }
    }

    fn retry_after_ms(self) -> Option<u128> {
        match self {
            Self::PeerRequestRate { retry_after, .. } => Some(retry_after.as_millis()),
            Self::MissingPeerIdentity | Self::GlobalInFlight { .. } | Self::PeerInFlight { .. } => {
                None
            }
        }
    }

    fn status(self) -> Status {
        match self {
            RejectReason::MissingPeerIdentity => {
                Status::permission_denied("missing UDS peer credentials")
            }
            RejectReason::GlobalInFlight { .. } | RejectReason::PeerInFlight { .. } => {
                Status::resource_exhausted(self.message())
            }
            RejectReason::PeerRequestRate { retry_after, .. } => {
                pplx_space_common_grpc::rate_limited_status(self.message(), Some(retry_after))
            }
        }
    }

    fn message(self) -> String {
        match self {
            RejectReason::MissingPeerIdentity => "missing UDS peer credentials".to_owned(),
            RejectReason::GlobalInFlight { .. } | RejectReason::PeerInFlight { .. } => {
                "UDS gRPC rate limit exceeded".to_owned()
            }
            RejectReason::PeerRequestRate { retry_after, .. } => format!(
                "UDS gRPC request-rate limit exceeded; retry_after_ms={}",
                retry_after.as_millis()
            ),
        }
    }
}

#[cfg(test)]
mod tests;
