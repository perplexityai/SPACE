//! Ingress preparation emits outcome/count and duration before forwarding; waiter
//! rejection and completion inactivity timeouts have separate counters. Established
//! stream lifetime is excluded from preparation latency. No sandbox ID labels.
//! `data_plane.upload.rejected{reason}` counts `POST /files` requests the
//! per-pod upload admission limiter answered with 429.
use std::{fmt, time::Instant};

use crate::emit::{counter, elapsed_ms, gauge, tag, timing_ms};
pub use crate::response::{
    DataPlaneResponseSurface as ResponseSurface, ResponseKind, ResponseOrigin,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendRpcSurface {
    Ecw,
    Native,
}

impl BackendRpcSurface {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
            Self::Native => "native",
        }
    }
}

impl fmt::Display for BackendRpcSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendRpcResult {
    Ok,
    Status,
    Timeout,
}

impl BackendRpcResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Status => "status",
            Self::Timeout => "timeout",
        }
    }
}

impl fmt::Display for BackendRpcResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStartDeliveryOutcome {
    NewStart,
    DeduplicatedRetry,
    Reattached,
    ReplayMiss,
    Backpressure,
}

impl ProcessStartDeliveryOutcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::NewStart => "new_start",
            Self::DeduplicatedRetry => "deduplicated_retry",
            Self::Reattached => "reattached",
            Self::ReplayMiss => "replay_miss",
            Self::Backpressure => "backpressure",
        }
    }
}

impl fmt::Display for ProcessStartDeliveryOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStartRecoveryInterruption {
    Cancelled,
    Internal,
    Unavailable,
    Unknown,
}

impl ProcessStartRecoveryInterruption {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for ProcessStartRecoveryInterruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStartRecoveryMode {
    RetryStart,
    Reattach,
}

impl ProcessStartRecoveryMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::RetryStart => "retry_start",
            Self::Reattach => "reattach",
        }
    }
}

impl fmt::Display for ProcessStartRecoveryMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteRepairSurface {
    Ecw,
}

impl RouteRepairSurface {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
        }
    }
}

impl fmt::Display for RouteRepairSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteRepairResult {
    Enqueued,
    Coalesced,
    Completed,
}

impl RouteRepairResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Enqueued => "enqueued",
            Self::Coalesced => "coalesced",
            Self::Completed => "completed",
        }
    }
}

impl fmt::Display for RouteRepairResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressOpenResult {
    IngressOk,
    Error,
}

impl ServicePortIngressOpenResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::IngressOk => "ingress_ok",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for ServicePortIngressOpenResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressOpenReason {
    Ok,
    GuestConnectionRefused,
    MissingRouteToken,
    Unsupported,
    OpenError,
}

impl ServicePortIngressOpenReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::GuestConnectionRefused => "guest_connection_refused",
            Self::MissingRouteToken => "missing_route_token",
            Self::Unsupported => "unsupported",
            Self::OpenError => "open_error",
        }
    }
}

impl fmt::Display for ServicePortIngressOpenReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteTokenRouteSource {
    DataPlaneSession,
    SandboxServiceRouteBridge,
    SandboxServiceRoute,
}

impl SpaceApiRouteTokenRouteSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::DataPlaneSession => "data_plane_session",
            Self::SandboxServiceRouteBridge => "sandbox_service_route_bridge",
            Self::SandboxServiceRoute => "sandbox_service_route",
        }
    }
}

impl fmt::Display for SpaceApiRouteTokenRouteSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpaceApiRouteTokenDropReason {
    Expired,
    InvalidCurrentNodeId,
    InvalidExpiry,
    InvalidPurpose,
    InvalidRouteGeneration,
    InvalidSandboxId,
    InvalidToken,
}

impl SpaceApiRouteTokenDropReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::InvalidCurrentNodeId => "invalid_current_node_id",
            Self::InvalidExpiry => "invalid_expiry",
            Self::InvalidPurpose => "invalid_purpose",
            Self::InvalidRouteGeneration => "invalid_route_generation",
            Self::InvalidSandboxId => "invalid_sandbox_id",
            Self::InvalidToken => "invalid_token",
        }
    }
}

impl fmt::Display for SpaceApiRouteTokenDropReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

metric_vocabulary!(
    /// Which per-pod upload admission budget a rejected upload waited on.
    UploadRejectReason {
        /// No concurrent-upload slot freed within the admission wait.
        Concurrency => "concurrency",
        /// The in-flight byte budget couldn't cover the reservation in time.
        Bytes => "bytes",
    }
);

pub fn upload_rejected(reason: UploadRejectReason) {
    counter(
        "data_plane.upload.rejected",
        1,
        &[tag("reason", reason.as_str())],
    );
}

pub fn attach(kind: &str, result: &str) {
    counter(
        "data_plane.attach",
        1,
        &[tag("kind", kind), tag("result", result)],
    );
}

pub fn stream_closed(kind: &str, result: &str) {
    counter(
        "data_plane.stream.closed",
        1,
        &[tag("kind", kind), tag("result", result)],
    );
}

pub fn stream_active(kind: &str, count: usize) {
    gauge(
        "data_plane.stream.active",
        count as f64,
        &[tag("kind", kind)],
    );
}

pub fn stream_bytes_in(kind: &str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    counter("data_plane.stream.bytes_in", bytes, &[tag("kind", kind)]);
}

pub fn stream_bytes_out(kind: &str, bytes: u64) {
    if bytes == 0 {
        return;
    }
    counter("data_plane.stream.bytes_out", bytes, &[tag("kind", kind)]);
}

pub fn backend_channel_pool_size(channels_per_endpoint: usize) {
    gauge(
        "data_plane.backend.channel_pool_size",
        channels_per_endpoint as f64,
        &[],
    );
}

pub fn backend_channel_pool_entries(entries: usize) {
    gauge(
        "data_plane.backend.channel_pool_entries",
        entries as f64,
        &[],
    );
}

pub fn backend_channel_reset(kind: &str, reason: &str) {
    counter(
        "data_plane.backend.channel_reset",
        1,
        &[tag("kind", kind), tag("reason", reason)],
    );
}

pub fn backend_rpc_latency(
    surface: BackendRpcSurface,
    operation: &str,
    result: BackendRpcResult,
    started_at: Instant,
) {
    timing_ms(
        "data_plane.backend.rpc_latency_ms",
        elapsed_ms(started_at),
        &[
            tag("surface", surface.as_str()),
            tag("operation", operation),
            tag("result", result.as_str()),
        ],
    );
}

pub fn backend_rpc_timeout(surface: BackendRpcSurface, operation: &str) {
    counter(
        "data_plane.backend.rpc_timeout",
        1,
        &[
            tag("surface", surface.as_str()),
            tag("operation", operation),
        ],
    );
}

pub fn backend_affected_streams(kind: &str, count: u64) {
    if count == 0 {
        return;
    }
    counter(
        "data_plane.backend.affected_streams",
        count,
        &[tag("kind", kind)],
    );
}

pub fn session_reconnect(kind: &str, reason: &str) {
    counter(
        "data_plane.session.reconnect",
        1,
        &[tag("kind", kind), tag("reason", reason)],
    );
}

pub fn process_start_delivery(outcome: ProcessStartDeliveryOutcome) {
    counter(
        "data_plane.process_start.delivery",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

pub fn process_start_recovery(
    interruption: ProcessStartRecoveryInterruption,
    mode: ProcessStartRecoveryMode,
) {
    counter(
        "data_plane.process_start.recovery",
        1,
        &[
            tag("interruption", interruption.as_str()),
            tag("mode", mode.as_str()),
        ],
    );
}

pub fn session_retention_deleted(count: u64) {
    if count == 0 {
        return;
    }
    counter("data_plane.session.retention_deleted", count, &[]);
}

pub fn replay_miss(kind: &str, reason: &str) {
    counter(
        "data_plane.replay.miss",
        1,
        &[tag("kind", kind), tag("reason", reason)],
    );
}

pub fn revocation_cache_probe(kind: &str, result: &str) {
    counter(
        "data_plane.revocation.cache_probe",
        1,
        &[tag("kind", kind), tag("result", result)],
    );
}

pub fn revocation_check(kind: &str, result: &str) {
    counter(
        "data_plane.revocation.check",
        1,
        &[tag("kind", kind), tag("result", result)],
    );
}

pub fn route_cache_lookup(surface: &str, kind: &str, result: &str) {
    counter(
        "data_plane.route_cache.lookup",
        1,
        &[
            tag("surface", surface),
            tag("kind", kind),
            tag("result", result),
        ],
    );
}

/// Outcome of one attempt to recover from a stale route-identity rejection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityRefreshOutcome {
    /// The route was re-resolved and the operation retried successfully.
    Succeeded,
    /// Re-resolving the route failed, so the original rejection stands.
    ResolveFailed,
    /// The retry on the refreshed route was itself rejected or failed.
    RetryFailed,
    /// No budget left in the caller's deadline to attempt recovery.
    DeadlineExceeded,
}

impl RouteIdentityRefreshOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::ResolveFailed => "resolve_failed",
            Self::RetryFailed => "retry_failed",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

impl std::fmt::Display for RouteIdentityRefreshOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteIdentityRefreshProtocol {
    Ecw,
    Native,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceRouteSurface {
    Ecw,
    Native,
}

impl ServiceRouteSurface {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
            Self::Native => "native",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceRouteTransport {
    Direct,
    ServicePortIngress,
}

impl ServiceRouteTransport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ServicePortIngress => "service_port_ingress",
        }
    }
}

/// Records the transport selected for one public sandbox-service request.
pub fn service_route_transport(surface: ServiceRouteSurface, transport: ServiceRouteTransport) {
    counter(
        "data_plane.service_route.transport",
        1,
        &[
            tag("surface", surface.as_str()),
            tag("transport", transport.as_str()),
        ],
    );
}

impl RouteIdentityRefreshProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
            Self::Native => "native",
        }
    }
}

impl std::fmt::Display for RouteIdentityRefreshProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Records recovery from a superseded route identity.
///
/// The endpoint's own rejection metric still counts every mismatch; this counts
/// only what the data plane managed to do about one, so a rise in rejections
/// with no matching recovery means the repair path is not engaging.
pub fn route_identity_refresh(
    protocol: RouteIdentityRefreshProtocol,
    dimension: &str,
    outcome: RouteIdentityRefreshOutcome,
) {
    counter(
        "data_plane.route_identity.refresh",
        1,
        &[
            tag("protocol", protocol.as_str()),
            tag("dimension", dimension),
            tag("outcome", outcome.as_str()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteCacheKind {
    Ecw,
    Native,
    NativeLatest,
}

impl RouteCacheKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
            Self::Native => "native",
            Self::NativeLatest => "native_latest",
        }
    }
}

impl std::fmt::Display for RouteCacheKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn route_cache_entries(cache: RouteCacheKind, entries: usize) {
    gauge(
        "data_plane.route_cache.entries",
        entries as f64,
        &[tag("cache", cache.as_str())],
    );
}

pub fn route_repair(surface: RouteRepairSurface, result: RouteRepairResult) {
    counter(
        "data_plane.route.repair",
        1,
        &[
            tag("surface", surface.as_str()),
            tag("result", result.as_str()),
        ],
    );
}

pub fn service_port_ingress_open(
    result: ServicePortIngressOpenResult,
    reason: ServicePortIngressOpenReason,
) {
    counter(
        "data_plane.service_port_ingress.open",
        1,
        &[
            tag("result", result.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

pub fn space_api_route_token_dropped(
    route_source: SpaceApiRouteTokenRouteSource,
    reason: SpaceApiRouteTokenDropReason,
) {
    counter(
        "data_plane.space_api_route_token.dropped",
        1,
        &[
            tag("route_source", route_source.as_str()),
            tag("reason", reason.as_str()),
        ],
    );
}

pub fn response(
    surface: ResponseSurface,
    origin: ResponseOrigin,
    kind: ResponseKind,
    http_status: u16,
) {
    crate::response::data_plane(surface, origin, kind, http_status);
}

/// Preparation excludes established HTTP/WS lifetime. Drop records failures,
/// including cancellation, while successful attachment records ingress_ok.
pub struct IngressPreparationObservation {
    started: std::time::Instant,
    result: ServicePortIngressOpenResult,
}
impl Default for IngressPreparationObservation {
    fn default() -> Self {
        Self {
            started: std::time::Instant::now(),
            result: ServicePortIngressOpenResult::Error,
        }
    }
}
impl IngressPreparationObservation {
    pub fn ready(&mut self) {
        self.result = ServicePortIngressOpenResult::IngressOk;
    }
}
impl Drop for IngressPreparationObservation {
    fn drop(&mut self) {
        let tags = [tag("result", self.result.as_str())];
        counter("data_plane.ingress.preparation", 1, &tags);
        timing_ms(
            "data_plane.ingress.preparation_ms",
            elapsed_ms(self.started),
            &tags,
        );
    }
}
pub fn ingress_completion_timeout() {
    counter("data_plane.ingress.completion_timeout", 1, &[]);
}
pub fn ingress_waiter_rejected() {
    counter("data_plane.ingress.waiter_rejected", 1, &[]);
}
