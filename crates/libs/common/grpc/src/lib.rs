//! Shared gRPC client retry helpers for SPACE services.
//!
//! This crate is intentionally conservative: it retries only short,
//! idempotent unary calls or connection establishment. Callers must not wrap
//! effectful RPCs unless the server operation is idempotent by request content
//! or by an explicit idempotency key.
//!
//! Primitive selection:
//!
//! * Use [`GrpcClient`] for long-lived generated tonic clients that should
//!   cache a fully configured client between calls. Its connect closure should
//!   build the client once, including any tracing/auth layers, without its own
//!   retry loop; [`GrpcClient`] owns one retry budget across connect and RPC.
//! * Use [`retry_grpc_status_with_context`] for a one-shot replay-safe RPC. If
//!   the closure opens a channel, use a single non-retrying connect attempt and
//!   return `Status::unavailable` on connect failure so the outer retry budget
//!   remains the only budget for the logical request.
//! * Use [`connect_uds_channel_lazy`] for a cached Unix-socket channel that must
//!   reconnect after the local server replaces its socket. Pair replay-safe
//!   calls with [`retry_grpc_status_with_context`] to cover the replacement gap.
//! * Use [`RotatingChannel`] in place of a `tonic::transport::Channel` for any
//!   long-lived client of a multi-replica TCP service. Retiring its connection
//!   on an age is what stops a ClusterIP from pinning the client to whichever
//!   pod it first dialed.
//! * Use [`connect_tcp_channel_with_retry`] or
//!   [`connect_uds_channel_with_retry`] when the caller only wants to buffer
//!   initial channel establishment and will not also retry the same RPC.

/// HTTP/2 frame size SPACE peers advertise, so a 64 KiB file chunk travels as
/// one DATA frame instead of five.
pub const BULK_HTTP2_MAX_FRAME_SIZE: u32 = 256 * 1024;

mod active_requests;
mod client;
mod connect;
mod context;
mod deadline;
mod forward;
mod idempotency;
mod policy;
mod preface;
pub use preface::PrefaceDeadline;
#[cfg(feature = "internal-tls")]
pub mod internal_transport;
mod queued_slots;
mod request_admission;
mod request_body;
mod retry;
mod rotate;
mod sandbox_trace;
mod server;
pub use sandbox_trace::{
    SandboxTraceContext, record_guest_service_outcome, record_guest_service_status,
};
mod status;
mod trace;

pub use client::GrpcClient;
pub use connect::{
    UdsConnectError, connect_tcp_channel_with_retry, connect_tcp_channel_with_retrying_redials,
    connect_uds_channel, connect_uds_channel_lazy, connect_uds_channel_with_endpoint,
    connect_uds_channel_with_endpoint_with_retry, connect_uds_channel_with_retry,
};
pub use context::GrpcRetryContext;
pub use idempotency::generated_idempotency_key;
pub use policy::{
    DEFAULT_GRPC_CONNECT_RETRY_POLICY, DEFAULT_LOCAL_GRPC_RETRY_POLICY,
    DEFAULT_TCP_GRPC_CONNECT_RETRY_POLICY, GrpcRetryPolicy,
};
pub use queued_slots::{QueuedSlotWait, QueuedSlots, SlotRequest};
pub use request_admission::{RequestAdmission, RequestPermit};
pub use request_body::PreserveInputErrors;
pub use retry::{
    is_client_connect_failure, is_temporary_tcp_failure, retry_grpc_status_with_context,
    retry_grpc_status_with_context_if, should_reconnect_status,
};
pub use rotate::{RotatingChannel, max_connection_age_from_env};
pub use server::{GrpcServer, grpc_server_builder};
pub use status::{
    DESIRED_STATE_APPLY_TERMINAL_METADATA, GuestCapability, GuestCapabilityUnsupported,
    GuestServiceUnavailable, RETRY_AFTER_MS_METADATA, RateLimited, SandboxServiceRouteNotReady,
    UdsPerSandboxLimitExceeded, guest_capability_unsupported_status, guest_service_connect_status,
    is_desired_state_apply_terminal, mark_desired_state_apply_terminal, rate_limited_status,
    retry_after, sandbox_service_route_not_ready_status, uds_per_sandbox_limit_exceeded_status,
    with_retry_after,
};
pub use trace::{
    CLIENT_SERVICE_METADATA_KEY, GrpcClientTrace, current_service_name, set_current_service_name,
    trace_grpc_client_transport,
};
