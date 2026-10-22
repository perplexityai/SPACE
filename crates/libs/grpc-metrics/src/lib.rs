//! gRPC client and server metrics, in their own crate so that the generic gRPC
//! infrastructure crates can emit them without depending on
//! `pplx-space-metrics`.
//!
//! The per-measurement `service` tag names the peer for client metrics and
//! the served interface for rate-limit metrics. OTLP exports it as
//! `service.role`, separately from process identity; native DogStatsD retains
//! the legacy `service` tag.

#[macro_use(metric_vocabulary)]
extern crate pplx_space_metrics_emit;

use pplx_space_metrics_emit::{counter, distribution, gauge, tag};

/// Untagged: identity comes from the constant process tags.
pub fn server_active_requests(depth: usize) {
    distribution("grpc.server.active_requests", depth as f64, &[]);
}

pub fn client_retry(service: &str, command: &str, phase: &str, reason: &str) {
    counter(
        "grpc.client.retry",
        1,
        &[
            tag("service", service),
            tag("command", command),
            tag("phase", phase),
            tag("reason", reason),
        ],
    );
}

metric_vocabulary! {
    /// What became of a client's replay of an RPC whose failure proved it was
    /// never sent.
    ClientReplayOutcome {
        /// The replay, on a fresh connection, succeeded.
        ReplayedOk => "replayed_ok",
        /// The replay was attempted and failed; that failure is the call's.
        ReplayedError => "replayed_error",
        /// No fresh connection to replay on: the peer could not be resolved
        /// again, or resolving it failed. The first failure is the call's.
        NoFreshClient => "no_fresh_client",
        /// The call's deadline expired before or during the replay.
        DeadlineExpired => "deadline_expired",
    }
}

/// `grpc.client.replay`: one per decision to replay an RPC whose first
/// attempt failed before any request bytes were sent, so that replaying it is
/// safe whatever the RPC does, mutations included. `cause` is the proof the
/// failure carried, as `pplx_space_common_grpc::ClientConnectFailureCause`
/// names it (`connect_phase`, `refused` or `not_found`).
pub fn client_replay(service: &str, cause: &str, outcome: ClientReplayOutcome) {
    counter(
        "grpc.client.replay",
        1,
        &[
            tag("service", service),
            tag("cause", cause),
            tag("outcome", outcome.as_str()),
        ],
    );
}

pub fn client_channel_cache(service: &str, outcome: &str) {
    counter(
        "grpc.client.channel_cache",
        1,
        &[tag("service", service), tag("outcome", outcome)],
    );
}

/// Outcome of a client retiring a connection that reached its maximum age.
pub fn client_connection_rotation(service: &str, outcome: &str) {
    counter(
        "grpc.client.connection_rotation",
        1,
        &[tag("service", service), tag("outcome", outcome)],
    );
}

/// Endpoints a client is currently balancing over.
pub fn client_lb_endpoints(service: &str, count: usize) {
    gauge(
        "grpc.client.lb.endpoints",
        count as f64,
        &[tag("service", service)],
    );
}

/// Whether a client is serving through its Service-address fallback.
pub fn client_lb_fallback(service: &str, active: bool) {
    gauge(
        "grpc.client.lb.fallback",
        u8::from(active).into(),
        &[tag("service", service)],
    );
}

pub fn server_rate_limiter_request(
    service: &str,
    bucket: &str,
    grpc_method: &str,
    decision: &str,
    reason: &str,
) {
    counter(
        "grpc.server.rate_limiter.requests",
        1,
        &[
            tag("service", service),
            tag("bucket", bucket),
            tag("grpc_method", grpc_method),
            tag("decision", decision),
            tag("reason", reason),
        ],
    );
}

pub fn server_rate_limiter_in_flight(service: &str, bucket: &str, value: f64) {
    gauge(
        "grpc.server.rate_limiter.in_flight",
        value,
        &[tag("service", service), tag("bucket", bucket)],
    );
}

/// Rate-bounded physical-listener admission diagnostics.
///
/// `listener`, `event`, and `reason` are bounded by the connection-admission
/// crate. The emitter deliberately reports diagnostic samples rather than
/// every occurrence so journald-backed sidecars cannot amplify accept-error
/// or capacity pressure into unbounded log traffic.
pub fn server_connection_admission(listener: &str, event: &str, reason: &str) {
    counter(
        "grpc.server.connection_admission",
        1,
        &[
            tag("listener", listener),
            tag("event", event),
            tag("reason", reason),
        ],
    );
}
