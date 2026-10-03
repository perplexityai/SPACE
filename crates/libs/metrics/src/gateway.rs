use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::emit::{counter, distribution, gauge, tag, timing_ms};

pub mod readiness;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressOpenResult {
    Ok,
    InvalidStart,
    PolicyNotReady,
    MissingToken,
    InvalidToken,
    VerifierSecretUnset,
    Expired,
    SandboxMismatch,
    PurposeMismatch,
    ServicePortMismatch,
    IdentityMissing,
    IdentityCheckDisabled,
    RouteGenerationMismatch,
    NodeMismatch,
    SubstrateMismatch,
    ExecutionGenerationMismatch,
    ActivationAttemptMismatch,
    PodUidMismatch,
    UpstreamConnectError,
    GuestConnectionRefused,
    CapacityExceeded,
    StartTimeout,
}

impl ServicePortIngressOpenResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::InvalidStart => "invalid_start",
            Self::PolicyNotReady => "policy_not_ready",
            Self::MissingToken => "missing_token",
            Self::InvalidToken => "invalid_token",
            Self::VerifierSecretUnset => "verifier_secret_unset",
            Self::Expired => "expired",
            Self::SandboxMismatch => "sandbox_mismatch",
            Self::PurposeMismatch => "purpose_mismatch",
            Self::ServicePortMismatch => "service_port_mismatch",
            Self::IdentityMissing => "identity_missing",
            Self::IdentityCheckDisabled => "identity_check_disabled",
            Self::RouteGenerationMismatch => "route_generation_mismatch",
            Self::NodeMismatch => "node_mismatch",
            Self::SubstrateMismatch => "substrate_mismatch",
            Self::ExecutionGenerationMismatch => "execution_generation_mismatch",
            Self::ActivationAttemptMismatch => "activation_attempt_mismatch",
            Self::PodUidMismatch => "pod_uid_mismatch",
            Self::GuestConnectionRefused => "guest_connection_refused",
            Self::UpstreamConnectError => "upstream_connect_error",
            Self::CapacityExceeded => "capacity_exceeded",
            Self::StartTimeout => "start_timeout",
        }
    }
}

impl fmt::Display for ServicePortIngressOpenResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PolicyDecisionKind {
    DnsAttributed,
}

impl PolicyDecisionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DnsAttributed => "dns_attributed",
        }
    }
}

pub fn policy_decision(
    kind: PolicyDecisionKind,
    protocol: &str,
    phase: &str,
    policy_version: &str,
    decision: &str,
    interception: &str,
) {
    counter(
        "gateway.policy.decision",
        1,
        &[
            tag("kind", kind.as_str()),
            tag("protocol", protocol),
            tag("phase", phase),
            tag("policy_version", policy_version),
            tag("decision", decision),
            tag("interception", interception),
        ],
    );
}

/// Result of binding a domain-derived TCP permission to the hostname stated by
/// TLS SNI or by each framed plaintext HTTP request.
pub fn domain_binding(transport: &str, evidence: &str, outcome: &str, detail: Option<&str>) {
    let mut tags = vec![
        tag("transport", transport),
        tag("evidence", evidence),
        tag("outcome", outcome),
    ];
    if let Some(detail) = detail {
        tags.push(tag("detail", detail));
    }
    counter("gateway.policy.domain_binding", 1, &tags);
}

/// The two read contracts served by the DNS attribution cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsAttributionCacheLookupKind {
    /// Whether an address has any live attribution still allowed by policy.
    IpAllow,
    /// Whether an address has a live attribution for one exact host.
    HostBinding,
}

impl DnsAttributionCacheLookupKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::IpAllow => "ip_allow",
            Self::HostBinding => "host_binding",
        }
    }
}

/// One DNS attribution-cache read, without exposing the address or hostname.
pub fn dns_attribution_cache_lookup(kind: DnsAttributionCacheLookupKind, hit: bool) {
    counter(
        "gateway.dns_attribution_cache.lookup",
        1,
        &[
            tag("kind", kind.as_str()),
            tag("result", if hit { "hit" } else { "miss" }),
        ],
    );
}

/// Occupied UDP slots sampled immediately after each reserved-DNS admission attempt.
pub fn reserved_dns_udp_in_flight(active: usize) {
    distribution(
        "gateway.reserved_dns.udp.in_flight",
        active as f64,
        &[tag("git_sha", pplx_space_observability::git_sha())],
    );
}

/// SPACE lookup occupancy sampled when Hickory reports Busy.
pub fn reserved_dns_upstream_busy_lookup_in_flight(active: usize) {
    distribution(
        "gateway.reserved_dns.upstream_busy.lookup_in_flight",
        active as f64,
        &[tag("git_sha", pplx_space_observability::git_sha())],
    );
}

/// One reserved-DNS duration sample; its count gives request outcomes.
pub fn reserved_dns_request_outcome(
    resolver: &str,
    qtype: &str,
    outcome: &str,
    reason: &str,
    upstream_rcode: &str,
    latency_ms: f64,
) {
    let tags = &[
        tag("resolver", resolver),
        tag("qtype", qtype),
        tag("outcome", outcome),
        tag("reason", reason),
        tag("upstream_rcode", upstream_rcode),
        tag("git_sha", pplx_space_observability::git_sha()),
    ];
    timing_ms("gateway.reserved_dns.request.duration_ms", latency_ms, tags);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsAttributionCacheUpdateResult {
    Insert,
    Refresh,
}

impl DnsAttributionCacheUpdateResult {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "insert",
            Self::Refresh => "refresh",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsAttributionCacheObservationSource {
    ReservedDns,
}

impl DnsAttributionCacheObservationSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReservedDns => "reserved_dns",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservedDnsCacheOutcome {
    Hit,
    Miss,
}

impl ReservedDnsCacheOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Miss => "miss",
        }
    }
}

/// One DNS response inspected as possible attribution evidence.
pub fn dns_attribution_cache_observation(
    source: DnsAttributionCacheObservationSource,
    accepted: bool,
) {
    counter(
        "gateway.dns_attribution_cache.observation",
        1,
        &[
            tag("source", source.as_str()),
            tag("result", if accepted { "accepted" } else { "ignored" }),
        ],
    );
}

/// DNS attribution bindings inserted or refreshed by observed responses.
pub fn dns_attribution_cache_update(result: DnsAttributionCacheUpdateResult, entries: usize) {
    if entries == 0 {
        return;
    }
    counter(
        "gateway.dns_attribution_cache.update",
        entries as u64,
        &[tag("result", result.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DnsAttributionCacheEvictionReason {
    Expired,
    Policy,
    PerIpCapacity,
    GlobalCapacity,
}

impl DnsAttributionCacheEvictionReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Expired => "expired",
            Self::Policy => "policy",
            Self::PerIpCapacity => "per_ip_capacity",
            Self::GlobalCapacity => "global_capacity",
        }
    }
}

/// Build the reserved-resolver Hickory clients with their native cache and
/// upstream-query counters routed to the reserved-DNS metric namespace.
pub fn with_reserved_dns_hickory_metrics<T>(build: impl FnOnce() -> T) -> T {
    metrics::with_local_recorder(&HickoryDnsMetricsRecorder, build)
}

struct HickoryDnsMetricsRecorder;

impl metrics::Recorder for HickoryDnsMetricsRecorder {
    fn describe_counter(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_gauge(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn describe_histogram(
        &self,
        _key: metrics::KeyName,
        _unit: Option<metrics::Unit>,
        _description: metrics::SharedString,
    ) {
    }

    fn register_counter(
        &self,
        key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Counter {
        let kind = match key.name() {
            "hickory_resolver_cache_hit_total" => HickoryDnsCounterKind::Cache {
                outcome: ReservedDnsCacheOutcome::Hit,
            },
            "hickory_resolver_cache_miss_total" => HickoryDnsCounterKind::Cache {
                outcome: ReservedDnsCacheOutcome::Miss,
            },
            "hickory_resolver_outgoing_queries_total" => {
                let Some(protocol) = HickoryDnsProtocol::from_key(key) else {
                    return metrics::Counter::noop();
                };
                HickoryDnsCounterKind::UpstreamQuery { protocol }
            }
            _ => return metrics::Counter::noop(),
        };
        metrics::Counter::from_arc(Arc::new(HickoryDnsCounter {
            kind,
            value: AtomicU64::new(0),
        }))
    }

    fn register_gauge(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(
        &self,
        _key: &metrics::Key,
        _metadata: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[derive(Clone, Copy)]
enum HickoryDnsCounterKind {
    Cache { outcome: ReservedDnsCacheOutcome },
    UpstreamQuery { protocol: HickoryDnsProtocol },
}

impl HickoryDnsCounterKind {
    fn emit(self, occurrences: u64) {
        match self {
            Self::Cache { outcome } => counter(
                "gateway.reserved_dns.dns_cache",
                occurrences,
                &[tag("outcome", outcome.as_str())],
            ),
            Self::UpstreamQuery { protocol } => counter(
                "gateway.reserved_dns.dns_upstream_query",
                occurrences,
                &[tag("protocol", protocol.as_str())],
            ),
        }
    }
}

#[derive(Clone, Copy)]
enum HickoryDnsProtocol {
    Udp,
    Tcp,
    Tls,
    Https,
    Quic,
    Http3,
}

impl HickoryDnsProtocol {
    fn from_key(key: &metrics::Key) -> Option<Self> {
        let protocol = key
            .labels()
            .find(|label| label.key() == "protocol")?
            .value();
        match protocol {
            "udp" => Some(Self::Udp),
            "tcp" => Some(Self::Tcp),
            "tls" => Some(Self::Tls),
            "https" => Some(Self::Https),
            "quic" => Some(Self::Quic),
            "http3" => Some(Self::Http3),
            _ => None,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Udp => "udp",
            Self::Tcp => "tcp",
            Self::Tls => "tls",
            Self::Https => "https",
            Self::Quic => "quic",
            Self::Http3 => "http3",
        }
    }
}

struct HickoryDnsCounter {
    kind: HickoryDnsCounterKind,
    value: AtomicU64,
}

impl metrics::CounterFn for HickoryDnsCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.kind.emit(value);
    }

    fn absolute(&self, value: u64) {
        let previous = self.value.fetch_max(value, Ordering::Relaxed);
        if value > previous {
            self.kind.emit(value - previous);
        }
    }
}

/// DNS attribution bindings removed from the cache, grouped by bounded cause.
pub fn dns_attribution_cache_eviction(reason: DnsAttributionCacheEvictionReason, entries: usize) {
    if entries == 0 {
        return;
    }
    counter(
        "gateway.dns_attribution_cache.eviction",
        entries as u64,
        &[tag("reason", reason.as_str())],
    );
}

/// Current live `(address, domain)` attribution bindings in one gateway.
pub fn dns_attribution_cache_entries(entries: usize) {
    gauge("gateway.dns_attribution_cache.entries", entries as f64, &[]);
}

pub fn service_port_ingress_open(result: ServicePortIngressOpenResult) {
    counter(
        "gateway.service_port_ingress.open",
        1,
        &[tag("result", result.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServicePortIngressCloseReason {
    IdentityChanged,
    IdleTimeout,
    TransportLoss,
    ClientClosed,
    ServiceShutdown,
    ProtocolError,
}

impl ServicePortIngressCloseReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::IdentityChanged => "identity_changed",
            Self::IdleTimeout => "idle_timeout",
            Self::TransportLoss => "transport_loss",
            Self::ClientClosed => "client_closed",
            Self::ServiceShutdown => "service_shutdown",
            Self::ProtocolError => "protocol_error",
        }
    }
}

impl fmt::Display for ServicePortIngressCloseReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

pub fn service_port_ingress_closed(reason: ServicePortIngressCloseReason) {
    counter(
        "gateway.service_port_ingress.closed",
        1,
        &[tag("reason", reason.as_str())],
    );
}

/// Wall-clock cost of reaching a single egress verdict, tagged by transport and
/// `phase` so the per-flow admission path and the policy-install resweep burst
/// are distinguishable. Recorded in fractional milliseconds because the decision
/// is typically sub-millisecond; integer-millisecond rounding would collapse it
/// to zero.
pub fn policy_decision_latency_ms(protocol: &str, phase: &str, value_ms: f64) {
    timing_ms(
        "gateway.policy.decision_latency_ms",
        value_ms,
        &[tag("protocol", protocol), tag("phase", phase)],
    );
}

pub fn tcp_accept_error(reason: &str) {
    counter("gateway.tcp.accept_error", 1, &[tag("reason", reason)]);
}

pub fn tcp_accept_backoff(reason: &str) {
    counter("gateway.tcp.accept_backoff", 1, &[tag("reason", reason)]);
}

pub fn control_accept_error(reason: &str) {
    counter("gateway.control.accept_error", 1, &[tag("reason", reason)]);
}

pub fn control_accept_backoff(reason: &str) {
    counter(
        "gateway.control.accept_backoff",
        1,
        &[tag("reason", reason)],
    );
}

pub fn control_connection_limit_wait() {
    counter("gateway.control.connection_limit_wait", 1, &[]);
}

pub fn tcp_connection_limit_wait() {
    counter("gateway.tcp.connection_limit_wait", 1, &[]);
}

/// Occupied TCP slots sampled immediately after each non-blocking admission attempt.
pub fn tcp_in_flight(active: usize) {
    distribution(
        "gateway.tcp.in_flight",
        active as f64,
        &[tag("git_sha", pplx_space_observability::git_sha())],
    );
}

/// Emitted exactly once for every TCP connection that reaches the proxy's
/// per-connection handler. `detail` is the bounded detail within `status`.
pub fn tcp_connection(status: &str, detail: &str) {
    counter(
        "gateway.tcp.connection",
        1,
        &[tag("status", status), tag("detail", detail)],
    );
}

/// One terminal inspection-admission outcome per accepted non-DNS TCP flow.
pub fn inspection_admission(outcome: &str, reason: &str) {
    counter(
        "gateway.inspection.admission",
        1,
        &[tag("outcome", outcome), tag("reason", reason)],
    );
}

/// One terminal authority and policy outcome per inspected HTTP request.
pub fn http_authorization(protocol: &str, outcome: &str, reason: &str) {
    counter(
        "gateway.http.authorization",
        1,
        &[
            tag("protocol", protocol),
            tag("outcome", outcome),
            tag("reason", reason),
        ],
    );
}

/// One terminal outcome per lazy inspected-origin TCP connection attempt.
pub fn origin_tcp_connect(outcome: &str, reason: &str) {
    counter(
        "gateway.origin.tcp_connect",
        1,
        &[tag("outcome", outcome), tag("reason", reason)],
    );
}

/// Lifetime of one bidirectional relay. `closed_by` names the peer that
/// reached end-of-stream first, or `none` when the gateway tore the relay down,
/// which separates a normal client disconnect from a gateway-initiated close.
pub fn relay_duration_ms(protocol: &str, outcome: &str, closed_by: &str, value_ms: f64) {
    timing_ms(
        "gateway.relay.duration_ms",
        value_ms,
        &[
            tag("protocol", protocol),
            tag("outcome", outcome),
            tag("closed_by", closed_by),
        ],
    );
}

/// One terminal transport outcome for a reached inspected HTTP forwarding operation.
pub fn forwarding(protocol: &str, operation: &str, outcome: &str, reason: &str) {
    counter(
        "gateway.forwarding",
        1,
        &[
            tag("protocol", protocol),
            tag("operation", operation),
            tag("outcome", outcome),
            tag("reason", reason),
        ],
    );
}

/// Bytes proxied on an established TCP flow, tagged by `direction` (`tx` guest to
/// destination, `rx` destination to guest). Accumulated per connection on close,
/// so the rate is the gateway's forwarded throughput.
pub fn tcp_bytes(direction: &str, bytes: u64) {
    counter("gateway.tcp.bytes", bytes, &[tag("direction", direction)]);
}

/// One terminal outcome per inspected-leaf certificate lookup. Cache reuse and
/// each refusal share one `result` space so a CA outage, a capacity limit and a
/// rate limit never collapse into a single undifferentiated failure bucket.
pub fn certificate_lookup(result: &str) {
    counter(
        "gateway.tls.certificate.lookup",
        1,
        &[
            tag("result", result),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

/// Cost of generating a leaf key and signing it against the gateway CA. Signing
/// runs on a blocking worker, so this is the latency a guest handshake inherits
/// on a cache miss.
pub fn certificate_signing_duration_ms(result: &str, value_ms: f64) {
    timing_ms(
        "gateway.tls.certificate.signing.duration_ms",
        value_ms,
        &[
            tag("result", result),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

/// A leaf-cache mutation that discards material a later lookup could have
/// reused. A sustained `evicted` rate means the fixed cache bound, not the CA,
/// is forcing re-signing.
pub fn certificate_cache_event(event: &str) {
    counter("gateway.tls.certificate.cache", 1, &[tag("event", event)]);
}

/// Cached leaves against the gateway's fixed cache bound.
pub fn certificate_cache_entries(entries: usize, capacity: usize) {
    gauge(
        "gateway.tls.certificate.cache.entries",
        entries as f64,
        &[tag("capacity", capacity)],
    );
}

/// One completed TLS handshake operation. `leg` names the two peers directly.
pub fn tls_handshake(leg: &str, outcome: &str, reason: &str, alert: &str) {
    counter(
        "gateway.tls.handshake",
        1,
        &[
            tag("leg", leg),
            tag("outcome", outcome),
            tag("reason", reason),
            tag("alert", alert),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

/// Time spent in one handshake operation, from permit acquisition to its
/// terminal outcome. Separates a slow handshake from a slow origin dial, which
/// previously shared one ten-second budget and one `timeout` reason.
pub fn tls_handshake_duration_ms(leg: &str, outcome: &str, value_ms: f64) {
    timing_ms(
        "gateway.tls.handshake.duration_ms",
        value_ms,
        &[
            tag("leg", leg),
            tag("outcome", outcome),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

/// Time spent dialling the origin, ending at the same boundary as
/// `gateway.origin.tcp_connect`.
pub fn origin_tcp_connect_duration_ms(outcome: &str, value_ms: f64) {
    timing_ms(
        "gateway.origin.tcp_connect.duration_ms",
        value_ms,
        &[tag("outcome", outcome)],
    );
}

/// In-flight TLS handshakes in one gateway's shared pool or on one pool leg.
pub fn tls_handshake_in_flight(leg: &str, active: usize, capacity: usize) {
    distribution(
        "gateway.tls.handshake.in_flight",
        active as f64,
        &[
            tag("leg", leg),
            tag("capacity", capacity),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

/// One TLS-protocol error after a successful handshake.
pub fn tls_connection_error(leg: &str, reason: &str) {
    counter(
        "gateway.tls.connection_error",
        1,
        &[
            tag("leg", leg),
            tag("reason", reason),
            tag("git_sha", pplx_space_observability::git_sha()),
        ],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UdpDatagramRoute {
    ReservedDns,
    GenericDns,
    Generic,
}

impl UdpDatagramRoute {
    const fn as_str(self) -> &'static str {
        match self {
            Self::ReservedDns => "reserved_dns",
            Self::GenericDns => "generic_dns",
            Self::Generic => "generic",
        }
    }
}

/// One UDP datagram evaluated against a ready network policy.
pub fn udp_datagram(route: UdpDatagramRoute, admitted: bool) {
    counter(
        "gateway.udp.datagram",
        1,
        &[
            tag("route", route.as_str()),
            tag("decision", if admitted { "allow" } else { "deny" }),
        ],
    );
}

/// Emitted exactly once when an admitted UDP flow mapping's response task
/// terminates. `detail` is the bounded detail within `status`.
pub fn udp_connection(status: &str, detail: &str) {
    counter(
        "gateway.udp.connection",
        1,
        &[tag("status", status), tag("detail", detail)],
    );
}

pub fn udp_socket_error(operation: &str) {
    counter(
        "gateway.udp.socket_error",
        1,
        &[tag("operation", operation)],
    );
}

pub fn udp_session_limit_drop() {
    counter("gateway.udp.session_limit_drop", 1, &[]);
}

pub fn udp_session_capacity_eviction() {
    counter("gateway.udp.session_capacity_eviction", 1, &[]);
}

pub fn udp_session_event_drop(reason: &str) {
    counter(
        "gateway.udp.session_event_drop",
        1,
        &[tag("reason", reason)],
    );
}

pub fn udp_session_create_error(stage: &str, reason: &str) {
    counter(
        "gateway.udp.session_create_error",
        1,
        &[tag("stage", stage), tag("reason", reason)],
    );
}

pub fn udp_active_sessions(active: u64) {
    gauge("gateway.udp.active_sessions", active as f64, &[]);
}

/// Outcome of sniffing a TCP flow's leading bytes for a TLS ClientHello SNI.
/// The closed set of `outcome` tag values for [`tls_client_hello`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsClientHelloOutcome {
    /// A host name was extracted from the SNI extension.
    Sni,
    /// A ClientHello without a usable SNI.
    NoSni,
    /// The flow is not a TLS handshake (e.g. plaintext HTTP).
    NotTls,
    /// Too few bytes were peeked before the deadline.
    Incomplete,
    /// Handshake bytes that failed to parse as a valid ClientHello.
    Malformed,
}

impl TlsClientHelloOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sni => "sni",
            Self::NoSni => "no_sni",
            Self::NotTls => "not_tls",
            Self::Incomplete => "incomplete",
            Self::Malformed => "malformed",
        }
    }
}

/// Emitted once per admitted TCP connection.
/// Cost of peeking and parsing the ClientHello. This wait precedes every
/// admitted flow, intercepted or not, so a regression here is latency added to
/// traffic that interception never touches.
pub fn tls_client_hello_duration_ms(outcome: TlsClientHelloOutcome, value_ms: f64) {
    timing_ms(
        "gateway.tls.client_hello.duration_ms",
        value_ms,
        &[tag("outcome", outcome.as_str())],
    )
}

pub fn tls_client_hello(outcome: TlsClientHelloOutcome, client_alpn: &str, detail: &str) {
    counter(
        "gateway.tls.client_hello",
        1,
        &[
            tag("outcome", outcome.as_str()),
            tag("client_alpn", client_alpn),
            tag("detail", detail),
        ],
    );
}

pub fn network_state_transition(from: &str, to: &str, reason: &str) {
    counter(
        "gateway.network_state.transition",
        1,
        &[tag("from", from), tag("to", to), tag("reason", reason)],
    );
}

pub fn policy_hydration_error(stage: &str) {
    counter("gateway.policy.hydration_error", 1, &[tag("stage", stage)]);
}

pub fn policy_hydration_latency(result: &str, value_ms: f64) {
    timing_ms(
        "gateway.policy.hydration_latency",
        value_ms,
        &[tag("result", result)],
    );
}

pub fn policy_hydration_retry(reason: &str) {
    counter(
        "gateway.policy.hydration_retry",
        1,
        &[tag("reason", reason)],
    );
}

pub fn policy_hydration_consecutive_failures(kind: &str, failures: u64) {
    gauge(
        "gateway.policy.hydration_consecutive_failures",
        failures as f64,
        &[tag("kind", kind)],
    );
}

pub fn policy_ref(source: &str, state: &str) {
    gauge(
        "gateway.policy.ref",
        1.0,
        &[tag("source", source), tag("state", state)],
    );
}

pub fn pending_timeout(protocol: &str) {
    counter(
        "gateway.policy.pending_timeout",
        1,
        &[tag("protocol", protocol)],
    );
}

pub fn pending_tcp_wait(result: &str) {
    counter("gateway.pending_tcp.wait", 1, &[tag("result", result)]);
}

pub fn pending_tcp_wait_ms(result: &str, value_ms: f64) {
    timing_ms(
        "gateway.pending_tcp.wait_ms",
        value_ms,
        &[tag("result", result)],
    );
}

pub fn pending_tcp_slow_wait(source: &str) {
    counter("gateway.pending_tcp.slow_wait", 1, &[tag("source", source)]);
}

pub fn udp_pending_buffer(result: &str) {
    counter("gateway.udp.pending_buffer", 1, &[tag("result", result)]);
}

pub fn pending_udp_datagram(result: &str) {
    pending_udp_datagrams(result, 1);
}

pub fn pending_udp_datagrams(result: &str, datagrams: u64) {
    counter(
        "gateway.pending_udp.datagram",
        datagrams,
        &[tag("result", result)],
    );
}

pub fn pending_udp_bytes_dropped(reason: &str, bytes: u64) {
    counter(
        "gateway.pending_udp.bytes_dropped",
        bytes,
        &[tag("reason", reason)],
    );
}

pub fn pending_udp_flow_dropped(reason: &str) {
    counter(
        "gateway.pending_udp.flow_dropped",
        1,
        &[tag("reason", reason)],
    );
}

pub fn udp_pending(datagrams: u64, bytes: u64) {
    gauge("gateway.udp.pending_datagrams", datagrams as f64, &[]);
    gauge("gateway.udp.pending_bytes", bytes as f64, &[]);
}

pub fn no_identity_denied(protocol: &str) {
    counter(
        "gateway.no_identity.denied",
        1,
        &[tag("protocol", protocol)],
    );
}

pub fn draining_closed(protocol: &str) {
    counter("gateway.draining.closed", 1, &[tag("protocol", protocol)]);
}

#[cfg(test)]
mod tests;
