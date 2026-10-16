//! Sandbox health: probes, status, transitions, raw vsock checks and evidence reports.

use super::*;

/// Count of local sandboxes by guest health category and node-local lifecycle.
/// Do not attach sandbox IDs or other per-sandbox identifiers to this gauge.
pub fn guest_health(value: usize, lifecycle: &str, health: &str) {
    gauge(
        "sandbox.guest.health",
        value as f64,
        &[tag("lifecycle", lifecycle), tag("health", health)],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthComponent {
    Firecracker,
    SpacedHost,
    SpacedGuest,
    NetGateway,
}

impl SandboxHealthComponent {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Firecracker => "firecracker",
            Self::SpacedHost => "spaced_host",
            Self::SpacedGuest => "spaced_guest",
            Self::NetGateway => "net_gateway",
        }
    }
}

impl fmt::Display for SandboxHealthComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthProbeOutcome {
    Success,
    Timeout,
    Unavailable,
    UnexpectedState,
    NotApplicable,
}

impl SandboxHealthProbeOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Timeout => "timeout",
            Self::Unavailable => "unavailable",
            Self::UnexpectedState => "unexpected_state",
            Self::NotApplicable => "not_applicable",
        }
    }
}

impl fmt::Display for SandboxHealthProbeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthStatus {
    Unspecified,
    Initializing,
    Healthy,
    Degraded,
}

impl SandboxHealthStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Initializing => "initializing",
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
        }
    }
}

impl fmt::Display for SandboxHealthStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn sandbox_health_probe(
    component: SandboxHealthComponent,
    outcome: SandboxHealthProbeOutcome,
    latency_micros: u64,
) {
    let tags = [
        tag("component", component.as_str()),
        tag("outcome", outcome.as_str()),
    ];
    counter("sandbox.health.probe", 1, &tags);
    timing_ms(
        "sandbox.health.probe.duration_ms",
        latency_micros as f64 / 1_000.0,
        &tags,
    );
}

pub fn sandbox_health_status(value: usize, lifecycle: &str, status: SandboxHealthStatus) {
    gauge(
        "sandbox.health.status",
        value as f64,
        &[tag("lifecycle", lifecycle), tag("status", status.as_str())],
    );
}

pub fn sandbox_health_transition(status: SandboxHealthStatus) {
    counter(
        "sandbox.health.transition",
        1,
        &[tag("status", status.as_str())],
    );
}

/// Result of the bare `CONNECT` handshake the sandbox manager sends to a VM's
/// spaced-guest vsock port once per degrading health transition. It separates
/// a guest kernel that still services vsock (`accepted`, `refused`) from one
/// that does not (`timeout`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxRawVsockCheckOutcome {
    /// Firecracker relayed the guest kernel's acceptance (`OK <port>`).
    Accepted,
    /// The connection closed before an acknowledgement: the guest kernel
    /// reset it, so nothing accepts on the port or its backlog is full.
    Refused,
    /// Firecracker answered with something other than an acknowledgement.
    UnexpectedReply,
    /// No answer within the check's deadline.
    Timeout,
    /// The VM's host-side vsock socket could not be connected.
    HostSocketUnavailable,
    /// The sandbox has no recorded vsock socket to check.
    NotAttempted,
}

impl SandboxRawVsockCheckOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Refused => "refused",
            Self::UnexpectedReply => "unexpected_reply",
            Self::Timeout => "timeout",
            Self::HostSocketUnavailable => "host_socket_unavailable",
            Self::NotAttempted => "not_attempted",
        }
    }
}

impl fmt::Display for SandboxRawVsockCheckOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One raw vsock check run for a sandbox whose composed or guest health just
/// degraded, tagged only by its outcome. Per-sandbox detail is in the
/// sandbox manager's health-transition log line, never in tags.
pub fn sandbox_health_raw_vsock_check(outcome: SandboxRawVsockCheckOutcome) {
    counter(
        "sandbox.health.raw_vsock_check",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}

/// What became of the evidence report a degrading health transition asks the
/// sandbox manager for. Each such transition counts exactly once: when its
/// report is merged or dropped at admission, or when an admitted report is
/// logged.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SandboxHealthEvidenceOutcome {
    /// The sandbox already had a report waiting for a collector, which now
    /// also covers this transition.
    Merged,
    /// The evidence queue was full; only the transition line was logged.
    Dropped,
    /// Collected in full before the collection deadline.
    Complete,
    /// The collection deadline expired; what was gathered was logged.
    Partial,
}

impl SandboxHealthEvidenceOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Merged => "merged",
            Self::Dropped => "dropped",
            Self::Complete => "complete",
            Self::Partial => "partial",
        }
    }
}

impl fmt::Display for SandboxHealthEvidenceOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One degrading health transition's evidence report, tagged only by its
/// outcome. A rising `dropped` or `partial` share means evidence is being
/// shed under correlated failures or slow host reads.
pub fn sandbox_health_evidence_report(outcome: SandboxHealthEvidenceOutcome) {
    counter(
        "sandbox.health.evidence_report",
        1,
        &[tag("outcome", outcome.as_str())],
    );
}
