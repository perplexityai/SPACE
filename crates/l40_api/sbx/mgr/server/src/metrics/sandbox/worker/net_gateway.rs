//! The net-gateway as sbx-mgr sees it: route verification bundles, route security pushes, liveness,
//! inventory and sync cycles.

use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteVerificationBundleReloadResult {
    Updated,
    Unchanged,
    Absent,
    Missing,
    Invalid,
    Stale,
    Conflict,
}

impl RouteVerificationBundleReloadResult {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Updated => "updated",
            Self::Unchanged => "unchanged",
            Self::Absent => "absent",
            Self::Missing => "missing",
            Self::Invalid => "invalid",
            Self::Stale => "stale",
            Self::Conflict => "conflict",
        }
    }
}

impl fmt::Display for RouteVerificationBundleReloadResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn route_verification_bundle_reload(result: RouteVerificationBundleReloadResult) {
    counter(
        "sandbox.worker.route_verification_bundle.reload",
        1,
        &[tag("result", result.as_str())],
    );
}

pub fn route_verification_bundle_generation(generation: u64) {
    gauge(
        "sandbox.worker.route_verification_bundle.generation",
        generation as f64,
        &[],
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetGatewayRouteVerificationStatus {
    Disabled,
    Matched,
    Probing,
    Converging,
    Mismatched,
    Unsupported,
    Unknown,
}

impl NetGatewayRouteVerificationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Matched => "matched",
            Self::Probing => "probing",
            Self::Converging => "converging",
            Self::Mismatched => "mismatched",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
        }
    }
}

impl fmt::Display for NetGatewayRouteVerificationStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn net_gateway_route_verification(value: usize, status: NetGatewayRouteVerificationStatus) {
    gauge(
        "sandbox.worker.net_gateway.route_verification",
        value as f64,
        &[tag("status", status.as_str())],
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayRouteSecurityPushOperation {
    Identity,
    VerificationKeys,
}

impl NetGatewayRouteSecurityPushOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Identity => "identity",
            Self::VerificationKeys => "verification_keys",
        }
    }
}

impl fmt::Display for NetGatewayRouteSecurityPushOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayRouteSecurityPushOutcome {
    Applied,
    /// There is no local route identity to push at all.
    Absent,
    Diverged,
    Unsupported,
    Failed,
}

impl NetGatewayRouteSecurityPushOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Absent => "absent",
            Self::Diverged => "diverged",
            Self::Unsupported => "unsupported",
            Self::Failed => "failed",
        }
    }
}

impl fmt::Display for NetGatewayRouteSecurityPushOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn net_gateway_route_security_push(
    operation: NetGatewayRouteSecurityPushOperation,
    outcome: NetGatewayRouteSecurityPushOutcome,
) {
    counter(
        "sandbox.worker.net_gateway.route_security_push",
        1,
        &[
            tag("operation", operation.as_str()),
            tag("outcome", outcome.as_str()),
        ],
    );
}

/// `gateway_sha` value for a gateway running a binary that reports no sha, and
/// for one that has not answered a stats probe yet.
pub const NET_GATEWAY_SHA_UNKNOWN: &str = "unknown";

/// Placeholder for template metadata absent from the local sandbox row.
pub const NET_GATEWAY_INVENTORY_UNKNOWN: &str = "unknown";

/// Live net-gateway sidecars by gateway build and sandbox lifecycle.
pub fn net_gateway_live(value: usize, gateway_sha: &str, lifecycle: &str) {
    gauge(
        "sandbox.worker.net_gateway.live",
        value as f64,
        &[tag("gateway_sha", gateway_sha), tag("lifecycle", lifecycle)],
    );
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NetGatewayTlsInterceptionMode {
    Off,
    On,
    Unknown,
}

impl NetGatewayTlsInterceptionMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::On => "on",
            Self::Unknown => "unknown",
        }
    }
}

/// Live net-gateway sidecars by accepted TLS mode, gateway build, pinned
/// template content identity, and sandbox lifecycle.
pub fn net_gateway_inventory(
    value: usize,
    tls_interception_mode: NetGatewayTlsInterceptionMode,
    gateway_sha: &str,
    template_content_hash: &str,
    lifecycle: &str,
) {
    gauge(
        "sandbox.worker.net_gateway.inventory",
        value as f64,
        &[
            tag("tls_interception_mode", tls_interception_mode.as_str()),
            tag("gateway_sha", gateway_sha),
            tag("template_content_hash", template_content_hash),
            tag("lifecycle", lifecycle),
        ],
    );
}

/// Terminal outcome of one deliberate operator-triggered net-gateway cycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetGatewayCycleResult {
    Ok,
    Error,
}

impl NetGatewayCycleResult {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
        }
    }
}

impl fmt::Display for NetGatewayCycleResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One deliberate operator-triggered net-gateway cycle (kill + relaunch of the
/// gateway sidecar) reaching a terminal outcome.
pub fn net_gateway_cycle(result: NetGatewayCycleResult) {
    counter(
        "sandbox.worker.net_gateway.cycles",
        1,
        &[tag("result", result.as_str())],
    );
}
