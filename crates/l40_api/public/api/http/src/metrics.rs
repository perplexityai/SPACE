use std::fmt;

use pplx_space_metrics_emit::{counter, tag};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseOrigin {
    /// The request was rejected before SPACE accepted it as valid work.
    Client,
    /// The public API service itself failed before reaching another SPACE layer.
    Api,
    /// The data-plane service itself failed before reaching another SPACE layer.
    DataPlane,
    /// Cluster/control-plane route, lifecycle, or authorization lookup failed.
    Cluster,
    /// The target node or node-api backend could not complete the request.
    Node,
    /// Node-api reached spaced, but spaced or the guest bridge returned failure.
    Spaced,
    /// SPACE proxied successfully far enough to return sandbox application traffic.
    SandboxTraffic,
}

impl ResponseOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Client => "client",
            Self::Api => "api",
            Self::DataPlane => "data_plane",
            Self::Cluster => "cluster",
            Self::Node => "node",
            Self::Spaced => "spaced",
            Self::SandboxTraffic => "sandbox_traffic",
        }
    }
}

impl fmt::Display for ResponseOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResponseKind {
    /// Request was rejected by a local capacity or concurrency gate.
    Admission,
    /// Authentication or authorization failed at the request boundary.
    Auth,
    /// Backend returned a structured status after accepting the RPC.
    BackendStatus,
    /// Backend or sandbox-service transport failed before a reliable status was returned.
    BackendTransport,
    /// SPACE hit an invariant violation or unexpected internal error.
    Internal,
    /// SPACE returned an HTTP status produced by proxied sandbox traffic.
    ProxiedStatus,
    /// Cluster route lookup failed before a usable route was returned.
    RouteResolution,
    /// Cluster route lookup succeeded but no usable backend route was available.
    RouteUnavailable,
    /// Route or runtime exists but is still converging and should be retried.
    RouteNotReady,
    /// A bounded SPACE-layer operation exceeded its request budget.
    Timeout,
    /// The request shape or parameters were invalid.
    Validation,
}

impl ResponseKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admission => "admission",
            Self::Auth => "auth",
            Self::BackendStatus => "backend_status",
            Self::BackendTransport => "backend_transport",
            Self::Internal => "internal",
            Self::ProxiedStatus => "proxied_status",
            Self::RouteResolution => "route_resolution",
            Self::RouteUnavailable => "route_unavailable",
            Self::RouteNotReady => "route_not_ready",
            Self::Timeout => "timeout",
            Self::Validation => "validation",
        }
    }
}

impl fmt::Display for ResponseKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataPlaneResponseSurface {
    Ecw,
    Native,
}

impl DataPlaneResponseSurface {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => "ecw",
            Self::Native => "native",
        }
    }
}

impl fmt::Display for DataPlaneResponseSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn data_plane(
    surface: DataPlaneResponseSurface,
    origin: ResponseOrigin,
    kind: ResponseKind,
    http_status: u16,
) {
    counter(
        "data_plane.response",
        1,
        &[
            tag("surface", surface.as_str()),
            tag("origin", origin.as_str()),
            tag("kind", kind.as_str()),
            tag("http_status", http_status.to_string()),
        ],
    );
}

pub fn public_api(origin: ResponseOrigin, kind: ResponseKind, http_status: u16) {
    counter(
        "public_api.response",
        1,
        &[
            tag("origin", origin.as_str()),
            tag("kind", kind.as_str()),
            tag("http_status", http_status.to_string()),
        ],
    );
}

pub fn ecw_api(origin: ResponseOrigin, kind: ResponseKind, http_status: u16) {
    counter(
        "ecw_api.response",
        1,
        &[
            tag("origin", origin.as_str()),
            tag("kind", kind.as_str()),
            tag("http_status", http_status.to_string()),
        ],
    );
}
