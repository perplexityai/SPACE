//! The net-gateway wire contract: the generated `space.net.gateway.v1`
//! protobuf types and service stubs, with the `as_str` and `blocks_admission`
//! methods and `Display` impls on the `NetworkState`, `NetworkPolicyErrorKind`
//! and `InterceptionReadiness` enums, which Rust allows only in the crate that
//! defines those types.
//!
//! It is its own crate so code that needs only the contract does not link
//! `pplx-space-common-grpc` and, through it, the observability, metrics and
//! tracing-subscriber crates. `pplx-space-net-gateway-client` re-exports the
//! `proto` module as `pplx_space_net_gateway_client::proto`.

pub mod proto {
    tonic::include_proto!("space.net.gateway.v1");
}

impl proto::NetworkState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::NoIdentity => "no_identity",
            Self::PendingPolicy => "pending_policy",
            Self::Ready => "ready",
            Self::PolicyError => "policy_error",
            Self::Draining => "draining",
            Self::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for proto::NetworkState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

impl proto::NetworkPolicyErrorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::LoaderError => "loader_error",
            Self::Timeout => "timeout",
            Self::PolicyRefMismatch => "policy_ref_mismatch",
            Self::GatewayStopping => "gateway_stopping",
        }
    }
}

impl std::fmt::Display for proto::NetworkPolicyErrorKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str((*self).as_str())
    }
}

impl proto::InterceptionReadiness {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::NotRequired => "not_required",
            Self::Ready => "ready",
            Self::DependenciesNotReady => "dependencies_not_ready",
        }
    }

    /// Whether the gateway reported that guest egress is currently failing
    /// closed on TLS-interception dependencies. `Unspecified` (old binary)
    /// is not treated as blocked.
    pub const fn blocks_admission(self) -> bool {
        matches!(self, Self::DependenciesNotReady)
    }
}

impl std::fmt::Display for proto::InterceptionReadiness {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str((*self).as_str())
    }
}
