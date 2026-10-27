//! What ended a failed connection, and how loudly the gateway reports it.
//!
//! The pass-through relay, inspected TLS and inspected HTTP all classify their
//! failures here. A guest that closes, resets or abandons a connection is
//! routine: guest HTTP clients close without TLS `close_notify` and drop
//! connections they no longer need. A destination that fails after its TCP
//! connect succeeded is worth seeing, but the gateway did not fail. A failed
//! connect, and the gateway's own capacity, certificate or configuration
//! failures, stay loud.

use super::relay::{RelayFault, Side};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Fault {
    /// The first failing socket operation on an established stream.
    Relay(RelayFault),
    /// The guest's TLS handshake failed or timed out. `alert` records a fatal
    /// alert from the guest: a guest that rejects the interception chain sends
    /// one; a guest that disconnects or resets does not.
    GuestHandshake { alert: bool },
    /// The guest's HTTP connection ended mid-message, idled out before a
    /// request, or broke HTTP framing.
    GuestHttp,
    /// The TCP connect to the destination failed or ran out of time.
    OriginConnect,
    /// The destination's TLS handshake or certificate failed after its TCP
    /// connect succeeded.
    OriginHandshake,
    /// The destination's HTTP connection or response failed after it began.
    OriginHttp,
    /// The gateway's own handshake capacity, certificate material,
    /// configuration, or target and protocol checks.
    Gateway,
    /// Gateway shutdown or a certificate lease change ended the work.
    Cancelled,
}

/// How loudly a [`Fault`] is reported. Ordered quietest first.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Severity {
    Debug,
    Info,
    Warn,
    Error,
}

impl Fault {
    /// The peer that failed, or `None` when neither peer did.
    pub(crate) const fn side(self) -> Option<Side> {
        match self {
            Self::Relay(fault) => Some(fault.side()),
            Self::GuestHandshake { .. } | Self::GuestHttp => Some(Side::Guest),
            Self::OriginConnect | Self::OriginHandshake | Self::OriginHttp => Some(Side::Origin),
            Self::Gateway | Self::Cancelled => None,
        }
    }

    pub(crate) const fn severity(self) -> Severity {
        match self {
            Self::Relay(fault) => match fault.side() {
                Side::Guest => Severity::Debug,
                Side::Origin => Severity::Info,
            },
            Self::GuestHandshake { alert: false } | Self::GuestHttp => Severity::Debug,
            // A guest refusing the interception chain cannot use inspection
            // at all, which is a deployment problem rather than a hang-up.
            Self::GuestHandshake { alert: true } => Severity::Warn,
            Self::OriginHandshake | Self::OriginHttp | Self::Cancelled => Severity::Info,
            Self::OriginConnect | Self::Gateway => Severity::Error,
        }
    }

    /// The `fault` log and span field. Relay faults keep the pass-through
    /// labels of the `fault` tag on `gateway.tcp.connection`.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Relay(fault) => fault.as_str(),
            Self::GuestHandshake { .. } => "guest_handshake",
            Self::GuestHttp => "guest_http",
            Self::OriginConnect => "origin_connect",
            Self::OriginHandshake => "origin_handshake",
            Self::OriginHttp => "origin_http",
            Self::Gateway => "gateway",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Emit one tracing event at a [`Severity`]. A tracing level must be constant
/// at its call site, so this expands to one call per level over a shared field
/// list. The event keeps the caller's module as its log target.
macro_rules! event_at {
    ($severity:expr, $($event:tt)+) => {
        match $severity {
            $crate::tcp_proxy::Severity::Debug => tracing::debug!($($event)+),
            $crate::tcp_proxy::Severity::Info => tracing::info!($($event)+),
            $crate::tcp_proxy::Severity::Warn => tracing::warn!($($event)+),
            $crate::tcp_proxy::Severity::Error => tracing::error!($($event)+),
        }
    };
}
pub(crate) use event_at;
