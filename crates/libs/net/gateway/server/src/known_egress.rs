use std::net::SocketAddr;
use std::sync::Arc;

use hickory_resolver::proto::op::Message;
use pplx_space_metrics::gateway::TcpConnectionFault;
use pplx_space_net_common::DomainName;
use tracing::{debug, error, info};

use crate::tcp_proxy::{Fault, RelaySide, Severity};

/// Diagnostic logging for failed egress to configured domains. A DNS query
/// matches by its query name and a TCP flow by its TLS SNI, so plaintext and
/// host-bound HTTP flows, and reserved DNS over TCP, are never logged here;
/// `gateway.tcp.connection` still counts them. A matched flow refused before
/// relaying, or whose connect or gateway-side setup failed, logs an error.
/// A relay or inspected session that the guest ended, or that the
/// destination failed after connecting, is logged below error.
/// `docs/observability.md` ("Known-egress logs") lists the events.
#[derive(Clone, Debug, Default)]
pub struct KnownEgressState {
    domains: Arc<[DomainName]>,
}

impl KnownEgressState {
    pub fn new(domains: Vec<DomainName>) -> Self {
        Self {
            domains: domains.into(),
        }
    }

    /// A failed SNI-matched TCP flow that never relayed: refused before
    /// relaying, or a failed connect.
    pub(crate) fn report_tcp_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        reason: &str,
    ) -> bool {
        self.report_failure(sandbox_id, original_dst, attempted_host, "tcp", reason)
    }

    /// A pass-through relay or inspected session that failed after
    /// admission. Only a failed connect, or the gateway's own setup failure,
    /// is a connect-failure error; a cancelled session is not reported.
    /// `fault` is what failed first, which is not necessarily the peer that
    /// caused the failure; `None` is a relay failure without a recorded leg.
    pub(crate) fn report_tcp_relay_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        fault: Option<Fault>,
        reason: &str,
    ) -> bool {
        match fault {
            Some(Fault::Cancelled) => false,
            Some(fault) if fault.severity() == Severity::Error => {
                self.report_tcp_failure(sandbox_id, original_dst, attempted_host, reason)
            }
            fault => self.report_established_failure(
                sandbox_id,
                original_dst,
                attempted_host,
                fault,
                reason,
            ),
        }
    }

    fn report_established_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        fault: Option<Fault>,
        reason: &str,
    ) -> bool {
        let Some(destination) = self.destination(attempted_host) else {
            return false;
        };
        let destination = destination.as_str();
        let original_dst_port = original_dst.port();
        let label = fault.map_or(TcpConnectionFault::RelayIo.as_str(), Fault::as_str);
        match fault.and_then(Fault::side) {
            // Clients routinely reset connections they are done with, so a
            // guest-leg failure usually says little about the destination.
            Some(RelaySide::Guest) => debug!(
                sandbox_id,
                original_dst_port,
                destination,
                protocol = "tcp",
                fault = label,
                reason,
                "guest side failed an established known egress connection"
            ),
            // A relay fails only through a leg, so a missing side is
            // unexpected; it stays at the level that keeps it visible.
            Some(RelaySide::Origin) | None => info!(
                sandbox_id,
                original_dst_port,
                destination,
                protocol = "tcp",
                fault = label,
                reason,
                "known egress destination failed an established connection"
            ),
        }
        true
    }

    fn destination(&self, attempted_host: &DomainName) -> Option<&DomainName> {
        self.domains
            .iter()
            .find(|destination| *destination == attempted_host)
    }

    fn report_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        protocol: &str,
        reason: &str,
    ) -> bool {
        let Some(destination) = self.destination(attempted_host) else {
            return false;
        };
        error!(
            sandbox_id,
            original_dst_port = original_dst.port(),
            destination = destination.as_str(),
            protocol,
            reason,
            "failed to connect to known egress destination"
        );
        true
    }

    pub(crate) fn report_dns_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        packet: &[u8],
        reason: &str,
    ) -> bool {
        if original_dst.port() != 53 {
            return false;
        }
        let Some(destination) = dns_host(packet) else {
            return false;
        };
        self.report_failure(sandbox_id, original_dst, &destination, "dns", reason)
    }
}

fn dns_host(packet: &[u8]) -> Option<DomainName> {
    let message = Message::from_vec(packet).ok()?;
    message.queries.first()?.name().to_string().parse().ok()
}

#[cfg(test)]
mod tests;
