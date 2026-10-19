use std::net::SocketAddr;
use std::sync::Arc;

use hickory_resolver::proto::op::Message;
use pplx_space_metrics::gateway::TcpConnectionFault;
use pplx_space_net_common::DomainName;
use tracing::{debug, error, info};

use crate::tcp_proxy::{RelayFault, RelaySide};

/// Diagnostic logging for failed egress to configured domains. A DNS query
/// matches by its query name and a TCP flow by its TLS SNI, so plaintext and
/// host-bound HTTP flows, and reserved DNS over TCP, are never logged here;
/// `gateway.tcp.connection` still counts them. A matched flow that fails logs
/// an error, as does an inspected TLS/HTTP session that fails after its
/// upstream connected. Only a pass-through relay that fails after connecting
/// is logged below error. `docs/observability.md` ("Known-egress logs") lists
/// the events.
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

    /// A failed SNI-matched TCP flow other than a pass-through transfer
    /// failure: refused before relaying, a failed connect, or a failed
    /// inspected session.
    pub(crate) fn report_tcp_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        reason: &str,
    ) -> bool {
        self.report_failure(sandbox_id, original_dst, attempted_host, "tcp", reason)
    }

    /// A pass-through relay that connected and then lost its byte stream did
    /// not fail to connect, so it stays out of the connect-failure error.
    /// `fault` is the first leg and operation the relay saw fail, which is not
    /// necessarily the peer that caused the failure.
    pub(crate) fn report_tcp_transfer_failure(
        &self,
        sandbox_id: &str,
        original_dst: SocketAddr,
        attempted_host: &DomainName,
        fault: Option<RelayFault>,
        reason: &str,
    ) -> bool {
        let Some(destination) = self.destination(attempted_host) else {
            return false;
        };
        let destination = destination.as_str();
        let original_dst_port = original_dst.port();
        let label = fault
            .map_or(TcpConnectionFault::RelayIo, RelayFault::metric)
            .as_str();
        match fault.map(RelayFault::side) {
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
            // The relay fails only through a leg, so a missing fault is
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
