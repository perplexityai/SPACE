//! Registry of established TCP flows, reconciled on policy changes.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use pplx_space_net_common::{DomainName, NetworkPolicyV1, TlsInterception};
use tokio_util::sync::CancellationToken;

use super::tcp::{dns_attributed_host, revalidate_tcp};
use crate::dns::DnsAllowCache;
use crate::tls_sni::ClientHelloScan;

/// Established-flow registry shared by admission and policy resweeps.
#[derive(Clone, Debug, Default)]
pub struct DnsAttributedTcpTracker {
    inner: Arc<DnsAttributedInner>,
}

#[derive(Debug, Default)]
struct DnsAttributedInner {
    next_id: AtomicU64,
    connections: Mutex<HashMap<u64, DnsAttributedConnection>>,
}

#[derive(Debug)]
pub(super) struct DnsAttributedConnection {
    pub(super) destination: SocketAddr,
    /// The admission-time ClientHello verdict. Only `NotTls` proves a flow is
    /// plaintext; `None` server names also arise from late, absent, malformed
    /// or oversized ClientHellos that Off relays opaquely.
    pub(super) scan: ClientHelloScan,
    // The admission policy fixes the transport for this connection's lifetime.
    // This is the effective mode after destination exemptions.
    pub(super) admitted_tls_interception: TlsInterception,
    // DNS expiry limits new admissions, not the lifetime of an exempt TLS flow.
    // Current policy permissions and exemptions are still checked on each sweep.
    pub(super) admitted_exempt_host: Option<DomainName>,
    shutdown_token: CancellationToken,
}

pub(crate) struct TcpConnectionGuard {
    pub(crate) interception_mode: TlsInterception,
    id: u64,
    tracker: DnsAttributedTcpTracker,
}

impl DnsAttributedTcpTracker {
    /// Register an established flow. The guard unregisters on drop, so the
    /// caller must hold it for the lifetime of the connection.
    #[must_use]
    pub(crate) fn register(
        &self,
        destination: SocketAddr,
        scan: ClientHelloScan,
        policy: &NetworkPolicyV1,
        dns_allow_cache: &DnsAllowCache,
        shutdown_token: CancellationToken,
    ) -> TcpConnectionGuard {
        let attributed_host = dns_attributed_host(destination, &scan, dns_allow_cache);
        let admitted_tls_interception =
            policy.tls_interception_for(destination.ip(), attributed_host);
        let admitted_exempt_host =
            if !policy.tls_interception.is_off() && admitted_tls_interception.is_off() {
                attributed_host.cloned()
            } else {
                None
            };
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        self.inner.connections.lock().insert(
            id,
            DnsAttributedConnection {
                destination,
                scan,
                admitted_tls_interception,
                admitted_exempt_host,
                shutdown_token,
            },
        );
        TcpConnectionGuard {
            interception_mode: admitted_tls_interception,
            id,
            tracker: self.clone(),
        }
    }

    /// Cancel every tracked flow the given policy no longer allows. Returns how
    /// many were cancelled.
    pub fn cancel_denied(
        &self,
        policy: &NetworkPolicyV1,
        dns_allow_cache: &DnsAllowCache,
    ) -> usize {
        self.cancel_matching(|connection| !revalidate_tcp(connection, policy, dns_allow_cache))
    }

    /// Cancel every tracked flow when there is no policy left to authorize it.
    pub fn cancel_all(&self) -> usize {
        self.cancel_matching(|_| true)
    }

    fn cancel_matching(&self, predicate: impl Fn(&DnsAttributedConnection) -> bool) -> usize {
        let tokens = self
            .inner
            .connections
            .lock()
            .values()
            .filter(|connection| !connection.shutdown_token.is_cancelled() && predicate(connection))
            .map(|connection| connection.shutdown_token.clone())
            .collect::<Vec<_>>();
        let cancelled = tokens.len();
        tokens.into_iter().for_each(|token| token.cancel());
        cancelled
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.inner.connections.lock().len()
    }

    fn unregister(&self, id: u64) {
        self.inner.connections.lock().remove(&id);
    }
}

impl Drop for TcpConnectionGuard {
    fn drop(&mut self) {
        self.tracker.unregister(self.id);
    }
}

#[cfg(test)]
mod tests;
