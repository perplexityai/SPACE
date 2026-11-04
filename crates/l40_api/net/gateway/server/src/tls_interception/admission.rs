//! Bounded FIFO admission for in-progress TLS handshakes.
//!
//! One pool per gateway process bounds the TLS setup work that is in progress
//! at the same time: CPU for handshake cryptography, transient Rustls and task
//! state, and origin sockets that are still connecting. Both legs share the
//! pool. A guest-facing permit covers the server handshake. An origin-facing
//! permit covers the TCP connect, the client handshake and certificate
//! verification. Established sessions release their permit, so this pool does
//! not bound open connections. The TCP admission budget bounds those.
//!
//! A full pool queues callers in FIFO order instead of refusing them at once.
//! HTTP clients normally queue at their own connection limits, so a short
//! burst of new connections from one guest program becomes added latency
//! instead of failed handshakes. Each wait stops at the earlier of the
//! caller's handshake deadline and [`MAX_QUEUE_WAIT`]. The TCP admission
//! budget bounds the number of waiters, because each waiter owns an admitted
//! connection.
//!
//! The gateway closes the pool when it shuts down. Queued and later callers
//! then fail at once with [`Refused::Closed`] instead of waiting.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};
use tokio::time::Instant;

/// In-progress handshakes per gateway, shared by both legs.
pub(crate) const MAX_IN_FLIGHT: usize = 256;

/// The longest time one handshake waits for a permit. The rest of the
/// caller's handshake budget stays available for the handshake itself.
pub(crate) const MAX_QUEUE_WAIT: Duration = Duration::from_secs(5);

/// The two peers of an intercepted connection that each need a handshake.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TlsHandshakeLeg {
    ClientToNetgw,
    NetgwToOrigin,
}

impl TlsHandshakeLeg {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::ClientToNetgw => "client_to_netgw",
            Self::NetgwToOrigin => "netgw_to_origin",
        }
    }
}

/// One counter per leg.
#[derive(Default)]
struct PerLeg {
    client_to_netgw: AtomicUsize,
    netgw_to_origin: AtomicUsize,
}

impl PerLeg {
    fn total(&self) -> usize {
        self.client_to_netgw.load(Ordering::Relaxed) + self.netgw_to_origin.load(Ordering::Relaxed)
    }

    const fn get(&self, leg: TlsHandshakeLeg) -> &AtomicUsize {
        match leg {
            TlsHandshakeLeg::ClientToNetgw => &self.client_to_netgw,
            TlsHandshakeLeg::NetgwToOrigin => &self.netgw_to_origin,
        }
    }
}

/// How a permit was obtained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Admission {
    /// A permit was free when the caller arrived.
    Immediate,
    /// The caller waited in the queue for this long before a permit was free.
    Queued { waited: Duration },
}

impl Admission {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Immediate => "immediate",
            Self::Queued { .. } => "queued",
        }
    }
}

/// Why a handshake was not admitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum Refused {
    /// The pool stayed full for the complete wait.
    #[error("TLS handshake capacity stayed exhausted for {waited:?}")]
    Saturated { waited: Duration },
    /// The gateway is shutting down and admits no more handshakes.
    #[error("TLS handshake admission is closed")]
    Closed,
}

/// Cloning shares the same pool.
#[derive(Clone)]
pub(crate) struct HandshakeAdmission {
    pool: Arc<Pool>,
}

struct Pool {
    permits: Arc<Semaphore>,
    limit: usize,
    max_wait: Duration,
    active: PerLeg,
    queued: PerLeg,
}

impl Default for HandshakeAdmission {
    fn default() -> Self {
        Self::new(MAX_IN_FLIGHT, MAX_QUEUE_WAIT)
    }
}

impl HandshakeAdmission {
    pub(crate) fn new(limit: usize, max_wait: Duration) -> Self {
        Self {
            pool: Arc::new(Pool {
                permits: Arc::new(Semaphore::new(limit)),
                limit,
                max_wait,
                active: PerLeg::default(),
                queued: PerLeg::default(),
            }),
        }
    }

    /// Stop admitting handshakes. Queued callers wake with
    /// [`Refused::Closed`]. Permits already issued stay valid until dropped.
    pub(crate) fn close(&self) {
        self.pool.permits.close();
    }

    /// Wait for a permit until `deadline` or the queue limit, whichever is
    /// earlier. The caller keeps the permit for the handshake and drops it
    /// when the handshake ends, on success, failure or cancellation.
    ///
    /// Cancellation safe: a dropped waiter leaves the queue and loses its
    /// place.
    pub(crate) async fn admit(
        &self,
        leg: TlsHandshakeLeg,
        deadline: Instant,
    ) -> Result<HandshakePermit, Refused> {
        match Arc::clone(&self.pool.permits).try_acquire_owned() {
            Ok(permit) => Ok(self.issue(leg, permit, Admission::Immediate)),
            Err(TryAcquireError::NoPermits) => self.wait(leg, deadline).await,
            Err(TryAcquireError::Closed) => Err(self.refuse(leg, Refused::Closed)),
        }
    }

    async fn wait(
        &self,
        leg: TlsHandshakeLeg,
        deadline: Instant,
    ) -> Result<HandshakePermit, Refused> {
        let started = Instant::now();
        let stop = deadline.min(started + self.pool.max_wait);
        let acquired = {
            let _queued = QueuedGuard::enter(&self.pool, leg);
            tokio::time::timeout_at(stop, Arc::clone(&self.pool.permits).acquire_owned()).await
        };
        let waited = started.elapsed();
        match acquired {
            Ok(Ok(permit)) => Ok(self.issue(leg, permit, Admission::Queued { waited })),
            Ok(Err(_closed)) => Err(self.refuse(leg, Refused::Closed)),
            Err(_elapsed) => {
                pplx_space_metrics::gateway::tls_handshake_admission_wait_ms(
                    leg.as_str(),
                    "saturated",
                    millis(waited),
                );
                Err(self.refuse(leg, Refused::Saturated { waited }))
            }
        }
    }

    fn refuse(&self, leg: TlsHandshakeLeg, refused: Refused) -> Refused {
        pplx_space_metrics::gateway::tls_handshake_admission(leg.as_str(), refused.as_str());
        self.pool.record_in_flight(leg);
        refused
    }

    fn issue(
        &self,
        leg: TlsHandshakeLeg,
        permit: OwnedSemaphorePermit,
        admission: Admission,
    ) -> HandshakePermit {
        self.pool.active.get(leg).fetch_add(1, Ordering::Relaxed);
        pplx_space_metrics::gateway::tls_handshake_admission(leg.as_str(), admission.as_str());
        if let Admission::Queued { waited } = admission {
            pplx_space_metrics::gateway::tls_handshake_admission_wait_ms(
                leg.as_str(),
                admission.as_str(),
                millis(waited),
            );
        }
        self.pool.record_in_flight(leg);
        HandshakePermit {
            pool: Arc::clone(&self.pool),
            leg,
            admission,
            _permit: permit,
        }
    }
}

impl Refused {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Saturated { .. } => "saturated",
            Self::Closed => "closed",
        }
    }
}

impl Pool {
    fn record_in_flight(&self, leg: TlsHandshakeLeg) {
        pplx_space_metrics::gateway::tls_handshake_in_flight(
            "shared",
            self.active.total(),
            self.limit,
        );
        pplx_space_metrics::gateway::tls_handshake_in_flight(
            leg.as_str(),
            self.active.get(leg).load(Ordering::Relaxed),
            self.limit,
        );
    }

    fn record_queued(&self, leg: TlsHandshakeLeg) {
        pplx_space_metrics::gateway::tls_handshake_queued(
            leg.as_str(),
            self.queued.get(leg).load(Ordering::Relaxed),
            self.limit,
        );
    }
}

/// Counts one waiter for its full time in the queue, including cancellation.
struct QueuedGuard<'a> {
    pool: &'a Pool,
    leg: TlsHandshakeLeg,
}

impl<'a> QueuedGuard<'a> {
    fn enter(pool: &'a Pool, leg: TlsHandshakeLeg) -> Self {
        pool.queued.get(leg).fetch_add(1, Ordering::Relaxed);
        pool.record_queued(leg);
        Self { pool, leg }
    }
}

impl Drop for QueuedGuard<'_> {
    fn drop(&mut self) {
        self.pool
            .queued
            .get(self.leg)
            .fetch_sub(1, Ordering::Relaxed);
        self.pool.record_queued(self.leg);
    }
}

/// One admitted handshake. Dropping it returns the permit to the pool.
pub(crate) struct HandshakePermit {
    pool: Arc<Pool>,
    leg: TlsHandshakeLeg,
    admission: Admission,
    // Released by the field drop that follows `Drop::drop`.
    _permit: OwnedSemaphorePermit,
}

impl HandshakePermit {
    pub(crate) const fn admission(&self) -> Admission {
        self.admission
    }
}

impl std::fmt::Debug for HandshakePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandshakePermit")
            .field("leg", &self.leg)
            .field("admission", &self.admission)
            .finish_non_exhaustive()
    }
}

impl Drop for HandshakePermit {
    fn drop(&mut self) {
        self.pool
            .active
            .get(self.leg)
            .fetch_sub(1, Ordering::Relaxed);
        self.pool.record_in_flight(self.leg);
    }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests;
