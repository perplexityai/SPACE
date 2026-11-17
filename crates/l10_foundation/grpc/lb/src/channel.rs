//! Feeds a discovered endpoint set into tonic's balancer.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{Stream, StreamExt, stream::FusedStream};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use tonic::transport::channel::Change;
use tonic::transport::{Channel, Endpoint, Uri};

use crate::{EndpointSet, Resolution};

/// Endpoint changes queued toward the balancer. Deep enough to absorb a whole
/// fleet's worth of pods turning over at once without the feed task blocking.
const CHANGE_BUFFER: usize = 64;

/// How long the last endpoint set survives after discovery starts failing.
///
/// A healthy watch is silent between endpoint changes, so silence means
/// nothing; only a reported failure starts this clock, anchored to the first
/// failure of the outage. Kubernetes reuses pod IPs, so a set held through a
/// long enough outage stops describing dead pods and starts describing live
/// unrelated ones — and SPACE's node-api listens on the same port cluster-api
/// does, so a stale entry can reach a wrong service rather than merely fail.
/// The fallback has no such failure mode, being maintained by kube-proxy
/// independently of this crate.
///
/// Comfortably longer than a relist plus backoff, comfortably shorter than pod
/// rescheduling.
const STALENESS_BOUND: Duration = Duration::from_secs(60);

/// Distinguishes the always-available fallback from discovered pods, so the
/// fallback is never confused with an endpoint the resolver reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EndpointKey {
    Fallback,
    Pod(SocketAddr),
}

/// Balances over the endpoints `resolutions` yields, falling back to `fallback`
/// whenever there are none.
///
/// `configure` turns an address into an [`Endpoint`], and is the single source of
/// dial settings: it is applied to the fallback and to every discovered pod, so
/// the two cannot drift apart.
///
/// Each `Endpoints` item is a complete set rather than a delta, and `Failed` is
/// how a resolver reports trouble — a healthy watch is legitimately silent for
/// hours, so silence carries no meaning and only failure starts the staleness
/// clock.
///
/// Returns as soon as the channel is usable. The first resolution has not
/// happened yet at that point, so early requests go to `fallback` — exactly where
/// they would have gone without endpoint discovery.
///
/// `fallback_max_age` applies only while that Service fallback is active. It
/// preserves connection rotation before the first usable discovery result and
/// whenever an empty or failing resolver returns traffic to the Service.
pub fn balanced_channel<S, F>(
    service: &'static str,
    fallback: Uri,
    configure: F,
    resolutions: S,
    fallback_max_age: Option<Duration>,
) -> Channel
where
    S: Stream<Item = Resolution> + Send + 'static,
    F: Fn(Uri) -> Endpoint + Send + 'static,
{
    spawn_feed(
        service,
        fallback,
        configure,
        resolutions,
        STALENESS_BOUND,
        fallback_max_age,
    )
}

fn spawn_feed<S, F>(
    service: &'static str,
    fallback: Uri,
    configure: F,
    resolutions: S,
    staleness_bound: Duration,
    fallback_max_age: Option<Duration>,
) -> Channel
where
    S: Stream<Item = Resolution> + Send + 'static,
    F: Fn(Uri) -> Endpoint + Send + 'static,
{
    let (channel, changes) = Channel::balance_channel::<EndpointKey>(CHANGE_BUFFER);
    let feed = Feed {
        service,
        fallback,
        configure,
        changes,
        staleness_bound,
        fallback_active: false,
        fallback_max_age: fallback_max_age.filter(|max_age| !max_age.is_zero()),
        fallback_expires_at: None,
        pods: EndpointSet::new(),
    };
    tokio::spawn(feed.run(resolutions));
    channel
}

struct Feed<F> {
    service: &'static str,
    fallback: Uri,
    configure: F,
    changes: Sender<Change<EndpointKey, Endpoint>>,
    staleness_bound: Duration,
    fallback_active: bool,
    fallback_max_age: Option<Duration>,
    fallback_expires_at: Option<Instant>,
    pods: EndpointSet,
}

/// The feed lost its balancer, so there is nothing left to feed.
struct BalancerGone;

impl<F: Fn(Uri) -> Endpoint> Feed<F> {
    async fn run<S: Stream<Item = Resolution> + Send>(mut self, resolutions: S) {
        if self.set_fallback(true).await.is_err() {
            return;
        }
        let resolutions = resolutions.fuse();
        futures_util::pin_mut!(resolutions);
        // `None` while discovery is healthy; otherwise the first failure of the
        // current outage. Repeat failures must not move it — watch retries
        // arrive far more often than the staleness bound, so re-anchoring on
        // each would keep a permanently broken watch's endpoints forever.
        let mut failing_since: Option<Instant> = None;

        loop {
            // Watched so a channel nobody holds anymore also stops its feed —
            // a quiet watch never sends, so send failure alone can't detect it,
            // and an undetected drop leaks the resolver and its connection.
            let balancer = self.changes.clone();
            let next = tokio::select! {
                resolution = resolutions.next(), if !resolutions.is_terminated() => match resolution {
                    Some(Resolution::Endpoints(next)) => {
                        failing_since = None;
                        next
                    }
                    Some(Resolution::Failed) => {
                        failing_since.get_or_insert_with(Instant::now);
                        continue;
                    }
                    // A terminated resolver never recovers, so remove its last
                    // set. Keep this feed alive to rotate the fallback until the
                    // balanced channel itself is dropped.
                    None => {
                        pplx_space_observability::with_peer_service!(tracing::error!(
                            peer.service = self.service,
                            "endpoint discovery ended; reverting to the service address"
                        ));
                        if self.apply(EndpointSet::new()).await.is_err() {
                            return;
                        }
                        continue;
                    }
                },
                () = sleep_after(failing_since, self.staleness_bound),
                    if failing_since.is_some() && !self.pods.is_empty() =>
                {
                    pplx_space_observability::with_peer_service!(tracing::warn!(
                        peer.service = self.service,
                        failing_for_secs = self.staleness_bound.as_secs(),
                        "endpoint discovery failing past the staleness bound; \
                         reverting to the service address"
                    ));
                    EndpointSet::new()
                }
                () = sleep_after(self.fallback_expires_at, Duration::ZERO),
                    if self.fallback_active => {
                    if self.rotate_fallback().await.is_err() {
                        return;
                    }
                    continue;
                }
                () = balancer.closed() => return,
            };
            if self.apply(next).await.is_err() {
                return;
            }
        }
    }

    /// Keeps at least one endpoint in the balancer at every instant: an empty
    /// balancer parks every request until its deadline, so the fallback goes in
    /// before pods come out and comes out only once pods are in.
    async fn apply(&mut self, next: EndpointSet) -> Result<(), BalancerGone> {
        if next.is_empty() {
            self.set_fallback(true).await?;
        }

        // Addresses in both sets are left alone. Re-inserting a key replaces its
        // service in the balancer and drops that endpoint's live connection, so a
        // relist that changed nothing has to produce no changes at all.
        let added: Vec<SocketAddr> = next.difference(&self.pods).copied().collect();
        let removed: Vec<SocketAddr> = self.pods.difference(&next).copied().collect();

        for addr in added {
            let endpoint = (self.configure)(pod_uri(&self.fallback, addr));
            self.send(Change::Insert(EndpointKey::Pod(addr), endpoint))
                .await?;
        }
        for addr in removed {
            self.send(Change::Remove(EndpointKey::Pod(addr))).await?;
        }

        if !next.is_empty() {
            self.set_fallback(false).await?;
        }

        pplx_space_grpc_metrics::client_lb_endpoints(self.service, next.len());
        pplx_space_observability::with_peer_service!(tracing::info!(
            peer.service = self.service,
            endpoints = next.len(),
            "balanced endpoint set updated"
        ));
        self.pods = next;
        Ok(())
    }

    /// Idempotent, so callers state the fallback's desired presence rather than
    /// tracking whether it is already there.
    async fn set_fallback(&mut self, active: bool) -> Result<(), BalancerGone> {
        if self.fallback_active == active {
            return Ok(());
        }
        let change = if active {
            let endpoint = (self.configure)(self.fallback.clone());
            Change::Insert(EndpointKey::Fallback, endpoint)
        } else {
            Change::Remove(EndpointKey::Fallback)
        };
        self.send(change).await?;
        self.fallback_active = active;
        self.fallback_expires_at = if active {
            self.fallback_max_age
                .map(|max_age| Instant::now() + max_age)
        } else {
            None
        };
        pplx_space_grpc_metrics::client_lb_fallback(self.service, active);
        Ok(())
    }

    /// Replacing a balancer entry retires its live connection without
    /// disturbing requests already dispatched through the previous service.
    async fn rotate_fallback(&mut self) -> Result<(), BalancerGone> {
        let Some(max_age) = self.fallback_max_age else {
            self.fallback_expires_at = None;
            return Ok(());
        };
        let endpoint = (self.configure)(self.fallback.clone());
        self.send(Change::Insert(EndpointKey::Fallback, endpoint))
            .await?;
        self.fallback_expires_at = Some(Instant::now() + max_age);
        pplx_space_grpc_metrics::client_connection_rotation(self.service, "rotated");
        pplx_space_observability::with_peer_service!(tracing::debug!(
            peer.service = self.service,
            "retired aged gRPC Service fallback connection"
        ));
        Ok(())
    }

    async fn send(&mut self, change: Change<EndpointKey, Endpoint>) -> Result<(), BalancerGone> {
        self.changes.send(change).await.map_err(|_| BalancerGone)
    }
}

async fn sleep_after(start: Option<Instant>, duration: Duration) {
    match start {
        Some(start) => tokio::time::sleep_until(start + duration).await,
        None => std::future::pending().await,
    }
}

/// One pod's address as an endpoint URI.
///
/// Only the scheme carries over from the address the caller configured. tonic
/// keeps just the scheme and authority of an endpoint URI — a request's path is
/// the gRPC method, not the endpoint's — and the scheme is what decides whether
/// the connection is TLS.
fn pod_uri(fallback: &Uri, addr: SocketAddr) -> Uri {
    let scheme = fallback.scheme_str().unwrap_or("http");
    // A `SocketAddr` always formats as a valid authority, bracketing IPv6, and
    // the scheme came from an already-parsed URI.
    format!("{scheme}://{addr}")
        .parse()
        .expect("scheme and socket address form a valid URI")
}

#[cfg(test)]
mod tests;
