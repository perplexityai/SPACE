//! Client-side maximum connection age for SPACE gRPC channels.
//!
//! A `tonic::transport::Channel` is one HTTP/2 connection, and a Kubernetes
//! ClusterIP Service picks the backend pod once, when that connection is
//! established. Every RPC multiplexed onto the channel therefore lands on the
//! pod chosen at dial time, for as long as the client process lives. Replicas
//! added afterwards — by an HPA scale-out, or by the second half of a rollout —
//! receive nothing, which is what a large `grpc.server.active_requests`
//! max-minus-min across a service's pods is measuring.
//!
//! The usual remedy is a server-side `max_connection_age`, which sends GOAWAY
//! and lets clients redial. SPACE cannot use tonic's
//! [`Server::max_connection_age`] today, so the age lives on the client
//! instead: [`RotatingChannel`] retires its connection once it reaches that
//! age, which re-runs the Service's per-connection placement.
//!
//! Rotation is graceful. A replacement channel is dialed in the background
//! while the existing one keeps serving; only once the replacement is connected
//! does new traffic move to it. Requests already in flight finish on the old
//! connection, because hyper keeps the connection task alive until its
//! outstanding streams complete. No request is failed by a rotation.
//!
//! Only TCP endpoints need this. A Unix-socket channel has exactly one possible
//! peer, so retiring its connection buys nothing.
//!
//! [`Server::max_connection_age`]: tonic::transport::Server::max_connection_age

use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};
// A plain `Instant`, not `tokio::time::Instant`: these deadlines are only ever
// compared, never slept on, and a channel can be built outside a runtime.
use std::time::{Duration, Instant};

use tonic::body::Body;
use tonic::transport::{Channel, Endpoint};

/// Seconds a client keeps a gRPC connection before retiring it. Rotation is
/// opt-in per deployment: only an explicit positive value turns it on, so a
/// process that has never heard of this behaves exactly as it did before.
const MAX_CONNECTION_AGE_ENV: &str = "SPACE_GRPC_CLIENT_MAX_CONNECTION_AGE_SECS";

/// Spread applied to every age after the first, so a fleet that started
/// together does not redial together and pile onto the same re-placement.
const JITTER_FACTOR: f64 = 0.2;

/// Retry gap after a failed rotation. The existing connection is still serving,
/// so this only decides how soon the next attempt happens.
const ROTATION_RETRY_DELAY: Duration = Duration::from_secs(30);

/// The connection age from `SPACE_GRPC_CLIENT_MAX_CONNECTION_AGE_SECS`, or
/// `None` when rotation is off.
///
/// Off is the answer for everything except an explicit positive number:
/// unset, empty, `0`, and unparseable all mean the client keeps its connection
/// the way it always has. Turning rotation on for a deployment is therefore
/// setting one env var, and nothing else can turn it on by accident.
pub fn max_connection_age_from_env() -> Option<Duration> {
    max_connection_age_from_value(std::env::var(MAX_CONNECTION_AGE_ENV).ok().as_deref())
}

fn max_connection_age_from_value(value: Option<&str>) -> Option<Duration> {
    let value = value.map(str::trim).filter(|value| !value.is_empty())?;
    match value.parse::<u64>() {
        Ok(0) => None,
        Ok(secs) => Some(Duration::from_secs(secs)),
        Err(error) => {
            tracing::warn!(
                env = MAX_CONNECTION_AGE_ENV,
                value,
                error = %error,
                "unparseable gRPC client max connection age; not rotating connections"
            );
            None
        }
    }
}

/// A [`Channel`] that redials itself once its connection reaches a maximum age.
///
/// Use it wherever a `Channel` would go, including under `InterceptedService`
/// and the shared client trace layer. Cloning is cheap, and every clone shares
/// one connection and one age.
pub struct RotatingChannel {
    shared: Arc<Shared>,
    /// The channel this clone readied, held from `poll_ready` to `call` so the
    /// request runs on the connection that was polled.
    ///
    /// A clone starts empty and a clone that is never polled stays empty. That
    /// matters: services keep a long-lived template client and clone *it* per
    /// request, and a template holding a channel would hold the process's first
    /// connection open for its whole life — one connection permanently pinned
    /// to one pod, which is what this type exists to prevent.
    current: Option<Channel>,
}

impl RotatingChannel {
    /// Dials `endpoint` through [`crate::connect_tcp_channel`], redialing it
    /// whenever the connection reaches `max_age`. `None` (or zero) never
    /// rotates. `tls_handshake_timeout` is the bound the endpoint's TLS config
    /// sets, or `None` for plaintext; an `https` endpoint without it fails with
    /// [`crate::TcpChannelError::Endpoint`]. Internal-RPC callers connect
    /// through `internal_transport::ConnectEndpoint::connect_rotating`
    /// instead.
    ///
    /// The first dial is eager and its failure is the caller's, exactly as with
    /// [`Endpoint::connect`]; only later rotations are backgrounded.
    pub async fn connect(
        service_name: &'static str,
        endpoint: Endpoint,
        tls_handshake_timeout: Option<Duration>,
        max_age: Option<Duration>,
    ) -> Result<Self, crate::TcpChannelError> {
        crate::connect::check_tcp_endpoint(&endpoint, tls_handshake_timeout)?;
        Ok(Self::connect_tcp(service_name, endpoint, tls_handshake_timeout, max_age).await?)
    }

    /// [`Self::connect`] for an endpoint already known to be a TCP endpoint
    /// whose `https` scheme, if any, comes with its handshake bound.
    pub(crate) async fn connect_tcp(
        service_name: &'static str,
        endpoint: Endpoint,
        tls_handshake_timeout: Option<Duration>,
        max_age: Option<Duration>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel =
            crate::connect::dial_tcp_channel(endpoint.clone(), tls_handshake_timeout).await?;
        // A zero age would redial on every request, so it reads as "off" like
        // the absent case rather than as an interval.
        let rotation = max_age
            .filter(|max_age| !max_age.is_zero())
            .map(|max_age| Rotation {
                endpoint,
                tls_handshake_timeout,
                max_age,
            });
        Ok(Self::new(service_name, rotation, channel))
    }

    /// Wraps a channel whose connection lifecycle is managed by its producer.
    ///
    /// A balanced channel is the primary case: it owns one connection per
    /// discovered endpoint, so retiring it as though it were one ClusterIP
    /// connection would discard the entire endpoint set.
    pub fn from_channel(service_name: &'static str, channel: Channel) -> Self {
        Self::new(service_name, None, channel)
    }

    fn new(service_name: &'static str, rotation: Option<Rotation>, channel: Channel) -> Self {
        let expires_at = Instant::now()
            + rotation
                .as_ref()
                .map_or(Duration::ZERO, |rotation| first_age(rotation.max_age));
        Self {
            shared: Arc::new(Shared {
                service_name,
                rotation,
                state: Mutex::new(State {
                    channel,
                    expires_at,
                    rotating: false,
                }),
            }),
            current: None,
        }
    }
}

impl Clone for RotatingChannel {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            current: None,
        }
    }
}

impl tower::Service<http::Request<Body>> for RotatingChannel {
    type Response = http::Response<Body>;
    type Error = <Channel as tower::Service<http::Request<Body>>>::Error;
    type Future = <Channel as tower::Service<http::Request<Body>>>::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.current.insert(self.shared.acquire()).poll_ready(cx)
    }

    /// Uses the channel `poll_ready` readied. Acquiring when there is none
    /// keeps a caller that skipped `poll_ready` — a tower contract violation,
    /// but not one worth panicking a request path over — on the current
    /// connection.
    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        self.current
            .get_or_insert_with(|| self.shared.acquire())
            .call(request)
    }
}

impl std::fmt::Debug for RotatingChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RotatingChannel")
            .field("service", &self.shared.service_name)
            .field("rotates", &self.shared.rotation.is_some())
            .finish()
    }
}

/// The one connection and one deadline every clone shares.
struct Shared {
    service_name: &'static str,
    /// `None` when there is nothing to redial: no age was configured, or the
    /// caller supplied the channel.
    rotation: Option<Rotation>,
    state: Mutex<State>,
}

struct Rotation {
    endpoint: Endpoint,
    tls_handshake_timeout: Option<Duration>,
    max_age: Duration,
}

struct State {
    channel: Channel,
    expires_at: Instant,
    /// Set while a replacement is being dialed, so concurrent clones raise one
    /// rotation between them rather than one each.
    rotating: bool,
}

impl Shared {
    /// Hands out the current channel, starting a rotation if this connection is
    /// due for one. Always reads the shared channel, so a clone can never serve
    /// on a connection a completed rotation already retired.
    fn acquire(self: &Arc<Self>) -> Channel {
        let mut state = lock(&self.state);
        let channel = state.channel.clone();
        let Some(rotation) = self.rotation.as_ref() else {
            return channel;
        };
        if state.rotating || Instant::now() < state.expires_at {
            return channel;
        }
        // `tokio::spawn` panics with no runtime; a caller driving this channel
        // from a foreign executor should not take a request-path panic for it,
        // and leaving `rotating` unset lets a later poll try again.
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return channel;
        };
        state.rotating = true;
        let endpoint = rotation.endpoint.clone();
        let tls_handshake_timeout = rotation.tls_handshake_timeout;
        let max_age = rotation.max_age;
        drop(state);
        runtime.spawn(Arc::clone(self).rotate(endpoint, tls_handshake_timeout, max_age));
        channel
    }

    async fn rotate(
        self: Arc<Self>,
        endpoint: Endpoint,
        tls_handshake_timeout: Option<Duration>,
        max_age: Duration,
    ) {
        let dialed = crate::connect::dial_tcp_channel(endpoint, tls_handshake_timeout).await;
        let mut state = lock(&self.state);
        match dialed {
            Ok(channel) => {
                // Replacing the shared channel drops this side's last
                // long-lived handle on the old one. Requests already dispatched
                // keep running: their clone holds the old channel until the
                // response completes.
                state.channel = channel;
                state.expires_at = Instant::now() + subsequent_age(max_age);
                pplx_space_grpc_metrics::client_connection_rotation(self.service_name, "rotated");
                pplx_space_observability::with_peer_service!(tracing::debug!(
                    peer.service = self.service_name,
                    "retired aged gRPC connection"
                ));
            }
            Err(error) => {
                // The old connection is still serving, so a failed dial is a
                // deferred rotation and not a request-path failure.
                state.expires_at = Instant::now() + ROTATION_RETRY_DELAY;
                pplx_space_grpc_metrics::client_connection_rotation(
                    self.service_name,
                    "dial_failed",
                );
                pplx_space_observability::with_peer_service!(tracing::warn!(
                    peer.service = self.service_name,
                    error = %error,
                    retry_in_ms = ROTATION_RETRY_DELAY.as_millis() as u64,
                    "failed to redial aged gRPC connection; keeping the existing one"
                ));
            }
        }
        state.rotating = false;
    }
}

/// The age of a connection dialed at process start.
///
/// Drawn from the whole `[0, max_age]` range rather than jittered around the
/// full age, because every client in a fleet rolled out together starts at the
/// same instant: without this, the fleet's first rotation would fall inside one
/// jitter window.
fn first_age(max_age: Duration) -> Duration {
    max_age.mul_f64(rand::random::<f64>())
}

/// The age of every connection after the first.
fn subsequent_age(max_age: Duration) -> Duration {
    max_age.mul_f64(1.0 + JITTER_FACTOR * (rand::random::<f64>() * 2.0 - 1.0))
}

/// A rotation task that panicked would otherwise wedge every clone on a
/// poisoned lock; the state it guards is fully rebuilt by the next rotation.
fn lock(state: &Mutex<State>) -> MutexGuard<'_, State> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests;
