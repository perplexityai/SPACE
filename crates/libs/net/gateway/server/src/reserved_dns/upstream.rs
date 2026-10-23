use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Poll, ready};
use std::time::{Duration, Instant};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use futures_util::stream::{self, BoxStream};
use futures_util::{StreamExt, TryStreamExt};
use hickory_resolver::config::ConnectionConfig;
use hickory_resolver::net::NetError;
use hickory_resolver::net::runtime::{RuntimeProvider, Spawn};
use hickory_resolver::net::xfer::{DnsExchange, DnsHandle};
use hickory_resolver::proto::op::{DnsRequest, DnsResponse};
use hickory_resolver::{ConnectionProvider, PoolContext, TlsConfig};
use parking_lot::Mutex;
use pplx_space_metrics::gateway::ReservedDnsUpstreamClosedOutcome as ClosedOutcome;
use pplx_space_metrics::gateway::ReservedDnsUpstreamResponseOutcome as ResponseOutcome;
use tokio::sync::{Semaphore, oneshot};

use super::{GatewayRuntimeProvider, MAX_CONCURRENT_LOOKUPS, ReservedResolver};

/// Log before Hickory's server pool can replace I/O or Busy with NoConnections,
/// and move sends off connections the upstream closed while they sat idle or
/// that failed in transport.
#[derive(Clone)]
pub(super) struct LoggingConnectionProvider {
    runtime: GatewayRuntimeProvider,
    resolver: ReservedResolver,
    // Hickory dials one connection config per server, so the address names it.
    upstreams: Arc<Mutex<HashMap<SocketAddr, Arc<Upstream>>>>,
}

impl LoggingConnectionProvider {
    pub(super) fn new(runtime: GatewayRuntimeProvider, resolver: ReservedResolver) -> Self {
        Self {
            runtime,
            resolver,
            upstreams: Arc::default(),
        }
    }
}

impl ConnectionProvider for LoggingConnectionProvider {
    type Conn = LoggingDnsHandle;
    type FutureConn = BoxFuture<'static, Result<Self::Conn, NetError>>;
    type RuntimeProvider = GatewayRuntimeProvider;

    fn new_connection(
        &self,
        ip: IpAddr,
        config: &ConnectionConfig,
        cx: &PoolContext,
    ) -> Result<Self::FutureConn, NetError> {
        let upstream_addr = SocketAddr::new(ip, config.port);
        let upstream = self
            .upstreams
            .lock()
            .entry(upstream_addr)
            .or_insert_with(|| {
                // Hickory lends its pool context only for this call. Opportunistic
                // encryption is off, so options and TLS are all a reconnect needs.
                Arc::new(Upstream {
                    connector: Connector {
                        runtime: self.runtime.clone(),
                        upstream_addr,
                        config: config.clone(),
                        cx: PoolContext::new(
                            cx.options.clone(),
                            TlsConfig {
                                config: cx.tls.clone(),
                            },
                        ),
                    },
                    resolver: self.resolver,
                    state: Mutex::default(),
                })
            })
            .clone();
        // Hickory asks only when no connection it holds is usable, as after a
        // request failed on each. Dial rather than alias a connection Hickory
        // has given up on.
        let connection = upstream.connector.connect()?;
        Ok(Box::pin(async move {
            let exchange = Arc::new(connection.await?);
            upstream.state.lock().add(exchange.clone());
            Ok(LoggingDnsHandle(Arc::new(PooledConnection {
                upstream,
                exchange: Mutex::new(exchange),
            })))
        }))
    }

    fn runtime_provider(&self) -> &Self::RuntimeProvider {
        &self.runtime
    }
}

struct Connector {
    runtime: GatewayRuntimeProvider,
    upstream_addr: SocketAddr,
    config: ConnectionConfig,
    cx: PoolContext,
}

impl Connector {
    fn connect(&self) -> Result<BoxFuture<'static, Result<Exchange, NetError>>, NetError> {
        let upstream_addr = self.upstream_addr;
        let log_error = move |error: &NetError| {
            tracing::warn!(%upstream_addr, ?error, "reserved DNS upstream connection failed");
        };
        let exited = Arc::new(AtomicBool::new(false));
        let connection = TrackedRuntime {
            runtime: self.runtime.clone(),
            exited: exited.clone(),
        }
        .new_connection(upstream_addr.ip(), &self.config, &self.cx)
        .inspect_err(log_error)?;
        Ok(Box::pin(async move {
            let exchange = connection.await.inspect_err(log_error)?;
            Ok(Exchange {
                exchange,
                exited,
                failed: AtomicBool::new(false),
            })
        }))
    }
}

/// One upstream connection: whether Hickory's task serving it has exited, and
/// whether a request on it failed in transport.
struct Exchange {
    exchange: DnsExchange<TrackedRuntime>,
    exited: Arc<AtomicBool>,
    failed: AtomicBool,
}

impl Exchange {
    fn is_usable(&self) -> bool {
        !self.exited.load(Ordering::Acquire) && !self.failed.load(Ordering::Acquire)
    }

    fn send(
        self: Arc<Self>,
        request: DnsRequest,
    ) -> BoxStream<'static, Result<DnsResponse, NetError>> {
        let mut responses = self.exchange.send(request).boxed();
        let mut answered = false;
        stream::poll_fn(move |cx| {
            let response = ready!(responses.poll_next_unpin(cx));
            match &response {
                Some(Ok(_)) => answered = true,
                // An open connection's request limit.
                Some(Err(NetError::Busy)) => {}
                // A reset or abandoned request, or a timeout, which Hickory
                // 0.26 reports by ending the stream unanswered: the connection
                // may be stalled without closing. The stall test in
                // `recovery_tests` guards this on upgrade.
                Some(Err(_)) => self.failed.store(true, Ordering::Release),
                None if !answered => self.failed.store(true, Ordering::Release),
                None => {}
            }
            Poll::Ready(response)
        })
        .boxed()
    }
}

/// Pooled connections whose connection closed or failed move to the newest
/// usable connection to the upstream, or share one reconnect, so the connection
/// count shrinks back after a burst.
struct Upstream {
    connector: Connector,
    resolver: ReservedResolver,
    state: Mutex<UpstreamState>,
}

type Reconnect = Shared<BoxFuture<'static, Result<Arc<Exchange>, NetError>>>;

#[derive(Default)]
struct UpstreamState {
    // Oldest first; dropped once found closed or failed.
    open: Vec<Arc<Exchange>>,
    // Only the result: the reconnect runs as its own task, so it completes and
    // clears this even when every send waiting on it is cancelled.
    reconnect: Option<Reconnect>,
}

impl UpstreamState {
    // Pruning here too releases connections Hickory has given up on, which no
    // send will find closed.
    fn add(&mut self, exchange: Arc<Exchange>) {
        self.open.retain(|exchange| exchange.is_usable());
        self.open.push(exchange);
    }

    fn usable(&mut self) -> Option<Arc<Exchange>> {
        self.open.retain(|exchange| exchange.is_usable());
        self.open.last().cloned()
    }
}

impl Upstream {
    async fn usable_exchange(self: &Arc<Self>) -> (Result<Arc<Exchange>, NetError>, ClosedOutcome) {
        let reconnect = {
            let mut state = self.state.lock();
            if let Some(usable) = state.usable() {
                return (Ok(usable), ClosedOutcome::Reused);
            }
            state
                .reconnect
                .get_or_insert_with(|| self.spawn_reconnect())
                .clone()
        };
        match reconnect.await {
            Ok(exchange) => (Ok(exchange), ClosedOutcome::Reconnected),
            // Hickory may have dialed a connection meanwhile; a failed
            // reconnect must not fail a send that connection can carry.
            Err(error) => match self.state.lock().usable() {
                Some(usable) => (Ok(usable), ClosedOutcome::Reused),
                None => (Err(error), ClosedOutcome::Failed),
            },
        }
    }

    fn spawn_reconnect(self: &Arc<Self>) -> Reconnect {
        let (result_sender, result) = oneshot::channel();
        let upstream = self.clone();
        self.connector.runtime.create_handle().spawn_bg(async move {
            let result = match upstream.connector.connect() {
                Ok(connection) => connection.await.map(Arc::new),
                Err(error) => Err(error),
            };
            let mut state = upstream.state.lock();
            state.reconnect = None;
            if let Ok(exchange) = &result {
                state.add(exchange.clone());
            }
            drop(state);
            let _ = result_sender.send(result);
        });
        result
            .map(|result| {
                result.unwrap_or_else(|_| Err(NetError::from("reserved DNS reconnect stopped")))
            })
            .boxed()
            .shared()
    }
}

/// One pooled Hickory connection. Every clone Hickory holds sees the
/// replacement for a connection that closed or failed.
#[derive(Clone)]
pub(super) struct LoggingDnsHandle(Arc<PooledConnection>);

struct PooledConnection {
    upstream: Arc<Upstream>,
    exchange: Mutex<Arc<Exchange>>,
}

impl PooledConnection {
    async fn replace(&self, closed: &Arc<Exchange>) -> Result<Arc<Exchange>, NetError> {
        let (result, outcome) = self.upstream.usable_exchange().await;
        pplx_space_metrics::gateway::reserved_dns_upstream_closed(
            self.upstream.resolver.as_str(),
            outcome,
        );
        let exchange = result?;
        let mut current = self.exchange.lock();
        if Arc::ptr_eq(&current, closed) {
            *current = exchange.clone();
        }
        Ok(exchange)
    }
}

impl DnsHandle for LoggingDnsHandle {
    type Response = BoxStream<'static, Result<DnsResponse, NetError>>;
    type Runtime = GatewayRuntimeProvider;

    fn send(&self, request: DnsRequest) -> Self::Response {
        let upstream_addr = self.0.upstream.connector.upstream_addr;
        let query_type = request.queries.first().map(|query| query.query_type);
        let lookup_slots = self.0.upstream.connector.runtime.lookup_slots.clone();
        let mut timing = UpstreamTiming {
            resolver: self.0.upstream.resolver,
            started_at: Instant::now(),
            recorded: false,
        };
        let exchange = self.0.exchange.lock().clone();
        let mut responses = if exchange.is_usable() {
            exchange.send(request)
        } else {
            // Upstreams close idle DNS-over-TLS connections routinely. Hickory
            // would see Busy from the closed queue, treat it as backpressure,
            // and report NoConnections after a few closed pooled connections.
            // Hickory also keeps every other pooled connection on a failed
            // connection until each of them fails in turn.
            tracing::debug!(
                %upstream_addr,
                ?query_type,
                "reserved DNS upstream connection closed or failed; replacing"
            );
            let connection = self.0.clone();
            stream::once(async move {
                match connection.replace(&exchange).await {
                    Ok(exchange) => exchange.send(request),
                    Err(error) => stream::iter([Err(error)]).boxed(),
                }
            })
            .flatten()
            .boxed()
        };
        stream::poll_fn(move |cx| {
            let response = ready!(responses.poll_next_unpin(cx));
            timing.record(match &response {
                Some(Ok(_)) => ResponseOutcome::Answered,
                Some(Err(NetError::Timeout)) => ResponseOutcome::Timeout,
                Some(Err(NetError::Busy)) => ResponseOutcome::Busy,
                Some(Err(_)) => ResponseOutcome::Error,
                None => ResponseOutcome::NoResponse,
            });
            Poll::Ready(response)
        })
        .inspect_err(move |error| {
            if matches!(error, NetError::Busy) {
                let space_lookups_in_flight = record_busy(&lookup_slots);
                tracing::warn!(
                    %upstream_addr,
                    ?query_type,
                    ?error,
                    space_lookups_in_flight,
                    "reserved DNS upstream request failed"
                );
                return;
            }
            tracing::warn!(
                %upstream_addr,
                ?query_type,
                ?error,
                "reserved DNS upstream request failed"
            );
        })
        .boxed()
    }
}

/// Records one upstream request/response time on the send's first response or
/// failure, or as cancelled if the send is dropped before either.
struct UpstreamTiming {
    resolver: ReservedResolver,
    started_at: Instant,
    recorded: bool,
}

impl UpstreamTiming {
    fn record(&mut self, outcome: ResponseOutcome) {
        if std::mem::replace(&mut self.recorded, true) {
            return;
        }
        pplx_space_metrics::gateway::reserved_dns_upstream_request(
            self.resolver.as_str(),
            outcome,
            self.started_at.elapsed().as_secs_f64() * 1000.0,
        );
    }
}

impl Drop for UpstreamTiming {
    fn drop(&mut self) {
        self.record(ResponseOutcome::Cancelled);
    }
}

/// The gateway runtime for one connection, reporting when the task Hickory
/// spawns to serve it exits. That task owns the request queue, so a send after
/// the exit would fail with Busy, the error a saturated open connection also
/// returns.
///
/// This relies on Hickory 0.26's TCP and TLS exchanges spawning exactly that
/// one task through the provider's handle. The TCP and TLS idle-close tests in
/// `recovery_tests` guard it on upgrade. A send racing the exit can still meet
/// the closed queue; Hickory then backs off and drops that connection, as it
/// did for every idle close before.
#[derive(Clone)]
struct TrackedRuntime {
    runtime: GatewayRuntimeProvider,
    exited: Arc<AtomicBool>,
}

impl RuntimeProvider for TrackedRuntime {
    type Handle = TrackedHandle;
    type Timer = <GatewayRuntimeProvider as RuntimeProvider>::Timer;
    type Udp = <GatewayRuntimeProvider as RuntimeProvider>::Udp;
    type Tcp = <GatewayRuntimeProvider as RuntimeProvider>::Tcp;

    fn create_handle(&self) -> Self::Handle {
        TrackedHandle {
            handle: self.runtime.create_handle(),
            exited: self.exited.clone(),
        }
    }

    fn connect_tcp(
        &self,
        server_addr: SocketAddr,
        bind_addr: Option<SocketAddr>,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Tcp, io::Error>>>> {
        self.runtime.connect_tcp(server_addr, bind_addr, timeout)
    }

    fn bind_udp(
        &self,
        local_addr: SocketAddr,
        server_addr: SocketAddr,
    ) -> Pin<Box<dyn Send + Future<Output = Result<Self::Udp, io::Error>>>> {
        self.runtime.bind_udp(local_addr, server_addr)
    }
}

#[derive(Clone)]
struct TrackedHandle {
    handle: <GatewayRuntimeProvider as RuntimeProvider>::Handle,
    exited: Arc<AtomicBool>,
}

impl Spawn for TrackedHandle {
    fn spawn_bg(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        let exited = SetOnDrop(self.exited.clone());
        self.handle.spawn_bg(async move {
            // Declared first, so it drops after the task and its queue.
            let _exited = exited;
            future.await;
        });
    }
}

struct SetOnDrop(Arc<AtomicBool>);

impl Drop for SetOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn record_busy(lookup_slots: &Semaphore) -> usize {
    let active = MAX_CONCURRENT_LOOKUPS - lookup_slots.available_permits();
    pplx_space_metrics::gateway::reserved_dns_upstream_busy_lookup_in_flight(active);
    active
}
