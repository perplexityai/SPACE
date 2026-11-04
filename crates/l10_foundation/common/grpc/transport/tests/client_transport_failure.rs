#![cfg(test)]

//! [`is_client_transport_failure`] on statuses a real tonic channel produced,
//! against peers that fail a call the ways HTTP/2 lets them: a stream reset
//! the peer sent, a GOAWAY, a handler panic, a graceful shutdown, plus the
//! client's own call timeout and a refused or stalled dial.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use pplx_space_common_grpc::is_client_transport_failure;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tonic::{Code, Status};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_client::HealthClient;

/// The dial bound every channel here sets.
const DIAL_TIMEOUT: Duration = Duration::from_millis(200);

/// The call bound of a call that does not test it, long enough that it never
/// decides a row and short enough that a hung call fails the test.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

const FRAME_HEADERS: u8 = 0x1;
const FRAME_RST_STREAM: u8 = 0x3;
const FRAME_SETTINGS: u8 = 0x4;
const FRAME_GOAWAY: u8 = 0x7;
const FLAG_ACK: u8 = 0x1;

/// How a hand-driven HTTP/2 peer answers the first request it reads. It
/// keeps the connection open afterwards, so the client sees only that frame.
#[derive(Clone, Copy, Debug)]
enum RawAnswer {
    /// `RST_STREAM` on the request's stream.
    Reset(h2::Reason),
    /// `GOAWAY` naming no stream processed: the peer never accepted the call.
    GoAway(h2::Reason),
}

/// A tonic server handler for `grpc.health.v1.Health`, whatever the method.
#[derive(Clone)]
enum Handler {
    /// Answers with this status, as a handler or tower layer rejects a call.
    Answer(Code, &'static str),
    /// Panics, with no catch-panic layer in front of it.
    Panic,
    /// Never answers, not even with response headers.
    Hold,
    /// Reports that the call arrived, and answers `NotFound` once released.
    HoldUntil {
        arrived: Arc<Notify>,
        release: Arc<Notify>,
    },
}

impl tonic::server::NamedService for Handler {
    const NAME: &'static str = "grpc.health.v1.Health";
}

impl tower::Service<http::Request<tonic::body::Body>> for Handler {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<tonic::body::Body>) -> Self::Future {
        let handler = self.clone();
        Box::pin(async move {
            match handler {
                Handler::Answer(code, message) => Ok(Status::new(code, message).into_http()),
                Handler::Panic => panic!("the handler panicked"),
                Handler::Hold => std::future::pending().await,
                Handler::HoldUntil { arrived, release } => {
                    arrived.notify_one();
                    release.notified().await;
                    Ok(Status::not_found("answered after the shutdown began").into_http())
                }
            }
        })
    }
}

/// What a call ran into.
enum Target {
    /// A hand-driven HTTP/2 peer.
    Raw(RawAnswer),
    /// A tonic server, called with this call timeout.
    Tonic(Handler, Duration),
    /// A client interceptor that fails the call before it is sent.
    Interceptor,
    /// A local port nothing listens on.
    ClosedPort,
    /// A listener whose accept queue is full, so the kernel drops the SYN
    /// and the dial times out.
    #[cfg(target_os = "linux")]
    FullAcceptQueue,
}

/// The error a row's source chain carries, which proves the row tests what
/// it names.
#[derive(Clone, Copy, Debug)]
enum Carries {
    /// An `h2` stream reset the peer sent.
    RemoteReset,
    /// An `h2` GOAWAY the peer sent.
    RemoteGoAway,
    /// The client's own call timeout.
    TimeoutExpired,
    /// A dial that timed out.
    #[cfg(target_os = "linux")]
    DialTimedOut,
    /// A refused dial.
    DialRefused,
    /// No source at all.
    NoSource,
}

impl Carries {
    fn in_chain(self, status: &Status) -> bool {
        let mut chain = Vec::new();
        let mut source = std::error::Error::source(status);
        while let Some(error) = source {
            chain.push(error);
            source = error.source();
        }
        let h2 = || {
            chain
                .iter()
                .find_map(|error| error.downcast_ref::<h2::Error>())
        };
        let io_kind = || {
            chain
                .iter()
                .find_map(|error| error.downcast_ref::<std::io::Error>())
                .map(std::io::Error::kind)
        };
        match self {
            Self::RemoteReset => h2().is_some_and(|error| error.is_reset() && error.is_remote()),
            Self::RemoteGoAway => h2().is_some_and(|error| error.is_go_away() && error.is_remote()),
            Self::TimeoutExpired => chain
                .iter()
                .any(|error| error.is::<tonic::TimeoutExpired>()),
            #[cfg(target_os = "linux")]
            Self::DialTimedOut => io_kind() == Some(std::io::ErrorKind::TimedOut),
            Self::DialRefused => io_kind() == Some(std::io::ErrorKind::ConnectionRefused),
            Self::NoSource => chain.is_empty(),
        }
    }
}

fn source_chain(status: &Status) -> Vec<String> {
    let mut chain = Vec::new();
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        chain.push(format!("{error:?}"));
        source = error.source();
    }
    chain
}

async fn write_frame(
    socket: &mut TcpStream,
    kind: u8,
    flags: u8,
    stream: u32,
    payload: &[u8],
) -> std::io::Result<()> {
    let length = u32::try_from(payload.len())
        .expect("a small frame")
        .to_be_bytes();
    let mut frame = Vec::with_capacity(9 + payload.len());
    frame.extend_from_slice(&length[1..]);
    frame.extend_from_slice(&[kind, flags]);
    frame.extend_from_slice(&stream.to_be_bytes());
    frame.extend_from_slice(payload);
    socket.write_all(&frame).await
}

/// Speaks just enough HTTP/2 to read the client's first request and answer
/// it with `answer`, then reads until the client goes away.
async fn answer_first_request(socket: &mut TcpStream, answer: RawAnswer) -> std::io::Result<()> {
    let mut preface = [0_u8; 24];
    socket.read_exact(&mut preface).await?;
    write_frame(socket, FRAME_SETTINGS, 0, 0, &[]).await?;
    let mut answered = false;
    loop {
        let mut header = [0_u8; 9];
        socket.read_exact(&mut header).await?;
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
        let (kind, flags) = (header[3], header[4]);
        let stream = u32::from_be_bytes([header[5], header[6], header[7], header[8]]) & 0x7fff_ffff;
        let mut payload = vec![0_u8; length as usize];
        socket.read_exact(&mut payload).await?;
        match kind {
            FRAME_SETTINGS if flags & FLAG_ACK == 0 => {
                write_frame(socket, FRAME_SETTINGS, FLAG_ACK, 0, &[]).await?;
            }
            FRAME_HEADERS if !answered => {
                answered = true;
                match answer {
                    RawAnswer::Reset(reason) => {
                        let code = u32::from(reason).to_be_bytes();
                        write_frame(socket, FRAME_RST_STREAM, 0, stream, &code).await?;
                    }
                    RawAnswer::GoAway(reason) => {
                        let mut payload = [0_u8; 8];
                        payload[4..].copy_from_slice(&u32::from(reason).to_be_bytes());
                        write_frame(socket, FRAME_GOAWAY, 0, 0, &payload).await?;
                    }
                }
            }
            _ => {}
        }
    }
}

async fn raw_peer(answer: RawAnswer) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the HTTP/2 peer");
    let address = listener.local_addr().expect("HTTP/2 peer address");
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept the client");
        let _ = answer_first_request(&mut socket, answer).await;
    });
    address
}

async fn serve(
    handler: Handler,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the tonic server");
    let address = listener.local_addr().expect("tonic server address");
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(handler)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown,
            ),
    );
    address
}

/// A listener whose accept queue is full, and the connections that fill it.
#[cfg(target_os = "linux")]
async fn full_accept_queue() -> (SocketAddr, tokio::net::TcpListener, Vec<TcpStream>) {
    let socket = tokio::net::TcpSocket::new_v4().expect("a TCP socket");
    socket
        .bind("127.0.0.1:0".parse().expect("loopback"))
        .expect("bind the listener");
    let listener = socket.listen(0).expect("listen with no backlog");
    let address = listener.local_addr().expect("listener address");
    let mut queued = Vec::new();
    for _ in 0..8 {
        match tokio::time::timeout(DIAL_TIMEOUT, TcpStream::connect(address)).await {
            Ok(connected) => queued.push(connected.expect("queue a connection")),
            Err(_) => return (address, listener, queued),
        }
    }
    panic!("the accept queue never filled");
}

fn channel(address: SocketAddr, call_timeout: Duration) -> tonic::transport::Channel {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
        .expect("endpoint")
        .connect_timeout(DIAL_TIMEOUT)
        .timeout(call_timeout);
    pplx_space_common_grpc::connect_tcp_channel_lazy(endpoint, None)
        .expect("plaintext TCP endpoint")
}

async fn check(channel: tonic::transport::Channel) -> Status {
    HealthClient::new(channel)
        .check(HealthCheckRequest::default())
        .await
        .expect_err("the call fails")
}

async fn closed_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve a local port");
    listener.local_addr().expect("reserved port")
}

async fn status_from(target: Target) -> Status {
    match target {
        Target::Raw(answer) => check(channel(raw_peer(answer).await, CALL_TIMEOUT)).await,
        Target::Tonic(handler, call_timeout) => {
            let address = serve(handler, std::future::pending()).await;
            check(channel(address, call_timeout)).await
        }
        Target::Interceptor => {
            HealthClient::with_interceptor(channel(closed_port().await, CALL_TIMEOUT), |_request| {
                Err(Status::unavailable("failed to attach bearer token"))
            })
            .check(HealthCheckRequest::default())
            .await
            .expect_err("the interceptor fails the call")
        }
        Target::ClosedPort => check(channel(closed_port().await, CALL_TIMEOUT)).await,
        #[cfg(target_os = "linux")]
        Target::FullAcceptQueue => {
            let (address, _listener, _queued) = full_accept_queue().await;
            check(channel(address, CALL_TIMEOUT)).await
        }
    }
}

/// A status counts only when the client's transport failed: the dial failed
/// or the connection was lost. A peer that answered, reset the call's stream
/// or shed load was reached and waits, and so does a call the client timed
/// out.
#[tokio::test]
async fn only_a_failed_dial_or_lost_connection_is_a_client_transport_failure() {
    let cases = [
        (
            Target::Raw(RawAnswer::Reset(h2::Reason::INTERNAL_ERROR)),
            Code::Internal,
            Carries::RemoteReset,
            false,
        ),
        (
            Target::Raw(RawAnswer::Reset(h2::Reason::REFUSED_STREAM)),
            Code::Unavailable,
            Carries::RemoteReset,
            false,
        ),
        (
            Target::Raw(RawAnswer::Reset(h2::Reason::ENHANCE_YOUR_CALM)),
            Code::ResourceExhausted,
            Carries::RemoteReset,
            false,
        ),
        (
            Target::Raw(RawAnswer::GoAway(h2::Reason::ENHANCE_YOUR_CALM)),
            Code::ResourceExhausted,
            Carries::RemoteGoAway,
            false,
        ),
        (
            Target::Raw(RawAnswer::GoAway(h2::Reason::NO_ERROR)),
            Code::Internal,
            Carries::RemoteGoAway,
            true,
        ),
        (
            Target::Tonic(Handler::Panic, CALL_TIMEOUT),
            Code::Cancelled,
            Carries::RemoteReset,
            false,
        ),
        (
            Target::Tonic(Handler::Hold, Duration::from_millis(100)),
            Code::Cancelled,
            Carries::TimeoutExpired,
            false,
        ),
        (
            Target::Tonic(
                Handler::Answer(Code::Unavailable, "tcp connect error: Connection refused"),
                CALL_TIMEOUT,
            ),
            Code::Unavailable,
            Carries::NoSource,
            false,
        ),
        (
            Target::Interceptor,
            Code::Unavailable,
            Carries::NoSource,
            false,
        ),
        (
            Target::ClosedPort,
            Code::Unavailable,
            Carries::DialRefused,
            true,
        ),
        // Linux drops a SYN that a full accept queue cannot take, so the dial
        // stalls; other kernels may refuse it instead.
        #[cfg(target_os = "linux")]
        (
            Target::FullAcceptQueue,
            Code::Unavailable,
            Carries::DialTimedOut,
            true,
        ),
    ];
    let mut misclassified = Vec::new();
    for (target, code, carries, retried) in cases {
        let status = status_from(target).await;
        let observed = (
            status.code(),
            carries.in_chain(&status),
            is_client_transport_failure(&status),
        );
        if observed != (code, true, retried) {
            misclassified.push(format!(
                "{status:?} (sources {:?}): expected {code:?} carrying {carries:?}, \
                 retried={retried}",
                source_chain(&status)
            ));
        }
    }
    assert!(misclassified.is_empty(), "{misclassified:#?}");
}

/// Passes the server's HTTP/2 frames on to the client one at a time and
/// reports when one is the GOAWAY naming the last stream the server
/// accepted, which follows a graceful shutdown's first GOAWAY.
async fn forward_reporting_final_goaway(
    mut from_server: tokio::net::tcp::OwnedReadHalf,
    mut to_client: tokio::net::tcp::OwnedWriteHalf,
    final_goaway: Arc<Notify>,
) -> std::io::Result<()> {
    const NO_LAST_STREAM: u32 = 0x7fff_ffff;
    loop {
        let mut header = [0_u8; 9];
        from_server.read_exact(&mut header).await?;
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]);
        let mut payload = vec![0_u8; length as usize];
        from_server.read_exact(&mut payload).await?;
        to_client.write_all(&header).await?;
        to_client.write_all(&payload).await?;
        if header[3] == FRAME_GOAWAY
            && u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]) & NO_LAST_STREAM
                != NO_LAST_STREAM
        {
            final_goaway.notify_one();
        }
    }
}

/// A TCP relay to `server` for one connection that reports the server's
/// final GOAWAY. It accepts no second connection, as a Service whose only
/// endpoint is draining refuses new ones.
async fn relay_reporting_final_goaway(server: SocketAddr, final_goaway: Arc<Notify>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the relay");
    let address = listener.local_addr().expect("relay address");
    tokio::spawn(async move {
        let (client, _) = listener.accept().await.expect("accept the client");
        drop(listener);
        let upstream = TcpStream::connect(server)
            .await
            .expect("dial the tonic server");
        let (mut from_client, to_client) = client.into_split();
        let (from_server, mut to_server) = upstream.into_split();
        tokio::spawn(async move {
            let _ = tokio::io::copy(&mut from_client, &mut to_server).await;
        });
        let _ = forward_reporting_final_goaway(from_server, to_client, final_goaway).await;
    });
    address
}

/// A server shutting down gracefully finishes the call it accepted. A call
/// made once its GOAWAY named that call as the last one never reached it,
/// and counts as the transport failing.
#[tokio::test]
async fn a_graceful_shutdown_answers_accepted_calls_and_new_calls_are_retried() {
    let arrived = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (shut_down, shutdown) = tokio::sync::oneshot::channel::<()>();
    let server = serve(
        Handler::HoldUntil {
            arrived: arrived.clone(),
            release: release.clone(),
        },
        async {
            let _ = shutdown.await;
        },
    )
    .await;
    let final_goaway = Arc::new(Notify::new());
    let channel = channel(
        relay_reporting_final_goaway(server, final_goaway.clone()).await,
        CALL_TIMEOUT,
    );

    let accepted = tokio::spawn(check(channel.clone()));
    arrived.notified().await;
    shut_down.send(()).expect("the server is running");
    final_goaway.notified().await;
    let after_goaway = check(channel).await;
    release.notify_one();
    let accepted = accepted.await.expect("the accepted call");

    assert_eq!(accepted.code(), Code::NotFound, "{accepted:?}");
    assert!(
        is_client_transport_failure(&after_goaway),
        "{after_goaway:?} (sources {:?})",
        source_chain(&after_goaway)
    );
}
