//! A call whose connection to the sandbox's runtime fails before the call is
//! answered: a connection kept to a sandbox endpoint can be dead when used, as
//! when the endpoint is a pod address a later pod was given.

use std::fmt;
use std::net::SocketAddr;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing_subscriber::layer::SubscriberExt as _;

use pplx_space_public_api::PublicErrorCode;

use super::*;

/// Forwards each connection to `target`. A connection marked stale is reset
/// when the client starts its next call on it, as one to a pod address a
/// later pod was given is; the client's control frames still pass, since a
/// connection that carries no call can outlive its far end unnoticed.
struct ResettingProxy {
    addr: SocketAddr,
    /// Connections numbered below this are stale.
    stale_below: Arc<AtomicU64>,
    opened: Arc<AtomicU64>,
    /// Each connection's relay, asked to have the client answer a ping.
    pings: Arc<Mutex<Vec<mpsc::UnboundedSender<oneshot::Sender<()>>>>>,
}

impl ResettingProxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Self {
            addr: listener.local_addr().unwrap(),
            stale_below: Arc::default(),
            opened: Arc::default(),
            pings: Arc::default(),
        };
        let (stale_below, opened, pings) = (
            Arc::clone(&proxy.stale_below),
            Arc::clone(&proxy.opened),
            Arc::clone(&proxy.pings),
        );
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let number = opened.fetch_add(1, Ordering::SeqCst);
                let upstream = TcpStream::connect(target).await.unwrap();
                let (ping, pinged) = mpsc::unbounded_channel();
                pings.lock().unwrap().push(ping);
                tokio::spawn(relay(
                    client,
                    upstream,
                    number,
                    Arc::clone(&stale_below),
                    pinged,
                ));
            }
        });
        proxy
    }

    /// Every connection open now is stale. Each then carries a ping and its
    /// answer, a control frame from the client as a late window update is,
    /// before this returns: whatever the client sent for the call before is
    /// past the proxy by then, and only its next call resets the connection.
    async fn stale(&self) {
        self.stale_below
            .store(self.opened.load(Ordering::SeqCst), Ordering::SeqCst);
        let pings = self.pings.lock().unwrap().clone();
        for ping in pings {
            let (answered, answer) = oneshot::channel();
            if ping.send(answered).is_ok() {
                tokio::time::timeout(Duration::from_secs(5), answer)
                    .await
                    .expect("the client answers a ping in time")
                    .expect("a stale connection carries the client's ping answer");
            }
        }
    }

    /// Every connection, open now or later, is stale.
    fn stale_forever(&self) {
        self.stale_below.store(u64::MAX, Ordering::SeqCst);
    }
}

/// The client connection preface, before its first frame.
const CLIENT_PREFACE_LEN: usize = 24;
const FRAME_HEADER_LEN: usize = 9;
const HEADERS_FRAME: u8 = 0x1;
const PING_FRAME: u8 = 0x6;
const PING_ACK: u8 = 0x1;
/// The payload of the proxy's own pings.
const PROXY_PING: [u8; 8] = *b"proxy-pi";

/// One HTTP/2 frame, or the client preface, as it was read.
struct Frame {
    /// `None` for the preface.
    kind: Option<u8>,
    flags: u8,
    bytes: Vec<u8>,
}

impl Frame {
    fn is_proxy_ping_answer(&self) -> bool {
        self.kind == Some(PING_FRAME)
            && self.flags & PING_ACK != 0
            && self.bytes[FRAME_HEADER_LEN..] == PROXY_PING
    }
}

/// One direction of a connection, cut into whole frames.
struct Frames {
    buffer: Vec<u8>,
    /// Bytes of preface still to come before the first frame.
    preface: usize,
}

impl Frames {
    fn new(preface: usize) -> Self {
        Self {
            buffer: Vec::new(),
            preface,
        }
    }

    fn next(&mut self) -> Option<Frame> {
        if self.preface > 0 {
            if self.buffer.len() < self.preface {
                return None;
            }
            let bytes = self.buffer.drain(..self.preface).collect();
            self.preface = 0;
            return Some(Frame {
                kind: None,
                flags: 0,
                bytes,
            });
        }
        let header = self.buffer.get(..FRAME_HEADER_LEN)?;
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        if self.buffer.len() < FRAME_HEADER_LEN + length {
            return None;
        }
        let bytes: Vec<u8> = self.buffer.drain(..FRAME_HEADER_LEN + length).collect();
        Some(Frame {
            kind: Some(bytes[3]),
            flags: bytes[4],
            bytes,
        })
    }
}

async fn relay(
    mut client: TcpStream,
    mut upstream: TcpStream,
    number: u64,
    stale_below: Arc<AtomicU64>,
    mut pings: mpsc::UnboundedReceiver<oneshot::Sender<()>>,
) {
    // Dropping the client's socket resets it rather than closing it. A zero
    // linger discards unsent data on drop, so it never blocks.
    #[allow(deprecated)]
    client.set_linger(Some(Duration::ZERO)).unwrap();
    let (mut from_client, mut from_upstream) = (Frames::new(CLIENT_PREFACE_LEN), Frames::new(0));
    let (mut client_read, mut upstream_read) = (vec![0; 64 * 1024], vec![0; 64 * 1024]);
    let mut awaiting_answer: Option<oneshot::Sender<()>> = None;
    loop {
        tokio::select! {
            read = client.read(&mut client_read) => {
                let Ok(read @ 1..) = read else { return };
                from_client.buffer.extend_from_slice(&client_read[..read]);
                while let Some(frame) = from_client.next() {
                    if frame.is_proxy_ping_answer() {
                        if let Some(answered) = awaiting_answer.take() {
                            let _ = answered.send(());
                        }
                        continue;
                    }
                    // The call never reaches the runtime.
                    if frame.kind == Some(HEADERS_FRAME)
                        && number < stale_below.load(Ordering::SeqCst)
                    {
                        return;
                    }
                    if upstream.write_all(&frame.bytes).await.is_err() {
                        return;
                    }
                }
            }
            read = upstream.read(&mut upstream_read) => {
                let Ok(read @ 1..) = read else { return };
                from_upstream.buffer.extend_from_slice(&upstream_read[..read]);
                while let Some(frame) = from_upstream.next() {
                    if client.write_all(&frame.bytes).await.is_err() {
                        return;
                    }
                }
            }
            Some(answered) = pings.recv() => {
                // Between the runtime's whole frames, so the client reads it
                // as a frame of its own.
                let mut ping = vec![0, 0, 8, PING_FRAME, 0, 0, 0, 0, 0];
                ping.extend_from_slice(&PROXY_PING);
                if client.write_all(&ping).await.is_err() {
                    return;
                }
                awaiting_answer = Some(answered);
            }
        }
    }
}

/// The level and message of each event the gRPC client trace logs.
#[derive(Clone, Default)]
struct ClientLog(Arc<Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for ClientLog {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Message(String);
        impl tracing::field::Visit for Message {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        if event.metadata().target() != "grpc.client" {
            return;
        }
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0
            .lock()
            .unwrap()
            .push((*event.metadata().level(), message.0));
    }
}

impl ClientLog {
    /// The transport failures logged, by level.
    fn transport_failures(&self) -> Vec<tracing::Level> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, message)| message.contains("transport error"))
            .map(|(level, _)| *level)
            .collect()
    }
}

/// A harness whose runtime is behind a [`ResettingProxy`], and what the gRPC
/// client trace logs while the returned guard lives.
async fn harness_behind_proxy() -> (
    Harness,
    ResettingProxy,
    ClientLog,
    tracing::subscriber::DefaultGuard,
) {
    let log = ClientLog::default();
    let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(log.clone()));
    let (worker, worker_addr) = FakeWorker::start().await;
    let proxy = ResettingProxy::start(worker_addr).await;
    let harness = Harness::with_runtime(worker, proxy.addr).await;
    (harness, proxy, log, guard)
}

/// A runtime that resets every call's stream before answering it, and the
/// number of calls it has received.
async fn start_resetting_runtime() -> (SocketAddr, Arc<AtomicU64>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let calls = Arc::<AtomicU64>::default();
    let received = Arc::clone(&calls);
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            let received = Arc::clone(&received);
            tokio::spawn(async move {
                let Ok(mut connection) = h2::server::handshake(socket).await else {
                    return;
                };
                while let Some(Ok((_, mut respond))) = connection.accept().await {
                    received.fetch_add(1, Ordering::SeqCst);
                    respond.send_reset(h2::Reason::CANCEL);
                }
            });
        }
    });
    (addr, calls)
}

fn stat_ok(harness: &Harness) {
    harness.worker.reply(Reply::Stat(Ok(StatResponse {
        info: Some(file_info("/workspace/a.txt", 5)),
    })));
}

#[tokio::test]
async fn a_read_whose_connection_was_reset_is_answered_over_a_new_one() {
    if !test_process() {
        return;
    }
    let (harness, proxy, log, _guard) = harness_behind_proxy().await;
    stat_ok(&harness);
    stat_ok(&harness);
    assert_eq!(harness.send(stat(&harness)).await.status(), StatusCode::OK);
    proxy.stale().await;

    let (status, _, body) = read_body(harness.send(stat(&harness)).await).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    // The reset call never reached the runtime; the one sent again did.
    assert_eq!(harness.worker.calls().len(), 2);
    // The reset is no error of the call's: it was sent again and answered.
    assert_eq!(log.transport_failures(), [tracing::Level::WARN]);
}

#[tokio::test]
async fn a_read_the_runtime_cannot_be_reached_for_is_the_retryable_503() {
    if !test_process() {
        return;
    }
    let (harness, proxy, log, _guard) = harness_behind_proxy().await;
    proxy.stale_forever();

    let (status, headers, body) = read_body(harness.send(stat(&harness)).await).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::ServiceUnavailable);
    assert!(envelope.retryable);
    assert_eq!(headers[header::RETRY_AFTER], "1");
    assert!(harness.worker.calls().is_empty());
    // The attempt sent again failed too, which is the error.
    assert_eq!(
        log.transport_failures(),
        [tracing::Level::WARN, tracing::Level::ERROR]
    );
}

#[tokio::test]
async fn a_mutation_whose_connection_was_reset_is_not_sent_again() {
    if !test_process() {
        return;
    }
    let (harness, proxy, log, _guard) = harness_behind_proxy().await;
    stat_ok(&harness);
    assert_eq!(harness.send(stat(&harness)).await.status(), StatusCode::OK);
    proxy.stale().await;

    let mkdir = harness.post_keyed("mkdir", "key-1", json!({"path": "/workspace/new"}));
    let (status, _, body) = read_body(harness.send(mkdir).await).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert_eq!(envelope.code, PublicErrorCode::ServiceUnavailable);
    // The mkdir may have reached the runtime before the connection failed,
    // and nothing deduplicates it, so it is not to be sent again blindly.
    assert!(!envelope.retryable);
    assert_eq!(harness.worker.calls().len(), 1);
    assert_eq!(log.transport_failures(), [tracing::Level::ERROR]);
}

/// The runtime resetting a call's stream is its own refusal of the call, not a
/// connection that failed under it, so neither a read-only call nor a stream
/// open sends it again, and its trace logs the failure as the error it is.
#[tokio::test]
async fn a_call_whose_stream_the_runtime_reset_is_not_sent_again_and_logs_an_error() {
    if !test_process() {
        return;
    }
    for (operation, body) in [
        ("stat", json!({"path": "/workspace/a.txt"})),
        ("list", json!({"path": "/workspace"})),
    ] {
        let log = ClientLog::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(log.clone()));
        let (worker, _) = FakeWorker::start().await;
        let (runtime, calls) = start_resetting_runtime().await;
        let harness = Harness::with_runtime(worker, runtime).await;

        let response = harness.send(harness.post(operation, body)).await;
        assert!(!response.status().is_success(), "{operation}");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "{operation}");
        assert_eq!(
            log.transport_failures(),
            [tracing::Level::ERROR],
            "{operation}"
        );
    }
}
