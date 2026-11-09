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
use tracing_subscriber::layer::SubscriberExt as _;

use pplx_space_public_api::PublicErrorCode;

use super::*;

/// Forwards each connection to `target`. A connection marked stale is reset
/// as soon as the client sends on it, as one to a pod address a later pod was
/// given is.
struct ResettingProxy {
    addr: SocketAddr,
    /// Connections numbered below this are stale.
    stale_below: Arc<AtomicU64>,
    opened: Arc<AtomicU64>,
}

impl ResettingProxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Self {
            addr: listener.local_addr().unwrap(),
            stale_below: Arc::default(),
            opened: Arc::default(),
        };
        let (stale_below, opened) = (Arc::clone(&proxy.stale_below), Arc::clone(&proxy.opened));
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let number = opened.fetch_add(1, Ordering::SeqCst);
                let upstream = TcpStream::connect(target).await.unwrap();
                tokio::spawn(relay(client, upstream, number, Arc::clone(&stale_below)));
            }
        });
        proxy
    }

    /// Every connection open now is stale, once the client has sent what
    /// it sends after a call is answered.
    async fn stale(&self) {
        tokio::time::sleep(Duration::from_millis(200)).await;
        self.stale_below
            .store(self.opened.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    /// Every connection, open now or later, is stale.
    fn stale_forever(&self) {
        self.stale_below.store(u64::MAX, Ordering::SeqCst);
    }
}

async fn relay(
    mut client: TcpStream,
    mut upstream: TcpStream,
    number: u64,
    stale_below: Arc<AtomicU64>,
) {
    // Dropping the client's socket resets it rather than closing it. A zero
    // linger discards unsent data on drop, so it never blocks.
    #[allow(deprecated)]
    client.set_linger(Some(Duration::ZERO)).unwrap();
    let (mut from_client, mut from_upstream) = (vec![0; 64 * 1024], vec![0; 64 * 1024]);
    loop {
        tokio::select! {
            read = client.read(&mut from_client) => {
                let Ok(read @ 1..) = read else { return };
                if number < stale_below.load(Ordering::SeqCst) {
                    return;
                }
                if upstream.write_all(&from_client[..read]).await.is_err() {
                    return;
                }
            }
            read = upstream.read(&mut from_upstream) => {
                let Ok(read @ 1..) = read else { return };
                if client.write_all(&from_upstream[..read]).await.is_err() {
                    return;
                }
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
