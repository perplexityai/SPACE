use super::port_forwarder::BindOutcome;
use super::*;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Bind a v4 loopback socket to grab an ephemeral port, then drop it
/// before the test reuses the port. Cheaper + more portable than
/// hand-coding port ranges.
fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

/// Opens a forwarded port on a free port and returns it. A port picked with
/// [`pick_free_port`] can be taken by a concurrent test (a listener, or an
/// outbound connection's ephemeral port) before the manager binds it; the
/// manager then rightly reports `Direct`. That is the test's precondition
/// failing, not the behaviour under test, so try another port.
async fn open_forwarded_on_a_free_port(manager: &LoopbackBridgeManager) -> u16 {
    for _ in 0..20 {
        let port = pick_free_port();
        match manager.open(port, Some(MAX_OPEN_WAIT)).await {
            (LoopbackBridgeStatus::LoopbackForwarded, _) => return port,
            (LoopbackBridgeStatus::Direct, _) => continue,
            (status, detail) => panic!("initial bridge failed: {status:?} {detail:?}"),
        }
    }
    panic!("no free port could be forwarded in 20 attempts");
}

/// Listener factory that fails the first `failures` calls with the
/// given error kind, then binds a real ephemeral loopback listener.
/// `calls` counts every bind attempt so tests can assert bounded
/// retries. The manager classifies on the factory result alone, so the
/// successful bind does not need to land on the requested address.
fn flaky_listener_factory(
    failures: usize,
    kind: std::io::ErrorKind,
    calls: Arc<AtomicUsize>,
) -> ListenerFactory {
    Arc::new(move |_addr| {
        let calls = calls.clone();
        Box::pin(async move {
            let attempt = calls.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt <= failures {
                Err(std::io::Error::new(kind, "injected bind failure"))
            } else {
                TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await
            }
        })
    })
}

fn test_manager(factory: ListenerFactory) -> (LoopbackBridgeManager, CancellationToken) {
    let shutdown = CancellationToken::new();
    let manager = LoopbackBridgeManager::for_tests(
        shutdown.clone(),
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        factory,
        MAX_ACTIVE_FORWARD_CONNECTIONS,
        FORWARD_CONNECTION_IDLE_TIMEOUT,
    );
    (manager, shutdown)
}

#[tokio::test]
async fn pending_open_finishes_when_closed_or_shutdown() {
    for shutdown_service in [false, true] {
        let (manager, shutdown) = test_manager(Arc::new(|_| Box::pin(std::future::pending())));
        let opening_manager = manager.clone();
        let opening =
            tokio::spawn(async move { opening_manager.open(9012, Some(MAX_OPEN_WAIT)).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !manager.state.ports.lock().await.contains_key(&9012) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if shutdown_service {
            manager.shutdown().await;
        } else {
            manager.close(9012).await;
        }
        let mut opening = opening;
        let result = tokio::time::timeout(Duration::from_millis(100), &mut opening).await;
        shutdown.cancel();
        if result.is_err() {
            opening.abort();
            let _ = opening.await;
        }
        assert_eq!(
            result.expect("closed port left open waiting").unwrap().0,
            LoopbackBridgeStatus::Failed
        );
    }
}

#[tokio::test(start_paused = true)]
async fn authorized_ports_have_a_limit_but_reopen_and_close_remain_available() {
    let (manager, shutdown) = test_manager(Arc::new(|_| Box::pin(std::future::pending())));
    for port in 1..=512 {
        assert_ne!(
            manager.open(port, Some(Duration::ZERO)).await.0,
            LoopbackBridgeStatus::Failed
        );
    }
    assert_eq!(
        manager.open(513, Some(Duration::ZERO)).await.0,
        LoopbackBridgeStatus::Failed
    );
    assert_ne!(
        manager.open(1, Some(Duration::ZERO)).await.0,
        LoopbackBridgeStatus::Failed
    );
    manager.close(1).await;
    assert_ne!(
        manager.open(513, Some(Duration::ZERO)).await.0,
        LoopbackBridgeStatus::Failed
    );
    shutdown.cancel();
}

/// The wake-path race: the first bind attempts land while the guest's
/// link-local address is not yet present (`EADDRNOTAVAIL`). The driver
/// must retry within its bounded budget and end `LoopbackForwarded`
/// once the address appears — a single failed attempt must not become
/// a terminal status that leaves the exposed port dead until the guest
/// agent restarts.
#[tokio::test(start_paused = true)]
async fn open_recovers_when_bind_races_the_address_coming_up() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (manager, shutdown) = test_manager(flaky_listener_factory(
        2,
        std::io::ErrorKind::AddrNotAvailable,
        calls.clone(),
    ));

    let (status, detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;

    assert_eq!(
        status,
        LoopbackBridgeStatus::LoopbackForwarded,
        "bind must recover after transient address failures; detail: {detail:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "two failures + one success"
    );
    shutdown.cancel();
}

/// Persistent failure: the retry budget is bounded. The driver stops
/// after exactly [`BIND_RETRY_ATTEMPTS`] attempts, reports
/// `BindConflict` with the final bind error in the detail, and never
/// spins in the background. The detail must tell the real story —
/// retry-exhausted transient errors (the wake-path `EADDRNOTAVAIL`
/// race) are the primary producer of this status, not a conflicting
/// listener, so a "something else holds the address" message would
/// send operators chasing a process that does not exist.
#[tokio::test(start_paused = true)]
async fn open_bind_retry_is_bounded() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (manager, shutdown) = test_manager(flaky_listener_factory(
        usize::MAX,
        std::io::ErrorKind::AddrNotAvailable,
        calls.clone(),
    ));

    let (status, detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;

    assert_eq!(status, LoopbackBridgeStatus::BindConflict);
    assert_eq!(calls.load(Ordering::SeqCst), BIND_RETRY_ATTEMPTS);
    let detail = detail.expect("a failed bridge must carry a detail");
    assert!(
        detail.contains(&format!("after {BIND_RETRY_ATTEMPTS} attempts"))
            && detail.contains("injected bind failure"),
        "detail must surface the final bind error, got: {detail}"
    );

    // No background attempts after the driver finished.
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(calls.load(Ordering::SeqCst), BIND_RETRY_ATTEMPTS);
    shutdown.cancel();
}

/// While the driver is still mid-bind-retry at the caller's deadline,
/// `open` reports `WaitingForListener` and the detail names both
/// in-flight phases — bind-IP autodetect and the bounded bind retries
/// — so an operator reading the persisted exposure row doesn't
/// misread a busy wake as a stuck autodetect.
#[tokio::test(start_paused = true)]
async fn open_deadline_reports_waiting_while_bind_retries_in_flight() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (manager, shutdown) = test_manager(flaky_listener_factory(
        usize::MAX,
        std::io::ErrorKind::AddrNotAvailable,
        calls.clone(),
    ));

    // Shorter than one retry backoff: the driver cannot reach a
    // terminal status before the caller's deadline.
    let (status, detail) = manager.open(9012, Some(BIND_RETRY_DELAY / 2)).await;

    assert_eq!(status, LoopbackBridgeStatus::WaitingForListener);
    let detail = detail.expect("a waiting bridge must carry a detail");
    assert!(
        detail.contains("bind retries"),
        "waiting detail must mention in-flight bind retries, got: {detail}"
    );
    shutdown.cancel();
}

/// The re-arm invariant for the wake path: once a port's bind failed
/// terminally (e.g. the address never came up within the retry
/// budget), a later `open` for the same port — the exposure
/// reconciler retries every tick — must re-drive the bind instead of
/// returning the cached failure forever.
#[tokio::test(start_paused = true)]
async fn open_rearms_port_after_terminal_bind_failure() {
    let fail = Arc::new(AtomicBool::new(true));
    let factory: ListenerFactory = Arc::new({
        let fail = fail.clone();
        move |_addr| {
            let fail = fail.clone();
            Box::pin(async move {
                if fail.load(Ordering::SeqCst) {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "address not yet present",
                    ))
                } else {
                    TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await
                }
            })
        }
    });
    let (manager, shutdown) = test_manager(factory);

    let (status, _detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(status, LoopbackBridgeStatus::BindConflict);

    // Address appears (e.g. the interface finished coming up); the
    // reconciler's next open must recover the listener.
    fail.store(false, Ordering::SeqCst);
    let (status, detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(
        status,
        LoopbackBridgeStatus::LoopbackForwarded,
        "re-open after terminal bind failure must re-drive the bind; detail: {detail:?}"
    );
    shutdown.cancel();
}

/// `EADDRINUSE` keeps its meaning: another process owns the address,
/// the port is `Direct`, and no retry burns attempts on it.
#[tokio::test(start_paused = true)]
async fn open_addr_in_use_reports_direct_without_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (manager, shutdown) = test_manager(flaky_listener_factory(
        usize::MAX,
        std::io::ErrorKind::AddrInUse,
        calls.clone(),
    ));

    let (status, _detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;

    assert_eq!(status, LoopbackBridgeStatus::Direct);
    assert_eq!(calls.load(Ordering::SeqCst), 1, "AddrInUse must not retry");

    // A healthy (`Direct`) port is steady state: a re-open reuses the
    // cached classification instead of re-driving the bind.
    let (status, _detail) = manager.open(9012, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(status, LoopbackBridgeStatus::Direct);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.cancel();
}

#[tokio::test]
async fn close_releases_the_listener_before_reopen_returns() {
    let (manager, shutdown) = test_manager(system_listener_factory());
    let port = open_forwarded_on_a_free_port(&manager).await;

    manager.close(port).await;

    let (status, detail) = manager.open(port, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(
        status,
        LoopbackBridgeStatus::LoopbackForwarded,
        "reopen raced the old listener teardown: {detail:?}"
    );
    shutdown.cancel();
}

/// Cancellation aborts the retry loop promptly — including mid-backoff
/// — without publishing a bogus terminal status or leaking attempts.
#[tokio::test(start_paused = true)]
async fn bind_retry_stops_on_cancellation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = flaky_listener_factory(
        usize::MAX,
        std::io::ErrorKind::AddrNotAvailable,
        calls.clone(),
    );
    let cancel = CancellationToken::new();
    let addr: SocketAddr = (Ipv4Addr::LOCALHOST, pick_free_port()).into();

    let task = tokio::spawn({
        let cancel = cancel.clone();
        async move { port_forwarder::bind_with_retry(factory, addr, &cancel).await }
    });
    // Let the first attempt fail and the backoff sleep start.
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();

    let outcome = task.await.expect("bind_with_retry task panicked");
    assert!(matches!(outcome, BindOutcome::Cancelled));
    let attempts_at_cancel = calls.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(5)).await;
    assert_eq!(calls.load(Ordering::SeqCst), attempts_at_cancel);
}

/// Echo server on an ephemeral loopback port; returns the bound
/// address. Binding `127.0.0.1:0` directly (instead of pre-picking a
/// free port and re-binding it) avoids the pick-then-bind TOCTOU race
/// that intermittently fails with `AddrInUse` when a concurrent test
/// grabs the port in between. Use [`spawn_echo_at`] only when the
/// address must be fixed before the listener exists.
async fn spawn_echo() -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = listener.local_addr().unwrap();
    (addr, spawn_echo_loop(listener))
}

/// Echo server at a caller-chosen address, for the lazy-dial recovery
/// test where the upstream must appear at a port that was fixed while
/// nothing listened on it. Inherently pick-then-bind; keep its use to
/// that one scenario.
async fn spawn_echo_at(addr: SocketAddr) -> tokio::task::JoinHandle<()> {
    let listener = TcpListener::bind(addr).await.unwrap();
    spawn_echo_loop(listener)
}

fn spawn_echo_loop(listener: TcpListener) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 64];
                while let Ok(n) = sock.read(&mut buf).await {
                    if n == 0 {
                        return;
                    }
                    if sock.write_all(&buf[..n]).await.is_err() {
                        return;
                    }
                }
            });
        }
    })
}

/// Forwarder on an ephemeral loopback port (same TOCTOU rationale as
/// [`spawn_echo`]); returns the address clients should dial.
async fn start_forwarder(
    dial_addr: SocketAddr,
) -> (SocketAddr, CancellationToken, tokio::task::JoinHandle<()>) {
    start_forwarder_with_limits(
        dial_addr,
        MAX_ACTIVE_FORWARD_CONNECTIONS,
        FORWARD_CONNECTION_IDLE_TIMEOUT,
    )
    .await
}

async fn start_forwarder_with_limits(
    dial_addr: SocketAddr,
    active_connection_limit: usize,
    idle_timeout: Duration,
) -> (SocketAddr, CancellationToken, tokio::task::JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("forwarder bind failed");
    let bind_addr = listener.local_addr().unwrap();
    let join = tokio::spawn(forward::run(
        listener,
        bind_addr,
        dial_addr,
        Arc::new(tokio::sync::Semaphore::new(active_connection_limit)),
        idle_timeout,
        cancel.clone(),
    ));
    (bind_addr, cancel, join)
}

#[tokio::test]
async fn forwarder_enforces_global_connection_limit_and_releases_capacity() {
    let (up_addr, echo) = spawn_echo().await;
    let (fw_addr, cancel, join) =
        start_forwarder_with_limits(up_addr, 1, FORWARD_CONNECTION_IDLE_TIMEOUT).await;

    let mut first = TcpStream::connect(fw_addr).await.unwrap();
    first.write_all(b"a").await.unwrap();
    let mut byte = [0u8; 1];
    first.read_exact(&mut byte).await.unwrap();

    let mut waiting = TcpStream::connect(fw_addr).await.unwrap();
    waiting.write_all(b"b").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), waiting.read(&mut byte))
            .await
            .is_err(),
        "over-capacity connection must remain queued instead of being reset"
    );

    drop(first);
    tokio::time::timeout(Duration::from_secs(1), waiting.read_exact(&mut byte))
        .await
        .expect("queued connection must start after capacity is released")
        .unwrap();
    assert_eq!(byte, [b'b']);

    cancel.cancel();
    let _ = join.await;
    echo.abort();
}

#[tokio::test]
async fn forwarder_closes_only_after_connection_is_idle() {
    let (up_addr, echo) = spawn_echo().await;
    let idle_timeout = Duration::from_millis(100);
    let (fw_addr, cancel, join) = start_forwarder_with_limits(up_addr, 1, idle_timeout).await;
    let mut client = TcpStream::connect(fw_addr).await.unwrap();
    let mut byte = [0u8; 1];
    client.write_all(b"0").await.unwrap();
    client.read_exact(&mut byte).await.unwrap();

    // Keep paused time from auto-advancing while real sockets await OS readiness.
    tokio::time::pause();
    let clock_guard = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    tokio::time::advance(Duration::from_millis(60)).await;
    tokio::task::yield_now().await;
    client.write_all(b"a").await.unwrap();
    client.read_exact(&mut byte).await.unwrap();
    tokio::time::advance(Duration::from_millis(60)).await;
    tokio::task::yield_now().await;
    client.write_all(b"b").await.unwrap();
    client.read_exact(&mut byte).await.unwrap();

    tokio::time::advance(Duration::from_millis(120)).await;
    clock_guard.abort();
    assert!(clock_guard.await.unwrap_err().is_cancelled());
    tokio::time::resume();
    let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut byte))
        .await
        .expect("idle connection closes");
    assert!(
        matches!(read, Ok(0) | Err(_)),
        "idle connection must be closed, got {read:?}"
    );

    cancel.cancel();
    let _ = join.await;
    echo.abort();
}

/// Happy path: forwarder accepts an inbound TCP connection and proxies
/// to a live upstream. Sanity that the new code path didn't break the
/// thing the bridge is supposed to do.
#[tokio::test]
async fn forwarder_relays_to_live_upstream() {
    let (up_addr, echo) = spawn_echo().await;
    let (fw_addr, cancel, join) = start_forwarder(up_addr).await;

    let mut client = TcpStream::connect(fw_addr).await.unwrap();
    client.write_all(b"hello").await.unwrap();
    let mut buf = [0u8; 5];
    client.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    cancel.cancel();
    let _ = join.await;
    echo.abort();
}

/// Lazy-dial recovery: the forwarder is up before the upstream
/// listener exists. The first inbound connection accepts, fails to
/// dial upstream (`ECONNREFUSED`), and closes. Once the upstream
/// comes up, subsequent inbound connections succeed — without anyone
/// calling `InstallLoopbackBridge` again. This is the property that
/// lets us delete the per-second listener-state polling loop and
/// still serve "user app boots after first request hits ECW" cleanly.
#[tokio::test]
async fn forwarder_recovers_when_upstream_appears_after_bind() {
    let up_port = pick_free_port();
    let up_addr: SocketAddr = (Ipv4Addr::LOCALHOST, up_port).into();
    // Upstream NOT listening yet.

    let (fw_addr, cancel, join) = start_forwarder(up_addr).await;

    // First client: accepted, then closed because dial fails.
    let mut early = TcpStream::connect(fw_addr).await.unwrap();
    let mut buf = [0u8; 1];
    // EOF (Ok(0)) within the 5 s LOOPBACK_DIAL_TIMEOUT in forward.rs.
    let n = tokio::time::timeout(Duration::from_secs(7), early.read(&mut buf))
        .await
        .expect("read should not hang past dial timeout")
        .expect("read should not error");
    assert_eq!(
        n, 0,
        "expected forwarder to close the inbound on dial failure"
    );

    // Bring upstream online.
    let echo = spawn_echo_at(up_addr).await;

    // Subsequent client succeeds — same forwarder, no re-install.
    let mut later = TcpStream::connect(fw_addr).await.unwrap();
    later.write_all(b"world").await.unwrap();
    let mut buf = [0u8; 5];
    later.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"world");

    cancel.cancel();
    let _ = join.await;
    echo.abort();
}

/// When something else already holds `bind_ip:port` (in production, the
/// user app bound `0.0.0.0:port` or `eth0:port` directly), the real
/// bind must surface `AddrInUse` through [`port_forwarder::bind_with_retry`]
/// so the manager can publish `Direct` instead of `BindConflict`. This
/// is the only path `LoopbackBridgeStatus::Direct` takes.
#[tokio::test]
async fn real_bind_conflict_surfaces_addr_in_use() {
    // Hold the port from the moment it is chosen. Picking a free port and
    // binding it again leaves a window in which a concurrent test can be
    // handed the same port, or a child forked by one still holds the picking
    // socket.
    let _hold = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let addr = _hold.local_addr().unwrap();

    let cancel = CancellationToken::new();
    let outcome = port_forwarder::bind_with_retry(system_listener_factory(), addr, &cancel).await;
    assert!(matches!(outcome, BindOutcome::AddrInUse));
}

/// Listener factory that binds for real and keeps a duplicate of each
/// listener's descriptor, as a child forked while the listener was open
/// holds a copy of it until it execs.
#[cfg(target_os = "linux")]
fn inheriting_listener_factory(
    inherited: Arc<std::sync::Mutex<Vec<std::os::fd::OwnedFd>>>,
) -> ListenerFactory {
    Arc::new(move |addr| {
        let inherited = inherited.clone();
        Box::pin(async move {
            let listener = TcpListener::bind(addr).await?;
            let copy = std::os::fd::AsFd::as_fd(&listener).try_clone_to_owned()?;
            inherited.lock().unwrap().push(copy);
            Ok(listener)
        })
    })
}

/// A child forked by a concurrent process spawn keeps the released listener
/// open until it execs. A reopen while it still holds that copy must forward
/// again, not report `Direct` for a port nothing else owns. Linux only: BSD
/// kernels keep a shut-down listening socket bound.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn reopen_succeeds_while_a_child_still_holds_the_released_listener() {
    let inherited = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (manager, shutdown) = test_manager(inheriting_listener_factory(inherited.clone()));
    let port = open_forwarded_on_a_free_port(&manager).await;
    manager.close(port).await;

    // The "child" still holds its copy of the released listener.
    assert_eq!(inherited.lock().unwrap().len(), 1);
    let (status, detail) = manager.open(port, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(
        status,
        LoopbackBridgeStatus::LoopbackForwarded,
        "reopen reported the inherited copy as a foreign listener: {detail:?}"
    );
    inherited.lock().unwrap().clear();
    shutdown.cancel();
}

/// Releasing a port shuts down only the manager's own listener: a port that
/// another process listens on is still reported `Direct`.
#[tokio::test]
async fn a_foreign_listener_is_still_reported_direct() {
    let (manager, shutdown) = test_manager(system_listener_factory());
    let holder = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = holder.local_addr().unwrap().port();
    let (status, _) = manager.open(port, Some(MAX_OPEN_WAIT)).await;
    assert_eq!(status, LoopbackBridgeStatus::Direct);
    shutdown.cancel();
}
