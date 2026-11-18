//! The network can stop carrying the data plane's packets to the API's
//! activation listener without resetting anything, as an egress policy that
//! drops them does. A files call must still be answered with the retryable
//! 503 promptly, not once its RPC's own deadline passes.

use std::net::SocketAddr;
use std::time::Duration;

use super::super::access::KeepAlive;
use super::proxy::{Fault, FaultyProxy};
use super::*;

/// Production's pings scaled down: a connection that has carried nothing for
/// 50 ms is asked to answer within 50 ms.
const FAST_KEEP_ALIVE: KeepAlive = KeepAlive {
    interval: Duration::from_millis(50),
    timeout: Duration::from_millis(50),
};

/// A harness whose data plane reaches its API at `api`, through the client
/// the server builds, pinging as `keep_alive` says.
async fn harness_reaching(
    api: FakeApi,
    worker: FakeWorker,
    api_addr: SocketAddr,
    keep_alive: KeepAlive,
) -> Harness {
    let mut state = crate::tests::test_data_plane_state().await;
    let access = NativeFileAccessClient::connect(format!("http://{api_addr}"), keep_alive)
        .await
        .unwrap();
    state.native_files = Some(Arc::new(NativeFiles::new(access, None)));
    Harness { state, api, worker }
}

async fn assert_retryable_503(response: Response) {
    let (status, headers, body) = read_body(response).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(headers[header::RETRY_AFTER], "1");
    let envelope: PublicErrorEnvelope = serde_json::from_slice(&body).unwrap();
    assert!(envelope.retryable);
}

#[tokio::test]
async fn a_call_on_an_activation_connection_that_went_quiet_is_a_prompt_503() {
    if !test_process() {
        return;
    }
    let (worker, worker_addr) = FakeWorker::start().await;
    let (api, api_addr) = FakeApi::start(worker_addr).await;
    let proxy = FaultyProxy::start(api_addr, Fault::Quiet).await;
    let harness = harness_reaching(api, worker, proxy.addr, FAST_KEEP_ALIVE).await;
    harness.worker.reply(Reply::Stat(Ok(StatResponse {
        info: Some(file_info("/workspace/a.txt", 5)),
    })));
    assert_eq!(harness.send(stat(&harness)).await.status(), StatusCode::OK);

    // The open connection, and any the client dials next, carry nothing.
    proxy.fault_every_connection();
    // Authorize's deadline is 15 s; the bound only stops a test whose call
    // waits for it.
    let response = tokio::time::timeout(Duration::from_secs(5), harness.send(stat(&harness)))
        .await
        .expect("the call is answered before its authorize deadline");
    assert_retryable_503(response).await;
    assert_eq!(harness.worker.calls().len(), 1);
}

/// Linux drops a SYN to a listener whose accept queue is full, so a dial to
/// it never completes, as one does where the network drops its packets.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn a_call_whose_activation_dial_goes_unanswered_is_a_prompt_503() {
    if !test_process() {
        return;
    }
    let (worker, worker_addr) = FakeWorker::start().await;
    let (api, _) = FakeApi::start(worker_addr).await;
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(0).unwrap();
    let unanswered = listener.local_addr().unwrap();
    // Never accepted: these fill the queue.
    let mut queued = Vec::new();
    for _ in 0..2 {
        if let Ok(Ok(stream)) = tokio::time::timeout(
            Duration::from_millis(200),
            tokio::net::TcpStream::connect(unanswered),
        )
        .await
        {
            queued.push(stream);
        }
    }
    let harness = harness_reaching(api, worker, unanswered, FAST_KEEP_ALIVE).await;

    let started = std::time::Instant::now();
    let response = tokio::time::timeout(Duration::from_secs(10), harness.send(stat(&harness)))
        .await
        .expect("the call is answered once its dial gives up");
    let elapsed = started.elapsed();
    assert_retryable_503(response).await;
    // The dial's own bound, with room for a slow runner; three times it
    // would not fit.
    assert!(
        elapsed < Duration::from_millis(2500),
        "the dial gave up after {elapsed:?}"
    );
    assert!(harness.worker.calls().is_empty());
    drop((listener, queued));
}
