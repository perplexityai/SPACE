//! A sandbox endpoint whose pod is gone can be given to a later pod. A
//! connection the data plane kept to it must not carry the later pod's calls.

use std::time::Duration;

use super::proxy::{Fault, FaultyProxy};
use super::*;
use crate::state::BackendChannelPool;

#[tokio::test]
async fn a_connection_whose_far_end_went_quiet_does_not_carry_the_next_call() {
    if !test_process() {
        return;
    }
    let (worker, worker_addr) = FakeWorker::start().await;
    let proxy = FaultyProxy::start(worker_addr, Fault::Quiet).await;
    let mut harness = Harness::with_runtime(worker, proxy.addr).await;
    // The production timings scaled down: a connection that has carried
    // nothing for 50 ms is asked to answer within 50 ms.
    harness.state.backend_channels = Arc::new(BackendChannelPool::with_endpoint_config(
        1,
        pplx_space_node_api_client::NodeApiEndpointConfig::default()
            .with_http2_keep_alive(Duration::from_millis(50), Duration::from_millis(50)),
    ));
    for _ in 0..2 {
        harness.worker.reply(Reply::Stat(Ok(StatResponse {
            info: Some(file_info("/workspace/a.txt", 5)),
        })));
    }

    assert_eq!(harness.send(stat(&harness)).await.status(), StatusCode::OK);
    proxy.fault_open_connections().await;
    // Its pings go unanswered, so the client closes the quiet connection
    // within about 100 ms; the bound only stops a test that never sees it.
    tokio::time::timeout(
        Duration::from_secs(10),
        proxy.closed.subscribe().wait_for(|&closed| closed == 1),
    )
    .await
    .expect("the client closes the connection that went quiet")
    .unwrap();

    // The call goes over a new connection, not the quiet one.
    let (status, _, body) = read_body(harness.send(stat(&harness)).await).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(harness.worker.calls().len(), 2);
    assert_eq!(proxy.opened.load(Ordering::SeqCst), 2);
}
