//! A sandbox endpoint whose pod is gone can be given to a later pod. A
//! connection the data plane kept to it must not carry the later pod's calls.

use std::net::SocketAddr;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::state::BackendChannelPool;

/// Forwards each connection to `target` until [`QuietingProxy::quiet`]: the
/// connections open then stay open but carry nothing more, as one to a pod
/// that is gone, while new ones are forwarded, as to the pod given its
/// address since.
struct QuietingProxy {
    addr: SocketAddr,
    /// Connections numbered below this carry nothing.
    quiet_below: Arc<AtomicU64>,
    opened: Arc<AtomicU64>,
}

impl QuietingProxy {
    async fn start(target: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Self {
            addr: listener.local_addr().unwrap(),
            quiet_below: Arc::default(),
            opened: Arc::default(),
        };
        let (quiet_below, opened) = (Arc::clone(&proxy.quiet_below), Arc::clone(&proxy.opened));
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let number = opened.fetch_add(1, Ordering::SeqCst);
                let upstream = TcpStream::connect(target).await.unwrap();
                let (client_read, client_write) = client.into_split();
                let (upstream_read, upstream_write) = upstream.into_split();
                for (from, to) in [
                    (
                        Box::new(client_read) as Box<dyn tokio::io::AsyncRead + Send + Unpin>,
                        Box::new(upstream_write) as Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
                    ),
                    (Box::new(upstream_read), Box::new(client_write)),
                ] {
                    let quiet_below = Arc::clone(&quiet_below);
                    tokio::spawn(forward(from, to, number, quiet_below));
                }
            }
        });
        proxy
    }

    /// Every connection open now goes quiet.
    fn quiet(&self) {
        self.quiet_below
            .store(self.opened.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

async fn forward(
    mut from: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    mut to: Box<dyn tokio::io::AsyncWrite + Send + Unpin>,
    number: u64,
    quiet_below: Arc<AtomicU64>,
) {
    let mut buffer = vec![0; 64 * 1024];
    while let Ok(read) = from.read(&mut buffer).await {
        if read == 0 {
            return;
        }
        if number < quiet_below.load(Ordering::SeqCst) {
            continue;
        }
        if to.write_all(&buffer[..read]).await.is_err() {
            return;
        }
    }
}

#[tokio::test]
async fn a_connection_whose_far_end_went_quiet_does_not_carry_the_next_call() {
    if !test_process() {
        return;
    }
    let (worker, worker_addr) = FakeWorker::start().await;
    let proxy = QuietingProxy::start(worker_addr).await;
    let (api, api_addr) = FakeApi::start(proxy.addr).await;
    let mut state = crate::tests::test_data_plane_state().await;
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{api_addr}"))
        .unwrap()
        .connect_lazy();
    state.native_files = Some(Arc::new(NativeFiles::new(
        NativeFileAccessClient::new(channel),
        None,
    )));
    // The production timings scaled down: a connection that has carried
    // nothing for 50 ms is asked to answer within 50 ms.
    state.backend_channels = Arc::new(BackendChannelPool::with_endpoint_config(
        1,
        pplx_space_node_api_client::NodeApiEndpointConfig::default()
            .with_http2_keep_alive(Duration::from_millis(50), Duration::from_millis(50)),
    ));
    let harness = Harness { state, api, worker };
    for _ in 0..2 {
        harness.worker.reply(Reply::Stat(Ok(StatResponse {
            info: Some(file_info("/workspace/a.txt", 5)),
        })));
    }

    assert_eq!(harness.send(stat(&harness)).await.status(), StatusCode::OK);
    proxy.quiet();
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The call goes over a new connection, not the quiet one.
    let (status, _, body) = read_body(harness.send(stat(&harness)).await).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(harness.worker.calls().len(), 2);
}
