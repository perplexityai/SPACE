//! Fixtures for tests of SPACE's gRPC transports, in this crate and its
//! dependents. Built only for tests and the `test-support` feature.

/// A loopback listener whose SYNs Linux drops, so a dial to it neither
/// connects nor fails before its own bound: Linux drops a SYN to a listener
/// whose accept queue is full, which stands in for a peer that is gone or a
/// SYN pinned to a stale conntrack entry. Dropping it releases the address.
#[cfg(target_os = "linux")]
pub struct Blackhole {
    address: std::net::SocketAddr,
    _listener: tokio::net::TcpListener,
    _queued: Vec<tokio::net::TcpStream>,
}

#[cfg(target_os = "linux")]
impl Blackhole {
    /// Listen at `address`, which may name port 0, and fill the accept queue.
    pub async fn bind(address: std::net::SocketAddr) -> std::io::Result<Self> {
        let socket = tokio::net::TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(address)?;
        let listener = socket.listen(2)?;
        let address = listener.local_addr()?;
        // Linux counts the accept queue full only once it holds more than the
        // backlog, so three unaccepted connections fill it. Dialing them one at
        // a time keeps at most one handshake pending, so the SYN queue never
        // counts as full and the setup does not depend on SYN cookies.
        let mut queued = Vec::new();
        for _ in 0..3 {
            queued.push(tokio::net::TcpStream::connect(address).await?);
        }
        Ok(Self {
            address,
            _listener: listener,
            _queued: queued,
        })
    }

    pub fn address(&self) -> std::net::SocketAddr {
        self.address
    }
}

/// The loop every retried TCP connect runs (attempt cap, backoff, budget,
/// spans, retry metric and logs, and the observer of
/// [`crate::connect_tcp_channel_with_retry_observed`]) over attempts the
/// caller supplies, so a test can make them fail, stall or succeed on cue
/// instead of depending on when the kernel completes a dial.
pub async fn retry_tcp_connect<T, E, F, Fut>(
    service_name: &'static str,
    target: &str,
    retry_policy: crate::GrpcRetryPolicy,
    attempt_cap: std::time::Duration,
    attempt: F,
    on_retry: impl FnMut(&crate::TcpConnectRetry<'_, E>),
) -> Result<T, E>
where
    E: std::fmt::Display + From<crate::TcpConnectError>,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    crate::connect::retry_tcp_connect_observed(
        service_name,
        target,
        retry_policy,
        attempt_cap,
        attempt,
        on_retry,
    )
    .await
}
