//! Reading the head of a guest flow without consuming it.
//!
//! `peek` copies bytes out of the kernel's receive buffer and leaves them
//! queued, so a flow can be inspected and then handed to a relay that reads the
//! very same bytes. Nothing here may consume. Parsing lives in
//! [`crate::tls_sni`], which stays free of I/O.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::time::Instant;
use tracing::{debug, info};

use crate::debug::GatewayDebugState;
use crate::tls_sni::{
    ClientAlpnOffer, ClientHelloParseDetail, ClientHelloScan, scan_client_hello_with_detail,
};

/// Bytes peeked looking for a ClientHello. This covers a maximum-size legal
/// plaintext handshake record plus framing while bounding transient memory per
/// flow.
const PEEK_MAX_BYTES: usize = 16_640;
/// How long to wait for the client's *first* bytes when a flow without a
/// ClientHello still proceeds. A server-speaks-first flow (SSH, SMTP) or an
/// idle one pays at most this before proceeding with no SNI.
const FIRST_BYTES_TIMEOUT: Duration = Duration::from_millis(50);
/// How long to wait for the client's first bytes when a flow without a
/// ClientHello is denied: a destination only its DNS evidence can admit, or
/// any admitted flow under TLS interception, which closes a flow it cannot
/// read SNI from. A domain-derived allow rejects an absent ClientHello
/// deliberately, so a guest cannot bypass hostname binding by delaying its
/// first write, and a longer wait grants no bypass. 50 ms is shorter than one
/// TCP retransmit (Linux's minimum RTO is 200 ms), so a cold guest's lost or
/// late first segment was denied as if never sent and surfaced as a TLS EOF on
/// an allowed hostname. 500 ms covered only one retransmit and still sat
/// inside the observed arrival tail of legitimate ClientHellos, so slow but
/// valid clients were reset. Since waiting grants no bypass, this bound only
/// reclaims a pending socket from a flow that never writes: 5 s covers four
/// consecutive retransmits (0.2 + 0.4 + 0.8 + 1.6 s), stays below common
/// client TLS handshake timeouts so the client sees this reset rather than
/// its own timeout, and matches the order of SNI-peek bounds in other proxies
/// (HAProxy `inspect-delay` 5 s, Envoy `listener_filters_timeout` 15 s,
/// nginx `preread_timeout` 30 s).
const FAIL_CLOSED_FIRST_BYTES_TIMEOUT: Duration = Duration::from_secs(5);
/// Budget for the rest of the ClientHello once the flow is confirmed to carry
/// handshake bytes, counted from those first bytes. Reaching it only delays a
/// TLS flow whose ClientHello is still arriving; a non-TLS flow is rejected on
/// the first peek instead. On a denied path, reaching it rejects a valid TLS
/// flow whose later segments were lost or delayed, so it covers more than one
/// retransmit.
const COMPLETION_DEADLINE: Duration = Duration::from_secs(1);

/// What admission does with a flow whose ClientHello never arrives, which
/// decides how long the peek waits for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MissingClientHello {
    /// The flow proceeds without SNI, so waiting only delays it.
    Proceeds,
    /// The flow is denied, so a late first segment must not read as none.
    Denied,
}

impl MissingClientHello {
    const fn first_bytes_timeout(self) -> Duration {
        match self {
            Self::Proceeds => FIRST_BYTES_TIMEOUT,
            Self::Denied => FAIL_CLOSED_FIRST_BYTES_TIMEOUT,
        }
    }
}
const PEEK_RETRY_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MissingSniDetail {
    SniNotFound,
    SniInvalid,
    MalformedClientHello,
    FirstByteTimeout,
    ClientHelloCompletionTimeout,
    ConnectionClosed,
    ReadError,
    ClientHelloTooLarge,
}

impl MissingSniDetail {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SniNotFound => "sni_not_found",
            Self::SniInvalid => "sni_invalid",
            Self::MalformedClientHello => "malformed_client_hello",
            Self::FirstByteTimeout => "first_byte_timeout",
            Self::ClientHelloCompletionTimeout => "client_hello_completion_timeout",
            Self::ConnectionClosed => "connection_closed",
            Self::ReadError => "read_error",
            Self::ClientHelloTooLarge => "client_hello_too_large",
        }
    }
}

pub(crate) struct ClientHelloObservation {
    pub(crate) scan: ClientHelloScan,
    pub(crate) client_alpn: ClientAlpnOffer,
    pub(crate) missing_sni_detail: Option<MissingSniDetail>,
    /// Wall-clock cost of the peek loop, filled in by the public entry point.
    pub(crate) elapsed: Duration,
}

impl ClientHelloObservation {
    fn incomplete(detail: MissingSniDetail) -> Self {
        Self {
            scan: ClientHelloScan::Incomplete,
            client_alpn: ClientAlpnOffer::Unavailable,
            missing_sni_detail: Some(detail),
            elapsed: Duration::ZERO,
        }
    }
}

/// Peek `client`'s leading bytes and scan for a TLS ClientHello. Best-effort:
/// returns [`ClientHelloScan::Incomplete`] rather than block a flow whose client
/// has not sent one. Interception forwards the supported ALPN offer upstream.
pub(crate) async fn client_hello_with_detail(
    client: &TcpStream,
    missing: MissingClientHello,
) -> ClientHelloObservation {
    let started_at = Instant::now();
    let mut observation = scan_stream(client, missing.first_bytes_timeout()).await;
    observation.elapsed = started_at.elapsed();
    observation
}

async fn scan_stream(client: &TcpStream, first_bytes_timeout: Duration) -> ClientHelloObservation {
    let mut buf = vec![0_u8; PEEK_MAX_BYTES];
    // Set by the first handshake bytes, so a late first segment does not also
    // spend the budget for the rest of the ClientHello.
    let mut completion_deadline: Option<Instant> = None;
    loop {
        let handshake_seen = completion_deadline.is_some();
        let timeout = match completion_deadline {
            Some(deadline) => deadline.saturating_duration_since(Instant::now()),
            None => first_bytes_timeout,
        };
        if timeout.is_zero() {
            return ClientHelloObservation::incomplete(
                MissingSniDetail::ClientHelloCompletionTimeout,
            );
        }
        let peeked = match tokio::time::timeout(timeout, client.peek(&mut buf)).await {
            Ok(Ok(0)) => {
                return ClientHelloObservation::incomplete(MissingSniDetail::ConnectionClosed);
            }
            Ok(Err(_)) => {
                return ClientHelloObservation::incomplete(MissingSniDetail::ReadError);
            }
            Err(_) if handshake_seen => {
                return ClientHelloObservation::incomplete(
                    MissingSniDetail::ClientHelloCompletionTimeout,
                );
            }
            Err(_) => {
                return ClientHelloObservation::incomplete(MissingSniDetail::FirstByteTimeout);
            }
            Ok(Ok(n)) => n,
        };
        let prefix = buf
            .get(..peeked)
            .expect("TcpStream::peek cannot exceed its destination buffer");
        match scan_client_hello_with_detail(prefix) {
            // Confirmed handshake bytes, ClientHello still arriving. `peek` is
            // cumulative, so re-peek until it completes or a budget runs out.
            (ClientHelloScan::Incomplete, _, _) if peeked < buf.len() => {
                let deadline = *completion_deadline
                    .get_or_insert_with(|| Instant::now() + COMPLETION_DEADLINE);
                let nap =
                    PEEK_RETRY_INTERVAL.min(deadline.saturating_duration_since(Instant::now()));
                if nap.is_zero() {
                    return ClientHelloObservation::incomplete(
                        MissingSniDetail::ClientHelloCompletionTimeout,
                    );
                }
                tokio::time::sleep(nap).await;
            }
            (ClientHelloScan::Incomplete, _, _) => {
                return ClientHelloObservation::incomplete(MissingSniDetail::ClientHelloTooLarge);
            }
            (scan, client_alpn, parse_detail) => {
                let missing_sni_detail = parse_detail.map(|detail| match detail {
                    ClientHelloParseDetail::SniNotFound => MissingSniDetail::SniNotFound,
                    ClientHelloParseDetail::SniInvalid => MissingSniDetail::SniInvalid,
                    ClientHelloParseDetail::MalformedClientHello => {
                        MissingSniDetail::MalformedClientHello
                    }
                });
                return ClientHelloObservation {
                    scan,
                    client_alpn,
                    missing_sni_detail,
                    elapsed: Duration::ZERO,
                };
            }
        }
    }
}

#[cfg(test)]
pub(crate) async fn client_hello(client: &TcpStream) -> (ClientHelloScan, ClientAlpnOffer) {
    let observation = client_hello_with_detail(client, MissingClientHello::Proceeds).await;
    (observation.scan, observation.client_alpn)
}

/// Meter and log one scan. Always emits a low-volume `debug!`; during an
/// operator's debug window the same fields are elevated to `info!` so the scan
/// is visible without raising the global log level.
pub(crate) fn record(
    sandbox_id: &str,
    original_dst: SocketAddr,
    scan: &ClientHelloScan,
    client_alpn: ClientAlpnOffer,
    missing_sni_detail: Option<MissingSniDetail>,
    elapsed: Duration,
    debug_state: &GatewayDebugState,
) {
    let client_alpn = client_alpn.as_str();
    // The detail rides the scan itself, not just an interception refusal, so a
    // fleet can size its missing-SNI exposure before interception is enabled.
    let detail = missing_sni_detail.map_or("none", MissingSniDetail::as_str);
    pplx_space_metrics::gateway::tls_client_hello(outcome(scan), client_alpn, detail);
    pplx_space_metrics::gateway::tls_client_hello_duration_ms(
        outcome(scan),
        elapsed.as_secs_f64() * 1_000.0,
    );

    let outcome = outcome(scan).as_str();
    match scan.server_name() {
        Some(server_name) => debug!(
            sandbox_id,
            %original_dst,
            outcome,
            client_alpn,
            server_name = server_name.as_str(),
            "extracted TLS SNI from ClientHello"
        ),
        None => debug!(
            sandbox_id,
            %original_dst,
            outcome,
            client_alpn,
            "scanned TCP flow for TLS SNI"
        ),
    }

    if !debug_state.is_enabled() {
        return;
    }
    match scan.server_name() {
        Some(server_name) => info!(
            sandbox_id,
            %original_dst,
            outcome,
            client_alpn,
            server_name = server_name.as_str(),
            "net-gateway debug: scanned TCP flow for TLS SNI"
        ),
        None => info!(
            sandbox_id,
            %original_dst,
            outcome,
            client_alpn,
            "net-gateway debug: scanned TCP flow for TLS SNI"
        ),
    }
}

/// Map a scan verdict to its metered outcome. `tls_sni` is kept free of the
/// metrics vocabulary, so this boundary mapping lives here.
fn outcome(scan: &ClientHelloScan) -> pplx_space_metrics::gateway::TlsClientHelloOutcome {
    use pplx_space_metrics::gateway::TlsClientHelloOutcome;
    match scan {
        ClientHelloScan::ServerName(_) => TlsClientHelloOutcome::Sni,
        ClientHelloScan::NoServerName => TlsClientHelloOutcome::NoSni,
        ClientHelloScan::NotTls => TlsClientHelloOutcome::NotTls,
        ClientHelloScan::Incomplete => TlsClientHelloOutcome::Incomplete,
        ClientHelloScan::Malformed => TlsClientHelloOutcome::Malformed,
    }
}

#[cfg(test)]
mod tests;
