//! CONNECT retains opaque forwarding. SPACE authorizes the outer proxy route;
//! the tunnel contents and inner destination remain outside HTTP inspection.

use hyper::upgrade::OnUpgrade;

use super::leg::{RelayFault, RelayWitness, Side};
use super::*;

/// The authority forwarded in an HTTP/1.1 CONNECT request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Target {
    host: pplx_space_net_common::DomainName,
    port: u16,
}

#[derive(Debug, thiserror::Error)]
#[error("expected an exact DNS hostname and nonzero port (hostname:port)")]
struct TargetParseError;

impl std::str::FromStr for Target {
    type Err = TargetParseError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let authority: http::uri::Authority = value.parse().map_err(|_| TargetParseError)?;
        if value.contains('@') {
            return Err(TargetParseError);
        }
        let host: pplx_space_net_common::DomainName =
            authority.host().parse().map_err(|_| TargetParseError)?;
        if host.as_str().parse::<std::net::IpAddr>().is_ok() {
            return Err(TargetParseError);
        }
        let port = authority
            .port_u16()
            .filter(|port| *port != 0)
            .ok_or(TargetParseError)?;
        Ok(Self { host, port })
    }
}

impl std::fmt::Display for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

fn target<B>(request: &SanitizedRequest<B>) -> Result<Target, ()> {
    if request.version() != Version::HTTP_11
        || request.uri().scheme().is_some()
        || request.uri().path_and_query().is_some()
        || request.headers().contains_key(header::UPGRADE)
        || request.headers().contains_key(header::TRANSFER_ENCODING)
        || request
            .headers()
            .get(header::CONTENT_LENGTH)
            .is_some_and(|value| value != "0")
    {
        return Err(());
    }
    let target: Target = request
        .uri()
        .authority()
        .ok_or(())?
        .as_str()
        .parse()
        .map_err(|_| ())?;
    let mut hosts = request.headers().get_all(header::HOST).iter();
    let host: Target = hosts
        .next()
        .ok_or(())?
        .to_str()
        .map_err(|_| ())?
        .parse()
        .map_err(|_| ())?;
    if hosts.next().is_some() || host != target {
        return Err(());
    }
    Ok(target)
}

impl<F, Fut, U> Session<F>
where
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(super) async fn connect_request(
        &self,
        mut request: SanitizedRequest<Incoming>,
    ) -> Response<Body> {
        let protocol = HttpProtocol::of(request.version());
        let mut authorization = Attempt::http_authorization(protocol.as_str());
        let target = match target(&request) {
            Ok(target) => target,
            Err(()) => {
                authorization.finish(Outcome::Denied, "invalid_connect");
                return self.refuse(protocol, "invalid_connect", StatusCode::FORBIDDEN);
            }
        };
        if !self.lease.is_usable() {
            authorization.finish(Outcome::Cancelled, "lease_invalidated");
            return self.refuse(protocol, "lease_invalidated", StatusCode::FORBIDDEN);
        }
        let proxy = Request::builder()
            .uri("/")
            .header(
                header::HOST,
                format!("{}:{}", self.lease.host(), self.lease.port()),
            )
            .body(())
            .expect("validated proxy origin");
        let proxy = SanitizedRequest::try_from(proxy).expect("proxy request has only Host");
        if let Err(reason) = self.binding.authorize(&proxy).await {
            authorization.finish(Outcome::Denied, reason.as_str());
            return self.refuse(protocol, reason.as_str(), reason.refusal_status());
        }
        authorization.finish(Outcome::Success, "authorized");
        let authority = target.to_string();
        *request.uri_mut() = authority.parse().expect("validated CONNECT target");
        request.set_host(authority.parse().expect("validated CONNECT target"));
        let guest = request.on_upgrade();
        let mut forwarding = Attempt::forwarding(protocol.as_str(), "request");
        let sender = match self.sender(protocol).await {
            Ok(Sender::Http1(sender)) => sender,
            Ok(Sender::Http2(_)) => {
                forwarding.finish(Outcome::Denied, "invalid_connect");
                return self.refuse(protocol, "invalid_connect", StatusCode::FORBIDDEN);
            }
            Err(error) => {
                forwarding.finish(Outcome::Error, "origin_setup");
                error.record("connect_proxy_setup");
                return self.refuse(protocol, error.as_str(), StatusCode::BAD_GATEWAY);
            }
        };
        let mut sender = sender.lock().await;
        if !self.lease.is_usable() {
            forwarding.finish(Outcome::Cancelled, "lease_invalidated");
            return self.refuse(protocol, "lease_invalidated", StatusCode::FORBIDDEN);
        }
        let mut response = match request
            .map(|body| Either::Left(Self::forwarding_body(protocol, body, "request_body")))
            .send_http1(&mut sender)
            .await
        {
            Ok(response) => response,
            Err(error) => {
                forwarding.finish(Outcome::Error, "upstream_http");
                TlsTransportError::Http(error).record("connect_proxy_request");
                return self.refuse(protocol, "upstream_http_error", StatusCode::BAD_GATEWAY);
            }
        };
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            forwarding.finish(Outcome::Error, "unexpected_upgrade");
            return self.refuse(protocol, "unexpected_upgrade", StatusCode::BAD_GATEWAY);
        }
        if !response.status().is_success() {
            Self::record(protocol, "connect_proxy_refused", response.status());
            forwarding.finish(Outcome::Success, "response_headers");
            return response
                .map(|body| Either::Left(Self::forwarding_body(protocol, body, "response_body")));
        }
        let origin = hyper::upgrade::on(&mut response);
        response.headers_mut().remove(header::CONTENT_LENGTH);
        self.upgrades
            .spawn(relay(guest, origin, self.shutdown.clone()).in_current_span());
        Self::record(protocol, "connect_forwarded", response.status());
        forwarding.finish(Outcome::Success, "response_headers");
        response.map(|_| Either::Right(Empty::new()))
    }
}

async fn relay(guest: OnUpgrade, origin: OnUpgrade, shutdown: CancellationToken) {
    let mut forwarding = Attempt::forwarding("http1", "relay");
    let witness = RelayWitness::default();
    let started_at = tokio::time::Instant::now();
    let operation = {
        let witness = witness.clone();
        async move {
            let (guest, origin) =
                tokio::time::timeout(HEADER_TIMEOUT, async { tokio::try_join!(guest, origin) })
                    .await
                    .map_err(|_| TlsTransportError::Timeout)??;
            let mut guest = witness.guest(TokioIo::new(guest));
            let mut origin = witness.origin(TokioIo::new(origin));
            tokio::io::copy_bidirectional(&mut guest, &mut origin)
                .await
                .map(|_| ())
                // Attribute the copy to the peer that actually failed. Forcing
                // every relay error onto the origin makes a guest hang-up look
                // like an upstream fault.
                .map_err(|error| match witness.fault().map(RelayFault::side) {
                    Some(Side::Guest) => TlsTransportError::Downstream(error),
                    _ => TlsTransportError::Upstream(error),
                })
        }
    };
    let result = tokio::select! {
        biased;
        () = shutdown.cancelled() => Err(TlsTransportError::Invalidated),
        result = operation => result,
    };
    let outcome = match result {
        Ok(()) => {
            forwarding.finish(Outcome::Success, "connect_complete");
            "connect_complete"
        }
        // This token is session-local and fires whenever the serve future is
        // dropped, so the relay cannot tell shutdown from a resweep or an
        // invalidated lease. Report the end without naming a cause; the
        // enclosing session watches all three and records the actual one.
        Err(TlsTransportError::Withdrawn | TlsTransportError::Invalidated) => {
            forwarding.finish(Outcome::Cancelled, "session_ended");
            "session_ended"
        }
        Err(error) => {
            let reason = witness.fault().map_or("relay_io", RelayFault::as_str);
            forwarding.finish(Outcome::Error, reason);
            error.record("connect_relay");
            error.as_str()
        }
    };
    let (guest_bytes, origin_bytes) = witness.bytes();
    pplx_space_metrics::gateway::domain_binding("tcp", "tls_connect", outcome, None);
    pplx_space_metrics::gateway::relay_duration_ms(
        "http1",
        outcome,
        witness.closed_by(),
        started_at.elapsed().as_secs_f64() * 1_000.0,
    );
    tracing::info!(
        boundary = "forwarding",
        operation = "relay",
        outcome,
        closed_by = witness.closed_by(),
        fault = witness.fault().map(RelayFault::as_str),
        guest_bytes,
        origin_bytes,
        "CONNECT forwarding ended"
    );
}

#[cfg(test)]
mod tests;
