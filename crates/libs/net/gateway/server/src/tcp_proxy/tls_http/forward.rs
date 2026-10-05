//! Authorization and forwarding for ordinary inspected HTTP requests.

use http::{HeaderMap, StatusCode, Version, header, uri::Authority};
use pplx_space_net_common::DomainName;

use super::credentials::{credential_headers, inject};
use super::*;
use crate::certificates::NetworkCredentialAccessError;

impl<F, Fut, U> Session<F>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), TlsTransportError>> + Send,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(super) async fn forward(&self, mut request: SanitizedRequest<Incoming>) -> Response<Body> {
        if request.method() == http::Method::CONNECT {
            return self.connect_request(request).await;
        }

        let mut authorization = Attempt::http_authorization(self.protocol.as_str());
        let handshake = match upgrade::Handshake::request(&request) {
            Ok(handshake) => handshake,
            Err(()) => {
                authorization.finish(Outcome::Denied, "unsupported_upgrade");
                return self.refuse("unsupported_upgrade", StatusCode::FORBIDDEN);
            }
        };
        if let Err(reason) = authorize_authority(&request, self.lease.host(), self.lease.port()) {
            authorization.finish(Outcome::Denied, reason.as_str());
            return self.refuse(reason.as_str(), StatusCode::FORBIDDEN);
        }
        if let Err(reason) = self.binding.authorize(&request).await {
            authorization.finish(Outcome::Denied, reason.as_str());
            return self.refuse(reason.as_str(), reason.refusal_status());
        }
        let credential_bearing = match credential_headers(&self.lease, &request) {
            Ok(headers) => !headers.is_empty(),
            Err(error) => {
                authorization.finish(initial_credential_outcome(error), error.as_str());
                return self.refuse(error.as_str(), error.status());
            }
        };
        if handshake.is_some() && credential_bearing {
            authorization.finish(Outcome::Denied, "credential_upgrade_unsupported");
            return self.refuse("credential_upgrade_unsupported", StatusCode::FORBIDDEN);
        }
        authorization.finish(Outcome::Success, "authorized");

        if matches!(handshake, Some(upgrade::Handshake::WebSocket { .. })) {
            request.set_websocket_upgrade();
        }
        let upgrade = handshake.map(|handshake| (handshake, request.on_upgrade()));
        let mut forwarding = Attempt::forwarding(self.protocol.as_str(), "request");
        let mut retirements = 0;
        let deadline = tokio::time::Instant::now() + UPSTREAM_PREPARATION_TIMEOUT;
        loop {
            let generation = match tokio::time::timeout_at(deadline, self.sender())
                .await
                .unwrap_or(Err(TlsTransportError::Timeout))
            {
                Ok(sender) => sender,
                Err(error) => {
                    forwarding.finish(Outcome::Error, "origin_setup");
                    error.record("upstream_setup");
                    return self.refuse(error.as_str(), StatusCode::BAD_GATEWAY);
                }
            };
            let sender = match tokio::time::timeout_at(deadline, generation.sender.ready()).await {
                Ok(Ok(sender)) => sender,
                Ok(Err(_)) if retirements < MAX_RETIREMENTS => {
                    generation.retired.cancel();
                    retirements += 1;
                    continue;
                }
                Ok(Err(error)) => {
                    forwarding.finish(Outcome::Error, "upstream_http");
                    self.draining.cancel();
                    TlsTransportError::Http(error).record("upstream_request");
                    return self.refuse("upstream_http_error", StatusCode::BAD_GATEWAY);
                }
                Err(_) => {
                    forwarding.finish(Outcome::Error, "origin_timeout");
                    self.draining.cancel();
                    return self
                        .refuse(TlsTransportError::Timeout.as_str(), StatusCode::BAD_GATEWAY);
                }
            };

            // Origin setup and sender readiness can outlive a policy update. This
            // is the final fence before request headers can leave the gateway.
            if !self.lease.is_usable() {
                forwarding.finish(Outcome::Cancelled, "lease_invalidated");
                return self.refuse("lease_invalidated", StatusCode::FORBIDDEN);
            }
            match tokio::time::timeout_at(deadline, self.binding.authorize(&request)).await {
                Ok(Ok(_)) => {}
                Ok(Err(reason)) => {
                    forwarding.finish(Outcome::Cancelled, reason.as_str());
                    return self.refuse(reason.as_str(), reason.refusal_status());
                }
                Err(_) => {
                    forwarding.finish(Outcome::Error, "origin_timeout");
                    return self
                        .refuse(TlsTransportError::Timeout.as_str(), StatusCode::BAD_GATEWAY);
                }
            }
            let headers = match credential_headers(&self.lease, &request) {
                Ok(headers) => headers,
                Err(error) => {
                    forwarding.finish(Outcome::Cancelled, error.as_str());
                    return self.refuse(error.as_str(), error.status());
                }
            };
            inject(&mut request, headers);
            if sender.is_http1() {
                translate_to_http1(&mut request);
            }
            let request =
                request.map(|body| Either::Left(self.forwarding_body(body, "request_body")));
            let response = sender.send(request).await;

            return match response {
                Ok(mut response)
                    if upgrade
                        .as_ref()
                        .is_some_and(|(handshake, _)| handshake.accepts(&response)) =>
                {
                    let (handshake, guest) = upgrade.expect("matched upgrade response");
                    let origin = hyper::upgrade::on(&mut response);

                    response.headers_mut().remove(header::CONTENT_LENGTH);
                    if matches!(handshake, upgrade::Handshake::WebSocket { .. }) {
                        websocket_response_headers(response.headers_mut());
                    }
                    self.record(handshake.as_str(), response.status());
                    let drain = self.draining.clone().drop_guard();
                    let shutdown = self.shutdown.clone();
                    let protocol = self.protocol.as_str();
                    self.upgrades.spawn(
                        async move {
                            let _drain = drain;
                            upgrade::relay(guest, origin, shutdown, protocol).await;
                        }
                        .in_current_span(),
                    );
                    forwarding.finish(Outcome::Success, "response_headers");
                    response.map(|_| Either::Right(Empty::new()))
                }
                Ok(response) if response.status() != StatusCode::SWITCHING_PROTOCOLS => {
                    self.record("allowed", response.status());
                    forwarding.finish(Outcome::Success, "response_headers");
                    response.map(|body| Either::Left(self.forwarding_body(body, "response_body")))
                }
                Ok(_) => {
                    forwarding.finish(Outcome::Error, "unexpected_upgrade");
                    self.refuse("unexpected_upgrade", StatusCode::BAD_GATEWAY)
                }
                Err(error) => {
                    forwarding.finish(Outcome::Error, "upstream_http");
                    generation.retired.cancel();
                    TlsTransportError::Http(error).record("upstream_request");
                    self.refuse("upstream_http_error", StatusCode::BAD_GATEWAY)
                }
            };
        }
    }
}

const fn initial_credential_outcome(error: NetworkCredentialAccessError) -> Outcome {
    if matches!(error, NetworkCredentialAccessError::Stale) {
        Outcome::Cancelled
    } else {
        Outcome::Denied
    }
}

fn translate_to_http1<B>(request: &mut SanitizedRequest<B>) {
    // Preserve the exact wire authority when translating: default ports can
    // change signatures.
    if !request.headers().contains_key(header::HOST) {
        let authority = request
            .uri()
            .authority()
            .expect("validated HTTP/2 authority");
        let host = authority.as_str().parse().expect("validated authority");
        request.set_host(host);
    }
    *request.uri_mut() = request
        .uri()
        .path_and_query()
        .map_or("/", |path| path.as_str())
        .parse()
        .expect("Hyper parsed path");
    *request.version_mut() = Version::HTTP_11;
}

fn websocket_response_headers(headers: &mut HeaderMap) {
    headers.insert(
        header::CONNECTION,
        http::HeaderValue::from_static("upgrade"),
    );
    headers.insert(header::UPGRADE, http::HeaderValue::from_static("websocket"));
}

#[derive(Debug, thiserror::Error)]
pub(super) enum AuthorityError {
    #[error("HTTP request authority is missing or malformed")]
    Invalid,
    #[error("HTTP request authority differs from the admitted TLS origin")]
    Mismatch,
}

impl AuthorityError {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Invalid => "invalid_authority",
            Self::Mismatch => "authority_mismatch",
        }
    }
}

pub(super) fn authorize_authority<B>(
    request: &SanitizedRequest<B>,
    host: &DomainName,
    port: u16,
) -> Result<(), AuthorityError> {
    let validate = |authority: &Authority| {
        if authority.as_str().contains('@')
            || authority.as_str().ends_with(':')
            || (authority.as_str().contains(':') && authority.port_u16().is_none())
        {
            return Err(AuthorityError::Invalid);
        }
        let name: DomainName = authority
            .host()
            .parse()
            .map_err(|_| AuthorityError::Invalid)?;
        if &name != host || authority.port_u16().unwrap_or(443) != port {
            return Err(AuthorityError::Mismatch);
        }
        Ok(())
    };
    if request
        .uri()
        .scheme_str()
        .is_some_and(|scheme| scheme != "https")
    {
        return Err(AuthorityError::Mismatch);
    }
    if let Some(authority) = request.uri().authority() {
        validate(authority)?;
    } else if request.version() == Version::HTTP_2 {
        return Err(AuthorityError::Invalid);
    }
    let mut hosts = request.headers().get_all(header::HOST).iter();
    if let Some(value) = hosts.next() {
        if hosts.next().is_some() {
            return Err(AuthorityError::Invalid);
        }
        let authority = value
            .to_str()
            .ok()
            .and_then(|value| value.parse::<Authority>().ok())
            .ok_or(AuthorityError::Invalid)?;
        validate(&authority)?;
    } else if request.version() != Version::HTTP_2 {
        return Err(AuthorityError::Invalid);
    }
    Ok(())
}
