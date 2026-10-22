//! One coordinator owns authorization, dispatch accounting, and the HTTP reply.
//! Operational helpers return typed results without borrowing accounting state.

use http::{HeaderMap, StatusCode, Version, header, uri::Authority};
use pplx_space_net_common::DomainName;

use super::credentials::credential_headers;
use super::dispatch::DispatchError;
use super::*;
use crate::certificates::NetworkCredentialAccessError;
use crate::tcp_proxy::http_host::BindingOutcome;

/// Initial admission is not a durable permit. Dispatch rechecks the lease after
/// origin setup and sender readiness before resolving or forwarding credentials.
struct PreparedRequest {
    request: SanitizedRequest<Incoming>,
    response: ResponsePlan,
}

enum ResponsePlan {
    Http(Option<(upgrade::Handshake, hyper::upgrade::OnUpgrade)>),
    Connect(hyper::upgrade::OnUpgrade),
}

#[derive(Debug, thiserror::Error)]
enum AuthorizationError {
    #[error("unsupported HTTP upgrade")]
    UnsupportedUpgrade,
    #[error("request authority is not admitted")]
    Authority(#[from] AuthorityError),
    #[error("origin binding refused: {0:?}")]
    Binding(BindingOutcome),
    #[error("request credentials are unavailable: {0:?}")]
    Credentials(NetworkCredentialAccessError),
    #[error("credential-bearing requests cannot upgrade")]
    CredentialUpgrade,
    #[error("invalid CONNECT request")]
    InvalidConnect,
}

impl AuthorizationError {
    fn refusal(&self) -> (Outcome, &'static str, StatusCode) {
        let (reason, status) = match self {
            Self::UnsupportedUpgrade => ("unsupported_upgrade", StatusCode::FORBIDDEN),
            Self::Authority(error) => (error.as_str(), StatusCode::FORBIDDEN),
            Self::Binding(error) => (error.as_str(), error.refusal_status()),
            Self::Credentials(error) => (error.as_str(), error.status()),
            Self::CredentialUpgrade => ("credential_upgrade_unsupported", StatusCode::FORBIDDEN),
            Self::InvalidConnect => ("invalid_connect", StatusCode::FORBIDDEN),
        };
        let outcome = if matches!(self, Self::Credentials(NetworkCredentialAccessError::Stale)) {
            Outcome::Cancelled
        } else {
            Outcome::Denied
        };
        (outcome, reason, status)
    }
}

impl<F, Fut, U> Session<F>
where
    F: FnOnce(HttpProtocol) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(U, HttpProtocol), upstream::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(super) async fn forward(&self, request: SanitizedRequest<Incoming>) -> Response<Body> {
        let protocol = HttpProtocol::of(request.version());
        let connect = request.method() == http::Method::CONNECT;
        let mut authorization = Attempt::http_authorization(protocol.as_str());
        let PreparedRequest {
            request,
            response: plan,
        } = match self.prepare(request).await {
            Ok(request) => request,
            Err(error) => {
                let (outcome, reason, status) = error.refusal();
                authorization.finish(outcome, reason);
                return self.refuse(protocol, reason, status);
            }
        };
        authorization.finish(Outcome::Success, "authorized");

        // The coordinator owns both attempts so cancellation is accounted for
        // without passing mutable accounting through operational helpers.
        let mut forwarding = Attempt::forwarding(protocol.as_str(), "request");
        // Keep the awaited operations together. Header preparation must follow
        // both waits so it cannot carry credentials across a policy change.
        let dispatched = async {
            let sender = self
                .upstream
                .sender(protocol)
                .await
                .map_err(DispatchError::Setup)?;
            let sender = sender.ready().await.map_err(DispatchError::Ready)?;
            let request = dispatch::prepare(request, &self.lease, sender.protocol())?;
            let request = request
                .map(|body| Either::Left(Self::forwarding_body(protocol, body, "request_body")));
            sender.send(request).await.map_err(DispatchError::Send)
        }
        .await;
        let mut response = match dispatched {
            Ok(response) => response,
            Err(error) => {
                let (reason, status) = match error {
                    DispatchError::Setup(error) => {
                        forwarding.finish(Outcome::Error, "origin_setup");
                        error.record(if connect {
                            "connect_proxy_setup"
                        } else {
                            "upstream_setup"
                        });
                        (error.as_str(), StatusCode::BAD_GATEWAY)
                    }
                    DispatchError::Ready(error) => {
                        if !connect {
                            forwarding.finish(Outcome::Error, "upstream_http");
                            self.draining.cancel();
                        }
                        HttpError::Protocol(error).record(if connect {
                            "connect_proxy_ready"
                        } else {
                            "upstream_request"
                        });
                        ("upstream_http_error", StatusCode::BAD_GATEWAY)
                    }
                    DispatchError::InvalidConnect => ("invalid_connect", StatusCode::FORBIDDEN),
                    DispatchError::Credentials(error) => {
                        forwarding.finish(Outcome::Cancelled, error.as_str());
                        (error.as_str(), error.status())
                    }
                    DispatchError::Send(error) => {
                        forwarding.finish(Outcome::Error, "upstream_http");
                        if !connect {
                            self.draining.cancel();
                        }
                        HttpError::Protocol(error).record(if connect {
                            "connect_proxy_request"
                        } else {
                            "upstream_request"
                        });
                        ("upstream_http_error", StatusCode::BAD_GATEWAY)
                    }
                };
                return self.refuse(protocol, reason, status);
            }
        };

        let (outcome, upgrade) = match plan {
            ResponsePlan::Http(Some((handshake, guest))) if handshake.accepts(&response) => {
                (handshake.as_str(), Some((guest, upgrade::Kind::WebSocket)))
            }
            ResponsePlan::Connect(guest) if response.status().is_success() => {
                ("connect_forwarded", Some((guest, upgrade::Kind::Connect)))
            }
            ResponsePlan::Connect(_) if response.status() != StatusCode::SWITCHING_PROTOCOLS => {
                ("connect_proxy_refused", None)
            }
            ResponsePlan::Http(_) if response.status() != StatusCode::SWITCHING_PROTOCOLS => {
                ("allowed", None)
            }
            _ => {
                forwarding.finish(Outcome::Error, "unexpected_upgrade");
                return self.refuse(protocol, "unexpected_upgrade", StatusCode::BAD_GATEWAY);
            }
        };
        if let Some((guest, kind)) = upgrade {
            let origin = hyper::upgrade::on(&mut response);
            response.headers_mut().remove(header::CONTENT_LENGTH);
            if matches!(kind, upgrade::Kind::WebSocket) {
                websocket_response_headers(response.headers_mut());
            }
            self.upgrades.spawn(
                upgrade::relay(
                    guest,
                    origin,
                    self.shutdown.clone(),
                    protocol.as_str(),
                    kind,
                )
                .in_current_span(),
            );
            Self::record(protocol, outcome, response.status());
            forwarding.finish(Outcome::Success, "response_headers");
            response.map(|_| Either::Right(Empty::new()))
        } else {
            Self::record(protocol, outcome, response.status());
            forwarding.finish(Outcome::Success, "response_headers");
            response
                .map(|body| Either::Left(Self::forwarding_body(protocol, body, "response_body")))
        }
    }

    async fn prepare(
        &self,
        mut request: SanitizedRequest<Incoming>,
    ) -> Result<PreparedRequest, AuthorizationError> {
        if request.method() == http::Method::CONNECT {
            let target =
                connect::target(&request).map_err(|()| AuthorizationError::InvalidConnect)?;
            if !self.lease.is_usable() {
                return Err(AuthorizationError::Credentials(
                    NetworkCredentialAccessError::Stale,
                ));
            }
            self.binding
                .authorize(self.lease.host())
                .await
                .map_err(AuthorizationError::Binding)?;
            request = target.prepare(request);
            let guest = request.on_upgrade();
            return Ok(PreparedRequest {
                request,
                response: ResponsePlan::Connect(guest),
            });
        }

        let handshake = upgrade::Handshake::request(&request)
            .map_err(|()| AuthorizationError::UnsupportedUpgrade)?;
        authorize_authority(&request, self.lease.host(), self.lease.port())?;
        self.binding
            .authorize(self.lease.host())
            .await
            .map_err(AuthorizationError::Binding)?;
        let credential_bearing = !credential_headers(&self.lease, &request)
            .map_err(AuthorizationError::Credentials)?
            .is_empty();
        if handshake.is_some() && credential_bearing {
            return Err(AuthorizationError::CredentialUpgrade);
        }
        if matches!(handshake, Some(upgrade::Handshake::WebSocket { .. })) {
            request.set_websocket_upgrade();
        }
        let upgrade = handshake.map(|handshake| (handshake, request.on_upgrade()));
        Ok(PreparedRequest {
            request,
            response: ResponsePlan::Http(upgrade),
        })
    }
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
