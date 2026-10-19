//! CONNECT retains opaque forwarding. SPACE authorizes the outer proxy route;
//! the tunnel contents and inner destination remain outside HTTP inspection.

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
    Fut: Future<Output = Result<(U, HttpProtocol), upstream::Error>> + Send + 'static,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub(super) async fn connect_request(
        &self,
        mut request: SanitizedRequest<Incoming>,
        observation: &mut request::Request,
    ) -> Reply {
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
        if let Err(reason) = self.binding.authorize(self.lease.host()).await {
            authorization.finish(Outcome::Denied, reason.as_str());
            return self.refuse(protocol, reason.as_str(), reason.refusal_status());
        }
        authorization.finish(Outcome::Success, "authorized");
        let authority = target.to_string();
        *request.uri_mut() = authority.parse().expect("validated CONNECT target");
        request.set_host(authority.parse().expect("validated CONNECT target"));
        let guest = request.on_upgrade();
        let mut forwarding = Attempt::forwarding(protocol.as_str(), "request");
        let mut response = match self.dispatch(request, &mut forwarding, observation).await {
            Ok(response) => response,
            Err(response) => return *response,
        };
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            forwarding.finish(Outcome::Error, "unexpected_upgrade");
            return self.refuse(protocol, "unexpected_upgrade", StatusCode::BAD_GATEWAY);
        }
        if !response.status().is_success() {
            Self::record(protocol, "connect_proxy_refused", response.status());
            forwarding.finish(Outcome::Success, "response_headers");
            return Reply::upstream(
                response.map(|body| {
                    Either::Left(Self::forwarding_body(protocol, body, "response_body"))
                }),
            );
        }
        let origin = hyper::upgrade::on(&mut response);
        response.headers_mut().remove(header::CONTENT_LENGTH);
        self.upgrades.spawn(
            upgrade::relay(
                guest,
                origin,
                self.shutdown.clone(),
                "http1",
                upgrade::Kind::Connect,
                self.observation.clone(),
            )
            .in_current_span(),
        );
        Self::record(protocol, "connect_forwarded", response.status());
        forwarding.finish(Outcome::Success, "response_headers");
        Reply::upstream(response.map(|_| Either::Right(Empty::new())))
    }
}

#[cfg(test)]
mod tests;
