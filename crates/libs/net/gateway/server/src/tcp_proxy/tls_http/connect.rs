//! CONNECT retains opaque forwarding. SPACE authorizes the outer proxy route;
//! the tunnel contents and inner destination remain outside HTTP inspection.

use super::*;

/// The authority forwarded in an HTTP/1.1 CONNECT request.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct Target {
    host: pplx_space_net_common::DomainName,
    port: u16,
}

#[derive(Debug, thiserror::Error)]
#[error("expected an exact DNS hostname and nonzero port (hostname:port)")]
pub(super) struct TargetParseError;

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

pub(super) fn target<B>(request: &SanitizedRequest<B>) -> Result<Target, ()> {
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

impl Target {
    pub(super) fn prepare<B>(self, mut request: SanitizedRequest<B>) -> SanitizedRequest<B> {
        let authority = self.to_string();
        *request.uri_mut() = authority.parse().expect("validated CONNECT target");
        request.set_host(authority.parse().expect("validated CONNECT target"));
        request
    }
}

#[cfg(test)]
mod tests;
