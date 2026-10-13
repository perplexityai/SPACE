//! Provider routing metadata that cannot cross a hostname-enforcing seam.
//!
//! Some CDN edges route on proprietary request headers instead of `Host`:
//! Fastly's `Fastly-Host` overrides `Host`, so a guest could reach any origin
//! behind an allowed Fastly edge. Sanitize on ingress, then keep headers behind
//! a type that permits only the proxy's known routing/upgrade rewrites.
//!
//! This is a denylist of edges known to route on guest-supplied metadata. A
//! new provider whose edge does the same must be added here. Other host
//! overrides (`X-Forwarded-Host`, `Forwarded`, etc.) pass through: downstream
//! configurations can interpret them as authority, outside this denylist's
//! guarantee. Hop-by-hop fields and request trailers are handled separately.

use std::pin::Pin;
use std::task::{Context, Poll};

use http::{HeaderMap, HeaderName, HeaderValue, Method, Request, Response, Uri, Version, header};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::client::conn::{http1, http2};

use super::tls_http::HttpProtocol;

#[cfg(test)]
mod tests;

struct Provider {
    /// Lowercase header-name prefix owned by the provider's edge.
    strip_prefix: &'static str,
    /// Exact lowercase names under `strip_prefix` a guest may still send.
    guest_allowed: &'static [&'static str],
}

const PROVIDERS: &[Provider] = &[Provider {
    strip_prefix: "fastly-",
    // `Fastly-Key` authenticates direct Fastly API calls; it does not route.
    guest_allowed: &["fastly-key"],
}];

const HOP_FIELDS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "transfer-encoding",
    "upgrade",
];

// A guest cannot ask a later hop to discard checked authority, credentials or
// framing. Proxy-Authorization otherwise remains intact for an explicit proxy.
const PROTECTED_FIELDS: &[&str] = &[
    "host",
    "authorization",
    "proxy-authorization",
    "content-length",
    "transfer-encoding",
    "trailer",
    "sec-websocket-key",
    "sec-websocket-version",
];

#[derive(Debug, thiserror::Error)]
#[error("invalid connection or transfer-coding headers")]
pub(super) struct InvalidHeaders;

/// Guest headers with known provider-routing metadata removed. This does not
/// authorize Host/URI or make unknown provider metadata safe.
#[derive(Debug)]
pub(super) struct SanitizedHeaders(HeaderMap);

impl From<HeaderMap> for SanitizedHeaders {
    fn from(mut headers: HeaderMap) -> Self {
        strip_untrusted(&mut headers);
        Self(headers)
    }
}

impl SanitizedHeaders {
    pub(super) fn contains_key(&self, name: impl header::AsHeaderName) -> bool {
        self.0.contains_key(name)
    }

    pub(super) fn get(&self, name: impl header::AsHeaderName) -> Option<&HeaderValue> {
        self.0.get(name)
    }

    pub(super) fn get_all(
        &self,
        name: impl header::AsHeaderName,
    ) -> header::GetAll<'_, HeaderValue> {
        self.0.get_all(name)
    }
}

/// Keeps Hyper's mutable HeaderMap out of the proxy after ingress. The private
/// request retains its body and extensions (including OnUpgrade), but headers
/// are restored only inside the final Hyper send adapters.
#[derive(Debug)]
pub(super) struct SanitizedRequest<B> {
    request: Request<B>,
    headers: SanitizedHeaders,
    connection_fields: Vec<HeaderName>,
    websocket_upgrade: bool,
}

impl<B> TryFrom<Request<B>> for SanitizedRequest<B> {
    type Error = InvalidHeaders;

    fn try_from(mut request: Request<B>) -> Result<Self, Self::Error> {
        let mut headers = std::mem::take(request.headers_mut());
        let connection_fields = field_names(&headers, header::CONNECTION)?;
        if connection_fields
            .iter()
            .any(|name| PROTECTED_FIELDS.contains(&name.as_str()))
        {
            return Err(InvalidHeaders);
        }
        // Hyper decodes chunk framing, not preceding transfer codings. Do not
        // silently turn `gzip, chunked` into an unencoded upstream body.
        let codings = field_names(&headers, header::TRANSFER_ENCODING)?;
        if headers.contains_key(header::TRANSFER_ENCODING)
            && !matches!(codings.as_slice(), [coding] if coding == "chunked")
        {
            return Err(InvalidHeaders);
        }
        let mut trailers = field_names(&headers, header::TRAILER)?;
        trailers.retain(|name| trailer_allowed(name, &connection_fields));
        headers.remove(header::TRAILER);
        if !trailers.is_empty() {
            let names = trailers
                .iter()
                .map(HeaderName::as_str)
                .collect::<Vec<_>>()
                .join(", ");
            headers.insert(header::TRAILER, names.parse().map_err(|_| InvalidHeaders)?);
        }
        Ok(Self {
            request,
            headers: headers.into(),
            connection_fields,
            websocket_upgrade: false,
        })
    }
}

impl<B> SanitizedRequest<B> {
    pub(super) fn method(&self) -> &Method {
        self.request.method()
    }
    pub(super) fn uri(&self) -> &Uri {
        self.request.uri()
    }
    pub(super) fn uri_mut(&mut self) -> &mut Uri {
        self.request.uri_mut()
    }
    pub(super) fn version(&self) -> Version {
        self.request.version()
    }
    pub(super) fn version_mut(&mut self) -> &mut Version {
        self.request.version_mut()
    }
    pub(super) fn headers(&self) -> &SanitizedHeaders {
        &self.headers
    }
    pub(super) fn set_host(&mut self, host: HeaderValue) {
        self.headers.0.insert(header::HOST, host);
    }

    /// Install one policy-authorized value after guest-controlled routing
    /// metadata has been stripped. Keeping this narrow preserves the invariant
    /// that ordinary proxy code cannot recover general HeaderMap mutability.
    pub(super) fn set_network_credential(&mut self, name: HeaderName, value: HeaderValue) {
        self.headers.0.insert(name, value);
    }

    pub(super) fn map<C>(self, f: impl FnOnce(B) -> C) -> SanitizedRequest<C> {
        SanitizedRequest {
            request: self.request.map(f),
            headers: self.headers,
            connection_fields: self.connection_fields,
            websocket_upgrade: self.websocket_upgrade,
        }
    }

    pub(super) fn on_upgrade(&mut self) -> hyper::upgrade::OnUpgrade {
        hyper::upgrade::on(&mut self.request)
    }

    pub(super) fn set_websocket_upgrade(&mut self) {
        self.websocket_upgrade = true;
    }
}

impl<B: Body + Unpin + 'static> SanitizedRequest<B> {
    fn into_http_request(mut self, protocol: HttpProtocol) -> Request<SanitizedBody<B>> {
        let http1 = protocol == HttpProtocol::Http1;
        let headers = &mut self.headers.0;
        let accepts_trailers = headers.get_all(header::TE).iter().any(|value| {
            value
                .as_bytes()
                .split(|byte| *byte == b',')
                .any(|token| token.trim_ascii().eq_ignore_ascii_case(b"trailers"))
        });
        let chunked = headers.contains_key(header::TRANSFER_ENCODING)
            || (!self.request.body().is_end_stream()
                && (self.request.body().size_hint().exact().is_none()
                    || headers.contains_key(header::TRAILER)));
        strip_hop_fields(headers, &self.connection_fields);
        if accepts_trailers {
            headers.insert(header::TE, HeaderValue::from_static("trailers"));
            if http1 {
                headers.insert(header::CONNECTION, HeaderValue::from_static("te"));
            }
        }
        // Hyper otherwise assumes an unknown-length GET/HEAD has no body.
        // Framing belongs to this hop, not to guest Connection nominations.
        if http1 && chunked {
            headers.remove(header::CONTENT_LENGTH);
            headers.insert(
                header::TRANSFER_ENCODING,
                HeaderValue::from_static("chunked"),
            );
        }
        if self.websocket_upgrade {
            headers.insert(
                header::CONNECTION,
                HeaderValue::from_static(if accepts_trailers {
                    "upgrade, te"
                } else {
                    "upgrade"
                }),
            );
            headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
        }
        *self.request.headers_mut() = self.headers.0;
        self.request.map(|inner| SanitizedBody {
            inner,
            connection_fields: self.connection_fields,
        })
    }

    pub(super) async fn send_http1(
        self,
        sender: &mut http1::SendRequest<SanitizedBody<B>>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        sender
            .send_request(self.into_http_request(HttpProtocol::Http1))
            .await
    }

    pub(super) async fn send_http2(
        self,
        sender: &mut http2::SendRequest<SanitizedBody<B>>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        sender
            .send_request(self.into_http_request(HttpProtocol::Http2))
            .await
    }
}

/// Streams data unchanged and filters trailer frames without buffering bodies.
#[derive(Debug)]
pub(super) struct SanitizedBody<B> {
    inner: B,
    connection_fields: Vec<HeaderName>,
}

impl<B: Body + Unpin> Body for SanitizedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx).map(|frame| {
            frame.map(|result| {
                result.map(|mut frame| {
                    if let Some(trailers) = frame.trailers_mut() {
                        let blocked = trailers
                            .keys()
                            .filter(|name| !trailer_allowed(name, &self.connection_fields))
                            .cloned()
                            .collect::<Vec<_>>();
                        for name in blocked {
                            trailers.remove(name);
                        }
                    }
                    frame
                })
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

fn field_names(headers: &HeaderMap, field: HeaderName) -> Result<Vec<HeaderName>, InvalidHeaders> {
    headers
        .get_all(field)
        .iter()
        .flat_map(|value| value.as_bytes().split(|byte| *byte == b','))
        .map(<[u8]>::trim_ascii)
        .filter(|name| !name.is_empty())
        .map(|name| HeaderName::from_bytes(name).map_err(|_| InvalidHeaders))
        .collect()
}

fn strip_hop_fields(headers: &mut HeaderMap, connection_fields: &[HeaderName]) {
    for name in connection_fields {
        headers.remove(name);
    }
    for name in HOP_FIELDS {
        headers.remove(*name);
    }
}

fn trailer_allowed(name: &HeaderName, connection_fields: &[HeaderName]) -> bool {
    !provider_routing_header(name)
        && !connection_fields.contains(name)
        && !HOP_FIELDS.contains(&name.as_str())
        && !PROTECTED_FIELDS.contains(&name.as_str())
}

fn provider_routing_header(name: &HeaderName) -> bool {
    // HeaderName is lowercase, so provider matching is case-insensitive.
    PROVIDERS.iter().any(|provider| {
        name.as_str().starts_with(provider.strip_prefix)
            && !provider.guest_allowed.contains(&name.as_str())
    })
}

fn strip_untrusted(headers: &mut HeaderMap) {
    let blocked = headers
        .keys()
        .filter(|name| provider_routing_header(name))
        .cloned()
        .collect::<Vec<_>>();
    for name in blocked {
        headers.remove(name);
    }
}
