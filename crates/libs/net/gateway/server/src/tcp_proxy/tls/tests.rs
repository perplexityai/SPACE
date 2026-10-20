use std::sync::{Arc, Mutex};

use pplx_space_observability::test_capture::{CapturedEvent, RecordingVisitor};
use tracing_subscriber::layer::SubscriberExt;

use super::super::{inspected::upstream_alpn_offer, tls_http::HttpProtocol};
use super::TlsTransportError;
use crate::tls_sni::{ClientAlpnOffer, HttpAlpn};

/// The origin leg follows the guest's ClientHello unless the guest offered h2
/// and then spoke HTTP/1.1, in which case the origin must not be able to pick
/// h2 (the gateway tunnels CONNECT over HTTP/1.1 only).
#[test]
fn origin_alpn_offer_matches_what_the_guest_actually_speaks() {
    use ClientAlpnOffer as Offer;
    use HttpProtocol as Http;

    for (client_alpn, guest_protocol, expected) in [
        (Offer::Http2ThenHttp1, Http::Http1, Offer::Http1),
        (Offer::Http1ThenHttp2, Http::Http1, Offer::Http1),
        (Offer::Http2, Http::Http1, Offer::Http1),
        (Offer::Http2ThenHttp1, Http::Http2, Offer::Http2ThenHttp1),
        (Offer::Http1ThenHttp2, Http::Http2, Offer::Http1ThenHttp2),
        (Offer::Http2, Http::Http2, Offer::Http2),
        (Offer::Http1, Http::Http1, Offer::Http1),
        (Offer::NoAlpn, Http::Http1, Offer::NoAlpn),
        (Offer::Unavailable, Http::Http1, Offer::Unavailable),
        (Offer::Unsupported, Http::Http1, Offer::Unsupported),
    ] {
        assert_eq!(
            upstream_alpn_offer(client_alpn, guest_protocol),
            expected,
            "{client_alpn:?} with guest {guest_protocol:?}"
        );
    }
}

#[test]
fn upstream_protocol_requires_guest_compatibility() {
    use ClientAlpnOffer as Offer;
    use HttpAlpn as Alpn;
    use HttpAlpn as Http;

    for (offered, selected, expected) in [
        (Offer::NoAlpn, None, Some(Http::Http1)),
        (Offer::Http1, None, Some(Http::Http1)),
        (Offer::Http2ThenHttp1, None, Some(Http::Http1)),
        (Offer::Http1ThenHttp2, None, Some(Http::Http1)),
        (Offer::Http2, None, None),
        (Offer::Http1, Some(Alpn::Http1), Some(Http::Http1)),
        (Offer::Http2, Some(Alpn::Http2), Some(Http::Http2)),
        (Offer::Http2ThenHttp1, Some(Alpn::Http1), Some(Http::Http1)),
        (Offer::Http2ThenHttp1, Some(Alpn::Http2), Some(Http::Http2)),
        (Offer::Http1ThenHttp2, Some(Alpn::Http1), Some(Http::Http1)),
        (Offer::Http1ThenHttp2, Some(Alpn::Http2), Some(Http::Http2)),
        (Offer::NoAlpn, Some(Alpn::Http1), None),
        (Offer::NoAlpn, Some(Alpn::Http2), None),
        (Offer::Http1, Some(Alpn::Http2), None),
        (Offer::Http2, Some(Alpn::Http1), None),
        (Offer::Unavailable, None, None),
        (Offer::Unsupported, None, None),
    ] {
        let result = super::negotiated_upstream(offered, selected);
        assert_eq!(
            result.as_ref().ok().copied(),
            expected,
            "{offered:?} selected {selected:?}"
        );
        if expected.is_none() {
            assert!(matches!(result, Err(TlsTransportError::Protocol)));
        }
    }
}

#[test]
fn tls_diagnostics_do_not_export_the_guest_hostname() {
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<CapturedEvent>>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Capture {
        fn on_new_span(
            &self,
            attributes: &tracing::span::Attributes<'_>,
            _id: &tracing::Id,
            _context: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = RecordingVisitor::default();
            attributes.record(&mut visitor);
            self.0.lock().unwrap().extend(visitor.into_fields());
        }
    }

    let captured = Capture::default();
    let subscriber = tracing_subscriber::registry().with(captured.clone());
    let hostname = "private-customer-destination.example";
    tracing::subscriber::with_default(subscriber, || {
        let _span = super::upstream_span(&hostname.parse().unwrap(), 443);
    });
    let fields = captured.0.lock().unwrap();
    assert!(fields.iter().all(|(_, value)| !value.contains(hostname)));
    assert!(fields.contains(&(
        "server.address_length".to_owned(),
        hostname.len().to_string(),
    )));
    assert!(fields.contains(&("server.port".to_owned(), "443".to_owned())));

    // TLS library errors can include expected SNI and certificate SAN values.
    // The enforcement event logs the outer error's Display, not just the span.
    for error in [
        super::TlsTransportError::Downstream(std::io::Error::other(hostname)),
        super::TlsTransportError::Upstream(std::io::Error::other(hostname)),
        super::TlsTransportError::Configuration(rustls::Error::General(hostname.to_owned())),
        super::TlsTransportError::Certificate(crate::certificates::CertificateError::Tls(
            rustls::Error::General(hostname.to_owned()),
        )),
    ] {
        let outcome = error.as_str();
        assert!(std::error::Error::source(&error).is_some());
        assert!(!error.to_string().contains(hostname));
        let message = super::super::tls_http::HttpError::from(error).to_string();
        assert!(!message.contains(hostname), "{outcome}: {message}");
    }

    let rpc_error = crate::tls_interception::TlsInterceptionError::CertificateRequest(
        tonic::Status::permission_denied(hostname),
    );
    assert!(std::error::Error::source(&rpc_error).is_some());
    assert!(!rpc_error.to_string().contains(hostname));
    assert!(rpc_error.to_string().contains("PermissionDenied"));
}

mod http_failure {
    use std::convert::Infallible;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use bytes::Bytes;
    use http_body_util::{Empty, StreamBody};
    use hyper::body::Frame;
    use hyper::service::service_fn;
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};

    use super::super::super::tls_http::http_reason;

    /// Delivers one request, then fails every write the way a guest reset does.
    struct ResetAfterRequest(&'static [u8]);

    impl AsyncRead for ResetAfterRequest {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let take = self.0.len().min(buf.remaining());
            if take == 0 {
                return Poll::Pending;
            }
            buf.put_slice(&self.0[..take]);
            self.0 = &self.0[take..];
            Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for ResetAfterRequest {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn empty() -> hyper::Response<Empty<Bytes>> {
        hyper::Response::new(Empty::new())
    }

    #[tokio::test]
    async fn opaque_socket_error_does_not_claim_a_native_kind() {
        let io = ResetAfterRequest(b"GET / HTTP/1.1\r\nhost: a\r\n\r\n");
        let error = hyper::server::conn::http1::Builder::new()
            .serve_connection(
                TokioIo::new(io),
                service_fn(|_| async { Ok::<_, Infallible>(empty()) }),
            )
            .await
            .expect_err("writes fail");
        let failure = http_reason(&error);
        assert_eq!(failure, "other");
    }

    #[tokio::test]
    async fn response_body_error_is_user() {
        let (mut client, server) = tokio::io::duplex(4096);
        let serve = hyper::server::conn::http1::Builder::new().serve_connection(
            TokioIo::new(server),
            service_fn(|_| async {
                let body = StreamBody::new(futures_util::stream::iter([Err::<Frame<Bytes>, _>(
                    io::Error::other("origin body reset"),
                )]));
                Ok::<_, Infallible>(hyper::Response::new(body))
            }),
        );
        client
            .write_all(b"GET / HTTP/1.1\r\nhost: a\r\n\r\n")
            .await
            .unwrap();
        let error = serve.await.expect_err("body stream fails");
        let failure = http_reason(&error);
        assert_eq!(failure, "user");
    }

    #[tokio::test]
    async fn opaque_h2_error_does_not_claim_a_code_or_origin() {
        let (mut client, server) = tokio::io::duplex(4096);
        let serve = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(
                TokioIo::new(server),
                service_fn(|_| async { Ok::<_, Infallible>(empty()) }),
            );
        let task = tokio::spawn(serve);
        // Valid preface and empty SETTINGS, then a PING on stream 1: a
        // connection-level PROTOCOL_ERROR under RFC 9113 section 6.7.
        let mut wire = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n".to_vec();
        wire.extend_from_slice(&[0, 0, 0, 0x4, 0, 0, 0, 0, 0]);
        wire.extend_from_slice(&[0, 0, 8, 0x6, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]);
        client.write_all(&wire).await.unwrap();
        let error = task.await.unwrap().expect_err("peer violated h2");
        let failure = http_reason(&error);
        assert_eq!(failure, "other");
    }

    #[tokio::test]
    async fn historical_buckets_keep_their_names() {
        let (mut client, server) = tokio::io::duplex(4096);
        let serve = hyper::server::conn::http1::Builder::new().serve_connection(
            TokioIo::new(server),
            service_fn(|_| async { Ok::<_, Infallible>(empty()) }),
        );
        client
            .write_all(b"\x00\x01 not http\r\n\r\n")
            .await
            .unwrap();
        let error = serve.await.expect_err("garbage request");
        assert_eq!(http_reason(&error), "parse");
    }
}
