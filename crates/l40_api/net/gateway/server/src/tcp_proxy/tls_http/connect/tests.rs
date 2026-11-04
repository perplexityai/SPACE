use super::*;

#[test]
fn connect_has_one_exact_dns_target_and_no_http_body_or_other_upgrade() {
    for (uri, host, version, extra, accepted) in [
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_11,
            None,
            true,
        ),
        (
            "INNER.test.:8443",
            "inner.test:8443",
            Version::HTTP_11,
            None,
            true,
        ),
        (
            "inner.test:443",
            "different.test:443",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "inner.test:443",
            "inner.test:80",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_2,
            None,
            false,
        ),
        ("inner.test", "inner.test", Version::HTTP_11, None, false),
        (
            "127.0.0.1:443",
            "127.0.0.1:443",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "inner.test:0",
            "inner.test:0",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "inner.test:65536",
            "inner.test:65536",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "https://inner.test/",
            "inner.test:443",
            Version::HTTP_11,
            None,
            false,
        ),
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_11,
            Some(("host", "inner.test:443")),
            false,
        ),
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_11,
            Some(("content-length", "1")),
            false,
        ),
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_11,
            Some(("transfer-encoding", "chunked")),
            false,
        ),
        (
            "inner.test:443",
            "inner.test:443",
            Version::HTTP_11,
            Some(("upgrade", "websocket")),
            false,
        ),
    ] {
        let mut request = Request::builder()
            .method("CONNECT")
            .uri(uri)
            .version(version)
            .header(header::HOST, host);
        if let Some((name, value)) = extra {
            request = request.header(name, value);
        }
        let request = SanitizedRequest::try_from(request.body(()).unwrap()).unwrap();
        assert_eq!(target(&request).is_ok(), accepted, "{request:?}");
    }
}

#[test]
fn connect_target_requires_exact_dns_names_and_ports() {
    for (text, accepted) in [
        ("proxy.test:443", true),
        ("Proxy.test.:8443", true),
        ("proxy.test", false),
        ("proxy.test:0", false),
        ("proxy.test:65536", false),
        ("proxy.test:", false),
        ("*.test:443", false),
        ("user@proxy.test:443", false),
        ("https://proxy.test:443", false),
        ("127.0.0.1:443", false),
    ] {
        assert_eq!(text.parse::<Target>().is_ok(), accepted, "{text}");
    }
}
