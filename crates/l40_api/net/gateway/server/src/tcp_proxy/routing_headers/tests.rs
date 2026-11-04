use super::*;

#[test]
fn rejects_malformed_or_security_critical_connection_nominations() {
    for name in [
        "Host",
        "Authorization",
        "Proxy-Authorization",
        "Content-Length",
        "Transfer-Encoding",
        "Trailer",
        "Sec-WebSocket-Key",
        "Sec-WebSocket-Version",
        "bad name",
        "bad\tname",
    ] {
        let request = Request::get("https://allowed.test/")
            .header(header::CONNECTION, "keep-alive")
            .header(header::CONNECTION, name)
            .body(())
            .unwrap();
        SanitizedRequest::try_from(request).expect_err(name);
    }
    let request = Request::get("https://allowed.test/")
        .header(header::TRANSFER_ENCODING, "gzip, chunked")
        .body(())
        .unwrap();
    SanitizedRequest::try_from(request).unwrap_err();
}

#[test]
fn strips_provider_metadata_and_keeps_explicit_exceptions() {
    let mut headers = HeaderMap::new();
    headers.insert("Fastly-Host", "forbidden.test".parse().unwrap());
    headers.append("fastly-backend", "a".parse().unwrap());
    headers.append("fastly-backend", "b".parse().unwrap());
    headers.insert("fastly-key", "credential".parse().unwrap());
    headers.insert("fastly-key-extra", "x".parse().unwrap());
    headers.insert("application-header", "value".parse().unwrap());

    let headers = SanitizedHeaders::from(headers);

    let mut names = headers
        .0
        .keys()
        .map(|name| name.as_str())
        .collect::<Vec<_>>();
    names.sort_unstable();
    assert_eq!(names, ["application-header", "fastly-key"]);
}

#[test]
fn provider_prefixes_and_exceptions_are_lowercase() {
    // HeaderName::as_str is lowercase; an uppercase entry would never match.
    for provider in PROVIDERS {
        assert_eq!(
            provider.strip_prefix,
            provider.strip_prefix.to_ascii_lowercase()
        );
        for allowed in provider.guest_allowed {
            assert_eq!(*allowed, allowed.to_ascii_lowercase());
            assert!(allowed.starts_with(provider.strip_prefix));
        }
    }
}
