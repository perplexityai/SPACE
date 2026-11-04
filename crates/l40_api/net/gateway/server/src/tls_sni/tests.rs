use super::test_fixtures::{
    alpn_extension, client_hello, client_hello_with_extensions, filler_extension, handshake_record,
    sni_extension,
};
use super::*;

#[test]
fn classifies_client_alpn_without_affecting_sni() {
    for (protocols, expected) in [
        (vec![b"http/1.1".as_slice()], ClientAlpnOffer::Http1),
        (vec![b"h2".as_slice()], ClientAlpnOffer::Http2),
        (
            vec![b"h2".as_slice(), b"http/1.1".as_slice()],
            ClientAlpnOffer::Http2ThenHttp1,
        ),
        (
            vec![b"http/1.1".as_slice(), b"h2".as_slice()],
            ClientAlpnOffer::Http1ThenHttp2,
        ),
        (
            vec![
                b"acme/1".as_slice(),
                b"h2".as_slice(),
                b"other/1".as_slice(),
                b"http/1.1".as_slice(),
            ],
            ClientAlpnOffer::Http2ThenHttp1,
        ),
        (vec![b"acme/1".as_slice()], ClientAlpnOffer::Unsupported),
    ] {
        let mut extensions = sni_extension(&[(0x00, b"example.com")]);
        extensions.extend(alpn_extension(&protocols));
        let (scan, offered) =
            scan_client_hello_with_alpn(&client_hello_with_extensions(&extensions));
        assert_eq!(scan.server_name().unwrap().as_str(), "example.com");
        assert_eq!(offered, expected);
    }
}

#[test]
fn exposes_only_supported_http_protocols_in_guest_order() {
    for (offer, expected) in [
        (ClientAlpnOffer::NoAlpn, Some([].as_slice())),
        (ClientAlpnOffer::Http1, Some([HttpAlpn::Http1].as_slice())),
        (ClientAlpnOffer::Http2, Some([HttpAlpn::Http2].as_slice())),
        (
            ClientAlpnOffer::Http2ThenHttp1,
            Some([HttpAlpn::Http2, HttpAlpn::Http1].as_slice()),
        ),
        (
            ClientAlpnOffer::Http1ThenHttp2,
            Some([HttpAlpn::Http1, HttpAlpn::Http2].as_slice()),
        ),
        (ClientAlpnOffer::Unsupported, None),
        (ClientAlpnOffer::Unavailable, None),
    ] {
        assert_eq!(offer.supported(), expected);
    }
}

#[test]
fn distinguishes_no_alpn_from_unavailable_alpn() {
    let (scan, offered) = scan_client_hello_with_alpn(&client_hello_with_extensions(
        &sni_extension(&[(0x00, b"example.com")]),
    ));
    assert!(scan.server_name().is_some());
    assert_eq!(offered, ClientAlpnOffer::NoAlpn);
    assert_eq!(
        scan_client_hello_with_alpn(b"not tls").1,
        ClientAlpnOffer::Unavailable,
    );
}

fn extracted(scan: ClientHelloScan) -> Option<String> {
    scan.server_name().map(|name| name.as_str().to_string())
}

// --- happy path -------------------------------------------------------

#[test]
fn extracts_host_name_sni() {
    assert_eq!(
        extracted(scan_client_hello(&client_hello(Some("api.anthropic.com")))),
        Some("api.anthropic.com".to_string())
    );
}

#[test]
fn extracts_sni_from_handshake_split_across_records() {
    let complete = client_hello(Some("api.anthropic.com"));
    let payload = complete.get(5..).expect("fixture has a record header");
    let middle = payload.len() / 2;
    let (first, second) = payload.split_at(middle);
    let mut fragmented = Vec::new();
    for part in [first, second] {
        let length = u16::try_from(part.len()).expect("fixture part fits in a TLS record");
        fragmented.extend_from_slice(&[TLS_HANDSHAKE_CONTENT_TYPE, 0x03, 0x01]);
        fragmented.extend_from_slice(&length.to_be_bytes());
        fragmented.extend_from_slice(part);
    }
    assert_eq!(
        extracted(scan_client_hello(&fragmented)),
        Some("api.anthropic.com".to_string())
    );
}

#[test]
fn normalizes_case_and_trailing_dot() {
    assert_eq!(
        extracted(scan_client_hello(&client_hello(Some("API.Anthropic.COM.")))),
        Some("api.anthropic.com".to_string())
    );
}

#[test]
fn accepts_punycode_idn_host() {
    assert_eq!(
        extracted(scan_client_hello(&client_hello(Some(
            "xn--80akhbyknj4f.example"
        )))),
        Some("xn--80akhbyknj4f.example".to_string())
    );
}

#[test]
fn accepts_hyphen_labels() {
    assert_eq!(
        extracted(scan_client_hello(&client_hello(Some(
            "mail-gateway.sub-domain.example.com"
        )))),
        Some("mail-gateway.sub-domain.example.com".to_string())
    );
}

#[test]
fn extracts_sni_among_other_extensions() {
    // GREASE-like filler before and a supported_versions-like filler after
    // the SNI: extraction must not depend on extension order/position.
    let mut extensions = filler_extension(0x0a0a, 4);
    extensions.extend_from_slice(&sni_extension(&[(0x00, b"host.example.com")]));
    extensions.extend_from_slice(&filler_extension(0x1a1a, 3));
    assert_eq!(
        extracted(scan_client_hello(&client_hello_with_extensions(
            &extensions
        ))),
        Some("host.example.com".to_string())
    );
}

#[test]
fn extracts_max_length_sni() {
    let name = format!(
        "{}.{}.{}.{}",
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(61)
    );
    assert_eq!(name.len(), 253);
    assert_eq!(
        extracted(scan_client_hello(&client_hello(Some(&name)))),
        Some(name.clone())
    );
}

// --- no SNI -----------------------------------------------------------

#[test]
fn client_hello_without_sni_has_no_sni() {
    assert_eq!(
        scan_client_hello(&client_hello(None)),
        ClientHelloScan::NoServerName
    );
}

#[test]
fn non_host_name_sni_entry_is_malformed() {
    // rustls rejects an SNI extension without a host_name entry. This still
    // denies domain-bound traffic, and surfaces the malformed peer input.
    let bytes = client_hello_with_extensions(&sni_extension(&[(0x01, b"api.anthropic.com")]));
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

#[test]
fn multiple_host_names_in_sni_are_malformed() {
    let bytes = client_hello_with_extensions(&sni_extension(&[
        (0x00, b"first.example.com"),
        (0x00, b"second.example.com"),
    ]));

    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

// --- malformed --------------------------------------------------------

#[test]
fn over_long_sni_is_malformed() {
    let name = format!(
        "{}.{}.{}.{}",
        "a".repeat(63),
        "b".repeat(63),
        "c".repeat(63),
        "d".repeat(62)
    );
    assert_eq!(name.len(), 254);
    let bytes = client_hello_with_extensions(&sni_extension(&[(0x00, name.as_bytes())]));
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

#[test]
fn sni_with_non_hostname_label_is_malformed() {
    for name in ["_dmarc.example.com", "-leading.example.com"] {
        assert_eq!(
            scan_client_hello(&client_hello(Some(name))),
            ClientHelloScan::Malformed,
            "{name} is not a policy DomainName"
        );
    }
}

#[test]
fn sni_with_control_characters_is_malformed() {
    let bytes = client_hello_with_extensions(&sni_extension(&[(0x00, b"bad\x00host.com")]));
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

#[test]
fn non_utf8_sni_is_malformed() {
    let bytes = client_hello_with_extensions(&sni_extension(&[(0x00, b"\xff.example.com")]));
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

#[test]
fn sni_with_whitespace_is_malformed() {
    for name in ["bad host.com", " example.com", "example.com "] {
        assert_eq!(
            scan_client_hello(&client_hello(Some(name))),
            ClientHelloScan::Malformed
        );
    }
}

#[test]
fn server_hello_handshake_is_malformed() {
    // A handshake record whose message is a ServerHello (0x02), not a
    // ClientHello — not what a fresh client flow opens with.
    let bytes = handshake_record(0x02, &[0x03, 0x03]);
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Malformed);
}

#[test]
fn short_handshake_record_remains_incomplete() {
    // A complete short record can be followed by another record carrying the
    // rest of a fragmented handshake. The peek deadline bounds this wait.
    let bytes = [
        TLS_HANDSHAKE_CONTENT_TYPE,
        0x03,
        0x01,
        0x00,
        0x02,
        0xff,
        0xff,
    ];
    assert_eq!(scan_client_hello(&bytes), ClientHelloScan::Incomplete);
}

// --- not TLS ----------------------------------------------------------

#[test]
fn plaintext_http_is_not_tls() {
    assert_eq!(
        scan_client_hello(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"),
        ClientHelloScan::NotTls
    );
}

#[test]
fn tls_alert_record_is_not_tls() {
    // content_type = alert (0x15), not handshake.
    assert_eq!(
        scan_client_hello(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28]),
        ClientHelloScan::NotTls
    );
}

#[test]
fn application_data_record_is_not_tls() {
    // content_type = application_data (0x17).
    assert_eq!(
        scan_client_hello(&[0x17, 0x03, 0x03, 0x00, 0x01, 0x00]),
        ClientHelloScan::NotTls
    );
}

// --- incomplete -------------------------------------------------------

#[test]
fn empty_buffer_is_incomplete() {
    assert_eq!(scan_client_hello(&[]), ClientHelloScan::Incomplete);
}

#[test]
fn partial_record_header_is_incomplete() {
    // First byte announces a handshake record, but fewer than the 5 record
    // header bytes are present.
    assert_eq!(
        scan_client_hello(&[TLS_HANDSHAKE_CONTENT_TYPE, 0x03]),
        ClientHelloScan::Incomplete
    );
}

#[test]
fn truncated_client_hello_is_incomplete() {
    let full = client_hello(Some("api.anthropic.com"));
    assert_eq!(
        scan_client_hello(&full[..full.len() - 10]),
        ClientHelloScan::Incomplete
    );
}

// --- record framing edge cases ----------------------------------------

#[test]
fn trailing_bytes_after_client_hello_are_ignored() {
    // The record length bounds the ClientHello; junk appended after a
    // complete record must not affect extraction.
    let mut bytes = client_hello(Some("trailing.example.com"));
    bytes.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(
        extracted(scan_client_hello(&bytes)),
        Some("trailing.example.com".to_string())
    );
}

#[test]
fn second_record_after_client_hello_is_ignored() {
    // Two TLS records back to back: the SNI comes from the first.
    let mut bytes = client_hello(Some("first.example.com"));
    bytes.extend_from_slice(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28]); // an alert record
    assert_eq!(
        extracted(scan_client_hello(&bytes)),
        Some("first.example.com".to_string())
    );
}

// --- robustness: never panic on hostile input -------------------------

#[test]
fn scanning_every_prefix_never_panics() {
    let full = client_hello(Some("prefix.example.com"));
    for len in 0..=full.len() {
        // Must not panic for any prefix length.
        let _ = scan_client_hello(&full[..len]);
    }
    // The complete buffer still extracts.
    assert_eq!(
        extracted(scan_client_hello(&full)),
        Some("prefix.example.com".to_string())
    );
}

#[test]
fn flipping_any_single_byte_never_panics() {
    let full = client_hello(Some("flip.example.com"));
    for index in 0..full.len() {
        let mut corrupted = full.clone();
        corrupted[index] ^= 0xff;
        // Any single-byte corruption must classify, not panic.
        let _ = scan_client_hello(&corrupted);
    }
}

#[test]
fn pseudo_random_bytes_never_panic() {
    // A small deterministic LCG; no external rng dependency and no reliance
    // on banned wall-clock/rng sources. Just proves the parser is panic-free
    // on arbitrary bytes (it is guest-controlled input).
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut buf = vec![0u8; 512];
    for _ in 0..2000 {
        for byte in buf.iter_mut() {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *byte = (state >> 33) as u8;
        }
        let _ = scan_client_hello(&buf);
    }
}
