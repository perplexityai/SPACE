use super::*;

#[test]
fn upstream_config_preserves_typed_http_alpn_order() {
    let interception = TlsInterception::default();
    for (offered, expected) in [
        (
            [HttpAlpn::Http2, HttpAlpn::Http1].as_slice(),
            [b"h2".as_slice(), b"http/1.1".as_slice()].as_slice(),
        ),
        (
            [HttpAlpn::Http1, HttpAlpn::Http2].as_slice(),
            [b"http/1.1".as_slice(), b"h2".as_slice()].as_slice(),
        ),
    ] {
        let config = interception.upstream_config(offered);
        assert_eq!(config.alpn_protocols, expected);
    }
}

#[tokio::test]
async fn off_preserves_every_scan_and_non_tls_skips_certificates_in_every_mode() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let recorded_calls = Arc::clone(&calls);
    let requester: CertificateRequester = Arc::new(move |_, _| {
        recorded_calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Box::pin(async { Err(tonic::Status::unavailable("unexpected certificate request")) })
    });
    let interception = TlsInterception::new(Arc::clone(&requester));
    for scan in [
        ClientHelloScan::NotTls,
        ClientHelloScan::Incomplete,
        ClientHelloScan::NoServerName,
        ClientHelloScan::Malformed,
        ClientHelloScan::ServerName("good.com".parse().unwrap()),
    ] {
        assert!(matches!(
            interception
                .before_relay(
                    pplx_space_net_common::TlsInterception::Off,
                    &scan,
                    None,
                    443,
                )
                .await
                .unwrap(),
            InterceptionAction::PassThrough
        ));
    }
    for mode in [
        pplx_space_net_common::TlsInterception::Off,
        pplx_space_net_common::TlsInterception::On,
    ] {
        assert!(matches!(
            interception
                .before_relay(mode, &ClientHelloScan::NotTls, None, 80)
                .await
                .unwrap(),
            InterceptionAction::PassThrough
        ));
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);

    let Err(error) = interception
        .before_relay(
            pplx_space_net_common::TlsInterception::On,
            &ClientHelloScan::ServerName("good.com".parse().unwrap()),
            None,
            443,
        )
        .await
    else {
        panic!("on policy must request a certificate")
    };
    assert!(matches!(error, TlsInterceptionError::CertificateRequest(_)));
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
}
