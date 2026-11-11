use super::*;

#[test]
fn reports_only_configured_dns_and_tcp_hosts() {
    let state = KnownEgressState::new(vec!["egress-proxy.example.test".parse().unwrap()]);
    let dns = "127.0.0.1:53".parse().unwrap();
    let tcp = "127.0.0.1:443".parse().unwrap();
    let mut query = vec![
        0x12, 0x34, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    ];
    for label in "egress-proxy.example.test".split('.') {
        query.push(u8::try_from(label.len()).unwrap());
        query.extend_from_slice(label.as_bytes());
    }
    query.extend_from_slice(&[0, 0, 1, 0, 1]);

    assert!(state.report_dns_failure("test", dns, &query, "denied"));
    assert!(state.report_tcp_failure(
        "test",
        tcp,
        &"EGRESS-PROXY.EXAMPLE.TEST.".parse().unwrap(),
        "connect"
    ));
    assert!(!state.report_tcp_failure("test", tcp, &"other.example".parse().unwrap(), "connect"));
}
