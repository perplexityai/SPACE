use pplx_space_net_gateway::tls_sni::{ClientHelloScan, scan_client_hello};

use super::*;

/// The builder is only useful if the gateway's own parser agrees, so assert
/// against the parser rather than against a byte snapshot.
#[test]
fn the_gateway_parser_extracts_the_server_name_we_encoded() {
    let scan = scan_client_hello(&with_server_name("bench.example.com"));
    assert_eq!(
        scan.server_name().map(|name| name.as_str().to_string()),
        Some("bench.example.com".to_string())
    );
}

#[test]
fn a_hello_without_sni_carries_no_server_name() {
    assert_eq!(
        scan_client_hello(&without_server_name()),
        ClientHelloScan::NoServerName
    );
}
