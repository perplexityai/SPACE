use k8s_openapi::api::discovery::v1::{EndpointConditions, EndpointPort};

use super::*;

fn slice(address_type: &str, endpoints: Vec<Endpoint>, ports: Vec<EndpointPort>) -> EndpointSlice {
    EndpointSlice {
        address_type: address_type.to_owned(),
        endpoints,
        ports: Some(ports),
        metadata: Default::default(),
    }
}

/// `ready: None` covers the absent-conditions case, which the API spec says to
/// read as ready.
fn endpoint(address: &str, ready: Option<bool>) -> Endpoint {
    Endpoint {
        addresses: vec![address.to_owned()],
        conditions: ready.map(|ready| EndpointConditions {
            ready: Some(ready),
            serving: Some(true),
            terminating: Some(!ready),
        }),
        ..Default::default()
    }
}

fn terminating_endpoint(address: &str) -> Endpoint {
    Endpoint {
        addresses: vec![address.to_owned()],
        conditions: Some(EndpointConditions {
            ready: Some(true),
            serving: Some(true),
            terminating: Some(true),
        }),
        ..Default::default()
    }
}

fn port(name: &str, number: i32) -> EndpointPort {
    EndpointPort {
        name: Some(name.to_owned()),
        port: Some(number),
        ..Default::default()
    }
}

fn addrs(slice: &EndpointSlice, selector: &PortSelector) -> Vec<String> {
    slice_addrs(slice, selector)
        .into_iter()
        .map(|addr| addr.to_string())
        .collect()
}

#[test]
fn only_ready_endpoints_are_balanced_over() {
    let slice = slice(
        "IPv4",
        vec![
            endpoint("10.0.0.1", Some(true)),
            endpoint("10.0.0.2", Some(false)),
            endpoint("10.0.0.3", None),
            terminating_endpoint("10.0.0.4"),
        ],
        vec![port("grpc", 9800)],
    );
    assert_eq!(
        addrs(&slice, &PortSelector::Sole),
        ["10.0.0.1:9800", "10.0.0.3:9800"]
    );
}

#[test]
fn port_selection() {
    let two_ports = slice(
        "IPv4",
        vec![endpoint("10.0.0.1", Some(true))],
        vec![port("grpc", 9800), port("metrics", 9100)],
    );
    assert_eq!(
        addrs(&two_ports, &PortSelector::Name("grpc".to_owned())),
        ["10.0.0.1:9800"]
    );
    assert_eq!(
        addrs(&two_ports, &PortSelector::Number(9100)),
        ["10.0.0.1:9100"]
    );
    // `Sole` cannot choose between two, and a name that is not there matches
    // nothing. Both yield no endpoints, which surfaces as the fallback.
    assert!(addrs(&two_ports, &PortSelector::Sole).is_empty());
    assert!(addrs(&two_ports, &PortSelector::Name("absent".to_owned())).is_empty());

    let one_port = slice(
        "IPv4",
        vec![endpoint("10.0.0.1", Some(true))],
        vec![port("grpc", 9800)],
    );
    assert_eq!(addrs(&one_port, &PortSelector::Sole), ["10.0.0.1:9800"]);
}

/// A slice this crate cannot use contributes nothing rather than half-working:
/// FQDN addresses are names, not addresses, and a slice with no ports has no
/// port to dial.
#[test]
fn unusable_slices_contribute_nothing() {
    let fqdn = slice(
        "FQDN",
        vec![endpoint("pod.space.svc.cluster.local", Some(true))],
        vec![port("grpc", 9800)],
    );
    assert!(addrs(&fqdn, &PortSelector::Sole).is_empty());

    let mut no_ports = slice("IPv4", vec![endpoint("10.0.0.1", Some(true))], vec![]);
    assert!(addrs(&no_ports, &PortSelector::Sole).is_empty());
    no_ports.ports = None;
    assert!(addrs(&no_ports, &PortSelector::Sole).is_empty());
}

/// One Service's endpoints span several slices, so the set is their union — and
/// IPv6 addresses have to come back bracketed to be dialable.
#[test]
fn slices_union_and_ipv6_is_bracketed() {
    let ipv4 = slice(
        "IPv4",
        vec![endpoint("10.0.0.1", Some(true))],
        vec![port("grpc", 9800)],
    );
    let ipv6 = slice(
        "IPv6",
        vec![endpoint("fd00::1", Some(true))],
        vec![port("grpc", 9800)],
    );
    let union: Vec<String> = [&ipv4, &ipv6]
        .into_iter()
        .flat_map(|slice| addrs(slice, &PortSelector::Sole))
        .collect();
    assert_eq!(union, ["10.0.0.1:9800", "[fd00::1]:9800"]);
}
