use super::*;

fn destination(address: Ipv4Addr) -> Vec<NeighbourAttribute> {
    vec![NeighbourAttribute::Destination(NeighbourAddress::Inet(
        address,
    ))]
}

// Only the gateway's own cached entry on the primary interface goes, as with
// `ip neigh flush <gateway> dev <interface>`; permanent and no-ARP entries
// stay.
#[test]
fn only_the_gateways_cached_entry_on_the_primary_interface_is_flushed() {
    let gateway = gateway().unwrap();
    let other: Ipv4Addr = "169.254.0.1".parse().unwrap();
    for state in [
        NeighbourState::Reachable,
        NeighbourState::Stale,
        NeighbourState::Delay,
        NeighbourState::Probe,
        NeighbourState::Failed,
    ] {
        assert!(
            is_flushable_gateway_entry(2, state, &destination(gateway), 2, gateway),
            "{state:?}"
        );
    }
    for state in [NeighbourState::Permanent, NeighbourState::Noarp] {
        assert!(
            !is_flushable_gateway_entry(2, state, &destination(gateway), 2, gateway),
            "{state:?}"
        );
    }
    assert!(!is_flushable_gateway_entry(
        3,
        NeighbourState::Reachable,
        &destination(gateway),
        2,
        gateway
    ));
    assert!(!is_flushable_gateway_entry(
        2,
        NeighbourState::Reachable,
        &destination(other),
        2,
        gateway
    ));
    assert!(!is_flushable_gateway_entry(
        2,
        NeighbourState::Reachable,
        &[],
        2,
        gateway
    ));
}
