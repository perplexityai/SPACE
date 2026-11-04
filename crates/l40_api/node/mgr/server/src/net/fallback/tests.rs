use super::*;

#[test]
fn default_route_probe_distinguishes_absent_space_link_from_failed_dump() {
    assert_eq!(
        interpret_default_route_output(
            true,
            b"default via 192.168.64.1 dev en0 proto dhcp\n",
            b"",
        )
        .unwrap(),
        false
    );
    assert_eq!(
        interpret_default_route_output(
            true,
            b"default via 10.12.0.1 dev sveth-000001 proto static\n",
            b"",
        )
        .unwrap(),
        true
    );
    assert_eq!(
        interpret_default_route_output(true, b"default via 10.12.0.1 dev vpeer-000001\n", b"",)
            .unwrap(),
        true
    );
    assert_eq!(
        interpret_default_route_output(true, b"default via 10.12.0.1 metric 100\n", b"",)
            .unwrap(),
        false
    );
    assert!(interpret_default_route_output(false, b"", b"ip route failed").is_err());
}
