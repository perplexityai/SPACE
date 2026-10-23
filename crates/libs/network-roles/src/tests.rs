use super::{
    AddressFamilyMode, GuestIpAddr, IpCidr, Ipv4Cidr, Ipv6Cidr, MacAddress, OverlayPeerIp,
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;

#[test]
fn ipv4_cidr_masks_host_bits() {
    let cidr = Ipv4Cidr::new(Ipv4Addr::new(192, 168, 1, 42), 24).unwrap();
    assert_eq!(cidr.network(), Ipv4Addr::new(192, 168, 1, 0));
    assert_eq!(cidr.to_string(), "192.168.1.0/24");
}

#[test]
fn ipv4_cidr_prefix_zero_masks_to_unspecified() {
    let cidr = Ipv4Cidr::new(Ipv4Addr::new(192, 168, 1, 42), 0).unwrap();
    assert_eq!(cidr.network(), Ipv4Addr::UNSPECIFIED);
    assert_eq!(cidr.to_string(), "0.0.0.0/0");
}

#[test]
fn ipv4_cidr_from_str_canonicalizes_network() {
    let cidr = Ipv4Cidr::from_str("192.168.1.42/24").unwrap();
    assert_eq!(cidr.network(), Ipv4Addr::new(192, 168, 1, 0));
}

#[test]
fn ipv6_cidr_masks_host_bits() {
    let cidr = Ipv6Cidr::from_str("fd00:1234::dead:beef/64").unwrap();
    assert_eq!(cidr.network(), Ipv6Addr::from_str("fd00:1234::").unwrap());
    assert_eq!(cidr.to_string(), "fd00:1234::/64");
    assert!(cidr.contains(Ipv6Addr::from_str("fd00:1234::1").unwrap()));
    assert!(!cidr.contains(Ipv6Addr::from_str("fd00:1235::1").unwrap()));
}

#[test]
fn address_family_mode_reports_enabled_planes() {
    assert!(AddressFamilyMode::Ipv4Only.has_ipv4());
    assert!(!AddressFamilyMode::Ipv4Only.has_ipv6());
    assert!(AddressFamilyMode::DualStack.has_ipv4());
    assert!(AddressFamilyMode::DualStack.has_ipv6());

    let cidr = IpCidr::V4(Ipv4Cidr::from_str("10.0.0.0/8").unwrap());
    assert!(cidr.contains(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3))));
    assert!(!cidr.contains(IpAddr::V6(Ipv6Addr::LOCALHOST)));
}

#[test]
fn ipv4_cidr_contains_masks_candidate_host_bits() {
    let cidr = Ipv4Cidr::new(Ipv4Addr::new(10, 12, 0, 0), 16).unwrap();
    assert!(cidr.contains(Ipv4Addr::new(10, 12, 255, 255)));
    assert!(!cidr.contains(Ipv4Addr::new(10, 13, 0, 1)));
    assert_eq!(cidr.broadcast(), Ipv4Addr::new(10, 12, 255, 255));
}

#[test]
fn role_ip_constructors_check_assignable_configured_subnets() {
    let guest = Ipv4Cidr::new(Ipv4Addr::new(172, 16, 0, 0), 16).unwrap();
    let overlay = Ipv4Cidr::new(Ipv4Addr::new(10, 10, 0, 0), 16).unwrap();

    GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 16, 1, 2), guest).unwrap();
    GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 17, 1, 2), guest).unwrap_err();
    GuestIpAddr::try_in_subnet(guest.network(), guest).unwrap_err();
    GuestIpAddr::try_in_subnet(guest.broadcast(), guest).unwrap_err();
    OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 10, 1, 2), overlay).unwrap();
    OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 11, 1, 2), overlay).unwrap_err();
    OverlayPeerIp::try_in_subnet(overlay.network(), overlay).unwrap_err();
    OverlayPeerIp::try_in_subnet(overlay.broadcast(), overlay).unwrap_err();
}

#[test]
fn mac_address_parses_canonical_colon_hex() {
    let mac = MacAddress::from_str("02:AB:00:00:00:01").unwrap();
    assert_eq!(mac.octets(), [0x02, 0xab, 0, 0, 0, 1]);
    assert_eq!(mac.to_string(), "02:ab:00:00:00:01");

    MacAddress::from_str("02:ab:00:00:00").unwrap_err();
    MacAddress::from_str("02:ab:00:00:00:01:02").unwrap_err();
    MacAddress::from_str("02-ab-00-00-00-01").unwrap_err();
    MacAddress::from_str("02:ab:00:00:00:zz").unwrap_err();
}
