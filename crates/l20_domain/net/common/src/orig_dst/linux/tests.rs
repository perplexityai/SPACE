use super::*;
use std::net::Ipv4Addr;

#[test]
fn converts_original_destination_sockaddr() {
    let sock = SockaddrIn::from(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), 5353));
    let parsed = sockaddr_in_to_v4(sock.into()).expect("valid sockaddr");

    assert_eq!(parsed.ip(), &Ipv4Addr::new(203, 0, 113, 7));
    assert_eq!(parsed.port(), 5353);
}

#[test]
fn rejects_non_ipv4_sockaddr() {
    let mut raw: libc::sockaddr_in =
        SockaddrIn::from(SocketAddrV4::new(Ipv4Addr::new(203, 0, 113, 7), 5353)).into();
    raw.sin_family = libc::AF_UNIX as libc::sa_family_t;

    assert!(sockaddr_in_to_v4(raw).is_none());
}
