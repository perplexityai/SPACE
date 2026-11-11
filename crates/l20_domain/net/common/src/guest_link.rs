//! Fixed execution-local link, shared by kernel configuration and DHCP.
use std::net::Ipv4Addr;

// Keep legacy string consumers and typed network code on the same addresses.
macro_rules! address {
    ($typed:ident, $text:ident, $a:literal, $b:literal, $c:literal, $d:literal $(; $cidr:ident, $prefix:literal)?) => {
        pub const $typed: Ipv4Addr = Ipv4Addr::new($a, $b, $c, $d);
        pub const $text: &str = concat!(
            stringify!($a),
            ".",
            stringify!($b),
            ".",
            stringify!($c),
            ".",
            stringify!($d)
        );
        $(pub const $cidr: &str = concat!(stringify!($a), ".", stringify!($b), ".", stringify!($c), ".", stringify!($d), "/", stringify!($prefix));)?
    };
}
address!(GUEST, IPV4_ADDR, 169, 254, 0, 21);
address!(GATEWAY, GATEWAY_IPV4_ADDR, 169, 254, 0, 22; GATEWAY_IPV4_CIDR, 30);
address!(NETMASK, IPV4_NETMASK, 255, 255, 255, 252);
