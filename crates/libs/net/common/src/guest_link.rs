//! Fixed execution-local link, shared by kernel configuration and DHCP.
use std::net::Ipv4Addr;
pub const GUEST: Ipv4Addr = Ipv4Addr::new(169, 254, 0, 21);
pub const GATEWAY: Ipv4Addr = Ipv4Addr::new(169, 254, 0, 22);
pub const NETMASK: Ipv4Addr = Ipv4Addr::new(255, 255, 255, 252);
