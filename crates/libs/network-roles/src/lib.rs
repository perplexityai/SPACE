//! Network role types used at sandbox, host, and overlay boundaries.
//!
//! These wrappers keep otherwise identical primitives from being mixed across
//! network roles. A host-facing port, a guest-facing port, and an overlay peer
//! address can all be encoded as small numbers or IPv4 addresses, but they mean
//! different things when configuring NAT, firewall rules, and guest interfaces.
//!
//! They are their own crate so that code can name them without depending on
//! `pplx-space-common-core`, which re-exports them from `types::network` and
//! `types`.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_network_roles::*;
//! let host = HostPort::try_from_u16(443)?;
//! let guest = GuestPort::try_from_u16(3000)?;
//! let selector = PortSelector::Port(guest);
//!
//! assert_eq!(host.get().get(), 443);
//! assert_eq!(selector, PortSelector::Port(guest));
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU16;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Error returned when a network primitive fails domain validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// assert!(HostPort::try_from_u16(0).is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct NetworkParseError(&'static str);

/// TCP/UDP port exposed on the host side of a sandbox mapping.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// let port = HostPort::try_from_u16(8443)?;
/// assert_eq!(port.get().get(), 8443);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HostPort(NonZeroU16);

impl HostPort {
    /// Wraps an already validated non-zero host port.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::num::NonZeroU16;
    /// let raw = NonZeroU16::new(443).unwrap();
    /// let port = HostPort::new(raw);
    /// assert_eq!(port.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU16) -> Self {
        Self(value)
    }

    /// Parses a host port from a raw `u16`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// assert!(HostPort::try_from_u16(1).is_ok());
    /// assert!(HostPort::try_from_u16(0).is_err());
    /// ```
    pub fn try_from_u16(value: u16) -> Result<Self, NetworkParseError> {
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(NetworkParseError("port must be greater than zero"))
    }

    /// Returns the non-zero host port value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// let port = HostPort::try_from_u16(8080)?;
    /// assert_eq!(port.get().get(), 8080);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU16 {
        self.0
    }
}

/// TCP/UDP port inside the guest VM.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// let port = GuestPort::try_from_u16(3000)?;
/// assert_eq!(port.get().get(), 3000);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GuestPort(NonZeroU16);

impl GuestPort {
    /// Wraps an already validated non-zero guest port.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::num::NonZeroU16;
    /// let raw = NonZeroU16::new(22).unwrap();
    /// let port = GuestPort::new(raw);
    /// assert_eq!(port.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU16) -> Self {
        Self(value)
    }

    /// Parses a guest port from a raw `u16`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// assert!(GuestPort::try_from_u16(22).is_ok());
    /// assert!(GuestPort::try_from_u16(0).is_err());
    /// ```
    pub fn try_from_u16(value: u16) -> Result<Self, NetworkParseError> {
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(NetworkParseError("port must be greater than zero"))
    }

    /// Returns the non-zero guest port value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// let port = GuestPort::try_from_u16(8080)?;
    /// assert_eq!(port.get().get(), 8080);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU16 {
        self.0
    }
}

/// Selector for either all guest ports or one specific guest port.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// let ssh = GuestPort::try_from_u16(22)?;
/// assert_eq!(PortSelector::Port(ssh), PortSelector::Port(ssh));
/// assert_eq!(PortSelector::All, PortSelector::All);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum PortSelector {
    All,
    Port(GuestPort),
}

/// Canonical IPv4 network and prefix pair.
///
/// The constructor masks host bits so equivalent networks compare and hash the
/// same way.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// # use std::net::Ipv4Addr;
/// let cidr = Ipv4Cidr::new(Ipv4Addr::new(192, 168, 1, 42), 24)?;
/// assert_eq!(cidr.network(), Ipv4Addr::new(192, 168, 1, 0));
/// assert_eq!(cidr.to_string(), "192.168.1.0/24");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Ipv4Cidr {
    network: Ipv4Addr,
    prefix: u8,
}

impl Ipv4Cidr {
    /// Builds a canonical IPv4 CIDR, masking host bits in `network`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let cidr = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 99), 24)?;
    /// assert_eq!(cidr.network(), Ipv4Addr::new(10, 0, 0, 0));
    /// assert!(Ipv4Cidr::new(Ipv4Addr::LOCALHOST, 33).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(network: Ipv4Addr, prefix: u8) -> Result<Self, NetworkParseError> {
        if prefix > 32 {
            return Err(NetworkParseError("IPv4 CIDR prefix must be <= 32"));
        }
        let network = mask_ipv4_network(network, prefix);
        Ok(Self { network, prefix })
    }

    /// Returns the canonical network address.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let cidr = Ipv4Cidr::new(Ipv4Addr::new(10, 1, 2, 3), 16)?;
    /// assert_eq!(cidr.network(), Ipv4Addr::new(10, 1, 0, 0));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn network(self) -> Ipv4Addr {
        self.network
    }

    /// Returns the CIDR prefix length.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let cidr = Ipv4Cidr::new(Ipv4Addr::new(10, 0, 0, 0), 8)?;
    /// assert_eq!(cidr.prefix(), 8);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn prefix(self) -> u8 {
        self.prefix
    }

    /// Returns true when `addr` is inside this CIDR.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let cidr = Ipv4Cidr::new(Ipv4Addr::new(10, 12, 0, 0), 16)?;
    /// assert!(cidr.contains(Ipv4Addr::new(10, 12, 1, 7)));
    /// assert!(!cidr.contains(Ipv4Addr::new(10, 13, 1, 7)));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn contains(self, addr: Ipv4Addr) -> bool {
        mask_ipv4_network(addr, self.prefix) == self.network
    }

    /// Returns the broadcast address for this IPv4 CIDR.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let cidr = Ipv4Cidr::new(Ipv4Addr::new(192, 168, 1, 0), 24)?;
    /// assert_eq!(cidr.broadcast(), Ipv4Addr::new(192, 168, 1, 255));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn broadcast(self) -> Ipv4Addr {
        let host_bits = 32 - self.prefix;
        let host_mask = if host_bits == 32 {
            u32::MAX
        } else {
            (1_u32 << host_bits) - 1
        };
        Ipv4Addr::from(u32::from(self.network) | host_mask)
    }
}

fn mask_ipv4_network(addr: Ipv4Addr, prefix: u8) -> Ipv4Addr {
    if prefix == 0 {
        return Ipv4Addr::UNSPECIFIED;
    }

    let mask = u32::MAX << (32 - prefix);
    Ipv4Addr::from(u32::from(addr) & mask)
}

fn subnet_contains_assignable(subnet: Ipv4Cidr, addr: Ipv4Addr) -> bool {
    subnet.contains(addr) && addr != subnet.network() && addr != subnet.broadcast()
}

impl fmt::Display for Ipv4Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

impl FromStr for Ipv4Cidr {
    type Err = NetworkParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (addr, prefix) = value
            .split_once('/')
            .ok_or(NetworkParseError("IPv4 CIDR must contain '/'"))?;
        let addr = addr
            .parse()
            .map_err(|_| NetworkParseError("invalid IPv4 address"))?;
        let prefix = prefix
            .parse()
            .map_err(|_| NetworkParseError("invalid IPv4 prefix"))?;
        Self::new(addr, prefix)
    }
}

/// Canonical IPv6 network and prefix pair.
///
/// The constructor masks host bits so equivalent networks compare and hash the
/// same way.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Ipv6Cidr {
    network: Ipv6Addr,
    prefix: u8,
}

impl Ipv6Cidr {
    pub fn new(network: Ipv6Addr, prefix: u8) -> Result<Self, NetworkParseError> {
        if prefix > 128 {
            return Err(NetworkParseError("IPv6 CIDR prefix must be <= 128"));
        }
        let network = mask_ipv6_network(network, prefix);
        Ok(Self { network, prefix })
    }

    pub const fn network(self) -> Ipv6Addr {
        self.network
    }

    pub const fn prefix(self) -> u8 {
        self.prefix
    }

    pub fn contains(self, addr: Ipv6Addr) -> bool {
        mask_ipv6_network(addr, self.prefix) == self.network
    }
}

fn mask_ipv6_network(addr: Ipv6Addr, prefix: u8) -> Ipv6Addr {
    if prefix == 0 {
        return Ipv6Addr::UNSPECIFIED;
    }

    let mask = u128::MAX << (128 - prefix);
    Ipv6Addr::from(u128::from(addr) & mask)
}

impl fmt::Display for Ipv6Cidr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix)
    }
}

impl FromStr for Ipv6Cidr {
    type Err = NetworkParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (addr, prefix) = value
            .split_once('/')
            .ok_or(NetworkParseError("IPv6 CIDR must contain '/'"))?;
        let addr = addr
            .parse()
            .map_err(|_| NetworkParseError("invalid IPv6 address"))?;
        let prefix = prefix
            .parse()
            .map_err(|_| NetworkParseError("invalid IPv6 prefix"))?;
        Self::new(addr, prefix)
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum IpCidr {
    V4(Ipv4Cidr),
    V6(Ipv6Cidr),
}

impl IpCidr {
    pub const fn prefix(self) -> u8 {
        match self {
            Self::V4(cidr) => cidr.prefix(),
            Self::V6(cidr) => cidr.prefix(),
        }
    }

    pub fn contains(self, addr: IpAddr) -> bool {
        match (self, addr) {
            (Self::V4(cidr), IpAddr::V4(addr)) => cidr.contains(addr),
            (Self::V6(cidr), IpAddr::V6(addr)) => cidr.contains(addr),
            _ => false,
        }
    }
}

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum AddressFamilyMode {
    Ipv4Only,
    Ipv6Only,
    DualStack,
}

impl AddressFamilyMode {
    pub const fn has_ipv4(self) -> bool {
        matches!(self, Self::Ipv4Only | Self::DualStack)
    }

    pub const fn has_ipv6(self) -> bool {
        matches!(self, Self::Ipv6Only | Self::DualStack)
    }
}

/// Six-octet MAC address used for guest or tap device configuration.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// let mac = MacAddress::new([0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
/// assert_eq!(mac.to_string(), "02:00:00:00:00:01");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MacAddress([u8; 6]);

impl MacAddress {
    /// Wraps six MAC address octets.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// let mac = MacAddress::new([0x02, 0, 0, 0, 0, 1]);
    /// assert_eq!(mac.octets(), [0x02, 0, 0, 0, 0, 1]);
    /// ```
    pub const fn new(value: [u8; 6]) -> Self {
        Self(value)
    }

    /// Returns the raw MAC address octets.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// let mac = MacAddress::new([0x02, 0, 0, 0, 0, 1]);
    /// assert_eq!(mac.octets()[0], 0x02);
    /// ```
    pub const fn octets(self) -> [u8; 6] {
        self.0
    }
}

impl fmt::Display for MacAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d, e, g] = self.0;
        write!(f, "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{g:02x}")
    }
}

impl FromStr for MacAddress {
    type Err = NetworkParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut octets = [0_u8; 6];
        let mut parts = value.split(':');

        for octet in &mut octets {
            let part = parts
                .next()
                .ok_or(NetworkParseError("MAC address must have 6 octets"))?;
            if part.len() != 2 || !part.chars().all(|ch| ch.is_ascii_hexdigit()) {
                return Err(NetworkParseError(
                    "MAC address octets must be two hex digits",
                ));
            }
            *octet = u8::from_str_radix(part, 16)
                .map_err(|_| NetworkParseError("invalid MAC address octet"))?;
        }

        if parts.next().is_some() {
            return Err(NetworkParseError("MAC address must have 6 octets"));
        }

        Ok(Self(octets))
    }
}

impl TryFrom<String> for MacAddress {
    type Error = NetworkParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<MacAddress> for String {
    fn from(value: MacAddress) -> Self {
        value.to_string()
    }
}

/// IPv4 address assigned to a guest.
///
/// Use [`GuestIpAddr::try_in_subnet`] at config or allocation boundaries when
/// the configured guest subnet is available.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// # use std::net::Ipv4Addr;
/// let subnet = Ipv4Cidr::new(Ipv4Addr::new(172, 16, 0, 0), 16)?;
/// let ip = GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 16, 0, 2), subnet)?;
/// assert_eq!(ip.to_string(), "172.16.0.2");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GuestIpAddr(Ipv4Addr);

impl GuestIpAddr {
    /// Builds a guest IP after checking it belongs to the configured guest subnet.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let subnet = Ipv4Cidr::new(Ipv4Addr::new(172, 16, 0, 0), 16)?;
    /// assert!(GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 16, 0, 2), subnet).is_ok());
    /// assert!(GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 16, 0, 0), subnet).is_err());
    /// assert!(GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 17, 0, 2), subnet).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn try_in_subnet(value: Ipv4Addr, subnet: Ipv4Cidr) -> Result<Self, NetworkParseError> {
        if subnet_contains_assignable(subnet, value) {
            Ok(Self(value))
        } else {
            Err(NetworkParseError(
                "guest IP must be an assignable address inside guest subnet",
            ))
        }
    }

    /// Returns the raw IPv4 address.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let subnet = Ipv4Cidr::new(Ipv4Addr::new(172, 16, 0, 0), 16)?;
    /// let ip = GuestIpAddr::try_in_subnet(Ipv4Addr::new(172, 16, 0, 2), subnet)?;
    /// assert_eq!(ip.get(), Ipv4Addr::new(172, 16, 0, 2));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> Ipv4Addr {
        self.0
    }
}

impl fmt::Display for GuestIpAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// IPv4 address used to reach another overlay peer.
///
/// Use [`OverlayPeerIp::try_in_subnet`] at config or allocation boundaries when
/// the configured overlay subnet is available.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_network_roles::*;
/// # use std::net::Ipv4Addr;
/// let subnet = Ipv4Cidr::new(Ipv4Addr::new(10, 10, 0, 0), 16)?;
/// let peer = OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 10, 0, 7), subnet)?;
/// assert_eq!(peer.to_string(), "10.10.0.7");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct OverlayPeerIp(Ipv4Addr);

impl OverlayPeerIp {
    /// Builds an overlay peer IP after checking it belongs to the overlay subnet.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let subnet = Ipv4Cidr::new(Ipv4Addr::new(10, 10, 0, 0), 16)?;
    /// assert!(OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 10, 0, 7), subnet).is_ok());
    /// assert!(OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 10, 0, 0), subnet).is_err());
    /// assert!(OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 11, 0, 7), subnet).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn try_in_subnet(value: Ipv4Addr, subnet: Ipv4Cidr) -> Result<Self, NetworkParseError> {
        if subnet_contains_assignable(subnet, value) {
            Ok(Self(value))
        } else {
            Err(NetworkParseError(
                "overlay peer IP must be an assignable address inside overlay subnet",
            ))
        }
    }

    /// Returns the raw IPv4 address.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_network_roles::*;
    /// # use std::net::Ipv4Addr;
    /// let subnet = Ipv4Cidr::new(Ipv4Addr::new(10, 10, 0, 0), 16)?;
    /// let peer = OverlayPeerIp::try_in_subnet(Ipv4Addr::new(10, 10, 0, 7), subnet)?;
    /// assert_eq!(peer.get(), Ipv4Addr::new(10, 10, 0, 7));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> Ipv4Addr {
        self.0
    }
}

impl fmt::Display for OverlayPeerIp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests;
