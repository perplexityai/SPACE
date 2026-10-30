//! SPACE guest-side runtime contracts.

pub mod linux;

/// Fixed link shared by direct-kernel and DHCP guests.
pub use pplx_space_net_common::guest_link as network;
