//! Socket option helpers for transparent proxying.
//!
//! Provides `IP_TRANSPARENT` (required for TPROXY), `IP_RECVORIGDSTADDR`
//! (to recover the original destination from ancillary data), and
//! `SO_ORIGINAL_DST` (legacy, for REDIRECT-based setups).

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod other;

#[cfg(target_os = "linux")]
pub use linux::*;
#[cfg(not(target_os = "linux"))]
pub use other::*;
