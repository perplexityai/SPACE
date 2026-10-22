//! Vsock transport identifiers for host-to-guest communication.
//!
//! Firecracker and guest-agent control paths use vsock CIDs and ports. These
//! wrappers keep the connection identifier and port roles distinct while
//! enforcing the kernel-facing non-zero invariant.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_linux_ids::*;
//! let cid = VsockCid::try_from_u32(3)?;
//! let port = VsockPort::try_from_u32(1024)?;
//!
//! assert_eq!(cid.get().get(), 3);
//! assert_eq!(port.get().get(), 1024);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::num::NonZeroU32;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Error returned when a vsock CID or port is zero.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// assert!(VsockCid::try_from_u32(0).is_err());
/// assert!(VsockPort::try_from_u32(0).is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("vsock value must be greater than zero")]
pub struct VsockParseError;

/// Non-zero vsock context identifier.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let cid = VsockCid::try_from_u32(3)?;
/// assert_eq!(cid.get().get(), 3);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VsockCid(NonZeroU32);

impl VsockCid {
    /// Wraps an already validated non-zero CID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// # use std::num::NonZeroU32;
    /// let raw = NonZeroU32::new(3).unwrap();
    /// let cid = VsockCid::new(raw);
    /// assert_eq!(cid.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Parses a CID from a raw `u32`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(VsockCid::try_from_u32(3).is_ok());
    /// assert!(VsockCid::try_from_u32(0).is_err());
    /// ```
    pub fn try_from_u32(value: u32) -> Result<Self, VsockParseError> {
        NonZeroU32::new(value).map(Self).ok_or(VsockParseError)
    }

    /// Returns the non-zero CID value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let cid = VsockCid::try_from_u32(3)?;
    /// assert_eq!(cid.get().get(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// Non-zero vsock port.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let port = VsockPort::try_from_u32(1024)?;
/// assert_eq!(port.get().get(), 1024);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VsockPort(NonZeroU32);

impl VsockPort {
    /// Wraps an already validated non-zero port.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// # use std::num::NonZeroU32;
    /// let raw = NonZeroU32::new(1024).unwrap();
    /// let port = VsockPort::new(raw);
    /// assert_eq!(port.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Parses a port from a raw `u32`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(VsockPort::try_from_u32(1024).is_ok());
    /// assert!(VsockPort::try_from_u32(0).is_err());
    /// ```
    pub fn try_from_u32(value: u32) -> Result<Self, VsockParseError> {
        NonZeroU32::new(value).map(Self).ok_or(VsockParseError)
    }

    /// Returns the non-zero port value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let port = VsockPort::try_from_u32(1024)?;
    /// assert_eq!(port.get().get(), 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}
