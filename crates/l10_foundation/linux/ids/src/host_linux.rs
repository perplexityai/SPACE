//! Linux host primitives used by node-side sandbox networking and process code.
//!
//! These types name kernel-facing identifiers that are otherwise plain
//! integers or strings. Keeping UID, PID, network slot, namespace name, and
//! interface name separate prevents accidental cross-use at syscall and command
//! construction boundaries.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_linux_ids::*;
//! let uid = LinuxUid::new(0);
//! let pid = LinuxPid::try_from_u32(1234)?;
//! let netns = LinuxNetnsName::parse("sbx-1234")?;
//!
//! assert_eq!(uid.get(), 0);
//! assert_eq!(pid.get().get(), 1234);
//! assert_eq!(netns.as_str(), "sbx-1234");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::num::NonZeroU32;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

const MAX_LINUX_NETNS_NAME_BYTES: usize = 64;
const MAX_LINUX_INTERFACE_NAME_BYTES: usize = 15;
pub const MAX_LINUX_NETWORK_SLOT: u32 = 999_999;
pub const LINUX_NETWORK_SLOT_LABEL_WIDTH: usize = 6;

/// Error returned when a Linux host value fails validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// assert!(LinuxPid::try_from_u32(0).is_err());
/// assert!(LinuxNetnsName::parse("bad/name").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct LinuxParseError(&'static str);

/// Linux user ID used for host or guest ownership decisions.
///
/// UID zero is representable because root ownership is a valid Linux state; use
/// a narrower policy type at a boundary that wants to reject it.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let root = LinuxUid::new(0);
/// assert_eq!(root.get(), 0);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LinuxUid(u32);

impl LinuxUid {
    /// Wraps a raw Linux UID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let uid = LinuxUid::new(1000);
    /// assert_eq!(uid.get(), 1000);
    /// ```
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the raw UID value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert_eq!(LinuxUid::new(1000).get(), 1000);
    /// ```
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Non-zero Linux process ID.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let pid = LinuxPid::try_from_u32(1234)?;
/// assert_eq!(pid.get().get(), 1234);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LinuxPid(NonZeroU32);

impl LinuxPid {
    /// Wraps an already validated non-zero process ID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// # use std::num::NonZeroU32;
    /// let raw = NonZeroU32::new(1234).unwrap();
    /// let pid = LinuxPid::new(raw);
    /// assert_eq!(pid.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Parses a Linux PID from a raw `u32`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(LinuxPid::try_from_u32(1).is_ok());
    /// assert!(LinuxPid::try_from_u32(0).is_err());
    /// ```
    pub fn try_from_u32(value: u32) -> Result<Self, LinuxParseError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(LinuxParseError("pid must be greater than zero"))
    }

    /// Returns the non-zero PID value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let pid = LinuxPid::try_from_u32(1234)?;
    /// assert_eq!(pid.get().get(), 1234);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// Stable index for deriving per-sandbox Linux network names.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let slot = LinuxNetworkSlot::try_from_u32(7, 100)?;
/// assert_eq!(slot.get(), 7);
/// assert_eq!(slot.label(), "000007");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LinuxNetworkSlot(u32);

impl LinuxNetworkSlot {
    /// Parses a slot against the configured slot capacity.
    ///
    /// `max_slots` is exclusive, matching node-manager allocation from
    /// `0..max_slots`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(LinuxNetworkSlot::try_from_u32(7, 1000).is_ok());
    /// assert!(LinuxNetworkSlot::try_from_u32(1000, 1000).is_err());
    /// assert!(LinuxNetworkSlot::try_from_u32(0, 0).is_err());
    /// assert!(LinuxNetworkSlot::try_from_u32(1_000_000, 1_000_001).is_err());
    /// ```
    pub fn try_from_u32(value: u32, max_slots: u32) -> Result<Self, LinuxParseError> {
        if max_slots == 0 {
            return Err(LinuxParseError("network slot capacity must be non-zero"));
        }
        if max_slots > MAX_LINUX_NETWORK_SLOT + 1 {
            return Err(LinuxParseError(
                "network slot capacity exceeds the six-digit slot namespace",
            ));
        }
        if value > MAX_LINUX_NETWORK_SLOT {
            return Err(LinuxParseError(
                "network slot exceeds the six-digit slot namespace",
            ));
        }
        if value >= max_slots {
            return Err(LinuxParseError(
                "network slot must be inside configured range",
            ));
        }
        Ok(Self(value))
    }

    /// Returns the raw network slot.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert_eq!(LinuxNetworkSlot::try_from_u32(2, 100)?.get(), 2);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> u32 {
        self.0
    }

    /// Returns the fixed-width slot label used in kernel object names.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let slot = LinuxNetworkSlot::try_from_u32(17, 300)?;
    /// assert_eq!(slot.label(), "000017");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn label(self) -> String {
        format!("{:0width$}", self.0, width = LINUX_NETWORK_SLOT_LABEL_WIDTH)
    }
}

/// Path-safe Linux network namespace name.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let name = LinuxNetnsName::parse("sbx-1234")?;
/// assert_eq!(name.as_str(), "sbx-1234");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LinuxNetnsName(String);

impl LinuxNetnsName {
    /// Parses a path-safe network namespace name.
    ///
    /// Names must be non-empty, at most 64 bytes, and must not contain `/` or
    /// NUL.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(LinuxNetnsName::parse("sbx-1234").is_ok());
    /// assert!(LinuxNetnsName::parse("bad/name").is_err());
    /// assert!(LinuxNetnsName::parse("n".repeat(64)).is_ok());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, LinuxParseError> {
        let value = value.into();
        validate_name(&value, MAX_LINUX_NETNS_NAME_BYTES)?;
        Ok(Self(value))
    }

    /// Returns the namespace name as a string slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let name = LinuxNetnsName::parse("sbx-1234")?;
    /// assert_eq!(name.as_str(), "sbx-1234");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinuxNetnsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<String> for LinuxNetnsName {
    type Error = LinuxParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<LinuxNetnsName> for String {
    fn from(value: LinuxNetnsName) -> Self {
        value.0
    }
}

/// Path-safe Linux network interface name.
///
/// Linux limits interface names to `IFNAMSIZ - 1`, which is 15 bytes. Enforcing
/// that here keeps overlong names from failing later at a syscall boundary.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let name = LinuxInterfaceName::parse("tap0")?;
/// assert_eq!(name.as_str(), "tap0");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LinuxInterfaceName(String);

impl LinuxInterfaceName {
    /// Parses a path-safe Linux interface name.
    ///
    /// Names must be non-empty, at most 15 bytes, and must not contain `/` or
    /// NUL.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(LinuxInterfaceName::parse("tap0").is_ok());
    /// assert!(LinuxInterfaceName::parse("bad/name").is_err());
    /// assert!(LinuxInterfaceName::parse("123456789012345").is_ok());
    /// assert!(LinuxInterfaceName::parse("1234567890123456").is_err());
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, LinuxParseError> {
        let value = value.into();
        validate_name(&value, MAX_LINUX_INTERFACE_NAME_BYTES)?;
        Ok(Self(value))
    }

    /// Returns the interface name as a string slice.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let name = LinuxInterfaceName::parse("tap0")?;
    /// assert_eq!(name.as_str(), "tap0");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for LinuxInterfaceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl TryFrom<String> for LinuxInterfaceName {
    type Error = LinuxParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<LinuxInterfaceName> for String {
    fn from(value: LinuxInterfaceName) -> Self {
        value.0
    }
}

/// Non-zero Linux PID limit for a process tree or sandbox cgroup.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_linux_ids::*;
/// let limit = PidLimit::try_from_u32(256)?;
/// assert_eq!(limit.get().get(), 256);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PidLimit(NonZeroU32);

impl PidLimit {
    /// Wraps an already validated non-zero PID limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// # use std::num::NonZeroU32;
    /// let raw = NonZeroU32::new(256).unwrap();
    /// let limit = PidLimit::new(raw);
    /// assert_eq!(limit.get(), raw);
    /// ```
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Parses a PID limit from a raw `u32`, rejecting zero.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// assert!(PidLimit::try_from_u32(256).is_ok());
    /// assert!(PidLimit::try_from_u32(0).is_err());
    /// ```
    pub fn try_from_u32(value: u32) -> Result<Self, LinuxParseError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(LinuxParseError("pid limit must be greater than zero"))
    }

    /// Returns the non-zero PID limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_linux_ids::*;
    /// let limit = PidLimit::try_from_u32(256)?;
    /// assert_eq!(limit.get().get(), 256);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

fn validate_name(value: &str, max_bytes: usize) -> Result<(), LinuxParseError> {
    if value.is_empty() || value.len() > max_bytes || value.contains('\0') || value.contains('/') {
        return Err(LinuxParseError(
            "linux name must be non-empty and path-safe",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
