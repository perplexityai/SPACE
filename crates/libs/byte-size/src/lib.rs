//! Byte-size types with domain roles.
//!
//! `ByteSize` is the shared raw byte-size representation. `MemorySize` and
//! `DiskSize` wrap it so API boundaries cannot accidentally pass disk bytes as
//! memory bytes or vice versa.
//!
//! These are generic validated value types over the `bytesize` crate with
//! nothing SPACE-specific in them, so they live in their own crate rather than
//! in the domain model. `pplx-space-common-core` re-exports these types as
//! `types::size`.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_byte_size::*;
//! let memory = MemorySize::gib(4)?;
//! let disk = DiskSize::gib(20)?;
//!
//! assert_eq!(memory.as_mib_floor(), 4096);
//! assert_eq!(disk.as_mib_floor(), 20 * 1024);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::fmt;
use std::str::FromStr;

/// Raw byte-size type re-exported from the `bytesize` crate.
///
/// Prefer `MemorySize` or `DiskSize` at domain boundaries. Use `ByteSize` for
/// local arithmetic or when no domain role is implied.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_byte_size::*;
/// let raw = ByteSize::mib(512);
/// assert_eq!(raw.as_u64(), 512 * 1024 * 1024);
/// ```
pub use bytesize::ByteSize;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Error returned when a byte-size role wrapper cannot be built.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_byte_size::*;
/// assert_eq!(MemorySize::b(0), Err(SizeParseError::Zero));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SizeParseError {
    /// Size values for role wrappers must be non-zero.
    #[error("size must be greater than zero")]
    Zero,
    /// Multiplying a unit constructor overflowed `u64`.
    #[error("size is too large")]
    Overflow,
    /// Text parsing failed in the underlying `bytesize` parser.
    #[error("invalid size value: {0}")]
    Invalid(String),
    /// A caller-supplied policy cap rejected this size.
    #[error("size exceeds policy limit")]
    ExceedsPolicy,
}

/// Non-zero memory size.
///
/// This wrapper marks bytes as memory rather than disk or arbitrary data.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_byte_size::*;
/// let memory = MemorySize::gib(4)?;
/// assert_eq!(memory.as_mib_floor(), 4096);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MemorySize(ByteSize);

impl MemorySize {
    /// Creates a memory size from bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::b(1024)?;
    /// assert_eq!(memory.as_bytes(), 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn b(value: u64) -> Result<Self, SizeParseError> {
        role_size_b(value).map(Self)
    }

    /// Creates a memory size from kibibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::kib(2)?;
    /// assert_eq!(memory.as_bytes(), 2 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn kib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::KIB).map(Self)
    }

    /// Creates a memory size from mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::mib(512)?;
    /// assert_eq!(memory.as_mib_floor(), 512);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn mib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::MIB).map(Self)
    }

    /// Creates a memory size from gibibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::gib(4)?;
    /// assert_eq!(memory.as_mib_floor(), 4096);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn gib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::GIB).map(Self)
    }

    /// Creates a memory size from tebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::tib(1)?;
    /// assert_eq!(memory.as_mib_floor(), 1024 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn tib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::TIB).map(Self)
    }

    /// Returns the memory size in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::mib(1)?;
    /// assert_eq!(memory.as_bytes(), 1024 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_bytes(self) -> u64 {
        self.0.as_u64()
    }

    /// Returns the memory size rounded down to whole mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::b(3 * 1024 * 1024 + 1)?;
    /// assert_eq!(memory.as_mib_floor(), 3);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_mib_floor(self) -> u64 {
        self.as_bytes() / bytesize::MIB
    }

    /// Returns the memory size rounded up to whole mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let memory = MemorySize::b(3 * 1024 * 1024 + 1)?;
    /// assert_eq!(memory.as_mib_ceil(), 4);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_mib_ceil(self) -> u64 {
        self.as_bytes().div_ceil(bytesize::MIB)
    }

    /// Returns this size only if it is within a caller-supplied policy cap.
    ///
    /// Shared size types do not encode deployment limits directly; placement,
    /// billing, and product code should call this at the policy boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let requested = MemorySize::gib(4)?;
    /// let max = MemorySize::gib(8)?;
    /// assert_eq!(requested.ensure_at_most(max)?, requested);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn ensure_at_most(self, max: Self) -> Result<Self, SizeParseError> {
        if self > max {
            Err(SizeParseError::ExceedsPolicy)
        } else {
            Ok(self)
        }
    }
}

impl TryFrom<u64> for MemorySize {
    type Error = SizeParseError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::b(value)
    }
}

impl From<MemorySize> for u64 {
    fn from(value: MemorySize) -> Self {
        value.as_bytes()
    }
}

impl fmt::Display for MemorySize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MemorySize {
    type Err = SizeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<ByteSize>()
            .map_err(SizeParseError::Invalid)
            .and_then(role_size)
            .map(Self)
    }
}

/// Non-zero disk size.
///
/// This wrapper marks bytes as disk/storage capacity rather than memory or
/// arbitrary data.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_byte_size::*;
/// let disk = DiskSize::gib(20)?;
/// assert_eq!(disk.as_mib_floor(), 20 * 1024);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "u64", into = "u64"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct DiskSize(ByteSize);

impl DiskSize {
    /// Creates a disk size from bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::b(4096)?;
    /// assert_eq!(disk.as_bytes(), 4096);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn b(value: u64) -> Result<Self, SizeParseError> {
        role_size_b(value).map(Self)
    }

    /// Creates a disk size from kibibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::kib(4)?;
    /// assert_eq!(disk.as_bytes(), 4 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn kib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::KIB).map(Self)
    }

    /// Creates a disk size from mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::mib(512)?;
    /// assert_eq!(disk.as_mib_floor(), 512);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn mib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::MIB).map(Self)
    }

    /// Creates a disk size from gibibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::gib(20)?;
    /// assert_eq!(disk.as_mib_floor(), 20 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn gib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::GIB).map(Self)
    }

    /// Creates a disk size from tebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::tib(1)?;
    /// assert_eq!(disk.as_mib_floor(), 1024 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn tib(value: u64) -> Result<Self, SizeParseError> {
        role_size_unit(value, bytesize::TIB).map(Self)
    }

    /// Returns the disk size in bytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::mib(1)?;
    /// assert_eq!(disk.as_bytes(), 1024 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_bytes(self) -> u64 {
        self.0.as_u64()
    }

    /// Returns the disk size rounded down to whole mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::b(5 * 1024 * 1024 + 1)?;
    /// assert_eq!(disk.as_mib_floor(), 5);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_mib_floor(self) -> u64 {
        self.as_bytes() / bytesize::MIB
    }

    /// Returns the disk size rounded up to whole mebibytes.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let disk = DiskSize::b(5 * 1024 * 1024 + 1)?;
    /// assert_eq!(disk.as_mib_ceil(), 6);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn as_mib_ceil(self) -> u64 {
        self.as_bytes().div_ceil(bytesize::MIB)
    }

    /// Returns this size only if it is within a caller-supplied policy cap.
    ///
    /// Shared size types do not encode deployment limits directly; placement,
    /// billing, and product code should call this at the policy boundary.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_byte_size::*;
    /// let requested = DiskSize::gib(20)?;
    /// let max = DiskSize::gib(100)?;
    /// assert_eq!(requested.ensure_at_most(max)?, requested);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn ensure_at_most(self, max: Self) -> Result<Self, SizeParseError> {
        if self > max {
            Err(SizeParseError::ExceedsPolicy)
        } else {
            Ok(self)
        }
    }
}

impl TryFrom<u64> for DiskSize {
    type Error = SizeParseError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::b(value)
    }
}

impl From<DiskSize> for u64 {
    fn from(value: DiskSize) -> Self {
        value.as_bytes()
    }
}

impl fmt::Display for DiskSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DiskSize {
    type Err = SizeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value
            .parse::<ByteSize>()
            .map_err(SizeParseError::Invalid)
            .and_then(role_size)
            .map(Self)
    }
}

fn role_size_b(value: u64) -> Result<ByteSize, SizeParseError> {
    role_size(ByteSize::b(value))
}

fn role_size_unit(value: u64, multiplier: u64) -> Result<ByteSize, SizeParseError> {
    value
        .checked_mul(multiplier)
        .map(ByteSize::b)
        .ok_or(SizeParseError::Overflow)
        .and_then(role_size)
}

fn role_size(value: ByteSize) -> Result<ByteSize, SizeParseError> {
    if value.as_u64() == 0 {
        Err(SizeParseError::Zero)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests;
