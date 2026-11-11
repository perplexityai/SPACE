//! Compute, storage, and sandbox resource requirements.
//!
//! These types keep resource requests role-aware: a CPU count is not a byte
//! size, sandbox storage can use the platform default, and template build
//! resources require an explicit disk size.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_execution_spec::*;
//! # use pplx_space_byte_size::{DiskSize, MemorySize};
//! let compute = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
//! let requirements = SandboxRequirements::new(
//!     compute,
//!     StorageRequest::Default,
//!     StartupPolicy::PreferBaseSnapshot,
//!     RuntimeCapabilities::full_node(),
//! );
//!
//! assert_eq!(requirements.compute().memory().as_mib_floor(), 4096);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use std::num::NonZeroU32;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::runtime::{RuntimeCapabilities, StartupPolicy};
use super::size::{DiskSize, MemorySize, SizeParseError};

/// Error returned when compute resources fail validation.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// assert_eq!(VcpuCount::try_from_u32(0), Err(ComputeParseError::Zero));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum ComputeParseError {
    /// A required positive value was zero.
    #[error("value must be greater than zero")]
    Zero,
    /// Size parsing or validation failed.
    #[error(transparent)]
    Size(#[from] SizeParseError),
}

/// Non-zero vCPU count.
///
/// Use this when code specifically needs the count rather than the broader
/// `CpuLimit` role wrapper.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let count = VcpuCount::try_from_u32(2)?;
/// assert_eq!(count.get().get(), 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct VcpuCount(NonZeroU32);

impl VcpuCount {
    /// Wraps a known non-zero vCPU count.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use std::num::NonZeroU32;
    /// # use pplx_space_execution_spec::*;
    /// let count = VcpuCount::new(NonZeroU32::new(2).unwrap());
    /// assert_eq!(count.get().get(), 2);
    /// ```
    pub const fn new(value: NonZeroU32) -> Self {
        Self(value)
    }

    /// Parses a `u32` into a non-zero vCPU count.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let count = VcpuCount::try_from_u32(4)?;
    /// assert_eq!(count.get().get(), 4);
    /// assert!(VcpuCount::try_from_u32(0).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn try_from_u32(value: u32) -> Result<Self, ComputeParseError> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(ComputeParseError::Zero)
    }

    /// Returns the underlying non-zero count.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let count = VcpuCount::try_from_u32(1)?;
    /// assert_eq!(count.get().get(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn get(self) -> NonZeroU32 {
        self.0
    }
}

/// CPU limit requested for a sandbox or build.
///
/// This enum leaves room for future CPU limit styles while callers today can
/// express the common "N vCPUs" request.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let cpu = CpuLimit::vcpu(2)?;
/// assert_eq!(cpu, CpuLimit::Vcpu(VcpuCount::try_from_u32(2)?));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum CpuLimit {
    /// Fixed vCPU count.
    Vcpu(VcpuCount),
}

impl CpuLimit {
    /// Builds a fixed-vCPU CPU limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let cpu = CpuLimit::vcpu(2)?;
    /// assert!(matches!(cpu, CpuLimit::Vcpu(_)));
    /// assert!(CpuLimit::vcpu(0).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn vcpu(value: u32) -> Result<Self, ComputeParseError> {
        VcpuCount::try_from_u32(value).map(Self::Vcpu)
    }
}

/// CPU and memory shape for sandbox execution.
///
/// Disk is intentionally separate because snapshots and sandbox creation have
/// different disk semantics.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// # use pplx_space_byte_size::{DiskSize, MemorySize};
/// let shape = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
/// assert_eq!(shape.memory().as_mib_floor(), 4096);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ComputeShape {
    cpu: CpuLimit,
    memory: MemorySize,
}

impl ComputeShape {
    /// Creates a compute shape from typed CPU and memory values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let shape = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?);
    /// assert_eq!(shape.memory().as_mib_floor(), 512);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn new(cpu: CpuLimit, memory: MemorySize) -> Self {
        Self { cpu, memory }
    }

    /// Returns the CPU limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let shape = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?);
    /// assert!(matches!(shape.cpu(), CpuLimit::Vcpu(_)));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn cpu(self) -> CpuLimit {
        self.cpu
    }

    /// Returns the memory size.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let shape = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?);
    /// assert_eq!(shape.memory().as_bytes(), 512 * 1024 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn memory(self) -> MemorySize {
        self.memory
    }
}

/// Disk request for sandbox creation.
///
/// `Default` means the caller accepts the service default. `Bytes` means the
/// caller supplied an explicit disk size.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let default = StorageRequest::from_optional_disk(None)?;
/// let explicit = StorageRequest::from_optional_disk(Some(1024))?;
/// assert_eq!(default, StorageRequest::Default);
/// assert!(matches!(explicit, StorageRequest::Bytes(_)));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum StorageRequest {
    /// Use the service default disk size.
    #[default]
    Default,
    /// Use an explicit disk size.
    Bytes(DiskSize),
}

impl StorageRequest {
    /// Converts an optional byte count into a storage request.
    ///
    /// `None` becomes `Default`; `Some(0)` is rejected through `DiskSize`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// assert_eq!(StorageRequest::from_optional_disk(None)?, StorageRequest::Default);
    /// assert!(StorageRequest::from_optional_disk(Some(0)).is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_optional_disk(value: Option<u64>) -> Result<Self, ComputeParseError> {
        value
            .map(|bytes| DiskSize::b(bytes).map(Self::Bytes))
            .transpose()
            .map_err(Into::into)
            .map(Option::unwrap_or_default)
    }
}

/// Complete resource and startup requirements for sandbox placement.
///
/// This is the handoff type between request parsing and runtime placement.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// # use pplx_space_byte_size::{DiskSize, MemorySize};
/// let requirements = SandboxRequirements::new(
///     ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?),
///     StorageRequest::Default,
///     StartupPolicy::PreferBaseSnapshot,
///     RuntimeCapabilities::full_node(),
/// );
///
/// assert_eq!(requirements.startup(), StartupPolicy::PreferBaseSnapshot);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct SandboxRequirements {
    compute: ComputeShape,
    storage: StorageRequest,
    startup: StartupPolicy,
    required_capabilities: RuntimeCapabilities,
}

impl SandboxRequirements {
    /// Creates sandbox placement requirements from typed resource values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let compute = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?);
    /// let requirements = SandboxRequirements::new(
    ///     compute,
    ///     StorageRequest::Default,
    ///     StartupPolicy::ColdOnly,
    ///     RuntimeCapabilities::default(),
    /// );
    /// assert_eq!(requirements.compute(), compute);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn new(
        compute: ComputeShape,
        storage: StorageRequest,
        startup: StartupPolicy,
        required_capabilities: RuntimeCapabilities,
    ) -> Self {
        Self {
            compute,
            storage,
            startup,
            required_capabilities,
        }
    }

    /// Returns the compute shape.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let compute = ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?);
    /// let requirements = SandboxRequirements::new(
    ///     compute,
    ///     StorageRequest::Default,
    ///     StartupPolicy::ColdOnly,
    ///     RuntimeCapabilities::default(),
    /// );
    /// assert_eq!(requirements.compute(), compute);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn compute(self) -> ComputeShape {
        self.compute
    }

    /// Returns the storage request.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let requirements = SandboxRequirements::new(
    ///     ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?),
    ///     StorageRequest::Default,
    ///     StartupPolicy::ColdOnly,
    ///     RuntimeCapabilities::default(),
    /// );
    /// assert_eq!(requirements.storage(), StorageRequest::Default);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn storage(self) -> StorageRequest {
        self.storage
    }

    /// Returns the requested startup policy.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let requirements = SandboxRequirements::new(
    ///     ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?),
    ///     StorageRequest::Default,
    ///     StartupPolicy::RequireBaseSnapshot,
    ///     RuntimeCapabilities::default(),
    /// );
    /// assert_eq!(requirements.startup(), StartupPolicy::RequireBaseSnapshot);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn startup(self) -> StartupPolicy {
        self.startup
    }

    /// Returns the runtime capabilities required by the caller.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let required_capabilities = RuntimeCapabilities {
    ///     snapshot: true,
    ///     ..RuntimeCapabilities::default()
    /// };
    /// let requirements = SandboxRequirements::new(
    ///     ComputeShape::new(CpuLimit::vcpu(1)?, MemorySize::mib(512)?),
    ///     StorageRequest::Default,
    ///     StartupPolicy::ColdOnly,
    ///     required_capabilities,
    /// );
    /// assert!(requirements.required_capabilities().snapshot);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn required_capabilities(self) -> RuntimeCapabilities {
        self.required_capabilities
    }
}

/// Resources used to build a template.
///
/// Template builds require an explicit disk size, unlike sandbox startup where
/// storage can use the service default.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// # use pplx_space_byte_size::{DiskSize, MemorySize};
/// let build = TemplateBuildResources::new(
///     ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?),
///     DiskSize::gib(20)?,
/// );
///
/// assert_eq!(build.disk().as_mib_floor(), 20 * 1024);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct TemplateBuildResources {
    compute: ComputeShape,
    disk: DiskSize,
}

impl TemplateBuildResources {
    /// Creates template build resources from compute and disk values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let compute = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
    /// let resources = TemplateBuildResources::new(compute, DiskSize::gib(10)?);
    /// assert_eq!(resources.compute(), compute);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn new(compute: ComputeShape, disk: DiskSize) -> Self {
        Self { compute, disk }
    }

    /// Returns the compute shape for the build.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let compute = ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?);
    /// let resources = TemplateBuildResources::new(compute, DiskSize::gib(10)?);
    /// assert_eq!(resources.compute(), compute);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn compute(self) -> ComputeShape {
        self.compute
    }

    /// Returns the disk size for the build.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// # use pplx_space_byte_size::{DiskSize, MemorySize};
    /// let resources = TemplateBuildResources::new(
    ///     ComputeShape::new(CpuLimit::vcpu(2)?, MemorySize::gib(4)?),
    ///     DiskSize::gib(10)?,
    /// );
    /// assert_eq!(resources.disk().as_mib_floor(), 10 * 1024);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub const fn disk(self) -> DiskSize {
        self.disk
    }
}

#[cfg(test)]
mod tests;
