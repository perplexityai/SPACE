//! Host CPU identity for VM-state portability.
//!
//! A Firecracker full snapshot carries the vCPU register state, including
//! model-specific registers (MSRs), of the host it was captured on. KVM refuses
//! to load that state on a host whose CPU doesn't implement the same MSRs, and
//! Firecracker reports it as `Failed to restore vCPUs: Failed to set all KVM
//! MSRs for this vCPU`. One EC2 instance type can be backed by more than one
//! CPU generation (m5d.metal runs on both Skylake and Cascade Lake), so the
//! instance type doesn't identify the CPU. [`HostCpu`] does.
//!
//! The identity is the processor's vendor, family, model, and stepping, read
//! from `/proc/cpuinfo` and rendered as a Kubernetes label value so the same
//! string can be recorded on a snapshot and published on a node. Microcode
//! revision is deliberately left out: it changes on routine host updates and
//! isn't what distinguishes the generations whose MSR sets differ.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_execution_spec::HostCpu;
//! let cpuinfo = "processor\t: 0\n\
//!                vendor_id\t: GenuineIntel\n\
//!                cpu family\t: 6\n\
//!                model\t\t: 85\n\
//!                stepping\t: 7\n";
//! let cpu = HostCpu::from_cpuinfo(cpuinfo).unwrap();
//! assert_eq!(cpu.as_str(), "genuineintel-6-85-7");
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use super::{LabelValue, LabelValueParseError};

/// Error returned when a host CPU identity isn't a non-empty Kubernetes label
/// value.
///
/// ```rust
/// # use pplx_space_execution_spec::HostCpu;
/// assert!(HostCpu::parse("").is_err());
/// assert!(HostCpu::parse("Intel(R) Xeon(R)").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum HostCpuParseError {
    /// The value was empty.
    #[error("host CPU identity must not be empty")]
    Empty,
    /// The value was not a valid Kubernetes label value.
    #[error(transparent)]
    InvalidLabelValue(#[from] LabelValueParseError),
}

/// CPU identity of the host that captured or will load VM state.
///
/// [`HostCpuCompatibility::can_load`] treats equal identities as compatible,
/// and unequal ones as not unless a [`HostCpuCompatibility`] lists the pair. Neither is a
/// guarantee. Unequal identities may be compatible in one direction: a CPU
/// whose MSR set is a superset of another's can load that one's state, but
/// not the reverse. And the MSRs KVM exposes
/// also depend on the host kernel and microcode, which this identity leaves
/// out, so equal identities aren't proof of compatibility either.
///
/// ```rust
/// # use pplx_space_execution_spec::HostCpu;
/// let cpu = HostCpu::parse("genuineintel-6-85-4").unwrap();
/// assert_eq!(cpu.to_string(), "genuineintel-6-85-4");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct HostCpu(LabelValue);

impl HostCpu {
    /// Parses a recorded or published host CPU identity: a non-empty label
    /// value (see [`LabelValue`]; the grammar isn't re-implemented here).
    ///
    /// The identity is case-insensitive and stored lowercase, the form
    /// [`HostCpu::from_cpuinfo`] produces, so an operator-written
    /// `GenuineIntel-6-85-4` still matches a host's `genuineintel-6-85-4`.
    pub fn parse(value: impl Into<String>) -> Result<Self, HostCpuParseError> {
        let mut value = value.into();
        value.make_ascii_lowercase();
        if value.is_empty() {
            return Err(HostCpuParseError::Empty);
        }
        Ok(Self(LabelValue::parse(value)?))
    }

    /// Derives the identity from the text of `/proc/cpuinfo`.
    ///
    /// Reads the first processor block. x86 hosts yield
    /// `<vendor_id>-<cpu family>-<model>-<stepping>` with the vendor
    /// lowercased; Arm hosts yield
    /// `arm-<implementer>-<part>-<variant>-<revision>`, the first three in
    /// lowercase hex without the `0x` prefix and the revision in decimal.
    /// Returns `None` when neither field set is complete or the result isn't a
    /// valid label value, so an unrecognized host is reported as unknown
    /// rather than as a guess.
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::HostCpu;
    /// let cpuinfo = "processor\t: 0\n\
    ///                CPU implementer\t: 0x41\n\
    ///                CPU architecture: 8\n\
    ///                CPU variant\t: 0x1\n\
    ///                CPU part\t: 0xd0c\n\
    ///                CPU revision\t: 1\n";
    /// assert_eq!(HostCpu::from_cpuinfo(cpuinfo).unwrap().as_str(), "arm-41-d0c-1-1");
    /// assert!(HostCpu::from_cpuinfo("processor\t: 0\n").is_none());
    /// ```
    pub fn from_cpuinfo(cpuinfo: &str) -> Option<Self> {
        let first_block = cpuinfo
            .split("\n\n")
            .find(|block| !block.trim().is_empty())?;
        let field = |name: &str| {
            first_block.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                (key.trim() == name).then(|| value.trim())
            })
        };
        let decimal = |name: &str| field(name)?.parse::<u32>().ok();
        let hex = |name: &str| {
            let raw = field(name)?;
            u32::from_str_radix(raw.strip_prefix("0x").unwrap_or(raw), 16).ok()
        };

        let identity = if let Some(vendor) = field("vendor_id") {
            format!(
                "{}-{}-{}-{}",
                vendor.to_ascii_lowercase(),
                decimal("cpu family")?,
                decimal("model")?,
                decimal("stepping")?,
            )
        } else {
            format!(
                "arm-{:x}-{:x}-{:x}-{}",
                hex("CPU implementer")?,
                hex("CPU part")?,
                hex("CPU variant")?,
                decimal("CPU revision")?,
            )
        };
        Self::parse(identity).ok()
    }

    /// The identity of the host this process runs on, read once from
    /// `/proc/cpuinfo`. `None` when the file is unreadable or doesn't
    /// identify the processor.
    ///
    /// `/proc/cpuinfo` isn't namespaced, so a containerized node daemon sees
    /// the host's processor.
    #[cfg(target_os = "linux")]
    pub fn current() -> Option<&'static Self> {
        static CURRENT: std::sync::OnceLock<Option<HostCpu>> = std::sync::OnceLock::new();
        CURRENT
            .get_or_init(|| {
                std::fs::read_to_string("/proc/cpuinfo")
                    .ok()
                    .and_then(|cpuinfo| Self::from_cpuinfo(&cpuinfo))
            })
            .as_ref()
    }

    /// Non-Linux hosts don't run Firecracker, so they never capture or load
    /// VM state and have no identity to report.
    #[cfg(not(target_os = "linux"))]
    pub fn current() -> Option<&'static Self> {
        None
    }

    /// Returns the identity as a label value.
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for HostCpu {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for HostCpu {
    type Err = HostCpuParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for HostCpu {
    type Error = HostCpuParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<HostCpu> for String {
    fn from(value: HostCpu) -> Self {
        value.as_str().to_owned()
    }
}

/// Error returned when a host CPU compatibility list can't be parsed.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum HostCpuCompatibilityParseError {
    /// An entry wasn't of the form `capture=>loader`.
    #[error("host CPU compatibility entry {entry:?} must be in capture=>loader form")]
    MissingArrow { entry: String },
    /// One side of an entry isn't a valid identity.
    #[error("host CPU compatibility entry {entry:?}: {source}")]
    InvalidHostCpu {
        entry: String,
        #[source]
        source: HostCpuParseError,
    },
}

/// One-way compatibilities between unequal [`HostCpu`] identities: which
/// hosts, beyond those of the capture CPU itself, can load a snapshot.
///
/// [`HostCpuCompatibility::can_load`] treats only equal identities as
/// compatible unless this list says otherwise, because the identity can't tell which pairs work. An
/// operator who knows a CPU's MSR set covers another's lists the pair, and
/// snapshots of the older CPU may then also restore on the newer one instead
/// of waiting for hosts of their own. Pairs aren't transitive or symmetric;
/// each allowed direction is listed.
///
/// ```rust
/// # use pplx_space_execution_spec::{HostCpu, HostCpuCompatibility};
/// let compat =
///     HostCpuCompatibility::parse("genuineintel-6-85-4=>genuineintel-6-85-7").unwrap();
/// let skylake = HostCpu::parse("genuineintel-6-85-4").unwrap();
/// let cascade_lake = HostCpu::parse("genuineintel-6-85-7").unwrap();
/// assert!(compat.can_load(&skylake, &cascade_lake));
/// assert!(!compat.can_load(&cascade_lake, &skylake));
/// assert!(compat.can_load(&skylake, &skylake));
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct HostCpuCompatibility {
    loaders: BTreeMap<HostCpu, BTreeSet<HostCpu>>,
}

impl HostCpuCompatibility {
    /// Parses a comma-separated list of `capture=>loader` pairs. Whitespace
    /// around entries and identities is trimmed, and empty entries are
    /// skipped, so an empty string is an empty list. A malformed entry fails
    /// rather than being dropped, so a typo can't silently remove a pair.
    pub fn parse(raw: &str) -> Result<Self, HostCpuCompatibilityParseError> {
        let mut loaders: BTreeMap<HostCpu, BTreeSet<HostCpu>> = BTreeMap::new();
        for entry in raw
            .split(',')
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
        {
            let (capture, loader) = entry.split_once("=>").ok_or_else(|| {
                HostCpuCompatibilityParseError::MissingArrow {
                    entry: entry.to_string(),
                }
            })?;
            let parse = |value: &str| {
                HostCpu::parse(value.trim()).map_err(|source| {
                    HostCpuCompatibilityParseError::InvalidHostCpu {
                        entry: entry.to_string(),
                        source,
                    }
                })
            };
            loaders
                .entry(parse(capture)?)
                .or_default()
                .insert(parse(loader)?);
        }
        Ok(Self { loaders })
    }

    /// Whether a host of CPU `host` can load state captured on `capture`:
    /// the same identity, or a listed pair.
    pub fn can_load(&self, capture: &HostCpu, host: &HostCpu) -> bool {
        capture == host
            || self
                .loaders
                .get(capture)
                .is_some_and(|loaders| loaders.contains(host))
    }
}

#[cfg(test)]
mod tests;
