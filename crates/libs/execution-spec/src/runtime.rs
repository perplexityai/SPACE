//! Runtime and startup vocabulary.
//!
//! These types describe which runtime technology executes a sandbox and which
//! startup path the caller is asking for. Runtime selection is the separate
//! CLI/API axis used beside a template reference, e.g.
//! `--runtime firecracker --template seed:latest`.
//!
//! # Examples
//!
//! ```rust
//! # use pplx_space_execution_spec::*;
//! let runtime = RuntimeKind::parse("fc").unwrap();
//! let startup = StartupPolicy::PreferBaseSnapshot;
//!
//! assert_eq!(runtime.to_string(), "firecracker");
//! assert_eq!(runtime.alias(), "fc");
//! assert_eq!(startup, StartupPolicy::PreferBaseSnapshot);
//! ```

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Runtime technology used to execute a sandbox.
///
/// This is the "what runs the VM/container" axis. It is intentionally separate
/// from template selection, so the same template tag can resolve to different
/// runtime-specific artifacts.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let runtime = RuntimeKind::Firecracker;
/// assert_eq!(runtime, RuntimeKind::Firecracker);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RuntimeKind {
    /// Firecracker microVM runtime.
    Firecracker,
    /// Cloud Hypervisor microVM runtime.
    CloudHypervisor,
    /// gVisor container runtime.
    Gvisor,
}

/// Error returned when a runtime identifier cannot be parsed.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_execution_spec::RuntimeKind;
/// assert!(RuntimeKind::from_str("unknown").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported runtime '{0}'")]
pub struct RuntimeKindParseError(String);

impl RuntimeKind {
    /// Returns the canonical full runtime identifier.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// assert_eq!(RuntimeKind::Firecracker.as_str(), "firecracker");
    /// ```
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Firecracker => RUNTIME_FIRECRACKER,
            Self::CloudHypervisor => RUNTIME_CLOUD_HYPERVISOR,
            Self::Gvisor => RUNTIME_GVISOR,
        }
    }

    /// Returns the short operator alias for this runtime.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// assert_eq!(RuntimeKind::Firecracker.alias(), "fc");
    /// ```
    pub const fn alias(self) -> &'static str {
        match self {
            Self::Firecracker => RUNTIME_FIRECRACKER_ALIAS,
            Self::CloudHypervisor => RUNTIME_CLOUD_HYPERVISOR_ALIAS,
            Self::Gvisor => RUNTIME_GVISOR_ALIAS,
        }
    }

    /// Parses a runtime identifier from wire or operator-facing spelling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// assert_eq!(RuntimeKind::parse("fc").unwrap(), RuntimeKind::Firecracker);
    /// assert_eq!(
    ///     RuntimeKind::parse("ch").unwrap(),
    ///     RuntimeKind::CloudHypervisor,
    /// );
    /// ```
    pub fn parse(value: &str) -> Result<Self, RuntimeKindParseError> {
        match value {
            RUNTIME_FIRECRACKER | RUNTIME_FIRECRACKER_ALIAS => Ok(Self::Firecracker),
            RUNTIME_CLOUD_HYPERVISOR | RUNTIME_CLOUD_HYPERVISOR_ALIAS => Ok(Self::CloudHypervisor),
            RUNTIME_GVISOR | RUNTIME_GVISOR_ALIAS => Ok(Self::Gvisor),
            other => Err(RuntimeKindParseError(other.to_string())),
        }
    }

    /// Returns the baseline capabilities normally expected from this runtime.
    ///
    /// Deployment-specific configuration can still narrow or extend this set.
    pub const fn baseline_capabilities(self) -> RuntimeCapabilities {
        match self {
            Self::Firecracker | Self::CloudHypervisor => RuntimeCapabilities::full_node(),
            Self::Gvisor => RuntimeCapabilities {
                pause_resume: false,
                snapshot: false,
                fork: false,
                prewarmed_pool: false,
                network_gateway: true,
                volume_manager: true,
            },
        }
    }

    /// Returns whether this runtime's baseline capabilities support a startup policy.
    pub const fn supports_startup(self, policy: StartupPolicy) -> bool {
        self.baseline_capabilities()
            .contains(policy.required_capabilities())
    }

    /// Validates a startup policy against this runtime's baseline capabilities.
    pub fn validate_startup(
        self,
        policy: StartupPolicy,
    ) -> Result<(), UnsupportedStartupPolicyError> {
        if self.supports_startup(policy) {
            Ok(())
        } else {
            Err(UnsupportedStartupPolicyError {
                runtime: self,
                policy,
            })
        }
    }
}

/// Canonical identifier for the Firecracker runtime.
pub const RUNTIME_FIRECRACKER: &str = "firecracker";
/// Short alias for the Firecracker runtime.
pub const RUNTIME_FIRECRACKER_ALIAS: &str = "fc";
/// Canonical identifier for the Cloud Hypervisor runtime.
pub const RUNTIME_CLOUD_HYPERVISOR: &str = "cloud-hypervisor";
/// Short alias for the Cloud Hypervisor runtime.
pub const RUNTIME_CLOUD_HYPERVISOR_ALIAS: &str = "ch";
/// Canonical identifier for the gVisor runtime.
pub const RUNTIME_GVISOR: &str = "gvisor";
/// Short alias for the gVisor runtime.
pub const RUNTIME_GVISOR_ALIAS: &str = "gv";

impl fmt::Display for RuntimeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RuntimeKind {
    type Err = RuntimeKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for RuntimeKind {
    type Error = RuntimeKindParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(&value)
    }
}

impl From<RuntimeKind> for String {
    fn from(value: RuntimeKind) -> Self {
        value.to_string()
    }
}

/// Error returned when a runtime deployment identifier is empty or unsafe.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// assert!(RuntimeDeploymentId::parse("").is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("runtime deployment id must be non-empty and bounded")]
pub struct RuntimeDeploymentIdParseError;

/// Opaque runtime deployment or cluster identifier.
///
/// Use this when a sandbox needs to remember the concrete deployment that
/// accepted placement. Common-core deliberately treats this as a bounded opaque
/// string rather than claiming a provider-neutral naming format.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let deployment = RuntimeDeploymentId::parse("prod-us-east-1").unwrap();
/// assert_eq!(deployment.as_str(), "prod-us-east-1");
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeDeploymentId(String);

impl RuntimeDeploymentId {
    /// Parses a runtime deployment identifier.
    ///
    /// The accepted value is non-empty, at most 256 bytes, and must not contain
    /// NUL.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let deployment = RuntimeDeploymentId::parse("cluster-a").unwrap();
    /// assert_eq!(deployment.as_str(), "cluster-a");
    /// ```
    pub fn parse(value: impl Into<String>) -> Result<Self, RuntimeDeploymentIdParseError> {
        let value = value.into();
        if value.is_empty() || value.len() > 256 || value.contains('\0') {
            return Err(RuntimeDeploymentIdParseError);
        }
        Ok(Self(value))
    }

    /// Returns the deployment identifier as a borrowed string.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let deployment = RuntimeDeploymentId::parse("cluster-a").unwrap();
    /// assert_eq!(deployment.as_str(), "cluster-a");
    /// ```
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for RuntimeDeploymentId {
    type Error = RuntimeDeploymentIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<RuntimeDeploymentId> for String {
    fn from(value: RuntimeDeploymentId) -> Self {
        value.0
    }
}

/// Error returned when a startup policy is impossible for a runtime.
#[derive(Debug, Clone, Copy, Eq, PartialEq, thiserror::Error)]
#[error("runtime {runtime} does not support startup policy {policy:?}")]
pub struct UnsupportedStartupPolicyError {
    runtime: RuntimeKind,
    policy: StartupPolicy,
}

impl UnsupportedStartupPolicyError {
    /// Returns the runtime that rejected the policy.
    pub const fn runtime(self) -> RuntimeKind {
        self.runtime
    }

    /// Returns the unsupported startup policy.
    pub const fn policy(self) -> StartupPolicy {
        self.policy
    }
}

/// Capabilities a runtime can provide for sandbox placement.
///
/// Match these against `SandboxRequirements::required_capabilities()` before
/// choosing a runtime or management path.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let capabilities = RuntimeCapabilities::full_node();
/// assert!(capabilities.pause_resume);
/// assert!(capabilities.prewarmed_pool);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeCapabilities {
    /// Runtime can pause and resume sandboxes.
    pub pause_resume: bool,
    /// Runtime can create or start from snapshots.
    pub snapshot: bool,
    /// Runtime can fork an existing sandbox.
    pub fork: bool,
    /// Runtime can serve sandboxes from a prewarmed pool.
    pub prewarmed_pool: bool,
    /// Runtime can provide gateway networking.
    pub network_gateway: bool,
    /// Runtime can attach or manage volumes.
    pub volume_manager: bool,
}

impl RuntimeCapabilities {
    /// Returns the capability set expected from a full SPACE node.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_execution_spec::*;
    /// let capabilities = RuntimeCapabilities::full_node();
    /// assert!(capabilities.snapshot);
    /// assert!(capabilities.volume_manager);
    /// ```
    pub const fn full_node() -> Self {
        Self {
            pause_resume: true,
            snapshot: true,
            fork: true,
            prewarmed_pool: true,
            network_gateway: true,
            volume_manager: true,
        }
    }

    /// Returns whether this capability set satisfies all required capabilities.
    pub const fn contains(self, required: Self) -> bool {
        (!required.pause_resume || self.pause_resume)
            && (!required.snapshot || self.snapshot)
            && (!required.fork || self.fork)
            && (!required.prewarmed_pool || self.prewarmed_pool)
            && (!required.network_gateway || self.network_gateway)
            && (!required.volume_manager || self.volume_manager)
    }
}

/// Requested startup strategy for a sandbox.
///
/// This is caller intent. The realized path is captured separately as
/// `EffectiveStartup` after placement decides what was actually used.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let policy = StartupPolicy::RequireWarmPool;
/// assert_eq!(policy, StartupPolicy::RequireWarmPool);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum StartupPolicy {
    /// Start without a base snapshot or warm pool.
    ColdOnly,
    /// Prefer a base snapshot but fall back if unavailable.
    PreferBaseSnapshot,
    /// Require a base snapshot.
    RequireBaseSnapshot,
    /// Prefer a warm pool but fall back if unavailable.
    PreferWarmPool,
    /// Require a warm pool.
    RequireWarmPool,
}

impl StartupPolicy {
    /// Returns the runtime capabilities needed to satisfy this startup policy.
    pub const fn required_capabilities(self) -> RuntimeCapabilities {
        match self {
            Self::ColdOnly => RuntimeCapabilities {
                pause_resume: false,
                snapshot: false,
                fork: false,
                prewarmed_pool: false,
                network_gateway: false,
                volume_manager: false,
            },
            Self::PreferBaseSnapshot | Self::RequireBaseSnapshot => RuntimeCapabilities {
                pause_resume: false,
                snapshot: true,
                fork: false,
                prewarmed_pool: false,
                network_gateway: false,
                volume_manager: false,
            },
            Self::PreferWarmPool | Self::RequireWarmPool => RuntimeCapabilities {
                pause_resume: false,
                snapshot: false,
                fork: false,
                prewarmed_pool: true,
                network_gateway: false,
                volume_manager: false,
            },
        }
    }
}

/// Startup path that placement actually used.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_execution_spec::*;
/// let effective = EffectiveStartup::BaseSnapshot;
/// assert_ne!(effective, EffectiveStartup::Cold);
/// ```
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum EffectiveStartup {
    /// Cold boot without snapshot or warm pool.
    Cold,
    /// Started from a base snapshot.
    BaseSnapshot,
    /// Allocated from a warm pool.
    WarmPool,
}

#[cfg(test)]
mod tests;
