//! Portable execution-environment metadata.
//!
//! These types describe the exact runtime, guest platform, persisted-state
//! format, and artifact components associated with a template, sandbox, or
//! snapshot. They intentionally do not contain compatibility policy: runtime
//! drivers own version ordering and restore decisions.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, de};

use super::{Arch, Digest, HostCpu, RuntimeKind, VcpuCount};

/// Firecracker release assigned to durable artifacts written before execution metadata existed.
pub const INITIAL_FIRECRACKER_RUNTIME_VERSION: &str = "1.15.1";
/// Guest kernel node images ship as their default and legacy default, which
/// existing templates and snapshots boot with
/// [`INITIAL_FIRECRACKER_RUNTIME_VERSION`].
pub const INITIAL_FIRECRACKER_KERNEL_VERSION: &str = "6.1.155";
/// Firecracker snapshot format assigned to durable artifacts written before execution metadata.
pub const INITIAL_FIRECRACKER_STATE_FORMAT_VERSION: &str = "9.0.0";
/// Canonical name of Firecracker's persisted snapshot format.
pub const FIRECRACKER_STATE_FORMAT: &str = "firecracker-snapshot";

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-' | b'+'))
}

macro_rules! identifier_type {
    ($name:ident, $error:ident, $label:literal) => {
        #[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
        #[error("invalid {label} '{value}'", label = $label, value = .0)]
        pub struct $error(String);

        #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        #[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl AsRef<str>) -> Result<Self, $error> {
                let value = value.as_ref();
                if valid_identifier(value) {
                    Ok(Self(value.to_string()))
                } else {
                    Err($error(value.to_string()))
                }
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = $error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = $error;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }
    };
}

identifier_type!(RuntimeVersion, RuntimeVersionParseError, "runtime version");
identifier_type!(
    StateFormatName,
    StateFormatNameParseError,
    "state format name"
);
identifier_type!(
    StateFormatVersion,
    StateFormatVersionParseError,
    "state format version"
);
identifier_type!(
    ArtifactVersion,
    ArtifactVersionParseError,
    "artifact version"
);
identifier_type!(
    ArtifactComponentRole,
    ArtifactComponentRoleParseError,
    "artifact component role"
);
identifier_type!(ArtifactFormat, ArtifactFormatParseError, "artifact format");

/// Operating system presented inside the sandbox boundary.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "kebab-case"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum GuestOperatingSystem {
    Linux,
    Windows,
    Macos,
}

impl GuestOperatingSystem {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
            Self::Macos => "macos",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported guest operating system '{0}'")]
pub struct GuestOperatingSystemParseError(String);

impl FromStr for GuestOperatingSystem {
    type Err = GuestOperatingSystemParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "linux" => Ok(Self::Linux),
            "windows" => Ok(Self::Windows),
            "macos" => Ok(Self::Macos),
            other => Err(GuestOperatingSystemParseError(other.to_string())),
        }
    }
}

impl fmt::Display for GuestOperatingSystem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Kind of a versioned file or bundle participating in sandbox execution.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactComponentKind {
    Kernel,
    Initrd,
    Firmware,
    Rootfs,
    BootDisk,
    GuestTools,
}

impl ArtifactComponentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kernel => "kernel",
            Self::Initrd => "initrd",
            Self::Firmware => "firmware",
            Self::Rootfs => "rootfs",
            Self::BootDisk => "boot_disk",
            Self::GuestTools => "guest_tools",
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported artifact component kind '{0}'")]
pub struct ArtifactComponentKindParseError(String);

impl FromStr for ArtifactComponentKind {
    type Err = ArtifactComponentKindParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "kernel" => Ok(Self::Kernel),
            "initrd" => Ok(Self::Initrd),
            "firmware" => Ok(Self::Firmware),
            "rootfs" => Ok(Self::Rootfs),
            "boot_disk" => Ok(Self::BootDisk),
            "guest_tools" => Ok(Self::GuestTools),
            other => Err(ArtifactComponentKindParseError(other.to_string())),
        }
    }
}

impl fmt::Display for ArtifactComponentKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Verified versioned component bytes, independent of runtime compatibility.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ComponentIdentity {
    pub kind: ArtifactComponentKind,
    pub version: ArtifactVersion,
    pub digest: Digest,
}

/// Concrete runtime binary identity.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RuntimeIdentity {
    pub kind: RuntimeKind,
    pub version: RuntimeVersion,
}

/// `<runtime> <version>`, as in `firecracker 1.15.1`.
impl fmt::Display for RuntimeIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.kind, self.version)
    }
}

/// Guest operating-system and CPU-architecture identity.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct GuestPlatform {
    pub os: GuestOperatingSystem,
    pub architecture: Arch,
}

/// Runtime-owned persisted-state format identity.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct StateFormatIdentity {
    pub format: StateFormatName,
    pub version: StateFormatVersion,
}

/// Versioned file or bundle associated with an execution environment.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct ArtifactComponentMetadata {
    pub kind: ArtifactComponentKind,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub role: Option<ArtifactComponentRole>,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub version: Option<ArtifactVersion>,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub digest: Option<Digest>,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    pub format: Option<ArtifactFormat>,
}

impl ArtifactComponentMetadata {
    fn identity(&self) -> (ArtifactComponentKind, Option<&ArtifactComponentRole>) {
        (self.kind, self.role.as_ref())
    }
}

/// Error returned when an execution metadata document violates structural invariants.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExecutionMetadataError {
    #[error("duplicate artifact component identity {kind}/{role}")]
    DuplicateComponent {
        kind: ArtifactComponentKind,
        role: DuplicateComponentRole,
    },
    #[error("persisted snapshot execution metadata requires a state format")]
    MissingSnapshotStateFormat,
}

/// Exact environment resolved for a template build or sandbox.
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionMetadata {
    /// Immutable environment binding. Absence is legacy evidence, never approval.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    environment: Option<super::EnvironmentReference>,
    runtime: RuntimeIdentity,
    guest: GuestPlatform,
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    state_format: Option<StateFormatIdentity>,
    components: Vec<ArtifactComponentMetadata>,
}

impl ExecutionMetadata {
    pub fn new(
        runtime: RuntimeIdentity,
        guest: GuestPlatform,
        state_format: Option<StateFormatIdentity>,
        mut components: Vec<ArtifactComponentMetadata>,
    ) -> Result<Self, ExecutionMetadataError> {
        components.sort_by(|left, right| left.identity().cmp(&right.identity()));
        for pair in components.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            if left.identity() == right.identity() {
                let component = left;
                return Err(ExecutionMetadataError::DuplicateComponent {
                    kind: component.kind,
                    role: DuplicateComponentRole(component.role.clone()),
                });
            }
        }
        Ok(Self {
            environment: None,
            runtime,
            guest,
            state_format,
            components,
        })
    }

    pub const fn environment(&self) -> Option<super::EnvironmentReference> {
        self.environment
    }

    /// Attach a binding after the owning resolver checks its definition against
    /// these observations. This setter does not authorize execution.
    pub fn with_environment(mut self, environment: super::EnvironmentReference) -> Self {
        self.environment = Some(environment);
        self
    }

    pub const fn runtime(&self) -> &RuntimeIdentity {
        &self.runtime
    }

    pub const fn guest(&self) -> GuestPlatform {
        self.guest
    }

    pub const fn state_format(&self) -> Option<&StateFormatIdentity> {
        self.state_format.as_ref()
    }

    pub fn components(&self) -> &[ArtifactComponentMetadata] {
        &self.components
    }

    pub fn component(
        &self,
        kind: ArtifactComponentKind,
        role: Option<&ArtifactComponentRole>,
    ) -> Option<&ArtifactComponentMetadata> {
        self.components
            .iter()
            .find(|component| component.identity() == (kind, role))
    }

    /// Metadata assigned to pre-metadata Linux/Firecracker artifacts.
    ///
    /// Guest architecture must be supplied by the Firecracker boundary because
    /// the legacy fallback from node architecture is not valid for every runtime.
    pub fn initial_firecracker(
        architecture: Arch,
        components: Vec<ArtifactComponentMetadata>,
    ) -> Self {
        Self::new(
            RuntimeIdentity {
                kind: RuntimeKind::Firecracker,
                version: RuntimeVersion::parse(INITIAL_FIRECRACKER_RUNTIME_VERSION)
                    .expect("initial Firecracker runtime version is valid"),
            },
            GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture,
            },
            Some(StateFormatIdentity {
                format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT)
                    .expect("Firecracker state format name is valid"),
                version: StateFormatVersion::parse(INITIAL_FIRECRACKER_STATE_FORMAT_VERSION)
                    .expect("initial Firecracker state format version is valid"),
            }),
            components,
        )
        .expect("legacy Firecracker metadata has unique component identities")
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DuplicateComponentRole(Option<ArtifactComponentRole>);

impl fmt::Display for DuplicateComponentRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(role) => role.fmt(formatter),
            None => formatter.write_str("unspecified"),
        }
    }
}

#[cfg(feature = "serde")]
#[derive(Deserialize)]
struct ExecutionMetadataWire {
    #[serde(default)]
    environment: Option<super::EnvironmentReference>,
    runtime: RuntimeIdentity,
    guest: GuestPlatform,
    #[serde(default)]
    state_format: Option<StateFormatIdentity>,
    #[serde(default)]
    components: Vec<ArtifactComponentMetadata>,
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for ExecutionMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ExecutionMetadataWire::deserialize(deserializer)?;
        let mut metadata = Self::new(wire.runtime, wire.guest, wire.state_format, wire.components)
            .map_err(de::Error::custom)?;
        metadata.environment = wire.environment;
        Ok(metadata)
    }
}

/// Execution metadata for persisted VM state. Construction requires a state format.
#[cfg_attr(feature = "serde", derive(Serialize))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotExecutionMetadata {
    #[cfg_attr(feature = "serde", serde(flatten))]
    execution: ExecutionMetadata,
    /// Admitted CPU allocation at capture, if known. Legacy absence remains unknown.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    cpu_budget: Option<VcpuCount>,
    /// CPU of the host that captured the VM state, if known. Legacy absence
    /// remains unknown; see [`HostCpu`] for why the instance type isn't enough.
    #[cfg_attr(
        feature = "serde",
        serde(default, skip_serializing_if = "Option::is_none")
    )]
    host_cpu: Option<HostCpu>,
}

impl SnapshotExecutionMetadata {
    pub fn new(metadata: ExecutionMetadata) -> Result<Self, ExecutionMetadataError> {
        metadata
            .state_format()
            .ok_or(ExecutionMetadataError::MissingSnapshotStateFormat)?;
        Ok(Self {
            execution: metadata,
            cpu_budget: None,
            host_cpu: None,
        })
    }

    pub fn with_cpu_budget(mut self, cpu_budget: Option<VcpuCount>) -> Self {
        self.cpu_budget = cpu_budget;
        self
    }

    pub const fn cpu_budget(&self) -> Option<VcpuCount> {
        self.cpu_budget
    }

    pub fn with_host_cpu(mut self, host_cpu: Option<HostCpu>) -> Self {
        self.host_cpu = host_cpu;
        self
    }

    pub const fn host_cpu(&self) -> Option<&HostCpu> {
        self.host_cpu.as_ref()
    }

    pub const fn execution(&self) -> &ExecutionMetadata {
        &self.execution
    }

    /// Whether `self` and `other` describe the same artifact.
    ///
    /// The capture host CPU is placement evidence, not identity: one backup
    /// attempt can be registered by a cluster API that predates the field
    /// (and drops it) and replayed after that API is upgraded, and the replay
    /// isn't a conflict. Every other field is identity; the exhaustive
    /// destructure makes a new field choose a side.
    pub fn same_artifact_identity(&self, other: &Self) -> bool {
        let Self {
            execution,
            cpu_budget,
            host_cpu: _,
        } = self;
        *execution == other.execution && *cpu_budget == other.cpu_budget
    }

    pub fn into_execution(self) -> ExecutionMetadata {
        self.execution
    }
}

#[cfg(feature = "serde")]
#[derive(Deserialize)]
struct SnapshotExecutionMetadataWire {
    #[serde(flatten)]
    execution: ExecutionMetadata,
    #[serde(default)]
    cpu_budget: Option<VcpuCount>,
    /// Read as any value and parsed leniently: the capture CPU is placement
    /// evidence, so a value this release doesn't accept (written by a newer
    /// release, then rolled back), whether a string or not, reads as unknown
    /// instead of making the whole row unreadable.
    #[serde(default)]
    host_cpu: Option<serde_json::Value>,
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for SnapshotExecutionMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = SnapshotExecutionMetadataWire::deserialize(deserializer)?;
        Self::new(wire.execution)
            .map(|metadata| {
                metadata.with_cpu_budget(wire.cpu_budget).with_host_cpu(
                    wire.host_cpu
                        .as_ref()
                        .and_then(serde_json::Value::as_str)
                        .and_then(|raw| HostCpu::parse(raw).ok()),
                )
            })
            .map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests;
