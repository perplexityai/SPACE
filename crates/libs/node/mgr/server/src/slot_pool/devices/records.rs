use super::*;

macro_rules! state {
    ($name:ident { $($variant:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone,Copy,Debug,Eq,PartialEq)]
        pub enum $name { $($variant),+ }
        impl $name { pub const fn as_str(self)-> &'static str { match self { $(Self::$variant=>$wire),+ } } }
        impl fmt::Display for $name { fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result { f.write_str(self.as_str()) } }
        impl FromStr for $name { type Err=DeviceStoreError; fn from_str(s:&str)->Result<Self,Self::Err> { match s { $($wire=>Ok(Self::$variant)),+, _=>Err(DeviceStoreError::CorruptState(s.into())) } } }
    }
}
state!(ParentState { Quarantined=>"quarantined", Ready=>"ready", Sanitizing=>"sanitizing", Retired=>"retired" });
state!(AllocationState { Reserved=>"reserved", Attaching=>"attaching", Attached=>"attached", Releasing=>"releasing", Released=>"released", Quarantined=>"quarantined" });
state!(ParentMode { Dedicated=>"dedicated" });
state!(HolderKind { Sandbox=>"sandbox", Sanitizer=>"sanitizer" });
state!(ExclusiveHolding { Exclusive=>"exclusive" });
/// The kernel boot UUID is observation scope, not proof of reset or cleanliness.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct HostBootId(Uuid);
impl HostBootId {
    pub fn current() -> Result<Self, DeviceStoreError> {
        let raw = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
            .map_err(DeviceStoreError::BootIo)?;
        raw.trim().parse().map_err(DeviceStoreError::BootIdentity)
    }
}
impl FromStr for HostBootId {
    type Err = uuid::Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self)
    }
}
impl fmt::Display for HostBootId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Evidence from the host inspector. It does not authorize first use or reuse.
/// A stable device identifier, when available, must be established by the
/// qualification procedure; a PCI locator alone never supplies that guarantee.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "format", deny_unknown_fields)]
pub enum EnrollmentIdentity {
    #[serde(rename = "linux_pci_v1")]
    LinuxPciV1 {
        locator: HostDeviceId,
        vendor: u16,
        device: u16,
        subsystem_vendor: u16,
        subsystem_device: u16,
        topology_digest: Digest,
        sysfs_device: u64,
        sysfs_inode: u64,
    },
}

impl EnrollmentIdentity {
    pub fn pci_identifiers(&self) -> [u16; 4] {
        match self {
            Self::LinuxPciV1 {
                vendor,
                device,
                subsystem_vendor,
                subsystem_device,
                ..
            } => [*vendor, *device, *subsystem_vendor, *subsystem_device],
        }
    }
    pub fn locator(&self) -> &HostDeviceId {
        match self {
            Self::LinuxPciV1 { locator, .. } => locator,
        }
    }
}

/// Outcomes only: a running sanitizer is represented by an allocation row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "format", deny_unknown_fields)]
pub enum QualificationEvidence {
    /// No VM ran: this is the host pool's explicit reset-only development result.
    #[serde(rename = "development_reset_v1")]
    DevelopmentResetV1 {
        profile: DeviceProfile,
        procedure: Digest,
        host_boot_id: HostBootId,
        members: Vec<HostDeviceId>,
        reset_started_at: jiff::Timestamp,
        completed_at: jiff::Timestamp,
    },
    #[serde(rename = "completed_v1")]
    CompletedV1 {
        execution_id: SandboxActivationAttemptId,
        profile: DeviceProfile,
        procedure: Digest,
        host_boot_id: HostBootId,
        members: Vec<HostDeviceId>,
        completed_at: jiff::Timestamp,
    },
}
impl QualificationEvidence {
    pub(super) fn matches(&self, p: &DeviceParent) -> bool {
        match self {
            Self::DevelopmentResetV1 {
                profile,
                procedure,
                host_boot_id,
                members,
                reset_started_at,
                completed_at,
            } => {
                cfg!(feature = "gpu-development")
                    && profile == &p.profile
                    && procedure == &p.procedure
                    && host_boot_id == &p.host_boot_id
                    && members == &p.members
                    && reset_started_at <= completed_at
            }
            Self::CompletedV1 {
                profile,
                procedure,
                host_boot_id,
                members,
                ..
            } => {
                profile == &p.profile
                    && procedure == &p.procedure
                    && host_boot_id == &p.host_boot_id
                    && members == &p.members
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeviceOwner {
    pub(super) sandbox_id: SandboxId,
    pub(super) execution_id: SandboxActivationAttemptId,
    pub(super) slot_generation: u64,
}
impl DeviceOwner {
    pub fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }
    pub fn execution_id(&self) -> SandboxActivationAttemptId {
        self.execution_id
    }

    pub fn new(
        sandbox_id: SandboxId,
        execution_id: SandboxActivationAttemptId,
        slot_generation: u64,
    ) -> Self {
        Self {
            sandbox_id,
            execution_id,
            slot_generation,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SanitizerAttempt {
    pub(super) execution_id: SandboxActivationAttemptId,
    pub(super) procedure: Digest,
}
impl SanitizerAttempt {
    pub fn new(execution_id: SandboxActivationAttemptId, procedure: Digest) -> Self {
        Self {
            execution_id,
            procedure,
        }
    }
    pub fn execution_id(&self) -> SandboxActivationAttemptId {
        self.execution_id
    }
    pub fn procedure(&self) -> Digest {
        self.procedure
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeviceHolder {
    Sandbox {
        owner: DeviceOwner,
        requirements_digest: Digest,
    },
    Sanitizer(SanitizerAttempt),
}
#[derive(Clone, Debug)]
pub struct DeviceParent {
    pub(super) id: DeviceParentId,
    pub(super) profile: DeviceProfile,
    pub(super) host_boot_id: HostBootId,
    pub(super) state: ParentState,
    pub(super) mode: ParentMode,
    pub(super) revision: u64,
    pub(super) procedure: Digest,
    pub(super) evidence: Option<QualificationEvidence>,
    pub(super) members: Vec<HostDeviceId>,
    pub(super) enrolled_members: Vec<EnrollmentIdentity>,
}
impl DeviceParent {
    pub(super) fn allocatable(&self, boot: HostBootId, enabled: bool, held: bool) -> bool {
        self.host_boot_id == boot && self.state == ParentState::Ready && enabled && !held
    }

    pub fn id(&self) -> DeviceParentId {
        self.id
    }
    pub fn profile(&self) -> &DeviceProfile {
        &self.profile
    }
    pub fn qualification_procedure(&self) -> Digest {
        self.procedure
    }
    pub fn host_boot_id(&self) -> HostBootId {
        self.host_boot_id
    }
    pub fn state(&self) -> ParentState {
        self.state
    }
    pub fn mode(&self) -> ParentMode {
        self.mode
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn members(&self) -> &[HostDeviceId] {
        &self.members
    }
    pub fn enrolled_members(&self) -> &[EnrollmentIdentity] {
        &self.enrolled_members
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceAllocation {
    pub(super) assignment: ExclusiveDeviceAssignment,
    pub(super) holder: DeviceHolder,
    pub(super) state: AllocationState,
    pub(super) revision: u64,
}
impl DeviceAllocation {
    pub fn assignment(&self) -> &ExclusiveDeviceAssignment {
        &self.assignment
    }
    pub fn holder(&self) -> DeviceHolder {
        self.holder
    }
    pub fn state(&self) -> AllocationState {
        self.state
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "format", content = "assignment", deny_unknown_fields)]
pub(super) enum AssignmentRecord {
    #[serde(rename = "dedicated_v1")]
    V1(ExclusiveDeviceAssignment),
}

/// Chosen by the host's compiled qualification policy, not an RPC caller.
#[derive(Clone, Copy, Debug)]
pub enum DeviceReleaseDisposition {
    Quarantine,
    QueueReset,
    Sanitize(SanitizerAttempt),
}

/// Validated enrollment and all unresolved holders from one read transaction.
pub struct DeviceCapacitySnapshot {
    pub(super) parent: DeviceParent,
    pub(super) allocations: Vec<DeviceAllocation>,
    pub(super) held: bool,
}
impl DeviceCapacitySnapshot {
    pub fn parent(&self) -> &DeviceParent {
        &self.parent
    }
    pub fn allocations(&self) -> &[DeviceAllocation] {
        &self.allocations
    }
    pub fn allocatable(&self, boot: HostBootId, enabled: bool) -> bool {
        self.parent.allocatable(boot, enabled, self.held)
    }
}
