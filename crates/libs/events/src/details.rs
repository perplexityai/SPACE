macro_rules! field_setters {
    ($($field:ident),+ $(,)?) => {
        $(
            pub fn $field(mut self, value: impl Into<String>) -> Self {
                self.$field = Some(value.into());
                self
            }
        )+
    };
}

#[derive(Debug, Default, Clone)]
pub struct SandboxEventDetails {
    pub action: Option<String>,
    pub result: Option<String>,
    pub error: Option<String>,
    /// Bounded name of the failure, e.g. a `RestoreError::kind_label`, so a
    /// consumer never has to classify the free-text `error`.
    pub error_kind: Option<String>,
    pub operation: Option<String>,
    pub reason: Option<String>,
    pub trigger: Option<String>,
    pub from_state: Option<String>,
    pub to_state: Option<String>,
    pub target_state: Option<String>,
    pub lifecycle_state: Option<String>,
    pub placement: Option<String>,
    pub execution_generation: Option<String>,
    pub route_activation_id: Option<String>,
    pub receipt_id: Option<String>,
    pub claim_id: Option<String>,
    pub pool_slot_id: Option<String>,
    pub pod_name: Option<String>,
    pub pod_uid: Option<String>,
    pub pod_ip: Option<String>,
    pub k8s_node_name: Option<String>,
    pub source_node_id: Option<String>,
    pub source_node_name: Option<String>,
    pub target_node_id: Option<String>,
    pub target_node_name: Option<String>,
    pub restore_storage_uri_present: Option<bool>,
    pub has_netns_path: Option<bool>,
    pub has_pod_netns: Option<bool>,
    pub network_slot: Option<String>,
    pub network_slot_generation: Option<String>,
    pub network_slot_label: Option<String>,
    pub network_slots_total: Option<String>,
    pub network_slots_ready: Option<String>,
    pub network_slots_leased: Option<String>,
    pub network_slots_dirty: Option<String>,
    pub network_slots_resetting: Option<String>,
    pub network_slots_quarantined: Option<String>,
    pub network_slots_degraded: Option<String>,
    pub sandbox_uid: Option<String>,
    pub pid: Option<String>,
    pub spaced_host_pid: Option<String>,
    pub net_gateway_pid: Option<String>,
    pub net_gateway_build_git_sha: Option<String>,
    pub program: Option<String>,
    pub boot_type: Option<String>,
    pub vcpu: Option<String>,
    pub memory_mib: Option<String>,
    pub disk_mib: Option<String>,
    pub shape_source: Option<String>,
    pub guest_cid: Option<String>,
    pub guest_ip: Option<String>,
    pub guest_gateway: Option<String>,
    pub guest_uptime_ms: Option<String>,
    pub timeout_secs: Option<String>,
    pub snapshot_id: Option<String>,
    pub rows_affected: Option<String>,
    pub elapsed_ms: Option<String>,
    pub ms_since_wake: Option<String>,
    pub age_secs: Option<String>,
    pub action_count: Option<String>,
    pub error_count: Option<String>,
    pub force: Option<bool>,
    pub keep_filesystem: Option<bool>,
    pub return_after_slot_release: Option<bool>,
    pub immediate_kill: Option<bool>,
    pub background: Option<bool>,
    pub cwd_present: Option<bool>,
    /// The sandbox's immutable `volume_encryption` stamp. Recorded on the
    /// lifecycle transitions so a timeline can be read without joining back
    /// to the row: it decides which suspend and restore path ran, and it
    /// cannot change over the sandbox's life, so an event that omits it is
    /// ambiguous about the code path it describes.
    pub volume_encryption: Option<bool>,
    pub condition_set: Option<String>,
    pub stage_index: Option<String>,
    pub due_at: Option<String>,
    pub requested_template: Option<String>,
    pub template_id: Option<String>,
    pub template_content_hash: Option<String>,
    pub template_release_tags: Option<String>,
    pub runtime: Option<String>,
    pub runtime_version: Option<String>,
    pub guest_os: Option<String>,
    pub guest_arch: Option<String>,
    pub state_format_name: Option<String>,
    pub state_format_version: Option<String>,
    pub kernel_version: Option<String>,
    pub kernel_digest: Option<String>,
    pub restored_snapshot_id: Option<String>,
    pub restored_snapshot_local_artifact_id: Option<String>,
    pub restore_provenance: Option<String>,
    pub snapshot_metadata_sha256: Option<String>,
    pub snapshot_rootfs_version: Option<String>,
    pub snapshot_kernel_version: Option<String>,
    pub restore_codec: Option<String>,
    pub rootfs_diff_format: Option<String>,
    pub host_port: Option<String>,
    pub guest_port: Option<String>,
    pub protocol: Option<String>,
    pub visibility: Option<String>,
    pub netns: Option<String>,
    pub vpeer_ip: Option<String>,
    pub ingress_iface: Option<String>,
    pub bridge_status: Option<String>,
    pub service_generation: Option<String>,
    pub network_policy_generation: Option<String>,
    pub expected_network_policy_generation: Option<String>,
    pub network_policy_source: Option<String>,
    pub network_policy_authority: Option<String>,
    pub expected_network_policy_ref: Option<String>,
    pub installed_network_policy_ref: Option<String>,
    pub network_state: Option<String>,
    /// Gateway-reported readiness of TLS-interception dependencies at the
    /// moment of a network policy refresh (`ready`, `not_required`, or
    /// `dependencies_not_ready`). Orthogonal to `network_state`.
    pub interception_readiness: Option<String>,
    /// The effective TLS interception mode (`on` or `off`) a sandbox create
    /// resolved from its network policy override or template default.
    pub tls_interception: Option<String>,
    pub artifact: Option<String>,
    pub local_artifact_id: Option<String>,
    pub artifact_kind: Option<String>,
    pub artifact_origin: Option<String>,
    pub backup_attempt_id: Option<String>,
    pub snapshot_upload_job_id: Option<String>,
    pub transfer_job_id: Option<String>,
    pub bytes_transferred: Option<String>,
    pub local_path: Option<String>,
    pub object_uri: Option<String>,
    pub owner_source: Option<String>,
    pub upstream_issuer: Option<String>,
    pub upstream_org_id: Option<String>,
    pub upstream_subject_id: Option<String>,
    /// Not an event field: keeps a failed-phase event at INFO; see
    /// [`SandboxEventDetails::logged_by_caller`] and
    /// [`SandboxEventDetails::routine`].
    pub stays_at_info: bool,
}

impl SandboxEventDetails {
    field_setters! {
        action,
        result,
        error,
        error_kind,
        operation,
        reason,
        target_state,
        lifecycle_state,
        placement,
        execution_generation,
        route_activation_id,
        receipt_id,
        claim_id,
        pool_slot_id,
        pod_name,
        pod_uid,
        pod_ip,
        k8s_node_name,
        source_node_id,
        source_node_name,
        target_node_id,
        target_node_name,
        network_slot,
        network_slot_generation,
        network_slot_label,
        network_slots_total,
        network_slots_ready,
        network_slots_leased,
        network_slots_dirty,
        network_slots_resetting,
        network_slots_quarantined,
        network_slots_degraded,
        sandbox_uid,
        pid,
        spaced_host_pid,
        net_gateway_pid,
        net_gateway_build_git_sha,
        program,
        boot_type,
        vcpu,
        memory_mib,
        disk_mib,
        guest_cid,
        guest_ip,
        guest_gateway,
        guest_uptime_ms,
        timeout_secs,
        snapshot_id,
        rows_affected,
        elapsed_ms,
        ms_since_wake,
        age_secs,
        action_count,
        error_count,
        condition_set,
        stage_index,
        due_at,
        requested_template,
        template_id,
        template_content_hash,
        template_release_tags,
        runtime,
        runtime_version,
        guest_os,
        guest_arch,
        state_format_name,
        state_format_version,
        kernel_version,
        kernel_digest,
        restored_snapshot_id,
        restored_snapshot_local_artifact_id,
        snapshot_metadata_sha256,
        snapshot_rootfs_version,
        snapshot_kernel_version,
        restore_codec,
        rootfs_diff_format,
        host_port,
        guest_port,
        protocol,
        visibility,
        netns,
        vpeer_ip,
        ingress_iface,
        bridge_status,
        service_generation,
        network_policy_generation,
        expected_network_policy_generation,
        network_policy_source,
        network_policy_authority,
        expected_network_policy_ref,
        installed_network_policy_ref,
        network_state,
        interception_readiness,
        tls_interception,
        artifact,
        local_artifact_id,
        artifact_kind,
        artifact_origin,
        backup_attempt_id,
        snapshot_upload_job_id,
        transfer_job_id,
        bytes_transferred,
        local_path,
        object_uri,
        owner_source,
        upstream_issuer,
        upstream_org_id,
        upstream_subject_id,
    }

    pub fn trigger(mut self, value: SandboxEventTrigger) -> Self {
        self.trigger = Some(value.as_str().to_string());
        self
    }

    pub fn execution_metadata(
        mut self,
        execution: &pplx_space_common_core::types::ExecutionMetadata,
    ) -> Self {
        use pplx_space_common_core::types::ArtifactComponentKind;

        self.runtime = Some(execution.runtime().kind.alias().to_string());
        self.runtime_version = Some(execution.runtime().version.to_string());
        self.guest_os = Some(execution.guest().os.to_string());
        self.guest_arch = Some(execution.guest().architecture.to_string());
        if let Some(state_format) = execution.state_format() {
            self.state_format_name = Some(state_format.format.to_string());
            self.state_format_version = Some(state_format.version.to_string());
        }
        if let Some(kernel) = execution
            .components()
            .iter()
            .find(|component| component.kind == ArtifactComponentKind::Kernel)
        {
            self.kernel_version = kernel.version.as_ref().map(ToString::to_string);
            self.kernel_digest = kernel.digest.map(|digest| digest.to_string());
        }
        self
    }

    /// Mark a failure the caller already logs at WARN or ERROR. A failed-phase
    /// event otherwise logs at WARN itself; marked, it stays at INFO, so level
    /// filters see the failure once.
    pub fn logged_by_caller(mut self) -> Self {
        self.stays_at_info = true;
        self
    }

    /// Mark an expected failed-phase outcome, such as a rejection the caller
    /// answers with a conflict or a capacity refusal, that is part of normal
    /// operation rather than an anomaly. It stays at INFO.
    pub fn routine(mut self) -> Self {
        self.stays_at_info = true;
        self
    }

    pub fn restore_provenance(mut self, value: SandboxRestoreProvenance) -> Self {
        self.restore_provenance = Some(value.as_str().to_string());
        self
    }

    pub fn shape_source(mut self, value: CreateShapeSource) -> Self {
        self.shape_source = Some(value.as_str().to_string());
        self
    }

    pub fn with_from_state(mut self, value: impl Into<String>) -> Self {
        self.from_state = Some(value.into());
        self
    }

    pub fn with_to_state(mut self, value: impl Into<String>) -> Self {
        self.to_state = Some(value.into());
        self
    }

    pub fn bool_field(mut self, field: SandboxEventBoolField, value: bool) -> Self {
        match field {
            SandboxEventBoolField::RestoreStorageUriPresent => {
                self.restore_storage_uri_present = Some(value);
            }
            SandboxEventBoolField::HasNetnsPath => self.has_netns_path = Some(value),
            SandboxEventBoolField::HasPodNetns => self.has_pod_netns = Some(value),
            SandboxEventBoolField::Force => self.force = Some(value),
            SandboxEventBoolField::KeepFilesystem => self.keep_filesystem = Some(value),
            SandboxEventBoolField::ReturnAfterSlotRelease => {
                self.return_after_slot_release = Some(value);
            }
            SandboxEventBoolField::ImmediateKill => self.immediate_kill = Some(value),
            SandboxEventBoolField::Background => self.background = Some(value),
            SandboxEventBoolField::CwdPresent => self.cwd_present = Some(value),
            SandboxEventBoolField::VolumeEncryption => self.volume_encryption = Some(value),
        }
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxEventBoolField {
    RestoreStorageUriPresent,
    HasNetnsPath,
    HasPodNetns,
    Force,
    KeepFilesystem,
    ReturnAfterSlotRelease,
    ImmediateKill,
    Background,
    CwdPresent,
    VolumeEncryption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxRestoreProvenance {
    Fresh,
    LocalOnly,
    LocalUnknown,
    RemoteMatchesLocal,
    RemoteLocalMismatch,
    RemoteOnly,
    RemoteMetadataUnknown,
}

impl SandboxRestoreProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::LocalOnly => "local_only",
            Self::LocalUnknown => "local_unknown",
            Self::RemoteMatchesLocal => "remote_matches_local",
            Self::RemoteLocalMismatch => "remote_local_mismatch",
            Self::RemoteOnly => "remote_only",
            Self::RemoteMetadataUnknown => "remote_metadata_unknown",
        }
    }
}

impl std::fmt::Display for SandboxRestoreProvenance {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Where a create's vCPU and memory came from, reported on `create_completed`.
/// A partial request reports the fallback that filled the gap (`build` or
/// `default`), not `request`. `default` means the node shim boots the unset
/// fields at its fixed defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateShapeSource {
    Request,
    PoolConfig,
    Policy,
    Build,
    Default,
}

impl CreateShapeSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Request => "request",
            Self::PoolConfig => "pool_config",
            Self::Policy => "policy",
            Self::Build => "build",
            Self::Default => "default",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxEventTrigger {
    User,
    AutoLifecycle,
    System,
}

impl SandboxEventTrigger {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::AutoLifecycle => "auto_lifecycle",
            Self::System => "system",
        }
    }
}

impl std::fmt::Display for SandboxEventTrigger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub(crate) fn detail_str(value: &Option<String>) -> Option<&str> {
    value.as_deref()
}

pub(crate) fn detail_bool(value: Option<bool>) -> Option<bool> {
    value
}
