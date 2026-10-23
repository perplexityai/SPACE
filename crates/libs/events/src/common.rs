use pplx_space_common_core::types::{ClusterId, SandboxId, process_cluster_id};

use crate::details::{SandboxEventDetails, detail_bool, detail_str};
use crate::event_type::SandboxEventType;
use crate::phase::SandboxEventPhase;
use crate::source::SandboxEventSource;

pub const SANDBOX_EVENT_STREAM: &str = "space.sandbox";
pub const SANDBOX_EVENT_SCHEMA_VERSION: u16 = 1;

/// The process's cluster id as an event-field string.
///
/// Every sandbox event carries this so a single Datadog org can reconstruct a
/// per-cluster timeline. Sourced from the shared process-global
/// [`process_cluster_id`] (the single owner shared with the metrics sink), so
/// events and metrics never disagree; an absent/invalid id renders as empty.
fn cluster_id_field() -> &'static str {
    process_cluster_id()
        .map(ClusterId::as_str)
        .unwrap_or_default()
}

/// One sandbox event record. `tracing` fixes a call site's level, so
/// [`SandboxEvent::emit_with`] expands this once per level it uses.
macro_rules! sandbox_event {
    ($level:expr, $event:ident, $details:ident) => {
        tracing::event!(
            $level,
            event_stream = SANDBOX_EVENT_STREAM,
            event_schema_version = SANDBOX_EVENT_SCHEMA_VERSION,
            event_type = %$event.event_type,
            sandbox_id = %$event.sandbox_id,
            source = %$event.source,
            cluster_id = cluster_id_field(),
            phase = $event.event_type.phase().as_str(),
            action = detail_str(&$details.action),
            result = detail_str(&$details.result),
            error = detail_str(&$details.error),
            error_kind = detail_str(&$details.error_kind),
            operation = detail_str(&$details.operation),
            reason = detail_str(&$details.reason),
            trigger = detail_str(&$details.trigger),
            from_state = detail_str(&$details.from_state),
            to_state = detail_str(&$details.to_state),
            target_state = detail_str(&$details.target_state),
            lifecycle_state = detail_str(&$details.lifecycle_state),
            placement = detail_str(&$details.placement),
            execution_generation = detail_str(&$details.execution_generation),
            route_activation_id = detail_str(&$details.route_activation_id),
            receipt_id = detail_str(&$details.receipt_id),
            claim_id = detail_str(&$details.claim_id),
            pool_slot_id = detail_str(&$details.pool_slot_id),
            pod_name = detail_str(&$details.pod_name),
            pod_uid = detail_str(&$details.pod_uid),
            pod_ip = detail_str(&$details.pod_ip),
            k8s_node_name = detail_str(&$details.k8s_node_name),
            source_node_id = detail_str(&$details.source_node_id),
            source_node_name = detail_str(&$details.source_node_name),
            target_node_id = detail_str(&$details.target_node_id),
            target_node_name = detail_str(&$details.target_node_name),
            restore_storage_uri_present = detail_bool($details.restore_storage_uri_present),
            has_netns_path = detail_bool($details.has_netns_path),
            has_pod_netns = detail_bool($details.has_pod_netns),
            network_slot = detail_str(&$details.network_slot),
            network_slot_generation = detail_str(&$details.network_slot_generation),
            network_slot_label = detail_str(&$details.network_slot_label),
            network_slots_total = detail_str(&$details.network_slots_total),
            network_slots_ready = detail_str(&$details.network_slots_ready),
            network_slots_leased = detail_str(&$details.network_slots_leased),
            network_slots_dirty = detail_str(&$details.network_slots_dirty),
            network_slots_resetting = detail_str(&$details.network_slots_resetting),
            network_slots_quarantined = detail_str(&$details.network_slots_quarantined),
            network_slots_degraded = detail_str(&$details.network_slots_degraded),
            sandbox_uid = detail_str(&$details.sandbox_uid),
            pid = detail_str(&$details.pid),
            spaced_host_pid = detail_str(&$details.spaced_host_pid),
            net_gateway_pid = detail_str(&$details.net_gateway_pid),
            net_gateway_build_git_sha = detail_str(&$details.net_gateway_build_git_sha),
            program = detail_str(&$details.program),
            boot_type = detail_str(&$details.boot_type),
            vcpu = detail_str(&$details.vcpu),
            memory_mib = detail_str(&$details.memory_mib),
            disk_mib = detail_str(&$details.disk_mib),
            shape_source = detail_str(&$details.shape_source),
            guest_cid = detail_str(&$details.guest_cid),
            guest_ip = detail_str(&$details.guest_ip),
            guest_gateway = detail_str(&$details.guest_gateway),
            guest_uptime_ms = detail_str(&$details.guest_uptime_ms),
            timeout_secs = detail_str(&$details.timeout_secs),
            snapshot_id = detail_str(&$details.snapshot_id),
            rows_affected = detail_str(&$details.rows_affected),
            elapsed_ms = detail_str(&$details.elapsed_ms),
            ms_since_wake = detail_str(&$details.ms_since_wake),
            age_secs = detail_str(&$details.age_secs),
            action_count = detail_str(&$details.action_count),
            error_count = detail_str(&$details.error_count),
            force = detail_bool($details.force),
            keep_filesystem = detail_bool($details.keep_filesystem),
            return_after_slot_release = detail_bool($details.return_after_slot_release),
            immediate_kill = detail_bool($details.immediate_kill),
            background = detail_bool($details.background),
            cwd_present = detail_bool($details.cwd_present),
            volume_encryption = detail_bool($details.volume_encryption),
            condition_set = detail_str(&$details.condition_set),
            stage_index = detail_str(&$details.stage_index),
            due_at = detail_str(&$details.due_at),
            requested_template = detail_str(&$details.requested_template),
            template_id = detail_str(&$details.template_id),
            template_content_hash = detail_str(&$details.template_content_hash),
            template_release_tags = detail_str(&$details.template_release_tags),
            runtime = detail_str(&$details.runtime),
            runtime_version = detail_str(&$details.runtime_version),
            guest_os = detail_str(&$details.guest_os),
            guest_arch = detail_str(&$details.guest_arch),
            state_format_name = detail_str(&$details.state_format_name),
            state_format_version = detail_str(&$details.state_format_version),
            kernel_version = detail_str(&$details.kernel_version),
            kernel_digest = detail_str(&$details.kernel_digest),
            restored_snapshot_id = detail_str(&$details.restored_snapshot_id),
            restored_snapshot_local_artifact_id = detail_str(&$details.restored_snapshot_local_artifact_id),
            restore_provenance = detail_str(&$details.restore_provenance),
            snapshot_metadata_sha256 = detail_str(&$details.snapshot_metadata_sha256),
            snapshot_rootfs_version = detail_str(&$details.snapshot_rootfs_version),
            snapshot_kernel_version = detail_str(&$details.snapshot_kernel_version),
            restore_codec = detail_str(&$details.restore_codec),
            rootfs_diff_format = detail_str(&$details.rootfs_diff_format),
            host_port = detail_str(&$details.host_port),
            guest_port = detail_str(&$details.guest_port),
            protocol = detail_str(&$details.protocol),
            visibility = detail_str(&$details.visibility),
            netns = detail_str(&$details.netns),
            vpeer_ip = detail_str(&$details.vpeer_ip),
            ingress_iface = detail_str(&$details.ingress_iface),
            bridge_status = detail_str(&$details.bridge_status),
            service_generation = detail_str(&$details.service_generation),
            network_policy_generation = detail_str(&$details.network_policy_generation),
            expected_network_policy_generation = detail_str(&$details.expected_network_policy_generation),
            network_policy_source = detail_str(&$details.network_policy_source),
            network_policy_authority = detail_str(&$details.network_policy_authority),
            expected_network_policy_ref = detail_str(&$details.expected_network_policy_ref),
            installed_network_policy_ref = detail_str(&$details.installed_network_policy_ref),
            network_state = detail_str(&$details.network_state),
            interception_readiness = detail_str(&$details.interception_readiness),
            tls_interception = detail_str(&$details.tls_interception),
            artifact = detail_str(&$details.artifact),
            local_artifact_id = detail_str(&$details.local_artifact_id),
            artifact_kind = detail_str(&$details.artifact_kind),
            artifact_origin = detail_str(&$details.artifact_origin),
            backup_attempt_id = detail_str(&$details.backup_attempt_id),
            snapshot_upload_job_id = detail_str(&$details.snapshot_upload_job_id),
            transfer_job_id = detail_str(&$details.transfer_job_id),
            bytes_transferred = detail_str(&$details.bytes_transferred),
            local_path = detail_str(&$details.local_path),
            object_uri = detail_str(&$details.object_uri),
            owner_source = detail_str(&$details.owner_source),
            upstream_issuer = detail_str(&$details.upstream_issuer),
            upstream_org_id = detail_str(&$details.upstream_org_id),
            upstream_subject_id = detail_str(&$details.upstream_subject_id),
            "space sandbox event",
        )
    };
}

#[derive(Debug, Clone, Copy)]
pub struct SandboxEvent {
    sandbox_id: SandboxId,
    event_type: SandboxEventType,
    source: SandboxEventSource,
}

impl SandboxEvent {
    pub const fn new(
        sandbox_id: SandboxId,
        event_type: SandboxEventType,
        source: SandboxEventSource,
    ) -> Self {
        Self {
            sandbox_id,
            event_type,
            source,
        }
    }

    pub fn emit(self) {
        self.emit_with(SandboxEventDetails::default());
    }

    /// Failures log at WARN so level filters surface them; everything else logs
    /// at INFO. A failure stays at INFO when the caller already logs it
    /// ([`SandboxEventDetails::logged_by_caller`]) or it is an expected outcome
    /// ([`SandboxEventDetails::routine`]), and a lost lifecycle CAS always does:
    /// the writer that won reports the transition, and the loser's own error
    /// path logs any failure its caller sees. The fields and message are the
    /// same at either level.
    pub fn emit_with(self, details: SandboxEventDetails) {
        match self.event_type.phase() {
            SandboxEventPhase::Failed
                if !details.stays_at_info
                    && self.event_type != SandboxEventType::LifecycleTransitionCasLost =>
            {
                sandbox_event!(tracing::Level::WARN, self, details)
            }
            _ => sandbox_event!(tracing::Level::INFO, self, details),
        }
        crate::observer::notify(&self, &details);
    }

    pub const fn sandbox_id(&self) -> SandboxId {
        self.sandbox_id
    }

    pub const fn event_type(&self) -> SandboxEventType {
        self.event_type
    }

    pub const fn source(&self) -> SandboxEventSource {
        self.source
    }
}

pub fn emit_sandbox_event(
    sandbox_id: &str,
    event_type: SandboxEventType,
    source: SandboxEventSource,
    details: SandboxEventDetails,
) {
    match sandbox_id.parse::<SandboxId>() {
        Ok(sandbox_id) => SandboxEvent::new(sandbox_id, event_type, source).emit_with(details),
        Err(error) => {
            tracing::warn!(
                event_stream = SANDBOX_EVENT_STREAM,
                event_schema_version = SANDBOX_EVENT_SCHEMA_VERSION,
                event_type = %event_type,
                sandbox_id,
                source = %source,
                cluster_id = cluster_id_field(),
                error = %error,
                "dropping sandbox event with invalid sandbox_id",
            );
        }
    }
}
