use pplx_space_common_core::types::{SandboxId, SandboxLifecycle};
use pplx_space_net_common::NetworkPolicyV1;
use uuid::Uuid;

use super::ArtifactComponentProjection;

/// Checked publication evidence, including the original spelling of open labels.
/// The legacy JSON batch keeps nullable fields explicit for existing SQL readers.
#[derive(serde::Serialize)]
pub(super) struct PublishedSandboxRow {
    pub(super) sandbox_id: SandboxId,
    pub(super) status: Option<String>,
    pub(super) template: Option<String>,
    pub(super) runtime: Option<String>,
    pub(super) spaced_version: Option<String>,
    pub(super) runtime_version: Option<String>,
    pub(super) guest_os: Option<String>,
    pub(super) guest_arch: Option<String>,
    pub(super) state_format_name: Option<String>,
    pub(super) state_format_version: Option<String>,
    pub(super) execution_metadata: Option<serde_json::Value>,
    pub(super) execution_components: Vec<ArtifactComponentProjection>,
    pub(super) pid: Option<i32>,
    pub(super) namespace: Option<String>,
    pub(super) uptime: Option<String>,
    pub(super) sandbox_owner: Option<String>,
    pub(super) sandbox_user: Option<String>,
    pub(super) image_hash: Option<String>,
    pub(super) snapshots_enabled: bool,
    pub(super) pool_template: Option<String>,
    pub(super) failure_reason: Option<String>,
    pub(super) observed_created_at: Option<String>,
    pub(super) last_used_at: Option<String>,
    pub(super) last_access_at: Option<String>,
    pub(super) last_command_at: Option<String>,
    pub(super) network_tx_bytes: Option<i64>,
    pub(super) network_rx_bytes: Option<i64>,
    pub(super) disk_usage_bytes: Option<i64>,
    pub(super) observed_network_policy_json: Option<NetworkPolicyV1>,
    pub(super) observed_network_policy_generation: Option<i64>,
    pub(super) observed_network_policy_source: Option<&'static str>,
    pub(super) observed_network_policy_authority: Option<&'static str>,
    pub(super) auto_lifecycle_authority: Option<&'static str>,
    pub(super) quarantine_reason_code: Option<String>,
    pub(super) transition_receipt_id: Option<Uuid>,
    pub(super) transition_receipt_kind: Option<&'static str>,
    pub(super) transition_receipt_target_state: Option<&'static str>,
    pub(super) transition_receipt_execution_generation: Option<i64>,
    pub(super) transition_receipt_reason_code: Option<&'static str>,
    pub(super) transition_receipt_activation_attempt_id: Option<Uuid>,
    pub(super) quarantine_recovery_payload: Option<serde_json::Value>,
    pub(super) quarantine_recovery_observation_degraded: bool,
    pub(super) transition_receipt_recovery_policy: Option<String>,
    pub(super) current_local_artifact_id: Option<String>,
    pub(super) current_local_artifact_kind: Option<String>,
    pub(super) current_local_artifact_origin: Option<String>,
    pub(super) state_changed_at: Option<String>,
    pub(super) execution_generation: Option<i64>,
    pub(super) pod_uid: Option<String>,
}

/// A narrow SQL projection. Status remains an open label with its exact spelling;
/// the database decides lifecycle eligibility under its own collation.
#[derive(serde::Serialize)]
pub(super) struct PublishedLifecycleRow<'a> {
    sandbox_id: SandboxId,
    status: Option<&'a str>,
}

impl PublishedSandboxRow {
    pub(super) fn may_report_lifecycle(&self, expected: SandboxLifecycle) -> bool {
        self.status.as_deref().is_some_and(|status| {
            // Non-ASCII labels stay in the SQL input rather than assuming Rust
            // and the database have identical case conversion rules.
            !status.is_ascii() || status.eq_ignore_ascii_case(expected.as_str())
        })
    }

    pub(super) fn lifecycle_projection(&self) -> PublishedLifecycleRow<'_> {
        PublishedLifecycleRow {
            sandbox_id: self.sandbox_id,
            status: self.status.as_deref(),
        }
    }
}

/// Checked activity timestamps retain their serialized RFC3339 spelling so the
/// existing PostgreSQL timestamp casts and update thresholds remain authoritative.
#[derive(serde::Serialize)]
pub(super) struct PublishedActivityRow<'a> {
    sandbox_id: SandboxId,
    last_used_at: Option<&'a str>,
    last_access_at: Option<&'a str>,
    last_command_at: Option<&'a str>,
}

impl PublishedSandboxRow {
    pub(super) fn activity_projection(&self) -> PublishedActivityRow<'_> {
        PublishedActivityRow {
            sandbox_id: self.sandbox_id,
            last_used_at: self.last_used_at.as_deref(),
            last_access_at: self.last_access_at.as_deref(),
            last_command_at: self.last_command_at.as_deref(),
        }
    }
}

#[derive(serde::Serialize)]
pub(super) struct PublishedRestoreRow<'a> {
    sandbox_id: SandboxId,
    status: Option<&'a str>,
    transition_receipt_kind: Option<&'static str>,
}

impl PublishedSandboxRow {
    pub(super) fn restore_projection(&self) -> PublishedRestoreRow<'_> {
        PublishedRestoreRow {
            sandbox_id: self.sandbox_id,
            status: self.status.as_deref(),
            transition_receipt_kind: self.transition_receipt_kind,
        }
    }
}

#[derive(serde::Serialize)]
pub(super) struct PublishedRecoveryObservationRow<'a> {
    sandbox_id: SandboxId,
    quarantine_recovery_payload: Option<&'a serde_json::Value>,
    quarantine_recovery_observation_degraded: bool,
}

impl PublishedSandboxRow {
    /// jsonb_to_recordset treats JSON null and an absent value as SQL NULL.
    pub(super) fn has_recovery_observation(&self) -> bool {
        self.quarantine_recovery_payload
            .as_ref()
            .is_some_and(|payload| !payload.is_null())
    }

    pub(super) fn recovery_observation_projection(&self) -> PublishedRecoveryObservationRow<'_> {
        PublishedRecoveryObservationRow {
            sandbox_id: self.sandbox_id,
            quarantine_recovery_payload: self.quarantine_recovery_payload.as_ref(),
            quarantine_recovery_observation_degraded: self.quarantine_recovery_observation_degraded,
        }
    }
}
