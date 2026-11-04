//! Shared quarantine/recovery `SandboxInfo` fixtures for wire-shape snapshot
//! tests, exposed under `cfg(test)` for this crate's own suite and under the
//! `test-support` feature for external dev-dependents (sbx-mgr, node-ctl).
//!
//! sbx-mgr and node-ctl each snapshot a different transform of the same
//! underlying [`SandboxInfo`] — sbx-mgr snapshots it directly, node-ctl
//! snapshots `published_sandbox_state` over it — so building it here does not
//! merge their assertions or `.snap` files; it only removes the duplicated
//! construction.

use pplx_space_common_core::types::SandboxQuarantineReasonCode;

use crate::proto::{ArtifactKind, ArtifactOrigin, SandboxInfo, TransitionReceipt};

/// Sandbox id shared by every fixture in this module.
pub const QUARANTINE_FIXTURE_SANDBOX_ID: &str = "019e6370-cfc9-7a43-9337-399c7b51a1d9";

/// Local artifact id [`with_suspend_artifact`] attaches.
pub const QUARANTINE_FIXTURE_ARTIFACT_ID: &str = "019e6370-cfc9-7a43-9337-399c7b51a1db";

/// Transition receipt id [`recovered_suspended_sandbox_info_fixture`] attaches.
pub const QUARANTINE_FIXTURE_RECEIPT_ID: &str = "019e6370-cfc9-7a43-9337-399c7b51a1da";

/// Build a quarantined `SandboxInfo`: fixed sandbox id, template `seed`,
/// status `quarantined`. `failure_reason` and `quarantine_reason_code` are
/// caller-supplied because sbx-mgr and node-ctl assert different wording for
/// each.
pub fn quarantined_sandbox_info_fixture(
    failure_reason: impl Into<String>,
    quarantine_reason_code: impl Into<String>,
) -> SandboxInfo {
    SandboxInfo {
        sandbox_id: QUARANTINE_FIXTURE_SANDBOX_ID.to_string(),
        template: Some("seed".to_string()),
        lifecycle: Some(crate::proto::SandboxLifecycleState::Quarantined as i32),
        failure_reason: Some(failure_reason.into()),
        quarantine_reason_code: Some(quarantine_reason_code.into()),
        ..SandboxInfo::default()
    }
}

/// Build a `SandboxInfo` recovered from quarantine back to `suspended`, with
/// the `TransitionReceipt` shape both sbx-mgr and node-ctl assert against.
pub fn recovered_suspended_sandbox_info_fixture() -> SandboxInfo {
    SandboxInfo {
        sandbox_id: QUARANTINE_FIXTURE_SANDBOX_ID.to_string(),
        template: Some("seed".to_string()),
        lifecycle: Some(crate::proto::SandboxLifecycleState::Suspended as i32),
        transition_receipt: Some(TransitionReceipt {
            id: QUARANTINE_FIXTURE_RECEIPT_ID.to_string(),
            kind: "quarantine_recovery".to_string(),
            target_state: "suspended".to_string(),
            execution_generation: 7,
            reason_code: SandboxQuarantineReasonCode::RestorePrepareFailed.to_string(),
            activation_attempt_id: None,
        }),
        ..SandboxInfo::default()
    }
}

/// Attach the full, `Suspend`-origin current-local-artifact triple that
/// node-ctl's published-state fixtures set and sbx-mgr's do not.
pub fn with_suspend_artifact(mut sandbox: SandboxInfo) -> SandboxInfo {
    sandbox.current_local_artifact_id = Some(QUARANTINE_FIXTURE_ARTIFACT_ID.to_string());
    sandbox.current_local_artifact_kind = Some(ArtifactKind::Full as i32);
    sandbox.current_local_artifact_origin = Some(ArtifactOrigin::Suspend as i32);
    sandbox
}
