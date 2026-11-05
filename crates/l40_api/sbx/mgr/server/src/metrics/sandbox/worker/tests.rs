use super::*;

#[test]
fn quarantine_recovery_no_progress_reasons_have_stable_tags() {
    // These shared reason strings also persist in sandbox_quarantine_state.last_error.
    // Keep that readable vocabulary stable independently of metric emission.
    let expected = [
        (
            QuarantineRecoveryNoProgressReason::PlanNotFound,
            "plan_not_found",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanIneligible,
            "plan_ineligible",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanRpcUnsupported,
            "plan_rpc_unsupported",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanMalformed,
            "plan_malformed",
        ),
        (
            QuarantineRecoveryNoProgressReason::UnsupportedTargetState,
            "unsupported_target_state",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanReasonIneligible,
            "plan_reason_ineligible",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanLookupFailed,
            "plan_lookup_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanLookupTimedOut,
            "plan_lookup_timed_out",
        ),
        (
            QuarantineRecoveryNoProgressReason::LocalReasonMismatch,
            "local_reason_mismatch",
        ),
        (
            QuarantineRecoveryNoProgressReason::LocalArtifactMismatch,
            "local_artifact_mismatch",
        ),
        (
            QuarantineRecoveryNoProgressReason::CleanupFenceBusy,
            "cleanup_fence_busy",
        ),
        (
            QuarantineRecoveryNoProgressReason::LocalArtifactInspectionFailed,
            "local_artifact_inspection_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged,
            "local_adoption_fence_changed",
        ),
        (
            QuarantineRecoveryNoProgressReason::LocalAdoptionFailed,
            "local_adoption_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred,
            "orphan_restore_deferred",
        ),
        (
            QuarantineRecoveryNoProgressReason::OrphanRestoreLocalContention,
            "orphan_restore_local_contention",
        ),
        (
            QuarantineRecoveryNoProgressReason::OrphanRestoreTaskFailed,
            "orphan_restore_task_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::OrphanRestoreTimedOut,
            "orphan_restore_timed_out",
        ),
        (
            QuarantineRecoveryNoProgressReason::ClaimSuperseded,
            "claim_superseded",
        ),
        (
            QuarantineRecoveryNoProgressReason::ClaimPreconditionsChanged,
            "claim_preconditions_changed",
        ),
        (
            QuarantineRecoveryNoProgressReason::ClaimAcquisitionFailed,
            "claim_acquisition_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::AbandonedAtShutdown,
            "abandoned_at_shutdown",
        ),
        (
            QuarantineRecoveryNoProgressReason::PlanArtifactMismatch,
            "plan_artifact_mismatch",
        ),
        (
            QuarantineRecoveryNoProgressReason::ClaimRenewalFailed,
            "claim_renewal_failed",
        ),
        (
            QuarantineRecoveryNoProgressReason::NodeDatabaseFailure,
            "node_database_failure",
        ),
        (
            QuarantineRecoveryNoProgressReason::NodeFilesystemFailure,
            "node_filesystem_failure",
        ),
        (
            QuarantineRecoveryNoProgressReason::RestoreEvidenceMismatch,
            "restore_evidence_mismatch",
        ),
        (
            QuarantineRecoveryNoProgressReason::ControlPlaneRefused,
            "control_plane_refused",
        ),
    ];
    for (reason, wire) in expected {
        assert_eq!(reason.as_str(), wire);
        assert_eq!(reason.to_string(), wire);
    }

    // What is and is not enforced here, precisely. The match below is exhaustive, so a
    // new variant fails to compile until it is given an arm with its wire string — that
    // is the pin, and it is what a rename has to defeat twice. Membership of the array
    // above is *not* enforced: a variant with an arm but no array entry compiles and is
    // simply never iterated. An earlier version of this comment claimed the array was
    // covered too, which is the kind of claim that stops anyone checking.
    for (reason, wire) in expected {
        // One arm per variant, each naming its wire string independently of
        // `as_str()`. Most are also persisted verbatim into
        // `sandbox_quarantine_state.last_error`, so a rename forks the durable column
        // as well as the metric. The pre-claim reasons —
        // `ClaimPreconditionsChanged` and `ClaimAcquisitionFailed` — and the post-claim
        // `OrphanRestoreTaskFailed` are metric-only and never written to the episode.
        let pinned = match reason {
            QuarantineRecoveryNoProgressReason::PlanNotFound => "plan_not_found",
            QuarantineRecoveryNoProgressReason::PlanIneligible => "plan_ineligible",
            QuarantineRecoveryNoProgressReason::PlanRpcUnsupported => "plan_rpc_unsupported",
            QuarantineRecoveryNoProgressReason::PlanMalformed => "plan_malformed",
            QuarantineRecoveryNoProgressReason::UnsupportedTargetState => {
                "unsupported_target_state"
            }
            QuarantineRecoveryNoProgressReason::PlanReasonIneligible => "plan_reason_ineligible",
            QuarantineRecoveryNoProgressReason::PlanLookupFailed => "plan_lookup_failed",
            QuarantineRecoveryNoProgressReason::PlanLookupTimedOut => "plan_lookup_timed_out",
            QuarantineRecoveryNoProgressReason::LocalReasonMismatch => "local_reason_mismatch",
            QuarantineRecoveryNoProgressReason::LocalArtifactMismatch => "local_artifact_mismatch",
            QuarantineRecoveryNoProgressReason::PlanArtifactMismatch => "plan_artifact_mismatch",
            QuarantineRecoveryNoProgressReason::ClaimRenewalFailed => "claim_renewal_failed",
            QuarantineRecoveryNoProgressReason::NodeDatabaseFailure => "node_database_failure",
            QuarantineRecoveryNoProgressReason::NodeFilesystemFailure => "node_filesystem_failure",
            QuarantineRecoveryNoProgressReason::RestoreEvidenceMismatch => {
                "restore_evidence_mismatch"
            }
            QuarantineRecoveryNoProgressReason::ControlPlaneRefused => "control_plane_refused",
            QuarantineRecoveryNoProgressReason::CleanupFenceBusy => "cleanup_fence_busy",
            QuarantineRecoveryNoProgressReason::LocalArtifactInspectionFailed => {
                "local_artifact_inspection_failed"
            }
            QuarantineRecoveryNoProgressReason::LocalAdoptionFenceChanged => {
                "local_adoption_fence_changed"
            }
            QuarantineRecoveryNoProgressReason::LocalAdoptionFailed => "local_adoption_failed",
            QuarantineRecoveryNoProgressReason::OrphanRestoreLocalContention => {
                "orphan_restore_local_contention"
            }
            QuarantineRecoveryNoProgressReason::OrphanRestoreTaskFailed => {
                "orphan_restore_task_failed"
            }
            QuarantineRecoveryNoProgressReason::OrphanRestoreDeferred => "orphan_restore_deferred",
            QuarantineRecoveryNoProgressReason::OrphanRestoreTimedOut => "orphan_restore_timed_out",
            QuarantineRecoveryNoProgressReason::ClaimSuperseded => "claim_superseded",
            QuarantineRecoveryNoProgressReason::ClaimPreconditionsChanged => {
                "claim_preconditions_changed"
            }
            QuarantineRecoveryNoProgressReason::ClaimAcquisitionFailed => {
                "claim_acquisition_failed"
            }
            QuarantineRecoveryNoProgressReason::AbandonedAtShutdown => "abandoned_at_shutdown",
        };
        assert_eq!(
            reason.as_str(),
            pinned,
            "the wire string changed without the pin being updated"
        );
        assert_eq!(pinned, wire);
    }
}
