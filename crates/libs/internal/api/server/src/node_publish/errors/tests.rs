use pplx_space_common_core::types::{SandboxCleanupEpisodeId, SandboxId};
use tonic::{Code, Status};
use uuid::Uuid;

use super::*;

#[test]
fn quarantine_invariant_failures_keep_aborted_status() {
    let sandbox_id = SandboxId::from_uuid(Uuid::nil());
    let receipt_id = SandboxCleanupEpisodeId::from_uuid(Uuid::nil());
    let cases = [
        (
            ApplySandboxClaimsError::ActivationRollback(
                ApplyActivationRollbacksError::LostOwnership(receipt_id),
            ),
            "activation rollback receipt 00000000-0000-0000-0000-000000000000 lost sandbox ownership",
        ),
        (
            ApplySandboxClaimsError::PauseRecovery(RecoverPauseFailedError::LostOwnership(
                sandbox_id,
            )),
            "pause-failed recovery CAS failed for 00000000-0000-0000-0000-000000000000",
        ),
        (
            ApplySandboxClaimsError::RecoveryHistory(RecordQuarantineRecoveryError::HistoryCas {
                sandbox_id,
                rows: 0,
            }),
            "quarantine recovery history CAS affected 0 rows for 00000000-0000-0000-0000-000000000000",
        ),
        (
            ApplySandboxClaimsError::RecoveryHistory(
                RecordQuarantineRecoveryError::RetirementCas {
                    sandbox_id,
                    rows: 0,
                },
            ),
            "quarantine recovery retirement CAS affected 0 rows for 00000000-0000-0000-0000-000000000000",
        ),
    ];
    for (error, message) in cases {
        let status = Status::from(ApplyPublishError::Sandboxes(error));
        assert_eq!(status.code(), Code::Aborted);
        assert_eq!(status.message(), message);
    }
}
