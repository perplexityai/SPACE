use super::*;
use pplx_space_common_core::types::{ExactStorageUri, TransferScopeId};

#[test]
fn corrupt_restore_quarantines_only_the_current_attempt_and_retains_its_source() {
    for state in [
        SandboxState::Restoring,
        SandboxState::Suspended,
        SandboxState::Deleting,
    ] {
        let conn = test_conn();
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, TestSandboxInsert::new(&id, "corrupt1", state));
        let source: ExactStorageUri = "s3://snapshot-bucket/backup-one?region=us-east-1"
            .parse()
            .unwrap();
        let scope = RestoreTransferScopeRow {
            transfer_scope_id: TransferScopeId::new(),
            execution_generation: Some(ExecutionGeneration::new(1).unwrap()),
            activation_attempt_id: Some(SandboxActivationAttemptId::new()),
            staging_dir: "/cache/restore-staging/attempt-one".into(),
        };
        insert_restore_transfer_scope(
            &conn,
            &id,
            scope.transfer_scope_id,
            scope.execution_generation,
            scope.activation_attempt_id,
            &scope.staging_dir,
        )
        .unwrap();
        // A late response from an old activation must leave both state and
        // quarantine evidence untouched, even when it reused the same scope ID.
        let mut stale = scope.clone();
        stale.activation_attempt_id = Some(SandboxActivationAttemptId::new());
        assert!(!quarantine_corrupt_restore(&conn, &id, &source, &stale, None).unwrap());
        assert_eq!(get_sandbox(&conn, &id).unwrap().unwrap().state, state);
        assert_eq!(
            quarantined_corrupt_restore_source(&conn, &id).unwrap(),
            None
        );
        let expected = state != SandboxState::Deleting;
        assert_eq!(
            quarantine_corrupt_restore(&conn, &id, &source, &scope, None).unwrap(),
            expected
        );
        if expected {
            assert_eq!(
                get_sandbox(&conn, &id).unwrap().unwrap().state,
                SandboxState::Quarantined
            );
            assert!(
                scan_quarantine_recovery_candidates(&conn)
                    .unwrap()
                    .candidates
                    .is_empty()
            );
            retire_restore_transfer_scope_if_matches(&conn, &id, scope.transfer_scope_id).unwrap();
            assert_eq!(
                quarantined_corrupt_restore_source(&conn, &id)
                    .unwrap()
                    .as_deref(),
                Some(source.as_wire())
            );
            let reason: String = conn
                .query_row(
                    "SELECT reason_code FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
                    params![id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                reason
                    .parse::<SandboxQuarantineReasonCode>()
                    .unwrap()
                    .automatic_recovery_policy(),
                None
            );
        } else {
            assert_eq!(get_sandbox(&conn, &id).unwrap().unwrap().state, state);
        }
    }
}
