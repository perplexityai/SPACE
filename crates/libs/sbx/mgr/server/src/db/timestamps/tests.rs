use super::*;
use crate::db;

#[test]
fn whole_epoch_seconds_accept_legacy_encodings_without_coercing_corruption() {
    for value in [
        Value::Integer(1),
        Value::Text("1".into()),
        Value::Text("0001".into()),
        Value::Text("1970-01-01 00:00:01".into()),
        Value::Text("1970-01-01T00:00:01Z".into()),
        Value::Text("1970-01-01T01:00:01+01:00".into()),
    ] {
        assert_eq!(EpochSeconds::decode(&value).unwrap().get(), 1, "{value:?}");
    }
    for value in [
        Value::Null,
        Value::Integer(-1),
        Value::Real(1.0),
        Value::Blob(b"1".to_vec()),
        Value::Text("".into()),
        Value::Text("-1".into()),
        Value::Text("+1".into()),
        Value::Text(" 1".into()),
        Value::Text("1junk".into()),
        Value::Text("1e3".into()),
        Value::Text("9223372036854775808".into()),
        Value::Text("1969-12-31 23:59:59".into()),
        Value::Text("1970-01-01T00:00:01.5Z".into()),
        Value::Text("2025-02-29 00:00:00".into()),
    ] {
        assert!(EpochSeconds::decode(&value).is_err(), "{value:?}");
    }
}

#[test]
fn sandbox_timestamps_preserve_raw_cas_and_diagnostic_evidence() {
    let conn = db::test_conn();
    let (sandbox_id, _) = db::tests::setup_quarantine_recovery_candidate(&conn);
    conn.execute(
        "UPDATE sandboxes SET created_at = '1970-01-01 00:00:01',
         updated_at = '1970-01-01T00:00:01Z', state_changed_at = '1970-01-01 00:00:01'
         WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let row = db::get_sandbox(&conn, &sandbox_id.to_string())
        .unwrap()
        .unwrap();
    assert_eq!(row.created_at, "1");
    assert_eq!(row.updated_at, "1970-01-01T00:00:01Z");
    assert_eq!(
        db::raw_sandbox_lifecycle(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .state_changed_at,
        "1"
    );
    conn.execute(
        "UPDATE sandboxes SET state_changed_at = 'invalid-time', updated_at = 'invalid-time' WHERE id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    assert_eq!(
        db::get_sandbox(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .updated_at,
        "invalid-time"
    );
    assert_eq!(
        db::raw_sandbox_lifecycle(&conn, &sandbox_id.to_string())
            .unwrap()
            .unwrap()
            .state_changed_at,
        "invalid-time"
    );
}

#[test]
fn scope_normalization_is_bounded_progressing_and_preserves_raw_invalid_evidence() {
    let conn = db::test_conn();
    let insert = |id: usize, timestamp: &str| {
        conn.execute(
            "INSERT INTO sandbox_restore_transfer_scopes
             (sandbox_id, transfer_scope_id, staging_dir, created_at) VALUES (?1, ?1, '/staging', ?2)",
            params![id.to_string(), timestamp],
        ).unwrap();
    };
    for id in 0..128 {
        insert(id, "invalid-time");
    }
    insert(128, "1970-01-01 00:00:01");
    insert(129, "0001");
    let mut scan = RestoreScopeTimestampScan::default();
    assert_eq!(scan.next_batch(&conn).unwrap(), 0);
    insert(130, "1970-01-01T00:00:02Z");
    assert_eq!(scan.next_batch(&conn).unwrap(), 1);
    let stored = |id: usize| {
        conn.query_row(
            "SELECT created_at FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
            [id.to_string()],
            |row| row.get::<_, String>(0),
        )
        .unwrap()
    };
    assert_eq!(stored(0), "invalid-time");
    assert_eq!(stored(128), "1");
    assert_eq!(stored(129), "0001");
    assert_eq!(stored(130), "1970-01-01T00:00:02Z");
    assert_eq!(scan.next_batch(&conn).unwrap(), 0);
    assert_eq!(scan.next_batch(&conn).unwrap(), 1);
    assert_eq!(stored(130), "2");
}

#[test]
fn failed_normalization_rolls_back_and_retries_the_whole_page() {
    let conn = db::test_conn();
    conn.execute_batch(
        "INSERT INTO sandbox_restore_transfer_scopes
           (sandbox_id, transfer_scope_id, staging_dir, created_at)
         VALUES ('one', 'one', '/one', '1970-01-01 00:00:01'),
                ('two', 'two', '/two', '1970-01-01 00:00:02');
         CREATE TEMP TRIGGER reject_second_timestamp BEFORE UPDATE ON sandbox_restore_transfer_scopes
         WHEN OLD.sandbox_id = 'two' BEGIN SELECT RAISE(ABORT, 'injected failure'); END;",
    ).unwrap();
    let mut scan = RestoreScopeTimestampScan::default();
    scan.next_batch(&conn).unwrap_err();
    let first: String = conn
        .query_row(
            "SELECT created_at FROM sandbox_restore_transfer_scopes WHERE sandbox_id = 'one'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(first, "1970-01-01 00:00:01");
    conn.execute_batch("DROP TRIGGER reject_second_timestamp")
        .unwrap();
    assert_eq!(scan.next_batch(&conn).unwrap(), 2);
}

#[test]
fn quarantine_deadline_claims_and_live_fences_reject_sqlite_numeric_prefixes() {
    for malformed in [
        "garbage",
        "-1",
        "1.5",
        "1e3",
        "99999999999999junk",
        "9223372036854775808",
    ] {
        for column in ["next_action_after", "claim_expires_at"] {
            let conn = db::test_conn();
            let (sandbox_id, candidate) = db::tests::setup_quarantine_recovery_candidate(&conn);
            let sql = match column {
                "next_action_after" => {
                    "UPDATE sandbox_quarantine_state SET next_action_after = ?1 WHERE sandbox_id = ?2"
                }
                _ => {
                    "UPDATE sandbox_quarantine_state SET claim_expires_at = ?1 WHERE sandbox_id = ?2"
                }
            };
            conn.execute(sql, params![malformed, sandbox_id.to_string()])
                .unwrap();
            assert!(
                db::claim_quarantine_recovery_candidate(
                    &conn,
                    &candidate,
                    crate::db::PlanRejectionRunEffect::Leaves
                )
                .unwrap()
                .is_none(),
                "{column}={malformed}"
            );
        }
        let conn = db::test_conn();
        let (sandbox_id, candidate) = db::tests::setup_quarantine_recovery_candidate(&conn);
        let claimed = db::claim_quarantine_recovery_candidate(
            &conn,
            &candidate,
            crate::db::PlanRejectionRunEffect::Leaves,
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "UPDATE sandbox_quarantine_state SET claim_expires_at = ?1 WHERE sandbox_id = ?2",
            params![malformed, sandbox_id.to_string()],
        )
        .unwrap();
        assert!(!db::quarantine_recovery_claim_is_current(&conn, &claimed).unwrap());
        assert!(!db::renew_quarantine_recovery_claim(&conn, &claimed).unwrap());
    }
}

#[test]
fn legacy_iso_deadlines_remain_claimable_and_raw_lease_evidence_is_cas_fenced() {
    let conn = db::test_conn();
    let (sandbox_id, candidate) = db::tests::setup_quarantine_recovery_candidate(&conn);
    conn.execute(
        "UPDATE sandbox_quarantine_state SET next_action_after = '1970-01-01 00:00:01'
         WHERE sandbox_id = ?1",
        [sandbox_id.to_string()],
    )
    .unwrap();
    let claimed = db::claim_quarantine_recovery_candidate(
        &conn,
        &candidate,
        crate::db::PlanRejectionRunEffect::Leaves,
    )
    .unwrap()
    .unwrap();
    let now = db::now_epoch_i64().unwrap();
    let future = jiff::Timestamp::from_second(now + 900).unwrap().to_string();
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_expires_at = ?1 WHERE sandbox_id = ?2",
        params![future, sandbox_id.to_string()],
    )
    .unwrap();
    assert!(db::quarantine_recovery_claim_is_current(&conn, &claimed).unwrap());
    let fence = db::ExactClaimFence::new(
        &conn,
        &claimed,
        now,
        db::LeaseRequirement::MustBeLive,
        db::SandboxStateRequirement::AnyState,
        db::CleanupRequirement::MayExist,
    )
    .unwrap();
    conn.execute(
        "UPDATE sandbox_quarantine_state SET claim_expires_at = ?1 WHERE sandbox_id = ?2",
        params![(now + 1800).to_string(), sandbox_id.to_string()],
    )
    .unwrap();
    let matches: bool = conn
        .prepare(&format!(
            "SELECT EXISTS(SELECT 1 FROM sandbox_quarantine_state WHERE {})",
            fence.quarantine_row_predicate(""),
        ))
        .unwrap()
        .query_row(&fence.binds()[..], |row| row.get(0))
        .unwrap();
    assert!(
        !matches,
        "a changed deadline cannot satisfy a previously read lease fence"
    );
    assert!(db::renew_quarantine_recovery_claim(&conn, &claimed).unwrap());
}

#[test]
fn malformed_expiry_cannot_clear_ownership_when_quarantine_is_reclassified() {
    for state in ["quarantined", "restoring"] {
        let conn = db::test_conn();
        let (sandbox_id, candidate) = db::tests::setup_quarantine_recovery_candidate(&conn);
        let claimed = db::claim_quarantine_recovery_candidate(
            &conn,
            &candidate,
            crate::db::PlanRejectionRunEffect::Leaves,
        )
        .unwrap()
        .unwrap();
        conn.execute(
            "UPDATE sandboxes SET state = ?1 WHERE id = ?2",
            params![state, sandbox_id.to_string()],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandbox_quarantine_state SET claim_expires_at = 'invalid-expiry' WHERE sandbox_id = ?1",
            [sandbox_id.to_string()],
        ).unwrap();
        db::mark_quarantined(
            &conn,
            &sandbox_id.to_string(),
            db::SandboxQuarantineSource::SbxManager,
            db::SandboxQuarantineReasonCode::RestorePrepareFailed,
            "reclassified",
            db::SandboxQuarantineNextAction::RequiresOperatorAttention,
        )
        .unwrap();
        let (_, attempts, _, token, expiry) = db::tests::quarantine_claim_state(&conn, sandbox_id);
        // New episodes reset pacing while retaining the previous writer's
        // exclusion; same-episode reconfirmation inherits both.
        let expected_attempts = if state == "restoring" {
            0
        } else {
            i64::from(claimed.candidate.attempt_count)
        };
        assert_eq!(attempts, expected_attempts, "{state}");
        assert_eq!(token, Some(claimed.claim_token.to_string()), "{state}");
        assert_eq!(expiry.as_deref(), Some("invalid-expiry"), "{state}");
    }
}
