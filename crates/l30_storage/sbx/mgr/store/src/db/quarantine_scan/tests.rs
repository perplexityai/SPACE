use pplx_space_common_core::types::SandboxId;

use super::*;
use crate::db::tests::setup_quarantine_recovery_episode;
use pplx_space_common_core::types::SandboxQuarantineReasonCode;

#[test]
fn partial_sweeps_do_not_renew_freshness_but_late_completion_does() {
    let conn = crate::db::test_conn();
    for _ in 0..9 {
        episode(&conn);
    }
    let mut cursor = PeriodicQuarantineRecoveryScan::default();
    assert!(!cursor.has_recent_completed_sweep());
    cursor.next_batch(&conn).unwrap();
    assert!(!cursor.has_recent_completed_sweep());
    assert!(cursor.next_batch(&conn).unwrap().completed_sweep.is_some());
    let completed = cursor.last_completed.unwrap();
    assert!(cursor.has_recent_completed_sweep_at(completed + Duration::from_secs(899)));
    assert!(!cursor.has_recent_completed_sweep_at(completed + Duration::from_secs(900)));
    assert!(cursor.next_batch(&conn).unwrap().completed_sweep.is_none());
    assert!(!cursor.has_recent_completed_sweep_at(completed + Duration::from_secs(901)));
    cursor.last_completed = Some(Instant::now() - Duration::from_secs(3600));
    assert!(cursor.next_batch(&conn).unwrap().completed_sweep.is_some());
    assert!(cursor.has_recent_completed_sweep());
}

fn episode(conn: &Connection) -> SandboxId {
    setup_quarantine_recovery_episode(conn, SandboxQuarantineReasonCode::OrphanedSandboxRowMissing)
}

#[test]
fn finite_sweep_progresses_past_retained_rows_and_every_due_candidate() {
    let conn = crate::db::test_conn();
    for index in 0..RAW_ROWS_PER_BATCH {
        let id = episode(&conn).to_string();
        if index == 0 {
            // A surviving quarantine row with no sandbox consumes raw scan
            // budget but is neither an eligible nor a malformed candidate.
            let foreign_keys: bool = conn
                .pragma_query_value(None, "foreign_keys", |row| row.get(0))
                .unwrap();
            conn.pragma_update(None, "foreign_keys", false).unwrap();
            conn.execute("DELETE FROM sandboxes WHERE id = ?1", [&id])
                .unwrap();
            conn.pragma_update(None, "foreign_keys", foreign_keys)
                .unwrap();
        } else if index.is_multiple_of(2) {
            conn.execute(
                "UPDATE sandbox_quarantine_state SET next_action_owner = 'operator_attention'
                 WHERE sandbox_id = ?1",
                [&id],
            )
            .unwrap();
        } else {
            conn.execute(
                "UPDATE sandbox_quarantine_state SET reason_code = X'00' WHERE sandbox_id = ?1",
                [&id],
            )
            .unwrap();
        }
    }
    let expected: Vec<_> = (0..17).map(|_| episode(&conn)).collect();
    conn.execute("UPDATE sandbox_quarantine_state SET entered_at = '0'", [])
        .unwrap();

    let mut cursor = PeriodicQuarantineRecoveryScan::default();
    let first = cursor.next_batch(&conn).unwrap();
    assert!(
        first.candidates.is_empty(),
        "one page cannot scan past the retained prefix"
    );
    assert!(
        first.completed_sweep.is_none(),
        "a page is not a full backlog sample"
    );

    // Inserting after the sweep starts must not extend that sweep indefinitely.
    let fresh = episode(&conn);
    let mut observed = Vec::new();
    for expected_len in [8, 8, 1] {
        let batch = cursor.next_batch(&conn).unwrap();
        assert_eq!(batch.candidates.len(), expected_len);
        observed.extend(
            batch
                .candidates
                .into_iter()
                .map(|candidate| candidate.sandbox_id),
        );
        if expected_len == 1 {
            let summary = batch.completed_sweep.unwrap();
            assert_eq!(summary.pending_count, 17);
            assert_eq!(summary.malformed_count, 64);
            assert!(summary.pending_oldest_age.as_secs() > 0);
        } else {
            assert!(batch.completed_sweep.is_none());
        }
    }
    assert_eq!(observed, expected);
    assert!(!observed.contains(&fresh));

    // Still-due first candidates must not starve later ones after wrapping.
    let mut next_sweep = Vec::new();
    let mut summary = None;
    for _ in 0..4 {
        let batch = cursor.next_batch(&conn).unwrap();
        next_sweep.extend(
            batch
                .candidates
                .into_iter()
                .map(|candidate| candidate.sandbox_id),
        );
        if batch.completed_sweep.is_some() {
            summary = batch.completed_sweep;
            break;
        }
    }
    let summary = summary.expect("one retained page and three due batches complete the sweep");
    assert_eq!(
        next_sweep,
        expected.into_iter().chain([fresh]).collect::<Vec<_>>()
    );
    assert_eq!(summary.pending_count, 18);
    assert_eq!(summary.malformed_count, 64);
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM sandbox_quarantine_state", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap(),
        146
    );
}

#[test]
fn failed_page_restarts_the_sweep_instead_of_publishing_a_partial_backlog() {
    let conn = crate::db::test_conn();
    let expected: Vec<_> = (0..9).map(|_| episode(&conn)).collect();
    let mut cursor = PeriodicQuarantineRecoveryScan::default();
    assert_eq!(cursor.next_batch(&conn).unwrap().candidates.len(), 8);
    conn.execute_batch(
        "ALTER TABLE sandbox_quarantine_state RENAME TO unavailable_quarantine_state;",
    )
    .unwrap();
    cursor.next_batch(&conn).unwrap_err();
    conn.execute_batch(
        "ALTER TABLE unavailable_quarantine_state RENAME TO sandbox_quarantine_state;",
    )
    .unwrap();

    let restarted = cursor.next_batch(&conn).unwrap();
    assert_eq!(
        restarted
            .candidates
            .iter()
            .map(|candidate| candidate.sandbox_id)
            .collect::<Vec<_>>(),
        expected[..8]
    );
    assert!(restarted.completed_sweep.is_none());
    let last = cursor.next_batch(&conn).unwrap();
    assert_eq!(last.candidates[0].sandbox_id, expected[8]);
    assert_eq!(last.completed_sweep.unwrap().pending_count, 9);
    assert!(
        cursor
            .next_batch(&crate::db::test_conn())
            .unwrap()
            .completed_sweep
            .is_some()
    );
}
