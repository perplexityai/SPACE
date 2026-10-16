use super::*;

const UNUSED: &str = "00000000-0000-0000-0000-000000000001";

fn history(conn: &Connection, id: &str, sandbox_id: &str, occurred_at: &str) {
    conn.execute(
        "INSERT INTO sandbox_quarantine_history
         (id, quarantine_id, sandbox_id, occurred_at, source, action)
         VALUES (?1, 'episode', ?2, ?3, 'sbx_manager', 'entered')",
        params![id, sandbox_id, occurred_at],
    )
    .unwrap();
}

#[test]
fn retention_is_bounded_and_preserves_recent_malformed_and_active_evidence() {
    let conn = crate::db::test_conn();
    let now = RETENTION_SECS + 100;
    for index in 0..ROWS_PER_BATCH {
        history(&conn, &format!("malformed-{index}"), UNUSED, "bad-time");
    }
    history(&conn, "expired", UNUSED, "99");
    history(&conn, "boundary", UNUSED, "100");
    history(&conn, "recent", UNUSED, &now.to_string());
    let active = crate::db::tests::setup_quarantine_recovery_episode(
        &conn,
        pplx_space_common_core::types::SandboxQuarantineReasonCode::OrphanedSandboxRowMissing,
    );
    history(&conn, "active-old-episode", &active.to_string(), "1");
    let mut cursor = QuarantineHistoryPruner::default();
    let first = cursor.next_batch(&conn, now).unwrap();
    assert_eq!(first.examined, ROWS_PER_BATCH);
    assert_eq!(first.deleted, 0);
    // Inserts must not extend the in-progress sweep.
    history(&conn, "inserted-during-sweep", UNUSED, "1");
    assert_eq!(cursor.next_batch(&conn, now).unwrap().deleted, 1);
    let remains = |id: &str| {
        conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sandbox_quarantine_history WHERE id = ?1)",
            [id],
            |row| row.get::<_, bool>(0),
        )
        .unwrap()
    };
    assert!(!remains("expired"));
    for id in [
        "boundary",
        "recent",
        "active-old-episode",
        "inserted-during-sweep",
        "malformed-0",
    ] {
        assert!(remains(id), "must retain {id}");
    }
    assert_eq!(cursor.next_batch(&conn, now).unwrap().deleted, 0);
    assert_eq!(cursor.next_batch(&conn, now).unwrap().deleted, 1);
    assert!(!remains("inserted-during-sweep"));
    // Completing recovery removes its protection, not its history immediately.
    conn.execute(
        "DELETE FROM sandbox_quarantine_state WHERE sandbox_id = ?1",
        [active.to_string()],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET state = 'stopped' WHERE id = ?1",
        [active.to_string()],
    )
    .unwrap();
    cursor.next_batch(&conn, now).unwrap();
    cursor.next_batch(&conn, now).unwrap();
    assert!(!remains("active-old-episode"));
}

#[test]
fn retention_keeps_cleanup_transfer_and_unacknowledged_delete_obligations() {
    let conn = crate::db::test_conn();
    conn.execute_batch(
        "INSERT INTO sandboxes (id, uuid8, state, template, runtime, created_at, updated_at)
         VALUES ('00000000-0000-0000-0000-000000000001', 'cleanup1', 'creating', 'template', 'fc', '1', '1');
         INSERT INTO sandbox_cleanup_episodes
            (sandbox_id, cleanup_id, completion, keep_filesystem, phase, activation_state,
             requested_at, execution_generation, attempt_count, created_at)
         VALUES ('00000000-0000-0000-0000-000000000001', 'cleanup-episode', 'delete_row', 0, 'requested', 'creating',
                 1, 7, 3, 1);
         INSERT INTO sandbox_restore_transfer_scopes (sandbox_id, transfer_scope_id, staging_dir)
         VALUES ('00000000-0000-0000-0000-000000000002', 'scope', '/tmp/staging');
         INSERT INTO auto_lifecycle_delete_outbox
            (receipt_id, sandbox_id, execution_generation, route_activation_id,
             auto_lifecycle_policy_fingerprint, state, created_at)
         VALUES ('receipt', '00000000-0000-0000-0000-000000000003', 1, 'activation', 'fingerprint', 'pending', '1');",
    )
    .unwrap();
    for id in [
        UNUSED,
        "00000000-0000-0000-0000-000000000002",
        "00000000-0000-0000-0000-000000000003",
    ] {
        history(&conn, id, id, "1");
        // Aliases parse as UUIDs but do not match the canonical ownership keys.
        history(&conn, &format!("alias-{id}"), &id.replace('-', ""), "1");
    }
    let mut cursor = QuarantineHistoryPruner::default();
    assert_eq!(
        cursor
            .next_batch(&conn, RETENTION_SECS + 2)
            .unwrap()
            .deleted,
        0
    );
    conn.execute_batch(
        "DELETE FROM sandbox_cleanup_episodes;
         UPDATE sandboxes SET state = 'stopped';
         DELETE FROM sandbox_restore_transfer_scopes;
         DELETE FROM auto_lifecycle_delete_outbox;",
    )
    .unwrap();
    assert_eq!(
        cursor
            .next_batch(&conn, RETENTION_SECS + 2)
            .unwrap()
            .deleted,
        3
    );
}

#[test]
fn failed_batch_rolls_back_and_retries_without_removing_unidentifiable_evidence() {
    let conn = crate::db::test_conn();
    history(&conn, "first", UNUSED, "1");
    history(&conn, "second", UNUSED, "1");
    history(&conn, "bad-identity", "not-a-sandbox", "1");
    for (id, timestamp) in [
        ("negative", "-1"),
        ("fractional", "1.5"),
        ("overflow", "18446744073709551616"),
    ] {
        history(&conn, id, UNUSED, timestamp);
    }
    conn.execute_batch(
        "CREATE TEMP TRIGGER reject_second_history_delete
         BEFORE DELETE ON sandbox_quarantine_history WHEN OLD.id = 'second'
         BEGIN SELECT RAISE(ABORT, 'test storage failure'); END;",
    )
    .unwrap();
    let mut cursor = QuarantineHistoryPruner::default();
    cursor.next_batch(&conn, RETENTION_SECS + 2).unwrap_err();
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sandbox_quarantine_history",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        6
    );
    conn.execute_batch("DROP TRIGGER reject_second_history_delete")
        .unwrap();
    let retried = cursor.next_batch(&conn, RETENTION_SECS + 2).unwrap();
    assert_eq!(retried.examined, 6);
    assert_eq!(retried.deleted, 2);
}
