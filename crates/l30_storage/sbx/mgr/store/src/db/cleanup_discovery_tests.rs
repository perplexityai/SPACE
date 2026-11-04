use super::*;
use crate::db::fixture::insert_test_sandbox;

#[test]
fn recovery_inventory_retains_malformed_keys_without_hiding_siblings() {
    let conn = test_conn();
    for value in [
        rusqlite::types::Value::Blob(vec![0xff]),
        rusqlite::types::Value::Null,
    ] {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Paused);
        conn.execute(
            "UPDATE sandboxes SET id = ?1 WHERE id = ?2",
            params![value, id],
        )
        .unwrap();
    }
    let good = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &good, SandboxState::Created);
    for _ in 0..2 {
        let rows = list_sandboxes_for_recovery(&conn).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, good);
    }
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM sandboxes", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        3
    );
    conn.execute_batch("DROP TABLE sandboxes").unwrap();
    assert!(list_sandboxes_for_recovery(&conn).is_err());
}

#[test]
fn recovery_scope_discovery_retains_bad_evidence_and_retires_valid_siblings() {
    let conn = test_conn();
    let bad = SandboxId::new().to_string();
    let good = SandboxId::new().to_string();
    let good_scope = pplx_space_common_core::types::TransferScopeId::new();
    for (id, scope) in [
        (&bad, pplx_space_common_core::types::TransferScopeId::new()),
        (&good, good_scope),
    ] {
        insert_restore_transfer_scope(&conn, id, scope, None, None, "/retained/staging").unwrap();
    }
    conn.execute("UPDATE sandbox_restore_transfer_scopes SET transfer_scope_id = 'malformed' WHERE sandbox_id = ?1", [&bad]).unwrap();
    let scopes = rowless_restore_transfer_scopes(&conn).unwrap();
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[0].0, good);
    assert!(retire_rowless_restore_transfer_scope(&conn, &good, good_scope).unwrap());
    assert!(rowless_restore_transfer_scopes(&conn).unwrap().is_empty());
    assert_eq!(
        conn.query_row(
            "SELECT transfer_scope_id FROM sandbox_restore_transfer_scopes WHERE sandbox_id = ?1",
            [&bad],
            |r| r.get::<_, String>(0)
        )
        .unwrap(),
        "malformed"
    );
    conn.execute_batch("DROP TABLE sandbox_restore_transfer_scopes")
        .unwrap();
    rowless_restore_transfer_scopes(&conn).expect_err("schema failure must propagate");
}

/// One due row per `SandboxState`, all with a due deadline; returns
/// `(state, id)` pairs (`SandboxState` is not `Ord`, so no map).
fn one_due_row_per_state(conn: &Connection) -> Vec<(SandboxState, SandboxId)> {
    let rows = SandboxState::all()
        .into_iter()
        .map(|state| {
            let id = SandboxId::new();
            insert_test_sandbox(conn, &id.to_string(), state);
            (state, id)
        })
        .collect::<Vec<_>>();
    conn.execute("UPDATE sandboxes SET next_auto_event_at = 1", [])
        .unwrap();
    rows
}

#[test]
fn rowid_lane_skips_only_deleting_rows_and_still_advances_past_them() {
    // A `deleting` row can take no idle action and is never due work; every
    // other state — a retained `restoring` row included — must stay
    // selectable, because a due Delete stage has to reach it (the state
    // machine force-deletes from transient states) and the per-action gate
    // lives in the transition. Drive the rowid lane alone (the deadline lane
    // gets zero slots when `available == 1` on its off turn, so alternate
    // turns are skipped by starting on the rowid turn) and keep calling
    // until every non-deleting state has been seen or a bounded budget is
    // spent — the lane must advance past the skipped row, not stall on it.
    let conn = test_conn();
    let rows = one_due_row_per_state(&conn);
    let mut cursor = AutoLifecycleScan {
        after: i64::MIN,
        fresh_turn: false,
    };
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..64 {
        for due in due_auto_lifecycle_batch(&conn, 100, &[], 1, &mut cursor).unwrap() {
            seen.insert(due.sandbox_id);
        }
        if seen.len() == rows.len() - 1 {
            break;
        }
    }
    for (state, id) in &rows {
        if *state == SandboxState::Deleting {
            assert!(!seen.contains(id), "deleting must never be due");
        } else {
            assert!(seen.contains(id), "{state} must stay selectable");
        }
    }
}

#[test]
fn deadline_lane_alone_skips_deleting_rows() {
    // The SQL predicate is the deadline lane's only filter. Park the rowid
    // cursor past every row so that lane contributes nothing, and give the
    // deadline lane enough slots (`available / 2`) to take every row.
    let conn = test_conn();
    let rows = one_due_row_per_state(&conn);
    let mut cursor = AutoLifecycleScan {
        after: i64::MAX,
        fresh_turn: true,
    };
    let selected = due_auto_lifecycle_batch(&conn, 100, &[], 2 * rows.len(), &mut cursor).unwrap();
    let ids: std::collections::BTreeSet<_> = selected.iter().map(|d| d.sandbox_id).collect();
    for (state, id) in &rows {
        if *state == SandboxState::Deleting {
            assert!(
                !ids.contains(id),
                "deleting must never be due via the deadline lane"
            );
        } else {
            assert!(
                ids.contains(id),
                "{state} must be due via the deadline lane"
            );
        }
    }
}

#[test]
fn auto_lifecycle_action_gate_is_exhaustive_over_states() {
    use pplx_space_common_core::types::AutoLifecycleIdleAction as Action;
    let transient = [
        SandboxState::Creating,
        SandboxState::Starting,
        SandboxState::Stopping,
        SandboxState::Deleting,
        SandboxState::Pausing,
        SandboxState::Resuming,
        SandboxState::Checkpointing,
        SandboxState::Suspending,
        SandboxState::Restoring,
    ];
    for state in SandboxState::all() {
        let settled = !transient.contains(&state);
        for action in [Action::Pause, Action::Suspend, Action::Stop] {
            assert_eq!(
                auto_lifecycle_action_can_act(action, state),
                settled,
                "{action} on {state}"
            );
        }
        assert_eq!(
            auto_lifecycle_action_can_act(Action::Delete, state),
            state.can_transition_to(SandboxState::Deleting),
            "Delete on {state} follows the state machine"
        );
        assert!(auto_lifecycle_action_can_act(Action::None, state));
    }
    // The retained-restore shape specifically: Suspend records nothing,
    // Delete still fires (force-delete contract).
    assert!(!auto_lifecycle_action_can_act(
        Action::Suspend,
        SandboxState::Restoring
    ));
    assert!(auto_lifecycle_action_can_act(
        Action::Delete,
        SandboxState::Restoring
    ));
}

#[test]
fn recovery_auto_lifecycle_scan_progresses_past_malformed_storage() {
    for available in [1, 8] {
        let conn = test_conn();
        let bad_key = SandboxId::new().to_string();
        let bad_deadline = SandboxId::new().to_string();
        let good = SandboxId::new();
        for id in [&bad_key, &bad_deadline, &good.to_string()] {
            insert_test_sandbox(&conn, id, SandboxState::Started);
        }
        conn.execute("UPDATE sandboxes SET next_auto_event_at = 1", [])
            .unwrap();
        conn.execute(
            "UPDATE sandboxes SET id = X'FF', next_auto_event_at = 2 WHERE id = ?1",
            [&bad_key],
        )
        .unwrap();
        conn.execute(
            "UPDATE sandboxes SET next_auto_event_at = 'malformed' WHERE id = ?1",
            [&bad_deadline],
        )
        .unwrap();
        let mut cursor = AutoLifecycleScan::default();
        let mut visits = 0;
        for _ in 0..8 {
            visits += due_auto_lifecycle_batch(&conn, 10, &[], available, &mut cursor)
                .unwrap()
                .iter()
                .filter(|row| row.sandbox_id == good)
                .count();
        }
        assert!(
            visits >= 2,
            "retained malformed rows must not prevent repeated sweeps"
        );
        assert_eq!(
            conn.query_row("SELECT COUNT(*) FROM sandboxes", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            3
        );
    }
}

#[test]
fn discovery_progresses_past_blocked_work_and_idle_pages_and_prioritizes_fresh_work() {
    for available in [1, 8] {
        let conn = test_conn();
        let mut healthy = Vec::new();
        for n in 0..400 {
            let id = SandboxId::new().to_string();
            insert_test_sandbox(&conn, &id, SandboxState::Started);
            let deadline = if n % 5 < 2 {
                200
            } else {
                healthy.push(id.parse::<SandboxId>().unwrap());
                1
            };
            conn.execute(
                "UPDATE sandboxes SET next_auto_event_at = ?1 WHERE id = ?2",
                params![deadline, id],
            )
            .unwrap();
        }
        let mut cursor = AutoLifecycleScan::default();
        let mut seen = std::collections::HashSet::new();
        // Every selected row remains due, modelling permanently blocked execution.
        // Progress must not depend on completing or rescheduling the edge rows.
        for _ in 0..600 {
            let batch = due_auto_lifecycle_batch(&conn, 100, &[], available, &mut cursor).unwrap();
            assert!(batch.len() <= available);
            seen.extend(batch.into_iter().map(|due| due.sandbox_id));
            if seen.len() == healthy.len() {
                break;
            }
        }
        assert!(healthy.iter().all(|id| seen.contains(id)));
        let fresh = SandboxId::new();
        insert_test_sandbox(&conn, &fresh.to_string(), SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET next_auto_event_at = 99 WHERE id = ?1",
            [fresh.to_string()],
        )
        .unwrap();
        let mut fresh_seen = false;
        for _ in 0..2 {
            fresh_seen |= due_auto_lifecycle_batch(&conn, 100, &[], available, &mut cursor)
                .unwrap()
                .iter()
                .any(|due| due.sandbox_id == fresh);
        }
        assert!(fresh_seen, "fresh work must not wait for backlog traversal");
        // A policy introduced behind the cursor is reconsidered after wrap.
        conn.execute("UPDATE sandboxes SET next_auto_event_at = NULL", [])
            .unwrap();
        due_auto_lifecycle_batch(&conn, 100, &[], available, &mut cursor).unwrap();
        conn.execute(
            "UPDATE sandboxes SET next_auto_event_at = 1 WHERE id = ?1",
            [healthy[0].to_string()],
        )
        .unwrap();
        assert!((0..8).any(|_| {
            due_auto_lifecycle_batch(&conn, 100, &[], available, &mut cursor)
                .unwrap()
                .iter()
                .any(|due| due.sandbox_id == healthy[0])
        }));
    }
}

#[test]
fn discovery_bounds_raw_sql_work_and_does_no_work_without_capacity() {
    let conn = test_conn();
    // A large due inventory must not be counted or scanned in full for one batch.
    conn.execute_batch("WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000) INSERT INTO sandboxes(id, uuid8, state, template, user, created_at, updated_at, next_auto_event_at) SELECT printf('019d84bd-280d-7331-99c6-%012x', x), 'test', 'started', 'test', 'test', '0', '0', 1 FROM n").unwrap();
    use rusqlite::trace::{TraceEvent, TraceEventCodes};
    static STEPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    fn trace(event: TraceEvent<'_>) {
        if let TraceEvent::Profile(statement, _) = event {
            STEPS.fetch_add(
                usize::try_from(statement.get_status(rusqlite::StatementStatus::VmStep)).unwrap(),
                std::sync::atomic::Ordering::Relaxed,
            );
        }
    }
    conn.trace_v2(TraceEventCodes::SQLITE_TRACE_PROFILE, Some(trace));
    let mut cursor = AutoLifecycleScan::default();
    for _ in 0..5 {
        STEPS.store(0, std::sync::atomic::Ordering::Relaxed);
        assert_eq!(
            due_auto_lifecycle_batch(&conn, 10, &[], 8, &mut cursor)
                .unwrap()
                .len(),
            8
        );
        assert!(
            STEPS.load(std::sync::atomic::Ordering::Relaxed) < 10000,
            "raw discovery exceeded its SQL work budget"
        );
    }
    conn.trace_v2(TraceEventCodes::empty(), None);
    conn.execute_batch("DROP TABLE sandboxes").unwrap();
    assert!(
        due_auto_lifecycle_batch(&conn, 10, &[], 0, &mut cursor)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn discovery_excludes_active_and_not_yet_due_work() {
    let conn = test_conn();
    let ids: Vec<_> = (0..4).map(|_| SandboxId::new()).collect();
    for (id, deadline) in ids.iter().zip([Some(1), Some(1), Some(11), None]) {
        let storage_id = id.to_string().to_uppercase();
        insert_test_sandbox(&conn, &storage_id, SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET next_auto_event_at = ?1 WHERE id = ?2",
            params![deadline, storage_id],
        )
        .unwrap();
    }
    let batch =
        due_auto_lifecycle_batch(&conn, 10, &[ids[0]], 8, &mut AutoLifecycleScan::default())
            .unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].sandbox_id, ids[1]);
    assert_eq!(batch[0].storage_id, ids[1].to_string().to_uppercase());
}

#[test]
fn lifecycle_deadline_write_rejects_out_of_range_without_partial_update() {
    let conn = test_conn();
    let id = SandboxId::new().to_string();
    crate::db::fixture::insert_test_sandbox(&conn, &id, SandboxState::Started);
    let mut state = AutoLifecycleRuntimeState {
        next_auto_event_at: Some(10),
        ..Default::default()
    };
    update_auto_lifecycle_state(&conn, &id, &state).unwrap();
    state.next_auto_event_at = Some(u64::MAX);
    assert!(update_auto_lifecycle_state(&conn, &id, &state).is_err());
    let (deadline, json): (i64, String) = conn
        .query_row(
            "SELECT next_auto_event_at, auto_lifecycle_state_json FROM sandboxes WHERE id = ?1",
            [&id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(deadline, 10);
    assert_eq!(
        serde_json::from_str::<AutoLifecycleRuntimeState>(&json)
            .unwrap()
            .next_auto_event_at,
        Some(10)
    );
}
