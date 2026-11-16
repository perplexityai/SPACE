//! Cleanup ownership is stored apart from the lifecycle: `state` keeps the
//! lifecycle and `cleanup_state` the cleanup that owns the row. These pin the
//! stored columns the cleanup writers leave; the other suites pin the combined
//! status every read decodes from the two.

use super::*;

/// `(state, cleanup_state)` as stored, and the combined status they decode to.
fn stored(conn: &Connection, id: &str) -> (String, Option<String>, String) {
    conn.query_row(
        &format!(
            "SELECT state, cleanup_state,
                {}
           FROM sandboxes WHERE id = ?1",
            crate::db::fixture::STATUS_LABEL_SQL
        ),
        params![id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn state_changed_at(conn: &Connection, id: &str) -> String {
    conn.query_row(
        "SELECT state_changed_at FROM sandboxes WHERE id = ?1",
        params![id],
        |row| row.get(0),
    )
    .unwrap()
}

fn set_state_changed_at(conn: &Connection, id: &str, value: &str) {
    conn.execute(
        "UPDATE sandboxes SET state_changed_at = ?1 WHERE id = ?2",
        params![value, id],
    )
    .unwrap();
}

#[test]
fn a_cleanup_handoff_stores_the_teardown_transition_its_intent_names() {
    let conn = test_conn();
    for (from, completion, lifecycle) in [
        (
            SandboxState::Stopping,
            CleanupCompletion::DeleteRow,
            "deleting",
        ),
        (
            SandboxState::Stopping,
            CleanupCompletion::MarkStopped,
            "stopping",
        ),
        (
            SandboxState::Starting,
            CleanupCompletion::MarkCreated,
            "stopping",
        ),
        (
            SandboxState::Starting,
            CleanupCompletion::Quarantine,
            "stopping",
        ),
        (
            SandboxState::Suspending,
            CleanupCompletion::MarkSuspended,
            "suspending",
        ),
    ] {
        let id = state_write_fixtures::insert_row(&conn, from);
        mark_cleanup_required(&conn, &id, !completion.deletes_row(), completion, "handoff")
            .unwrap();
        assert_eq!(
            stored(&conn, &id),
            (
                lifecycle.to_owned(),
                Some("required".to_owned()),
                "cleanup_required".to_owned()
            ),
            "{from} {completion}"
        );
        assert_eq!(
            get_sandbox(&conn, &id).unwrap().unwrap().state,
            SandboxStatus::owned(
                Teardown::of(lifecycle.parse().unwrap()).unwrap(),
                CleanupState::Required
            ),
            "{from} {completion}"
        );
    }
}

#[test]
fn a_plain_teardown_failure_is_its_cleanup_state_s_first_failure() {
    let conn = test_conn();
    let id = state_write_fixtures::insert_row(&conn, SandboxState::Stopping);
    set_state_changed_at(&conn, &id, "0");

    assert!(
        mark_cleanup_failed(
            &conn,
            &id,
            true,
            CleanupCompletion::MarkStopped,
            "teardown failed"
        )
        .unwrap()
    );
    assert_eq!(
        stored(&conn, &id),
        (
            "stopping".to_owned(),
            Some("failed".to_owned()),
            "cleanup_failed".to_owned()
        )
    );
    assert_ne!(
        state_changed_at(&conn, &id),
        "0",
        "entering cleanup_failed is a change"
    );

    // A repeated failure leaves the combined status, and so its time, alone.
    set_state_changed_at(&conn, &id, "1");
    mark_cleanup_failed(
        &conn,
        &id,
        true,
        CleanupCompletion::MarkStopped,
        "teardown failed",
    )
    .unwrap();
    assert_eq!(state_changed_at(&conn, &id), "1");

    // Re-requesting a failed cleanup moves it back to required.
    mark_cleanup_required(&conn, &id, true, CleanupCompletion::MarkStopped, "retry").unwrap();
    assert_eq!(
        stored(&conn, &id),
        (
            "stopping".to_owned(),
            Some("required".to_owned()),
            "cleanup_required".to_owned()
        )
    );
    assert_ne!(
        state_changed_at(&conn, &id),
        "1",
        "cleanup_failed to cleanup_required"
    );
}

#[test]
fn a_lifecycle_write_over_a_cleanup_row_clears_its_cleanup_state() {
    let conn = test_conn();
    let id = state_write_fixtures::insert_row(&conn, SandboxState::Stopping);
    mark_cleanup_required(&conn, &id, false, CleanupCompletion::DeleteRow, "handoff").unwrap();
    assert_eq!(stored(&conn, &id).0, "deleting");

    mark_deleting_superseding_activation(&conn, &id).unwrap();
    assert_eq!(
        stored(&conn, &id),
        ("deleting".to_owned(), None, "deleting".to_owned())
    );
}

#[test]
fn an_inserted_cleanup_row_starts_in_stopping() {
    let conn = test_conn();
    let id = state_write_fixtures::insert_row(&conn, SandboxStatus::CLEANUP_REQUIRED);
    assert_eq!(
        stored(&conn, &id),
        (
            "stopping".to_owned(),
            Some("required".to_owned()),
            "cleanup_required".to_owned()
        )
    );
    assert_eq!(
        get_sandbox(&conn, &id).unwrap().unwrap().state,
        SandboxStatus::CLEANUP_REQUIRED
    );
}

const MIGRATION_UP: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261105142930_sandbox_cleanup_state.up.sql"
);
const MIGRATION_DOWN: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261105142930_sandbox_cleanup_state.down.sql"
);
/// The migration after it adds `unowned_state`, which reads `cleanup_state`.
const UNOWNED_UP: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261108130000_sandbox_unowned_state.up.sql"
);
const UNOWNED_DOWN: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261108130000_sandbox_unowned_state.down.sql"
);
/// The release after drops `legacy_state`, which the split's down migration
/// reads.
const LEGACY_DROP_UP: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261109120000_drop_sandbox_legacy_state.up.sql"
);
const LEGACY_DROP_DOWN: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261109120000_drop_sandbox_legacy_state.down.sql"
);
/// The migration after `suspend_attempt` restores `legacy_state` for releases
/// that still read it.
const LEGACY_RESTORE_UP: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261113190000_restore_sandbox_legacy_state.up.sql"
);
const LEGACY_RESTORE_DOWN: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261113190000_restore_sandbox_legacy_state.down.sql"
);
/// The migration after the restore drops `legacy_state` again, once no
/// rollback target reads it.
const LEGACY_REDROP_UP: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261114020000_redrop_sandbox_legacy_state.up.sql"
);
const LEGACY_REDROP_DOWN: &str = include_str!(
    "../../../../../../../../db/sandbox-manager/20261114020000_redrop_sandbox_legacy_state.down.sql"
);

/// Insert a row in the split form, keeping the filesystem for every
/// completion that keeps the row.
fn insert_split(
    conn: &Connection,
    id: &str,
    state: &str,
    cleanup_state: Option<&str>,
    completion: Option<&str>,
) -> crate::db::Result<usize> {
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, cleanup_state, cleanup_completion, cleanup_keep_filesystem,
             template, created_at, updated_at
         ) VALUES (
             ?1, 'sandbox1', ?2, ?3, ?4,
             CASE WHEN ?4 IS NULL THEN NULL
                  WHEN ?4 IN ('delete_row', 'delete_pool_clone') THEN 0
                  ELSE 1 END,
             'template', '0', '0'
         )",
        params![id, state, cleanup_state, completion],
    )
}

fn refused_by_cleanup_trigger(result: crate::db::Result<usize>) -> bool {
    match result {
        Err(error) => error.to_string().contains("invalid sandbox cleanup state"),
        Ok(_) => false,
    }
}

#[test]
fn the_cleanup_triggers_admit_only_a_teardown_transition_its_intent_allows() {
    let conn = test_conn();
    for (index, (state, cleanup_state, completion, admitted)) in [
        ("stopping", "required", None, true),
        ("stopping", "failed", Some("mark_created"), true),
        ("stopping", "required", Some("mark_suspended"), true),
        ("deleting", "required", Some("delete_row"), true),
        ("deleting", "failed", Some("delete_pool_clone"), true),
        ("suspending", "failed", Some("mark_suspended"), true),
        ("started", "required", None, false),
        ("deleting", "required", None, false),
        ("deleting", "failed", Some("mark_stopped"), false),
        ("suspending", "required", None, false),
        ("suspending", "failed", Some("mark_stopped"), false),
        ("stopping", "required", Some("delete_row"), false),
        ("stopping", "failed", Some("delete_pool_clone"), false),
    ]
    .into_iter()
    .enumerate()
    {
        let case = format!("{state} {cleanup_state} {completion:?}");
        let result = insert_split(
            &conn,
            &format!("insert-{index}"),
            state,
            Some(cleanup_state),
            completion,
        );
        if admitted {
            result.unwrap_or_else(|error| panic!("{case}: {error}"));
        } else {
            assert!(refused_by_cleanup_trigger(result), "{case}");
        }
    }

    // An update must keep the shape too: a row-deleting intent moves the row
    // to `deleting` in the same statement, or is refused.
    insert_split(&conn, "update", "stopping", Some("required"), None).unwrap();
    assert!(refused_by_cleanup_trigger(conn.execute(
        "UPDATE sandboxes SET cleanup_completion = 'delete_row', cleanup_keep_filesystem = 0
          WHERE id = 'update'",
        [],
    )));
    assert!(refused_by_cleanup_trigger(conn.execute(
        "UPDATE sandboxes SET state = 'started' WHERE id = 'update'",
        [],
    )));
    conn.execute(
        "UPDATE sandboxes
            SET state = 'deleting', cleanup_completion = 'delete_row', cleanup_keep_filesystem = 0
          WHERE id = 'update'",
        [],
    )
    .unwrap();
    assert_eq!(stored(&conn, "update").0, "deleting");
}

/// `unowned_state` is the lifecycle while no cleanup owns the row and NULL
/// while one does, so a predicate on it never matches a cleanup.
#[test]
fn unowned_state_is_the_lifecycle_only_while_no_cleanup_owns_the_row() {
    let conn = test_conn();
    for (id, state, cleanup_state, completion, expected) in [
        ("a", "stopping", None, None, Some("stopping")),
        ("b", "stopping", Some("required"), None, None),
        ("c", "deleting", Some("failed"), Some("delete_row"), None),
        (
            "d",
            "suspending",
            Some("required"),
            Some("mark_suspended"),
            None,
        ),
        ("e", "started", None, None, Some("started")),
    ] {
        insert_split(&conn, id, state, cleanup_state, completion).unwrap();
        let unowned: Option<String> = conn
            .query_row(
                "SELECT unowned_state FROM sandboxes WHERE id = ?1",
                params![id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unowned.as_deref(), expected, "{id}");
    }
}

#[test]
fn the_migration_round_trips_every_cleanup_shape_and_keeps_historical_labels() {
    let conn = test_conn();
    for (id, state, cleanup_state, completion) in [
        ("a", "stopping", Some("required"), None),
        ("b", "deleting", Some("failed"), Some("delete_row")),
        ("c", "suspending", Some("required"), Some("mark_suspended")),
        ("d", "deleting", Some("required"), Some("delete_pool_clone")),
        ("e", "stopping", Some("failed"), Some("mark_stopped")),
        ("f", "started", None, None),
    ] {
        insert_split(&conn, id, state, cleanup_state, completion).unwrap();
    }
    seed_historical_lifecycle_rows(&conn, |conn| {
        insert_split(conn, "g", "future_state", None, None).unwrap();
    });
    let labels = |conn: &Connection| -> Vec<(String, String)> {
        conn.prepare("SELECT id, state FROM sandboxes ORDER BY id")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    };
    let owned = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(id, label)| ((*id).to_owned(), (*label).to_owned()))
            .collect()
    };

    // Reverse dependent migrations newest first.
    conn.execute_batch(LEGACY_REDROP_DOWN).unwrap();
    conn.execute_batch(LEGACY_RESTORE_DOWN).unwrap();
    conn.execute_batch(include_str!(
        "../../../../../../../../db/sandbox-manager/20261112160000_suspend_attempt.down.sql"
    ))
    .unwrap();
    conn.execute_batch(LEGACY_DROP_DOWN).unwrap();
    conn.execute_batch(UNOWNED_DOWN).unwrap();
    conn.execute_batch(MIGRATION_DOWN).unwrap();
    assert_eq!(
        labels(&conn),
        owned(&[
            ("a", "cleanup_required"),
            ("b", "cleanup_failed"),
            ("c", "cleanup_required"),
            ("d", "cleanup_required"),
            ("e", "cleanup_failed"),
            ("f", "started"),
            ("g", "future_state"),
        ])
    );

    conn.execute_batch(MIGRATION_UP).unwrap();
    conn.execute_batch(UNOWNED_UP).unwrap();
    conn.execute_batch(LEGACY_DROP_UP).unwrap();
    conn.execute_batch(include_str!(
        "../../../../../../../../db/sandbox-manager/20261112160000_suspend_attempt.up.sql"
    ))
    .unwrap();
    conn.execute_batch(LEGACY_RESTORE_UP).unwrap();
    conn.execute_batch(LEGACY_REDROP_UP).unwrap();
    // A migration can't tell a handed-off suspend from a restore rollback, so
    // `c` comes back `stopping`; its combined status is unchanged.
    for (id, expected) in [
        ("a", ("stopping", Some("required"), "cleanup_required")),
        ("b", ("deleting", Some("failed"), "cleanup_failed")),
        ("c", ("stopping", Some("required"), "cleanup_required")),
        ("d", ("deleting", Some("required"), "cleanup_required")),
        ("e", ("stopping", Some("failed"), "cleanup_failed")),
        ("f", ("started", None, "started")),
        ("g", ("future_state", None, "future_state")),
    ] {
        let (state, cleanup_state, legacy) = expected;
        assert_eq!(
            stored(&conn, id),
            (
                state.to_owned(),
                cleanup_state.map(str::to_owned),
                legacy.to_owned()
            ),
            "{id}"
        );
    }
}

#[test]
fn each_cleanup_writer_stores_the_teardown_transition_its_intent_names() {
    let conn = test_conn();

    // A plain suspend's first failure stays `suspending`.
    let id = state_write_fixtures::insert_row(&conn, SandboxState::Suspending);
    mark_cleanup_failed(&conn, &id, true, CleanupCompletion::MarkSuspended, "x").unwrap();
    assert_eq!(stored(&conn, &id).0, "suspending");
    assert_eq!(stored(&conn, &id).1.as_deref(), Some("failed"));

    // A plain delete's first failure stays `deleting`.
    let id = state_write_fixtures::insert_row(&conn, SandboxState::Deleting);
    mark_cleanup_failed(&conn, &id, false, CleanupCompletion::DeleteRow, "x").unwrap();
    assert_eq!(stored(&conn, &id).0, "deleting");

    // Changing a cleanup row's intent moves it to the transition the new
    // intent names, and clearing the intent leaves the cleanup over `stopping`.
    let id = state_write_fixtures::insert_row(&conn, SandboxStatus::CLEANUP_REQUIRED);
    assert!(set_cleanup_intent(&conn, &id, false, CleanupCompletion::DeleteRow).unwrap());
    assert_eq!(stored(&conn, &id).0, "deleting");
    clear_cleanup_intent(&conn, &id).unwrap();
    assert_eq!(
        stored(&conn, &id),
        (
            "stopping".to_owned(),
            Some("required".to_owned()),
            "cleanup_required".to_owned()
        )
    );
    assert!(set_cleanup_intent(&conn, &id, true, CleanupCompletion::MarkSuspended).unwrap());
    assert_eq!(stored(&conn, &id).0, "stopping");
}

/// Rows that share a lifecycle but not a cleanup, with no intent, episode or
/// runtime fields: the reads that decide from the combined status must tell
/// them apart.
fn insert_intentless(conn: &Connection, state: &str, cleanup_state: Option<&str>) -> String {
    let id = state_write_fixtures::insert_row(conn, SandboxState::Started);
    conn.execute(
        "UPDATE sandboxes
            SET state = ?1, cleanup_state = ?2,
                cleanup_completion = NULL, cleanup_keep_filesystem = NULL,
                uid = NULL, slot = NULL, guest_cid = NULL, pid = NULL, spaced_host_pid = NULL,
                netgtw_pid = NULL, vsock_uds_path = NULL, api_socket_path = NULL
          WHERE id = ?3",
        params![state, cleanup_state, id],
    )
    .unwrap();
    id
}

#[test]
fn intent_less_reads_tell_a_failed_cleanup_from_its_lifecycle() {
    let conn = test_conn();
    let plain = insert_intentless(&conn, "stopping", None);
    let required = insert_intentless(&conn, "stopping", Some("required"));
    let failed = insert_intentless(&conn, "stopping", Some("failed"));
    let deleting = insert_intentless(&conn, "deleting", None);

    // Cleanup recovery re-drives an intent-less `stopping` or
    // `cleanup_required` row, but a failed one only with runtime fields.
    let candidates: Vec<String> = list_requested_cleanup_recovery_candidate_ids(&conn)
        .unwrap()
        .into_iter()
        .map(|id| id.to_string())
        .collect();
    let page = cleanup_recovery_page(&conn, None, 128).unwrap().ids;
    for ids in [&candidates, &page] {
        assert!(ids.contains(&plain), "{ids:?}");
        assert!(ids.contains(&required), "{ids:?}");
        assert!(!ids.contains(&failed), "{ids:?}");
    }

    // Runtime fields land on an intent-less row unless it is deleting or its
    // cleanup failed.
    for (id, applies) in [
        (&plain, true),
        (&required, true),
        (&failed, false),
        (&deleting, false),
    ] {
        let result = update_runtime_fields(
            &conn,
            id.parse().unwrap(),
            runtime_fields(ExecutionGeneration::new(1).unwrap()),
        );
        assert_eq!(
            result.is_ok(),
            applies,
            "{:?}: {result:?}",
            stored(&conn, id)
        );
    }
}

#[test]
fn a_cleanup_row_decodes_its_status_and_the_lifecycle_under_it() {
    let conn = test_conn();
    for (completion, lifecycle) in [
        (CleanupCompletion::DeleteRow, SandboxState::Deleting),
        (CleanupCompletion::MarkStopped, SandboxState::Stopping),
    ] {
        let id = state_write_fixtures::insert_row(&conn, SandboxState::Stopping);
        mark_cleanup_required(&conn, &id, !completion.deletes_row(), completion, "handoff")
            .unwrap();
        let row = get_sandbox(&conn, &id).unwrap().unwrap();
        assert_eq!(
            row.state,
            SandboxStatus::owned(Teardown::of(lifecycle).unwrap(), CleanupState::Required),
            "{completion}"
        );
    }

    let id = state_write_fixtures::insert_row(&conn, SandboxState::Started);
    let row = get_sandbox(&conn, &id).unwrap().unwrap();
    assert_eq!(row.state, SandboxStatus::from(SandboxState::Started));
}
