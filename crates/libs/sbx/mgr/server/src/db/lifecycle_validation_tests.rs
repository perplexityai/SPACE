use crate::db::Connection;
use rusqlite::{params, types::Value};

use super::{SandboxState, seed_historical_lifecycle_rows, test_conn};

fn insert(conn: &Connection, id: &str, state: &Value) -> crate::db::Result<usize> {
    conn.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
         VALUES (?1, 'sandbox1', ?2, 'template', '0', '0')",
        params![id, state],
    )
}

fn assert_lifecycle_rejected(result: crate::db::Result<usize>) {
    let error = result.unwrap_err();
    assert_eq!(
        error.raw().sqlite_error().unwrap().extended_code,
        rusqlite::ffi::SQLITE_CONSTRAINT_TRIGGER,
    );
}

#[test]
fn lifecycle_validation_accepts_retained_labels_and_rejects_unknown_writes() {
    let conn = test_conn();
    for state in SandboxState::all() {
        let label = state.as_str();
        insert(&conn, label, &Value::Text(label.into())).unwrap();
        conn.execute("UPDATE sandboxes SET state = ?1 WHERE id = ?1", [label])
            .unwrap();
    }
    for state in [
        Value::Text("future_state".into()),
        Value::Text("".into()),
        Value::Text("Started".into()),
        Value::Blob(b"started".to_vec()),
        Value::Integer(1),
    ] {
        assert_lifecycle_rejected(insert(&conn, "invalid", &state));
        // Conflict handling must never turn an invalid lifecycle write into a
        // successful no-op or replace the previous row before validation.
        for sql in [
            "UPDATE sandboxes SET state = ?1 WHERE id = 'started'",
            "UPDATE OR IGNORE sandboxes SET state = ?1 WHERE id = 'started'",
            "UPDATE OR REPLACE sandboxes SET state = ?1 WHERE id = 'started'",
            "UPDATE OR FAIL sandboxes SET state = ?1 WHERE id = 'started'",
            "UPDATE OR ROLLBACK sandboxes SET state = ?1 WHERE id = 'started'",
            "INSERT OR IGNORE INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('started', 'sandbox1', ?1, 'template', '0', '0')",
            "INSERT OR REPLACE INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('started', 'sandbox1', ?1, 'template', '0', '0')",
            "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('started', 'sandbox1', 'started', 'template', '0', '0')
             ON CONFLICT (id) DO UPDATE SET state = ?1",
        ] {
            assert_lifecycle_rejected(conn.execute(sql, [&state]));
        }
        assert_eq!(
            conn.query_row(
                "SELECT state FROM sandboxes WHERE id = 'started'",
                [],
                |row| { row.get::<_, String>(0) }
            )
            .unwrap(),
            "started"
        );
    }
    // Both UPSERT branches must still commit a supported lifecycle label.
    for id in ["started", "new-upsert"] {
        conn.execute(
            "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES (?1, 'sandbox1', 'stopped', 'template', '0', '0')
             ON CONFLICT (id) DO UPDATE SET state = excluded.state",
            [id],
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT state FROM sandboxes WHERE id = ?1", [id], |row| {
                row.get::<_, String>(0)
            })
            .unwrap(),
            "stopped"
        );
    }
}

#[test]
fn lifecycle_validation_preserves_native_not_null_conflict_handling() {
    let conn = test_conn();
    insert(&conn, "existing", &Value::Text("started".into())).unwrap();
    for (sql, ignored) in [
        (
            "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('new', 'sandbox1', NULL, 'template', '0', '0')",
            false,
        ),
        (
            "UPDATE sandboxes SET state = NULL WHERE id = 'existing'",
            false,
        ),
        (
            "INSERT OR IGNORE INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('new', 'sandbox1', NULL, 'template', '0', '0')",
            true,
        ),
        (
            "INSERT OR IGNORE INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('existing', 'sandbox1', NULL, 'template', '0', '0')",
            true,
        ),
        (
            "UPDATE OR IGNORE sandboxes SET state = NULL WHERE id = 'existing'",
            true,
        ),
        (
            "INSERT OR REPLACE INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('existing', 'sandbox1', NULL, 'template', '0', '0')",
            false,
        ),
        (
            "UPDATE OR REPLACE sandboxes SET state = NULL WHERE id = 'existing'",
            false,
        ),
    ] {
        let result = conn.execute(sql, []);
        if ignored {
            assert_eq!(result.unwrap(), 0, "{sql}");
        } else {
            let error = result.unwrap_err();
            assert_eq!(
                error.raw().sqlite_error().unwrap().extended_code,
                rusqlite::ffi::SQLITE_CONSTRAINT_NOTNULL,
                "{sql}"
            );
        }
        let rows: Vec<(String, String)> = conn
            .prepare("SELECT id, state FROM sandboxes")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows, vec![("existing".into(), "started".into())], "{sql}");
    }
}

#[test]
fn lifecycle_validation_aborts_the_whole_statement_without_poisoning_the_transaction() {
    let conn = test_conn();
    insert(&conn, "first", &Value::Text("started".into())).unwrap();
    insert(&conn, "second", &Value::Text("started".into())).unwrap();
    let tx = crate::db::Transaction::deferred(&conn).unwrap();
    tx.execute("UPDATE sandboxes SET template = 'retained'", [])
        .unwrap();
    assert_lifecycle_rejected(tx.execute(
        "UPDATE OR FAIL sandboxes
                SET state = CASE WHEN id = 'first' THEN 'stopped' ELSE 'future_state' END",
        [],
    ));
    assert_lifecycle_rejected(tx.execute(
        "INSERT INTO sandboxes (id, uuid8, state, template, created_at, updated_at)
             VALUES ('valid', 'sandbox1', 'stopped', 'template', '0', '0'),
                    ('invalid', 'sandbox1', 'future_state', 'template', '0', '0')",
        [],
    ));
    tx.commit().unwrap();
    let rows: Vec<(String, String, String)> = conn
        .prepare("SELECT id, state, template FROM sandboxes ORDER BY id")
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("first".into(), "started".into(), "retained".into()),
            ("second".into(), "started".into(), "retained".into()),
        ]
    );
}

#[test]
fn historical_lifecycle_evidence_can_be_maintained_but_not_rewritten_as_unknown() {
    let conn = test_conn();
    seed_historical_lifecycle_rows(&conn, |conn| {
        insert(conn, "historical", &Value::Text("future_state".into())).unwrap();
    });
    conn.execute(
        "UPDATE sandboxes SET template = 'updated' WHERE id = 'historical'",
        [],
    )
    .unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT state, template FROM sandboxes WHERE id = 'historical'",
            [],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .unwrap(),
        ("future_state".into(), "updated".into())
    );
    assert_lifecycle_rejected(conn.execute(
        "UPDATE sandboxes SET state = state WHERE id = 'historical'",
        [],
    ));
    // Repairing the retained evidence to a recognized label remains possible.
    conn.execute(
        "UPDATE sandboxes SET state = 'failed' WHERE id = 'historical'",
        [],
    )
    .unwrap();

    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        seed_historical_lifecycle_rows(&conn, |conn| {
            insert(conn, "panicked", &Value::Text("future_state".into())).unwrap();
            panic!("seed interrupted");
        });
    }));
    let _panic = panic.unwrap_err();
    assert_lifecycle_rejected(insert(
        &conn,
        "invalid",
        &Value::Text("future_state".into()),
    ));
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM sandboxes WHERE id = 'panicked'",
            [],
            |row| { row.get::<_, i64>(0) }
        )
        .unwrap(),
        0
    );
}
