use std::path::Path;

use rusqlite::Connection;

use super::{OpenError, open_writer, require_durable};

fn refused_setting(result: Result<impl std::fmt::Debug, OpenError>) -> &'static str {
    match result {
        Err(OpenError::NotDurable { setting, .. }) => setting,
        other => panic!("expected a refusal, got {other:?}"),
    }
}

/// Catches a writer that trusts the pragmas it requested: SQLite keeps an
/// in-memory database out of WAL without an error, and a file-backed one can
/// be left at NORMAL.
#[test]
fn weaker_settings_are_refused() {
    assert_eq!(
        refused_setting(open_writer(Path::new(":memory:"))),
        "journal_mode"
    );

    let dir = tempfile::tempdir().unwrap();
    let conn = Connection::open(dir.path().join("node-manager.db")).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")
        .unwrap();
    assert_eq!(refused_setting(require_durable(&conn)), "synchronous");
}
