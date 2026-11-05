use rusqlite::Connection;

use super::*;

#[test]
fn sqlite_stores_canonical_text_and_rejects_invalid_values() {
    let id = JobId::new();
    let conn = Connection::open_in_memory().unwrap();
    let stored: String = conn
        .query_row("SELECT ?1", [&id], |row| row.get(0))
        .unwrap();
    assert_eq!(stored, id.to_string());
    let decoded: JobId = conn
        .query_row("SELECT ?1", [&stored], |row| row.get(0))
        .unwrap();
    assert_eq!(decoded, id);
    conn.query_row::<JobId, _, _>("SELECT 'not-an-id'", [], |row| row.get(0))
        .unwrap_err();
}

#[test]
fn debug_names_the_type_and_shows_the_display_text() {
    let id = JobId::from_uuid(uuid::Uuid::nil());
    assert_eq!(
        format!("{id:?}"),
        "JobId(\"00000000-0000-0000-0000-000000000000\")"
    );
}
