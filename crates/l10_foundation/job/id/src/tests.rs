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
