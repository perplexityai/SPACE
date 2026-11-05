use super::*;
use rusqlite::{Connection, types::FromSql};
use std::fmt::Debug;

fn assert_text_identity_round_trip<T>(id: T)
where
    T: rusqlite::ToSql + FromSql + ToString + Eq + Debug,
{
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE identities (id TEXT NOT NULL)")
        .unwrap();
    conn.execute("INSERT INTO identities (id) VALUES (?1)", [&id])
        .unwrap();
    let stored: String = conn
        .query_row("SELECT id FROM identities", [], |row| row.get(0))
        .unwrap();
    assert_eq!(stored, id.to_string());
    let decoded: T = conn
        .query_row("SELECT ?1", [&stored], |row| row.get(0))
        .unwrap();
    assert_eq!(decoded, id);
    conn.query_row::<T, _, _>("SELECT 'not-an-id'", [], |row| row.get(0))
        .unwrap_err();
}

#[test]
fn uuid_identities_use_canonical_sqlite_text_and_reject_invalid_values() {
    assert_text_identity_round_trip(NodeId::new());
    assert_text_identity_round_trip(SandboxId::new());
}
