use super::*;
use crate::db::test_conn;
use crate::db::test_fixtures::state_write_fixtures::{admit_stop, insert_row};
use crate::state::SandboxState;

/// A full page ends at the 128th rowid with a cursor; the next page resumes
/// after it and, short, ends the pass, so the page after that starts over.
#[test]
fn cleanup_obligations_page_through_one_pass_and_start_over() {
    let conn = test_conn();
    let ids: Vec<String> = (0..130)
        .map(|_| {
            let id = insert_row(&conn, SandboxState::Started);
            admit_stop(&conn, &id);
            id
        })
        .collect();
    let mut cursor = CleanupObligationScan::default();

    let first = sandbox_pages(&conn, &mut cursor).unwrap();
    assert_eq!(first.ids, ids[..128]);
    assert!(first.next_cursor.is_some());

    let second = sandbox_pages(&conn, &mut cursor).unwrap();
    assert_eq!(second.ids, ids[128..]);
    assert_eq!(second.next_cursor, None);

    let restarted = sandbox_pages(&conn, &mut cursor).unwrap();
    assert_eq!(restarted.ids, ids[..128]);
}
