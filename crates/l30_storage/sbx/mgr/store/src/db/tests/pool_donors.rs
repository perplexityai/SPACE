//! The oldest-first donor scan that pool adoption walks.

use super::*;

#[test]
fn pool_donors_lists_paused_members_of_the_build_oldest_first_up_to_the_limit() {
    let conn = test_conn();
    for (id, label, state, hash, created_at) in [
        (
            "donor-b",
            "donorb",
            SandboxState::Paused,
            "sha256:build",
            "2026-01-02T00:00:00Z",
        ),
        (
            "donor-a",
            "donora",
            SandboxState::Paused,
            "sha256:build",
            "2026-01-01T00:00:00Z",
        ),
        (
            "donor-c",
            "donorc",
            SandboxState::Paused,
            "sha256:build",
            "2026-01-03T00:00:00Z",
        ),
        (
            "started",
            "started",
            SandboxState::Started,
            "sha256:build",
            "2026-01-01T00:00:00Z",
        ),
        (
            "other-build",
            "otherbld",
            SandboxState::Paused,
            "sha256:other",
            "2026-01-01T00:00:00Z",
        ),
    ] {
        insert_test_sandbox(
            &conn,
            TestSandboxInsert {
                pool_template: Some("pool-seed".to_string()),
                ..TestSandboxInsert::new(id, label, state)
            },
        );
        conn.execute(
            "UPDATE sandboxes SET template_hash = ?1, created_at = ?2 WHERE id = ?3",
            rusqlite::params![hash, created_at, id],
        )
        .unwrap();
    }
    let ids = |limit| -> Vec<String> {
        crate::db::scan::pool_donors(&conn, "pool-seed", Some("sha256:build"), None, limit)
            .unwrap()
            .into_iter()
            .map(|row| row.id)
            .collect()
    };
    assert_eq!(ids(8), ["donor-a", "donor-b", "donor-c"]);
    assert_eq!(ids(2), ["donor-a", "donor-b"]);
    // The single-donor scan agrees on the head of that order.
    assert_eq!(
        crate::db::scan::pool_donor(&conn, "pool-seed", Some("sha256:build"), None)
            .unwrap()
            .map(|row| row.id),
        Some("donor-a".to_owned())
    );
}
