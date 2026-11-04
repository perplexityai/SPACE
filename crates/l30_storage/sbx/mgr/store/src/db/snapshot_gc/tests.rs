use super::*;

#[test]
fn finite_pages_progress_past_malformed_and_protected_rows_before_new_insertions() {
    let conn = crate::db::test_conn();
    let insert = |id: usize, timestamp: &str| {
        conn.execute(
            "INSERT INTO local_artifacts
             (local_artifact_id, sandbox_id, artifact_kind, artifact_origin, local_path,
              created_at, last_touched_at, backup_completed_at)
             VALUES (?1, ?1, 'full', 'suspend', '/unused', ?2, ?2, NULL)",
            params![id.to_string(), timestamp],
        )
        .unwrap();
    };
    for id in 0..PAGE_SIZE {
        insert(id, "malformed");
    }
    for id in PAGE_SIZE..PAGE_SIZE * 2 + 1 {
        insert(id, "1");
    }
    conn.execute(
        "UPDATE local_artifacts SET created_at = '1', last_touched_at = '1', local_path = ?1 WHERE local_artifact_id = '0'",
        ["x".repeat(MAX_RAW_BYTES + 1)],
    ).unwrap();
    let mut scan = ArtifactScan::default();
    let first = scan.next_page(&conn).unwrap();
    assert_eq!(first.examined, PAGE_SIZE);
    assert!(first.artifacts.is_empty());
    let fresh = PAGE_SIZE * 2 + 1;
    insert(fresh, "1");
    let second = scan.next_page(&conn).unwrap();
    assert_eq!(second.examined, PAGE_SIZE);
    assert!(second.artifacts.iter().all(|row| !row.backup_completed));
    let last = scan.next_page(&conn).unwrap();
    assert_eq!(last.examined, 1);
    assert_eq!(last.artifacts[0].local_artifact_id, (fresh - 1).to_string());
    let mut next_sweep = Vec::new();
    for _ in 0..3 {
        next_sweep.extend(scan.next_page(&conn).unwrap().artifacts);
    }
    assert!(
        next_sweep
            .iter()
            .any(|row| row.local_artifact_id == fresh.to_string())
    );
    assert_eq!(next_sweep.len(), PAGE_SIZE + 2);

    // Each row fits alone, but not together. Both must be visited on
    // successive pages instead of permanently skipping the second row.
    let conn = crate::db::test_conn();
    for id in ["one", "two"] {
        conn.execute(
            "INSERT INTO local_artifacts
             (local_artifact_id, sandbox_id, artifact_kind, artifact_origin, local_path, created_at)
             VALUES (?1, ?1, 'full', 'suspend', ?2, '1')",
            params![id, "x".repeat(MAX_RAW_BYTES / 2)],
        )
        .unwrap();
    }
    let mut scan = ArtifactScan::default();
    for expected in ["one", "two"] {
        let page = scan.next_page(&conn).unwrap();
        assert_eq!(page.artifacts.len(), 1);
        assert_eq!(page.artifacts[0].local_artifact_id, expected);
    }
}

#[test]
fn pin_projection_uses_indexed_lookup_instead_of_scanning_sandboxes() {
    let conn = crate::db::test_conn();
    let mut statement = conn
        .prepare(&format!("EXPLAIN QUERY PLAN {PAGE_SQL}"))
        .unwrap();
    let plan = statement
        .query_map(params![i64::MIN, i64::MAX, PAGE_SIZE as i64], |row| {
            row.get::<_, String>(3)
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert!(
        plan.iter().any(|step| step
            .contains("SEARCH s USING COVERING INDEX idx_sandboxes_current_local_artifact")),
        "{plan:?}"
    );
    assert!(!plan.iter().any(|step| step.contains("SCAN s")), "{plan:?}");
    assert!(
        plan.iter().any(|step| step
            .contains("SEARCH s USING COVERING INDEX idx_sandboxes_suspend_predecessor_artifact")),
        "{plan:?}"
    );
}
