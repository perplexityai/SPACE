use super::*;
use crate::db::fixture::insert_test_sandbox;
use crate::state::SandboxState;

#[test]
fn cleanup_discovery_rejects_oversized_metadata_before_string_decoding() {
    let conn = crate::db::test_conn();
    insert_test_sandbox(&conn, &SandboxId::new().to_string(), SandboxState::Started);
    conn.execute(
        "UPDATE sandboxes SET template = zeroblob(?1)",
        [(MAX_ENCODED_BYTES + 1) as i64],
    )
    .unwrap();
    let mut scan = CleanupTargetScan::default();
    assert!(matches!(
        scan.next_batch(&conn),
        Err(InventoryReadError::Limit(_))
    ));
}

#[test]
fn raw_rows_bound_discovery_and_new_rows_wait_for_another_sweep() {
    let conn = crate::db::test_conn();
    for _ in 0..257 {
        insert_test_sandbox(&conn, &SandboxId::new().to_string(), SandboxState::Started);
    }
    conn.execute("UPDATE sandboxes SET user = X'00' WHERE rowid <= 129", [])
        .unwrap();
    let mut scan = CleanupTargetScan::default();
    let first = scan.next_batch(&conn).unwrap();
    assert_eq!(first.len(), 128);
    assert!(first.iter().all(|row| row.metadata.is_none()));
    let later = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &later, SandboxState::Started);
    let second = scan.next_batch(&conn).unwrap();
    assert_eq!(second.len(), 128);
    assert_eq!(
        second.iter().filter(|row| row.metadata.is_none()).count(),
        1
    );
    let third = scan.next_batch(&conn).unwrap();
    assert_eq!(third.len(), 1);
    assert_ne!(third[0].raw_id, later);
    assert!(scan.complete());
    assert!(scan.next_batch(&conn).unwrap().is_empty());
    let mut next = CleanupTargetScan::default();
    let mut observed = Vec::new();
    while !next.complete() {
        observed.extend(
            next.next_batch(&conn)
                .unwrap()
                .into_iter()
                .map(|row| row.raw_id),
        );
    }
    assert!(observed.contains(&later));
}
