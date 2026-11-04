use super::*;
use crate::db::fixture::insert_test_sandbox;
use crate::state::SandboxState;
use pplx_space_common_core::types::SandboxId;

#[test]
fn orphan_scope_sweep_bounds_discovery_and_progresses_past_retained_rows() {
    let conn = crate::db::test_conn();
    for index in 0..INVENTORY_BATCH_SIZE {
        let id = SandboxId::new().to_string();
        crate::db::insert_restore_transfer_scope(
            &conn,
            &id,
            pplx_space_common_core::types::TransferScopeId::new(),
            None,
            None,
            "/retained/staging",
        )
        .unwrap();
        if index % 2 == 0 {
            insert_test_sandbox(&conn, &id, SandboxState::Stopped);
        } else {
            conn.execute("UPDATE sandbox_restore_transfer_scopes SET transfer_scope_id = ?2 WHERE sandbox_id = ?1", [&id, &format!("invalid-{index}")]).unwrap();
        }
    }
    for _ in 0..9 {
        crate::db::insert_restore_transfer_scope(
            &conn,
            &SandboxId::new().to_string(),
            pplx_space_common_core::types::TransferScopeId::new(),
            None,
            None,
            "/retained/staging",
        )
        .unwrap();
    }
    let mut scan = RestoreTransferScopeScan::default();
    assert!(scan.next_batch(&conn).unwrap().is_empty());
    assert_eq!(scan.next_batch(&conn).unwrap().len(), 8);
    assert_eq!(scan.next_batch(&conn).unwrap().len(), 1);
    assert!(scan.next_batch(&conn).unwrap().is_empty());
    assert_eq!(scan.next_batch(&conn).unwrap().len(), 8);
}

#[test]
fn finite_inventory_sweep_advances_past_malformed_rows_and_defers_new_inserts() {
    let conn = crate::db::test_conn();
    for _ in 0..INVENTORY_BATCH_SIZE {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, SandboxState::Stopped);
        conn.execute(
            "UPDATE sandboxes SET process_env_json = 'invalid' WHERE id = ?1",
            [&id],
        )
        .unwrap();
    }
    let old = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &old, SandboxState::Stopped);
    let mut scan = SandboxInventoryScan::default();
    assert!(scan.next_batch(&conn).unwrap().is_empty());
    let new = SandboxId::new().to_string();
    insert_test_sandbox(&conn, &new, SandboxState::Stopped);
    let batch = scan.next_batch(&conn).unwrap();
    assert_eq!(
        batch.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        [old.as_str()]
    );
    assert!(scan.next_batch(&conn).unwrap().is_empty());
    let batch = scan.next_batch(&conn).unwrap();
    assert_eq!(
        batch.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        [old.as_str(), new.as_str()]
    );
    assert_eq!(
        conn.query_row("SELECT COUNT(*) FROM sandboxes", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        (INVENTORY_BATCH_SIZE + 2) as i64
    );
}
