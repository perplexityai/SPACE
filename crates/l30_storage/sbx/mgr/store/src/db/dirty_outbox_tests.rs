use super::*;

#[test]
fn dirty_outbox_acknowledges_only_the_sent_obligation() {
    let conn = test_conn();
    for reenqueue in [false, true] {
        enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
        let sent = list_node_dirty_refs(&conn, 10).unwrap();
        if reenqueue {
            enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
        }
        assert_eq!(
            delete_node_dirty_refs(&conn, &sent).unwrap(),
            usize::from(!reenqueue)
        );
        assert_eq!(delete_node_dirty_refs(&conn, &sent).unwrap(), 0);
        let pending = list_node_dirty_refs(&conn, 10).unwrap();
        assert_eq!(pending.len(), usize::from(reenqueue));
        delete_node_dirty_refs(&conn, &pending).unwrap();
    }
}

#[test]
fn dirty_outbox_stale_ack_cannot_delete_a_recreated_key() {
    let conn = test_conn();
    enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
    let sent = list_node_dirty_refs(&conn, 10).unwrap();
    assert_eq!(delete_node_dirty_refs(&conn, &sent).unwrap(), 1);
    enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
    assert_eq!(delete_node_dirty_refs(&conn, &sent).unwrap(), 0);
    assert_eq!(list_node_dirty_refs(&conn, 10).unwrap().len(), 1);
}

#[test]
fn dirty_outbox_legacy_writer_same_timestamp_invalidates_ack() {
    let conn = test_conn();
    conn.pragma_update(None, "recursive_triggers", true)
        .unwrap();
    enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
    let sent = list_node_dirty_refs(&conn, 10).unwrap();
    // Pre-revision writers only touch these timestamps, including twice in
    // one second. Schema-side revisioning also fences those writes.
    conn.execute(
        "UPDATE node_dirty_refs SET requested_at = requested_at, updated_at = updated_at",
        [],
    )
    .unwrap();
    assert_eq!(delete_node_dirty_refs(&conn, &sent).unwrap(), 0);
    assert_eq!(list_node_dirty_refs(&conn, 10).unwrap().len(), 1);
}

#[test]
fn dirty_outbox_revision_exhaustion_keeps_the_pending_obligation() {
    let conn = test_conn();
    enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap();
    let pending = list_node_dirty_refs(&conn, 10).unwrap();
    conn.execute("UPDATE node_dirty_ref_revision SET value = ?1", [i64::MAX])
        .unwrap();
    enqueue_node_dirty_sandbox_inventory(&conn, "sandbox").unwrap_err();
    assert_eq!(list_node_dirty_refs(&conn, 10).unwrap(), pending);
}
