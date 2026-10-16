use pplx_space_common_core::types::SandboxId;

use super::*;

#[test]
fn state_metadata_batches_share_the_callers_budget() {
    use crate::db::fixture::insert_test_sandbox;
    let conn = crate::db::test_conn();
    let mut budget = InventoryBudget::new(1, MAX_RAW_BYTES);
    for index in 0..2 {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, crate::state::SandboxState::Stopped);
        let result = check_state_metadata(&conn, &[id], &mut budget);
        if index == 0 {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(InventoryReadError::Limit(_))));
        }
    }
}

#[test]
fn raw_value_limit_precedes_decoding_and_rejects_whole_collection() {
    let conn = Connection::open_in_memory();
    let mut stmt = conn
        .prepare("SELECT 'valid' UNION ALL SELECT zeroblob(6)")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut budget = InventoryBudget::new(2, 10);
    check_row(rows.next().unwrap().unwrap(), &mut budget).unwrap();
    assert!(matches!(
        check_row(rows.next().unwrap().unwrap(), &mut budget),
        Err(InventoryReadError::Limit(_))
    ));
}

#[test]
fn unary_list_serves_the_complete_inventory_past_the_item_budget() {
    use crate::db::fixture::insert_test_sandbox;
    let conn = crate::db::test_conn();
    let tx = crate::db::Transaction::deferred(&conn).unwrap();
    for _ in 0..=MAX_ITEMS {
        insert_test_sandbox(
            &tx,
            &SandboxId::new().to_string(),
            crate::state::SandboxState::Started,
        );
    }
    tx.commit().unwrap();
    assert_eq!(list(&conn, "List").unwrap().len(), MAX_ITEMS + 1);
}

#[test]
fn unary_list_serves_the_complete_inventory_past_the_raw_budget() {
    use crate::db::fixture::insert_test_sandbox;
    let conn = crate::db::test_conn();
    let template = "t".repeat(MAX_RAW_BYTES / 2);
    for _ in 0..3 {
        let id = SandboxId::new().to_string();
        insert_test_sandbox(&conn, &id, crate::state::SandboxState::Started);
        conn.execute(
            "UPDATE sandboxes SET template = ?2 WHERE id = ?1",
            rusqlite::params![id, template],
        )
        .unwrap();
    }
    assert_eq!(list(&conn, "List").unwrap().len(), 3);
}

fn insert_listed(conn: &Connection, count: usize) -> Vec<String> {
    use crate::db::fixture::insert_test_sandbox;
    let mut ids = (0..count)
        .map(|_| {
            let id = SandboxId::new().to_string();
            insert_test_sandbox(conn, &id, crate::state::SandboxState::Started);
            id
        })
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn page_ids(page: &ListPage) -> Vec<String> {
    page.rows.iter().map(|row| row.id().to_owned()).collect()
}

#[test]
fn list_pages_cover_every_row_once_in_id_order() {
    let conn = crate::db::test_conn();
    let ids = insert_listed(&conn, 5);
    let mut seen = Vec::new();
    let mut after = String::new();
    loop {
        let page = list_page(&conn, &after, 2).unwrap();
        assert!(page.rows.len() <= 2);
        seen.extend(page_ids(&page));
        if !page.more {
            break;
        }
        after = page.rows.last().unwrap().id().to_owned();
    }
    assert_eq!(seen, ids);
}

#[test]
fn list_page_ends_early_at_the_raw_budget_and_rejects_only_a_single_oversized_row() {
    let conn = crate::db::test_conn();
    let ids = insert_listed(&conn, 3);
    let template = "t".repeat(MAX_RAW_BYTES / 2);
    conn.execute(
        "UPDATE sandboxes SET template = ?2 WHERE id = ?1",
        rusqlite::params![ids[1], template],
    )
    .unwrap();
    conn.execute(
        "UPDATE sandboxes SET template = ?2 WHERE id = ?1",
        rusqlite::params![ids[2], template],
    )
    .unwrap();
    let page = list_page(&conn, "", MAX_ITEMS).unwrap();
    assert_eq!(page_ids(&page), ids[..2]);
    assert!(page.more);
    let page = list_page(&conn, &ids[1], MAX_ITEMS).unwrap();
    assert_eq!(page_ids(&page), ids[2..]);
    assert!(!page.more);

    conn.execute(
        "UPDATE sandboxes SET template = ?2 WHERE id = ?1",
        rusqlite::params![ids[0], "t".repeat(MAX_RAW_BYTES + 1)],
    )
    .unwrap();
    assert!(matches!(
        list_page(&conn, "", MAX_ITEMS),
        Err(InventoryReadError::Limit(_))
    ));
}

#[test]
fn list_page_keeps_the_identity_of_an_undecodable_row() {
    let conn = crate::db::test_conn();
    let ids = insert_listed(&conn, 2);
    conn.execute("UPDATE sandboxes SET user = '' WHERE id = ?1", [&ids[0]])
        .unwrap();
    let page = list_page(&conn, "", MAX_ITEMS).unwrap();
    assert_eq!(page_ids(&page), ids);
    assert!(matches!(
        &page.rows[0],
        ListPageRow::Undecodable { state: Some(state), .. } if state == "started"
    ));
    assert!(matches!(page.rows[1], ListPageRow::Decoded(_)));
}
