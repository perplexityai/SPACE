use rusqlite::Connection;

use crate::{dirty_facets, facet_sync_overrides, reconciliation_status};

static NODE_CTL_MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../../../../db/node-ctl");

async fn migrated_db() -> (tempfile::TempDir, Connection) {
    let temp = tempfile::tempdir().expect("create temp dir");
    let db_path = temp.path().join("node-ctl.db");
    pplx_space_sqlite::migrate("node-ctl-store-test", &db_path, &NODE_CTL_MIGRATOR)
        .await
        .expect("migrate node-ctl database");
    let conn = pplx_space_sqlite::open_rusqlite(
        &db_path,
        pplx_space_sqlite::RusqliteOpenOptions::default(),
    )
    .expect("open node-ctl database");
    (temp, conn)
}

#[tokio::test]
async fn dirty_facets_mark_list_and_count_attempts() {
    let (_temp, mut conn) = migrated_db().await;
    let tx = conn.transaction().expect("begin");
    dirty_facets::mark_dirty(&tx, "template_inventory", "tpl", 12).expect("mark other facet");
    dirty_facets::mark_dirty(&tx, "sandbox_inventory", "", 10).expect("mark whole facet");
    dirty_facets::mark_dirty(&tx, "sandbox_inventory", "sbx-a", 10).expect("mark element");
    dirty_facets::mark_dirty(&tx, "sandbox_inventory", "sbx-a", 11).expect("remark element");
    tx.commit().expect("commit");

    assert_eq!(
        dirty_facets::whole_facet_dirty(&conn, "sandbox_inventory").expect("read whole facet"),
        Some(1)
    );
    assert_eq!(
        dirty_facets::whole_facet_dirty(&conn, "template_inventory").expect("read whole facet"),
        None
    );
    assert_eq!(
        dirty_facets::attempts(&conn, "sandbox_inventory", "sbx-a").expect("read attempts"),
        Some(0)
    );
    assert_eq!(
        dirty_facets::attempts(&conn, "sandbox_inventory", "missing").expect("read attempts"),
        None
    );
    conn.execute(
        "UPDATE dirty_facets SET attempts = 2, last_error = 'boom', last_attempt_at = 9 WHERE facet = 'sandbox_inventory' AND element = 'sbx-a'",
        [],
    )
    .expect("record failed attempts");
    assert_eq!(
        dirty_facets::attempts(&conn, "sandbox_inventory", "sbx-a").expect("read attempts"),
        Some(2)
    );

    let queue = dirty_facets::queue(&conn).expect("list queue");
    let keys: Vec<_> = queue
        .iter()
        .map(|row| (row.facet.as_str(), row.element.as_str(), row.updated_at))
        .collect();
    assert_eq!(
        keys,
        [
            ("sandbox_inventory", "", 10),
            ("sandbox_inventory", "sbx-a", 11),
            ("template_inventory", "tpl", 12),
        ]
    );
    let element_row = queue
        .iter()
        .find(|row| row.element == "sbx-a")
        .expect("sbx-a queue row");
    assert_eq!(
        (
            element_row.attempts,
            element_row.last_error.as_str(),
            element_row.last_attempt_at
        ),
        (2, "boom", 9)
    );
    assert_eq!(
        dirty_facets::facet_queue(&conn, "template_inventory")
            .expect("list facet queue")
            .len(),
        1
    );

    dirty_facets::delete_element_rows(&conn, "sandbox_inventory").expect("delete elements");
    let remaining: Vec<_> = dirty_facets::facet_queue(&conn, "sandbox_inventory")
        .expect("list facet queue")
        .into_iter()
        .map(|row| row.element)
        .collect();
    assert_eq!(remaining, [String::new()]);
}

#[tokio::test]
async fn dirty_facets_claimed_lists_only_the_claims_rows() {
    let (_temp, conn) = migrated_db().await;
    dirty_facets::mark_dirty(&conn, "node_status", "", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "node_status", "", 11).expect("remark");
    dirty_facets::mark_dirty(&conn, "network_policy", "", 10).expect("mark");
    conn.execute(
        "UPDATE dirty_facets SET claim_token = 'claim-1' WHERE facet = 'node_status'",
        [],
    )
    .expect("claim");

    assert_eq!(
        dirty_facets::claimed(&conn, "claim-1").expect("list claimed"),
        [dirty_facets::ClaimedDirtyFacetRow {
            facet: "node_status".to_string(),
            element: String::new(),
            dirty_generation: 2,
        }]
    );
}

#[tokio::test]
async fn facet_sync_overrides_round_trip() {
    let (_temp, conn) = migrated_db().await;
    facet_sync_overrides::upsert(&conn, "node_status", "outbound", Some(5), "old", 1)
        .expect("insert");
    facet_sync_overrides::upsert(&conn, "node_status", "inbound", None, "forever", 2)
        .expect("insert");
    facet_sync_overrides::upsert(&conn, "node_status", "outbound", Some(50), "new", 3)
        .expect("replace");

    assert_eq!(
        facet_sync_overrides::disabled_until(&conn, "node_status", "outbound").expect("read"),
        Some(Some(50))
    );
    assert_eq!(
        facet_sync_overrides::disabled_until(&conn, "node_status", "inbound").expect("read"),
        Some(None)
    );
    assert_eq!(
        facet_sync_overrides::disabled_until(&conn, "sandbox_inventory", "inbound").expect("read"),
        None
    );

    let active = facet_sync_overrides::active(&conn, 50).expect("list active");
    assert_eq!(
        active,
        [facet_sync_overrides::FacetSyncOverrideRow {
            facet: "node_status".to_string(),
            direction: "inbound".to_string(),
            disabled_until: None,
            reason: "forever".to_string(),
            updated_at: 2,
        }]
    );
    assert_eq!(
        facet_sync_overrides::active(&conn, 10).expect("list active"),
        [
            facet_sync_overrides::FacetSyncOverrideRow {
                facet: "node_status".to_string(),
                direction: "inbound".to_string(),
                disabled_until: None,
                reason: "forever".to_string(),
                updated_at: 2,
            },
            facet_sync_overrides::FacetSyncOverrideRow {
                facet: "node_status".to_string(),
                direction: "outbound".to_string(),
                disabled_until: Some(50),
                reason: "new".to_string(),
                updated_at: 3,
            },
        ]
    );

    facet_sync_overrides::delete(&conn, "node_status", "inbound").expect("delete");
    assert_eq!(
        facet_sync_overrides::disabled_until(&conn, "node_status", "inbound").expect("read"),
        None
    );
}

#[tokio::test]
async fn reconciliation_status_register_run_and_list() {
    let (_temp, conn) = migrated_db().await;
    reconciliation_status::register(&conn, "node-status", 30, 100).expect("register");
    reconciliation_status::set_next_due(&conn, "node-status", 500, 110).expect("set next due");
    let [row] =
        <[_; 1]>::try_from(reconciliation_status::list(&conn).expect("list")).expect("one row");
    assert_eq!((row.next_due_at, row.running), (500, false));
    reconciliation_status::mark_started(&conn, "node-status", 120).expect("start");

    // A run left running is due again when the reconciler re-registers.
    reconciliation_status::register(&conn, "node-status", 60, 130).expect("re-register");
    let [row] =
        <[_; 1]>::try_from(reconciliation_status::list(&conn).expect("list")).expect("one row");
    assert_eq!(
        (
            row.poll_interval_secs,
            row.next_due_at,
            row.running,
            row.last_started_at
        ),
        (60, 130, false, 120)
    );

    reconciliation_status::mark_started(&conn, "node-status", 140).expect("start");
    reconciliation_status::mark_finished(
        &conn,
        "node-status",
        &reconciliation_status::FinishedRun {
            completed_at: 150,
            success: true,
            templates_seen: 1,
            sandboxes_seen: 2,
            templates_applied: 3,
            sandboxes_applied: 4,
            last_error: "",
        },
    )
    .expect("finish");
    let [row] =
        <[_; 1]>::try_from(reconciliation_status::list(&conn).expect("list")).expect("one row");
    assert_eq!(
        (
            row.templates_seen,
            row.sandboxes_seen,
            row.templates_applied,
            row.sandboxes_applied,
            row.last_success_at,
            row.last_error.as_str()
        ),
        (1, 2, 3, 4, 150, "")
    );
    reconciliation_status::mark_finished(
        &conn,
        "node-status",
        &reconciliation_status::FinishedRun {
            completed_at: 160,
            success: false,
            templates_seen: 0,
            sandboxes_seen: 0,
            templates_applied: 0,
            sandboxes_applied: 0,
            last_error: "boom",
        },
    )
    .expect("finish");

    let [row] =
        <[_; 1]>::try_from(reconciliation_status::list(&conn).expect("list")).expect("one row");
    assert_eq!(
        row,
        reconciliation_status::ReconciliationStatusRow {
            name: "node-status".to_string(),
            poll_interval_secs: 60,
            last_started_at: 140,
            last_completed_at: 160,
            last_success_at: 150,
            next_due_at: 130,
            running: false,
            templates_seen: 0,
            sandboxes_seen: 0,
            templates_applied: 0,
            sandboxes_applied: 0,
            last_error: "boom".to_string(),
        }
    );

    // A reconciler that is not running keeps its next due time on register.
    reconciliation_status::register(&conn, "node-status", 90, 170).expect("re-register");
    let [row] =
        <[_; 1]>::try_from(reconciliation_status::list(&conn).expect("list")).expect("one row");
    assert_eq!(
        (row.poll_interval_secs, row.next_due_at, row.running),
        (90, 130, false)
    );
}
