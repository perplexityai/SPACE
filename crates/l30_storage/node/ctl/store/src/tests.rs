use rusqlite::params;
use rusqlite::types::Value;

use crate::database::{Connection, Transaction};
use crate::{database, dirty_facets, facet_sync_overrides, reconciliation_status};

async fn migrated_db() -> (tempfile::TempDir, Connection) {
    let temp = tempfile::tempdir().expect("create temp dir");
    let db_path = temp.path().join("node-ctl.db");
    database::migrate(&db_path)
        .await
        .expect("migrate node-ctl database");
    let conn = database::open(&db_path).expect("open node-ctl database");
    (temp, conn)
}

#[tokio::test]
async fn dirty_facets_mark_list_and_count_attempts() {
    let (_temp, conn) = migrated_db().await;
    let tx = Transaction::deferred(&conn).expect("begin");
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
    conn.0.execute(
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
    conn.0
        .execute(
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

/// Every column of the element's `dirty_facets` row, to show a write that
/// lost its guard changed nothing.
fn dirty_facet_row(conn: &Connection, facet: &str, element: &str) -> Vec<Value> {
    conn.0
        .query_row(
            "SELECT facet, element, dirty, dirty_generation, attempts, last_error,
                updated_at, last_attempt_at, next_attempt_at, claim_token,
                claimed_at, dead_at, dead_reason
         FROM dirty_facets
         WHERE facet = ?1 AND element = ?2",
            params![facet, element],
            |row| (0..13).map(|index| row.get(index)).collect(),
        )
        .expect("read dirty facet row")
}

/// A `dirty_facets` row's columns in [`dirty_facet_row`]'s order.
type DirtyFacetColumns<'a> = (
    &'a str,
    &'a str,
    i64,
    i64,
    i64,
    &'a str,
    i64,
    i64,
    i64,
    &'a str,
    i64,
    i64,
    &'a str,
);

/// The row [`dirty_facet_row`] reads when its columns hold `columns`.
fn expected_row(columns: DirtyFacetColumns<'_>) -> Vec<Value> {
    let (
        facet,
        element,
        dirty,
        dirty_generation,
        attempts,
        last_error,
        updated_at,
        last_attempt_at,
        next_attempt_at,
        claim_token,
        claimed_at,
        dead_at,
        dead_reason,
    ) = columns;
    let text = |value: &str| Value::Text(value.to_string());
    vec![
        text(facet),
        text(element),
        Value::Integer(dirty),
        Value::Integer(dirty_generation),
        Value::Integer(attempts),
        text(last_error),
        Value::Integer(updated_at),
        Value::Integer(last_attempt_at),
        Value::Integer(next_attempt_at),
        text(claim_token),
        Value::Integer(claimed_at),
        Value::Integer(dead_at),
        text(dead_reason),
    ]
}

#[tokio::test]
async fn dirty_facets_claim_due_takes_unclaimed_and_stale_claims_only() {
    let (_temp, conn) = migrated_db().await;
    dirty_facets::mark_dirty(&conn, "node_status", "", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "network_policy", "", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "template_inventory", "tpl", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "sandbox_inventory", "dead", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "sandbox_inventory", "later", 10).expect("mark");
    assert_eq!(
        dirty_facets::claim_due(&conn, "claim-old", 20, 0).expect("claim"),
        5
    );
    // node_status's claim goes stale; network_policy's stays fresh.
    conn.0
        .execute(
            "UPDATE dirty_facets SET claimed_at = 5 WHERE facet = 'node_status'",
            [],
        )
        .expect("age claim");
    conn.0.execute(
        "UPDATE dirty_facets SET claim_token = '', claimed_at = 0 WHERE facet = 'template_inventory'",
        [],
    )
    .expect("release claim");
    conn.0.execute(
        "UPDATE dirty_facets SET claim_token = '', claimed_at = 0, dead_at = 15 WHERE element = 'dead'",
        [],
    )
    .expect("dead-letter row");
    conn.0.execute(
        "UPDATE dirty_facets SET claim_token = '', claimed_at = 0, next_attempt_at = 31 WHERE element = 'later'",
        [],
    )
    .expect("back off row");
    let fresh = dirty_facet_row(&conn, "network_policy", "");
    let dead = dirty_facet_row(&conn, "sandbox_inventory", "dead");
    let later = dirty_facet_row(&conn, "sandbox_inventory", "later");

    assert_eq!(
        dirty_facets::claim_due(&conn, "claim-new", 30, 10).expect("claim"),
        2
    );
    assert_eq!(
        dirty_facet_row(&conn, "node_status", ""),
        expected_row((
            "node_status",
            "",
            1,
            1,
            0,
            "",
            10,
            30,
            10,
            "claim-new",
            30,
            0,
            ""
        ))
    );
    assert_eq!(
        dirty_facet_row(&conn, "template_inventory", "tpl"),
        expected_row((
            "template_inventory",
            "tpl",
            1,
            1,
            0,
            "",
            10,
            30,
            10,
            "claim-new",
            30,
            0,
            ""
        ))
    );
    assert_eq!(dirty_facet_row(&conn, "network_policy", ""), fresh);
    assert_eq!(dirty_facet_row(&conn, "sandbox_inventory", "dead"), dead);
    assert_eq!(dirty_facet_row(&conn, "sandbox_inventory", "later"), later);

    // Nothing is left to claim.
    assert_eq!(
        dirty_facets::claim_due(&conn, "claim-other", 30, 10).expect("claim"),
        0
    );
    assert_eq!(dirty_facet_row(&conn, "network_policy", ""), fresh);
}

#[tokio::test]
async fn dirty_facets_delete_synced_and_release_claim_need_the_claim() {
    let (_temp, conn) = migrated_db().await;
    dirty_facets::mark_dirty(&conn, "node_status", "", 10).expect("mark");
    dirty_facets::claim_due(&conn, "claim-1", 20, 0).expect("claim");
    let claimed = dirty_facet_row(&conn, "node_status", "");

    assert!(!dirty_facets::delete_synced(&conn, "node_status", "", 1, "claim-2").expect("delete"));
    assert!(!dirty_facets::delete_synced(&conn, "node_status", "", 2, "claim-1").expect("delete"));
    assert!(!dirty_facets::release_claim(&conn, "node_status", "", "claim-2").expect("release"));
    assert_eq!(dirty_facet_row(&conn, "node_status", ""), claimed);

    // The generation moved while the claim still holds (set directly here;
    // `mark_dirty` would also clear the claim): the delete loses on the
    // generation alone, and the release still matches.
    conn.0
        .execute(
            "UPDATE dirty_facets SET dirty_generation = 2 WHERE facet = 'node_status'",
            [],
        )
        .expect("bump the generation under the claim");
    assert!(!dirty_facets::delete_synced(&conn, "node_status", "", 1, "claim-1").expect("delete"));
    assert!(dirty_facets::release_claim(&conn, "node_status", "", "claim-1").expect("release"));
    assert_eq!(
        dirty_facet_row(&conn, "node_status", ""),
        expected_row(("node_status", "", 1, 2, 0, "", 10, 20, 10, "", 0, 0, ""))
    );
    assert!(!dirty_facets::release_claim(&conn, "node_status", "", "claim-1").expect("release"));

    dirty_facets::claim_due(&conn, "claim-3", 30, 0).expect("claim");
    assert!(dirty_facets::delete_synced(&conn, "node_status", "", 2, "claim-3").expect("delete"));
    assert!(
        dirty_facets::facet_queue(&conn, "node_status")
            .expect("list facet queue")
            .is_empty()
    );
    assert!(!dirty_facets::delete_synced(&conn, "node_status", "", 2, "claim-3").expect("delete"));
    assert!(!dirty_facets::release_claim(&conn, "node_status", "", "claim-3").expect("release"));
}

#[tokio::test]
async fn dirty_facets_failed_attempts_need_the_claimed_generation() {
    let (_temp, conn) = migrated_db().await;
    dirty_facets::mark_dirty(&conn, "node_status", "", 10).expect("mark");
    dirty_facets::mark_dirty(&conn, "network_policy", "", 10).expect("mark");
    dirty_facets::claim_due(&conn, "claim-1", 20, 0).expect("claim");
    let claimed = dirty_facet_row(&conn, "node_status", "");
    let attempt = dirty_facets::FailedAttempt {
        facet: "node_status",
        element: "",
        dirty_generation: 1,
        claim_token: "claim-1",
        error: "boom",
        failed_at: 25,
    };

    for losing in [
        dirty_facets::FailedAttempt {
            dirty_generation: 2,
            ..attempt
        },
        dirty_facets::FailedAttempt {
            claim_token: "claim-2",
            ..attempt
        },
    ] {
        assert!(!dirty_facets::dead_letter_failed_attempt(&conn, &losing).expect("dead-letter"));
        assert!(!dirty_facets::back_off_failed_attempt(&conn, &losing, 40).expect("back off"));
        assert_eq!(dirty_facet_row(&conn, "node_status", ""), claimed);
    }
    assert!(
        !dirty_facets::release_claim_due_now(&conn, "node_status", "", "claim-2", 25)
            .expect("release")
    );
    assert_eq!(dirty_facet_row(&conn, "node_status", ""), claimed);

    assert!(dirty_facets::back_off_failed_attempt(&conn, &attempt, 40).expect("back off"));
    assert_eq!(
        dirty_facets::attempts(&conn, "node_status", "").expect("read attempts"),
        Some(1)
    );
    assert_eq!(
        dirty_facet_row(&conn, "node_status", ""),
        expected_row(("node_status", "", 1, 1, 1, "boom", 25, 25, 40, "", 0, 0, ""))
    );
    let backed_off = dirty_facet_row(&conn, "node_status", "");
    assert!(!dirty_facets::back_off_failed_attempt(&conn, &attempt, 40).expect("back off"));
    assert!(!dirty_facets::dead_letter_failed_attempt(&conn, &attempt).expect("dead-letter"));
    assert_eq!(dirty_facet_row(&conn, "node_status", ""), backed_off);

    let dead_letter = dirty_facets::FailedAttempt {
        facet: "network_policy",
        ..attempt
    };
    assert!(dirty_facets::dead_letter_failed_attempt(&conn, &dead_letter).expect("dead-letter"));
    let dead = dirty_facet_row(&conn, "network_policy", "");
    assert_eq!(
        dead,
        expected_row((
            "network_policy",
            "",
            1,
            1,
            1,
            "boom",
            25,
            25,
            10,
            "",
            0,
            25,
            "boom"
        ))
    );
    assert_eq!(
        dirty_facets::claim_due(&conn, "claim-3", 100, 0).expect("claim"),
        1
    );
    assert_eq!(dirty_facet_row(&conn, "network_policy", ""), dead);
}

#[tokio::test]
async fn dirty_facets_release_claim_due_now_ignores_the_generation() {
    let (_temp, conn) = migrated_db().await;
    dirty_facets::mark_dirty(&conn, "node_status", "", 10).expect("mark");
    dirty_facets::claim_due(&conn, "claim-1", 20, 0).expect("claim");
    conn.0.execute(
        "UPDATE dirty_facets SET dirty_generation = 2, next_attempt_at = 90 WHERE facet = 'node_status'",
        [],
    )
    .expect("bump the generation under the claim");

    assert!(
        dirty_facets::release_claim_due_now(&conn, "node_status", "", "claim-1", 25)
            .expect("release")
    );
    let row = dirty_facet_row(&conn, "node_status", "");
    assert_eq!(
        row,
        expected_row(("node_status", "", 1, 2, 0, "", 25, 20, 25, "", 0, 0, ""))
    );
    assert!(
        !dirty_facets::release_claim_due_now(&conn, "node_status", "", "claim-1", 30)
            .expect("release")
    );
    assert_eq!(dirty_facet_row(&conn, "node_status", ""), row);
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
