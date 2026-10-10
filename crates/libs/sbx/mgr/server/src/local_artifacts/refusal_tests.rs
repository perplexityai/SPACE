//! Refusal outcomes of local-artifact checkouts, lease resolution, and the
//! named-publication inventory.

use std::sync::PoisonError;

use super::*;

const ID: &str = "01939e3e-aaaa-7000-8000-000000000001";
const OTHER_ID: &str = "01939e3e-aaaa-7000-8000-000000000002";

/// Old enough, unpinned and backed up: every GC eligibility check passes.
const ELIGIBLE: GcEligibility = GcEligibility {
    now_secs: 1_000,
    idle_secs: 0,
    grace_secs: 60,
};

fn artifact_id(id: &str) -> LocalArtifactId {
    id.parse().unwrap()
}

fn shared(conn: Connection) -> (Arc<Mutex<Connection>>, LocalArtifacts) {
    let db = Arc::new(Mutex::new(conn));
    let store = LocalArtifacts::new(Arc::clone(&db));
    (db, store)
}

/// A backed-up, long-idle `suspend` row: the only obstacle to any checkout
/// is whatever the test adds.
fn insert_row(conn: &Connection, id: &str, sandbox_id: &str, local_path: &str) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, last_touched_at,
             backup_completed_at
         ) VALUES (?1, ?2, 'full', 'suspend', NULL, ?3, '0', '0', '0')",
        params![id, sandbox_id, local_path],
    )
    .unwrap();
}

/// Reads through a poisoned mutex too, so failure tests can inspect the row.
fn row_state(db: &Arc<Mutex<Connection>>, id: &str) -> Option<String> {
    db.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .query_row(
            "SELECT state FROM local_artifacts WHERE local_artifact_id = ?1",
            [id],
            |row| row.get(0),
        )
        .optional()
        .unwrap()
}

fn last_touched_at(db: &Arc<Mutex<Connection>>, id: &str) -> String {
    db.lock()
        .unwrap()
        .query_row(
            "SELECT last_touched_at FROM local_artifacts WHERE local_artifact_id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap()
}

type Checkout = fn(&LocalArtifacts, LocalArtifactId) -> Result<(), CheckoutError>;

const CHECKOUTS: [(&str, Checkout); 3] = [
    ("restore", |store, id| {
        store.check_out_for_restore(id).map(drop)
    }),
    ("delete", |store, id| {
        store.check_out_for_delete(id).map(drop)
    }),
    ("gc", |store, id| {
        store.check_out_for_gc(id, "sbx", ELIGIBLE).map(|checkout| {
            assert!(matches!(checkout, GcDeleteCheckout::Leased(_)));
        })
    }),
];

#[test]
fn every_checkout_refuses_an_unrecognized_state_and_leaves_the_row_alone() {
    for (checkout_name, checkout) in CHECKOUTS {
        let conn = crate::db::test_conn();
        insert_row(&conn, ID, "sbx", "/artifact");
        conn.pragma_update(None, "ignore_check_constraints", true)
            .unwrap();
        conn.execute(
            "UPDATE local_artifacts SET state = 'bogus' WHERE local_artifact_id = ?1",
            [ID],
        )
        .unwrap();
        conn.pragma_update(None, "ignore_check_constraints", false)
            .unwrap();
        let (db, store) = shared(conn);

        let result = checkout(&store, artifact_id(ID));

        assert!(
            matches!(
                &result,
                Err(CheckoutError::CorruptState { id, raw_state })
                    if *id == artifact_id(ID) && raw_state == "bogus"
            ),
            "{checkout_name}: {result:?}"
        );
        assert_eq!(
            row_state(&db, ID).as_deref(),
            Some("bogus"),
            "{checkout_name}"
        );
    }
}

#[test]
fn delete_and_gc_checkouts_report_a_missing_or_foreign_row_as_not_found() {
    let (db, store) = shared(crate::db::test_conn());
    assert!(matches!(
        store.check_out_for_delete(artifact_id(ID)),
        Err(CheckoutError::NotFound(id)) if id == artifact_id(ID)
    ));
    assert!(matches!(
        store.check_out_for_gc(artifact_id(ID), "sbx", ELIGIBLE),
        Err(CheckoutError::NotFound(id)) if id == artifact_id(ID)
    ));

    // The reaper's candidate names its owner; a row owned by another sandbox
    // is not that candidate.
    insert_row(&db.lock().unwrap(), ID, "owner", "/artifact");
    assert!(matches!(
        store.check_out_for_gc(artifact_id(ID), "other", ELIGIBLE),
        Err(CheckoutError::NotFound(id)) if id == artifact_id(ID)
    ));
    assert!(matches!(
        store.check_out_for_gc(artifact_id(ID), "owner", ELIGIBLE),
        Ok(GcDeleteCheckout::Leased(_))
    ));
}

#[test]
fn named_delete_checkout_resolves_by_sandbox_and_snapshot_name() {
    let (db, store) = shared(crate::db::test_conn());
    let name: SnapshotName = "f-one".parse().unwrap();
    let missing: SnapshotName = "f-missing".parse().unwrap();
    store
        .publish_named_snapshot("sbx-a", artifact_id(ID), ArtifactKind::Full, &name, "/a")
        .unwrap();
    store
        .publish_named_snapshot(
            "sbx-b",
            artifact_id(OTHER_ID),
            ArtifactKind::Full,
            &name,
            "/b",
        )
        .unwrap();

    for (sandbox, snapshot) in [("sbx-a", &missing), ("sbx-c", &name)] {
        assert!(
            matches!(
                store.check_out_named_for_delete(sandbox, snapshot),
                Err(CheckoutError::NamedNotFound { sandbox_id, snapshot_name })
                    if sandbox_id == sandbox && &snapshot_name == snapshot
            ),
            "{sandbox}/{snapshot}"
        );
    }

    let restore = store.check_out_for_restore(artifact_id(ID)).unwrap();
    assert!(matches!(
        store.check_out_named_for_delete("sbx-a", &name),
        Err(CheckoutError::NotReady {
            id,
            state: ArtifactState::Restoring,
        }) if id == artifact_id(ID)
    ));
    restore.release().unwrap();

    let lease = store.check_out_named_for_delete("sbx-a", &name).unwrap();
    assert_eq!(lease.id(), artifact_id(ID));
    assert_eq!(lease.local_path(), "/a");
    lease.commit().unwrap();
    assert_eq!(row_state(&db, ID), None);
    assert_eq!(row_state(&db, OTHER_ID).as_deref(), Some("ready"));
}

type Hold = fn(&LocalArtifacts, LocalArtifactId) -> Box<dyn std::any::Any>;

/// A held row refuses every checkout with the holder's state, and dropping
/// the holder's lease makes the row available to each checkout again.
#[test]
fn a_held_artifact_refuses_every_checkout_until_its_lease_drops() {
    const HOLDERS: [(&str, ArtifactState, Hold); 3] = [
        ("restore", ArtifactState::Restoring, |store, id| {
            Box::new(store.check_out_for_restore(id).unwrap())
        }),
        ("delete", ArtifactState::Deleting, |store, id| {
            Box::new(store.check_out_for_delete(id).unwrap())
        }),
        ("gc", ArtifactState::Deleting, |store, id| {
            let Ok(GcDeleteCheckout::Leased(lease)) = store.check_out_for_gc(id, "sbx", ELIGIBLE)
            else {
                panic!("gc checkout issued no lease");
            };
            Box::new(lease)
        }),
    ];
    for (holder, held, hold) in HOLDERS {
        for (checkout_name, checkout) in CHECKOUTS {
            let case = format!("{holder} held, {checkout_name} checkout");
            let conn = crate::db::test_conn();
            insert_row(&conn, ID, "sbx", "/artifact");
            let (db, store) = shared(conn);
            let lease = hold(&store, artifact_id(ID));

            let result = checkout(&store, artifact_id(ID));

            assert!(
                matches!(
                    &result,
                    Err(CheckoutError::NotReady { id, state })
                        if *id == artifact_id(ID) && *state == held
                ),
                "{case}: {result:?}"
            );
            assert_eq!(row_state(&db, ID).as_deref(), Some(held.as_str()), "{case}");
            drop(lease);
            assert_eq!(row_state(&db, ID).as_deref(), Some("ready"), "{case}");
            checkout(&store, artifact_id(ID)).unwrap_or_else(|error| panic!("{case}: {error}"));
        }
    }
}

/// Startup recovery resetting a `restoring` row stands in for any path that
/// takes the row away from a live lease: once another owner holds it, the
/// stale lease must not hand it back to `ready`.
#[test]
fn restore_lease_resolution_only_moves_a_row_it_still_holds() {
    for release in [true, false] {
        let conn = crate::db::test_conn();
        insert_row(&conn, ID, "sbx", "/artifact");
        let (db, store) = shared(conn);
        let stale = store.check_out_for_restore(artifact_id(ID)).unwrap();
        assert_eq!(store.recover_stuck_states().unwrap().restoring_reset, 1);
        let delete = store.check_out_for_delete(artifact_id(ID)).unwrap();

        if release {
            stale.release().unwrap();
        } else {
            drop(stale);
        }

        assert_eq!(
            row_state(&db, ID).as_deref(),
            Some("deleting"),
            "release={release}"
        );
        assert_eq!(last_touched_at(&db, ID), "0", "release={release}");
        delete.commit().unwrap();
        assert_eq!(row_state(&db, ID), None, "release={release}");
    }
}

/// Same fence for deletion: recovery rolls a `deleting` row with intact bytes
/// back to `ready`, after which the stale delete lease owns nothing.
#[test]
fn delete_lease_resolution_only_moves_a_row_it_still_holds() {
    for commit in [true, false] {
        let root = tempfile::TempDir::new().unwrap();
        let local_path = root.path().join("artifact");
        std::fs::create_dir(&local_path).unwrap();
        let conn = crate::db::test_conn();
        insert_row(&conn, ID, "sbx", local_path.to_str().unwrap());
        let (db, store) = shared(conn);
        let stale = store.check_out_for_delete(artifact_id(ID)).unwrap();
        assert_eq!(
            store.recover_stuck_states().unwrap().deleting_rolled_back,
            1
        );

        if commit {
            stale.commit().unwrap();
            assert_eq!(row_state(&db, ID).as_deref(), Some("ready"));
            store
                .check_out_for_restore(artifact_id(ID))
                .unwrap()
                .release()
                .unwrap();
        } else {
            let restore = store.check_out_for_restore(artifact_id(ID)).unwrap();
            drop(stale);
            assert_eq!(row_state(&db, ID).as_deref(), Some("restoring"));
            restore.release().unwrap();
        }
        assert_eq!(
            row_state(&db, ID).as_deref(),
            Some("ready"),
            "commit={commit}"
        );
    }
}

#[derive(Debug, Clone, Copy)]
enum ResolutionFailure {
    PoisonedConnection,
    StatementAborts,
}

/// Explicit resolution reports the failure; `Drop` cannot, and must neither
/// panic nor move the row. The row stays held for recovery to resolve.
#[test]
fn lease_resolution_failure_is_reported_and_leaves_the_row_held() {
    const RELEASED: &str = "01939e3e-aaaa-7000-8000-000000000011";
    const DROPPED_RESTORE: &str = "01939e3e-aaaa-7000-8000-000000000012";
    const COMMITTED: &str = "01939e3e-aaaa-7000-8000-000000000013";
    const DROPPED_DELETE: &str = "01939e3e-aaaa-7000-8000-000000000014";
    for failure in [
        ResolutionFailure::PoisonedConnection,
        ResolutionFailure::StatementAborts,
    ] {
        let conn = crate::db::test_conn();
        for id in [RELEASED, DROPPED_RESTORE, COMMITTED, DROPPED_DELETE] {
            insert_row(&conn, id, "sbx", &format!("/artifact/{id}"));
        }
        let (db, store) = shared(conn);
        let released = store.check_out_for_restore(artifact_id(RELEASED)).unwrap();
        let dropped_restore = store
            .check_out_for_restore(artifact_id(DROPPED_RESTORE))
            .unwrap();
        let committed = store.check_out_for_delete(artifact_id(COMMITTED)).unwrap();
        let dropped_delete = store
            .check_out_for_delete(artifact_id(DROPPED_DELETE))
            .unwrap();
        match failure {
            ResolutionFailure::PoisonedConnection => {
                let poisoner = Arc::clone(&db);
                std::thread::spawn(move || {
                    let _held = poisoner.lock().unwrap();
                    panic!("poison the shared connection");
                })
                .join()
                .unwrap_err();
                assert!(matches!(
                    store.check_out_for_restore(artifact_id(RELEASED)),
                    Err(CheckoutError::Db(_))
                ));
            }
            ResolutionFailure::StatementAborts => db
                .lock()
                .unwrap()
                .execute_batch(
                    "CREATE TEMP TRIGGER fail_update BEFORE UPDATE ON local_artifacts
                     BEGIN SELECT RAISE(ABORT, 'injected update failure'); END;
                     CREATE TEMP TRIGGER fail_delete BEFORE DELETE ON local_artifacts
                     BEGIN SELECT RAISE(ABORT, 'injected delete failure'); END;",
                )
                .unwrap(),
        }

        assert!(released.release().is_err(), "{failure:?}");
        assert!(committed.commit().is_err(), "{failure:?}");
        drop(dropped_restore);
        drop(dropped_delete);

        for (id, state) in [
            (RELEASED, "restoring"),
            (DROPPED_RESTORE, "restoring"),
            (COMMITTED, "deleting"),
            (DROPPED_DELETE, "deleting"),
        ] {
            assert_eq!(row_state(&db, id).as_deref(), Some(state), "{failure:?}");
        }
    }
}

fn insert_named_rows(conn: &Connection, sandbox_id: &str, count: usize) {
    let tx = rusqlite::Transaction::new_unchecked(conn, rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    for index in 0..count {
        crate::db::publish_named_local_artifact(
            &tx,
            sandbox_id,
            LocalArtifactId::new(),
            ArtifactKind::Disk,
            &format!("d-{index}").parse().unwrap(),
            &format!("/snapshots/d-{index}"),
        )
        .unwrap();
    }
    tx.commit().unwrap();
}

/// The inventory is complete or an error: an undecodable row or an
/// over-budget result fails the read instead of shortening it.
#[test]
fn named_publication_inventory_fails_rather_than_returning_a_partial_answer() {
    use crate::db::inventory_read::InventoryReadError;
    use pplx_space_common_core::inventory::MAX_ITEMS;

    let conn = crate::db::test_conn();
    insert_named_rows(&conn, "full", MAX_ITEMS);
    insert_named_rows(&conn, "over", MAX_ITEMS + 1);
    let corruptions = [
        (
            "corrupt-name",
            "UPDATE local_artifacts SET snapshot_name = 'not/a/name'
              WHERE sandbox_id = ?1 AND snapshot_name = 'd-1'",
        ),
        (
            "corrupt-kind",
            "UPDATE local_artifacts SET artifact_kind = 'bogus'
              WHERE sandbox_id = ?1 AND snapshot_name = 'd-1'",
        ),
        (
            "corrupt-state",
            "UPDATE local_artifacts SET state = 'bogus'
              WHERE sandbox_id = ?1 AND snapshot_name = 'd-1'",
        ),
    ];
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    for (sandbox_id, corrupt) in corruptions {
        insert_named_rows(&conn, sandbox_id, 2);
        conn.execute(corrupt, [sandbox_id]).unwrap();
    }
    conn.pragma_update(None, "ignore_check_constraints", false)
        .unwrap();
    let (_db, store) = shared(conn);

    assert_eq!(
        store.list_named_publications("full").unwrap().len(),
        MAX_ITEMS
    );
    assert!(matches!(
        store.list_named_publications("over"),
        Err(InventoryReadError::Limit(_))
    ));
    for (sandbox_id, _) in corruptions {
        assert!(
            matches!(
                store.list_named_publications(sandbox_id),
                Err(InventoryReadError::Database(_))
            ),
            "{sandbox_id}"
        );
    }
}
