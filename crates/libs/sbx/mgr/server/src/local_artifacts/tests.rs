use crate::db::Connection;
use crate::db::fixture::params;
use pplx_space_common_core::types::SandboxId;

use super::*;

struct GcReadRace {
    started: std::sync::mpsc::Sender<()>,
    finished: std::sync::mpsc::Receiver<()>,
}

thread_local! {
    static GC_READ_RACE: std::cell::RefCell<Option<GcReadRace>> = const { std::cell::RefCell::new(None) };
}

fn race_restore_release_at_gc_read(event: crate::db::fixture::TraceEvent<'_>) {
    if let crate::db::fixture::TraceEvent::Row(statement) = event
        && statement
            .sql()
            .starts_with("SELECT created_at, COALESCE(last_touched_at, created_at)")
    {
        GC_READ_RACE.with(|race| {
            if let Some(race) = race.take() {
                race.started.send(()).unwrap();
                race.finished
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
        });
    }
}

fn open_test_db() -> Connection {
    crate::db::test_conn()
}

fn insert_row(conn: &Connection, id: &str, sandbox_id: &str, local_path: &str) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at
         ) VALUES (?1, ?2, 'full', 'suspend', NULL, ?3, '0')",
        params![id, sandbox_id, local_path],
    )
    .unwrap();
}

fn artifact_id(s: &str) -> LocalArtifactId {
    s.parse().unwrap()
}

#[test]
fn gc_checkout_serializes_eligibility_with_a_concurrent_restore_release() {
    let root = tempfile::TempDir::new().unwrap();
    let path = root.path().join("artifacts.db");
    let mut conn = Connection::open_at(&path).unwrap();
    crate::db::create_test_schema(&mut conn);
    conn.pragma_update(None, "journal_mode", "WAL").unwrap();
    let id = "01939e3e-1111-7000-8000-000000000001";
    insert_row(&conn, id, "sbx", "/artifact");
    conn.execute(
        "UPDATE local_artifacts SET backup_completed_at = '0' WHERE local_artifact_id = ?1",
        [id],
    )
    .unwrap();
    let competing = Connection::open_at(&path).unwrap();
    competing.busy_timeout(std::time::Duration::ZERO).unwrap();
    let competing_store = LocalArtifacts::new(crate::db::SharedConnection::new(competing));
    let restore = competing_store
        .check_out_for_restore(artifact_id(id))
        .unwrap();
    let (start_tx, start_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    GC_READ_RACE.with(|race| {
        *race.borrow_mut() = Some(GcReadRace {
            started: start_tx,
            finished: done_rx,
        })
    });
    conn.trace_v2(
        crate::db::fixture::TraceEventCodes::SQLITE_TRACE_ROW,
        Some(race_restore_release_at_gc_read),
    );
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));
    let writer = std::thread::spawn(move || {
        start_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let result = restore.release();
        done_tx.send(()).unwrap();
        result
    });
    let checkout = store.check_out_for_gc(
        artifact_id(id),
        "sbx",
        GcEligibility {
            now_secs: crate::auto_lifecycle::now_secs(),
            idle_secs: 0,
            grace_secs: 60,
        },
    );
    assert!(matches!(
        checkout,
        Err(CheckoutError::NotReady {
            state: ArtifactState::Restoring,
            ..
        })
    ));
    assert!(matches!(writer.join().unwrap(), Err(error) if error.is_database_busy()));
    assert!(
        GC_READ_RACE.with(|race| race.borrow().is_none()),
        "the competing writer must run at the eligibility read"
    );
}

#[test]
fn gc_checkout_requires_current_backup_pin_and_time_evidence() {
    let conn = open_test_db();
    let id = "01939e3e-1111-7000-8000-000000000001";
    insert_row(&conn, id, "sbx", "/artifact");
    let shared = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(shared.clone());
    let eligibility = GcEligibility {
        now_secs: 100,
        idle_secs: 0,
        grace_secs: 60,
    };
    assert!(matches!(
        store
            .check_out_for_gc(artifact_id(id), "sbx", eligibility)
            .unwrap(),
        GcDeleteCheckout::Unbacked
    ));
    shared
        .lock()
        .unwrap()
        .execute_batch(
            "UPDATE local_artifacts SET backup_completed_at = '0', last_touched_at = '99';",
        )
        .unwrap();
    assert!(matches!(
        store
            .check_out_for_gc(artifact_id(id), "sbx", eligibility)
            .unwrap(),
        GcDeleteCheckout::Recent
    ));
    shared.lock().unwrap().execute_batch(
        "UPDATE local_artifacts SET last_touched_at = '0';
         INSERT INTO sandboxes (id, uuid8, state, template, user, runtime, created_at, updated_at, current_local_artifact_id, current_local_artifact_kind, current_local_artifact_origin)
         VALUES ('sbx', 'gc-test1', 'suspended', 'seed', 'u', 'fc', '0', '0', '01939e3e-1111-7000-8000-000000000001', 'full', 'suspend');",
    ).unwrap();
    assert!(matches!(
        store
            .check_out_for_gc(artifact_id(id), "sbx", eligibility)
            .unwrap(),
        GcDeleteCheckout::Pinned
    ));
    assert!(matches!(
        store.check_out_for_delete(artifact_id(id)),
        Err(CheckoutError::Pinned(pinned)) if pinned == artifact_id(id)
    ));
    let state: String = shared
        .lock()
        .unwrap()
        .query_row(
            "SELECT state FROM local_artifacts WHERE local_artifact_id = ?1",
            [id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(state, "ready");
    shared
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes
                SET suspend_predecessor_artifact_id = current_local_artifact_id,
                    current_local_artifact_id = NULL,
                    current_local_artifact_kind = NULL,
                    current_local_artifact_origin = NULL
              WHERE id = 'sbx'",
            [],
        )
        .unwrap();
    assert!(matches!(
        store
            .check_out_for_gc(artifact_id(id), "sbx", eligibility)
            .unwrap(),
        GcDeleteCheckout::Pinned
    ));
    assert!(matches!(
        store.check_out_for_delete(artifact_id(id)),
        Err(CheckoutError::Pinned(pinned)) if pinned == artifact_id(id)
    ));
    shared
        .lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes
                SET suspend_predecessor_artifact_id = NULL
              WHERE id = 'sbx'",
            [],
        )
        .unwrap();
    let GcDeleteCheckout::Leased(lease) = store
        .check_out_for_gc(artifact_id(id), "sbx", eligibility)
        .unwrap()
    else {
        panic!("an old unpinned backed-up Ready artifact must be eligible");
    };
    assert_eq!(lease.local_path(), "/artifact");
    assert!(matches!(
        store.check_out_for_restore(artifact_id(id)),
        Err(CheckoutError::NotReady {
            state: ArtifactState::Deleting,
            ..
        })
    ));
}

#[test]
fn named_publication_inventory_reports_state_without_taking_a_lease() {
    let conn = open_test_db();
    let id = "01939e3e-1111-7000-8000-000000000001";
    insert_row(&conn, id, "sbx", "/tmp/art");
    conn.execute("UPDATE local_artifacts SET artifact_origin = 'snapshot', snapshot_name = 'f-manual' WHERE local_artifact_id = ?1", [id]).unwrap();
    insert_row(
        &conn,
        "01939e3e-1111-7000-8000-000000000002",
        "other",
        "/tmp/other",
    );
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));
    let check = |state| {
        let publications = store.list_named_publications("sbx").unwrap();
        assert_eq!(publications.len(), 1);
        assert_eq!(publications[0].snapshot_name.as_str(), "f-manual");
        assert_eq!(publications[0].local_path, "/tmp/art");
        assert_eq!(publications[0].artifact_kind, ArtifactKind::Full);
        assert_eq!(publications[0].state, state);
        assert!(store.list_named_publications("other").unwrap().is_empty());
    };
    check(ArtifactState::Ready);
    let restore = store.check_out_for_restore(artifact_id(id)).unwrap();
    check(ArtifactState::Restoring);
    restore.release().unwrap();
    let delete = store.check_out_for_delete(artifact_id(id)).unwrap();
    check(ArtifactState::Deleting);
    delete.commit().unwrap();
    assert!(store.list_named_publications("sbx").unwrap().is_empty());
}

#[test]
fn check_out_for_restore_flips_state_and_releases_back() {
    let conn = open_test_db();
    let id = "01939e3e-1111-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-1", "/tmp/art");
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let lease = store.check_out_for_restore(artifact_id(id)).unwrap();
    // While the lease is held, a second checkout (restore or
    // delete) must reject with NotReady.
    let second = store.check_out_for_restore(artifact_id(id));
    assert!(matches!(
        second,
        Err(CheckoutError::NotReady {
            state: ArtifactState::Restoring,
            ..
        })
    ));

    lease.release().unwrap();

    // After release, a fresh checkout works.
    let _lease = store.check_out_for_restore(artifact_id(id)).unwrap();
}

#[test]
fn check_out_for_delete_blocks_concurrent_restore_and_vice_versa() {
    let conn = open_test_db();
    let id = "01939e3e-2222-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-2", "/tmp/art2");
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let del = store.check_out_for_delete(artifact_id(id)).unwrap();
    let restore_attempt = store.check_out_for_restore(artifact_id(id));
    assert!(matches!(
        restore_attempt,
        Err(CheckoutError::NotReady {
            state: ArtifactState::Deleting,
            ..
        })
    ));
    // Dropping the delete lease rolls back to ready; restore now works.
    drop(del);
    let _restore_ok = store.check_out_for_restore(artifact_id(id)).unwrap();
}

#[test]
fn check_out_for_delete_refuses_shared_local_path() {
    let conn = open_test_db();
    let id_a = "01939e3e-3333-7000-8000-000000000001";
    let id_b = "01939e3e-3333-7000-8000-000000000002";
    insert_row(&conn, id_a, "sbx-3", "/tmp/shared");
    insert_row(&conn, id_b, "sbx-3", "/tmp/shared");
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let attempt = store.check_out_for_delete(artifact_id(id_a));
    assert!(matches!(
        attempt,
        Err(CheckoutError::SharedLocalPath {
            sibling_count: 1,
            ..
        })
    ));
}

#[test]
fn delete_lease_commit_removes_row() {
    let conn = open_test_db();
    let id = "01939e3e-4444-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-4", "/tmp/art4");
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let lease = store.check_out_for_delete(artifact_id(id)).unwrap();
    lease.commit().unwrap();

    let gone = store.get(artifact_id(id)).unwrap();
    assert!(gone.is_none());
}

#[test]
fn restore_lease_drop_without_release_rolls_back_to_ready() {
    let conn = open_test_db();
    let id = "01939e3e-5555-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-5", "/tmp/art5");
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    {
        let _lease = store.check_out_for_restore(artifact_id(id)).unwrap();
        // simulate panic / early return: leave the scope without calling release()
    }

    // Drop should have rolled back to ready; next checkout succeeds.
    let _again = store.check_out_for_restore(artifact_id(id)).unwrap();
}

/// `release()` is the success path and must bump
/// `last_touched_at` so the snapshot reaper's idle clock resets
/// after a successful operation (typically restore). `Drop`
/// without release is the abort path and must NOT bump the
/// timestamp — that would mask an aborted restore from the
/// reaper.
#[test]
fn restore_lease_release_bumps_last_touched_at_but_drop_does_not() {
    let conn = crate::db::test_conn();
    // Seed an artifact with last_touched_at = "0" (long in the past).
    let id_a = "01939e3e-7777-7000-8000-000000000001";
    let id_b = "01939e3e-7777-7000-8000-000000000002";
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, last_touched_at
         ) VALUES (?1, 'sbx-7', 'full', 'suspend', NULL, '/tmp/a', '0', '0'),
                 (?2, 'sbx-7', 'full', 'suspend', NULL, '/tmp/b', '0', '0')",
        params![id_a, id_b],
    )
    .unwrap();
    // Two distinct sandboxes so the dedupe in publish_suspended doesn't apply
    // — these are direct-SQL inserts so dedupe never fires anyway. Fine.
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    // Path A: release() — must bump last_touched_at.
    let lease_a = store.check_out_for_restore(artifact_id(id_a)).unwrap();
    lease_a.release().unwrap();
    let touched_a: String = store
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT last_touched_at FROM local_artifacts WHERE local_artifact_id = ?1",
            params![id_a],
            |r| r.get(0),
        )
        .unwrap();
    assert_ne!(
        touched_a, "0",
        "release() must bump last_touched_at; saw it still equal to seed value '0'"
    );

    // Path B: Drop without release() — must NOT bump.
    {
        let _lease_b = store.check_out_for_restore(artifact_id(id_b)).unwrap();
        // dropped at scope end without release()
    }
    let touched_b: String = store
        .db
        .lock()
        .unwrap()
        .query_row(
            "SELECT last_touched_at FROM local_artifacts WHERE local_artifact_id = ?1",
            params![id_b],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        touched_b, "0",
        "Drop must not bump last_touched_at (it's the abort path)"
    );
}

#[test]
fn check_out_returns_not_found_for_missing_row() {
    let conn = open_test_db();
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));
    let attempt = store.check_out_for_restore(artifact_id("01939e3e-6666-7000-8000-000000000001"));
    assert!(matches!(attempt, Err(CheckoutError::NotFound(_))));
}

#[test]
fn artifact_state_round_trips_via_from_str() {
    for state in [
        ArtifactState::Ready,
        ArtifactState::Restoring,
        ArtifactState::Deleting,
    ] {
        assert_eq!(state.as_str().parse::<ArtifactState>().unwrap(), state);
    }
    "bogus"
        .parse::<ArtifactState>()
        .expect_err("bogus artifact state must fail to parse");
}

fn force_state(conn: &crate::db::SharedConnection, id: &str, state: &str) {
    conn.lock()
        .unwrap()
        .execute(
            "UPDATE local_artifacts SET state = ?1 WHERE local_artifact_id = ?2",
            params![state, id],
        )
        .unwrap();
}

/// Process death skips lease `Drop`; a row left in `restoring`
/// must be reset to `ready` at startup (the bytes were only
/// being read, so the disk is intact).
#[test]
fn recover_resets_restoring_rows_to_ready() {
    let conn = open_test_db();
    let id = "01939e3e-8881-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-r1", "/tmp/recover-restoring");
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());
    force_state(&db, id, "restoring");

    let report = store.recover_stuck_states().unwrap();
    assert_eq!(report.restoring_reset, 1);
    assert_eq!(report.deleting_rolled_back, 0);
    assert_eq!(report.deleting_finished, 0);
    // The row is usable again.
    let _lease = store.check_out_for_restore(artifact_id(id)).unwrap();
}

/// Crash before the rename: the original directory still exists
/// and no staging dir does → roll back to `ready`, keep the row.
#[test]
fn recover_rolls_back_deleting_when_original_bytes_intact() {
    let conn = open_test_db();
    let tmp = tempfile::TempDir::new().unwrap();
    let local_path = tmp.path().join("artifact");
    std::fs::create_dir_all(&local_path).unwrap();
    let local_path = local_path.to_str().unwrap().to_string();

    let id = "01939e3e-8882-7000-8000-000000000001";
    insert_row(&conn, id, "sbx-r2", &local_path);
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());
    force_state(&db, id, "deleting");

    let report = store.recover_stuck_states().unwrap();
    assert_eq!(report.deleting_rolled_back, 1);
    assert_eq!(report.deleting_finished, 0);
    assert!(store.get(artifact_id(id)).unwrap().is_some());
    // The row is checkout-able again.
    let _lease = store.check_out_for_delete(artifact_id(id)).unwrap();
}

/// Crash after the rename but before the row DELETE: the bytes
/// sit at the `.gc-pending-<id>` staging path → finish the
/// delete (remove the row). Resetting to `ready` would advertise
/// an artifact whose `local_path` is gone.
#[test]
fn recover_finishes_delete_when_staging_dir_present() {
    let conn = open_test_db();
    let tmp = tempfile::TempDir::new().unwrap();
    let local_path = tmp.path().join("artifact").to_str().unwrap().to_string();
    let id = "01939e3e-8883-7000-8000-000000000001";
    std::fs::create_dir_all(format!("{local_path}.gc-pending-{id}")).unwrap();

    insert_row(&conn, id, "sbx-r3", &local_path);
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());
    force_state(&db, id, "deleting");

    let report = store.recover_stuck_states().unwrap();
    assert_eq!(report.deleting_finished, 1);
    assert_eq!(report.deleting_rolled_back, 0);
    assert!(store.get(artifact_id(id)).unwrap().is_none());
}

/// Crash after a rename that hit ENOENT (disk already gone) and
/// before the row DELETE: neither path exists → finish the
/// delete.
#[test]
fn recover_finishes_delete_when_neither_path_present() {
    let conn = open_test_db();
    let tmp = tempfile::TempDir::new().unwrap();
    let local_path = tmp
        .path()
        .join("never-created")
        .to_str()
        .unwrap()
        .to_string();
    let id = "01939e3e-8884-7000-8000-000000000001";

    insert_row(&conn, id, "sbx-r4", &local_path);
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());
    force_state(&db, id, "deleting");

    let report = store.recover_stuck_states().unwrap();
    assert_eq!(report.deleting_finished, 1);
    assert!(store.get(artifact_id(id)).unwrap().is_none());
}

#[test]
fn prune_missing_orphan_rows_deletes_only_missing_unowned_rows() {
    let conn = open_test_db();
    let tmp = tempfile::TempDir::new().unwrap();
    let present = tmp.path().join("present");
    std::fs::create_dir_all(&present).unwrap();
    let present = present.to_str().unwrap().to_string();
    let missing_orphan = "01939e3e-8890-7000-8000-000000000001";
    let present_orphan = "01939e3e-8890-7000-8000-000000000002";
    let missing_owned = "01939e3e-8890-7000-8000-000000000003";

    insert_row(
        &conn,
        missing_orphan,
        "sbx-missing-orphan",
        "/tmp/space-missing-orphan",
    );
    insert_row(&conn, present_orphan, "sbx-present-orphan", &present);
    insert_row(
        &conn,
        missing_owned,
        "sbx-owned",
        "/tmp/space-missing-owned",
    );
    conn.execute(
        "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json
         ) VALUES ('sbx-owned', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}')",
        [],
    )
    .unwrap();
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());

    let pruned = store.prune_missing_orphan_rows(10).unwrap();

    assert_eq!(pruned, 1);
    assert!(store.get(artifact_id(missing_orphan)).unwrap().is_none());
    assert!(store.get(artifact_id(present_orphan)).unwrap().is_some());
    assert!(store.get(artifact_id(missing_owned)).unwrap().is_some());
}

#[test]
fn prune_missing_orphan_rows_obeys_batch_limit() {
    let conn = open_test_db();
    let first = "01939e3e-8891-7000-8000-000000000001";
    let second = "01939e3e-8891-7000-8000-000000000002";
    insert_row(&conn, first, "sbx-orphan-1", "/tmp/space-orphan-1");
    insert_row(&conn, second, "sbx-orphan-2", "/tmp/space-orphan-2");
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());

    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 1);
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 1);
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 0);
}

#[test]
fn prune_missing_orphan_rows_advances_past_preserved_rows_and_wraps() {
    let conn = crate::db::test_conn();
    let tmp = tempfile::TempDir::new().unwrap();
    let present = tmp.path().join("present");
    std::fs::create_dir(&present).unwrap();
    let first = "01939e3e-8892-7000-8000-000000000001";
    let malformed = "01939e3e-8892-7000-8000-000000000002";
    let owned = "01939e3e-8892-7000-8000-000000000003";
    let missing = "01939e3e-8892-7000-8000-000000000004";
    let fresh = "01939e3e-8892-7000-8000-000000000005";
    insert_row(&conn, first, "preserved", present.to_str().unwrap());
    insert_row(&conn, malformed, "malformed", "/unused");
    conn.execute(
        "UPDATE local_artifacts SET local_path = X'00' WHERE local_artifact_id = ?1",
        [malformed],
    )
    .unwrap();
    let owner = SandboxId::new().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(
        &conn,
        &owner,
        crate::state::SandboxState::Suspended,
    );
    insert_row(
        &conn,
        owned,
        &owner,
        tmp.path().join("owned-missing").to_str().unwrap(),
    );
    insert_row(
        &conn,
        missing,
        "missing",
        tmp.path().join("missing").to_str().unwrap(),
    );
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());

    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 0);
    insert_row(
        &db.lock().unwrap(),
        fresh,
        "fresh",
        tmp.path().join("fresh").to_str().unwrap(),
    );
    // Malformed and owned entries consume the same raw discovery budget.
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 0);
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 0);
    // A cloned worker handle must retain the progress of the previous tick.
    assert_eq!(store.clone().prune_missing_orphan_rows(1).unwrap(), 1);
    assert!(store.get(artifact_id(missing)).unwrap().is_none());
    std::fs::remove_dir(&present).unwrap();
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 1);
    assert!(store.get(artifact_id(first)).unwrap().is_none());
    // Inserts cannot extend a running sweep and delay its retained prefix.
    assert!(store.get(artifact_id(fresh)).unwrap().is_some());
    assert!(store.get(artifact_id(owned)).unwrap().is_some());
    assert_eq!(store.prune_missing_orphan_rows(10).unwrap(), 1);
    assert!(store.get(artifact_id(fresh)).unwrap().is_none());
}

#[cfg(unix)]
#[test]
fn prune_missing_orphan_rows_preserves_unreadable_paths() {
    let conn = crate::db::test_conn();
    let tmp = tempfile::TempDir::new().unwrap();
    let loop_path = tmp.path().join("loop");
    std::os::unix::fs::symlink(&loop_path, &loop_path).unwrap();
    loop_path.try_exists().unwrap_err();
    let id = "01939e3e-8893-7000-8000-000000000001";
    insert_row(&conn, id, "unreadable", loop_path.to_str().unwrap());
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));
    assert_eq!(store.prune_missing_orphan_rows(1).unwrap(), 0);
    assert!(store.get(artifact_id(id)).unwrap().is_some());
}

#[test]
fn recover_pinned_deletion_requires_unambiguous_original_bytes() {
    for (original, staging, unreadable) in [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (true, true, false),
        (false, false, true),
    ] {
        let conn = open_test_db();
        let root = tempfile::TempDir::new().unwrap();
        let path = root.path().join("snapshot");
        let id = "01939e3e-8885-7000-8000-000000000001";
        if original {
            std::fs::create_dir(&path).unwrap();
            std::fs::write(path.join("memory"), b"retained evidence").unwrap();
        }
        if staging {
            std::fs::create_dir(format!("{}.gc-pending-{id}", path.display())).unwrap();
        }
        if unreadable {
            std::os::unix::fs::symlink(&path, &path).unwrap();
        }
        insert_row(&conn, id, "sbx-r5", path.to_str().unwrap());
        conn.execute(
            "INSERT INTO sandboxes (
             id, uuid8, state, template, user, runtime, created_at, updated_at,
             process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
             current_local_artifact_id, current_local_artifact_kind,
             current_local_artifact_origin
         ) VALUES ('sbx-r5', 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                   '{}', '{}', '{}', ?1, 'full', 'suspend')",
            params![id],
        )
        .unwrap();
        let db = crate::db::SharedConnection::new(conn);
        let store = LocalArtifacts::new(db.clone());
        force_state(&db, id, "deleting");

        let report = store.recover_stuck_states().unwrap();
        let ready = original && !staging;
        assert_eq!(report.deleting_rolled_back, usize::from(ready));
        assert_eq!(report.deleting_deferred, usize::from(!ready));
        assert_eq!(report.deleting_finished, 0);
        let state: String = db
            .lock()
            .unwrap()
            .query_row(
                "SELECT state FROM local_artifacts WHERE local_artifact_id = ?1",
                [id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, if ready { "ready" } else { "deleting" });
        assert!(!store.gc_staging_is_unowned(artifact_id(id)).unwrap());
        if !ready && !unreadable {
            db.lock()
                .unwrap()
                .execute(
                    "UPDATE sandboxes SET current_local_artifact_id = NULL,
             current_local_artifact_kind = NULL, current_local_artifact_origin = NULL",
                    [],
                )
                .unwrap();
            assert_eq!(store.recover_stuck_states().unwrap().deleting_finished, 1);
            assert!(store.gc_staging_is_unowned(artifact_id(id)).unwrap());
            db.lock()
                .unwrap()
                .execute(
                    "UPDATE sandboxes SET suspend_predecessor_artifact_id = ?1",
                    [id],
                )
                .unwrap();
            assert!(!store.gc_staging_is_unowned(artifact_id(id)).unwrap());
        }
    }
}

fn pending_entry(sandbox_id: &str, artifact_id: &str, kind: ArtifactKind) -> PendingBackup {
    PendingBackup {
        sandbox_id: sandbox_id.to_string(),
        local_artifact_id: artifact_id.parse().unwrap(),
        artifact_kind: kind,
    }
}

/// Seed a sandbox row, optionally pinning `pin` as its current lifecycle
/// artifact. Mirrors what every publish path writes: the pin's kind and
/// origin land on the sandbox row in the same statement as its id.
fn insert_pinned_sandbox(conn: &Connection, sandbox_id: &str, pin: Option<(&str, &str, &str)>) {
    match pin {
        Some((artifact_id, kind, origin)) => conn.execute(
            "INSERT INTO sandboxes (
                 id, uuid8, state, template, user, runtime, created_at, updated_at,
                 process_env_json, auto_lifecycle_json, auto_lifecycle_state_json,
                 current_local_artifact_id, current_local_artifact_kind,
                 current_local_artifact_origin
             ) VALUES (?1, 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                       '{}', '{}', '{}', ?2, ?3, ?4)",
            params![sandbox_id, artifact_id, kind, origin],
        ),
        None => conn.execute(
            "INSERT INTO sandboxes (
                 id, uuid8, state, template, user, runtime, created_at, updated_at,
                 process_env_json, auto_lifecycle_json, auto_lifecycle_state_json
             ) VALUES (?1, 'uuid8', 'suspended', 'seed', 'u', 'fc', '0', '0',
                       '{}', '{}', '{}')",
            params![sandbox_id],
        ),
    }
    .unwrap();
}

fn insert_artifact(
    conn: &Connection,
    id: &str,
    sandbox_id: &str,
    kind: &str,
    origin: &str,
    snapshot_name: Option<&str>,
    backup_completed_at: Option<&str>,
) {
    conn.execute(
        "INSERT INTO local_artifacts (
             local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
             snapshot_name, local_path, created_at, backup_completed_at
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, '0', ?7)",
        params![
            id,
            sandbox_id,
            kind,
            origin,
            snapshot_name,
            format!("/tmp/{id}"),
            backup_completed_at
        ],
    )
    .unwrap();
}

/// The acceptance matrix: inclusion and exclusion both asserted against
/// one fixture that mixes backed-up, pending, and multi-origin artifacts.
///
/// The exclusions are each a different mechanism, so they are asserted
/// separately below rather than only through the set equality.
#[test]
fn list_pending_backups_returns_exactly_the_unbacked_pinned_artifacts() {
    let conn = open_test_db();

    // Pending and pinned: the answer.
    let pending_full = "01939e3e-9001-7000-8000-000000000001";
    insert_artifact(
        &conn,
        pending_full,
        "sbx-pending-full",
        "full",
        "suspend",
        None,
        None,
    );
    insert_pinned_sandbox(
        &conn,
        "sbx-pending-full",
        Some((pending_full, "full", "suspend")),
    );

    // A stopped sandbox's pending disk artifact is equally at risk, and
    // the queue's artifact_kind column admits both kinds.
    let pending_disk = "01939e3e-9002-7000-8000-000000000001";
    insert_artifact(
        &conn,
        pending_disk,
        "sbx-pending-disk",
        "disk",
        "stop",
        None,
        None,
    );
    insert_pinned_sandbox(
        &conn,
        "sbx-pending-disk",
        Some((pending_disk, "disk", "stop")),
    );

    // Backed up and pinned: excluded by the backup_completed_at stamp.
    let backed_up = "01939e3e-9003-7000-8000-000000000001";
    insert_artifact(
        &conn,
        backed_up,
        "sbx-backed-up",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_pinned_sandbox(&conn, "sbx-backed-up", Some((backed_up, "full", "suspend")));

    // Multi-origin: this sandbox's pin is backed up, but it also holds an
    // unbacked named snapshot. Named snapshots are not lifecycle artifacts
    // and cannot be a pin, so the sandbox is absent from the answer.
    let multi_origin_pin = "01939e3e-9004-7000-8000-000000000001";
    let multi_origin_snapshot = "01939e3e-9004-7000-8000-000000000002";
    insert_artifact(
        &conn,
        multi_origin_pin,
        "sbx-multi-origin",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_artifact(
        &conn,
        multi_origin_snapshot,
        "sbx-multi-origin",
        "disk",
        "snapshot",
        Some("checkpoint"),
        None,
    );
    insert_pinned_sandbox(
        &conn,
        "sbx-multi-origin",
        Some((multi_origin_pin, "full", "suspend")),
    );

    // Superseded: an unbacked artifact the sandbox no longer pins. One row
    // per (node, sandbox) is the queue's key, and the pin is which one.
    let superseded = "01939e3e-9005-7000-8000-000000000001";
    let current = "01939e3e-9005-7000-8000-000000000002";
    insert_artifact(
        &conn,
        superseded,
        "sbx-resuspended",
        "full",
        "suspend",
        None,
        None,
    );
    insert_artifact(
        &conn,
        current,
        "sbx-resuspended",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_pinned_sandbox(&conn, "sbx-resuspended", Some((current, "full", "suspend")));

    // A running sandbox pins nothing and holds nothing.
    insert_pinned_sandbox(&conn, "sbx-running", None);

    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let pending = store.list_pending_backups().unwrap();

    assert_eq!(
        pending,
        vec![
            pending_entry("sbx-pending-disk", pending_disk, ArtifactKind::Disk),
            pending_entry("sbx-pending-full", pending_full, ArtifactKind::Full),
        ]
    );
}

/// Exclusion by pin, isolated from exclusion by backup stamp: move the
/// pin onto the unbacked artifact and it appears. Without this the set
/// equality above would also hold if the query ignored the pin entirely
/// and the superseded row had simply been missed.
#[test]
fn list_pending_backups_follows_the_pin_not_the_sandbox_id() {
    let conn = open_test_db();
    let unpinned = "01939e3e-9101-7000-8000-000000000001";
    let pinned = "01939e3e-9101-7000-8000-000000000002";
    insert_artifact(&conn, unpinned, "sbx-repin", "full", "suspend", None, None);
    insert_artifact(
        &conn,
        pinned,
        "sbx-repin",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_pinned_sandbox(&conn, "sbx-repin", Some((pinned, "full", "suspend")));
    let db = crate::db::SharedConnection::new(conn);
    let store = LocalArtifacts::new(db.clone());

    assert_eq!(store.list_pending_backups().unwrap(), Vec::new());

    db.lock()
        .unwrap()
        .execute(
            "UPDATE sandboxes SET current_local_artifact_id = ?1 WHERE id = 'sbx-repin'",
            params![unpinned],
        )
        .unwrap();

    assert_eq!(
        store.list_pending_backups().unwrap(),
        vec![pending_entry("sbx-repin", unpinned, ArtifactKind::Full)]
    );
}

/// A pin naming an artifact row that belongs to a *different* sandbox is
/// the same unprovable state as a pin naming no row at all, so dropping it
/// would omit the sandbox from a report the drain trusts as complete.
///
/// `sandbox_eviction` keys its own lookup on the same pair and reaches
/// `PinnedRowMissing` here, so the two surfaces agree that a borrowed pin
/// proves nothing.
///
/// The `a.sandbox_id = s.id` conjunct is what makes the join miss; without
/// it the borrower would inherit the owner's backup stamp and read as
/// durable.
#[test]
fn list_pending_backups_reports_a_pin_owned_by_another_sandbox() {
    let conn = open_test_db();
    let owned_by_other = "01939e3e-9201-7000-8000-000000000001";
    insert_artifact(
        &conn,
        owned_by_other,
        "sbx-owner",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_pinned_sandbox(&conn, "sbx-owner", None);
    insert_pinned_sandbox(
        &conn,
        "sbx-borrower",
        Some((owned_by_other, "full", "suspend")),
    );
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    assert_eq!(
        store.list_pending_backups().unwrap(),
        vec![pending_entry(
            "sbx-borrower",
            owned_by_other,
            ArtifactKind::Full
        )]
    );
}

/// A pin whose `local_artifacts` row is gone is reported, not skipped.
/// The bytes may still be on disk with nothing left to describe them, and
/// an inner join would drop the sandbox out of a report the drain trusts
/// as complete — releasing the node over exactly the artifact nobody can
/// account for. Reporting only ever adds a candidate: the drain re-applies
/// the durability predicate, so a pin whose bytes are genuinely durable
/// still clears.
#[test]
fn list_pending_backups_reports_a_pin_whose_artifact_row_is_gone() {
    let conn = open_test_db();
    let vanished = "01939e3e-9202-7000-8000-000000000001";
    let healthy = "01939e3e-9202-7000-8000-000000000002";
    insert_pinned_sandbox(&conn, "sbx-vanished", Some((vanished, "disk", "stop")));
    insert_artifact(
        &conn,
        healthy,
        "sbx-healthy",
        "full",
        "suspend",
        None,
        Some("2026-09-19T00:00:00Z"),
    );
    insert_pinned_sandbox(&conn, "sbx-healthy", Some((healthy, "full", "suspend")));
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    assert_eq!(
        store.list_pending_backups().unwrap(),
        vec![pending_entry("sbx-vanished", vanished, ArtifactKind::Disk)],
        "the backed-up sibling proves the query still excludes what it can prove"
    );
}

/// A row nobody can decode fails the call. Reporting the remaining rows
/// would look like a whole answer and be short by exactly the artifact
/// whose identity is unreadable — and a caller that trusts it releases a
/// node over bytes it was never told about.
#[test]
fn list_pending_backups_fails_rather_than_dropping_an_undecodable_row() {
    let conn = open_test_db();
    let readable = "01939e3e-9301-7000-8000-000000000001";
    insert_artifact(
        &conn,
        readable,
        "sbx-readable",
        "full",
        "suspend",
        None,
        None,
    );
    insert_pinned_sandbox(&conn, "sbx-readable", Some((readable, "full", "suspend")));
    conn.pragma_update(None, "ignore_check_constraints", true)
        .unwrap();
    insert_artifact(
        &conn,
        "not-a-uuid",
        "sbx-corrupt",
        "full",
        "suspend",
        None,
        None,
    );
    insert_pinned_sandbox(
        &conn,
        "sbx-corrupt",
        Some(("not-a-uuid", "full", "suspend")),
    );
    conn.pragma_update(None, "ignore_check_constraints", false)
        .unwrap();
    let store = LocalArtifacts::new(crate::db::SharedConnection::new(conn));

    let error = store.list_pending_backups().unwrap_err();

    assert!(
        error.conversion_failure().is_some(),
        "expected a decode failure, got {error:?}"
    );
}
