//! Refusal outcomes of local-artifact checkouts, lease resolution, the
//! named-publication inventory, and named-snapshot import publication.

use std::path::{Path, PathBuf};
use std::sync::PoisonError;

use crate::db::transfer::{SnapshotPublication, TransferBinding, publish_snapshot};
use crate::service::tests::service_tests::insert_test_sandbox;
use crate::state::SandboxState;

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
    assert_eq!(row_state(&db, ID).as_deref(), Some("ready"));
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

/// The checkout's `SELECT` and guarded `UPDATE` share an immediate
/// transaction, so no writer can move the row between them in production.
/// A trigger that skips the update stands in for the guard matching no row.
#[test]
fn checkout_whose_guarded_update_matches_no_row_issues_no_lease() {
    for (checkout_name, checkout) in CHECKOUTS {
        let conn = crate::db::test_conn();
        insert_row(&conn, ID, "sbx", "/artifact");
        conn.execute_batch(
            "CREATE TEMP TRIGGER skip_state_update BEFORE UPDATE OF state ON local_artifacts
             BEGIN SELECT RAISE(IGNORE); END;",
        )
        .unwrap();
        let (db, store) = shared(conn);

        let result = checkout(&store, artifact_id(ID));

        assert!(
            matches!(&result, Err(CheckoutError::NotReady { id, .. }) if *id == artifact_id(ID)),
            "{checkout_name}: {result:?}"
        );
        assert_eq!(
            row_state(&db, ID).as_deref(),
            Some("ready"),
            "{checkout_name}"
        );
        db.lock()
            .unwrap()
            .execute_batch("DROP TRIGGER skip_state_update;")
            .unwrap();
        checkout(&store, artifact_id(ID)).unwrap();
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
                Err(InventoryReadError::Database(
                    rusqlite::Error::FromSqlConversionFailure(..)
                ))
            ),
            "{sandbox_id}"
        );
    }
}

struct ImportFixture {
    _root: tempfile::TempDir,
    db: Connection,
    id: String,
    binding: TransferBinding,
    staging: PathBuf,
    destination: PathBuf,
}

const STAGED_ROOTFS: &[u8] = b"complete";

/// A stopped sandbox and a fully staged import of `d-imported`.
fn import_fixture() -> ImportFixture {
    let root = tempfile::TempDir::new().unwrap();
    let db = crate::db::test_conn();
    let id = uuid::Uuid::now_v7().to_string();
    insert_test_sandbox(&db, &id, SandboxState::Stopped);
    let binding = TransferBinding {
        sandbox_id: id.clone(),
        template: "seed".into(),
        content_hash: "hash".into(),
        stream_fingerprint: "staged-stream".into(),
    };
    let staging = root.path().join("stage");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("rootfs.ext4"), STAGED_ROOTFS).unwrap();
    std::fs::write(
        staging.join(".transfer.json"),
        serde_json::to_vec(&binding).unwrap(),
    )
    .unwrap();
    let destination = root.path().join("snapshots/d-imported");
    ImportFixture {
        _root: root,
        db,
        id,
        binding,
        staging,
        destination,
    }
}

impl ImportFixture {
    fn publish(&self, template: &str, verified_existing: bool) -> Result<bool, String> {
        publish_snapshot(
            &self.db,
            SnapshotPublication {
                id: &self.id,
                template,
                hash: "hash",
                staging: &self.staging,
                destination: &self.destination,
                binding: &self.binding,
                verified_existing,
            },
        )
        .map_err(|error| error.to_string())
    }

    fn named_rows(&self) -> Vec<(String, ArtifactKind, String)> {
        self.db
            .prepare(
                "SELECT local_path, artifact_kind, state FROM local_artifacts
                  WHERE sandbox_id = ?1",
            )
            .unwrap()
            .query_map([&self.id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?.parse().unwrap(),
                    row.get::<_, String>(2)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn assert_staging_retained(&self, case: &str) {
        assert_eq!(
            std::fs::read(self.staging.join("rootfs.ext4")).unwrap(),
            STAGED_ROOTFS,
            "{case}"
        );
    }
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

#[test]
fn snapshot_publication_refuses_a_sandbox_whose_identity_or_directory_is_unsettled() {
    for (case, state, encrypted, template) in [
        ("creating", SandboxState::Creating, false, "seed"),
        ("stopping", SandboxState::Stopping, false, "seed"),
        ("quarantined", SandboxState::Quarantined, false, "seed"),
        ("encrypted", SandboxState::Stopped, true, "seed"),
        ("other template", SandboxState::Stopped, false, "other"),
    ] {
        let fixture = import_fixture();
        fixture
            .db
            .execute(
                "UPDATE sandboxes SET state = ?1, volume_encryption = ?2 WHERE id = ?3",
                params![state.as_str(), encrypted, fixture.id],
            )
            .unwrap();

        assert_eq!(fixture.publish(template, false), Ok(false), "{case}");
        assert!(fixture.named_rows().is_empty(), "{case}");
        assert!(!fixture.destination.exists(), "{case}");
        fixture.assert_staging_retained(case);
    }
}

#[derive(Debug, Clone, Copy)]
enum ChangedDestination {
    SymlinkToMatchingCopy,
    DifferentBinding,
    DifferentFileLength,
    MissingFile,
}

/// A verified retry may adopt an already-published directory only while it
/// still is that publication: a real directory carrying the same binding and
/// every staged file at the staged length.
#[test]
fn verified_snapshot_retry_refuses_a_destination_that_no_longer_matches() {
    for change in [
        ChangedDestination::SymlinkToMatchingCopy,
        ChangedDestination::DifferentBinding,
        ChangedDestination::DifferentFileLength,
        ChangedDestination::MissingFile,
    ] {
        let fixture = import_fixture();
        let case = format!("{change:?}");
        match change {
            ChangedDestination::SymlinkToMatchingCopy => {
                let elsewhere = fixture.staging.with_file_name("elsewhere");
                copy_dir(&fixture.staging, &elsewhere);
                std::fs::create_dir_all(fixture.destination.parent().unwrap()).unwrap();
                std::os::unix::fs::symlink(&elsewhere, &fixture.destination).unwrap();
            }
            ChangedDestination::DifferentBinding => {
                copy_dir(&fixture.staging, &fixture.destination);
                let other = TransferBinding {
                    sandbox_id: fixture.id.clone(),
                    template: "seed".into(),
                    content_hash: "hash".into(),
                    // Same length as the staged binding, so only the
                    // binding comparison can tell the two apart.
                    stream_fingerprint: "stream-staged".into(),
                };
                std::fs::write(
                    fixture.destination.join(".transfer.json"),
                    serde_json::to_vec(&other).unwrap(),
                )
                .unwrap();
            }
            ChangedDestination::DifferentFileLength => {
                copy_dir(&fixture.staging, &fixture.destination);
                std::fs::write(fixture.destination.join("rootfs.ext4"), b"short").unwrap();
            }
            ChangedDestination::MissingFile => {
                copy_dir(&fixture.staging, &fixture.destination);
                std::fs::remove_file(fixture.destination.join("rootfs.ext4")).unwrap();
            }
        }

        assert!(fixture.publish("seed", true).is_err(), "{case}");
        assert!(fixture.named_rows().is_empty(), "{case}");
        fixture.assert_staging_retained(&case);
    }
}

#[test]
fn snapshot_publication_refuses_a_catalog_entry_it_does_not_own() {
    for (case, other_path, kind, state) in [
        ("other path", true, ArtifactKind::Disk, "ready"),
        ("other kind", false, ArtifactKind::Full, "ready"),
        ("held", false, ArtifactKind::Disk, "restoring"),
    ] {
        let fixture = import_fixture();
        let existing_path = if other_path {
            fixture.staging.with_file_name("elsewhere")
        } else {
            fixture.destination.clone()
        };
        let existing_path = existing_path.to_str().unwrap().to_owned();
        crate::db::publish_named_local_artifact(
            &fixture.db,
            &fixture.id,
            LocalArtifactId::new(),
            kind,
            &"d-imported".parse().unwrap(),
            &existing_path,
        )
        .unwrap();
        fixture
            .db
            .execute(
                "UPDATE local_artifacts SET state = ?1 WHERE sandbox_id = ?2",
                params![state, fixture.id],
            )
            .unwrap();

        assert!(fixture.publish("seed", false).is_err(), "{case}");
        assert_eq!(
            fixture.named_rows(),
            vec![(existing_path, kind, state.to_owned())],
            "{case}"
        );
        assert!(!fixture.destination.exists(), "{case}");
        fixture.assert_staging_retained(case);
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy)]
enum UnusableDestination {
    /// A path beyond `PATH_MAX` whose parent is still creatable: probing it
    /// fails with `ENAMETOOLONG` regardless of privileges.
    Unprobeable,
    NotASnapshotName,
    NotUtf8,
}

/// Every refusal here precedes the catalog commit, so a rejected destination
/// leaves no catalog evidence that a later import would have to reconcile.
/// Linux-only: the cases rely on Linux `PATH_MAX` and on filesystems that
/// accept non-UTF-8 names.
#[cfg(target_os = "linux")]
#[test]
fn snapshot_publication_refuses_an_unusable_destination_without_cataloging_it() {
    use std::os::unix::ffi::OsStrExt;

    const PATH_MAX: usize = 4096;
    for unusable in [
        UnusableDestination::Unprobeable,
        UnusableDestination::NotASnapshotName,
        UnusableDestination::NotUtf8,
    ] {
        let mut fixture = import_fixture();
        let snapshots = fixture.destination.parent().unwrap().to_owned();
        fixture.destination = match unusable {
            UnusableDestination::Unprobeable => {
                // The parent stays below PATH_MAX; the snapshot name exceeds it.
                let parent_len = PATH_MAX - 6;
                let mut parent = snapshots;
                while parent_len - parent.as_os_str().len() > 202 {
                    parent.push("p".repeat(200));
                }
                parent.push("p".repeat(parent_len - parent.as_os_str().len() - 1));
                assert_eq!(parent.as_os_str().len(), parent_len);
                parent.join("d-imported")
            }
            UnusableDestination::NotASnapshotName => snapshots.join("d imported"),
            UnusableDestination::NotUtf8 => snapshots
                .join(std::ffi::OsStr::from_bytes(b"\xff"))
                .join("d-imported"),
        };
        let case = format!("{unusable:?}");

        assert!(fixture.publish("seed", false).is_err(), "{case}");
        assert!(fixture.named_rows().is_empty(), "{case}");
        assert!(!fixture.destination.exists(), "{case}");
        fixture.assert_staging_retained(&case);
    }
}
