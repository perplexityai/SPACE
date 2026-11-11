//! A suspended eviction deletes the sandbox's whole cache directory, so a
//! user snapshot directory no local artifact records (a publication that
//! failed after writing it) holds off both suspended evictions, the
//! replacement and the disk-pressure tick. Row-less leftovers that aren't a
//! user's to keep don't, and a cluster-deleted sandbox's eviction doesn't
//! look.

use super::*;

/// A manual disk snapshot no row records.
const UNRECORDED: &str = "d-manual-20261107T120000Z";

/// A manual full snapshot no row records.
const UNRECORDED_FULL: &str = "f-manual-20261107T120000Z";

/// What a snapshot directory holds.
#[derive(Clone, Copy, Debug)]
enum Contents {
    /// A disk, and for a full snapshot its VM state too.
    Complete,
    /// A full snapshot's disk without its VM state file `missing`, and with
    /// its other one empty: what a failed capture leaves.
    MissingVmState { missing: &'static str },
    /// A full snapshot's disk and VM state, with its VM state file `empty`
    /// written but empty.
    EmptyVmState { empty: &'static str },
}

/// Create `name` holding `contents` under `snapshots_dir`.
fn write_snapshot_dir(snapshots_dir: &std::path::Path, name: &str, contents: Contents) {
    let path = snapshots_dir.join(name);
    std::fs::create_dir_all(&path).unwrap();
    std::fs::write(path.join("rootfs.ext4"), b"snapshot").unwrap();
    match contents {
        Contents::Complete if name.starts_with("f-") => {
            std::fs::write(path.join("vmstate"), b"vmstate").unwrap();
            std::fs::write(path.join("memory"), b"memory").unwrap();
        }
        Contents::Complete => {}
        Contents::MissingVmState { missing } => {
            for file in ["vmstate", "memory"] {
                if file != missing {
                    std::fs::write(path.join(file), b"").unwrap();
                }
            }
        }
        Contents::EmptyVmState { empty } => {
            for file in ["vmstate", "memory"] {
                let bytes: &[u8] = if file == empty { b"" } else { b"state" };
                std::fs::write(path.join(file), bytes).unwrap();
            }
        }
    }
}

/// Row-less leftovers that aren't a user's to keep, by kind: internal names,
/// names without a user snapshot's prefix, and full snapshots without their
/// VM state.
fn unprotected_leftovers() -> [Vec<(String, Contents)>; 3] {
    let capture = format!("d-operation-{}", uuid::Uuid::now_v7());
    let complete = |name: String| (name, Contents::Complete);
    [
        vec![
            complete(capture.clone()),
            complete(format!("{capture}-artifact")),
            complete(format!("d-operation-runtime-upgrade-{}", operation())),
            complete(format!("d-stop-{}", LocalArtifactId::new())),
            complete(format!("d-gone.gc-pending-{}", LocalArtifactId::new())),
        ],
        vec![
            complete(format!(".encrypted-capture-{}", uuid::Uuid::now_v7())),
            complete(format!(".suspend-publish-{}", LocalArtifactId::new())),
        ],
        vec![
            (
                UNRECORDED_FULL.to_string(),
                Contents::MissingVmState { missing: "memory" },
            ),
            (
                "f-10m-20261107T120000Z".to_string(),
                Contents::MissingVmState { missing: "vmstate" },
            ),
            (
                "f-1h-20261107T120000Z".to_string(),
                Contents::EmptyVmState { empty: "memory" },
            ),
            (
                "f-6h-20261107T120000Z".to_string(),
                Contents::EmptyVmState { empty: "vmstate" },
            ),
        ],
    ]
}

/// Row-less snapshots a user keeps: either type, manual, scheduled or
/// imported. A disk snapshot is kept whatever it holds.
const KEPT: [&str; 8] = [
    UNRECORDED,
    UNRECORDED_FULL,
    "d-imported-x",
    "f-imported-x",
    "d-1m-20261107T120000Z",
    "d-5m-20261107T120000Z",
    "f-10m-20261107T120000Z",
    "f-6h-20261107T120000Z",
];

/// Each kind of unprotected leftover is left out on its own; every row-less
/// user snapshot, scheduled captures included, is kept unless a row records
/// its name; and a disk snapshot without a disk is kept too.
#[test]
fn only_row_less_user_snapshots_are_kept() {
    let kept = |dir: &std::path::Path, name: &str, recorded: &std::collections::HashSet<String>| {
        crate::service::eviction::is_unrecorded_user_snapshot(dir, name, recorded).unwrap()
    };
    let none = std::collections::HashSet::new();
    for (kind, leftovers) in ["internal", "unprefixed", "incomplete full"]
        .into_iter()
        .zip(unprotected_leftovers())
    {
        for (name, contents) in leftovers {
            let dir = tempfile::tempdir().unwrap();
            write_snapshot_dir(dir.path(), &name, contents);
            assert!(!kept(dir.path(), &name, &none), "{kind}: {name}");
        }
    }
    for name in KEPT {
        let dir = tempfile::tempdir().unwrap();
        write_snapshot_dir(dir.path(), name, Contents::Complete);
        assert!(kept(dir.path(), name, &none), "{name}");
        let recorded = std::collections::HashSet::from([name.to_string()]);
        assert!(!kept(dir.path(), name, &recorded), "recorded {name}");
    }
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join(UNRECORDED)).unwrap();
    assert!(
        kept(dir.path(), UNRECORDED, &none),
        "an empty disk snapshot"
    );
    let root = tempfile::tempdir().unwrap();
    let snapshots = root.path().join("snapshots");
    for name in add_non_directories(&snapshots) {
        assert!(!kept(&snapshots, &name, &none), "not a directory: {name}");
    }
}

/// Entries under `snapshots_dir` with a user snapshot's name that the listing
/// doesn't show, as they aren't directories: a disk symlink and a full one,
/// each to a complete snapshot outside `snapshots_dir`, and a disk regular
/// file. Returns their names.
fn add_non_directories(snapshots_dir: &std::path::Path) -> [String; 3] {
    let targets = snapshots_dir.parent().unwrap().join("link-targets");
    write_snapshot_dir(&targets, UNRECORDED, Contents::Complete);
    write_snapshot_dir(&targets, UNRECORDED_FULL, Contents::Complete);
    std::fs::create_dir_all(snapshots_dir).unwrap();
    let names = [
        "d-manual-link".to_string(),
        "f-manual-link".to_string(),
        "d-manual-file".to_string(),
    ];
    std::os::unix::fs::symlink(targets.join(UNRECORDED), snapshots_dir.join(&names[0])).unwrap();
    std::os::unix::fs::symlink(targets.join(UNRECORDED_FULL), snapshots_dir.join(&names[1]))
        .unwrap();
    std::fs::write(snapshots_dir.join(&names[2]), b"snapshot").unwrap();
    names
}

/// A directory whose name isn't UTF-8 is left out of the inventory: the
/// listing can't show it and no snapshot RPC can name it. Decoded lossily, it
/// would pass for a real snapshot named with the replacement character, which
/// is listed once, beside it.
#[test]
fn a_name_that_isnt_utf8_is_left_out() {
    use std::os::unix::ffi::OsStrExt as _;
    let root = tempfile::tempdir().unwrap();
    let snapshots = root.path().join("snapshots");
    let undecodable = std::ffi::OsStr::from_bytes(b"d-manual-\xff");
    std::fs::create_dir_all(snapshots.join(undecodable)).unwrap();
    let replaced = "d-manual-\u{FFFD}";
    write_snapshot_dir(&snapshots, replaced, Contents::Complete);
    write_snapshot_dir(&snapshots, UNRECORDED, Contents::Complete);

    let found = crate::service::eviction::unrecorded_snapshots(
        &snapshots,
        &std::collections::HashSet::new(),
    )
    .unwrap();

    assert_eq!(found, vec![UNRECORDED.to_string(), replaced.to_string()]);
}

impl Fixture {
    fn snapshots_dir(&self) -> std::path::PathBuf {
        self.checkpoint().parent().unwrap().join("snapshots")
    }

    /// Create a complete `name` under the sandbox's snapshot directory,
    /// recording no row for it.
    fn add_snapshot_dir(&self, name: &str) -> std::path::PathBuf {
        write_snapshot_dir(&self.snapshots_dir(), name, Contents::Complete);
        self.snapshots_dir().join(name)
    }

    /// Leave a row-less leftover of every kind that isn't a user's to keep.
    fn add_unprotected_leftovers(&self) {
        for (name, contents) in unprotected_leftovers().into_iter().flatten() {
            write_snapshot_dir(&self.snapshots_dir(), &name, contents);
        }
        add_non_directories(&self.snapshots_dir());
    }

    /// Give the sandbox the user snapshot [`NAMED`], with its row and a
    /// backup stamp.
    fn add_stamped_named_snapshot(&self) {
        let path = self.add_snapshot_dir("d-named");
        self.service
            .db
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO local_artifacts (
                     local_artifact_id, sandbox_id, artifact_kind, artifact_origin,
                     snapshot_name, local_path, created_at, backup_completed_at
                 ) VALUES (?1, ?2, 'disk', 'snapshot', 'd-named', ?3, '0', '0')",
                crate::db::fixture::params![NAMED, self.sandbox_id, path.to_string_lossy()],
            )
            .unwrap();
    }
}

/// A row-less complete `name` beside a backed-up stale copy holds off the
/// replacement before any node work, naming it, and both stay.
async fn assert_holds_off_the_replacement(name: &'static str) {
    with_cluster_api(BackupReadiness::backs(STALE), async move || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let unrecorded = f.add_snapshot_dir(name);

        let refused = f.pod_upgrade_error(operation()).await;

        assert_eq!(refused.code(), tonic::Code::FailedPrecondition, "{refused}");
        assert!(refused.message().contains(name), "{refused}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
        assert!(unrecorded.exists());
    })
    .await;
}

/// A row-less complete `name` beside a backed-up stale copy makes a real
/// disk-pressure tick skip the sandbox, and both stay.
async fn assert_holds_off_the_disk_pressure_eviction(name: &'static str) {
    with_cluster_api(BackupReadiness::backs(STALE), async move || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        let unrecorded = f.add_snapshot_dir(name);

        let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

        assert_eq!((evicted, skipped), (0, 1), "{name}");
        f.assert_copy_untouched(Some(STALE), &cleanups);
        assert!(unrecorded.exists());
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrecorded_snapshot_holds_off_the_replacement() {
    assert_holds_off_the_replacement(UNRECORDED).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrecorded_snapshot_holds_off_the_disk_pressure_eviction() {
    assert_holds_off_the_disk_pressure_eviction(UNRECORDED).await;
}

/// A full snapshot with its VM state is a restorable checkpoint, kept row or
/// no row; the same name without it is an unprotected leftover (see
/// [`unprotected_leftovers`]).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_complete_unrecorded_full_snapshot_holds_off_both_evictions() {
    assert_holds_off_the_replacement(UNRECORDED_FULL).await;
    assert_holds_off_the_disk_pressure_eviction(UNRECORDED_FULL).await;
}

/// A suspended sandbox's scheduler is stopped, so retention never prunes a
/// row-less scheduled capture; it's kept like a manual one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unrecorded_scheduled_capture_holds_off_both_evictions() {
    assert_holds_off_the_replacement("f-10m-20261107T120000Z").await;
    assert_holds_off_the_disk_pressure_eviction("d-1m-20261107T120000Z").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unprotected_leftovers_without_a_row_let_the_replacement_go() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        f.add_unprotected_leftovers();

        let error = f.pod_upgrade_error(operation()).await;

        assert!(error.message().contains("stage"), "{error}");
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unprotected_leftovers_without_a_row_let_the_disk_pressure_eviction_go() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        f.add_unprotected_leftovers();

        let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

        assert_eq!((evicted, skipped), (1, 0));
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// The control for the name filter's other half: a user snapshot with its
/// row and a backup stamp is recorded, and neither eviction holds off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recorded_backed_up_snapshot_lets_the_replacement_go() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        f.add_stamped_named_snapshot();

        let error = f.pod_upgrade_error(operation()).await;

        assert!(error.message().contains("stage"), "{error}");
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recorded_backed_up_snapshot_lets_the_disk_pressure_eviction_go() {
    with_cluster_api(BackupReadiness::backs(STALE), async || {
        let (f, cleanups) = stale_copy(false, Some(STALE));
        f.add_stamped_named_snapshot();

        let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

        assert_eq!((evicted, skipped), (1, 0));
        f.assert_copy_evicted(&cleanups);
    })
    .await;
}

/// A cluster-deleted sandbox's snapshots went with its cluster row, so its
/// eviction goes ahead over an unrecorded snapshot that holds off the same
/// copy's pinned-suspended eviction.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cluster_deleted_eviction_ignores_an_unrecorded_snapshot() {
    let (f, cleanups) = stale_copy(false, Some(STALE));
    f.add_snapshot_dir(UNRECORDED);

    let refused = f
        .service
        .evict_local_sandbox(
            &f.sandbox_id,
            crate::service::EvictionClaim::SuspendedPinned(Some(artifact(STALE))),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        crate::service::is_unrecorded_snapshot_refusal(&refused),
        "{refused}"
    );
    // The disk-pressure loop counts it under its own reason, not as a hold
    // that ends on its own.
    use crate::sandbox_eviction::{SkipReason, claim_refusal_skip_reason};
    assert_eq!(
        claim_refusal_skip_reason(&refused, true),
        SkipReason::UnrecordedSnapshot
    );
    let held = tonic::Status::failed_precondition(refused.message());
    assert_eq!(
        claim_refusal_skip_reason(&held, true),
        SkipReason::HeldAtExecute
    );
    f.assert_copy_untouched(Some(STALE), &cleanups);

    f.service
        .evict_local_sandbox(
            &f.sandbox_id,
            crate::service::EvictionClaim::DeletedSuspended(Some(artifact(STALE))),
            None,
        )
        .await
        .unwrap();
    f.assert_copy_evicted(&cleanups);
}

/// Where a metadata read fails while the inventory looks at a row-less user
/// snapshot: its entry, or one of a full snapshot's VM state files.
#[derive(Clone, Copy, Debug)]
enum MetadataFault {
    Entry,
    RequiredFile,
}

impl MetadataFault {
    /// The snapshot the fault hides and the path whose metadata read fails.
    fn target(self, snapshots_dir: &std::path::Path) -> (&'static str, std::path::PathBuf) {
        match self {
            Self::Entry => (UNRECORDED, snapshots_dir.join(UNRECORDED)),
            Self::RequiredFile => (
                UNRECORDED_FULL,
                snapshots_dir.join(UNRECORDED_FULL).join("vmstate"),
            ),
        }
    }
}

/// A metadata error other than `NotFound` is returned, not taken for "not a
/// user snapshot", at either read.
#[test]
fn a_metadata_error_is_returned_not_taken_for_an_answer() {
    for fault in [MetadataFault::Entry, MetadataFault::RequiredFile] {
        let root = tempfile::tempdir().unwrap();
        let snapshots = root.path().join("snapshots");
        let (name, failing) = fault.target(&snapshots);
        write_snapshot_dir(&snapshots, name, Contents::Complete);
        crate::service::grpc_spaced_process_snapshots::fail_snapshot_metadata(failing);

        let read = crate::service::eviction::is_unrecorded_user_snapshot(
            &snapshots,
            name,
            &std::collections::HashSet::new(),
        );

        assert!(read.is_err(), "{fault:?}: {read:?}");
    }
}

/// An unreadable row-less user snapshot refuses both suspended evictions as
/// `Unavailable` before the claim: nothing is claimed or cleaned up, and the
/// row, its pin and the snapshot stay.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unreadable_unrecorded_snapshot_holds_off_both_evictions() {
    for fault in [MetadataFault::Entry, MetadataFault::RequiredFile] {
        with_cluster_api(BackupReadiness::backs(STALE), async move || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            let (name, failing) = fault.target(&f.snapshots_dir());
            let snapshot = f.add_snapshot_dir(name);
            crate::service::grpc_spaced_process_snapshots::fail_snapshot_metadata(failing);

            let refused = f.pod_upgrade_error(operation()).await;

            assert_eq!(
                refused.code(),
                tonic::Code::Unavailable,
                "{fault:?}: {refused}"
            );
            f.assert_copy_untouched(Some(STALE), &cleanups);
            assert!(snapshot.exists(), "{fault:?}");
        })
        .await;
        with_cluster_api(BackupReadiness::backs(STALE), async move || {
            let (f, cleanups) = stale_copy(false, Some(STALE));
            let (name, failing) = fault.target(&f.snapshots_dir());
            let snapshot = f.add_snapshot_dir(name);
            crate::service::grpc_spaced_process_snapshots::fail_snapshot_metadata(failing);

            let (evicted, skipped) = crate::sandbox_eviction::evict_tick_for_test(&f.service).await;

            assert_eq!((evicted, skipped), (0, 0), "{fault:?}");
            f.assert_copy_untouched(Some(STALE), &cleanups);
            assert!(snapshot.exists(), "{fault:?}");
        })
        .await;
    }
}
