use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{ArtifactKind, LocalArtifactId};

use crate::db::transfer::{
    ReserveImportError, SnapshotPublication, TransferBinding, publish_snapshot,
};
use crate::local_artifacts::ArtifactState;

use super::*;

#[tokio::test]
async fn full_import_allows_memory_only_but_rejects_supplied_empty_disks() {
    use super::super::grpc_sandbox_transfer::receive::SparseImport;

    for (file, disk, accepted) in [
        (None, None, true),
        (Some("rootfs.ext4"), Some(b"".as_slice()), false),
        (Some("rootfs.ext4"), Some(b"disk".as_slice()), true),
        (Some("rootfs.ext4.diff"), None, false),
        (Some("rootfs.ext4.diff"), Some(b"".as_slice()), false),
        (Some("rootfs.ext4.diff"), Some(b"disk".as_slice()), true),
    ] {
        let root = tempfile::tempdir().unwrap();
        let mut receiver = SparseImport::default();
        for (name, bytes) in [
            ("vmstate", b"vm".as_slice()),
            ("memory", b"memory".as_slice()),
        ]
        .into_iter()
        .chain(file.map(|name| {
            (
                name,
                if name.ends_with(".diff") {
                    b"delta"
                } else {
                    disk.unwrap()
                },
            )
        })) {
            let path = root.path().join(name);
            if !bytes.is_empty() {
                receiver.write(name, &path, 0, bytes, false).await.unwrap();
            }
            receiver
                .write(name, &path, bytes.len() as u64, &[], true)
                .await
                .unwrap();
        }
        receiver.finish().unwrap();
        if file == Some("rootfs.ext4.diff")
            && let Some(disk) = disk
        {
            // Validate the reconstructed file, not the nonempty delta stream.
            std::fs::write(root.path().join("rootfs.ext4"), disk).unwrap();
        }
        let result = validate_imported_snapshot_rootfs(root.path(), &receiver);
        assert_eq!(result.is_ok(), accepted, "file={file:?}, disk={disk:?}");
        if let Err(error) = result {
            assert_eq!(error.code(), tonic::Code::DataLoss);
        }
    }
}

#[tokio::test]
async fn snapshot_staging_reaper_preserves_live_owner_and_unrelated_entries() {
    use crate::service::tests::service_tests::service_for_teardown_test;
    let service = service_for_teardown_test(crate::db::test_conn());
    let temp = tempfile::tempdir().unwrap();
    let sandbox = uuid::Uuid::now_v7().to_string();
    let path = temp.path().join(format!(
        ".snapshot-import-{sandbox}_{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("partial"), b"not published").unwrap();
    let guard = service
        .auto_lifecycle
        .artifact_lock(&sandbox)
        .lock_owned()
        .await;
    assert!(
        !service
            .reclaim_abandoned_transfer_staging_entry(&path)
            .unwrap()
    );
    assert!(path.join("partial").exists());
    drop(guard);
    assert!(
        service
            .reclaim_abandoned_transfer_staging_entry(&path)
            .unwrap()
    );
    assert!(!path.exists());
    let unrelated = temp.path().join(".snapshot-import-not-owned");
    std::fs::create_dir(&unrelated).unwrap();
    assert!(
        !service
            .reclaim_abandoned_transfer_staging_entry(&unrelated)
            .unwrap()
    );
    assert!(unrelated.exists());
}

#[test]
fn rowless_snapshot_publication_does_not_claim_the_sandbox_directory() {
    let db = crate::db::test_conn();
    let temp = tempfile::tempdir().unwrap();
    let id = uuid::Uuid::now_v7().to_string();
    let staging = temp.path().join("stage");
    let sandbox_root = temp.path().join(&id);
    let destination = sandbox_root.join("snapshots/d-imported");
    std::fs::create_dir(&staging).unwrap();
    std::fs::write(staging.join("rootfs.ext4"), b"complete").unwrap();
    let binding = crate::db::transfer::TransferBinding {
        sandbox_id: id.clone(),
        template: "seed".into(),
        content_hash: "hash".into(),
        stream_fingerprint: "complete-stream".into(),
    };
    assert!(
        !crate::db::transfer::publish_snapshot(
            &db,
            crate::db::transfer::SnapshotPublication {
                id: &id,
                template: "seed",
                hash: "hash",
                staging: &staging,
                destination: &destination,
                binding: &binding,
                verified_existing: false,
            },
        )
        .unwrap()
    );
    assert!(!sandbox_root.exists());
    assert!(staging.join("rootfs.ext4").exists());
    assert!(
        crate::db::get_named_local_artifact(&db, &id, &"d-imported".parse().unwrap())
            .unwrap()
            .is_none()
    );
}

#[test]
fn snapshot_publication_rechecks_identity_and_never_replaces() {
    // Only these states refuse. Every other state is admitted today,
    // including the failed, cleanup and in-flight lifecycle states; that set
    // pins current behavior, and whether each of those states should accept a
    // published snapshot is unresolved.
    const REFUSED: [SandboxState; 4] = [
        SandboxState::Creating,
        SandboxState::Deleting,
        SandboxState::Stopping,
        SandboxState::Quarantined,
    ];
    // Stopped with the seed identity is the accepting control for the
    // identity-mismatch rows.
    let cases = SandboxState::all()
        .into_iter()
        .map(|state| (state, Identity::Seed, !REFUSED.contains(&state)))
        .chain([
            (SandboxState::Stopped, Identity::Encrypted, false),
            (SandboxState::Stopped, Identity::OtherTemplate, false),
            (SandboxState::Stopped, Identity::OtherHash, false),
        ]);
    for (state, identity, admitted) in cases {
        let case = format!("{state:?} {identity:?}");
        let fixture = import_fixture(state);
        let result = match identity {
            Identity::Seed => fixture.publish(false),
            Identity::Encrypted => {
                fixture
                    .db
                    .execute(
                        "UPDATE sandboxes SET volume_encryption = 1 WHERE id = ?1",
                        [&fixture.id],
                    )
                    .unwrap();
                fixture.publish(false)
            }
            Identity::OtherTemplate => fixture.publish_as("other", "hash", false),
            Identity::OtherHash => fixture.publish_as("seed", "different", false),
        };

        assert_eq!(result, Ok(admitted), "{case}");
        if admitted {
            assert_eq!(
                fixture.named_rows(),
                vec![(
                    fixture.destination_path(),
                    ArtifactKind::Disk,
                    "ready".into()
                )],
                "{case}"
            );
            assert_eq!(
                std::fs::read(fixture.destination.join("rootfs.ext4")).unwrap(),
                STAGED_ROOTFS,
                "{case}"
            );
        } else {
            assert!(fixture.named_rows().is_empty(), "{case}");
            assert!(!fixture.destination.exists(), "{case}");
            assert!(
                crate::db::sandbox_exists(&fixture.db, &fixture.id).unwrap(),
                "{case}"
            );
            fixture.assert_staging_retained(&case);
        }
    }

    let fixture = import_fixture(SandboxState::Stopped);
    std::fs::create_dir_all(&fixture.destination).unwrap();
    assert_eq!(fixture.publish(false), Err(ErrorKind::AlreadyExists));
    assert!(!fixture.destination.join("rootfs.ext4").exists());
    assert!(fixture.named_rows().is_empty());
    fixture.assert_staging_retained("occupied");
}

#[derive(Debug, Clone, Copy)]
enum Identity {
    Seed,
    Encrypted,
    OtherTemplate,
    OtherHash,
}

#[test]
fn snapshot_publication_failure_keeps_files_hidden_and_retry_preserves_bytes() {
    let db = crate::db::test_conn();
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("d-imported");
    let id = uuid::Uuid::now_v7().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&db, &id, SandboxState::Stopped);
    let binding = crate::db::transfer::TransferBinding {
        sandbox_id: id.clone(),
        template: "seed".to_owned(),
        content_hash: "hash".to_owned(),
        stream_fingerprint: "same-complete-stream".to_owned(),
    };
    db.execute_batch("CREATE TEMP TRIGGER fail_catalog BEFORE INSERT ON local_artifacts BEGIN SELECT RAISE(ABORT, 'injected persistence failure'); END;").unwrap();
    for retry in [false, true] {
        let staging = temp.path().join(if retry { "retry" } else { "initial" });
        std::fs::create_dir(&staging).unwrap();
        std::fs::write(staging.join("rootfs.ext4"), b"immutable content").unwrap();
        std::fs::write(
            staging.join(".transfer.json"),
            serde_json::to_vec(&binding).unwrap(),
        )
        .unwrap();
        let result = crate::db::transfer::publish_snapshot(
            &db,
            crate::db::transfer::SnapshotPublication {
                id: &id,
                template: "seed",
                hash: "hash",
                staging: &staging,
                destination: &destination,
                binding: &binding,
                verified_existing:
                    super::super::grpc_sandbox_transfer::receive::verify_existing_snapshot(
                        &staging,
                        &destination,
                    )
                    .unwrap(),
            },
        );
        let named =
            crate::db::get_named_local_artifact(&db, &id, &"d-imported".parse().unwrap()).unwrap();
        if retry {
            assert!(result.unwrap());
            assert_eq!(named.unwrap().local_path, destination.to_string_lossy());
            assert!(!staging.exists());
        } else {
            result.unwrap_err();
            assert!(named.is_none());
            assert!(
                !destination.exists(),
                "old directory-only readers must not see a failed publication"
            );
            db.execute_batch("DROP TRIGGER fail_catalog;").unwrap();
        }
        let visible = if retry { &destination } else { &staging };
        assert_eq!(
            std::fs::read(visible.join("rootfs.ext4")).unwrap(),
            b"immutable content"
        );
    }
    let retry = temp.path().join("retry");
    std::fs::create_dir(&retry).unwrap();
    for name in ["rootfs.ext4", ".transfer.json"] {
        std::fs::copy(destination.join(name), retry.join(name)).unwrap();
    }
    std::fs::write(destination.join("rootfs.ext4"), b"corrupted content").unwrap();
    assert!(
        super::super::grpc_sandbox_transfer::receive::verify_existing_snapshot(
            &retry,
            &destination
        )
        .is_err(),
        "same-sized mutation must not be accepted using the old transfer receipt"
    );
}

#[test]
fn snapshot_retry_handles_either_retained_half_without_replacing_published_files() {
    // Catalog-only is the commit-before-rename crash window. Files-only is
    // retained evidence from the predecessor's rename-before-commit window.
    for catalog_only in [false, true] {
        let db = crate::db::test_conn();
        let temp = tempfile::tempdir().unwrap();
        let staging = temp.path().join("stage");
        let destination = temp.path().join("d-imported");
        std::fs::create_dir(&staging).unwrap();
        let id = uuid::Uuid::now_v7().to_string();
        crate::service::tests::service_tests::insert_test_sandbox(&db, &id, SandboxState::Stopped);
        let binding = crate::db::transfer::TransferBinding {
            sandbox_id: id.clone(),
            template: "seed".into(),
            content_hash: "hash".into(),
            stream_fingerprint: "same-stream".into(),
        };
        std::fs::write(staging.join("rootfs.ext4"), b"complete").unwrap();
        std::fs::write(
            staging.join(".transfer.json"),
            serde_json::to_vec(&binding).unwrap(),
        )
        .unwrap();
        if catalog_only {
            crate::db::publish_named_local_artifact(
                &db,
                &id,
                LocalArtifactId::new(),
                pplx_space_common_core::types::ArtifactKind::Disk,
                &"d-imported".parse().unwrap(),
                destination.to_str().unwrap(),
            )
            .unwrap();
        } else {
            std::fs::create_dir(&destination).unwrap();
            for name in ["rootfs.ext4", ".transfer.json"] {
                std::fs::copy(staging.join(name), destination.join(name)).unwrap();
            }
        }
        assert!(
            crate::db::transfer::publish_snapshot(
                &db,
                crate::db::transfer::SnapshotPublication {
                    id: &id,
                    template: "seed",
                    hash: "hash",
                    staging: &staging,
                    destination: &destination,
                    binding: &binding,
                    verified_existing:
                        super::super::grpc_sandbox_transfer::receive::verify_existing_snapshot(
                            &staging,
                            &destination
                        )
                        .unwrap(),
                }
            )
            .unwrap()
        );
        assert!(
            crate::db::get_named_local_artifact(&db, &id, &"d-imported".parse().unwrap())
                .unwrap()
                .is_some()
        );
        assert_eq!(
            std::fs::read(destination.join("rootfs.ext4")).unwrap(),
            b"complete"
        );
        assert_eq!(staging.exists(), !catalog_only);
    }
}

struct ImportFixture {
    _root: tempfile::TempDir,
    db: crate::db::Connection,
    id: String,
    binding: TransferBinding,
    staging: PathBuf,
    destination: PathBuf,
}

const STAGED_ROOTFS: &[u8] = b"complete";

/// A sandbox in `state` with the seed identity and a fully staged import of
/// `d-imported`.
fn import_fixture(state: SandboxState) -> ImportFixture {
    let root = tempfile::TempDir::new().unwrap();
    let db = crate::db::test_conn();
    let id = uuid::Uuid::now_v7().to_string();
    crate::service::tests::service_tests::insert_test_sandbox(&db, &id, state);
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
    fn publish(&self, verified_existing: bool) -> Result<bool, ErrorKind> {
        self.publish_as("seed", "hash", verified_existing)
    }

    /// The import handler maps every error to `Status::internal`; the I/O
    /// kind is what distinguishes one refused destination from another.
    fn publish_as(
        &self,
        template: &str,
        hash: &str,
        verified_existing: bool,
    ) -> Result<bool, ErrorKind> {
        publish_snapshot(
            &self.db,
            SnapshotPublication {
                id: &self.id,
                template,
                hash,
                staging: &self.staging,
                destination: &self.destination,
                binding: &self.binding,
                verified_existing,
            },
        )
        .map_err(|error| match error {
            ReserveImportError::Claim(error) => error.kind(),
            ReserveImportError::Database(error) => panic!("unexpected database error: {error}"),
        })
    }

    fn destination_path(&self) -> String {
        self.destination.to_str().unwrap().to_owned()
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
            .collect::<Result<_, _>>()
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

#[derive(Debug, Clone, Copy)]
enum RetryDestination {
    Unchanged,
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
    for (destination, expected) in [
        (RetryDestination::Unchanged, Ok(true)),
        (
            RetryDestination::SymlinkToMatchingCopy,
            Err(ErrorKind::AlreadyExists),
        ),
        (
            RetryDestination::DifferentBinding,
            Err(ErrorKind::AlreadyExists),
        ),
        (
            RetryDestination::DifferentFileLength,
            Err(ErrorKind::AlreadyExists),
        ),
        (RetryDestination::MissingFile, Err(ErrorKind::NotFound)),
    ] {
        let fixture = import_fixture(SandboxState::Stopped);
        let case = format!("{destination:?}");
        match destination {
            RetryDestination::Unchanged => copy_dir(&fixture.staging, &fixture.destination),
            RetryDestination::SymlinkToMatchingCopy => {
                let elsewhere = fixture.staging.with_file_name("elsewhere");
                copy_dir(&fixture.staging, &elsewhere);
                std::fs::create_dir_all(fixture.destination.parent().unwrap()).unwrap();
                std::os::unix::fs::symlink(&elsewhere, &fixture.destination).unwrap();
            }
            RetryDestination::DifferentBinding => {
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
            RetryDestination::DifferentFileLength => {
                copy_dir(&fixture.staging, &fixture.destination);
                std::fs::write(fixture.destination.join("rootfs.ext4"), b"short").unwrap();
            }
            RetryDestination::MissingFile => {
                copy_dir(&fixture.staging, &fixture.destination);
                std::fs::remove_file(fixture.destination.join("rootfs.ext4")).unwrap();
            }
        }

        assert_eq!(fixture.publish(true), expected, "{case}");
        if expected.is_ok() {
            assert_eq!(
                fixture.named_rows(),
                vec![(
                    fixture.destination_path(),
                    ArtifactKind::Disk,
                    "ready".into()
                )],
                "{case}"
            );
            assert_eq!(
                std::fs::read(fixture.destination.join("rootfs.ext4")).unwrap(),
                STAGED_ROOTFS,
                "{case}"
            );
        } else {
            assert!(fixture.named_rows().is_empty(), "{case}");
        }
        // An adopted retry publishes nothing from staging; the caller
        // removes it afterwards.
        fixture.assert_staging_retained(&case);
    }
}

/// An existing `(sandbox, name)` catalog row is reused only when it is a
/// ready row for this destination and kind.
#[test]
fn snapshot_publication_refuses_a_catalog_entry_it_does_not_own() {
    let reusable = |state: ArtifactState| match state {
        ArtifactState::Ready => Ok(true),
        ArtifactState::Restoring | ArtifactState::Deleting => Err(ErrorKind::AlreadyExists),
    };
    let cases = [
        ArtifactState::Ready,
        ArtifactState::Restoring,
        ArtifactState::Deleting,
    ]
    .map(|state| {
        (
            state.as_str(),
            false,
            ArtifactKind::Disk,
            state,
            reusable(state),
        )
    })
    .into_iter()
    .chain([
        (
            "other path",
            true,
            ArtifactKind::Disk,
            ArtifactState::Ready,
            Err(ErrorKind::AlreadyExists),
        ),
        (
            "other kind",
            false,
            ArtifactKind::Full,
            ArtifactState::Ready,
            Err(ErrorKind::AlreadyExists),
        ),
    ]);
    for (case, other_path, kind, state, expected) in cases {
        let fixture = import_fixture(SandboxState::Stopped);
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
                crate::db::fixture::params![state.as_str(), fixture.id],
            )
            .unwrap();

        assert_eq!(fixture.publish(false), expected, "{case}");
        assert_eq!(
            fixture.named_rows(),
            vec![(existing_path, kind, state.as_str().to_owned())],
            "{case}"
        );
        if expected.is_ok() {
            assert_eq!(
                std::fs::read(fixture.destination.join("rootfs.ext4")).unwrap(),
                STAGED_ROOTFS,
                "{case}"
            );
        } else {
            assert!(!fixture.destination.exists(), "{case}");
            fixture.assert_staging_retained(case);
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum UnusableDestination {
    NotASnapshotName,
    /// A path beyond Linux `PATH_MAX` whose parent is still creatable:
    /// probing it fails with `ENAMETOOLONG` regardless of privileges.
    #[cfg(target_os = "linux")]
    Unprobeable,
    /// Linux filesystems accept non-UTF-8 names; APFS does not.
    #[cfg(target_os = "linux")]
    NotUtf8,
}

/// Every refusal here precedes the catalog commit, so a rejected destination
/// leaves no catalog evidence that a later import would have to reconcile.
#[test]
fn snapshot_publication_refuses_an_unusable_destination_without_cataloging_it() {
    #[cfg(target_os = "linux")]
    let platform_cases = [
        (UnusableDestination::Unprobeable, ErrorKind::InvalidFilename),
        (UnusableDestination::NotUtf8, ErrorKind::InvalidInput),
    ];
    // macOS refuses these names before the publisher sees them. Chained rather
    // than one literal, which clippy rejects as a loop when it has one element.
    #[cfg(not(target_os = "linux"))]
    let platform_cases: [(UnusableDestination, ErrorKind); 0] = [];
    let cases = std::iter::once((
        UnusableDestination::NotASnapshotName,
        ErrorKind::InvalidInput,
    ))
    .chain(platform_cases);
    for (unusable, expected) in cases {
        let mut fixture = import_fixture(SandboxState::Stopped);
        let snapshots = fixture.destination.parent().unwrap().to_owned();
        fixture.destination = match unusable {
            UnusableDestination::NotASnapshotName => snapshots.join("d imported"),
            #[cfg(target_os = "linux")]
            UnusableDestination::Unprobeable => {
                const PATH_MAX: usize = 4096;
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
            #[cfg(target_os = "linux")]
            UnusableDestination::NotUtf8 => {
                use std::os::unix::ffi::OsStrExt;
                snapshots
                    .join(std::ffi::OsStr::from_bytes(b"\xff"))
                    .join("d-imported")
            }
        };
        let case = format!("{unusable:?}");

        assert_eq!(fixture.publish(false), Err(expected), "{case}");
        assert!(fixture.named_rows().is_empty(), "{case}");
        fixture.assert_staging_retained(&case);
    }
}
