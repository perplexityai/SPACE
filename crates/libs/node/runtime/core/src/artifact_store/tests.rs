//! # Test matrix
//!
//! - Publish: pinned bytes land root-owned and read-only to others, and a
//!   repeat finds them (`a_pinned_release_is_published_as_the_stores_own_copy`).
//! - Refused publications leave nothing: other bytes, links, an unsafe root, oversized files,
//!   a target holding other bytes (`a_publication_that_does_not_check_out_leaves_nothing`).
//! - Removal: any managed version, including the image-provided version
//!   (`every_managed_release_can_be_removed_without_changing_execution`).
//! - Kernels: size and digest pins, removable managed copies
//!   (`a_managed_copy_of_the_image_kernel_is_verified_and_removable`).
//! - Leftovers of interrupted work are cleared (`interrupted_work_is_cleared`), and never reused
//!   by a later publication (`a_publication_never_uses_another_ones_work_directory`).
//! - Concurrent publications of one artifact all succeed with the pinned bytes
//!   (`concurrent_publications_of_one_release_publish_the_pinned_bytes`).
//! - The staging root is created private to its account, and anything else at its path is
//!   replaced (`the_staging_root_is_created_private_and_anything_else_there_replaced`).

use pplx_space_common_core::types::{Arch, RuntimeVersion};

use super::*;
use crate::catalog::PinnedFile;

struct Fixture {
    _dir: tempfile::TempDir,
    releases: PathBuf,
    kernels: PathBuf,
    staging: PathBuf,
    store: ArtifactStore,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let node = dir.path().join("node");
        let releases = node.join("firecracker");
        let kernels = node.join("kernels");
        let staging = dir.path().join("staging");
        for path in [&node, &releases, &kernels, &staging] {
            std::fs::create_dir_all(path).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let owner = std::fs::metadata(&releases).unwrap().uid();
        let store = ArtifactStore::new(&releases, &kernels, &staging, 1 << 20).owned_by(owner);
        let fixture = Self {
            _dir: dir,
            releases,
            kernels,
            staging,
            store,
        };
        // A managed copy of the image-provided release.
        fixture.place_release("1.15.1", 1);
        fixture
    }

    /// Stage release `version`'s binaries under job `job`.
    fn stage_release(&self, job: &str, version: &str, fill: u8) {
        let bin = self
            .staged_releases()
            .join(job)
            .join(format!("v{version}/bin"));
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::write(bin.join("firecracker"), binary(fill)).unwrap();
        std::fs::write(bin.join("jailer"), binary(fill.wrapping_add(1))).unwrap();
    }

    /// Where the runtime manager stages Firecracker releases.
    fn staged_releases(&self) -> PathBuf {
        self.staging.join("firecracker/releases")
    }

    /// Put a verified release copy in the managed store.
    fn place_release(&self, version: &str, fill: u8) {
        let bin = self.releases.join(format!("v{version}/bin"));
        std::fs::create_dir_all(&bin).unwrap();
        for (name, fill) in [("firecracker", fill), ("jailer", fill.wrapping_add(1))] {
            std::fs::write(bin.join(name), binary(fill)).unwrap();
            std::fs::set_permissions(bin.join(name), std::fs::Permissions::from_mode(0o755))
                .unwrap();
        }
    }
}

fn binary(fill: u8) -> Vec<u8> {
    vec![fill; 4096]
}

fn sha256(bytes: &[u8]) -> Digest {
    Digest::sha256(Sha256::digest(bytes).into())
}

fn firecracker(version: &str) -> RuntimeIdentity {
    RuntimeIdentity {
        kind: RuntimeKind::Firecracker,
        version: RuntimeVersion::parse(version).unwrap(),
    }
}

/// Release `version`'s pins for binaries made from `fill`.
fn pin(version: &str, fill: u8) -> PinnedRelease {
    PinnedRelease {
        release: firecracker(version),
        architecture: Arch::X86_64,
        archive_sha256: sha256(b"archive"),
        binaries: vec![
            PinnedFile {
                path: "bin/firecracker",
                sha256: sha256(&binary(fill)),
            },
            PinnedFile {
                path: "bin/jailer",
                sha256: sha256(&binary(fill.wrapping_add(1))),
            },
        ],
    }
}

fn staged(job: &str) -> StagedName {
    StagedName::parse(job).unwrap()
}

fn no_leftovers(root: &Path) -> bool {
    std::fs::read_dir(root).unwrap().all(|entry| {
        !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .starts_with('.')
    })
}

#[test]
fn a_pinned_release_is_published_as_the_stores_own_copy() {
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);

    let published = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap();
    assert_eq!(published.outcome, PublishOutcome::Published);
    assert_eq!(published.path, fixture.releases.join("v1.16.2"));
    assert_eq!(published.digest, pin("1.16.2", 7).digest());
    let firecracker = fixture.releases.join("v1.16.2/bin/firecracker");
    assert_eq!(std::fs::read(&firecracker).unwrap(), binary(7));
    assert_eq!(
        std::fs::metadata(&firecracker)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::metadata(fixture.releases.join("v1.16.2"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o755
    );
    assert!(no_leftovers(&fixture.releases));

    // The staged file is a separate inode: changing it later changes nothing
    // in the store.
    std::fs::write(
        fixture
            .staged_releases()
            .join("job-1/v1.16.2/bin/firecracker"),
        binary(9),
    )
    .unwrap();
    assert_eq!(std::fs::read(&firecracker).unwrap(), binary(7));

    // Asking again finds it.
    let again = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-2"))
        .unwrap();
    assert_eq!(again.outcome, PublishOutcome::AlreadyPresent);
}

#[test]
fn a_publication_that_does_not_check_out_leaves_nothing() {
    // Other bytes than pinned.
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    let error = fixture
        .store
        .publish_release(&pin("1.16.2", 8), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::Mismatch { .. }), "{error}");
    assert!(!error.is_transient());
    assert!(!fixture.releases.join("v1.16.2").exists());
    assert!(no_leftovers(&fixture.releases));

    // A staged binary that is a link, and a staged directory that is one.
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    let firecracker = fixture
        .staged_releases()
        .join("job-1/v1.16.2/bin/firecracker");
    let elsewhere = fixture.staging.join("elsewhere");
    std::fs::write(&elsewhere, binary(7)).unwrap();
    std::fs::remove_file(&firecracker).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &firecracker).unwrap();
    let error = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::Linked { .. }), "{error}");
    assert!(!error.is_transient());
    let fixture = Fixture::new();
    fixture.stage_release("job-0", "1.16.2", 7);
    std::os::unix::fs::symlink(
        fixture.staged_releases().join("job-0"),
        fixture.staged_releases().join("job-1"),
    )
    .unwrap();
    let error = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::Linked { .. }), "{error}");
    assert!(no_leftovers(&fixture.releases));

    // A root another account could write.
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    std::fs::set_permissions(&fixture.releases, std::fs::Permissions::from_mode(0o775)).unwrap();
    let error = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::UnsafeRoot { .. }), "{error}");

    // An oversized binary.
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    let store = fixture.store.clone();
    let small = ArtifactStore {
        max_file_bytes: 1024,
        ..store
    };
    let error = small
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::TooLarge { .. }), "{error}");

    // The target already holds other bytes.
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    fixture.place_release("1.16.2", 3);
    let error = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap_err();
    assert!(
        matches!(error, PublishError::TargetDiffers { .. }),
        "{error}"
    );
    assert_eq!(
        std::fs::read(fixture.releases.join("v1.16.2/bin/firecracker")).unwrap(),
        binary(3),
        "left in place"
    );
}

#[test]
fn every_managed_release_can_be_removed_without_changing_execution() {
    let fixture = Fixture::new();
    let execution = fixture._dir.path().join("execution/v1.15.1/bin");
    std::fs::create_dir_all(&execution).unwrap();
    std::fs::write(execution.join("firecracker"), binary(1)).unwrap();
    for (version, fill) in [("1.15.1", 1), ("1.16.1", 3), ("1.16.2", 5)] {
        fixture.stage_release("job-1", version, fill);
        fixture
            .store
            .publish_release(&pin(version, fill), &staged("job-1"))
            .unwrap();
        for expected in [RemoveOutcome::Removed, RemoveOutcome::Absent] {
            assert_eq!(
                fixture
                    .store
                    .remove_release(&firecracker(version), pin(version, fill).digest())
                    .unwrap(),
                expected
            );
        }
        assert!(!fixture.releases.join(format!("v{version}")).exists());
        assert!(no_leftovers(&fixture.releases));
        assert_eq!(
            std::fs::read(execution.join("firecracker")).unwrap(),
            binary(1)
        );
    }
}

#[test]
fn a_managed_copy_of_the_image_kernel_is_verified_and_removable() {
    let fixture = Fixture::new();
    let image = vec![0x42; 8192];
    let kernel = |version: &str, size| PinnedKernel {
        version: ArtifactVersion::parse(version).unwrap(),
        runtime: RuntimeKind::Firecracker,
        runtime_line: RuntimeVersion::parse("1.15").unwrap(),
        architecture: Arch::X86_64,
        size,
        sha256: sha256(&image),
    };
    let staged_image = fixture
        .staging
        .join("firecracker/kernels/job-1/6.1.155/vmlinux.bin");
    std::fs::create_dir_all(staged_image.parent().unwrap()).unwrap();
    std::fs::write(&staged_image, &image).unwrap();

    let error = fixture
        .store
        .publish_kernel(&kernel("6.1.155", 4096), &staged("job-1"))
        .unwrap_err();
    assert!(matches!(error, PublishError::WrongSize { .. }), "{error}");
    let published = fixture
        .store
        .publish_kernel(&kernel("6.1.155", 8192), &staged("job-1"))
        .unwrap();
    assert_eq!(published.digest, sha256(&image));
    let installed = fixture.kernels.join("6.1.155/vmlinux.bin");
    assert_eq!(std::fs::read(&installed).unwrap(), image);
    assert_eq!(
        std::fs::metadata(&installed).unwrap().permissions().mode() & 0o777,
        0o644
    );

    let version = ArtifactVersion::parse("6.1.155").unwrap();
    assert!(matches!(
        fixture
            .store
            .remove_kernel(&version, Digest::sha256([0; 32])),
        Err(RemoveError::IdentityChanged { .. })
    ));
    assert_eq!(std::fs::read(&installed).unwrap(), image);
    assert_eq!(
        fixture
            .store
            .remove_kernel(&version, sha256(&image))
            .unwrap(),
        RemoveOutcome::Removed
    );
    assert!(!fixture.kernels.join("6.1.155").exists());
}

#[test]
fn interrupted_work_is_cleared() {
    let fixture = Fixture::new();
    fixture.place_release("1.16.2", 5);
    for leftover in [".incoming-v1.16.1-job-1", ".removing-v1.16.0"] {
        std::fs::create_dir_all(fixture.releases.join(leftover).join("bin")).unwrap();
    }
    std::fs::create_dir_all(fixture.kernels.join(".incoming-6.1.158-job-2")).unwrap();

    assert_eq!(fixture.store.clear_leftovers().unwrap(), 3);
    assert!(fixture.releases.join("v1.15.1/bin/firecracker").exists());
    assert!(no_leftovers(&fixture.releases));
    assert!(no_leftovers(&fixture.kernels));
    assert!(fixture.releases.join("v1.16.2/bin/firecracker").exists());
}

#[test]
fn a_staged_name_is_one_plain_component() {
    StagedName::parse("01a0f799-a8d6-70df-bdc9-be151f12112a").unwrap();
    for value in ["", "..", "a/b", ".hidden", "a b", &"x".repeat(65)] {
        StagedName::parse(value).expect_err(value);
    }
}

/// A directory an earlier publication left, holding other bytes, is never
/// what a publication renames into the store.
#[test]
fn a_publication_never_uses_another_ones_work_directory() {
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    let stale = fixture.releases.join(".incoming-v1.16.2-job-1/bin");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("firecracker"), binary(9)).unwrap();

    let published = fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
        .unwrap();
    assert_eq!(published.outcome, PublishOutcome::Published);
    assert_eq!(
        std::fs::read(fixture.releases.join("v1.16.2/bin/firecracker")).unwrap(),
        binary(7)
    );
    assert_eq!(std::fs::read(stale.join("firecracker")).unwrap(), binary(9));
}

/// Publications of one artifact from one staging directory, at once: the
/// lock on the root serializes them, so one publishes, the rest find it,
/// and nothing is left behind.
#[test]
fn concurrent_publications_of_one_release_publish_the_pinned_bytes() {
    let fixture = Fixture::new();
    fixture.stage_release("job-1", "1.16.2", 7);
    let outcomes: Vec<PublishOutcome> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    fixture
                        .store
                        .publish_release(&pin("1.16.2", 7), &staged("job-1"))
                        .unwrap()
                        .outcome
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect()
    });
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| **outcome == PublishOutcome::Published)
            .count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        std::fs::read(fixture.releases.join("v1.16.2/bin/firecracker")).unwrap(),
        binary(7)
    );
    assert!(no_leftovers(&fixture.releases));
}

#[test]
fn the_staging_root_is_created_private_and_anything_else_there_replaced() {
    let fixture = Fixture::new();
    let metadata = std::fs::metadata(&fixture.staging).unwrap();
    let (uid, gid) = (metadata.uid(), metadata.gid());
    let mode = |path: &Path| std::fs::symlink_metadata(path).unwrap().mode() & 0o7777;

    // The fixture's 0755 directory is not private: replaced.
    std::fs::write(fixture.staging.join("left"), b"x").unwrap();
    assert!(fixture.store.ensure_staging_root(uid, gid).unwrap());
    assert_eq!(mode(&fixture.staging), 0o700);
    assert!(!fixture.staging.join("left").exists());

    // A private one is kept with what is being staged in it.
    std::fs::write(fixture.staging.join("staging"), b"x").unwrap();
    assert!(!fixture.store.ensure_staging_root(uid, gid).unwrap());
    assert!(fixture.staging.join("staging").exists());

    // A link at the path is removed, never followed.
    let elsewhere = fixture.releases.join("v1.15.1");
    std::fs::remove_dir_all(&fixture.staging).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &fixture.staging).unwrap();
    assert!(fixture.store.ensure_staging_root(uid, gid).unwrap());
    assert!(
        std::fs::symlink_metadata(&fixture.staging)
            .unwrap()
            .is_dir()
    );
    assert!(elsewhere.join("bin/firecracker").exists());

    // Nothing is created without its parent.
    let orphan = ArtifactStore::new(
        &fixture.releases,
        &fixture.kernels,
        fixture.staging.join("missing/staging"),
        1 << 20,
    );
    assert_eq!(
        orphan.ensure_staging_root(uid, gid).unwrap_err().kind(),
        std::io::ErrorKind::NotFound
    );
}

#[test]
fn an_empty_managed_store_is_additive_and_can_return_to_empty() {
    let fixture = Fixture::new();
    std::fs::remove_dir_all(&fixture.releases).unwrap();
    std::fs::remove_dir_all(&fixture.kernels).unwrap();
    let execution = fixture._dir.path().join("firecracker/v1.15.1/kernel");
    std::fs::create_dir_all(&execution).unwrap();
    std::fs::write(execution.join("vmlinux.bin"), b"existing image").unwrap();
    fixture.store.prepare_roots().unwrap();
    fixture.store.prepare_roots().unwrap();
    fixture.stage_release("first", "1.16.2", 7);
    fixture
        .store
        .publish_release(&pin("1.16.2", 7), &staged("first"))
        .unwrap();
    assert!(matches!(
        fixture
            .store
            .remove_release(&firecracker("1.16.2"), Digest::sha256([0; 32])),
        Err(RemoveError::IdentityChanged { .. })
    ));
    fixture
        .store
        .remove_release(&firecracker("1.16.2"), pin("1.16.2", 7).digest())
        .unwrap();
    assert_eq!(std::fs::read_dir(&fixture.releases).unwrap().count(), 0);
    assert_eq!(
        std::fs::read(execution.join("vmlinux.bin")).unwrap(),
        b"existing image"
    );
    // A prior prototype's link is not followed or repaired during preparation.
    std::fs::remove_dir(&fixture.releases).unwrap();
    std::os::unix::fs::symlink(&execution, &fixture.releases).unwrap();
    assert!(fixture.store.prepare_roots().is_err());
    assert_eq!(
        std::fs::read(execution.join("vmlinux.bin")).unwrap(),
        b"existing image"
    );
}

#[test]
fn reserved_kernel_names_are_refused_before_touching_the_store() {
    let fixture = Fixture::new();
    for name in [".", "..", ".incoming-hidden", ".removing-hidden"] {
        let error = fixture
            .store
            .remove_kernel(&ArtifactVersion::parse(name).unwrap(), sha256(b"missing"))
            .expect_err("reserved store names cannot be removed");
        assert!(!error.is_transient());
    }
    assert!(fixture.kernels.is_dir());
}

#[test]
fn missing_managed_files_require_repair_not_a_retry() {
    let fixture = Fixture::new();
    std::fs::remove_file(fixture.releases.join("v1.15.1/bin/jailer")).unwrap();
    let error = fixture
        .store
        .remove_release(&firecracker("1.15.1"), pin("1.15.1", 1).digest())
        .unwrap_err();
    assert!(!error.is_transient(), "{error:?}");
    std::fs::create_dir(fixture.kernels.join("6.1.155")).unwrap();
    let error = fixture
        .store
        .remove_kernel(&"6.1.155".parse().unwrap(), sha256(b"missing"))
        .unwrap_err();
    assert!(!error.is_transient(), "{error:?}");
}

#[test]
fn cleanup_skips_a_busy_root_and_clears_the_other_root() {
    let fixture = Fixture::new();
    let busy = fixture.releases.join(".incoming-busy");
    let idle = fixture.kernels.join(".incoming-abandoned");
    std::fs::create_dir(&busy).unwrap();
    std::fs::create_dir(&idle).unwrap();
    let locked = lock_root(&fixture.releases).unwrap();
    let store = fixture.store.clone();
    let (sent, received) = std::sync::mpsc::channel();
    let cleanup = std::thread::spawn(move || sent.send(store.clear_leftovers()).unwrap());
    let result = received.recv_timeout(std::time::Duration::from_secs(1));
    drop(locked);
    cleanup.join().unwrap();
    assert_eq!(
        result
            .expect("cleanup must not wait for publication")
            .unwrap(),
        1
    );
    assert!(busy.exists());
    assert!(!idle.exists());
}
