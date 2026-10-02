//! # Test matrix
//!
//! - Install: publishes verified bytes (`an_install_downloads_verifies_and_publishes_the_kernel`)
//!   whose banner, if any, names the listed release (`an_image_banner_must_name_the_listed_release`),
//!   is a no-op when installed (`installing_an_installed_kernel_again_writes_nothing`) or
//!   provided externally with the same bytes
//!   (`an_external_kernel_with_the_same_bytes_satisfies_the_install_and_stays_external`).
//! - Refusals up front: another line's install, missing MD5, unlisted, oversized
//!   (`a_kernel_recorded_from_elsewhere_is_refused`, `a_kernel_the_source_cannot_vouch_for_is_refused`).
//! - Job failures: external kernel with other bytes, mismatched or short or long or broken or
//!   non-boot or other-release downloads, stalls (`an_external_kernel_with_other_bytes_fails_the_job`,
//!   `a_download_that_does_not_match_the_listing_fails_and_leaves_nothing`,
//!   `a_stalled_download_fails_after_the_idle_timeout`).
//! - Serialization: queueing and joining (`operations_run_one_at_a_time_and_a_repeat_joins_the_running_one`).
//! - Cancel: downloading, queued, too late (`cancelling_a_download_stops_it_and_leaves_nothing`,
//!   `a_queued_operation_can_be_cancelled`, `an_operation_past_downloading_cannot_be_cancelled`).
//! - Shutdown and restart: `shutdown_leaves_a_download_queued_and_the_next_start_reruns_it`,
//!   `a_restart_finishes_interrupted_operations_from_the_disk`,
//!   `a_restart_finishes_an_interrupted_uninstall_either_way`.
//! - Uninstall: only space-managed, never selected (`uninstall_removes_only_a_kernel_the_runtime_manager_installed`,
//!   `a_kernel_an_alias_selects_is_not_uninstalled`).
//! - Install root: records under another root are never acted on
//!   (`a_changed_install_root_leaves_the_old_roots_kernels_alone`).
//! - Committed steps outlive their job: a recorded outcome cannot be cancelled
//!   (`an_install_whose_outcome_is_recorded_cannot_be_cancelled`), and a publication whose
//!   record write ran out of attempts is recorded on restart or adopted by the next install
//!   (`a_restart_records_a_publication_whose_job_ran_out_of_attempts`,
//!   `the_next_install_adopts_a_publication_that_lost_its_record`).
//! - Seeding: a pre-installed kernel the image's manifest lists is managed like an installed one
//!   (`a_seeded_kernel_is_managed_like_an_installed_one`; the manifest rules are in `seed/tests.rs`).

use sha2::{Digest as _, Sha256};

use super::*;
use crate::installs::source::{ContentMd5, SourceKernel};
use crate::installs::store::JobPhase;
use crate::installs::test_support::{
    Body, FakeSource, banner_kernel, boot_kernel, fc_line, md5_of,
};

const LIMITS: InstallLimits = InstallLimits {
    max_bytes: 1 << 20,
    download_idle_timeout: Duration::from_secs(30),
    // One attempt, so a transfer failure settles without the shared worker's
    // backoff.
    max_attempts: 1,
};

struct Fixture {
    _dir: tempfile::TempDir,
    root: PathBuf,
    state: PathBuf,
    source: Arc<FakeSource>,
    installer: Installer,
}

impl Fixture {
    async fn new() -> Self {
        Self::with_limits(LIMITS).await
    }

    async fn with_limits(limits: InstallLimits) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("vendor/kernels");
        let state = dir.path().join("vendor/runtime-manager/state");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        let source = Arc::new(FakeSource::default());
        let installer = Installer::open(root.clone(), &state, limits, source.clone())
            .await
            .unwrap();
        Self {
            _dir: dir,
            root,
            state,
            source,
            installer,
        }
    }

    /// Open a second installer over the same directories, as a restart does.
    async fn reopen(&self) -> Installer {
        Installer::open(self.root.clone(), &self.state, LIMITS, self.source.clone())
            .await
            .unwrap()
    }

    async fn install(&self, line: &str, version: &str) -> Result<Operation, InstallKernelError> {
        self.installer
            .install(fc_line(line), version.parse().unwrap())
            .await
    }

    async fn uninstall(&self, version: &str) -> Result<Operation, UninstallKernelError> {
        self.installer.uninstall(version.parse().unwrap()).await
    }

    fn install_record(&self, version: &str) -> Option<KernelInstall> {
        store::get_kernel_install(&self.installer.executor.db(), &version.parse().unwrap()).unwrap()
    }

    fn staging_is_empty(&self) -> bool {
        std::fs::read_dir(self.state.join(STAGING_DIR))
            .unwrap()
            .next()
            .is_none()
    }
}

/// Wait for an operation to finish.
async fn settled(installer: &Installer, job_id: JobId) -> Operation {
    for _ in 0..500 {
        let operation = installer.get(job_id).await.unwrap().unwrap();
        if operation.state.is_finished() {
            return operation;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("operation {job_id} did not settle");
}

/// Wait until an operation is in `state`.
async fn reaches(installer: &Installer, job_id: JobId, state: OperationState) {
    for _ in 0..500 {
        if installer.get(job_id).await.unwrap().unwrap().state == state {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("operation {job_id} never reached {state:?}");
}

fn sha256_of(bytes: &[u8]) -> Digest {
    Digest::sha256(Sha256::digest(bytes).into())
}

fn stalled(fixture: &Fixture, version: &str, image: &[u8]) -> SourceKernel {
    fixture.source.offer_with(
        "1.15",
        version,
        image.len() as u64 * 2,
        Some(md5_of(image)),
        Body::Stall(image.to_vec()),
    )
}

#[tokio::test]
async fn an_install_downloads_verifies_and_publishes_the_kernel() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    fixture.source.offer("1.15", "6.1.155", &image);

    let accepted = fixture.install("1.15.1", "6.1.155").await.unwrap();
    assert!(matches!(
        accepted.state,
        OperationState::Queued | OperationState::Downloading | OperationState::Succeeded(_)
    ));
    let finished = settled(&fixture.installer, accepted.job_id).await;
    assert_eq!(
        finished.state,
        OperationState::Succeeded(JobOutcome::Installed),
        "{finished:?}"
    );
    let dir = fixture.root.join("6.1.155");
    assert_eq!(
        finished.kernel,
        Some(KernelOnDisk {
            path: dir.clone(),
            digest: sha256_of(&image)
        })
    );
    assert_eq!(std::fs::read(dir.join("vmlinux.bin")).unwrap(), image);
    let mode = |path: &Path| {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
    };
    assert_eq!(mode(&dir), 0o755);
    assert_eq!(mode(&dir.join("vmlinux.bin")), 0o644);

    let recorded = fixture.install_record("6.1.155").unwrap();
    assert_eq!(recorded.digest, sha256_of(&image));
    assert_eq!(recorded.line, fc_line("1.15"));
    assert_eq!(recorded.md5, md5_of(&image));
    assert_eq!(recorded.job_id, Some(accepted.job_id));
    assert!(fixture.staging_is_empty());
}

#[tokio::test]
async fn an_image_banner_must_name_the_listed_release() {
    let fixture = Fixture::new().await;
    // A modified-tree marker and a variant suffix are not part of the release.
    for (version, release) in [("6.1.155", "6.1.155+"), ("5.10.245-no-acpi", "5.10.245")] {
        fixture
            .source
            .offer("1.15", version, &banner_kernel(4096, release));
        let accepted = fixture.install("1.15", version).await.unwrap();
        let finished = settled(&fixture.installer, accepted.job_id).await;
        assert_eq!(
            finished.state,
            OperationState::Succeeded(JobOutcome::Installed),
            "{version}: {finished:?}"
        );
    }

    fixture
        .source
        .offer("1.15", "6.1.1", &banner_kernel(4096, "6.1.155"));
    let accepted = fixture.install("1.15", "6.1.1").await.unwrap();
    let finished = settled(&fixture.installer, accepted.job_id).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert!(
        finished
            .error
            .as_deref()
            .is_some_and(|error| error.contains("release 6.1.155")),
        "{finished:?}"
    );
    assert!(!fixture.root.join("6.1.1").exists());
}

#[tokio::test]
async fn installing_an_installed_kernel_again_writes_nothing() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let first = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, first.job_id).await;

    let again = fixture.install("1.15", "6.1.155").await.unwrap();
    let again = settled(&fixture.installer, again.job_id).await;
    assert_eq!(
        again.state,
        OperationState::Succeeded(JobOutcome::AlreadyInstalled)
    );
    assert_eq!(fixture.source.opens(), 1);
}

#[tokio::test]
async fn an_external_kernel_with_the_same_bytes_satisfies_the_install_and_stays_external() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    fixture.source.offer("1.15", "6.1.155", &image);
    std::fs::create_dir_all(fixture.root.join("6.1.155")).unwrap();
    std::fs::write(fixture.root.join("6.1.155/vmlinux.bin"), &image).unwrap();

    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    let installed = settled(&fixture.installer, installed.job_id).await;
    assert_eq!(
        installed.state,
        OperationState::Succeeded(JobOutcome::ProvidedExternally)
    );
    assert_eq!(installed.kernel.unwrap().digest, sha256_of(&image));
    assert_eq!(fixture.source.opens(), 0);
    assert!(fixture.install_record("6.1.155").is_none());
}

#[tokio::test]
async fn an_external_kernel_with_other_bytes_fails_the_job() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    std::fs::create_dir_all(fixture.root.join("6.1.155")).unwrap();
    std::fs::write(
        fixture.root.join("6.1.155/vmlinux.bin"),
        boot_kernel(4096, 8),
    )
    .unwrap();
    // Comparing the bytes is the job's work, not the request's.
    let accepted = fixture.install("1.15", "6.1.155").await.unwrap();
    let finished = settled(&fixture.installer, accepted.job_id).await;
    assert_eq!(finished.state, OperationState::Failed);
    assert!(
        finished.error.unwrap().contains("different bytes"),
        "the error names the conflict"
    );
    assert_eq!(fixture.source.opens(), 0);
}

#[tokio::test]
async fn a_kernel_recorded_from_elsewhere_is_refused() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    fixture
        .source
        .offer("1.14", "6.1.155", &boot_kernel(4096, 9));
    let first = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, first.job_id).await;
    let error = fixture.install("1.14", "6.1.155").await.unwrap_err();
    assert!(
        matches!(
            error,
            InstallKernelError::Conflict(KernelConflict::InstalledFromElsewhere { .. })
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_kernel_the_source_cannot_vouch_for_is_refused() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    fixture.source.offer_with(
        "1.15",
        "6.1.155",
        image.len() as u64,
        None,
        Body::Bytes(image.clone()),
    );
    let error = fixture.install("1.15", "6.1.155").await.unwrap_err();
    assert!(
        matches!(error, InstallKernelError::Unverifiable { .. }),
        "{error}"
    );

    let error = fixture.install("1.15", "6.1.999").await.unwrap_err();
    assert!(
        matches!(error, InstallKernelError::NotOffered { .. }),
        "{error}"
    );

    fixture.source.offer_with(
        "1.15",
        "6.1.200",
        LIMITS.max_bytes + 1,
        Some(md5_of(&image)),
        Body::Bytes(image),
    );
    let error = fixture.install("1.15", "6.1.200").await.unwrap_err();
    assert!(
        matches!(error, InstallKernelError::TooLarge { .. }),
        "{error}"
    );
    assert_eq!(fixture.source.opens(), 0);
}

/// Each way a download can go wrong fails the operation and leaves nothing.
#[tokio::test]
async fn a_download_that_does_not_match_the_listing_fails_and_leaves_nothing() {
    let image = boot_kernel(4096, 7);
    let cases: Vec<(&str, u64, ContentMd5, Body)> = vec![
        // Bytes changed on the way: the MD5 does not match.
        (
            "6.1.1",
            image.len() as u64,
            md5_of(&image),
            Body::Bytes(boot_kernel(4096, 8)),
        ),
        // Fewer bytes than listed.
        (
            "6.1.2",
            image.len() as u64 + 10,
            md5_of(&image),
            Body::Bytes(image.clone()),
        ),
        // More bytes than listed.
        (
            "6.1.3",
            image.len() as u64 - 10,
            md5_of(&image),
            Body::Bytes(image.clone()),
        ),
        // A broken transfer.
        (
            "6.1.4",
            image.len() as u64 * 2,
            md5_of(&image),
            Body::FailAfter(image.clone()),
        ),
        // Not a kernel this node can boot.
        (
            "6.1.5",
            4096,
            md5_of(&[0u8; 4096]),
            Body::Bytes(vec![0u8; 4096]),
        ),
        // A kernel of another release than the one listed.
        (
            "6.1.6",
            4096,
            md5_of(&banner_kernel(4096, "6.1.155+")),
            Body::Bytes(banner_kernel(4096, "6.1.155+")),
        ),
    ];
    let fixture = Fixture::new().await;
    for (version, size, md5, body) in cases {
        fixture
            .source
            .offer_with("1.15", version, size, Some(md5), body);
        let accepted = fixture.install("1.15", version).await.unwrap();
        let finished = settled(&fixture.installer, accepted.job_id).await;
        assert_eq!(
            finished.state,
            OperationState::Failed,
            "{version}: {finished:?}"
        );
        assert!(finished.error.is_some(), "{version}");
        assert!(!fixture.root.join(version).exists(), "{version}");
        assert!(fixture.install_record(version).is_none(), "{version}");
        assert!(fixture.staging_is_empty(), "{version}");
    }
}

#[tokio::test]
async fn operations_run_one_at_a_time_and_a_repeat_joins_the_running_one() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    stalled(&fixture, "6.1.155", &image);
    fixture.source.offer("1.15", "5.10.245", &image);

    let running = fixture.install("1.15", "6.1.155").await.unwrap();
    reaches(
        &fixture.installer,
        running.job_id,
        OperationState::Downloading,
    )
    .await;
    let joined = fixture.install("1.15.2", "6.1.155").await.unwrap();
    assert_eq!(joined.job_id, running.job_id);

    let queued = fixture.install("1.15", "5.10.245").await.unwrap();
    assert_eq!(queued.state, OperationState::Queued);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        fixture
            .installer
            .get(queued.job_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Queued
    );

    fixture.installer.cancel(running.job_id).await.unwrap();
    assert_eq!(
        settled(&fixture.installer, queued.job_id).await.state,
        OperationState::Succeeded(JobOutcome::Installed)
    );
}

#[tokio::test]
async fn cancelling_a_download_stops_it_and_leaves_nothing() {
    let fixture = Fixture::new().await;
    stalled(&fixture, "6.1.155", &boot_kernel(4096, 7));
    let running = fixture.install("1.15", "6.1.155").await.unwrap();
    reaches(
        &fixture.installer,
        running.job_id,
        OperationState::Downloading,
    )
    .await;
    let cancelled = fixture.installer.cancel(running.job_id).await.unwrap();
    assert_eq!(cancelled.state, OperationState::Cancelled);
    assert!(!fixture.root.join("6.1.155").exists());
    assert!(fixture.staging_is_empty());

    // Cancelling a finished operation returns it unchanged.
    let again = fixture.installer.cancel(running.job_id).await.unwrap();
    assert_eq!(again.state, OperationState::Cancelled);
    let unknown = fixture.installer.cancel(JobId::new()).await.unwrap_err();
    assert!(
        matches!(unknown, CancelOperationError::NotFound(_)),
        "{unknown}"
    );
}

#[tokio::test]
async fn a_queued_operation_can_be_cancelled() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    stalled(&fixture, "6.1.155", &image);
    fixture.source.offer("1.15", "5.10.245", &image);
    let running = fixture.install("1.15", "6.1.155").await.unwrap();
    let queued = fixture.install("1.15", "5.10.245").await.unwrap();
    assert_eq!(
        fixture.installer.cancel(queued.job_id).await.unwrap().state,
        OperationState::Cancelled
    );
    fixture.installer.cancel(running.job_id).await.unwrap();
    assert_eq!(fixture.source.opens(), 1);
}

#[tokio::test]
async fn an_operation_past_downloading_cannot_be_cancelled() {
    let fixture = Fixture::new().await;
    // As a job records just before it renames.
    let removing = interrupted(
        &fixture,
        &KernelOperation::Uninstall {
            version: "6.1.155".parse().unwrap(),
        },
        &JobResult::InProgress {
            phase: JobPhase::Removing,
            path: fixture.root.join("6.1.155"),
            digest: Digest::sha256([0xab; 32]),
        },
    );
    let error = fixture.installer.cancel(removing).await.unwrap_err();
    assert!(matches!(error, CancelOperationError::TooLate(_)), "{error}");
}

#[tokio::test]
async fn a_stalled_download_fails_after_the_idle_timeout() {
    let fixture = Fixture::with_limits(InstallLimits {
        download_idle_timeout: Duration::from_millis(50),
        ..LIMITS
    })
    .await;
    stalled(&fixture, "6.1.155", &boot_kernel(4096, 7));
    let running = fixture.install("1.15", "6.1.155").await.unwrap();
    let finished = settled(&fixture.installer, running.job_id).await;
    assert_eq!(finished.state, OperationState::Failed);
    assert!(finished.error.unwrap().contains("stalled"));
}

#[tokio::test]
async fn shutdown_leaves_a_download_queued_and_the_next_start_reruns_it() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    let kernel = stalled(&fixture, "6.1.155", &image);
    let running = fixture.install("1.15", "6.1.155").await.unwrap();
    reaches(
        &fixture.installer,
        running.job_id,
        OperationState::Downloading,
    )
    .await;
    fixture.installer.shutdown().await;
    assert_eq!(
        fixture
            .installer
            .get(running.job_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Queued
    );

    // The source serves the listed number of bytes the next time, but not
    // the listed MD5: the re-run downloaded afresh and checked it.
    fixture.source.serve(
        &kernel,
        Body::Bytes(image.iter().copied().chain(image.iter().copied()).collect()),
    );
    let restarted = fixture.reopen().await;
    let finished = settled(&restarted, running.job_id).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert!(finished.error.unwrap().contains("MD5"));
    assert!(fixture.staging_is_empty());
}

#[tokio::test]
async fn uninstall_removes_only_a_kernel_the_runtime_manager_installed() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, installed.job_id).await;

    let removed = fixture.uninstall("6.1.155").await.unwrap();
    let removed = settled(&fixture.installer, removed.job_id).await;
    assert_eq!(
        removed.state,
        OperationState::Succeeded(JobOutcome::Uninstalled),
        "{removed:?}"
    );
    assert!(!fixture.root.join("6.1.155").exists());
    assert!(fixture.install_record("6.1.155").is_none());
    assert!(fixture.staging_is_empty());

    let error = fixture.uninstall("6.1.155").await.unwrap_err();
    assert!(
        matches!(error, UninstallKernelError::NotInstalled { .. }),
        "{error}"
    );

    std::fs::create_dir_all(fixture.root.join("5.10.245")).unwrap();
    std::fs::write(fixture.root.join("5.10.245/vmlinux.bin"), b"baked").unwrap();
    let error = fixture.uninstall("5.10.245").await.unwrap_err();
    assert!(
        matches!(error, UninstallKernelError::External { .. }),
        "{error}"
    );
    assert!(fixture.root.join("5.10.245").exists());
}

#[tokio::test]
async fn a_kernel_an_alias_selects_is_not_uninstalled() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, installed.job_id).await;
    std::os::unix::fs::symlink("6.1.155", fixture.root.join("legacy-default")).unwrap();

    let error = fixture.uninstall("6.1.155").await.unwrap_err();
    assert!(
        matches!(error, UninstallKernelError::Selected { .. }),
        "{error}"
    );
    assert!(fixture.root.join("6.1.155/vmlinux.bin").exists());
    assert!(fixture.install_record("6.1.155").is_some());
}

/// A job a restart interrupted mid-rename, recreated as the database and disk
/// would hold it.
fn interrupted(fixture: &Fixture, request: &KernelOperation, result: &JobResult) -> JobId {
    let job_id = queued_after(fixture, request, result);
    pplx_space_common_jobs::db::mark_job_running(&fixture.installer.executor.db(), &job_id)
        .unwrap();
    job_id
}

/// A job queued again after an attempt recorded `result`, as a retry leaves
/// it.
fn queued_after(fixture: &Fixture, request: &KernelOperation, result: &JobResult) -> JobId {
    let job_id = JobId::new();
    let db = fixture.installer.executor.db();
    pplx_space_common_jobs::db::insert_job(
        &db,
        pplx_space_common_jobs::db::InsertJob {
            job_id: &job_id,
            kind: KERNEL_JOB_KIND,
            payload: &serde_json::to_string(request).unwrap(),
            subject_id: None,
            dedup_key: None,
            max_attempts: 1,
            traceparent: None,
            priority: pplx_space_common_jobs::JobPriority::default(),
        },
    )
    .unwrap();
    store::put_job_result(&db, &job_id, result, jiff::Timestamp::now()).unwrap();
    job_id
}

/// A restart re-runs each interrupted job, which resolves what it finds on
/// disk instead of repeating work that landed.
#[tokio::test]
async fn a_restart_finishes_interrupted_operations_from_the_disk() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    let digest = sha256_of(&image);
    let place = |version: &str, bytes: &[u8]| {
        std::fs::create_dir_all(fixture.root.join(version)).unwrap();
        std::fs::write(fixture.root.join(version).join("vmlinux.bin"), bytes).unwrap();
    };
    let install = |kernel: &SourceKernel| KernelOperation::Install {
        kernel: kernel.verifiable().unwrap(),
    };
    let publishing = |version: &str| JobResult::InProgress {
        phase: JobPhase::Publishing,
        path: fixture.root.join(version),
        digest,
    };

    // Renamed before the crash: recorded without downloading again.
    let landed = fixture.source.offer("1.15", "6.1.1", &image);
    place("6.1.1", &image);
    let landed_job = interrupted(&fixture, &install(&landed), &publishing("6.1.1"));
    // Not renamed yet: downloaded again.
    let unlanded = fixture.source.offer("1.15", "6.1.2", &image);
    let unlanded_job = interrupted(&fixture, &install(&unlanded), &publishing("6.1.2"));
    // Something else is there now: left alone.
    let replaced = fixture.source.offer("1.15", "6.1.3", &image);
    place("6.1.3", &boot_kernel(4096, 8));
    let replaced_job = interrupted(&fixture, &install(&replaced), &publishing("6.1.3"));

    let restarted = fixture.reopen().await;
    let finished = settled(&restarted, landed_job).await;
    assert_eq!(
        finished.state,
        OperationState::Succeeded(JobOutcome::Installed),
        "{finished:?}"
    );
    assert_eq!(
        fixture.install_record("6.1.1").unwrap().job_id,
        Some(landed_job)
    );
    let finished = settled(&restarted, unlanded_job).await;
    assert_eq!(
        finished.state,
        OperationState::Succeeded(JobOutcome::Installed),
        "{finished:?}"
    );
    assert_eq!(
        std::fs::read(fixture.root.join("6.1.2/vmlinux.bin")).unwrap(),
        image
    );
    let finished = settled(&restarted, replaced_job).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert!(fixture.install_record("6.1.3").is_none());
    assert_eq!(
        std::fs::read(fixture.root.join("6.1.3/vmlinux.bin")).unwrap(),
        boot_kernel(4096, 8),
        "left for maintenance"
    );
    // Only the kernel whose rename never landed was downloaded again.
    assert_eq!(fixture.source.opens(), 1);
}

#[tokio::test]
async fn a_restart_finishes_an_interrupted_uninstall_either_way() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    for version in ["6.1.1", "6.1.2"] {
        fixture.source.offer("1.15", version, &image);
        let installed = fixture.install("1.15", version).await.unwrap();
        settled(&fixture.installer, installed.job_id).await;
    }
    let removing = |version: &str| JobResult::InProgress {
        phase: JobPhase::Removing,
        path: fixture.root.join(version),
        digest: sha256_of(&image),
    };
    let request = |version: &str| KernelOperation::Uninstall {
        version: version.parse().unwrap(),
    };
    // Renamed away before the crash.
    std::fs::remove_dir_all(fixture.root.join("6.1.1")).unwrap();
    let renamed = interrupted(&fixture, &request("6.1.1"), &removing("6.1.1"));
    // Not renamed yet.
    let unrenamed = interrupted(&fixture, &request("6.1.2"), &removing("6.1.2"));

    let restarted = fixture.reopen().await;
    for (job_id, version) in [(renamed, "6.1.1"), (unrenamed, "6.1.2")] {
        let finished = settled(&restarted, job_id).await;
        assert_eq!(
            finished.state,
            OperationState::Succeeded(JobOutcome::Uninstalled),
            "{version}: {finished:?}"
        );
        assert!(fixture.install_record(version).is_none(), "{version}");
        assert!(!fixture.root.join(version).exists(), "{version}");
    }
}

/// Records and job results keep absolute paths. After a restart with another
/// install root, nothing under the old root is reported present, removed,
/// recorded, or forgotten.
#[tokio::test]
async fn a_changed_install_root_leaves_the_old_roots_kernels_alone() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    let digest = sha256_of(&image);
    for version in ["6.1.155", "6.1.1", "6.1.2"] {
        fixture.source.offer("1.15", version, &image);
        let installed = fixture.install("1.15", version).await.unwrap();
        settled(&fixture.installer, installed.job_id).await;
    }
    let old = |version: &str| fixture.root.join(version);
    let uninstall = |version: &str| KernelOperation::Uninstall {
        version: version.parse().unwrap(),
    };
    let removing = |version: &str| JobResult::InProgress {
        phase: JobPhase::Removing,
        path: old(version),
        digest,
    };
    // Interrupted before its rename: would rename the old root's kernel.
    let unrenamed = interrupted(&fixture, &uninstall("6.1.1"), &removing("6.1.1"));
    // Gone from the old root: would forget its record.
    std::fs::remove_dir_all(old("6.1.2")).unwrap();
    let renamed = interrupted(&fixture, &uninstall("6.1.2"), &removing("6.1.2"));
    // Interrupted publishing into the old root: would record it.
    let published = fixture.source.offer("1.15", "6.1.3", &image);
    std::fs::create_dir_all(old("6.1.3")).unwrap();
    std::fs::write(old("6.1.3").join("vmlinux.bin"), &image).unwrap();
    let publishing = interrupted(
        &fixture,
        &KernelOperation::Install {
            kernel: published.verifiable().unwrap(),
        },
        &JobResult::InProgress {
            phase: JobPhase::Publishing,
            path: old("6.1.3"),
            digest,
        },
    );

    // The old root's worker would otherwise pick the re-queued jobs up.
    fixture.installer.shutdown().await;
    let new_root = fixture.root.with_file_name("other-kernels");
    std::fs::create_dir_all(&new_root).unwrap();
    let moved = Installer::open(
        new_root.clone(),
        &fixture.state,
        LIMITS,
        fixture.source.clone(),
    )
    .await
    .unwrap();

    let error = moved
        .uninstall("6.1.155".parse().unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(error, UninstallKernelError::OutsideRoot { .. }),
        "{error}"
    );
    let error = moved
        .install(fc_line("1.15"), "6.1.155".parse().unwrap())
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            InstallKernelError::Conflict(KernelConflict::OutsideRoot { .. })
        ),
        "{error}"
    );
    for job_id in [unrenamed, renamed, publishing] {
        let finished = settled(&moved, job_id).await;
        assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    }

    for version in ["6.1.155", "6.1.1", "6.1.3"] {
        assert_eq!(
            std::fs::read(old(version).join("vmlinux.bin")).unwrap(),
            image
        );
    }
    for version in ["6.1.155", "6.1.1", "6.1.2"] {
        assert!(fixture.install_record(version).is_some(), "{version}");
    }
    assert!(fixture.install_record("6.1.3").is_none());
    assert_eq!(std::fs::read_dir(&new_root).unwrap().count(), 0);
}

/// A kernel the node image pre-installed and listed in its manifest is
/// managed like one the runtime manager installed: a reinstall from its
/// source changes nothing, and it can be uninstalled.
#[tokio::test]
async fn a_seeded_kernel_is_managed_like_an_installed_one() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    let listed = fixture.source.offer("1.15", "6.1.155", &image);
    std::fs::create_dir_all(fixture.root.join("6.1.155")).unwrap();
    std::fs::write(fixture.root.join("6.1.155/vmlinux.bin"), &image).unwrap();
    let manifest = fixture.state.join("seed.json");
    std::fs::write(
        &manifest,
        serde_json::to_vec(&serde_json::json!({
            "schema": 1,
            "kernels": [{
                "version": "6.1.155",
                "line": { "runtime": "firecracker", "release": "1.15" },
                "source": listed.location.to_string(),
                "size": image.len(),
                "md5": md5_of(&image).to_string(),
                "sha256": sha256_of(&image).to_string(),
            }],
        }))
        .unwrap(),
    )
    .unwrap();

    let report = fixture.installer.import_seed(manifest).await.unwrap();
    assert_eq!(report.seeded, vec!["6.1.155".parse().unwrap()]);
    assert_eq!(fixture.install_record("6.1.155").unwrap().job_id, None);

    let reinstalled = fixture.install("1.15", "6.1.155").await.unwrap();
    let reinstalled = settled(&fixture.installer, reinstalled.job_id).await;
    assert_eq!(
        reinstalled.state,
        OperationState::Succeeded(JobOutcome::AlreadyInstalled),
        "{reinstalled:?}"
    );
    assert_eq!(fixture.source.opens(), 0);

    let removed = fixture.uninstall("6.1.155").await.unwrap();
    let removed = settled(&fixture.installer, removed.job_id).await;
    assert_eq!(
        removed.state,
        OperationState::Succeeded(JobOutcome::Uninstalled),
        "{removed:?}"
    );
    assert!(!fixture.root.join("6.1.155").exists());
}

/// Run `sql` against the store, here to make writes fail as storage that
/// stops accepting them would, and to stop that again.
fn failing_writes(fixture: &Fixture, sql: &str) {
    fixture.installer.executor.db().execute_batch(sql).unwrap();
}

#[tokio::test]
async fn an_install_whose_outcome_is_recorded_cannot_be_cancelled() {
    let fixture = Fixture::new().await;
    // The worker cannot mark the job completed, so it stays running after
    // the install and its record are written.
    failing_writes(
        &fixture,
        "CREATE TRIGGER no_completion BEFORE UPDATE OF status ON jobs
         WHEN NEW.status = 'completed' BEGIN SELECT RAISE(FAIL, 'completion write unavailable'); END;",
    );
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let accepted = fixture.install("1.15", "6.1.155").await.unwrap();
    for _ in 0..500 {
        if fixture.install_record("6.1.155").is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(fixture.install_record("6.1.155").is_some());

    let result = fixture.installer.cancel(accepted.job_id).await;
    failing_writes(&fixture, "DROP TRIGGER no_completion;");
    assert!(
        matches!(result, Err(CancelOperationError::TooLate(_))),
        "{result:?}"
    );
    assert!(fixture.root.join("6.1.155/vmlinux.bin").exists());
}

/// A job whose record write keeps failing after its rename runs out of
/// attempts with the kernel published and unrecorded.
async fn install_losing_its_record(fixture: &Fixture) -> JobId {
    failing_writes(
        fixture,
        "CREATE TRIGGER no_install_record BEFORE INSERT ON kernel_installs
         BEGIN SELECT RAISE(FAIL, 'install record write unavailable'); END;",
    );
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let accepted = fixture.install("1.15", "6.1.155").await.unwrap();
    let failed = settled(&fixture.installer, accepted.job_id).await;
    assert_eq!(failed.state, OperationState::Failed, "{failed:?}");
    assert!(fixture.root.join("6.1.155/vmlinux.bin").exists());
    assert!(fixture.install_record("6.1.155").is_none());
    failing_writes(fixture, "DROP TRIGGER no_install_record;");
    accepted.job_id
}

#[tokio::test]
async fn a_restart_records_a_publication_whose_job_ran_out_of_attempts() {
    let fixture = Fixture::new().await;
    let lost = install_losing_its_record(&fixture).await;
    fixture.installer.shutdown().await;

    let _restarted = fixture.reopen().await;
    let recorded = fixture.install_record("6.1.155").unwrap();
    assert_eq!(recorded.job_id, Some(lost));
    assert_eq!(recorded.digest, sha256_of(&boot_kernel(4096, 7)));
}

#[tokio::test]
async fn the_next_install_adopts_a_publication_that_lost_its_record() {
    let fixture = Fixture::new().await;
    install_losing_its_record(&fixture).await;

    let again = fixture.install("1.15", "6.1.155").await.unwrap();
    let again = settled(&fixture.installer, again.job_id).await;
    assert_eq!(
        again.state,
        OperationState::Succeeded(JobOutcome::Installed),
        "{again:?}"
    );
    assert_eq!(
        fixture.install_record("6.1.155").unwrap().job_id,
        Some(again.job_id)
    );
    // Nothing was downloaded again, and the kernel can be uninstalled.
    assert_eq!(fixture.source.opens(), 1);
    let removed = fixture.uninstall("6.1.155").await.unwrap();
    let removed = settled(&fixture.installer, removed.job_id).await;
    assert_eq!(
        removed.state,
        OperationState::Succeeded(JobOutcome::Uninstalled),
        "{removed:?}"
    );
}

#[tokio::test]
async fn opening_refuses_a_missing_root() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    std::fs::create_dir_all(&state).unwrap();
    let result = Installer::open(
        dir.path().join("missing"),
        &state,
        LIMITS,
        Arc::new(FakeSource::default()),
    )
    .await;
    assert!(matches!(
        result,
        Err(OpenInstallerError::Staging(PrepareStagingError::Staging(_)))
    ));
}

#[tokio::test]
async fn a_retry_queued_after_its_rename_cannot_be_cancelled() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    let kernel = fixture.source.offer("1.15", "6.1.155", &image);
    std::fs::create_dir_all(fixture.root.join("6.1.155")).unwrap();
    std::fs::write(fixture.root.join("6.1.155/vmlinux.bin"), &image).unwrap();
    // The rename landed, recording the install failed, and the job is queued
    // for another attempt.
    let retrying = queued_after(
        &fixture,
        &KernelOperation::Install {
            kernel: kernel.verifiable().unwrap(),
        },
        &JobResult::InProgress {
            phase: JobPhase::Publishing,
            path: fixture.root.join("6.1.155"),
            digest: sha256_of(&image),
        },
    );
    assert_eq!(
        fixture
            .installer
            .get(retrying)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Queued
    );
    let error = fixture.installer.cancel(retrying).await.unwrap_err();
    assert!(matches!(error, CancelOperationError::TooLate(_)), "{error}");

    // The retry still records the install the rename made.
    let restarted = fixture.reopen().await;
    let finished = settled(&restarted, retrying).await;
    assert_eq!(
        finished.state,
        OperationState::Succeeded(JobOutcome::Installed),
        "{finished:?}"
    );
    assert_eq!(
        fixture.install_record("6.1.155").unwrap().job_id,
        Some(retrying)
    );
}

#[tokio::test]
async fn uninstall_leaves_bytes_it_did_not_install() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, installed.job_id).await;
    let replacement = boot_kernel(4096, 9);
    std::fs::write(fixture.root.join("6.1.155/vmlinux.bin"), &replacement).unwrap();

    let removing = fixture.uninstall("6.1.155").await.unwrap();
    let finished = settled(&fixture.installer, removing.job_id).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert!(finished.error.unwrap().contains("no longer holds"));
    assert_eq!(
        std::fs::read(fixture.root.join("6.1.155/vmlinux.bin")).unwrap(),
        replacement
    );
    assert!(fixture.install_record("6.1.155").is_some());
}

#[tokio::test]
async fn a_restarted_uninstall_leaves_bytes_replaced_after_its_intent() {
    let fixture = Fixture::new().await;
    let image = boot_kernel(4096, 7);
    fixture.source.offer("1.15", "6.1.155", &image);
    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, installed.job_id).await;
    let removing = interrupted(
        &fixture,
        &KernelOperation::Uninstall {
            version: "6.1.155".parse().unwrap(),
        },
        &JobResult::InProgress {
            phase: JobPhase::Removing,
            path: fixture.root.join("6.1.155"),
            digest: sha256_of(&image),
        },
    );
    let replacement = boot_kernel(4096, 9);
    std::fs::write(fixture.root.join("6.1.155/vmlinux.bin"), &replacement).unwrap();

    let restarted = fixture.reopen().await;
    let finished = settled(&restarted, removing).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert_eq!(
        std::fs::read(fixture.root.join("6.1.155/vmlinux.bin")).unwrap(),
        replacement
    );
    assert!(fixture.install_record("6.1.155").is_some());
}

#[tokio::test]
async fn a_reinstall_checks_the_installed_image_not_just_its_directory() {
    let fixture = Fixture::new().await;
    fixture
        .source
        .offer("1.15", "6.1.155", &boot_kernel(4096, 7));
    let installed = fixture.install("1.15", "6.1.155").await.unwrap();
    settled(&fixture.installer, installed.job_id).await;
    let image = fixture.root.join("6.1.155/vmlinux.bin");

    // Rewritten in place: the request cannot tell, the job does.
    std::fs::write(&image, boot_kernel(4096, 9)).unwrap();
    let rewritten = fixture.install("1.15", "6.1.155").await.unwrap();
    let finished = settled(&fixture.installer, rewritten.job_id).await;
    assert_eq!(finished.state, OperationState::Failed, "{finished:?}");
    assert!(finished.error.unwrap().contains("no longer holds"));

    // Gone: refused up front.
    std::fs::remove_file(&image).unwrap();
    let error = fixture.install("1.15", "6.1.155").await.unwrap_err();
    assert!(
        matches!(
            error,
            InstallKernelError::Conflict(KernelConflict::RecordedButMissing { .. })
        ),
        "{error}"
    );
}
