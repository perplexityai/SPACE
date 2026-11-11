//! Shared lifecycle contracts across both artifact kinds. Store permission,
//! link, concurrency and removal policy are covered by artifact_store tests.
use super::*;
use crate::installs::ReleaseSource as _;
use crate::installs::publisher::{ArtifactPublisher, NodeArtifactRef, Published, PublisherError};
use crate::installs::release::ReleaseContext;
use crate::installs::store::JobPhase;
use crate::installs::test_support::*;
use pplx_space_common_jobs::JobExecutor as _;
use pplx_space_node_runtime_core::artifact_store::{StagedName, release_directory};
use std::sync::atomic::{AtomicBool, Ordering};

const LIMITS: InstallLimits = InstallLimits {
    max_bytes: 1 << 20,
    download_idle_timeout: Duration::from_millis(100),
    max_attempts: 1,
};

/// Inject failures at the publication boundary, after bytes have been prepared.
struct FaultPublisher {
    inner: Arc<LocalPublisher>,
    lose: AtomicBool,
    break_cleanup: AtomicBool,
    refuse: AtomicBool,
    staging: PathBuf,
}
#[async_trait::async_trait]
impl ArtifactPublisher for FaultPublisher {
    async fn publish(
        &self,
        artifact: &NodeArtifactRef,
        staged: &StagedName,
    ) -> Result<Published, PublisherError> {
        let result = if self.refuse.load(Ordering::SeqCst) {
            Err(PublisherError::Refused(Box::new(
                tonic::Status::failed_precondition("publication refused"),
            )))
        } else {
            self.inner.publish(artifact, staged).await
        };
        if self.break_cleanup.load(Ordering::SeqCst) {
            let namespace = match artifact {
                NodeArtifactRef::Boot(_) => {
                    panic!("fault fixture only injects kernel/release cleanup errors")
                }
                NodeArtifactRef::Kernel(..) => "firecracker/kernels",
                NodeArtifactRef::Release(..) => "firecracker/releases",
            };
            let path = self.staging.join(namespace).join(staged.as_str());
            std::fs::remove_dir_all(&path).unwrap();
            std::fs::write(path, b"not a directory").unwrap();
        }
        let result = result?;
        if self.lose.swap(false, Ordering::SeqCst) {
            return Err(PublisherError::Unavailable("lost response".into()));
        }
        Ok(result)
    }
    async fn remove(&self, artifact: &NodeArtifactRef) -> Result<bool, PublisherError> {
        let result = self.inner.remove(artifact).await?;
        if self.lose.swap(false, Ordering::SeqCst) {
            return Err(PublisherError::Unavailable("lost response".into()));
        }
        Ok(result)
    }
}

struct Fixture {
    dir: tempfile::TempDir,
    source: Arc<FakeSource>,
    releases: Arc<FakeReleaseSource>,
    pins: Arc<TestPins>,
    publisher: Arc<FaultPublisher>,
    installer: Installer,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        for name in ["node/kernels", "node/firecracker", "state"] {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
        }
        let source = Arc::new(FakeSource::default());
        let releases = Arc::new(FakeReleaseSource::default());
        let pins = TestPins::new(source.clone());
        let publisher = Arc::new(FaultPublisher {
            inner: LocalPublisher::new(
                &dir.path().join("node/firecracker"),
                &dir.path().join("node/kernels"),
                &dir.path().join("staging"),
                pins.clone(),
            ),
            lose: AtomicBool::new(false),
            break_cleanup: AtomicBool::new(false),
            refuse: AtomicBool::new(false),
            staging: dir.path().join("staging"),
        });
        let installer = Installer::open(
            Self::parts(
                dir.path(),
                source.clone(),
                releases.clone(),
                pins.clone(),
                publisher.clone(),
            ),
            &dir.path().join("state"),
        )
        .await
        .unwrap();
        Self {
            dir,
            source,
            releases,
            pins,
            publisher,
            installer,
        }
    }
    fn parts(
        base: &Path,
        source: Arc<FakeSource>,
        releases: Arc<FakeReleaseSource>,
        pins: Arc<TestPins>,
        publisher: Arc<FaultPublisher>,
    ) -> ExecutorParts {
        ExecutorParts {
            boot_source: None,
            kernels_root: base.join("node/kernels"),
            staging: base.join("staging"),
            limits: LIMITS,
            host_arch: crate::installs::host_arch().unwrap(),
            source: crate::installs::test_support::kernel_sources(source),
            releases: super::super::release::ReleaseSources::new(vec![ReleaseContext {
                root: base.join("node/firecracker"),
                source: releases,
            }])
            .unwrap(),
            publisher,
            pins,
        }
    }
    fn request(&self, runtime: bool) -> InstallOperation {
        if runtime {
            self.releases
                .publish("1.16.2", release_archive("1.16.2", 7));
            let pin = release_pin("1.16.2", 7);
            self.pins.pin_release(pin.clone());
            InstallOperation::InstallRelease {
                release: VerifiableRelease {
                    release: pin.release.clone(),
                    architecture: pin.architecture,
                    archive: self.releases.archive(&pin.release, pin.architecture),
                    sha256: pin.archive_sha256,
                },
                digest: pin.digest(),
            }
        } else {
            let kernel = self.source.offer("1.16", "6.1.158", &boot_kernel(256, 7));
            InstallOperation::InstallKernel {
                kernel: kernel.download_request(self.source.pins()[0].sha256),
            }
        }
    }
    fn remove(request: &InstallOperation) -> InstallOperation {
        match request {
            InstallOperation::InstallKernel { kernel } => InstallOperation::UninstallKernel {
                version: kernel.version.clone(),
                digest: kernel.digest,
            },
            InstallOperation::InstallRelease { release, digest } => {
                InstallOperation::UninstallRelease {
                    release: release.release.clone(),
                    digest: *digest,
                }
            }
            _ => panic!("install request required"),
        }
    }
    fn job(&self, request: &InstallOperation) -> Job {
        let job_id = JobId::new();
        let db = self.installer.executor.db();
        pplx_space_common_jobs::db::insert_job(
            &db,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: INSTALL_JOB_KIND,
                payload: &serde_json::to_string(request).unwrap(),
                subject_id: None,
                dedup_key: None,
                max_attempts: 1,
                traceparent: None,
                priority: pplx_space_common_jobs::JobPriority::default(),
            },
        )
        .unwrap();
        pplx_space_common_jobs::db::get_job(&db, &job_id)
            .unwrap()
            .unwrap()
    }
    fn downloads(&self) -> usize {
        self.source.opens() + self.releases.opens()
    }
}

#[tokio::test]
async fn both_kinds_install_retry_and_delete_without_touching_execution() {
    for (runtime, historical) in [(false, false), (true, false), (false, true), (true, true)] {
        let fixture = Fixture::new().await;
        // An existing image's paths remain independent, including flat kernels.
        let execution = fixture.dir.path().join("vendor/firecracker/v1.15.1");
        std::fs::create_dir_all(&execution).unwrap();
        std::fs::write(execution.join("vmlinux.bin"), b"existing image kernel").unwrap();
        std::os::unix::fs::symlink("v1.15.1", execution.parent().unwrap().join("default")).unwrap();
        let request = fixture.request(runtime);
        let job = fixture.job(&request);
        let executor = &fixture.installer.executor;
        executor
            .execute(&job, CancellationToken::new())
            .await
            .unwrap();
        assert!(matches!(
            store::get_job_result(&executor.db(), &job.job_id).unwrap(),
            Some(JobResult::Done {
                outcome: JobOutcome::Installed,
                ..
            })
        ));
        if historical {
            use pplx_space_node_runtime_core::artifact_store::{
                kernel_directory_names, release_directory_names,
            };
            let compact = executor.target(&request.artifact()).unwrap();
            let names = match request.artifact() {
                NodeArtifactRef::Boot(_) => {
                    unreachable!("fixture offers only kernels and releases")
                }
                NodeArtifactRef::Kernel(version, digest) => {
                    kernel_directory_names(version.artifact_version(), digest)
                }
                NodeArtifactRef::Release(release, digest) => {
                    release_directory_names(&release, digest)
                }
            };
            let old = compact.parent().unwrap().join(&names[1]);
            std::fs::rename(&compact, &old).unwrap();
            assert_eq!(executor.target(&request.artifact()).unwrap(), old);
        }
        let repeated = fixture.job(&request);
        executor
            .execute(&repeated, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(fixture.downloads(), 1);
        assert!(matches!(
            store::get_job_result(&executor.db(), &repeated.job_id).unwrap(),
            Some(JobResult::Done {
                outcome: JobOutcome::AlreadyInstalled,
                ..
            })
        ));
        let removal = fixture.job(&Fixture::remove(&request));
        executor
            .execute(&removal, CancellationToken::new())
            .await
            .unwrap();
        let absent = fixture.job(&Fixture::remove(&request));
        executor
            .execute(&absent, CancellationToken::new())
            .await
            .unwrap();
        executor
            .execute(&removal, CancellationToken::new())
            .await
            .unwrap();
        assert!(!executor.target(&request.artifact()).unwrap().exists());
        assert_eq!(
            std::fs::read(execution.join("vmlinux.bin")).unwrap(),
            b"existing image kernel"
        );
        assert_eq!(
            std::fs::read_link(execution.parent().unwrap().join("default")).unwrap(),
            PathBuf::from("v1.15.1")
        );
        assert!(staging_holds_no_job(&fixture.dir.path().join("staging")));
        fixture.installer.shutdown().await;
    }
}

#[tokio::test]
async fn lost_mutation_responses_are_recoverable_for_both_kinds() {
    for runtime in [false, true] {
        for removal in [false, true] {
            let fixture = Fixture::new().await;
            let install = fixture.request(runtime);
            if removal {
                fixture
                    .installer
                    .executor
                    .execute(&fixture.job(&install), CancellationToken::new())
                    .await
                    .unwrap();
            }
            let request = if removal {
                Fixture::remove(&install)
            } else {
                install
            };
            let job = fixture.job(&request);
            fixture.publisher.lose.store(true, Ordering::SeqCst);
            fixture
                .installer
                .executor
                .execute(&job, CancellationToken::new())
                .await
                .unwrap_err();
            assert!(matches!(
                store::get_job_result(&fixture.installer.executor.db(), &job.job_id).unwrap(),
                Some(JobResult::InProgress { .. })
            ));
            // A new executor and DB connection have only durable intent and bytes.
            let restarted = crate::installs::executor::InstallExecutor::new(
                Fixture::parts(
                    fixture.dir.path(),
                    fixture.source.clone(),
                    fixture.releases.clone(),
                    fixture.pins.clone(),
                    fixture.publisher.clone(),
                ),
                Arc::new(Mutex::new(
                    store::open(&fixture.dir.path().join("state"))
                        .await
                        .unwrap(),
                )),
            );
            restarted
                .execute(&job, CancellationToken::new())
                .await
                .unwrap();
            assert!(matches!(
                store::get_job_result(&restarted.db(), &job.job_id).unwrap(),
                Some(JobResult::Done { .. })
            ));
            assert_eq!(
                fixture.downloads(),
                1,
                "recovery does not need the artifact source"
            );
            fixture.installer.shutdown().await;
        }
    }
}

#[tokio::test]
async fn cancellation_never_crosses_durable_mutation_intent() {
    let fixture = Fixture::new().await;
    let request = fixture.request(false);
    let job = fixture.job(&request);
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    fixture
        .installer
        .executor
        .execute(&job, cancelled)
        .await
        .unwrap_err();
    assert!(
        store::get_job_result(&fixture.installer.executor.db(), &job.job_id)
            .unwrap()
            .is_none()
    );
    assert!(
        !fixture
            .installer
            .executor
            .target(&request.artifact())
            .unwrap()
            .exists()
    );
    let digest = fixture.source.pins()[0].sha256;
    store::put_job_result(
        &fixture.installer.executor.db(),
        &job.job_id,
        &JobResult::InProgress {
            phase: JobPhase::Publishing,
            path: fixture
                .installer
                .executor
                .target(&request.artifact())
                .unwrap(),
            digest,
        },
        jiff::Timestamp::now(),
    )
    .unwrap();
    assert!(matches!(
        fixture.installer.cancel(job.job_id).await,
        Err(CancelOperationError::TooLate(_))
    ));
    fixture.installer.shutdown().await;
}

#[tokio::test]
async fn rejected_or_corrupt_downloads_never_publish() {
    for case in ["digest", "size", "format", "banner", "transfer"] {
        let fixture = Fixture::new().await;
        let original = if case == "format" {
            vec![9; 256]
        } else if case == "banner" {
            banner_kernel(256, "6.1.159")
        } else {
            boot_kernel(256, 7)
        };
        let kernel = fixture.source.offer("1.16", "6.1.158", &original);
        let digest = fixture.source.pins()[0].sha256;
        match case {
            "digest" => fixture
                .source
                .serve(&kernel, Body::Bytes(boot_kernel(256, 8))),
            "size" => fixture
                .source
                .serve(&kernel, Body::Bytes(boot_kernel(512, 7))),
            "transfer" => fixture.source.serve(&kernel, Body::FailAfter(vec![0; 64])),
            _ => {}
        }
        let request = InstallOperation::InstallKernel {
            kernel: kernel.download_request(digest),
        };
        fixture
            .installer
            .executor
            .execute(&fixture.job(&request), CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            !fixture
                .installer
                .executor
                .target(&request.artifact())
                .unwrap()
                .exists(),
            "{case}"
        );
        assert!(staging_holds_no_job(&fixture.dir.path().join("staging")));
        fixture.installer.shutdown().await;
    }
}

#[tokio::test]
async fn wrong_release_archives_or_binaries_never_publish() {
    for wrong_archive in [true, false] {
        let fixture = Fixture::new().await;
        let mut pin = release_pin("1.16.2", 7);
        let archive = release_archive("1.16.2", 8);
        if !wrong_archive {
            pin.archive_sha256 = release_pin("1.16.2", 8).archive_sha256;
        }
        fixture.pins.pin_release(pin.clone());
        fixture.releases.publish("1.16.2", archive);
        let request = InstallOperation::InstallRelease {
            release: VerifiableRelease {
                release: pin.release.clone(),
                architecture: pin.architecture,
                archive: fixture.releases.archive(&pin.release, pin.architecture),
                sha256: pin.archive_sha256,
            },
            digest: pin.digest(),
        };
        fixture
            .installer
            .executor
            .execute(&fixture.job(&request), CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            !fixture
                .dir
                .path()
                .join("node/firecracker")
                .join(release_directory(&pin.release))
                .exists()
        );
        fixture.installer.shutdown().await;
    }
}

#[tokio::test]
async fn cleanup_failure_preserves_the_publication_result() {
    for runtime in [false, true] {
        for refuse in [false, true] {
            let fixture = Fixture::new().await;
            fixture
                .publisher
                .break_cleanup
                .store(true, Ordering::SeqCst);
            fixture.publisher.refuse.store(refuse, Ordering::SeqCst);
            let request = fixture.request(runtime);
            let job = fixture.job(&request);
            let executor = &fixture.installer.executor;
            let result = executor.execute(&job, CancellationToken::new()).await;
            // The failed cleanup is real for both the success and refusal paths.
            executor.cleanup_cancelled(&job).await.unwrap_err();
            if refuse {
                assert_eq!(
                    result.unwrap_err().kind(),
                    pplx_space_common_jobs::JobFailureKind::Permanent
                );
                assert!(!executor.target(&request.artifact()).unwrap().exists());
            } else {
                result.unwrap();
                assert!(matches!(
                    store::get_job_result(&executor.db(), &job.job_id).unwrap(),
                    Some(JobResult::Done {
                        outcome: JobOutcome::Installed,
                        ..
                    })
                ));
                assert!(executor.target(&request.artifact()).unwrap().exists());
            }
        }
    }
}

#[tokio::test]
async fn changed_release_pins_fail_before_downloading() {
    for archive_only in [false, true] {
        let fixture = Fixture::new().await;
        let request = fixture.request(true);
        let mut changed = release_pin("1.16.2", 8);
        if archive_only {
            let archive_sha256 = changed.archive_sha256;
            changed = release_pin("1.16.2", 7);
            changed.archive_sha256 = archive_sha256;
        }
        fixture.pins.pin_release(changed);
        let result = fixture
            .installer
            .executor
            .execute(&fixture.job(&request), CancellationToken::new())
            .await;
        assert!(
            matches!(result, Err(pplx_space_common_jobs::JobError::Permanent(_))),
            "{result:?}"
        );
        assert_eq!(
            fixture.downloads(),
            0,
            "changed pins must not contact the source"
        );
        assert!(
            !fixture
                .installer
                .executor
                .target(&request.artifact())
                .unwrap()
                .exists()
        );
        fixture.installer.shutdown().await;
    }
}
