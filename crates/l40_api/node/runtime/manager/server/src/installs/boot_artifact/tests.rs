use super::*;
use crate::installs::{
    executor::ExecutorParts,
    operations::{INSTALL_JOB_KIND, InstallLimits, InstallOperation, Installer},
    publisher::{ArtifactPublisher, NodeArtifactRef, PinSource, Published, PublisherError},
    store::{self, JobOutcome, JobResult},
    test_support::LocalPublisher,
};
use pplx_space_common_core::types::{ArtifactComponentKind, ComponentIdentity, Digest, JobId};
use pplx_space_common_jobs::{Job, JobExecutor as _};
use pplx_space_node_runtime_core::{
    artifact_store::StagedName,
    catalog::{CatalogError, RuntimeCatalog},
};
use sha2::{Digest as _, Sha256};
use std::os::unix::fs::PermissionsExt as _;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

struct Pins(PathBuf);
impl PinSource for Pins {
    fn catalog(&self) -> Result<RuntimeCatalog, CatalogError> {
        RuntimeCatalog::load_owned_by(&self.0, nix::unistd::geteuid().as_raw())
    }
}
struct Publisher {
    inner: Arc<LocalPublisher>,
    lose: AtomicBool,
}
#[async_trait::async_trait]
impl ArtifactPublisher for Publisher {
    async fn publish(
        &self,
        artifact: &NodeArtifactRef,
        staged: &StagedName,
    ) -> Result<Published, PublisherError> {
        let result = self.inner.publish(artifact, staged).await?;
        if self.lose.swap(false, Ordering::SeqCst) {
            return Err(PublisherError::Unavailable("lost publication reply".into()));
        }
        Ok(result)
    }
    async fn remove(&self, artifact: &NodeArtifactRef) -> Result<bool, PublisherError> {
        self.inner.remove(artifact).await
    }
}
struct Fixture {
    dir: tempfile::TempDir,
    installer: Installer,
    pin: PinnedBootArtifact,
    source: PathBuf,
    publisher: Arc<Publisher>,
}
impl Fixture {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        for name in [
            "state",
            "catalog",
            "node/kernels",
            "node/releases",
            "node/boot-artifacts",
            "source",
        ] {
            std::fs::create_dir_all(dir.path().join(name)).unwrap();
            std::fs::set_permissions(
                dir.path().join(name),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
        }
        let architecture = crate::installs::host_arch().unwrap();
        let pin = PinnedBootArtifact::new(
            ComponentIdentity {
                kind: ArtifactComponentKind::Firmware,
                version: "1.0-test".parse().unwrap(),
                digest: Digest::sha256(Sha256::digest(b"boot bytes").into()),
            },
            architecture,
            10,
        )
        .unwrap();
        let pins = Arc::new(Pins(dir.path().join("catalog")));
        std::fs::write(
            pins.0.join("boot.json"),
            serde_json::to_vec(&serde_json::json!({"schema": 1, "boot_artifacts": [pin]})).unwrap(),
        )
        .unwrap();
        std::fs::set_permissions(
            pins.0.join("boot.json"),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        let source = dir
            .path()
            .join("source")
            .join(boot_artifact_directory(pin.identity()))
            .join(BOOT_ARTIFACT_IMAGE);
        std::fs::create_dir_all(source.parent().unwrap()).unwrap();
        std::fs::write(&source, b"boot bytes").unwrap();
        let publisher = Arc::new(Publisher {
            inner: LocalPublisher::new(
                &dir.path().join("node/releases"),
                &dir.path().join("node/kernels"),
                &dir.path().join("staging"),
                pins.clone(),
            ),
            lose: AtomicBool::new(false),
        });
        let installer = Installer::open(
            ExecutorParts {
                kernels_root: dir.path().join("node/kernels"),
                boot_source: Some(dir.path().join("source")),
                staging: dir.path().join("staging"),
                limits: InstallLimits {
                    max_bytes: 1024,
                    download_idle_timeout: Duration::from_secs(1),
                    max_attempts: 1,
                },
                host_arch: architecture,
                source: crate::installs::source::KernelSources::default(),
                releases: crate::installs::release::ReleaseSources::new(Vec::new()).unwrap(),
                publisher: publisher.clone(),
                pins,
            },
            &dir.path().join("state"),
        )
        .await
        .unwrap();
        Self {
            dir,
            installer,
            pin,
            source,
            publisher,
        }
    }
    fn job(&self, request: InstallOperation) -> Job {
        let job_id = JobId::new();
        let db = self.installer.executor.db();
        pplx_space_common_jobs::db::insert_job(
            &db,
            pplx_space_common_jobs::db::InsertJob {
                job_id: &job_id,
                kind: INSTALL_JOB_KIND,
                payload: &serde_json::to_string(&request).unwrap(),
                subject_id: None,
                dedup_key: None,
                max_attempts: 1,
                traceparent: None,
                priority: Default::default(),
            },
        )
        .unwrap();
        pplx_space_common_jobs::db::get_job(&db, &job_id)
            .unwrap()
            .unwrap()
    }
    fn install_job(&self) -> Job {
        self.job(InstallOperation::InstallBootArtifact {
            pin: self.pin.clone(),
        })
    }
}

#[tokio::test]
async fn auxiliary_install_replays_lost_publication_without_a_source_and_removes() {
    let fixture = Fixture::new().await;
    let executor = &fixture.installer.executor;
    let job = fixture.install_job();
    fixture.publisher.lose.store(true, Ordering::SeqCst);
    executor
        .execute(&job, CancellationToken::new())
        .await
        .unwrap_err();
    assert!(matches!(
        store::get_job_result(&executor.db(), &job.job_id).unwrap(),
        Some(JobResult::InProgress { .. })
    ));
    std::fs::remove_file(&fixture.source).unwrap();
    executor
        .execute(&job, CancellationToken::new())
        .await
        .unwrap();
    let target = executor
        .target(&NodeArtifactRef::Boot(fixture.pin.identity().clone()))
        .unwrap();
    assert_eq!(
        std::fs::read(target.join(BOOT_ARTIFACT_IMAGE)).unwrap(),
        b"boot bytes"
    );
    assert!(matches!(
        store::get_job_result(&executor.db(), &job.job_id).unwrap(),
        Some(JobResult::Done {
            outcome: JobOutcome::AlreadyInstalled,
            ..
        })
    ));
    let remove = fixture.job(InstallOperation::UninstallBootArtifact {
        identity: fixture.pin.identity().clone(),
    });
    executor
        .execute(&remove, CancellationToken::new())
        .await
        .unwrap();
    assert!(!target.exists());
    assert!(fixture.dir.path().join("node/kernels").is_dir());
    fixture.installer.shutdown().await;
}

#[tokio::test]
async fn auxiliary_install_refuses_changed_pins_corrupt_bytes_and_symlinks_before_publication() {
    for fault in ["digest", "size", "symlink", "pin"] {
        let fixture = Fixture::new().await;
        match fault {
            "digest" => std::fs::write(&fixture.source, b"bad! bytes").unwrap(),
            "size" => std::fs::write(&fixture.source, b"short").unwrap(),
            "symlink" => {
                let other = fixture.dir.path().join("other");
                std::fs::rename(&fixture.source, &other).unwrap();
                std::os::unix::fs::symlink(other, &fixture.source).unwrap();
            }
            "pin" => std::fs::remove_file(fixture.dir.path().join("catalog/boot.json")).unwrap(),
            _ => unreachable!(),
        }
        let executor = &fixture.installer.executor;
        let job = fixture.install_job();
        executor
            .execute(&job, CancellationToken::new())
            .await
            .unwrap_err();
        assert!(
            store::get_job_result(&executor.db(), &job.job_id)
                .unwrap()
                .is_none(),
            "{fault}"
        );
        assert!(
            !executor
                .target(&NodeArtifactRef::Boot(fixture.pin.identity().clone()))
                .unwrap()
                .exists()
        );
        fixture.installer.shutdown().await;
    }
}
