use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::ExitStatusExt as _;

use rstest::{fixture, rstest};

use super::*;

struct OciFixture {
    _tempdir: tempfile::TempDir,
    request: OciMountRequest,
    arch: Arch,
}

#[fixture]
fn oci_fixture() -> OciFixture {
    let tempdir = tempfile::tempdir().expect("create OCI fixture");
    OciFixture {
        request: OciMountRequest {
            cache_dir: tempdir.path().join("cache"),
            reference: "docker.io/library/ubuntu:24.04"
                .parse()
                .expect("valid OCI reference"),
        },
        arch: Arch::X86_64,
        _tempdir: tempdir,
    }
}

impl OciFixture {
    fn pull_command(&self) -> OciCommand {
        OciCommand::PullImage {
            reference: self.request.reference.clone(),
            arch: self.arch,
            hosts_dir: None,
        }
    }

    fn chain_id(&self) -> Digest {
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
            .parse()
            .expect("valid chain ID")
    }

    fn snapshot_key(&self, mount_path: &Path) -> ContainerdSnapshotKey {
        snapshot_key(mount_path).expect("snapshot key")
    }

    fn owned_reference(&self, mount_path: &Path) -> OciReference {
        owned_image_reference(mount_path).expect("owned image reference")
    }

    fn tag_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::TagImage {
            source_reference: self.request.reference.clone(),
            owned_reference: self.owned_reference(mount_path),
        }
    }

    fn create_view_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::CreateSnapshotView {
            snapshot_key: self.snapshot_key(mount_path),
            parent: self.chain_id(),
            mount_path: mount_path.to_path_buf(),
        }
    }

    fn mount_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::MountSnapshot {
            source: PathBuf::from(format!("{NATIVE_SNAPSHOT_PATH_PREFIX}fixture")),
            mount_path: mount_path.to_path_buf(),
        }
    }

    fn unmount_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::UnmountSnapshot {
            mount_path: mount_path.to_path_buf(),
        }
    }

    fn remove_view_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::RemoveSnapshotView {
            snapshot_key: self.snapshot_key(mount_path),
        }
    }

    fn remove_image_command(&self) -> OciCommand {
        OciCommand::RemoveImage {
            reference: self.request.reference.clone(),
        }
    }

    fn remove_owned_image_command(&self, mount_path: &Path) -> OciCommand {
        OciCommand::RemoveImage {
            reference: self.owned_reference(mount_path),
        }
    }
}

#[derive(Default)]
struct RecordingRunner {
    commands: Mutex<Vec<OciCommand>>,
    fail_on: Option<fn(&OciCommand) -> bool>,
    failures_remaining: AtomicUsize,
    cleanup_delay: Duration,
    mount_barrier: Option<Arc<tokio::sync::Barrier>>,
    blocked_operation: Option<(OciOperation, Arc<tokio::sync::Barrier>)>,
    snapshot_missing_on_remove: bool,
}

#[derive(Debug)]
struct ObservedEcrAuth {
    command_args: Vec<OsString>,
    contents: String,
    mode: u32,
}

#[derive(Default)]
struct EcrAuthObservingRunner {
    inner: RecordingRunner,
    observed: Mutex<Option<ObservedEcrAuth>>,
}

struct SequenceStorageProbe {
    statuses: Mutex<VecDeque<pplx_space_common_core::host::disk::DiskStatus>>,
}

const FIXTURE_MANIFEST_DIGEST: &str =
    "sha256:2222222222222222222222222222222222222222222222222222222222222222";

struct FixtureManifestSource;

#[async_trait]
impl registry::ManifestSource for FixtureManifestSource {
    async fn fetch(
        &self,
        _repository: &str,
        _manifest: &str,
    ) -> Result<registry::ManifestPayload, registry::RegistryError> {
        Ok(registry::ManifestPayload {
            digest: FIXTURE_MANIFEST_DIGEST
                .parse()
                .expect("valid manifest digest"),
            body: br#"{
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "config": {
                    "mediaType": "application/vnd.oci.image.config.v1+json",
                    "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "size": 1
                },
                "layers": []
            }"#
            .to_vec(),
        })
    }
}

impl SequenceStorageProbe {
    fn new(available_bytes: impl IntoIterator<Item = u64>) -> Self {
        Self {
            statuses: Mutex::new(
                available_bytes
                    .into_iter()
                    .map(
                        |available_bytes| pplx_space_common_core::host::disk::DiskStatus {
                            total_bytes: 1_000,
                            free_bytes: available_bytes,
                            available_bytes,
                        },
                    )
                    .collect(),
            ),
        }
    }
}

impl StorageProbe for SequenceStorageProbe {
    fn status(&self, _root: &Path) -> Option<pplx_space_common_core::host::disk::DiskStatus> {
        self.statuses
            .lock()
            .expect("storage status lock")
            .pop_front()
    }
}

fn storage_config(max_image_bytes: u64, min_available_bytes: u64) -> OciStorageConfig {
    OciStorageConfig {
        root: PathBuf::from("/test/containerd"),
        max_image_bytes,
        min_available_bytes,
        poll_interval: Duration::from_millis(1),
    }
}

impl RecordingRunner {
    fn failing(predicate: fn(&OciCommand) -> bool) -> Self {
        Self {
            fail_on: Some(predicate),
            failures_remaining: AtomicUsize::new(usize::MAX),
            ..Self::default()
        }
    }

    fn failing_once(predicate: fn(&OciCommand) -> bool) -> Self {
        Self::failing_times(predicate, 1)
    }

    fn failing_times(predicate: fn(&OciCommand) -> bool, count: usize) -> Self {
        Self {
            fail_on: Some(predicate),
            failures_remaining: AtomicUsize::new(count),
            ..Self::default()
        }
    }

    fn slow_cleanup(delay: Duration) -> Self {
        Self {
            cleanup_delay: delay,
            ..Self::default()
        }
    }

    fn synchronize_mounts(count: usize) -> Self {
        Self {
            mount_barrier: Some(Arc::new(tokio::sync::Barrier::new(count))),
            ..Self::default()
        }
    }

    fn block_after(operation: OciOperation, barrier: Arc<tokio::sync::Barrier>) -> Self {
        Self {
            blocked_operation: Some((operation, barrier)),
            ..Self::default()
        }
    }

    fn missing_snapshot_on_remove() -> Self {
        Self {
            snapshot_missing_on_remove: true,
            ..Self::default()
        }
    }

    fn commands(&self) -> Vec<OciCommand> {
        self.commands.lock().expect("command lock").clone()
    }
}

#[async_trait]
impl CommandRunner for RecordingRunner {
    async fn run(&self, command: &OciCommand) -> Result<CommandResult, CommandRunError> {
        let failed = self.record(command);
        if let Some((operation, barrier)) = &self.blocked_operation
            && command.operation() == *operation
        {
            barrier.wait().await;
            std::future::pending::<()>().await;
        }
        if matches!(command, OciCommand::MountSnapshot { .. })
            && let Some(barrier) = &self.mount_barrier
        {
            barrier.wait().await;
        }
        if matches!(
            command,
            OciCommand::UnmountSnapshot { .. } | OciCommand::RemoveSnapshotView { .. }
        ) {
            tokio::time::sleep(self.cleanup_delay).await;
        }
        if failed {
            return Err(CommandRunError::Exit {
                code: Some(1),
                stderr: "fixture failure".to_string(),
            });
        }
        if self.snapshot_missing_on_remove
            && let OciCommand::RemoveSnapshotView { snapshot_key, .. } = command
        {
            return Err(CommandRunError::SnapshotViewNotFound {
                snapshot_key: snapshot_key.clone(),
            });
        }
        match command {
            OciCommand::PullImage { .. } => Ok(CommandResult {
                stdout: format!("{IMAGE_CHAIN_ID_PREFIX}{}\n", self.chain_id()).into_bytes(),
            }),
            OciCommand::CreateSnapshotView { mount_path, .. } => Ok(CommandResult {
                stdout: format!(
                    "mount -t bind {NATIVE_SNAPSHOT_PATH_PREFIX}fixture {} -o rbind,ro\n",
                    mount_path.display()
                )
                .into_bytes(),
            }),
            _ => Ok(CommandResult::default()),
        }
    }
}

#[async_trait]
impl CommandRunner for EcrAuthObservingRunner {
    async fn run(&self, command: &OciCommand) -> Result<CommandResult, CommandRunError> {
        if let OciCommand::PullImage {
            reference,
            hosts_dir: Some(hosts_dir),
            ..
        } = command
        {
            let hosts_path = hosts_dir.join(reference.registry()).join("hosts.toml");
            let contents = fs::read_to_string(&hosts_path).expect("read registry auth fixture");
            #[cfg(unix)]
            let mode = {
                use std::os::unix::fs::PermissionsExt as _;
                fs::metadata(&hosts_path)
                    .expect("stat registry auth fixture")
                    .permissions()
                    .mode()
                    & 0o777
            };
            #[cfg(not(unix))]
            let mode = 0o600;
            *self.observed.lock().expect("ECR auth lock") = Some(ObservedEcrAuth {
                command_args: command.args(),
                contents,
                mode,
            });
        }
        self.inner.run(command).await
    }
}

impl RecordingRunner {
    fn chain_id(&self) -> &str {
        "sha256:1111111111111111111111111111111111111111111111111111111111111111"
    }

    fn record(&self, command: &OciCommand) -> bool {
        self.commands
            .lock()
            .expect("command lock")
            .push(command.clone());
        self.fail_on.is_some_and(|predicate| predicate(command))
            && self
                .failures_remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
    }
}

#[rstest]
fn caller_controlled_references_do_not_inherit_ctr_output(oci_fixture: OciFixture) {
    let mount_path = oci_fixture.request.cache_dir.join("mount");

    assert_eq!(oci_fixture.pull_command().output(), CommandOutput::Capture);
    assert_eq!(
        oci_fixture.tag_command(&mount_path).output(),
        CommandOutput::Quiet
    );
    assert_eq!(
        oci_fixture.create_view_command(&mount_path).output(),
        CommandOutput::Capture
    );
    assert_eq!(
        oci_fixture.mount_command(&mount_path).output(),
        CommandOutput::Quiet
    );
    assert_eq!(
        oci_fixture.remove_view_command(&mount_path).output(),
        CommandOutput::Quiet
    );
    assert_eq!(
        oci_fixture.remove_image_command().output(),
        CommandOutput::Quiet
    );
}

#[rstest]
fn pulls_only_inspected_platform_metadata(oci_fixture: OciFixture) {
    let args = oci_fixture.pull_command().args();

    assert!(args.contains(&OsString::from("--skip-metadata")));
}

#[rstest]
#[tokio::test]
async fn resolves_the_manifest_before_containerd_pull(oci_fixture: OciFixture) {
    let digest: Digest = FIXTURE_MANIFEST_DIGEST
        .parse()
        .expect("valid manifest digest");
    let source = FixtureManifestSource;
    let runner = Arc::new(RecordingRunner::default());
    let expected_reference = oci_fixture
        .request
        .reference
        .clone_with_digest(digest.to_string());

    let rootfs = mount_with_manifest_source_and_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        &source,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");

    assert!(runner.commands().iter().any(|command| {
        matches!(
            command,
            OciCommand::PullImage { reference, .. } if reference == &expected_reference
        )
    }));
    rootfs.unmount().await.expect("unmount OCI rootfs");
}

#[rstest]
#[tokio::test]
async fn ecr_pull_uses_a_short_lived_credential_file(mut oci_fixture: OciFixture) {
    const REGISTRY: &str = "111111111111.dkr.ecr.us-east-1.amazonaws.com";
    const TOKEN: &str = "fixture-ecr-token";
    oci_fixture.request.reference =
        format!("{REGISTRY}/example/browser-runtime@{FIXTURE_MANIFEST_DIGEST}")
            .parse()
            .expect("valid ECR fixture reference");
    let auth_root = oci_fixture._tempdir.path().join("ecr-auth");
    fs::create_dir(&auth_root).expect("create memory-volume fixture");
    let auth = registry::EcrPullAuth::new(REGISTRY.to_string(), TOKEN.to_string());
    let runner = Arc::new(EcrAuthObservingRunner::default());

    let rootfs = mount_with_registry_dependencies(
        oci_fixture.request,
        oci_fixture.arch,
        Some(&auth),
        &auth_root,
        OciMountDependencies {
            runner: runner.clone(),
            storage_config: storage_config(1, 1),
            storage_probe: Arc::new(OpenStorageProbe),
            pull_lock: Arc::new(AsyncMutex::new(())),
        },
    )
    .await
    .expect("mount private ECR rootfs");

    let observed = runner
        .observed
        .lock()
        .expect("ECR auth lock")
        .take()
        .expect("containerd pull should observe registry auth");
    assert_eq!(observed.mode, 0o600);
    assert!(observed.contents.contains(&format!("https://{REGISTRY}")));
    assert!(observed.contents.contains(&format!("Basic {TOKEN}")));
    assert!(
        observed
            .command_args
            .contains(&OsString::from("--hosts-dir"))
    );
    assert!(
        observed
            .command_args
            .iter()
            .all(|arg| !arg.to_string_lossy().contains(TOKEN)),
        "registry token reached the containerd process arguments"
    );
    assert!(
        fs::read_dir(&auth_root)
            .expect("read registry auth root")
            .next()
            .is_none(),
        "registry credential directory survived the pull"
    );
    rootfs.unmount().await.expect("unmount private ECR rootfs");
}

#[rstest]
fn snapshot_view_returns_its_mount_without_a_second_lookup(oci_fixture: OciFixture) {
    let mount_path = oci_fixture.request.cache_dir.join("mount");
    let command = oci_fixture.create_view_command(&mount_path);
    let expected: Vec<OsString> = vec![
        "--namespace".into(),
        CONTAINERD_NAMESPACE.into(),
        "snapshots".into(),
        "--snapshotter".into(),
        NATIVE_SNAPSHOTTER.into(),
        "view".into(),
        "--target".into(),
        mount_path.as_os_str().into(),
        oci_fixture.snapshot_key(&mount_path).as_str().into(),
        oci_fixture.chain_id().to_string().into(),
    ];

    assert_eq!(command.output(), CommandOutput::Capture);
    assert_eq!(command.args(), expected);
}

#[cfg(unix)]
#[rstest]
fn failed_pull_redacts_the_caller_controlled_reference(oci_fixture: OciFixture) {
    let reference = oci_fixture.request.reference.to_string();
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(1 << 8),
        stdout: Vec::new(),
        stderr: format!("failed to resolve reference {reference}").into_bytes(),
    };

    let error = record_command_output(&oci_fixture.pull_command(), Ok(output))
        .expect_err("pull should fail");
    let message = error.to_string();

    assert!(!message.contains(&reference));
    assert!(message.contains("redacted"));
}

#[cfg(unix)]
#[rstest]
fn cleanup_accepts_an_already_unmounted_snapshot(oci_fixture: OciFixture) {
    let mount_path = oci_fixture.request.cache_dir.join("mount");
    let output = std::process::Output {
        status: std::process::ExitStatus::from_raw(32 << 8),
        stdout: Vec::new(),
        stderr: format!("umount: {}: not mounted.", mount_path.display()).into_bytes(),
    };

    let result = record_command_output(&oci_fixture.unmount_command(&mount_path), Ok(output));

    assert_eq!(
        result.expect("already unmounted is successful cleanup"),
        CommandResult::default()
    );
}

#[rstest]
#[tokio::test]
async fn failed_pull_removes_a_partial_source_image_record(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::failing(|command| {
        matches!(command, OciCommand::PullImage { .. })
    }));

    let error = match mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    {
        Ok(_) => panic!("pull should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        OciBuildError::Command {
            operation: OciOperation::PullImage,
            ..
        }
    ));
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.remove_image_command(),
        ]
    );
}

#[rstest]
#[tokio::test]
async fn explicit_cleanup_releases_the_build_owned_image(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::default());
    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    rootfs.unmount().await.expect("unmount OCI rootfs");

    assert_eq!(
        runner.commands().last(),
        Some(&oci_fixture.remove_owned_image_command(&mount_path)),
        "cleanup left the build-owned image record in containerd"
    );
}

#[rstest]
#[tokio::test]
async fn mounted_rootfs_does_not_retain_the_shared_source_image(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::default());

    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let commands = runner.commands();
    let tag_index = commands
        .iter()
        .position(|command| matches!(command, OciCommand::TagImage { .. }))
        .expect("build-owned image tag");
    let source_delete_index = commands
        .iter()
        .position(|command| command == &oci_fixture.remove_image_command())
        .expect("shared source image deletion");
    let snapshot_index = commands
        .iter()
        .position(|command| matches!(command, OciCommand::CreateSnapshotView { .. }))
        .expect("snapshot view");

    assert!(tag_index < source_delete_index);
    assert!(source_delete_index < snapshot_index);

    rootfs.unmount().await.expect("unmount OCI rootfs");
}

#[rstest]
#[tokio::test]
async fn pull_is_rejected_before_containerd_loses_reserved_headroom(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::default());
    let storage = Arc::new(SequenceStorageProbe::new([59]));

    let error = match mount_with_dependencies(
        oci_fixture.request,
        oci_fixture.arch,
        runner.clone(),
        storage_config(40, 20),
        storage,
        Arc::new(AsyncMutex::new(())),
    )
    .await
    {
        Ok(_) => panic!("pull should be rejected"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        OciBuildError::Storage(OciStorageError::Admission(
            StorageAdmissionError::AdmissionRejected {
                available_bytes: 59,
                required_available_bytes: 60,
            }
        ))
    ));
    assert_eq!(runner.commands(), vec![OciCommand::CheckContainerd]);
}

#[rstest]
#[tokio::test]
async fn pull_is_stopped_when_it_crosses_its_containerd_budget(oci_fixture: OciFixture) {
    let pull_started = Arc::new(tokio::sync::Barrier::new(2));
    let runner = Arc::new(RecordingRunner::block_after(
        OciOperation::PullImage,
        pull_started.clone(),
    ));
    let storage = Arc::new(SequenceStorageProbe::new([100, 59]));
    let task = tokio::spawn(mount_with_dependencies(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
        storage_config(40, 20),
        storage,
        Arc::new(AsyncMutex::new(())),
    ));

    pull_started.wait().await;
    let result = task.await.expect("mount task");
    let error = match result {
        Ok(_) => panic!("pull should cross its storage budget"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        OciBuildError::Storage(OciStorageError::ImageBudgetExceeded {
            consumed_bytes: 41,
            max_image_bytes: 40,
        })
    ));
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.remove_image_command(),
        ]
    );
}

#[rstest]
#[tokio::test]
async fn cancelled_pull_removes_partial_source_and_releases_admission(oci_fixture: OciFixture) {
    let pull_started = Arc::new(tokio::sync::Barrier::new(2));
    let first_runner = Arc::new(RecordingRunner::block_after(
        OciOperation::PullImage,
        pull_started.clone(),
    ));
    let pull_lock = Arc::new(AsyncMutex::new(()));
    let first = tokio::spawn(mount_with_dependencies(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        first_runner.clone(),
        storage_config(1, 1),
        Arc::new(OpenStorageProbe),
        pull_lock.clone(),
    ));

    pull_started.wait().await;
    first.abort();
    let error = match first.await {
        Ok(_) => panic!("pull should be cancelled"),
        Err(error) => error,
    };
    assert!(error.is_cancelled());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !first_runner
            .commands()
            .contains(&oci_fixture.remove_image_command())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled pull should remove its partial source");

    let second_runner = Arc::new(RecordingRunner::default());
    let rootfs = tokio::time::timeout(
        Duration::from_secs(1),
        mount_with_dependencies(
            oci_fixture.request,
            oci_fixture.arch,
            second_runner,
            storage_config(1, 1),
            Arc::new(OpenStorageProbe),
            pull_lock,
        ),
    )
    .await
    .expect("cancelled pull should release storage admission")
    .expect("second OCI mount");
    rootfs.unmount().await.expect("unmount second OCI rootfs");
}

#[rstest]
#[tokio::test]
async fn mounted_rootfs_lives_until_explicit_cleanup(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::default());

    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    assert!(mount_path.is_dir());
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.tag_command(&mount_path),
            oci_fixture.remove_image_command(),
            oci_fixture.create_view_command(&mount_path),
            oci_fixture.mount_command(&mount_path),
        ]
    );

    rootfs.unmount().await.expect("unmount OCI rootfs");

    assert!(!mount_path.exists());
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.tag_command(&mount_path),
            oci_fixture.remove_image_command(),
            oci_fixture.create_view_command(&mount_path),
            oci_fixture.mount_command(&mount_path),
            oci_fixture.unmount_command(&mount_path),
            oci_fixture.remove_view_command(&mount_path),
            oci_fixture.remove_owned_image_command(&mount_path),
        ]
    );
}

#[rstest]
#[tokio::test]
async fn mounts_rootfs_when_cache_path_contains_spaces(mut oci_fixture: OciFixture) {
    oci_fixture.request.cache_dir = oci_fixture._tempdir.path().join("cache path with spaces");
    let runner = Arc::new(RecordingRunner::default());

    let rootfs = mount_with_runner(oci_fixture.request.clone(), oci_fixture.arch, runner)
        .await
        .expect("mount OCI rootfs");

    assert!(rootfs.path().starts_with(&oci_fixture.request.cache_dir));
    rootfs.unmount().await.expect("unmount OCI rootfs");
}

#[rstest]
#[tokio::test]
async fn dropped_rootfs_releases_its_containerd_mount(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::slow_cleanup(Duration::from_millis(250)));
    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    let drop_started = Instant::now();
    drop(rootfs);

    assert!(
        drop_started.elapsed() < Duration::from_millis(100),
        "fallback cleanup blocked the async caller"
    );

    let final_cleanup_command = oci_fixture.remove_owned_image_command(&mount_path);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while runner.commands().last() != Some(&final_cleanup_command) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropped rootfs should clean up promptly");
    assert!(!mount_path.exists());
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.tag_command(&mount_path),
            oci_fixture.remove_image_command(),
            oci_fixture.create_view_command(&mount_path),
            oci_fixture.mount_command(&mount_path),
            oci_fixture.unmount_command(&mount_path),
            oci_fixture.remove_view_command(&mount_path),
            oci_fixture.remove_owned_image_command(&mount_path),
        ]
    );
}

#[rstest]
#[tokio::test]
async fn dropped_rootfs_retries_transient_cleanup_failures(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::failing_once(|command| {
        matches!(command, OciCommand::UnmountSnapshot { .. })
    }));
    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    drop(rootfs);

    let final_cleanup_command = oci_fixture.remove_owned_image_command(&mount_path);
    tokio::time::timeout(Duration::from_secs(1), async {
        while runner.commands().last() != Some(&final_cleanup_command) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fallback cleanup should retry the transient failure");
    assert!(!mount_path.exists());
    assert_eq!(
        runner
            .commands()
            .iter()
            .filter(|command| matches!(command, OciCommand::UnmountSnapshot { .. }))
            .count(),
        2
    );
}

#[rstest]
#[case(OciOperation::CreateSnapshotView)]
#[case(OciOperation::MountSnapshot)]
#[tokio::test]
async fn cancelled_mount_releases_in_progress_containerd_resources(
    oci_fixture: OciFixture,
    #[case] cancelled_operation: OciOperation,
) {
    let operation_barrier = Arc::new(tokio::sync::Barrier::new(2));
    let runner = Arc::new(RecordingRunner::block_after(
        cancelled_operation,
        operation_barrier.clone(),
    ));
    let task = tokio::spawn(mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    ));
    operation_barrier.wait().await;
    let mount_path = runner
        .commands()
        .into_iter()
        .find_map(|command| match command {
            OciCommand::CreateSnapshotView { mount_path, .. } => Some(mount_path),
            _ => None,
        })
        .expect("created snapshot view");

    task.abort();
    let error = match task.await {
        Ok(_) => panic!("mount task should be cancelled"),
        Err(error) => error,
    };

    assert!(error.is_cancelled());
    let final_cleanup_command = oci_fixture.remove_owned_image_command(&mount_path);
    tokio::time::timeout(Duration::from_secs(1), async {
        while runner.commands().last() != Some(&final_cleanup_command) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled mount should clean up promptly");
    assert!(!mount_path.exists());
    assert!(
        runner
            .commands()
            .contains(&oci_fixture.remove_view_command(&mount_path)),
        "cancelled snapshot operation left its view behind"
    );
    if cancelled_operation == OciOperation::MountSnapshot {
        assert!(
            runner
                .commands()
                .contains(&oci_fixture.unmount_command(&mount_path)),
            "cancelled mount operation left its bind mount behind"
        );
    }
}

#[rstest]
fn runtime_shutdown_still_releases_dropped_rootfs(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create test runtime");
    let mount_path = runtime.block_on(async {
        let rootfs = mount_with_runner(oci_fixture.request, oci_fixture.arch, runner.clone())
            .await
            .expect("mount OCI rootfs");
        let mount_path = rootfs.path().to_path_buf();
        tokio::spawn(async move {
            std::future::pending::<()>().await;
            drop(rootfs);
        });
        tokio::task::yield_now().await;
        mount_path
    });

    drop(runtime);

    let deadline = Instant::now() + Duration::from_secs(1);
    while mount_path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(runner.commands().len(), 9);
    assert!(!mount_path.exists());
}

#[rstest]
#[tokio::test]
async fn failed_explicit_cleanup_retries_the_remaining_resources(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::failing_once(|command| {
        matches!(command, OciCommand::UnmountSnapshot { .. })
    }));
    let rootfs = mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    let error = rootfs
        .unmount()
        .await
        .expect_err("first unmount should fail");

    assert!(matches!(
        error,
        OciBuildError::Command {
            operation: OciOperation::UnmountSnapshot,
            ..
        }
    ));
    tokio::time::timeout(Duration::from_secs(1), async {
        while mount_path.exists() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fallback cleanup should retry the failed unmount");
    assert!(!mount_path.exists());
}

#[rstest]
#[tokio::test]
async fn cleanup_succeeds_when_containerd_already_removed_the_snapshot(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::missing_snapshot_on_remove());
    let rootfs = mount_with_runner(oci_fixture.request.clone(), oci_fixture.arch, runner)
        .await
        .expect("mount OCI rootfs");
    let mount_path = rootfs.path().to_path_buf();

    rootfs.unmount().await.expect("unmount OCI rootfs");

    assert!(!mount_path.exists());
}

#[rstest]
#[tokio::test]
async fn concurrent_mounts_of_one_source_clean_distinct_image_records(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::synchronize_mounts(2));

    let (first, second) = tokio::join!(
        mount_with_runner(
            oci_fixture.request.clone(),
            oci_fixture.arch,
            runner.clone(),
        ),
        mount_with_runner(
            oci_fixture.request.clone(),
            oci_fixture.arch,
            runner.clone(),
        ),
    );
    let first = first.expect("mount first OCI rootfs");
    let second = second.expect("mount second OCI rootfs");

    let snapshot_keys = runner
        .commands()
        .into_iter()
        .filter_map(|command| match command {
            OciCommand::CreateSnapshotView { snapshot_key, .. } => Some(snapshot_key),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(snapshot_keys.len(), 2);
    assert_ne!(snapshot_keys[0], snapshot_keys[1]);

    first.unmount().await.expect("unmount first rootfs");
    second.unmount().await.expect("unmount second rootfs");

    let removed_references = runner
        .commands()
        .into_iter()
        .filter_map(|command| match command {
            OciCommand::RemoveImage { reference } => Some(reference),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        removed_references
            .iter()
            .filter(|reference| *reference == &oci_fixture.request.reference)
            .count(),
        2
    );
    let owned_references = removed_references
        .into_iter()
        .filter(|reference| reference != &oci_fixture.request.reference)
        .collect::<Vec<_>>();
    assert_eq!(owned_references.len(), 2);
    assert_ne!(owned_references[0], owned_references[1]);
}

#[rstest]
#[tokio::test]
async fn failed_snapshot_mount_removes_view_and_empty_mountpoint(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::failing(|command| {
        matches!(command, OciCommand::MountSnapshot { .. })
    }));
    let cache_dir = oci_fixture.request.cache_dir.clone();

    let error = match mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    {
        Ok(_) => panic!("mount should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        OciBuildError::Command {
            operation: OciOperation::MountSnapshot,
            ..
        }
    ));
    assert_eq!(
        std::fs::read_dir(&cache_dir)
            .expect("read OCI cache directory")
            .count(),
        0
    );
    let commands = runner.commands();
    assert_eq!(commands.len(), 9);
    assert_eq!(commands[0], OciCommand::CheckContainerd);
    assert_eq!(commands[1], oci_fixture.pull_command());
    let OciCommand::CreateSnapshotView {
        snapshot_key,
        parent,
        mount_path,
    } = &commands[4]
    else {
        panic!("fifth command should create the snapshot view");
    };
    assert_eq!(parent, &oci_fixture.chain_id());
    assert!(mount_path.starts_with(&cache_dir));
    assert_eq!(commands[2], oci_fixture.tag_command(mount_path));
    assert_eq!(commands[3], oci_fixture.remove_image_command());
    assert_eq!(commands[5], oci_fixture.mount_command(mount_path));
    assert_eq!(commands[6], oci_fixture.unmount_command(mount_path));
    assert_eq!(
        commands[7],
        OciCommand::RemoveSnapshotView {
            snapshot_key: snapshot_key.clone(),
        }
    );
    assert_eq!(
        commands[8],
        oci_fixture.remove_owned_image_command(mount_path)
    );
}

#[rstest]
#[tokio::test]
async fn failed_mount_retries_remaining_cleanup_after_transient_failure(oci_fixture: OciFixture) {
    let runner = Arc::new(RecordingRunner::failing_times(
        |command| {
            matches!(
                command,
                OciCommand::MountSnapshot { .. } | OciCommand::RemoveSnapshotView { .. }
            )
        },
        2,
    ));
    let cache_dir = oci_fixture.request.cache_dir.clone();

    let error = match mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    {
        Ok(_) => panic!("mount should fail"),
        Err(error) => error,
    };

    assert!(matches!(
        error,
        OciBuildError::Command {
            operation: OciOperation::MountSnapshot,
            ..
        }
    ));
    let owned_reference = runner
        .commands()
        .into_iter()
        .find_map(|command| match command {
            OciCommand::TagImage {
                owned_reference, ..
            } => Some(owned_reference),
            _ => None,
        })
        .expect("tagged build-owned image");
    let final_cleanup_command = OciCommand::RemoveImage {
        reference: owned_reference,
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        while runner.commands().last() != Some(&final_cleanup_command) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("remaining cleanup should be retried");
    assert_eq!(
        runner
            .commands()
            .iter()
            .filter(|command| matches!(command, OciCommand::RemoveSnapshotView { .. }))
            .count(),
        2
    );
    assert_eq!(
        std::fs::read_dir(cache_dir)
            .expect("read OCI cache directory")
            .count(),
        0
    );
}

#[rstest]
#[tokio::test]
async fn failed_mount_directory_creation_removes_the_source_image(oci_fixture: OciFixture) {
    std::fs::write(&oci_fixture.request.cache_dir, b"not a directory")
        .expect("create invalid cache path");
    let runner = Arc::new(RecordingRunner::default());

    let error = match mount_with_runner(
        oci_fixture.request.clone(),
        oci_fixture.arch,
        runner.clone(),
    )
    .await
    {
        Ok(_) => panic!("mount should fail"),
        Err(error) => error,
    };

    assert!(matches!(error, OciBuildError::File { .. }));
    assert_eq!(
        runner.commands(),
        vec![
            OciCommand::CheckContainerd,
            oci_fixture.pull_command(),
            oci_fixture.remove_image_command(),
        ]
    );
}

#[rstest]
#[case("docker.io/library/busybox:1.37.0", "bin/busybox")]
#[case("docker.io/library/python:3.12-slim-bookworm", "usr/local/bin/python3")]
#[case("docker.io/library/debian:bookworm-slim", "bin/sh")]
#[tokio::test]
async fn real_containerd_exposes_a_flattened_read_only_rootfs(
    mut oci_fixture: OciFixture,
    #[case] reference: &str,
    #[case] expected_path: &str,
) {
    if std::env::var_os("SPACE_TEST_REAL_CONTAINERD").is_none() {
        return;
    }
    oci_fixture.request.reference = reference.parse().expect("valid OCI reference");

    let rootfs = mount(oci_fixture.request)
        .await
        .expect("mount real OCI image");

    assert!(rootfs.path().join(expected_path).is_file());
    assert!(std::fs::write(rootfs.path().join("space-write-test"), b"write").is_err());

    rootfs.unmount().await.expect("unmount real OCI image");
}

#[test]
fn oci_cache_rejects_redirects_and_shared_or_foreign_directories() {
    use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _, symlink};

    let root = tempfile::tempdir().expect("fixture");
    let cache = root.path().join("cache");
    prepare_cache_directory(&cache).expect("create owned cache");
    prepare_cache_directory(&cache).expect("reuse owned cache");
    assert_eq!(
        fs::symlink_metadata(&cache).expect("cache metadata").mode() & 0o777,
        0o755,
        "the cache is created 0755 whatever the process umask"
    );
    for mode in [0o775, 0o757] {
        fs::set_permissions(&cache, fs::Permissions::from_mode(mode)).expect("share cache");
        assert!(prepare_cache_directory(&cache).is_err());
    }
    fs::set_permissions(&cache, fs::Permissions::from_mode(0o755)).expect("restore cache");
    // A group-writable parent lets another account rename the cache away
    // before the workspace is created by path, unless the parent is sticky.
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o775)).expect("share parent");
    assert!(prepare_cache_directory(&cache).is_err());
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o1775)).expect("sticky parent");
    prepare_cache_directory(&cache).expect("sticky shared parent is accepted");
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("restore parent");
    let alias = root.path().join("alias");
    symlink(&cache, &alias).expect("cache alias");
    assert!(prepare_cache_directory(&alias).is_err());
    assert_eq!(fs::read_dir(&cache).expect("cache contents").count(), 0);
    // A cache another account owns is refused in both CI halves: root hands
    // the fixture to another uid, and an unprivileged caller does not own the
    // root-owned filesystem root.
    if nix::unistd::geteuid().is_root() {
        nix::unistd::chown(&cache, Some(nix::unistd::Uid::from_raw(65534)), None)
            .expect("foreign cache");
        assert!(prepare_cache_directory(&cache).is_err());
    } else {
        assert!(prepare_cache_directory(Path::new("/")).is_err());
    }
}
