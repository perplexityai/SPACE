use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use oci_spec::distribution::Reference as OciReference;
use pplx_space_common_core::types::{Arch, ArchParseError, Digest, DigestParseError};
use tokio::process::Command as TokioCommand;
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};
use tracing::Instrument as _;

use super::registry;
use super::storage::{OciPullBudget, OciStorageConfig, OciStorageError};
#[cfg(test)]
use crate::storage::StorageAdmissionError;
use crate::storage::{StatvfsStorageProbe, StorageProbe};

const CONTAINERD_NAMESPACE: &str = "space-builds";
const NATIVE_SNAPSHOTTER: &str = "native";
const IMAGE_CHAIN_ID_PREFIX: &str = "image chain ID: ";
const MAX_COMMAND_ERROR_BYTES: usize = 4096;
const FALLBACK_CLEANUP_ATTEMPTS: usize = 2;
const NATIVE_SNAPSHOT_PATH_PREFIX: &str =
    "/var/lib/containerd/io.containerd.snapshotter.v1.native/snapshots/";
static OCI_PULL_LOCK: LazyLock<Arc<AsyncMutex<()>>> =
    LazyLock::new(|| Arc::new(AsyncMutex::new(())));

#[derive(Clone, Debug, Eq, PartialEq)]
struct ContainerdSnapshotKey(String);

impl ContainerdSnapshotKey {
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ContainerdSnapshotKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OciFileOperation {
    CreateMountDirectory,
    RemoveMountDirectory,
}

impl OciFileOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CreateMountDirectory => "create_mount_directory",
            Self::RemoveMountDirectory => "remove_mount_directory",
        }
    }
}

impl std::fmt::Display for OciFileOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OciOperation {
    CheckContainerd,
    PullImage,
    TagImage,
    CreateSnapshotView,
    MountSnapshot,
    UnmountSnapshot,
    RemoveSnapshotView,
    RemoveImage,
}

impl OciOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CheckContainerd => "check_containerd",
            Self::PullImage => "pull_image",
            Self::TagImage => "tag_image",
            Self::CreateSnapshotView => "create_snapshot_view",
            Self::MountSnapshot => "mount_snapshot",
            Self::UnmountSnapshot => "unmount_snapshot",
            Self::RemoveSnapshotView => "remove_snapshot_view",
            Self::RemoveImage => "remove_image",
        }
    }
}

impl std::fmt::Display for OciOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum OciCommand {
    CheckContainerd,
    PullImage {
        reference: OciReference,
        arch: Arch,
        hosts_dir: Option<PathBuf>,
    },
    TagImage {
        source_reference: OciReference,
        owned_reference: OciReference,
    },
    CreateSnapshotView {
        snapshot_key: ContainerdSnapshotKey,
        parent: Digest,
        mount_path: PathBuf,
    },
    MountSnapshot {
        source: PathBuf,
        mount_path: PathBuf,
    },
    UnmountSnapshot {
        mount_path: PathBuf,
    },
    RemoveSnapshotView {
        snapshot_key: ContainerdSnapshotKey,
    },
    RemoveImage {
        reference: OciReference,
    },
}

impl OciCommand {
    const fn operation(&self) -> OciOperation {
        match self {
            Self::CheckContainerd => OciOperation::CheckContainerd,
            Self::PullImage { .. } => OciOperation::PullImage,
            Self::TagImage { .. } => OciOperation::TagImage,
            Self::CreateSnapshotView { .. } => OciOperation::CreateSnapshotView,
            Self::MountSnapshot { .. } => OciOperation::MountSnapshot,
            Self::UnmountSnapshot { .. } => OciOperation::UnmountSnapshot,
            Self::RemoveSnapshotView { .. } => OciOperation::RemoveSnapshotView,
            Self::RemoveImage { .. } => OciOperation::RemoveImage,
        }
    }

    const fn program(&self) -> &'static str {
        match self {
            Self::MountSnapshot { .. } => "mount",
            Self::UnmountSnapshot { .. } => "umount",
            _ => "ctr",
        }
    }

    fn args(&self) -> Vec<OsString> {
        match self {
            Self::CheckContainerd => vec!["version".into()],
            Self::PullImage {
                reference,
                arch,
                hosts_dir,
            } => {
                let mut args: Vec<OsString> = vec![
                    "--namespace".into(),
                    CONTAINERD_NAMESPACE.into(),
                    "images".into(),
                    "pull".into(),
                    "--local".into(),
                    "--platform".into(),
                    container_platform(*arch).into(),
                    "--snapshotter".into(),
                    NATIVE_SNAPSHOTTER.into(),
                    "--skip-metadata".into(),
                    "--print-chainid".into(),
                ];
                if let Some(hosts_dir) = hosts_dir {
                    args.push("--hosts-dir".into());
                    args.push(hosts_dir.as_os_str().into());
                }
                args.push(reference.to_string().into());
                args
            }
            Self::TagImage {
                source_reference,
                owned_reference,
            } => vec![
                "--namespace".into(),
                CONTAINERD_NAMESPACE.into(),
                "images".into(),
                "tag".into(),
                source_reference.to_string().into(),
                owned_reference.to_string().into(),
            ],
            Self::CreateSnapshotView {
                snapshot_key,
                parent,
                mount_path,
            } => vec![
                "--namespace".into(),
                CONTAINERD_NAMESPACE.into(),
                "snapshots".into(),
                "--snapshotter".into(),
                NATIVE_SNAPSHOTTER.into(),
                "view".into(),
                "--target".into(),
                mount_path.as_os_str().into(),
                snapshot_key.as_str().into(),
                parent.to_string().into(),
            ],
            Self::MountSnapshot { source, mount_path } => vec![
                "-t".into(),
                "bind".into(),
                source.as_os_str().into(),
                mount_path.as_os_str().into(),
                "-o".into(),
                "rbind,ro".into(),
            ],
            Self::UnmountSnapshot { mount_path } => vec![mount_path.as_os_str().into()],
            Self::RemoveSnapshotView { snapshot_key } => vec![
                "--namespace".into(),
                CONTAINERD_NAMESPACE.into(),
                "snapshots".into(),
                "--snapshotter".into(),
                NATIVE_SNAPSHOTTER.into(),
                "rm".into(),
                snapshot_key.as_str().into(),
            ],
            Self::RemoveImage { reference } => vec![
                "--namespace".into(),
                CONTAINERD_NAMESPACE.into(),
                "images".into(),
                "rm".into(),
                reference.to_string().into(),
            ],
        }
    }

    const fn output(&self) -> CommandOutput {
        match self {
            Self::CheckContainerd => CommandOutput::Inherit,
            Self::PullImage { .. } | Self::CreateSnapshotView { .. } => CommandOutput::Capture,
            Self::TagImage { .. }
            | Self::MountSnapshot { .. }
            | Self::UnmountSnapshot { .. }
            | Self::RemoveSnapshotView { .. }
            | Self::RemoveImage { .. } => CommandOutput::Quiet,
        }
    }
}

impl std::fmt::Display for OciCommand {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.operation().fmt(formatter)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OciBuildError {
    #[error("{operation} failed for {path}: {source}")]
    File {
        operation: OciFileOperation,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{operation} failed: {source}")]
    Command {
        operation: OciOperation,
        #[source]
        source: OciCommandFailure,
    },
    #[error("generated OCI mount path has no UTF-8 file name: {path}")]
    InvalidMountPath { path: PathBuf },
    #[error("containerd did not report an image chain ID")]
    MissingChainId,
    #[error("containerd reported an invalid image chain ID: {source}")]
    InvalidChainId {
        #[source]
        source: DigestParseError,
    },
    #[error("containerd returned an invalid native snapshot mount: {output}")]
    InvalidSnapshotMount { output: String },
    #[error(transparent)]
    UnsupportedArch(#[from] ArchParseError),
    #[error(transparent)]
    Storage(#[from] OciStorageError),
    #[error("OCI registry validation failed")]
    Registry {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl OciBuildError {
    /// The bounded cause metrics report for a failed containerd step.
    pub fn error_kind(&self) -> crate::TemplateBuildErrorKind {
        use crate::TemplateBuildErrorKind as Kind;
        match self {
            Self::Registry { .. }
            | Self::UnsupportedArch(_)
            | Self::Command {
                operation: OciOperation::PullImage,
                ..
            } => Kind::ImagePull,
            Self::Storage(_) => Kind::StorageAdmission,
            Self::Command {
                operation:
                    OciOperation::TagImage
                    | OciOperation::CreateSnapshotView
                    | OciOperation::MountSnapshot
                    | OciOperation::UnmountSnapshot
                    | OciOperation::RemoveSnapshotView
                    | OciOperation::RemoveImage,
                ..
            }
            | Self::File { .. }
            | Self::InvalidMountPath { .. }
            | Self::MissingChainId
            | Self::InvalidChainId { .. }
            | Self::InvalidSnapshotMount { .. } => Kind::Unpack,
            Self::Command {
                operation: OciOperation::CheckContainerd,
                ..
            } => Kind::Other,
        }
    }
}

impl From<registry::RegistryError> for OciBuildError {
    fn from(source: registry::RegistryError) -> Self {
        Self::Registry {
            source: Box::new(source),
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum CommandRunError {
    #[error("failed to start: {0}")]
    Io(#[from] std::io::Error),
    #[error("exited unsuccessfully with code {code:?}: {stderr}")]
    Exit { code: Option<i32>, stderr: String },
    #[error("exited unsuccessfully with code {code:?}; stderr redacted")]
    RedactedExit { code: Option<i32> },
    #[error("snapshot view no longer exists: {snapshot_key}")]
    SnapshotViewNotFound { snapshot_key: ContainerdSnapshotKey },
    #[error("image record no longer exists")]
    ImageNotFound,
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct OciCommandFailure(CommandRunError);

impl OciCommandFailure {
    fn is_snapshot_view_not_found(&self) -> bool {
        matches!(self.0, CommandRunError::SnapshotViewNotFound { .. })
    }

    fn is_image_not_found(&self) -> bool {
        matches!(self.0, CommandRunError::ImageNotFound)
    }
}

#[derive(Clone, Debug)]
pub struct OciMountRequest {
    pub cache_dir: PathBuf,
    pub reference: OciReference,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommandOutput {
    Inherit,
    Quiet,
    Capture,
}

#[derive(Debug, Default, Eq, PartialEq)]
struct CommandResult {
    stdout: Vec<u8>,
}

#[async_trait]
trait CommandRunner: Send + Sync {
    async fn run(&self, command: &OciCommand) -> Result<CommandResult, CommandRunError>;
}

struct OciMountDependencies {
    runner: Arc<dyn CommandRunner>,
    storage_config: OciStorageConfig,
    storage_probe: Arc<dyn StorageProbe>,
    pull_lock: Arc<AsyncMutex<()>>,
}

// Keep the containerd CLI behind this runner. Available Rust clients do not
// cover the local pull and snapshot behavior used by deployed containerd versions.
struct ProcessCommandRunner;

#[async_trait]
impl CommandRunner for ProcessCommandRunner {
    async fn run(&self, command: &OciCommand) -> Result<CommandResult, CommandRunError> {
        let mut process = if crate::effective_uid() == Some(0) {
            TokioCommand::new(command.program())
        } else {
            let mut process = TokioCommand::new("sudo");
            process.args(["-E", command.program()]);
            process
        };
        process
            .args(command.args())
            .kill_on_drop(true)
            .stdin(Stdio::null());
        match command.output() {
            CommandOutput::Inherit => {
                process.stdout(Stdio::inherit()).stderr(Stdio::inherit());
            }
            CommandOutput::Quiet => {
                process.stdout(Stdio::null()).stderr(Stdio::piped());
            }
            CommandOutput::Capture => {
                process.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
        }

        let span = tracing::info_span!(
            "containerd.client",
            otel.kind = "client",
            "operation.name" = "containerd.client",
            containerd.operation = %command,
            otel.status_code = tracing::field::Empty,
            otel.status_description = tracing::field::Empty,
        );
        async move {
            match command.output() {
                CommandOutput::Inherit => record_command_status(process.status().await),
                CommandOutput::Quiet | CommandOutput::Capture => {
                    record_command_output(command, process.output().await)
                }
            }
        }
        .instrument(span)
        .await
    }
}

fn record_command_status(
    result: std::io::Result<std::process::ExitStatus>,
) -> Result<CommandResult, CommandRunError> {
    match result {
        Ok(status) if status.success() => {
            tracing::Span::current().record("otel.status_code", "OK");
            Ok(CommandResult::default())
        }
        Ok(status) => {
            let error = CommandRunError::Exit {
                code: status.code(),
                stderr: String::new(),
            };
            tracing::Span::current().record("otel.status_code", "ERROR");
            tracing::Span::current().record("otel.status_description", error.to_string().as_str());
            Err(error)
        }
        Err(error) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            tracing::Span::current().record("otel.status_description", error.to_string().as_str());
            Err(CommandRunError::Io(error))
        }
    }
}

fn record_command_output(
    command: &OciCommand,
    result: std::io::Result<std::process::Output>,
) -> Result<CommandResult, CommandRunError> {
    match result {
        Ok(output) if output.status.success() => {
            tracing::Span::current().record("otel.status_code", "OK");
            Ok(CommandResult {
                stdout: output.stdout,
            })
        }
        Ok(output) => {
            let stderr = bounded_output(&output.stderr);
            if matches!(command, OciCommand::UnmountSnapshot { .. })
                && stderr.contains("not mounted")
            {
                tracing::Span::current().record("otel.status_code", "OK");
                return Ok(CommandResult::default());
            }
            let error = match command {
                OciCommand::RemoveSnapshotView { snapshot_key, .. }
                    if stderr.contains(
                        format!("snapshot {snapshot_key} does not exist: not found").as_str(),
                    ) =>
                {
                    CommandRunError::SnapshotViewNotFound {
                        snapshot_key: snapshot_key.clone(),
                    }
                }
                OciCommand::RemoveImage { .. } if stderr.contains("not found") => {
                    CommandRunError::ImageNotFound
                }
                OciCommand::PullImage { .. }
                | OciCommand::TagImage { .. }
                | OciCommand::RemoveImage { .. } => CommandRunError::RedactedExit {
                    code: output.status.code(),
                },
                _ => CommandRunError::Exit {
                    code: output.status.code(),
                    stderr,
                },
            };
            tracing::Span::current().record("otel.status_code", "ERROR");
            tracing::Span::current().record("otel.status_description", error.to_string().as_str());
            Err(error)
        }
        Err(error) => {
            tracing::Span::current().record("otel.status_code", "ERROR");
            tracing::Span::current().record("otel.status_description", error.to_string().as_str());
            Err(CommandRunError::Io(error))
        }
    }
}

fn bounded_output(bytes: &[u8]) -> String {
    let length = bytes.len().min(MAX_COMMAND_ERROR_BYTES);
    String::from_utf8_lossy(bytes.get(..length).unwrap_or(bytes))
        .trim()
        .to_string()
}

pub struct OciRootfs {
    mount_path: PathBuf,
    cleanup: Option<OciCleanup>,
}

struct OciCleanup {
    active_mount: Option<PathBuf>,
    snapshot_key: Option<ContainerdSnapshotKey>,
    mount_directory: Option<PathBuf>,
    owned_image_reference: Option<OciReference>,
    runner: Arc<dyn CommandRunner>,
    armed: bool,
}

struct SourceImageGuard {
    reference: Option<OciReference>,
    runner: Arc<dyn CommandRunner>,
    pull_guard: Option<OwnedMutexGuard<()>>,
}

impl OciRootfs {
    pub fn path(&self) -> &Path {
        &self.mount_path
    }

    /// Releases the rootfs mount and containerd snapshot view, reporting cleanup failures.
    pub async fn unmount(mut self) -> Result<(), OciBuildError> {
        self.cleanup
            .as_mut()
            .expect("OCI rootfs cleanup is still armed")
            .run()
            .await?;
        self.cleanup = None;
        Ok(())
    }
}

impl Drop for OciRootfs {
    fn drop(&mut self) {
        let Some(cleanup) = self.cleanup.take() else {
            return;
        };
        schedule_cleanup(cleanup);
    }
}

fn schedule_cleanup(mut cleanup: OciCleanup) {
    cleanup.armed = false;
    if let Err(error) = std::thread::Builder::new()
        .name("space-oci-cleanup".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(%error, "failed to start OCI rootfs cleanup runtime");
                    return;
                }
            };
            runtime.block_on(cleanup_dropped_rootfs(cleanup));
        })
    {
        tracing::warn!(%error, "failed to start OCI rootfs cleanup thread");
    }
}

impl OciCleanup {
    fn take_armed(&mut self) -> Option<Self> {
        if !self.armed {
            return None;
        }
        self.armed = false;
        if self.active_mount.is_none()
            && self.snapshot_key.is_none()
            && self.mount_directory.is_none()
            && self.owned_image_reference.is_none()
        {
            return None;
        }
        Some(Self {
            active_mount: self.active_mount.take(),
            snapshot_key: self.snapshot_key.take(),
            mount_directory: self.mount_directory.take(),
            owned_image_reference: self.owned_image_reference.take(),
            runner: self.runner.clone(),
            armed: false,
        })
    }

    async fn run(&mut self) -> Result<(), OciBuildError> {
        if let Some(mount_path) = &self.active_mount {
            run(
                self.runner.as_ref(),
                OciCommand::UnmountSnapshot {
                    mount_path: mount_path.clone(),
                },
            )
            .await?;
            self.active_mount = None;
        }
        if let Some(snapshot_key) = &self.snapshot_key {
            let result = run(
                self.runner.as_ref(),
                OciCommand::RemoveSnapshotView {
                    snapshot_key: snapshot_key.clone(),
                },
            )
            .await;
            match result {
                Ok(_) => {}
                Err(OciBuildError::Command { source, .. })
                    if source.is_snapshot_view_not_found() => {}
                Err(error) => return Err(error),
            }
            self.snapshot_key = None;
        }
        if let Some(mount_directory) = &self.mount_directory {
            remove_mount_directory(mount_directory)?;
            self.mount_directory = None;
        }
        if let Some(reference) = &self.owned_image_reference {
            let result = run(
                self.runner.as_ref(),
                OciCommand::RemoveImage {
                    reference: reference.clone(),
                },
            )
            .await;
            match result {
                Ok(_) => {}
                Err(OciBuildError::Command { source, .. }) if source.is_image_not_found() => {}
                Err(error) => return Err(error),
            }
            self.owned_image_reference = None;
        }
        Ok(())
    }
}

impl Drop for OciCleanup {
    fn drop(&mut self) {
        if let Some(cleanup) = self.take_armed() {
            schedule_cleanup(cleanup);
        }
    }
}

impl SourceImageGuard {
    async fn release(&mut self) -> Result<(), OciBuildError> {
        let Some(reference) = &self.reference else {
            return Ok(());
        };
        let result = run(
            self.runner.as_ref(),
            OciCommand::RemoveImage {
                reference: reference.clone(),
            },
        )
        .await;
        match result {
            Ok(_) => {}
            Err(OciBuildError::Command { source, .. }) if source.is_image_not_found() => {}
            Err(error) => return Err(error),
        }
        self.reference = None;
        self.pull_guard = None;
        Ok(())
    }
}

impl Drop for SourceImageGuard {
    fn drop(&mut self) {
        let Some(reference) = self.reference.take() else {
            return;
        };
        let Some(pull_guard) = self.pull_guard.take() else {
            return;
        };
        schedule_source_image_cleanup(SourceImageGuard {
            reference: Some(reference),
            runner: self.runner.clone(),
            pull_guard: Some(pull_guard),
        });
    }
}

fn schedule_source_image_cleanup(mut source: SourceImageGuard) {
    let Some(reference) = source.reference.take() else {
        return;
    };
    let Some(pull_guard) = source.pull_guard.take() else {
        return;
    };
    let runner = source.runner.clone();
    if let Err(error) = std::thread::Builder::new()
        .name("space-oci-source-cleanup".to_string())
        .spawn(move || {
            let _pull_guard = pull_guard;
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    tracing::warn!(%error, "failed to start OCI source cleanup runtime");
                    return;
                }
            };
            runtime.block_on(async {
                for attempt in 1..=FALLBACK_CLEANUP_ATTEMPTS {
                    let result = run(
                        runner.as_ref(),
                        OciCommand::RemoveImage {
                            reference: reference.clone(),
                        },
                    )
                    .await;
                    match result {
                        Ok(_) => return,
                        Err(OciBuildError::Command { source, .. })
                            if source.is_image_not_found() =>
                        {
                            return;
                        }
                        Err(error) if attempt < FALLBACK_CLEANUP_ATTEMPTS => {
                            tracing::warn!(%error, attempt, "retrying OCI source cleanup");
                        }
                        Err(error) => {
                            tracing::warn!(%error, attempt, "failed to clean up OCI source image");
                        }
                    }
                }
            });
        })
    {
        tracing::warn!(%error, "failed to start OCI source cleanup thread");
    }
}

async fn cleanup_dropped_rootfs(mut cleanup: OciCleanup) {
    for attempt in 1..=FALLBACK_CLEANUP_ATTEMPTS {
        match cleanup.run().await {
            Ok(()) => return,
            Err(error) if attempt < FALLBACK_CLEANUP_ATTEMPTS => {
                tracing::warn!(%error, attempt, "retrying dropped OCI rootfs cleanup");
            }
            Err(error) => {
                tracing::warn!(%error, attempt, "failed to clean up dropped OCI rootfs mount");
            }
        }
    }
}

pub async fn mount(request: OciMountRequest) -> Result<OciRootfs, OciBuildError> {
    let arch = std::env::consts::ARCH.parse()?;
    let registry = registry::session_for(&request.reference).await?;
    let ecr_auth_root = registry::ecr_auth_root();
    mount_with_manifest_source(
        request,
        arch,
        registry.manifest_source(),
        registry.ecr_pull_auth(),
        &ecr_auth_root,
        OciMountDependencies {
            runner: Arc::new(ProcessCommandRunner),
            storage_config: OciStorageConfig::with_defaults(),
            storage_probe: Arc::new(StatvfsStorageProbe),
            pull_lock: Arc::clone(&OCI_PULL_LOCK),
        },
    )
    .await
}

async fn mount_with_manifest_source(
    request: OciMountRequest,
    arch: Arch,
    manifest_source: &dyn registry::ManifestSource,
    ecr_pull_auth: Option<&registry::EcrPullAuth>,
    ecr_auth_root: &Path,
    dependencies: OciMountDependencies,
) -> Result<OciRootfs, OciBuildError> {
    let reference =
        registry::resolve_with_source(&request.reference, arch, manifest_source).await?;
    mount_with_registry_dependencies(
        OciMountRequest {
            cache_dir: request.cache_dir,
            reference,
        },
        arch,
        ecr_pull_auth,
        ecr_auth_root,
        dependencies,
    )
    .await
}

#[cfg(test)]
async fn mount_with_runner(
    request: OciMountRequest,
    arch: Arch,
    runner: Arc<dyn CommandRunner>,
) -> Result<OciRootfs, OciBuildError> {
    mount_with_dependencies(
        request,
        arch,
        runner,
        OciStorageConfig {
            root: PathBuf::from("/test/containerd"),
            max_image_bytes: 1,
            min_available_bytes: 1,
            poll_interval: std::time::Duration::from_millis(1),
        },
        Arc::new(OpenStorageProbe),
        Arc::clone(&OCI_PULL_LOCK),
    )
    .await
}

#[cfg(test)]
async fn mount_with_manifest_source_and_runner(
    request: OciMountRequest,
    arch: Arch,
    manifest_source: &dyn registry::ManifestSource,
    runner: Arc<dyn CommandRunner>,
) -> Result<OciRootfs, OciBuildError> {
    mount_with_manifest_source(
        request,
        arch,
        manifest_source,
        None,
        Path::new("/unused-ecr-auth"),
        OciMountDependencies {
            runner,
            storage_config: OciStorageConfig {
                root: PathBuf::from("/test/containerd"),
                max_image_bytes: 1,
                min_available_bytes: 1,
                poll_interval: std::time::Duration::from_millis(1),
            },
            storage_probe: Arc::new(OpenStorageProbe),
            pull_lock: Arc::new(AsyncMutex::new(())),
        },
    )
    .await
}

#[cfg(test)]
async fn mount_with_dependencies(
    request: OciMountRequest,
    arch: Arch,
    runner: Arc<dyn CommandRunner>,
    storage_config: OciStorageConfig,
    storage_probe: Arc<dyn StorageProbe>,
    pull_lock: Arc<AsyncMutex<()>>,
) -> Result<OciRootfs, OciBuildError> {
    mount_with_registry_dependencies(
        request,
        arch,
        None,
        Path::new("/unused-ecr-auth"),
        OciMountDependencies {
            runner,
            storage_config,
            storage_probe,
            pull_lock,
        },
    )
    .await
}

async fn mount_with_registry_dependencies(
    request: OciMountRequest,
    arch: Arch,
    ecr_pull_auth: Option<&registry::EcrPullAuth>,
    ecr_auth_root: &Path,
    dependencies: OciMountDependencies,
) -> Result<OciRootfs, OciBuildError> {
    let OciMountDependencies {
        runner,
        storage_config,
        storage_probe,
        pull_lock,
    } = dependencies;
    run(runner.as_ref(), OciCommand::CheckContainerd).await?;
    let pull_guard = pull_lock.lock_owned().await;
    let pull_budget = OciPullBudget::admit(storage_config, storage_probe.as_ref())?;
    let ecr_auth_directory = ecr_pull_auth
        .map(|auth| auth.materialize(ecr_auth_root))
        .transpose()?;
    let mut source_image = SourceImageGuard {
        reference: Some(request.reference.clone()),
        runner: runner.clone(),
        pull_guard: Some(pull_guard),
    };
    let pull_output = run_pull_with_budget(
        runner.as_ref(),
        OciCommand::PullImage {
            reference: request.reference.clone(),
            arch,
            hosts_dir: ecr_auth_directory
                .as_ref()
                .map(|directory| directory.path().to_path_buf()),
        },
        &pull_budget,
        storage_probe.as_ref(),
    )
    .await;
    drop(ecr_auth_directory);
    let pull_output = match pull_output {
        Ok(output) => output,
        Err(error) => {
            cleanup_source_after_error(source_image).await;
            return Err(error);
        }
    };
    let parent = match parse_chain_id(&pull_output.stdout) {
        Ok(parent) => parent,
        Err(error) => {
            cleanup_source_after_error(source_image).await;
            return Err(error);
        }
    };
    // containerd's image-mount path depends on its mount-manager plugin. A
    // snapshot view exposes the same flattened rootfs without that dependency.
    let mount_path = match create_mount_directory(&request.cache_dir) {
        Ok(mount_path) => mount_path,
        Err(error) => {
            cleanup_source_after_error(source_image).await;
            return Err(error);
        }
    };
    let mut cleanup = OciCleanup {
        active_mount: None,
        snapshot_key: None,
        mount_directory: Some(mount_path.clone()),
        owned_image_reference: None,
        runner: runner.clone(),
        armed: true,
    };
    let owned_reference = match owned_image_reference(&mount_path) {
        Ok(reference) => reference,
        Err(error) => {
            cleanup_after_mount_error(cleanup).await;
            return Err(error);
        }
    };
    cleanup.owned_image_reference = Some(owned_reference.clone());
    if let Err(error) = run(
        runner.as_ref(),
        OciCommand::TagImage {
            source_reference: request.reference,
            owned_reference,
        },
    )
    .await
    {
        cleanup_source_after_error(source_image).await;
        cleanup_after_mount_error(cleanup).await;
        return Err(error);
    }
    if let Err(error) = source_image.release().await {
        cleanup_after_mount_error(cleanup).await;
        return Err(error);
    }
    let snapshot_key = match snapshot_key(&mount_path) {
        Ok(snapshot_key) => snapshot_key,
        Err(error) => {
            cleanup_after_mount_error(cleanup).await;
            return Err(error);
        }
    };
    cleanup.snapshot_key = Some(snapshot_key.clone());
    let mount_output = match run(
        runner.as_ref(),
        OciCommand::CreateSnapshotView {
            snapshot_key: snapshot_key.clone(),
            parent,
            mount_path: mount_path.clone(),
        },
    )
    .await
    {
        Ok(output) => output,
        Err(error) => {
            cleanup_after_mount_error(cleanup).await;
            return Err(error);
        }
    };
    let source = match parse_native_snapshot_mount(&mount_output.stdout, &mount_path) {
        Ok(source) => source,
        Err(error) => {
            cleanup_after_mount_error(cleanup).await;
            return Err(error);
        }
    };
    cleanup.active_mount = Some(mount_path.clone());
    let mount_result = run(
        runner.as_ref(),
        OciCommand::MountSnapshot {
            source,
            mount_path: mount_path.clone(),
        },
    )
    .await;
    if let Err(error) = mount_result {
        cleanup_after_mount_error(cleanup).await;
        return Err(error);
    }
    Ok(OciRootfs {
        mount_path: mount_path.clone(),
        cleanup: Some(cleanup),
    })
}

#[cfg(test)]
struct OpenStorageProbe;

#[cfg(test)]
impl StorageProbe for OpenStorageProbe {
    fn status(&self, _root: &Path) -> Option<pplx_space_common_core::host::disk::DiskStatus> {
        Some(pplx_space_common_core::host::disk::DiskStatus {
            total_bytes: 4,
            free_bytes: 4,
            available_bytes: 4,
        })
    }
}

async fn cleanup_source_after_error(mut source: SourceImageGuard) {
    if let Err(error) = source.release().await {
        tracing::warn!(%error, "failed to clean up OCI source image after mount error");
    }
}

async fn cleanup_after_mount_error(mut cleanup: OciCleanup) {
    if let Err(cleanup_error) = cleanup.run().await {
        tracing::warn!(%cleanup_error, "failed to clean up OCI rootfs after mount error");
        schedule_cleanup(cleanup);
    }
}

fn parse_chain_id(output: &[u8]) -> Result<Digest, OciBuildError> {
    let output = String::from_utf8_lossy(output);
    let chain_id = output
        .lines()
        .rev()
        .find_map(|line| line.trim().strip_prefix(IMAGE_CHAIN_ID_PREFIX))
        .filter(|chain_id| !chain_id.is_empty())
        .ok_or(OciBuildError::MissingChainId)?;
    chain_id
        .parse()
        .map_err(|source| OciBuildError::InvalidChainId { source })
}

fn parse_native_snapshot_mount(
    output: &[u8],
    expected_target: &Path,
) -> Result<PathBuf, OciBuildError> {
    let output = bounded_output(output);
    let target_and_options = format!(" {} -o rbind,ro", expected_target.display());
    let Some(source) = output
        .strip_prefix("mount -t bind ")
        .and_then(|mount| mount.strip_suffix(&target_and_options))
    else {
        return Err(OciBuildError::InvalidSnapshotMount { output });
    };
    let source = PathBuf::from(source);
    if !source.starts_with(NATIVE_SNAPSHOT_PATH_PREFIX) {
        return Err(OciBuildError::InvalidSnapshotMount { output });
    }
    Ok(source)
}

fn snapshot_key(mount_path: &Path) -> Result<ContainerdSnapshotKey, OciBuildError> {
    let key = mount_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| OciBuildError::InvalidMountPath {
            path: mount_path.to_path_buf(),
        })?;
    Ok(ContainerdSnapshotKey(key.to_string()))
}

fn owned_image_reference(mount_path: &Path) -> Result<OciReference, OciBuildError> {
    let tag = snapshot_key(mount_path)?;
    Ok(OciReference::with_tag(
        "space.local".to_string(),
        "oci-builds".to_string(),
        tag.to_string(),
    ))
}

async fn run(
    runner: &dyn CommandRunner,
    command: OciCommand,
) -> Result<CommandResult, OciBuildError> {
    let operation = command.operation();
    runner
        .run(&command)
        .await
        .map_err(|source| OciBuildError::Command {
            operation,
            source: OciCommandFailure(source),
        })
}

async fn run_pull_with_budget(
    runner: &dyn CommandRunner,
    command: OciCommand,
    budget: &OciPullBudget,
    storage_probe: &dyn StorageProbe,
) -> Result<CommandResult, OciBuildError> {
    let pull = run(runner, command);
    tokio::pin!(pull);
    loop {
        tokio::select! {
            result = &mut pull => {
                let result = result?;
                budget.check(storage_probe)?;
                return Ok(result);
            }
            () = tokio::time::sleep(budget.poll_interval()) => {
                budget.check(storage_probe)?;
            }
        }
    }
}

fn create_mount_directory(cache_dir: &Path) -> Result<PathBuf, OciBuildError> {
    fs::create_dir_all(cache_dir).map_err(|source| OciBuildError::File {
        operation: OciFileOperation::CreateMountDirectory,
        path: cache_dir.to_path_buf(),
        source,
    })?;
    tempfile::Builder::new()
        .prefix("space-oci-rootfs-")
        .tempdir_in(cache_dir)
        .map(tempfile::TempDir::keep)
        .map_err(|source| OciBuildError::File {
            operation: OciFileOperation::CreateMountDirectory,
            path: cache_dir.to_path_buf(),
            source,
        })
}

fn remove_mount_directory(path: &Path) -> Result<(), OciBuildError> {
    fs::remove_dir(path).map_err(|source| OciBuildError::File {
        operation: OciFileOperation::RemoveMountDirectory,
        path: path.to_path_buf(),
        source,
    })
}

fn container_platform(arch: Arch) -> &'static str {
    match arch {
        Arch::Aarch64 => "linux/arm64",
        Arch::X86_64 => "linux/amd64",
    }
}

#[cfg(test)]
mod tests;
