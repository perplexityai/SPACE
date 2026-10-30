use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::Read;
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole, ArtifactFormat,
    ArtifactVersion, Digest, EnvironmentReference, ExecutionMetadata, ExecutionMetadataError,
    FIRECRACKER_STATE_FORMAT, GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind,
    RuntimeVersion, StateFormatIdentity, StateFormatName, StateFormatVersion, VersionSortKey,
};
use sha2::{Digest as _, Sha256};

mod cloud_hypervisor;
pub mod diagnostic;
pub mod kernel_release;
#[cfg(feature = "artifact-store")]
mod managed;
mod observation;

const BOOT_COMPONENT_ROLE: &str = "boot";
const VMLINUX_COMPONENT_FORMAT: &str = "vmlinux";

pub fn boot_component_role() -> ArtifactComponentRole {
    ArtifactComponentRole::parse(BOOT_COMPONENT_ROLE).expect("boot component role is valid")
}

use pplx_space_execution_catalog::layout::FIRECRACKER_STATE_FORMAT_RELEASES;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInstallation {
    pub runtime: RuntimeIdentity,
    pub guest: GuestPlatform,
    pub state_format: Option<StateFormatIdentity>,
    pub files: RuntimeInstallationFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RuntimeFileRole {
    RuntimeExecutable,
    LauncherExecutable,
}

impl RuntimeFileRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeExecutable => "runtime executable",
            Self::LauncherExecutable => "launcher executable",
        }
    }
}

impl std::fmt::Display for RuntimeFileRole {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInstallationFiles {
    files: BTreeMap<RuntimeFileRole, PathBuf>,
    integrity: ExecutionSourceIntegrity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionSourceIntegrity {
    /// Discovered without authentication against the compiled catalog.
    UnverifiedExternal,
    /// Verified at discovery; protected consumers must hash their opened file again.
    CatalogSha256,
    /// Sealed against the catalog measurement; consumers measure the opened file.
    FsVerity,
}

impl RuntimeInstallationFiles {
    fn new(files: impl IntoIterator<Item = (RuntimeFileRole, PathBuf)>) -> Self {
        Self {
            files: files.into_iter().collect(),
            integrity: ExecutionSourceIntegrity::UnverifiedExternal,
        }
    }

    pub fn paths(&self) -> impl Iterator<Item = &Path> {
        self.files.values().map(PathBuf::as_path)
    }

    pub fn integrity(&self) -> ExecutionSourceIntegrity {
        self.integrity
    }

    pub fn path(&self, role: RuntimeFileRole) -> Option<&Path> {
        self.files.get(&role).map(PathBuf::as_path)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArtifactComponentInstallation {
    metadata: ArtifactComponentMetadata,
    path: PathBuf,
    default_for_new: bool,
    legacy_default: bool,
    integrity: ExecutionSourceIntegrity,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedExecutionEnvironment {
    /// Inventory-owned source binding for authenticating these runtime files.
    /// An unbound restore can use its verified runtime without asserting a
    /// kernel identity that its historical snapshot never recorded.
    pub environment: Option<EnvironmentReference>,
    pub execution: ExecutionMetadata,
    pub runtime_files: RuntimeInstallationFiles,
    pub components: Vec<ResolvedArtifactComponent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedArtifactComponent {
    pub metadata: ArtifactComponentMetadata,
    pub path: PathBuf,
    pub integrity: ExecutionSourceIntegrity,
}

impl ResolvedExecutionEnvironment {
    pub fn component(
        &self,
        kind: ArtifactComponentKind,
        role: Option<&ArtifactComponentRole>,
    ) -> Option<&ResolvedArtifactComponent> {
        self.components
            .iter()
            .find(|component| component_metadata_identity(&component.metadata) == (kind, role))
    }
}

/// Artifacts verified for one explicit binding, separate from legacy discovery.
#[derive(Clone, Debug)]
struct InstalledEnvironment {
    runtime: Result<RuntimeInstallation, std::sync::Arc<RuntimeInventoryError>>,
    components: Result<Vec<ArtifactComponentInstallation>, std::sync::Arc<RuntimeInventoryError>>,
}

#[derive(Clone, Debug)]
pub struct RuntimeInventory {
    runtimes: Vec<RuntimeInstallation>,
    components: Vec<ArtifactComponentInstallation>,
    /// Explicit selection: compiled E0 for managed nodes, or the external
    /// layout's default for the bounded legacy execution path.
    defaults: Vec<RuntimeIdentity>,
    fingerprints: Vec<(PathBuf, PathFingerprint)>,
    environments: BTreeMap<EnvironmentReference, InstalledEnvironment>,
    #[cfg(feature = "artifact-store")]
    unavailable: Vec<UnavailableArtifact>,
    retry_after: Option<tokio::time::Instant>,
    initial_environment: Option<pplx_space_execution_common::environment::EnvironmentDefinition>,
}

#[cfg(feature = "artifact-store")]
#[derive(Clone, Debug)]
enum UnavailableArtifact {
    Runtime {
        identity: RuntimeIdentity,
        error: std::sync::Arc<RuntimeInventoryError>,
    },
    Kernel {
        digest: Digest,
        legacy: bool,
        error: std::sync::Arc<RuntimeInventoryError>,
    },
}

#[derive(Clone, Debug)]
enum PathFingerprint {
    Metadata(Option<FileFingerprint>),
    DirectoryEntries(Option<BTreeSet<OsString>>),
}

impl PathFingerprint {
    fn directory(path: &Path) -> Result<Self, RuntimeInventoryError> {
        directory_entries(path)
            .map(Self::DirectoryEntries)
            .map_err(|source| RuntimeInventoryError::ReadDir {
                path: path.to_path_buf(),
                source,
            })
    }

    fn matches(&self, path: &Path) -> bool {
        match self {
            Self::Metadata(expected) => {
                std::fs::metadata(path)
                    .ok()
                    .as_ref()
                    .map(FileFingerprint::of)
                    == *expected
            }
            Self::DirectoryEntries(expected) => {
                directory_entries(path).is_ok_and(|entries| entries == *expected)
            }
        }
    }
}

fn directory_entries(path: &Path) -> std::io::Result<Option<BTreeSet<OsString>>> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    entries
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<_>>()
        .map(Some)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FileFingerprint {
    device: u64,
    inode: u64,
    size: u64,
    modified: (i64, i64),
    mode: u32,
    owner: u32,
}

impl FileFingerprint {
    fn of(md: &std::fs::Metadata) -> Self {
        Self {
            device: md.dev(),
            inode: md.ino(),
            // Adding an unrelated candidate changes a directory's timestamps,
            // not the identity or protection of its existing children.
            size: if md.is_dir() { 0 } else { md.len() },
            modified: if md.is_dir() {
                (0, 0)
            } else {
                (md.mtime(), md.mtime_nsec())
            },
            mode: md.mode(),
            owner: md.uid(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInstallationRoot {
    pub kind: RuntimeKind,
    pub path: PathBuf,
}

impl RuntimeInstallationRoot {
    pub fn new(kind: RuntimeKind, path: impl Into<PathBuf>) -> Self {
        Self {
            kind,
            path: path.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionSelectionMode {
    NewColdBoot,
    ExistingColdBoot,
    PersistedStateRestore,
}

pub trait ExecutionResolver {
    fn resolve_execution(
        &self,
        requested: Option<&ExecutionMetadata>,
        runtime: RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, RuntimeInventoryError>;
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeInventoryError {
    #[cfg(feature = "artifact-store")]
    #[error(transparent)]
    StageExecution(#[from] crate::artifact_store::StageExecutionError),
    #[error("execution binding unavailable: {0}")]
    UnavailableBinding(#[source] std::sync::Arc<RuntimeInventoryError>),
    #[cfg(feature = "artifact-store")]
    #[error("managed execution artifact: {0}")]
    ManagedArtifact(#[from] crate::artifact_store::PublishError),
    #[cfg(feature = "artifact-store")]
    #[error("execution artifact verification: {0}")]
    Verification(#[from] crate::artifact_store::VerifyError),
    #[cfg(feature = "artifact-store")]
    #[error(transparent)]
    KernelStaging(#[from] crate::artifact_store::StageKernelError),
    #[error("read runtime directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("kernel root {0} has no parent for its execution cache")]
    ParentlessKernelRoot(PathBuf),
    #[error("no installed {0} runtime found")]
    NoInstalledRuntime(RuntimeKind),
    #[error("runtime binary {path} returned an unparseable version: {output}")]
    InvalidVersionOutput { path: PathBuf, output: String },
    #[error("static runtime inventory identifier {field}={value:?} is invalid: {reason}")]
    InvalidStaticIdentifier {
        field: &'static str,
        value: String,
        reason: String,
    },
    #[error("compiled execution environment is invalid: {0}")]
    EnvironmentDefinition(#[from] crate::catalog::CompiledEnvironmentDefinitionError),
    #[error("resolved execution metadata is invalid: {0}")]
    InvalidExecutionMetadata(#[from] ExecutionMetadataError),
    #[error("Firecracker runtime version {0} has no known snapshot format mapping")]
    UnknownFirecrackerSnapshotFormat(RuntimeVersion),
    #[error("runtime installation {root} is missing required {role} file")]
    IncompleteInstallation {
        root: PathBuf,
        role: RuntimeFileRole,
    },
    #[error("inspect kernel image {path}: {source}")]
    InspectKernel {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("probe runtime inventory path {path}: {source}")]
    ProbeInventoryPath {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("kernel read returned {read} bytes for a {len}-byte buffer")]
    InvalidKernelReadBuffer { read: usize, len: usize },
    #[error("compiled environment has no auxiliary boot-file pin for {0:?}")]
    MissingBootArtifactPin(pplx_space_common_core::types::ComponentIdentity),
    #[error("no installed artifact component matches {0:?}")]
    NoCompatibleComponent(ArtifactComponentMetadata),
    #[error("no {selection} {kind} component is installed")]
    NoDefaultComponent {
        kind: ArtifactComponentKind,
        selection: ComponentDefaultSelection,
    },
    #[error("no installed {runtime} runtime can execute the requested artifact")]
    NoCompatibleRuntime { runtime: RuntimeKind },
    /// The alias exists but does not name a listed release. It cannot supply
    /// a default, and must never imply selection of the newest release.
    #[error("{} does not name an installed runtime release", alias.display())]
    InvalidDefaultRelease { alias: PathBuf },
    #[error("persisted-state restore metadata is missing a state format")]
    MissingStateFormat,
    #[error("{0:?} selection requires requested execution metadata")]
    MissingRequestedExecution(ExecutionSelectionMode),
    #[error("runtime {0} is not supported by the runtime resolver")]
    UnsupportedRuntime(RuntimeKind),
    #[error("host architecture {0} is not supported")]
    UnsupportedHostArchitecture(String),
}

impl RuntimeInventoryError {
    /// Whether a retry of the same resolution against the same node can
    /// plausibly succeed without anyone installing or repairing anything.
    ///
    /// Almost every variant reports the node's installed inventory not
    /// containing what the caller needs, which no amount of retrying changes:
    /// the runtime, kernel, or artifact component has to be installed first.
    /// The exceptions are the variants that report a *read* failing rather than
    /// an inventory verdict, and then only when the read itself is the kind a
    /// later attempt can win — see
    /// [`host_path_io_error_is_transient`](pplx_space_common_core::host::path_errors::host_path_io_error_is_transient).
    ///
    /// This distinction is load-bearing in two directions. A non-transient
    /// error becomes a terminal `FailedPrecondition`, which sandbox-manager
    /// forwards to its caller rather than flattening — so a control-plane
    /// restore moves to another node instead of re-attempting this one. A
    /// transient one stays `Internal` and keeps the retry that would recover
    /// it. Calling a recoverable read terminal is the costlier mistake: it
    /// fails an activation that a second attempt would have satisfied.
    pub fn is_transient(&self) -> bool {
        match self {
            #[cfg(feature = "artifact-store")]
            Self::StageExecution(error) => error.is_transient(),
            Self::UnavailableBinding(error) => error.is_transient(),
            #[cfg(feature = "artifact-store")]
            Self::ManagedArtifact(error) => error.is_transient(),
            #[cfg(feature = "artifact-store")]
            Self::Verification(error) => error.is_transient(),
            #[cfg(feature = "artifact-store")]
            Self::KernelStaging(error) => error.is_transient(),
            Self::ReadDir { source, .. }
            | Self::InspectKernel { source, .. }
            | Self::ProbeInventoryPath { source, .. } => {
                pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(source)
            }
            // A short read of a kernel image carries no error kind to judge.
            // It is most often a file the prefetch is still writing, so it
            // stays re-readable rather than permanently condemning the
            // installation.
            Self::InvalidKernelReadBuffer { .. } => true,
            Self::ParentlessKernelRoot(_)
            | Self::NoInstalledRuntime(_)
            | Self::InvalidVersionOutput { .. }
            | Self::InvalidStaticIdentifier { .. }
            | Self::EnvironmentDefinition(_)
            | Self::InvalidExecutionMetadata(_)
            | Self::UnknownFirecrackerSnapshotFormat(_)
            | Self::IncompleteInstallation { .. }
            | Self::MissingBootArtifactPin(_)
            | Self::NoCompatibleComponent(_)
            | Self::NoDefaultComponent { .. }
            | Self::NoCompatibleRuntime { .. }
            | Self::InvalidDefaultRelease { .. }
            | Self::MissingStateFormat
            | Self::MissingRequestedExecution(_)
            | Self::UnsupportedRuntime(_)
            | Self::UnsupportedHostArchitecture(_) => false,
        }
    }

    /// Render the error as the gRPC status node-manager returns to its callers.
    ///
    /// `FailedPrecondition` states the node cannot satisfy this request until
    /// its inventory changes; `Internal` keeps the retryable shape for the
    /// read failures [`Self::is_transient`] separates out.
    pub fn into_status(self) -> tonic::Status {
        if self.is_transient() {
            tonic::Status::internal(self.to_string())
        } else {
            tonic::Status::failed_precondition(self.to_string())
        }
    }
}

/// The version a kernel-root entry claims by its directory name, or `None` if
/// the name rules it out as a candidate.
///
/// Name-only and filesystem-free on purpose: it runs before the scan probes
/// anything, so an entry that is not a kernel installation cannot fail the scan
/// with an unlucky stat. `default` and `legacy-default` are the marker symlinks
/// the scan resolves separately, never installations in their own right.
fn kernel_candidate_version(entry_name: &std::ffi::OsStr) -> Option<ArtifactVersion> {
    let entry_name = entry_name.to_str()?;
    if matches!(entry_name, "default" | "legacy-default") {
        return None;
    }
    ArtifactVersion::parse(entry_name).ok()
}

/// What a kernel scan does with a candidate whose image it could not read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UnreadableCandidate {
    /// Abandon the whole scan and report the read failure.
    FailScan,
    /// Leave this candidate out and keep scanning.
    Skip,
}

/// Decide whether an unreadable kernel candidate invalidates the scan.
///
/// Skipping is only safe when the candidate is *permanently* unreadable — the
/// wrong permissions, the wrong object kind. Then it genuinely is not usable
/// and an inventory without it is accurate.
///
/// A transient read is different, and skipping it is what makes the terminal
/// verdicts in this module untrustworthy. Node-manager caches the first
/// successful scan for the life of the process and never re-scans, so a
/// candidate dropped from that one scan is dropped forever: every later
/// resolution asking for it answers `NoCompatibleComponent` or
/// `NoDefaultComponent`, both terminal, long after the read recovered. The
/// scan that could not read a candidate does not know what this node has, so
/// it reports that instead of publishing a half-answer.
fn unreadable_candidate(error: &RuntimeInventoryError) -> UnreadableCandidate {
    if error.is_transient() {
        UnreadableCandidate::FailScan
    } else {
        UnreadableCandidate::Skip
    }
}

/// Canonicalize a path in the runtime inventory, distinguishing "absent"
/// (`Ok(None)`) from "could not be read this time" (`Err`).
///
/// The bare `.ok()` this replaces erased the difference, so a transient
/// failure to resolve the `default` symlink silently produced an inventory
/// with no default kernel — and `NoDefaultComponent` is terminal.
fn canonicalize_inventory_path(path: &Path) -> Result<Option<PathBuf>, RuntimeInventoryError> {
    match std::fs::canonicalize(path) {
        Ok(resolved) => Ok(Some(resolved)),
        Err(source)
            if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(
                &source,
            ) =>
        {
            Err(RuntimeInventoryError::ProbeInventoryPath {
                path: path.to_path_buf(),
                source,
            })
        }
        Err(_) => Ok(None),
    }
}

fn resolve_kernel_marker(
    root: &Path,
    marker: &str,
) -> Result<Option<PathBuf>, RuntimeInventoryError> {
    canonicalize_inventory_path(&root.join(marker))
}

/// Stat a path in the runtime inventory, distinguishing "absent"
/// (`Ok(None)`) from "could not be read this time" (`Err`).
///
/// `Path::is_dir` / `Path::is_file` answer `false` for both, which turns a
/// transient stat failure into a silently skipped installation for the same
/// reason [`unreadable_candidate`] documents.
fn probe_inventory_path(path: &Path) -> Result<Option<std::fs::Metadata>, RuntimeInventoryError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(source)
            if pplx_space_common_core::host::path_errors::host_path_io_error_is_transient(
                &source,
            ) =>
        {
            Err(RuntimeInventoryError::ProbeInventoryPath {
                path: path.to_path_buf(),
                source,
            })
        }
        Err(_) => Ok(None),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentDefaultSelection {
    NewArtifact,
    LegacyArtifact,
}

impl std::fmt::Display for ComponentDefaultSelection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::NewArtifact => "new-artifact default",
            Self::LegacyArtifact => "legacy-artifact default",
        })
    }
}

trait RuntimeDriver: Send + Sync {
    fn restore_requires_boot_components(&self) -> bool;
    fn kind(&self) -> RuntimeKind;
    #[cfg(any(feature = "artifact-store", test))]
    fn inspect_installation(
        &self,
        root: PathBuf,
        version: RuntimeVersion,
    ) -> Result<RuntimeInstallation, RuntimeInventoryError>;
    fn discover_installations(
        &self,
        root: &Path,
    ) -> Result<Vec<RuntimeInstallation>, RuntimeInventoryError>;
    /// The release the root's `default` alias names, or `None` when the root
    /// has no alias. An alias that exists but does not resolve to a release
    /// directory is an error.
    fn default_release(&self, root: &Path)
    -> Result<Option<RuntimeVersion>, RuntimeInventoryError>;
    fn compare_versions(
        &self,
        left: &RuntimeVersion,
        right: &RuntimeVersion,
    ) -> Result<Ordering, RuntimeInventoryError>;
    /// A portable key that orders as `compare_versions` does, for versions
    /// whose order fits integer components. `None` publishes no order.
    fn version_sort_key(&self, version: &RuntimeVersion) -> Option<VersionSortKey>;
    fn can_execute(
        &self,
        installed: &RuntimeInstallation,
        requested: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<bool, RuntimeInventoryError>;
}

struct FirecrackerDriver;

impl FirecrackerDriver {
    fn parse_version(
        &self,
        version: &RuntimeVersion,
    ) -> Result<semver::Version, RuntimeInventoryError> {
        semver::Version::parse(
            version
                .as_str()
                .strip_prefix('v')
                .unwrap_or(version.as_str()),
        )
        .map_err(|source| RuntimeInventoryError::InvalidVersionOutput {
            path: PathBuf::from("firecracker"),
            output: format!("{version}: {source}"),
        })
    }

    fn parse_state_version(
        &self,
        version: &StateFormatVersion,
    ) -> Result<semver::Version, RuntimeInventoryError> {
        semver::Version::parse(
            version
                .as_str()
                .strip_prefix('v')
                .unwrap_or(version.as_str()),
        )
        .map_err(|source| RuntimeInventoryError::InvalidVersionOutput {
            path: PathBuf::from("firecracker --snapshot-version"),
            output: format!("{version}: {source}"),
        })
    }

    fn state_format_version(
        &self,
        runtime_version: &RuntimeVersion,
    ) -> Result<StateFormatVersion, RuntimeInventoryError> {
        let release = runtime_version
            .as_str()
            .strip_prefix('v')
            .unwrap_or(runtime_version.as_str());
        let state_format_version = FIRECRACKER_STATE_FORMAT_RELEASES
            .iter()
            .find(|entry| entry.runtime_version == release)
            .map(|entry| entry.state_format_version)
            .ok_or_else(|| {
                RuntimeInventoryError::UnknownFirecrackerSnapshotFormat(runtime_version.clone())
            })?;
        StateFormatVersion::parse(state_format_version).map_err(|source| {
            RuntimeInventoryError::InvalidStaticIdentifier {
                field: "state_format_version",
                value: state_format_version.to_string(),
                reason: source.to_string(),
            }
        })
    }

    fn inspect_installation(
        &self,
        root: PathBuf,
        directory_version: RuntimeVersion,
    ) -> Result<RuntimeInstallation, RuntimeInventoryError> {
        let firecracker_bin = root.join("bin/firecracker");
        let jailer_bin = root.join("bin/jailer");
        for (binary, role) in [
            (&firecracker_bin, RuntimeFileRole::RuntimeExecutable),
            (&jailer_bin, RuntimeFileRole::LauncherExecutable),
        ] {
            // `IncompleteInstallation` is a terminal verdict, so it may only be
            // reported when the binary is genuinely absent — a transient stat
            // propagates instead.
            if !probe_inventory_path(binary)?.is_some_and(|metadata| metadata.is_file()) {
                return Err(RuntimeInventoryError::IncompleteInstallation { root, role });
            }
        }

        self.parse_version(&directory_version)?;
        let state_format_version = self.state_format_version(&directory_version)?;
        self.parse_state_version(&state_format_version)?;

        Ok(RuntimeInstallation {
            runtime: RuntimeIdentity {
                kind: RuntimeKind::Firecracker,
                version: directory_version,
            },
            guest: GuestPlatform {
                os: GuestOperatingSystem::Linux,
                architecture: host_arch()?,
            },
            state_format: Some(StateFormatIdentity {
                format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).map_err(|source| {
                    RuntimeInventoryError::InvalidStaticIdentifier {
                        field: "state_format_name",
                        value: FIRECRACKER_STATE_FORMAT.to_string(),
                        reason: source.to_string(),
                    }
                })?,
                version: state_format_version,
            }),
            files: RuntimeInstallationFiles::new([
                (RuntimeFileRole::RuntimeExecutable, firecracker_bin),
                (RuntimeFileRole::LauncherExecutable, jailer_bin),
            ]),
        })
    }
}

impl RuntimeDriver for FirecrackerDriver {
    fn restore_requires_boot_components(&self) -> bool {
        false
    }
    #[cfg(any(feature = "artifact-store", test))]
    fn inspect_installation(
        &self,
        root: PathBuf,
        version: RuntimeVersion,
    ) -> Result<RuntimeInstallation, RuntimeInventoryError> {
        FirecrackerDriver::inspect_installation(self, root, version)
    }
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    fn discover_installations(
        &self,
        root: &Path,
    ) -> Result<Vec<RuntimeInstallation>, RuntimeInventoryError> {
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(source) => {
                return Err(RuntimeInventoryError::ReadDir {
                    path: root.to_path_buf(),
                    source,
                });
            }
        };
        let mut runtimes = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|source| RuntimeInventoryError::ReadDir {
                path: root.to_path_buf(),
                source,
            })?;
            let path = entry.path();
            // Screen by name before touching the filesystem, for the reason
            // `kernel_candidate_version` documents.
            if entry.file_name() == "default"
                || entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with('.'))
            {
                continue;
            }
            let Some(directory_version) = version_from_dir(&path) else {
                continue;
            };
            if !probe_inventory_path(&path)?.is_some_and(|metadata| metadata.is_dir()) {
                continue;
            }
            match self.inspect_installation(path, directory_version) {
                Ok(installation) => runtimes.push(installation),
                Err(error) if unreadable_candidate(&error) == UnreadableCandidate::FailScan => {
                    return Err(error);
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        "skipping invalid Firecracker runtime installation"
                    );
                }
            }
        }
        Ok(runtimes)
    }

    fn default_release(
        &self,
        root: &Path,
    ) -> Result<Option<RuntimeVersion>, RuntimeInventoryError> {
        let alias = root.join("default");
        let probe = |source| RuntimeInventoryError::ProbeInventoryPath {
            path: alias.clone(),
            source,
        };
        match std::fs::symlink_metadata(&alias) {
            Ok(_) => {}
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(probe(source)),
        }
        let invalid = || RuntimeInventoryError::InvalidDefaultRelease {
            alias: alias.clone(),
        };
        let resolved = match std::fs::canonicalize(&alias) {
            Ok(resolved) => resolved,
            // A dangling alias.
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
                return Err(invalid());
            }
            Err(source) => return Err(probe(source)),
        };
        version_from_dir(&resolved).map(Some).ok_or_else(invalid)
    }

    fn compare_versions(
        &self,
        left: &RuntimeVersion,
        right: &RuntimeVersion,
    ) -> Result<Ordering, RuntimeInventoryError> {
        Ok(self.parse_version(left)?.cmp(&self.parse_version(right)?))
    }

    fn version_sort_key(&self, version: &RuntimeVersion) -> Option<VersionSortKey> {
        semver_sort_key(&self.parse_version(version).ok()?)
    }

    fn can_execute(
        &self,
        installed: &RuntimeInstallation,
        requested: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<bool, RuntimeInventoryError> {
        if installed.runtime.kind != self.kind()
            || requested.runtime().kind != self.kind()
            || installed.guest != requested.guest()
        {
            return Ok(false);
        }

        if mode != ExecutionSelectionMode::PersistedStateRestore {
            return Ok(true);
        }
        let requested_state = requested
            .state_format()
            .ok_or(RuntimeInventoryError::MissingStateFormat)?;
        let Some(installed_format) = &installed.state_format else {
            return Ok(false);
        };
        if installed_format.format != requested_state.format
            || installed_format.format.as_str() != FIRECRACKER_STATE_FORMAT
        {
            return Ok(false);
        }
        let installed_state = self.parse_state_version(&installed_format.version)?;
        let requested_state = self.parse_state_version(&requested_state.version)?;
        Ok(installed_state.major == requested_state.major
            && requested_state.minor <= installed_state.minor)
    }
}

struct RuntimeDriverRegistry {
    firecracker: FirecrackerDriver,
    cloud_hypervisor: cloud_hypervisor::CloudHypervisorDriver,
}

impl RuntimeDriverRegistry {
    const fn new() -> Self {
        Self {
            firecracker: FirecrackerDriver,
            cloud_hypervisor: cloud_hypervisor::CloudHypervisorDriver,
        }
    }

    fn get(&self, kind: RuntimeKind) -> Result<&dyn RuntimeDriver, RuntimeInventoryError> {
        match kind {
            RuntimeKind::Firecracker => Ok(&self.firecracker),
            RuntimeKind::CloudHypervisor => Ok(&self.cloud_hypervisor),
            RuntimeKind::Gvisor => Err(RuntimeInventoryError::UnsupportedRuntime(kind)),
        }
    }
}

static DRIVERS: RuntimeDriverRegistry = RuntimeDriverRegistry::new();

/// The runtime driver's ordering key for a release, when it defines one.
fn semver_sort_key(version: &semver::Version) -> Option<VersionSortKey> {
    // A pre-release sorts before its release, which [major, minor, patch]
    // can't express; build metadata has no defined order.
    if !version.pre.is_empty() || !version.build.is_empty() {
        return None;
    }
    let component = |value: u64| u32::try_from(value).ok();
    VersionSortKey::new(vec![
        component(version.major)?,
        component(version.minor)?,
        component(version.patch)?,
    ])
    .ok()
}

pub(crate) fn version_sort_key(runtime: &RuntimeIdentity) -> Option<VersionSortKey> {
    DRIVERS
        .get(runtime.kind)
        .ok()?
        .version_sort_key(&runtime.version)
}

impl RuntimeInventory {
    /// Root-owned artifacts are immutable; missing/replaced files invalidate a
    /// cached snapshot before it can be used by admission or observation.
    fn paths(&self) -> impl Iterator<Item = &Path> {
        self.runtimes
            .iter()
            .flat_map(|entry| entry.files.files.values().map(PathBuf::as_path))
            .chain(self.components.iter().map(|entry| entry.path.as_path()))
            .chain(self.environments.values().flat_map(|environment| {
                environment
                    .runtime
                    .iter()
                    .flat_map(|runtime| runtime.files.paths())
                    .chain(
                        environment
                            .components
                            .iter()
                            .flatten()
                            .map(|entry| entry.path.as_path()),
                    )
            }))
    }

    /// Blocking validation of root-controlled file metadata and, for external
    /// discovery, directory entry names. Does not rehash artifact contents.
    /// ctime is deliberately excluded: each jail's kernel hard link changes it
    /// without changing the approved bytes or their ownership.
    pub fn check_files_unchanged(&self) -> bool {
        self.retry_after
            .is_none_or(|deadline| tokio::time::Instant::now() < deadline)
            && self
                .fingerprints
                .iter()
                .all(|(path, fingerprint)| fingerprint.matches(path))
    }

    // Observe unavailable bindings too: repairing a missing file or unsafe
    // directory must invalidate negative results without restarting services.
    fn remember_paths(&mut self, additional: impl IntoIterator<Item = PathBuf>) {
        let paths: std::collections::BTreeSet<_> = self
            .paths()
            .map(Path::to_path_buf)
            .chain(additional)
            .flat_map(|path| {
                path.ancestors()
                    .take(4)
                    .map(Path::to_path_buf)
                    .collect::<Vec<_>>()
            })
            .collect();
        self.fingerprints = paths
            .into_iter()
            .map(|path| {
                let observed = std::fs::metadata(&path)
                    .ok()
                    .as_ref()
                    .map(FileFingerprint::of);
                (path, PathFingerprint::Metadata(observed))
            })
            .collect();
    }

    fn first_compatible<'a>(
        &'a self,
        driver: &dyn RuntimeDriver,
        requested: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<Option<&'a RuntimeInstallation>, RuntimeInventoryError> {
        for installation in &self.runtimes {
            if driver.can_execute(installation, requested, mode)? {
                return Ok(Some(installation));
            }
        }
        Ok(None)
    }

    pub fn discover(
        runtime_roots: impl IntoIterator<Item = RuntimeInstallationRoot>,
        asset_root: impl AsRef<Path>,
        kernel_root: impl AsRef<Path>,
    ) -> Result<Self, RuntimeInventoryError> {
        let asset_root = asset_root.as_ref();
        let kernel_root = kernel_root.as_ref();
        // Observe entries before scanning: an installation published during
        // discovery must invalidate the snapshot on the next query.
        let mut roots = vec![(
            kernel_root.to_path_buf(),
            PathFingerprint::directory(kernel_root)?,
        )];
        let mut watched = vec![
            kernel_root.join("default/vmlinux.bin"),
            kernel_root.join("legacy-default/vmlinux.bin"),
            asset_root.join("default/kernel/vmlinux.bin"),
        ];
        let kernels = if probe_inventory_path(kernel_root)?.is_some_and(|md| md.is_dir()) {
            discover_kernels(kernel_root)?
        } else {
            discover_legacy_kernels(asset_root)?
        };
        let mut runtimes = Vec::new();
        let mut defaults = Vec::new();
        for root in runtime_roots {
            roots.push((root.path.clone(), PathFingerprint::directory(&root.path)?));
            // Only Firecracker has mutable external/default aliases. CH
            // execution is discovered from pinned managed bindings, whose
            // complete binary paths are watched by include_environment.
            if root.kind == RuntimeKind::Firecracker {
                watched.extend([
                    root.path.join("default/bin/firecracker"),
                    root.path.join("default/bin/jailer"),
                ]);
            }
            let driver = DRIVERS.get(root.kind)?;
            let installations = driver.discover_installations(&root.path)?;
            let default = match driver.default_release(&root.path) {
                Ok(default) => default,
                Err(error) if error.is_transient() => return Err(error),
                Err(error) => {
                    tracing::warn!(%error, "ignoring invalid runtime default alias");
                    None
                }
            };
            if let Some(version) = default {
                let default = RuntimeIdentity {
                    kind: root.kind,
                    version,
                };
                if !installations
                    .iter()
                    .any(|installation| installation.runtime == default)
                {
                    tracing::warn!(path = %root.path.display(), "ignoring default alias without an installed release");
                } else {
                    defaults.push(default);
                }
            }
            runtimes.extend(installations);
        }
        runtimes.sort_by(|left, right| {
            if left.runtime.kind == right.runtime.kind {
                DRIVERS
                    .get(left.runtime.kind)
                    .and_then(|driver| {
                        driver.compare_versions(&right.runtime.version, &left.runtime.version)
                    })
                    .unwrap_or(Ordering::Equal)
            } else {
                left.runtime.kind.cmp(&right.runtime.kind)
            }
        });
        let mut inventory = Self {
            runtimes,
            components: kernels,
            defaults,
            fingerprints: Vec::new(),
            environments: BTreeMap::new(),
            #[cfg(feature = "artifact-store")]
            unavailable: Vec::new(),
            retry_after: None,
            initial_environment: None,
        };
        inventory.remember_paths(watched);
        // External discovery must notice new installations even when the
        // previous scan found nothing. Managed discovery replaces these root
        // watches with exact binding paths so candidates do not trigger hashing.
        inventory.fingerprints.extend(roots);
        Ok(inventory)
    }

    /// Look up the exact installed runtime identity, without default selection
    /// or compatibility substitution.
    pub fn runtime_installation(&self, identity: &RuntimeIdentity) -> Option<&RuntimeInstallation> {
        self.runtimes
            .iter()
            .find(|installation| &installation.runtime == identity)
    }

    /// The installed release a runtime's `default` alias names.
    fn default_installation(&self, runtime: RuntimeKind) -> Option<&RuntimeInstallation> {
        let default = self
            .defaults
            .iter()
            .find(|default| default.kind == runtime)?;
        self.runtime_installation(default)
    }

    /// Restore may use the selected loader or another compatible installed
    /// loader. Cold boots never use this fallback.
    fn default_or_first_compatible<'a>(
        &'a self,
        driver: &dyn RuntimeDriver,
        runtime: RuntimeKind,
        requested: &ExecutionMetadata,
        mode: ExecutionSelectionMode,
    ) -> Result<Option<&'a RuntimeInstallation>, RuntimeInventoryError> {
        if let Some(default) = self.default_installation(runtime)
            && driver.can_execute(default, requested, mode)?
        {
            return Ok(Some(default));
        }
        self.first_compatible(driver, requested, mode)
    }

    pub fn resolve(
        &self,
        requested: Option<&ExecutionMetadata>,
        runtime: RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, RuntimeInventoryError> {
        let result = self.resolve_available(requested, runtime, mode);
        #[cfg(feature = "artifact-store")]
        let result = result.map_err(|failure| {
            // Preserve retryability and diagnostics for the affected binding,
            // without making a healthy explicit binding depend on E0.
            for unavailable in &self.unavailable {
                let source = match unavailable {
                    UnavailableArtifact::Runtime { identity, error }
                        if matches!(
                            failure,
                            RuntimeInventoryError::NoInstalledRuntime(_)
                                | RuntimeInventoryError::NoCompatibleRuntime { .. }
                        ) && identity.kind == runtime
                            && ((mode == ExecutionSelectionMode::NewColdBoot
                                && requested
                                    .is_none_or(|metadata| metadata.environment().is_none()))
                                || requested
                                    .is_some_and(|metadata| metadata.runtime() == identity)) =>
                    {
                        Some(error)
                    }
                    UnavailableArtifact::Kernel {
                        digest,
                        legacy,
                        error,
                    } => match &failure {
                        RuntimeInventoryError::NoCompatibleComponent(component)
                            if component.digest == Some(*digest) =>
                        {
                            Some(error)
                        }
                        RuntimeInventoryError::NoDefaultComponent { selection, .. }
                            if *selection == ComponentDefaultSelection::NewArtifact || *legacy =>
                        {
                            Some(error)
                        }
                        _ => None,
                    },
                    _ => None,
                };
                if let Some(source) = source {
                    return RuntimeInventoryError::UnavailableBinding(source.clone());
                }
            }
            failure
        });
        result
    }

    fn resolve_available(
        &self,
        requested: Option<&ExecutionMetadata>,
        runtime: RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, RuntimeInventoryError> {
        let binding = requested
            .and_then(|metadata| metadata.environment())
            .and_then(|reference| self.environments.get(&reference));
        let runtimes = match binding {
            Some(environment) => std::slice::from_ref(
                environment
                    .runtime
                    .as_ref()
                    .map_err(|error| RuntimeInventoryError::UnavailableBinding(error.clone()))?,
            ),
            None => &self.runtimes,
        };
        let driver = DRIVERS.get(runtime)?;
        // Firecracker restores from RAM without reopening its kernel. CH also
        // validates the saved boot configuration and installs Landlock rules
        // for its payload, so its restore needs the authenticated boot files.
        let omit_boot_components = mode == ExecutionSelectionMode::PersistedStateRestore
            && !driver.restore_requires_boot_components();
        let components = match binding {
            Some(environment) if !omit_boot_components => environment
                .components
                .as_ref()
                .map_err(|error| RuntimeInventoryError::UnavailableBinding(error.clone()))?
                .as_slice(),
            Some(environment) => environment
                .components
                .as_ref()
                .map(Vec::as_slice)
                .unwrap_or(&[]),
            None => &self.components,
        };
        let find_runtime = |identity: &RuntimeIdentity| {
            runtimes
                .iter()
                .find(|installation| &installation.runtime == identity)
        };
        if requested.is_none() && mode != ExecutionSelectionMode::NewColdBoot {
            return Err(RuntimeInventoryError::MissingRequestedExecution(mode));
        }
        if mode == ExecutionSelectionMode::PersistedStateRestore
            && requested.is_some_and(|execution| execution.state_format().is_none())
        {
            return Err(RuntimeInventoryError::MissingStateFormat);
        }
        let selected_runtime = match requested {
            None => self
                .default_installation(runtime)
                .ok_or(RuntimeInventoryError::NoInstalledRuntime(runtime))?,
            Some(requested) => {
                if requested.runtime().kind != runtime {
                    return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                }
                match mode {
                    ExecutionSelectionMode::NewColdBoot if requested.environment().is_none() => {
                        let selected = self
                            .default_installation(runtime)
                            .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?;
                        if !driver.can_execute(selected, requested, mode)? {
                            return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                        }
                        selected
                    }
                    ExecutionSelectionMode::ExistingColdBoot
                    | ExecutionSelectionMode::NewColdBoot => {
                        let installation = find_runtime(requested.runtime())
                            .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?;
                        if !driver.can_execute(installation, requested, mode)? {
                            return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                        }
                        installation
                    }
                    ExecutionSelectionMode::PersistedStateRestore => {
                        let exact = find_runtime(requested.runtime());
                        if let Some(installation) = exact
                            && driver.can_execute(installation, requested, mode)?
                        {
                            installation
                        } else if binding.is_none() {
                            self.default_or_first_compatible(driver, runtime, requested, mode)?
                                .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?
                        } else {
                            return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                        }
                    }
                }
            }
        };

        let mut resolved_components = Vec::new();
        let mut selected_components = Vec::new();
        if let Some(requested) = requested {
            for component in requested.components() {
                if omit_boot_components && component.kind == ArtifactComponentKind::Kernel {
                    resolved_components.push(component.clone());
                    continue;
                }
                let installed = components
                    .iter()
                    .find(|installed| component_matches(&installed.metadata, component))
                    .ok_or_else(|| {
                        RuntimeInventoryError::NoCompatibleComponent(component.clone())
                    })?;
                resolved_components.push(installed.metadata.clone());
                selected_components.push(ResolvedArtifactComponent {
                    metadata: installed.metadata.clone(),
                    path: installed.path.clone(),
                    integrity: installed.integrity,
                });
            }
        }
        if !omit_boot_components && binding.is_none() {
            let boot_role = boot_component_role();
            let requested_kernel = requested.and_then(|metadata| {
                metadata.component(ArtifactComponentKind::Kernel, Some(&boot_role))
            });
            let kernel = if let Some(requested_kernel) = requested_kernel {
                components
                    .iter()
                    .find(|installed| component_matches(&installed.metadata, requested_kernel))
            } else {
                components.iter().find(|installed| {
                    installed.metadata.kind == ArtifactComponentKind::Kernel
                        && if mode == ExecutionSelectionMode::NewColdBoot {
                            installed.default_for_new
                        } else {
                            installed.legacy_default
                        }
                })
            }
            .ok_or_else(|| match requested_kernel {
                Some(component) => RuntimeInventoryError::NoCompatibleComponent(component.clone()),
                None => RuntimeInventoryError::NoDefaultComponent {
                    kind: ArtifactComponentKind::Kernel,
                    selection: if mode == ExecutionSelectionMode::NewColdBoot {
                        ComponentDefaultSelection::NewArtifact
                    } else {
                        ComponentDefaultSelection::LegacyArtifact
                    },
                },
            })?;
            if requested_kernel.is_none() {
                resolved_components.push(kernel.metadata.clone());
            }
            if requested_kernel.is_none() {
                selected_components.push(ResolvedArtifactComponent {
                    metadata: kernel.metadata.clone(),
                    path: kernel.path.clone(),
                    integrity: kernel.integrity,
                });
            }
        }
        let execution = ExecutionMetadata::new(
            selected_runtime.runtime.clone(),
            selected_runtime.guest,
            selected_runtime.state_format.clone(),
            resolved_components,
        )?;
        let environment = if binding.is_some() {
            requested.and_then(ExecutionMetadata::environment)
        } else {
            self.initial_environment
                .as_ref()
                .filter(|definition| {
                    if definition.check_execution(&execution).is_ok() {
                        return true;
                    }
                    // Restore does not consume boot inputs. Authenticate the
                    // initial runtime independently without inventing kernel
                    // provenance in the returned ExecutionMetadata.
                    omit_boot_components && definition.check_runtime(&execution).is_ok()
                })
                .map(|definition| definition.reference())
        };
        Ok(ResolvedExecutionEnvironment {
            environment,
            execution,
            runtime_files: selected_runtime.files.clone(),
            components: selected_components,
        })
    }

    /// Legacy placement evidence. Additional environment-only artifacts are
    /// available through explicit resolution, never as unselected capacity.
    pub fn to_proto(&self) -> pplx_space_execution_common::proto::NodeExecutionInventory {
        use pplx_space_execution_common::proto;

        let runtimes = self
            .runtimes
            .iter()
            .map(|installation| proto::RuntimeInstallation {
                runtime: Some((&installation.runtime).into()),
                supported_guests: vec![installation.guest.into()],
                state_formats: installation
                    .state_format
                    .as_ref()
                    .map(|format| proto::StateFormatCapability {
                        identity: Some(format.into()),
                        can_create: true,
                        can_load: true,
                    })
                    .into_iter()
                    .collect(),
                version_sort_key: VersionSortKey::to_wire(
                    version_sort_key(&installation.runtime).as_ref(),
                ),
            })
            .collect();
        let components = self
            .components
            .iter()
            .map(|installation| proto::ArtifactComponentInstallation {
                component: Some((&installation.metadata).into()),
                default_for_new: installation.default_for_new,
                legacy_default: installation.legacy_default,
            })
            .collect();
        proto::NodeExecutionInventory {
            policy: None,
            environments: None,
            runtimes,
            components,
        }
    }
}

impl ExecutionResolver for RuntimeInventory {
    fn resolve_execution(
        &self,
        requested: Option<&ExecutionMetadata>,
        runtime: RuntimeKind,
        mode: ExecutionSelectionMode,
    ) -> Result<ResolvedExecutionEnvironment, RuntimeInventoryError> {
        self.resolve(requested, runtime, mode)
    }
}

fn component_matches(
    installed: &ArtifactComponentMetadata,
    requested: &ArtifactComponentMetadata,
) -> bool {
    installed.kind == requested.kind
        && installed.role == requested.role
        && requested
            .digest
            .map(|digest| installed.digest == Some(digest))
            .unwrap_or_else(|| {
                requested
                    .version
                    .as_ref()
                    .is_none_or(|version| installed.version.as_ref() == Some(version))
                    && requested
                        .format
                        .as_ref()
                        .is_none_or(|format| installed.format.as_ref() == Some(format))
            })
}

fn component_metadata_identity(
    component: &ArtifactComponentMetadata,
) -> (ArtifactComponentKind, Option<&ArtifactComponentRole>) {
    (component.kind, component.role.as_ref())
}

fn version_from_dir(path: &Path) -> Option<RuntimeVersion> {
    path.file_name()
        .and_then(|value| value.to_str())
        .and_then(|value| RuntimeVersion::parse(value.strip_prefix('v').unwrap_or(value)).ok())
}

/// Architecture of the host running execution inventory and launch admission.
pub fn host_arch() -> Result<Arch, RuntimeInventoryError> {
    match std::env::consts::ARCH {
        "x86_64" => Ok(Arch::X86_64),
        "aarch64" => Ok(Arch::Aarch64),
        other => Err(RuntimeInventoryError::UnsupportedHostArchitecture(
            other.to_string(),
        )),
    }
}

type KernelInstallation = ArtifactComponentInstallation;

fn kernel_installation(
    metadata: ArtifactComponentMetadata,
    path: PathBuf,
    default_for_new: bool,
    legacy_default: bool,
) -> KernelInstallation {
    ArtifactComponentInstallation {
        metadata,
        path,
        default_for_new,
        legacy_default,
        integrity: ExecutionSourceIntegrity::UnverifiedExternal,
    }
}

fn kernel_metadata(
    version: ArtifactVersion,
    digest: Digest,
) -> Result<ArtifactComponentMetadata, RuntimeInventoryError> {
    Ok(ArtifactComponentMetadata {
        kind: ArtifactComponentKind::Kernel,
        role: Some(
            ArtifactComponentRole::parse(BOOT_COMPONENT_ROLE).map_err(|source| {
                RuntimeInventoryError::InvalidStaticIdentifier {
                    field: "artifact_component_role",
                    value: BOOT_COMPONENT_ROLE.to_string(),
                    reason: source.to_string(),
                }
            })?,
        ),
        version: Some(version),
        digest: Some(digest),
        format: Some(
            ArtifactFormat::parse(VMLINUX_COMPONENT_FORMAT).map_err(|source| {
                RuntimeInventoryError::InvalidStaticIdentifier {
                    field: "artifact_format",
                    value: VMLINUX_COMPONENT_FORMAT.to_string(),
                    reason: source.to_string(),
                }
            })?,
        ),
    })
}

fn discover_kernels(root: &Path) -> Result<Vec<KernelInstallation>, RuntimeInventoryError> {
    let default = resolve_kernel_marker(root, "default")?;
    let legacy_default = resolve_kernel_marker(root, "legacy-default")?.or_else(|| default.clone());
    let entries = std::fs::read_dir(root).map_err(|source| RuntimeInventoryError::ReadDir {
        path: root.to_path_buf(),
        source,
    })?;
    let mut kernels = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| RuntimeInventoryError::ReadDir {
            path: root.to_path_buf(),
            source,
        })?;
        // Disqualify by name before touching the filesystem. An entry the name
        // alone rules out is not a candidate, so a transient stat error on it
        // must not be able to abandon a scan that would otherwise succeed —
        // and the name check costs nothing.
        let path = entry.path();
        let Some(version) = kernel_candidate_version(&entry.file_name()) else {
            continue;
        };
        if !probe_inventory_path(&path)?.is_some_and(|metadata| metadata.is_dir()) {
            continue;
        }
        let kernel = path.join("vmlinux.bin");
        if !probe_inventory_path(&kernel)?.is_some_and(|metadata| metadata.is_file()) {
            tracing::warn!(
                path = %path.display(),
                "skipping incomplete kernel installation"
            );
            continue;
        }
        let digest = match sha256_file(&kernel) {
            Ok(digest) => digest,
            Err(error) => match unreadable_candidate(&error) {
                UnreadableCandidate::FailScan => return Err(error),
                UnreadableCandidate::Skip => {
                    tracing::warn!(%error, "skipping unreadable kernel installation");
                    continue;
                }
            },
        };
        let canonical_path = canonicalize_inventory_path(&path)?;
        kernels.push(kernel_installation(
            kernel_metadata(version, digest)?,
            kernel,
            default
                .as_ref()
                .is_some_and(|default| canonical_path.as_ref() == Some(default)),
            legacy_default
                .as_ref()
                .is_some_and(|legacy_default| canonical_path.as_ref() == Some(legacy_default)),
        ));
    }
    Ok(kernels)
}

fn discover_legacy_kernels(
    asset_root: &Path,
) -> Result<Vec<KernelInstallation>, RuntimeInventoryError> {
    let kernel = asset_root.join("default/kernel/vmlinux.bin");
    if !probe_inventory_path(&kernel)?.is_some_and(|metadata| metadata.is_file()) {
        return Ok(Vec::new());
    }
    let digest = sha256_file(&kernel)?;
    let digest_hex = digest.hex_string();
    let legacy_suffix =
        digest_hex
            .get(..12)
            .ok_or_else(|| RuntimeInventoryError::InvalidStaticIdentifier {
                field: "legacy_kernel_digest_prefix",
                value: digest_hex.clone(),
                reason: "digest hex string is shorter than 12 bytes".to_string(),
            })?;
    let legacy_version = format!("legacy-{legacy_suffix}");
    Ok(vec![kernel_installation(
        kernel_metadata(
            ArtifactVersion::parse(&legacy_version).map_err(|source| {
                RuntimeInventoryError::InvalidStaticIdentifier {
                    field: "legacy_kernel_version",
                    value: legacy_version,
                    reason: source.to_string(),
                }
            })?,
            digest,
        )?,
        kernel,
        true,
        true,
    )])
}

fn sha256_file(path: &Path) -> Result<Digest, RuntimeInventoryError> {
    let mut file =
        std::fs::File::open(path).map_err(|source| RuntimeInventoryError::InspectKernel {
            path: path.to_path_buf(),
            source,
        })?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read =
            file.read(&mut buffer)
                .map_err(|source| RuntimeInventoryError::InspectKernel {
                    path: path.to_path_buf(),
                    source,
                })?;
        if read == 0 {
            break;
        }
        let chunk = buffer
            .get(..read)
            .ok_or(RuntimeInventoryError::InvalidKernelReadBuffer {
                read,
                len: buffer.len(),
            })?;
        hasher.update(chunk);
    }
    Ok(Digest::sha256(hasher.finalize().into()))
}

#[cfg(test)]
mod tests;
