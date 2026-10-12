use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole, ArtifactFormat,
    ArtifactVersion, Digest, ExecutionMetadata, ExecutionMetadataError, FIRECRACKER_STATE_FORMAT,
    GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind, RuntimeVersion,
    StateFormatIdentity, StateFormatName, StateFormatVersion,
};
use sha2::{Digest as _, Sha256};

pub mod diagnostic;
pub mod kernel_release;

const BOOT_COMPONENT_ROLE: &str = "boot";
const VMLINUX_COMPONENT_FORMAT: &str = "vmlinux";

pub fn boot_component_role() -> ArtifactComponentRole {
    ArtifactComponentRole::parse(BOOT_COMPONENT_ROLE).expect("boot component role is valid")
}

struct FirecrackerStateFormatRelease {
    runtime_version: &'static str,
    state_format_version: &'static str,
}

const FIRECRACKER_STATE_FORMAT_RELEASES: &[FirecrackerStateFormatRelease] = &[
    FirecrackerStateFormatRelease {
        runtime_version: "1.15.1",
        state_format_version: "9.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.0",
        state_format_version: "10.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.1",
        state_format_version: "10.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.16.2",
        state_format_version: "11.0.0",
    },
    FirecrackerStateFormatRelease {
        runtime_version: "1.17.0",
        state_format_version: "12.0.0",
    },
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeInstallation {
    pub runtime: RuntimeIdentity,
    pub guest: GuestPlatform,
    pub state_format: StateFormatIdentity,
    pub files: RuntimeInstallationFiles,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RuntimeFileRole {
    FirecrackerBinary,
    JailerBinary,
}

impl RuntimeFileRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FirecrackerBinary => "firecracker",
            Self::JailerBinary => "jailer",
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
}

impl RuntimeInstallationFiles {
    fn new(files: impl IntoIterator<Item = (RuntimeFileRole, PathBuf)>) -> Self {
        Self {
            files: files.into_iter().collect(),
        }
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedExecutionEnvironment {
    pub execution: ExecutionMetadata,
    pub runtime_files: RuntimeInstallationFiles,
    pub components: Vec<ResolvedArtifactComponent>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedArtifactComponent {
    pub metadata: ArtifactComponentMetadata,
    pub path: PathBuf,
}

impl ResolvedExecutionEnvironment {
    pub fn component_path(
        &self,
        kind: ArtifactComponentKind,
        role: Option<&ArtifactComponentRole>,
    ) -> Option<&Path> {
        self.components
            .iter()
            .find(|component| component_metadata_identity(&component.metadata) == (kind, role))
            .map(|component| component.path.as_path())
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeInventory {
    runtimes: Vec<RuntimeInstallation>,
    components: Vec<ArtifactComponentInstallation>,
    /// The release each runtime root's `default` alias names. New cold boots
    /// use it, so installing another release never changes what they boot.
    defaults: Vec<RuntimeIdentity>,
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
    #[error("read runtime directory {path}: {source}")]
    ReadDir {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("no valid runtime installations found under {0}")]
    Empty(PathBuf),
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
    #[error("resolved execution metadata is invalid: {0}")]
    InvalidExecutionMetadata(#[from] ExecutionMetadataError),
    #[error("Firecracker runtime version {0} has no known snapshot format mapping")]
    UnknownFirecrackerSnapshotFormat(RuntimeVersion),
    #[error("runtime installation {root} is missing required {role} file")]
    IncompleteInstallation {
        root: PathBuf,
        role: RuntimeFileRole,
    },
    #[error("kernel installation {0} is missing vmlinux.bin")]
    IncompleteKernelInstallation(PathBuf),
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
    #[error("no installed artifact component matches {0:?}")]
    NoCompatibleComponent(ArtifactComponentMetadata),
    #[error("no {selection} {kind} component is installed")]
    NoDefaultComponent {
        kind: ArtifactComponentKind,
        selection: ComponentDefaultSelection,
    },
    #[error("no installed {runtime} runtime can execute the requested artifact")]
    NoCompatibleRuntime { runtime: RuntimeKind },
    /// The alias exists but does not name a listed release. Without an alias
    /// the newest release is used; a broken one is not silently replaced.
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
            Self::Empty(_)
            | Self::NoInstalledRuntime(_)
            | Self::InvalidVersionOutput { .. }
            | Self::InvalidStaticIdentifier { .. }
            | Self::InvalidExecutionMetadata(_)
            | Self::UnknownFirecrackerSnapshotFormat(_)
            | Self::IncompleteInstallation { .. }
            | Self::IncompleteKernelInstallation(_)
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
    fn kind(&self) -> RuntimeKind;
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
            (&firecracker_bin, RuntimeFileRole::FirecrackerBinary),
            (&jailer_bin, RuntimeFileRole::JailerBinary),
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
            state_format: StateFormatIdentity {
                format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT).map_err(|source| {
                    RuntimeInventoryError::InvalidStaticIdentifier {
                        field: "state_format_name",
                        value: FIRECRACKER_STATE_FORMAT.to_string(),
                        reason: source.to_string(),
                    }
                })?,
                version: state_format_version,
            },
            files: RuntimeInstallationFiles::new([
                (RuntimeFileRole::FirecrackerBinary, firecracker_bin),
                (RuntimeFileRole::JailerBinary, jailer_bin),
            ]),
        })
    }
}

impl RuntimeDriver for FirecrackerDriver {
    fn kind(&self) -> RuntimeKind {
        RuntimeKind::Firecracker
    }

    fn discover_installations(
        &self,
        root: &Path,
    ) -> Result<Vec<RuntimeInstallation>, RuntimeInventoryError> {
        let entries = std::fs::read_dir(root).map_err(|source| RuntimeInventoryError::ReadDir {
            path: root.to_path_buf(),
            source,
        })?;
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
                Err(
                    error @ RuntimeInventoryError::UnknownFirecrackerSnapshotFormat(_)
                    | error @ RuntimeInventoryError::InvalidVersionOutput { .. },
                ) => return Err(error),
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
        if installed.state_format.format != requested_state.format
            || installed.state_format.format.as_str() != FIRECRACKER_STATE_FORMAT
        {
            return Ok(false);
        }
        let installed_state = self.parse_state_version(&installed.state_format.version)?;
        let requested_state = self.parse_state_version(&requested_state.version)?;
        Ok(installed_state.major == requested_state.major
            && requested_state.minor <= installed_state.minor)
    }
}

struct RuntimeDriverRegistry {
    firecracker: FirecrackerDriver,
}

impl RuntimeDriverRegistry {
    const fn new() -> Self {
        Self {
            firecracker: FirecrackerDriver,
        }
    }

    fn get(&self, kind: RuntimeKind) -> Result<&dyn RuntimeDriver, RuntimeInventoryError> {
        match kind {
            RuntimeKind::Firecracker => Ok(&self.firecracker),
            RuntimeKind::CloudHypervisor | RuntimeKind::Gvisor => {
                Err(RuntimeInventoryError::UnsupportedRuntime(kind))
            }
        }
    }
}

static DRIVERS: RuntimeDriverRegistry = RuntimeDriverRegistry::new();

impl RuntimeInventory {
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
        let kernels = if probe_inventory_path(kernel_root)?.is_some_and(|md| md.is_dir()) {
            discover_kernels(kernel_root)?
        } else {
            discover_legacy_kernels(asset_root)?
        };
        let mut runtimes = Vec::new();
        let mut defaults = Vec::new();
        let mut searched_roots = Vec::new();
        for root in runtime_roots {
            let driver = DRIVERS.get(root.kind)?;
            searched_roots.push(root.path.clone());
            let installations = driver.discover_installations(&root.path)?;
            if let Some(version) = driver.default_release(&root.path)? {
                let default = RuntimeIdentity {
                    kind: root.kind,
                    version,
                };
                if !installations
                    .iter()
                    .any(|installation| installation.runtime == default)
                {
                    return Err(RuntimeInventoryError::InvalidDefaultRelease {
                        alias: root.path.join("default"),
                    });
                }
                defaults.push(default);
            }
            runtimes.extend(installations);
        }
        if runtimes.is_empty() {
            return Err(RuntimeInventoryError::Empty(
                searched_roots
                    .into_iter()
                    .next()
                    .unwrap_or_else(|| PathBuf::from("<runtime roots>")),
            ));
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
        Ok(Self {
            runtimes,
            components: kernels,
            defaults,
        })
    }

    /// The installed release a runtime's `default` alias names.
    fn default_installation(&self, runtime: RuntimeKind) -> Option<&RuntimeInstallation> {
        let default = self
            .defaults
            .iter()
            .find(|default| default.kind == runtime)?;
        self.runtimes
            .iter()
            .find(|installation| installation.runtime == *default)
    }

    /// The default release when it can serve `requested`, otherwise the
    /// newest that can. A node without a default alias falls back to the
    /// newest, as before defaults were honoured.
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
        let driver = DRIVERS.get(runtime)?;
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
                .or_else(|| {
                    self.runtimes
                        .iter()
                        .find(|installation| installation.runtime.kind == runtime)
                })
                .ok_or(RuntimeInventoryError::NoInstalledRuntime(runtime))?,
            Some(requested) => {
                if requested.runtime().kind != runtime {
                    return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                }
                match mode {
                    ExecutionSelectionMode::NewColdBoot => self
                        .default_or_first_compatible(driver, runtime, requested, mode)?
                        .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?,
                    ExecutionSelectionMode::ExistingColdBoot => {
                        let installation = self
                            .runtimes
                            .iter()
                            .find(|installation| installation.runtime == *requested.runtime())
                            .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?;
                        if !driver.can_execute(installation, requested, mode)? {
                            return Err(RuntimeInventoryError::NoCompatibleRuntime { runtime });
                        }
                        installation
                    }
                    ExecutionSelectionMode::PersistedStateRestore => {
                        let exact = self
                            .runtimes
                            .iter()
                            .find(|installation| installation.runtime == *requested.runtime());
                        if let Some(installation) = exact
                            && driver.can_execute(installation, requested, mode)?
                        {
                            installation
                        } else {
                            self.default_or_first_compatible(driver, runtime, requested, mode)?
                                .ok_or(RuntimeInventoryError::NoCompatibleRuntime { runtime })?
                        }
                    }
                }
            }
        };

        let mut resolved_components = Vec::new();
        let mut selected_components = Vec::new();
        if let Some(requested) = requested {
            for component in requested.components() {
                match self
                    .components
                    .iter()
                    .find(|installed| component_matches(&installed.metadata, component))
                {
                    Some(installed) => resolved_components.push(installed.metadata.clone()),
                    None if mode == ExecutionSelectionMode::PersistedStateRestore
                        && component.kind == ArtifactComponentKind::Kernel =>
                    {
                        tracing::warn!(
                            ?component,
                            "requested restore kernel component is not installed; snapshot restore does not require the boot kernel"
                        );
                        resolved_components.push(component.clone());
                    }
                    None => {
                        return Err(RuntimeInventoryError::NoCompatibleComponent(
                            component.clone(),
                        ));
                    }
                }
            }
        }
        let boot_role = boot_component_role();
        let requested_kernel = requested.and_then(|metadata| {
            metadata.component(ArtifactComponentKind::Kernel, Some(&boot_role))
        });
        let kernel = if let Some(requested_kernel) = requested_kernel
            && mode != ExecutionSelectionMode::PersistedStateRestore
        {
            self.components
                .iter()
                .find(|installed| component_matches(&installed.metadata, requested_kernel))
        } else if let Some(requested_kernel) = requested_kernel {
            self.components
                .iter()
                .find(|installed| component_matches(&installed.metadata, requested_kernel))
                .or_else(|| {
                    self.components.iter().find(|installed| {
                        installed.metadata.kind == ArtifactComponentKind::Kernel
                            && installed.legacy_default
                    })
                })
        } else if mode != ExecutionSelectionMode::NewColdBoot {
            self.components.iter().find(|installed| {
                installed.metadata.kind == ArtifactComponentKind::Kernel && installed.legacy_default
            })
        } else {
            self.components.iter().find(|installed| {
                installed.metadata.kind == ArtifactComponentKind::Kernel
                    && installed.default_for_new
            })
        }
        .ok_or_else(|| match requested_kernel {
            Some(component) => RuntimeInventoryError::NoCompatibleComponent(component.clone()),
            None => RuntimeInventoryError::NoDefaultComponent {
                kind: ArtifactComponentKind::Kernel,
                selection: if mode != ExecutionSelectionMode::NewColdBoot {
                    ComponentDefaultSelection::LegacyArtifact
                } else {
                    ComponentDefaultSelection::NewArtifact
                },
            },
        })?;
        if requested_kernel.is_none() {
            resolved_components.push(kernel.metadata.clone());
        }
        selected_components.push(ResolvedArtifactComponent {
            metadata: kernel.metadata.clone(),
            path: kernel.path.clone(),
        });
        let execution = ExecutionMetadata::new(
            selected_runtime.runtime.clone(),
            selected_runtime.guest,
            Some(selected_runtime.state_format.clone()),
            resolved_components,
        )?;
        Ok(ResolvedExecutionEnvironment {
            execution,
            runtime_files: selected_runtime.files.clone(),
            components: selected_components,
        })
    }

    pub fn to_proto(&self) -> pplx_space_execution_common::proto::NodeExecutionInventory {
        use pplx_space_execution_common::proto;

        let runtimes = self
            .runtimes
            .iter()
            .map(|installation| proto::RuntimeInstallation {
                runtime: Some((&installation.runtime).into()),
                supported_guests: vec![installation.guest.into()],
                state_formats: vec![proto::StateFormatCapability {
                    identity: Some((&installation.state_format).into()),
                    can_create: true,
                    can_load: true,
                }],
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

fn host_arch() -> Result<Arch, RuntimeInventoryError> {
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
    if kernels.is_empty() {
        // Every candidate that could not be read transiently already returned
        // above, so an empty list here means the node genuinely has no usable
        // kernel installation — a terminal verdict.
        return Err(RuntimeInventoryError::Empty(root.to_path_buf()));
    }
    Ok(kernels)
}

fn discover_legacy_kernels(
    asset_root: &Path,
) -> Result<Vec<KernelInstallation>, RuntimeInventoryError> {
    let kernel = asset_root.join("default/kernel/vmlinux.bin");
    if !probe_inventory_path(&kernel)?.is_some_and(|metadata| metadata.is_file()) {
        return Err(RuntimeInventoryError::IncompleteKernelInstallation(kernel));
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
