//! Read-only diagnostic scan of the runtime releases and guest kernels a
//! node carries in its current vendor and cache layouts.
//!
//! This is deliberately separate from [`RuntimeInventory::discover`]. The
//! execution inventory is strict: it rejects an empty runtime set and abandons
//! the whole scan on one transient read, because node-manager caches that
//! answer for launch resolution and a half-answer would turn into terminal
//! verdicts. A diagnostic listing has the opposite job. It shows everything it
//! can see, reports each damaged entry next to the valid ones, treats an empty
//! node as an empty answer, and says explicitly when it could not finish.
//!
//! Nothing here writes, repairs, adopts, or executes anything. Versions come
//! from directory names, or for a kernel whose layout names none from the
//! release banner in its image; architectures from ELF headers, state formats
//! from the known-release map, and kernel digests from hashing the image.
//! Evidence that is not available stays `None` rather than being guessed.
//!
//! [`RuntimeInventory::discover`]: super::RuntimeInventory::discover

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::Read as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use pplx_space_common_core::host::linux::paths;
use pplx_space_common_core::types::{
    Arch, ArtifactComponentKind, ArtifactComponentRole, ArtifactVersion, Digest, RuntimeIdentity,
    RuntimeKind, RuntimeVersion, StateFormatIdentity,
};
use sha2::{Digest as _, Sha256};

mod boot_artifact;

use crate::catalog::ManagedDirectoryName;

use super::kernel_release::KernelReleaseScanner;
use super::{
    RuntimeInstallationRoot, boot_component_role, kernel_candidate_version, version_from_dir,
};

const KERNEL_IMAGE: &str = "vmlinux.bin";
/// The alias in a release or kernel root that names its default: what new
/// cold boots use.
pub const DEFAULT_MARKER: &str = "default";
/// The alias in a kernel root that names the kernel legacy launches use.
pub const LEGACY_DEFAULT_MARKER: &str = "legacy-default";
const HASH_CHUNK_BYTES: usize = 1024 * 1024;
const ELF_HEADER_PROBE_BYTES: usize = 64;
const ELF_MACHINE_X86_64: u16 = 62;
const ELF_MACHINE_AARCH64: u16 = 183;
/// Filesystem timestamps advance in coarse ticks, so a file changed this
/// recently may be changed again without its fingerprint moving.
const RACY_CHANGE_WINDOW: Duration = Duration::from_secs(2);

/// Where the scan looks. Every path comes from service configuration; a
/// listing request can narrow the kinds but never name a path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanRoots {
    /// Each runtime's `<version>/` release directories, holding the binaries
    /// its [`ReleaseLayout`] names, plus the `default` alias and any bundled
    /// `<version>/kernel/vmlinux.bin` kernel.
    pub runtime_releases: Vec<RuntimeInstallationRoot>,
    /// Versioned vendor kernels, `<version>/vmlinux.bin` plus markers.
    pub vendor_kernels: PathBuf,
    /// The runtime manager's own kernel install root, in the same layout.
    /// Scanned beside `vendor_kernels`, once when both name one directory.
    pub managed_kernels: Option<PathBuf>,
    /// The runtime manager's per-runtime release stores. Each is scanned beside
    /// external roots, once when both name the same runtime and directory.
    pub managed_releases: Vec<RuntimeInstallationRoot>,
    /// Versioned cache kernels the execution inventory resolves boots from.
    pub cache_kernels: PathBuf,
    /// Legacy flat cache layout, `default/kernel/vmlinux.bin`.
    pub cache_legacy_assets: PathBuf,
    /// Root-owned compatibility links such as `/space/bin/firecracker`.
    pub runtime_aliases: Vec<PathBuf>,
}

impl Default for ScanRoots {
    fn default() -> Self {
        Self {
            runtime_releases: vec![RuntimeInstallationRoot::new(
                RuntimeKind::Firecracker,
                paths::FIRECRACKER_VENDOR_DIR,
            )],
            vendor_kernels: paths::FIRECRACKER_KERNEL_VENDOR_DIR.into(),
            managed_kernels: None,
            managed_releases: Vec::new(),
            cache_kernels: paths::FIRECRACKER_KERNEL_ASSET_DIR.into(),
            cache_legacy_assets: paths::FIRECRACKER_ASSET_DIR.into(),
            runtime_aliases: vec![paths::FIRECRACKER_BIN.into(), paths::JAILER_BIN.into()],
        }
    }
}

/// Work budget for one scan. A limit hit is reported as a [`ScanIssue`], so a
/// bounded scan is never mistaken for a complete one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScanLimits {
    /// Directory entries read from one root before the root is abandoned.
    pub max_entries_per_root: usize,
    /// Largest file the scan hashes. Bigger images are listed without a digest.
    pub max_digest_bytes: u64,
    /// Wall-clock budget for the whole scan, checked between entries and
    /// between hash chunks.
    pub deadline: Duration,
}

/// Which artifact kinds a scan covers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArtifactKinds {
    pub runtimes: bool,
    pub kernels: bool,
    pub firmware: bool,
}

impl ArtifactKinds {
    pub const ALL: Self = Self {
        runtimes: true,
        kernels: true,
        firmware: true,
    };

    pub const fn includes_component(self, kind: ArtifactComponentKind) -> bool {
        match kind {
            ArtifactComponentKind::Kernel => self.kernels,
            ArtifactComponentKind::Firmware => self.firmware,
            _ => false,
        }
    }
}

/// What an artifact is, in the shared execution vocabulary. A kernel's
/// identity is its own: finding one inside a runtime release says where it
/// was, not which runtimes can boot it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ArtifactKind {
    Runtime(RuntimeKind),
    Component(ArtifactComponentKind),
}

/// Which kind of storage an artifact was found on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactLocation {
    /// The image-provided vendor tree, which survives reboots.
    Vendor,
    /// The recreatable cache filesystem that launches read from.
    Cache,
}

impl ArtifactLocation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Vendor => "vendor",
            Self::Cache => "cache",
        }
    }
}

impl std::fmt::Display for ArtifactLocation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// How an artifact is laid out where it was found.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactProvenance {
    /// A directory named for the artifact's version.
    VersionDirectory,
    /// Shipped inside a runtime release directory.
    RuntimeReleaseBundle,
    /// The unversioned `default` layout that predates version directories.
    LegacyFlat,
}

impl ArtifactProvenance {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VersionDirectory => "version-directory",
            Self::RuntimeReleaseBundle => "runtime-release-bundle",
            Self::LegacyFlat => "legacy-flat",
        }
    }
}

impl std::fmt::Display for ArtifactProvenance {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What an artifact or scan root holds and where. The fields describe any
/// runtime's layouts without naming one; what a runtime's release directory
/// holds is its [`ReleaseLayout`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ArtifactOrigin {
    pub kind: ArtifactKind,
    pub location: ArtifactLocation,
    pub provenance: ArtifactProvenance,
}

impl ArtifactOrigin {
    /// `<release root>/<version>/` of `runtime`.
    pub const fn runtime_release(runtime: RuntimeKind) -> Self {
        Self {
            kind: ArtifactKind::Runtime(runtime),
            location: ArtifactLocation::Vendor,
            provenance: ArtifactProvenance::VersionDirectory,
        }
    }

    /// `<runtime_releases>/<release>/kernel/vmlinux.bin`.
    pub const RELEASE_BUNDLED_KERNEL: Self = Self {
        kind: ArtifactKind::Component(ArtifactComponentKind::Kernel),
        location: ArtifactLocation::Vendor,
        provenance: ArtifactProvenance::RuntimeReleaseBundle,
    };
    /// `<vendor_kernels>/<version>/vmlinux.bin`.
    pub const VENDOR_KERNEL: Self = Self {
        kind: ArtifactKind::Component(ArtifactComponentKind::Kernel),
        location: ArtifactLocation::Vendor,
        provenance: ArtifactProvenance::VersionDirectory,
    };
    /// `<cache_kernels>/<version>/vmlinux.bin`.
    pub const CACHE_KERNEL: Self = Self {
        kind: ArtifactKind::Component(ArtifactComponentKind::Kernel),
        location: ArtifactLocation::Cache,
        provenance: ArtifactProvenance::VersionDirectory,
    };
    /// `<cache_legacy_assets>/default/kernel/vmlinux.bin`.
    pub const LEGACY_CACHE_KERNEL: Self = Self {
        kind: ArtifactKind::Component(ArtifactComponentKind::Kernel),
        location: ArtifactLocation::Cache,
        provenance: ArtifactProvenance::LegacyFlat,
    };
}

/// Who owns an artifact's bytes, and so whether the runtime manager may ever
/// change them. Ownership comes from the runtime manager's own install
/// records, never from where the files sit: its installs and an image's
/// baked artifacts share the vendor tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum ArtifactManagement {
    /// Installed by the runtime manager, which holds its verified descriptor
    /// and may remove it under the uninstall rules.
    SpaceManaged,
    /// Supplied by something else. Listed and usable, never modified or
    /// removed by the runtime manager, and never adopted.
    External(ArtifactProvider),
}

/// Who supplied an external artifact, when there is evidence of it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactProvider {
    /// Baked into the node image.
    NodeImage,
    /// Part of the host operating system, such as an OS hypervisor feature.
    HostOs,
    /// Not the runtime manager's, with no evidence of whose. Everything a
    /// scan finds before the runtime manager installs anything is this: the
    /// scan cannot tell an image-baked file from one a deployment script
    /// staged.
    Unknown,
}

impl ArtifactProvider {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NodeImage => "node-image",
            Self::HostOs => "host-os",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for ArtifactProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Where a reported version came from.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum VersionSource {
    /// The installation directory's name.
    DirectoryName,
    /// The `Linux version` banner in a kernel image whose layout names no
    /// version.
    ImageBanner,
}

impl VersionSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DirectoryName => "directory-name",
            Self::ImageBanner => "image-banner",
        }
    }
}

impl std::fmt::Display for VersionSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ArtifactStatus {
    /// Every required file is present and every probed property is readable.
    Valid,
    /// A required file is missing.
    Incomplete,
    /// A required file exists but could not be read.
    Unreadable,
    /// Present, but in a shape the current consumers cannot use as-is.
    Invalid,
    /// The entry's name is not a version this layout recognizes.
    Unrecognized,
    /// No release layout is known, or the layout requires a state-format
    /// mapping this build does not have for the release.
    Unsupported,
}

impl ArtifactStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Incomplete => "incomplete",
            Self::Unreadable => "unreadable",
            Self::Invalid => "invalid",
            Self::Unrecognized => "unrecognized",
            Self::Unsupported => "unsupported",
        }
    }
}

impl std::fmt::Display for ArtifactStatus {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// A discovered version retains its domain until the protocol boundary.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ScannedVersion {
    Runtime(RuntimeVersion),
    Component(ArtifactVersion),
}

impl std::fmt::Display for ScannedVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Runtime(version) => version.fmt(formatter),
            Self::Component(version) => version.fmt(formatter),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedArtifact {
    pub origin: ArtifactOrigin,
    /// The component's role, such as `boot` for a guest kernel.
    pub role: Option<ArtifactComponentRole>,
    pub management: ArtifactManagement,
    /// The directory entry that named the artifact.
    pub name: String,
    /// The release directory for runtimes, the image file for kernels.
    pub path: PathBuf,
    pub version: Option<ScannedVersion>,
    pub version_source: Option<VersionSource>,
    /// From the ELF header of the runtime's binary or the kernel image.
    pub architecture: Option<Arch>,
    /// A qualified memory-state format, when this runtime and release have one.
    /// `None` also covers runtimes whose installation needs no state format.
    pub state_format: Option<StateFormatIdentity>,
    /// SHA-256 of the kernel image, or the aggregate release digest of a managed
    /// runtime's named binaries. Execution-root runtimes are not hashed.
    pub digest: Option<Digest>,
    /// For a kernel, the runtime it ships with or was published for. `None`
    /// when nothing on disk ties it to one.
    pub for_runtime: Option<RuntimeIdentity>,
    pub status: ArtifactStatus,
    /// Why the status is not [`ArtifactStatus::Valid`], or why a piece of
    /// evidence is missing from a valid entry.
    pub detail: Option<String>,
}

/// A compatibility link the scan observed. Aliases are reported as found on
/// disk; they are not a runtime-manager selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedAlias {
    pub path: PathBuf,
    /// The raw link target, when `path` is a symlink.
    pub target: Option<PathBuf>,
    /// The fully resolved target, when it exists.
    pub resolved: Option<PathBuf>,
    pub detail: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RootPresence {
    Present,
    /// The root does not exist. An empty node is not an error.
    Absent,
    /// The root exists but could not be listed; see the matching issue.
    Unreadable,
}

impl RootPresence {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Absent => "absent",
            Self::Unreadable => "unreadable",
        }
    }
}

impl std::fmt::Display for RootPresence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScannedRoot {
    /// What the root's entries are expected to be.
    pub origin: ArtifactOrigin,
    pub path: PathBuf,
    pub presence: RootPresence,
}

/// Something that stopped the scan from seeing everything. Any issue makes
/// the scan incomplete.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ScanIssueKind {
    /// A root had more entries than [`ScanLimits::max_entries_per_root`].
    EntryLimit,
    /// [`ScanLimits::deadline`] expired before the scan finished.
    Deadline,
    /// A root exists but could not be listed.
    RootUnreadable,
    /// An image was larger than [`ScanLimits::max_digest_bytes`], or grew
    /// past it while being hashed, and is listed without a digest.
    DigestLimit,
}

impl ScanIssueKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EntryLimit => "entry-limit",
            Self::Deadline => "deadline",
            Self::RootUnreadable => "root-unreadable",
            Self::DigestLimit => "digest-limit",
        }
    }
}

impl std::fmt::Display for ScanIssueKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanIssue {
    pub kind: ScanIssueKind,
    pub path: PathBuf,
    pub detail: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ArtifactScan {
    pub roots: Vec<ScannedRoot>,
    pub artifacts: Vec<ScannedArtifact>,
    pub aliases: Vec<ObservedAlias>,
    pub issues: Vec<ScanIssue>,
}

impl ArtifactScan {
    /// True when no limit, deadline, or unreadable root cut the scan short.
    /// Damaged entries do not make a scan incomplete; they are reported.
    pub fn is_complete(&self) -> bool {
        self.issues.is_empty()
    }
}

/// Time source for the scan deadline, injectable so tests can expire it.
pub trait ScanClock {
    fn now(&self) -> Instant;

    /// Wall-clock time, compared with file change times.
    fn wall_time(&self) -> SystemTime {
        SystemTime::now()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemScanClock;

impl ScanClock for SystemScanClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Identity of an image file's bytes as far as the kernel reports it. A
/// rewrite changes the size, mtime, or ctime, and ctime cannot be set back by
/// an unprivileged writer, so an unchanged fingerprint reuses a digest. Two
/// writes within one timestamp tick can share a fingerprint, so digests of
/// files changed within [`RACY_CHANGE_WINDOW`] are never cached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileFingerprint {
    dev: u64,
    ino: u64,
    len: u64,
    mtime_ns: i128,
    ctime_ns: i128,
}

impl FileFingerprint {
    fn of(metadata: &std::fs::Metadata) -> Self {
        Self {
            dev: metadata.dev(),
            ino: metadata.ino(),
            len: metadata.len(),
            mtime_ns: i128::from(metadata.mtime()) * 1_000_000_000
                + i128::from(metadata.mtime_nsec()),
            ctime_ns: i128::from(metadata.ctime()) * 1_000_000_000
                + i128::from(metadata.ctime_nsec()),
        }
    }
}

/// Managed binary and kernel digests from earlier scans, so a repeated listing
/// does not re-read unchanged files. Each kind retains only files seen in its
/// last scan, bounded by the scan's entry limits.
#[derive(Debug, Default)]
pub struct DigestCache {
    entries: HashMap<(ArtifactKind, PathBuf), (FileFingerprint, ImageMeasure)>,
}

/// What one read of a kernel image yields.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ImageMeasure {
    digest: Digest,
    release: Option<ArtifactVersion>,
}

impl DigestCache {
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// The vendor kernel roots to scan: the runtime manager's own, then the
/// image's unless it is the same directory, as when one links to the other.
fn vendor_kernel_roots(roots: &ScanRoots) -> Vec<&Path> {
    let mut found: Vec<&Path> = roots.managed_kernels.iter().map(PathBuf::as_path).collect();
    let duplicate = roots
        .managed_kernels
        .as_deref()
        .is_some_and(|managed| same_directory(managed, &roots.vendor_kernels));
    if !duplicate {
        found.push(&roots.vendor_kernels);
    }
    found
}

/// The release roots to scan: the configured ones, then the runtime manager's
/// own. A configured root that is the same directory as the runtime
/// manager's is scanned once, under the runtime manager's path, which its
/// install records name, as the kernel roots are.
fn release_roots(roots: &ScanRoots) -> Vec<&RuntimeInstallationRoot> {
    let mut found: Vec<&RuntimeInstallationRoot> = roots
        .runtime_releases
        .iter()
        .filter(|root| {
            !roots.managed_releases.iter().any(|managed| {
                root.kind == managed.kind && same_directory(&root.path, &managed.path)
            })
        })
        .collect();
    found.extend(&roots.managed_releases);
    found
}

/// Whether `left` and `right` resolve to one directory; when either cannot
/// be resolved, whether they are the same path.
fn same_directory(left: &Path, right: &Path) -> bool {
    match (std::fs::canonicalize(left), std::fs::canonicalize(right)) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

/// Scan the configured roots for the requested kinds.
///
/// Blocking: filesystem reads and hashing run on the calling thread, so async
/// callers must move this off their workers.
pub fn scan(
    roots: &ScanRoots,
    limits: ScanLimits,
    kinds: ArtifactKinds,
    clock: &dyn ScanClock,
    cache: &mut DigestCache,
) -> ArtifactScan {
    let mut scanner = Scanner {
        limits,
        clock,
        deadline: clock.now() + limits.deadline,
        deadline_reported: false,
        previous: std::mem::take(&mut cache.entries),
        seen: HashMap::new(),
        scan: ArtifactScan::default(),
    };

    if kinds.runtimes {
        for root in release_roots(roots) {
            scanner.observe_alias(&root.path.join(DEFAULT_MARKER));
        }
        for alias in &roots.runtime_aliases {
            scanner.observe_alias(alias);
        }
    }
    if kinds.kernels {
        for root in vendor_kernel_roots(roots)
            .into_iter()
            .chain([roots.cache_kernels.as_path()])
        {
            scanner.observe_alias(&root.join(DEFAULT_MARKER));
            scanner.observe_alias(&root.join(LEGACY_DEFAULT_MARKER));
        }
    }

    if kinds.runtimes || kinds.kernels {
        for root in release_roots(roots) {
            scanner.scan_runtime_releases(
                root,
                kinds,
                roots.managed_releases.iter().any(|managed| managed == root),
            );
        }
    }
    if kinds.kernels {
        for root in vendor_kernel_roots(roots) {
            scanner.scan_versioned_kernels(
                ArtifactOrigin::VENDOR_KERNEL,
                root,
                roots.managed_kernels.as_deref() == Some(root),
            );
        }
        scanner.scan_versioned_kernels(ArtifactOrigin::CACHE_KERNEL, &roots.cache_kernels, false);
        scanner.scan_legacy_assets(&roots.cache_legacy_assets);
    }

    if let Some(kernels) = &roots.managed_kernels {
        let root = crate::artifact_layout::boot_artifacts_root(kernels);
        if kinds.includes_component(ArtifactComponentKind::Firmware) {
            scanner.scan_boot_artifacts(&root, ArtifactComponentKind::Firmware);
        }
    }

    // A filtered scan evicts only its own kind; the untouched partition is
    // still bounded by the last scan that visited it.
    scanner.previous.retain(|(kind, _), _| match kind {
        ArtifactKind::Runtime(_) => !kinds.runtimes,
        ArtifactKind::Component(kind) => !kinds.includes_component(*kind),
    });
    scanner.previous.extend(scanner.seen);
    cache.entries = scanner.previous;
    scanner.scan
}

struct Scanner<'a> {
    limits: ScanLimits,
    clock: &'a dyn ScanClock,
    deadline: Instant,
    deadline_reported: bool,
    previous: HashMap<(ArtifactKind, PathBuf), (FileFingerprint, ImageMeasure)>,
    seen: HashMap<(ArtifactKind, PathBuf), (FileFingerprint, ImageMeasure)>,
    scan: ArtifactScan,
}

enum DigestOutcome {
    Measured(ImageMeasure),
    /// The image is, or grew to, more than this many bytes over the limit.
    TooLarge(u64),
    DeadlineExpired,
    /// The file's metadata changed while it was being read, so the bytes
    /// hashed may belong to no single version of the image.
    Changed,
    Unreadable(std::io::Error),
}

impl Scanner<'_> {
    /// True once the deadline has passed, recording the issue the first time.
    fn expired(&mut self, path: &Path) -> bool {
        if self.deadline_reported {
            return true;
        }
        if self.clock.now() < self.deadline {
            return false;
        }
        self.deadline_reported = true;
        self.scan.issues.push(ScanIssue {
            kind: ScanIssueKind::Deadline,
            path: path.to_path_buf(),
            detail: format!(
                "scan deadline of {:?} expired; later entries were not examined",
                self.limits.deadline
            ),
        });
        true
    }

    fn observe_alias(&mut self, path: &Path) {
        let metadata = match std::fs::symlink_metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
            Err(error) => {
                self.scan.aliases.push(ObservedAlias {
                    path: path.to_path_buf(),
                    target: None,
                    resolved: None,
                    detail: Some(format!("inspect alias: {error}")),
                });
                return;
            }
        };
        if !metadata.file_type().is_symlink() {
            self.scan.aliases.push(ObservedAlias {
                path: path.to_path_buf(),
                target: None,
                resolved: None,
                detail: Some("exists but is not a symlink".to_string()),
            });
            return;
        }
        let target = std::fs::read_link(path);
        let resolved = std::fs::canonicalize(path);
        let detail = match (&target, &resolved) {
            (Err(error), _) => Some(format!("read alias target: {error}")),
            (Ok(_), Err(error)) => Some(format!("alias does not resolve: {error}")),
            (Ok(_), Ok(_)) => None,
        };
        self.scan.aliases.push(ObservedAlias {
            path: path.to_path_buf(),
            target: target.ok(),
            resolved: resolved.ok(),
            detail,
        });
    }

    /// Record the root's presence and return its entry names, sorted and
    /// bounded by the entry limit. `None` when there is nothing to scan.
    fn list_root(&mut self, origin: ArtifactOrigin, root: &Path) -> Option<Vec<OsString>> {
        let entries = match std::fs::read_dir(root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.push_root(origin, root, RootPresence::Absent);
                return None;
            }
            Err(error) => {
                self.push_root(origin, root, RootPresence::Unreadable);
                self.push_root_unreadable(root, &error);
                return None;
            }
        };
        self.push_root(origin, root, RootPresence::Present);
        let mut names = Vec::new();
        for entry in entries {
            if names.len() == self.limits.max_entries_per_root {
                self.scan.issues.push(ScanIssue {
                    kind: ScanIssueKind::EntryLimit,
                    path: root.to_path_buf(),
                    detail: format!(
                        "root has more than {} entries; the rest were not examined",
                        self.limits.max_entries_per_root
                    ),
                });
                break;
            }
            match entry {
                Ok(entry) => names.push(entry.file_name()),
                Err(error) => {
                    // An entry read failing mid-listing leaves the rest of the
                    // root unknown, so the root is reported, not the entry.
                    self.push_root_unreadable(root, &error);
                    break;
                }
            }
        }
        names.sort();
        Some(names)
    }

    fn push_root(&mut self, origin: ArtifactOrigin, path: &Path, presence: RootPresence) {
        self.scan.roots.push(ScannedRoot {
            origin,
            path: path.to_path_buf(),
            presence,
        });
    }

    fn push_root_unreadable(&mut self, root: &Path, error: &std::io::Error) {
        self.scan.issues.push(ScanIssue {
            kind: ScanIssueKind::RootUnreadable,
            path: root.to_path_buf(),
            detail: format!("list root: {error}"),
        });
    }

    fn scan_runtime_releases(
        &mut self,
        releases: &RuntimeInstallationRoot,
        kinds: ArtifactKinds,
        managed: bool,
    ) {
        let runtime = releases.kind;
        let origin = ArtifactOrigin::runtime_release(runtime);
        let Some(names) = self.list_root(origin, &releases.path) else {
            return;
        };
        for name in names {
            let path = releases.path.join(&name);
            if self.expired(&path) {
                return;
            }
            let display_name = name.to_string_lossy().into_owned();
            // `default` is the alias observed above and dot entries are
            // staging, the same screen the execution inventory applies.
            if display_name == DEFAULT_MARKER || display_name.starts_with('.') {
                continue;
            }
            match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => continue,
                Err(error) => {
                    if kinds.kernels {
                        // The release could contain a bundled kernel. Failure to
                        // inspect it cannot establish that no kernel is present.
                        self.push_root_unreadable(&path, &error);
                    }
                    if kinds.runtimes {
                        self.push_unreadable_entry(origin, display_name, path, &error);
                    }
                    continue;
                }
            }
            let directory = managed
                .then(|| display_name.parse::<ManagedDirectoryName>().ok())
                .flatten();
            if kinds.runtimes {
                let mut artifact = inspect_runtime_release(
                    runtime,
                    display_name.clone(),
                    path.clone(),
                    directory.as_ref(),
                );
                // Uninstall identifies installed bytes, even if their catalog
                // pin has since been removed. Execution roots stay probe-only.
                if managed
                    && matches!(
                        artifact.status,
                        ArtifactStatus::Valid | ArtifactStatus::Unsupported
                    )
                    && let Some(layout) = ReleaseLayout::of(runtime)
                {
                    let mut binaries = Vec::new();
                    for binary in layout.binaries {
                        let image = path.join(binary);
                        let measured = match open_nonblocking(&image)
                            .and_then(|file| file.metadata().map(|metadata| (file, metadata)))
                        {
                            Ok((mut file, metadata)) if metadata.is_file() => {
                                self.digest(origin.kind, &image, &mut file, &metadata)
                            }
                            Ok(_) => DigestOutcome::Unreadable(std::io::Error::other(
                                "not a regular file",
                            )),
                            Err(error) => DigestOutcome::Unreadable(error),
                        };
                        let Some(measure) = self.measured(measured, &image, &mut artifact) else {
                            break;
                        };
                        binaries.push((*binary, measure.digest));
                    }
                    if binaries.len() == layout.binaries.len() {
                        artifact.digest = Some(crate::catalog::release_digest(binaries));
                    }
                }
                if let Some(directory) = &directory {
                    check_directory_digest(&mut artifact, directory);
                }
                self.scan.artifacts.push(artifact);
            }
            if kinds.kernels {
                let image = path.join("kernel").join(KERNEL_IMAGE);
                match std::fs::symlink_metadata(&image) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    _ => {
                        let release =
                            diagnostic_runtime_version(&path, directory.as_ref()).map(|version| {
                                RuntimeIdentity {
                                    kind: runtime,
                                    version,
                                }
                            });
                        self.inspect_kernel(
                            ArtifactOrigin::RELEASE_BUNDLED_KERNEL,
                            display_name,
                            None,
                            release,
                            image,
                            None,
                        );
                    }
                }
            }
        }
    }

    fn scan_versioned_kernels(&mut self, origin: ArtifactOrigin, root: &Path, managed: bool) {
        let Some(names) = self.list_root(origin, root) else {
            return;
        };
        for name in names {
            let path = root.join(&name);
            if self.expired(&path) {
                return;
            }
            let display_name = name.to_string_lossy().into_owned();
            if display_name == DEFAULT_MARKER
                || display_name == LEGACY_DEFAULT_MARKER
                || display_name.starts_with('.')
            {
                continue;
            }
            match std::fs::metadata(&path) {
                Ok(metadata) if metadata.is_dir() => {}
                Ok(_) => continue,
                Err(error) => {
                    self.push_unreadable_entry(origin, display_name, path, &error);
                    continue;
                }
            }
            let image = path.join(KERNEL_IMAGE);
            let directory = managed
                .then(|| display_name.parse::<ManagedDirectoryName>().ok())
                .flatten();
            let version_name = directory.as_ref().map_or(name.as_os_str(), |directory| {
                std::ffi::OsStr::new(directory.label())
            });
            let Some(version) = kernel_candidate_version(version_name) else {
                self.scan.artifacts.push(ScannedArtifact {
                    status: ArtifactStatus::Unrecognized,
                    detail: Some("directory name is not a kernel version".to_string()),
                    ..empty_artifact(origin, display_name, image)
                });
                continue;
            };
            self.inspect_kernel(
                origin,
                display_name,
                Some(version),
                None,
                image,
                directory.as_ref(),
            );
        }
    }

    fn scan_legacy_assets(&mut self, root: &Path) {
        match std::fs::metadata(root) {
            Ok(metadata) if metadata.is_dir() => {
                self.push_root(
                    ArtifactOrigin::LEGACY_CACHE_KERNEL,
                    root,
                    RootPresence::Present,
                );
            }
            Ok(_) => {
                self.push_root(
                    ArtifactOrigin::LEGACY_CACHE_KERNEL,
                    root,
                    RootPresence::Unreadable,
                );
                self.push_root_unreadable(
                    root,
                    &std::io::Error::from(std::io::ErrorKind::NotADirectory),
                );
                return;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                self.push_root(
                    ArtifactOrigin::LEGACY_CACHE_KERNEL,
                    root,
                    RootPresence::Absent,
                );
                return;
            }
            Err(error) => {
                self.push_root(
                    ArtifactOrigin::LEGACY_CACHE_KERNEL,
                    root,
                    RootPresence::Unreadable,
                );
                self.push_root_unreadable(root, &error);
                return;
            }
        }
        let image = root.join(DEFAULT_MARKER).join("kernel").join(KERNEL_IMAGE);
        if self.expired(&image) {
            return;
        }
        match std::fs::symlink_metadata(&image) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => self.inspect_kernel(
                ArtifactOrigin::LEGACY_CACHE_KERNEL,
                DEFAULT_MARKER.to_string(),
                None,
                None,
                image,
                None,
            ),
        }
    }

    fn push_unreadable_entry(
        &mut self,
        origin: ArtifactOrigin,
        name: String,
        path: PathBuf,
        error: &std::io::Error,
    ) {
        self.scan.artifacts.push(ScannedArtifact {
            status: ArtifactStatus::Unreadable,
            detail: Some(format!("inspect entry: {error}")),
            ..empty_artifact(origin, name, path)
        });
    }

    /// Inspect one kernel image. `version` is the directory-name version for
    /// versioned layouts; flat layouts read the release banner in the image
    /// instead. `for_runtime` is the release a bundled kernel ships in.
    fn inspect_kernel(
        &mut self,
        origin: ArtifactOrigin,
        name: String,
        version: Option<ArtifactVersion>,
        for_runtime: Option<RuntimeIdentity>,
        image: PathBuf,
        directory: Option<&ManagedDirectoryName>,
    ) {
        let mut artifact = empty_artifact(origin, name, image.clone());
        artifact.for_runtime = for_runtime;
        if let Some(version) = &version {
            artifact.version = Some(ScannedVersion::Component(version.clone()));
            artifact.version_source = Some(VersionSource::DirectoryName);
        }

        let link_metadata = match std::fs::symlink_metadata(&image) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                artifact.status = ArtifactStatus::Incomplete;
                artifact.detail = Some(format!("missing {KERNEL_IMAGE}"));
                self.scan.artifacts.push(artifact);
                return;
            }
            Err(error) => {
                artifact.status = ArtifactStatus::Unreadable;
                artifact.detail = Some(format!("inspect {KERNEL_IMAGE}: {error}"));
                self.scan.artifacts.push(artifact);
                return;
            }
        };

        let mut file = match open_nonblocking(&image) {
            Ok(file) => file,
            Err(error) => {
                artifact.status = if error.kind() == std::io::ErrorKind::NotFound {
                    ArtifactStatus::Incomplete
                } else {
                    ArtifactStatus::Unreadable
                };
                artifact.detail = Some(format!("open {KERNEL_IMAGE}: {error}"));
                self.scan.artifacts.push(artifact);
                return;
            }
        };
        let metadata = match file.metadata() {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                artifact.status = ArtifactStatus::Invalid;
                artifact.detail = Some(format!("{KERNEL_IMAGE} is not a regular file"));
                self.scan.artifacts.push(artifact);
                return;
            }
            Err(error) => {
                artifact.status = ArtifactStatus::Unreadable;
                artifact.detail = Some(format!("inspect {KERNEL_IMAGE}: {error}"));
                self.scan.artifacts.push(artifact);
                return;
            }
        };

        match probe_elf(&mut file) {
            // A kernel need not be ELF (arm64 boots a raw `Image`), so a
            // non-ELF image only leaves the architecture unknown.
            Ok(ElfProbe::Elf { architecture }) => artifact.architecture = architecture,
            Ok(ElfProbe::NotElf) => {}
            Err(error) => {
                artifact.status = ArtifactStatus::Unreadable;
                artifact.detail = Some(format!("read {KERNEL_IMAGE} header: {error}"));
                self.scan.artifacts.push(artifact);
                return;
            }
        }

        let measured = self.digest(origin.kind, &image, &mut file, &metadata);
        if let Some(measure) = self.measured(measured, &image, &mut artifact) {
            artifact.digest = Some(measure.digest);
            if version.is_none()
                && let Some(release) = measure.release
            {
                artifact.version = Some(ScannedVersion::Component(release));
                artifact.version_source = Some(VersionSource::ImageBanner);
            }
        }

        if let Some(directory) = directory {
            check_directory_digest(&mut artifact, directory);
        }

        // Jail setup hard-links the cache kernel into each chroot, and a
        // hard link to a symlink is the symlink, which Firecracker cannot
        // open once chrooted. Vendor copies are staging sources and may be
        // links.
        if origin.location == ArtifactLocation::Cache && link_metadata.file_type().is_symlink() {
            artifact.status = ArtifactStatus::Invalid;
            artifact.detail = Some(format!(
                "cache {KERNEL_IMAGE} is a symlink; jail setup needs a regular file"
            ));
        }
        self.scan.artifacts.push(artifact);
    }

    fn measured(
        &mut self,
        outcome: DigestOutcome,
        image: &Path,
        artifact: &mut ScannedArtifact,
    ) -> Option<ImageMeasure> {
        match outcome {
            DigestOutcome::Measured(measure) => return Some(measure),
            DigestOutcome::TooLarge(len) => {
                let detail = format!(
                    "image of at least {len} bytes exceeds the {}-byte digest limit; digest not computed",
                    self.limits.max_digest_bytes
                );
                self.scan.issues.push(ScanIssue {
                    kind: ScanIssueKind::DigestLimit,
                    path: image.to_path_buf(),
                    detail: detail.clone(),
                });
                artifact.detail = Some(detail);
            }
            DigestOutcome::Changed => {
                artifact.detail = Some(
                    "image changed while it was being hashed; digest not computed".to_string(),
                );
            }
            DigestOutcome::DeadlineExpired => {
                artifact.detail =
                    Some("scan deadline expired before the digest completed".to_string());
            }
            DigestOutcome::Unreadable(error) => {
                artifact.status = ArtifactStatus::Unreadable;
                artifact.detail = Some(format!("read {}: {error}", image.display()));
            }
        }
        None
    }

    fn digest(
        &mut self,
        kind: ArtifactKind,
        image: &Path,
        file: &mut std::fs::File,
        metadata: &std::fs::Metadata,
    ) -> DigestOutcome {
        let fingerprint = FileFingerprint::of(metadata);
        let key = (kind, image.to_path_buf());
        if let Some((cached, measure)) = self.previous.get(&key)
            && *cached == fingerprint
        {
            let measure = measure.clone();
            self.seen.insert(key, (fingerprint, measure.clone()));
            return DigestOutcome::Measured(measure);
        }
        if metadata.len() > self.limits.max_digest_bytes {
            return DigestOutcome::TooLarge(metadata.len());
        }
        // The header probe moved the cursor; the digest covers the whole file.
        if let Err(error) = std::io::Seek::rewind(file) {
            return DigestOutcome::Unreadable(error);
        }
        let mut hasher = Sha256::new();
        let mut release = KernelReleaseScanner::new();
        let mut buffer = vec![0u8; HASH_CHUNK_BYTES];
        // The size check above saw the file before reading it; a writer can
        // still extend it, so the budget is enforced on the bytes actually read.
        let mut hashed: u64 = 0;
        loop {
            if self.expired(image) {
                return DigestOutcome::DeadlineExpired;
            }
            let read = match file.read(&mut buffer) {
                Ok(read) => read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return DigestOutcome::Unreadable(error),
            };
            let Some(chunk) = buffer.get(..read) else {
                return DigestOutcome::Unreadable(std::io::Error::other(format!(
                    "read returned {read} bytes for a {}-byte buffer",
                    buffer.len()
                )));
            };
            if chunk.is_empty() {
                break;
            }
            hashed = hashed.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
            if hashed > self.limits.max_digest_bytes {
                return DigestOutcome::TooLarge(hashed);
            }
            hasher.update(chunk);
            release.update(chunk);
        }
        // A write during the read changes size, mtime, or ctime. Such a digest
        // may describe neither the old nor the new bytes, so it is neither
        // reported nor cached.
        match file.metadata() {
            Ok(after) if FileFingerprint::of(&after) == fingerprint => {}
            Ok(_) => return DigestOutcome::Changed,
            Err(error) => return DigestOutcome::Unreadable(error),
        }
        let measure = ImageMeasure {
            digest: Digest::sha256(hasher.finalize().into()),
            release: release.finish(),
        };
        if !changed_recently(metadata, self.clock.wall_time()) {
            self.seen.insert(key, (fingerprint, measure.clone()));
        }
        DigestOutcome::Measured(measure)
    }
}

/// True when `metadata` records a change within [`RACY_CHANGE_WINDOW`] of
/// `wall`, or at a time that cannot be compared with it.
fn changed_recently(metadata: &std::fs::Metadata, wall: SystemTime) -> bool {
    let (Ok(seconds), Ok(nanos)) = (
        u64::try_from(metadata.ctime()),
        u32::try_from(metadata.ctime_nsec()),
    ) else {
        return true;
    };
    SystemTime::UNIX_EPOCH
        .checked_add(Duration::new(seconds, nanos))
        .and_then(|changed| wall.duration_since(changed).ok())
        .is_none_or(|age| age < RACY_CHANGE_WINDOW)
}

fn empty_artifact(origin: ArtifactOrigin, name: String, path: PathBuf) -> ScannedArtifact {
    ScannedArtifact {
        origin,
        role: match origin.kind {
            ArtifactKind::Component(ArtifactComponentKind::Kernel) => Some(boot_component_role()),
            ArtifactKind::Component(_) | ArtifactKind::Runtime(_) => None,
        },
        // Management attribution is applied from the configured store roots,
        // never inferred from a directory label alone.
        management: ArtifactManagement::External(ArtifactProvider::Unknown),
        name,
        path,
        version: None,
        version_source: None,
        architecture: None,
        state_format: None,
        digest: None,
        for_runtime: None,
        status: ArtifactStatus::Valid,
        detail: None,
    }
}

/// Classify `path`'s ELF header without loading or running it. Opened
/// non-blocking, like every image the scan reads.
pub fn probe_elf_file(path: &Path) -> std::io::Result<ElfProbe> {
    probe_elf(&mut open_nonblocking(path)?)
}

pub use pplx_space_execution_catalog::layout::{
    FIRECRACKER_RELEASE_LAYOUT, ReleaseLayout, StateFormatResolver,
};

fn inspect_runtime_release(
    runtime: RuntimeKind,
    name: String,
    root: PathBuf,
    directory: Option<&ManagedDirectoryName>,
) -> ScannedArtifact {
    let mut artifact = empty_artifact(ArtifactOrigin::runtime_release(runtime), name, root.clone());
    let Some(version) = diagnostic_runtime_version(&root, directory) else {
        artifact.status = ArtifactStatus::Unrecognized;
        artifact.detail = Some("directory name is not a runtime version".to_string());
        return artifact;
    };
    artifact.version = Some(ScannedVersion::Runtime(version.clone()));
    artifact.version_source = Some(VersionSource::DirectoryName);
    let Some(layout) = ReleaseLayout::of(runtime) else {
        artifact.status = ArtifactStatus::Unsupported;
        artifact.detail = Some(format!("no release layout is known for {runtime}"));
        return artifact;
    };

    let binaries: Vec<PathBuf> = layout
        .binaries
        .iter()
        .map(|binary| root.join(binary))
        .collect();
    // Presence first, so a release missing one binary is incomplete rather
    // than invalid because of another.
    for binary in &binaries {
        match std::fs::metadata(binary) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                artifact.status = ArtifactStatus::Incomplete;
                artifact.detail = Some(format!("missing {}", binary.display()));
                return artifact;
            }
            Err(error) => {
                artifact.status = ArtifactStatus::Unreadable;
                artifact.detail = Some(format!("inspect {}: {error}", binary.display()));
                return artifact;
            }
        }
    }
    for (index, binary) in binaries.iter().enumerate() {
        match inspect_runtime_binary(binary) {
            Ok(architecture) => {
                if index == 0 {
                    artifact.architecture = architecture;
                }
            }
            Err((status, detail)) => {
                artifact.status = status;
                artifact.detail = Some(detail);
                return artifact;
            }
        }
    }

    match layout
        .state_format
        .map(|format| format(&version))
        .transpose()
    {
        Ok(state_format) => artifact.state_format = state_format,
        Err(detail) => {
            artifact.status = ArtifactStatus::Unsupported;
            artifact.detail = Some(detail);
        }
    }
    artifact
}

/// Check that `binary` is plausibly runnable without running it: a regular
/// file with an execute bit and an ELF header. Returns the ELF architecture,
/// which is `None` for a machine this build does not name. Every runtime
/// binary a release layout names is an ELF executable, so a file that is not
/// one is damaged, not merely unidentified.
fn inspect_runtime_binary(binary: &Path) -> Result<Option<Arch>, (ArtifactStatus, String)> {
    let unreadable = |error: std::io::Error| {
        (
            ArtifactStatus::Unreadable,
            format!("read {}: {error}", binary.display()),
        )
    };
    let mut file = open_nonblocking(binary).map_err(unreadable)?;
    let metadata = file.metadata().map_err(unreadable)?;
    if !metadata.is_file() {
        return Err((
            ArtifactStatus::Invalid,
            format!("{} is not a regular file", binary.display()),
        ));
    }
    if metadata.mode() & 0o111 == 0 {
        return Err((
            ArtifactStatus::Invalid,
            format!("{} is not executable", binary.display()),
        ));
    }
    match probe_elf(&mut file).map_err(unreadable)? {
        ElfProbe::Elf { architecture } => Ok(architecture),
        ElfProbe::NotElf => Err((
            ArtifactStatus::Invalid,
            format!("{} is not an ELF executable", binary.display()),
        )),
    }
}

/// Open `path` for reading without blocking. The cache kernel roots sit under
/// a directory other services can write, so a FIFO can appear where an image
/// is expected; a blocking open on it would outlast any scan deadline.
/// `O_NONBLOCK` does not change reads of a regular file, and callers reject
/// anything else after `fstat`.
fn open_nonblocking(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ElfProbe {
    /// No complete, structurally valid executable ELF header.
    NotElf,
    /// An ELF header; `architecture` is `None` for an unnamed machine.
    Elf { architecture: Option<Arch> },
}

/// Classify the complete ELF32/ELF64 header without loading or running the image.
fn probe_elf(file: &mut std::fs::File) -> std::io::Result<ElfProbe> {
    let mut header = [0u8; ELF_HEADER_PROBE_BYTES];
    let mut filled = 0;
    while filled < header.len() {
        let Some(rest) = header.get_mut(filled..) else {
            break;
        };
        match file.read(rest) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(header
        .get(..filled)
        .map_or(ElfProbe::NotElf, parse_elf_header))
}

fn parse_elf_header(header: &[u8]) -> ElfProbe {
    parse_elf_machine(header).map_or(ElfProbe::NotElf, |machine| ElfProbe::Elf {
        architecture: match machine {
            ELF_MACHINE_X86_64 => Some(Arch::X86_64),
            ELF_MACHINE_AARCH64 => Some(Arch::Aarch64),
            _ => None,
        },
    })
}

fn parse_elf_machine(header: &[u8]) -> Option<u16> {
    if header.get(..4)? != b"\x7fELF" || *header.get(6)? != 1 {
        return None;
    }
    let (size, size_offset) = match header.get(4)? {
        1 => (52, 40), // ELF32
        2 => (64, 52), // ELF64
        _ => return None,
    };
    header.get(..size)?;
    let little_endian = match header.get(5)? {
        1 => true,
        2 => false,
        _ => return None,
    };
    let read_u16 = |offset: usize| -> Option<u16> {
        let bytes = [*header.get(offset)?, *header.get(offset + 1)?];
        Some(if little_endian {
            u16::from_le_bytes(bytes)
        } else {
            u16::from_be_bytes(bytes)
        })
    };
    let version_bytes = header.get(20..24)?.try_into().ok()?;
    let version = if little_endian {
        u32::from_le_bytes(version_bytes)
    } else {
        u32::from_be_bytes(version_bytes)
    };
    // ET_EXEC and ET_DYN (PIE) can be executable images. A valid but unknown
    // machine remains unidentified; malformed class/version/size is damage.
    if !matches!(read_u16(16)?, 2 | 3)
        || version != 1
        || usize::from(read_u16(size_offset)?) != size
    {
        return None;
    }
    read_u16(18)
}

#[cfg(test)]
mod tests;

fn diagnostic_runtime_version(
    root: &Path,
    directory: Option<&ManagedDirectoryName>,
) -> Option<RuntimeVersion> {
    version_from_dir(directory.map_or(root, |directory| Path::new(directory.label())))
}

fn check_directory_digest(artifact: &mut ScannedArtifact, directory: &ManagedDirectoryName) {
    if let Some(found) = artifact.digest
        && !directory.matches_digest(found)
    {
        artifact.status = ArtifactStatus::Invalid;
        artifact.detail = Some(format!(
            "managed directory prefix {} does not match installed bytes {found}",
            directory.digest_prefix()
        ));
    }
}
