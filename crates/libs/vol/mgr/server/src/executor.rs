pub(crate) mod logical_digest;
use logical_digest::{DigestReader, DigestSink, LogicalArtifactDigest};
mod stream_push;
use stream_push::StreamPushConfig;
pub use stream_push::{
    DEFAULT_STREAM_PUSH_BUFFER_BUDGET_BYTES, SnapshotUploadMode, StreamingPushBudget,
};

use pplx_space_common_core::types::JobId;

use std::collections::HashMap;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::artifact::ArtifactEntryRole;
use crate::codec_admission::{
    CodecAdmission, CodecAdmissionClosed, CodecPermit, CodecPriority, CodecSlot,
};
use crate::credentials::{StorageAccess, UnwrapDekError, exchange_credentials, unwrap_dek};
use crate::crypto::{self, CryptoError, decrypted_sibling, encrypted_sibling};
use dashmap::DashMap;
use pplx_space_common_core::types::{
    KekProvider, MasterKeyIdentifier, MaterializationLayout, MaterializationMode, OrgId, SandboxId,
    SnapshotArtifact, StorageUri, SubjectId, SubjectIdParseError, TransferScopeId,
};
use pplx_space_common_io::sparse::{SparseDataRange, SparseFileWriter, SparseMaterialization};
use pplx_space_common_jobs::{
    Job, JobError, JobExecutor, JobFailureKind, JobFailureReason, JobPriority,
};
use pplx_space_common_svc::page_cache;
use pplx_space_object_store::{
    ObjectMeta, ObjectStore, ObjectStoreError, ObjectTarget, S3ObjectStore,
};
use pplx_space_volmgr_client::{
    VERIFIED_REPLICA_PROVENANCE_VERSION, VerifiedReplicaProvenance,
    verified_replica_provenance_path,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

// zstd frame magic: first 4 bytes of any zstd-compressed stream.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

// zstd compression level: 3 gives a good balance of speed vs. ratio for large
// binary files. Level 1 is ~2× faster with ~10%
// lower ratio; level 9 is ~4× slower with ~5% better ratio.
const ZSTD_LEVEL: i32 = 3;
const MEMORY_SPARSE_BLOCK_BYTES: usize = 4 * 1024;
const ROOTFS_SPARSE_BLOCK_BYTES: usize = 64 * 1024;

// A decode whose frame pledges fewer decompressed bytes than this skips
// codec admission: it costs at most about a second of CPU, and queueing
// metadata-sized artifacts behind gigabyte decodes would add pull latency
// where today there is none. Frames with an unknown decompressed size
// (streaming zstd, all ciphertext) are never proven small, so they always
// take a permit regardless of compressed length.
const CODEC_ADMISSION_MIN_INPUT_BYTES: u64 = 256 * 1024 * 1024;
const SHADOW_COMPARISON_CONCURRENCY: usize = 1;

/// Best-effort decompressed size from the zstd frame header. Streaming
/// encoders (including this executor's own push path) omit the
/// content-size field, so `None` means "unknown", which decode admission
/// treats as potentially large.
async fn frame_content_size(path: &Path) -> Option<u64> {
    use tokio::io::AsyncReadExt;
    let mut f = tokio::fs::File::open(path).await.ok()?;
    // Magic(4) + descriptor(1) + window(<=1) + dictionary id(<=4) +
    // content size(<=8): 18 bytes always covers the frame header.
    let mut header = [0u8; 18];
    let mut filled = 0;
    while let Some(buf) = header.get_mut(filled..) {
        if buf.is_empty() {
            break;
        }
        match f.read(buf).await {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(_) => return None,
        }
    }
    header
        .get(..filled)
        .and_then(|h| zstd::zstd_safe::get_frame_content_size(h).ok().flatten())
}

/// How many CPU-bound zstd passes (compress or decompress) may run at
/// once. `0` means auto-size to the CPU quota, or to the deployed CPU
/// request when the pod has no CPU limit.
///
/// Each zstd pass is single-threaded, and `spawn_blocking`'s pool is ~512
/// threads, so without admission a pull storm runs every decode
/// concurrently on the pod's CPU share — N jobs each at 1/N speed instead
/// of share-many at full speed. Bounding to the share keeps the same
/// aggregate throughput but finishes jobs in arrival order, which is what
/// per-job completion latency is made of.
fn resolved_codec_concurrency(configured: usize) -> usize {
    if configured > 0 {
        return configured;
    }
    // Kubernetes enforces CPU *limits* through the cgroup CFS quota, which
    // `available_parallelism` (scheduler-affinity based) does not observe
    // unless the pod is cpuset-pinned. Read the quota directly. Without a
    // quota (the pod runs limit-less), the deployed whole-core CPU request
    // arrives as WORKER_THREADS (Downward API on Kubernetes, Environment=
    // in the direct systemd units) and is the intended share — affinity
    // would size to every core of a metal host. Affinity remains the last
    // resort for undeployed contexts.
    detect_cgroup_cpu_quota_cores()
        .or_else(cpu_request_cores_from_env)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(std::num::NonZeroUsize::get)
        })
        .unwrap_or(2)
        .max(1)
}

/// The whole-core CPU request the deployment stamped into `WORKER_THREADS`,
/// or `None` when unset or malformed.
fn cpu_request_cores_from_env() -> Option<usize> {
    parse_cpu_request_cores(&std::env::var("WORKER_THREADS").ok()?)
}

fn parse_cpu_request_cores(raw: &str) -> Option<usize> {
    raw.trim().parse().ok().filter(|cores| *cores > 0)
}

/// Cores implied by a CFS `quota`/`period` pair, rounded to the nearest
/// whole core and floored at 1. Integer arithmetic keeps this free of
/// float sign/precision casts.
fn cores_from_cfs_quota(quota: u64, period: u64) -> Option<usize> {
    if period == 0 {
        return None;
    }
    let rounded = (quota + period / 2) / period;
    Some(usize::try_from(rounded).unwrap_or(usize::MAX).max(1))
}

/// Read the cgroup CPU quota (in whole cores) the process is limited to,
/// or `None` when unlimited or unreadable. Handles cgroup v2 (`cpu.max`)
/// and v1 (`cpu.cfs_quota_us` / `cpu.cfs_period_us`). Sync file reads at
/// startup only.
fn detect_cgroup_cpu_quota_cores() -> Option<usize> {
    // cgroup v2: "$QUOTA $PERIOD" where QUOTA is "max" or microseconds.
    if let Ok(contents) = std::fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut fields = contents.split_whitespace();
        let quota = fields.next()?;
        let period: u64 = fields.next()?.parse().ok()?;
        if quota == "max" {
            return None;
        }
        return cores_from_cfs_quota(quota.parse().ok()?, period);
    }
    // cgroup v1.
    let quota: i64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    // -1 (unlimited) and any non-positive value have no quota.
    let quota = u64::try_from(quota).ok().filter(|&q| q > 0)?;
    let period: u64 = std::fs::read_to_string("/sys/fs/cgroup/cpu/cpu.cfs_period_us")
        .ok()?
        .trim()
        .parse()
        .ok()?;
    cores_from_cfs_quota(quota, period)
}

type DestinationPullLocks = Arc<DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>;

fn transfer_artifact_name(local_path: &Path) -> Option<&str> {
    let name = local_path.file_name()?.to_str()?;
    let name = name.strip_prefix('.').unwrap_or(name);
    Some(name.split(".restore-").next().unwrap_or(name))
}

pub(crate) fn snapshot_artifact_from_local_path(local_path: &Path) -> Option<SnapshotArtifact> {
    match transfer_artifact_name(local_path)? {
        "snapshot.json" => Some(SnapshotArtifact::Metadata),
        "vmstate" => Some(SnapshotArtifact::VmState),
        "memory" => Some(SnapshotArtifact::Memory),
        "rootfs.ext4" => Some(SnapshotArtifact::Rootfs),
        "rootfs.btrfs-send" => Some(SnapshotArtifact::RootfsBtrfsSend),
        "snapshot.luks" => Some(SnapshotArtifact::EncryptedSnapshotVolume),
        "cow.luks" => Some(SnapshotArtifact::EncryptedRootfsCow),
        _ => None,
    }
}

pub(crate) fn snapshot_artifact_from_entry_role(role: &str) -> Option<SnapshotArtifact> {
    [
        SnapshotArtifact::Rootfs,
        SnapshotArtifact::RootfsBtrfsSend,
        SnapshotArtifact::Memory,
        SnapshotArtifact::VmState,
        SnapshotArtifact::EncryptedSnapshotVolume,
        SnapshotArtifact::EncryptedRootfsCow,
        SnapshotArtifact::Metadata,
    ]
    .into_iter()
    .find(|artifact| artifact.file_name() == role)
}

fn transfer_artifact_tag(artifact: Option<SnapshotArtifact>) -> &'static str {
    match artifact {
        Some(SnapshotArtifact::Metadata) => "metadata",
        Some(SnapshotArtifact::VmState) => "vmstate",
        Some(SnapshotArtifact::Memory) => "memory",
        Some(SnapshotArtifact::Rootfs) => "rootfs_ext4",
        Some(SnapshotArtifact::RootfsBtrfsSend) => "rootfs_btrfs_send",
        Some(SnapshotArtifact::EncryptedSnapshotVolume) => "encrypted_snapshot_volume",
        Some(SnapshotArtifact::EncryptedRootfsCow) => "encrypted_rootfs_cow",
        None => "other",
    }
}

/// Closed vocabulary for the `phase` tag on transfer metrics. The string
/// form exists only at the emission boundary; everything in between
/// passes the enum so additions stay exhaustive and typos can't compile.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TransferPhase {
    Download,
    DownloadDecode,
    Decrypt,
    DecryptDecompress,
    DecryptWait,
    Decompress,
    DecompressWait,
    CompressWait,
    Promote,
    ShadowReflink,
}

impl TransferPhase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Download => "download",
            Self::DownloadDecode => "download_decode",
            Self::Decrypt => "decrypt",
            Self::DecryptDecompress => "decrypt_decompress",
            Self::DecryptWait => "decrypt_wait",
            Self::Decompress => "decompress",
            Self::DecompressWait => "decompress_wait",
            Self::CompressWait => "compress_wait",
            Self::Promote => "promote",
            Self::ShadowReflink => "shadow_reflink",
        }
    }
}

impl std::fmt::Display for TransferPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn emit_transfer_phase_duration(
    kind: &str,
    artifact: &str,
    phase: TransferPhase,
    mode: MaterializationMode,
    started_at: Instant,
) {
    pplx_space_metrics::volume_manager::transfer_phase_duration(
        kind,
        artifact,
        phase.as_str(),
        mode,
        started_at,
    );
}

fn effective_materialization_mode(
    artifact: Option<SnapshotArtifact>,
    requested: MaterializationMode,
) -> MaterializationMode {
    if matches!(
        artifact,
        Some(
            SnapshotArtifact::Memory
                | SnapshotArtifact::Rootfs
                | SnapshotArtifact::EncryptedSnapshotVolume
                | SnapshotArtifact::EncryptedRootfsCow
        )
    ) {
        requested
    } else {
        MaterializationMode::Disabled
    }
}

fn execution_materialization_mode(
    artifact: Option<SnapshotArtifact>,
    requested: MaterializationMode,
) -> MaterializationMode {
    // LUKS backings contain literal zero regions outside their written encrypted
    // sectors. Exact-zero materialization preserves the ciphertext and logical
    // length, including when retrying jobs persisted before sparse support.
    let requested = if matches!(
        artifact,
        Some(
            SnapshotArtifact::Memory
                | SnapshotArtifact::EncryptedSnapshotVolume
                | SnapshotArtifact::EncryptedRootfsCow
        )
    ) {
        MaterializationMode::Enabled
    } else {
        requested
    };
    effective_materialization_mode(artifact, requested)
}

fn materialization_block_bytes(artifact: Option<SnapshotArtifact>) -> usize {
    match artifact {
        Some(SnapshotArtifact::Rootfs) => ROOTFS_SPARSE_BLOCK_BYTES,
        _ => MEMORY_SPARSE_BLOCK_BYTES,
    }
}

fn emit_transfer_throughput_mib_s(
    kind: &str,
    artifact: &str,
    phase: TransferPhase,
    mode: MaterializationMode,
    bytes: u64,
    started_at: Instant,
) {
    let elapsed = started_at.elapsed().as_secs_f64();
    if elapsed > 0.0 {
        let mib_s = (bytes as f64 / 1024.0 / 1024.0) / elapsed;
        pplx_space_metrics::volume_manager::transfer_throughput_mib_s(
            kind,
            artifact,
            phase.as_str(),
            mode,
            mib_s,
        );
    }
}

#[cfg(unix)]
async fn observe_decompression_allocation(
    kind: &str,
    artifact: &str,
    job_id: &JobId,
    subject_id: &str,
    local_path: &Path,
    report: DecompressionReport,
) -> Option<MaterializationObservation> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match tokio::fs::metadata(local_path).await {
        Ok(metadata) => metadata,
        Err(error) => {
            tracing::debug!(
                %error,
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "could not observe decompressed artifact allocation"
            );
            return None;
        }
    };
    let logical_bytes = metadata.len();
    let allocated_bytes = metadata.blocks().saturating_mul(512);
    let saved_bytes = logical_bytes.saturating_sub(allocated_bytes);
    let allocation_ratio = if logical_bytes == 0 {
        0.0
    } else {
        allocated_bytes as f64 / logical_bytes as f64
    };
    let layout = if report.mode == MaterializationMode::Enabled {
        MaterializationLayout::Sparse
    } else {
        MaterializationLayout::Dense
    };
    pplx_space_metrics::volume_manager::transfer_materialization_allocation(
        kind,
        artifact,
        report.mode,
        layout,
        logical_bytes,
        allocated_bytes,
        report.candidate_hole_bytes,
    );
    let projected_allocated_bytes = logical_bytes.saturating_sub(report.candidate_hole_bytes);
    tracing::info!(
        job_id = %job_id,
        subject_id,
        local_path = %local_path.display(),
        artifact,
        layout = %layout,
        mode = %report.mode,
        logical_bytes,
        allocated_bytes,
        candidate_hole_bytes = report.candidate_hole_bytes,
        projected_allocated_bytes,
        saved_bytes,
        allocation_ratio,
        "decompressed artifact allocation observed"
    );
    Some(MaterializationObservation {
        mode: report.mode,
        layout,
        logical_bytes,
        allocated_bytes,
        candidate_hole_bytes: report.candidate_hole_bytes,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ShadowComparisonOutcome {
    Match,
    Mismatch,
    Error,
    SkippedBusy,
    SkippedDisk,
}

impl ShadowComparisonOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::Mismatch => "mismatch",
            Self::Error => "error",
            Self::SkippedBusy => "skipped_busy",
            Self::SkippedDisk => "skipped_disk",
        }
    }
}

impl std::fmt::Display for ShadowComparisonOutcome {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ShadowComparisonObservation {
    logical_bytes: u64,
    allocated_bytes: u64,
    candidate_hole_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaterializationObservation {
    pub mode: MaterializationMode,
    pub layout: MaterializationLayout,
    pub logical_bytes: u64,
    pub allocated_bytes: u64,
    pub candidate_hole_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
struct FileReplicaProvenance {
    #[serde(flatten)]
    replica: VerifiedReplicaProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    materialization: Option<MaterializationObservation>,
}

struct DestinationPullGuard {
    locks: DestinationPullLocks,
    path: PathBuf,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for DestinationPullGuard {
    fn drop(&mut self) {
        self.guard.take();
        self.locks
            .remove_if(&self.path, |_path, lock| Arc::strong_count(lock) == 1);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum FileModeError {
    #[error("file_mode must only contain rwx bits: {mode:o}")]
    InvalidBits { mode: u32 },
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct FileMode(u32);

impl FileMode {
    pub fn new(mode: u32) -> Result<Self, FileModeError> {
        if mode & !0o777 != 0 {
            return Err(FileModeError::InvalidBits { mode });
        }
        Ok(Self(mode))
    }

    pub fn bits(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum FileModeApplyPolicy {
    Required,
    ExistingFile,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PullExistingPolicy {
    #[default]
    LegacySkipPresent,
    VerifiedReplica,
}

impl PullExistingPolicy {
    pub fn is_legacy_skip_present(policy: &Self) -> bool {
        *policy == Self::LegacySkipPresent
    }

    pub fn as_dedup_str(self) -> &'static str {
        match self {
            Self::LegacySkipPresent => "legacy_skip_present",
            Self::VerifiedReplica => "verified_replica",
        }
    }
}

struct PullExecutionOptions<'a> {
    resolved_dek: Option<&'a [u8]>,
    existing_policy: PullExistingPolicy,
    cancellation: CancellationToken,
}

impl serde::Serialize for FileMode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> serde::Deserialize<'de> for FileMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let mode = <u32 as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(mode).map_err(serde::de::Error::custom)
    }
}

/// Payload stored in the generic `Job.payload` field for transfer jobs.
///
/// `subject_id` is the wire form of [`SubjectId`] (`"<kind>:<id>"`); the
/// executor re-parses it at run time so a node restart between submit and
/// execute does not lose the typed identity. The `#[serde(alias = "sandbox_id")]`
/// keeps deserialization of pre-rename payloads sitting in the SQLite job
/// queue working — those store a bare sandbox UUID in this slot, which is
/// re-typed via [`parse_subject_id_with_legacy_fallback`] at execute time.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct TransferPayload {
    #[serde(alias = "sandbox_id")]
    pub subject_id: String,
    pub local_path: String,
    pub s3_uri: String,
    /// Restore-attempt scope used to cancel a superseded set of pulls without
    /// affecting a newer generation for the same sandbox.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer_scope_id: Option<TransferScopeId>,
    /// Semantic snapshot artifact selected at the request boundary. This is
    /// persisted because batch callers may choose an arbitrary destination
    /// filename; restore materialization must follow the entry's role rather
    /// than rediscovering it from that filename at execution time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_artifact: Option<SnapshotArtifact>,
    /// Path to a vol-mgr-owned reflink/copy of `local_path` made at
    /// submit time. Set on push jobs to insulate the upload from later
    /// unlinks of the original. Absent on pull jobs, and absent on
    /// legacy push jobs queued before this field existed — in those
    /// cases the executor falls back to `local_path`. See
    /// `crate::staging` for the staging-dir lifecycle.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_path: Option<String>,
    /// When true, return the file in exactly the byte form S3 served it —
    /// no transparent zstd decompression. Pull jobs from template-manager
    /// set this so they can run their own SHA-validating decoder over the
    /// same bytes vol-mgr fetched; non-raw transfers leave it false
    /// so vol-mgr's decompress-on-zstd-magic behavior is preserved.
    #[serde(default)]
    pub raw: bool,
    /// Optional Unix mode to apply after a successful pull. Absence preserves
    /// the pre-existing default mode behavior for legacy callers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_mode: Option<FileMode>,
    /// Existing-destination handling for pull jobs. The default preserves
    /// legacy PullFile behavior for persisted rows that predate this field.
    #[serde(
        default,
        skip_serializing_if = "PullExistingPolicy::is_legacy_skip_present"
    )]
    pub existing_file_policy: PullExistingPolicy,
    /// Sparse-decompression rollout assignment captured when the job was
    /// submitted. Persisting it makes retries deterministic even if the Eppo
    /// allocation changes while a transfer is queued or running.
    #[serde(default, skip_serializing_if = "MaterializationMode::is_disabled")]
    pub materialization_mode: MaterializationMode,
    /// Rootfs-specific sparse-decompression assignment. `None` identifies a
    /// payload written before rootfs rollout support and must remain dense.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_materialization_mode: Option<MaterializationMode>,
    /// Optional envelope-encryption parameters. Present only on the
    /// synthetic per-entry payloads the artifact executor builds when it
    /// fans a `push_artifact`/`pull_artifact` job out over its entries:
    /// push entries AES-256-GCM-encrypt their (post-zstd) bytes before
    /// uploading, pull entries decrypt before any transparent zstd
    /// decompression. The plaintext DEK is fetched from credmgr at
    /// execute time using the wrapped form stored here. Single-file
    /// `push`/`pull` jobs are always plaintext and leave this `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<TransferEncryption>,
    /// Artifact identity (UUID string), set together with `encryption`
    /// on the synthetic per-entry payloads. Binds the artifact-level
    /// wrap AAD (DEK) so the per-chunk AEAD AAD reproduces on push and
    /// on pull. Never serialized onto a single-file job row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    /// This entry's role within the artifact, set together with
    /// `artifact_id`. Bound into the per-chunk AEAD AAD so entries
    /// sharing the artifact DEK are not interchangeable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_role: Option<String>,
}

/// Envelope-encryption parameters for an artifact, stored on its
/// [`ArtifactPayload`] (and copied onto each synthetic per-entry
/// [`TransferPayload`]). A serde-friendly Rust struct so the persisted
/// JSON stays self-contained. Identifying fields are typed via the
/// shared `common-core` newtypes, so deserialising a stored payload
/// re-runs the same parse/validate path as the gRPC boundary; corrupt
/// or hand-edited rows fail the worker's `from_json` step rather than
/// reaching credmgr. `wrapped_dek` is the opaque ciphertext we persist;
/// the AADs (artifact-level wrap + per-entry chunk) are derived at
/// execute time from the payload's subject, `artifact_id`, and entry
/// `role` via [`derive_artifact_aads`], so no AAD bytes round-trip
/// through the request or the SQLite row.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TransferEncryption {
    pub organization_id: OrgId,
    pub provider: KekProvider,
    pub master_key_identifier: MasterKeyIdentifier,
    pub wrapped_dek: Vec<u8>,
}

impl TransferEncryption {
    /// Stable fingerprint of `(org_id, provider, master_key_identifier,
    /// wrapped_dek)` for use in the job dedup key. Different
    /// callers asking to encrypt the same `(subject, local_path,
    /// s3_uri)` with different DEKs (or even the same wrapped bytes
    /// under different orgs) produce different ciphertext, so they
    /// must NOT collapse onto a single in-flight job. SHA-256 of the
    /// NUL-separated concatenation, hex-encoded.
    fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.organization_id.as_uuid().as_bytes());
        hasher.update([0]);
        hasher.update(self.provider.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.master_key_identifier.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(&self.wrapped_dek);
        let digest = hasher.finalize();
        hex_encode(&digest)
    }
}

/// Derive the two AADs an encrypted artifact entry needs from its
/// synthetic [`TransferPayload`]:
///
/// - the artifact-level **wrap AAD** `(org, sandbox)` that binds the
///   DEK at credmgr `IssueDek`/`UnwrapDek` (cross-artifact swaps are
///   blocked by per-DEK isolation, not by binding artifact_id here), and
/// - the per-entry **chunk AAD** `wrap_aad || role` that binds each
///   file's AES-GCM chunks so entries sharing the artifact DEK are not
///   interchangeable.
///
/// Both reproduce deterministically on push and on pull without
/// the AAD bytes ever crossing the wire — the caller round-trips
/// the entry `role`; `(org_id, sandbox_id)` are inherent properties
/// of the encrypted artifact. Encrypted transfers are
/// sandbox-scoped artifacts: a non-sandbox subject or a missing
/// `entry_role` is a permanent error (the artifact executor always
/// populates `entry_role` on the synthetic payload).
fn derive_artifact_aads(
    p: &TransferPayload,
    organization_id: OrgId,
) -> Result<(Vec<u8>, Vec<u8>), JobError> {
    let subject = parse_subject_id_with_legacy_fallback(&p.subject_id)
        .map_err(|e| JobError::Permanent(format!("derive AAD: subject_id parse: {e}")))?;
    let sandbox_id = match subject {
        SubjectId::Sandbox(id) => id,
        SubjectId::Template(_) => {
            return Err(JobError::Permanent(
                "envelope-encrypted transfers require a sandbox subject".into(),
            ));
        }
    };
    let role: ArtifactEntryRole = p
        .entry_role
        .as_deref()
        .ok_or_else(|| JobError::Permanent("derive AAD: missing entry_role".into()))?
        .parse()
        .map_err(|e| JobError::Permanent(format!("derive AAD: entry_role parse: {e}")))?;
    let wrap = crypto::wrap_aad(organization_id, &sandbox_id);
    let entry = crypto::entry_aad(&wrap, &role);
    Ok((wrap, entry))
}

fn hex_encode(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(char::from(
            *LUT.get(usize::from(b >> 4))
                .expect("high nibble always indexes hex LUT"),
        ));
        out.push(char::from(
            *LUT.get(usize::from(b & 0x0F))
                .expect("low nibble always indexes hex LUT"),
        ));
    }
    out
}

fn bytes_len_to_job_i64(bytes: u64, context: &'static str) -> Result<i64, JobError> {
    i64::try_from(bytes).map_err(|source| {
        JobError::Permanent(format!("{context}: byte count exceeds i64::MAX: {source}"))
    })
}

impl TransferPayload {
    fn effective_snapshot_artifact(&self) -> Option<SnapshotArtifact> {
        self.snapshot_artifact
            .or_else(|| {
                self.entry_role
                    .as_deref()
                    .and_then(snapshot_artifact_from_entry_role)
            })
            .or_else(|| snapshot_artifact_from_local_path(Path::new(&self.local_path)))
    }

    fn requested_materialization_mode(
        &self,
        artifact: Option<SnapshotArtifact>,
    ) -> MaterializationMode {
        if artifact == Some(SnapshotArtifact::Rootfs) {
            self.rootfs_materialization_mode
                .unwrap_or(MaterializationMode::Disabled)
        } else {
            self.materialization_mode
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("TransferPayload serialization is infallible")
    }

    pub fn from_json(s: &str) -> Result<Self, JobError> {
        serde_json::from_str(s).map_err(|e| {
            JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                format!("invalid transfer payload: {e}"),
            )
        })
    }

    /// Path the executor should read from for an upload — staged if set,
    /// otherwise the caller's original `local_path` (legacy behaviour).
    pub fn upload_source(&self) -> &str {
        self.staged_path.as_deref().unwrap_or(&self.local_path)
    }

    /// Whether `execute_pull` should run the transparent zstd-decompress
    /// pass after a successful download.
    ///
    /// `raw=true` (template-manager) suppresses the decompress regardless of
    /// the bytes on disk, so callers that opted into wire-form receive what
    /// they asked for. `raw=false` (the default) keeps the existing
    /// symmetric behavior with push-side compression: zstd files get
    /// transparently inflated, plain files pass through.
    pub fn should_transparent_decompress(&self, file_is_zstd: bool) -> bool {
        !self.raw && file_is_zstd
    }
}

/// Parse a `subject_id` string into a typed [`SubjectId`], accepting two
/// wire shapes:
///
/// - The kinded `"<kind>:<id>"` form — `"sandbox:<uuid>"` or
///   `"template:<name>"`.
/// - A bare sandbox UUID, accepted for compatibility with older clients and
///   persisted `sandbox_id` fields. Resolves to [`SubjectId::Sandbox`].
///
/// This dual acceptance is what lets the vol-mgr DaemonSet roll out
/// without coordinating with sandbox-manager: a new vol-mgr happily
/// decodes the bare UUIDs old or new sbx-mgr pods are sending, and a
/// future release that switches sbx-mgr to the prefixed form keeps
/// working too. Persisted SQLite jobs from an older vol-mgr image that
/// stored a bare UUID under the JSON field `sandbox_id` (now aliased
/// via [`serde(alias)`]) get re-typed the same way on recovery.
///
/// Errors are reported as the *new-form* [`SubjectIdParseError`] — that
/// describes what the boundary saw and lets the caller log a single
/// typed reason. A bare-UUID failure is silent: we only try the bare
/// shape when the kinded shape didn't parse and the input has no colon,
/// and either way the kinded-form error is the one that names the
/// contract the caller violated.
pub(crate) fn parse_subject_id_with_legacy_fallback(
    raw: &str,
) -> Result<SubjectId, SubjectIdParseError> {
    match raw.parse::<SubjectId>() {
        Ok(s) => Ok(s),
        Err(new_form_err) => {
            if !raw.contains(':')
                && let Ok(id) = raw.parse::<SandboxId>()
            {
                return Ok(SubjectId::Sandbox(id));
            }
            Err(new_form_err)
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TransferDedupOptions<'a> {
    pub file_mode: Option<FileMode>,
    pub existing_file_policy: PullExistingPolicy,
    pub snapshot_artifact: Option<SnapshotArtifact>,
    pub materialization_mode: MaterializationMode,
    pub encryption: Option<&'a TransferEncryption>,
}

/// Builds the deduplication key for a transfer job.
///
/// `raw` is part of the key for pull jobs: two callers asking for the same
/// `(subject, local_path, s3_uri)` but with different `raw` flags want
/// different bytes on disk (one wants the zstd wire bytes, the other wants
/// the transparently-decompressed plain bytes), so they must NOT collapse
/// onto a single in-flight job. Push jobs ignore `raw` (it has no meaning
/// there) and conventionally pass `false`. File mode and existing-file policy
/// are also part of the key because successful pull permissions and
/// existing-file guarantees are observable postconditions. Snapshot artifact
/// identity and non-disabled materialization mode are included because they
/// control the retained file and shadow-verification postconditions.
pub fn dedup_key(
    kind: &str,
    subject_id: &str,
    local_path: &str,
    s3_uri: &str,
    raw: bool,
    options: TransferDedupOptions<'_>,
) -> String {
    let file_mode = options
        .file_mode
        .map(|mode| format!("{:o}", mode.bits()))
        .unwrap_or_else(|| "-".to_string());
    let enc_fp = options
        .encryption
        .map(TransferEncryption::fingerprint)
        .unwrap_or_else(|| "-".to_string());
    let materialization_suffix = options.materialization_mode.dedup_suffix();
    match options.snapshot_artifact {
        Some(snapshot_artifact) => format!(
            "{kind}\0{subject_id}\0{local_path}\0{s3_uri}\0{raw}\0{file_mode}\0{}\0{}\0{enc_fp}{materialization_suffix}",
            options.existing_file_policy.as_dedup_str(),
            snapshot_artifact.file_name(),
        ),
        None => format!(
            "{kind}\0{subject_id}\0{local_path}\0{s3_uri}\0{raw}\0{file_mode}\0{}\0{enc_fp}{materialization_suffix}",
            options.existing_file_policy.as_dedup_str(),
        ),
    }
}

/// Sibling `.partial` path used for atomic-rename downloads.
///
/// The download streams into `<local_path>.partial`; on success the file is
/// renamed to `<local_path>` so callers never observe a torn file. We append
/// the suffix rather than calling `with_extension` so that `foo.zst` becomes
/// `foo.zst.partial` and not `foo.partial` (which would alias with siblings).
fn partial_path(local_path: &Path) -> PathBuf {
    let mut p = local_path.to_path_buf();
    let mut name = local_path
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(".partial");
    p.set_file_name(name);
    p
}

fn requested_file_mode_bits(p: &TransferPayload) -> Option<u32> {
    p.file_mode.map(FileMode::bits)
}

fn file_replica_provenance(
    p: &TransferPayload,
    object: &ObjectMeta,
    local_size: u64,
) -> VerifiedReplicaProvenance {
    VerifiedReplicaProvenance {
        version: VERIFIED_REPLICA_PROVENANCE_VERSION,
        subject_id: p.subject_id.clone(),
        s3_uri: p.s3_uri.clone(),
        raw: p.raw,
        file_mode: requested_file_mode_bits(p),
        snapshot_artifact: p.snapshot_artifact,
        object_size: object.size,
        object_etag: object.etag.clone(),
        local_size,
    }
}

impl FileReplicaProvenance {
    fn new(
        p: &TransferPayload,
        object: &ObjectMeta,
        local_size: u64,
        materialization: Option<MaterializationObservation>,
    ) -> Self {
        Self {
            replica: file_replica_provenance(p, object, local_size),
            materialization,
        }
    }
}

#[cfg(test)]
fn read_materialization_observation(local_path: &Path) -> Option<MaterializationObservation> {
    let bytes = std::fs::read(verified_replica_provenance_path(local_path)).ok()?;
    serde_json::from_slice::<FileReplicaProvenance>(&bytes)
        .ok()?
        .materialization
}

fn materialization_is_compatible(
    requested: MaterializationMode,
    observation: Option<&MaterializationObservation>,
) -> bool {
    match requested {
        MaterializationMode::Disabled => true,
        // The dense observation records the requested writer mode, not a
        // successful background comparison. Shadow results are not persisted,
        // so no completed replica can prove that postcondition yet.
        MaterializationMode::Shadow => false,
        MaterializationMode::Enabled => observation.is_some_and(|observation| {
            observation.mode == MaterializationMode::Enabled
                && observation.layout == MaterializationLayout::Sparse
        }),
    }
}

/// Best-effort unlink of a `.partial` sibling.
///
/// `NotFound` is the common expected case (no stale file to sweep), so we
/// silence it. Any other failure is logged so silent fs corruption — a
/// permission flip, a stuck mount — doesn't accumulate without diagnostics.
/// On a successful removal we emit a debug log so audits ("what was deleted
/// and why?") can trace each unlink to the code path that triggered it.
async fn unlink_partial(path: &Path, reason: &str) {
    match tokio::fs::remove_file(path).await {
        Ok(()) => tracing::debug!(
            path = %path.display(),
            reason,
            "unlinked .partial"
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(
            error = %err,
            path = %path.display(),
            reason,
            "unlink .partial failed"
        ),
    }
}

fn classify(e: &ObjectStoreError) -> JobError {
    let msg = e.to_string();
    match e {
        ObjectStoreError::NotFound { .. } => {
            JobError::permanent_with_reason(JobFailureReason::NotFound, msg)
        }
        ObjectStoreError::PermissionDenied { .. } => {
            JobError::permanent_with_reason(JobFailureReason::PermissionDenied, msg)
        }
        ObjectStoreError::Io(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
            ) =>
        {
            JobError::permanent_with_reason(JobFailureReason::Internal, msg)
        }
        _ => JobError::Retriable(msg),
    }
}

fn ensure_pull_failure_reason(error: JobError) -> JobError {
    error.with_default_failure_reason(JobFailureReason::Internal)
}

struct TransformInput {
    codec_permit: Option<CodecPermit>,
    digest: Option<DigestSink>,
}

/// Compress `src` to a sibling `.zst.tmp` file using zstd.
/// Returns the temp path and the original (uncompressed) size.
/// The caller is responsible for deleting the temp file.
///
/// `codec_permit` (if the caller took one) is moved into the blocking
/// task and released only when the CPU pass exits, so a cancelled caller
/// cannot free the permit while the detached `spawn_blocking` keeps
/// burning CPU.
async fn compress_to_temp(src: &Path, input: TransformInput) -> Result<(PathBuf, u64), JobError> {
    let original_size = tokio::fs::metadata(src)
        .await
        .map(|m| m.len())
        .map_err(|e| JobError::Permanent(format!("stat source file: {e}")))?;

    // Put the temp file next to the source to avoid cross-device copies.
    let tmp_path = src.with_extension("zst.tmp");
    let src = src.to_path_buf();
    let tmp = tmp_path.clone();

    tokio::task::spawn_blocking(move || -> Result<(), JobError> {
        let _codec_permit = input.codec_permit;
        let reader = std::fs::File::open(&src)
            .map_err(|e| JobError::Permanent(format!("open source for compression: {e}")))?;
        let mut reader = DigestReader::new(reader, input.digest);
        let mut writer = std::fs::File::create(&tmp)
            .map_err(|e| JobError::Permanent(format!("create compression temp file: {e}")))?;
        zstd::stream::copy_encode(&mut reader, &mut writer, ZSTD_LEVEL)
            .map_err(|e| JobError::Permanent(format!("zstd compress: {e}")))?;
        page_cache::advise_dontneed(reader.inner(), &src, "compress volume upload input");
        page_cache::advise_dontneed(&writer, &tmp, "write compressed volume upload temp");
        Ok(())
    })
    .await
    .map_err(|e| JobError::Permanent(format!("compress task panicked: {e}")))??;

    Ok((tmp_path, original_size))
}

/// Check the first 4 bytes of `path` to determine if it is zstd-compressed.
async fn is_zstd(path: &Path) -> bool {
    use tokio::io::AsyncReadExt;
    let Ok(mut f) = tokio::fs::File::open(path).await else {
        return false;
    };
    let mut magic = [0u8; 4];
    f.read_exact(&mut magic).await.is_ok() && magic == ZSTD_MAGIC
}

fn sparse_shadow_path(path: &Path) -> PathBuf {
    let mut shadow = path.to_path_buf();
    let mut name = shadow.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".sparse-shadow-{}.tmp", uuid::Uuid::now_v7()));
    shadow.set_file_name(name);
    shadow
}

fn sparse_shadow_source_path(staging_dir: &Path) -> PathBuf {
    staging_dir.join(format!("sparse-shadow-source-{}.tmp", uuid::Uuid::now_v7()))
}

#[derive(Debug, thiserror::Error)]
enum SparseShadowCmpError {
    #[error("sparse shadow comparison cancelled")]
    Cancelled,
    #[error("sparse shadow bytes differ from dense output")]
    Mismatch,
    #[error("{operation}: {source}")]
    Io {
        operation: &'static str,
        #[source]
        source: std::io::Error,
    },
}

impl SparseShadowCmpError {
    fn io(operation: &'static str, source: std::io::Error) -> Self {
        Self::Io { operation, source }
    }
}

fn compare_completed_files(
    dense: &Path,
    sparse: &Path,
    shutdown_token: &CancellationToken,
) -> Result<(), SparseShadowCmpError> {
    let mut dense_file = std::fs::File::open(dense)
        .map_err(|error| SparseShadowCmpError::io("open dense comparison input", error))?;
    let mut sparse_file = std::fs::File::open(sparse)
        .map_err(|error| SparseShadowCmpError::io("open sparse comparison input", error))?;
    let dense_len = dense_file
        .metadata()
        .map_err(|error| SparseShadowCmpError::io("stat dense comparison input", error))?
        .len();
    let sparse_len = sparse_file
        .metadata()
        .map_err(|error| SparseShadowCmpError::io("stat sparse comparison input", error))?
        .len();
    if dense_len != sparse_len {
        return Err(SparseShadowCmpError::Mismatch);
    }

    let mut dense_buffer = vec![0; 1024 * 1024];
    let mut sparse_buffer = vec![0; 1024 * 1024];
    let mut compared_through = 0_u64;
    let mut advised_through = 0_u64;
    while compared_through < dense_len {
        if shutdown_token.is_cancelled() {
            return Err(SparseShadowCmpError::Cancelled);
        }
        let remaining = dense_len.saturating_sub(compared_through);
        let read_len = dense_buffer
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let dense_chunk = dense_buffer.get_mut(..read_len).ok_or_else(|| {
            SparseShadowCmpError::io(
                "prepare dense comparison buffer",
                std::io::Error::other("comparison read exceeds dense buffer"),
            )
        })?;
        let sparse_chunk = sparse_buffer.get_mut(..read_len).ok_or_else(|| {
            SparseShadowCmpError::io(
                "prepare sparse comparison buffer",
                std::io::Error::other("comparison read exceeds sparse buffer"),
            )
        })?;
        dense_file
            .read_exact(dense_chunk)
            .map_err(|error| SparseShadowCmpError::io("read dense comparison input", error))?;
        sparse_file
            .read_exact(sparse_chunk)
            .map_err(|error| SparseShadowCmpError::io("read sparse comparison input", error))?;
        if dense_chunk != sparse_chunk {
            return Err(SparseShadowCmpError::Mismatch);
        }
        compared_through = compared_through.saturating_add(read_len as u64);
        if compared_through.saturating_sub(advised_through) >= STREAM_WRITEBACK_WINDOW_BYTES {
            advise_shadow_read_range(
                &dense_file,
                dense,
                advised_through,
                compared_through.saturating_sub(advised_through),
                "dense sparse-shadow comparison input",
            );
            advise_shadow_read_range(
                &sparse_file,
                sparse,
                advised_through,
                compared_through.saturating_sub(advised_through),
                "sparse shadow comparison input",
            );
            advised_through = compared_through;
        }
    }
    if advised_through < compared_through {
        let length = compared_through.saturating_sub(advised_through);
        advise_shadow_read_range(
            &dense_file,
            dense,
            advised_through,
            length,
            "dense sparse-shadow comparison input",
        );
        advise_shadow_read_range(
            &sparse_file,
            sparse,
            advised_through,
            length,
            "sparse shadow comparison input",
        );
    }
    Ok(())
}

fn advise_shadow_read_range(
    file: &std::fs::File,
    path: &Path,
    offset: u64,
    length: u64,
    operation: &'static str,
) {
    if let Err(error) = page_cache::advise_range_dontneed(file, offset, length) {
        tracing::warn!(
            %error,
            path_len = path.as_os_str().len(),
            offset,
            length,
            operation,
            "failed to evict consumed sparse-shadow read range"
        );
    }
}

fn compare_sparse_shadow(
    path: &Path,
    block_bytes: usize,
    shutdown_token: &CancellationToken,
    writeback_coordinator: Arc<StreamWritebackCoordinator>,
) -> Result<ShadowComparisonObservation, SparseShadowCmpError> {
    let shadow = sparse_shadow_path(path);
    let result = (|| {
        if shutdown_token.is_cancelled() {
            return Err(SparseShadowCmpError::Cancelled);
        }
        let mut reader = std::fs::File::open(path)
            .map_err(|error| SparseShadowCmpError::io("open dense shadow source", error))?;
        let output = std::fs::File::create(&shadow)
            .map_err(|error| SparseShadowCmpError::io("create sparse shadow output", error))?;
        let candidate_observer = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let writeback_timing = Arc::new(StreamPullTiming::default());
        let mut writer = DecryptedFileSink::new(
            output,
            MaterializationMode::Enabled,
            block_bytes,
            Arc::clone(&candidate_observer),
            Some(writeback_coordinator),
            Some(writeback_timing),
            JobPriority::default(),
        )
        .map_err(|error| SparseShadowCmpError::io("create sparse shadow writer", error))?;
        let mut buffer = vec![0; 1024 * 1024];
        let mut read_through = 0_u64;
        let mut advised_through = 0_u64;
        loop {
            if shutdown_token.is_cancelled() {
                return Err(SparseShadowCmpError::Cancelled);
            }
            let read = reader
                .read(&mut buffer)
                .map_err(|error| SparseShadowCmpError::io("read dense shadow source", error))?;
            if read == 0 {
                break;
            }
            let chunk = buffer.get(..read).ok_or_else(|| {
                SparseShadowCmpError::io(
                    "read dense shadow source",
                    std::io::Error::other("read exceeded shadow buffer"),
                )
            })?;
            writer
                .write_all(chunk)
                .map_err(|error| SparseShadowCmpError::io("write sparse shadow output", error))?;
            read_through = read_through.saturating_add(read as u64);
            if read_through.saturating_sub(advised_through) >= STREAM_WRITEBACK_WINDOW_BYTES {
                advise_shadow_read_range(
                    &reader,
                    path,
                    advised_through,
                    read_through.saturating_sub(advised_through),
                    "read dense sparse-shadow source",
                );
                advised_through = read_through;
            }
        }
        writer
            .complete()
            .map_err(|error| SparseShadowCmpError::io("finalize sparse shadow output", error))?;
        if advised_through < read_through {
            advise_shadow_read_range(
                &reader,
                path,
                advised_through,
                read_through.saturating_sub(advised_through),
                "read dense sparse-shadow source",
            );
        }
        let candidate_hole_bytes = candidate_observer.load(std::sync::atomic::Ordering::Relaxed);
        let output = std::fs::File::open(&shadow)
            .map_err(|error| SparseShadowCmpError::io("open sparse shadow output", error))?;
        page_cache::advise_dontneed(&reader, path, "read dense sparse-shadow source");
        page_cache::advise_dontneed(&output, &shadow, "write sparse shadow comparison file");
        drop(reader);
        drop(output);

        compare_completed_files(path, &shadow, shutdown_token)?;

        let shadow_metadata = std::fs::metadata(&shadow)
            .map_err(|error| SparseShadowCmpError::io("stat sparse shadow output", error))?;
        let shadow_logical_bytes = shadow_metadata.len();
        #[cfg(unix)]
        let shadow_allocated_bytes = {
            use std::os::unix::fs::MetadataExt;
            shadow_metadata.blocks().saturating_mul(512)
        };
        #[cfg(not(unix))]
        let shadow_allocated_bytes = shadow_logical_bytes;
        Ok(ShadowComparisonObservation {
            logical_bytes: shadow_logical_bytes,
            allocated_bytes: shadow_allocated_bytes,
            candidate_hole_bytes,
        })
    })();

    match std::fs::remove_file(&shadow) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => tracing::warn!(
            path = %shadow.display(),
            %error,
            "failed to remove sparse shadow comparison file"
        ),
    }
    result
}

/// Decompress a zstd-compressed file in-place: decompress to a
/// `{path}.<unique>.dec.tmp` scratch sibling, then atomically replace
/// `path`.
///
/// The scratch name is unique per invocation: a timed-out attempt's
/// `spawn_blocking` closure is uncancellable and finishes detached, so
/// a fixed scratch path would let a newer attempt — or, now that
/// sbx-mgr names its pull destinations deterministically, a newer
/// *job* — truncate the orphan's in-progress output and race it for
/// the terminal rename (the loser failing with a spurious ENOENT).
/// With unique scratch names each writer owns its inode and whichever
/// rename lands last simply overwrites identical bytes. The name keeps
/// the `.dec.tmp` suffix residue tooling matches on.
///
/// `codec_permit` (if the caller took one) is moved into the blocking
/// task and released only when the decode exits, so a cancelled caller
/// cannot free the permit while the detached `spawn_blocking` keeps
/// burning CPU.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DecompressionReport {
    mode: MaterializationMode,
    candidate_hole_bytes: u64,
}

async fn decompress_in_place(
    path: &Path,
    snapshot_artifact: Option<SnapshotArtifact>,
    requested_mode: MaterializationMode,
    codec_permit: Option<CodecPermit>,
) -> Result<DecompressionReport, JobError> {
    let snapshot_artifact = snapshot_artifact.or_else(|| snapshot_artifact_from_local_path(path));
    let mode = effective_materialization_mode(snapshot_artifact, requested_mode);
    let block_bytes = materialization_block_bytes(snapshot_artifact);
    let tmp_path = {
        let mut p = path.to_path_buf();
        let mut name = p.file_name().unwrap_or_default().to_os_string();
        name.push(format!(".{}.dec.tmp", uuid::Uuid::now_v7()));
        p.set_file_name(name);
        p
    };

    let src = path.to_path_buf();
    let tmp = tmp_path.clone();
    let dst = path.to_path_buf();

    tokio::task::spawn_blocking(move || -> Result<DecompressionReport, JobError> {
        let _codec_permit = codec_permit;
        let result = (|| -> Result<DecompressionReport, JobError> {
            let mut reader = std::fs::File::open(&src)
                .map_err(|e| JobError::Permanent(format!("open for decompression: {e}")))?;
            let writer = std::fs::File::create(&tmp)
                .map_err(|e| JobError::Permanent(format!("create decompression temp: {e}")))?;
            let (writer, candidate_hole_bytes) = match mode {
                MaterializationMode::Enabled => {
                    let mut writer =
                        SparseFileWriter::new(writer, block_bytes).map_err(|error| {
                            JobError::Permanent(format!(
                                "create sparse decompression writer: {error}"
                            ))
                        })?;
                    zstd::stream::copy_decode(&mut reader, &mut writer)
                        .map_err(|e| JobError::Permanent(format!("zstd decompress: {e}")))?;
                    writer.finish_with_stats().map_err(|e| {
                        JobError::Permanent(format!("finalize decompression temp: {e}"))
                    })?
                }
                MaterializationMode::Disabled | MaterializationMode::Shadow => {
                    let mut writer = writer;
                    zstd::stream::copy_decode(&mut reader, &mut writer)
                        .map_err(|e| JobError::Permanent(format!("zstd decompress: {e}")))?;
                    writer.flush().map_err(|e| {
                        JobError::Permanent(format!("flush decompression temp: {e}"))
                    })?;
                    (writer, 0)
                }
            };
            page_cache::advise_dontneed(&reader, &src, "decompress volume download input");
            page_cache::advise_dontneed(&writer, &tmp, "write decompressed volume download temp");
            // Drop the reader before rename so Windows (and any strict POSIX
            // implementations) can close the source fd.
            drop(reader);
            drop(writer);
            std::fs::rename(&tmp, &dst)
                .map_err(|e| JobError::Permanent(format!("rename decompressed file: {e}")))?;
            page_cache::advise_path_dontneed(&dst, "decompressed volume download output");
            Ok(DecompressionReport {
                mode,
                candidate_hole_bytes,
            })
        })();
        if result.is_err() {
            // The unique scratch name has no successor to truncate-and-
            // reuse it, and non-restore destinations have no sweeper —
            // a failed decompress must reclaim its own scratch or every
            // failure strands a destination-sized orphan.
            match std::fs::remove_file(&tmp) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(
                    path = %tmp.display(),
                    %error,
                    "failed to remove decompression scratch after failed decompress"
                ),
            }
        }
        result
    })
    .await
    .map_err(|e| JobError::Permanent(format!("decompress task panicked: {e}")))?
}

fn should_ignore_chmod_permission_denied(
    policy: FileModeApplyPolicy,
    current_mode: u32,
    desired_mode: u32,
) -> bool {
    let current_rwx = current_mode & 0o777;
    let desired_rwx = desired_mode & 0o777;
    if current_rwx & desired_rwx == desired_rwx {
        return true;
    }

    // The skip-present path can encounter a complete artifact owned by a
    // sibling service. If it is already readable in every class the requested
    // mode would make readable, let the caller adopt it and run its own final
    // normalization instead of dead-ending on an unfixable chmod.
    let desired_read_bits = desired_rwx & 0o444;
    matches!(policy, FileModeApplyPolicy::ExistingFile)
        && desired_read_bits != 0
        && current_rwx & desired_read_bits == desired_read_bits
}

/// Buffered writer size between the (optional) zstd decoder and the
/// destination file when decrypt + decompress run back-to-back on the
/// pull side. Match the decoder output window to batch file writes and their
/// accounting. Both buffers remain bounded per admitted decoder.
const DECRYPT_PULL_WRITE_BUF_BYTES: usize = 1024 * 1024;

// Submit decoded output to the process-wide writeback worker in the same
// increments used by ranged downloads. Writers share one byte budget, so
// aggregate dirty output stays bounded independently of pull concurrency.
const STREAM_WRITEBACK_WINDOW_BYTES: u64 = 32 * 1024 * 1024;
pub const DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const STREAM_WRITEBACK_IDLE_DRAIN: std::time::Duration = std::time::Duration::from_secs(1);
const STREAM_WRITEBACK_METRIC_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

#[derive(Debug, Default)]
struct StreamPullTiming {
    compressed_input_bytes: std::sync::atomic::AtomicU64,
    decoded_output_bytes: std::sync::atomic::AtomicU64,
    decode_writer_us: std::sync::atomic::AtomicU64,
    decode_writer_cpu_us: std::sync::atomic::AtomicU64,
    output_writer_us: std::sync::atomic::AtomicU64,
    output_writer_cpu_us: std::sync::atomic::AtomicU64,
    writeback_reserve_us: std::sync::atomic::AtomicU64,
    writeback_reserve_cpu_us: std::sync::atomic::AtomicU64,
    writeback_start_us: std::sync::atomic::AtomicU64,
    writeback_start_cpu_us: std::sync::atomic::AtomicU64,
    writeback_wait_us: std::sync::atomic::AtomicU64,
    writeback_start_calls: std::sync::atomic::AtomicU64,
    writeback_submitted_logical_bytes: std::sync::atomic::AtomicU64,
    writeback_submitted_dirty_bytes: std::sync::atomic::AtomicU64,
    writeback_hole_bytes_released: std::sync::atomic::AtomicU64,
    writeback_low_watermark_activations: std::sync::atomic::AtomicU64,
}

fn current_thread_cpu_us() -> u64 {
    let Ok(value) = nix::time::clock_gettime(nix::time::ClockId::CLOCK_THREAD_CPUTIME_ID) else {
        return 0;
    };
    let seconds = u64::try_from(value.tv_sec()).unwrap_or(0);
    let nanoseconds = u64::try_from(value.tv_nsec()).unwrap_or(0);
    seconds
        .saturating_mul(1_000_000)
        .saturating_add(nanoseconds / 1_000)
}

impl StreamPullTiming {
    fn add(counter: &std::sync::atomic::AtomicU64, value: u64) {
        match counter.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |current| Some(current.saturating_add(value)),
        ) {
            Ok(_) | Err(_) => {}
        }
    }

    fn add_elapsed(counter: &std::sync::atomic::AtomicU64, started_at: Instant) {
        Self::add(
            counter,
            u64::try_from(started_at.elapsed().as_micros()).unwrap_or(u64::MAX),
        );
    }

    fn add_wall_and_thread_cpu(
        wall_counter: &std::sync::atomic::AtomicU64,
        cpu_counter: &std::sync::atomic::AtomicU64,
        wall_started_at: Instant,
        cpu_started_at_us: u64,
    ) {
        let cpu_elapsed_us = current_thread_cpu_us().saturating_sub(cpu_started_at_us);
        let wall_elapsed_us =
            u64::try_from(wall_started_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        Self::add(wall_counter, wall_elapsed_us);
        Self::add(cpu_counter, cpu_elapsed_us);
    }

    fn load(counter: &std::sync::atomic::AtomicU64) -> u64 {
        counter.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn log_streamed_pull_pipeline_timing(
    job: &Job,
    payload: &TransferPayload,
    timing: &StreamPullTiming,
    streamed_decoded: bool,
    download_wall_ms: u64,
) {
    let decode_writer_us = StreamPullTiming::load(&timing.decode_writer_us);
    let decode_writer_cpu_us = StreamPullTiming::load(&timing.decode_writer_cpu_us);
    let output_writer_us = StreamPullTiming::load(&timing.output_writer_us);
    let output_writer_cpu_us = StreamPullTiming::load(&timing.output_writer_cpu_us);
    let writeback_reserve_us = StreamPullTiming::load(&timing.writeback_reserve_us);
    let writeback_reserve_cpu_us = StreamPullTiming::load(&timing.writeback_reserve_cpu_us);
    tracing::info!(
        job_id = %job.job_id,
        subject_id = %payload.subject_id,
        s3_uri_len = payload.s3_uri.len(),
        local_path_len = payload.local_path.len(),
        streamed_decoded,
        download_decode_wall_ms = download_wall_ms,
        compressed_input_bytes = StreamPullTiming::load(&timing.compressed_input_bytes),
        decoded_output_bytes = StreamPullTiming::load(&timing.decoded_output_bytes),
        decode_writer_ms = decode_writer_us / 1_000,
        decode_stream_processing_ms = decode_writer_us
            .saturating_sub(output_writer_us)
            .saturating_sub(writeback_reserve_us)
            / 1_000,
        decode_stream_processing_cpu_ms = decode_writer_cpu_us
            .saturating_sub(output_writer_cpu_us)
            .saturating_sub(writeback_reserve_cpu_us)
            / 1_000,
        output_writer_ms = output_writer_us / 1_000,
        output_writer_cpu_ms = output_writer_cpu_us / 1_000,
        writeback_reserve_ms = writeback_reserve_us / 1_000,
        writeback_reserve_cpu_ms = writeback_reserve_cpu_us / 1_000,
        writeback_start_ms = StreamPullTiming::load(&timing.writeback_start_us) / 1_000,
        writeback_start_cpu_ms = StreamPullTiming::load(&timing.writeback_start_cpu_us) / 1_000,
        writeback_wait_observed_ms = StreamPullTiming::load(&timing.writeback_wait_us) / 1_000,
        writeback_start_calls = StreamPullTiming::load(&timing.writeback_start_calls),
        writeback_submitted_logical_bytes =
            StreamPullTiming::load(&timing.writeback_submitted_logical_bytes),
        writeback_submitted_dirty_bytes =
            StreamPullTiming::load(&timing.writeback_submitted_dirty_bytes),
        writeback_hole_bytes_released =
            StreamPullTiming::load(&timing.writeback_hole_bytes_released),
        writeback_low_watermark_activations =
            StreamPullTiming::load(&timing.writeback_low_watermark_activations),
        "streamed pull pipeline timing"
    );
}

#[derive(Debug)]
struct StreamWritebackRange {
    file: Arc<std::fs::File>,
    offset: u64,
    length: u64,
    debt_bytes: u64,
    started: bool,
    timing: Arc<StreamPullTiming>,
}

#[derive(Debug)]
enum StreamWritebackCommand {
    Range(StreamWritebackRange),
    ReapToLowWatermark,
}

trait StreamPageCacheOperations: std::fmt::Debug + Send + Sync {
    fn start(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()>;
    fn wait(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()>;
    fn advise_dontneed(
        &self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()>;
}

#[derive(Debug)]
struct SystemStreamPageCacheOperations;

impl StreamPageCacheOperations for SystemStreamPageCacheOperations {
    fn start(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        page_cache::start_writeback_range(file, offset, length)
    }

    fn wait(&self, file: &std::fs::File, offset: u64, length: u64) -> std::io::Result<()> {
        page_cache::wait_writeback_range(file, offset, length)
    }

    fn advise_dontneed(
        &self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        page_cache::advise_range_dontneed(file, offset, length)
    }
}

#[derive(Debug, Default)]
struct StreamWritebackBudget {
    outstanding_bytes: std::sync::Mutex<u64>,
    available: std::sync::Condvar,
    failed: std::sync::atomic::AtomicBool,
}

#[derive(Debug)]
struct StreamWritebackCoordinator {
    sender: std::sync::mpsc::Sender<StreamWritebackCommand>,
    budget: Arc<StreamWritebackBudget>,
    high_watermark_bytes: u64,
    priority_threshold: Option<JobPriority>,
    priority_reserve_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamWritebackFailureOperation {
    Start,
    Wait,
    Advice,
    Queue,
}

impl StreamWritebackFailureOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Wait => "wait",
            Self::Advice => "advice",
            Self::Queue => "queue",
        }
    }
}

impl std::fmt::Display for StreamWritebackFailureOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreamWritebackPriorityClass {
    Shared,
    High,
}

impl StreamWritebackPriorityClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::High => "high",
        }
    }
}

impl std::fmt::Display for StreamWritebackPriorityClass {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

fn fail_stream_writeback_budget(
    budget: &StreamWritebackBudget,
    operation: StreamWritebackFailureOperation,
) {
    let was_failed = budget
        .failed
        .swap(true, std::sync::atomic::Ordering::AcqRel);
    budget.available.notify_all();
    if !was_failed {
        pplx_space_metrics::volume_manager::stream_writeback_failure(operation.as_str());
    }
}

fn release_stream_writeback_budget(budget: &StreamWritebackBudget, bytes: u64) {
    let mut outstanding = budget
        .outstanding_bytes
        .lock()
        .expect("stream writeback budget lock poisoned");
    *outstanding = outstanding.saturating_sub(bytes);
    budget.available.notify_all();
}

fn emit_stream_writeback_outstanding_if_due(
    budget: &StreamWritebackBudget,
    last_emitted_at: &mut Instant,
    force: bool,
) {
    let outstanding = stream_writeback_outstanding_bytes(budget);
    if !should_emit_stream_writeback_outstanding(last_emitted_at.elapsed(), force) {
        return;
    }
    pplx_space_metrics::volume_manager::stream_writeback_outstanding_bytes(outstanding);
    *last_emitted_at = Instant::now();
}

fn should_emit_stream_writeback_outstanding(elapsed: std::time::Duration, force: bool) -> bool {
    force || elapsed >= STREAM_WRITEBACK_METRIC_INTERVAL
}

fn retire_stream_writeback_range(
    range: StreamWritebackRange,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) {
    if *writeback_enabled && range.started {
        let wait_started_at = Instant::now();
        let result = page_cache_operations.wait(&range.file, range.offset, range.length);
        StreamPullTiming::add_elapsed(&range.timing.writeback_wait_us, wait_started_at);
        if let Err(error) = result {
            tracing::warn!(
                %error,
                offset = range.offset,
                length = range.length,
                "waiting for streamed output range writeback failed; closing streamed pull admission"
            );
            *writeback_enabled = false;
            fail_stream_writeback_budget(budget, StreamWritebackFailureOperation::Wait);
        }
    }
    if *writeback_enabled
        && range.started
        && *cache_advice_enabled
        && let Err(error) =
            page_cache_operations.advise_dontneed(&range.file, range.offset, range.length)
    {
        tracing::warn!(
            %error,
            offset = range.offset,
            length = range.length,
            "streamed output range cache advice failed; disabling cache advice"
        );
        pplx_space_metrics::volume_manager::stream_writeback_failure(
            StreamWritebackFailureOperation::Advice.as_str(),
        );
        *cache_advice_enabled = false;
    }
    release_stream_writeback_budget(budget, range.debt_bytes);
}

fn stream_writeback_outstanding_bytes(budget: &StreamWritebackBudget) -> u64 {
    *budget
        .outstanding_bytes
        .lock()
        .expect("stream writeback budget lock poisoned")
}

fn pending_stream_writeback_debt(
    pending: &std::collections::VecDeque<StreamWritebackRange>,
) -> u64 {
    pending
        .iter()
        .fold(0_u64, |total, range| total.saturating_add(range.debt_bytes))
}

fn drain_stream_writeback_ranges_to(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    target_bytes: u64,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) -> bool {
    while stream_writeback_outstanding_bytes(budget) > target_bytes {
        let Some(range) = pending.pop_front() else {
            return false;
        };
        retire_stream_writeback_range(
            range,
            budget,
            page_cache_operations,
            writeback_enabled,
            cache_advice_enabled,
        );
    }
    true
}

fn drain_all_stream_writeback_ranges(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    cache_advice_enabled: &mut bool,
) {
    while let Some(range) = pending.pop_front() {
        retire_stream_writeback_range(
            range,
            budget,
            page_cache_operations,
            writeback_enabled,
            cache_advice_enabled,
        );
    }
}

fn queue_stream_writeback_range(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    range: StreamWritebackRange,
) {
    if let Some(previous) = pending.iter_mut().rev().find(|previous| {
        Arc::ptr_eq(&previous.file, &range.file) && Arc::ptr_eq(&previous.timing, &range.timing)
    }) {
        let can_merge = !previous.started
            && previous
                .offset
                .checked_add(previous.length)
                .is_some_and(|end| end <= range.offset)
            && previous
                .debt_bytes
                .checked_add(range.debt_bytes)
                .is_some_and(|debt| debt <= STREAM_WRITEBACK_WINDOW_BYTES);
        if can_merge
            && let (Some(length), Some(debt_bytes)) = (
                range
                    .offset
                    .checked_add(range.length)
                    .and_then(|end| end.checked_sub(previous.offset)),
                previous.debt_bytes.checked_add(range.debt_bytes),
            )
        {
            previous.length = length;
            previous.debt_bytes = debt_bytes;
            return;
        }
    }
    pending.push_back(range);
}

fn start_pending_stream_writeback_ranges(
    pending: &mut std::collections::VecDeque<StreamWritebackRange>,
    budget: &StreamWritebackBudget,
    page_cache_operations: &dyn StreamPageCacheOperations,
    writeback_enabled: &mut bool,
    minimum_debt_bytes: u64,
) {
    for range in pending
        .iter_mut()
        .filter(|range| !range.started && range.debt_bytes >= minimum_debt_bytes)
    {
        if !*writeback_enabled {
            break;
        }
        let start_started_at = Instant::now();
        let start_cpu_started_at = current_thread_cpu_us();
        let result = page_cache_operations.start(&range.file, range.offset, range.length);
        StreamPullTiming::add_wall_and_thread_cpu(
            &range.timing.writeback_start_us,
            &range.timing.writeback_start_cpu_us,
            start_started_at,
            start_cpu_started_at,
        );
        match result {
            Ok(()) => {
                range.started = true;
                StreamPullTiming::add(&range.timing.writeback_start_calls, 1);
                StreamPullTiming::add(
                    &range.timing.writeback_submitted_logical_bytes,
                    range.length,
                );
                StreamPullTiming::add(
                    &range.timing.writeback_submitted_dirty_bytes,
                    range.debt_bytes,
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    offset = range.offset,
                    length = range.length,
                    debt_bytes = range.debt_bytes,
                    "starting streamed output range writeback failed; closing streamed pull admission"
                );
                *writeback_enabled = false;
                fail_stream_writeback_budget(budget, StreamWritebackFailureOperation::Start);
            }
        }
    }
}

impl StreamWritebackCoordinator {
    fn new(high_watermark_bytes: u64, priority_threshold: Option<JobPriority>) -> Arc<Self> {
        Self::new_with_page_cache_operations(
            high_watermark_bytes,
            priority_threshold,
            Arc::new(SystemStreamPageCacheOperations),
        )
    }

    fn new_with_page_cache_operations(
        high_watermark_bytes: u64,
        priority_threshold: Option<JobPriority>,
        page_cache_operations: Arc<dyn StreamPageCacheOperations>,
    ) -> Arc<Self> {
        let (sender, receiver) = std::sync::mpsc::channel::<StreamWritebackCommand>();
        let budget = Arc::new(StreamWritebackBudget::default());
        let worker_budget = Arc::clone(&budget);
        let low_watermark_bytes = high_watermark_bytes / 2;
        let writeback_window_bytes = STREAM_WRITEBACK_WINDOW_BYTES.min(low_watermark_bytes);
        std::thread::Builder::new()
            .name("volmgr-writeback".to_string())
            .spawn(move || {
                let mut writeback_enabled = true;
                let mut cache_advice_enabled = true;
                let mut pending = std::collections::VecDeque::new();
                let mut drain_target = None;
                let mut pressure_active = false;
                let mut last_outstanding_metric_at = Instant::now();

                loop {
                    let mut force_outstanding_metric = false;
                    let writeback_enabled_at_start = writeback_enabled;
                    match receiver.recv_timeout(STREAM_WRITEBACK_IDLE_DRAIN) {
                        Ok(StreamWritebackCommand::Range(range)) if writeback_enabled => {
                            let activation_timing = Arc::clone(&range.timing);
                            queue_stream_writeback_range(&mut pending, range);
                            // Overlap complete dirty windows with decoding even
                            // below the pressure watermark. Keep their debt
                            // charged until the existing completion/drain path.
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                writeback_window_bytes,
                            );
                            if pending_stream_writeback_debt(&pending) >= low_watermark_bytes {
                                let newly_activated = !pressure_active;
                                if !pressure_active {
                                    pressure_active = true;
                                    force_outstanding_metric = true;
                                    StreamPullTiming::add(
                                        &activation_timing.writeback_low_watermark_activations,
                                        1,
                                    );
                                }
                                start_pending_stream_writeback_ranges(
                                    &mut pending,
                                    &worker_budget,
                                    page_cache_operations.as_ref(),
                                    &mut writeback_enabled,
                                    if newly_activated {
                                        0
                                    } else {
                                        writeback_window_bytes
                                    },
                                );
                            }
                        }
                        Ok(StreamWritebackCommand::Range(range)) => {
                            release_stream_writeback_budget(&worker_budget, range.debt_bytes);
                        }
                        Ok(StreamWritebackCommand::ReapToLowWatermark) => {
                            force_outstanding_metric = true;
                            drain_target = Some(low_watermark_bytes);
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                0,
                            );
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            if pending_stream_writeback_debt(&pending) >= low_watermark_bytes {
                                start_pending_stream_writeback_ranges(
                                    &mut pending,
                                    &worker_budget,
                                    page_cache_operations.as_ref(),
                                    &mut writeback_enabled,
                                    0,
                                );
                                drain_target = Some(low_watermark_bytes);
                            }
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            start_pending_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                0,
                            );
                            drain_all_stream_writeback_ranges(
                                &mut pending,
                                &worker_budget,
                                page_cache_operations.as_ref(),
                                &mut writeback_enabled,
                                &mut cache_advice_enabled,
                            );
                            emit_stream_writeback_outstanding_if_due(
                                &worker_budget,
                                &mut last_outstanding_metric_at,
                                true,
                            );
                            break;
                        }
                    }

                    if drain_target.is_some() {
                        start_pending_stream_writeback_ranges(
                            &mut pending,
                            &worker_budget,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            0,
                        );
                    }
                    if drain_target.is_some_and(|target| {
                        drain_stream_writeback_ranges_to(
                            &mut pending,
                            &worker_budget,
                            target,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            &mut cache_advice_enabled,
                        )
                    }) {
                        drain_target = None;
                        pressure_active = stream_writeback_outstanding_bytes(&worker_budget)
                            > low_watermark_bytes;
                        force_outstanding_metric = true;
                    }
                    if !writeback_enabled {
                        drain_all_stream_writeback_ranges(
                            &mut pending,
                            &worker_budget,
                            page_cache_operations.as_ref(),
                            &mut writeback_enabled,
                            &mut cache_advice_enabled,
                        );
                        drain_target = None;
                        pressure_active = false;
                        force_outstanding_metric |= writeback_enabled_at_start;
                    }
                    emit_stream_writeback_outstanding_if_due(
                        &worker_budget,
                        &mut last_outstanding_metric_at,
                        force_outstanding_metric,
                    );
                }
            })
            .expect("spawn volume-manager writeback worker");
        Arc::new(Self {
            sender,
            budget,
            high_watermark_bytes,
            priority_threshold,
            priority_reserve_bytes: if priority_threshold.is_some() {
                STREAM_WRITEBACK_WINDOW_BYTES.min(high_watermark_bytes / 2)
            } else {
                0
            },
        })
    }

    fn admission_limit(&self, priority: JobPriority) -> u64 {
        if self
            .priority_threshold
            .is_some_and(|threshold| priority < threshold)
        {
            self.high_watermark_bytes
                .saturating_sub(self.priority_reserve_bytes)
        } else {
            self.high_watermark_bytes
        }
    }

    fn try_reserve(&self, bytes: u64, priority: JobPriority) -> std::io::Result<bool> {
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .map_err(|_| std::io::Error::other("stream writeback budget lock poisoned"))?;
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let admission_limit = self.admission_limit(priority);
        if *outstanding != 0 && outstanding.saturating_add(bytes) > admission_limit {
            return Ok(false);
        }
        *outstanding = outstanding.saturating_add(bytes);
        Ok(true)
    }

    fn reserve(&self, bytes: u64, priority: JobPriority) -> std::io::Result<()> {
        let reserve_started_at = Instant::now();
        if self.try_reserve(bytes, priority)? {
            return Ok(());
        }
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .map_err(|_| std::io::Error::other("stream writeback budget lock poisoned"))?;
        if self
            .budget
            .failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(std::io::Error::other("stream writeback coordinator failed"));
        }
        let admission_limit = self.admission_limit(priority);
        let priority_class = if admission_limit == self.high_watermark_bytes {
            StreamWritebackPriorityClass::High
        } else {
            StreamWritebackPriorityClass::Shared
        };
        while *outstanding != 0 && outstanding.saturating_add(bytes) > admission_limit {
            if self
                .sender
                .send(StreamWritebackCommand::ReapToLowWatermark)
                .is_err()
            {
                tracing::warn!(
                    "stream writeback worker stopped while a producer was blocked; closing streamed pull admission"
                );
                fail_stream_writeback_budget(&self.budget, StreamWritebackFailureOperation::Queue);
                return Err(std::io::Error::other("stream writeback worker stopped"));
            }
            outstanding = self.budget.available.wait(outstanding).map_err(|_| {
                std::io::Error::other("stream writeback budget lock poisoned while waiting")
            })?;
            if self
                .budget
                .failed
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(std::io::Error::other("stream writeback coordinator failed"));
            }
        }
        *outstanding = outstanding.saturating_add(bytes);
        pplx_space_metrics::volume_manager::stream_writeback_producer_blocked_ms(
            reserve_started_at.elapsed().as_secs_f64() * 1000.0,
            priority_class.as_str(),
        );
        Ok(())
    }

    fn release(&self, bytes: u64) {
        let mut outstanding = self
            .budget
            .outstanding_bytes
            .lock()
            .expect("stream writeback budget lock poisoned");
        *outstanding = outstanding.saturating_sub(bytes);
        self.budget.available.notify_all();
    }

    fn submit(
        &self,
        file: Arc<std::fs::File>,
        offset: u64,
        length: u64,
        debt_bytes: u64,
        timing: Arc<StreamPullTiming>,
    ) {
        let result = self
            .sender
            .send(StreamWritebackCommand::Range(StreamWritebackRange {
                file,
                offset,
                length,
                debt_bytes,
                started: false,
                timing,
            }))
            .map_err(|_| std::io::Error::other("stream writeback worker stopped"));
        if let Err(error) = result {
            tracing::warn!(
                %error,
                offset,
                length,
                debt_bytes,
                "streamed output range could not be queued for writeback; closing streamed pull admission"
            );
            fail_stream_writeback_budget(&self.budget, StreamWritebackFailureOperation::Queue);
            self.release(debt_bytes);
        }
    }
}

#[derive(Debug)]
struct StreamWritebackAccumulator {
    offset: u64,
    length: u64,
    debt_bytes: u64,
}

#[derive(Debug)]
struct StreamWriteback {
    coordinator: Option<Arc<StreamWritebackCoordinator>>,
    timing: Option<Arc<StreamPullTiming>>,
    priority: JobPriority,
    writeback_file: Option<Arc<std::fs::File>>,
    unclassified_bytes: u64,
    reserved_bytes: u64,
    deferred_hole_bytes: u64,
    pending: Option<StreamWritebackAccumulator>,
}

impl StreamWriteback {
    fn new(
        coordinator: Option<Arc<StreamWritebackCoordinator>>,
        timing: Option<Arc<StreamPullTiming>>,
        priority: JobPriority,
    ) -> Self {
        Self {
            coordinator,
            timing,
            priority,
            writeback_file: None,
            unclassified_bytes: 0,
            reserved_bytes: 0,
            deferred_hole_bytes: 0,
            pending: None,
        }
    }

    fn pending_debt_bytes(&self) -> u64 {
        self.pending
            .as_ref()
            .map_or(0, |pending| pending.debt_bytes)
    }

    fn additional_reservation_bytes(&self, bytes: u64) -> u64 {
        let Some(coordinator) = &self.coordinator else {
            return 0;
        };
        let owned_bytes = self
            .unclassified_bytes
            .saturating_add(self.pending_debt_bytes())
            .saturating_add(self.deferred_hole_bytes);
        let required_bytes = owned_bytes.saturating_add(bytes);
        if required_bytes <= self.reserved_bytes {
            return 0;
        }
        let reservation_window = STREAM_WRITEBACK_WINDOW_BYTES
            .min(coordinator.admission_limit(self.priority) / 2)
            .max(bytes);
        owned_bytes
            .saturating_add(reservation_window)
            .saturating_sub(self.reserved_bytes)
    }

    fn reserve(&mut self, bytes: usize) -> std::io::Result<()> {
        let bytes = bytes as u64;
        if self.coordinator.is_some() {
            let mut additional = self.additional_reservation_bytes(bytes);
            if additional != 0 {
                self.release_deferred_holes();
                additional = self.additional_reservation_bytes(bytes);
                let coordinator = self
                    .coordinator
                    .as_ref()
                    .expect("coordinator checked above");
                let reserve_started_at = Instant::now();
                let reserve_cpu_started_at = current_thread_cpu_us();
                coordinator.reserve(additional, self.priority)?;
                if let Some(timing) = &self.timing {
                    StreamPullTiming::add_wall_and_thread_cpu(
                        &timing.writeback_reserve_us,
                        &timing.writeback_reserve_cpu_us,
                        reserve_started_at,
                        reserve_cpu_started_at,
                    );
                }
                self.reserved_bytes = self.reserved_bytes.saturating_add(additional);
            }
        }
        Ok(())
    }

    fn try_reserve(&mut self, bytes: usize) -> std::io::Result<bool> {
        let bytes = bytes as u64;
        if self.coordinator.is_none() {
            return Ok(true);
        }
        let mut additional = self.additional_reservation_bytes(bytes);
        if additional == 0 {
            return Ok(true);
        }
        self.release_deferred_holes();
        additional = self.additional_reservation_bytes(bytes);
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("coordinator checked above");
        let reserve_started_at = Instant::now();
        let reserve_cpu_started_at = current_thread_cpu_us();
        if !coordinator.try_reserve(additional, self.priority)? {
            return Ok(false);
        }
        if let Some(timing) = &self.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.writeback_reserve_us,
                &timing.writeback_reserve_cpu_us,
                reserve_started_at,
                reserve_cpu_started_at,
            );
        }
        self.reserved_bytes = self.reserved_bytes.saturating_add(additional);
        Ok(true)
    }

    fn record_written(&mut self, bytes: usize) {
        if self.coordinator.is_some() {
            self.unclassified_bytes = self.unclassified_bytes.saturating_add(bytes as u64);
        }
    }

    fn release_unwritten(&mut self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        if let Some(coordinator) = &self.coordinator {
            self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
            coordinator.release(bytes);
        }
    }

    fn writeback_file(&mut self, file: &std::fs::File) -> std::io::Result<Arc<std::fs::File>> {
        if let Some(file) = &self.writeback_file {
            return Ok(Arc::clone(file));
        }
        let file = Arc::new(file.try_clone()?);
        self.writeback_file = Some(Arc::clone(&file));
        Ok(file)
    }

    fn release_deferred_holes(&mut self) {
        let hole_bytes = std::mem::take(&mut self.deferred_hole_bytes);
        if hole_bytes == 0 {
            return;
        }
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(hole_bytes)
            .expect("deferred holes exceed producer reservation");
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("deferred holes require a coordinator");
        coordinator.release(hole_bytes);
        if let Some(timing) = &self.timing {
            StreamPullTiming::add(&timing.writeback_hole_bytes_released, hole_bytes);
        }
    }

    fn flush_pending(&mut self, file: &std::fs::File) -> std::io::Result<()> {
        self.release_deferred_holes();
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let writeback_file = self.writeback_file(file)?;
        let coordinator = self
            .coordinator
            .as_ref()
            .expect("pending writeback requires a coordinator");
        let timing = self
            .timing
            .clone()
            .unwrap_or_else(|| Arc::new(StreamPullTiming::default()));
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(pending.debt_bytes)
            .ok_or_else(|| {
                std::io::Error::other("pending writeback exceeds producer reservation")
            })?;
        coordinator.submit(
            writeback_file,
            pending.offset,
            pending.length,
            pending.debt_bytes,
            timing,
        );
        Ok(())
    }

    fn accumulate_dirty_range(
        &mut self,
        file: &std::fs::File,
        mut offset: u64,
        mut length: u64,
    ) -> std::io::Result<()> {
        while length != 0 {
            if self.pending_debt_bytes() == STREAM_WRITEBACK_WINDOW_BYTES {
                self.flush_pending(file)?;
            }
            let available = STREAM_WRITEBACK_WINDOW_BYTES - self.pending_debt_bytes();
            let debt_bytes = length.min(available);
            let end = offset
                .checked_add(debt_bytes)
                .ok_or_else(|| std::io::Error::other("streamed output range exceeds u64::MAX"))?;
            if let Some(pending) = &mut self.pending {
                let pending_end = pending.offset.checked_add(pending.length).ok_or_else(|| {
                    std::io::Error::other("pending streamed output range exceeds u64::MAX")
                })?;
                if offset < pending_end {
                    return Err(std::io::Error::other(
                        "streamed dirty output ranges are not ordered",
                    ));
                }
                pending.length = end.checked_sub(pending.offset).ok_or_else(|| {
                    std::io::Error::other("streamed output range precedes pending range")
                })?;
                pending.debt_bytes =
                    pending.debt_bytes.checked_add(debt_bytes).ok_or_else(|| {
                        std::io::Error::other("pending streamed dirty debt exceeds u64::MAX")
                    })?;
            } else {
                self.pending = Some(StreamWritebackAccumulator {
                    offset,
                    length: debt_bytes,
                    debt_bytes,
                });
            }
            offset = end;
            length -= debt_bytes;
        }
        if self.pending_debt_bytes() == STREAM_WRITEBACK_WINDOW_BYTES {
            self.flush_pending(file)?;
        }
        Ok(())
    }

    fn classify(
        &mut self,
        file: &std::fs::File,
        materialization: SparseMaterialization,
    ) -> std::io::Result<()> {
        let (data_ranges, hole_bytes) = materialization.into_parts();
        let dirty_bytes = data_ranges
            .iter()
            .try_fold(0_u64, |total, range| total.checked_add(range.length()))
            .ok_or_else(|| std::io::Error::other("streamed dirty output exceeds u64::MAX"))?;
        let classified_bytes = dirty_bytes
            .checked_add(hole_bytes)
            .ok_or_else(|| std::io::Error::other("streamed classified output exceeds u64::MAX"))?;
        if classified_bytes == 0 || self.coordinator.is_none() {
            return Ok(());
        }
        if classified_bytes > self.unclassified_bytes || classified_bytes > self.reserved_bytes {
            return Err(std::io::Error::other(
                "sparse writer classified more bytes than writeback admission reserved",
            ));
        }
        self.unclassified_bytes -= classified_bytes;
        if hole_bytes != 0 {
            self.deferred_hole_bytes = self
                .deferred_hole_bytes
                .checked_add(hole_bytes)
                .ok_or_else(|| std::io::Error::other("deferred sparse holes exceed u64::MAX"))?;
            if self.deferred_hole_bytes >= STREAM_WRITEBACK_WINDOW_BYTES {
                self.release_deferred_holes();
            }
        }
        for range in data_ranges {
            self.accumulate_dirty_range(file, range.offset(), range.length())?;
        }
        Ok(())
    }

    fn classify_dense(
        &mut self,
        file: &std::fs::File,
        offset: u64,
        length: u64,
    ) -> std::io::Result<()> {
        self.classify(
            file,
            SparseMaterialization::from(SparseDataRange::new(offset, length)),
        )
    }
}

impl Drop for StreamWriteback {
    fn drop(&mut self) {
        self.release_deferred_holes();
        if self.reserved_bytes != 0
            && let Some(coordinator) = &self.coordinator
        {
            coordinator.release(self.reserved_bytes);
        }
    }
}

/// A stream destination whose buffered state must be finalized after its
/// upstream decoder reaches end-of-stream.
trait CompletingWrite: Write {
    fn complete(self) -> std::io::Result<()>;
}

impl<W: Write + ?Sized> CompletingWrite for &mut W {
    fn complete(self) -> std::io::Result<()> {
        self.flush()
    }
}

/// Pull-side writer adapter that peeks the first four bytes written
/// into it, compares against the zstd magic, and either routes the
/// stream through a `zstd::stream::write::Decoder` (if the magic
/// matches AND `decompress_if_zstd` is true) or passes the bytes
/// through to the inner sink unchanged.
///
/// Lets `decrypt_stream_blocking` feed straight into this writer so a
/// pull of an encrypted-and-compressed object decrypts + decompresses
/// in one streaming pass: no `.plaintext.tmp` intermediate file, no
/// re-read of the decrypted bytes for the magic sniff. The decision
/// is final after the first four bytes — empty or sub-4-byte writes
/// keep the bytes pending until enough has arrived to decide, and
/// `finish` flushes any still-pending bytes through the passthrough
/// path so a sub-4-byte plaintext (never zstd-magic) still round-trips
/// correctly.
struct MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    state: MaybeZstdState<Input, Output>,
    decompress_if_zstd: bool,
    pending: Vec<u8>,
    select_sink: Option<Select>,
}

enum MaybeZstdState<Input: Write, Output: Write> {
    /// First four bytes not yet seen; sink is parked here until we can
    /// decide whether to wrap it with a zstd decoder.
    Init(Input),
    /// First four bytes were the zstd magic and the caller wanted
    /// transparent decompress; subsequent writes flow through the
    /// decoder. Built on the raw zio writer (not
    /// `stream::write::Decoder`) because only the zio writer exposes the
    /// fallible end-of-stream `finish` that rejects a truncated frame —
    /// the higher-level decoder can only flush, which silently accepts
    /// incomplete input.
    Decoding(
        zstd::stream::zio::Writer<std::io::BufWriter<Output>, zstd::stream::raw::Decoder<'static>>,
    ),
    /// First four bytes either weren't the zstd magic, or the caller
    /// asked for raw (`raw = true`); subsequent writes go straight to
    /// the buffered sink.
    Passthrough(std::io::BufWriter<Output>),
    /// Transition slot: `state` holds this value briefly while we
    /// move the inner writer between variants. Should never be the
    /// observed state at the start or end of a `Write` call.
    Transient,
}

#[cfg(test)]
fn identity_decoding_sink<W: Write>(sink: W, _decompressing: bool) -> std::io::Result<W> {
    Ok(sink)
}

#[cfg(test)]
impl<W: Write> MaybeZstdDecodingWriter<W, W, fn(W, bool) -> std::io::Result<W>> {
    fn new(sink: W, decompress_if_zstd: bool) -> Self {
        Self::with_sink_selector(sink, decompress_if_zstd, identity_decoding_sink::<W>)
    }
}

impl<Input, Output, Select> MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    fn with_sink_selector(sink: Input, decompress_if_zstd: bool, select_sink: Select) -> Self {
        Self {
            state: MaybeZstdState::Init(sink),
            decompress_if_zstd,
            pending: Vec::with_capacity(ZSTD_MAGIC.len()),
            select_sink: Some(select_sink),
        }
    }

    fn select_sink(&mut self, sink: Input, decompressing: bool) -> std::io::Result<Output> {
        self.select_sink
            .take()
            .ok_or_else(|| std::io::Error::other("zstd sink selector already consumed"))?(
            sink,
            decompressing,
        )
    }

    /// Consume the writer. Returns `true` iff the stream was routed
    /// through a zstd decoder (i.e. transparent decompression actually
    /// happened). Flushes the trailing buffer and finalises the
    /// decoder and destination when applicable.
    fn finish(mut self) -> std::io::Result<bool>
    where
        Output: CompletingWrite,
    {
        // If we never saw 4 bytes of plaintext (sub-magic-length input,
        // or fully empty), flush the pending bytes through the
        // passthrough path so the file at least contains whatever we
        // did receive.
        let pending = std::mem::take(&mut self.pending);
        match std::mem::replace(&mut self.state, MaybeZstdState::Transient) {
            MaybeZstdState::Init(sink) => {
                let mut sink = self.select_sink(sink, false)?;
                sink.write_all(&pending)?;
                sink.complete()?;
                Ok(false)
            }
            MaybeZstdState::Decoding(mut decoder) => {
                // Fallible end-of-stream finalization FIRST: the raw
                // decoder rejects a truncated or unterminated zstd frame
                // with UnexpectedEof ("incomplete frame"). Flushing alone
                // would silently accept partial input and commit corrupt
                // decoded output. Then recover the BufWriter and
                // explicitly flush it through to the destination file —
                // without the explicit BufWriter::into_inner the trailing
                // bytes would only land on disk via Drop, and a Drop
                // error would be silently swallowed instead of surfaced
                // as a JobError::Retriable.
                decoder.finish()?;
                let (buf_writer, _decoder) = decoder.into_inner();
                let sink = buf_writer.into_inner().map_err(|e| {
                    std::io::Error::other(format!("flush BufWriter at decoder finish: {e}"))
                })?;
                sink.complete()?;
                Ok(true)
            }
            MaybeZstdState::Passthrough(buf_writer) => {
                let sink = buf_writer.into_inner().map_err(|e| {
                    std::io::Error::other(format!("flush BufWriter at stream finish: {e}"))
                })?;
                sink.complete()?;
                Ok(false)
            }
            MaybeZstdState::Transient => Err(std::io::Error::other(
                "MaybeZstdDecodingWriter::finish observed transient state",
            )),
        }
    }
}

impl<Input, Output, Select> Write for MaybeZstdDecodingWriter<Input, Output, Select>
where
    Input: Write,
    Output: Write,
    Select: FnOnce(Input, bool) -> std::io::Result<Output>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // Decide once: accumulate up to 4 bytes in `pending`, then
        // transition state and dispatch the remaining bytes of this
        // write call into the chosen sink.
        if matches!(self.state, MaybeZstdState::Init(_)) {
            let needed = ZSTD_MAGIC.len() - self.pending.len();
            let take = needed.min(buf.len());
            let (prefix_chunk, suffix) = buf.split_at(take);
            self.pending.extend_from_slice(prefix_chunk);
            if self.pending.len() < ZSTD_MAGIC.len() {
                return Ok(take);
            }
            let is_zstd = self.pending == ZSTD_MAGIC;
            let want_decompress = self.decompress_if_zstd && is_zstd;
            let prefix = std::mem::take(&mut self.pending);
            let sink = match std::mem::replace(&mut self.state, MaybeZstdState::Transient) {
                MaybeZstdState::Init(sink) => sink,
                _ => unreachable!("matches!(.., Init) above"),
            };
            let sink = self.select_sink(sink, want_decompress)?;
            if want_decompress {
                let buf_writer =
                    std::io::BufWriter::with_capacity(DECRYPT_PULL_WRITE_BUF_BYTES, sink);
                let raw_decoder = zstd::stream::raw::Decoder::new()
                    .map_err(|e| std::io::Error::other(format!("zstd decoder init: {e}")))?;
                let mut decoder = zstd::stream::zio::Writer::new_with_capacity(
                    buf_writer,
                    raw_decoder,
                    DECRYPT_PULL_WRITE_BUF_BYTES,
                );
                decoder.write_all(&prefix)?;
                if !suffix.is_empty() {
                    decoder.write_all(suffix)?;
                }
                self.state = MaybeZstdState::Decoding(decoder);
            } else {
                let mut buf_writer =
                    std::io::BufWriter::with_capacity(DECRYPT_PULL_WRITE_BUF_BYTES, sink);
                buf_writer.write_all(&prefix)?;
                if !suffix.is_empty() {
                    buf_writer.write_all(suffix)?;
                }
                self.state = MaybeZstdState::Passthrough(buf_writer);
            }
            return Ok(buf.len());
        }

        match &mut self.state {
            MaybeZstdState::Decoding(decoder) => decoder.write(buf),
            MaybeZstdState::Passthrough(buf_writer) => buf_writer.write(buf),
            MaybeZstdState::Init(_) | MaybeZstdState::Transient => unreachable!(
                "Init handled above; Transient never observed outside the state-swap window"
            ),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.state {
            MaybeZstdState::Init(sink) => sink.flush(),
            MaybeZstdState::Decoding(decoder) => decoder.flush(),
            MaybeZstdState::Passthrough(buf_writer) => buf_writer.flush(),
            MaybeZstdState::Transient => Ok(()),
        }
    }
}

enum DecryptedFileSinkInner {
    Dense(std::fs::File),
    Classified(Box<SparseFileWriter>),
}

struct DecryptedFileSink {
    inner: DecryptedFileSinkInner,
    writeback: StreamWriteback,
    materialized_through: u64,
}

impl DecryptedFileSink {
    fn new(
        file: std::fs::File,
        mode: MaterializationMode,
        block_bytes: usize,
        candidate_observer: Arc<std::sync::atomic::AtomicU64>,
        writeback_coordinator: Option<Arc<StreamWritebackCoordinator>>,
        timing: Option<Arc<StreamPullTiming>>,
        priority: JobPriority,
    ) -> std::io::Result<Self> {
        let inner = match mode {
            MaterializationMode::Disabled | MaterializationMode::Shadow => {
                DecryptedFileSinkInner::Dense(file)
            }
            MaterializationMode::Enabled => DecryptedFileSinkInner::Classified(Box::new(
                SparseFileWriter::with_observer(file, block_bytes, candidate_observer)?,
            )),
        };
        Ok(Self {
            inner,
            writeback: StreamWriteback::new(writeback_coordinator, timing, priority),
            materialized_through: 0,
        })
    }

    fn expose_classified_debt_before_blocking(&mut self) -> std::io::Result<()> {
        let DecryptedFileSinkInner::Classified(writer) = &mut self.inner else {
            return Ok(());
        };
        writer.materialize_complete_blocks_if_buffered_at_least(1)?;
        self.materialized_through = writer.materialized_through();
        let materialization = writer.take_materialization();
        self.writeback.classify(writer.file(), materialization)
    }
}

impl CompletingWrite for DecryptedFileSink {
    fn complete(self) -> std::io::Result<()> {
        let Self {
            inner,
            mut writeback,
            materialized_through: _,
        } = self;
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result: std::io::Result<(std::fs::File, Option<SparseMaterialization>)> = match inner {
            DecryptedFileSinkInner::Dense(mut file) => {
                file.flush()?;
                Ok((file, None))
            }
            DecryptedFileSinkInner::Classified(writer) => {
                let (file, _, materialization) = writer.finish_with_stats_and_materialization()?;
                Ok((file, Some(materialization)))
            }
        };
        if let Some(timing) = &writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                flush_started_at,
                flush_cpu_started_at,
            );
        }
        let (file, materialization) = result?;
        if let Some(materialization) = materialization {
            writeback.classify(&file, materialization)?;
        }
        writeback.flush_pending(&file)?;
        Ok(())
    }
}

impl Write for DecryptedFileSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if !self.writeback.try_reserve(buf.len())? {
            if matches!(&self.inner, DecryptedFileSinkInner::Classified(_)) {
                self.expose_classified_debt_before_blocking()?;
            }
            let file = match &self.inner {
                DecryptedFileSinkInner::Dense(file) => file,
                DecryptedFileSinkInner::Classified(writer) => writer.file(),
            };
            self.writeback.flush_pending(file)?;
            self.writeback.reserve(buf.len())?;
        }
        let write_started_at = Instant::now();
        let write_cpu_started_at = current_thread_cpu_us();
        let write_offset = self.materialized_through;
        let result = match &mut self.inner {
            DecryptedFileSinkInner::Dense(file) => file.write(buf).and_then(|written| {
                self.materialized_through =
                    self.materialized_through
                        .checked_add(written as u64)
                        .ok_or_else(|| std::io::Error::other("streamed output exceeds u64::MAX"))?;
                Ok((written, None))
            }),
            DecryptedFileSinkInner::Classified(writer) => writer.write(buf).and_then(|written| {
                writer.materialize_complete_blocks_if_buffered_at_least(
                    STREAM_WRITEBACK_WINDOW_BYTES,
                )?;
                self.materialized_through = writer.materialized_through();
                Ok((written, Some(writer.take_materialization())))
            }),
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                write_started_at,
                write_cpu_started_at,
            );
        }
        let (written, materialization) = match result {
            Ok(result) => result,
            Err(error) => {
                self.writeback.release_unwritten(buf.len());
                return Err(error);
            }
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add(&timing.decoded_output_bytes, written as u64);
        }
        self.writeback.record_written(written);
        self.writeback
            .release_unwritten(buf.len().saturating_sub(written));
        let file = match &self.inner {
            DecryptedFileSinkInner::Dense(file) => file,
            DecryptedFileSinkInner::Classified(writer) => writer.file(),
        };
        if let Some(materialization) = materialization {
            self.writeback.classify(file, materialization)?;
        } else if written != 0 {
            self.writeback
                .classify_dense(file, write_offset, written as u64)?;
        }
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result = match &mut self.inner {
            DecryptedFileSinkInner::Dense(file) => file.flush().map(|()| None),
            DecryptedFileSinkInner::Classified(writer) => {
                writer.flush()?;
                self.materialized_through = writer.materialized_through();
                Ok(Some(writer.take_materialization()))
            }
        };
        if let Some(timing) = &self.writeback.timing {
            StreamPullTiming::add_wall_and_thread_cpu(
                &timing.output_writer_us,
                &timing.output_writer_cpu_us,
                flush_started_at,
                flush_cpu_started_at,
            );
        }
        let materialization = result?;
        let file = match &self.inner {
            DecryptedFileSinkInner::Dense(file) => file,
            DecryptedFileSinkInner::Classified(writer) => writer.file(),
        };
        if let Some(materialization) = materialization {
            self.writeback.classify(file, materialization)?;
        }
        self.writeback.flush_pending(file)?;
        Ok(())
    }
}

type StreamSinkSelector =
    Box<dyn FnOnce(std::fs::File, bool) -> std::io::Result<DecryptedFileSink> + Send>;

/// Adapts [`MaybeZstdDecodingWriter`] to the object store's
/// [`pplx_space_object_store::FinishingWrite`] so the ranged download's
/// sequential writer decodes inline (one pass: fetch, decode, write).
/// Sparse selection happens only after the zstd-magic decision so a
/// passthrough stream remains dense.
struct StreamDecodeSink {
    inner: MaybeZstdDecodingWriter<std::fs::File, DecryptedFileSink, StreamSinkSelector>,
    timing: Arc<StreamPullTiming>,
}

impl StreamDecodeSink {
    fn new(
        file: std::fs::File,
        mode_if_decompressed: MaterializationMode,
        block_bytes: usize,
        candidate_observer: Arc<std::sync::atomic::AtomicU64>,
        writeback_coordinator: Arc<StreamWritebackCoordinator>,
        timing: Arc<StreamPullTiming>,
        priority: JobPriority,
    ) -> Self {
        let timing_for_sink = Arc::clone(&timing);
        let select_sink: StreamSinkSelector = Box::new(move |file, decompressing| {
            let mode = if decompressing {
                mode_if_decompressed
            } else {
                MaterializationMode::Disabled
            };
            DecryptedFileSink::new(
                file,
                mode,
                block_bytes,
                candidate_observer,
                Some(writeback_coordinator),
                Some(timing_for_sink),
                priority,
            )
        });
        Self {
            inner: MaybeZstdDecodingWriter::with_sink_selector(file, true, select_sink),
            timing,
        }
    }
}

impl Write for StreamDecodeSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let write_started_at = Instant::now();
        let write_cpu_started_at = current_thread_cpu_us();
        let result = self.inner.write(buf);
        StreamPullTiming::add_wall_and_thread_cpu(
            &self.timing.decode_writer_us,
            &self.timing.decode_writer_cpu_us,
            write_started_at,
            write_cpu_started_at,
        );
        if let Ok(written) = &result {
            StreamPullTiming::add(&self.timing.compressed_input_bytes, *written as u64);
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let flush_started_at = Instant::now();
        let flush_cpu_started_at = current_thread_cpu_us();
        let result = self.inner.flush();
        StreamPullTiming::add_wall_and_thread_cpu(
            &self.timing.decode_writer_us,
            &self.timing.decode_writer_cpu_us,
            flush_started_at,
            flush_cpu_started_at,
        );
        result
    }
}

impl pplx_space_object_store::FinishingWrite for StreamDecodeSink {
    fn finish(self: Box<Self>) -> std::io::Result<bool> {
        let StreamDecodeSink { inner, timing } = *self;
        let finish_started_at = Instant::now();
        let finish_cpu_started_at = current_thread_cpu_us();
        let result = inner.finish();
        StreamPullTiming::add_wall_and_thread_cpu(
            &timing.decode_writer_us,
            &timing.decode_writer_cpu_us,
            finish_started_at,
            finish_cpu_started_at,
        );
        result
    }
}

/// Codec admission bridge for streaming-decode pulls: one permit spans the
/// whole overlapped download+decode. The streamed path only runs for
/// ranged-size objects, whose decode cannot be proven small before the
/// bytes arrive (the frame header is not on disk yet), so admission is
/// unconditional — the conservative direction, and exactly what keeps the
/// highly-compressible multi-GiB snapshot class bounded.
struct StreamedPullAdmission<'a> {
    executor: &'a TransferExecutor,
    kind: &'a str,
    artifact: &'static str,
    mode: MaterializationMode,
    priority: JobPriority,
}

#[async_trait::async_trait]
impl pplx_space_object_store::DecodeAdmission for StreamedPullAdmission<'_> {
    async fn admit(
        &self,
        _object_size: u64,
    ) -> Result<
        Option<pplx_space_object_store::DecodeGuard>,
        pplx_space_object_store::DecodeAdmissionError,
    > {
        let wait_started = Instant::now();
        // Typed so the failure crosses the object-store boundary as a source
        // error rather than a string.
        let permit = self
            .executor
            .codec
            .acquire(self.executor.codec_priority(self.priority))
            .await
            .map_err(pplx_space_object_store::DecodeAdmissionError::admission)?;
        emit_transfer_phase_duration(
            self.kind,
            self.artifact,
            TransferPhase::DecompressWait,
            self.mode,
            wait_started,
        );
        Ok(Some(Box::new(permit)))
    }
}

/// Pull-side counterpart of [`stage_encrypted_temp`]. Reads the
/// downloaded ciphertext at `ciphertext_path`, unwraps the DEK,
/// AES-256-GCM-decrypts in 1 MiB chunks, and feeds the plaintext
/// straight into a [`MaybeZstdDecodingWriter`] writing the final
/// `.plaintext.tmp` next to the input. No `.plaintext.tmp` for the
/// decrypted-but-still-compressed intermediate — when the plaintext
/// is zstd-magic and the caller asked for transparent decompression,
/// it lands decompressed on disk in one pass.
///
/// Returns the plaintext temp path and decompression report on success. On
/// failure the partial `.plaintext.tmp` may exist — the caller is
/// responsible for cleaning it up, same contract as
/// [`stage_encrypted_temp`].
#[derive(Clone, Copy)]
struct DecryptedPullOptions {
    decompress_if_zstd: bool,
    materialization_mode: MaterializationMode,
}

async fn stage_decrypted_temp(
    job: &Job,
    p: &TransferPayload,
    enc: &TransferEncryption,
    ciphertext_path: &Path,
    options: DecryptedPullOptions,
    resolved_dek: Option<&[u8]>,
    codec_permit: Option<CodecPermit>,
) -> Result<(PathBuf, bool, DecompressionReport), (PathBuf, JobError)> {
    let DecryptedPullOptions {
        decompress_if_zstd,
        materialization_mode: requested_mode,
    } = options;
    let plaintext_tmp = decrypted_sibling(ciphertext_path);
    let (wrap_aad, entry_aad) = match derive_artifact_aads(p, enc.organization_id) {
        Ok(a) => a,
        Err(e) => return Err((plaintext_tmp, e)),
    };
    // The artifact path unwraps once for the whole set and passes the DEK in
    // (`resolved_dek`), avoiding a per-entry credmgr `UnwrapDek` / KMS
    // `Decrypt` for the same artifact DEK.
    let dek = match resolved_dek {
        Some(d) => zeroize::Zeroizing::new(d.to_vec()),
        None => match unwrap_dek(
            enc.organization_id,
            enc.provider,
            &enc.master_key_identifier,
            &enc.wrapped_dek,
            &wrap_aad,
        )
        .await
        {
            Ok(d) => d,
            Err(e) => return Err((plaintext_tmp, unwrap_error_to_job_error(e))),
        },
    };
    tracing::debug!(
        job_id = %job.job_id,
        subject_id = %p.subject_id,
        local_path = %p.local_path,
        plaintext_tmp = %plaintext_tmp.display(),
        decompress_if_zstd,
        "decrypt + maybe-decompress streaming pull"
    );
    let ciphertext = ciphertext_path.to_path_buf();
    let plaintext = plaintext_tmp.clone();
    let snapshot_artifact = p.effective_snapshot_artifact();
    let mode_if_decompressed = effective_materialization_mode(snapshot_artifact, requested_mode);
    let block_bytes = materialization_block_bytes(snapshot_artifact);
    let job_id = job.job_id;
    let subject_id = p.subject_id.clone();
    let job_priority = job.priority;
    let result: Result<(bool, DecompressionReport), JobError> =
        tokio::task::spawn_blocking(move || {
            let _codec_permit = codec_permit;
            let mut src_file = std::fs::File::open(&ciphertext)
                .map_err(|e| JobError::Permanent(format!("open ciphertext: {e}")))?;
            let dst_file = std::fs::File::create(&plaintext)
                .map_err(|e| JobError::Permanent(format!("create plaintext temp: {e}")))?;
            let candidate_observer = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let observer_for_sink = Arc::clone(&candidate_observer);
            let select_sink = move |file, decompressing| {
                let mode = if decompressing {
                    mode_if_decompressed
                } else {
                    MaterializationMode::Disabled
                };
                DecryptedFileSink::new(
                    file,
                    mode,
                    block_bytes,
                    observer_for_sink,
                    None,
                    None,
                    job_priority,
                )
            };
            let mut writer = MaybeZstdDecodingWriter::with_sink_selector(
                dst_file,
                decompress_if_zstd,
                select_sink,
            );
            crypto::decrypt_stream_blocking(&mut src_file, &mut writer, &dek, &entry_aad)
                .map_err(crypto_error_to_job_error)?;
            let was_decompressed = writer
                .finish()
                .map_err(|e| JobError::Permanent(format!("finalize plaintext writer: {e}")))?;
            page_cache::advise_path_dontneed(&plaintext, "decrypted volume download temp");
            tracing::debug!(
                job_id = %job_id,
                subject_id,
                plaintext_tmp = %plaintext.display(),
                was_decompressed,
                "decrypt + maybe-decompress finished"
            );
            let candidate_hole_bytes =
                candidate_observer.load(std::sync::atomic::Ordering::Relaxed);
            Ok((
                was_decompressed,
                DecompressionReport {
                    mode: if was_decompressed {
                        mode_if_decompressed
                    } else {
                        MaterializationMode::Disabled
                    },
                    candidate_hole_bytes,
                },
            ))
        })
        .await
        .unwrap_or_else(|e| Err(JobError::Permanent(format!("decrypt task panicked: {e}"))));
    match result {
        Ok((was_decompressed, report)) => Ok((plaintext_tmp, was_decompressed, report)),
        Err(err) => Err((plaintext_tmp, err)),
    }
}

/// Map a [`CryptoError`] to the executor's [`JobError`] taxonomy.
///
/// I/O errors are retriable (transient disk pressure can clear up);
/// header / framing / auth failures are permanent because the bytes
/// already on disk won't decrypt successfully on a re-try; bad-key-
/// length is a programming error and surfaces as permanent.
fn crypto_error_to_job_error(e: CryptoError) -> JobError {
    match e {
        CryptoError::Io(io) => JobError::Retriable(format!("crypto io: {io}")),
        CryptoError::InvalidKeyLength(n) => {
            JobError::Permanent(format!("crypto invalid DEK length: {n}"))
        }
        CryptoError::BadHeader => JobError::Permanent("crypto bad header".into()),
        CryptoError::UnsupportedVersion(v) => {
            JobError::Permanent(format!("crypto unsupported version: {v}"))
        }
        CryptoError::Truncated => JobError::Permanent("crypto truncated".into()),
        CryptoError::AuthFailed => JobError::Permanent("crypto auth failed".into()),
    }
}

/// Map an [`UnwrapDekError`] to the executor's [`JobError`] taxonomy.
fn unwrap_error_to_job_error(e: UnwrapDekError) -> JobError {
    match e {
        UnwrapDekError::Connect(msg) => {
            JobError::Retriable(format!("credmgr unwrap connect: {msg}"))
        }
        UnwrapDekError::Permanent { code, message } => {
            JobError::Permanent(format!("credmgr unwrap rejected ({code:?}): {message}"))
        }
        UnwrapDekError::Transient { code, message } => {
            JobError::Retriable(format!("credmgr unwrap transient ({code:?}): {message}"))
        }
    }
}

/// Best-effort cleanup of a per-job temp file. ENOENT is silent (the
/// common case when the encrypt/decrypt step failed before the file
/// was created); other errors are logged so a stuck mount or perm flip
/// surfaces in diagnostics without breaking the upload outcome.
async fn unlink_crypto_tmp(path: &Path, job_id: &JobId, subject_id: &str, reason: &str) {
    match tokio::fs::remove_file(path).await {
        Ok(()) => tracing::debug!(
            job_id = %job_id,
            subject_id,
            path = %path.display(),
            reason,
            "unlinked crypto .enc.tmp"
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => tracing::warn!(
            error = %err,
            job_id = %job_id,
            subject_id,
            path = %path.display(),
            reason,
            "unlink crypto .enc.tmp failed"
        ),
    }
}

async fn apply_file_mode(
    path: &Path,
    file_mode: Option<FileMode>,
    policy: FileModeApplyPolicy,
) -> Result<(), JobError> {
    let Some(file_mode) = file_mode else {
        return Ok(());
    };
    #[cfg(unix)]
    {
        let mode = file_mode.bits();
        let mut permissions = tokio::fs::metadata(path)
            .await
            .map_err(|e| JobError::Permanent(format!("stat {} before chmod: {e}", path.display())))?
            .permissions();
        let current_mode = permissions.mode() & 0o7777;
        if current_mode & 0o777 == mode {
            return Ok(());
        }

        let desired_mode = (current_mode & !0o777) | mode;
        permissions.set_mode(desired_mode);
        match tokio::fs::set_permissions(path, permissions).await {
            Ok(()) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::PermissionDenied
                    && should_ignore_chmod_permission_denied(
                        policy,
                        current_mode,
                        desired_mode,
                    ) =>
            {
                tracing::warn!(
                    path = %path.display(),
                    current_mode = format_args!("{current_mode:o}"),
                    desired_mode = format_args!("{desired_mode:o}"),
                    policy = ?policy,
                    error = %e,
                    "chmod permission denied; continuing with existing readable mode"
                );
            }
            Err(e) => {
                return Err(JobError::Permanent(format!(
                    "chmod {mode:o} {}: {e}",
                    path.display()
                )));
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, policy);
    }
    Ok(())
}

/// Executes transfer jobs (push/pull) against S3, optionally compressing data
/// with zstd before upload and transparently decompressing on download.
///
/// **Push with compression:**
/// Eligible staged snapshots can stream compression directly to S3; other
/// sources use a sibling `.zst.tmp` file, removed after the upload attempt.
/// Either path preserves the S3 key and compressed-object format.
///
/// **Pull with compression awareness:**
/// After download the first 4 bytes are checked for the zstd frame magic. If
/// found the file is decompressed in-place, so pull is always backward-compatible
/// with uncompressed objects already in S3.
///
/// `Clone` is cheap — `S3ObjectStore` is internally `Arc`-wrapped.
#[derive(Clone)]
pub struct TransferExecutor {
    logical_digests: Option<Arc<std::sync::Mutex<Vec<LogicalArtifactDigest>>>>,
    node_manager: Option<pplx_space_node_mgr_client::NodeManagerGrpcClient>,
    store: Arc<dyn ObjectStore>,
    config: TransferExecutorConfig,
    destination_pull_locks: DestinationPullLocks,
    /// Admission for CPU-bound codec passes, with an optional reserve for
    /// high-priority jobs.
    codec: CodecAdmission,
    /// Priority at or above which a job may draw on the codec reserve;
    /// `None` disables priority-aware admission.
    codec_priority_threshold: Option<JobPriority>,
    stream_writeback: Arc<StreamWritebackCoordinator>,
    stream_push: Option<StreamPushConfig>,
    shadow_staging_dir: PathBuf,
    shadow_comparison_permits: Arc<tokio::sync::Semaphore>,
    shadow_comparisons: ShadowComparisonController,
}

struct ShadowComparisonLifecycle {
    admission: std::sync::Mutex<()>,
    shutdown_token: CancellationToken,
    tasks: TaskTracker,
}

#[derive(Clone)]
pub struct ShadowComparisonController {
    inner: Arc<ShadowComparisonLifecycle>,
}

impl ShadowComparisonController {
    fn new() -> Self {
        Self {
            inner: Arc::new(ShadowComparisonLifecycle {
                admission: std::sync::Mutex::new(()),
                shutdown_token: CancellationToken::new(),
                tasks: TaskTracker::new(),
            }),
        }
    }

    fn spawn<F>(&self, future: F) -> Option<tokio::task::JoinHandle<F::Output>>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let _admission = self
            .inner
            .admission
            .lock()
            .expect("shadow comparison admission lock poisoned");
        (!self.inner.shutdown_token.is_cancelled()).then(|| self.inner.tasks.spawn(future))
    }

    fn is_accepting(&self) -> bool {
        !self.inner.shutdown_token.is_cancelled()
    }

    fn shutdown_token(&self) -> CancellationToken {
        self.inner.shutdown_token.clone()
    }

    pub async fn shutdown(&self, timeout: std::time::Duration) -> bool {
        {
            let _admission = self
                .inner
                .admission
                .lock()
                .expect("shadow comparison admission lock poisoned");
            self.inner.shutdown_token.cancel();
            self.inner.tasks.close();
        }
        tokio::time::timeout(timeout, self.inner.tasks.wait())
            .await
            .is_ok()
    }
}

struct SparseShadowTask {
    job_id: JobId,
    subject_id: String,
    local_path: PathBuf,
    snapshot_path: PathBuf,
    block_bytes: usize,
    artifact: &'static str,
}

struct SparseShadowAdmission {
    _shadow_permit: tokio::sync::OwnedSemaphorePermit,
    _codec_permit: CodecPermit,
}

fn sparse_shadow_disk_floor_bytes(status: pplx_space_common_core::host::disk::DiskStatus) -> u64 {
    pplx_space_common_core::host::disk_admission::CacheDiskAdmissionConfig::default()
        .hard_floor_bytes(status.total_bytes)
}

fn sparse_shadow_has_disk_headroom(
    status: pplx_space_common_core::host::disk::DiskStatus,
    logical_bytes: u64,
) -> bool {
    status.available_bytes >= sparse_shadow_disk_floor_bytes(status).saturating_add(logical_bytes)
}

async fn remove_sparse_shadow_snapshot(path: &Path, reason: &'static str) {
    if let Err(error) = tokio::fs::remove_file(path).await
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(
            shadow_source_path = %path.display(),
            %error,
            reason,
            "failed to remove sparse shadow snapshot"
        );
    }
}

fn codec_admission_closed(closed: CodecAdmissionClosed) -> JobError {
    JobError::Retriable(closed.to_string())
}

/// Executor tuning resolved by the volume-manager binary and passed into the
/// service. Groups the knobs that the transfer executor needs so the service
/// constructor stays narrow.
#[derive(Clone, Default)]
pub struct TransferExecutorConfig {
    /// Compress snapshot files with zstd before uploading to S3.
    pub compress: bool,
    /// Deployment upload policy with one shared streaming-buffer budget.
    pub snapshot_upload_mode: SnapshotUploadMode,
    /// Maximum concurrent CPU-bound zstd passes (compress or decompress).
    /// `0` auto-sizes to the CPU quota. See [`resolved_codec_concurrency`].
    pub codec_concurrency: usize,
    /// Decode zstd pulls inline with the ranged download (one pass) instead
    /// of a separate read-back decompress step. Kill switch for rollout;
    /// non-ranged downloads always keep the two-pass path.
    pub stream_decode_pulls: bool,
    /// Codec permits carved from the total and reserved for high-priority
    /// jobs (see `codec_priority_threshold`). Unlike the pull priority
    /// bands — which add *extra* I/O slots — codec is CPU-bound at the
    /// quota, so protected capacity must be reserved from the total, not
    /// added to it. Keep it small; `0` disables priority-aware admission.
    pub codec_priority_reserve: usize,
    /// Jobs at or above this priority may draw on the reserved codec
    /// permits. `None` disables priority-aware admission (all jobs share
    /// one pool). Derived from the lowest configured pull priority band.
    pub codec_priority_threshold: Option<JobPriority>,
    /// Process-wide high watermark for streamed output bytes awaiting
    /// writeback and cache eviction. `0` uses
    /// [`DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES`].
    pub stream_writeback_budget_bytes: u64,
    /// Optional caller-supplied S3 credentials. When set, they are used
    /// directly instead of exchanging a SPACE subject for short-lived AWS
    /// credentials via credmgr. The volume-manager binary resolves these from
    /// its deployment environment (e.g. the local kind stack) and injects
    /// them here; production leaves this `None` and uses the credmgr exchange.
    pub static_credentials: Option<pplx_space_object_store::ObjectCredentials>,
}

impl TransferExecutor {
    pub fn new(config: TransferExecutorConfig, shadow_staging_dir: PathBuf) -> Self {
        Self::with_store(config, Arc::new(S3ObjectStore::new()), shadow_staging_dir)
    }

    fn with_store(
        config: TransferExecutorConfig,
        store: Arc<dyn ObjectStore>,
        shadow_staging_dir: PathBuf,
    ) -> Self {
        let total = resolved_codec_concurrency(config.codec_concurrency);
        let codec = CodecAdmission::new(total, config.codec_priority_reserve);
        let stream_writeback_priority_threshold = config.codec_priority_threshold;
        // Priority-aware admission only engages when there is both a
        // threshold and a non-empty reserve; otherwise everything shares
        // one pool exactly as before.
        let codec_priority_threshold = config
            .codec_priority_threshold
            .filter(|_| codec.has_reserve());
        let stream_writeback_budget_bytes = if config.stream_writeback_budget_bytes == 0 {
            DEFAULT_STREAM_WRITEBACK_BUDGET_BYTES
        } else {
            config.stream_writeback_budget_bytes
        };
        let shadow_comparisons = ShadowComparisonController::new();
        Self {
            logical_digests: None,
            node_manager: None,
            stream_push: StreamPushConfig::new(&config.snapshot_upload_mode, total),
            store,
            config,
            destination_pull_locks: Arc::new(DashMap::new()),
            codec,
            codec_priority_threshold,
            stream_writeback: StreamWritebackCoordinator::new(
                stream_writeback_budget_bytes,
                stream_writeback_priority_threshold,
            ),
            shadow_staging_dir,
            shadow_comparison_permits: Arc::new(tokio::sync::Semaphore::new(
                SHADOW_COMPARISON_CONCURRENCY,
            )),
            shadow_comparisons,
        }
    }

    /// Supply the privileged source service while retaining the executor's
    /// shared admission and upload budget. Normal deployments use the local UDS.
    pub fn with_node_manager(
        mut self,
        client: pplx_space_node_mgr_client::NodeManagerGrpcClient,
    ) -> Self {
        self.node_manager = Some(client);
        self
    }

    pub fn shadow_comparison_controller(&self) -> ShadowComparisonController {
        self.shadow_comparisons.clone()
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(
        config: TransferExecutorConfig,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self::with_store(
            config,
            store,
            std::env::temp_dir().join("space-volmgr-shadow-tests"),
        )
    }

    /// Admit one CPU-bound codec pass whose work scales with its *input*
    /// (compression, encryption). Inputs below
    /// [`CODEC_ADMISSION_MIN_INPUT_BYTES`] skip admission and return
    /// `None`. Pass the returned permit into the blocking codec closure so
    /// it releases only when the CPU work actually exits — the async
    /// caller can be cancelled by the worker's transfer timeout while
    /// `spawn_blocking` runs on.
    async fn acquire_codec_permit_for_input(
        &self,
        input_bytes: u64,
        priority: JobPriority,
    ) -> Result<Option<CodecPermit>, JobError> {
        if input_bytes < CODEC_ADMISSION_MIN_INPUT_BYTES {
            return Ok(None);
        }
        self.acquire_codec_permit(priority).await.map(Some)
    }

    /// Admit one CPU-bound decode pass. Decode work scales with the
    /// *decompressed* size, and a small compressed input can inflate to a
    /// multi-GiB decode, so gate on the pledged frame content size — never
    /// the compressed length. Bypass only when the frame header pledges a
    /// content size below the threshold; an unknown content size (streaming
    /// zstd frames and all ciphertext, which never parses as a frame)
    /// cannot be proven small and always takes a permit.
    async fn acquire_codec_permit_for_decode(
        &self,
        path: &Path,
        priority: JobPriority,
    ) -> Result<Option<CodecPermit>, JobError> {
        if frame_content_size(path)
            .await
            .is_some_and(|size| size < CODEC_ADMISSION_MIN_INPUT_BYTES)
        {
            return Ok(None);
        }
        self.acquire_codec_permit(priority).await.map(Some)
    }

    /// Map a job priority onto codec admission. Jobs at or above
    /// `codec_priority_threshold` race the reserved and shared pools, so a
    /// protected restore never queues behind the low-priority codec backlog
    /// on the shared pool; everything else draws on the shared pool only.
    fn codec_priority(&self, priority: JobPriority) -> CodecPriority {
        if self
            .codec_priority_threshold
            .is_some_and(|threshold| priority >= threshold)
        {
            CodecPriority::High
        } else {
            CodecPriority::Normal
        }
    }

    /// Acquire one codec permit, respecting the priority reservation.
    async fn acquire_codec_permit(&self, priority: JobPriority) -> Result<CodecPermit, JobError> {
        self.codec
            .acquire(self.codec_priority(priority))
            .await
            .map_err(codec_admission_closed)
    }

    /// [`Self::acquire_codec_permit`] for a pass that lends its permit while
    /// it cannot run (see [`CodecSlot`]).
    async fn acquire_codec_slot(&self, priority: JobPriority) -> Result<CodecSlot, JobError> {
        self.codec
            .acquire_slot(self.codec_priority(priority))
            .await
            .map_err(codec_admission_closed)
    }

    fn try_acquire_sparse_shadow_admission(&self) -> Option<SparseShadowAdmission> {
        let shadow_permit = Arc::clone(&self.shadow_comparison_permits)
            .try_acquire_owned()
            .ok()?;
        let codec_permit = self.codec.try_acquire_shared()?;
        Some(SparseShadowAdmission {
            _shadow_permit: shadow_permit,
            _codec_permit: codec_permit,
        })
    }

    async fn schedule_sparse_shadow_comparison(
        &self,
        job_id: &JobId,
        kind: &str,
        subject_id: &str,
        local_path: &Path,
        snapshot_artifact: Option<SnapshotArtifact>,
        mode: MaterializationMode,
    ) -> Option<tokio::task::JoinHandle<Option<ShadowComparisonObservation>>> {
        if mode != MaterializationMode::Shadow {
            return None;
        }
        if !self.shadow_comparisons.is_accepting() {
            return None;
        }

        let snapshot_artifact =
            snapshot_artifact.or_else(|| snapshot_artifact_from_local_path(local_path));
        let artifact = transfer_artifact_tag(snapshot_artifact);
        let block_bytes = materialization_block_bytes(snapshot_artifact);
        let Some(admission) = self.try_acquire_sparse_shadow_admission() else {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::SkippedBusy.as_str(),
            );
            tracing::info!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "background sparse shadow comparison skipped because the node is busy"
            );
            return None;
        };
        let Ok(metadata) = tokio::fs::metadata(local_path).await else {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::Error.as_str(),
            );
            tracing::error!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                "could not inspect completed artifact before sparse shadow comparison"
            );
            return None;
        };
        let logical_bytes = metadata.len();
        #[cfg(unix)]
        let disk_status = self
            .shadow_staging_dir
            .to_str()
            .and_then(pplx_space_common_core::host::disk::statvfs_status);
        #[cfg(not(unix))]
        let disk_status: Option<pplx_space_common_core::host::disk::DiskStatus> = None;
        let disk_floor_bytes = disk_status.map_or(0, sparse_shadow_disk_floor_bytes);
        let has_disk_headroom = disk_status
            .is_some_and(|status| sparse_shadow_has_disk_headroom(status, logical_bytes));
        if !has_disk_headroom {
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::SkippedDisk.as_str(),
            );
            tracing::warn!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                logical_bytes,
                available_bytes = disk_status.map(|status| status.available_bytes),
                disk_floor_bytes,
                "background sparse shadow comparison skipped for disk headroom"
            );
            return None;
        }
        let snapshot_path = sparse_shadow_source_path(&self.shadow_staging_dir);
        let snapshot_started_at = Instant::now();
        if let Err(error) = crate::staging::reflink_only(local_path, &snapshot_path).await {
            remove_sparse_shadow_snapshot(&snapshot_path, "reflink failed").await;
            pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                artifact,
                ShadowComparisonOutcome::Error.as_str(),
            );
            tracing::error!(
                job_id = %job_id,
                subject_id,
                local_path = %local_path.display(),
                shadow_source_path = %snapshot_path.display(),
                %error,
                "could not snapshot completed artifact for background sparse shadow comparison"
            );
            return None;
        }
        emit_transfer_phase_duration(
            kind,
            artifact,
            TransferPhase::ShadowReflink,
            MaterializationMode::Shadow,
            snapshot_started_at,
        );

        tracing::info!(
            job_id = %job_id,
            subject_id,
            local_path = %local_path.display(),
            shadow_source_path = %snapshot_path.display(),
            snapshot_elapsed_ms = snapshot_started_at.elapsed().as_millis(),
            "background sparse shadow comparison scheduled"
        );

        let cleanup_path = snapshot_path.clone();
        let handle = self.spawn_sparse_shadow_comparison(
            SparseShadowTask {
                job_id: *job_id,
                subject_id: subject_id.to_string(),
                local_path: local_path.to_path_buf(),
                snapshot_path,
                block_bytes,
                artifact,
            },
            admission,
        );
        if handle.is_none() {
            remove_sparse_shadow_snapshot(&cleanup_path, "task admission closed").await;
        }
        handle
    }

    fn spawn_sparse_shadow_comparison(
        &self,
        task: SparseShadowTask,
        admission: SparseShadowAdmission,
    ) -> Option<tokio::task::JoinHandle<Option<ShadowComparisonObservation>>> {
        let shutdown_token = self.shadow_comparisons.shutdown_token();
        let writeback_coordinator = Arc::clone(&self.stream_writeback);
        self.shadow_comparisons.spawn(async move {
            let SparseShadowTask {
                job_id,
                subject_id,
                local_path,
                snapshot_path,
                block_bytes,
                artifact,
            } = task;
            let _admission = admission;
            if shutdown_token.is_cancelled() {
                remove_sparse_shadow_snapshot(&snapshot_path, "shutdown before comparison").await;
                return None;
            }
            let comparison_path = snapshot_path.clone();
            let blocking_shutdown_token = shutdown_token.clone();
            let result = tokio::task::spawn_blocking(move || {
                compare_sparse_shadow(
                    &comparison_path,
                    block_bytes,
                    &blocking_shutdown_token,
                    writeback_coordinator,
                )
            })
            .await;
            remove_sparse_shadow_snapshot(&snapshot_path, "comparison finished").await;

            match result {
                Ok(Ok(observation)) => {
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        ShadowComparisonOutcome::Match.as_str(),
                    );
                    pplx_space_metrics::volume_manager::transfer_materialization_allocation(
                        "pull",
                        artifact,
                        MaterializationMode::Shadow,
                        MaterializationLayout::Sparse,
                        observation.logical_bytes,
                        observation.allocated_bytes,
                        observation.candidate_hole_bytes,
                    );
                    tracing::info!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        shadow_logical_bytes = observation.logical_bytes,
                        shadow_allocated_bytes = observation.allocated_bytes,
                        candidate_hole_bytes = observation.candidate_hole_bytes,
                        "background sparse shadow comparison completed"
                    );
                    Some(observation)
                }
                Ok(Err(SparseShadowCmpError::Cancelled)) => {
                    tracing::info!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        "background sparse shadow comparison cancelled during shutdown"
                    );
                    None
                }
                Ok(Err(error)) => {
                    let outcome = if matches!(error, SparseShadowCmpError::Mismatch) {
                        ShadowComparisonOutcome::Mismatch
                    } else {
                        ShadowComparisonOutcome::Error
                    };
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        outcome.as_str(),
                    );
                    tracing::error!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        ?error,
                        comparison = outcome.as_str(),
                        "background sparse shadow comparison failed"
                    );
                    None
                }
                Err(error) => {
                    pplx_space_metrics::volume_manager::transfer_materialization_shadow_comparison(
                        artifact,
                        ShadowComparisonOutcome::Error.as_str(),
                    );
                    tracing::error!(
                        job_id = %job_id,
                        subject_id,
                        local_path = %local_path.display(),
                        %error,
                        "background sparse shadow comparison task panicked"
                    );
                    None
                }
            }
        })
    }
}

/// Which direction an artifact job runs. The job `kind` string
/// (`"push_artifact"`/`"pull_artifact"`) is the wire form; this is the
/// in-process closed-set vocabulary the executor dispatches on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ArtifactKind {
    Push,
    Pull,
}

/// Artifact identities can be retried under a different materialization mode
/// while targeting the same local paths. Only verified provenance can prove
/// that an existing file satisfies the new postcondition.
const fn artifact_pull_existing_policy() -> PullExistingPolicy {
    PullExistingPolicy::VerifiedReplica
}

#[async_trait::async_trait]
impl JobExecutor for TransferExecutor {
    async fn execute(&self, job: &Job, cancellation: CancellationToken) -> Result<i64, JobError> {
        match job.kind.as_str() {
            // Single-file transfers (template raw pulls, single uploads):
            // one TransferPayload, always plaintext.
            "push" | "pull" => self.execute_single(job, cancellation).await,
            "push_btrfs_send" => self.execute_single(job, cancellation).await,
            // Artifact transfers: one ArtifactPayload covering N entries,
            // optionally envelope-encrypted under one per-artifact DEK.
            "push_artifact" => {
                self.execute_artifact(job, ArtifactKind::Push, cancellation)
                    .await
            }
            "pull_artifact" => {
                self.execute_artifact(job, ArtifactKind::Pull, cancellation)
                    .await
            }
            other => Err(JobError::Permanent(format!("unknown kind: {other}"))),
        }
    }

    async fn execute_with_completion(
        &self,
        job: &Job,
        cancellation: CancellationToken,
    ) -> Result<pplx_space_common_jobs::JobCompletion, JobError> {
        let mut executor = self.clone();
        let digests = Arc::new(std::sync::Mutex::new(Vec::new()));
        executor.logical_digests = Some(digests.clone());
        let bytes_processed = executor.execute(job, cancellation).await?;
        let mut proofs = digests
            .lock()
            .map_err(|_| JobError::permanent("logical digest collection lock poisoned"))?
            .clone();
        proofs.sort_by_key(|entry| entry.artifact);
        if proofs
            .windows(2)
            .any(|pair| matches!(pair, [a,b] if a.artifact == b.artifact))
        {
            return Err(JobError::permanent(
                "upload reports duplicate logical artifact roles",
            ));
        }
        let payload =
            if proofs.is_empty() {
                None
            } else {
                let mut payload: serde_json::Value = job.payload_as()?;
                payload
                    .as_object_mut()
                    .ok_or_else(|| JobError::permanent("transfer payload is not an object"))?
                    .insert(
                        "logical_artifact_digests".into(),
                        serde_json::to_value(proofs).map_err(|e| {
                            JobError::permanent(format!("serialize logical digests: {e}"))
                        })?,
                    );
                Some(serde_json::to_string(&payload).map_err(|e| {
                    JobError::permanent(format!("serialize transfer completion: {e}"))
                })?)
            };
        Ok(pplx_space_common_jobs::JobCompletion {
            bytes_processed,
            payload,
        })
    }

    async fn cleanup_cancelled(&self, job: &Job) -> Result<(), JobError> {
        match job.kind.as_str() {
            "push_btrfs_send" => {
                let payload: crate::btrfs_push::Payload = job.payload_as()?;
                let mut client = self
                    .node_manager
                    .clone()
                    .unwrap_or_else(pplx_space_node_mgr_client::lazy);
                client
                    .btrfs_delete_send_source(pplx_space_node_mgr_client::proto::BtrfsSendLease {
                        lease_id: payload.send_lease.to_string(),
                    })
                    .await
                    .map(|_| ())
                    .map_err(crate::btrfs_push::classify)
            }
            "pull" => {
                let payload = TransferPayload::from_json(&job.payload)?;
                self.cleanup_cancelled_pull_path(Path::new(&payload.local_path))
                    .await
            }
            "pull_artifact" => {
                let payload = ArtifactPayload::from_json(&job.payload)?;
                for entry in payload.entries {
                    self.cleanup_cancelled_pull_path(Path::new(&entry.local_path))
                        .await?;
                }
                Ok(())
            }
            "push" => {
                let payload = TransferPayload::from_json(&job.payload)?;
                if let Some(staged) = payload.staged_path {
                    remove_cancelled_path(Path::new(&staged)).await?;
                }
                Ok(())
            }
            "push_artifact" => {
                let payload = ArtifactPayload::from_json(&job.payload)?;
                for entry in payload.entries {
                    if let Some(staged) = entry.staged_path {
                        remove_cancelled_path(Path::new(&staged)).await?;
                    }
                }
                Ok(())
            }
            other => Err(JobError::Permanent(format!("unknown kind: {other}"))),
        }
    }
}

async fn remove_cancelled_path(path: &Path) -> Result<(), JobError> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(JobError::Retriable(format!(
            "remove cancelled transfer output {}: {error}",
            path.display()
        ))),
    }
}

async fn cleanup_cancelled_pull_temp_paths(local_path: &Path) -> Result<(), JobError> {
    remove_cancelled_path(&partial_path(local_path)).await?;
    remove_cancelled_path(&encrypted_sibling(local_path)).await?;
    remove_cancelled_path(&decrypted_sibling(local_path)).await
}

/// Persisted payload of a `push_artifact`/`pull_artifact` job: the
/// durable record of one multi-file artifact. Written to the job row at
/// submit time — *before* any entry is uploaded — so the wrapped DEK and
/// the entry set survive a crash and a retry keyed on `artifact_id`
/// recovers the same record (no fresh DEK). The executor fans this out
/// over the entries, building a synthetic [`TransferPayload`] per entry.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactPayload {
    /// Wire form of [`SubjectId`] (`"<kind>:<id>"`); re-parsed at run
    /// time. `sandbox_id` alias keeps any pre-rename rows decodable.
    #[serde(alias = "sandbox_id")]
    pub subject_id: String,
    /// Caller-minted stable identity (UUID string). Pull job deduplication
    /// combines this identity with non-default materialization postconditions;
    /// the persisted artifact identity itself remains unchanged. It is not
    /// bound into the wrap AAD — cross-artifact ciphertext is already
    /// non-interchangeable via per-DEK isolation (one fresh `IssueDek` per
    /// artifact), so callers can recover the wrap without persisting
    /// `artifact_id` alongside it.
    pub artifact_id: String,
    /// The files of the artifact.
    pub entries: Vec<ArtifactEntryPayload>,
    /// Sparse-decompression assignment persisted once for the whole artifact.
    #[serde(default, skip_serializing_if = "MaterializationMode::is_disabled")]
    pub materialization_mode: MaterializationMode,
    /// Rootfs-specific assignment. Missing on payloads persisted before
    /// rootfs rollout support; those rootfs entries fail closed to dense.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_materialization_mode: Option<MaterializationMode>,
    /// Envelope-encryption parameters shared by every entry. The wrapped
    /// DEK here is the one persisted before any upload. Absent =
    /// plaintext artifact.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<TransferEncryption>,
}

/// One entry (file) of an [`ArtifactPayload`].
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ArtifactEntryPayload {
    /// Role within the artifact; bound into this entry's per-chunk AEAD.
    pub role: String,
    /// Typed semantic identity parsed from the batch-entry wire field. Older
    /// callers and queued payloads may still populate it from a canonical
    /// role or destination-name fallback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_artifact: Option<SnapshotArtifact>,
    /// Worker-local path: source on push, destination on pull.
    pub local_path: String,
    /// S3 URI for this entry.
    pub s3_uri: String,
    /// Push only: vol-mgr-owned reflink of `local_path` staged at submit
    /// time so the upload is immune to a later unlink of the original.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staged_path: Option<String>,
    /// Pull only: optional Unix mode to apply after a successful pull.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file_mode: Option<FileMode>,
}

impl ArtifactPayload {
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ArtifactPayload serialization is infallible")
    }

    pub fn from_json(s: &str) -> Result<Self, JobError> {
        serde_json::from_str(s).map_err(|e| {
            JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                format!("invalid artifact payload: {e}"),
            )
        })
    }
}

impl TransferExecutor {
    /// Execute a single-file `push`/`pull` job — one [`TransferPayload`],
    /// one S3 object, always plaintext. (Encryption is a property of an
    /// artifact, never a single file.)
    async fn execute_single(
        &self,
        job: &Job,
        cancellation: CancellationToken,
    ) -> Result<i64, JobError> {
        let p = TransferPayload::from_json(&job.payload)?;

        let subject = parse_subject_id_with_legacy_fallback(&p.subject_id).map_err(|e| {
            JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                format!("invalid subject_id: {e}"),
            )
        })?;

        let uri = StorageUri::parse(&p.s3_uri).map_err(|e| {
            JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                format!("invalid s3_uri: {e}"),
            )
        })?;

        tracing::info!(
            job_id = %job.job_id,
            kind = %job.kind,
            subject_id = %p.subject_id,
            subject_kind = subject.kind().as_str(),
            s3_uri = %p.s3_uri,
            local_path = %p.local_path,
            bucket = %uri.bucket,
            key = %uri.key_prefix,
            region = %uri.region,
            "transfer job execution starting"
        );

        let access = match job.kind.as_str() {
            // Push completion is verified with HeadObject before the job is
            // committed, so the exact object lease needs read as well as write.
            "push" | "push_btrfs_send" => StorageAccess::ReadWrite,
            "pull" => StorageAccess::Read,
            other => return Err(JobError::Permanent(format!("unknown kind: {other}"))),
        };
        let creds = match &self.config.static_credentials {
            Some(creds) => creds.clone(),
            None => exchange_credentials(&uri.bucket, &uri.key_prefix, false, &subject, access)
                .await
                .map_err(|e| JobError::Retriable(format!("credential exchange failed: {e}")))?,
        };

        let target = ObjectTarget {
            bucket: &uri.bucket,
            key: &uri.key_prefix,
            region: &uri.region,
            endpoint: uri.endpoint.as_deref(),
            path_style: uri.path_style,
        };

        // Single-file transfers are always plaintext (`resolved_dek = None`)
        // and own their staged source cleanup (`cleanup_staged = true`).
        let result = match job.kind.as_str() {
            "push_btrfs_send" => {
                self.execute_btrfs_push(job, &creds, target, cancellation)
                    .await
            }
            "push" => {
                self.execute_push(
                    job,
                    &p,
                    &creds,
                    target,
                    PushExecutionOptions {
                        cleanup_staged: true,
                        resolved_dek: None,
                        cancellation,
                    },
                )
                .await
            }
            "pull" => self
                .execute_pull_cancellable(
                    job,
                    &p,
                    &creds,
                    target,
                    PullExecutionOptions {
                        resolved_dek: None,
                        existing_policy: p.existing_file_policy,
                        cancellation,
                    },
                )
                .await
                .map_err(ensure_pull_failure_reason),
            other => Err(JobError::Permanent(format!("unknown kind: {other}"))),
        };

        match &result {
            Ok(bytes) => tracing::info!(
                job_id = %job.job_id,
                kind = %job.kind,
                subject_id = %p.subject_id,
                local_path = %p.local_path,
                bytes,
                "transfer job completed"
            ),
            Err(err) => tracing::warn!(
                job_id = %job.job_id,
                kind = %job.kind,
                subject_id = %p.subject_id,
                local_path = %p.local_path,
                error_kind = err.kind().as_str(),
                error = %err.message(),
                "transfer job failed"
            ),
        }

        result
    }

    /// Execute an artifact job: drive every entry through the proven
    /// single-file pipeline concurrently, each as a synthetic
    /// [`TransferPayload`] carrying the artifact identity + entry role so
    /// its per-chunk AEAD AAD reproduces. Each entry is idempotent (S3
    /// PUT overwrites on push; pull skips a complete local file), so the
    /// whole job retries safely. A Permanent failure on any entry fails
    /// the artifact permanently (a retry won't fix it); otherwise a
    /// Retriable failure retries the whole set.
    async fn execute_artifact(
        &self,
        job: &Job,
        kind: ArtifactKind,
        cancellation: CancellationToken,
    ) -> Result<i64, JobError> {
        let art = ArtifactPayload::from_json(&job.payload)?;
        if art.entries.is_empty() {
            return Err(JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                "artifact has no entries",
            ));
        }

        tracing::info!(
            job_id = %job.job_id,
            kind = %job.kind,
            subject_id = %art.subject_id,
            artifact_id = %art.artifact_id,
            entries = art.entries.len(),
            encrypted = art.encryption.is_some(),
            "artifact job execution starting"
        );

        let subject = parse_subject_id_with_legacy_fallback(&art.subject_id).map_err(|e| {
            JobError::permanent_with_reason(
                JobFailureReason::InvalidInput,
                format!("invalid subject_id: {e}"),
            )
        })?;

        // Parse every entry URI once.
        let uris: Vec<StorageUri> = art
            .entries
            .iter()
            .map(|entry| {
                StorageUri::parse(&entry.s3_uri).map_err(|e| {
                    JobError::permanent_with_reason(
                        JobFailureReason::InvalidInput,
                        format!("invalid s3_uri: {e}"),
                    )
                })
            })
            .collect::<Result<_, _>>()?;

        // Exchange once per distinct bucket for the entries' common canonical
        // directory. The authority rejects a common parent broader than the
        // subject's exact snapshot attempt or template content-hash prefix.
        let mut creds_by_bucket: HashMap<String, pplx_space_object_store::ObjectCredentials> =
            HashMap::new();
        for uri in &uris {
            if !creds_by_bucket.contains_key(&uri.bucket) {
                let prefix = common_object_parent(
                    uris.iter()
                        .filter(|candidate| candidate.bucket == uri.bucket)
                        .map(|candidate| candidate.key_prefix.as_str()),
                )
                .ok_or_else(|| {
                    JobError::permanent_with_reason(
                        JobFailureReason::InvalidInput,
                        "artifact entries do not share a scoped object prefix",
                    )
                })?;
                let access = match kind {
                    ArtifactKind::Push => StorageAccess::ReadWrite,
                    ArtifactKind::Pull => StorageAccess::Read,
                };
                let creds = exchange_credentials(&uri.bucket, &prefix, true, &subject, access)
                    .await
                    .map_err(|e| JobError::Retriable(format!("credential exchange failed: {e}")))?;
                creds_by_bucket.insert(uri.bucket.clone(), creds);
            }
        }

        // Unwrap the artifact DEK once. The wrap AAD is artifact-scoped
        // (org, sandbox, artifact_id), so a single unwrap is
        // correctness-equivalent to unwrapping per entry — and avoids N
        // parallel credmgr `UnwrapDek` / KMS `Decrypt` calls, which a retry
        // would otherwise multiply on every attempt.
        let dek = match &art.encryption {
            None => None,
            Some(enc) => {
                let sandbox_id = match &subject {
                    SubjectId::Sandbox(id) => id,
                    SubjectId::Template(_) => {
                        return Err(JobError::permanent_with_reason(
                            JobFailureReason::InvalidInput,
                            "envelope-encrypted transfers require a sandbox subject",
                        ));
                    }
                };
                let aad = crypto::wrap_aad(enc.organization_id, sandbox_id);
                let dek = unwrap_dek(
                    enc.organization_id,
                    enc.provider,
                    &enc.master_key_identifier,
                    &enc.wrapped_dek,
                    &aad,
                )
                .await
                .map_err(unwrap_error_to_job_error)?;
                Some(dek)
            }
        };
        let resolved_dek: Option<&[u8]> = dek.as_ref().map(|d| d.as_slice());

        let entry_futures = art.entries.iter().zip(uris.iter()).map(|(entry, uri)| {
            let cancellation = cancellation.clone();
            let synthetic = TransferPayload {
                subject_id: art.subject_id.clone(),
                local_path: entry.local_path.clone(),
                s3_uri: entry.s3_uri.clone(),
                transfer_scope_id: None,
                snapshot_artifact: entry.snapshot_artifact,
                staged_path: entry.staged_path.clone(),
                raw: false,
                file_mode: entry.file_mode,
                existing_file_policy: PullExistingPolicy::LegacySkipPresent,
                materialization_mode: art.materialization_mode,
                rootfs_materialization_mode: art.rootfs_materialization_mode,
                encryption: art.encryption.clone(),
                artifact_id: Some(art.artifact_id.clone()),
                entry_role: Some(entry.role.clone()),
            };
            let creds = creds_by_bucket
                .get(&uri.bucket)
                .expect("credentials exchanged for every bucket above");
            async move {
                let target = ObjectTarget {
                    bucket: &uri.bucket,
                    key: &uri.key_prefix,
                    region: &uri.region,
                    endpoint: uri.endpoint.as_deref(),
                    path_style: uri.path_style,
                };
                match kind {
                    // `cleanup_staged = false`: artifact-level cleanup runs
                    // after the fan-out (a per-entry success is not terminal
                    // for the artifact).
                    ArtifactKind::Push => {
                        self.execute_push(
                            job,
                            &synthetic,
                            creds,
                            target,
                            PushExecutionOptions {
                                cleanup_staged: false,
                                resolved_dek,
                                cancellation,
                            },
                        )
                        .await
                    }
                    ArtifactKind::Pull => self
                        .execute_pull_cancellable(
                            job,
                            &synthetic,
                            creds,
                            target,
                            PullExecutionOptions {
                                resolved_dek,
                                existing_policy: artifact_pull_existing_policy(),
                                cancellation,
                            },
                        )
                        .await
                        .map_err(ensure_pull_failure_reason),
                }
            }
        });

        let results = futures_util::future::join_all(entry_futures).await;

        let mut total: i64 = 0;
        let mut permanent: Vec<(&str, JobError)> = Vec::new();
        let mut retriable: Vec<String> = Vec::new();
        for (entry, result) in art.entries.iter().zip(results) {
            match result {
                Ok(bytes) => total += bytes,
                Err(error) if error.kind() == JobFailureKind::Permanent => {
                    permanent.push((&entry.role, error));
                }
                Err(error) => retriable.push(format!("{}: {}", entry.role, error.message())),
            }
        }

        let outcome = if !permanent.is_empty() {
            let reason = permanent
                .iter()
                .find_map(|(_, error)| error.failure_reason());
            let messages = permanent
                .into_iter()
                .map(|(role, error)| format!("{role}: {}", error.message()))
                .collect::<Vec<_>>()
                .join("; ");
            let message = format!("artifact entry failed permanently: {messages}");
            Err(match reason {
                Some(reason) => JobError::permanent_with_reason(reason, message),
                None => JobError::Permanent(message),
            })
        } else if !retriable.is_empty() {
            Err(JobError::Retriable(format!(
                "artifact entry failed: {}",
                retriable.join("; ")
            )))
        } else {
            Ok(total)
        };

        // Artifact-level staged-source cleanup. A per-entry success is not
        // terminal for the artifact: if a sibling failed Retriable and the job
        // will retry, the succeeded entries' staged sources must survive so the
        // next attempt can re-read them (deleting them would turn one transient
        // blip into a permanent "source file inaccessible" on every sibling).
        // Delete only when the artifact as a whole is done — success, a
        // permanent failure, or a retriable failure on the last attempt.
        let last_attempt = job.attempts >= job.max_attempts;
        let terminal = match &outcome {
            Ok(_) => true,
            Err(error) => error.kind() == JobFailureKind::Permanent || last_attempt,
        };
        if terminal {
            for entry in &art.entries {
                if let Some(staged) = &entry.staged_path {
                    remove_staged_best_effort(staged, &job.job_id).await;
                }
            }
        }

        match &outcome {
            Ok(bytes) => tracing::info!(
                job_id = %job.job_id,
                kind = %job.kind,
                subject_id = %art.subject_id,
                artifact_id = %art.artifact_id,
                bytes,
                "artifact job completed"
            ),
            Err(err) => tracing::warn!(
                job_id = %job.job_id,
                kind = %job.kind,
                subject_id = %art.subject_id,
                artifact_id = %art.artifact_id,
                error_kind = err.kind().as_str(),
                error = %err.message(),
                "artifact job failed"
            ),
        }

        outcome
    }
}

fn common_object_parent<'a>(keys: impl Iterator<Item = &'a str>) -> Option<String> {
    let mut keys = keys;
    let mut common: Vec<&str> = keys.next()?.split('/').collect();
    common.pop()?;
    for key in keys {
        let mut parent: Vec<&str> = key.split('/').collect();
        parent.pop()?;
        let shared = common
            .iter()
            .zip(parent.iter())
            .take_while(|(left, right)| left == right)
            .count();
        common.truncate(shared);
        if common.is_empty() {
            return None;
        }
    }
    (!common.is_empty()).then(|| common.join("/"))
}

/// Buffered reader size between the source file and the zstd encoder
/// when compress is chained through the encryption stage. Sized to
/// amortise read syscalls over a large fraction of typical NVMe
/// readahead without inflating per-job memory beyond a single page-
/// cache window.
const ENCRYPT_PUSH_READ_BUF_BYTES: usize = 64 * 1024;

/// Best-effort unlink of a vol-mgr-owned staged push source once its owning
/// transfer is terminal. ENOENT is silent (already gone); any other error is
/// logged so a stuck mount or permission flip is diagnosable without failing
/// the transfer outcome. Shared by single-file ([`TransferExecutor::cleanup_staged_on_terminal`])
/// and artifact-level cleanup.
async fn remove_staged_best_effort(staged: &str, job_id: &JobId) {
    match tokio::fs::remove_file(staged).await {
        Ok(()) => tracing::debug!(
            job_id = %job_id,
            staged_path = %staged,
            "removed staged push source after terminal outcome"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(
            job_id = %job_id,
            staged_path = %staged,
            error = %e,
            "failed to remove staged push source after terminal outcome"
        ),
    }
}

/// Run the AAD derivation → DEK unwrap → (optionally zstd-encode) →
/// AES-GCM-stream encrypt sequence for a single push, writing the
/// final ciphertext to `.enc.tmp` next to `src_path` in **one pass**.
/// When `compress = true`, the bytes flow source → BufReader →
/// `zstd::stream::read::Encoder` → AES-GCM encryptor → `.enc.tmp`
/// with no intermediate `.zst.tmp` on disk. When `compress = false`,
/// the source file feeds the AEAD directly.
///
/// Returns the `.enc.tmp` sibling path on success. On failure, returns
/// the path **alongside** the error so the caller unconditionally
/// includes it in the `transient_temps` cleanup loop — a transient
/// credmgr blip mid-encrypt or an I/O hiccup may have written part of
/// the file, and skipping cleanup would orphan it under `/space/cache`
/// until the next process-restart sweep.
async fn stage_encrypted_temp(
    job: &Job,
    p: &TransferPayload,
    enc: &TransferEncryption,
    src_path: &Path,
    compress: bool,
    resolved_dek: Option<&[u8]>,
    input: TransformInput,
) -> Result<PathBuf, (PathBuf, JobError)> {
    let enc_path = encrypted_sibling(src_path);
    let src_path_buf = src_path.to_path_buf();
    let enc_path_buf = enc_path.clone();
    let subject_id = p.subject_id.clone();
    let result: Result<(), JobError> = async {
        // wrap_aad binds the DEK (IssueDek/UnwrapDek); entry_aad binds the
        // per-chunk AEAD so this file's ciphertext can't be fed into a
        // different entry's slot under the same artifact DEK.
        let (wrap_aad, entry_aad) = derive_artifact_aads(p, enc.organization_id)?;
        // The artifact path unwraps the DEK once for the whole set and passes
        // it in (`resolved_dek`), so the per-entry fan-out does not multiply
        // credmgr `UnwrapDek` / KMS `Decrypt` calls. The copy into `Zeroizing`
        // keeps the move-into-`spawn_blocking` shape and scrubs on drop.
        let dek = match resolved_dek {
            Some(d) => zeroize::Zeroizing::new(d.to_vec()),
            None => unwrap_dek(
                enc.organization_id,
                enc.provider,
                &enc.master_key_identifier,
                &enc.wrapped_dek,
                &wrap_aad,
            )
            .await
            .map_err(unwrap_error_to_job_error)?,
        };
        tracing::debug!(
            job_id = %job.job_id,
            subject_id = %p.subject_id,
            local_path = %p.local_path,
            enc_path = %enc_path.display(),
            compress,
            "compress + encrypt streaming artifact for upload"
        );
        let job_id = job.job_id;
        tokio::task::spawn_blocking(move || -> Result<(), JobError> {
            let _codec_permit = input.codec_permit;
            let src_file = std::fs::File::open(&src_path_buf)
                .map_err(|e| JobError::Permanent(format!("open source for encrypt: {e}")))?;
            let buf_reader = std::io::BufReader::with_capacity(
                ENCRYPT_PUSH_READ_BUF_BYTES,
                DigestReader::new(src_file, input.digest),
            );
            let mut dst_file = std::fs::File::create(&enc_path_buf)
                .map_err(|e| JobError::Permanent(format!("create enc temp: {e}")))?;
            if compress {
                let mut zstd_reader = zstd::stream::read::Encoder::new(buf_reader, ZSTD_LEVEL)
                    .map_err(|e| JobError::Permanent(format!("zstd encoder init: {e}")))?;
                crypto::encrypt_stream_blocking(&mut zstd_reader, &mut dst_file, &dek, &entry_aad)
                    .map_err(crypto_error_to_job_error)?;
            } else {
                let mut reader = buf_reader;
                crypto::encrypt_stream_blocking(&mut reader, &mut dst_file, &dek, &entry_aad)
                    .map_err(crypto_error_to_job_error)?;
            }
            page_cache::advise_path_dontneed(&enc_path_buf, "encrypted volume upload temp");
            tracing::debug!(
                job_id = %job_id,
                subject_id,
                enc_path = %enc_path_buf.display(),
                compress,
                "compress + encrypt finished"
            );
            Ok(())
        })
        .await
        .map_err(|e| JobError::Permanent(format!("encrypt task panicked: {e}")))??;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(enc_path),
        Err(err) => Err((enc_path, err)),
    }
}

struct PushReadContext<'a> {
    resolved_dek: Option<&'a [u8]>,
    cancellation: CancellationToken,
    digest: Option<DigestSink>,
}

struct PushExecutionOptions<'a> {
    cleanup_staged: bool,
    resolved_dek: Option<&'a [u8]>,
    cancellation: CancellationToken,
}

impl TransferExecutor {
    async fn execute_push(
        &self,
        job: &Job,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        options: PushExecutionOptions<'_>,
    ) -> Result<i64, JobError> {
        let artifact = p.effective_snapshot_artifact();
        let digest = self
            .logical_digests
            .as_ref()
            .and_then(|_| artifact.map(DigestSink::for_artifact));
        let mut result = self
            .execute_push_inner(
                job,
                p,
                creds,
                target,
                PushReadContext {
                    resolved_dek: options.resolved_dek,
                    cancellation: options.cancellation,
                    digest: digest.clone(),
                },
            )
            .await;
        if let (Ok(bytes), Some(sink), Some(artifact), Some(collection)) =
            (&result, digest, artifact, &self.logical_digests)
        {
            result = sink
                .completed()
                .and_then(|(digest, size_bytes, metadata_binding_digest)| {
                    if i64::try_from(size_bytes).ok() != Some(*bytes) {
                        return Err(JobError::retriable(
                            "logical digest size differs from uploaded source size",
                        ));
                    }
                    collection
                        .lock()
                        .map_err(|_| {
                            JobError::permanent("logical digest collection lock poisoned")
                        })?
                        .push(LogicalArtifactDigest {
                            artifact,
                            digest,
                            size_bytes,
                            metadata_binding_digest,
                        });
                    Ok(*bytes)
                });
        }
        // Single-file pushes clean their staged source here. Artifact entries
        // pass `cleanup_staged = false`: a per-entry success is NOT terminal
        // for the artifact (a sibling may force a retry that must re-read this
        // entry's staged bytes), so `execute_artifact` runs one artifact-level
        // cleanup after the whole fan-out instead.
        if options.cleanup_staged {
            self.cleanup_staged_on_terminal(job, p, &result).await;
        }
        result
    }

    async fn execute_push_inner(
        &self,
        job: &Job,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        read: PushReadContext<'_>,
    ) -> Result<i64, JobError> {
        let PushReadContext {
            resolved_dek,
            cancellation,
            digest,
        } = read;
        // Prefer the staged copy when set: bytes there are immune to a later
        // unlink of the caller's original path between submit and execute.
        // Legacy jobs queued before the staging field existed fall back to
        // `local_path` unchanged.
        let upload_source = p.upload_source();
        let upload_path = Path::new(upload_source);

        // Verify the source file is accessible before attempting any upload.
        // NotFound and PermissionDenied are permanent — retrying won't help.
        match tokio::fs::metadata(upload_path).await {
            Ok(_) => {}
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound
                    || e.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return Err(JobError::Permanent(format!(
                    "source file inaccessible ({}): {}",
                    e.kind(),
                    upload_source,
                )));
            }
            Err(e) => {
                return Err(JobError::Retriable(format!(
                    "source file stat failed: {upload_source}: {e}",
                )));
            }
        }

        // The job queue marks jobs `completed` only after `execute` returns Ok,
        // so a job that reaches `execute` is one we have not yet finished. We
        // cannot tell whether bytes at `target.key` are ours, a stray previous
        // upload, or a different sandbox writing to the same prefix — so we
        // unconditionally re-upload. S3 put is idempotent (overwrites), and
        // the typical retry path bounces on a transient error mid-upload, where
        // re-uploading is the correct action anyway.

        // Source-size accounting: pre-compression, pre-encryption bytes.
        // Callers report what they pushed, not the wire-encoded size.
        let original_size = tokio::fs::metadata(upload_path)
            .await
            .map(|m| m.len())
            .map_err(|e| JobError::Permanent(format!("stat source file for size: {e}")))?;

        if let Some(config) = &self.stream_push
            && self.config.compress
            && let Some(options) = config.options(p, original_size)
            && let Some(uploader) = self.store.streaming_upload()
        {
            return self
                .execute_stream_push(
                    job,
                    p,
                    creds,
                    target,
                    stream_push::StreamPushAttempt {
                        uploader,
                        config,
                        options,
                        original_size,
                        cancellation,
                        digest,
                    },
                )
                .await;
        }

        // Build the upload pipeline: source → [zstd?] → [AES-GCM?] → S3.
        //
        // On the encrypted path the two transforms run back-to-back inside
        // one `spawn_blocking`: the source file feeds a `BufReader` →
        // `zstd::stream::read::Encoder` whose compressed bytes are read by
        // the AEAD encryptor that writes directly to `.enc.tmp`. No
        // intermediate `.zst.tmp` ever exists on disk — saving one full
        // compressed-bytes write + read per artifact (gigabytes per upload
        // for large artifacts).
        //
        // Ineligible sources and stores without streaming support retain the
        // file-backed transform and upload path.
        let mut transient_temps: Vec<PathBuf> = Vec::new();
        let mut raw_digest_temp = None;

        let put_source: PathBuf = match &p.encryption {
            None => {
                if self.config.compress {
                    let artifact = transfer_artifact_tag(p.effective_snapshot_artifact());
                    let codec_wait_started_at = Instant::now();
                    let codec_permit = self
                        .acquire_codec_permit_for_input(original_size, job.priority)
                        .await?;
                    if codec_permit.is_some() {
                        emit_transfer_phase_duration(
                            &job.kind,
                            artifact,
                            TransferPhase::CompressWait,
                            MaterializationMode::Disabled,
                            codec_wait_started_at,
                        );
                    }
                    let (zst_path, _) = compress_to_temp(
                        upload_path,
                        TransformInput {
                            codec_permit,
                            digest: digest.clone(),
                        },
                    )
                    .await?;
                    tracing::debug!(
                        job_id = %job.job_id,
                        subject_id = %p.subject_id,
                        local_path = %p.local_path,
                        tmp_path = %zst_path.display(),
                        original_bytes = original_size,
                        "compressed artifact for upload"
                    );
                    transient_temps.push(zst_path.clone());
                    zst_path
                } else {
                    if let Some(sink) = digest.clone() {
                        // The raw path has no codec reader to wrap. Stream into
                        // a private temporary upload object while hashing; only
                        // that exact owned copy is handed to the object store.
                        let source = upload_path.to_owned();
                        let temp = tokio::task::spawn_blocking(move || {
                            let file = std::fs::File::open(&source)?;
                            let mut reader = DigestReader::new(file, Some(sink));
                            let mut temp =
                                tempfile::NamedTempFile::new_in(source.parent().ok_or_else(
                                    || std::io::Error::other("upload source has no parent"),
                                )?)?;
                            std::io::copy(&mut reader, &mut temp)?;
                            temp.as_file_mut().flush()?;
                            Ok::<_, std::io::Error>(temp)
                        })
                        .await
                        .map_err(|e| JobError::retriable(format!("raw upload digest task: {e}")))?
                        .map_err(|e| {
                            JobError::retriable(format!("raw upload digest source: {e}"))
                        })?;
                        let path = temp.path().to_owned();
                        raw_digest_temp = Some(temp);
                        path
                    } else {
                        upload_path.to_path_buf()
                    }
                }
            }
            Some(enc) => {
                // `stage_encrypted_temp` runs source → [zstd] → AEAD → `.enc.tmp`
                // in one pass. On failure it returns the partial `.enc.tmp`
                // path alongside the error so the cleanup loop below also
                // unlinks half-written temps from credmgr blips or mid-
                // encrypt I/O errors.
                //
                // Admit the zstd pass to the executor-wide codec limit when
                // compression runs; encryption-only pushes are AEAD-bound
                // (multi-GiB/s) and left unadmitted.
                let artifact = transfer_artifact_tag(p.effective_snapshot_artifact());
                let codec_permit = if self.config.compress {
                    let codec_wait_started_at = Instant::now();
                    let permit = self
                        .acquire_codec_permit_for_input(original_size, job.priority)
                        .await?;
                    if permit.is_some() {
                        emit_transfer_phase_duration(
                            &job.kind,
                            artifact,
                            TransferPhase::CompressWait,
                            MaterializationMode::Disabled,
                            codec_wait_started_at,
                        );
                    }
                    permit
                } else {
                    None
                };
                match stage_encrypted_temp(
                    job,
                    p,
                    enc,
                    upload_path,
                    self.config.compress,
                    resolved_dek,
                    TransformInput {
                        codec_permit,
                        digest,
                    },
                )
                .await
                {
                    Ok(enc_path) => {
                        transient_temps.push(enc_path.clone());
                        enc_path
                    }
                    Err((partial_enc_path, err)) => {
                        transient_temps.push(partial_enc_path);
                        for tmp in &transient_temps {
                            unlink_crypto_tmp(
                                tmp,
                                &job.job_id,
                                &p.subject_id,
                                "encrypt_stage_error_cleanup",
                            )
                            .await;
                        }
                        return Err(err);
                    }
                }
            }
        };

        // The exact post-compress / post-encrypt wire length we hand to the
        // store. This is what S3 must durably hold for the object to be
        // complete (for the no-transform path it equals `original_size`).
        // Computed into `upload_result` rather than `?`-returned so the
        // transient-temp cleanup below still runs on a stat failure.
        let upload_result = match tokio::fs::metadata(&put_source).await {
            Err(e) => Err(JobError::Retriable(format!(
                "stat upload payload {}: {e}",
                put_source.display()
            ))),
            Ok(meta) => {
                let wire_size = meta.len();
                use pplx_space_metrics::volume_manager::{
                    PushPayloadEncoding, PushPayloadPhase, push_payload,
                };
                let artifact = transfer_artifact_tag(p.effective_snapshot_artifact());
                let encoding = match (self.config.compress, p.encryption.is_some()) {
                    (false, false) => PushPayloadEncoding::Raw,
                    (true, false) => PushPayloadEncoding::Zstd,
                    (false, true) => PushPayloadEncoding::Aead,
                    (true, true) => PushPayloadEncoding::ZstdAead,
                };
                push_payload(
                    artifact,
                    encoding,
                    PushPayloadPhase::Prepared,
                    original_size,
                    wire_size,
                );
                match self.store.put_file(creds, target, &put_source).await {
                    // A successful `put_file` is not sufficient: a truncated /
                    // partial multipart upload can still return `Ok` (e.g. a
                    // completion that races an S3 `RequestTimeout`). Re-read the
                    // durable object and confirm it landed at the full wire size
                    // before reporting the transfer complete. A short or missing
                    // object is `Retriable` so the upload redrives; once the job
                    // exhausts its attempts the snapshot is finalized `failed`
                    // rather than `ready`, so the restore marker never advances
                    // to a corrupt artifact.
                    Ok(_) => match self.store.head(creds, target).await {
                        Ok(Some(durable)) if durable.size == wire_size => {
                            push_payload(
                                artifact,
                                encoding,
                                PushPayloadPhase::Verified,
                                original_size,
                                wire_size,
                            );
                            bytes_len_to_job_i64(original_size, "upload source size")
                        }
                        Ok(Some(durable)) => Err(JobError::Retriable(format!(
                            "uploaded object truncated: {} holds {} durable bytes, expected {wire_size}",
                            target.key, durable.size,
                        ))),
                        Ok(None) => Err(JobError::Retriable(format!(
                            "uploaded object missing after put: {}",
                            target.key
                        ))),
                        Err(e) => Err(classify(&e)),
                    },
                    Err(e) => Err(classify(&e)),
                }
            }
        };

        // Hint the kernel to drop the page cache for the file that just
        // went up the wire — same intent as before the BYOK refactor; now
        // applied uniformly to whichever staging file actually got read
        // (the staged source, the compressed temp, or the encrypted temp).
        if upload_result.is_ok() {
            page_cache::advise_path_dontneed(&put_source, "upload volume input");
        }

        // Best-effort cleanup of every transient artifact. Failures here
        // don't affect the upload outcome but should be diagnosable.
        for tmp in &transient_temps {
            unlink_crypto_tmp(tmp, &job.job_id, &p.subject_id, "post_upload_cleanup").await;
        }

        drop(raw_digest_temp);
        upload_result
    }

    /// Remove the staged source file once the upload has reached a state
    /// from which it will not be re-tried. A Retriable error on an attempt
    /// that still has budget left keeps the file around so the next try
    /// reads the same bytes; the last-attempt case (or a Permanent /
    /// success) removes it. The startup [`cleanup_orphans`] sweep is the
    /// safety net for process crashes between these branches.
    ///
    /// [`cleanup_orphans`]: crate::staging::cleanup_orphans
    async fn cleanup_staged_on_terminal(
        &self,
        job: &Job,
        p: &TransferPayload,
        result: &Result<i64, JobError>,
    ) {
        let Some(ref staged) = p.staged_path else {
            return;
        };
        let last_attempt = job.attempts >= job.max_attempts;
        let terminal = match result {
            Ok(_) => true,
            Err(error) => error.kind() == JobFailureKind::Permanent || last_attempt,
        };
        if !terminal {
            return;
        }
        remove_staged_best_effort(staged, &job.job_id).await;
    }

    async fn head_required(
        &self,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
    ) -> Result<ObjectMeta, JobError> {
        match self.store.head(creds, target).await {
            Ok(Some(meta)) => Ok(meta),
            Ok(None) => Err(JobError::permanent_with_reason(
                JobFailureReason::NotFound,
                format!("source object not found: {}", p.s3_uri),
            )),
            Err(e) => Err(classify(&e)),
        }
    }

    async fn confirm_source_meta_unchanged(
        &self,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        expected: &ObjectMeta,
    ) -> Result<ObjectMeta, JobError> {
        let current = self.head_required(p, creds, target).await?;
        if &current == expected {
            Ok(current)
        } else {
            Err(JobError::Retriable(format!(
                "source object changed during download: {}",
                p.s3_uri
            )))
        }
    }

    async fn validate_existing_file_replica(
        &self,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        local_path: &Path,
        requested_materialization: MaterializationMode,
    ) -> Result<Option<u64>, JobError> {
        let local_size = match tokio::fs::metadata(local_path).await {
            Ok(meta) if meta.is_file() => meta.len(),
            Ok(_) => return Ok(None),
            Err(_) => return Ok(None),
        };

        if !p.raw && is_zstd(local_path).await {
            return Ok(None);
        }

        let provenance_path = verified_replica_provenance_path(local_path);
        let provenance_bytes = match tokio::fs::read(&provenance_path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Ok(None),
        };
        let provenance = match serde_json::from_slice::<FileReplicaProvenance>(&provenance_bytes) {
            Ok(provenance) => provenance,
            Err(_) => return Ok(None),
        };

        let Some(object_meta) = self
            .store
            .head(creds, target)
            .await
            .map_err(|e| classify(&e))?
        else {
            return Ok(None);
        };

        // Sparse LUKS is a write optimization, not a new replica contract.
        // Keep verified dense backings from older downloads: fetching them
        // again only to punch holes would add disk and network work on retry.
        let requested_materialization = if matches!(
            p.effective_snapshot_artifact(),
            Some(SnapshotArtifact::EncryptedSnapshotVolume | SnapshotArtifact::EncryptedRootfsCow)
        ) {
            MaterializationMode::Disabled
        } else {
            requested_materialization
        };
        if provenance.replica == file_replica_provenance(p, &object_meta, local_size)
            && materialization_is_compatible(
                requested_materialization,
                provenance.materialization.as_ref(),
            )
        {
            Ok(Some(local_size))
        } else {
            Ok(None)
        }
    }

    async fn write_file_replica_provenance(
        &self,
        job: &Job,
        p: &TransferPayload,
        local_path: &Path,
        object_meta: &ObjectMeta,
        local_size: u64,
        materialization: Option<MaterializationObservation>,
    ) -> Result<(), JobError> {
        let provenance_path = verified_replica_provenance_path(local_path);
        let mut tmp_path = provenance_path.clone();
        let mut tmp_name = provenance_path
            .file_name()
            .map(|name| name.to_os_string())
            .unwrap_or_else(|| OsString::from(".volmgr-replica.json"));
        tmp_name.push(format!(".tmp-{}", job.job_id));
        tmp_path.set_file_name(tmp_name);

        let provenance = FileReplicaProvenance::new(p, object_meta, local_size, materialization);
        let bytes = serde_json::to_vec(&provenance)
            .map_err(|e| JobError::Permanent(format!("serialize file replica provenance: {e}")))?;
        tokio::fs::write(&tmp_path, bytes)
            .await
            .map_err(|e| JobError::Permanent(format!("write file replica provenance: {e}")))?;
        tokio::fs::rename(&tmp_path, &provenance_path)
            .await
            .map_err(|e| JobError::Permanent(format!("promote file replica provenance: {e}")))?;
        Ok(())
    }

    async fn invalidate_file_replica_provenance(
        &self,
        local_path: &Path,
        reason: &'static str,
    ) -> Result<(), JobError> {
        let provenance_path = verified_replica_provenance_path(local_path);
        match tokio::fs::remove_file(&provenance_path).await {
            Ok(()) => {
                tracing::debug!(
                    path = %provenance_path.display(),
                    reason,
                    "invalidated file replica provenance"
                );
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(JobError::Retriable(format!(
                "remove file replica provenance {} before {reason}: {err}",
                provenance_path.display()
            ))),
        }
    }

    async fn lock_pull_destination(&self, local_path: &Path) -> DestinationPullGuard {
        let path = local_path.to_path_buf();
        let lock = self
            .destination_pull_locks
            .entry(path.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let guard = lock.lock_owned().await;
        DestinationPullGuard {
            locks: Arc::clone(&self.destination_pull_locks),
            path,
            guard: Some(guard),
        }
    }

    async fn cleanup_cancelled_pull_path(&self, local_path: &Path) -> Result<(), JobError> {
        let _destination_guard = self.lock_pull_destination(local_path).await;
        cleanup_cancelled_pull_temp_paths(local_path).await
    }

    async fn execute_pull_cancellable(
        &self,
        job: &Job,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        options: PullExecutionOptions<'_>,
    ) -> Result<i64, JobError> {
        let local_path = Path::new(&p.local_path);
        let _destination_guard = self.lock_pull_destination(local_path).await;
        self.execute_pull_locked(job, p, creds, target, options)
            .await
    }

    #[cfg(test)]
    async fn execute_pull(
        &self,
        job: &Job,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        resolved_dek: Option<&[u8]>,
        existing_policy: PullExistingPolicy,
    ) -> Result<i64, JobError> {
        self.execute_pull_cancellable(
            job,
            p,
            creds,
            target,
            PullExecutionOptions {
                resolved_dek,
                existing_policy,
                cancellation: CancellationToken::new(),
            },
        )
        .await
    }

    async fn execute_pull_locked(
        &self,
        job: &Job,
        p: &TransferPayload,
        creds: &pplx_space_object_store::ObjectCredentials,
        target: ObjectTarget<'_>,
        options: PullExecutionOptions<'_>,
    ) -> Result<i64, JobError> {
        let PullExecutionOptions {
            resolved_dek,
            existing_policy,
            cancellation,
        } = options;
        let local_path = Path::new(&p.local_path);
        let snapshot_artifact = p.effective_snapshot_artifact();
        let execution_materialization_mode = execution_materialization_mode(
            snapshot_artifact,
            p.requested_materialization_mode(snapshot_artifact),
        );
        let artifact = transfer_artifact_tag(snapshot_artifact);
        let tmp_path = partial_path(local_path);

        let source_meta = match existing_policy {
            PullExistingPolicy::LegacySkipPresent => {
                // Pre-check: if the local file already exists, looks the way
                // our raw/non-raw contract says it should, and S3 still has
                // the source object, we assume a previous download completed.
                //
                // Non-raw mode: the on-disk shape is the decompressed form, so
                // a zstd-magic leftover means a previous attempt died between
                // `get_file` and `decompress_in_place`. Fall through and
                // re-download.
                let half_decompressed = !p.raw && is_zstd(local_path).await;
                if tokio::fs::metadata(local_path).await.is_ok()
                    && !half_decompressed
                    && let Ok(Some(_)) = self.store.head(creds, target).await
                {
                    tracing::info!(
                        job_id = %job.job_id,
                        subject_id = %p.subject_id,
                        s3_uri = %p.s3_uri,
                        local_path = %p.local_path,
                        "pull skipped: local file already present"
                    );
                    // Sweep any leftover .partial from an earlier aborted run
                    // so it doesn't accumulate.
                    unlink_partial(&tmp_path, "skip_present").await;
                    apply_file_mode(local_path, p.file_mode, FileModeApplyPolicy::ExistingFile)
                        .await?;
                    let size = tokio::fs::metadata(local_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);
                    return bytes_len_to_job_i64(size, "existing pull output size");
                }
                None
            }
            PullExistingPolicy::VerifiedReplica => {
                if let Some(size) = self
                    .validate_existing_file_replica(
                        p,
                        creds,
                        target,
                        local_path,
                        execution_materialization_mode,
                    )
                    .await?
                {
                    tracing::info!(
                        job_id = %job.job_id,
                        subject_id = %p.subject_id,
                        s3_uri = %p.s3_uri,
                        local_path = %p.local_path,
                        "file replica already present"
                    );
                    unlink_partial(&tmp_path, "verified_replica").await;
                    apply_file_mode(local_path, p.file_mode, FileModeApplyPolicy::ExistingFile)
                        .await?;
                    return bytes_len_to_job_i64(size, "existing file replica size");
                }
                Some(self.head_required(p, creds, target).await?)
            }
        };

        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                JobError::permanent_with_reason(
                    JobFailureReason::Internal,
                    format!("create destination dir: {e}"),
                )
            })?;
        }

        // Clear any stale .partial left by a previous aborted attempt before
        // streaming new bytes into it.
        unlink_partial(&tmp_path, "pre_download").await;

        // Stream into the sibling .partial path; rename to the caller-visible
        // local_path only on success so callers never observe a torn file.
        //
        // Plain compressed pulls decode inline with the ranged download when
        // enabled: the reorder window already delivers sequential bytes, so
        // routing them through the zstd decoder on the way to disk makes the
        // job ≈ max(download, decode) instead of download + decode, and
        // skips writing + re-reading the compressed intermediate entirely.
        // Encrypted pulls keep their own chained decrypt+decode pass, and
        // raw pulls must land wire bytes untouched.
        let download_started_at = Instant::now();
        let stream_decode = self.config.stream_decode_pulls && p.encryption.is_none() && !p.raw;
        let stream_pull_timing = Arc::new(StreamPullTiming::default());
        let (bytes, streamed, streamed_decoded, streamed_decompression_report) = if stream_decode {
            let admission = StreamedPullAdmission {
                executor: self,
                kind: &job.kind,
                artifact,
                mode: execution_materialization_mode,
                priority: job.priority,
            };
            let mode_if_decompressed =
                effective_materialization_mode(snapshot_artifact, execution_materialization_mode);
            let block_bytes = materialization_block_bytes(snapshot_artifact);
            let candidate_observer = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let observer_for_sink = Arc::clone(&candidate_observer);
            let writeback_coordinator = Arc::clone(&self.stream_writeback);
            let timing_for_sink = Arc::clone(&stream_pull_timing);
            let stream_priority = job.priority;
            let wrap: &pplx_space_object_store::WrapWriter = &move |file| {
                Box::new(StreamDecodeSink::new(
                    file,
                    mode_if_decompressed,
                    block_bytes,
                    Arc::clone(&observer_for_sink),
                    Arc::clone(&writeback_coordinator),
                    Arc::clone(&timing_for_sink),
                    stream_priority,
                ))
            };
            let streamed_result = tokio::select! {
                result = self.store.get_file_streamed(
                    creds,
                    target,
                    &tmp_path,
                    pplx_space_object_store::StreamDecode {
                        admission: &admission,
                        wrap_writer: wrap,
                    },
                ) => result,
                _ = cancellation.cancelled() => {
                    unlink_partial(&tmp_path, "cancelled").await;
                    return Err(JobError::Permanent("pull cancelled".to_string()));
                }
            };
            match streamed_result {
                Ok(pplx_space_object_store::StreamedGetFile::Streamed { bytes, decoded }) => {
                    let report = decoded.then(|| DecompressionReport {
                        mode: mode_if_decompressed,
                        candidate_hole_bytes: candidate_observer
                            .load(std::sync::atomic::Ordering::Relaxed),
                    });
                    (bytes, true, decoded, report)
                }
                Ok(pplx_space_object_store::StreamedGetFile::Fallback { bytes }) => {
                    (bytes, false, false, None)
                }
                Err(e) => {
                    unlink_partial(&tmp_path, "download_failed").await;
                    return Err(classify(&e));
                }
            }
        } else {
            let download_result = tokio::select! {
                result = self.store.get_file(creds, target, &tmp_path) => result,
                _ = cancellation.cancelled() => {
                    unlink_partial(&tmp_path, "cancelled").await;
                    return Err(JobError::Permanent("pull cancelled".to_string()));
                }
            };
            match download_result {
                Ok(b) => (b, false, false, None),
                Err(e) => {
                    unlink_partial(&tmp_path, "download_failed").await;
                    return Err(classify(&e));
                }
            }
        };
        let download_wall_ms =
            u64::try_from(download_started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
        let download_phase = if streamed {
            TransferPhase::DownloadDecode
        } else {
            TransferPhase::Download
        };
        emit_transfer_phase_duration(
            &job.kind,
            artifact,
            download_phase,
            execution_materialization_mode,
            download_started_at,
        );
        pplx_space_metrics::volume_manager::transfer_bytes(
            &job.kind,
            artifact,
            "downloaded",
            bytes,
        );
        emit_transfer_throughput_mib_s(
            &job.kind,
            artifact,
            download_phase,
            execution_materialization_mode,
            bytes,
            download_started_at,
        );
        if streamed {
            log_streamed_pull_pipeline_timing(
                job,
                p,
                &stream_pull_timing,
                streamed_decoded,
                download_wall_ms,
            );
        }

        // Decrypt + maybe-decompress in one pass when the request
        // carries envelope-encryption. `.partial` holds the ciphertext
        // we just downloaded; `stage_decrypted_temp` decrypts it
        // streaming into a `MaybeZstdDecodingWriter` that peeks the
        // first four plaintext bytes for the zstd magic and either
        // pipes the rest through `zstd::stream::write::Decoder` or
        // dumps it directly to `.plaintext.tmp`. No intermediate
        // decrypted-but-still-compressed file on disk for the common
        // BYOK + compressed case.
        //
        // After this block, `tmp_path` (the `.partial`) is gone and
        // the bytes live at `plaintext_tmp` ready to be promoted to
        // `local_path`. The variable `chained_decompressed` records
        // whether the chain already ran the zstd decode so the
        // separate `decompress_in_place` step further below can skip
        // when it would otherwise be a no-op.
        let mut chained_decompressed = false;
        let mut decompression_report = streamed_decompression_report.unwrap_or_default();
        if let Some(enc) = &p.encryption {
            let decompress_if_zstd = !p.raw;
            // The chained decode runs inside the decrypt pass, so admit it
            // to the codec limit up front. The frame size can't be read
            // through ciphertext, so this admits conservatively on the
            // compressed-input floor whenever a decode will run.
            let codec_permit = if decompress_if_zstd {
                let codec_wait_started_at = Instant::now();
                let permit = self
                    .acquire_codec_permit_for_decode(&tmp_path, job.priority)
                    .await?;
                if permit.is_some() {
                    emit_transfer_phase_duration(
                        &job.kind,
                        artifact,
                        TransferPhase::DecryptWait,
                        execution_materialization_mode,
                        codec_wait_started_at,
                    );
                }
                permit
            } else {
                None
            };
            let decrypt_started_at = Instant::now();
            match stage_decrypted_temp(
                job,
                p,
                enc,
                &tmp_path,
                DecryptedPullOptions {
                    decompress_if_zstd,
                    materialization_mode: execution_materialization_mode,
                },
                resolved_dek,
                codec_permit,
            )
            .await
            {
                Ok((plaintext_tmp, was_decompressed, report)) => {
                    chained_decompressed = was_decompressed;
                    decompression_report = report;
                    let phase = if was_decompressed {
                        TransferPhase::DecryptDecompress
                    } else {
                        TransferPhase::Decrypt
                    };
                    emit_transfer_phase_duration(
                        &job.kind,
                        artifact,
                        phase,
                        execution_materialization_mode,
                        decrypt_started_at,
                    );
                    let plaintext_size = tokio::fs::metadata(&plaintext_tmp)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(bytes);
                    pplx_space_metrics::volume_manager::transfer_bytes(
                        &job.kind,
                        artifact,
                        if was_decompressed {
                            "decompressed"
                        } else {
                            "decrypted"
                        },
                        plaintext_size,
                    );
                    emit_transfer_throughput_mib_s(
                        &job.kind,
                        artifact,
                        phase,
                        execution_materialization_mode,
                        plaintext_size,
                        decrypt_started_at,
                    );
                    if was_decompressed && bytes > 0 {
                        pplx_space_metrics::volume_manager::transfer_inflation_ratio(
                            &job.kind,
                            artifact,
                            plaintext_size as f64 / bytes as f64,
                        );
                    }
                    // Replace `.partial` with the plaintext temp so the
                    // rename below targets a single canonical path
                    // regardless of whether encryption ran.
                    unlink_partial(&tmp_path, "post_decrypt").await;
                    if let Err(e) = tokio::fs::rename(&plaintext_tmp, &tmp_path).await {
                        unlink_crypto_tmp(
                            &plaintext_tmp,
                            &job.job_id,
                            &p.subject_id,
                            "rename_plaintext_failed",
                        )
                        .await;
                        return Err(JobError::permanent_with_reason(
                            JobFailureReason::Internal,
                            format!("rename .plaintext.tmp -> .partial: {e}"),
                        ));
                    }
                }
                Err((partial_plaintext, err)) => {
                    unlink_partial(&tmp_path, "decrypt_failed").await;
                    unlink_crypto_tmp(
                        &partial_plaintext,
                        &job.job_id,
                        &p.subject_id,
                        "decrypt_failed",
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        let promote_started_at = Instant::now();
        if let Err(err) = self
            .invalidate_file_replica_provenance(local_path, "promote pull output")
            .await
        {
            unlink_partial(&tmp_path, "provenance_invalidate_failed").await;
            return Err(err);
        }

        if let Err(e) = tokio::fs::rename(&tmp_path, local_path).await {
            unlink_partial(&tmp_path, "rename_failed").await;
            return Err(JobError::permanent_with_reason(
                JobFailureReason::Internal,
                format!("rename .partial -> final: {e}"),
            ));
        }
        emit_transfer_phase_duration(
            &job.kind,
            artifact,
            TransferPhase::Promote,
            execution_materialization_mode,
            promote_started_at,
        );
        tracing::debug!(
            job_id = %job.job_id,
            subject_id = %p.subject_id,
            from = %tmp_path.display(),
            to = %p.local_path,
            bytes,
            chained_decompressed,
            "renamed .partial to final path"
        );

        // The chained decrypt + maybe-decompress above already routes
        // an encrypted-and-compressed object through the zstd decoder
        // when applicable, so the standalone decompress step only
        // matters for non-encrypted-but-compressed pulls.
        // A streamed download already made the decode decision in-flight
        // (decode or passthrough) — never re-sniff its output: decoded
        // content that happens to start with the zstd magic must not be
        // decoded a second time.
        let file_is_zstd = !chained_decompressed && !streamed && is_zstd(local_path).await;
        if p.should_transparent_decompress(file_is_zstd) {
            pplx_space_metrics::volume_manager::transfer_bytes(
                &job.kind,
                artifact,
                "compressed",
                bytes,
            );
            tracing::debug!(
                job_id = %job.job_id,
                subject_id = %p.subject_id,
                local_path = %p.local_path,
                compressed_bytes = bytes,
                "decompressing downloaded artifact"
            );
            let codec_wait_started_at = Instant::now();
            let codec_permit = self
                .acquire_codec_permit_for_decode(local_path, job.priority)
                .await?;
            if codec_permit.is_some() {
                emit_transfer_phase_duration(
                    &job.kind,
                    artifact,
                    TransferPhase::DecompressWait,
                    execution_materialization_mode,
                    codec_wait_started_at,
                );
            }
            let decompress_started_at = Instant::now();
            let report = decompress_in_place(
                local_path,
                snapshot_artifact,
                execution_materialization_mode,
                codec_permit,
            )
            .await?;
            apply_file_mode(local_path, p.file_mode, FileModeApplyPolicy::Required).await?;
            let decompressed = tokio::fs::metadata(local_path)
                .await
                .map(|m| m.len())
                .unwrap_or(bytes);
            emit_transfer_phase_duration(
                &job.kind,
                artifact,
                TransferPhase::Decompress,
                execution_materialization_mode,
                decompress_started_at,
            );
            pplx_space_metrics::volume_manager::transfer_bytes(
                &job.kind,
                artifact,
                "decompressed",
                decompressed,
            );
            emit_transfer_throughput_mib_s(
                &job.kind,
                artifact,
                TransferPhase::Decompress,
                execution_materialization_mode,
                decompressed,
                decompress_started_at,
            );
            if bytes > 0 {
                pplx_space_metrics::volume_manager::transfer_inflation_ratio(
                    &job.kind,
                    artifact,
                    decompressed as f64 / bytes as f64,
                );
            }
            tracing::debug!(
                job_id = %job.job_id,
                subject_id = %p.subject_id,
                local_path = %p.local_path,
                decompressed_bytes = decompressed,
                "artifact decompression complete"
            );
            let materialization_mode = report.mode;
            let materialization = {
                #[cfg(unix)]
                {
                    observe_decompression_allocation(
                        &job.kind,
                        artifact,
                        &job.job_id,
                        &p.subject_id,
                        local_path,
                        report,
                    )
                    .await
                }
                #[cfg(not(unix))]
                {
                    None
                }
            };
            if let Some(object_meta) = source_meta.as_ref() {
                let object_meta = self
                    .confirm_source_meta_unchanged(p, creds, target, object_meta)
                    .await?;
                self.write_file_replica_provenance(
                    job,
                    p,
                    local_path,
                    &object_meta,
                    decompressed,
                    materialization,
                )
                .await?;
            }
            drop(
                self.schedule_sparse_shadow_comparison(
                    &job.job_id,
                    &job.kind,
                    &p.subject_id,
                    local_path,
                    snapshot_artifact,
                    materialization_mode,
                )
                .await,
            );
            return bytes_len_to_job_i64(decompressed, "decompressed pull output size");
        }

        if streamed_decoded {
            // The ranged download decoded inline; account the compressed and
            // decoded sizes the way the legacy decompress pass would have.
            pplx_space_metrics::volume_manager::transfer_bytes(
                &job.kind,
                artifact,
                "compressed",
                bytes,
            );
            let decompressed = tokio::fs::metadata(local_path)
                .await
                .map(|m| m.len())
                .unwrap_or(bytes);
            pplx_space_metrics::volume_manager::transfer_bytes(
                &job.kind,
                artifact,
                "decompressed",
                decompressed,
            );
            if bytes > 0 {
                pplx_space_metrics::volume_manager::transfer_inflation_ratio(
                    &job.kind,
                    artifact,
                    decompressed as f64 / bytes as f64,
                );
            }
        }
        apply_file_mode(local_path, p.file_mode, FileModeApplyPolicy::Required).await?;
        page_cache::advise_path_dontneed(local_path, "download volume output");
        // For any encrypted pull — chained decrypt+decompress *or*
        // straight decrypt — the on-disk file is the decrypted plaintext
        // payload, not the downloaded ciphertext: AEAD stripped the
        // 17-byte header and a 16-byte tag per chunk during decrypt, and
        // zstd-decompression (when chained) stripped that further. Report
        // the on-disk size so `bytes_processed` and transfer metrics
        // stay in parity across the BYOK toggle. Fall back to `bytes`
        // (download/ciphertext size) only when the stat fails — a
        // missing local path here is a downstream invariant we'd rather
        // surface as metric noise than as a JobError, since the rename
        // already succeeded.
        let output_size = if chained_decompressed || streamed_decoded || p.encryption.is_some() {
            tokio::fs::metadata(local_path)
                .await
                .map(|m| m.len())
                .unwrap_or(bytes)
        } else {
            bytes
        };
        pplx_space_metrics::volume_manager::transfer_bytes(
            &job.kind,
            artifact,
            "output",
            output_size,
        );
        let materialization_mode = decompression_report.mode;
        let materialization = if chained_decompressed || streamed_decoded {
            #[cfg(unix)]
            {
                observe_decompression_allocation(
                    &job.kind,
                    artifact,
                    &job.job_id,
                    &p.subject_id,
                    local_path,
                    decompression_report,
                )
                .await
            }
            #[cfg(not(unix))]
            {
                None
            }
        } else {
            None
        };
        if let Some(object_meta) = source_meta.as_ref() {
            let object_meta = self
                .confirm_source_meta_unchanged(p, creds, target, object_meta)
                .await?;
            self.write_file_replica_provenance(
                job,
                p,
                local_path,
                &object_meta,
                output_size,
                materialization,
            )
            .await?;
        }
        if chained_decompressed || streamed_decoded {
            drop(
                self.schedule_sparse_shadow_comparison(
                    &job.job_id,
                    &job.kind,
                    &p.subject_id,
                    local_path,
                    snapshot_artifact,
                    materialization_mode,
                )
                .await,
            );
        }
        let size_context = if chained_decompressed || p.encryption.is_some() {
            "decrypted pull output size"
        } else if streamed_decoded {
            "decompressed pull output size"
        } else {
            "pull download size"
        };
        bytes_len_to_job_i64(output_size, size_context)
    }
}

#[cfg(test)]
mod tests;
