pub(crate) mod copy;
mod decode_admission;
mod stream_push;
use decode_admission::{
    CappedDecode, DecodeGate, DecodedOutputCap, copy_decode_capped, is_output_cap_exceeded,
    over_cap_or,
};
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

mod artifact_role;
mod codec_concurrency;
mod decode_sinks;
mod pull;
mod replica_provenance;
mod sparse_shadow;
mod stream_pull_timing;
mod stream_writeback;
mod transfer_metrics;
mod writeback_coordinator;

pub(crate) use artifact_role::*;
use codec_concurrency::*;
use decode_sinks::*;
pub use pull::*;
pub use replica_provenance::*;
pub use sparse_shadow::*;
use stream_pull_timing::*;
use stream_writeback::*;
use transfer_metrics::*;
pub use writeback_coordinator::*;

// zstd frame magic: first 4 bytes of any zstd-compressed stream.
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

// zstd compression level: 3 gives a good balance of speed vs. ratio for large
// binary files. Level 1 is ~2× faster with ~10%
// lower ratio; level 9 is ~4× slower with ~5% better ratio.
const ZSTD_LEVEL: i32 = 3;

// A decode whose frame pledges fewer decompressed bytes than this skips
// codec admission: it costs at most about a second of CPU, and queueing
// metadata-sized artifacts behind gigabyte decodes would add pull latency
// where today there is none. Frames with an unknown decompressed size
// (streaming zstd, all ciphertext) are never proven small, so they always
// take a permit regardless of compressed length, unless their artifact
// class bypasses admission under an output cap (see `decode_admission`).
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

type DestinationPullLocks = Arc<DashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>;

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

/// Compress `src` to a sibling `.zst.tmp` file using zstd.
/// Returns the temp path and the original (uncompressed) size.
/// The caller is responsible for deleting the temp file.
///
/// `codec_permit` (if the caller took one) is moved into the blocking
/// task and released only when the CPU pass exits, so a cancelled caller
/// cannot free the permit while the detached `spawn_blocking` keeps
/// burning CPU.
async fn compress_to_temp(
    src: &Path,
    codec_permit: Option<CodecPermit>,
) -> Result<(PathBuf, u64), JobError> {
    let original_size = tokio::fs::metadata(src)
        .await
        .map(|m| m.len())
        .map_err(|e| JobError::Permanent(format!("stat source file: {e}")))?;

    // Put the temp file next to the source to avoid cross-device copies.
    let tmp_path = src.with_extension("zst.tmp");
    let src = src.to_path_buf();
    let tmp = tmp_path.clone();

    tokio::task::spawn_blocking(move || -> Result<(), JobError> {
        let _codec_permit = codec_permit;
        let mut reader = std::fs::File::open(&src)
            .map_err(|e| JobError::Permanent(format!("open source for compression: {e}")))?;
        let mut writer = std::fs::File::create(&tmp)
            .map_err(|e| JobError::Permanent(format!("create compression temp file: {e}")))?;
        zstd::stream::copy_encode(&mut reader, &mut writer, ZSTD_LEVEL)
            .map_err(|e| JobError::Permanent(format!("zstd compress: {e}")))?;
        page_cache::advise_dontneed(&reader, &src, "compress volume upload input");
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
/// The permit in `gate` (if the caller took one) is moved into the
/// blocking task and released only when the decode exits, so a cancelled caller
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
    gate: DecodeGate,
) -> Result<CappedDecode<DecompressionReport>, JobError> {
    let (codec_permit, max_decoded_bytes) = gate.into_parts();
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

    tokio::task::spawn_blocking(move || {
        let _codec_permit = codec_permit;
        let result = (|| -> Result<CappedDecode<DecompressionReport>, JobError> {
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
                    if let CappedDecode::OverCap =
                        copy_decode_capped(&mut reader, &mut writer, max_decoded_bytes)?
                    {
                        return Ok(CappedDecode::OverCap);
                    }
                    writer.finish_with_stats().map_err(|e| {
                        JobError::Permanent(format!("finalize decompression temp: {e}"))
                    })?
                }
                MaterializationMode::Disabled | MaterializationMode::Shadow => {
                    let mut writer = writer;
                    if let CappedDecode::OverCap =
                        copy_decode_capped(&mut reader, &mut writer, max_decoded_bytes)?
                    {
                        return Ok(CappedDecode::OverCap);
                    }
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
            Ok(CappedDecode::Complete(DecompressionReport {
                mode,
                candidate_hole_bytes,
            }))
        })();
        if !matches!(result, Ok(CappedDecode::Complete(_))) {
            // The unique scratch name has no successor to truncate-and-
            // reuse it, and non-restore destinations have no sweeper —
            // a failed or over-cap decompress must reclaim its own
            // scratch or every failure strands a destination-sized orphan.
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

/// Pull-side counterpart of [`stage_encrypted_temp`]. Reads the
/// downloaded ciphertext at `ciphertext_path`, unwraps the DEK,
/// AES-256-GCM-decrypts in 1 MiB chunks, and feeds the plaintext
/// straight into a [`MaybeZstdDecodingWriter`] writing the final
/// `.plaintext.tmp` next to the input. No `.plaintext.tmp` for the
/// decrypted-but-still-compressed intermediate — when the plaintext
/// is zstd-magic and the caller asked for transparent decompression,
/// it lands decompressed on disk in one pass.
///
/// Returns the plaintext temp path and decompression report on success. A
/// [`DecodeGate::Capped`] pass that overflows its cap removes its own
/// partial output and returns [`CappedDecode::OverCap`] with the ciphertext
/// intact. On failure the partial `.plaintext.tmp` may exist — the caller is
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
    gate: DecodeGate,
) -> Result<CappedDecode<(PathBuf, bool, DecompressionReport)>, (PathBuf, JobError)> {
    let DecryptedPullOptions {
        decompress_if_zstd,
        materialization_mode: requested_mode,
    } = options;
    let (codec_permit, max_decoded_bytes) = gate.into_parts();
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
    let result: Result<CappedDecode<(bool, DecompressionReport)>, JobError> =
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
                // A passthrough stream decodes nothing, so only a decode is capped.
                .map(|sink| {
                    DecodedOutputCap::new(sink, max_decoded_bytes.filter(|_| decompressing))
                })
            };
            let mut writer = MaybeZstdDecodingWriter::with_sink_selector(
                dst_file,
                decompress_if_zstd,
                select_sink,
            );
            match crypto::decrypt_stream_blocking(&mut src_file, &mut writer, &dek, &entry_aad) {
                Ok(()) => {}
                Err(CryptoError::Io(error)) if is_output_cap_exceeded(&error) => {
                    return Ok(CappedDecode::OverCap);
                }
                Err(error) => return Err(crypto_error_to_job_error(error)),
            }
            let CappedDecode::Complete(was_decompressed) = over_cap_or(writer.finish())
                .map_err(|e| JobError::Permanent(format!("finalize plaintext writer: {e}")))?
            else {
                return Ok(CappedDecode::OverCap);
            };
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
            Ok(CappedDecode::Complete((
                was_decompressed,
                DecompressionReport {
                    mode: if was_decompressed {
                        mode_if_decompressed
                    } else {
                        MaterializationMode::Disabled
                    },
                    candidate_hole_bytes,
                },
            )))
        })
        .await
        .unwrap_or_else(|e| Err(JobError::Permanent(format!("decrypt task panicked: {e}"))));
    match result {
        Ok(CappedDecode::Complete((was_decompressed, report))) => Ok(CappedDecode::Complete((
            plaintext_tmp,
            was_decompressed,
            report,
        ))),
        Ok(CappedDecode::OverCap) => {
            unlink_crypto_tmp(
                &plaintext_tmp,
                &job.job_id,
                &p.subject_id,
                "decode_over_cap",
            )
            .await;
            Ok(CappedDecode::OverCap)
        }
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
            copy::KIND => self.execute_copy_object(job, cancellation).await,
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

    async fn cleanup_cancelled(&self, job: &Job) -> Result<(), JobError> {
        match job.kind.as_str() {
            // The object store owns multipart abort; copies create no local files.
            copy::KIND => Ok(()),
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
    codec_permit: Option<CodecPermit>,
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
            let _codec_permit = codec_permit;
            let src_file = std::fs::File::open(&src_path_buf)
                .map_err(|e| JobError::Permanent(format!("open source for encrypt: {e}")))?;
            let buf_reader =
                std::io::BufReader::with_capacity(ENCRYPT_PUSH_READ_BUF_BYTES, src_file);
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
        let result = self
            .execute_push_inner(
                job,
                p,
                creds,
                target,
                options.resolved_dek,
                options.cancellation,
            )
            .await;
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
        resolved_dek: Option<&[u8]>,
        cancellation: CancellationToken,
    ) -> Result<i64, JobError> {
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
                    let (zst_path, _) = compress_to_temp(upload_path, codec_permit).await?;
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
                    upload_path.to_path_buf()
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
                    codec_permit,
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
}

#[cfg(test)]
mod tests;
