//! S3 upload of suspend snapshot files via the volume-manager service.
//!
//! The suspend path first persists an exact-artifact intent in the existing
//! durable `"snapshot_upload"` worker. That worker uses vol-mgr's **async**
//! API (`SubmitPushFile`) and owns the pipeline across process restarts. The
//! teardown-gated suspend path also stages the transfer handles inline and
//! requires ClusterApi to accept the exact backup attempt before runtime
//! cleanup. Object upload and finalization remain asynchronous.
//! Every
//! suspend allocates a fresh `snapshot_id`
//! (UUIDv7) and writes its artifacts under a per-attempt S3 prefix
//! (`suspend_snapshots/<shard>/<sandbox_id>/<snapshot_id>/<file>`), so two
//! concurrent suspends on the same sandbox never share or overwrite bytes.
//! The worker registers the backup with ClusterApi, submits or rejoins each
//! vol-mgr job (VM artifacts and the metadata document), polls them to
//! completion, then finalizes the backup with the cluster. Restore-side readers filter for
//! `state='ready'` rows, so a partial upload is invisible until its row flips
//! to ready. Because the worker persists its intent in sbx-mgr's local
//! SQLite before those remote side effects, an sbx-mgr restart anywhere after
//! local publication is recoverable.
//!
//! # Environment variables
//!
//! | Variable                          | Required | Default     |
//! |:----------------------------------|:---------|:------------|
//! | `SPACE_SNAPSHOT_BUCKET`           | yes      | —           |
//! | `SPACE_SNAPSHOT_REGION`           | yes      | —           |
//! | `SPACE_SNAPSHOT_KEY_PREFIX`       | no       | `""`        |
//! | `SPACE_SNAPSHOT_ENDPOINT`         | no       | AWS default |
//! | `SPACE_INTERNAL_API_ENDPOINT`     | no       | —           |
//! | `SPACE_INTERNAL_TOKEN`            | no       | —           |

use pplx_space_common_core::types::JobId;

use std::path::{Path, PathBuf};

#[cfg(test)]
use pplx_space_common_core::types::snapshot_shard;
use pplx_space_common_core::types::{
    ArtifactKind, ArtifactOrigin, LocalArtifactId, SnapshotArtifact, SnapshotName,
    snapshot_object_key,
};
use pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat;
use pplx_space_volmgr_client::{VOLMGR_SOCKET, proto::PushFileRequest};

/// Resolved S3 configuration for snapshot uploads.
///
/// Built once at startup from env vars and then shared across suspend calls.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotUploadConfig {
    pub bucket: String,
    pub region: String,
    pub key_prefix: String,
    pub endpoint: Option<String>,
    pub path_style: bool,
}

impl SnapshotUploadConfig {
    pub fn same_store(&self, other: &Self) -> bool {
        self.bucket == other.bucket
            && self.region == other.region
            && self.endpoint == other.endpoint
            && self.path_style == other.path_style
    }

    /// Reads the configuration from environment variables.
    ///
    /// Returns `None` if `SPACE_SNAPSHOT_BUCKET` or `SPACE_SNAPSHOT_REGION`
    /// are absent, which disables S3 uploads without error.
    pub fn from_env() -> Option<Self> {
        let bucket = std::env::var("SPACE_SNAPSHOT_BUCKET").ok();
        let region = std::env::var("SPACE_SNAPSHOT_REGION").ok();
        let (bucket, region) = match (bucket, region) {
            (Some(bucket), Some(region)) => (bucket, region),
            (None, None) => return None,
            (bucket, _) => {
                tracing::warn!(
                    missing = if bucket.is_none() {
                        "SPACE_SNAPSHOT_BUCKET"
                    } else {
                        "SPACE_SNAPSHOT_REGION"
                    },
                    "partial snapshot upload configuration; S3 uploads stay disabled until both \
                     SPACE_SNAPSHOT_BUCKET and SPACE_SNAPSHOT_REGION are set"
                );
                return None;
            }
        };
        let key_prefix = std::env::var("SPACE_SNAPSHOT_KEY_PREFIX").unwrap_or_default();
        let endpoint = std::env::var("SPACE_SNAPSHOT_ENDPOINT").ok();
        let path_style = endpoint.is_some();
        Some(Self {
            bucket,
            region,
            key_prefix,
            endpoint,
            path_style,
        })
    }

    /// Build a config from an `s3://bucket/prefix?region=...[&endpoint=...][&path_style=...]`
    /// URI. The org-level override lives in this form on the `organizations`
    /// row and is forwarded through the substrate.
    pub fn from_uri(uri: &str) -> Result<Self, String> {
        let parsed = pplx_space_common_core::types::StorageUri::parse(uri)
            .map_err(|e| format!("invalid snapshot storage uri: {e}"))?;
        Ok(Self {
            bucket: parsed.bucket,
            region: parsed.region,
            key_prefix: parsed.key_prefix,
            endpoint: parsed.endpoint,
            path_style: parsed.path_style,
        })
    }
}

/// Metadata written to `snapshot.json` / `metadata.json` on S3. Mirrors the
/// fields stored in the `snapshots` Postgres table so the snapshot can be
/// recovered from S3 without querying the database.
///
/// The capture-time fields under `source` describe what the snapshot was
/// taken *from* and let restore validate compatibility before loading:
/// snapshots are only safe to resume against a matching runtime + rootfs +
/// kernel combination. Future restore paths may also use them to surface
/// "stale snapshot" warnings when the underlying template build advances.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotMeta {
    /// Schema version — increment when adding required fields.
    pub version: u32,
    /// Version 2 execution metadata. Version 1 snapshots derive this from
    /// the legacy flat fields in `source` at the Firecracker boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<pplx_space_common_core::types::SnapshotExecutionMetadata>,
    pub sandbox_id: String,
    /// Total logical (uncompressed) bytes of all VM artifacts, summed from
    /// the local files at submit time. vol-mgr may compress the bytes it
    /// ships to S3; this records the on-disk snapshot size, which is what
    /// restore and operators reason about.
    pub size_bytes: u64,
    pub created_at: String,
    /// Capture-time description of the VM image stack. Restore must match
    /// these to guarantee a consistent guest environment.
    pub source: SnapshotSource,
    /// Sandbox-row fields needed to round-trip an evacuating sandbox.
    /// Optional for forward compatibility with snapshots written before
    /// this field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SnapshotSandbox>,
    /// Codec marker for the rootfs artifact. `None` (or field absent on
    /// older snapshots) means the historical `rootfs.ext4` payload.
    /// `Some("btrfs-send-v1")` indicates a `btrfs send -p
    /// <template_subvol> <instance_ro_snap>` stream named
    /// `rootfs.btrfs-send`; restoring requires the same `(template_id,
    /// content_hash)` parent subvolume to be present locally on the
    /// receiving node. **Wire-stable**: the string value here is what
    /// restore-side codec dispatch keys off — never rename without a
    /// migration story for already-uploaded snapshots.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_diff_format: Option<String>,
    /// Node-local immutable source identity. Retention belongs to the enclosing
    /// local artifact; remote restores use the ordinary btrfs-send object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_send_source_id: Option<uuid::Uuid>,
    /// BYOK encrypted snapshot marker. When `true`, the artifact set is the
    /// encrypted shape — `snapshot.luks` (sealed vmstate+memory) and `cow.luks`
    /// (encrypted rootfs delta) — instead of the plaintext vmstate/memory/rootfs
    /// trio, and restore reconstructs the dm-crypt rootfs + opens the encrypted
    /// snapshot volume. Absent/false on every legacy snapshot.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub encrypted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_artifact_id: Option<LocalArtifactId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<ArtifactKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_origin: Option<ArtifactOrigin>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_snapshot_name: Option<SnapshotName>,
}

pub const SNAPSHOT_METADATA_VERSION: u32 = 2;

impl SnapshotMeta {
    pub fn execution_metadata(
        &self,
        legacy_architecture: pplx_space_common_core::types::Arch,
    ) -> Result<
        pplx_space_common_core::types::SnapshotExecutionMetadata,
        SnapshotRuntimeMetadataError,
    > {
        match self.version {
            1 => pplx_space_common_core::types::SnapshotExecutionMetadata::new(
                self.source.legacy_execution_metadata(legacy_architecture)?,
            )
            .map_err(SnapshotRuntimeMetadataError::InvalidExecution),
            SNAPSHOT_METADATA_VERSION => self
                .execution
                .clone()
                .ok_or(SnapshotRuntimeMetadataError::MissingExecution),
            version => Err(SnapshotRuntimeMetadataError::UnsupportedVersion(version)),
        }
    }
}

/// Wire-format constant for the btrfs-send rootfs codec. Stored in
/// `SnapshotMeta.rootfs_diff_format` for snapshots taken with
/// `SuspendSnapshotFormat::BtrfsSend`. The restore-side dispatch
/// matches this exact string, so changing it would orphan every
/// already-uploaded btrfs-send snapshot.
pub const ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1: &str = "btrfs-send-v1";

/// Local on-disk filename for the rootfs artifact under the snapshot
/// directory when the codec is `btrfs-send-v1`. Mirrors the S3 wire
/// name returned by [`SnapshotArtifact::RootfsBtrfsSend::file_name`].
pub const ROOTFS_BTRFS_SEND_FILENAME: &str = "rootfs.btrfs-send";

/// Local on-disk filename for the rootfs artifact under the snapshot
/// directory when the codec is `Full` — the historical shape every
/// pre-codec snapshot uses.
pub(crate) const ROOTFS_FULL_FILENAME: &str = "rootfs.ext4";

/// Local on-disk filename for the snapshot metadata sidecar under the
/// snapshot directory. Uploaded to S3 under the `metadata.json` wire name
/// (see [`SnapshotArtifact::Metadata`]), but kept as `snapshot.json`
/// locally — the name the reupload/recovery sidecar sniffers read.
pub(crate) const SNAPSHOT_METADATA_FILENAME: &str = "snapshot.json";

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotMetadataWriteError {
    #[error("stat {path} failed: {source}")]
    StatArtifact {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    ResolveRootfs(#[from] crate::retained_source::RootfsResolveError),
    #[error(
        "retained send source {source_id} in {artifact_dir} has no rootfs size from node-manager"
    )]
    UnsizedRetainedSource {
        artifact_dir: PathBuf,
        source_id: uuid::Uuid,
    },
    #[error("snapshot metadata serialization failed: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("create metadata staging directory failed: {0}")]
    CreateMetadataDir(#[source] std::io::Error),
    #[error("write snapshot.json failed: {0}")]
    WriteMetadata(#[source] std::io::Error),
}

/// Sandbox-row fields that live only in node-local sqlite (not in the
/// cluster Postgres `sandboxes` table) and so must be carried in the
/// snapshot metadata for restores on other nodes.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotSandbox {
    /// Sandbox owner identifier. Checked against the template-manager owner.
    /// in `auth_lookup` to gate template-manager privileges.
    #[serde(alias = "user")]
    pub owner: String,
    /// Whether the sandbox is enrolled in the snapshot scheduler.
    pub snapshots_enabled: bool,
    /// Guest env vars set at create time.
    #[serde(default)]
    pub process_env: std::collections::BTreeMap<String, String>,
    /// Auto-suspend / auto-stop policy.
    pub auto_lifecycle: pplx_space_common_core::types::AutoLifecyclePolicy,
}

/// Capture-time description of what a snapshot was taken from. Stored inline
/// in [`SnapshotMeta`] so restore can validate compatibility without querying
/// the cluster API or template manager.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotSource {
    /// VM runtime backend that produced the snapshot (`"fc"` for Firecracker
    /// today; future backends extend this).
    pub runtime: String,
    /// Version of the runtime binary that produced the snapshot (e.g. the
    /// Firecracker release `"1.7.0"`). Snapshot files are not portable across
    /// arbitrary runtime versions — restore must match this to a compatible
    /// runtime. Snapshots written before this field existed are assigned the
    /// initial Firecracker `1.15.1` identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_version: Option<pplx_space_common_core::types::RuntimeVersion>,
    /// Persisted VM state format produced by the runtime. Legacy snapshots
    /// without either version field are assigned Firecracker format `9.0.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_format_version: Option<pplx_space_common_core::types::StateFormatVersion>,
    /// Template the sandbox was created from.
    pub template_id: String,
    /// Pinned content hash of the template build at suspend time. Restoring
    /// against a different content hash is unsafe — the rootfs/kernel pair
    /// must match.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    /// Sized rootfs capacity in MiB at suspend time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_size_mib: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_io_max_operations_per_sec: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_io_max_bandwidth_mib_per_sec: Option<u64>,
    /// Version identifier for the rootfs image inside the template build.
    /// Distinct from `content_hash` (which pins the whole build): a single
    /// rootfs image may be reused across multiple template builds. Optional
    /// — restore falls back to `content_hash` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rootfs_version: Option<String>,
    /// Kernel image version baked into the template build (e.g. `"6.6.10"`).
    /// Optional — restore falls back to `content_hash` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel_version: Option<String>,
    /// SHA-256 identity of the guest kernel. Paired with `kernel_version`; the digest is
    /// authoritative when selecting an installed image for restore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kernel_digest: Option<String>,
}

impl SnapshotSource {
    pub fn legacy_execution_metadata(
        &self,
        architecture: pplx_space_common_core::types::Arch,
    ) -> Result<pplx_space_common_core::types::ExecutionMetadata, SnapshotRuntimeMetadataError>
    {
        use pplx_space_common_core::types::{
            ArtifactComponentKind, ArtifactComponentMetadata, ArtifactComponentRole,
            ArtifactFormat, ArtifactVersion, ExecutionMetadata, FIRECRACKER_STATE_FORMAT,
            GuestOperatingSystem, GuestPlatform, RuntimeIdentity, RuntimeKind,
            SnapshotExecutionMetadata, StateFormatIdentity, StateFormatName,
        };
        let runtime = self
            .runtime
            .parse::<RuntimeKind>()
            .map_err(SnapshotRuntimeMetadataError::Runtime)?;
        if runtime != RuntimeKind::Firecracker {
            return Err(SnapshotRuntimeMetadataError::UnsupportedLegacyRuntime(
                runtime,
            ));
        }
        let kernel = match (&self.kernel_version, &self.kernel_digest) {
            (None, None) => vec![],
            (Some(version), Some(digest)) => vec![ArtifactComponentMetadata {
                kind: ArtifactComponentKind::Kernel,
                role: Some(
                    ArtifactComponentRole::parse("boot")
                        .map_err(SnapshotRuntimeMetadataError::KernelRole)?,
                ),
                version: Some(
                    ArtifactVersion::parse(version)
                        .map_err(SnapshotRuntimeMetadataError::KernelVersion)?,
                ),
                digest: Some(
                    digest
                        .parse()
                        .map_err(SnapshotRuntimeMetadataError::KernelDigest)?,
                ),
                format: Some(
                    ArtifactFormat::parse("vmlinux")
                        .map_err(SnapshotRuntimeMetadataError::KernelFormat)?,
                ),
            }],
            _ => return Err(SnapshotRuntimeMetadataError::PartialKernel),
        };
        match (&self.runtime_version, &self.state_format_version) {
            (None, None) => Ok(ExecutionMetadata::initial_firecracker(architecture, kernel)),
            (Some(runtime_version), Some(state_format_version)) => {
                let execution = ExecutionMetadata::new(
                    RuntimeIdentity {
                        kind: runtime,
                        version: runtime_version.clone(),
                    },
                    GuestPlatform {
                        os: GuestOperatingSystem::Linux,
                        architecture,
                    },
                    Some(StateFormatIdentity {
                        format: StateFormatName::parse(FIRECRACKER_STATE_FORMAT)
                            .map_err(SnapshotRuntimeMetadataError::StateFormat)?,
                        version: state_format_version.clone(),
                    }),
                    kernel,
                )
                .map_err(SnapshotRuntimeMetadataError::InvalidExecution)?;
                SnapshotExecutionMetadata::new(execution.clone())
                    .map_err(SnapshotRuntimeMetadataError::InvalidExecution)?;
                Ok(execution)
            }
            _ => Err(SnapshotRuntimeMetadataError::PartialRuntime),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SnapshotRuntimeMetadataError {
    #[error("unsupported snapshot metadata version {0}")]
    UnsupportedVersion(u32),
    #[error("snapshot metadata version 2 requires execution metadata")]
    MissingExecution,
    #[error("legacy snapshot metadata cannot describe runtime {0}")]
    UnsupportedLegacyRuntime(pplx_space_common_core::types::RuntimeKind),
    #[error(transparent)]
    Runtime(#[from] pplx_space_common_core::types::RuntimeKindParseError),
    #[error("runtime_version and state_format_version must be populated together")]
    PartialRuntime,
    #[error(transparent)]
    InvalidExecution(#[from] pplx_space_common_core::types::ExecutionMetadataError),
    #[error(transparent)]
    KernelVersion(#[from] pplx_space_common_core::types::ArtifactVersionParseError),
    #[error(transparent)]
    KernelRole(#[from] pplx_space_common_core::types::ArtifactComponentRoleParseError),
    #[error(transparent)]
    KernelDigest(#[from] pplx_space_common_core::types::DigestParseError),
    #[error(transparent)]
    KernelFormat(#[from] pplx_space_common_core::types::ArtifactFormatParseError),
    #[error(transparent)]
    StateFormat(#[from] pplx_space_common_core::types::StateFormatNameParseError),
    #[error("kernel_version and kernel_digest must be populated together")]
    PartialKernel,
}

/// Durable upload intent and, after remote submission, transfer handles.
/// Serialized as the
/// payload of a `"snapshot_upload"` job so the durable worker in
/// [`crate::snapshot_upload_jobs`] can drive the complete remote pipeline
/// (stage/submit → begin → poll → finalize) across process restarts.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SnapshotUploadJobs {
    pub meta: SnapshotMeta,
    /// UUID identity of the snapshot row this upload is for. Generated by
    /// sbx-mgr (UUIDv7) before staging artifacts; the row is INSERTed in
    /// state `creating` on the cluster API, then UPDATEd to `ready` or
    /// `failed` by the durable worker tail.
    pub snapshot_id: String,
    pub snapshot_dir: PathBuf,
    pub config: SnapshotUploadConfig,
    /// `s3://` connection string for `snapshot_storage_locations`. Encodes
    /// the per-attempt directory (`<prefix>/suspend_snapshots/<shard>/<sandbox_id>/<snapshot_id>`).
    pub storage_uri: String,
    /// Vol-mgr job IDs for every uploaded file, in the order returned by
    /// [`upload_artifacts_for_kind`]: the VM artifacts (vmstate, memory,
    /// rootfs) followed — when [`Self::includes_metadata_job`] is set — by
    /// the `snapshot.json` metadata document. The durable worker polls all
    /// of them before finalizing `ready`.
    /// Empty means this is a prepared intent that has not reached vol-mgr.
    /// The durable worker owns remote submission for that shape.
    pub job_ids: Vec<JobId>,
    /// Temporary directory used only when a read-only disk artifact needs a
    /// writable home for generated metadata. Owned by the durable worker after
    /// `submit_suspend_snapshot` succeeds; legacy/full payloads leave it unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_staging_dir: Option<PathBuf>,
    /// Whether `job_ids` already contains the `snapshot.json` upload job.
    ///
    /// New payloads stage metadata up front and set this `true`. Payloads
    /// persisted before metadata moved onto the staged push path queued
    /// only the VM artifact jobs and uploaded `snapshot.json` later from
    /// the worker; those deserialize this as `false` (serde default).
    /// Startup rejects active jobs with this flag absent or false before
    /// applying the forward-only schema cutover. Terminal historical evidence
    /// remains readable without pretending that its metadata was staged.
    #[serde(default)]
    pub includes_metadata_job: bool,
    /// Source length is known only after upload. Submit the metadata last,
    /// after all VM jobs, and persist that handle before finalization.
    #[serde(default)]
    pub deferred_metadata: bool,
}

struct MetadataStagingDirGuard {
    path: Option<PathBuf>,
}

impl MetadataStagingDirGuard {
    fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    fn disarm(mut self) -> Option<PathBuf> {
        self.path.take()
    }
}

impl Drop for MetadataStagingDirGuard {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(error) = std::fs::remove_dir_all(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "failed to clean artifact metadata staging directory"
            );
        }
    }
}

/// Maps one local snapshot file to a vol-mgr push job. The VM-artifact
/// entries (via [`vm_artifacts_for_format`]) are exposed `pub(crate)` so
/// artifact-backup admission can drive its
/// on-disk precondition check off the same source of truth. The metadata
/// entry (appended by [`upload_artifacts_for_kind`]) is regenerated at
/// submit time rather than required on disk, so it is intentionally absent
/// from that precondition check.
#[derive(Clone)]
pub(crate) struct Artifact {
    pub(crate) kind: SnapshotArtifact,
    pub(crate) local_name: &'static str,
}

/// Snapshot-format-agnostic VM artifacts: vmstate + memory are
/// identical bytes regardless of the rootfs codec.
pub(crate) const COMMON_VM_ARTIFACTS: &[Artifact] = &[
    Artifact {
        kind: SnapshotArtifact::VmState,
        local_name: "vmstate",
    },
    Artifact {
        kind: SnapshotArtifact::Memory,
        local_name: "memory",
    },
];

/// Per-format full artifact set including the rootfs slot. Returns a
/// fresh `Vec` because the rootfs entry varies between codecs; the
/// caller iterates immediately and the allocation is cheap. Backwards
/// compatibility note: the historical `Full` codec has these exact three
/// artifact slots, and the restore path keys off the same set.
pub(crate) fn vm_artifacts_for_format(format: SuspendSnapshotFormat) -> Vec<Artifact> {
    let mut out: Vec<Artifact> = COMMON_VM_ARTIFACTS.to_vec();
    match format {
        SuspendSnapshotFormat::BtrfsSend => out.push(Artifact {
            kind: SnapshotArtifact::RootfsBtrfsSend,
            local_name: ROOTFS_BTRFS_SEND_FILENAME,
        }),
        // Unspecified resolves to Full at the suspend handler before
        // we ever build the artifact set; either is the historical
        // `rootfs.ext4` -> `rootfs` shape.
        SuspendSnapshotFormat::Unspecified | SuspendSnapshotFormat::Full => out.push(Artifact {
            kind: SnapshotArtifact::Rootfs,
            local_name: ROOTFS_FULL_FILENAME,
        }),
    }
    out
}

pub(crate) fn artifact_files_for_kind(
    artifact_kind: ArtifactKind,
    format: SuspendSnapshotFormat,
) -> Vec<Artifact> {
    match artifact_kind {
        ArtifactKind::Disk => match format {
            SuspendSnapshotFormat::BtrfsSend => vec![Artifact {
                kind: SnapshotArtifact::RootfsBtrfsSend,
                local_name: ROOTFS_BTRFS_SEND_FILENAME,
            }],
            SuspendSnapshotFormat::Unspecified | SuspendSnapshotFormat::Full => vec![Artifact {
                kind: SnapshotArtifact::Rootfs,
                local_name: ROOTFS_FULL_FILENAME,
            }],
        },
        ArtifactKind::Full => vm_artifacts_for_format(format),
    }
}

/// The full set of files uploaded for a suspend snapshot, in the order
/// [`submit_suspend_snapshot`] queues them: the per-codec VM artifacts
/// followed by the `snapshot.json` metadata document. The durable worker
/// polls *every* job in this set to a terminal state before finalizing
/// the snapshot row `ready`, so the metadata upload gates readiness
/// exactly like the VM artifacts — restore never observes a `ready` row
/// whose `metadata.json` has not landed.
pub(crate) fn upload_artifacts_for_kind(
    artifact_kind: ArtifactKind,
    format: SuspendSnapshotFormat,
) -> Vec<Artifact> {
    let mut out = artifact_files_for_kind(artifact_kind, format);
    out.push(Artifact {
        kind: SnapshotArtifact::Metadata,
        local_name: SNAPSHOT_METADATA_FILENAME,
    });
    out
}

/// Complete encrypted VM artifact set. Restore-side materialization checks use
/// these local names to decide whether an encrypted snapshot is reusable.
pub(crate) fn encrypted_vm_artifacts() -> Vec<Artifact> {
    vec![
        Artifact {
            kind: SnapshotArtifact::EncryptedSnapshotVolume,
            local_name: "snapshot.luks",
        },
        Artifact {
            kind: SnapshotArtifact::EncryptedRootfsCow,
            local_name: "cow.luks",
        },
    ]
}

/// Encrypted-snapshot upload set: the encrypted VM artifacts plus the metadata
/// document, in submit order (mirrors [`upload_artifacts_for_kind`]).
pub(crate) fn encrypted_upload_artifacts() -> Vec<Artifact> {
    let mut out = encrypted_vm_artifacts();
    out.push(Artifact {
        kind: SnapshotArtifact::Metadata,
        local_name: SNAPSHOT_METADATA_FILENAME,
    });
    out
}

pub(crate) fn upload_artifacts_for_meta(meta: &SnapshotMeta) -> Vec<Artifact> {
    if meta.encrypted {
        encrypted_upload_artifacts()
    } else {
        upload_artifacts_for_kind(artifact_kind_from_meta(meta), format_from_meta(meta))
    }
}

/// Which reader a `snapshot.json` is written for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MetadataDestination {
    /// The artifact directory's own sidecar: keeps the retained source
    /// identity, which recovery on this node needs.
    Local,
    /// A copy staged for upload: omits the node-local identity, which no
    /// other node can resolve.
    Remote,
}

/// Compute the snapshot's logical size from the on-disk VM artifacts and
/// write the `snapshot.json` sidecar so it can be uploaded as a regular
/// staged artifact alongside them.
///
/// `size_bytes` is the sum of the uncompressed local artifact sizes at
/// submit time. vol-mgr may compress the bytes it ships to S3, but the
/// metadata records the logical snapshot size — what restore and
/// operators reason about — and, unlike the post-upload compressed total,
/// it is knowable here before any upload starts. Writing the sidecar now
/// (rather than after the uploads finish) lets the metadata ride the same
/// reflink-staged push path as the VM artifacts, so it never races the
/// reclamation of `snapshot_dir`.
///
/// A stream file counts its own length. Without one, `meta.rootfs_send_source_id`
/// names a retained source and `retained_rootfs_bytes` is node-manager's
/// length for it.
pub(crate) fn write_snapshot_metadata(
    meta: &SnapshotMeta,
    snapshot_dir: &Path,
    vm_artifacts: &[Artifact],
    retained_rootfs_bytes: Option<u64>,
) -> Result<SnapshotMeta, SnapshotMetadataWriteError> {
    write_snapshot_metadata_to(
        meta,
        snapshot_dir,
        vm_artifacts,
        snapshot_dir,
        MetadataDestination::Local,
        retained_rootfs_bytes,
    )
}

/// Returns the metadata as the artifact's owner holds it; the file written
/// for [`MetadataDestination::Remote`] differs only by the omitted identity.
pub(crate) fn write_snapshot_metadata_to(
    meta: &SnapshotMeta,
    artifact_dir: &Path,
    vm_artifacts: &[Artifact],
    metadata_dir: &Path,
    destination: MetadataDestination,
    retained_rootfs_bytes: Option<u64>,
) -> Result<SnapshotMeta, SnapshotMetadataWriteError> {
    let btrfs_send = format_from_meta(meta) == SuspendSnapshotFormat::BtrfsSend;
    let rootfs_send_source_id = meta.rootfs_send_source_id.filter(|_| btrfs_send);
    let stat = |path: PathBuf| {
        std::fs::metadata(&path)
            .map(|metadata| metadata.len())
            .map_err(|source| SnapshotMetadataWriteError::StatArtifact { path, source })
    };
    let mut size_bytes: u64 = 0;
    for artifact in vm_artifacts {
        let path = artifact_dir.join(artifact.local_name);
        let len = if artifact.kind == SnapshotArtifact::RootfsBtrfsSend {
            match crate::retained_source::RootfsArtifact::resolve(
                &meta.sandbox_id,
                artifact_dir,
                rootfs_send_source_id,
            )? {
                Some(crate::retained_source::RootfsArtifact::SendFile { path }) => stat(path)?,
                // A deferred upload has no stream yet; its remote metadata is
                // rewritten from the transfer total once the stream is known.
                Some(crate::retained_source::RootfsArtifact::RetainedSource(source)) => {
                    retained_rootfs_bytes.ok_or(
                        SnapshotMetadataWriteError::UnsizedRetainedSource {
                            artifact_dir: source.artifact_dir,
                            source_id: source.source_id,
                        },
                    )?
                }
                None => stat(path)?,
            }
        } else {
            stat(path)?
        };
        size_bytes = size_bytes.saturating_add(len);
    }

    let meta_final = SnapshotMeta {
        size_bytes,
        rootfs_send_source_id,
        ..meta.clone()
    };
    let written = match destination {
        MetadataDestination::Local => meta_final.clone(),
        MetadataDestination::Remote => SnapshotMeta {
            rootfs_send_source_id: None,
            ..meta_final.clone()
        },
    };
    let meta_json = serde_json::to_string_pretty(&written)?;
    std::fs::create_dir_all(metadata_dir).map_err(SnapshotMetadataWriteError::CreateMetadataDir)?;
    // Restores may leave a sidecar owned by volume-manager. Directory write
    // permission lets us replace it without writing another service's inode;
    // readers keep a complete old or new document even across a crash.
    replace_file_durably(
        metadata_dir,
        ".snapshot.json.upload",
        SNAPSHOT_METADATA_FILENAME,
        meta_json.as_bytes(),
    )
    .map_err(SnapshotMetadataWriteError::WriteMetadata)?;
    Ok(meta_final)
}

/// Replace `final_name` in `dir` with `bytes` through a sibling temporary
/// file: the file is synced before the rename and the directory after it, so
/// a crash cannot leave the final name pointing at an empty or partial file.
pub(crate) fn replace_file_durably(
    dir: &Path,
    temporary_name: &str,
    final_name: &str,
    bytes: &[u8],
) -> std::io::Result<()> {
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt;
    let temporary = dir.join(temporary_name);
    let mut file = std::fs::File::create(&temporary)?;
    file.write_all(bytes)?;
    // Volume-manager reads metadata published by sandbox-manager.
    file.set_permissions(std::fs::Permissions::from_mode(0o644))?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary, dir.join(final_name))?;
    std::fs::File::open(dir)?.sync_all()
}

/// A retained source that a backup will upload, as node-manager just
/// reported it. The evidence names the source it was gathered for, so a
/// preparation cannot apply it to another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetainedUpload {
    /// The inspected source.
    pub(crate) source: crate::retained_source::RetainedSource,
    /// The retained rootfs image length, for local accounting.
    pub(crate) rootfs_bytes: u64,
    /// Volume-manager streams the source. Otherwise the worker materializes
    /// `rootfs.btrfs-send` from it before uploading the file.
    pub(crate) stream: bool,
}

impl Artifact {
    /// Low-cardinality metric tag for the `artifact:` dimension of
    /// `snapshot.create.bytes`. Distinct
    /// per codec for the rootfs slot — `rootfs` vs `rootfs.btrfs-send`
    /// — so dashboards split on `(codec, artifact)` correctly. Keep
    /// the values aligned with the docs entry; renaming any one of
    /// these breaks every existing dashboard.
    pub(crate) fn metric_tag(&self) -> &'static str {
        match self.kind {
            SnapshotArtifact::VmState => "vmstate",
            SnapshotArtifact::Memory => "memory",
            SnapshotArtifact::Rootfs => "rootfs",
            SnapshotArtifact::RootfsBtrfsSend => "rootfs.btrfs-send",
            SnapshotArtifact::EncryptedSnapshotVolume => "snapshot.luks",
            SnapshotArtifact::EncryptedRootfsCow => "cow.luks",
            SnapshotArtifact::Metadata => "metadata",
        }
    }
}

/// Project the codec recorded on a snapshot's sidecar back to the
/// `SuspendSnapshotFormat` enum used by the artifact builder. Mirrors
/// the convention in [`vm_artifacts_for_format`]: an absent or unknown
/// `rootfs_diff_format` resolves to `Full` (preserving wire-shape
/// forward compatibility — a future codec marker an older binary
/// doesn't recognize must keep emitting metrics under the `Full`
/// fallback rather than `unknown`).
pub(crate) fn format_from_meta(meta: &SnapshotMeta) -> SuspendSnapshotFormat {
    match meta.rootfs_diff_format.as_deref() {
        Some(ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1) => SuspendSnapshotFormat::BtrfsSend,
        _ => SuspendSnapshotFormat::Full,
    }
}

pub(crate) fn artifact_kind_from_meta(meta: &SnapshotMeta) -> ArtifactKind {
    meta.artifact_kind.unwrap_or(ArtifactKind::Full)
}

/// Submits all three VM snapshot artifacts (vmstate, memory, rootfs.ext4) to
/// the volume-manager as background upload jobs and returns immediately.
///
/// `snapshot_id` is the per-attempt UUIDv7 the caller allocated for this
/// suspend. Every artifact lands under
/// `<prefix>/suspend_snapshots/<shard>/<sandbox_id>/<snapshot_id>/<file>`, so
/// two suspends on the same sandbox never share or overwrite each other's
/// bytes. Restore-side readers find the latest `state='ready'` snapshot row
/// and pull from the URI on that row, making partial uploads invisible by
/// construction.
///
/// The `snapshot.json` metadata document is submitted here too, as the
/// final artifact: its `size_bytes` is computed from the local VM artifact
/// sizes (see [`write_snapshot_metadata`]) rather than the post-upload
/// transfer total, so it is knowable before any upload starts. Submitting
/// it on the same staged push path as the VM artifacts means it survives
/// reclamation of `snapshot_dir` and that finalizing the snapshot `ready`
/// can be gated on its upload completing.
///
/// The caller persists the returned intent in the durable `"snapshot_upload"`
/// worker before any remote side effect or runtime teardown.
///
/// A btrfs-send rootfs without a send file must be a retained source that
/// the caller has just inspected, passed as `retained`.
pub(crate) fn prepare_suspend_snapshot(
    meta: &SnapshotMeta,
    snapshot_id: &str,
    snapshot_dir: &Path,
    config: &SnapshotUploadConfig,
    artifact_kind: ArtifactKind,
    format: SuspendSnapshotFormat,
    retained: Option<RetainedUpload>,
) -> Result<SnapshotUploadJobs, String> {
    let sandbox_id = &meta.sandbox_id;
    let vm_artifacts = if meta.encrypted {
        encrypted_vm_artifacts()
    } else {
        artifact_files_for_kind(artifact_kind, format)
    };

    let mut retained_rootfs = false;
    for artifact in &vm_artifacts {
        let present = if artifact.kind == SnapshotArtifact::RootfsBtrfsSend {
            match crate::retained_source::RootfsArtifact::resolve_meta(meta, snapshot_dir)
                .map_err(|error| error.to_string())?
            {
                Some(crate::retained_source::RootfsArtifact::SendFile { path }) => path.is_file(),
                Some(crate::retained_source::RootfsArtifact::RetainedSource(source)) => {
                    match retained.as_ref() {
                        Some(inspected) if inspected.source == source => {}
                        Some(inspected) => {
                            return Err(format!(
                                "retained send source {} in {} was inspected as {} in {}",
                                source.source_id,
                                source.artifact_dir.display(),
                                inspected.source.source_id,
                                inspected.source.artifact_dir.display()
                            ));
                        }
                        None => {
                            return Err(format!(
                                "retained send source {} in {} was not inspected before upload",
                                source.source_id,
                                source.artifact_dir.display()
                            ));
                        }
                    }
                    retained_rootfs = true;
                    true
                }
                None => false,
            }
        } else {
            snapshot_dir.join(artifact.local_name).is_file()
        };
        if !present {
            return Err(format!(
                "required snapshot artifact missing: {}",
                snapshot_dir.join(artifact.local_name).display()
            ));
        }
    }

    // A stream file already on disk uploads as a file with a known length.
    // Only a retained source without one streams from node-manager.
    let deferred_metadata = !meta.encrypted
        && format == SuspendSnapshotFormat::BtrfsSend
        && retained_rootfs
        && retained.as_ref().is_some_and(|retained| retained.stream);
    // Size and write `snapshot.json` now so it uploads as a regular staged
    // artifact below; `meta_final` carries the computed `size_bytes`
    // through to the durable worker payload. An artifact with a retained
    // source keeps that identity in its own sidecar, so the uploaded copy
    // is always staged separately.
    let retained_identity = !meta.encrypted
        && format == SuspendSnapshotFormat::BtrfsSend
        && meta.rootfs_send_source_id.is_some();
    let metadata_staging_dir =
        if artifact_kind == ArtifactKind::Disk || deferred_metadata || retained_identity {
            Some(
                snapshot_dir
                    .parent()
                    .unwrap_or(snapshot_dir)
                    .join(format!(".artifact-metadata-{snapshot_id}")),
            )
        } else {
            None
        };
    let metadata_staging_guard = MetadataStagingDirGuard::new(metadata_staging_dir.clone());
    let (metadata_dir, destination) = match metadata_staging_dir.as_deref() {
        Some(staging) => (staging.to_path_buf(), MetadataDestination::Remote),
        None => (snapshot_dir.to_path_buf(), MetadataDestination::Local),
    };
    let meta_final = write_snapshot_metadata_to(
        meta,
        snapshot_dir,
        &vm_artifacts,
        &metadata_dir,
        destination,
        retained.as_ref().map(|retained| retained.rootfs_bytes),
    )
    .map_err(|error| error.to_string())?;

    Ok(SnapshotUploadJobs {
        meta: meta_final,
        snapshot_id: snapshot_id.to_owned(),
        snapshot_dir: snapshot_dir.to_path_buf(),
        config: config.clone(),
        storage_uri: build_storage_uri(sandbox_id, snapshot_id, config),
        job_ids: Vec::new(),
        metadata_staging_dir: metadata_staging_guard.disarm(),
        includes_metadata_job: true,
        deferred_metadata,
    })
}

/// Submit a prepared upload intent to vol-mgr.
///
/// The immutable snapshot attempt determines every source and destination, so
/// replay after a worker restart rejoins vol-mgr's persisted transfer jobs.
pub async fn submit_prepared_snapshot(
    prepared: &SnapshotUploadJobs,
    node_manager: &crate::node_manager_transport::NodeManagerConnector,
) -> Result<SnapshotUploadJobs, String> {
    let mut prepared = prepared.clone();
    if !prepared.deferred_metadata
        && let Some(source_id) = prepared.meta.rootfs_send_source_id
    {
        if !tokio::fs::try_exists(prepared.snapshot_dir.join(ROOTFS_BTRFS_SEND_FILENAME))
            .await
            .map_err(|error| format!("stat rootfs send: {error}"))?
        {
            crate::btrfs_via_nodemgr::materialize_rootfs_send(
                &prepared.meta.sandbox_id,
                &prepared.snapshot_dir,
                source_id,
                node_manager,
            )
            .await
            .map_err(|error| error.to_string())?;
        }
        // The stream exists now, whether this attempt or an interrupted one
        // materialized it: the uploaded metadata must count it, so rewrite
        // unconditionally rather than only after materializing.
        let (metadata_dir, destination) = match prepared.metadata_staging_dir.as_deref() {
            Some(staging) => (staging, MetadataDestination::Remote),
            None => (prepared.snapshot_dir.as_path(), MetadataDestination::Local),
        };
        prepared.meta = write_snapshot_metadata_to(
            &prepared.meta,
            &prepared.snapshot_dir,
            &artifact_files_for_kind(
                artifact_kind_from_meta(&prepared.meta),
                format_from_meta(&prepared.meta),
            ),
            metadata_dir,
            destination,
            None,
        )
        .map_err(|error| error.to_string())?;
    }
    let artifacts = upload_artifacts_for_meta(&prepared.meta);
    let metadata_dir = prepared
        .metadata_staging_dir
        .as_deref()
        .unwrap_or(&prepared.snapshot_dir);
    let mut client = pplx_space_volmgr_client::connect(VOLMGR_SOCKET)
        .await
        .map_err(|e| format!("vol-mgr connect failed: {e}"))?;
    let mut job_ids = Vec::with_capacity(artifacts.len());
    let mut errors = Vec::new();

    for artifact in &artifacts {
        if prepared.deferred_metadata && artifact.kind == SnapshotArtifact::Metadata {
            continue;
        }
        let src = if artifact.kind == SnapshotArtifact::Metadata {
            metadata_dir.join(artifact.local_name)
        } else {
            prepared.snapshot_dir.join(artifact.local_name)
        };
        let key = snapshot_object_key(
            &prepared.config.key_prefix,
            &prepared.meta.sandbox_id,
            &prepared.snapshot_id,
            artifact.kind,
        );
        let s3_uri = build_artifact_uri(&key, &prepared.config);
        let local_path = src.to_string_lossy().into_owned();

        let submitted =
            if prepared.deferred_metadata && artifact.kind == SnapshotArtifact::RootfsBtrfsSend {
                client
                    .submit_push_btrfs_send(pplx_space_volmgr_client::proto::PushBtrfsSendRequest {
                        sandbox_id: prepared.meta.sandbox_id.clone(),
                        artifact_dir: prepared.snapshot_dir.to_string_lossy().into_owned(),
                        source_id: prepared
                            .meta
                            .rootfs_send_source_id
                            .ok_or("deferred send requires source identity")?
                            .to_string(),
                        s3_uri,
                    })
                    .await
            } else {
                client
                    .submit_push_file(PushFileRequest {
                        subject_id: format!("sandbox:{}", prepared.meta.sandbox_id),
                        local_path,
                        s3_uri,
                    })
                    .await
            };
        match submitted {
            Ok(resp) => {
                let job_id = match resp.into_inner().job_id.parse::<JobId>() {
                    Ok(job_id) => job_id,
                    Err(error) => {
                        errors.push(format!(
                            "{}: invalid transfer job ID: {error}",
                            artifact.local_name
                        ));
                        continue;
                    }
                };
                tracing::debug!(
                    sandbox_id = %prepared.meta.sandbox_id,
                    snapshot_id = %prepared.snapshot_id,
                    artifact = artifact.local_name,
                    job_id = %job_id,
                    "snapshot artifact upload submitted"
                );
                job_ids.push(job_id);
            }
            Err(e) => {
                errors.push(format!("{}: {e}", artifact.local_name));
            }
        }
    }

    if !errors.is_empty() {
        return Err(format!(
            "vol-mgr submit_push_file failed for {} of {} artifacts; \
             {} successfully-queued upload(s) will run as S3 orphans for \
             snapshot {} until retention prunes them: [{}]",
            errors.len(),
            artifacts.len(),
            job_ids.len(),
            prepared.snapshot_id,
            errors.join("; ")
        ));
    }

    let mut submitted = prepared.clone();
    submitted.job_ids = job_ids;
    Ok(submitted)
}

pub(crate) fn cleanup_metadata_staging_dir(jobs: &SnapshotUploadJobs) {
    drop(MetadataStagingDirGuard::new(
        jobs.metadata_staging_dir.clone(),
    ));
}

pub(crate) fn build_artifact_uri(key: &str, config: &SnapshotUploadConfig) -> String {
    let mut uri = format!("s3://{}/{}", config.bucket, key);
    uri.push_str(&format!("?region={}", config.region));
    if let Some(ref endpoint) = config.endpoint {
        uri.push_str(&format!("&endpoint={endpoint}"));
    }
    if config.path_style {
        uri.push_str("&path_style=true");
    }
    uri
}

#[cfg(test)]
pub(crate) fn build_storage_uri_for_tests(
    sandbox_id: &str,
    snapshot_id: &str,
    bucket: &str,
    region: &str,
    key_prefix: &str,
) -> String {
    let config = SnapshotUploadConfig {
        bucket: bucket.to_string(),
        region: region.to_string(),
        key_prefix: key_prefix.to_string(),
        endpoint: None,
        path_style: false,
    };
    build_storage_uri(sandbox_id, snapshot_id, &config)
}

pub(crate) fn build_storage_uri(
    sandbox_id: &str,
    snapshot_id: &str,
    config: &SnapshotUploadConfig,
) -> String {
    let dir_prefix = pplx_space_common_core::types::snapshot_attempt_key_prefix(
        &config.key_prefix,
        sandbox_id,
        snapshot_id,
    );
    let mut uri = format!("s3://{}/{}", config.bucket, dir_prefix);
    uri.push_str(&format!("?region={}", config.region));
    if let Some(ref endpoint) = config.endpoint {
        uri.push_str(&format!("&endpoint={endpoint}"));
    }
    if config.path_style {
        uri.push_str("&path_style=true");
    }
    uri
}

#[cfg(test)]
mod tests;

/// Publish remote metadata only after all stream lengths are known. The local
/// sidecar continues describing its retained representation for crash recovery.
pub(crate) async fn submit_streamed_metadata(
    payload: &SnapshotUploadJobs,
    logical_bytes: u64,
) -> Result<JobId, String> {
    let directory = payload
        .metadata_staging_dir
        .as_ref()
        .ok_or("deferred metadata requires private staging")?;
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|error| error.to_string())?;
    let path = directory.join(SNAPSHOT_METADATA_FILENAME);
    let remote = SnapshotMeta {
        size_bytes: logical_bytes,
        rootfs_send_source_id: None,
        ..payload.meta.clone()
    };
    let bytes = serde_json::to_vec_pretty(&remote).map_err(|error| error.to_string())?;
    // Volume-manager stages only after the complete write. The job's immutable
    // backup-attempt key makes resubmission after an ambiguous reply idempotent.
    tokio::fs::write(&path, bytes)
        .await
        .map_err(|error| error.to_string())?;
    let key = snapshot_object_key(
        &payload.config.key_prefix,
        &payload.meta.sandbox_id,
        &payload.snapshot_id,
        SnapshotArtifact::Metadata,
    );
    let mut client = pplx_space_volmgr_client::connect(VOLMGR_SOCKET)
        .await
        .map_err(|error| error.to_string())?;
    let response = client
        .submit_push_file(PushFileRequest {
            subject_id: format!("sandbox:{}", payload.meta.sandbox_id),
            local_path: path.to_string_lossy().into_owned(),
            s3_uri: build_artifact_uri(&key, &payload.config),
        })
        .await
        .map_err(|error| error.to_string())?;
    response
        .into_inner()
        .job_id
        .parse::<JobId>()
        .map_err(|error| error.to_string())
}
