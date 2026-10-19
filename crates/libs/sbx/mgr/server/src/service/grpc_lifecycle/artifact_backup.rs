//! `EnqueueArtifactBackup` RPC admission and the legacy-sandbox reupload used
//! by eviction, with their artifact-format and precondition helpers.

use super::*;

/// Precondition for durable artifact backup: the node must
/// have both cluster-api env vars wired so the durable worker can
/// register the uploaded snapshot. If wiring is missing, the RPC fails
/// loudly instead of returning a job id for an enqueue that the
/// downstream `register_with_cluster_api` step would mark
/// `JobError::permanent` (see
/// `crates/libs/sbx/mgr/server/src/snapshot_upload_jobs.rs`).
///
/// The status carries neither `REUPLOAD_ARTIFACT_MISSING_MARKER` nor
/// `REUPLOAD_SANDBOX_UNKNOWN_MARKER`, so the future cluster-controller
/// classifier will treat this as retryable rather than terminal — the
/// right behavior, since an operator wiring SPACE_INTERNAL_TOKEN fixes
/// the deployment without losing the sandbox.
///
/// Booleans (rather than `&str` values) keep this testable without
/// touching the process environment. Callers must apply the same
/// "non-empty" predicate the node-api client's `required_env` helper
/// uses — an env var present but set to the empty string is treated as
/// missing throughout the codebase.
pub(in crate::service) fn reupload_precondition_for_cluster_api_env(
    endpoint_set: bool,
    token_set: bool,
) -> Option<Status> {
    if endpoint_set && token_set {
        return None;
    }
    let missing: Vec<&str> = [
        (!endpoint_set).then_some("SPACE_INTERNAL_API_ENDPOINT"),
        (!token_set).then_some("SPACE_INTERNAL_TOKEN"),
    ]
    .into_iter()
    .flatten()
    .collect();
    Some(Status::failed_precondition(format!(
        "cannot reupload snapshot: this node is missing cluster-api wiring ({}); \
         even on a successful S3 upload, the durable worker cannot register the \
         snapshot location so the reconciler would never see a ready row land",
        missing.join(", "),
    )))
}

/// "Set" means the env var is present *and* non-empty — matching the
/// node-api client's `required_env` helper, which `connect_from_env()`
/// uses to decide between `Ok(client)` and `MissingEndpoint`/
/// `MissingToken`. An env var present-but-empty (e.g. an unfilled Helm
/// template that rendered to `""`) must be classified as missing so
/// this precondition matches the downstream cluster-api connect
/// behavior; otherwise the RPC would happily enqueue a job that the
/// durable worker's `register_with_cluster_api` then marks
/// `JobError::permanent`.
fn cluster_api_env_var_set(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| !v.is_empty())
}

/// Sniff the local `snapshot.json` sidecar to recover the rootfs codec
/// the original suspend used. Reupload has no caller-supplied format
/// (the RPC predates any per-request opt-in), so we read it off the
/// snapshot we're re-uploading.
///
/// Three outcomes, only one of which is `Full`:
///
/// - **Sidecar missing** (`Ok(sniffed)`). Resolved from the rootfs
///   artifact actually on disk via
///   [`sidecarless_format_from_artifacts`]: pre-codec snapshots didn't
///   write a sidecar, but neither did the first btrfs-send suspends,
///   so "no sidecar" alone does not imply the historical full-rootfs
///   shape.
/// - **Sidecar present and parses** (`Ok(format)`). Use the codec the
///   original suspend recorded.
/// - **Sidecar present but unparseable** (`Err(Status::internal)`).
///   This is corruption (a truncated `std::fs::write` from a crash
///   mid-write is the realistic source) and **must not** silently
///   degrade to `Full`. Reuploading a btrfs-send snapshot under the
///   `Full` artifact set probes for `rootfs.ext4`, which the
///   `encode_rootfs_as_btrfs_send` step deleted from the snapshot
///   dir; the precondition loop would then return
///   `REUPLOAD_ARTIFACT_MISSING_MARKER` and the cluster controller's
///   reconciler treats that marker as terminal — marking the sandbox
///   `Failed`. Surfacing the parse error as `Status::internal`
///   instead keeps the reconciler in the retry path until either the
///   sidecar is rewritten by another suspend or operator action
///   intervenes.
///
/// Wire-format string compared here is the same constant the suspend
/// handler writes; don't fork the literal in either direction.
pub(super) fn reupload_format_from_sidecar(
    snapshot_dir: &str,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    let path = format!("{snapshot_dir}/snapshot.json");
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return sidecarless_format_from_artifacts(snapshot_dir);
        }
        Err(err) => {
            return Err(Status::internal(format!(
                "reupload: failed to read snapshot.json at {path}: {err}",
            )));
        }
    };
    let meta: crate::snapshot_upload::SnapshotMeta =
        serde_json::from_slice(&bytes).map_err(|err| {
            Status::internal(format!(
                "reupload: snapshot.json present but unparseable at {path}: {err}",
            ))
        })?;
    Ok(match meta.rootfs_diff_format.as_deref() {
        Some(crate::snapshot_upload::ROOTFS_DIFF_FORMAT_BTRFS_SEND_V1) => {
            pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend
        }
        _ => pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full,
    })
}

/// Resolve the rootfs codec of a sidecar-less snapshot from the rootfs
/// artifact on disk. The two codecs' artifacts are mutually exclusive
/// in a snapshot dir (`encode_rootfs_as_btrfs_send` deletes
/// `rootfs.ext4` when it writes `rootfs.btrfs-send`), so presence is an
/// unambiguous witness:
///
/// - `rootfs.ext4` present → `Full`. The historical shape wins even in
///   the (never-produced) both-present case: the `Full` artifact set is
///   satisfiable, so reupload proceeds.
/// - only `rootfs.btrfs-send` present → `BtrfsSend`. These are the
///   early btrfs-send suspends that predate local sidecar writing;
///   defaulting them to `Full` would probe `rootfs.ext4` and turn a
///   complete snapshot into the terminal
///   `REUPLOAD_ARTIFACT_MISSING_MARKER` (mark-as-failed to the drain
///   promoter, a forever-skip to the eviction legacy-repair lane).
/// - neither present → `Full`, the pre-codec fallback; the caller's
///   artifact-presence check then reports the genuinely-missing rootfs.
/// - a stat error → `Err(Status::internal)` (retryable), same rationale
///   as the unparseable-sidecar branch: transient IO must not pick a
///   codec whose artifact probe escalates to the terminal marker.
fn sidecarless_format_from_artifacts(
    snapshot_dir: &str,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    let probe = |file_name: &str| -> Result<bool, Status> {
        let path = format!("{snapshot_dir}/{file_name}");
        std::path::Path::new(&path).try_exists().map_err(|err| {
            Status::internal(format!(
                "reupload: failed to stat rootfs artifact at {path}: {err}",
            ))
        })
    };
    if probe(crate::snapshot_upload::ROOTFS_FULL_FILENAME)? {
        return Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full);
    }
    // The bundle's own entry, not its contents: a retained-only artifact is
    // still a btrfs-send artifact, whose rootfs the presence check reports.
    if probe(crate::snapshot_upload::ROOTFS_BTRFS_SEND_FILENAME)?
        || probe(crate::retained_source::BUNDLE_DIRECTORY)?
    {
        return Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::BtrfsSend);
    }
    Ok(pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat::Full)
}

pub(super) fn artifact_backup_format(
    artifact: &crate::db::LocalArtifactRow,
) -> Result<pplx_space_sbx_mgr_client::proto::SuspendSnapshotFormat, Status> {
    match artifact.artifact_kind {
        pplx_space_common_core::types::ArtifactKind::Full => {
            reupload_format_from_sidecar(&artifact.local_path)
        }
        pplx_space_common_core::types::ArtifactKind::Disk => {
            sidecarless_format_from_artifacts(&artifact.local_path)
        }
    }
}

pub(super) fn current_instance_backup_precondition(
    state: SandboxState,
    expected_kind: pplx_space_common_core::types::ArtifactKind,
) -> Result<(), Status> {
    use pplx_space_common_core::types::ArtifactKind;
    let current_kind = match state {
        SandboxState::Stopped => ArtifactKind::Disk,
        SandboxState::Suspended => ArtifactKind::Full,
        SandboxState::Restoring if expected_kind == ArtifactKind::Full => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: {}",
                pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_RESTORING_MARKER
            )));
        }
        SandboxState::Failed => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: {}",
                pplx_space_common_core::artifact_backup_signals::CURRENT_INSTANCE_BACKUP_SANDBOX_FAILED_MARKER
            )));
        }
        other => {
            return Err(Status::failed_precondition(format!(
                "cannot back up current instance: sandbox is {other}, must be stopped or suspended"
            )));
        }
    };
    if expected_kind == current_kind {
        return Ok(());
    }
    let message = if state == SandboxState::Suspended {
        format!(
            "requested {expected_kind} backup but {}",
            pplx_space_common_core::artifact_backup_signals::CURRENT_INSTANCE_BACKUP_PUBLISHES_FULL_MARKER
        )
    } else {
        format!(
            "requested {expected_kind} backup but current {state} instance publishes {current_kind}"
        )
    };
    Err(Status::failed_precondition(message))
}

pub(super) fn artifact_backup_lookup_error(status: Status, full_current_instance: bool) -> Status {
    if full_current_instance && status.code() == tonic::Code::NotFound {
        Status::failed_precondition(format!(
            "cannot back up current instance: {}",
            pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_UNKNOWN_MARKER
        ))
    } else {
        status
    }
}

/// Which caller a backup file preflight answers for. The drain reconciler
/// classifies `FailedPrecondition` responses carrying the artifact-missing
/// marker as terminal, so the policy decides where that marker appears.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BackupFilesPolicy {
    /// An artifact backup; the marker only when the sandbox's current full
    /// instance is being backed up for a drain.
    Backup { full_current_instance: bool },
    /// A reupload of the canonical suspend snapshot for promotion.
    Reupload,
}

impl BackupFilesPolicy {
    fn missing(self, path: &str) -> Status {
        use pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER;
        match self {
            Self::Backup {
                full_current_instance: true,
            } => Status::failed_precondition(format!(
                "cannot back up artifact: {REUPLOAD_ARTIFACT_MISSING_MARKER} at {path}"
            )),
            Self::Backup {
                full_current_instance: false,
            } => Status::failed_precondition(format!(
                "cannot back up artifact: local file is missing at {path}"
            )),
            // The marker substring is the classifier signal the cluster
            // controller's promote handler keys off; both sides reference
            // the constant.
            Self::Reupload => Status::failed_precondition(format!(
                "cannot reupload snapshot: {REUPLOAD_ARTIFACT_MISSING_MARKER} at {path} \
                 — the local snapshot has already been pruned; the sandbox cannot \
                 be promoted from this node"
            )),
        }
    }

    fn unusable(self, detail: &str) -> Status {
        use pplx_space_common_core::node_drain_signals::REUPLOAD_ARTIFACT_MISSING_MARKER;
        match self {
            Self::Backup {
                full_current_instance: true,
            } => Status::failed_precondition(format!(
                "cannot back up artifact: {REUPLOAD_ARTIFACT_MISSING_MARKER}: {detail}"
            )),
            Self::Backup {
                full_current_instance: false,
            } => Status::failed_precondition(format!(
                "cannot back up artifact: local file is missing: {detail}"
            )),
            Self::Reupload => Status::failed_precondition(format!(
                "cannot reupload snapshot: {REUPLOAD_ARTIFACT_MISSING_MARKER}: {detail}"
            )),
        }
    }

    /// Retryable: nothing proved the artifact unusable.
    fn unverified(self, detail: &str) -> Status {
        match self {
            Self::Backup { .. } => Status::internal(format!("cannot back up artifact: {detail}")),
            Self::Reupload => Status::internal(format!("cannot reupload snapshot: {detail}")),
        }
    }
}

/// Require every local file a backup of `local_path` reads, with the retained
/// btrfs-send rootfs checked through node-manager. `try_exists` separates a
/// genuinely missing file (terminal) from a failed stat (retryable).
pub(super) async fn require_artifact_backup_files(
    inspector: &dyn crate::retained_source::RetainedSourceInspector,
    sandbox_id: &str,
    local_path: &str,
    kind: pplx_space_common_core::types::ArtifactKind,
    format: crate::proto::SuspendSnapshotFormat,
    policy: BackupFilesPolicy,
) -> Result<(), Status> {
    use crate::retained_source::BtrfsSendRootfsPresence;
    let mut files = crate::snapshot_upload::artifact_files_for_kind(kind, format);
    if kind == pplx_space_common_core::types::ArtifactKind::Full {
        let sidecar = std::path::Path::new(local_path)
            .join(crate::snapshot_upload::SNAPSHOT_METADATA_FILENAME);
        match std::fs::read(&sidecar) {
            Ok(bytes) => {
                let meta: crate::snapshot_upload::SnapshotMeta = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        policy.unverified(&format!(
                            "snapshot metadata is unparseable at {}: {error}",
                            sidecar.display()
                        ))
                    })?;
                // Encrypted capture seals memory and rootfs into containers.
                // Admission must require the same files the uploader reads,
                // regardless of the plaintext rootfs codec in the sidecar.
                if meta.encrypted {
                    files = crate::snapshot_upload::encrypted_vm_artifacts();
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(policy.unverified(&format!(
                    "failed to read snapshot metadata at {}: {error}",
                    sidecar.display()
                )));
            }
        }
    }
    for file in files {
        if file.kind == pplx_space_common_core::types::SnapshotArtifact::RootfsBtrfsSend {
            // Disk artifacts never retain a source: they have no sidecar.
            let source_id = match kind {
                pplx_space_common_core::types::ArtifactKind::Full => {
                    super::sidecar_rootfs_send_source_id(local_path)?
                }
                pplx_space_common_core::types::ArtifactKind::Disk => None,
            };
            match crate::retained_source::btrfs_send_rootfs_presence(
                inspector,
                sandbox_id,
                std::path::Path::new(local_path),
                source_id,
            )
            .await
            {
                BtrfsSendRootfsPresence::Present => continue,
                BtrfsSendRootfsPresence::Missing { path } => {
                    return Err(policy.missing(&path.to_string_lossy()));
                }
                BtrfsSendRootfsPresence::Unusable { detail } => {
                    return Err(policy.unusable(&detail));
                }
                BtrfsSendRootfsPresence::Unverified { detail } => {
                    return Err(policy.unverified(&detail));
                }
            }
        }
        let path = std::path::Path::new(local_path)
            .join(file.local_name)
            .to_string_lossy()
            .into_owned();
        match std::path::Path::new(&path).try_exists() {
            Ok(true) => {}
            Ok(false) => return Err(policy.missing(&path)),
            Err(error) => {
                return Err(
                    policy.unverified(&format!("failed to stat artifact at {path}: {error}"))
                );
            }
        }
    }
    Ok(())
}

/// Map an [`OrgSnapshotConfigResolution`] to the upload-config the
/// artifact-backup admission should use, or to a tonic `Status` the
/// RPC should return immediately. The classification matters because
/// the drain reconciler's `FailedPrecondition`-marker classifier treats
/// reupload errors as terminal (mark-as-failed); a transient lookup
/// failure must therefore surface as `Internal` (retryable) instead.
///
/// - `Resolved(cfg)` → `Ok(Some(cfg))`.
/// - `Absent` + env fallback present → `Ok(None)` (handler proceeds with
///   the env default in `self.snapshot_upload_config`).
/// - `Absent` + no env fallback → `Err(FailedPrecondition)` (operator
///   must wire `SPACE_SNAPSHOT_BUCKET` or register the org override).
/// - `LookupFailed(_)` → `Err(Internal)` regardless of env fallback, so
///   the reconciler keeps retrying through a transient cluster-api blip.
pub(in crate::service) fn reupload_resolution_to_config(
    resolution: OrgSnapshotConfigResolution,
    env_fallback_present: bool,
) -> Result<Option<crate::snapshot_upload::SnapshotUploadConfig>, Status> {
    match resolution {
        OrgSnapshotConfigResolution::Resolved(cfg) => Ok(Some(cfg)),
        OrgSnapshotConfigResolution::Absent => {
            if env_fallback_present {
                Ok(None)
            } else {
                Err(Status::failed_precondition(
                    "cannot reupload snapshot: no snapshot upload destination configured \
                     (set SPACE_SNAPSHOT_BUCKET / SPACE_SNAPSHOT_REGION on this node, or \
                     register an org-level snapshot_base_object_storage_uri override)",
                ))
            }
        }
        OrgSnapshotConfigResolution::LookupFailed(reason) => Err(Status::internal(format!(
            "cannot reupload snapshot: org snapshot upload override lookup failed: \
             {reason}; the reconciler should retry once the cluster-api recovers",
        ))),
        OrgSnapshotConfigResolution::ClusterRowMissing => Err(Status::failed_precondition(
            "cannot reupload snapshot: cluster-api has no row for this sandbox \
             (GetSandboxSnapshotStorageUri returned NotFound). Reupload requires \
             a prior suspend to have registered the sandbox; the durable worker's \
             BeginArtifactBackup would otherwise also fail permanently. \
             Investigate state divergence between this node and the cluster.",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
enum SuspendedArtifactAdoption {
    TouchUpdatedAt,
    PreserveUpdatedAt,
}

impl SandboxManagerService {
    pub(super) fn adopt_or_get_suspended_local_artifact(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> Result<LocalArtifactId, Status> {
        self.adopt_or_get_suspended_local_artifact_with_mode(
            sandbox_id,
            local_artifact_id,
            local_path,
            SuspendedArtifactAdoption::TouchUpdatedAt,
        )
    }

    fn adopt_or_get_suspended_local_artifact_preserving_updated_at(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
    ) -> Result<LocalArtifactId, Status> {
        self.adopt_or_get_suspended_local_artifact_with_mode(
            sandbox_id,
            local_artifact_id,
            local_path,
            SuspendedArtifactAdoption::PreserveUpdatedAt,
        )
    }

    fn adopt_or_get_suspended_local_artifact_with_mode(
        &self,
        sandbox_id: &str,
        local_artifact_id: LocalArtifactId,
        local_path: &str,
        mode: SuspendedArtifactAdoption,
    ) -> Result<LocalArtifactId, Status> {
        let adopted = match mode {
            SuspendedArtifactAdoption::TouchUpdatedAt => {
                self.local_artifacts
                    .adopt_suspended(sandbox_id, local_artifact_id, local_path)
            }
            SuspendedArtifactAdoption::PreserveUpdatedAt => self
                .local_artifacts
                .adopt_suspended_preserving_updated_at(sandbox_id, local_artifact_id, local_path),
        }
        .map_err(|err| Status::internal(format!("db artifact adoption failed: {err}")))?;
        if adopted {
            return Ok(local_artifact_id);
        }

        let sandbox = {
            let db = self.db_conn()?;
            crate::db::get_sandbox(&db, sandbox_id)
                .map_err(|err| Status::internal(format!("db sandbox lookup failed: {err}")))?
                .ok_or_else(|| Status::not_found(format!("sandbox {sandbox_id} not found")))?
        };
        if sandbox.state != SandboxState::Suspended {
            return Err(Status::failed_precondition(format!(
                "cannot adopt suspended local artifact: sandbox is {}, must be suspended",
                sandbox.state
            )));
        }
        let current_id = sandbox.current_local_artifact_id.ok_or_else(|| {
            Status::failed_precondition(
                "current suspended instance has no published local artifact",
            )
        })?;
        if sandbox.current_local_artifact_kind
            != Some(pplx_space_common_core::types::ArtifactKind::Full)
            || sandbox.current_local_artifact_origin
                != Some(pplx_space_common_core::types::ArtifactOrigin::Suspend)
        {
            return Err(Status::failed_precondition(
                "current suspended instance artifact is not a full suspend artifact",
            ));
        }
        if self
            .local_artifacts
            .get(current_id)
            .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
            .is_none()
        {
            return Err(Status::failed_precondition(format!(
                "local artifact {current_id} is not published"
            )));
        }
        Ok(current_id)
    }

    pub(super) async fn existing_ready_artifact_backup_response(
        &self,
        artifact: &crate::db::LocalArtifactRow,
    ) -> Result<Option<EnqueueArtifactBackupResponse>, Status> {
        let mut client = match pplx_space_node_api_client::cluster::connect_from_env().await {
            Ok(client) => client,
            Err(error) => {
                tracing::warn!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %error,
                    "failed to check existing artifact backup"
                );
                return Err(Status::unavailable(format!(
                    "cannot check existing artifact backup: {error}"
                )));
            }
        };
        let request = pplx_space_node_api_client::proto::GetArtifactBackupLocationRequest {
            sandbox_id: artifact.sandbox_id.clone(),
            local_artifact_id: artifact.local_artifact_id.to_string(),
            artifact_kind: match artifact.artifact_kind {
                pplx_space_common_core::types::ArtifactKind::Disk => {
                    crate::proto::ArtifactKind::Disk as i32
                }
                pplx_space_common_core::types::ArtifactKind::Full => {
                    crate::proto::ArtifactKind::Full as i32
                }
            },
            readiness: None,
        };

        match client.get_artifact_backup_location(request).await {
            Ok(response) => {
                if let Err(error) = self
                    .local_artifacts
                    .mark_backup_completed(artifact.local_artifact_id)
                {
                    tracing::warn!(
                        sandbox_id = %artifact.sandbox_id,
                        local_artifact_id = %artifact.local_artifact_id,
                        %error,
                        "failed to stamp local artifact backup-completed marker after cluster proof"
                    );
                }
                let response = response.into_inner();
                Ok(Some(EnqueueArtifactBackupResponse {
                    local_artifact_id: artifact.local_artifact_id.to_string(),
                    artifact_kind: response.artifact_kind,
                    backup_attempt_id: response.backup_attempt_id,
                }))
            }
            Err(status) if status.code() == tonic::Code::NotFound => Ok(None),
            Err(status) => {
                tracing::warn!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %status,
                    "failed to check existing artifact backup"
                );
                Err(status)
            }
        }
    }

    pub(super) fn reusable_artifact_backup_attempt(
        status: pplx_space_common_jobs::JobStatus,
        has_failure: bool,
        payload: &str,
    ) -> Result<Option<String>, serde_json::Error> {
        // A completed job records a past upload, not present remote durability.
        // Ready backups were verified against the cluster before this lookup;
        // authoritative absence must allow a new attempt instead of joining a
        // terminal job forever.
        if has_failure
            || !matches!(
                status,
                pplx_space_common_jobs::JobStatus::Pending
                    | pplx_space_common_jobs::JobStatus::Running
            )
        {
            return Ok(None);
        }

        #[derive(serde::Deserialize)]
        struct PersistedArtifactBackupIdentity {
            snapshot_id: String,
        }

        serde_json::from_str::<PersistedArtifactBackupIdentity>(payload)
            .map(|identity| Some(identity.snapshot_id))
    }

    pub(in crate::service) async fn enqueue_artifact_backup(
        &self,
        request: Request<EnqueueArtifactBackupRequest>,
    ) -> Result<Response<EnqueueArtifactBackupResponse>, Status> {
        self.authorize_peer(&request, "EnqueueArtifactBackup", ALLOW_SANDBOX_OPERATOR)?;
        let req = request.into_inner();
        let sandbox_id = req.sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        let expected_kind = match crate::proto::ArtifactKind::try_from(req.expected_artifact_kind) {
            Ok(crate::proto::ArtifactKind::Disk) => {
                pplx_space_common_core::types::ArtifactKind::Disk
            }
            Ok(crate::proto::ArtifactKind::Full) => {
                pplx_space_common_core::types::ArtifactKind::Full
            }
            _ => {
                return Err(Status::invalid_argument(
                    "expected_artifact_kind is required",
                ));
            }
        };
        if let Some(status) = reupload_precondition_for_cluster_api_env(
            cluster_api_env_var_set(pplx_space_node_api_client::cluster::CLUSTER_API_ENDPOINT_ENV),
            cluster_api_env_var_set("SPACE_INTERNAL_TOKEN"),
        ) {
            return Err(status);
        }

        let full_current_instance = expected_kind
            == pplx_space_common_core::types::ArtifactKind::Full
            && matches!(
                req.source.as_ref(),
                Some(crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(_))
                    | None
            );
        let (sandbox, _guard) = self
            .lock_artifact_for_sandbox(sandbox_id)
            .await
            .map_err(|status| artifact_backup_lookup_error(status, full_current_instance))?;
        if full_current_instance {
            self.ensure_snapshot_restore_supported("enqueue_artifact_backup")?;
        }
        // Outer Some means the destination was already resolved before adopting
        // an unpinned suspend; its inner None selects the configured env default.
        let mut pre_adoption_upload_config = None;
        let (artifact, skip_ready_lookup) = match req.source {
            Some(crate::proto::enqueue_artifact_backup_request::Source::LocalArtifactId(value)) => {
                let id = value.parse::<LocalArtifactId>().map_err(|err| {
                    Status::invalid_argument(format!("invalid local_artifact_id: {err}"))
                })?;
                let artifact = self
                    .local_artifacts
                    .get(id)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
                    .ok_or_else(|| {
                        Status::failed_precondition(format!("local artifact {id} is not published"))
                    })?;
                (artifact, false)
            }
            Some(crate::proto::enqueue_artifact_backup_request::Source::SnapshotName(value)) => {
                let name =
                    pplx_space_common_core::types::SnapshotName::parse(value).map_err(|err| {
                        Status::invalid_argument(format!("invalid snapshot_name: {err}"))
                    })?;
                let existing = self
                    .local_artifacts
                    .get_named(&sandbox.id, &name)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?;
                if let Some(existing) = existing {
                    (existing, false)
                } else {
                    let adopted_kind = if name.as_str().starts_with("d-") {
                        pplx_space_common_core::types::ArtifactKind::Disk
                    } else if name.as_str().starts_with("f-") {
                        pplx_space_common_core::types::ArtifactKind::Full
                    } else {
                        return Err(Status::failed_precondition(format!(
                            "snapshot {name} does not identify a disk or full artifact"
                        )));
                    };
                    if adopted_kind != expected_kind {
                        return Err(Status::failed_precondition(format!(
                            "requested {expected_kind} backup but snapshot {name} is {adopted_kind}"
                        )));
                    }
                    let local_path =
                        pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                            &sandbox.id,
                            name.as_str(),
                        );
                    if !std::path::Path::new(&local_path).is_dir() {
                        return Err(Status::failed_precondition(format!(
                            "snapshot {name} has no published local artifact"
                        )));
                    }
                    let local_artifact_id = LocalArtifactId::new();
                    self.local_artifacts
                        .publish_named_snapshot(
                            &sandbox.id,
                            local_artifact_id,
                            adopted_kind,
                            &name,
                            &local_path,
                        )
                        .map_err(|err| {
                            Status::internal(format!("db artifact adoption failed: {err}"))
                        })?;
                    self.local_artifacts
                        .get(local_artifact_id)
                        .map_err(|err| {
                            Status::internal(format!("db artifact lookup failed: {err}"))
                        })?
                        .map(|artifact| (artifact, true))
                        .ok_or_else(|| Status::internal("adopted local artifact missing"))?
                }
            }
            Some(crate::proto::enqueue_artifact_backup_request::Source::CurrentInstance(_))
            | None => {
                current_instance_backup_precondition(sandbox.state, expected_kind)?;
                let mut skip_ready_lookup = false;
                let current_id = match (sandbox.state, sandbox.current_local_artifact_id) {
                    (SandboxState::Suspended, None) => {
                        let sandbox_dir =
                            std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
                        let local_path = canonical_suspend_snapshot_dir(&sandbox_dir)
                            .to_string_lossy()
                            .into_owned();
                        require_artifact_backup_files(
                            self.retained_source_inspector.as_ref(),
                            &sandbox.id,
                            &local_path,
                            expected_kind,
                            reupload_format_from_sidecar(&local_path)?,
                            BackupFilesPolicy::Backup {
                                full_current_instance,
                            },
                        )
                        .await?;
                        pre_adoption_upload_config = Some(reupload_resolution_to_config(
                            self.resolve_org_snapshot_upload_config(&sandbox.id).await,
                            self.snapshot_upload_config.is_some(),
                        )?);
                        let id = LocalArtifactId::new();
                        let current_id = self.adopt_or_get_suspended_local_artifact(
                            &sandbox.id,
                            id,
                            &local_path,
                        )?;
                        skip_ready_lookup = current_id == id;
                        current_id
                    }
                    (SandboxState::Stopped, None) => {
                        let id = LocalArtifactId::new();
                        let snapshot_name = format!("d-stop-{id}");
                        let mut node_client =
                            connect_node(&self.node_manager).await.map_err(|error| {
                                Status::internal(format!("connect to node-manager: {error}"))
                            })?;
                        node_client
                            .create_btrfs_snapshot(
                                pplx_space_node_mgr_client::proto::CreateBtrfsSnapshotRequest {
                                    sandbox_id: sandbox.id.clone(),
                                    snapshot_name: snapshot_name.clone(),
                                    writable: false,
                                    execution_fence: sandbox.node_execution_fence(),
                                },
                            )
                            .await
                            .map_err(|error| {
                                Status::internal(format!("adopt stopped disk artifact: {error}"))
                            })?;
                        let local_path =
                            pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
                                &sandbox.id,
                                &snapshot_name,
                            );
                        let sandbox_id = sandbox.id.parse().map_err(|error| {
                            Status::internal(format!("persisted sandbox id is invalid: {error}"))
                        })?;
                        let publish_result = self.apply_route_identity_mutation(
                            sandbox_id,
                            "stopped_artifact_adoption",
                            || {
                                self.local_artifacts
                                    .publish_stopped(&sandbox.id, id, &local_path)
                                    .map(|()| ((), true))
                            },
                        );
                        if let Err(err) = publish_result {
                            if let Err(cleanup_error) = node_client
                                .delete_btrfs_snapshot(
                                    pplx_space_node_mgr_client::proto::DeleteBtrfsSnapshotRequest {
                                        sandbox_id: sandbox.id.clone(),
                                        snapshot_name: snapshot_name.clone(),
                                    },
                                )
                                .await
                            {
                                tracing::warn!(
                                    sandbox_id = %sandbox.id,
                                    snapshot_name,
                                    error = %cleanup_error,
                                    "failed to remove stopped artifact snapshot after adoption db failure"
                                );
                            }
                            return Err(Status::internal(format!(
                                "db artifact adoption failed: {err}"
                            )));
                        }
                        skip_ready_lookup = true;
                        id
                    }
                    (_, Some(id)) => id,
                    _ => {
                        return Err(Status::failed_precondition(
                            "current instance has no published local artifact",
                        ));
                    }
                };
                let artifact = self
                    .local_artifacts
                    .get(current_id)
                    .map_err(|err| Status::internal(format!("db artifact lookup failed: {err}")))?
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "local artifact {current_id} is not published"
                        ))
                    })?;
                (artifact, skip_ready_lookup)
            }
        };
        if artifact.sandbox_id != sandbox.id {
            return Err(Status::failed_precondition(
                "local artifact does not belong to requested sandbox",
            ));
        }
        if artifact.artifact_kind != expected_kind {
            return Err(Status::failed_precondition(format!(
                "requested {expected_kind} backup but published artifact is {}",
                artifact.artifact_kind
            )));
        }
        let existing_ready_backup = if skip_ready_lookup {
            None
        } else {
            self.existing_ready_artifact_backup_response(&artifact)
                .await?
        };
        if let Some(response) = existing_ready_backup {
            tracing::info!(
                sandbox_id = %artifact.sandbox_id,
                local_artifact_id = %artifact.local_artifact_id,
                backup_attempt_id = %response.backup_attempt_id,
                "local artifact backup already ready; skipping duplicate enqueue"
            );
            return Ok(Response::new(response));
        }

        let artifact_dedup_key = artifact.local_artifact_id.to_string();
        let previous_job = self.snapshot_upload_worker.find_job_by_dedup_key(
            crate::snapshot_upload_jobs::SNAPSHOT_UPLOAD_JOB_KIND,
            &artifact_dedup_key,
        )?;
        if let Some(job) = &previous_job {
            let job_status = job
                .status
                .parse::<pplx_space_common_jobs::JobStatus>()
                .map_err(|error| {
                    Status::internal(format!(
                        "persisted artifact backup status is invalid for {}: {error}",
                        artifact.local_artifact_id
                    ))
                })?;
            if let Some(backup_attempt_id) = Self::reusable_artifact_backup_attempt(
                job_status,
                job.failure.is_some(),
                &job.payload,
            )
            .map_err(|error| {
                Status::internal(format!(
                    "persisted artifact backup payload is invalid for {}: {error}",
                    artifact.local_artifact_id
                ))
            })? {
                self.establish_restore_visible_artifact_backup(&job.job_id)
                    .await
                    .map_err(|error| {
                        artifact_backup_submission_status(
                            "establish restore-visible artifact backup",
                            error,
                        )
                    })?;
                tracing::info!(
                    sandbox_id = %artifact.sandbox_id,
                    local_artifact_id = %artifact.local_artifact_id,
                    %backup_attempt_id,
                    snapshot_upload_job_id = %job.job_id,
                    "local artifact backup already in progress; joining existing attempt"
                );
                return Ok(Response::new(EnqueueArtifactBackupResponse {
                    local_artifact_id: artifact.local_artifact_id.to_string(),
                    artifact_kind: match artifact.artifact_kind {
                        pplx_space_common_core::types::ArtifactKind::Disk => {
                            crate::proto::ArtifactKind::Disk as i32
                        }
                        pplx_space_common_core::types::ArtifactKind::Full => {
                            crate::proto::ArtifactKind::Full as i32
                        }
                    },
                    backup_attempt_id,
                }));
            }
        }

        let format = artifact_backup_format(&artifact)?;
        require_artifact_backup_files(
            self.retained_source_inspector.as_ref(),
            &artifact.sandbox_id,
            &artifact.local_path,
            artifact.artifact_kind,
            format,
            BackupFilesPolicy::Backup {
                full_current_instance,
            },
        )
        .await?;

        let parsed_upload_config = match pre_adoption_upload_config {
            Some(config) => config,
            None => reupload_resolution_to_config(
                self.resolve_org_snapshot_upload_config(&sandbox.id).await,
                self.snapshot_upload_config.is_some(),
            )?,
        };
        // A fresh attempt registers under the execution generation the
        // artifact's earlier attempt carried, so a teardown capture the
        // control plane refused stays refused when the backup is requested
        // again by artifact id. The control plane fences only an artifact's
        // first registration, so a capture it admitted once registers again.
        // Every attempt carries the generation forward, so the chain lasts
        // until the jobs are pruned.
        // An earlier upload that can't be decoded can't vouch for its
        // generation, so the re-request is refused rather than sent unfenced.
        let execution_generation = previous_job
            .as_ref()
            .map(|job| job.payload_as::<crate::snapshot_upload::SnapshotUploadJobs>())
            .transpose()
            .map_err(|error| {
                Status::failed_precondition(format!(
                    "earlier artifact backup payload is invalid for {}, so its execution \
                     generation is unknown: {error}",
                    artifact.local_artifact_id
                ))
            })?
            .and_then(|payload| payload.execution_generation);
        // A teardown capture requested before its own upload job exists has no
        // earlier attempt to carry the generation, so take it from the running
        // teardown that staged it: the row still pins the capture, in any state
        // the teardown drive treats as its own (a restart mid-teardown leaves it
        // in cleanup). A suspended row's artifact belongs to an earlier
        // node-local suspend and stays unfenced.
        let execution_generation = match execution_generation {
            Some(generation) => Some(generation),
            None if matches!(
                sandbox.state,
                SandboxState::Suspending
                    | SandboxState::CleanupRequired
                    | SandboxState::CleanupFailed
            ) && sandbox.current_local_artifact_id == Some(artifact.local_artifact_id) =>
            {
                let db = self.db_conn()?;
                crate::db::latest_teardown_suspend_operation(&db, &sandbox.id)
                    .map_err(|error| Status::internal(format!("teardown job lookup: {error}")))?
                    .filter(|operation| {
                        operation.status == crate::db::TeardownSuspendOperationStatus::Running
                    })
                    .map(|operation| operation.execution_generation)
            }
            None => None,
        };
        let backup_attempt_id = pplx_space_common_core::types::BackupAttemptId::new();
        let backup_attempt_id = match self
            .submit_artifact_backup(
                &sandbox,
                ArtifactBackupSubmission {
                    local_artifact_id: artifact.local_artifact_id,
                    artifact_kind: artifact.artifact_kind,
                    artifact_origin: artifact.artifact_origin,
                    snapshot_name: artifact.snapshot_name.as_ref().map(|name| name.as_str()),
                    backup_attempt_id,
                    snapshot_dir: &artifact.local_path,
                    request_override: parsed_upload_config.as_ref(),
                    format,
                    dedup_key: Some(&artifact_dedup_key),
                    handoff: ArtifactBackupHandoff::RestoreVisible,
                    execution_generation,
                },
            )
            .await
            .map_err(|err| {
                artifact_backup_submission_status("artifact backup enqueue failed", err)
            })? {
            SubmitSuspendSnapshotOutcome::Enqueued {
                backup_attempt_id, ..
            } => backup_attempt_id,
            SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                return Err(Status::failed_precondition(
                    "cannot back up artifact: no snapshot upload destination configured",
                ));
            }
        };
        Ok(Response::new(EnqueueArtifactBackupResponse {
            local_artifact_id: artifact.local_artifact_id.to_string(),
            artifact_kind: match artifact.artifact_kind {
                pplx_space_common_core::types::ArtifactKind::Disk => {
                    crate::proto::ArtifactKind::Disk as i32
                }
                pplx_space_common_core::types::ArtifactKind::Full => {
                    crate::proto::ArtifactKind::Full as i32
                }
            },
            backup_attempt_id,
        }))
    }

    /// Queue a fresh backup for a legacy suspended sandbox so background
    /// eviction can later prove and remove its local cache presence.
    pub(crate) async fn reupload_legacy_sandbox_for_eviction(
        &self,
        sandbox_id: &str,
    ) -> Result<JobId, Status> {
        let sandbox_id = sandbox_id.trim();
        if sandbox_id.is_empty() {
            return Err(Status::invalid_argument("sandbox_id is required"));
        }
        self.enqueue_suspend_snapshot_reupload(
            sandbox_id,
            SuspendedArtifactAdoption::PreserveUpdatedAt,
        )
        .await
    }

    pub(super) async fn lock_artifact_for_sandbox(
        &self,
        selector: &str,
    ) -> Result<
        (
            crate::db::SandboxRow,
            crate::auto_lifecycle::LifecycleMutexGuard,
        ),
        Status,
    > {
        let id = self.lookup_sandbox(selector)?.id;
        let guard = super::restart_admission::wait_for_activation_admission(
            &id,
            self.auto_lifecycle.artifact_lock(&id).lock_owned(),
        )
        .await?;
        // Re-resolve only the canonical identity: the selector may match a
        // different sandbox after waiting behind deletion or publication.
        let sandbox = self.lookup_sandbox(&id)?;
        Ok((sandbox, guard))
    }

    async fn enqueue_suspend_snapshot_reupload(
        &self,
        sandbox_id: &str,
        adoption: SuspendedArtifactAdoption,
    ) -> Result<JobId, Status> {
        // Cluster-api wiring is a hard precondition for reupload: the
        // durable worker's registration step uses the
        // same `connect_from_env` and treats MissingEndpoint/MissingToken
        // as `JobError::permanent`. Without this guard we'd return a job
        // id for an enqueue that can never close the loop (no `ready`
        // snapshot row would land), so the reconciler would never see
        // promotion progress.
        if let Some(status) = reupload_precondition_for_cluster_api_env(
            cluster_api_env_var_set("SPACE_INTERNAL_API_ENDPOINT"),
            cluster_api_env_var_set("SPACE_INTERNAL_TOKEN"),
        ) {
            return Err(status);
        }
        // Re-wrap `NotFound` from the local sqlite lookup as
        // `FailedPrecondition` carrying `REUPLOAD_SANDBOX_UNKNOWN_MARKER`.
        // This is a terminal condition for promotion: the sandbox isn't
        // on this node, so reupload can never succeed here. The cluster
        // controller's classifier keys off the marker (same pattern as
        // `REUPLOAD_ARTIFACT_MISSING_MARKER`) and flips the sandbox to
        // `failed`. We deliberately don't return raw `NotFound` because
        // the substrate adapter also surfaces endpoint-resolution
        // failures as `SubstrateError::NotFound`, which must stay
        // retryable.
        let (sandbox, _guard) =
            self.lock_artifact_for_sandbox(sandbox_id)
                .await
                .map_err(|status| {
                    if status.code() == tonic::Code::NotFound {
                        Status::failed_precondition(format!(
                    "cannot reupload snapshot: {marker} for sandbox_id={sandbox_id} \
                     — sbx-mgr has no record of this sandbox on this node",
                    marker =
                        pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_UNKNOWN_MARKER,
                ))
                    } else {
                        status
                    }
                })?;
        self.ensure_snapshot_restore_supported("reupload_suspend_snapshot")?;

        if sandbox.state != SandboxState::Suspended {
            // The cluster-controller's node-drain local-snapshot promotion
            // classifies ONLY the `restoring` rejection on
            // `REUPLOAD_SANDBOX_RESTORING_MARKER` to take its give-up path: a
            // permanently-refused reupload from a stuck on-node `restoring`
            // state would otherwise wedge the node's drain forever, and the
            // snapshot is canonical (the VM is loading from it, holds no newer
            // state). Render that exact phrase via the shared constant —
            // byte-identical to the historical message for compatibility with
            // older sandbox-managers. Every other non-suspended
            // state (started/paused — live state; stopped — possibly-modified
            // disk) gets a generic message carrying no marker, so the give-up
            // never abandons their potentially-newer state.
            let msg = if sandbox.state == SandboxState::Restoring {
                format!(
                    "cannot reupload snapshot: {}",
                    pplx_space_common_core::node_drain_signals::REUPLOAD_SANDBOX_RESTORING_MARKER,
                )
            } else {
                format!(
                    "cannot reupload snapshot: sandbox is {}, must be suspended",
                    sandbox.state
                )
            };
            return Err(Status::failed_precondition(msg));
        }

        let sandbox_dir = std::path::PathBuf::from(format!("{CACHE_DIR}/{}", sandbox.id));
        let snapshot_dir = canonical_suspend_snapshot_dir(&sandbox_dir)
            .to_string_lossy()
            .into_owned();
        // The reupload codec must match what the original suspend wrote.
        // Sniff the on-disk sidecar to learn it; absence of the file
        // (older snapshots that pre-date the sidecar) is treated as
        // Full, the historical shape. The codec choice changes only
        // which rootfs artifact filename we require on disk
        // (`rootfs.ext4` for Full, `rootfs.btrfs-send` for BtrfsSend).
        let reupload_format = reupload_format_from_sidecar(&snapshot_dir)?;
        // Exactly the VM artifacts `submit_suspend_snapshot` reads off disk;
        // `snapshot.json` is regenerated at submit time and stays out of it.
        require_artifact_backup_files(
            self.retained_source_inspector.as_ref(),
            &sandbox.id,
            &snapshot_dir,
            pplx_space_common_core::types::ArtifactKind::Full,
            reupload_format,
            BackupFilesPolicy::Reupload,
        )
        .await?;

        // Backup admission must distinguish a transient cluster-api lookup
        // failure (retryable — surface as `Internal` so the reconciler keeps
        // ticking) from a definitively-absent override (terminal —
        // `FailedPrecondition` if there's also no env fallback). Collapsing
        // both into `Option<_>` would let a transient blip masquerade as
        // a terminal "no destination configured" and trigger spurious
        // mark-as-failed by the reconciler's classifier.
        let resolution = self.resolve_org_snapshot_upload_config(&sandbox.id).await;
        let parsed_upload_config =
            reupload_resolution_to_config(resolution, self.snapshot_upload_config.is_some())?;

        let local_artifact_id = match sandbox.current_local_artifact_id {
            Some(local_artifact_id) => local_artifact_id,
            None => {
                let local_artifact_id = LocalArtifactId::new();
                match adoption {
                    SuspendedArtifactAdoption::TouchUpdatedAt => self
                        .adopt_or_get_suspended_local_artifact(
                            &sandbox.id,
                            local_artifact_id,
                            &snapshot_dir,
                        )?,
                    SuspendedArtifactAdoption::PreserveUpdatedAt => self
                        .adopt_or_get_suspended_local_artifact_preserving_updated_at(
                            &sandbox.id,
                            local_artifact_id,
                            &snapshot_dir,
                        )?,
                }
            }
        };
        let backup_attempt_id = pplx_space_common_core::types::BackupAttemptId::new();
        let outcome = self
            .submit_artifact_backup(
                &sandbox,
                ArtifactBackupSubmission {
                    local_artifact_id,
                    artifact_kind: pplx_space_common_core::types::ArtifactKind::Full,
                    artifact_origin: pplx_space_common_core::types::ArtifactOrigin::Suspend,
                    snapshot_name: None,
                    backup_attempt_id,
                    snapshot_dir: &snapshot_dir,
                    request_override: parsed_upload_config.as_ref(),
                    format: reupload_format,
                    dedup_key: None,
                    handoff: ArtifactBackupHandoff::DurableWorker,
                    execution_generation: None,
                },
            )
            .await
            .map_err(|e| {
                tracing::error!(
                    sandbox_id = %sandbox.id,
                    error = %e,
                    "reupload_suspend_snapshot: durable worker submission failed",
                );
                Status::internal(format!("snapshot upload enqueue failed: {e}"))
            })?;
        let job_id = match outcome {
            SubmitSuspendSnapshotOutcome::Enqueued { job_id, .. } => job_id,
            SubmitSuspendSnapshotOutcome::NoUploadConfig => {
                // The earlier guard already rejected this case; preserve
                // the invariant defensively in case the request override
                // resolution races with config init.
                return Err(Status::failed_precondition(
                    "cannot reupload snapshot: no snapshot upload destination configured",
                ));
            }
        };
        Ok(job_id)
    }
}

/// The status an `EnqueueArtifactBackup` submission failure answers with. A
/// refusal of a superseded execution keeps the control plane's verdict, since
/// a retry by artifact id is refused again while the capture's earlier upload
/// is retained; anything else is internal.
fn artifact_backup_submission_status(
    context: &str,
    error: ArtifactBackupSubmissionError,
) -> Status {
    match error {
        ArtifactBackupSubmissionError::Begin {
            superseded_generation: Some(generation),
            ref error,
        } => pplx_space_node_api_client::cluster::backup_execution_superseded_status(
            format!("{context}: {error}"),
            generation,
        ),
        error => Status::internal(format!("{context}: {error}")),
    }
}
