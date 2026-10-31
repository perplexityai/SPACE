use super::cleanup_support::CleanupLockTryResult;
use super::create_support::{validate_path_component, validate_snapshot_name};
use super::*;

#[cfg(test)]
mod tests;

fn validate_imported_snapshot_rootfs(
    staging: &std::path::Path,
    receiver: &super::grpc_sandbox_transfer::receive::SparseImport,
) -> Result<(), Status> {
    // Absence is supported for memory-only full transfers. A supplied disk,
    // including a reconstructed delta, must not publish an empty file: older
    // node-managers check only existence before replacing the live instance.
    if receiver.contains("rootfs.ext4") || receiver.contains("rootfs.ext4.diff") {
        pplx_space_node_runtime_core::snapshot_files::validate_snapshot_rootfs(staging)
            .map_err(|error| Status::data_loss(format!("invalid imported rootfs: {error}")))?;
    }
    Ok(())
}

impl SandboxManagerService {
    /// The importer holds this same keyed artifact lock through stream receipt,
    /// publication and cleanup. A crashed or failed owner leaves reclaimable
    /// staging without exposing an incomplete canonical sandbox directory.
    pub(super) fn reclaim_abandoned_transfer_staging_entry(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<bool> {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            return Ok(false);
        };
        let Some(ids) = name.strip_prefix(".snapshot-import-") else {
            return Ok(false);
        };
        let Some((sandbox, transfer)) = ids.split_once('_') else {
            return Ok(false);
        };
        let (Ok(sandbox), Ok(transfer)) =
            (sandbox.parse::<SandboxId>(), transfer.parse::<uuid::Uuid>())
        else {
            return Ok(false);
        };
        if transfer.get_version_num() != 7 {
            return Ok(false);
        }
        let artifact = self.auto_lifecycle.artifact_lock(&sandbox.to_string());
        let Ok(_guard) = artifact.try_lock_owned() else {
            return Ok(false);
        };
        let metadata = std::fs::symlink_metadata(path)?;
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            super::grpc_sandbox_transfer::receive::remove_transfer_directory(path)?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(super) async fn export_snapshot(
        &self,
        request: Request<ExportSnapshotRequest>,
    ) -> Result<Response<SbxExportSnapshotStream>, Status> {
        self.authorize_peer(&request, "ExportSnapshot", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let req = request.into_inner();
        let id = self.lookup_sandbox(&req.sandbox_id)?.id;
        let wake = self.auto_lifecycle.wake_lock(&id).lock_owned().await;
        let artifact = self.auto_lifecycle.artifact_lock(&id).lock_owned().await;
        let CleanupLockTryResult::Acquired(fence) = self.try_cleanup_fence(&id) else {
            return Err(Status::aborted("sandbox cleanup is in progress"));
        };
        let sandbox = self.lookup_sandbox(&id)?;

        let snapshot_name = req.snapshot_name.clone();
        if snapshot_name.is_empty() {
            return Err(Status::invalid_argument("snapshot_name is required"));
        }
        validate_snapshot_name(&snapshot_name)?;

        // Both d-* (disk) and f-* (full) snapshots are exportable.
        // d-* contains rootfs.ext4 only, f-* contains rootfs.ext4 + VM state files.

        let snapshot_dir = pplx_space_common_core::host::linux::paths::sandbox_snapshot_dir(
            &sandbox.id,
            &snapshot_name,
        );
        crate::db::transfer::validate_snapshot_binding(
            std::path::Path::new(&snapshot_dir),
            &sandbox,
        )
        .map_err(|error| Status::failed_precondition(format!("snapshot identity: {error}")))?;
        if !std::path::Path::new(&snapshot_dir).exists() {
            return Err(Status::not_found(format!(
                "snapshot dir not found: {snapshot_name}"
            )));
        }

        let sandbox_id = sandbox.id.clone();
        let template = sandbox.template.clone();
        let template_hash = sandbox.template_hash.clone().ok_or_else(|| {
            Status::failed_precondition("sandbox has no pinned template content hash")
        })?;
        let include_rootfs = req.include_rootfs;
        let use_delta = req.use_delta;
        let writer = self.state_writer_lease("ExportSnapshot")?;
        let snapshot_key = snapshot_name
            .parse::<pplx_space_common_core::types::SnapshotName>()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let lease = self
            .local_artifacts
            .get_named(&id, &snapshot_key)
            .map_err(|error| Status::internal(format!("inspect snapshot publication: {error}")))?
            .map(|publication| {
                self.local_artifacts
                    .check_out_for_restore(publication.local_artifact_id)
                    .map_err(|error| {
                        Status::failed_precondition(format!("snapshot is in use: {error}"))
                    })
            })
            .transpose()?;

        let (tx, stream) = super::grpc_sandbox_transfer::receive::export_channel();

        let service = self.clone();
        let errors = tx.clone();
        self.task_tracker
            .spawn(super::drain::with_state_writer(writer, async move {
                let result = service
                    .blocking_export_io(move |_| {
                        let _ownership = (wake, artifact, fence, lease);
                        (|| {
                            // Determine snapshot type from name prefix (f- = full, d- = disk)
                            let is_full = snapshot_name.starts_with("f-");
                            let snap_type = if is_full {
                                crate::proto::SnapshotType::Full
                            } else {
                                crate::proto::SnapshotType::Disk
                            };

                            // Read sidecar metadata if present
                            let metadata = std::fs::read(format!("{snapshot_dir}/snapshot.json"))
                                .unwrap_or_default();

                            // Send header
                            let header = ExportSnapshotChunk {
                                payload: Some(
                                    crate::proto::export_snapshot_chunk::Payload::Header(
                                        crate::proto::ExportSnapshotHeader {
                                            sandbox_id: sandbox_id.clone(),
                                            template_id: template.clone(),
                                            content_hash: template_hash.clone(),
                                            snapshot_metadata: metadata,
                                            snapshot_type: snap_type.into(),
                                        },
                                    ),
                                ),
                            };
                            if tx.blocking_send(Ok(header)).is_err() {
                                return;
                            }

                            // Build file list based on snapshot type
                            let mut files: Vec<(&str, String, bool)> = Vec::new();
                            files.push((
                                "snapshot.json",
                                format!("{snapshot_dir}/snapshot.json"),
                                false,
                            ));

                            if is_full {
                                files.push(("vmstate", format!("{snapshot_dir}/vmstate"), true));
                                files.push(("memory", format!("{snapshot_dir}/memory"), true));
                            }

                            // Track diff cleanup for rootfs delta
                            struct DiffCleanup(Option<String>);
                            impl Drop for DiffCleanup {
                                fn drop(&mut self) {
                                    if let Some(ref p) = self.0 {
                                        let _ = std::fs::remove_file(p);
                                    }
                                }
                            }
                            let mut _rootfs_diff_guard = DiffCleanup(None);

                            if include_rootfs {
                                // Prefer rootfs from the snapshot itself (full snapshots are btrfs clones of instance/)
                                let snap_rootfs = format!("{snapshot_dir}/rootfs.ext4");
                                let rootfs_path = snap_rootfs;
                                if !std::path::Path::new(&rootfs_path).is_file() {
                                    let _ = tx.blocking_send(Err(Status::failed_precondition(
                                        "snapshot rootfs is missing",
                                    )));
                                    return;
                                }

                                if use_delta && std::path::Path::new(&rootfs_path).exists() {
                                    let template_rootfs =
                        pplx_space_common_core::host::linux::paths::cached_template_rootfs_path(
                            &template,
                            &template_hash,
                        );
                                    if std::path::Path::new(&template_rootfs).exists() {
                                        let diff_id = uuid::Uuid::now_v7();
                                        let diff_path =
                                            format!("{snapshot_dir}/.rootfs.diff.{diff_id}.tmp");
                                        _rootfs_diff_guard = DiffCleanup(Some(diff_path.clone()));
                                        if pplx_space_node_runtime_core::file_delta::compute_diff(
                                            std::path::Path::new(&template_rootfs),
                                            std::path::Path::new(&rootfs_path),
                                            std::path::Path::new(&diff_path),
                                        )
                                        .is_ok()
                                        {
                                            files.push(("rootfs.ext4.diff", diff_path, true));
                                        } else {
                                            files.push(("rootfs.ext4", rootfs_path, true));
                                        }
                                    } else {
                                        files.push(("rootfs.ext4", rootfs_path, true));
                                    }
                                } else if std::path::Path::new(&rootfs_path).exists() {
                                    files.push(("rootfs.ext4", rootfs_path, true));
                                }
                            }

                            // Stream files in chunks
                            for (file_name, path, required) in files {
                                if let Err(error) =
                                    super::grpc_sandbox_transfer::receive::stream_sparse_file(
                                        std::path::Path::new(&path),
                                        required,
                                        || tx.is_closed(),
                                        |offset, data, eof| {
                                            tx.blocking_send(Ok(ExportSnapshotChunk {
            payload: Some(crate::proto::export_snapshot_chunk::Payload::Chunk(
                crate::proto::ExportSnapshotFileChunk {
                    file_name: file_name.to_string(), offset, data, eof,
                },
            )),
        })).map_err(|_| Status::cancelled("export consumer disconnected"))
                                        },
                                    )
                                {
                                    let _ = tx.blocking_send(Err(error));
                                    return;
                                }
                            }
                        })();
                        Ok(())
                    })
                    .await;
                errors.finish(result);
            }));

        Ok(Response::new(stream))
    }

    pub(super) async fn import_snapshot(
        &self,
        request: Request<tonic::Streaming<ImportSnapshotRequest>>,
    ) -> Result<Response<ImportSnapshotResponse>, Status> {
        self.authorize_peer(&request, "ImportSnapshot", ALLOW_LOCAL_ARTIFACT_OPERATOR)?;
        let service = self.clone();
        let owned = super::restart_admission::inherit_admission_cancellation(async move {
            service.import_snapshot_owned(request.into_inner()).await
        });
        self.spawn_state_writer("ImportSnapshot", owned)?
            .await
            .map_err(|error| Status::internal(format!("snapshot import task: {error}")))?
    }

    async fn import_snapshot_owned(
        &self,
        mut stream: tonic::Streaming<ImportSnapshotRequest>,
    ) -> Result<Response<ImportSnapshotResponse>, Status> {
        use super::grpc_sandbox_transfer::receive;
        let deadline = receive::ImportDeadline::new();
        let first = receive::next_message(&mut stream, deadline)
            .await?
            .ok_or_else(|| Status::invalid_argument("empty import stream"))?;
        let Some(crate::proto::import_snapshot_request::Payload::Header(header)) = first.payload
        else {
            return Err(Status::invalid_argument(
                "first message must be an ImportSnapshotHeader",
            ));
        };
        let sandbox_id = header
            .sandbox_id
            .parse::<SandboxId>()
            .map_err(|error| Status::invalid_argument(format!("invalid sandbox id: {error}")))?
            .to_string();
        validate_path_component("template_id", &header.template_id)?;
        validate_path_component("content_hash", &header.content_hash)?;
        let snapshot_type = super::validation::snapshot_type_from_proto(header.snapshot_type)?;
        let prefix = if snapshot_type == crate::proto::SnapshotType::Disk {
            "d"
        } else {
            "f"
        };
        let name = match header
            .target_name
            .as_deref()
            .filter(|name| !name.is_empty())
        {
            Some(name) => {
                validate_snapshot_name(name)?;
                if !name.starts_with(&format!("{prefix}-")) {
                    return Err(Status::invalid_argument(
                        "snapshot name does not match snapshot type",
                    ));
                }
                name.to_owned()
            }
            None => format!("{prefix}-imported-{}", uuid::Uuid::now_v7()),
        };
        let _wake = super::restart_admission::wait_for_activation_admission(
            &sandbox_id,
            self.auto_lifecycle.wake_lock(&sandbox_id).lock_owned(),
        )
        .await?;
        let _artifact = super::restart_admission::wait_for_activation_admission(
            &sandbox_id,
            self.auto_lifecycle.artifact_lock(&sandbox_id).lock_owned(),
        )
        .await?;
        let CleanupLockTryResult::Acquired(_fence) = self.try_cleanup_fence(&sandbox_id) else {
            return Err(Status::aborted("sandbox cleanup is in progress"));
        };
        let lookup_id = sandbox_id.clone();
        let sandbox = self
            .storage(move |db| {
                crate::db::read::sandbox(db, &lookup_id)
                    .map_err(|error| Status::internal(format!("inspect snapshot target: {error}")))
            })
            .await?
            .ok_or_else(|| {
                Status::failed_precondition("import the sandbox before its snapshots")
            })?;
        if sandbox.template != header.template_id
            || sandbox.template_hash.as_deref() != Some(header.content_hash.as_str())
            || sandbox.volume_encryption
            || matches!(
                sandbox.state,
                SandboxState::Creating
                    | SandboxState::Deleting
                    | SandboxState::Stopping
                    | SandboxState::Quarantined
            )
        {
            return Err(Status::failed_precondition(
                "snapshot target identity or state does not match",
            ));
        }
        let base = pplx_space_common_core::host::linux::paths::sandbox_snapshots_dir(&sandbox_id);
        let final_dir = format!("{base}/{name}");
        match tokio::fs::symlink_metadata(&final_dir).await {
            Ok(metadata)
                if metadata.is_dir()
                    && !metadata.file_type().is_symlink()
                    && tokio::fs::metadata(format!("{final_dir}/.transfer.json"))
                        .await
                        .is_ok() => {}
            Ok(_) => {
                return Err(Status::already_exists(
                    "snapshot already exists without transfer evidence",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(Status::internal(format!(
                    "inspect snapshot destination: {error}"
                )));
            }
        }
        let staging = format!(
            "{}/.snapshot-import-{sandbox_id}_{}",
            pplx_space_common_core::host::linux::paths::SANDBOX_CACHE_DIR,
            uuid::Uuid::now_v7()
        );
        let staging_claim = staging.clone();
        self.blocking_artifact_io(move |_| {
            if snapshot_type == crate::proto::SnapshotType::Full {
                // Full transfers may intentionally omit rootfs. Older node-managers
                // check disk completeness only for plain directories, so importing
                // these as subvolumes could let rollback destroy a live instance.
                // Keep full imports plain even when this transfer includes rootfs;
                // disk restoration from them still uses reflink copies.
                std::fs::create_dir(&staging_claim)
                    .map_err(|error| Status::internal(format!("create snapshot staging: {error}")))
            } else {
                super::grpc_sandbox_transfer::receive::create_transfer_directory(
                    std::path::Path::new(&staging_claim),
                )
            }
        })
        .await?;
        let result = async {
            let mut receiver = super::grpc_sandbox_transfer::receive::SparseImport::default();
            while let Some(message) = receive::next_message(&mut stream, deadline).await? {
                let Some(crate::proto::import_snapshot_request::Payload::Chunk(chunk)) =
                    message.payload
                else {
                    return Err(Status::data_loss("expected file chunk after import header"));
                };
                let name = &chunk.file_name;
                if name.is_empty()
                    || name == "."
                    || name.contains("..")
                    || name.contains('/')
                    || name.contains('\\')
                {
                    return Err(Status::invalid_argument(
                        "invalid snapshot transfer file name",
                    ));
                }
                receiver
                    .write(
                        name,
                        &std::path::Path::new(&staging).join(name),
                        chunk.offset,
                        &chunk.data,
                        chunk.eof,
                    )
                    .await?;
            }
            let bytes = receiver.finish()?;
            if prefix == "f" && (!receiver.nonempty("vmstate") || !receiver.nonempty("memory")) {
                return Err(Status::data_loss(
                    "full snapshot is missing vmstate or memory",
                ));
            }
            if receiver.contains("rootfs.ext4") && receiver.contains("rootfs.ext4.diff") {
                return Err(Status::data_loss(
                    "snapshot contains both full and delta rootfs",
                ));
            }
            if prefix == "d"
                && !receiver.nonempty("rootfs.ext4")
                && !receiver.nonempty("rootfs.ext4.diff")
            {
                return Err(Status::data_loss("disk snapshot is missing rootfs"));
            }
            if !header.snapshot_metadata.is_empty() {
                let path = format!("{staging}/snapshot.json");
                if receiver.contains("snapshot.json") {
                    if tokio::fs::read(&path).await.map_err(|error| {
                        Status::internal(format!("read snapshot metadata: {error}"))
                    })? != header.snapshot_metadata
                    {
                        return Err(Status::data_loss("snapshot metadata differs from header"));
                    }
                } else {
                    tokio::fs::write(&path, &header.snapshot_metadata)
                        .await
                        .map_err(|error| {
                            Status::internal(format!("write snapshot metadata: {error}"))
                        })?;
                }
            }
            if receiver.contains("rootfs.ext4.diff") {
                let template =
                    pplx_space_common_core::host::linux::paths::cached_template_rootfs_path(
                        &header.template_id,
                        &header.content_hash,
                    );
                let diff = format!("{staging}/rootfs.ext4.diff");
                let dest = format!("{staging}/rootfs.ext4");
                self.blocking_artifact_io(move |_| {
                    pplx_space_node_runtime_core::file_delta::apply_diff_with_limit(
                        std::path::Path::new(&template),
                        std::path::Path::new(&diff),
                        std::path::Path::new(&dest),
                        receive::MAX_TRANSFER_FILE_BYTES,
                    )
                    .map_err(|error| {
                        Status::failed_precondition(format!("apply rootfs delta: {error}"))
                    })?;
                    std::fs::remove_file(diff).map_err(|error| {
                        Status::internal(format!("remove rootfs delta: {error}"))
                    })?;
                    Ok(())
                })
                .await?;
            }
            let binding = crate::db::transfer::TransferBinding {
                sandbox_id: sandbox_id.clone(),
                template: header.template_id.clone(),
                content_hash: header.content_hash.clone(),
                stream_fingerprint: receiver.fingerprint(&header.snapshot_metadata),
            };
            let binding_bytes = serde_json::to_vec(&binding)
                .map_err(|error| Status::internal(format!("encode transfer binding: {error}")))?;
            let binding_path = format!("{staging}/.transfer.json");
            use tokio::io::AsyncWriteExt;
            let mut binding_file = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(binding_path)
                .await
                .map_err(|error| Status::internal(format!("create transfer binding: {error}")))?;
            binding_file
                .write_all(&binding_bytes)
                .await
                .map_err(|error| Status::internal(format!("write transfer binding: {error}")))?;
            // sync_all waits for the queued write but drops its error.
            binding_file
                .flush()
                .await
                .map_err(|error| Status::internal(format!("write transfer binding: {error}")))?;
            binding_file
                .sync_all()
                .await
                .map_err(|error| Status::internal(format!("sync transfer binding: {error}")))?;
            let publish_id = sandbox_id.clone();
            let template = header.template_id.clone();
            let hash = header.content_hash.clone();
            let staged = staging.clone();
            let target = final_dir.clone();
            let comparison_stage = staging.clone();
            let comparison_target = final_dir.clone();
            let verified_existing = self
                .blocking_artifact_io(move |_| {
                    validate_imported_snapshot_rootfs(
                        std::path::Path::new(&comparison_stage),
                        &receiver,
                    )?;
                    crate::db::transfer::sync_import_directory(std::path::Path::new(
                        &comparison_stage,
                    ))
                    .map_err(|error| {
                        Status::internal(format!("sync imported snapshot: {error}"))
                    })?;
                    receive::verify_existing_snapshot(
                        std::path::Path::new(&comparison_stage),
                        std::path::Path::new(&comparison_target),
                    )
                    .map_err(|error| {
                        Status::already_exists(format!("snapshot retry does not match: {error}"))
                    })
                })
                .await?;
            if !self
                .storage(move |db| {
                    crate::db::transfer::publish_snapshot(
                        db,
                        crate::db::transfer::SnapshotPublication {
                            id: &publish_id,
                            template: &template,
                            hash: &hash,
                            staging: std::path::Path::new(&staged),
                            destination: std::path::Path::new(&target),
                            binding: &binding,
                            verified_existing,
                        },
                    )
                    .map_err(|error| Status::internal(format!("publish snapshot: {error}")))
                })
                .await?
            {
                return Err(Status::aborted(
                    "snapshot destination identity changed during transfer",
                ));
            }
            Ok(bytes)
        }
        .await;
        let size_bytes = match result {
            Ok(bytes) => {
                if tokio::fs::symlink_metadata(&staging).await.is_ok() {
                    let cleanup_staging = staging.clone();
                    if let Err(error) = self
                        .blocking_artifact_io(move |_| {
                            receive::remove_transfer_directory(std::path::Path::new(
                                &cleanup_staging,
                            ))
                            .map_err(|error| Status::internal(error.to_string()))
                        })
                        .await
                    {
                        tracing::warn!(%error, %staging, "snapshot retry staging cleanup deferred");
                    }
                }
                bytes
            }
            Err(error) => {
                let cleanup_staging = staging.clone();
                if let Err(cleanup_error) = self
                    .blocking_artifact_io(move |_| {
                        super::grpc_sandbox_transfer::receive::remove_transfer_directory(
                            std::path::Path::new(&cleanup_staging),
                        )
                        .map_err(|error| Status::internal(error.to_string()))
                    })
                    .await
                {
                    tracing::warn!(%cleanup_error, %staging, "snapshot import staging cleanup failed");
                }
                return Err(error);
            }
        };
        Ok(Response::new(ImportSnapshotResponse {
            sandbox_id,
            content_hash: header.content_hash,
            local_path: final_dir,
            size_bytes,
        }))
    }
}
