use super::*;
use crate::rootfs_backing::RootfsBackingPolicy;

fn template_import_target_path(
    staging_dir: &str,
    file_name: &str,
) -> Result<Option<PathBuf>, Status> {
    match file_name {
        TEMPLATE_ROOTFS_FILE | TEMPLATE_ROOTFS_ZSTD_FILE | "metadata.json" | "manifest.json" => {
            Ok(Some(Path::new(staging_dir).join(file_name)))
        }
        _ => Ok(None),
    }
}

fn import_template_response(
    template_id: &str,
    content_hash: &str,
) -> Response<crate::proto::ImportTemplateResponse> {
    Response::new(crate::proto::ImportTemplateResponse {
        template: Some(crate::proto::TemplateRef {
            name: template_id.to_string(),
            content_hash: Some(content_hash.to_string()),
            tag: None,
        }),
    })
}

fn parse_template_manifest_value(context: &str, bytes: &[u8]) -> Result<serde_json::Value, Status> {
    let json = std::str::from_utf8(bytes).map_err(|error| {
        Status::invalid_argument(format!("{context} is not valid UTF-8: {error}"))
    })?;
    serde_json::from_str(json)
        .map_err(|error| Status::invalid_argument(format!("{context} is not valid JSON: {error}")))
}

pub(super) fn template_manifest_identity_conflicts(
    existing: &serde_json::Value,
    incoming: &serde_json::Value,
) -> bool {
    let identity = |value: &serde_json::Value| {
        let mut value = value.clone();
        if let Some(object) = value.as_object_mut() {
            object.remove("build_date");
            if let Some(files) = object
                .get_mut("files")
                .and_then(serde_json::Value::as_object_mut)
            {
                files.remove(TEMPLATE_ROOTFS_ZSTD_FILE);
            }
        }
        value
    };
    identity(existing) != identity(incoming)
}

pub(super) fn validate_existing_refresh_metadata(
    digest_path: &Path,
    staging_path: &Path,
) -> Result<(), Status> {
    // Only descriptive fields may refresh through import. Execution, sizing,
    // lineage and unknown fields retain the locally authorized values.
    let identity = |path: &Path| -> Result<serde_json::Value, Status> {
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => b"{}".to_vec(),
            Err(error) => {
                return Err(Status::internal(format!(
                    "read {}: {error}",
                    path.display()
                )));
            }
        };
        let mut value = parse_template_manifest_value("template metadata", &bytes)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| Status::invalid_argument("template metadata must be an object"))?;
        object.remove("description");
        object.remove("built_at");
        Ok(value)
    };
    if identity(&digest_path.join("metadata.json"))?
        != identity(&staging_path.join("metadata.json"))?
    {
        return Err(Status::already_exists(
            "existing build has different execution or identity metadata",
        ));
    }
    let existing = std::fs::read(digest_path.join("manifest.json"))
        .map_err(|error| Status::data_loss(format!("read existing manifest: {error}")))?;
    let incoming = std::fs::read(staging_path.join("manifest.json"))
        .map_err(|error| Status::internal(format!("read staged manifest: {error}")))?;
    if template_manifest_identity_conflicts(
        &parse_template_manifest_value("existing manifest", &existing)?,
        &parse_template_manifest_value("staged manifest", &incoming)?,
    ) {
        return Err(Status::already_exists(
            "existing build has different manifest identity fields",
        ));
    }
    Ok(())
}

pub(super) fn parse_template_manifest(
    context: &str,
    bytes: &[u8],
) -> Result<pplx_space_common_core::types::TemplateManifest, Status> {
    if bytes.is_empty() {
        return Ok(pplx_space_common_core::types::TemplateManifest::default());
    }
    let json = std::str::from_utf8(bytes).map_err(|error| {
        Status::invalid_argument(format!("{context} is not valid UTF-8: {error}"))
    })?;
    serde_json::from_str(json)
        .map_err(|error| Status::invalid_argument(format!("{context} is not valid JSON: {error}")))
}

struct ImportedRootfs {
    expanded_size: u64,
    compressed_hash: String,
    compressed_size: u64,
}

fn required_snapshot_dimensions(
    header: &crate::proto::ImportTemplateHeader,
) -> Result<(u32, u32), Status> {
    optional_snapshot_dimensions(header.snapshot_vcpu, header.snapshot_memory_mib)?.ok_or_else(
        || {
            Status::invalid_argument(
                "snapshot_vcpu and snapshot_memory_mib are required when include_snapshot is true",
            )
        },
    )
}

pub(super) fn import_chunk_target_path(
    staging_dir: &str,
    snap_dir: Option<&str>,
    file_name: &str,
) -> Result<Option<PathBuf>, Status> {
    let is_snapshot_file = matches!(
        file_name,
        "snapshot.base" | "vmstate.base" | "snapshot-metadata.json"
    );
    let template_target = template_import_target_path(staging_dir, file_name)?;

    if template_target.is_none() && !is_snapshot_file {
        return Err(Status::invalid_argument(format!(
            "unexpected file: {file_name}"
        )));
    }

    if is_snapshot_file {
        return Ok(snap_dir.map(|sd| {
            Path::new(sd).join(if file_name == "snapshot-metadata.json" {
                "metadata.json"
            } else {
                file_name
            })
        }));
    }

    Ok(template_target)
}

async fn write_import_chunk(
    file_handles: &mut std::collections::HashMap<String, (tokio::fs::File, u64)>,
    target_path: PathBuf,
    chunk: crate::proto::FileChunk,
) -> Result<(), Status> {
    use tokio::io::AsyncWriteExt;

    let handle_key = target_path.display().to_string();

    if chunk.eof {
        if let Some((mut file, _)) = file_handles.remove(&handle_key) {
            file.flush()
                .await
                .map_err(|e| Status::internal(format!("flush file: {e}")))?;
        }
        return Ok(());
    }

    if !file_handles.contains_key(&handle_key) {
        if let Some(parent) = target_path.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                Status::internal(format!("create parent dir {}: {e}", parent.display()))
            })?;
        }
        let file = tokio::fs::File::create(&target_path)
            .await
            .map_err(|e| Status::internal(format!("create file {}: {e}", target_path.display())))?;
        file_handles.insert(handle_key.clone(), (file, 0));
    }

    let (mut file, mut expected_offset) = file_handles.remove(&handle_key).ok_or_else(|| {
        Status::internal(format!(
            "missing open import file handle for {}",
            target_path.display()
        ))
    })?;

    if chunk.offset != expected_offset {
        return Err(Status::invalid_argument(format!(
            "non-sequential offset for {}: expected {}, got {}",
            chunk.file_name, expected_offset, chunk.offset
        )));
    }

    file.write_all(&chunk.data)
        .await
        .map_err(|e| Status::internal(format!("write chunk: {e}")))?;
    let chunk_len = u64::try_from(chunk.data.len()).map_err(|_| {
        Status::internal(format!(
            "chunk for {} is too large to track offset",
            chunk.file_name
        ))
    })?;
    expected_offset = expected_offset
        .checked_add(chunk_len)
        .ok_or_else(|| Status::internal(format!("offset overflow for {}", chunk.file_name)))?;
    file_handles.insert(handle_key, (file, expected_offset));
    Ok(())
}

async fn materialize_compressed_imported_rootfs(
    template_id: &str,
    content_hash: &str,
    rootfs_path: &Path,
    compressed_rootfs_path: &Path,
    backing_policy: RootfsBackingPolicy,
) -> Result<ImportedRootfs, Status> {
    let decompress_compressed = compressed_rootfs_path.to_path_buf();
    let decompress_rootfs = rootfs_path.to_path_buf();
    let expected_rootfs_hash = content_hash.to_string();
    // The decoder refuses to publish bytes whose digest is not the content
    // hash, so a mismatch surfaces as a decode error with nothing on disk.
    let stats = tokio::task::spawn_blocking(move || {
        crate::rootfs_zstd::decompress_rootfs(
            &decompress_compressed,
            &decompress_rootfs,
            Some(&expected_rootfs_hash),
            backing_policy,
        )
    })
    .await
    .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
    .map_err(|error| Status::data_loss(format!("decompress template rootfs: {error}")))?;
    tracing::info!(
        template = %template_id,
        content_hash,
        rootfs_size = stats.rootfs_size,
        compressed_size = stats.compressed_size,
        "decompressed template rootfs import artifact"
    );
    Ok(ImportedRootfs {
        expanded_size: stats.rootfs_size,
        compressed_hash: stats.compressed_hash,
        compressed_size: stats.compressed_size,
    })
}

impl TemplateManagerService {
    async fn materialize_uncompressed_imported_rootfs(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        staging_dir: &str,
        rootfs_path: &Path,
        compressed_rootfs_path: &Path,
    ) -> Result<ImportedRootfs, Status> {
        let backing_policy = self.volume_puller.backing_policy;
        let read_mode = self.template_read_mode(template_id);
        if backing_policy == RootfsBackingPolicy::NoCow {
            let raw_path = rootfs_path.to_path_buf();
            tokio::task::spawn_blocking(move || backing_policy.prepare_raw(&raw_path))
                .await
                .map_err(|error| Status::internal(format!("prepare rootfs task: {error}")))?
                .map_err(|error| Status::internal(format!("prepare rootfs backing: {error}")))?;
        }
        let expected_rootfs_hash = header
            .rootfs_hash
            .as_deref()
            .filter(|hash| !hash.is_empty())
            .unwrap_or(content_hash);
        let actual_rootfs_hash = compute_template_file_hash(
            rootfs_path.to_path_buf(),
            content_hash_algorithm(expected_rootfs_hash)?,
            "compute streamed rootfs hash",
            read_mode,
        )
        .await?;
        if actual_rootfs_hash != expected_rootfs_hash {
            return Err(Status::data_loss(format!(
                "rootfs hash mismatch: expected {expected_rootfs_hash}, got {actual_rootfs_hash}"
            )));
        }

        expand_template_rootfs(template_id, content_hash, staging_dir, "local_import").await?;
        let expanded_hash = compute_template_file_hash(
            rootfs_path.to_path_buf(),
            content_hash_algorithm(content_hash)?,
            "compute expanded rootfs hash",
            read_mode,
        )
        .await?;
        if expanded_hash != content_hash {
            return Err(Status::data_loss(format!(
                "expanded rootfs hash mismatch: expected {content_hash}, got {expanded_hash}"
            )));
        }
        generate_template_compressed_rootfs(
            template_id,
            content_hash,
            staging_dir,
            "local_import",
            read_mode,
        )
        .await?;

        let expanded_size = std::fs::metadata(rootfs_path)
            .map(|m| m.len())
            .map_err(|e| Status::internal(format!("stat rootfs {}: {e}", rootfs_path.display())))?;
        let compressed_hash = compute_template_file_hash(
            compressed_rootfs_path.to_path_buf(),
            HashAlg::Sha256,
            "hash compressed rootfs",
            read_mode,
        )
        .await?;
        let compressed_size = std::fs::metadata(compressed_rootfs_path)
            .map(|m| m.len())
            .map_err(|e| {
                Status::internal(format!(
                    "stat compressed rootfs {}: {e}",
                    compressed_rootfs_path.display()
                ))
            })?;

        Ok(ImportedRootfs {
            expanded_size,
            compressed_hash,
            compressed_size,
        })
    }
    pub(super) async fn do_import(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        stream: &mut tonic::Streaming<crate::proto::ImportTemplateRequest>,
    ) -> Result<Response<crate::proto::ImportTemplateResponse>, Status> {
        validate_guest_documents(&header.metadata_json, &header.manifest_json)?;
        let digest_dir = build_artifacts_dir(&self.artifact_store.root, template_id, content_hash);
        let digest_dir = digest_dir.to_string_lossy().into_owned();
        let staging_dir = format!("{digest_dir}.importing.{}", Uuid::now_v7());

        if std::path::Path::new(&digest_dir).exists() {
            return self
                .import_existing_build(template_id, content_hash, header, stream, &digest_dir)
                .await;
        }

        self.import_new_build(
            template_id,
            content_hash,
            header,
            stream,
            &digest_dir,
            &staging_dir,
        )
        .await
    }

    async fn import_existing_build(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        stream: &mut tonic::Streaming<crate::proto::ImportTemplateRequest>,
        digest_dir: &str,
    ) -> Result<Response<crate::proto::ImportTemplateResponse>, Status> {
        use tokio_stream::StreamExt;

        self.validate_existing_import_identity(template_id, content_hash, header, digest_dir)?;
        let manifest_json =
            (!header.manifest_json.is_empty()).then_some(header.manifest_json.as_slice());
        let metadata_json =
            (!header.metadata_json.is_empty()).then_some(header.metadata_json.as_slice());
        let prepared = self
            .prepare_existing_build_refresh(
                template_id,
                content_hash,
                digest_dir,
                manifest_json,
                metadata_json,
            )
            .await?;
        if header.include_snapshot {
            let dimensions = required_snapshot_dimensions(header)?;
            let snap_dir = snapshot_artifacts_dir(
                &self.artifact_store.root,
                template_id,
                content_hash,
                dimensions.0,
                dimensions.1,
            )
            .to_string_lossy()
            .into_owned();
            std::fs::create_dir_all(&snap_dir)
                .map_err(|e| Status::internal(format!("create snapshot dir: {e}")))?;
            self.receive_snapshot_chunks(stream, &snap_dir).await?;
            self.persist_imported_snapshot(template_id, content_hash, &snap_dir, dimensions)
                .await?;
        } else {
            while let Some(msg) = stream.next().await {
                msg?;
            }
        }

        self.publish_existing_build_refresh(template_id, content_hash, digest_dir, prepared)
            .await?;

        crate::node_state_sync::mark_template_dirty(&self.db, template_id);
        Ok(import_template_response(template_id, content_hash))
    }

    fn validate_existing_import_identity(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        digest_dir: &str,
    ) -> Result<(), Status> {
        if header.manifest_json.is_empty() {
            return Err(Status::invalid_argument(format!(
                "manifest_json is required when importing existing build {template_id}@{content_hash}"
            )));
        }

        let existing_manifest =
            std::fs::read(format!("{digest_dir}/manifest.json")).map_err(|error| {
                Status::data_loss(format!(
                    "read existing manifest for {template_id}@{content_hash}: {error}"
                ))
            })?;
        let existing = parse_template_manifest_value("existing manifest_json", &existing_manifest)?;
        let incoming =
            parse_template_manifest_value("incoming manifest_json", &header.manifest_json)?;

        if template_manifest_identity_conflicts(&existing, &incoming) {
            return Err(Status::already_exists(format!(
                "build {template_id}@{content_hash} already exists with different identity fields"
            )));
        }
        Ok(())
    }

    async fn import_new_build(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        stream: &mut tonic::Streaming<crate::proto::ImportTemplateRequest>,
        digest_dir: &str,
        staging_dir: &str,
    ) -> Result<Response<crate::proto::ImportTemplateResponse>, Status> {
        // Ensure the name dir exists
        let name_dir = template_artifacts_dir(&self.artifact_store.root, template_id);
        std::fs::create_dir_all(&name_dir)
            .map_err(|e| Status::internal(format!("create name dir: {e}")))?;
        set_template_cache_permissions(Path::new(&name_dir), TEMPLATE_CACHE_DIR_MODE)?;

        // Imports use a UUID-suffixed staging path that gets renamed
        // atomically into the canonical digest dir at the end. The
        // nodemgr btrfs-delegation RPCs only operate on canonical
        // paths (their security model rejects arbitrary path inputs),
        // so we can't ask nodemgr to create a subvolume at the
        // staging path. Imports therefore land as plain directories —
        // the canonical post-rename dir is also a plain dir, which
        // means imported templates are not eligible for the
        // btrfs-send codec. The Full codec still works for them, and
        // imports are a less-common path (operator-driven export +
        // re-import flows for migration / backup) where the codec
        // optimization isn't critical.
        //
        // On-demand pulls (the dominant codepath) continue to land
        // as subvolumes via `volume::try_pull_from`, which goes
        // through nodemgr with canonical paths.
        std::fs::create_dir(staging_dir)
            .map_err(|e| Status::internal(format!("create staging dir: {e}")))?;
        let mut staging_guard = ImportStagingGuard::new(staging_dir);
        set_template_cache_permissions(Path::new(&staging_dir), TEMPLATE_CACHE_DIR_MODE)?;

        // Write manifest.json from header
        if !header.manifest_json.is_empty() {
            write_template_manifest_atomic(
                &Path::new(&staging_dir).join("manifest.json"),
                &header.manifest_json,
            )?;
        }

        // Write metadata.json from header
        if !header.metadata_json.is_empty() {
            std::fs::write(
                format!("{staging_dir}/metadata.json"),
                &header.metadata_json,
            )
            .map_err(|e| Status::internal(format!("write metadata.json: {e}")))?;
        }

        // Prepare snapshot dir if snapshot is included
        let snapshot_dimensions =
            optional_snapshot_dimensions(header.snapshot_vcpu, header.snapshot_memory_mib)?;
        let snap_dir = match (header.include_snapshot, snapshot_dimensions) {
            (true, Some((snapshot_vcpu, snapshot_memory_mib))) => {
                let sd = snapshot_dir(
                    template_id,
                    content_hash,
                    snapshot_vcpu,
                    snapshot_memory_mib,
                );
                std::fs::create_dir_all(&sd)
                    .map_err(|e| Status::internal(format!("create snapshot dir: {e}")))?;
                Some(sd)
            }
            (true, None) => {
                return Err(Status::invalid_argument(
                    "snapshot_vcpu and snapshot_memory_mib are required when include_snapshot is true",
                ));
            }
            (false, _) => None,
        };

        // Receive file chunks — route template transfer files to staging,
        // snapshot files to snap_dir.
        self.receive_import_files(stream, staging_dir, snap_dir.as_deref())
            .await?;

        let rootfs = self
            .materialize_imported_rootfs(template_id, content_hash, header, staging_dir)
            .await?;
        let metadata_json = String::from_utf8_lossy(&header.metadata_json).to_string();
        let manifest = parse_template_manifest("incoming manifest_json", &header.manifest_json)?;
        let metadata = validate_guest_documents(metadata_json.as_bytes(), &header.manifest_json)?;
        let manifest = normalize_template_manifest(
            manifest,
            template_id,
            content_hash,
            &metadata,
            rootfs.expanded_size,
            Some((&rootfs.compressed_hash, rootfs.compressed_size)),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| Status::internal(format!("serialize manifest: {e}")))?;
        write_template_manifest_atomic(
            &Path::new(&staging_dir).join("manifest.json"),
            &manifest_json,
        )?;

        let artifact_size = match rootfs_artifact_size_from_manifest(&manifest) {
            Some(size) => size,
            None => rootfs.expanded_size,
        };
        normalize_template_cache_tree(Path::new(&staging_dir))?;

        // Atomic rename staging -> digest dir. Staging is a plain
        // directory (see the comment above where staging is created),
        // so no btrfs subvolume / received_uuid / RO finalization
        // applies on this path. Imported templates land as plain
        // directories; the Full codec covers them. On-demand pulls
        // via `volume::try_pull_from` retain the btrfs-send-eligible
        // shape through node-manager delegation.
        if let Err(e) = std::fs::rename(staging_dir, digest_dir) {
            return Err(Status::internal(format!("rename staging -> digest: {e}")));
        }
        staging_guard.disarm();

        // Nothing to RO-toggle: digest_dir is a plain dir. If a
        // future PR re-introduces subvolume-form imports via a new
        // nodemgr "staging subvolume" RPC, finalize would slot in
        // after the rename.

        let build_date = manifest.build_date.clone();
        let runtime = if manifest.runtime.is_empty() {
            RUNTIME_FIRECRACKER_ALIAS.to_string()
        } else {
            manifest.runtime.clone()
        };
        let arch = template_arch(&metadata, &manifest);

        // Insert build into DB
        let db = Arc::clone(&self.db);
        let n = template_id.to_string();
        let h = content_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("upsert imported build"))?;
            crate::db::upsert_build(
                &conn,
                &crate::db::BuildUpsert {
                    name: &n,
                    content_hash: &h,
                    build_date: &build_date,
                    manifest_json: &manifest_json,
                    runtime: &runtime,
                    arch: &arch,
                    artifact_size_bytes: artifact_size,
                    state: "ready",
                },
            )
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| Status::internal(format!("db upsert_build: {e}")))?;

        // If snapshot was included, verify completeness and record in DB
        if let Some(sd) = &snap_dir {
            let dimensions = snapshot_dimensions.ok_or_else(|| {
                Status::internal("snapshot dimensions missing after snapshot import")
            })?;
            self.persist_imported_snapshot(template_id, content_hash, sd, dimensions)
                .await?;
        }

        crate::node_state_sync::mark_template_dirty(&self.db, template_id);
        Ok(import_template_response(template_id, content_hash))
    }

    async fn receive_import_files(
        &self,
        stream: &mut tonic::Streaming<crate::proto::ImportTemplateRequest>,
        staging_dir: &str,
        snap_dir: Option<&str>,
    ) -> Result<(), Status> {
        use std::collections::HashMap;
        use tokio::io::AsyncWriteExt;
        use tokio_stream::StreamExt;

        let mut file_handles: HashMap<String, (tokio::fs::File, u64)> = HashMap::new();

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| Status::internal(format!("stream error: {e}")))?;
            match msg.payload {
                Some(crate::proto::import_template_request::Payload::Chunk(chunk)) => {
                    let Some(target_path) =
                        import_chunk_target_path(staging_dir, snap_dir, &chunk.file_name)?
                    else {
                        continue;
                    };
                    write_import_chunk(&mut file_handles, target_path, chunk).await?;
                }
                Some(crate::proto::import_template_request::Payload::Header(_)) => {
                    return Err(Status::invalid_argument("unexpected second header"));
                }
                None => continue,
            }
        }
        for (_, (mut file, _)) in file_handles {
            file.flush()
                .await
                .map_err(|e| Status::internal(format!("flush file: {e}")))?;
        }
        Ok(())
    }

    async fn materialize_imported_rootfs(
        &self,
        template_id: &str,
        content_hash: &str,
        header: &crate::proto::ImportTemplateHeader,
        staging_dir: &str,
    ) -> Result<ImportedRootfs, Status> {
        let rootfs_path = Path::new(staging_dir).join(TEMPLATE_ROOTFS_FILE);
        let compressed_rootfs_path = Path::new(staging_dir).join(TEMPLATE_ROOTFS_ZSTD_FILE);

        if compressed_rootfs_path.exists() {
            return materialize_compressed_imported_rootfs(
                template_id,
                content_hash,
                &rootfs_path,
                &compressed_rootfs_path,
                self.volume_puller.backing_policy,
            )
            .await;
        }
        if !rootfs_path.exists() {
            return Err(Status::data_loss(format!(
                "import for {template_id}@{content_hash} did not include {TEMPLATE_ROOTFS_ZSTD_FILE}"
            )));
        }

        self.materialize_uncompressed_imported_rootfs(
            template_id,
            content_hash,
            header,
            staging_dir,
            &rootfs_path,
            &compressed_rootfs_path,
        )
        .await
    }

    async fn persist_imported_snapshot(
        &self,
        template_id: &str,
        content_hash: &str,
        snap_dir: &str,
        dimensions: (u32, u32),
    ) -> Result<(), Status> {
        if !std::path::Path::new(&format!("{snap_dir}/snapshot.base")).exists()
            || !std::path::Path::new(&format!("{snap_dir}/vmstate.base")).exists()
        {
            let _ = std::fs::remove_dir_all(snap_dir);
            return Err(Status::data_loss(
                "incomplete snapshot: missing snapshot.base or vmstate.base",
            ));
        }

        let sd_clone = snap_dir.to_string();
        let snap_hash = tokio::task::spawn_blocking(move || compute_snapshot_hash(&sd_clone))
            .await
            .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
            .map_err(|e| Status::internal(format!("compute snapshot hash: {e}")))?;
        let _ = std::fs::write(format!("{snap_dir}/snapshot_hash"), &snap_hash);
        normalize_template_cache_tree(Path::new(snap_dir))?;

        let db = Arc::clone(&self.db);
        let n = template_id.to_string();
        let h = content_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("upsert imported snapshot"))?;
            crate::db::upsert_snapshot(&conn, &n, &h, dimensions.0, dimensions.1, &snap_hash)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| Status::internal(format!("db upsert_snapshot: {e}")))?;
        Ok(())
    }

    /// Helper: receive only snapshot file chunks from the stream.
    pub(super) async fn receive_snapshot_chunks(
        &self,
        stream: &mut tonic::Streaming<crate::proto::ImportTemplateRequest>,
        snap_dir: &str,
    ) -> Result<(), Status> {
        use std::collections::HashMap;
        use std::io::Write;
        use tokio_stream::StreamExt;

        let mut file_handles: HashMap<String, (std::fs::File, u64)> = HashMap::new();

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| Status::internal(format!("stream error: {e}")))?;
            match msg.payload {
                Some(crate::proto::import_template_request::Payload::Chunk(chunk)) => {
                    if chunk.file_name != "snapshot.base"
                        && chunk.file_name != "vmstate.base"
                        && chunk.file_name != "snapshot-metadata.json"
                        && chunk.file_name != "rootfs.ext4"
                    {
                        continue; // skip unknown files
                    }
                    // Skip template files (rootfs) when we're only receiving snapshot
                    if chunk.file_name == "rootfs.ext4" {
                        continue;
                    }
                    if chunk.eof {
                        file_handles.remove(&chunk.file_name);
                        continue;
                    }
                    if !file_handles.contains_key(&chunk.file_name) {
                        let local_name = if chunk.file_name == "snapshot-metadata.json" {
                            "metadata.json"
                        } else {
                            &chunk.file_name
                        };
                        let f = std::fs::File::create(format!("{snap_dir}/{local_name}")).map_err(
                            |e| Status::internal(format!("create file {}: {e}", chunk.file_name)),
                        )?;
                        file_handles.insert(chunk.file_name.clone(), (f, 0u64));
                    }
                    let (file, expected_offset) =
                        file_handles.get_mut(&chunk.file_name).ok_or_else(|| {
                            Status::internal(format!(
                                "missing open snapshot file handle for {}",
                                chunk.file_name
                            ))
                        })?;
                    if chunk.offset != *expected_offset {
                        return Err(Status::invalid_argument(format!(
                            "non-sequential offset for {}",
                            chunk.file_name
                        )));
                    }
                    file.write_all(&chunk.data)
                        .map_err(|e| Status::internal(format!("write: {e}")))?;
                    let chunk_len = u64::try_from(chunk.data.len()).map_err(|_| {
                        Status::internal(format!(
                            "chunk for {} is too large to track offset",
                            chunk.file_name
                        ))
                    })?;
                    *expected_offset = expected_offset.checked_add(chunk_len).ok_or_else(|| {
                        Status::internal(format!("offset overflow for {}", chunk.file_name))
                    })?;
                }
                _ => continue,
            }
        }

        Ok(())
    }
}

/// Validate that a template name is safe (no path traversal).
pub(super) fn validate_template_name(name: &str) -> Result<(), tonic::Status> {
    name.parse::<pplx_space_common_core::types::TemplateName>()
        .map(|_| ())
        .map_err(|err| tonic::Status::invalid_argument(format!("invalid template name: {err}")))
}

/// Validate an algorithm-prefixed content hash.
pub(super) fn validate_content_hash(hash: &str) -> Result<(), tonic::Status> {
    hash.parse::<pplx_space_common_core::types::ContentHash>()
        .map(|_| ())
        .map_err(|_| {
            tonic::Status::invalid_argument(format!(
                "invalid content hash (expected sha256:<hex> or blake3:<hex>): {hash}"
            ))
        })
}

pub(super) fn normalize_arch(arch: &str) -> &str {
    match arch {
        "amd64" => "x86_64",
        "arm64" => "aarch64",
        other => other,
    }
}

pub(super) fn template_arch(
    metadata: &pplx_space_common_core::types::TemplateMetadata,
    manifest: &pplx_space_common_core::types::TemplateManifest,
) -> String {
    // Order: metadata wins (build-time fact), then manifest (artifact-side
    // label), then the host's compile-time ARCH as a last-ditch fallback —
    // matches the pre-typed behavior at every callsite. `normalize_arch`
    // smooths over the `arm64`/`aarch64` synonym at both sources.
    if let Some(arch) = metadata.arch.as_deref().filter(|s| !s.is_empty()) {
        return normalize_arch(arch).to_string();
    }
    if !manifest.arch.is_empty() {
        return normalize_arch(&manifest.arch).to_string();
    }
    std::env::consts::ARCH.to_string()
}

#[test]
fn test_validate_content_hash() {
    let good = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    validate_content_hash(good).unwrap();

    let bad = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/../../foo";
    assert!(validate_content_hash(bad).is_err());
}

#[test]
fn test_validate_template_name() {
    validate_template_name("seed").unwrap();
    assert!(validate_template_name("../seed").is_err());
    assert!(validate_template_name("nested\\seed").is_err());
    assert!(validate_template_name("seed:latest").is_err());
    assert!(validate_template_name("seed name").is_err());
    assert!(validate_template_name(&"a".repeat(256)).is_err());
}
