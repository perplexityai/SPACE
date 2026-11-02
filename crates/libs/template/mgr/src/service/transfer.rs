use super::*;

#[derive(Debug)]
struct TemplateExportFile {
    file_name: String,
    path: PathBuf,
    required: bool,
}

fn compressed_template_export_files(
    build_path: &Path,
    manifest: &pplx_space_common_core::types::TemplateManifest,
) -> Option<Vec<TemplateExportFile>> {
    let compressed_rootfs_path = build_path.join(TEMPLATE_ROOTFS_ZSTD_FILE);
    if !compressed_rootfs_path.is_file() || !manifest_has_compressed_rootfs_entry(manifest) {
        return None;
    }
    Some(vec![TemplateExportFile {
        file_name: TEMPLATE_ROOTFS_ZSTD_FILE.to_string(),
        path: compressed_rootfs_path,
        required: true,
    }])
}

impl TemplateManagerService {
    pub(super) async fn export_template(
        &self,
        request: Request<crate::proto::ExportTemplateRequest>,
    ) -> Result<Response<TemplateExportStream>, Status> {
        let peer = self.authorize_peer(&request, "ExportTemplate", ALLOW_TEMPLATE_OPERATOR)?;
        pplx_space_common_svc::auth::template_transfer::authorize_forwarded_template_export(
            &request,
            peer.principal(),
        )?;
        let req = request.into_inner();
        let tmpl_ref = req
            .template
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("template is required"))?;
        let template_id = &tmpl_ref.name;
        validate_template_name(template_id)?;

        // A missing digest means a catalog tag, never the newest local build.
        let content_hash = if let Some(hash) = tmpl_ref
            .content_hash
            .as_deref()
            .filter(|hash| !hash.is_empty())
        {
            hash.to_string()
        } else {
            crate::volume::find_release_inner(
                template_id,
                "",
                tmpl_ref.tag.as_deref().unwrap_or(""),
                None,
            )
            .await
            .map_err(|error| match error {
                crate::volume::FindReleaseError::NotFound => {
                    Status::not_found("template release not found")
                }
                crate::volume::FindReleaseError::Unavailable => {
                    Status::unavailable("cluster metadata unavailable")
                }
                crate::volume::FindReleaseError::InvalidResponse => {
                    Status::failed_precondition("cluster did not return an exact release")
                }
                crate::volume::FindReleaseError::Status(status) => status,
            })?
            .hash
        };
        validate_content_hash(&content_hash)?;

        // Verify build exists and is ready
        let db = Arc::clone(&self.db);
        let name = template_id.clone();
        let hash = content_hash.clone();
        let build = tokio::task::spawn_blocking(move || {
            let conn = db
                .lock()
                .map_err(|_| crate::db::mutex_poison_error("load export build"))?;
            crate::db::get_build(&conn, &name, &hash)
        })
        .await
        .map_err(|e| Status::internal(format!("spawn_blocking: {e}")))?
        .map_err(|e| Status::internal(format!("db get_build: {e}")))?;

        let build = build.ok_or_else(|| {
            Status::not_found(format!("build not found: {template_id}@{content_hash}"))
        })?;
        if build.state != "ready" {
            return Err(Status::failed_precondition(format!(
                "build '{}@{}' is in state '{}', expected 'ready'",
                template_id, content_hash, build.state
            )));
        }

        let build_path = build_artifacts_dir(&self.artifact_store.root, template_id, &content_hash);
        let bdir = build_path.to_string_lossy().into_owned();
        let include_snapshot = req.include_snapshot;
        let snapshot_dimensions =
            optional_snapshot_dimensions(req.snapshot_vcpu, req.snapshot_memory_mib)?;

        let metadata_json = std::fs::read(build_path.join("metadata.json")).unwrap_or_default();
        let manifest_json = std::fs::read(build_path.join("manifest.json")).unwrap_or_default();
        let metadata =
            validate_guest_documents(&metadata_json, &manifest_json).map_err(|error| {
                if error.code() == tonic::Code::InvalidArgument {
                    Status::data_loss(error.message())
                } else {
                    error
                }
            })?;
        if metadata.guest_image.is_some() {
            return Err(Status::failed_precondition(
                "firmware templates cannot be exported or distributed",
            ));
        }
        let manifest: pplx_space_common_core::types::TemplateManifest =
            serde_json::from_slice(&manifest_json)
                .map_err(|error| Status::data_loss(error.to_string()))?;
        let (rootfs_hash, mut files) = if let Some(files) =
            compressed_template_export_files(&build_path, &manifest)
        {
            (content_hash.clone(), files)
        } else {
            self.spawn_template_compressed_rootfs_generation(
                template_id.clone(),
                content_hash.clone(),
                bdir.clone(),
                "export",
            );
            return Err(Status::unavailable(format!(
                "compressed rootfs not ready for {template_id}@{content_hash}; retry after background generation completes"
            )));
        };

        // Determine if snapshot exists for the requested config
        let snap_dir_str = if include_snapshot {
            let (snap_vcpu, snap_memory_mib) = snapshot_dimensions.ok_or_else(|| {
                Status::invalid_argument(
                    "snapshot_vcpu and snapshot_memory_mib are required when include_snapshot is true",
                )
            })?;
            let sd = snapshot_dir(template_id, &content_hash, snap_vcpu, snap_memory_mib);
            if std::path::Path::new(&format!("{sd}/snapshot.base")).exists()
                && std::path::Path::new(&format!("{sd}/vmstate.base")).exists()
            {
                Some(sd)
            } else {
                return Err(Status::not_found(format!(
                    "snapshot not found for {template_id}@{content_hash} with vcpu={snap_vcpu}, memory_mib={snap_memory_mib}"
                )));
            }
        } else {
            None
        };
        let has_snapshot = snap_dir_str.is_some();

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let tmpl_name = build.name.clone();
        let tmpl_hash = build.content_hash.clone();
        let header_snapshot_dimensions = snapshot_dimensions.filter(|_| has_snapshot);
        let header_rootfs_hash = rootfs_hash;

        tokio::task::spawn_blocking(move || {
            // Send header
            let header = crate::proto::ExportTemplateChunk {
                payload: Some(crate::proto::export_template_chunk::Payload::Header(
                    crate::proto::ExportTemplateHeader {
                        template: Some(crate::proto::TemplateRef {
                            name: tmpl_name,
                            content_hash: Some(tmpl_hash),
                            tag: None,
                        }),
                        metadata_json,
                        includes_snapshot: has_snapshot,
                        snapshot_vcpu: header_snapshot_dimensions.map(|(vcpu, _)| vcpu),
                        snapshot_memory_mib: header_snapshot_dimensions
                            .map(|(_, memory_mib)| memory_mib),
                        manifest_json,
                        rootfs_hash: Some(header_rootfs_hash),
                    },
                )),
            };
            if tx.blocking_send(Ok(header)).is_err() {
                return;
            }

            // Add snapshot files if available
            if let Some(ref sd) = snap_dir_str {
                files.push(TemplateExportFile {
                    file_name: "snapshot.base".to_string(),
                    path: PathBuf::from(format!("{sd}/snapshot.base")),
                    required: true,
                });
                files.push(TemplateExportFile {
                    file_name: "vmstate.base".to_string(),
                    path: PathBuf::from(format!("{sd}/vmstate.base")),
                    required: true,
                });
                files.push(TemplateExportFile {
                    file_name: "snapshot-metadata.json".to_string(),
                    path: PathBuf::from(format!("{sd}/metadata.json")),
                    required: false,
                });
            }

            for export_file in files {
                let file = match std::fs::File::open(&export_file.path) {
                    Ok(f) => f,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound && !export_file.required => {
                        continue;
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(Status::internal(format!(
                            "open {}: {e}",
                            export_file.file_name
                        ))));
                        return;
                    }
                };

                let mut reader = std::io::BufReader::new(file);
                let mut offset: u64 = 0;
                let mut buf = vec![0u8; 512 * 1024]; // 512KB chunks
                loop {
                    use std::io::Read;
                    let n = match reader.read(&mut buf) {
                        Ok(n) => n,
                        Err(e) => {
                            let _ = tx.blocking_send(Err(Status::internal(format!(
                                "read {}: {e}",
                                export_file.file_name
                            ))));
                            return;
                        }
                    };
                    if n > 0 {
                        let data = match buf.get(..n) {
                            Some(data) => data.to_vec(),
                            None => {
                                let _ = tx.blocking_send(Err(Status::internal(format!(
                                    "read size for {} exceeded buffer length",
                                    export_file.file_name
                                ))));
                                return;
                            }
                        };
                        let chunk = crate::proto::ExportTemplateChunk {
                            payload: Some(crate::proto::export_template_chunk::Payload::Chunk(
                                crate::proto::FileChunk {
                                    file_name: export_file.file_name.clone(),
                                    data,
                                    offset,
                                    eof: false,
                                },
                            )),
                        };
                        if tx.blocking_send(Ok(chunk)).is_err() {
                            return;
                        }
                        let chunk_len = match u64::try_from(n) {
                            Ok(chunk_len) => chunk_len,
                            Err(error) => {
                                let _ = tx.blocking_send(Err(Status::internal(format!(
                                    "read size for {} is too large to track offset: {error}",
                                    export_file.file_name
                                ))));
                                return;
                            }
                        };
                        offset = match offset.checked_add(chunk_len) {
                            Some(offset) => offset,
                            None => {
                                let _ = tx.blocking_send(Err(Status::internal(format!(
                                    "offset overflow while exporting {}",
                                    export_file.file_name
                                ))));
                                return;
                            }
                        };
                    } else {
                        // Send EOF marker for this file
                        let chunk = crate::proto::ExportTemplateChunk {
                            payload: Some(crate::proto::export_template_chunk::Payload::Chunk(
                                crate::proto::FileChunk {
                                    file_name: export_file.file_name.clone(),
                                    data: vec![],
                                    offset,
                                    eof: true,
                                },
                            )),
                        };
                        let _ = tx.blocking_send(Ok(chunk));
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }

    pub(super) async fn import_template(
        &self,
        request: Request<tonic::Streaming<crate::proto::ImportTemplateRequest>>,
    ) -> Result<Response<crate::proto::ImportTemplateResponse>, Status> {
        use tokio_stream::StreamExt;

        let peer = self.authorize_peer(&request, "ImportTemplate", ALLOW_TEMPLATE_OPERATOR)?;
        let mut stream = request.into_inner();

        // First message must be the header
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty stream"))?
            .map_err(|e| Status::internal(format!("stream error: {e}")))?;

        let header = match first.payload {
            Some(crate::proto::import_template_request::Payload::Header(h)) => h,
            _ => return Err(Status::invalid_argument("first message must be header")),
        };

        let metadata = validate_guest_documents(&header.metadata_json, &header.manifest_json)?;
        if metadata.guest_image.is_some() {
            if !matches!(
                peer.principal(),
                UdsPeerPrincipal::Root | UdsPeerPrincipal::Role(LocalRole::Operator)
            ) {
                return Err(Status::permission_denied(
                    "firmware image import is local operator-only",
                ));
            }
            if header.include_snapshot {
                return Err(Status::failed_precondition(
                    "firmware image snapshots are unsupported",
                ));
            }
        }

        let tmpl_ref = header
            .template
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("template is required in header"))?;
        let template_id = &tmpl_ref.name;
        validate_template_name(template_id)?;

        let content_hash = tmpl_ref.content_hash.as_deref().unwrap_or("");
        if content_hash.is_empty() {
            return Err(Status::invalid_argument("content_hash is required"));
        }
        validate_content_hash(content_hash)?;
        if let Some(rootfs_hash) = header
            .rootfs_hash
            .as_deref()
            .filter(|hash| !hash.is_empty())
        {
            validate_content_hash(rootfs_hash)?;
        }

        // Acquire per-template import lock (keyed on name+hash)
        let lock_key = format!("{template_id}@{content_hash}");
        let _import_guard = {
            let mut importing = self.importing_templates.lock().map_err(|_| {
                Status::internal("import lock set mutex poisoned while starting import")
            })?;
            if !importing.insert(lock_key.clone()) {
                return Err(Status::already_exists(format!(
                    "import already in progress for '{lock_key}'"
                )));
            }
            ImportGuard {
                set: Arc::clone(&self.importing_templates),
                key: lock_key,
            }
        };

        let response = self
            .do_import(template_id, content_hash, &header, &mut stream)
            .await?;
        if header.block_on_snapshot_policy_achieved.unwrap_or(false) {
            let outcome = self.reconcile_desired_base_snapshot(template_id).await;
            if let Some(error) = outcome.error {
                tracing::warn!(
                    template = %template_id,
                    error = %error,
                    "desired base snapshot reconcile after import failed"
                );
            }
        } else {
            self.spawn_desired_base_snapshot_reconcile(
                template_id.to_string(),
                "import_template_completed",
            );
        }
        Ok(response)
    }
}
