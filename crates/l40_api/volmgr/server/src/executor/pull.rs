//! Pull jobs: destination locks, the existing-file policy, cancellation cleanup and the pull
//! itself.

use super::*;

pub(super) struct DestinationPullGuard {
    pub(super) locks: DestinationPullLocks,
    pub(super) path: PathBuf,
    pub(super) guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for DestinationPullGuard {
    fn drop(&mut self) {
        self.guard.take();
        self.locks
            .remove_if(&self.path, |_path, lock| Arc::strong_count(lock) == 1);
    }
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

pub(super) struct PullExecutionOptions<'a> {
    pub(super) resolved_dek: Option<&'a [u8]>,
    pub(super) existing_policy: PullExistingPolicy,
    pub(super) cancellation: CancellationToken,
}

/// Best-effort unlink of a `.partial` sibling.
///
/// `NotFound` is the common expected case (no stale file to sweep), so we
/// silence it. Any other failure is logged so silent fs corruption — a
/// permission flip, a stuck mount — doesn't accumulate without diagnostics.
/// On a successful removal we emit a debug log so audits ("what was deleted
/// and why?") can trace each unlink to the code path that triggered it.
pub(super) async fn unlink_partial(path: &Path, reason: &str) {
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

pub(super) async fn cleanup_cancelled_pull_temp_paths(local_path: &Path) -> Result<(), JobError> {
    remove_cancelled_path(&partial_path(local_path)).await?;
    remove_cancelled_path(&encrypted_sibling(local_path)).await?;
    remove_cancelled_path(&decrypted_sibling(local_path)).await
}

impl TransferExecutor {
    pub(super) async fn lock_pull_destination(&self, local_path: &Path) -> DestinationPullGuard {
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

    pub(super) async fn cleanup_cancelled_pull_path(
        &self,
        local_path: &Path,
    ) -> Result<(), JobError> {
        let _destination_guard = self.lock_pull_destination(local_path).await;
        cleanup_cancelled_pull_temp_paths(local_path).await
    }

    pub(super) async fn execute_pull_cancellable(
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
    pub(super) async fn execute_pull(
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

    pub(super) async fn execute_pull_locked(
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
            // compressed-input floor whenever a decode will run, unless the
            // artifact class bypasses admission under an output cap.
            let ciphertext_path: &Path = &tmp_path;
            let options = DecryptedPullOptions {
                decompress_if_zstd,
                materialization_mode: execution_materialization_mode,
            };
            let (decrypt_started_at, staged) = self
                .run_admitted_decode(
                    job,
                    p,
                    execution_materialization_mode,
                    decompress_if_zstd.then_some(TransferPhase::DecryptWait),
                    ciphertext_path,
                    |gate| {
                        stage_decrypted_temp(
                            job,
                            p,
                            enc,
                            ciphertext_path,
                            options,
                            resolved_dek,
                            gate,
                        )
                    },
                )
                .await?;
            match staged {
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
            let (decompress_started_at, report) = self
                .run_admitted_decode(
                    job,
                    p,
                    execution_materialization_mode,
                    Some(TransferPhase::DecompressWait),
                    local_path,
                    |gate| {
                        decompress_in_place(
                            local_path,
                            snapshot_artifact,
                            execution_materialization_mode,
                            gate,
                        )
                    },
                )
                .await?;
            let report = report?;
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
