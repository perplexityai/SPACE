//! The provenance recorded next to a pulled file, and whether an existing replica can be reused.

use super::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct MaterializationObservation {
    pub mode: MaterializationMode,
    pub layout: MaterializationLayout,
    pub logical_bytes: u64,
    pub allocated_bytes: u64,
    pub candidate_hole_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct FileReplicaProvenance {
    #[serde(flatten)]
    pub(super) replica: VerifiedReplicaProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) materialization: Option<MaterializationObservation>,
}

pub(super) fn requested_file_mode_bits(p: &TransferPayload) -> Option<u32> {
    p.file_mode.map(FileMode::bits)
}

pub(super) fn file_replica_provenance(
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
    pub(super) fn new(
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
pub(super) fn read_materialization_observation(
    local_path: &Path,
) -> Option<MaterializationObservation> {
    let bytes = std::fs::read(verified_replica_provenance_path(local_path)).ok()?;
    serde_json::from_slice::<FileReplicaProvenance>(&bytes)
        .ok()?
        .materialization
}

pub(super) fn materialization_is_compatible(
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

impl TransferExecutor {
    pub(super) async fn validate_existing_file_replica(
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

    pub(super) async fn write_file_replica_provenance(
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

    pub(super) async fn invalidate_file_replica_provenance(
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
}
