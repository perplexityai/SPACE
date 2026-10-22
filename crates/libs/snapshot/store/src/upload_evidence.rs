//! Optional upload evidence, committed alongside the write-once logical digests.
use super::*;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ArtifactUploadEvidence {
    pub artifact: SnapshotArtifactFile,
    pub object_key: String,
    pub wire_size_bytes: u64,
    pub etag: Option<String>,
    pub version_id: Option<String>,
    pub metadata_binding_digest: Option<Digest>,
}

pub async fn record(
    conn: &mut sqlx::PgConnection,
    snapshot: SnapshotId,
    evidence: &[ArtifactUploadEvidence],
) -> Result<(), RecordArtifactDigestsError> {
    for entry in evidence {
        let changed = sqlx::query(sql!("UPDATE snapshot_artifact_digests SET upload_evidence=$3
            WHERE snapshot_id=$1 AND artifact=$2 AND (upload_evidence IS NULL OR upload_evidence=$3)"))
            .bind(snapshot).bind(entry.artifact.file_name()).bind(sqlx::types::Json(entry)).execute(&mut *conn).await?;
        if changed.rows_affected() != 1 {
            return Err(RecordArtifactDigestsError::Conflict {
                snapshot_id: snapshot,
                artifact: entry.artifact,
            });
        }
    }
    Ok(())
}

pub async fn get(
    executor: impl sqlx::PgExecutor<'_>,
    snapshot: SnapshotId,
) -> Result<Vec<ArtifactUploadEvidence>, sqlx::Error> {
    let rows: Vec<sqlx::types::Json<ArtifactUploadEvidence>> = sqlx::query_scalar(sql!("SELECT upload_evidence FROM snapshot_artifact_digests WHERE snapshot_id=$1 AND upload_evidence IS NOT NULL ORDER BY artifact"))
        .bind(snapshot).fetch_all(executor).await?;
    Ok(rows.into_iter().map(|entry| entry.0).collect())
}
