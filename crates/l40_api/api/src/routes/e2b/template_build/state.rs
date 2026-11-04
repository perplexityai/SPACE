//! Postgres-backed bookkeeping shared across requests.
//!
//! Three logical maps:
//! * `builds`: `(templateID, buildID) → BuildJob` — what `BuilderApi`
//!   job id the build was submitted under, so the SDK's status polls
//!   can resolve it to a buildmgr `GetBuildStatusRequest`.
//! * `uploaded_files`: `(templateID, filesHash) → blob_sha256` — what
//!   build-manager UploadBlob sha256 the file is stored under, so a
//!   `COPY` step in the e2b payload (referenced by `filesHash`) can be
//!   rewritten to a SPACE `Recipe.Step.Copy { blob_sha256 }` at translate
//!   time.
//! * `build_specs`: `(templateID, buildID) → BuildSpec` — sizing + tags
//!   posted to `POST /v3/templates` and the build id we synth'd back to
//!   the SDK. The SDK doesn't repeat these in the subsequent
//!   `POST /v2/templates/{tid}/builds/{bid}` call, so we stash them
//!   keyed by `(tid, bid)` (NOT `tid` alone — concurrent V3 calls for
//!   the same alias would otherwise clobber each other's tags or
//!   sizing).
//!
use pplx_space_common_core::{
    sql,
    types::{OrgId, TemplateName},
};
use std::sync::Arc;

use sqlx::PgPool;

pub(super) use crate::template_build_jobs_store::{BuildJob, BuildSpec};
use crate::template_build_jobs_store::{
    TemplateBuildJobsStore, TemplateBuildJobsStoreError, template_build_owner_matches,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum RecordBlobError {
    #[error("template build blob storage failed: {0}")]
    Storage(#[from] sqlx::Error),
    #[error("template build upload conflicts with existing ownership or content")]
    Conflict,
}

pub struct TemplateBuildState {
    pool: PgPool,
    jobs: TemplateBuildJobsStore,
}

impl TemplateBuildState {
    pub fn new(pool: PgPool) -> Arc<Self> {
        Arc::new(Self {
            jobs: TemplateBuildJobsStore::new(pool.clone()),
            pool,
        })
    }

    pub(super) async fn lookup_build(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
    ) -> Result<Option<BuildJob>, TemplateBuildJobsStoreError> {
        self.jobs
            .lookup_build(org_id, template_name, build_id)
            .await
    }

    pub(super) async fn record_blob(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        files_hash: &str,
        blob_sha256: &str,
    ) -> Result<(), RecordBlobError> {
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Err(RecordBlobError::Conflict);
        }
        // The SDK's file hash excludes archive metadata, so same-owner reuploads can
        // have different blob digests even when the file contents are unchanged.
        // Mappings recorded before uploads carried an owner have a NULL org_id;
        // the first owned upload claims them so later writers meet the owner check.
        let result = sqlx::query(sql!(
            "INSERT INTO template_build_blobs (template_id, files_hash, blob_sha256, org_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (template_id, files_hash) DO UPDATE
             SET blob_sha256 = EXCLUDED.blob_sha256,
                 org_id = EXCLUDED.org_id
             WHERE template_build_blobs.org_id IS NULL
                OR template_build_blobs.org_id = EXCLUDED.org_id"
        ))
        .bind(template_name.as_str())
        .bind(files_hash)
        .bind(blob_sha256)
        .bind(org_id.as_uuid())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() == 0 {
            return Err(RecordBlobError::Conflict);
        }
        Ok(())
    }

    pub(super) async fn lookup_blob(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        files_hash: &str,
    ) -> Result<Option<String>, sqlx::Error> {
        if !template_build_owner_matches(&self.pool, template_name, org_id).await? {
            return Ok(None);
        }
        sqlx::query_scalar::<_, String>(sql!(
            r#"
            SELECT blob_sha256
            FROM template_build_blobs
            WHERE template_id = $1
              AND files_hash = $2
              AND (org_id = $3 OR org_id IS NULL)
            "#
        ))
        .bind(template_name.as_str())
        .bind(files_hash)
        .bind(org_id.as_uuid())
        .fetch_optional(&self.pool)
        .await
    }

    pub(super) async fn record_build_spec(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
        spec: BuildSpec,
    ) -> Result<(), TemplateBuildJobsStoreError> {
        self.jobs
            .record_build_spec(org_id, template_name, build_id, spec)
            .await
    }

    pub(super) async fn lookup_build_spec(
        &self,
        org_id: OrgId,
        template_name: &TemplateName,
        build_id: &str,
    ) -> Result<Option<BuildSpec>, TemplateBuildJobsStoreError> {
        self.jobs
            .lookup_build_spec(org_id, template_name, build_id)
            .await
    }
}

#[cfg(test)]
mod tests;
