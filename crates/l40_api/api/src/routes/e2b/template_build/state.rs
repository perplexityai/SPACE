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
use pplx_space_accounts::OrgId;
use pplx_space_template_model::TemplateName;
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
        let recorded = pplx_space_template_store::build_blobs::record(
            &self.pool,
            template_name,
            files_hash,
            blob_sha256,
            org_id,
        )
        .await?;
        if !recorded {
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
        pplx_space_template_store::build_blobs::blob_sha256(
            &self.pool,
            template_name,
            files_hash,
            org_id,
        )
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
