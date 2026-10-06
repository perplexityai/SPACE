use std::{future::Future, time::Duration};

use pplx_space_build_mgr_client::proto::{
    self as build_proto, BuildOptions, ClientContext, GetBuildStatusRequest,
    GetBuildStatusResponse, StartBuildRequest, StartBuildResponse, UploadBlobChunk,
    UploadBlobResponse,
};
use pplx_space_common_core::types::{
    BuildRunId, NodeId, OrgId, SystemTemplate, TemplateName, TemplateReference,
};
use pplx_space_node_api_client::proto::BuildTemplateResponse;
use pplx_space_template_mgr_client::proto::{
    CreatePoolRequest, DeletePoolRequest, DeletePoolResponse, DeleteTemplateRequest,
    GetTemplateRequest, GetTemplateResponse, ListTemplatesRequest, PoolStatusRequest,
    PoolStatusResponse, TemplateFullInfo, UpdateTemplateSettingsRequest,
};

use super::{
    ControllerError, db_contention_error,
    substrate::{Substrate, SubstrateAdapter},
};
use crate::models::{ClusterPoolConfigRequest, ClusterPoolConfigResponse};
use crate::template_build_jobs_store::{TemplateBuildJobsStore, TemplateBuildJobsStoreError};

/// The build-jobs store's errors at the controller boundary.
///
/// The statements behind `Storage` are an `INSERT ... ON CONFLICT` on
/// `(template_id, build_id)` and a read of the same row, so both can lose a
/// lock race and both succeed on retry. A malformed stored `daemon_job_id`
/// cannot, so it keeps the 500.
fn build_jobs_store_error(
    message: &'static str,
    error: TemplateBuildJobsStoreError,
) -> ControllerError {
    match error.as_sql() {
        Some(sql) => db_contention_error(message, sql),
        None => crate::controllers::internal_error(message, error),
    }
}

const TEMPLATE_BUILD_SUBMIT_ATTEMPTS: usize = 3;
const TEMPLATE_BUILD_SUBMIT_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateBuildStatusTransientReason {
    Rpc,
    Transport,
    /// A control-plane statement lost a lock race. Retried like the other two,
    /// but its own reason because no RPC and no connection failed — folding it
    /// into `Rpc` reports this service's database as the builder's fault, and
    /// hides one of the outcomes the `lock_timeout` rollout watches.
    Contention,
}

impl TemplateBuildStatusTransientReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rpc => "rpc",
            Self::Transport => "transport",
            Self::Contention => "lock_contention",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TemplateBuildStatusPollError {
    #[error("transient builder status error: {source}")]
    Transient {
        reason: TemplateBuildStatusTransientReason,
        #[source]
        source: ControllerError,
    },
    #[error("{0}")]
    Failed(#[source] ControllerError),
}

#[derive(Debug, Clone)]
pub enum TemplateBuildTarget {
    Organization(TemplateName),
    System(SystemTemplate),
}

impl TemplateBuildTarget {
    pub(crate) fn template_name(&self) -> TemplateName {
        match self {
            Self::Organization(name) => name.clone(),
            Self::System(template) => (*template).into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TemplateBuildSubmission {
    pub target: TemplateBuildTarget,
    pub client_build_id: Option<String>,
    pub recipe: build_proto::Recipe,
    pub force: bool,
    pub publish_tags: Vec<String>,
    pub client: ClientContext,
    /// Requesting organization for base access and job visibility.
    pub org_id: OrgId,
}

impl TemplateBuildSubmission {
    /// Authorize the recipe's base for the requesting organization and pin it
    /// to the resolved release digest. Returns the base template's name, or
    /// `None` for a recipe that builds from scratch.
    async fn authorize_base(
        &mut self,
        pool: &sqlx::PgPool,
    ) -> Result<Option<TemplateName>, ControllerError> {
        use build_proto::recipe::origin::Kind;
        if let Some(base) =
            self.recipe
                .origin
                .as_mut()
                .and_then(|origin| match origin.kind.as_mut() {
                    Some(Kind::FromTemplate(origin)) => Some(&mut origin.base),
                    Some(Kind::FromOciImage(origin)) => Some(&mut origin.base),
                    _ => None,
                })
        {
            let reference = base
                .parse::<TemplateReference>()
                .map_err(|error| ControllerError::bad_request(error.to_string()))?;
            let hash = crate::template_releases_store::resolve_base(pool, &reference, self.org_id)
                .await
                .map_err(|error| match error {
                    crate::template_releases_store::ResolveBaseError::NotFound => {
                        tracing::warn!(
                            template = %reference,
                            caller_org = %self.org_id,
                            "build base unavailable or unauthorized; reporting not-found",
                        );
                        ControllerError::not_found("base template not found")
                    }
                    crate::template_releases_store::ResolveBaseError::DigestPrefix => {
                        ControllerError::bad_request(error.to_string())
                    }
                    crate::template_releases_store::ResolveBaseError::Database(error) => {
                        db_contention_error("failed to authorize base template", &error)
                    }
                    crate::template_releases_store::ResolveBaseError::Digest(error) => {
                        super::internal_error("invalid stored base digest", error)
                    }
                })?;
            *base = TemplateReference::with_artifact_digest(reference.name().clone(), hash.into())
                .to_string();
            return Ok(Some(reference.name().clone()));
        }
        Ok(None)
    }

    fn into_start_build_request(self) -> StartBuildRequest {
        StartBuildRequest {
            recipe: Some(self.recipe),
            template_id: self.target.template_name().to_string(),
            options: Some(BuildOptions {
                force: self.force,
                publish_tags: self.publish_tags,
            }),
            client: Some(self.client),
            org_id: match self.target {
                TemplateBuildTarget::Organization(_) => Some(self.org_id.to_string()),
                TemplateBuildTarget::System(_) => None,
            },
        }
    }
}

#[derive(Clone)]
pub struct TemplateController {
    substrate: Substrate,
    build_jobs: TemplateBuildJobsStore,
    pool: sqlx::PgPool,
}

#[derive(Debug, thiserror::Error)]
enum TemplateBuildAttemptError {
    #[error("builder route: {0}")]
    Route(#[source] ControllerError),
    #[error("builder RPC: {0}")]
    Rpc(#[source] tonic::Status),
}

impl TemplateBuildAttemptError {
    fn into_controller_error(self) -> ControllerError {
        match self {
            Self::Route(error) => error,
            Self::Rpc(status) => status.into(),
        }
    }
}

async fn start_template_build_with_retry<F, Fut>(
    request: StartBuildRequest,
    mut start: F,
) -> Result<BuildTemplateResponse, ControllerError>
where
    F: FnMut(StartBuildRequest) -> Fut,
    Fut: Future<Output = Result<BuildTemplateResponse, TemplateBuildAttemptError>>,
{
    let mut attempt = 1;
    loop {
        match start(request.clone()).await {
            Ok(response) => return Ok(response),
            Err(TemplateBuildAttemptError::Rpc(status))
                if pplx_space_build_mgr_client::is_build_manager_draining_status(&status)
                    && attempt < TEMPLATE_BUILD_SUBMIT_ATTEMPTS =>
            {
                tracing::warn!(
                    attempt,
                    max_attempts = TEMPLATE_BUILD_SUBMIT_ATTEMPTS,
                    "builder rejected template build while draining; retrying on a fresh route"
                );
                attempt += 1;
                tokio::time::sleep(TEMPLATE_BUILD_SUBMIT_RETRY_DELAY).await;
            }
            Err(error) => return Err(error.into_controller_error()),
        }
    }
}

impl TemplateController {
    pub fn new(substrate: Substrate, pool: sqlx::PgPool) -> Self {
        Self {
            substrate,
            build_jobs: TemplateBuildJobsStore::new(pool.clone()),
            pool,
        }
    }

    pub async fn list_templates(&self) -> Result<Vec<TemplateFullInfo>, ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        client
            .list_templates(ListTemplatesRequest {})
            .await
            .map(|resp| resp.into_inner().templates)
            .map_err(|err| ControllerError::internal(format!("node-api: {err}")))
    }

    pub async fn get_template(
        &self,
        request: GetTemplateRequest,
    ) -> Result<GetTemplateResponse, ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        Ok(client.get_template(request).await?.into_inner())
    }

    pub async fn update_template_settings(
        &self,
        request: UpdateTemplateSettingsRequest,
    ) -> Result<(), ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        client.update_template_settings(request).await?;
        Ok(())
    }

    pub async fn delete_template(&self, name: String) -> Result<(), ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        client
            .delete_template(DeleteTemplateRequest { template: name })
            .await
            .map(|_| ())
            .map_err(|err| ControllerError::internal(format!("node-api: {err}")))
    }

    pub async fn submit_template_build(
        &self,
        mut submission: TemplateBuildSubmission,
    ) -> Result<StartBuildResponse, ControllerError> {
        let org_id = submission.org_id;
        let template_name = submission.target.template_name();
        if !self
            .build_jobs
            .template_owner_matches(&template_name, org_id)
            .await
            .map_err(|error| {
                ControllerError::internal(format!("failed to check template ownership: {error}"))
            })?
        {
            return Err(ControllerError::conflict(
                "template belongs to another organization",
            ));
        }
        // The grant must land before dispatch: the builder pulls the base as
        // soon as it accepts the job, and ClusterApi only lets a builder node
        // resolve an org-specific base that a fresh grant names. The response
        // then pins this submission's grant to the accepting builder.
        let base_grant = match submission.authorize_base(&self.pool).await? {
            Some(base) => Some(
                self.build_jobs
                    .record_base_grant(org_id, &template_name, &base)
                    .await
                    .map_err(|error| {
                        db_contention_error("failed to record build base grant", &error)
                    })?,
            ),
            None => None,
        };
        let client_build_id = submission.client_build_id.clone();
        let response = self
            .start_template_build_request(submission.into_start_build_request())
            .await?;
        self.record_template_build_response(
            org_id,
            template_name,
            client_build_id.as_deref(),
            response,
            base_grant,
        )
        .await
    }

    async fn record_template_build_response(
        &self,
        org_id: OrgId,
        template_name: TemplateName,
        client_build_id: Option<&str>,
        response: BuildTemplateResponse,
        base_grant: Option<crate::template_build_jobs_store::BuildBaseGrantId>,
    ) -> Result<StartBuildResponse, ControllerError> {
        let job_id = response.job_id.parse::<BuildRunId>().map_err(|error| {
            ControllerError::internal(format!("builder returned an invalid build job ID: {error}"))
        })?;
        let builder_node_id = if response.builder_node_id.is_empty() {
            pplx_space_metrics::template::build_routing_fallback_missing_identity();
            tracing::warn!(
                %job_id,
                %template_name,
                "builder response omitted node identity; using unpinned status routing"
            );
            None
        } else {
            Some(
                response
                    .builder_node_id
                    .parse::<NodeId>()
                    .map_err(|error| {
                        ControllerError::internal(format!(
                            "builder returned an invalid node ID: {error}"
                        ))
                    })?,
            )
        };
        self.build_jobs
            .record_accepted_build(
                org_id,
                &template_name,
                client_build_id,
                job_id,
                builder_node_id,
                base_grant,
            )
            .await
            // Not classified, deliberately: build-manager has already
            // accepted the job by this point. The request carries no client
            // build ID and build-manager creates a distinct job per
            // submission, so a retry starts a second build and may overwrite
            // this mapping. A 500 after irreversible work is correct — see
            // `db_contention_error`'s exception.
            .map_err(|error| {
                ControllerError::internal(format!("failed to record template build job: {error}"))
            })?;

        Ok(StartBuildResponse {
            job_id: job_id.to_string(),
        })
    }

    async fn start_template_build_request(
        &self,
        request: StartBuildRequest,
    ) -> Result<BuildTemplateResponse, ControllerError> {
        start_template_build_with_retry(request, |request| async move {
            let mut client = self
                .substrate
                .builder_client()
                .await
                .map_err(TemplateBuildAttemptError::Route)?;
            client
                .build_template(request)
                .await
                .map(tonic::Response::into_inner)
                .map_err(TemplateBuildAttemptError::Rpc)
        })
        .await
    }

    pub async fn get_template_build_status_by_job_id(
        &self,
        job_id: impl Into<String>,
    ) -> Result<GetBuildStatusResponse, ControllerError> {
        let job_id = job_id.into();
        let parsed_job_id = job_id.parse::<BuildRunId>().map_err(|error| {
            ControllerError::bad_request(format!("invalid template build job ID: {error}"))
        })?;
        let builder_node_id = self
            .build_jobs
            .find_builder_node(parsed_job_id)
            .await
            .map_err(|error| build_jobs_store_error("failed to load template build job", error))?;
        let mut client = match builder_node_id {
            Some(builder_node_id) => {
                self.substrate
                    .builder_client_for_node(builder_node_id)
                    .await?
            }
            // Jobs without routing ownership use unpinned discovery.
            None => self.substrate.builder_client().await?,
        };
        Ok(client
            .get_build_template_status(GetBuildStatusRequest { job_id })
            .await?
            .into_inner())
    }

    pub async fn poll_template_build_status(
        &self,
        job_id: impl Into<String>,
    ) -> Result<GetBuildStatusResponse, TemplateBuildStatusPollError> {
        self.get_template_build_status_by_job_id(job_id)
            .await
            .map_err(
                |error| match template_build_status_transient_reason(&error) {
                    Some(reason) => TemplateBuildStatusPollError::Transient {
                        reason,
                        source: error,
                    },
                    None => TemplateBuildStatusPollError::Failed(error),
                },
            )
    }

    pub async fn upload_template_build_blob<S>(
        &self,
        stream: S,
    ) -> Result<UploadBlobResponse, ControllerError>
    where
        S: tonic::IntoStreamingRequest<Message = UploadBlobChunk>,
    {
        let mut client = self.substrate.builder_client().await?;
        Ok(client.upload_build_blob(stream).await?.into_inner())
    }

    pub async fn create_pool(&self, request: CreatePoolRequest) -> Result<(), ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        client.create_pool(request).await?;
        Ok(())
    }

    pub async fn delete_pool(
        &self,
        request: DeletePoolRequest,
    ) -> Result<DeletePoolResponse, ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        Ok(client.delete_pool(request).await?.into_inner())
    }

    pub async fn pool_status(
        &self,
        request: PoolStatusRequest,
    ) -> Result<PoolStatusResponse, ControllerError> {
        let mut client = self.substrate.builder_client().await?;
        Ok(client.pool_status(request).await?.into_inner())
    }

    pub async fn list_cluster_pool_configs(
        &self,
    ) -> Result<Vec<ClusterPoolConfigResponse>, ControllerError> {
        self.substrate.list_pool_configs().await
    }

    pub async fn upsert_cluster_pool_config(
        &self,
        request: ClusterPoolConfigRequest,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        self.substrate.upsert_pool_config(request).await
    }

    pub async fn disable_cluster_pool_config(
        &self,
        id: uuid::Uuid,
    ) -> Result<ClusterPoolConfigResponse, ControllerError> {
        self.substrate.disable_pool_config(id).await
    }
}

fn template_build_status_transient_reason(
    error: &ControllerError,
) -> Option<TemplateBuildStatusTransientReason> {
    if error.status() == axum::http::StatusCode::GATEWAY_TIMEOUT {
        return Some(TemplateBuildStatusTransientReason::Rpc);
    }
    // Ahead of the status check as well as the fragment match: a contention
    // 503 matches none of the transport fragments below and would fall through
    // to the `Rpc` catch-all.
    if error.is_lock_contention() {
        return Some(TemplateBuildStatusTransientReason::Contention);
    }
    if error.status() != axum::http::StatusCode::SERVICE_UNAVAILABLE {
        return None;
    }
    let message = error.to_string().to_ascii_lowercase();
    if [
        "transport error",
        "h2 protocol error",
        "broken pipe",
        "connection reset",
        "stream closed",
    ]
    .iter()
    .any(|needle| message.contains(needle))
    {
        Some(TemplateBuildStatusTransientReason::Transport)
    } else {
        Some(TemplateBuildStatusTransientReason::Rpc)
    }
}

#[cfg(test)]
mod tests;
