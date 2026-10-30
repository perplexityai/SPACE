use crate::clients::BackendClients;
use crate::node_ctl;
use crate::proto::builder_api_server::BuilderApi;
use crate::proto::{
    BuildTemplateResponse, TriggerStateReconciliationRequest, TriggerStateReconciliationResponse,
};
use pplx_space_build_mgr_client::proto as build_proto;
use pplx_space_common_core::types::{NodeId, SystemTemplate};
use pplx_space_common_grpc::{RequestAdmission, fwd_retry};
use pplx_space_common_svc::auth::build::authorize_template_build;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{Code, Request, Response, Status};

pub struct BuilderApiService {
    clients: BackendClients,
    node_id: NodeId,
}

impl BuilderApiService {
    pub fn new(clients: BackendClients, node_id: NodeId) -> Self {
        Self { clients, node_id }
    }

    /// Every listener serving this builder must receive the same upload pool.
    /// UploadBuildBlob is client-streaming with a unary response, so admission
    /// stays held until all forwarding and backend processing have completed.
    pub fn into_grpc_service(
        self,
        upload_slots: Arc<Semaphore>,
    ) -> RequestAdmission<crate::proto::builder_api_server::BuilderApiServer<Self>> {
        RequestAdmission::new(
            crate::proto::builder_api_server::BuilderApiServer::new(self),
            pplx_space_common_grpc::QueuedSlots::new(upload_slots),
            &[("UploadBuildBlob", 1)],
        )
    }
}

/// build-manager authenticates node-api's own credential, never the end
/// user's, so its refusal is a server-side credential fault. Forwarding the
/// code unchanged would report an auth error for a caller whose credential is
/// fine; report it as a server error instead.
fn gateway_credential_rejected(rpc: &'static str, status: &Status) -> Option<Status> {
    if !matches!(
        status.code(),
        Code::Unauthenticated | Code::PermissionDenied
    ) {
        return None;
    }
    tracing::warn!(
        rpc,
        code = ?status.code(),
        error = status.message(),
        "build-manager rejected the builder gateway credential"
    );
    Some(Status::internal(format!(
        "build-manager {rpc}: builder rejected the gateway credential"
    )))
}

/// Forward a request to a backend client RPC, converting the error to `Status`.
macro_rules! fwd {
    ($client:expr, $rpc:ident, $req:expr) => {
        $client.$rpc($req).await.map_err(Into::into)
    };
}

#[tonic::async_trait]
impl BuilderApi for BuilderApiService {
    // ── Template management (template-manager) ──────────────────────────────

    async fn delete_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateResponse>, Status>
    {
        pplx_space_common_svc::auth::template::require_template_mutation_authority(&r)?;
        fwd!(self.clients.tmpl(), delete_template, r)
    }

    async fn delete_template_build(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateBuildRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateBuildResponse>, Status>
    {
        pplx_space_common_svc::auth::template::require_template_mutation_authority(&r)?;
        fwd!(self.clients.tmpl(), delete_template_build, r)
    }

    // ── Custom orchestration ─────────────────────────────────────────────────

    async fn trigger_state_reconciliation(
        &self,
        r: Request<TriggerStateReconciliationRequest>,
    ) -> Result<Response<TriggerStateReconciliationResponse>, Status> {
        node_ctl::trigger_state_reconciliation(r.into_inner()).await?;
        Ok(Response::new(TriggerStateReconciliationResponse {}))
    }

    // ── Template transfer ────────────────────────────────────────────────────

    type NodeExportTemplateStream =
        tonic::Streaming<pplx_space_template_mgr_client::proto::ExportTemplateChunk>;

    async fn node_export_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::ExportTemplateRequest>,
    ) -> Result<Response<Self::NodeExportTemplateStream>, Status> {
        let mut r = r;
        pplx_space_common_svc::auth::template_transfer::prepare_template_export(&mut r)?;
        Ok(self.clients.tmpl().export_template(r).await?)
    }

    async fn pull_template(
        &self,
        mut r: Request<crate::proto::PullTemplateRequest>,
    ) -> Result<Response<crate::proto::PullTemplateResponse>, Status> {
        pplx_space_common_svc::auth::template_transfer::require_template_transfer_authority(&r)?;
        let scope = crate::template_lookup::bind_caller_scope(&mut r)?;
        let req = r.into_inner();
        let resp = crate::transfer::pull_template(&mut self.clients.tmpl(), &req, scope).await?;
        Ok(Response::new(resp))
    }

    async fn list_node_templates(
        &self,
        _r: Request<crate::proto::ListNodeTemplatesRequest>,
    ) -> Result<Response<crate::proto::ListNodeTemplatesResponse>, Status> {
        let request =
            Request::new(pplx_space_template_mgr_client::proto::ListTemplatesRequest::default());
        let resp = fwd_retry!(
            "template-manager",
            self.clients.tmpl(),
            list_templates,
            request
        )
        .map_err(|e| Status::internal(format!("list_templates: {e}")))?;

        let templates = resp
            .into_inner()
            .templates
            .into_iter()
            .map(|t| crate::proto::NodeTemplateBuild {
                name: t.name,
                content_hash: t.content_hash,
                build_date: t.built_at,
                artifact_size_bytes: t.artifact_size_bytes,
                cache_size_bytes: t.cache_size_bytes,
                disk_size_mb: t.disk_size_mb,
                runtime: t.runtime,
                arch: t.arch,
                snapshots: t
                    .snapshots
                    .into_iter()
                    .map(|s| crate::proto::SnapshotInfo {
                        vcpu: s.vcpu,
                        memory_mib: s.memory_mib,
                        snapshot_hash: s.snapshot_hash,
                    })
                    .collect(),
            })
            .collect();

        Ok(Response::new(crate::proto::ListNodeTemplatesResponse {
            builds: templates,
        }))
    }

    async fn build_template(
        &self,
        r: Request<build_proto::StartBuildRequest>,
    ) -> Result<Response<BuildTemplateResponse>, Status> {
        let request = r.get_ref();
        authorize_template_build(&r, request.org_id.as_deref())?;
        if request.org_id.is_none() && request.template_id.parse::<SystemTemplate>().is_err() {
            return Err(Status::permission_denied(
                "gateway system builds are limited to bundled system templates",
            ));
        }
        let response = self
            .clients
            .build_mgr()
            .start_build(r)
            .await
            .map_err(|err| {
                gateway_credential_rejected("StartBuild", &err).unwrap_or_else(|| {
                    Status::new(
                        err.code(),
                        format!("build-manager StartBuild: {}", err.message()),
                    )
                })
            })?
            .into_inner();
        Ok(Response::new(BuildTemplateResponse {
            job_id: response.job_id,
            builder_node_id: self.node_id.to_string(),
        }))
    }

    async fn get_build_template_status(
        &self,
        r: Request<build_proto::GetBuildStatusRequest>,
    ) -> Result<Response<build_proto::GetBuildStatusResponse>, Status> {
        fwd_retry!(
            "build-manager",
            self.clients.build_mgr(),
            get_build_status,
            r
        )
        .map_err(|err| gateway_credential_rejected("GetBuildStatus", &err).unwrap_or(err))
    }

    async fn upload_build_blob(
        &self,
        request: Request<tonic::Streaming<build_proto::UploadBlobChunk>>,
    ) -> Result<Response<build_proto::UploadBlobResponse>, Status> {
        let mut inbound = request.into_inner();
        let first = crate::client_stream::first(&mut inbound, "UploadBuildBlob").await?;
        let mut client = self.clients.build_mgr();
        crate::client_stream::forward(
            inbound,
            first,
            "UploadBuildBlob",
            |_| Ok(()),
            move |upstream| async move {
                client.upload_blob(upstream).await.map_err(|err| {
                    gateway_credential_rejected("UploadBlob", &err).unwrap_or_else(|| {
                        Status::new(
                            err.code(),
                            format!("build-manager UploadBlob: {}", err.message()),
                        )
                    })
                })
            },
        )
        .await
    }
}
