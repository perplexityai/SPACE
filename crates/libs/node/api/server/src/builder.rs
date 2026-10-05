use crate::clients::BackendClients;
use crate::node_ctl;
use crate::proto::builder_api_server::BuilderApi;
use crate::proto::{
    BuildTemplateResponse, RefreshFacetStateRequest, RefreshFacetStateResponse,
    TriggerStateReconciliationRequest, TriggerStateReconciliationResponse,
};
use crate::restart_handoff::RpcReplaySafety;
use pplx_space_build_mgr_client::proto as build_proto;
use pplx_space_common_core::types::{NodeId, SystemTemplate};
use pplx_space_common_grpc::{RequestAdmission, fwd_retry};
use pplx_space_common_svc::auth::build::authorize_template_build;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

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

/// Forward a request to a backend client RPC, converting the error to `Status`.
macro_rules! fwd {
    ($client:expr, $rpc:ident, $req:expr) => {
        $client.$rpc($req).await.map_err(Into::into)
    };
}

// Sandbox-manager forwards must survive a restart handoff: the drain
// interceptor can mark any RPC, so writers replay on proof and reads replay
// broadly, exactly as on the worker surface.
macro_rules! fwd_sbx_handoff {
    ($service:expr, $method:literal, $safety:expr, $rpc:ident, $req:expr) => {
        crate::restart_handoff::call_with_restart_handoff(
            $method,
            $safety,
            $req,
            $service.clients.sbx(),
            |mut client, request| async move { client.$rpc(request).await },
        )
        .await
    };
}

#[tonic::async_trait]
impl BuilderApi for BuilderApiService {
    // ── Sandbox query + cleanup (sandbox-manager) ────────────────────────────

    async fn sandbox_state(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StateRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StateResponse>, Status> {
        fwd_sbx_handoff!(self, "SandboxState", RpcReplaySafety::ReadOnly, state, r)
    }

    async fn list_sandboxes(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::ListRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::ListResponse>, Status> {
        fwd_sbx_handoff!(self, "ListSandboxes", RpcReplaySafety::ReadOnly, list, r)
    }

    async fn stop_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::StopRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::StopResponse>, Status> {
        fwd_sbx_handoff!(self, "StopSandbox", RpcReplaySafety::RequiresProof, stop, r)
    }

    async fn delete_sandbox(
        &self,
        r: Request<pplx_space_sbx_mgr_client::proto::DeleteRequest>,
    ) -> Result<Response<pplx_space_sbx_mgr_client::proto::DeleteResponse>, Status> {
        fwd_sbx_handoff!(
            self,
            "DeleteSandbox",
            RpcReplaySafety::RequiresProof,
            delete,
            r
        )
    }

    // ── Template management (template-manager) ──────────────────────────────

    async fn delete_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateResponse>, Status>
    {
        fwd!(self.clients.tmpl(), delete_template, r)
    }

    async fn delete_template_build(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeleteTemplateBuildRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeleteTemplateBuildResponse>, Status>
    {
        fwd!(self.clients.tmpl(), delete_template_build, r)
    }

    async fn list_templates(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::ListTemplatesRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::ListTemplatesResponse>, Status>
    {
        fwd_retry!("template-manager", self.clients.tmpl(), list_templates, r)
    }

    async fn get_template(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::GetTemplateRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::GetTemplateResponse>, Status> {
        fwd_retry!("template-manager", self.clients.tmpl(), get_template, r)
    }

    async fn update_template_settings(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::UpdateTemplateSettingsRequest>,
    ) -> Result<
        Response<pplx_space_template_mgr_client::proto::UpdateTemplateSettingsResponse>,
        Status,
    > {
        // A policy-bearing template update sets the default network or
        // auto-lifecycle policy inherited by every sandbox of this template, so
        // it must be gated on the same control-plane capability as the sandbox
        // policy RPCs.
        let req = r.get_ref();
        if req.network_policy.is_some() || req.default_auto_lifecycle.is_some() {
            crate::worker::require_policy_mutation_authority(&r)?;
        }
        fwd!(self.clients.tmpl(), update_template_settings, r)
    }

    async fn update_template_snapshot_policy(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyRequest>,
    ) -> Result<
        Response<pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyResponse>,
        Status,
    > {
        fwd!(self.clients.tmpl(), update_template_snapshot_policy, r)
    }

    // ── Template pools (template-manager) ────────────────────────────────────

    async fn create_pool(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::CreatePoolRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::CreatePoolResponse>, Status> {
        fwd!(self.clients.tmpl(), create_pool, r)
    }

    async fn delete_pool(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::DeletePoolRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::DeletePoolResponse>, Status> {
        fwd!(self.clients.tmpl(), delete_pool, r)
    }

    async fn pool_status(
        &self,
        r: Request<pplx_space_template_mgr_client::proto::PoolStatusRequest>,
    ) -> Result<Response<pplx_space_template_mgr_client::proto::PoolStatusResponse>, Status> {
        fwd_retry!("template-manager", self.clients.tmpl(), pool_status, r)
    }

    // ── Custom orchestration ─────────────────────────────────────────────────

    async fn refresh_facet_state(
        &self,
        r: Request<RefreshFacetStateRequest>,
    ) -> Result<Response<RefreshFacetStateResponse>, Status> {
        node_ctl::refresh_facet_state(r.into_inner()).await?;
        Ok(Response::new(RefreshFacetStateResponse {}))
    }

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
        Ok(self.clients.tmpl().export_template(r).await?)
    }

    async fn pull_template(
        &self,
        r: Request<crate::proto::PullTemplateRequest>,
    ) -> Result<Response<crate::proto::PullTemplateResponse>, Status> {
        let req = r.into_inner();
        let resp = crate::transfer::pull_template(&mut self.clients.tmpl(), &req).await?;
        Ok(Response::new(resp))
    }

    async fn list_node_templates(
        &self,
        _r: Request<crate::proto::ListNodeTemplatesRequest>,
    ) -> Result<Response<crate::proto::ListNodeTemplatesResponse>, Status> {
        let request = Request::new(pplx_space_template_mgr_client::proto::ListTemplatesRequest {});
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
                Status::new(
                    err.code(),
                    format!("build-manager StartBuild: {}", err.message()),
                )
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
                    Status::new(
                        err.code(),
                        format!("build-manager UploadBlob: {}", err.message()),
                    )
                })
            },
        )
        .await
    }
}
