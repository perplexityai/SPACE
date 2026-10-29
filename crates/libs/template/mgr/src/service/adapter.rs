use super::*;

#[tonic::async_trait]
impl TemplateManager for TemplateManagerService {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<Response<CreatePoolResponse>, Status> {
        TemplateManagerService::create_pool(self, request).await
    }

    async fn delete_pool(
        &self,
        request: Request<DeletePoolRequest>,
    ) -> Result<Response<DeletePoolResponse>, Status> {
        TemplateManagerService::delete_pool(self, request).await
    }

    async fn pool_status(
        &self,
        request: Request<PoolStatusRequest>,
    ) -> Result<Response<PoolStatusResponse>, Status> {
        TemplateManagerService::pool_status(self, request).await
    }

    async fn delete_template(
        &self,
        request: Request<DeleteTemplateRequest>,
    ) -> Result<Response<DeleteTemplateResponse>, Status> {
        TemplateManagerService::delete_template(self, request).await
    }

    async fn delete_idle_template(
        &self,
        request: Request<crate::proto::DeleteIdleTemplateRequest>,
    ) -> Result<Response<crate::proto::DeleteIdleTemplateResponse>, Status> {
        TemplateManagerService::delete_idle_template(self, request).await
    }

    async fn list_templates(
        &self,
        request: Request<ListTemplatesRequest>,
    ) -> Result<Response<ListTemplatesResponse>, Status> {
        TemplateManagerService::list_templates(self, request).await
    }

    async fn register_template(
        &self,
        request: Request<RegisterTemplateRequest>,
    ) -> Result<Response<RegisterTemplateResponse>, Status> {
        TemplateManagerService::register_template(self, request).await
    }

    type ExportTemplateStream = TemplateExportStream;

    async fn export_template(
        &self,
        request: Request<ExportTemplateRequest>,
    ) -> Result<Response<Self::ExportTemplateStream>, Status> {
        TemplateManagerService::export_template(self, request).await
    }

    async fn import_template(
        &self,
        request: Request<tonic::Streaming<crate::proto::ImportTemplateRequest>>,
    ) -> Result<Response<crate::proto::ImportTemplateResponse>, Status> {
        TemplateManagerService::import_template(self, request).await
    }

    async fn get_template(
        &self,
        request: Request<crate::proto::GetTemplateRequest>,
    ) -> Result<Response<crate::proto::GetTemplateResponse>, Status> {
        TemplateManagerService::get_template(self, request).await
    }

    async fn update_template_settings(
        &self,
        request: Request<UpdateTemplateSettingsRequest>,
    ) -> Result<Response<UpdateTemplateSettingsResponse>, Status> {
        TemplateManagerService::update_template_settings(self, request).await
    }

    async fn update_template_snapshot_policy(
        &self,
        request: Request<UpdateTemplateSnapshotPolicyRequest>,
    ) -> Result<Response<UpdateTemplateSnapshotPolicyResponse>, Status> {
        TemplateManagerService::update_template_snapshot_policy(self, request).await
    }

    async fn delete_template_build(
        &self,
        request: Request<crate::proto::DeleteTemplateBuildRequest>,
    ) -> Result<Response<crate::proto::DeleteTemplateBuildResponse>, Status> {
        TemplateManagerService::delete_template_build(self, request).await
    }
}
