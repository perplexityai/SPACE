//! Qualification comes from the published template, never a running guest.
use super::*;
use pplx_space_common_core::types::TemplateMetadata;
use pplx_space_template_mgr_client::{lookup_scope::TemplateLookupScope, proto};

pub(super) fn platform(row: &crate::db::SandboxRow) -> Option<model::RecoveryPlatform> {
    use pplx_space_common_core::types::guest_image::QualifiedGuestAgent;
    match super::super::guest_image::profile_for_sandbox(row)
        .ok()?
        .agent_protocol()
    {
        QualifiedGuestAgent::LinuxV1 => Some(model::RecoveryPlatform::Linux),
        QualifiedGuestAgent::WindowsV1 => Some(model::RecoveryPlatform::Windows),
        QualifiedGuestAgent::Absent => None,
    }
}

impl SandboxManagerService {
    pub(super) async fn require_update_qualification(
        &self,
        row: &crate::db::SandboxRow,
        policy: &policy::Policy,
    ) -> Result<(), Status> {
        if self.update_template_qualified(row, policy).await? {
            Ok(())
        } else {
            Err(Status::failed_precondition(
                "published template has no matching guest recovery attestation",
            ))
        }
    }

    pub(super) async fn update_template_qualified(
        &self,
        row: &crate::db::SandboxRow,
        policy: &policy::Policy,
    ) -> Result<bool, Status> {
        let Some(expected) = platform(row) else {
            return Ok(false);
        };
        let name = &row.template;
        let Some(digest) = row.template_hash.as_ref() else {
            return Ok(false);
        };
        // Explicit qualification remains useful for images published before the
        // metadata attestation. Revocation is checked with the current catalog.
        if policy.qualified_templates.contains(digest) {
            return Ok(true);
        }
        let mut request = request(proto::GetTemplateRequest {
            template: Some(proto::TemplateRef {
                name: name.clone(),
                content_hash: Some(digest.clone()),
                tag: None,
                environment: super::super::create_support::sandbox_template_preference(row),
            }),
            cached_only: true,
            caller_org_id: None,
            vcpu: None,
            memory_mib: None,
        });
        let scope = TemplateLookupScope::LocalExecution;
        scope.attach(&mut request);
        let response = tokio::time::timeout(
            RPC_BUDGET,
            self.template_resolver
                .get_template_on_shared_channel(request),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("template qualification lookup deadline"))??;
        scope.observe_response(&response)?;
        let response = response.into_inner();
        // Older servers may ignore cached_only. Never use a response that fails
        // to prove the exact cached release was selected.
        if !response.cached_only
            || response
                .template
                .as_ref()
                .is_none_or(|t| t.name != *name || t.content_hash.as_ref() != Some(digest))
        {
            return Err(Status::unavailable(
                "template lookup did not acknowledge the exact cached release",
            ));
        }
        let path = Path::new(&response.template_dir).join("metadata.json");
        use tokio::io::AsyncReadExt;
        let file = tokio::fs::File::open(path).await.map_err(internal)?;
        let mut bytes = Vec::new();
        file.take(model::MAX_METADATA_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .await
            .map_err(internal)?;
        if bytes.len() > model::MAX_METADATA_BYTES {
            return Err(Status::resource_exhausted("template metadata too large"));
        }
        let metadata: TemplateMetadata = serde_json::from_slice(&bytes).map_err(internal)?;
        metadata.execution_metadata().map_err(internal)?;
        Ok(metadata
            .spaced_update_contract
            .as_deref()
            .and_then(model::RecoveryPlatform::from_contract)
            == Some(expected))
    }
}
