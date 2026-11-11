use pplx_space_common_core::types::SandboxId;

pub(super) const ECW_SANDBOX_API_LOG_EVENT: &str = "ecw_sandbox_api_request";
pub(super) const ECW_SANDBOX_ID_SPAN_FIELD: &str = "sandbox_id";

pub(super) fn record_ecw_sandbox_span_tag(sandbox_id: SandboxId) {
    let sandbox_id = sandbox_id.to_string();
    tracing::Span::current().record(ECW_SANDBOX_ID_SPAN_FIELD, sandbox_id.as_str());
}

#[derive(Debug, Clone, Copy)]
pub(super) struct EcwRequestLogContext {
    pub(super) sandbox_id: Option<SandboxId>,
}

impl EcwRequestLogContext {
    pub(super) fn sandbox(sandbox_id: SandboxId) -> Self {
        Self {
            sandbox_id: Some(sandbox_id),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct E2bRuntimeSpec {
    pub(super) template_id: Option<String>,
    pub(super) cpu_count: Option<u32>,
    pub(super) memory_mib: Option<u32>,
    pub(super) disk_size_mib: Option<u32>,
}
