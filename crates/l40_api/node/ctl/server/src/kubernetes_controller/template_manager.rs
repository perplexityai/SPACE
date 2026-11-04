use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pplx_space_common_core::kubernetes::template_build_presence_label_key;
use pplx_space_common_grpc::{GrpcClient, GrpcRetryContext};
use pplx_space_template_mgr_client::TemplateManagerGrpcClient;
use pplx_space_template_mgr_client::proto::{
    GetTemplateRequest, ListTemplatesRequest, TemplateRef,
};
use tonic::{Request, Status};

type InnerClient = TemplateManagerGrpcClient;
type ConnectFuture = Pin<
    Box<
        dyn Future<Output = Result<InnerClient, pplx_space_template_mgr_client::ConnectError>>
            + Send,
    >,
>;
type TemplateManagerConnector = fn() -> ConnectFuture;

const TEMPLATE_READY_STATE: &str = "ready";

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct EnsuredTemplate {
    pub content_hash: Option<String>,
}

pub struct Client {
    inner: GrpcClient<InnerClient, TemplateManagerConnector>,
    list_rpc_timeout: Duration,
    pull_rpc_timeout: Duration,
}

impl Client {
    pub fn new(list_rpc_timeout: Duration, pull_rpc_timeout: Duration) -> Self {
        Self {
            // Keep the default short local policy: this buffers UDS/service
            // restart flaps, not long-running template pull stalls.
            inner: GrpcClient::new("template-manager", connect_template_manager),
            list_rpc_timeout,
            pull_rpc_timeout,
        }
    }

    pub async fn local_template_presence(&mut self) -> Result<BTreeMap<String, String>, Status> {
        let timeout = self.list_rpc_timeout;
        self.inner
            .call(
                "ListTemplates",
                GrpcRetryContext::with_timeout(timeout),
                move |mut client| async move { local_template_presence(&mut client, timeout).await },
            )
            .await
    }

    pub async fn ensure_template(
        &mut self,
        template: &str,
        content_hash: Option<&str>,
        tag: Option<&str>,
    ) -> Result<EnsuredTemplate, Status> {
        let timeout = self.pull_rpc_timeout;
        let template = template.to_string();
        let content_hash = content_hash.map(ToOwned::to_owned);
        let tag = tag.map(ToOwned::to_owned);
        self.inner
            .call(
                "GetTemplate",
                GrpcRetryContext::with_timeout(timeout),
                move |mut client| {
                    let template = template.clone();
                    let content_hash = content_hash.clone();
                    let tag = tag.clone();
                    async move {
                        ensure_template(
                            &mut client,
                            timeout,
                            &template,
                            content_hash.as_deref(),
                            tag.as_deref(),
                        )
                        .await
                    }
                },
            )
            .await
    }
}

fn connect_template_manager() -> ConnectFuture {
    Box::pin(async {
        tracing::info!("connecting to template-manager UDS");
        pplx_space_template_mgr_client::connect().await
    })
}

async fn local_template_presence(
    tmpl: &mut InnerClient,
    timeout: Duration,
) -> Result<BTreeMap<String, String>, Status> {
    let mut request = Request::new(ListTemplatesRequest {
        omit_sandbox_counts: true,
    });
    request.set_timeout(timeout);

    let response = tmpl.list_templates(request).await?.into_inner();

    Ok(response
        .templates
        .into_iter()
        .filter_map(|template| {
            let content_hash = template.content_hash?;
            let label = template_build_presence_label_key(&template.name, &content_hash);
            Some((label, content_hash))
        })
        .collect())
}

async fn ensure_template(
    tmpl: &mut InnerClient,
    timeout: Duration,
    template: &str,
    content_hash: Option<&str>,
    tag: Option<&str>,
) -> Result<EnsuredTemplate, Status> {
    let mut request = Request::new(GetTemplateRequest {
        caller_org_id: None,
        template: Some(TemplateRef {
            name: template.to_string(),
            content_hash: content_hash.map(ToOwned::to_owned),
            tag: tag.map(ToOwned::to_owned),
        }),
        vcpu: None,
        memory_mib: None,
    });
    request.set_timeout(timeout);

    let response = tmpl.get_template(request).await?.into_inner();
    let resolved = response
        .template
        .as_ref()
        .map(|template| {
            let content_hash = template.content_hash.as_deref().unwrap_or("<unknown>");
            format!("{}@{}", template.name, content_hash)
        })
        .unwrap_or_else(|| template.to_string());
    ensure_ready_state(&response.state, &resolved)?;

    Ok(EnsuredTemplate {
        content_hash: response.template.and_then(|template| template.content_hash),
    })
}

fn ensure_ready_state(state: &str, resolved_template: &str) -> Result<(), Status> {
    if state == TEMPLATE_READY_STATE {
        return Ok(());
    }

    Err(Status::failed_precondition(format!(
        "template {resolved_template} is in state '{state}', expected '{TEMPLATE_READY_STATE}'"
    )))
}

#[cfg(test)]
mod tests;
