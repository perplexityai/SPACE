use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use pplx_space_common_grpc::{GrpcClient, GrpcRetryContext};
use pplx_space_sandbox_pod::{
    template_build_presence_label_key, template_partition_presence_label_key,
};
use pplx_space_template_mgr_client::TemplateManagerGrpcClient;
use pplx_space_template_mgr_client::proto::{
    GetTemplateRequest, ListTemplatesRequest, TemplateRef,
};
use pplx_space_template_model::TemplateCachePartition;
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
        environment: Option<pplx_space_execution_spec::EnvironmentReference>,
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
                            environment,
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

    Ok(template_presence(response.templates))
}

/// The presence metadata of every listed build: its partition-less label,
/// whose annotation is the content hash, as before partitions, and its
/// partition's, whose annotation is `<content_hash>@<partition key>` (the key
/// is empty for the legacy partition). For a build in another partition that
/// is its build directory's name; for a legacy build it never names a
/// directory, so each build directory keeps exactly one annotation naming it:
/// the partition-less one for a legacy build, as before, and the partition's
/// for a build in another partition. A template-manager that predates
/// partitions reports none, and its builds are legacy. A partition key that
/// doesn't parse writes only the partition-less label.
///
/// A legacy-layout build bound to another environment, which startup
/// recovery adopts in the legacy partition, carries that environment's
/// partition label instead of the legacy partition's: lookups requiring that
/// environment take it, so a pod requiring it can run there, while lookups
/// requiring the initial environment, whose partition is the legacy one,
/// don't. Its annotation names the legacy directory the build lives in.
pub(super) fn template_presence(
    templates: Vec<pplx_space_template_mgr_client::proto::TemplateFullInfo>,
) -> BTreeMap<String, String> {
    let mut presence = BTreeMap::new();
    for template in templates {
        // Before the content hash moves out of `template`.
        let adopted = crate::facets::adopted_partition(&template);
        let Some(content_hash) = template.content_hash else {
            continue;
        };
        let partition = match template.cache_partition.as_deref() {
            None => Some(TemplateCachePartition::Legacy),
            Some(key) => TemplateCachePartition::from_key(key).ok(),
        };
        if let Some(partition) = partition {
            let directory = format!("{content_hash}@{}", partition.key());
            // An adopted build answers lookups requiring its own environment,
            // not the initial environment's, whose partition is the legacy
            // one: it carries only its environment's label.
            let labelled = match adopted {
                Some(bound) if partition == TemplateCachePartition::Legacy => bound,
                _ => partition,
            };
            let key =
                template_partition_presence_label_key(&template.name, &content_hash, labelled);
            if labelled == partition {
                presence.insert(key, directory);
            } else {
                // The build in that environment's own partition, when the
                // node has one too, keeps the label naming its directory,
                // whichever row comes last.
                presence.entry(key).or_insert(directory);
            }
        }
        presence.insert(
            template_build_presence_label_key(&template.name, &content_hash),
            content_hash,
        );
    }
    presence
}

/// The lookup a template write sends. One naming an environment requires that
/// environment's build of the digest, so template-manager pulls it, a twin
/// included, rather than answer with another environment's build.
pub(super) fn template_request(
    template: &str,
    content_hash: Option<&str>,
    tag: Option<&str>,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
) -> GetTemplateRequest {
    GetTemplateRequest {
        caller_org_id: None,
        cached_only: false,
        template: Some(TemplateRef {
            name: template.to_string(),
            content_hash: content_hash.map(ToOwned::to_owned),
            tag: tag.map(ToOwned::to_owned),
            environment: environment.map(|environment| environment.to_string()),
        }),
        vcpu: None,
        memory_mib: None,
        environment_required: environment.is_some(),
    }
}

async fn ensure_template(
    tmpl: &mut InnerClient,
    timeout: Duration,
    template: &str,
    content_hash: Option<&str>,
    tag: Option<&str>,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
) -> Result<EnsuredTemplate, Status> {
    let mut request = Request::new(template_request(template, content_hash, tag, environment));
    request.set_timeout(timeout);

    let response = tmpl.get_template(request).await?.into_inner();
    ensured_template(response, template, environment)
}

/// What a `GetTemplate` answer ensured: a ready build and, for a lookup
/// naming an environment, one in that environment's cache partition by its
/// binding, which the answer's `environment` reports. A template-manager
/// that predates `environment_required` ignores it and can answer with
/// another environment's build of the digest, as the original for a twin;
/// that's an error, so the write stays pending. An unbound build counts as
/// the initial environment's, whose partition is the legacy one, and an
/// adopted legacy-layout build as its binding's.
fn ensured_template(
    response: pplx_space_template_mgr_client::proto::GetTemplateResponse,
    template: &str,
    environment: Option<pplx_space_execution_spec::EnvironmentReference>,
) -> Result<EnsuredTemplate, Status> {
    let resolved = response
        .template
        .as_ref()
        .map(|template| {
            let content_hash = template.content_hash.as_deref().unwrap_or("<unknown>");
            format!("{}@{}", template.name, content_hash)
        })
        .unwrap_or_else(|| template.to_string());
    ensure_ready_state(&response.state, &resolved)?;
    if let Some(required) = environment {
        let partition = |environment| {
            pplx_space_node_runtime_core::catalog::template_cache_partition(environment)
        };
        let bound = response
            .template
            .as_ref()
            .and_then(|template| template.environment.as_deref())
            .map(str::parse::<pplx_space_execution_spec::EnvironmentReference>)
            .transpose();
        match bound {
            Ok(bound) if partition(bound) == partition(Some(required)) => {}
            _ => {
                return Err(Status::failed_precondition(format!(
                    "template {resolved} isn't bound to environment {required}"
                )));
            }
        }
    }

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
