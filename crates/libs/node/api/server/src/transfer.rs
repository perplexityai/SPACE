use std::time::Duration;

use pplx_space_common_svc::bootstrap::wrap_grpc_client_channel;
use tokio_stream::StreamExt;
use tonic::Status;
use tonic::codegen::InterceptedService;

use crate::proto::{PullTemplateRequest, PullTemplateResponse, transfer_source::Source};

const REMOTE_NODE_API_CONNECT_RPC_TIMEOUT: Duration = Duration::from_secs(10);

fn transfer_source_uri(address: &str) -> Result<tonic::transport::Uri, Status> {
    let endpoint = pplx_space_common_grpc::internal_transport::qualify_endpoint(address);
    let uri: tonic::transport::Uri = endpoint
        .parse()
        .map_err(|_| Status::invalid_argument("invalid transfer source endpoint"))?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri
            .authority()
            .is_none_or(|authority| authority.as_str().contains('@'))
        || uri.path() != "/"
        || uri.query().is_some()
    {
        return Err(Status::invalid_argument(
            "transfer source must be a node endpoint without a path, query, or user information",
        ));
    }
    Ok(uri)
}

fn registered_transfer_source(
    requested: &tonic::transport::Uri,
    nodes: Vec<crate::proto::NodeRecord>,
) -> Result<(tonic::transport::Uri, crate::proto::NodeRole), Status> {
    let mut source = None;
    for node in nodes {
        if node.deleted_at.is_some() {
            continue;
        }
        let Ok(endpoint) = transfer_source_uri(&node.node_api_endpoint) else {
            continue;
        };
        if endpoint != *requested {
            continue;
        }
        let role = crate::proto::NodeRole::try_from(node.role)
            .map_err(|_| Status::failed_precondition("registered source has an unknown role"))?;
        if !matches!(
            role,
            crate::proto::NodeRole::Worker | crate::proto::NodeRole::Builder
        ) {
            return Err(Status::permission_denied(
                "registered node is not an artifact source",
            ));
        }
        if source.replace((endpoint, role)).is_some() {
            return Err(Status::failed_precondition(
                "transfer source endpoint belongs to multiple registered nodes",
            ));
        }
    }
    source.ok_or_else(|| {
        Status::permission_denied("transfer source is not a registered node endpoint")
    })
}

async fn connect_after_transfer_admission<T, F>(
    requested: &tonic::transport::Uri,
    lookup: impl std::future::Future<Output = Result<Vec<crate::proto::NodeRecord>, Status>>,
    connect: impl FnOnce(tonic::transport::Uri, crate::proto::NodeRole) -> F,
) -> Result<T, Status>
where
    F: std::future::Future<Output = Result<T, Status>>,
{
    let nodes = tokio::time::timeout(REMOTE_NODE_API_CONNECT_RPC_TIMEOUT, lookup)
        .await
        .map_err(|_| Status::deadline_exceeded("transfer source registry lookup timed out"))??;
    let (endpoint, role) = registered_transfer_source(requested, nodes)?;
    connect(endpoint, role).await
}

/// Resolve authority before sending this worker's bearer to a transfer peer.
/// The registry, not the peer's self-reported NodeInfo, owns endpoint and role.
/// This retains the cluster's trusted-network transport contract; endpoint
/// registration is not a substitute for TLS peer authentication.
async fn connect_remote(
    remote_addr: &str,
) -> Result<(tonic::transport::Channel, String, crate::proto::NodeRole), Status> {
    let requested = transfer_source_uri(remote_addr)?;
    connect_after_transfer_admission(
        &requested,
        async {
            let mut registry = pplx_space_node_api_client::cluster::connect_from_env()
                .await
                .map_err(|error| {
                    Status::unavailable(format!(
                        "cannot connect to transfer source registry: {error}"
                    ))
                })?;
            Ok(registry
                .list_nodes(crate::proto::ListNodesRequest {
                    role: None,
                    include_deleted: false,
                })
                .await?
                .into_inner()
                .nodes)
        },
        |registered, role| async move {
            let endpoint = tonic::transport::Endpoint::from(registered)
                .tcp_nodelay(true)
                .connect_timeout(REMOTE_NODE_API_CONNECT_RPC_TIMEOUT);

            // Read the projected node-api token (falls back to the static
            // SPACE_INTERNAL_TOKEN). Read it only after registered-source admission;
            // the transfer stream authenticates its headers once when opened.
            let token = crate::auth::required_space_bearer_token(
                pplx_space_common_svc::bearer_token::SpaceBearerToken::NodeApi,
            )?;
            let endpoint = pplx_space_common_grpc::internal_transport::client_connect_endpoint(
                pplx_space_common_grpc::internal_transport::Service::NodeApi,
                endpoint,
            )
            .await
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
            let channel = endpoint.connect().await.map_err(|error| {
                Status::unavailable(format!(
                    "cannot connect to registered transfer source {remote_addr}: {error}"
                ))
            })?;

            Ok((channel, token, role))
        },
    )
    .await
}

fn optional_snapshot_dimensions(
    vcpu: Option<u32>,
    memory_mib: Option<u32>,
) -> Result<Option<(u32, u32)>, Status> {
    match (vcpu, memory_mib) {
        (None, None) => Ok(None),
        (Some(0), _) => Err(Status::invalid_argument(
            "snapshot_vcpu must be greater than 0 when set",
        )),
        (_, Some(0)) => Err(Status::invalid_argument(
            "snapshot_memory_mib must be greater than 0 when set",
        )),
        (Some(vcpu), Some(memory_mib)) => Ok(Some((vcpu, memory_mib))),
        _ => Err(Status::invalid_argument(
            "snapshot_vcpu and snapshot_memory_mib must be set together",
        )),
    }
}

/// Shared PullTemplate orchestration logic used by both WorkerApi and BuilderApi.
///
/// 1. Checks if the template already exists locally (`skip_if_exists`).
///    The `GetTemplate` call here auto-pulls from the configured S3
///    volume on local miss, sourcing the storage URI through
///    `ClusterApi.FindTemplateRelease` (see
///    `pplx_space_template_mgr::volume`). The skip check therefore
///    covers two cases: build was already local, or build was just
///    materialized from S3.
/// 2. Resolves the first `NodeSource`.
/// 3. Creates a gRPC client to the remote node-api-svc with bearer auth.
/// 4. Calls NodeExportTemplate on the remote, pipes chunks to local ImportTemplate.
///
/// Firecracker base snapshots are CPU-model sensitive. A snapshot captured on
/// one node may fail to restore on another node with a different exposed CPU
/// feature set, so node-to-node template pulls transfer the rootfs artifact and
/// rebuild any requested base snapshot locally after import.
pub async fn pull_template(
    tmpl_client: &mut pplx_space_template_mgr_client::TemplateManagerGrpcClient,
    req: &PullTemplateRequest,
) -> Result<PullTemplateResponse, Status> {
    let template_id = &req.template_id;

    let req_content_hash = req.content_hash.as_deref().unwrap_or("");
    let req_snapshot_dimensions =
        optional_snapshot_dimensions(req.snapshot_vcpu, req.snapshot_memory_mib)?;
    if req.include_snapshot && req_snapshot_dimensions.is_none() {
        return Err(Status::invalid_argument(
            "snapshot_vcpu and snapshot_memory_mib are required when include_snapshot is true",
        ));
    }

    // 1. Check local existence — only when an exact content_hash is provided.
    // Tag-based pulls (content_hash empty) always contact the source because
    // the remote's tag may point to a different build than the local one.
    if req.skip_if_exists
        && !req_content_hash.is_empty()
        && let Ok(resp) = pplx_space_common_grpc::fwd_retry!(
            "template-manager",
            tmpl_client.clone(),
            get_template,
            tonic::Request::new(pplx_space_template_mgr_client::proto::GetTemplateRequest {
                caller_org_id: None,
                template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                    name: template_id.clone(),
                    content_hash: req.content_hash.clone(),
                    tag: None,
                }),
                vcpu: req.snapshot_vcpu,
                memory_mib: req.snapshot_memory_mib,
            })
        )
    {
        let local = resp.into_inner();
        let local_hash = local
            .template
            .as_ref()
            .and_then(|t| t.content_hash.as_deref())
            .unwrap_or("");
        if local.state == "ready" && local_hash == req_content_hash {
            // When include_snapshot is requested, only skip if matching snapshot exists locally
            let snapshot_satisfied = !req.include_snapshot
                || (local.snapshot_dir.is_some()
                    && optional_snapshot_dimensions(
                        local.snapshot_vcpu,
                        local.snapshot_memory_mib,
                    )? == req_snapshot_dimensions);
            if snapshot_satisfied {
                return Ok(PullTemplateResponse {
                    content_hash: local_hash.to_string(),
                    already_existed: true,
                    bytes_transferred: 0,
                });
            }
        }
    }

    // 2. Resolve source
    let source = req
        .sources
        .first()
        .ok_or_else(|| Status::unimplemented("auto-resolve not yet supported, provide sources"))?;

    let node_source = match &source.source {
        Some(Source::Node(n)) => n,
        None => {
            return Err(Status::invalid_argument("empty source"));
        }
    };

    // 3. Connect to the remote node-api with bearer auth
    let remote_addr = &node_source.address;
    let (channel, token, role) = connect_remote(remote_addr).await?;

    // 4. Call NodeExportTemplate on remote, using the right client for the node's role
    let export_req = pplx_space_template_mgr_client::proto::ExportTemplateRequest {
        template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
            name: template_id.clone(),
            content_hash: req.content_hash.clone(),
            tag: req.tag.clone(),
        }),
        include_snapshot: false,
        snapshot_vcpu: None,
        snapshot_memory_mib: None,
    };
    let export_err = |e: Status| match e.code() {
        tonic::Code::NotFound => Status::not_found(format!("{remote_addr}: {}", e.message())),
        _ => Status::unavailable(format!("export from {remote_addr} failed: {e}")),
    };

    let mut export_stream = match role {
        crate::proto::NodeRole::Worker => {
            let intercepted =
                InterceptedService::new(channel, crate::auth::bearer_client_interceptor(token));
            crate::proto::worker_api_client::WorkerApiClient::new(wrap_grpc_client_channel(
                intercepted,
            ))
            .node_export_template(export_req)
            .await
            .map_err(export_err)?
            .into_inner()
        }
        crate::proto::NodeRole::Builder => {
            let intercepted =
                InterceptedService::new(channel, crate::auth::bearer_client_interceptor(token));
            crate::proto::builder_api_client::BuilderApiClient::new(wrap_grpc_client_channel(
                intercepted,
            ))
            .node_export_template(export_req)
            .await
            .map_err(export_err)?
            .into_inner()
        }
        crate::proto::NodeRole::Cluster => {
            return Err(Status::failed_precondition(
                "cannot pull templates from cluster nodes",
            ));
        }
    };

    // 5. Receive header
    let first = export_stream
        .next()
        .await
        .ok_or_else(|| Status::internal("empty export stream"))?
        .map_err(|e| Status::internal(format!("export stream error: {e}")))?;

    let export_header = match first.payload {
        Some(pplx_space_template_mgr_client::proto::export_template_chunk::Payload::Header(h)) => h,
        _ => return Err(Status::internal("first export message was not a header")),
    };

    // Extract resolved content_hash from the export header's TemplateRef
    let export_content_hash = export_header
        .template
        .as_ref()
        .and_then(|t| t.content_hash.as_deref())
        .unwrap_or("");

    // Validate content hash if specified
    if !req_content_hash.is_empty() && export_content_hash != req_content_hash {
        return Err(Status::data_loss(format!(
            "remote content hash {} doesn't match requested {}",
            export_content_hash, req_content_hash
        )));
    }

    // For tag-based pulls: now that we know the resolved hash from the source,
    // check if this exact build (+ snapshot if requested) already exists locally.
    if req.skip_if_exists
        && let Ok(resp) = pplx_space_common_grpc::fwd_retry!(
            "template-manager",
            tmpl_client.clone(),
            get_template,
            tonic::Request::new(pplx_space_template_mgr_client::proto::GetTemplateRequest {
                caller_org_id: None,
                template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                    name: template_id.clone(),
                    content_hash: Some(export_content_hash.to_string()),
                    tag: None,
                }),
                vcpu: req.snapshot_vcpu,
                memory_mib: req.snapshot_memory_mib,
            })
        )
    {
        let local = resp.into_inner();
        let local_hash = local
            .template
            .as_ref()
            .and_then(|t| t.content_hash.as_deref())
            .unwrap_or("");
        if local.state == "ready" && local_hash == export_content_hash {
            let snapshot_satisfied = !req.include_snapshot
                || (local.snapshot_dir.is_some()
                    && optional_snapshot_dimensions(
                        local.snapshot_vcpu,
                        local.snapshot_memory_mib,
                    )? == req_snapshot_dimensions);
            if snapshot_satisfied {
                return Ok(PullTemplateResponse {
                    content_hash: local_hash.to_string(),
                    already_existed: true,
                    bytes_transferred: 0,
                });
            }
        }
    }

    // 6. Pipe to local ImportTemplate
    let (tx, rx) = tokio::sync::mpsc::channel(8);

    // Send import header first
    let import_header = pplx_space_template_mgr_client::proto::ImportTemplateRequest {
        payload: Some(
            pplx_space_template_mgr_client::proto::import_template_request::Payload::Header(
                pplx_space_template_mgr_client::proto::ImportTemplateHeader {
                    template: Some(pplx_space_template_mgr_client::proto::TemplateRef {
                        name: template_id.clone(),
                        content_hash: Some(export_content_hash.to_string()),
                        tag: None,
                    }),
                    metadata_json: export_header.metadata_json,
                    include_snapshot: false,
                    snapshot_vcpu: export_header.snapshot_vcpu,
                    snapshot_memory_mib: export_header.snapshot_memory_mib,
                    manifest_json: export_header.manifest_json,
                    block_on_snapshot_policy_achieved: Some(false),
                    rootfs_hash: export_header.rootfs_hash,
                },
            ),
        ),
    };
    tx.send(import_header)
        .await
        .map_err(|error| Status::internal(format!("import channel closed: {error}")))?;

    // Spawn a task to pipe remaining chunks
    let pipe_handle = tokio::spawn(async move {
        let mut bytes: u64 = 0;
        while let Some(chunk_result) = export_stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    if let Some(
                        pplx_space_template_mgr_client::proto::export_template_chunk::Payload::Chunk(
                            file_chunk,
                        ),
                    ) = &chunk.payload
                    {
                        bytes += file_chunk.data.len() as u64;
                    }

                    let import_msg = pplx_space_template_mgr_client::proto::ImportTemplateRequest {
                        payload: match chunk.payload {
                            Some(
                                pplx_space_template_mgr_client::proto::export_template_chunk::Payload::Chunk(
                                    fc,
                                ),
                            ) => Some(
                                pplx_space_template_mgr_client::proto::import_template_request::Payload::Chunk(
                                    fc,
                                ),
                            ),
                            _ => continue, // skip unexpected messages
                        },
                    };
                    if tx.send(import_msg).await.is_err() {
                        return Err(Status::aborted("import channel closed during transfer"));
                    }
                }
                Err(e) => {
                    return Err(Status::aborted(format!("export stream error: {e}")));
                }
            }
        }
        Ok(bytes)
    });

    // Call local template-manager ImportTemplate
    let import_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let import_resp = tmpl_client
        .import_template(import_stream)
        .await
        .map_err(|e| Status::internal(format!("local import failed: {e}")))?;

    let bytes_transferred = pipe_handle
        .await
        .map_err(|e| Status::internal(format!("pipe task panicked: {e}")))?
        .map_err(|e| Status::internal(format!("pipe error: {e}")))?;

    let inner = import_resp.into_inner();
    let imported_hash = inner
        .template
        .as_ref()
        .and_then(|t| t.content_hash.clone())
        .unwrap_or_default();

    if req.include_snapshot {
        let (snapshot_vcpu, snapshot_memory_mib) = req_snapshot_dimensions.ok_or_else(|| {
            Status::internal("validated snapshot dimensions missing after template pull")
        })?;
        let snapshot_response = tmpl_client
            .update_template_snapshot_policy(
                pplx_space_template_mgr_client::proto::UpdateTemplateSnapshotPolicyRequest {
                    template: template_id.clone(),
                    ensure_base_snapshot: true,
                    snapshot_vcpu: Some(snapshot_vcpu),
                    snapshot_memory_mib: Some(snapshot_memory_mib),
                },
            )
            .await
            .map_err(|error| {
                Status::internal(format!(
                    "local base snapshot rebuild after template pull failed: {error}"
                ))
            })?
            .into_inner();
        if let Some(error) = snapshot_response.error.filter(|error| !error.is_empty()) {
            return Err(Status::failed_precondition(format!(
                "local base snapshot rebuild after template pull failed: {error}"
            )));
        }
    }

    Ok(PullTemplateResponse {
        content_hash: imported_hash,
        already_existed: false,
        bytes_transferred,
    })
}

#[cfg(test)]
mod tests;
