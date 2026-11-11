//! Reaching a node's node-api from admin routes: endpoint normalization and verification, the
//! client, and RPC error mapping.

use super::*;

pub(super) const NODE_API_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) const NODE_API_FAST_RPC_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) fn normalize_node_api_endpoint(
    raw: &str,
) -> Result<String, (StatusCode, Json<serde_json::Value>)> {
    let mut endpoint = raw.trim().trim_end_matches('/').to_string();
    if let Some(stripped) = endpoint.strip_prefix("http://") {
        endpoint = stripped.to_string();
    }
    if endpoint.starts_with("https://") {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must use plaintext node-api host:port",
        ));
    }
    if endpoint.is_empty() || endpoint.contains('/') {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must be a host:port endpoint",
        ));
    }
    let uri: axum::http::Uri = endpoint_uri(&endpoint).parse().map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid node_api_endpoint: {err}"),
        )
    })?;
    if uri
        .authority()
        .and_then(axum::http::uri::Authority::port_u16)
        .is_none()
    {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            "node_api_endpoint must include an explicit port",
        ));
    }
    Ok(endpoint)
}

pub(super) fn endpoint_uri(endpoint: &str) -> String {
    pplx_space_common_grpc::internal_transport::qualify_endpoint(endpoint)
}

pub(super) fn node_endpoint(
    endpoint: &str,
    request_timeout: Option<Duration>,
) -> Result<Endpoint, Box<dyn std::error::Error + Send + Sync>> {
    // `node_api_endpoint` is scheme-aware: it supplies the internal RPC scheme for bare
    // `host:port` and passes `http://`/`https://`-qualified inputs through
    // untouched. Trim whitespace only — stripping `http://` here would mangle
    // an `https://` input into `http://https://...`.
    let endpoint = endpoint.trim();
    Ok(crate::controllers::substrate::node_api_endpoint(
        endpoint,
        NODE_API_CONNECT_TIMEOUT,
        request_timeout.unwrap_or(crate::controllers::substrate::NODE_RPC_TIMEOUT),
    )?)
}

pub(super) fn proto_role_name(role: i32) -> Option<&'static str> {
    if role == ProtoNodeRole::Worker as i32 {
        Some(NodeRole::Worker.as_str())
    } else if role == ProtoNodeRole::Builder as i32 {
        Some(NodeRole::Builder.as_str())
    } else if role == ProtoNodeRole::Cluster as i32 {
        Some(NodeRole::Cluster.as_str())
    } else {
        None
    }
}

pub(super) fn bearer_kind_for_role(role: NodeRole) -> SpaceBearerToken {
    match role {
        NodeRole::Cluster => SpaceBearerToken::ClusterApi,
        NodeRole::Worker | NodeRole::Builder => SpaceBearerToken::NodeApi,
    }
}

pub(super) async fn verify_node_endpoint(
    endpoint: &str,
    expected_role: NodeRole,
) -> Result<NodeId, (StatusCode, Json<serde_json::Value>)> {
    let endpoint = node_endpoint(endpoint, Some(NODE_API_FAST_RPC_TIMEOUT)).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid node_api_endpoint: {err}"),
        )
    })?;
    let service = match expected_role {
        NodeRole::Cluster => pplx_space_common_grpc::internal_transport::Service::ClusterApi,
        NodeRole::Worker | NodeRole::Builder => {
            pplx_space_common_grpc::internal_transport::Service::NodeApi
        }
    };
    let endpoint =
        pplx_space_common_grpc::internal_transport::client_connect_endpoint(service, endpoint)
            .await
            .map_err(|error| api_error(StatusCode::BAD_GATEWAY, error.to_string()))?;
    let channel = endpoint.connect().await.map_err(|err| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api endpoint is not reachable: {err}"),
        )
    })?;
    let mut client = NodeInfoClient::new(crate::controllers::substrate::node_api_channel(
        channel,
        bearer_kind_for_role(expected_role),
    ));
    let response = client
        .get_node_info(GetNodeInfoRequest {})
        .await
        .map_err(|err| {
            api_error(
                StatusCode::BAD_GATEWAY,
                format!("node-api NodeInfo probe failed: {err}"),
            )
        })?
        .into_inner();

    let actual_role = proto_role_name(response.role).ok_or_else(|| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api returned unknown role {}", response.role),
        )
    })?;
    if actual_role != expected_role.as_str() {
        return Err(api_error(
            StatusCode::BAD_REQUEST,
            format!("node-api role mismatch: expected {expected_role}, got {actual_role}"),
        ));
    }

    response.node_id.parse::<NodeId>().map_err(|err| {
        api_error(
            StatusCode::BAD_GATEWAY,
            format!("node-api returned invalid node_id: {err}"),
        )
    })
}

pub(super) fn node_api_client(
    node: &NodeRow,
    label: &str,
    request_timeout: Duration,
) -> Result<NodeApiClient, (StatusCode, Json<serde_json::Value>)> {
    let raw_endpoint = node.node_api_endpoint.as_deref().ok_or_else(|| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{label} node has no node_api_endpoint"),
        )
    })?;
    let endpoint = node_endpoint(raw_endpoint, Some(request_timeout)).map_err(|err| {
        api_error(
            StatusCode::BAD_REQUEST,
            format!("invalid {label} node_api_endpoint: {err}"),
        )
    })?;
    Ok(NodeApiClient::new(endpoint))
}

pub(super) fn node_rpc_error(
    operation: &str,
    err: tonic::Status,
) -> (StatusCode, Json<serde_json::Value>) {
    let status = match err.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::FailedPrecondition => StatusCode::CONFLICT,
        _ => StatusCode::BAD_GATEWAY,
    };
    api_error(status, format!("{operation} failed: {err}"))
}

pub(super) fn node_api_rpc_error_response(
    node: &NodeRow,
    command: &'static str,
    status: tonic::Status,
) -> (StatusCode, Json<serde_json::Value>) {
    tracing::warn!(
        node_id = %node.id,
        endpoint = %node.node_api_endpoint.as_deref().unwrap_or("-"),
        command,
        grpc_code = ?status.code(),
        "node-api RPC failed"
    );
    node_rpc_error(command, status)
}
