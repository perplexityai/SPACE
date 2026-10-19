use pplx_space_common_core::types::OrgId;

use axum::{
    Extension, Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::IdempotencyKey;
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_public_api::{
    ClosePortExposureRequest, ListPortExposuresResponse, OpenPortExposureRequest, PortExposure,
    PortExposureApplyState, PortExposureDesiredState, PortExposureProtocol, PortExposureResponse,
    PortExposureVisibility, RequestFingerprint,
};
use sha2::{Digest, Sha256};
use sqlx::types::Json as SqlJson;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::{
    controllers::ControllerError,
    middleware::auth::AuthOrg,
    models::{
        Sandbox, SandboxPortExposure as ControllerPortExposure,
        SandboxPortProtocol as ControllerPortProtocol,
        SandboxPortVisibility as ControllerPortVisibility,
    },
    public_operations::{
        ApiProduct, HttpOperationMethod, OperationPathTemplate,
        delete_expired_idempotency_record_tx,
    },
    routes::AppState,
};

use super::{
    error::{PublicApiError, with_retry_after_hint},
    owner_scope::{AuthorizedSandbox, authorized_sandbox},
    sandboxes::parse_public_sandbox_id,
};

const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
const OPEN_PORT_PATH_TEMPLATE: &str = pplx_space_public_api::routes::OPEN_PORT.path();
const CLOSE_PORT_PATH_TEMPLATE: &str = pplx_space_public_api::routes::CLOSE_PORT.path();

pub async fn list_ports(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    Path(sandbox_id): Path<String>,
) -> Result<Json<ListPortExposuresResponse>, PublicApiError> {
    let sandbox = ensure_sandbox_owner(authorized, &state, org.id, &sandbox_id).await?;
    let exposures = state
        .sandbox_controller
        .list_sandbox_port_exposures(&state.pool, &sandbox)
        .await
        .map_err(controller_error)?
        .into_iter()
        .map(public_port_exposure)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListPortExposuresResponse { exposures }))
}

pub async fn open_port(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path((sandbox_id, port)): Path<(String, u16)>,
    body: Option<Json<serde_json::Value>>,
) -> Result<Json<PortExposureResponse>, PublicApiError> {
    let request = body
        .map(|Json(value)| serde_json::from_value::<OpenPortExposureRequest>(value))
        .transpose()
        .map_err(|_| PublicApiError::bad_request("invalid open port request"))?
        .unwrap_or(OpenPortExposureRequest {
            protocol: None,
            visibility: None,
        });
    apply_port_action(
        state,
        authorized,
        org.id,
        headers,
        sandbox_id,
        port,
        PortAction::Open(request),
    )
    .await
    .map(Json)
}

pub async fn close_port(
    State(state): State<AppState>,
    Extension(AuthOrg(org)): Extension<AuthOrg>,
    authorized: Option<Extension<AuthorizedSandbox>>,
    headers: HeaderMap,
    Path((sandbox_id, port)): Path<(String, u16)>,
    body: Option<Json<serde_json::Value>>,
) -> Result<Json<PortExposureResponse>, PublicApiError> {
    let request = body
        .map(|Json(value)| serde_json::from_value::<ClosePortExposureRequest>(value))
        .transpose()
        .map_err(|_| PublicApiError::bad_request("invalid close port request"))?
        .unwrap_or(ClosePortExposureRequest { protocol: None });
    apply_port_action(
        state,
        authorized,
        org.id,
        headers,
        sandbox_id,
        port,
        PortAction::Close(request),
    )
    .await
    .map(Json)
}

async fn apply_port_action(
    state: AppState,
    authorized: Option<Extension<AuthorizedSandbox>>,
    org_id: OrgId,
    headers: HeaderMap,
    sandbox_id: String,
    port: u16,
    action: PortAction,
) -> Result<PortExposureResponse, PublicApiError> {
    validate_port(port)?;
    let idempotency_key = idempotency_key(&headers)?;
    let fingerprint = port_action_fingerprint(&sandbox_id, port, &action)?;
    let path_template = action.path_template();
    if let Some(response) = find_idempotent_port_response(
        &state,
        org_id,
        &idempotency_key,
        path_template,
        &fingerprint,
    )
    .await?
    {
        return Ok(response);
    }

    let sandbox = ensure_sandbox_owner(authorized, &state, org_id, &sandbox_id).await?;
    let exposure = match &action {
        PortAction::Open(request) => {
            state
                .sandbox_controller
                .open_sandbox_port(
                    &state.pool,
                    &sandbox,
                    port,
                    request.protocol.map(controller_port_protocol),
                    request.visibility.map(controller_port_visibility),
                    "native",
                )
                .await
        }
        PortAction::Close(request) => {
            state
                .sandbox_controller
                .close_sandbox_port(
                    &state.pool,
                    &sandbox,
                    port,
                    request.protocol.map(controller_port_protocol),
                )
                .await
        }
    }
    .map_err(controller_error)?;
    let response = PortExposureResponse {
        exposure: public_port_exposure(exposure)?,
    };
    record_idempotent_port_response(
        &state,
        org_id,
        idempotency_key,
        path_template,
        fingerprint,
        &response,
    )
    .await
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PortAction {
    Open(OpenPortExposureRequest),
    Close(ClosePortExposureRequest),
}

impl PortAction {
    const fn path_template(&self) -> OperationPathTemplate {
        match self {
            Self::Open(_) => OperationPathTemplate::new(OPEN_PORT_PATH_TEMPLATE),
            Self::Close(_) => OperationPathTemplate::new(CLOSE_PORT_PATH_TEMPLATE),
        }
    }

    const fn label(&self) -> &'static str {
        match self {
            Self::Open(_) => "open",
            Self::Close(_) => "close",
        }
    }
}

async fn ensure_sandbox_owner(
    authorized: Option<Extension<AuthorizedSandbox>>,
    state: &AppState,
    org_id: OrgId,
    sandbox_id: &str,
) -> Result<Sandbox, PublicApiError> {
    let sandbox_id = parse_public_sandbox_id(sandbox_id)?;
    authorized_sandbox(authorized, &state.pool, org_id, sandbox_id).await
}

fn validate_port(port: u16) -> Result<(), PublicApiError> {
    if port == 0 {
        return Err(PublicApiError::bad_request(
            "port must be between 1 and 65535",
        ));
    }
    Ok(())
}

fn idempotency_key(headers: &HeaderMap) -> Result<IdempotencyKey, PublicApiError> {
    let Some(value) = headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(PublicApiError::bad_request(
            "Idempotency-Key header is required",
        ));
    };
    IdempotencyKey::parse(value)
        .map_err(|error| PublicApiError::bad_request(format!("invalid Idempotency-Key: {error}")))
}

fn controller_port_protocol(value: PortExposureProtocol) -> ControllerPortProtocol {
    match value {
        PortExposureProtocol::Tcp => ControllerPortProtocol::Tcp,
    }
}

fn controller_port_visibility(value: PortExposureVisibility) -> ControllerPortVisibility {
    match value {
        PortExposureVisibility::Private => ControllerPortVisibility::Private,
        PortExposureVisibility::Public => ControllerPortVisibility::Public,
    }
}

async fn find_idempotent_port_response(
    state: &AppState,
    org_id: OrgId,
    key: &IdempotencyKey,
    path_template: OperationPathTemplate,
    fingerprint: &RequestFingerprint,
) -> Result<Option<PortExposureResponse>, PublicApiError> {
    let row = sqlx::query_as::<_, (String, Option<SqlJson<serde_json::Value>>)>(sql!(
        "SELECT request_fingerprint, response_json
         FROM public_idempotency_records
         WHERE org_id = $1
           AND api_product = 'native'
           AND method = 'POST'
           AND path_template = $2
           AND idempotency_key = $3
           AND expires_at > NOW()"
    ))
    .bind(org_id)
    .bind(path_template.as_str())
    .bind(key.as_str())
    .fetch_optional(&state.pool)
    .await?;

    let Some((stored_fingerprint, response_json)) = row else {
        return Ok(None);
    };
    let stored_fingerprint =
        RequestFingerprint::parse(stored_fingerprint).map_err(|_| PublicApiError::internal())?;
    if stored_fingerprint != *fingerprint {
        return Err(PublicApiError::idempotency_conflict(
            "idempotency key was already used with a different request",
        ));
    }
    let Some(response_json) = response_json else {
        return Err(PublicApiError::idempotency_conflict(
            "idempotency key is already bound to an operation",
        ));
    };
    serde_json::from_value(response_json.0)
        .map(Some)
        .map_err(|error| {
            tracing::error!(%error, "stored port idempotency response is invalid");
            PublicApiError::internal()
        })
}

async fn record_idempotent_port_response(
    state: &AppState,
    org_id: OrgId,
    key: IdempotencyKey,
    path_template: OperationPathTemplate,
    fingerprint: RequestFingerprint,
    response: &PortExposureResponse,
) -> Result<PortExposureResponse, PublicApiError> {
    let response_json = serde_json::to_value(response).map_err(|error| {
        tracing::error!(%error, "failed to serialize port response for idempotency");
        PublicApiError::internal()
    })?;
    let mut tx = state.pool.begin().await?;
    delete_expired_idempotency_record_tx(
        &mut tx,
        org_id,
        ApiProduct::Native,
        HttpOperationMethod::Post,
        path_template,
        &key,
    )
    .await?;
    let inserted = sqlx::query(sql!(
        "INSERT INTO public_idempotency_records (
             id,
             org_id,
             api_product,
             method,
             path_template,
             idempotency_key,
             request_fingerprint,
             response_json
         )
         VALUES ($1, $2, 'native', 'POST', $3, $4, $5, $6)
         ON CONFLICT (org_id, api_product, method, path_template, idempotency_key) DO NOTHING"
    ))
    .bind(Uuid::now_v7())
    .bind(org_id)
    .bind(path_template.as_str())
    .bind(key.as_str())
    .bind(fingerprint.as_str())
    .bind(SqlJson(response_json))
    .execute(&mut *tx)
    .await?;

    if inserted.rows_affected() == 1 {
        tx.commit().await?;
        return Ok(response.clone());
    }

    tx.commit().await?;

    find_idempotent_port_response(state, org_id, &key, path_template, &fingerprint)
        .await?
        .ok_or_else(PublicApiError::internal)
}

fn port_action_fingerprint(
    sandbox_id: &str,
    port: u16,
    action: &PortAction,
) -> Result<RequestFingerprint, PublicApiError> {
    let body = match action {
        PortAction::Open(request) => serde_json::json!({
            "sandbox_id": sandbox_id,
            "port": port,
            "action": action.label(),
            "request": request,
        }),
        PortAction::Close(request) => serde_json::json!({
            "sandbox_id": sandbox_id,
            "port": port,
            "action": action.label(),
            "request": request,
        }),
    };
    let bytes = serde_json::to_vec(&body).map_err(|error| {
        tracing::error!(%error, "failed to serialize port idempotency fingerprint");
        PublicApiError::internal()
    })?;
    RequestFingerprint::parse(hex::encode(Sha256::digest(bytes))).map_err(|error| {
        tracing::error!(%error, "failed to build port idempotency fingerprint");
        PublicApiError::internal()
    })
}

fn public_port_exposure(exposure: ControllerPortExposure) -> Result<PortExposure, PublicApiError> {
    let port = u16::try_from(exposure.guest_port).map_err(|_| PublicApiError::internal())?;
    Ok(PortExposure {
        port,
        protocol: exposure
            .protocol
            .parse()
            .unwrap_or(PortExposureProtocol::Tcp),
        visibility: exposure
            .visibility
            .parse()
            .unwrap_or(PortExposureVisibility::Private),
        desired_state: exposure
            .desired_state
            .parse()
            .unwrap_or(PortExposureDesiredState::Closed),
        apply_state: exposure
            .apply_state
            .parse()
            .unwrap_or(PortExposureApplyState::Pending),
        last_error: exposure.last_error,
        last_bridge_status: exposure.last_bridge_status,
        last_applied_bridge_generation: exposure.last_applied_bridge_generation,
        last_observed_at: exposure.last_observed_at.and_then(format_time),
        created_at: format_time(exposure.created_at).ok_or_else(PublicApiError::internal)?,
        updated_at: format_time(exposure.updated_at).ok_or_else(PublicApiError::internal)?,
    })
}

fn format_time(value: OffsetDateTime) -> Option<String> {
    value
        .format(&Rfc3339)
        .ok()
        .or_else(|| Some(value.unix_timestamp().to_string()))
}

pub(in crate::routes::public) fn controller_error(error: ControllerError) -> PublicApiError {
    // A lock race is this service's own database, not the sandbox fleet. The
    // status match below cannot tell it from an out-of-capacity 503, so it
    // would attribute it to the cluster or the node.
    if error.is_lock_contention() {
        return PublicApiError::lock_contention();
    }
    let retry_after = error.retry_after_header();
    match error.status() {
        StatusCode::BAD_REQUEST => PublicApiError::bad_request(error.to_string()),
        StatusCode::FORBIDDEN => PublicApiError::forbidden(error.to_string()),
        StatusCode::NOT_FOUND => PublicApiError::not_found(error.to_string()),
        StatusCode::CONFLICT => PublicApiError::conflict(error.to_string()),
        StatusCode::PRECONDITION_FAILED => PublicApiError::failed_precondition(error.to_string()),
        StatusCode::TOO_MANY_REQUESTS => with_retry_after_hint(
            PublicApiError::rate_limited(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::Admission),
            retry_after,
        ),
        StatusCode::SERVICE_UNAVAILABLE => with_retry_after_hint(
            PublicApiError::service_unavailable(error.to_string())
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus),
            retry_after,
        ),
        status if status.is_server_error() => {
            tracing::warn!(%error, "native port operation failed");
            PublicApiError::internal()
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendStatus)
        }
        _ => PublicApiError::internal(),
    }
}

#[cfg(test)]
mod tests;
