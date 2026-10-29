//! Shared types and helpers for the E2B Compatibility Wrapper (ECW), SPACE's
//! E2B-compatible API surface. ECW translates at the HTTP boundary and remains
//! separate from the SPACE API.

use std::collections::BTreeMap;
use std::net::IpAddr;

use axum::{
    Json,
    body::Bytes,
    extract::{
        FromRequest, FromRequestParts, OptionalFromRequest, Query, Request,
        rejection::{BytesRejection, JsonRejection, QueryRejection},
    },
    http::{StatusCode, header},
    response::IntoResponse,
};
use pplx_space_common_core::types::{
    AutoLifecycleActivitySource, AutoLifecycleConditionKind, AutoLifecycleConditionSet,
    AutoLifecycleConditionSetId, AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleStage,
    AutoLifecycleWakePolicy, AutoLifecycleWakeSource, EcwCredentialEpoch, SandboxId,
};
use pplx_space_common_svc::ecw_credential::{EcwCredentialKeyring, EcwCredentialKind};
use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};
use pplx_space_net_common::{EgressDestinationV1, IpCidr};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::OffsetDateTime;

use crate::controllers::auto_lifecycle::ACCESS_IDLE_CONDITION_SET;
use crate::models::SandboxResponse;

pub(crate) const ECW_RESPONSE_DEFAULT_ENVD_VERSION: &str = "0.5.7";
const ECW_STATIC_RESOLVER_ALLOW_IPS: &[&str] = &["8.8.8.8", "1.1.1.1"];

pub(crate) fn with_ecw_dns_resolver_allows(
    allow_destinations: Vec<EgressDestinationV1>,
) -> Vec<EgressDestinationV1> {
    let missing_resolvers = ECW_STATIC_RESOLVER_ALLOW_IPS
        .iter()
        .filter(|raw_ip| {
            let ip = raw_ip
                .parse::<IpAddr>()
                .expect("static ECW resolver IP must be valid");
            !allow_destinations
                .iter()
                .any(|destination| destination.matches_ip(ip))
        })
        .map(|raw_ip| {
            let cidr = format!("{raw_ip}/32")
                .parse::<IpCidr>()
                .expect("static ECW resolver CIDR must be valid");
            EgressDestinationV1::Cidr(cidr)
        })
        .collect::<Vec<_>>();

    allow_destinations
        .into_iter()
        .chain(missing_resolvers)
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct E2bLifecycleConfig {
    pub(crate) timeout_secs: u32,
    pub(crate) auto_pause: bool,
    pub(crate) auto_resume: bool,
}

pub(crate) fn auto_lifecycle_policy(
    timeout_secs: u32,
    auto_pause: bool,
    auto_resume: bool,
) -> AutoLifecyclePolicy {
    access_idle_policy(u64::from(timeout_secs.max(1)), auto_pause, auto_resume)
}

pub(crate) fn access_idle_policy(
    timeout_secs: u64,
    auto_pause: bool,
    auto_resume: bool,
) -> AutoLifecyclePolicy {
    let action = if auto_pause {
        AutoLifecycleIdleAction::Suspend
    } else {
        AutoLifecycleIdleAction::Delete
    };
    AutoLifecyclePolicy {
        wake: AutoLifecycleWakePolicy {
            on: if auto_pause && auto_resume {
                vec![AutoLifecycleWakeSource::Access]
            } else {
                Vec::new()
            },
        },
        condition_sets: BTreeMap::from([(
            AutoLifecycleConditionSetId::AccessIdle,
            AutoLifecycleConditionSet::access_idle(),
        )]),
        stages: vec![AutoLifecycleStage {
            condition_set: AutoLifecycleConditionSetId::AccessIdle,
            at: timeout_secs,
            action,
        }],
    }
}

pub(crate) fn lifecycle_config_from_policy(
    policy: &AutoLifecyclePolicy,
) -> Option<E2bLifecycleConfig> {
    let condition = policy.condition_sets.get(&ACCESS_IDLE_CONDITION_SET)?;
    if condition.kind != AutoLifecycleConditionKind::Inactivity
        || condition.activity_source != Some(AutoLifecycleActivitySource::Access)
    {
        return None;
    }
    let mut access_stages = policy
        .stages
        .iter()
        .filter(|stage| stage.condition_set == ACCESS_IDLE_CONDITION_SET);
    let stage = access_stages.next()?;
    if access_stages.next().is_some() {
        return None;
    }
    let auto_pause = match stage.action {
        AutoLifecycleIdleAction::Suspend => true,
        AutoLifecycleIdleAction::Delete => false,
        _ => return None,
    };
    Some(E2bLifecycleConfig {
        timeout_secs: stage.at.min(u64::from(u32::MAX)) as u32,
        auto_pause,
        auto_resume: auto_pause && policy.wake_on_access(),
    })
}

pub(crate) fn policy_for_timeout_update(
    current_policy: &AutoLifecyclePolicy,
    timeout_secs: u32,
) -> AutoLifecyclePolicy {
    let current_config = lifecycle_config_from_policy(current_policy);
    auto_lifecycle_policy(
        timeout_secs,
        current_config
            .map(|config| config.auto_pause)
            .unwrap_or(false),
        current_config
            .map(|config| config.auto_resume)
            .unwrap_or(false),
    )
}

pub(crate) fn build_sandbox_response(
    keyring: &EcwCredentialKeyring,
    native: &SandboxResponse,
    template_id: String,
    domain: Option<String>,
    credential_epoch: EcwCredentialEpoch,
) -> schema::SandboxResponseE2b {
    schema::SandboxResponseE2b {
        sandbox_id: native.id.to_string(),
        template_id,
        client_id: native.id.to_string(),
        alias: None,
        envd_version: ECW_RESPONSE_DEFAULT_ENVD_VERSION.to_string(),
        envd_access_token: ecw_token(
            keyring,
            native.id,
            credential_epoch,
            EcwCredentialKind::Envd,
        ),
        traffic_access_token: ecw_token(
            keyring,
            native.id,
            credential_epoch,
            EcwCredentialKind::Traffic,
        ),
        domain,
    }
}

pub(crate) fn build_fork_sandbox_result(
    keyring: &EcwCredentialKeyring,
    sandbox_id: SandboxId,
    template_id: String,
    domain: Option<&str>,
    credential_epoch: EcwCredentialEpoch,
) -> Value {
    let native = SandboxResponse {
        id: sandbox_id,
        ..SandboxResponse::default()
    };
    serde_json::to_value(schema::SandboxForkResultE2b::Sandbox(
        build_sandbox_response(
            keyring,
            &native,
            template_id,
            domain.map(str::to_string),
            credential_epoch,
        ),
    ))
    .expect("sandbox fork result serializes")
}

pub(crate) fn build_fork_error_result(code: i32, message: impl Into<String>) -> Value {
    serde_json::to_value(schema::SandboxForkResultE2b::Error {
        error: schema::SandboxForkErrorE2b {
            code,
            message: message.into(),
        },
    })
    .expect("sandbox fork error serializes")
}

pub(crate) fn ecw_token(
    keyring: &EcwCredentialKeyring,
    sandbox_id: SandboxId,
    credential_epoch: EcwCredentialEpoch,
    kind: EcwCredentialKind,
) -> Option<String> {
    keyring
        .mint(sandbox_id, credential_epoch, kind)
        .inspect_err(|error| tracing::error!(%error, "failed to mint ECW credential"))
        .ok()
        .flatten()
        .map(|token| token.secret().to_owned())
}

/// E2B-compatible HTTP schema and translation helpers.
///
/// These types intentionally model the JSON boundary of the E2B SDK. Keep them
/// decoupled from the SPACE API's `/v1/*` DTOs so ECW route handlers can do
/// explicit request/response translation at the edge.
pub mod schema {
    use super::*;

    pub type StringMap = BTreeMap<String, String>;

    #[derive(Debug, Clone, Deserialize)]
    pub struct CreateSandboxRequestE2b {
        #[serde(default, rename = "templateID")]
        pub template_id: Option<String>,
        #[serde(default)]
        pub timeout: Option<u32>,
        #[serde(default)]
        #[serde(rename = "autoPause")]
        pub auto_pause: Option<bool>,
        #[serde(default)]
        #[serde(rename = "autoResume")]
        pub auto_resume: Option<Value>,
        /// Legacy create flag. E2B's newer contract always creates securely,
        /// so ECW accepts but ignores either value on both route aliases.
        #[serde(default)]
        pub secure: Option<bool>,
        #[serde(default)]
        #[serde(alias = "allowInternetAccess")]
        pub allow_internet_access: Option<bool>,
        #[serde(default)]
        pub network: Option<Value>,
        #[serde(default)]
        pub metadata: StringMap,
        #[serde(default, rename = "envVars")]
        pub env_vars: StringMap,
        #[serde(default)]
        pub mcp: Option<Value>,
        #[serde(default, rename = "volumeMounts")]
        pub volume_mounts: Vec<SandboxVolumeMountE2b>,
        #[serde(default, rename = "autoPauseMemory")]
        pub auto_pause_memory: Option<bool>,
        #[serde(default)]
        pub iam: Option<Value>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct SandboxResponseE2b {
        #[serde(rename = "sandboxID")]
        pub sandbox_id: String,
        #[serde(rename = "templateID")]
        pub template_id: String,
        #[serde(rename = "clientID")]
        pub client_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
        #[serde(rename = "envdVersion")]
        pub envd_version: String,
        #[serde(rename = "envdAccessToken", skip_serializing_if = "Option::is_none")]
        pub envd_access_token: Option<String>,
        #[serde(rename = "trafficAccessToken", skip_serializing_if = "Option::is_none")]
        pub traffic_access_token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub domain: Option<String>,
    }

    #[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
    pub struct SandboxForkRequestE2b {
        #[serde(default)]
        pub timeout: Option<u32>,
        #[serde(default)]
        pub count: Option<u32>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Serialize)]
    pub struct SandboxForkErrorE2b {
        pub code: i32,
        pub message: String,
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(untagged)]
    pub enum SandboxForkResultE2b {
        Sandbox(SandboxResponseE2b),
        Error { error: SandboxForkErrorE2b },
    }

    /// Stored ownership, independent of caller-editable sandbox metadata.
    #[derive(Debug, Clone, Serialize)]
    #[serde(tag = "source", rename_all = "snake_case")]
    pub enum SandboxOwnershipE2b {
        Local,
        FederatedIdentity {
            issuer: pplx_space_common_core::types::UpstreamIssuer,
            #[serde(rename = "orgID")]
            org_id: pplx_space_common_core::types::UpstreamOrgId,
        },
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct SandboxDetailE2b {
        pub ownership: SandboxOwnershipE2b,
        #[serde(rename = "templateID")]
        pub template_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
        #[serde(rename = "sandboxID")]
        pub sandbox_id: String,
        #[serde(rename = "clientID")]
        pub client_id: String,
        #[serde(rename = "startedAt")]
        pub started_at: String,
        #[serde(rename = "endAt")]
        pub end_at: String,
        #[serde(rename = "envdVersion")]
        pub envd_version: String,
        #[serde(rename = "envdAccessToken", skip_serializing_if = "Option::is_none")]
        pub envd_access_token: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub domain: Option<String>,
        #[serde(rename = "cpuCount")]
        pub cpu_count: u32,
        #[serde(rename = "memoryMB")]
        pub memory_mb: u32,
        #[serde(rename = "diskSizeMB")]
        pub disk_size_mb: u32,
        pub metadata: StringMap,
        pub state: SandboxStateE2b,
        #[serde(
            rename = "volumeMounts",
            default,
            skip_serializing_if = "Vec::is_empty"
        )]
        pub volume_mounts: Vec<SandboxVolumeMountE2b>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct ListedSandboxE2b {
        #[serde(rename = "templateID")]
        pub template_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        pub alias: Option<String>,
        #[serde(rename = "sandboxID")]
        pub sandbox_id: String,
        #[serde(rename = "clientID")]
        pub client_id: String,
        #[serde(rename = "startedAt")]
        pub started_at: String,
        #[serde(rename = "endAt")]
        pub end_at: String,
        #[serde(rename = "cpuCount")]
        pub cpu_count: u32,
        #[serde(rename = "memoryMB")]
        pub memory_mb: u32,
        #[serde(rename = "diskSizeMB")]
        pub disk_size_mb: u32,
        pub metadata: StringMap,
        pub state: SandboxStateE2b,
        #[serde(rename = "envdVersion")]
        pub envd_version: String,
        #[serde(
            rename = "volumeMounts",
            default,
            skip_serializing_if = "Vec::is_empty"
        )]
        pub volume_mounts: Vec<SandboxVolumeMountE2b>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct SandboxTimeoutRequestE2b {
        pub timeout: u32,
    }

    #[derive(Debug, Clone, Copy, Default, Deserialize)]
    pub struct ConnectSandboxRequestE2b {
        #[serde(default)]
        pub timeout: Option<u32>,
        #[serde(default)]
        pub memory: Option<bool>,
    }

    #[derive(Debug, Clone, Copy, Default, Deserialize)]
    pub struct SandboxPauseRequestE2b {
        #[serde(default)]
        pub memory: Option<bool>,
    }

    #[derive(Debug, Clone, Copy, Default, Deserialize)]
    pub struct SandboxResumeRequestE2b {
        #[serde(default)]
        pub timeout: Option<u32>,
        #[serde(default, rename = "autoPause")]
        pub auto_pause: Option<bool>,
        #[serde(default)]
        pub memory: Option<bool>,
    }

    #[derive(Debug, Clone, Copy, Default, Deserialize)]
    pub struct SandboxRefreshRequestE2b {
        #[serde(default)]
        pub duration: Option<u32>,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct SandboxListQueryE2b {
        #[serde(default)]
        pub limit: Option<u32>,
        #[serde(default)]
        #[serde(rename = "nextToken", alias = "cursor")]
        pub next_token: Option<String>,
        #[serde(default)]
        pub state: Option<String>,
        #[serde(default)]
        pub metadata: Option<String>,
        #[serde(default)]
        pub order: Option<String>,
        #[serde(default, rename = "startedAfter")]
        pub started_after: Option<String>,
        #[serde(default)]
        pub template: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum SandboxStateE2b {
        Running,
        Paused,
        Failed,
    }

    #[derive(Debug, Clone, Serialize)]
    #[serde(transparent)]
    pub struct ListedSandboxesResponseE2b(pub Vec<ListedSandboxE2b>);

    #[derive(Debug, Clone, Serialize)]
    pub struct HostResponseE2b {
        pub host: String,
        pub port: u16,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct FileReadQueryE2b {
        pub path: String,
        #[serde(default)]
        pub format: Option<FileReadFormatE2b>,
        #[serde(default)]
        pub username: Option<String>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum FileReadFormatE2b {
        Text,
        Bytes,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct FileWriteRequestE2b {
        pub path: String,
        pub content: Value,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SandboxVolumeMountE2b {
        pub name: String,
        pub path: String,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct FileInfoE2b {
        pub name: String,
        pub path: String,
        #[serde(rename = "type")]
        pub file_type: FileTypeE2b,
        pub size: u64,
        pub mode: u32,
        #[serde(rename = "modTime")]
        pub mod_time: Option<String>,
    }

    #[derive(Debug, Clone, Copy, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum FileTypeE2b {
        File,
        Dir,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ProcessStartRequestE2b {
        pub cmd: String,
        #[serde(default)]
        pub args: Vec<String>,
        #[serde(default)]
        pub cwd: Option<String>,
        #[serde(default)]
        pub envs: StringMap,
        #[serde(default)]
        pub background: Option<bool>,
    }

    #[derive(Debug, Clone, Serialize)]
    pub struct ProcessStartResponseE2b {
        #[serde(rename = "processID")]
        pub process_id: String,
        #[serde(rename = "exitCode")]
        pub exit_code: Option<i32>,
        pub stdout: String,
        pub stderr: String,
    }

    #[derive(Debug, Clone, Deserialize)]
    pub struct ProcessSignalRequestE2b {
        pub signal: ProcessSignalE2b,
    }

    #[derive(Debug, Clone, Copy, Deserialize, Serialize)]
    #[serde(rename_all = "SCREAMING_SNAKE_CASE")]
    pub enum ProcessSignalE2b {
        Sigterm,
        Sigkill,
        Sigint,
    }

    pub fn e2b_timestamp(ts: OffsetDateTime) -> String {
        ts.format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| ts.unix_timestamp().to_string())
    }
}

/// JSON extractor that keeps Axum/serde failures on the E2B error contract.
///
/// E2B's OpenAPI schemas are open objects, so wire DTOs intentionally accept
/// unknown fields for forward compatibility. Fields whose values would change
/// semantics are modeled and validated explicitly by their owning handler.
pub(crate) struct E2bJson<T>(pub(crate) T);

#[derive(Debug, Default, Deserialize)]
#[cfg(test)]
pub(crate) struct EmptyE2bBody {}

fn json_rejection_error(rejection: JsonRejection) -> errors::E2bApiError {
    match rejection {
        JsonRejection::MissingJsonContentType(_) => errors::E2bApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "E2B request bodies require Content-Type: application/json",
        ),
        JsonRejection::JsonDataError(_) => {
            errors::E2bApiError::bad_request("Invalid E2B request body")
        }
        JsonRejection::JsonSyntaxError(_) => {
            errors::E2bApiError::bad_request("Malformed E2B JSON request body")
        }
        JsonRejection::BytesRejection(_) => {
            errors::E2bApiError::new(rejection.status(), "Unable to read E2B request body")
        }
        _ => errors::E2bApiError::new(rejection.status(), "Invalid E2B request body"),
    }
}

impl<S, T> FromRequest<S> for E2bJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = errors::E2bApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        <Json<T> as FromRequest<S>>::from_request(request, state)
            .await
            .map(|Json(value)| Self(value))
            .map_err(json_rejection_error)
    }
}

impl<S, T> OptionalFromRequest<S> for E2bJson<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = errors::E2bApiError;

    async fn from_request(request: Request, state: &S) -> Result<Option<Self>, Self::Rejection> {
        let content_type = request.headers().get(header::CONTENT_TYPE).cloned();
        let body =
            Bytes::from_request(request, state)
                .await
                .map_err(|rejection: BytesRejection| {
                    errors::E2bApiError::new(rejection.status(), "Unable to read E2B request body")
                })?;
        if body.is_empty() {
            return Ok(None);
        }
        let Some(content_type) = content_type else {
            return Err(errors::E2bApiError::new(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "E2B request bodies require Content-Type: application/json",
            ));
        };

        // The body has already passed the configured size limit. Rebuild the
        // in-memory request so Axum remains the source of truth for JSON media
        // type validation and serde rejection classification.
        let mut request = Request::new(body.into());
        request
            .headers_mut()
            .insert(header::CONTENT_TYPE, content_type);
        <Self as FromRequest<S>>::from_request(request, state)
            .await
            .map(Some)
    }
}

/// Query extractor that reports malformed recognized E2B query parameters
/// using the same JSON error envelope as handler-level failures. Unknown keys
/// remain accepted to match E2B's open OpenAPI contract.
pub(crate) struct E2bQuery<T>(pub(crate) T);

impl<S, T> FromRequestParts<S> for E2bQuery<T>
where
    S: Send + Sync,
    T: serde::de::DeserializeOwned,
{
    type Rejection = errors::E2bApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(value)| Self(value))
            .map_err(|_rejection: QueryRejection| {
                errors::E2bApiError::bad_request("Invalid E2B query parameters")
            })
    }
}

pub mod errors {
    use super::*;

    #[derive(Debug, Clone, thiserror::Error)]
    #[error("{message}")]
    pub struct E2bApiError {
        pub status: StatusCode,
        pub message: String,
        pub origin: ResponseOrigin,
        pub kind: ResponseKind,
        /// When set, the response carries a `Retry-After: <secs>` header and
        /// marks a retryable transient outcome. Mirrors
        /// [`crate::controllers::ControllerError`]'s retry hint.
        retry_after: Option<std::time::Duration>,
    }

    impl E2bApiError {
        pub fn bad_request(message: impl Into<String>) -> Self {
            Self::new(StatusCode::BAD_REQUEST, message)
        }

        pub fn unauthorized() -> Self {
            Self::new(StatusCode::UNAUTHORIZED, "Invalid or missing API key")
        }

        pub fn not_found(resource: &'static str) -> Self {
            Self::new(StatusCode::NOT_FOUND, format!("{resource} not found"))
        }

        pub fn not_implemented(message: impl Into<String>) -> Self {
            Self::new(StatusCode::NOT_IMPLEMENTED, message)
        }

        pub fn sandbox_not_ready() -> Self {
            Self::new(StatusCode::CONFLICT, "Sandbox is not running")
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteNotReady)
        }

        /// The per-client concurrency limiter rejected this request: the client
        /// already has the maximum number of concurrent create/reconnect
        /// operations in flight. This is a *per-client* limit the caller
        /// controls — the message says so explicitly, so a client can tell this
        /// (which it can avoid by capping its own concurrency) apart from a
        /// server-wide capacity throttle (which it cannot). Retryable once the
        /// caller's own in-flight operations drain. Pair with
        /// [`Self::with_retry_after`].
        pub fn too_many_client_operations() -> Self {
            Self::new(
                StatusCode::TOO_MANY_REQUESTS,
                "Per-client rate limit: too many concurrent sandbox operations for this client. \
                 Reduce your in-flight create/reconnect requests and retry.",
            )
        }

        /// ECW compatibility alias for the retryable data-plane-unavailable
        /// wire shape. Keep this as HTTP 502 "Sandbox is not running": E2B
        /// clients can retry this `SandboxException`. A true SDK
        /// `TimeoutException` carrying the sandbox-timeout marker is not
        /// retried by the SDK retry path, so using that shape would turn a
        /// transient failure into a terminal one.
        pub fn sandbox_timeout() -> Self {
            Self::sandbox_data_plane_unavailable()
        }

        pub fn sandbox_data_plane_unavailable() -> Self {
            Self::new(StatusCode::BAD_GATEWAY, "Sandbox is not running")
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteUnavailable)
        }

        /// Retryable shape for a data-plane request that failed while the
        /// sandbox is plausibly mid-wake (wake/restore in flight, or the
        /// guest still re-establishing listeners right after the lifecycle
        /// flip). Kept at 502 for SDK compatibility, but with a body message
        /// distinct from both the terminal
        /// [`Self::sandbox_data_plane_unavailable`] shape and the
        /// genuine-fault upstream shape, so clients can retry this one and
        /// give up on the others.
        pub fn sandbox_waking() -> Self {
            Self::new(StatusCode::BAD_GATEWAY, "Sandbox is waking; retry")
                .with_classification(ResponseOrigin::Cluster, ResponseKind::RouteNotReady)
                .with_retry_after(crate::controllers::substrate::GATEWAY_RETRY_AFTER)
        }

        pub fn failed_to_place_sandbox() -> Self {
            Self::new(StatusCode::INTERNAL_SERVER_ERROR, "Failed to place sandbox")
                .with_classification(ResponseOrigin::Cluster, ResponseKind::Admission)
        }

        pub fn upstream(message: impl Into<String>) -> Self {
            Self::new(StatusCode::BAD_GATEWAY, message)
                .with_classification(ResponseOrigin::Node, ResponseKind::BackendTransport)
        }

        pub fn internal(message: impl Into<String>) -> Self {
            Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
        }

        /// A database statement lost a race for a lock; the same request
        /// succeeds on retry.
        ///
        /// 503 rather than the 500 every other database error takes, and
        /// classified `Api`/`Internal` rather than `Cluster`/`Node`: the
        /// contention is in this service's own database, so attributing it to
        /// the fleet would blame the wrong component in the response metrics
        /// and tell the SDK the request is unrecoverable.
        pub fn lock_contention() -> Self {
            Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "Operation conflicted with a concurrent change; retry the request",
            )
            .with_classification(ResponseOrigin::Api, ResponseKind::Internal)
            // Not the 5s gateway constant (~20x the sub-second bound this
            // request hit), and not nothing either: without a hint an SDK
            // retries at RTT against the row that is already contended. See
            // `LOCK_CONTENTION_RETRY_AFTER`.
            .with_retry_after(crate::controllers::substrate::LOCK_CONTENTION_RETRY_AFTER)
        }

        /// Status and response classification, for tests that assert how a
        /// translator attributes an error. Fields stay private.
        #[cfg(test)]
        pub(crate) fn status_for_test(&self) -> StatusCode {
            self.status
        }

        /// Companion to [`Self::status_for_test`].
        #[cfg(test)]
        pub(crate) fn classification_for_test(&self) -> (ResponseOrigin, ResponseKind) {
            (self.origin, self.kind)
        }

        pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
            let (origin, kind) = default_classification(status);
            Self {
                status,
                message: message.into(),
                origin,
                kind,
                retry_after: None,
            }
        }

        /// Attach a `Retry-After` hint, signalling a retryable transient
        /// outcome.
        pub fn with_retry_after(mut self, retry_after: std::time::Duration) -> Self {
            self.retry_after = Some(retry_after);
            self
        }

        pub fn with_classification(mut self, origin: ResponseOrigin, kind: ResponseKind) -> Self {
            self.origin = origin;
            self.kind = kind;
            self
        }

        /// The `Retry-After` hint, if this error is a retryable transient
        /// outcome.
        pub fn retry_after(&self) -> Option<std::time::Duration> {
            self.retry_after
        }
    }

    impl IntoResponse for E2bApiError {
        fn into_response(self) -> axum::response::Response {
            let origin = self.origin;
            let kind = self.kind;
            pplx_space_metrics::response::ecw_api(origin, kind, self.status.as_u16());
            tracing::Span::current().record("space.response_origin", origin.as_str());
            tracing::Span::current().record("space.response_kind", kind.as_str());
            // Client-error bodies may legitimately identify a rejected field,
            // but they can also contain caller-controlled values. Keep those
            // values out of telemetry while retaining the detailed wire error.
            // Non-client failures are already sanitized at their translators.
            let span_description = if origin == ResponseOrigin::Client {
                kind.as_str()
            } else {
                self.message.as_str()
            };
            tracing::Span::current().record("otel.status_description", span_description);

            let body = E2bErrorResponse {
                code: self.status.as_u16().into(),
                message: self.message,
            };

            let mut response = (self.status, Json(body)).into_response();
            // Through the shared renderer, not a bare `as_secs()`: that
            // truncates, so any sub-second hint would emit `Retry-After: 0`
            // and invite an immediate retry into the same contended lock.
            if let Some(retry_after) = self.retry_after
                && let Some(value) = crate::controllers::retry_after_header_value(retry_after)
            {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            response
        }
    }

    #[derive(Debug, Serialize)]
    pub struct E2bErrorResponse {
        pub code: i32,
        pub message: String,
    }

    fn default_classification(status: StatusCode) -> (ResponseOrigin, ResponseKind) {
        if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
            return (ResponseOrigin::Client, ResponseKind::Auth);
        }
        if status == StatusCode::TOO_MANY_REQUESTS {
            return (ResponseOrigin::Client, ResponseKind::Admission);
        }
        if status.is_client_error() {
            return (ResponseOrigin::Client, ResponseKind::Validation);
        }
        (ResponseOrigin::Api, ResponseKind::Internal)
    }
}

#[cfg(test)]
mod tests;
