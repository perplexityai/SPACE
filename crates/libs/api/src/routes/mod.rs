pub mod admin;
pub(crate) mod authz;
mod debug;
pub mod e2b;
mod ecw_credential_epoch;
mod metrics;
pub(crate) mod public;
mod public_docs;
mod session_auth;

use axum::{
    Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
};
use pplx_space_common_svc::{HttpServerMakeSpan, HttpSpanMetadata, http_trace_layer};
use sqlx::PgPool;
use std::borrow::Cow;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower_http::cors::CorsLayer;

pub fn validate_public_cursor_secret() -> Result<(), String> {
    public::validate_public_cursor_secret().map_err(|error| error.to_string())
}

#[derive(Clone)]
pub struct AppState {
    /// General-purpose pool connected as `space_user`, which holds no grants
    /// on the credential-fenced tables.
    pub pool: PgPool,
    /// Pool pinned to `space_creds`, the login holding grants on the
    /// credential-fenced tables (`user_credentials`, `org_credentials`,
    /// `org_master_keys`). Fenced statements — and any transaction that
    /// mixes them with unfenced ones — must run here instead of `pool`.
    pub credentials_pool: pplx_space_common_svc::sql::CredentialsPool,
    pub sandbox_controller: crate::controllers::sandbox::SandboxController,
    pub template_controller: crate::controllers::template::TemplateController,
    pub client_classification: crate::client_classification::ClientClassification,
    /// Per-client concurrency limiter for control-plane-touching create and
    /// reconnect operations. Guardrail against a single client saturating the
    /// Kubernetes control-plane API. See [`crate::client_concurrency`].
    pub client_concurrency: crate::client_concurrency::ClientConcurrencyLimiter,
    /// Per-org and per-user live-sandbox limits for native creates. See
    /// [`crate::native_sandbox_limits`].
    pub native_sandbox_limits: crate::native_sandbox_limits::NativeSandboxLimits,
    /// Flipped to `true` once the sandbox pod reflector has completed its
    /// initial `LIST`. `/readyz` gates the Service endpoint on this so a
    /// fresh replica does not receive traffic with a cold cache.
    pub readiness: crate::controllers::substrate::ReflectorReady,
    pub drain_signal: pplx_space_common_svc::DrainSignal,
    /// Claimed operations this process executes itself instead of leaving
    /// them to the dispatcher. See [`crate::inline_operations`].
    pub inline_operations: crate::inline_operations::InlineOperations,
    /// Bookkeeping for the e2b-compat template build endpoints.
    /// See `routes::e2b::template_build` for the persisted keys.
    pub template_build_state: std::sync::Arc<crate::routes::e2b::TemplateBuildState>,
    pub federated_identity: crate::federated_identity::FederatedIdentityConfig,
    /// External console identity; `None` keeps password sessions. See
    /// [`crate::external_console`].
    pub external_console: Option<crate::external_console::ExternalConsole>,
    pub data_plane_base_url: String,
    /// Relays a new command's event stream from `POST .../commands` when the
    /// caller accepts `text/event-stream`; `None` keeps the JSON-only answer.
    pub command_stream: Option<crate::command_stream::CommandStreamProxy>,
    pub feature_flags: pplx_space_feature_flags::FeatureFlagClient,
    /// Orgs the environment enables for hidden native operations; see
    /// [`public::hidden_operations`].
    pub native_hidden_operations: public::hidden_operations::HiddenOperationsGate,
    pub ecw_credential_keyring:
        std::sync::Arc<pplx_space_common_svc::ecw_credential::EcwCredentialKeyring>,
}

#[derive(Debug, thiserror::Error)]
pub enum AppStateError {
    #[error(transparent)]
    TlsInterceptionExemptions(
        #[from] crate::tls_interception_exemptions::TlsInterceptionExemptionsConfigError,
    ),
    #[error(transparent)]
    EgressProfiles(#[from] crate::egress_profiles::EgressProfilesConfigError),
    #[error("federated identity configuration failed: {0}")]
    FederatedIdentity(#[from] crate::federated_identity::FederatedIdentityPolicyError),
    #[error("sandbox controller initialization failed: {0}")]
    SandboxController(#[from] crate::controllers::substrate::SubstrateError),
    #[error("route-token authority initialization failed: {0}")]
    RouteTokenAuthority(
        #[from] pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Error,
    ),
    #[error("ECW credential keyring initialization failed: {0}")]
    EcwCredentialKeyring(#[from] pplx_space_common_svc::ecw_credential::EcwCredentialError),
    #[error("client classification configuration failed: {0}")]
    ClientClassification(#[from] crate::client_classification::ClientClassificationError),
    #[error(transparent)]
    NativeSandboxLimits(#[from] crate::native_sandbox_limits::NativeSandboxLimitsConfigError),
    #[error(transparent)]
    CommandStream(#[from] crate::command_stream::CommandStreamConfigError),
    #[error(transparent)]
    NativeHiddenOperations(#[from] public::hidden_operations::HiddenOperationsConfigError),
}

impl AppState {
    #[cfg(test)]
    pub async fn from_pool(pool: PgPool) -> Result<Self, AppStateError> {
        let route_ed25519_authority =
            pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority::load_from_env()?
                .map(std::sync::Arc::new);
        Self::from_pools_with_lifecycle(
            pool.clone(),
            pplx_space_common_svc::sql::CredentialsPool::shared(pool),
            pplx_space_common_svc::DrainSignal::new(),
            CancellationToken::new(),
            TaskTracker::new(),
            route_ed25519_authority,
        )
        .await
    }

    pub async fn from_pools_with_lifecycle(
        pool: PgPool,
        credentials_pool: pplx_space_common_svc::sql::CredentialsPool,
        drain_signal: pplx_space_common_svc::DrainSignal,
        shutdown_token: CancellationToken,
        task_tracker: TaskTracker,
        route_ed25519_authority: Option<
            std::sync::Arc<
                pplx_space_common_svc::space_api_route_ed25519::SpaceApiRouteEd25519Authority,
            >,
        >,
    ) -> Result<Self, AppStateError> {
        let route_signing = route_ed25519_authority.is_some();
        let federated_identity = crate::federated_identity::FederatedIdentityConfig::from_env()?;
        let egress_profiles = crate::egress_profiles::EgressProfiles::from_env()?;
        let tls_interception_exemptions =
            crate::tls_interception_exemptions::TlsInterceptionExemptions::from_env()?;
        let feature_flags = pplx_space_feature_flags::FeatureFlagClient::from_env();
        let native_hidden_operations = public::hidden_operations::HiddenOperationsGate::from_env()?;
        let client_classification = crate::client_classification::ClientClassification::from_env()?;
        let native_sandbox_limits = crate::native_sandbox_limits::NativeSandboxLimits::from_env()?;
        // Cloned into the substrate, which consumes both, so the limiter
        // heartbeats below can still be spawned from the originals.
        let substrate = crate::controllers::substrate::Substrate::from_env_with_lifecycle(
            &pool,
            shutdown_token.clone(),
            task_tracker.clone(),
        )
        .await?;
        let readiness = substrate.readiness();
        let cluster_admission = crate::cluster_admission::ClusterSandboxAdmission::new(
            substrate.clone(),
            feature_flags.clone(),
        );
        task_tracker.spawn(cluster_admission.liveness_heartbeat(shutdown_token.child_token()));
        let sandbox_controller =
            crate::controllers::sandbox::SandboxController::with_cluster_admission(
                substrate.clone(),
                cluster_admission,
            )
            .with_feature_flags(feature_flags.clone())
            .with_route_ed25519_authority(route_ed25519_authority)
            .with_federated_grant_audience(federated_identity.policy.grant_audience.clone())
            .with_egress_profiles(egress_profiles)
            .with_tls_interception_exemptions(tls_interception_exemptions);
        let template_controller =
            crate::controllers::template::TemplateController::new(substrate.clone(), pool.clone());
        let client_concurrency =
            crate::client_concurrency::ClientConcurrencyLimiter::from_env(feature_flags.clone());
        // Both limiters are built here, above the `--surface` branch in
        // `space-api-svc`, so every space-api deployment reports its own
        // limiter liveness — including the dedicated admin host, which mounts
        // none of the ECW routes that gate on the per-client limiter. More than
        // one deployment emits each heartbeat (the cluster controller runs a
        // second copy of the cluster limiter), so a liveness query has to group
        // by `service`: per-cluster alone, a surviving deployment covers for a
        // dead one's silence.
        task_tracker.spawn(client_concurrency.liveness_heartbeat(shutdown_token.child_token()));

        let template_build_state = crate::routes::e2b::TemplateBuildState::new(pool.clone());
        let data_plane_base_url = crate::public_sessions::data_plane_base_url_from_env();
        let command_stream =
            crate::command_stream::CommandStreamProxy::from_env(shutdown_token.child_token())?;
        // Startup check: without a route-signing authority, nothing runs
        // inline.
        let inline_operations = crate::inline_operations::InlineOperations::new(
            crate::inline_operations::InlineOperationsConfig::from_env(),
            task_tracker.clone(),
            drain_signal.clone(),
            crate::public_operation_driver::OperationExecutionConfig::from_env(),
            route_signing,
        );
        task_tracker.spawn(inline_operations.report_slots(shutdown_token.child_token()));
        let ecw_credential_keyring = std::sync::Arc::new(
            pplx_space_common_svc::ecw_credential::EcwCredentialKeyring::load_from_env()?,
        );

        Ok(Self {
            pool,
            credentials_pool,
            sandbox_controller,
            template_controller,
            client_classification,
            client_concurrency,
            native_sandbox_limits,
            readiness,
            drain_signal,
            inline_operations,
            template_build_state,
            federated_identity,
            external_console: None,
            data_plane_base_url,
            command_stream,
            feature_flags,
            native_hidden_operations,
            ecw_credential_keyring,
        })
    }

    pub(crate) fn public_session_store(&self) -> crate::public_sessions::PublicSessionStore {
        crate::public_sessions::PublicSessionStore::with_data_plane_base_url(
            self.pool.clone(),
            self.data_plane_base_url.clone(),
        )
    }
}

// Deliberately pod-local; never probes Postgres, which is shared by every
// replica and would empty the Service during a database outage.
async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    readyz_response(state.readiness.is_ready(), state.drain_signal.is_draining())
}

fn readyz_response(reflector_ready: bool, draining: bool) -> (StatusCode, &'static str) {
    match (draining, reflector_ready) {
        (true, _) => (StatusCode::SERVICE_UNAVAILABLE, "draining"),
        (false, true) => (StatusCode::OK, "ok"),
        (false, false) => (StatusCode::SERVICE_UNAVAILABLE, "pod reflector warming"),
    }
}

fn service_router() -> Router<AppState> {
    Router::new()
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .route("/readyz", axum::routing::get(readyz))
}

fn internal_router() -> Router<AppState> {
    Router::new().route(
        "/internal/v1/sandboxes/{sandbox_id}/ecw-credential-epoch",
        axum::routing::post(ecw_credential_epoch::advance_sandbox_ecw_credential_epoch),
    )
}

pub(crate) const PUBLIC_API_SHARED_ROOT: &str = "/api/v1";
pub(crate) const E2B_API_PATH_ROOT: &str = "/e2b/api";

/// Returns the same M1 OpenAPI document served at `/openapi-m1.json`.
pub fn m1_openapi_document() -> serde_json::Value {
    public_docs::m1_sdk_openapi_value()
}

/// Native public API route tree.
///
/// Public SDK contracts live under `/v1`; E2B compatibility stays on its own
/// adapter routes so it cannot shape the native API.
pub fn public_v1_router(state: AppState) -> Router<AppState> {
    public::v1_router(state)
}

fn public_api_router(state: AppState) -> Router<AppState> {
    Router::new()
        .route(
            "/openapi.json",
            axum::routing::get(public_docs::openapi_json),
        )
        .route(
            "/openapi-m1.json",
            axum::routing::get(public_docs::m1_openapi_json),
        )
        .route(
            "/openapi-full.json",
            public::with_native_auth(
                state.clone(),
                axum::routing::get(public_docs::full_openapi_json),
            ),
        )
        .route("/llms.txt", axum::routing::get(public_docs::llms_txt))
        .route(
            "/api/openapi.json",
            axum::routing::get(public_docs::openapi_json),
        )
        .route(
            "/api/openapi-m1.json",
            axum::routing::get(public_docs::m1_openapi_json),
        )
        .route(
            "/api/openapi-full.json",
            public::with_native_auth(
                state.clone(),
                axum::routing::get(public_docs::full_openapi_json),
            ),
        )
        .route("/api/llms.txt", axum::routing::get(public_docs::llms_txt))
        .nest("/v1", public_v1_router(state.clone()))
        .nest(PUBLIC_API_SHARED_ROOT, public_v1_router(state))
}

fn session_auth_router(state: AppState) -> Router<AppState> {
    session_auth::router(state)
}

fn e2b_dedicated_host_router(state: AppState) -> Router<AppState> {
    e2b::dedicated_host_router(state)
}

fn e2b_path_router(state: AppState) -> Router<AppState> {
    e2b::path_root_router(state)
}

fn admin_shared_host_router(state: AppState) -> Router<AppState> {
    admin::shared_host_router(state)
}

pub fn router(state: AppState) -> Router {
    service_router()
        .merge(internal_router())
        .merge(public_api_router(state.clone()))
        .merge(session_auth_router(state.clone()))
        .merge(e2b_dedicated_host_router(state.clone()))
        .with_state(state)
        .layer(middleware::from_fn(metrics::record_sandbox_route_metrics))
        .layer(http_trace_layer(HttpServerMakeSpan::new(
            ApiHttpSpanMetadata,
        )))
        .layer(CorsLayer::permissive())
}

pub fn shared_host_router(state: AppState) -> Router {
    service_router()
        .merge(internal_router())
        .merge(public_api_router(state.clone()))
        .merge(session_auth_router(state.clone()))
        .merge(e2b_dedicated_host_router(state.clone()))
        .nest(E2B_API_PATH_ROOT, e2b_path_router(state.clone()))
        .merge(admin_shared_host_router(state.clone()))
        .with_state(state)
        .layer(middleware::from_fn(metrics::record_sandbox_route_metrics))
        .layer(http_trace_layer(HttpServerMakeSpan::new(
            ApiHttpSpanMetadata,
        )))
        .layer(CorsLayer::permissive())
}

pub fn dedicated_admin_router(state: AppState) -> Router {
    service_router()
        .merge(session_auth_router(state.clone()))
        .merge(admin::dedicated_host_router(state.clone()))
        .merge(admin::shared_host_router(state.clone()))
        .with_state(state)
        .layer(http_trace_layer(HttpServerMakeSpan::new(
            ApiHttpSpanMetadata,
        )))
        .layer(CorsLayer::permissive())
}

/// space-api customization of the shared SPACE `http.server` trace layer.
///
/// The span construction, response/failure status recording, and traceparent
/// extraction all live in `pplx_space_common_svc::http_trace_layer`.
#[derive(Clone)]
struct ApiHttpSpanMetadata;

impl HttpSpanMetadata for ApiHttpSpanMetadata {
    /// Returns the `(url.path, http.route)` to record on the `http.server` span.
    fn span_path_and_route<'a>(
        &self,
        method: &axum::http::Method,
        _headers: &HeaderMap,
        raw_path: &'a str,
        matched_route: Option<&'a str>,
    ) -> (Cow<'a, str>, Cow<'a, str>) {
        let route = matched_route.unwrap_or(raw_path);
        let route =
            native_action_route(method, route, raw_path).map_or(Cow::Borrowed(route), Cow::Owned);
        (Cow::Borrowed(raw_path), route)
    }

    /// Records the sandbox a request's path names as the canonical
    /// `sandbox_id`, its bare UUID: native routes spell it `sbx_<uuid>`.
    fn record_extra_fields(
        &self,
        _headers: &HeaderMap,
        raw_path: &str,
        _matched_route: Option<&str>,
        span: &tracing::Span,
    ) {
        if let Some(sandbox_id) = sandbox_path_segment(raw_path) {
            pplx_space_common_svc::record_sandbox_id(span, sandbox_id);
        }
    }
}

/// The path segment after `/sandboxes/`, without a native `:action` suffix.
fn sandbox_path_segment(raw_path: &str) -> Option<&str> {
    let (_, rest) = raw_path.split_once("/sandboxes/")?;
    let segment = rest.split('/').next()?;
    Some(
        segment
            .split_once(':')
            .map_or(segment, |(sandbox_id, _)| sandbox_id),
    )
}

/// The declared route of a native `:action` request, such as
/// `/v1/sandboxes/{sandbox_id}:stop`.
///
/// Axum cannot match an `:action` suffix inside a path segment, so every
/// action on a resource is mounted at the resource's own path and dispatched
/// on the suffix. Left alone, stop, start, suspend, wake and pause would share
/// one span name and `http.route`. A request whose method or action the
/// manifest does not declare keeps the mounted route, so the set of names
/// stays bounded and never names a route that does not exist.
fn native_action_route(
    method: &axum::http::Method,
    matched_route: &str,
    raw_path: &str,
) -> Option<String> {
    let (_, action) = raw_path.rsplit_once('/')?.1.rsplit_once(':')?;
    pplx_space_public_api::routes::NATIVE_V1_OPERATIONS
        .iter()
        .find_map(|operation| {
            let prefix = matched_route.strip_suffix(operation.relative_runtime_path())?;
            (prefix.ends_with("/v1")
                && method
                    .as_str()
                    .eq_ignore_ascii_case(operation.method().openapi_name())
                && operation.path() != operation.runtime_path()
                && operation.path().strip_suffix(action)?.ends_with(':'))
            .then(|| format!("{prefix}{}", operation.relative_path()))
        })
}

#[cfg(test)]
mod tests;
