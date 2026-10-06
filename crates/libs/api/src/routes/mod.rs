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
    /// Flipped to `true` once the sandbox pod reflector has completed its
    /// initial `LIST`. `/readyz` gates the Service endpoint on this so a
    /// fresh replica does not receive traffic with a cold cache.
    pub readiness: crate::controllers::substrate::ReflectorReady,
    pub drain_signal: pplx_space_common_svc::DrainSignal,
    /// Bookkeeping for the e2b-compat template build endpoints.
    /// See `routes::e2b::template_build` for the persisted keys.
    pub template_build_state: std::sync::Arc<crate::routes::e2b::TemplateBuildState>,
    pub federated_identity: crate::federated_identity::FederatedIdentityConfig,
    pub data_plane_base_url: String,
    pub feature_flags: pplx_space_feature_flags::FeatureFlagClient,
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
    FalconEgress(#[from] crate::falcon_egress::FalconEgressConfigError),
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
        let federated_identity = crate::federated_identity::FederatedIdentityConfig::from_env()?;
        let falcon_us2_egress = crate::falcon_egress::FalconUs2Egress::from_env()?;
        let tls_interception_exemptions =
            crate::tls_interception_exemptions::TlsInterceptionExemptions::from_env()?;
        let feature_flags = pplx_space_feature_flags::FeatureFlagClient::from_env();
        let client_classification = crate::client_classification::ClientClassification::from_env()?;
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
            .with_route_ed25519_authority(route_ed25519_authority)
            .with_federated_grant_audience(federated_identity.policy.grant_audience.clone())
            .with_falcon_us2_egress(falcon_us2_egress)
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
            readiness,
            drain_signal,
            template_build_state,
            federated_identity,
            data_plane_base_url,
            feature_flags,
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
        .route("/llms.txt", axum::routing::get(public_docs::llms_txt))
        .route(
            "/api/openapi.json",
            axum::routing::get(public_docs::openapi_json),
        )
        .route(
            "/api/openapi-m1.json",
            axum::routing::get(public_docs::m1_openapi_json),
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
        _headers: &HeaderMap,
        raw_path: &'a str,
        matched_route: Option<&'a str>,
    ) -> (Cow<'a, str>, Cow<'a, str>) {
        (
            Cow::Borrowed(raw_path),
            Cow::Borrowed(matched_route.unwrap_or(raw_path)),
        )
    }
}

#[cfg(test)]
mod tests;
