use pplx_space_common_core::types::{NodeId, OrgId, SandboxId, UserId};

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::{
    Extension, Router,
    body::{Body, to_bytes},
    http::{Error as HttpError, HeaderMap, Method, Request, StatusCode, Uri, header},
    response::{IntoResponse, Response},
};
use base64::Engine;
#[cfg(feature = "kubernetes")]
use k8s_openapi::api::core::v1::{Pod, PodIP, PodStatus};
use pplx_space_common_core::sql;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, pg_test_db};
use pplx_space_node_api_sim::{SimWorkerHarness, state::status};
use rstest::{fixture, rstest};
use serde::Deserialize;
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use time::OffsetDateTime;
#[cfg(feature = "kubernetes")]
use tokio::task::JoinHandle;
use tower::util::ServiceExt;
use tracing_subscriber::{Layer, layer::Context, prelude::*};
use uuid::Uuid;

#[cfg(feature = "kubernetes")]
use pplx_space_common_core::kubernetes::SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION;
#[cfg(feature = "kubernetes")]
use pplx_space_common_core::types::SnapshotId;
use pplx_space_common_core::types::{
    AutoLifecycleIdleAction, AutoLifecyclePolicy, AutoLifecycleRuntimeState, DEFAULT_TEMPLATE_TAG,
    SandboxLifecycle, SandboxOwnerSource, TemplateVisibility,
};
use pplx_space_common_svc::ecw_credential::EcwCredentialKeyring;

use super::{
    ConnectTouchRecovery, ECW_DEFAULT_CREATE_TIMEOUT_SECS, ECW_DEFAULT_TEMPLATE_ID,
    ECW_RESPONSE_DEFAULT_TIMEOUT_SECS, ECW_SANDBOX_API_LOG_EVENT, EcwHostConfig,
    LiveKeepaliveTouchOutcome, build_detail_sandbox, build_metadata_filter_jsonb,
    build_sandbox_response, can_retry_unreachable_connect_touch, classify_touch,
    connect_touch_recovery, decode_list_cursor, dedicated_host_router_with_ecw_hosts,
    e2b_api_routes, e2b_auto_lifecycle_policy, e2b_lifecycle_config_from_policy,
    e2b_native_pause_action, e2b_native_resume_action, e2b_policy_for_refresh,
    e2b_policy_for_resume_update, ecw_template_selector_metric_labels, encode_list_cursor,
    file_upload_capability_org, file_upload_signing_secret, mint_file_upload_query,
    parse_metadata_query, parse_state_filter, path_root_router_with_ecw_hosts, request_client_id,
    sandbox_metadata, sandbox_template_id, should_ensure_usable_pod_on_connect,
    should_touch_on_connect, should_wake_on_connect, terminal_sandbox_error,
    translate_create_request, translate_sandbox_state, validate_connect_request,
    validate_list_query, validate_pause_request, validate_refresh_request, validate_resume_request,
};
use crate::{
    controllers::sandbox::AutoLifecycleTouchOutcome,
    e2b::{
        E2bJson, E2bQuery,
        schema::{
            ConnectSandboxRequestE2b, CreateSandboxRequestE2b, SandboxListQueryE2b,
            SandboxPauseRequestE2b, SandboxRefreshRequestE2b, SandboxResumeRequestE2b,
            SandboxStateE2b, StringMap,
        },
    },
    middleware::auth::{AuthEcwActor, AuthOrg},
    models::{
        Organization, Sandbox, SandboxMetadataValue, SandboxNetworkPolicyPatch, SandboxResponse,
    },
    routes::AppState,
};

use pplx_space_metrics::sandbox::api::{TemplateSelectorKind, TemplateTagShape};

const TEST_ECW_API_HOST: &str = "api.e2b.example.test";
const TEST_ECW_HOST: &str = "e2b.example.test";
const TEST_ECW_ALIAS_API_HOST: &str = "api.e2b.staging.example.test";
const TEST_ECW_ALIAS_HOST: &str = "e2b.staging.example.test";
const TEMPLATE_ALIAS_API_KEY: &str = "e2b_0123456789abcdef0123456789abcdef01234567";
const CUSTOM_ALIAS: &str = "custom-runtime__0123456789ab";
const TEST_ECW_CREDENTIAL_KEY: &[u8] = b"space-api-ecw-credential-test-key";
const OTHER_ORG_ID: Uuid = Uuid::from_u128(2);
use pplx_space_test_env::TestResult;

#[cfg(feature = "kubernetes")]
const CONNECT_SUSPEND_RACE_POD_NAME: &str = "space-sbx-connect-suspend-race-g1";

fn test_ecw_credential_keyring() -> EcwCredentialKeyring {
    EcwCredentialKeyring::from_current_key_for_test(TEST_ECW_CREDENTIAL_KEY)
}

fn configure_test_ecw_credential_keyring(state: &mut AppState) {
    state.ecw_credential_keyring = Arc::new(test_ecw_credential_keyring());
}

async fn test_state() -> AppState {
    // Port 1 is closed, so this never reaches a Postgres the host happens to
    // serve. A route that reaches the database should fail at once, not wait
    // out sqlx's 30 s default acquire timeout.
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgresql://127.0.0.1:1/unused")
        .unwrap();

    AppState::from_pool(pool).await.unwrap()
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SdkTemplateAliasResponse {
    #[serde(rename = "templateID")]
    template_id: String,
    public: bool,
}

#[derive(Debug, Clone, Copy)]
enum TemplateAliasOwner {
    Caller,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemplateAliasRelease {
    Latest,
    Untagged,
    Missing,
}

struct TemplateAliasClient {
    app: Router,
}

impl TemplateAliasClient {
    async fn get(&self, alias: &str) -> TestResult<Response> {
        let request = Request::builder()
            .uri(format!("/templates/aliases/{alias}"))
            .header(header::HOST, TEST_ECW_API_HOST)
            .header("x-api-key", TEMPLATE_ALIAS_API_KEY)
            .body(Body::empty())?;
        Ok(self.app.clone().oneshot(request).await?)
    }
}

struct TemplateAliasFixture {
    client: TemplateAliasClient,
    organization: Organization,
    pool: PgPool,
    _db: TestDb,
}

impl TemplateAliasFixture {
    async fn insert_alias(
        &self,
        alias: &str,
        owner: TemplateAliasOwner,
        visibility: TemplateVisibility,
        desired_state: &str,
        release: TemplateAliasRelease,
    ) -> TestResult {
        self.insert_template(alias, owner, visibility, desired_state)
            .await?;
        if release == TemplateAliasRelease::Missing {
            return Ok(());
        }

        let release_id = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash, org_id)
             VALUES ($1, $2, 'fc', $3, $4)"
        ))
        .bind(release_id)
        .bind(alias)
        .bind(format!("sha256:{:064x}", Uuid::now_v7().as_u128()))
        .bind(self.owner_org_id(owner))
        .execute(&self.pool)
        .await?;
        if release == TemplateAliasRelease::Latest {
            sqlx::query(sql!(
                "INSERT INTO template_release_tags (template_release_id, template_name, tag)
                 VALUES ($1, $2, $3)"
            ))
            .bind(release_id)
            .bind(alias)
            .bind(DEFAULT_TEMPLATE_TAG)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    async fn insert_template(
        &self,
        alias: &str,
        owner: TemplateAliasOwner,
        visibility: TemplateVisibility,
        desired_state: &str,
    ) -> TestResult {
        sqlx::query(sql!(
            "INSERT INTO templates (id, name, org_id, visibility, desired_state)
             VALUES ($1, $2, $3, $4, $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(alias)
        .bind(self.owner_org_id(owner))
        .bind(visibility.as_str())
        .bind(desired_state)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn owner_org_id(&self, owner: TemplateAliasOwner) -> OrgId {
        match owner {
            TemplateAliasOwner::Caller => self.organization.id,
            TemplateAliasOwner::Other => OrgId::from_uuid(OTHER_ORG_ID),
        }
    }
}

#[fixture]
async fn template_alias_fixture() -> TestResult<TemplateAliasFixture> {
    let db = pg_test_db().await?;
    let organization = sqlx::query_as::<_, Organization>(sql!(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations
         WHERE id = $1"
    ))
    .bind(SEED_ORG_ID)
    .fetch_one(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (org_id, name, key_hash)
         VALUES ($1, 'template-alias-test', $2)"
    ))
    .bind(organization.id)
    .bind(crate::auth::hash_token(TEMPLATE_ALIAS_API_KEY))
    .execute(&db.pool)
    .await?;

    let state = AppState::from_pool(db.pool.clone()).await?;
    let app =
        path_root_router_with_ecw_hosts(state.clone(), test_ecw_host_config()).with_state(state);

    Ok(TemplateAliasFixture {
        client: TemplateAliasClient { app },
        organization,
        pool: db.pool.clone(),
        _db: db,
    })
}

#[cfg(feature = "kubernetes")]
struct KubernetesAppStateFixture {
    state: AppState,
    substrate: crate::controllers::substrate::Substrate,
    pool: PgPool,
    worker_node_id: NodeId,
    worker: SimWorkerHarness,
    pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
    _db: TestDb,
}

#[cfg(feature = "kubernetes")]
struct MissingRecoveryAppStateFixture {
    state: AppState,
    pool: PgPool,
    _pod_creates: Arc<Mutex<Vec<serde_json::Value>>>,
    _db: TestDb,
}

#[cfg(feature = "kubernetes")]
struct ConnectSuspendRaceFixture {
    app: axum::Router,
    sandbox_id: SandboxId,
    storage_uri: &'static str,
    app_state: KubernetesAppStateFixture,
}

#[cfg(feature = "kubernetes")]
struct MissingPodSuspendFixture {
    app: axum::Router,
    sandbox_id: SandboxId,
    storage_uri: &'static str,
    app_state: KubernetesAppStateFixture,
}

#[cfg(feature = "kubernetes")]
#[derive(Clone, Copy, Debug)]
enum MissingPodSuspendMarker {
    BeforeLifecycle,
    Markerless,
    BetweenLifecycleAndActivity,
}

#[cfg(feature = "kubernetes")]
struct InFlightConnectSuspendRaceFixture {
    race: ConnectSuspendRaceFixture,
    requests: Vec<JoinHandle<Response>>,
}

#[cfg(feature = "kubernetes")]
async fn build_kubernetes_app_state_fixture(
    existing_pod: bool,
) -> TestResult<KubernetesAppStateFixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let worker_node_id = NodeId::new();
    let worker = start_connect_route_sim_worker(worker_node_id).await?;
    let pod_creates = Arc::new(Mutex::new(Vec::new()));
    let initial_pod = existing_pod.then(|| Pod {
        metadata: kube::api::ObjectMeta {
            name: Some(CONNECT_SUSPEND_RACE_POD_NAME.to_string()),
            namespace: Some("space".to_string()),
            uid: Some(format!("{CONNECT_SUSPEND_RACE_POD_NAME}-uid")),
            ..Default::default()
        },
        status: Some(PodStatus {
            phase: Some("Running".to_string()),
            pod_ip: Some("127.0.0.2".to_string()),
            pod_ips: Some(vec![PodIP {
                ip: "127.0.0.2".to_string(),
            }]),
            ..Default::default()
        }),
        ..Default::default()
    });
    let client = if existing_pod {
        crate::controllers::substrate::kubernetes::tests::existing_then_created_running_pod_kubernetes_client(
            Arc::clone(&pod_creates),
            CONNECT_SUSPEND_RACE_POD_NAME,
        )
    } else {
        crate::controllers::substrate::kubernetes::tests::running_pod_kubernetes_client(
            Arc::clone(&pod_creates),
            "127.0.0.1",
        )
    };
    let kubernetes =
        crate::controllers::substrate::kubernetes::tests::substrate_with_store_client_proxy_port_and_pod(
            pool.clone(),
            client,
            worker.port(),
            initial_pod,
        );
    let substrate = crate::controllers::substrate::Substrate::from_kubernetes(kubernetes);
    let mut state = AppState::from_pool(pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    state.sandbox_controller =
        crate::controllers::sandbox::SandboxController::new(substrate.clone());
    state.template_controller =
        crate::controllers::template::TemplateController::new(substrate.clone(), pool.clone());

    Ok(KubernetesAppStateFixture {
        state,
        substrate,
        pool,
        worker_node_id,
        worker,
        pod_creates,
        _db: db,
    })
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn kubernetes_app_state_fixture() -> TestResult<KubernetesAppStateFixture> {
    build_kubernetes_app_state_fixture(true).await
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn missing_pod_kubernetes_app_state_fixture() -> TestResult<KubernetesAppStateFixture> {
    build_kubernetes_app_state_fixture(false).await
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn missing_recovery_app_state_fixture() -> TestResult<MissingRecoveryAppStateFixture> {
    let db = pg_test_db().await?;
    let pool = db.pool.clone();
    let pod_creates = Arc::new(Mutex::new(Vec::new()));
    let kubernetes =
        crate::controllers::substrate::kubernetes::tests::substrate_with_store_and_client(
            pool.clone(),
            crate::controllers::substrate::kubernetes::tests::mock_kubernetes_client(Arc::clone(
                &pod_creates,
            )),
        );
    let substrate = crate::controllers::substrate::Substrate::from_kubernetes(kubernetes);
    let mut state = AppState::from_pool(pool.clone()).await?;
    state.sandbox_controller =
        crate::controllers::sandbox::SandboxController::new(substrate.clone());
    state.template_controller =
        crate::controllers::template::TemplateController::new(substrate, pool.clone());

    Ok(MissingRecoveryAppStateFixture {
        state,
        pool,
        _pod_creates: pod_creates,
        _db: db,
    })
}

#[cfg(feature = "kubernetes")]
struct MissingFailedRecoveryFixture {
    app: axum::Router,
    sandbox_id: SandboxId,
    pool: PgPool,
    _app_state: MissingRecoveryAppStateFixture,
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn missing_failed_recovery_fixture(
    #[future] missing_recovery_app_state_fixture: TestResult<MissingRecoveryAppStateFixture>,
) -> TestResult<MissingFailedRecoveryFixture> {
    let fixture = missing_recovery_app_state_fixture.await?;
    let sandbox_id = SandboxId::new();
    insert_connect_route_sandbox(&fixture.pool, sandbox_id, "started", "on_node", None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET managed_by = 'k8s',
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1",
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
              k8s_node_name, state, active)
         VALUES
             ($1, 1, 'space-sbx-missing-g1', 'space', NULL, NULL, NULL,
              'forgotten', FALSE),
             ($1, 2, 'space-sbx-missing-g2', 'space', 'stale-pod-uid',
              '10.90.0.17', 'stale-node', 'recovery_failed', TRUE)",
    ))
    .bind(sandbox_id)
    .execute(&fixture.pool)
    .await?;
    let org = fetch_local_org(&fixture.pool).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(fixture.state.clone());

    Ok(MissingFailedRecoveryFixture {
        app,
        sandbox_id,
        pool: fixture.pool.clone(),
        _app_state: fixture,
    })
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn missing_pod_suspend_fixture(
    #[default("ready")] snapshot_state: &'static str,
    #[future] missing_pod_kubernetes_app_state_fixture: TestResult<KubernetesAppStateFixture>,
) -> TestResult<MissingPodSuspendFixture> {
    let app_state = missing_pod_kubernetes_app_state_fixture.await?;
    let sandbox_id = SandboxId::new();
    let old_artifact_id = Uuid::now_v7();
    let old_snapshot_id = SnapshotId::new();
    let current_artifact_id = Uuid::now_v7();
    let current_snapshot_id = SnapshotId::new();
    let storage_uri = "s3://space-test/suspend-snapshots/missing-pod?region=us-east-1";
    insert_connect_route_ready_worker(
        &app_state.pool,
        app_state.worker_node_id,
        app_state.worker.endpoint(),
    )
    .await?;
    insert_connect_route_sandbox(
        &app_state.pool,
        sandbox_id,
        "suspending",
        "on_node",
        Some(app_state.worker_node_id),
    )
    .await?;
    sqlx::query(
        "UPDATE sandboxes
            SET managed_by = 'k8s',
                lifecycle_changed_at = NOW() - INTERVAL '1 minute',
                last_command_at = NOW() - INTERVAL '30 seconds',
                last_access_at = NOW() - INTERVAL '40 seconds',
                current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(old_artifact_id)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id, source_node_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES
             ($1, $3, 'full', 'ready', NOW() - INTERVAL '2 minutes',
              '00000000-0000-0000-0000-000000000001', $4, $2, 'full', 'suspend'),
             ($5, $3, 'full', $6, NOW(),
              '00000000-0000-0000-0000-000000000001', $4, $7, 'full', 'suspend')",
    )
    .bind(old_snapshot_id)
    .bind(old_artifact_id)
    .bind(sandbox_id)
    .bind(app_state.worker_node_id)
    .bind(current_snapshot_id)
    .bind(snapshot_state)
    .bind(current_artifact_id)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES
             ($1, 's3://space-test/suspend-snapshots/old?region=us-east-1',
              '00000000-0000-0000-0000-000000000001'),
             ($2, $3, '00000000-0000-0000-0000-000000000001')",
    )
    .bind(old_snapshot_id)
    .bind(current_snapshot_id)
    .bind(storage_uri)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip,
              k8s_node_name, state, active)
         VALUES
             ($1, 1, 'space-sbx-missing-suspend-g1', 'space',
              'space-sbx-missing-suspend-g1-uid', '127.0.0.2', 'stale-node',
              'running', TRUE)",
    )
    .bind(sandbox_id)
    .execute(&app_state.pool)
    .await?;
    let org = fetch_local_org(&app_state.pool).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(app_state.state.clone());

    Ok(MissingPodSuspendFixture {
        app,
        sandbox_id,
        storage_uri,
        app_state,
    })
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn connect_suspend_race_fixture(
    #[future] kubernetes_app_state_fixture: TestResult<KubernetesAppStateFixture>,
) -> TestResult<ConnectSuspendRaceFixture> {
    let app_state = kubernetes_app_state_fixture.await?;
    let sandbox_id = SandboxId::new();
    let artifact_id = Uuid::now_v7();
    let snapshot_id = SnapshotId::new();
    let storage_uri = "s3://space-test/suspend-snapshots/connect-race?region=us-east-1";
    insert_connect_route_ready_worker(
        &app_state.pool,
        app_state.worker_node_id,
        app_state.worker.endpoint(),
    )
    .await?;
    insert_connect_route_sandbox(
        &app_state.pool,
        sandbox_id,
        "started",
        "on_node",
        Some(app_state.worker_node_id),
    )
    .await?;
    sqlx::query(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES (
             $1, $2, 'full', 'creating',
             (SELECT lifecycle_changed_at - INTERVAL '1 second'
                FROM sandboxes WHERE id = $2),
             '00000000-0000-0000-0000-000000000001',
             $3, 'full', 'suspend'
         )",
    )
    .bind(snapshot_id)
    .bind(sandbox_id)
    .bind(artifact_id)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO snapshot_storage_locations
             (snapshot_id, storage_connection_string, org_id)
         VALUES ($1, $2, '00000000-0000-0000-0000-000000000001')",
    )
    .bind(snapshot_id)
    .bind(storage_uri)
    .execute(&app_state.pool)
    .await?;
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, pod_ip, state, active)
         VALUES ($1, 1, $2, 'space', $3, '127.0.0.2', 'running', TRUE)",
    )
    .bind(sandbox_id)
    .bind(CONNECT_SUSPEND_RACE_POD_NAME)
    .bind(format!("{CONNECT_SUSPEND_RACE_POD_NAME}-uid"))
    .execute(&app_state.pool)
    .await?;
    insert_connect_route_observed_state(
        &app_state.pool,
        sandbox_id,
        app_state.worker_node_id,
        "paused",
    )
    .await?;
    let org = fetch_local_org(&app_state.pool).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(app_state.state.clone());

    Ok(ConnectSuspendRaceFixture {
        app,
        sandbox_id,
        storage_uri,
        app_state,
    })
}

#[cfg(feature = "kubernetes")]
fn spawn_connect_request(fixture: &ConnectSuspendRaceFixture) -> TestResult<JoinHandle<Response>> {
    let request = connect_route_request(
        fixture.sandbox_id,
        ConnectRouteRequestShape {
            label: "empty body",
            body: None,
        },
    )?;
    let app = fixture.app.clone();
    Ok(tokio::spawn(async move {
        app.oneshot(request)
            .await
            .expect("ECW test router is infallible")
    }))
}

#[cfg(feature = "kubernetes")]
#[fixture]
async fn in_flight_connect_suspend_race_fixture(
    #[default(1)] connection_count: usize,
    #[future] connect_suspend_race_fixture: TestResult<ConnectSuspendRaceFixture>,
) -> TestResult<InFlightConnectSuspendRaceFixture> {
    let race = connect_suspend_race_fixture.await?;
    let mut requests = Vec::with_capacity(connection_count);
    requests.push(spawn_connect_request(&race)?);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let state: String = sqlx::query_scalar(
                "SELECT state
                   FROM sandbox_k8s_executions
                  WHERE sandbox_id = $1 AND generation = 1",
            )
            .bind(race.sandbox_id)
            .fetch_one(&race.app_state.pool)
            .await?;
            if state == "recovering" {
                return Ok::<_, sqlx::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    for _ in 1..connection_count {
        requests.push(spawn_connect_request(&race)?);
    }
    tokio::task::yield_now().await;

    Ok(InFlightConnectSuspendRaceFixture { race, requests })
}

/// All-fields-unset `CreateSandboxRequestE2b` for tests to mutate. The type
/// deliberately has no `Default` derive (every field must be considered at
/// the wire boundary), so this is the one exhaustive literal call sites build
/// on via field assignment rather than struct-update syntax.
fn base_create_request() -> CreateSandboxRequestE2b {
    CreateSandboxRequestE2b {
        template_id: None,
        timeout: None,
        auto_pause: None,
        auto_resume: None,
        secure: None,
        allow_internet_access: None,
        network: None,
        metadata: StringMap::new(),
        env_vars: StringMap::new(),
        mcp: None,
        volume_mounts: Vec::new(),
        auto_pause_memory: None,
        iam: None,
    }
}

fn native_sandbox_response(id: SandboxId) -> SandboxResponse {
    SandboxResponse {
        id,
        lifecycle: Some("started".to_string()),
        status: Some("started".to_string()),
        template: Some("seed".to_string()),
        ..Default::default()
    }
}

fn test_sandbox() -> Sandbox {
    Sandbox {
        id: SandboxId::new(),
        name: Some("session-123".to_string()),
        user_id: UserId::new(),
        org_id: OrgId::new(),
        owner_source: SandboxOwnerSource::Local,
        upstream_issuer: None,
        upstream_org_id: None,
        upstream_subject_id: None,
        managed_by: "space".to_string(),
        lifecycle: "creating".to_string(),
        placement: "unplaced".to_string(),
        active_node_id: None,
        active_node_name: None,
        active_node_role: None,
        active_node_address: None,
        active_node_managed_by: None,
        active_execution_generation: None,
        active_pod_uid: None,
        active_activation_attempt_id: None,
        ecw_credential_epoch: pplx_space_common_core::types::EcwCredentialEpoch::new(1)
            .expect("credential epoch is valid"),
        target_node_id: None,
        placement_changed_at: None,
        template_id: None,
        content_hash: None,
        release_tags: Vec::new(),
        vcpu: None,
        memory_mib: None,
        disk_mib: None,
        disk_io_max_operations_per_sec: 1_000,
        disk_io_max_bandwidth_mib_per_sec: 100,
        env: sqlx::types::Json(std::collections::BTreeMap::new()),
        metadata: sqlx::types::Json(std::collections::BTreeMap::new()),
        auto_lifecycle: sqlx::types::Json(AutoLifecyclePolicy::disabled()),
        auto_lifecycle_state: sqlx::types::Json(AutoLifecycleRuntimeState::default()),
        network_policy: Some(sqlx::types::Json(
            pplx_space_net_common::NetworkPolicyV1::default(),
        )),
        network_policy_generation: 1,
        template_network_policy: None,
        template_network_policy_generation: None,
        snapshots_enabled_desired: false,
        volume_encryption: false,
        command_ready_pending: false,
        next_auto_event_at: None,
        last_used_at: None,
        last_access_at: None,
        last_command_at: None,
        created_at: time::OffsetDateTime::now_utc(),
        updated_at: time::OffsetDateTime::now_utc(),
    }
}

async fn assert_e2b_error_response(
    response: Response,
    status: StatusCode,
    code: i32,
    message: &str,
    label: &str,
) {
    assert_eq!(response.status(), status, "{label}");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        value,
        json!({
            "code": code,
            "message": message
        }),
        "{label}",
    );
}

async fn insert_failed_terminal_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
) -> TestResult<Organization> {
    let org_id = SEED_ORG_ID;
    let user_id = SEED_USER_ID;

    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement, template_id,
             network_policy_generation
         )
         VALUES ($1, $2, $3, 'space', 'failed', 'unplaced', 'seed', 1)",
    )
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;

    Ok(sqlx::query_as::<_, Organization>(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations
         WHERE id = $1",
    )
    .bind(org_id)
    .fetch_one(pool)
    .await?)
}

#[derive(Clone)]
struct EventFieldCapture {
    events: CapturedEvents,
}

type CapturedEvent = Vec<(String, String)>;
type CapturedEvents = Arc<Mutex<Vec<CapturedEvent>>>;

impl<S> Layer<S> for EventFieldCapture
where
    S: tracing::Subscriber,
{
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(visitor.into_fields());
    }
}

fn assert_event_field(fields: &[(String, String)], name: &str, value: &str) {
    assert!(
        fields
            .iter()
            .any(|(field_name, field_value)| field_name == name && field_value == value),
        "missing event field {name}={value:?}; fields={fields:?}",
    );
}

fn assert_missing_event_field(fields: &[(String, String)], name: &str) {
    assert!(
        fields.iter().all(|(field_name, _)| field_name != name),
        "unexpected event field {name}; fields={fields:?}",
    );
}

#[tokio::test]
async fn e2b_root_requires_api_key() {
    e2b_path_requires_api_key("/sandboxes", Some(TEST_ECW_API_HOST)).await;
}

#[rstest]
#[case::owned_release(
    TemplateAliasOwner::Caller,
    TemplateVisibility::OrgSpecific,
    "present",
    TemplateAliasRelease::Latest,
    Some(false)
)]
#[case::shared_release(
    TemplateAliasOwner::Other,
    TemplateVisibility::Generic,
    "present",
    TemplateAliasRelease::Latest,
    Some(true)
)]
#[case::other_org_private(
    TemplateAliasOwner::Other,
    TemplateVisibility::OrgSpecific,
    "present",
    TemplateAliasRelease::Latest,
    None
)]
#[case::deleting(
    TemplateAliasOwner::Caller,
    TemplateVisibility::OrgSpecific,
    "deleting",
    TemplateAliasRelease::Latest,
    None
)]
#[case::release_without_latest_tag(
    TemplateAliasOwner::Caller,
    TemplateVisibility::OrgSpecific,
    "present",
    TemplateAliasRelease::Untagged,
    None
)]
#[case::release_registration_failed(
    TemplateAliasOwner::Caller,
    TemplateVisibility::OrgSpecific,
    "present",
    TemplateAliasRelease::Missing,
    None
)]
#[tokio::test]
async fn template_alias_lookup_uses_authenticated_sdk_route_contract(
    #[future] template_alias_fixture: TestResult<TemplateAliasFixture>,
    #[case] owner: TemplateAliasOwner,
    #[case] visibility: TemplateVisibility,
    #[case] desired_state: &'static str,
    #[case] release: TemplateAliasRelease,
    #[case] expected_public: Option<bool>,
) -> TestResult {
    let fixture = template_alias_fixture.await?;
    fixture
        .insert_alias(CUSTOM_ALIAS, owner, visibility, desired_state, release)
        .await?;

    let response = fixture.client.get(CUSTOM_ALIAS).await?;
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await?;

    let Some(public) = expected_public else {
        assert_eq!(status, StatusCode::NOT_FOUND);
        return Ok(());
    };
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        serde_json::from_slice::<SdkTemplateAliasResponse>(&body)?,
        SdkTemplateAliasResponse {
            template_id: CUSTOM_ALIAS.to_string(),
            public,
        }
    );
    Ok(())
}

/// A correctly signed copy of the retired `e2bu1_` upload token. It named
/// only the path, so any holder could upload build inputs for any template;
/// the server must treat it like any other anonymous request.
fn retired_legacy_upload_query(path: &str) -> String {
    use hmac::{Hmac, KeyInit, Mac};
    let expires_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 300;
    let secret = file_upload_signing_secret().unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(b"e2b-template-file-upload-v1\0");
    mac.update(path.as_bytes());
    mac.update(b"\0");
    mac.update(&expires_at.to_be_bytes());
    format!(
        "upload_token=e2bu1_{expires_at}_{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

#[rstest]
#[case::owner_capability("owner", StatusCode::BAD_REQUEST)]
#[case::retired_legacy_token("legacy", StatusCode::UNAUTHORIZED)]
#[case::garbage("garbage", StatusCode::UNAUTHORIZED)]
#[tokio::test]
async fn template_file_upload_without_api_key_admits_only_an_owner_capability(
    #[case] query: &str,
    #[case] expected: StatusCode,
) {
    let state = test_state().await;
    let path = "/e2b/api/v1/internal/e2b-files/code-runtime/abc123";
    let query = match query {
        "owner" => mint_file_upload_query(OrgId::from_uuid(SEED_ORG_ID), path).unwrap(),
        "legacy" => retired_legacy_upload_query(path),
        _ => "upload_owner=invalid".to_string(),
    };
    let app = crate::routes::shared_host_router(state);

    let response = app
        .oneshot(
            Request::builder()
                .method(Method::PUT)
                .uri(format!("{path}?{query}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), expected);
    if expected == StatusCode::BAD_REQUEST {
        // Reaching body validation proves the capability passed the auth layer.
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&body).contains("upload body is empty"));
    }
}

#[rstest]
#[tokio::test]
async fn template_file_upload_link_is_absolute_and_capability_authenticated(
    #[future] template_alias_fixture: TestResult<TemplateAliasFixture>,
) -> TestResult {
    let fixture = template_alias_fixture.await?;
    let app = fixture.client.app;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/templates/code-runtime/files/abc123")
                .header("x-api-key", TEMPLATE_ALIAS_API_KEY)
                .header(header::HOST, TEST_ECW_API_HOST)
                .header("x-forwarded-proto", "https")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let upload_url = value["url"].as_str().unwrap();
    assert!(upload_url.starts_with(&format!("https://{TEST_ECW_API_HOST}/")));
    assert_eq!(
        file_upload_capability_org(&upload_url.parse::<Uri>()?),
        Some(fixture.organization.id)
    );
    Ok(())
}

#[rstest]
#[tokio::test]
async fn template_build_create_persists_authenticated_org(
    #[future] template_alias_fixture: TestResult<TemplateAliasFixture>,
) -> TestResult {
    let fixture = template_alias_fixture.await?;

    let response = fixture
        .client
        .app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v3/templates")
                .header("x-api-key", TEMPLATE_ALIAS_API_KEY)
                .header(header::HOST, TEST_ECW_API_HOST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"tenant-inputs","cpuCount":2,"memoryMB":1024}"#,
                ))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
    let (name, org): (String, Option<OrgId>) = sqlx::query_as(pplx_space_common_core::sql!(
        "SELECT template_id, org_id FROM template_build_jobs WHERE build_id = $1"
    ))
    .bind(body["buildID"].as_str().unwrap())
    .fetch_one(&fixture.pool)
    .await?;
    assert_eq!(name, "tenant-inputs");
    assert_eq!(org, Some(fixture.organization.id));
    Ok(())
}

#[tokio::test]
async fn e2b_root_auth_reject_emits_ecw_request_log() {
    let sandbox_id = SandboxId::new();
    let (response, events) = e2b_path_response_with_logs(
        &format!("/sandboxes/{sandbox_id}?metadata=%7B%22team%22%3A%22secret%22%7D&username=alice"),
        Some(TEST_ECW_API_HOST),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let event = only_ecw_request_log(&events);
    assert_event_field(event, "event_type", ECW_SANDBOX_API_LOG_EVENT);
    assert_event_field(event, "http.response.status_code", "401");
    assert_event_field(event, "url.path", &format!("/sandboxes/{sandbox_id}"));
    assert_event_field(event, "sandbox_id", &sandbox_id.to_string());
    assert_missing_event_field(event, "org_id");
    assert_missing_event_field(event, "ecw_surface");
    assert_missing_event_field(event, "http.request.method");
    assert_missing_event_field(event, "http.route");
    assert_missing_event_field(event, "url.query");
    assert_missing_event_field(event, "url.query_present");
}

mod client_source_ip {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use axum::http::{HeaderMap, HeaderValue};

    use super::super::router::client_source_ip;

    const PEER: IpAddr = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 9));

    fn headers_with_xff(value: &'static str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static(value));
        headers
    }

    #[test]
    fn missing_header_falls_back_to_peer() {
        assert_eq!(client_source_ip(&HeaderMap::new(), Some(PEER)), Some(PEER));
    }

    #[test]
    fn missing_header_and_peer_yields_none() {
        assert_eq!(client_source_ip(&HeaderMap::new(), None), None);
    }

    #[test]
    fn single_hop_wins_over_peer() {
        assert_eq!(
            client_source_ip(&headers_with_xff("203.0.113.7"), Some(PEER)),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))),
        );
    }

    #[test]
    fn first_of_multiple_hops_wins() {
        assert_eq!(
            client_source_ip(
                &headers_with_xff("203.0.113.7, 198.51.100.4, 10.0.0.1"),
                Some(PEER),
            ),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))),
        );
    }

    #[test]
    fn hop_with_port_parses_to_ip() {
        assert_eq!(
            client_source_ip(&headers_with_xff("203.0.113.7:4711, 10.0.0.1"), Some(PEER)),
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))),
        );
    }

    #[test]
    fn ipv6_hop_parses() {
        assert_eq!(
            client_source_ip(&headers_with_xff("2001:db8::7"), Some(PEER)),
            Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 7))),
        );
    }

    #[test]
    fn garbage_hop_falls_back_to_peer() {
        assert_eq!(
            client_source_ip(&headers_with_xff("unknown, 203.0.113.7"), Some(PEER)),
            Some(PEER),
        );
    }

    #[test]
    fn garbage_hop_without_peer_yields_none() {
        assert_eq!(client_source_ip(&headers_with_xff("<garbage>"), None), None);
    }

    #[test]
    fn non_utf8_compatible_header_falls_back_to_peer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_bytes(b"\xfe\xff203.0.113.7").unwrap(),
        );

        assert_eq!(client_source_ip(&headers, Some(PEER)), Some(PEER));
    }

    /// `SocketAddr` parsing requires brackets for IPv6-with-port, so a
    /// bracketed hop must still resolve to the bare IPv6 address.
    #[test]
    fn bracketed_ipv6_hop_with_port_parses_to_ip() {
        assert_eq!(
            client_source_ip(&headers_with_xff("[2001:db8::7]:443"), Some(PEER)),
            Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 7))),
        );
    }

    #[test]
    fn empty_header_falls_back_to_peer() {
        assert_eq!(
            client_source_ip(&headers_with_xff(""), Some(PEER)),
            Some(PEER)
        );
    }
}

#[tokio::test]
async fn e2b_request_log_carries_client_address_and_user_agent() {
    // Caller attribution must land even on auth rejections so rejected
    // lifecycle calls retain source-IP and user-agent evidence.
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let state = test_state().await;
    let app =
        path_root_router_with_ecw_hosts(state.clone(), test_ecw_host_config()).with_state(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/sandboxes")
                .header(header::HOST, TEST_ECW_API_HOST)
                .header("x-forwarded-for", "203.0.113.7, 10.0.0.1")
                .header(header::USER_AGENT, "incident-hunter/1.0")
                // Padded value exercises the trim in `request_client_id`.
                .header("x-client-id", "  example-client  ")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    drop(guard);
    let events = events.lock().unwrap().clone();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let event = only_ecw_request_log(&events);
    assert_event_field(event, "client.address", "203.0.113.7");
    assert_event_field(event, "client.id", "example-client");
    assert_event_field(event, "user_agent.original", "incident-hunter/1.0");
    // No authenticated principal on an auth rejection.
    assert_missing_event_field(event, "api_key_id");
    assert_missing_event_field(event, "org_id");
}

#[tokio::test]
async fn e2b_fork_request_log_omits_raw_caller_metadata() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let state = test_state().await;
    let app =
        path_root_router_with_ecw_hosts(state.clone(), test_ecw_host_config()).with_state(state);
    let response = app
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/sandboxes/sbx_019fb7b3a70c7922bd11cd5fe233f199/fork")
                .header(header::HOST, TEST_ECW_API_HOST)
                .header("x-forwarded-for", "203.0.113.7")
                .header(header::USER_AGENT, "fork-client/1.0")
                .header("x-client-id", "untrusted-caller-value")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    drop(guard);
    let events = events.lock().unwrap().clone();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let event = only_ecw_request_log(&events);
    assert_missing_event_field(event, "client.address");
    assert_missing_event_field(event, "client.id");
    assert_missing_event_field(event, "user_agent.original");
}

#[tokio::test]
async fn e2b_request_log_falls_back_to_peer_address_without_forwarded_header() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let state = test_state().await;
    let app =
        path_root_router_with_ecw_hosts(state.clone(), test_ecw_host_config()).with_state(state);
    let peer: std::net::SocketAddr = "192.0.2.33:50123".parse().unwrap();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/sandboxes")
                .header(header::HOST, TEST_ECW_API_HOST)
                .extension(axum::extract::ConnectInfo(peer))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    drop(guard);
    let events = events.lock().unwrap().clone();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let event = only_ecw_request_log(&events);
    assert_event_field(event, "client.address", "192.0.2.33");
    // Raw-SDK callers send no X-Client-ID; the log still carries the field so
    // the 429-sources triplet stays fully populated under one bucket.
    assert_event_field(event, "client.id", "unspecified");
    assert_missing_event_field(event, "user_agent.original");
}

#[tokio::test]
async fn e2b_error_request_log_carries_authenticated_caller_identity() {
    // `require_e2b_api_key` inserts `EcwCallerIdentity` into response
    // extensions after successful key auth; exercising real auth needs a live
    // Postgres, so this drives the log middleware with a stub route that
    // attaches the same extension.
    let api_key_id = Uuid::now_v7();
    let org_id = OrgId::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let app = axum::Router::new()
        .route(
            "/stub",
            axum::routing::get(move || async move {
                (
                    StatusCode::BAD_REQUEST,
                    Extension(crate::middleware::auth::EcwCallerIdentity { api_key_id, org_id }),
                )
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            test_ecw_host_config(),
            super::router::log_ecw_request,
        ));
    let response = app
        .oneshot(Request::builder().uri("/stub").body(Body::empty()).unwrap())
        .await
        .unwrap();
    drop(guard);
    let events = events.lock().unwrap().clone();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let event = only_ecw_request_log(&events);
    assert_event_field(event, "api_key_id", &api_key_id.to_string());
    assert_event_field(event, "org_id", &org_id.to_string());
}

#[tokio::test]
async fn e2b_success_request_log_is_suppressed() {
    let api_key_id = Uuid::now_v7();
    let org_id = OrgId::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let app = axum::Router::new()
        .route(
            "/stub",
            axum::routing::get(move || async move {
                (
                    StatusCode::NO_CONTENT,
                    Extension(crate::middleware::auth::EcwCallerIdentity { api_key_id, org_id }),
                )
            }),
        )
        .layer(axum::middleware::from_fn_with_state(
            test_ecw_host_config(),
            super::router::log_ecw_request,
        ));
    let response = app
        .oneshot(Request::builder().uri("/stub").body(Body::empty()).unwrap())
        .await
        .unwrap();
    drop(guard);
    let events = events.lock().unwrap().clone();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_no_ecw_request_log(&events);
}

#[tokio::test]
async fn ecw_control_plane_request_log_keeps_path() {
    // Control-plane API requests carry only the sandbox-id route segment (not
    // customer PII), so their path stays in the log for observability.
    let sandbox_id = SandboxId::new();
    let (response, events) =
        e2b_path_response_with_logs(&format!("/sandboxes/{sandbox_id}"), Some(TEST_ECW_API_HOST))
            .await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let event = only_ecw_request_log(&events);
    assert_event_field(event, "url.path", &format!("/sandboxes/{sandbox_id}"));
}

#[tokio::test]
async fn e2b_root_requires_api_key_without_host() {
    let response = e2b_path_response("/sandboxes", None).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn e2b_root_allows_shared_api_host() {
    e2b_path_requires_api_key("/sandboxes", Some("api.testing.example.com")).await;
}

#[tokio::test]
async fn e2b_root_allows_ecw_api_host_with_port() {
    e2b_path_requires_api_key("/sandboxes", Some(&format!("{TEST_ECW_API_HOST}:443"))).await;
}

#[tokio::test]
async fn dedicated_host_root_rejects_non_e2b_api_host() {
    let state = test_state().await;
    let app = dedicated_host_router_with_ecw_hosts(state.clone(), test_ecw_host_config())
        .with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/sandboxes")
                .header(header::HOST, "api.testing.example.com")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn dedicated_host_root_allows_e2b_api_host() {
    let state = test_state().await;
    let app = dedicated_host_router_with_ecw_hosts(state.clone(), test_ecw_host_config())
        .with_state(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/sandboxes")
                .header(header::HOST, TEST_ECW_API_HOST)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn unsupported_e2b_endpoint_returns_e2b_error() {
    let state = test_state().await;
    let app = e2b_api_routes().with_state(state);
    for path in ["/sandboxes/metrics", "/snapshots"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED, "{path}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({
                "code": 501,
                "message": "This E2B API endpoint is not supported by SPACE ECW"
            }),
            "{path}",
        );
    }
}

#[tokio::test]
async fn unsupported_e2b_method_returns_e2b_error() {
    let state = test_state().await;
    let response = e2b_api_routes()
        .with_state(state)
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri("/sandboxes/example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
        json!({
            "code": 405,
            "message": "This HTTP method is not supported for the requested E2B API endpoint"
        })
    );
}

#[tokio::test]
async fn sdk_v2_create_and_connect_routes_are_mounted() {
    let state = test_state().await;
    let now = OffsetDateTime::now_utc();
    let app = e2b_api_routes()
        .layer(Extension(AuthEcwActor(None)))
        .layer(Extension(AuthOrg(Organization {
            id: OrgId::new(),
            name: "ECW route test".to_string(),
            slug: "ecw-route-test".to_string(),
            parent_id: None,
            theme_primary: None,
            theme_secondary: None,
            logo_url: None,
            byok_enabled: false,
            snapshot_base_object_storage_uri: None,
            created_at: now,
            updated_at: now,
        })))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);
    for (path, body, status, message) in [
        (
            "/v2/sandboxes",
            r#"{"secure":false}"#,
            StatusCode::UNAUTHORIZED,
            "Invalid or missing API key",
        ),
        (
            "/v2/sandboxes/example/connect",
            r#"{"memory":false}"#,
            StatusCode::BAD_REQUEST,
            "unsupported sandbox connect parameter: memory=false",
        ),
        (
            "/v2/sandboxes/example/connect",
            r#"{"timeout":0}"#,
            StatusCode::BAD_REQUEST,
            "Timeout must be greater than 0",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri(path)
                    .header(header::HOST, TEST_ECW_API_HOST)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), status, "{path}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["message"],
            message,
            "{path}",
        );
    }
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn egress_profile_header_applies_only_to_federated_creates(
    #[future] kubernetes_app_state_fixture: TestResult<KubernetesAppStateFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    use crate::federated_identity::{
        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, FEDERATED_SERVICE_GRANT_HEADER,
    };
    let mut fixture = kubernetes_app_state_fixture.await?;
    sqlx::query(sql!(
        "INSERT INTO templates (id, name, visibility, network_policy_json)
         VALUES ($1, 'seed', 'generic', '{\"egress\":{\"default_action\":\"deny\"}}')"
    ))
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'seed', 'fc', 'sha256:egress-profile-test')"
    ))
    .bind(release_id)
    .execute(&fixture.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_release_id, template_name, tag)
         VALUES ($1, 'seed', 'latest')"
    ))
    .bind(release_id)
    .execute(&fixture.pool)
    .await?;
    fixture.state.sandbox_controller = fixture
        .state
        .sandbox_controller
        .clone()
        .with_egress_profiles(crate::egress_profiles::EgressProfiles::from_config(
            r#"{"sensor":{"allow":[{"domain":"sensor.example.test"}]}}"#,
        )?);
    use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};
    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("example-org")?,
        subject_id: UpstreamSubjectId::parse("example-user")?,
    };
    let materialized = crate::federated_identity::materialize_owner(&fixture.pool, &owner).await?;
    for federated in [false, true] {
        let user_id = if federated {
            materialized.user_id
        } else {
            UserId::from_uuid(SEED_USER_ID)
        };
        let actor = sqlx::query_as::<_, crate::models::User>(sql!(
            "SELECT id, email, password_hash, display_name, org_id, role, owner_source,
                    created_at, updated_at FROM users WHERE id = $1"
        ))
        .bind(user_id)
        .fetch_one(&fixture.pool)
        .await?;
        let mut app = e2b_api_routes()
            .layer(Extension(AuthEcwActor(Some(actor))))
            .layer(Extension(test_ecw_host_config()));
        if federated {
            app = app.layer(Extension(crate::middleware::auth::AuthFederatedOwner(
                owner.clone(),
            )));
        }
        let response = app
            .with_state(fixture.state.clone())
            .oneshot(
                Request::builder()
                    .method(Method::POST)
                    .uri("/sandboxes")
                    .header(header::HOST, TEST_ECW_API_HOST)
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("X-Space-Egress-Profiles", "+sensor")
                    .header(FEDERATED_SERVICE_GRANT_HEADER, "test-profile-grant")
                    .header(
                        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                        (OffsetDateTime::now_utc() + time::Duration::hours(1))
                            .unix_timestamp()
                            .to_string(),
                    )
                    .body(Body::from(
                        r#"{"templateID":"seed","allowInternetAccess":false}"#,
                    ))?,
            )
            .await?;
        let status = response.status();
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        let sandbox_id = body["sandboxID"].as_str().unwrap().parse()?;
        let sandbox = crate::controllers::sandbox::fetch_sandbox(&fixture.pool, sandbox_id)
            .await?
            .expect("created sandbox");
        let policy = sandbox.network_policy.expect("resolved policy").0;
        assert_eq!(policy.allows_domain("sensor.example.test"), federated);
        assert!(!policy.allows_domain("unlisted.example.test"));
        assert_eq!(
            crate::egress_profiles::load(&fixture.pool, sandbox_id)
                .await?
                .is_empty(),
            !federated
        );
    }
    Ok(())
}

#[tokio::test]
async fn unknown_e2b_create_parameter_is_forward_compatible() {
    let response = Router::new()
        .route(
            "/sandboxes",
            axum::routing::post(
                |E2bJson(_request): E2bJson<CreateSandboxRequestE2b>| async {
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/sandboxes")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"templateID":"seed","mystery":true}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn build_status_accepts_the_sdk_log_query_parameters() {
    let response = Router::new()
        .route(
            "/probe",
            axum::routing::get(
                |E2bQuery(_query): E2bQuery<super::template_build::BuildStatusQuery>| async {
                    StatusCode::NO_CONTENT
                },
            ),
        )
        .oneshot(
            Request::builder()
                .uri("/probe?logsOffset=4&limit=100&level=info")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn optional_e2b_body_distinguishes_empty_from_untyped_nonempty() {
    let app = Router::new().route(
        "/probe",
        axum::routing::post(
            |body: Option<E2bJson<crate::e2b::EmptyE2bBody>>| async move {
                if body.is_some() {
                    StatusCode::CREATED
                } else {
                    StatusCode::NO_CONTENT
                }
            },
        ),
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/probe")
                .body(Body::from(r#"{"secret":"do-not-record"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(!String::from_utf8_lossy(&body).contains("do-not-record"));

    for content_type in [None, Some("application/json")] {
        let mut request = Request::builder().method(Method::POST).uri("/probe");
        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
    }
}

#[test]
fn create_translation_applies_resource_overrides() {
    for (raw, cpu, memory) in [
        (r#"{"vcpu":4,"memory_mib":8192}"#, Some(4), Some(8192)),
        (r#"{"vcpu":4}"#, Some(4), None),
        (r#"{"memory_mib":8192}"#, None, Some(8192)),
        ("{}", None, None),
    ] {
        let mut request = base_create_request();
        request
            .metadata
            .insert("space.resources".into(), raw.into());
        request.metadata.insert("purpose".into(), "example".into());
        let translated = translate_create_request(request).unwrap();
        assert_eq!(translated.native.vcpu, cpu);
        assert_eq!(translated.native.memory_mib, memory);
        assert!(!translated.native.metadata.contains_key("space.resources"));
        assert!(translated.native.metadata.contains_key("purpose"));
    }
    let translated = translate_create_request(base_create_request()).unwrap();
    assert_eq!(translated.native.vcpu, None);
    assert_eq!(translated.native.memory_mib, None);
}

#[test]
fn create_translation_rejects_invalid_resource_overrides() {
    for raw in [
        "not json",
        "null",
        r#"{"vcpu":0}"#,
        r#"{"memory_mib":-1}"#,
        r#"{"memory_mib":0}"#,
        r#"{"vcpu":1.5}"#,
        r#"{"vcpu":"4"}"#,
        r#"{"vcpu":2147483648}"#,
        r#"{"memory_mib":2147483648}"#,
        r#"{"memory_mb":8192}"#,
    ] {
        let mut request = base_create_request();
        request
            .metadata
            .insert("space.resources".into(), raw.into());
        assert_eq!(
            translate_create_request(request).unwrap_err().status,
            StatusCode::BAD_REQUEST,
            "{raw}"
        );
    }
}

#[test]
fn create_translation_maps_supported_fields() {
    let mut request = base_create_request();
    request.template_id = Some("tmpl-123".to_string());
    request.timeout = Some(120);
    request.auto_pause = Some(false);
    request.secure = Some(true);
    request.allow_internet_access = Some(true);
    request.metadata = StringMap::from([(String::from("name"), String::from("session-123"))]);
    request.env_vars = StringMap::from([(String::from("FOO"), String::from("bar"))]);
    let translation = translate_create_request(request).unwrap();

    assert_eq!(translation.template_id, "tmpl-123");
    assert_eq!(
        translation
            .native
            .template
            .as_ref()
            .map(|template| template.name().as_str()),
        Some("tmpl-123")
    );
    assert_eq!(translation.native.name, None);
    assert_eq!(
        translation.native.metadata.get("name"),
        Some(&SandboxMetadataValue::String("session-123".to_string()))
    );
    assert_eq!(
        translation.native.env.get("FOO").map(String::as_str),
        Some("bar")
    );
    let lifecycle = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .expect("translated auto lifecycle");
    assert_eq!(lifecycle.timeout_secs, 120);
    assert!(!lifecycle.auto_pause);
    assert!(!lifecycle.auto_resume);
    assert!(translation.native.wait_until_ready);
    assert_eq!(
        translation
            .native
            .network_policy
            .as_ref()
            .map(|policy| policy.default_action()),
        Some(pplx_space_net_common::EgressDefaultAction::Allow)
    );
}

#[test]
fn create_translation_parses_template_reference_digest() {
    let template_id =
        "seed@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let mut request = base_create_request();
    request.template_id = Some(template_id.to_string());
    let translation = translate_create_request(request).unwrap();

    assert_eq!(translation.template_id, template_id);
    assert_eq!(
        translation
            .native
            .template
            .as_ref()
            .map(|template| template.name().as_str()),
        Some("seed")
    );
    assert_eq!(
        translation
            .native
            .template
            .as_ref()
            .map(ToString::to_string),
        Some(template_id.to_string())
    );
}

#[test]
fn create_translation_treats_hash_looking_suffix_as_tag() {
    let template_id = "seed:66c258cefa258e64704c18490fee0c7e2ef4da15";
    let mut request = base_create_request();
    request.template_id = Some(template_id.to_string());
    let translation = translate_create_request(request).unwrap();

    let template = translation
        .native
        .template
        .as_ref()
        .expect("template reference");
    assert_eq!(translation.template_id, template_id);
    assert_eq!(template.name().as_str(), "seed");
    assert_eq!(
        template.tag().map(|tag| tag.as_str()),
        Some("66c258cefa258e64704c18490fee0c7e2ef4da15")
    );
    assert_eq!(template.artifact_digest(), None);
}

#[test]
fn ecw_template_selector_metric_labels_identify_git_sha_tag() {
    let template = "seed:66c258cefa258e64704c18490fee0c7e2ef4da15"
        .parse()
        .unwrap();

    let (selector_kind, tag_shape, requested_tag) = ecw_template_selector_metric_labels(&template);

    assert_eq!(selector_kind, TemplateSelectorKind::Tag);
    assert_eq!(tag_shape, TemplateTagShape::GitSha40);
    assert_eq!(requested_tag, "66c258cefa258e64704c18490fee0c7e2ef4da15");
}

#[test]
fn ecw_template_selector_metric_labels_distinguish_latest_other_and_digest() {
    let latest = "seed".parse().unwrap();
    assert_eq!(
        ecw_template_selector_metric_labels(&latest),
        (
            TemplateSelectorKind::Latest,
            TemplateTagShape::Latest,
            "latest"
        )
    );

    let other = "seed:release-2026.05".parse().unwrap();
    assert_eq!(
        ecw_template_selector_metric_labels(&other),
        (
            TemplateSelectorKind::Tag,
            TemplateTagShape::Other,
            "release-2026.05"
        )
    );

    let digest = "seed@sha256:1111111111111111111111111111111111111111111111111111111111111111"
        .parse()
        .unwrap();
    assert_eq!(
        ecw_template_selector_metric_labels(&digest),
        (
            TemplateSelectorKind::Digest,
            TemplateTagShape::NotTag,
            "<none>"
        )
    );
}

#[test]
fn create_translation_preserves_app_runtime_tag() {
    let template_id = "app-runtime:62f56c72e6af1173d3e6445c778be4a392d9e442";
    let mut request = base_create_request();
    request.template_id = Some(template_id.to_string());
    let translation = translate_create_request(request).unwrap();

    assert_eq!(translation.template_id, template_id);
    let template = translation
        .native
        .template
        .as_ref()
        .expect("template reference");
    assert_eq!(template.name().as_str(), "app-runtime");
    assert_eq!(
        template.tag().map(|tag| tag.as_str()),
        Some("62f56c72e6af1173d3e6445c778be4a392d9e442")
    );
    assert!(!template.uses_default_selector());
}

#[test]
fn create_translation_preserves_app_runtime_digest() {
    let template_id =
        "app-runtime@sha256:1111111111111111111111111111111111111111111111111111111111111111";
    let mut request = base_create_request();
    request.template_id = Some(template_id.to_string());
    let translation = translate_create_request(request).unwrap();

    assert_eq!(translation.template_id, template_id);
    let template = translation
        .native
        .template
        .as_ref()
        .expect("template reference");
    assert_eq!(template.name().as_str(), "app-runtime");
    assert_eq!(
        template.artifact_digest().map(|digest| digest.to_string()),
        Some("sha256:1111111111111111111111111111111111111111111111111111111111111111".to_string())
    );
    assert!(!template.uses_default_selector());
}

#[test]
fn create_translation_keeps_app_runtime_latest_unchanged() {
    let mut request = base_create_request();
    request.template_id = Some("app-runtime".to_string());
    let translation = translate_create_request(request).unwrap();

    let template = translation
        .native
        .template
        .as_ref()
        .expect("template reference");
    assert_eq!(template.name().as_str(), "app-runtime");
    assert!(template.uses_default_selector());
}

#[test]
fn create_translation_uses_native_metadata_scalar_types() {
    let mut request = base_create_request();
    request.metadata = StringMap::from([
        (String::from("debug"), String::from("true")),
        (String::from("priority"), String::from("3")),
        (String::from("session_id"), String::from("00123")),
    ]);
    let translation = translate_create_request(request).unwrap();

    assert_eq!(
        translation.native.metadata.get("debug"),
        Some(&SandboxMetadataValue::Bool(true))
    );
    assert_eq!(
        translation.native.metadata.get("priority"),
        Some(&SandboxMetadataValue::Number(3))
    );
    assert_eq!(
        translation.native.metadata.get("session_id"),
        Some(&SandboxMetadataValue::String("00123".to_string()))
    );
}

#[test]
fn create_translation_defaults_template() {
    let translation = translate_create_request(base_create_request()).unwrap();

    assert_eq!(translation.template_id, ECW_DEFAULT_TEMPLATE_ID);
    assert_eq!(
        translation
            .native
            .template
            .as_ref()
            .map(|template| template.name().as_str()),
        Some(ECW_DEFAULT_TEMPLATE_ID)
    );
}

#[test]
fn create_translation_uses_e2b_create_timeout_default() {
    let translation = translate_create_request(base_create_request()).unwrap();

    let lifecycle = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .expect("translated auto lifecycle");
    assert_eq!(lifecycle.timeout_secs, ECW_DEFAULT_CREATE_TIMEOUT_SECS);
}

#[test]
fn create_translation_ignores_legacy_secure_field() {
    for secure in [false, true] {
        let mut request = base_create_request();
        request.secure = Some(secure);
        let translation = translate_create_request(request)
            .expect("E2B's newer contract ignores the legacy secure field");
        assert!(translation.native.network_policy.is_none());
    }
}

#[test]
fn create_translation_rejects_public_network_traffic_config() {
    let mut request = base_create_request();
    request.network = Some(serde_json::json!({"allowPublicTraffic": true}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "network.allowPublicTraffic=true is not supported; ECW sandbox service traffic is token-authenticated"
    );
}

#[test]
fn create_translation_maps_network_policy_permutations() {
    use pplx_space_net_common::EgressDefaultAction;
    use std::collections::BTreeSet;

    struct ExpectedNetworkPolicy {
        default_action: EgressDefaultAction,
        allow: &'static [&'static str],
        deny: &'static [&'static str],
    }

    struct Case {
        name: &'static str,
        allow_internet_access: Option<bool>,
        network: Option<serde_json::Value>,
        expected: Option<ExpectedNetworkPolicy>,
    }

    fn string_set(values: &[&str]) -> BTreeSet<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn destination_set(values: &[pplx_space_net_common::EgressDestinationV1]) -> BTreeSet<String> {
        values.iter().map(ToString::to_string).collect()
    }

    let cases = [
        Case {
            name: "no network config leaves template policy alone",
            allow_internet_access: None,
            network: None,
            expected: None,
        },
        Case {
            name: "allow_internet_access=false creates default deny",
            allow_internet_access: Some(false),
            network: None,
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &[],
                deny: &[],
            }),
        },
        Case {
            name: "allowPublicTraffic=false does not block allow_internet_access=true",
            allow_internet_access: Some(true),
            network: Some(json!({"allowPublicTraffic": false})),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Allow,
                allow: &[],
                deny: &[],
            }),
        },
        Case {
            name: "allowOut IP and CIDR entries create an allowlist",
            allow_internet_access: None,
            network: Some(json!({"allowOut": ["1.2.3.4/32", "10.0.0.0/8", "203.0.113.5"]})),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &["1.2.3.4/32", "10.0.0.0/8", "203.0.113.5"],
                deny: &[],
            }),
        },
        Case {
            name: "allowOut domain entries include DNS resolver allows",
            allow_internet_access: None,
            network: Some(json!({"allowOut": ["example.com", "*.example.org"]})),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &["example.com", "*.example.org", "8.8.8.8/32", "1.1.1.1/32"],
                deny: &[],
            }),
        },
        Case {
            name: "denyOut CIDR creates a blocklist",
            allow_internet_access: None,
            network: Some(json!({"denyOut": ["198.51.100.0/24"]})),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Allow,
                allow: &[],
                deny: &["198.51.100.0/24"],
            }),
        },
        Case {
            name: "allow_internet_access=true preserves real denyOut CIDRs",
            allow_internet_access: Some(true),
            network: Some(json!({"denyOut": ["198.51.100.0/24"]})),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Allow,
                allow: &[],
                deny: &["198.51.100.0/24"],
            }),
        },
        Case {
            name: "allow_internet_access=false strips E2B deny-all markers",
            allow_internet_access: Some(false),
            network: Some(json!({
                "allowOut": ["egress-proxy.example.test"],
                "denyOut": ["0.0.0.0/0", "::/0"]
            })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &["egress-proxy.example.test", "8.8.8.8/32", "1.1.1.1/32"],
                deny: &[],
            }),
        },
        Case {
            name: "implicit E2B deny-all markers create a default-deny allowlist",
            allow_internet_access: None,
            network: Some(json!({
                "allowOut": [
                    "egress-proxy.example.test",
                    "example-user-uploads.s3.amazonaws.com"
                ],
                "denyOut": ["0.0.0.0/0", "::/0"]
            })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &[
                    "egress-proxy.example.test",
                    "example-user-uploads.s3.amazonaws.com",
                    "8.8.8.8/32",
                    "1.1.1.1/32",
                ],
                deny: &[],
            }),
        },
        Case {
            name: "Explicit allow_internet_access=true still treats deny-all as an E2B marker",
            allow_internet_access: Some(true),
            network: Some(json!({
                "allowOut": [
                    "ism.bible",
                    "example-interpreter-files.s3.amazonaws.com",
                    "egress-proxy.example.test",
                    "git-proxy.example.test",
                    "example-user-uploads.s3.amazonaws.com",
                    "example-runtime-assets.s3.us-east-1.amazonaws.com"
                ],
                "denyOut": ["0.0.0.0/0"]
            })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &[
                    "ism.bible",
                    "example-interpreter-files.s3.amazonaws.com",
                    "egress-proxy.example.test",
                    "git-proxy.example.test",
                    "example-user-uploads.s3.amazonaws.com",
                    "example-runtime-assets.s3.us-east-1.amazonaws.com",
                    "8.8.8.8/32",
                    "1.1.1.1/32",
                ],
                deny: &[],
            }),
        },
        Case {
            name: "allow_internet_access=false preserves real denyOut CIDRs",
            allow_internet_access: Some(false),
            network: Some(json!({
                "allowOut": ["egress-proxy.example.test"],
                "denyOut": ["198.51.100.0/24"]
            })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &["egress-proxy.example.test", "8.8.8.8/32", "1.1.1.1/32"],
                deny: &["198.51.100.0/24"],
            }),
        },
        // Pins the two V1 corners the shared `ecw_egress_default` must preserve
        // (both were previously unpinned and silently changed once):
        // (1) a marker-only denyOut with no allowOut is a block-list →
        //     default-Allow, and the marker stays in `deny` (strip only runs
        //     under default-Deny). It must NOT flip to default-Deny.
        Case {
            name: "marker-only denyOut without allowOut stays a block-list (default-allow)",
            allow_internet_access: None,
            network: Some(json!({ "denyOut": ["0.0.0.0/0"] })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Allow,
                allow: &[],
                deny: &["0.0.0.0/0"],
            }),
        },
        // (2) a marker mixed with a real deny under an explicit internet-off is
        //     accepted (NOT rejected): default-Deny, markers stripped, the real
        //     deny kept. V1 tolerates the mixed form; only V2 rejects it.
        Case {
            name: "marker mixed with real deny under allow_internet_access=false is accepted",
            allow_internet_access: Some(false),
            network: Some(json!({
                "allowOut": ["egress-proxy.example.test"],
                "denyOut": ["0.0.0.0/0", "198.51.100.0/24"]
            })),
            expected: Some(ExpectedNetworkPolicy {
                default_action: EgressDefaultAction::Deny,
                allow: &["egress-proxy.example.test", "8.8.8.8/32", "1.1.1.1/32"],
                deny: &["198.51.100.0/24"],
            }),
        },
    ];

    for case in cases {
        let mut request = base_create_request();
        request.allow_internet_access = case.allow_internet_access;
        request.network = case.network;
        let translation = translate_create_request(request)
            .unwrap_or_else(|error| panic!("{}: {error:?}", case.name));

        match (
            translation.native.network_policy.as_ref(),
            case.expected.as_ref(),
        ) {
            (None, None) => {}
            (Some(policy), Some(expected)) => {
                assert_eq!(
                    policy.default_action(),
                    expected.default_action,
                    "{}",
                    case.name
                );
                assert_eq!(
                    destination_set(&policy.egress.allow),
                    string_set(expected.allow),
                    "{} allow",
                    case.name
                );
                assert_eq!(
                    destination_set(&policy.egress.deny),
                    string_set(expected.deny),
                    "{} deny",
                    case.name
                );
            }
            (None, Some(_)) => panic!("{}: missing network policy", case.name),
            (Some(policy), None) => panic!("{}: unexpected network policy {policy:?}", case.name),
        }
    }
}

#[test]
fn create_translation_maps_tls_interception_metadata() {
    use pplx_space_net_common::TlsInterception;

    struct Case {
        name: &'static str,
        marker: Option<&'static str>,
        resolved_policy: bool,
        expected: Result<Option<TlsInterception>, StatusCode>,
    }

    let cases = [
        Case {
            name: "missing marker defaults a resolved policy off",
            marker: None,
            resolved_policy: true,
            expected: Ok(Some(TlsInterception::Off)),
        },
        Case {
            name: "explicit off keeps a resolved policy off",
            marker: Some("off"),
            resolved_policy: true,
            expected: Ok(Some(TlsInterception::Off)),
        },
        Case {
            name: "on enables a resolved policy",
            marker: Some("on"),
            resolved_policy: true,
            expected: Ok(Some(TlsInterception::On)),
        },
        Case {
            name: "invalid value is a client error",
            marker: Some("enforce"),
            resolved_policy: true,
            expected: Err(StatusCode::BAD_REQUEST),
        },
        Case {
            name: "on without a resolved policy is a client error",
            marker: Some("on"),
            resolved_policy: false,
            expected: Err(StatusCode::BAD_REQUEST),
        },
        Case {
            name: "missing marker preserves no policy",
            marker: None,
            resolved_policy: false,
            expected: Ok(None),
        },
        Case {
            name: "explicit off preserves no policy",
            marker: Some("off"),
            resolved_policy: false,
            expected: Ok(None),
        },
    ];

    for case in cases {
        let mut request = base_create_request();
        if case.resolved_policy {
            request.allow_internet_access = Some(true);
        }
        if let Some(marker) = case.marker {
            request
                .metadata
                .insert("network.tls_interception".to_string(), marker.to_string());
        }

        match case.expected {
            Ok(expected) => {
                let translation = translate_create_request(request)
                    .unwrap_or_else(|error| panic!("{}: {error:?}", case.name));
                assert_eq!(
                    translation
                        .native
                        .network_policy
                        .as_ref()
                        .map(|policy| policy.tls_interception),
                    expected,
                    "{}",
                    case.name
                );
                if let Some(marker) = case.marker {
                    assert_eq!(
                        translation.native.metadata.get("network.tls_interception"),
                        Some(&SandboxMetadataValue::String(marker.to_string())),
                        "{} metadata",
                        case.name
                    );
                }
            }
            Err(status) => {
                let error = translate_create_request(request).unwrap_err();
                assert_eq!(error.status, status, "{}", case.name);
            }
        }
    }
}

#[test]
fn e2b_full_network_policy_translation() {
    use pplx_space_net_common::NetworkPolicyV1;
    let raw = serde_json::json!({
        "version": "v1",
        "egress": {"default_action": "deny", "allow": [{"domain": "sensor.example.com"}],
                   "deny": [{"ip": "192.0.2.1"}]},
        "tls_interception": "on",
        "tls_interception_exemptions": [{"domain": "sensor.example.com"}],
        "network_credential_rules": [{
            "rule_id": "api", "destination": {"exact_host": "api.example.com", "path_prefix": "/"},
            "credential_header_bindings": [{"header_name": "Authorization",
                "credential_reference_id": "018f1e9d-7b7a-7d3a-8000-000000000001"}],
            "supported_http_methods": ["GET"]
        }]
    })
    .to_string();
    let expected: NetworkPolicyV1 = serde_json::from_str(&raw).unwrap();
    let mut request = base_create_request();
    request
        .metadata
        .insert("network.policy".into(), raw.clone());
    request.metadata.insert("name".into(), "sample".into());
    let translated = translate_create_request(request.clone()).unwrap();
    assert_eq!(translated.native.network_policy, Some(expected));
    assert!(!translated.native.metadata.contains_key("network.policy"));
    assert_eq!(
        translated.native.metadata.get("name"),
        Some(&SandboxMetadataValue::String("sample".into()))
    );

    for conflict in [
        "network",
        "allow_internet_access",
        "network.tls_interception",
    ] {
        let mut conflicting = request.clone();
        match conflict {
            "network" => conflicting.network = Some(serde_json::json!({})),
            "allow_internet_access" => conflicting.allow_internet_access = Some(false),
            _ => {
                conflicting.metadata.insert(conflict.into(), "off".into());
            }
        }
        assert_eq!(
            translate_create_request(conflicting).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }
    for malformed in [
        "null",
        "[]",
        "{",
        r#"{"egress":{"default_action":"invalid"}}"#,
    ] {
        let mut invalid = request.clone();
        invalid
            .metadata
            .insert("network.policy".into(), malformed.into());
        assert_eq!(
            translate_create_request(invalid).unwrap_err().status,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn e2b_tls_interception_on_persists_and_reads_through_policy_api() -> TestResult {
    use pplx_space_net_common::{NetworkPolicyV1, TlsInterception};

    let mut request = base_create_request();
    request.allow_internet_access = Some(true);
    request
        .metadata
        .insert("network.tls_interception".to_string(), "on".to_string());
    let translated = translate_create_request(request)?;
    let policy = translated
        .native
        .network_policy
        .expect("on must resolve to a concrete policy");
    assert_eq!(policy.tls_interception, TlsInterception::On);

    let db = pg_test_db().await?;
    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEED_USER_ID)
    .bind(SEED_ORG_ID)
    .execute(&db.pool)
    .await?;

    let state = AppState::from_pool(db.pool.clone()).await?;
    let sandbox = crate::controllers::sandbox::fetch_sandbox(&db.pool, sandbox_id)
        .await?
        .expect("inserted sandbox");
    state
        .sandbox_controller
        .set_network_policy(
            &db.pool,
            &sandbox,
            SandboxNetworkPolicyPatch::Set { policy },
            Some(1),
        )
        .await?;

    let response = crate::routes::public::network_policy::get_sandbox_network_policy(
        axum::extract::State(state),
        Extension(AuthOrg(fetch_local_org(&db.pool).await?)),
        axum::extract::Path(pplx_space_public_api::PublicSandboxId::from(sandbox_id).to_string()),
    )
    .await
    .expect("policy read");
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    let persisted: NetworkPolicyV1 = serde_json::from_slice(&body)?;
    assert_eq!(persisted.tls_interception, TlsInterception::On);
    Ok(())
}

#[test]
fn create_translation_rejects_deny_out_domain_entries() {
    let mut request = base_create_request();
    request.network = Some(serde_json::json!({"denyOut": ["example.com"]}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn create_translation_requires_default_action_when_both_lists_set() {
    let mut request = base_create_request();
    request.network =
        Some(serde_json::json!({"allowOut": ["1.2.3.4/32"], "denyOut": ["5.6.7.8/32"]}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn create_translation_requires_default_action_when_deny_all_is_mixed_with_real_denies() {
    let mut request = base_create_request();
    request.network = Some(serde_json::json!({
        "allowOut": ["egress-proxy.example.test"],
        "denyOut": ["0.0.0.0/0", "198.51.100.0/24"]
    }));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn create_translation_rejects_invalid_egress_entry() {
    let mut request = base_create_request();
    request.network = Some(serde_json::json!({"allowOut": ["not a host"]}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
}

#[test]
fn create_translation_rejects_unknown_network_field() {
    let mut request = base_create_request();
    request.network = Some(serde_json::json!({"allowIn": ["1.2.3.4/32"]}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "unsupported network fields: allowIn; supported fields are allowPublicTraffic, allowOut, denyOut"
    );
}

#[test]
fn create_translation_rejects_unsupported_fields() {
    let mut request = base_create_request();
    request.secure = Some(true);
    request.allow_internet_access = Some(false);
    request.mcp = Some(serde_json::json!({"servers": []}));
    request.volume_mounts = vec![crate::e2b::schema::SandboxVolumeMountE2b {
        name: "vol".to_string(),
        path: "/workspace".to_string(),
    }];
    request.auto_pause_memory = Some(false);
    request.iam = Some(serde_json::json!({"role": "example"}));
    let error = translate_create_request(request).unwrap_err();

    assert_eq!(error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        error.message,
        "unsupported sandbox create parameters: mcp, volumeMounts, autoPauseMemory=false, iam"
    );

    let mut compatible = base_create_request();
    compatible.auto_pause_memory = Some(true);
    translate_create_request(compatible).expect("memory snapshots are the supported default");
}

/// An ECW list page stops at the list byte budget, returning at least one
/// sandbox, and still sets `x-next-token`, which following visits every
/// sandbox once. The rows are inserted directly, as ECW create stores
/// metadata without size limits.
#[tokio::test]
async fn sandbox_list_pages_stop_at_the_byte_budget() -> TestResult {
    const MIB: usize = 1024 * 1024;
    let db = pg_test_db().await?;
    let org = sqlx::query_as::<_, Organization>(sql!(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations
         WHERE id = $1"
    ))
    .bind(SEED_ORG_ID)
    .fetch_one(&db.pool)
    .await?;
    // Newest first: one row over the whole budget, four 3 MiB rows, then two
    // small ones. They are suspended, which ECW lists as paused.
    let newest = OffsetDateTime::from_unix_timestamp(1_800_000_000)?;
    let sizes = std::iter::once(9 * MIB)
        .chain(std::iter::repeat_n(3 * MIB, 4))
        .chain(std::iter::repeat_n(16, 2));
    let mut expected = Vec::new();
    for (offset, size) in sizes.enumerate() {
        let sandbox_id = SandboxId::new();
        sqlx::query(sql!(
            "INSERT INTO sandboxes (
                 id, user_id, org_id, lifecycle, created_at, metadata,
                 network_policy_override_json, network_policy_generation
             ) VALUES ($1, $2, $3, 'suspended', $4, $5, $6, 1)"
        ))
        .bind(sandbox_id)
        .bind(SEED_USER_ID)
        .bind(SEED_ORG_ID)
        .bind(newest - time::Duration::seconds(offset as i64))
        .bind(sqlx::types::Json(json!({ "legacy": "m".repeat(size) })))
        .bind(sqlx::types::Json(
            &pplx_space_net_common::NetworkPolicyV1::default(),
        ))
        .execute(&db.pool)
        .await?;
        expected.push(sandbox_id.to_string());
    }
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(AppState::from_pool(db.pool.clone()).await?);

    let mut pages = Vec::new();
    let mut next_token: Option<String> = None;
    loop {
        let uri = match &next_token {
            Some(token) => format!("/sandboxes?limit=1000&nextToken={token}"),
            None => "/sandboxes?limit=1000".to_string(),
        };
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header(header::HOST, TEST_ECW_API_HOST)
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        next_token = response
            .headers()
            .get(super::lifecycle::NEXT_TOKEN_HEADER)
            .map(|value| value.to_str().map(str::to_owned))
            .transpose()?;
        let body: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await?)?;
        let ids = body
            .as_array()
            .ok_or("list body is an array")?
            .iter()
            .map(|item| item["sandboxID"].as_str().map(str::to_owned))
            .collect::<Option<Vec<_>>>()
            .ok_or("listed sandboxes carry an ID")?;
        pages.push(ids);
        assert!(pages.len() <= 8, "the cursor stopped advancing");
        if next_token.is_none() {
            break;
        }
    }

    // The oversized row comes back alone; 3 MiB rows fit two to a page, and
    // the small rows fit beside the last two.
    assert_eq!(
        pages.iter().map(Vec::len).collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
    assert_eq!(pages.concat(), expected);
    Ok(())
}

#[test]
fn create_translation_rejects_nul_in_metadata_and_env() {
    let entry = |key: &str, value: &str| StringMap::from([(key.to_string(), value.to_string())]);
    for (metadata, env_vars, message) in [
        (
            entry("a\0b", "v"),
            StringMap::new(),
            "metadata must not contain NUL",
        ),
        (
            entry("k", "a\0b"),
            StringMap::new(),
            "metadata must not contain NUL",
        ),
        (
            StringMap::new(),
            entry("A\0B", "v"),
            "envVars must not contain NUL",
        ),
        (
            StringMap::new(),
            entry("K", "a\0b"),
            "envVars must not contain NUL",
        ),
    ] {
        let mut request = base_create_request();
        request.metadata = metadata;
        request.env_vars = env_vars;
        let error = translate_create_request(request).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.message, message);
    }
}

#[test]
fn lifecycle_requests_reject_only_unsupported_parameter_values() {
    let pause_error = validate_pause_request(Some(SandboxPauseRequestE2b {
        memory: Some(false),
    }))
    .unwrap_err();
    assert_eq!(pause_error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        pause_error.message,
        "unsupported sandbox pause parameter: memory=false"
    );

    validate_connect_request(Some(ConnectSandboxRequestE2b {
        timeout: Some(300),
        memory: Some(true),
    }))
    .expect("memory restore is supported");
    let connect_error = validate_connect_request(Some(ConnectSandboxRequestE2b {
        timeout: Some(300),
        memory: Some(false),
    }))
    .unwrap_err();
    assert_eq!(
        connect_error.message,
        "unsupported sandbox connect parameter: memory=false"
    );

    let resume_error = validate_resume_request(Some(SandboxResumeRequestE2b {
        timeout: Some(300),
        auto_pause: Some(true),
        memory: Some(false),
    }))
    .unwrap_err();
    assert_eq!(
        resume_error.message,
        "unsupported sandbox resume parameter: memory=false"
    );

    let refresh_error =
        validate_refresh_request(Some(SandboxRefreshRequestE2b { duration: Some(60) }))
            .unwrap_err();
    assert_eq!(refresh_error.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        refresh_error.message,
        "unsupported sandbox refresh parameter: duration"
    );
}

#[test]
fn list_reports_spec_fields_that_space_cannot_honor() {
    let error = validate_list_query(&SandboxListQueryE2b {
        limit: None,
        next_token: None,
        state: None,
        metadata: None,
        order: Some("asc".to_string()),
        started_after: Some("2026-01-01T00:00:00Z".to_string()),
        template: Some("seed".to_string()),
    })
    .unwrap_err();
    assert_eq!(
        error.message,
        "unsupported sandbox list parameters: order, startedAfter, template"
    );
}

#[test]
fn create_translation_maps_pause_and_resume_policy() {
    let mut request = base_create_request();
    request.timeout = Some(45);
    request.auto_pause = Some(true);
    request.auto_resume = Some(serde_json::json!({"enabled": true}));
    let translation = translate_create_request(request).unwrap();

    let lifecycle = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .expect("translated auto lifecycle");
    let stage = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(|policy| policy.stages.first())
        .expect("access idle stage");
    assert_eq!(lifecycle.timeout_secs, 45);
    assert!(lifecycle.auto_pause);
    assert!(lifecycle.auto_resume);
    assert!(translation.native.wait_until_ready);
    assert_eq!(
        stage.action,
        pplx_space_common_core::types::AutoLifecycleIdleAction::Suspend
    );
}

#[test]
fn create_translation_treats_empty_auto_resume_object_as_disabled() {
    let mut request = base_create_request();
    request.timeout = Some(45);
    request.auto_pause = Some(true);
    request.auto_resume = Some(serde_json::json!({}));
    let translation = translate_create_request(request).unwrap();

    let lifecycle = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .expect("translated auto lifecycle");
    assert_eq!(lifecycle.timeout_secs, 45);
    assert!(lifecycle.auto_pause);
    assert!(!lifecycle.auto_resume);
    assert!(translation.native.wait_until_ready);
}

#[test]
fn create_translation_treats_null_auto_resume_as_disabled() {
    let mut request = base_create_request();
    request.timeout = Some(45);
    request.auto_pause = Some(true);
    request.auto_resume = Some(serde_json::Value::Null);
    let translation = translate_create_request(request).unwrap();

    let lifecycle = translation
        .native
        .auto_lifecycle
        .as_ref()
        .and_then(e2b_lifecycle_config_from_policy)
        .expect("translated auto lifecycle");
    assert_eq!(lifecycle.timeout_secs, 45);
    assert!(lifecycle.auto_pause);
    assert!(!lifecycle.auto_resume);
    assert!(translation.native.wait_until_ready);
}

#[test]
fn e2b_policy_can_disable_auto_resume_while_still_auto_pausing() {
    let policy = e2b_auto_lifecycle_policy(ECW_RESPONSE_DEFAULT_TIMEOUT_SECS, true, false);
    let lifecycle = e2b_lifecycle_config_from_policy(&policy).expect("e2b lifecycle");

    assert_eq!(lifecycle.timeout_secs, ECW_RESPONSE_DEFAULT_TIMEOUT_SECS);
    assert!(lifecycle.auto_pause);
    assert!(!lifecycle.auto_resume);
    assert!(policy.wake.on.is_empty());
}

#[test]
fn e2b_refresh_preserves_non_e2b_policy() {
    assert!(e2b_policy_for_refresh(&AutoLifecyclePolicy::disabled()).is_none());
}

#[test]
fn e2b_refresh_treats_stop_action_as_non_e2b_policy() {
    let mut policy = e2b_auto_lifecycle_policy(45, false, false);
    policy.stages[0].action = AutoLifecycleIdleAction::Stop;

    assert!(e2b_lifecycle_config_from_policy(&policy).is_none());
    assert!(e2b_policy_for_refresh(&policy).is_none());
}

#[test]
fn e2b_resume_honors_timeout_and_auto_pause() {
    let current = e2b_auto_lifecycle_policy(45, false, false);
    let updated = e2b_policy_for_resume_update(&current, Some(120), Some(true))
        .expect("resume fields should update policy");
    let lifecycle = e2b_lifecycle_config_from_policy(&updated).expect("e2b lifecycle");

    assert_eq!(lifecycle.timeout_secs, 120);
    assert!(lifecycle.auto_pause);
    assert!(!lifecycle.auto_resume);
}

#[test]
fn e2b_timeout_update_preserves_e2b_pause_resume_flags() {
    let policy = e2b_auto_lifecycle_policy(45, true, true);
    let updated = crate::e2b::policy_for_timeout_update(&policy, 120);
    let lifecycle = e2b_lifecycle_config_from_policy(&updated).expect("e2b lifecycle");

    assert_eq!(lifecycle.timeout_secs, 120);
    assert!(lifecycle.auto_pause);
    assert!(lifecycle.auto_resume);
}

#[test]
fn e2b_runtime_unreachable_touch_is_not_a_delivery() {
    assert_eq!(
        classify_touch(Some(AutoLifecycleTouchOutcome::RuntimeUnreachable)),
        LiveKeepaliveTouchOutcome::RuntimeUnreachable
    );
}

#[test]
fn e2b_transport_unavailable_touch_is_not_a_delivery() {
    assert_eq!(
        classify_touch(Some(AutoLifecycleTouchOutcome::TransportUnavailable)),
        LiveKeepaliveTouchOutcome::TransportUnavailable
    );
}

#[test]
fn e2b_not_started_touch_is_not_a_delivery() {
    assert_eq!(
        classify_touch(Some(AutoLifecycleTouchOutcome::NotStarted)),
        LiveKeepaliveTouchOutcome::NotStarted
    );
}

#[test]
fn e2b_applied_touch_is_delivered() {
    assert_eq!(
        classify_touch(Some(AutoLifecycleTouchOutcome::Applied)),
        LiveKeepaliveTouchOutcome::Delivered
    );
}

#[test]
fn e2b_already_current_touch_is_delivered() {
    assert_eq!(
        classify_touch(Some(AutoLifecycleTouchOutcome::AlreadyCurrent)),
        LiveKeepaliveTouchOutcome::Delivered
    );
}

#[test]
fn e2b_touch_on_not_running_sandbox_is_delivered() {
    // Not Running: no touch was attempted; the deadline applies on the next restore.
    assert_eq!(classify_touch(None), LiveKeepaliveTouchOutcome::Delivered);
}

#[test]
fn e2b_pause_and_resume_actions_use_suspend_and_restore() {
    use crate::controllers::sandbox::lifecycle::SandboxAction;
    assert_eq!(e2b_native_pause_action(), SandboxAction::Suspend);
    assert_eq!(e2b_native_resume_action(), SandboxAction::Restore);
}

#[test]
fn request_client_id_reads_header_case_insensitively() {
    let mut headers = HeaderMap::new();
    headers.insert("X-Client-ID", "scheduled-client".parse().unwrap());
    assert_eq!(
        request_client_id(&headers),
        Some("scheduled-client".to_string())
    );

    // HTTP header names are case-insensitive, so any casing resolves.
    let mut lower = HeaderMap::new();
    lower.insert("x-client-id", "cleanup".parse().unwrap());
    assert_eq!(request_client_id(&lower), Some("cleanup".to_string()));
}

#[test]
fn request_client_id_trims_and_treats_blank_as_absent() {
    let mut padded = HeaderMap::new();
    padded.insert("x-client-id", "  example-client  ".parse().unwrap());
    assert_eq!(
        request_client_id(&padded),
        Some("example-client".to_string())
    );

    let mut blank = HeaderMap::new();
    blank.insert("x-client-id", "   ".parse().unwrap());
    assert_eq!(request_client_id(&blank), None);

    assert_eq!(request_client_id(&HeaderMap::new()), None);
}

#[test]
fn connect_wakes_when_persisted_lifecycle_requires_restore() {
    let mut sandbox = test_sandbox();
    let mut native = native_sandbox_response(sandbox.id);
    native.lifecycle = Some("started".to_string());
    native.status = Some("started".to_string());

    for lifecycle in ["suspended", "restoring"] {
        sandbox.lifecycle = lifecycle.to_string();
        assert!(should_wake_on_connect(&sandbox, &native), "{lifecycle}");
    }
}

#[test]
fn connect_wake_decision_keeps_running_sandbox_running() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "started".to_string();
    let native = native_sandbox_response(sandbox.id);

    assert!(!should_wake_on_connect(&sandbox, &native));
}

#[test]
fn connect_ensures_usable_pod_for_every_running_shape() {
    let mut native = native_sandbox_response(SandboxId::new());

    assert!(should_ensure_usable_pod_on_connect(&native));

    native.lifecycle = Some("restoring".to_string());
    native.status = Some("restoring".to_string());
    assert!(should_ensure_usable_pod_on_connect(&native));

    native.lifecycle = Some("paused".to_string());
    native.status = Some("paused".to_string());
    assert!(!should_ensure_usable_pod_on_connect(&native));

    native.lifecycle = Some("failed".to_string());
    native.status = Some("failed".to_string());
    assert!(!should_ensure_usable_pod_on_connect(&native));
}

#[test]
fn connect_touch_preserves_legacy_paths() {
    let native = native_sandbox_response(SandboxId::new());
    let disabled = AutoLifecyclePolicy::disabled();
    let e2b_policy = e2b_auto_lifecycle_policy(90, true, false);

    assert!(should_touch_on_connect(&disabled, Some(90), &native));
    assert!(should_touch_on_connect(&e2b_policy, None, &native));
    assert!(should_touch_on_connect(&disabled, None, &native));
}

#[test]
fn connect_touch_recovery_wakes_paused_state_without_retrying() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "suspended".to_string();
    let mut native = native_sandbox_response(sandbox.id);
    native.lifecycle = Some("started".to_string());
    native.status = Some("started".to_string());

    assert_eq!(
        connect_touch_recovery(
            LiveKeepaliveTouchOutcome::RuntimeUnreachable,
            &sandbox,
            &native,
            2
        ),
        ConnectTouchRecovery::Wake
    );
    assert_eq!(
        connect_touch_recovery(LiveKeepaliveTouchOutcome::Delivered, &sandbox, &native, 2),
        ConnectTouchRecovery::Continue
    );

    sandbox.lifecycle = "started".to_string();
    assert_eq!(
        connect_touch_recovery(
            LiveKeepaliveTouchOutcome::RuntimeUnreachable,
            &sandbox,
            &native,
            2
        ),
        ConnectTouchRecovery::Retry
    );
}

#[test]
fn unreachable_connect_touch_retries_only_running_state_with_budget() {
    let mut native = native_sandbox_response(SandboxId::new());

    assert!(can_retry_unreachable_connect_touch(&native, 1));
    assert!(!can_retry_unreachable_connect_touch(&native, 0));

    native.lifecycle = Some("paused".to_string());
    native.status = Some("paused".to_string());
    assert!(!can_retry_unreachable_connect_touch(&native, 1));
}

#[test]
fn connect_touch_recovery_wakes_running_state_after_unreachable_retries_are_spent() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "started".to_string();
    let native = native_sandbox_response(sandbox.id);

    let recovery = connect_touch_recovery(
        LiveKeepaliveTouchOutcome::RuntimeUnreachable,
        &sandbox,
        &native,
        0,
    );

    assert_eq!(recovery, ConnectTouchRecovery::Wake);
}

#[test]
fn connect_touch_transport_outage_fails_fast_instead_of_waking() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "started".to_string();
    let native = native_sandbox_response(sandbox.id);

    let recovery = connect_touch_recovery(
        LiveKeepaliveTouchOutcome::TransportUnavailable,
        &sandbox,
        &native,
        2,
    );

    assert_eq!(recovery, ConnectTouchRecovery::Timeout);
}

#[test]
fn connect_touch_recovery_wakes_running_state_after_not_started_touch() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "started".to_string();
    let native = native_sandbox_response(sandbox.id);

    let recovery =
        connect_touch_recovery(LiveKeepaliveTouchOutcome::NotStarted, &sandbox, &native, 2);

    assert_eq!(recovery, ConnectTouchRecovery::Wake);
}

#[test]
fn connect_touch_recovery_joins_active_recovery_after_not_started_touch() {
    for lifecycle in [SandboxLifecycle::Started, SandboxLifecycle::Restoring] {
        let mut sandbox = test_sandbox();
        sandbox.lifecycle = lifecycle.to_string();
        let mut native = native_sandbox_response(sandbox.id);
        native.status = Some("stopped".to_string());

        let recovery =
            connect_touch_recovery(LiveKeepaliveTouchOutcome::NotStarted, &sandbox, &native, 2);

        assert_eq!(
            recovery,
            ConnectTouchRecovery::Wake,
            "{lifecycle} recovery should be joined"
        );
    }
}

#[test]
fn connect_touch_recovery_does_not_wake_teardown_state() {
    let mut native = native_sandbox_response(SandboxId::new());
    native.lifecycle = Some("deleting".to_string());
    native.status = Some("deleting".to_string());
    let mut paused_native = native_sandbox_response(SandboxId::new());
    paused_native.lifecycle = Some("paused".to_string());
    paused_native.status = Some("paused".to_string());

    for lifecycle in ["stopping", "deleting", "cleanup_required", "deleted"] {
        let mut sandbox = test_sandbox();
        sandbox.lifecycle = lifecycle.to_string();

        assert_eq!(
            connect_touch_recovery(LiveKeepaliveTouchOutcome::NotStarted, &sandbox, &native, 0),
            ConnectTouchRecovery::Timeout,
            "{lifecycle} should not be woken by connect recovery"
        );
        assert_eq!(
            connect_touch_recovery(
                LiveKeepaliveTouchOutcome::RuntimeUnreachable,
                &sandbox,
                &native,
                0
            ),
            ConnectTouchRecovery::Timeout,
            "{lifecycle} should not be woken by connect recovery"
        );
        assert_eq!(
            connect_touch_recovery(
                LiveKeepaliveTouchOutcome::RuntimeUnreachable,
                &sandbox,
                &paused_native,
                0
            ),
            ConnectTouchRecovery::Timeout,
            "{lifecycle} should not be woken by paused-state connect recovery"
        );
    }
}

#[test]
fn not_started_connect_touch_can_wake_refetched_paused_state() {
    let mut sandbox = test_sandbox();
    sandbox.lifecycle = "suspended".to_string();
    let native = native_sandbox_response(sandbox.id);

    assert_eq!(
        connect_touch_recovery(LiveKeepaliveTouchOutcome::NotStarted, &sandbox, &native, 2),
        ConnectTouchRecovery::Wake
    );
}

#[test]
fn create_translation_allows_session_metadata() {
    let mut request = base_create_request();
    request.template_id = Some("app-runtime".to_string());
    request.metadata = StringMap::from([(String::from("session_id"), String::from("session-123"))]);
    let translation = translate_create_request(request).unwrap();

    assert_eq!(translation.template_id, "app-runtime");
    assert_eq!(
        translation
            .native
            .template
            .as_ref()
            .map(|template| template.name().as_str()),
        Some("app-runtime")
    );
    assert_eq!(translation.native.name, None);
}

#[test]
fn sandbox_response_uses_ecw_host_domain() {
    let native = native_sandbox_response(SandboxId::new());
    let response = build_sandbox_response(
        &test_ecw_credential_keyring(),
        &native,
        "seed".to_string(),
        Some(TEST_ECW_HOST.to_string()),
        pplx_space_common_core::types::EcwCredentialEpoch::new(1).unwrap(),
    );

    assert_eq!(response.domain.as_deref(), Some(TEST_ECW_HOST));
}

#[test]
fn ecw_host_alias_selects_alias_response_domain() {
    let config = test_ecw_host_config();
    let mut headers = HeaderMap::new();
    headers.insert(header::HOST, TEST_ECW_ALIAS_API_HOST.parse().unwrap());

    let pair = config
        .api_pair_for_headers(&headers)
        .expect("alias API host should match");
    let native = native_sandbox_response(SandboxId::new());
    let response = build_sandbox_response(
        &test_ecw_credential_keyring(),
        &native,
        "seed".to_string(),
        Some(pair.host),
        pplx_space_common_core::types::EcwCredentialEpoch::new(1).unwrap(),
    );
    assert_eq!(response.domain.as_deref(), Some(TEST_ECW_ALIAS_HOST));
}

#[tokio::test]
async fn metadata_query_supports_sdk_encoded_strings() {
    use axum::extract::FromRequestParts;

    // SDK wire queries include the HTTP query layer around two component
    // encoding layers. Expected strings are the caller's original metadata.
    for (query, key, value) in [
        ("metadata=image%3Dpython%25253A3.11", "image", "python:3.11"),
        (
            "metadata=user%3Dalice%252540example.com",
            "user",
            "alice@example.com",
        ),
        ("metadata=path%3D%252Ftmp%252Fa%252520b", "path", "/tmp/a b"),
        (
            "metadata=key%252526%25253D%25252B%3Dvalue%252526%25253D%25252B",
            "key&=+",
            "value&=+",
        ),
        ("metadata=unicode%3Dcaf%2525C3%2525A9", "unicode", "café"),
        ("metadata=literal%3D%2525253A", "literal", "%3A"),
        ("metadata=literal%3D%252525253A", "literal", "%253A"),
        ("metadata=empty%3D", "empty", ""),
        ("metadata=value%3D%252525", "value", "%"),
        ("metadata=value%3D%2525252G", "value", "%2G"),
        // Direct clients can use unescaped or singly encoded values too.
        ("metadata=image%3Dpython%3A3.11", "image", "python:3.11"),
        ("metadata=value%3Da%2526b%253Dc%252Bd", "value", "a&b=c+d"),
    ] {
        let (mut parts, _) = Request::builder()
            .uri(format!("/v2/sandboxes?{query}"))
            .body(())
            .unwrap()
            .into_parts();
        let E2bQuery(query) = E2bQuery::<SandboxListQueryE2b>::from_request_parts(&mut parts, &())
            .await
            .unwrap();
        assert_eq!(
            parse_metadata_query(query.metadata.as_deref()).unwrap(),
            vec![(key.to_owned(), value.to_owned())],
        );
    }
    assert_eq!(
        parse_metadata_query(Some("name=session-123&user=abc")).unwrap(),
        vec![
            ("name".into(), "session-123".into()),
            ("user".into(), "abc".into())
        ],
    );
    for metadata in [None, Some("")] {
        assert!(parse_metadata_query(metadata).unwrap().is_empty());
    }
    for invalid in ["invalid", "value=%FF", "value=%25FF", "%FF=value"] {
        let err = parse_metadata_query(Some(invalid)).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }
}

#[test]
fn metadata_query_rejects_duplicate_keys() {
    // JSONB containment is single-value per key. Silently collapsing
    // `env=prod&env=staging` to the last entry would drop a caller-supplied
    // predicate before the SQL `@>` runs, so we reject it explicitly at the
    // boundary.
    for metadata in [
        "env=prod&env=staging",
        "env=prod&%65nv=staging",
        "env=prod&%2565nv=staging",
    ] {
        let err = parse_metadata_query(Some(metadata))
            .expect_err("duplicate decoded keys must be rejected");
        assert!(err.message.contains("duplicate metadata key"), "{err:?}");
    }
}

#[test]
fn state_filter_accepts_running_and_paused() {
    let states = parse_state_filter(Some("running,paused")).unwrap();

    assert_eq!(
        states,
        vec![SandboxStateE2b::Running, SandboxStateE2b::Paused]
    );
    parse_state_filter(Some("stopped")).unwrap_err();
}

#[test]
fn metadata_filter_jsonb_round_trips_typed_values() {
    // No filter -> None.
    assert!(build_metadata_filter_jsonb(&[]).is_none());

    // String, integer, and boolean values must serialize to the same shape
    // that `e2b_metadata_value_to_native` produced when the row was created,
    // otherwise `metadata @> $1::jsonb` won't match. The conversion follows
    // the same rules: "true"/"false" -> bool, parseable i64 -> number,
    // everything else -> string.
    let filters = vec![
        ("name".to_string(), "session-123".to_string()),
        ("count".to_string(), "3".to_string()),
        ("active".to_string(), "true".to_string()),
    ];
    let value = build_metadata_filter_jsonb(&filters).expect("non-empty filter");
    let object = value.as_object().expect("object");
    assert_eq!(object.get("name"), Some(&json!("session-123")));
    assert_eq!(object.get("count"), Some(&json!(3)));
    assert_eq!(object.get("active"), Some(&json!(true)));
}

#[test]
fn list_cursor_round_trip_preserves_keyset() {
    let sandbox = test_sandbox();
    let token = encode_list_cursor(&sandbox);
    let decoded = decode_list_cursor(&token).unwrap();
    assert_eq!(decoded.id, sandbox.id);
    assert_eq!(decoded.created_at, sandbox.created_at);
}

#[test]
fn list_cursor_rejects_invalid_tokens() {
    // Not base64.
    decode_list_cursor("not a token").unwrap_err();
    // Base64 of a string that isn't `<rfc3339>:<uuid>`.
    let garbage = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"garbage");
    decode_list_cursor(&garbage).unwrap_err();
    // Valid timestamp with a non-UUID id.
    let bad_id =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"2026-04-29T12:00:00Z:not-a-uuid");
    decode_list_cursor(&bad_id).unwrap_err();
}

#[test]
fn native_state_translation_hides_terminal_sandboxes() {
    for lifecycle in ["stopped", "failed", "quarantined"] {
        let sandbox = SandboxResponse {
            lifecycle: Some(lifecycle.to_string()),
            status: Some(lifecycle.to_string()),
            ..native_sandbox_response(SandboxId::new())
        };

        assert_eq!(translate_sandbox_state(&sandbox), None);
    }
}

#[tokio::test]
async fn failed_terminal_state_detail_serializes_failed_and_recover_mapper_fails_to_place() {
    let detail_sandbox = test_sandbox();
    let sandbox = SandboxResponse {
        lifecycle: Some("failed".to_string()),
        status: Some("failed".to_string()),
        ..native_sandbox_response(detail_sandbox.id)
    };

    let detail = build_detail_sandbox(
        &test_ecw_credential_keyring(),
        &detail_sandbox,
        &sandbox,
        Some(TEST_ECW_HOST.to_string()),
    )
    .expect("failed detail sandbox should serialize");
    assert_eq!(detail.state, SandboxStateE2b::Failed);

    let response = terminal_sandbox_error(&sandbox).into_response();

    assert_e2b_error_response(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        500,
        "Failed to place sandbox",
        "failed terminal read",
    )
    .await;

    let quarantined = SandboxResponse {
        lifecycle: Some("quarantined".to_string()),
        status: Some("quarantined".to_string()),
        ..native_sandbox_response(detail_sandbox.id)
    };
    let response = terminal_sandbox_error(&quarantined).into_response();
    assert_e2b_error_response(
        response,
        StatusCode::INTERNAL_SERVER_ERROR,
        500,
        "Failed to place sandbox",
        "quarantined terminal read",
    )
    .await;
}

#[tokio::test]
async fn stopped_terminal_state_stays_not_found() {
    let sandbox = SandboxResponse {
        lifecycle: Some("stopped".to_string()),
        status: Some("stopped".to_string()),
        ..native_sandbox_response(SandboxId::new())
    };

    let response = terminal_sandbox_error(&sandbox).into_response();

    assert_e2b_error_response(
        response,
        StatusCode::NOT_FOUND,
        404,
        "Sandbox not found",
        "stopped terminal read",
    )
    .await;
}

#[tokio::test]
async fn unknown_terminal_state_defaults_to_not_found() {
    let sandbox = SandboxResponse {
        lifecycle: Some("lost".to_string()),
        status: Some("lost".to_string()),
        ..native_sandbox_response(SandboxId::new())
    };

    let response = terminal_sandbox_error(&sandbox).into_response();

    assert_e2b_error_response(
        response,
        StatusCode::NOT_FOUND,
        404,
        "Sandbox not found",
        "unknown terminal read",
    )
    .await;
}

#[derive(Clone, Copy)]
enum ConnectRouteActiveNode {
    None,
    ReadyWorker,
    MissingWorker,
}

#[derive(Clone, Copy)]
enum ConnectRouteExpectation {
    Success(StatusCode),
    Error(StatusCode),
}

#[derive(Clone, Copy)]
struct ConnectRouteDbShape {
    label: &'static str,
    lifecycle: &'static str,
    placement: &'static str,
    active_node: ConnectRouteActiveNode,
    observed_status: Option<&'static str>,
    preload_runtime: bool,
    expectation: ConnectRouteExpectation,
}

impl ConnectRouteDbShape {
    fn ok(
        label: &'static str,
        lifecycle: &'static str,
        placement: &'static str,
        active_node: ConnectRouteActiveNode,
    ) -> Self {
        Self::new(
            label,
            lifecycle,
            placement,
            active_node,
            ConnectRouteExpectation::Success(StatusCode::OK),
        )
    }

    fn error(
        label: &'static str,
        lifecycle: &'static str,
        placement: &'static str,
        active_node: ConnectRouteActiveNode,
        status: StatusCode,
    ) -> Self {
        Self::new(
            label,
            lifecycle,
            placement,
            active_node,
            ConnectRouteExpectation::Error(status),
        )
    }

    fn service_unavailable(
        label: &'static str,
        lifecycle: &'static str,
        placement: &'static str,
        active_node: ConnectRouteActiveNode,
    ) -> Self {
        Self::new(
            label,
            lifecycle,
            placement,
            active_node,
            ConnectRouteExpectation::Error(StatusCode::SERVICE_UNAVAILABLE),
        )
    }

    fn with_observed_status(mut self, observed_status: &'static str) -> Self {
        self.observed_status = Some(observed_status);
        self
    }

    fn with_preloaded_runtime(mut self) -> Self {
        self.preload_runtime = true;
        self
    }

    fn new(
        label: &'static str,
        lifecycle: &'static str,
        placement: &'static str,
        active_node: ConnectRouteActiveNode,
        expectation: ConnectRouteExpectation,
    ) -> Self {
        Self {
            label,
            lifecycle,
            placement,
            active_node,
            observed_status: None,
            preload_runtime: false,
            expectation,
        }
    }
}

#[derive(Clone, Copy)]
struct ConnectRouteRequestShape {
    label: &'static str,
    body: Option<&'static str>,
}

#[tokio::test(flavor = "current_thread")]
async fn connect_route_extends_deadline_without_replacing_policy() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    struct Case {
        label: &'static str,
        path_prefix: &'static str,
        body: Option<&'static str>,
        requested_timeout_secs: Option<u32>,
    }

    let db = pg_test_db().await?;
    let worker_node_id = NodeId::new();
    let worker = start_connect_route_sim_worker(worker_node_id).await?;
    insert_connect_route_ready_worker(&db.pool, worker_node_id, worker.endpoint()).await?;
    let org = fetch_local_org(&db.pool).await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);

    let cases = [
        Case {
            label: "legacy empty body",
            path_prefix: "",
            body: None,
            requested_timeout_secs: None,
        },
        Case {
            label: "legacy empty object",
            path_prefix: "",
            body: Some("{}"),
            requested_timeout_secs: None,
        },
        Case {
            label: "v2 empty body",
            path_prefix: "/v2",
            body: None,
            requested_timeout_secs: Some(300),
        },
        Case {
            label: "v2 empty object",
            path_prefix: "/v2",
            body: Some("{}"),
            requested_timeout_secs: Some(300),
        },
        Case {
            label: "v2 explicit default",
            path_prefix: "/v2",
            body: Some(r#"{"timeout":300}"#),
            requested_timeout_secs: Some(300),
        },
        Case {
            label: "v2 explicit shorter timeout",
            path_prefix: "/v2",
            body: Some(r#"{"timeout":60}"#),
            requested_timeout_secs: Some(60),
        },
        Case {
            label: "legacy explicit default",
            path_prefix: "",
            body: Some(r#"{"timeout":300}"#),
            requested_timeout_secs: Some(300),
        },
        Case {
            label: "legacy explicit longer timeout",
            path_prefix: "",
            body: Some(r#"{"timeout":900}"#),
            requested_timeout_secs: Some(900),
        },
        Case {
            label: "v2 explicit longer timeout",
            path_prefix: "/v2",
            body: Some(r#"{"timeout":1200}"#),
            requested_timeout_secs: Some(1_200),
        },
    ];

    for (lifecycle, policy_timeout) in [
        ("started", 120),
        ("suspended", 120),
        ("started", 300),
        ("suspended", 300),
        ("started", 900),
        ("suspended", 900),
    ] {
        for case in &cases {
            let sandbox_id = SandboxId::new();
            insert_connect_route_sandbox(
                &db.pool,
                sandbox_id,
                lifecycle,
                "on_node",
                Some(worker_node_id),
            )
            .await?;
            let policy = e2b_auto_lifecycle_policy(policy_timeout, true, false);
            sqlx::query(sql!(
                "UPDATE sandboxes
                 SET auto_lifecycle_json = $1
                 WHERE id = $2"
            ))
            .bind(sqlx::types::Json(&policy))
            .bind(sandbox_id)
            .execute(&db.pool)
            .await?;
            if lifecycle == "suspended" {
                sqlx::query(sql!(
                    "INSERT INTO sandbox_direct_route_provenance (
                         sandbox_id,
                         last_route_generation,
                         last_execution_generation,
                         updated_at
                     )
                     VALUES ($1, 1, 1, NOW())"
                ))
                .bind(sandbox_id)
                .execute(&db.pool)
                .await?;
            } else {
                // Deadline mutations require the resident execution's published identity.
                sqlx::query(sql!(
                    "INSERT INTO sandbox_direct_route_identities
                    (sandbox_id, route_generation, current_node_id, substrate_kind,
                     execution_generation, activation_attempt_id)
                    VALUES ($1, 1, $2, 'space', 1, $3)"
                ))
                .bind(sandbox_id)
                .bind(worker_node_id)
                .bind(Uuid::now_v7())
                .execute(&db.pool)
                .await?;
                worker
                    .state()
                    .ensure_default_running(&sandbox_id.to_string())
                    .await?;
            }

            let accepted_at = jiff::Timestamp::now().as_second();
            // A pre-existing extension must also survive a shorter reconnect request.
            let previous_floor =
                (lifecycle == "started" && policy_timeout == 900).then_some(accepted_at + 1_800);
            if previous_floor.is_some() {
                worker
                    .state()
                    .apply_access_deadline(&sandbox_id.to_string(), previous_floor, None)
                    .await?;
            }
            let mut request = connect_route_request(
                sandbox_id,
                ConnectRouteRequestShape {
                    label: case.label,
                    body: case.body,
                },
            )?;
            *request.uri_mut() =
                format!("{}/sandboxes/{sandbox_id}/connect", case.path_prefix).parse()?;
            let response = app.clone().oneshot(request).await?;
            let completed_at = jiff::Timestamp::now().as_second();
            let status = response.status();
            let body = to_bytes(response.into_body(), usize::MAX).await?;
            assert_eq!(
                status,
                StatusCode::OK,
                "{lifecycle} / {policy_timeout}s / {}: {}",
                case.label,
                String::from_utf8_lossy(&body)
            );

            let sqlx::types::Json(stored_policy): sqlx::types::Json<AutoLifecyclePolicy> =
                sqlx::query_scalar(sql!(
                    "SELECT auto_lifecycle_json
                     FROM sandboxes
                     WHERE id = $1"
                ))
                .bind(sandbox_id)
                .fetch_one(&db.pool)
                .await?;
            let config = e2b_lifecycle_config_from_policy(&stored_policy)
                .expect("connect must leave an E2B-compatible policy");
            assert_eq!(
                config.timeout_secs, policy_timeout,
                "{lifecycle} / {policy_timeout}s / {}",
                case.label
            );
            let record = worker.state().get(&sandbox_id.to_string()).await?;
            if let Some(timeout) = case.requested_timeout_secs {
                assert!(
                    record.access_extended_until.is_some_and(|deadline| {
                        let earliest =
                            (accepted_at + i64::from(timeout)).max(previous_floor.unwrap_or(0));
                        let latest =
                            (completed_at + i64::from(timeout)).max(previous_floor.unwrap_or(0));
                        (earliest..=latest).contains(&deadline)
                    }),
                    "{lifecycle} / {policy_timeout}s / {}",
                    case.label
                );
            } else {
                assert_eq!(
                    record.access_extended_until, previous_floor,
                    "{lifecycle} / {policy_timeout}s / {} must retain legacy omission semantics",
                    case.label
                );
            }
            assert!(config.auto_pause, "{lifecycle} / {}", case.label);
            assert!(!config.auto_resume, "{lifecycle} / {}", case.label);
        }
    }

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn connect_route_never_bad_gateways_for_db_state_shapes() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    let db = pg_test_db().await?;
    let worker_node_id = NodeId::new();
    let worker = start_connect_route_sim_worker(worker_node_id).await?;
    insert_connect_route_ready_worker(&db.pool, worker_node_id, worker.endpoint()).await?;
    let org = fetch_local_org(&db.pool).await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);

    let request_shapes = [
        ConnectRouteRequestShape {
            label: "empty body",
            body: None,
        },
        ConnectRouteRequestShape {
            label: "timeout body",
            body: Some(r#"{"timeout":120}"#),
        },
    ];
    let db_shapes = [
        ConnectRouteDbShape::ok(
            "started active worker, runtime already present",
            "started",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        )
        .with_preloaded_runtime(),
        ConnectRouteDbShape::ok(
            "started active worker, runtime missing",
            "started",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "creating active worker, runtime missing",
            "creating",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "starting active worker, runtime missing",
            "starting",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "restoring active worker, runtime missing",
            "restoring",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "paused active worker, runtime missing",
            "paused",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "suspended active worker, runtime missing",
            "suspended",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "pausing active worker, runtime missing",
            "pausing",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "resuming active worker, runtime missing",
            "resuming",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::ok(
            "suspending active worker, runtime missing",
            "suspending",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        ),
        ConnectRouteDbShape::service_unavailable(
            "started unplaced without active worker",
            "started",
            "unplaced",
            ConnectRouteActiveNode::None,
        ),
        ConnectRouteDbShape::service_unavailable(
            "started on_node without active worker",
            "started",
            "on_node",
            ConnectRouteActiveNode::None,
        ),
        ConnectRouteDbShape::service_unavailable(
            "started active worker id missing",
            "started",
            "on_node",
            ConnectRouteActiveNode::MissingWorker,
        ),
        ConnectRouteDbShape::service_unavailable(
            "creating unplaced without active worker",
            "creating",
            "unplaced",
            ConnectRouteActiveNode::None,
        ),
        ConnectRouteDbShape::error(
            "stopping teardown row",
            "stopping",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
            StatusCode::CONFLICT,
        ),
        ConnectRouteDbShape::error(
            "deleting teardown row",
            "deleting",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
            StatusCode::CONFLICT,
        ),
        ConnectRouteDbShape::error(
            "cleanup_required teardown row",
            "cleanup_required",
            "unplaced",
            ConnectRouteActiveNode::None,
            StatusCode::CONFLICT,
        ),
        ConnectRouteDbShape::error(
            "deleted teardown row",
            "deleted",
            "unplaced",
            ConnectRouteActiveNode::None,
            StatusCode::NOT_FOUND,
        ),
        ConnectRouteDbShape::error(
            "failed terminal row",
            "failed",
            "unplaced",
            ConnectRouteActiveNode::None,
            StatusCode::INTERNAL_SERVER_ERROR,
        ),
        ConnectRouteDbShape::error(
            "stopped terminal row",
            "stopped",
            "unplaced",
            ConnectRouteActiveNode::None,
            StatusCode::NOT_FOUND,
        ),
        ConnectRouteDbShape::error(
            "started row with failed observation",
            "started",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .with_observed_status("failed"),
        ConnectRouteDbShape::error(
            "started row with stopped observation",
            "started",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
            StatusCode::NOT_FOUND,
        )
        .with_observed_status("stopped"),
        ConnectRouteDbShape::ok(
            "started row with paused observation",
            "started",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
        )
        .with_observed_status("paused"),
        ConnectRouteDbShape::error(
            "failed row with running observation",
            "failed",
            "on_node",
            ConnectRouteActiveNode::ReadyWorker,
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .with_observed_status("running"),
    ];

    for request_shape in request_shapes {
        for db_shape in db_shapes {
            let sandbox_id = SandboxId::new();
            let active_node_id = match db_shape.active_node {
                ConnectRouteActiveNode::None => None,
                ConnectRouteActiveNode::ReadyWorker => Some(worker_node_id),
                ConnectRouteActiveNode::MissingWorker => Some(NodeId::new()),
            };
            insert_connect_route_sandbox(
                &db.pool,
                sandbox_id,
                db_shape.lifecycle,
                db_shape.placement,
                active_node_id,
            )
            .await?;
            if db_shape.lifecycle == "suspended" {
                sqlx::query(
                    "INSERT INTO sandbox_direct_route_provenance (
                         sandbox_id,
                         last_route_generation,
                         last_execution_generation,
                         updated_at
                     )
                     VALUES ($1, 1, 1, NOW())",
                )
                .bind(sandbox_id)
                .execute(&db.pool)
                .await?;
            }
            if let Some(observed_status) = db_shape.observed_status {
                insert_connect_route_observed_state(
                    &db.pool,
                    sandbox_id,
                    worker_node_id,
                    observed_status,
                )
                .await?;
            }
            if db_shape.preload_runtime {
                // A resident runtime has a published execution identity. Deadline
                // mutations are fenced by that generation, not just active_node_id.
                sqlx::query(sql!(
                    "INSERT INTO sandbox_direct_route_identities
                    (sandbox_id, route_generation, current_node_id, substrate_kind,
                     execution_generation, activation_attempt_id)
                    VALUES ($1, 1, $2, 'space', 1, $3)"
                ))
                .bind(sandbox_id)
                .bind(worker_node_id)
                .bind(Uuid::now_v7())
                .execute(&db.pool)
                .await?;
                worker
                    .state()
                    .ensure_default_running(&sandbox_id.to_string())
                    .await?;
            }

            let response = app
                .clone()
                .oneshot(connect_route_request(sandbox_id, request_shape)?)
                .await?;
            let response_status = response.status();
            assert_ne!(
                response_status,
                StatusCode::BAD_GATEWAY,
                "{} / {} returned 502",
                request_shape.label,
                db_shape.label
            );

            match db_shape.expectation {
                ConnectRouteExpectation::Success(expected) => {
                    assert_connect_success_response(
                        response,
                        expected,
                        sandbox_id,
                        request_shape.label,
                        db_shape.label,
                    )
                    .await?;
                    let record = worker.state().get(&sandbox_id.to_string()).await?;
                    assert_eq!(
                        record.status,
                        status::RUNNING,
                        "{} / {} did not leave a running local runtime",
                        request_shape.label,
                        db_shape.label
                    );
                }
                ConnectRouteExpectation::Error(expected) => {
                    assert_eq!(
                        response_status, expected,
                        "{} / {} returned an unexpected status",
                        request_shape.label, db_shape.label
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_recovers_interrupted_source_pin_after_flag_rollback() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    use crate::controllers::substrate::kubernetes::tests::{
        insert_ready_full_snapshot_relative_to_lifecycle,
        running_pod_kubernetes_client_with_create_barrier,
        substrate_with_store_client_proxy_port_and_pod,
    };

    let fixture = build_kubernetes_app_state_fixture(false).await?;
    let sandbox_id = SandboxId::new();
    let pod_name = format!("space-sbx-{}-g2", sandbox_id.to_string().replace('-', ""));
    let storage_uri = "s3://space-test/suspend-snapshots/interrupted?region=us-east-1";
    insert_connect_route_ready_worker(
        &fixture.pool,
        fixture.worker_node_id,
        fixture.worker.endpoint(),
    )
    .await?;
    insert_connect_route_sandbox(&fixture.pool, sandbox_id, "restoring", "unplaced", None).await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET managed_by = 'k8s',
                restore_lease_token = $2,
                restore_lease_expires_at = NOW() - INTERVAL '1 second'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.pool)
    .await?;
    insert_ready_full_snapshot_relative_to_lifecycle(
        &fixture.pool,
        sandbox_id,
        storage_uri,
        -60,
        Uuid::now_v7(),
    )
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_k8s_executions
             (sandbox_id, generation, pod_name, pod_namespace, pod_uid, state, active,
              created_at, updated_at)
         VALUES ($1, 2, $2, 'space', 'pinned-predecessor', 'created', TRUE,
                 NOW() - INTERVAL '10 minutes', NOW() - INTERVAL '10 minutes')"
    ))
    .bind(sandbox_id)
    .bind(&pod_name)
    .execute(&fixture.pool)
    .await?;
    let pinned = json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {
            "name": pod_name, "namespace": "space",
            "uid": "pinned-predecessor", "resourceVersion": "1",
            "annotations": {
                "space.pplx.net/sandbox-id": sandbox_id.to_string(),
                "space.pplx.net/execution-generation": "2",
                "space.pplx.net/suspend-restore-source-node": "unavailable-source",
                "space.pplx.net/suspend-restore-storage-uri": storage_uri
            }
        },
        "spec": {
            "containers": [],
            "affinity": {"nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {"nodeSelectorTerms": [{
                    "matchFields": [{"key": "metadata.name", "operator": "In", "values": ["unavailable-source"]}]
                }]}
            }}
        },
        "status": {"phase": "Pending", "conditions": [{
            "type": "PodScheduled", "status": "False", "reason": "Unschedulable",
            "message": "source node unavailable"
        }]}
    });
    // The default flag is off. A persisted pin must recover through the public
    // route even when a different replica handles the retry after rollback.
    let kubernetes = substrate_with_store_client_proxy_port_and_pod(
        fixture.pool.clone(),
        running_pod_kubernetes_client_with_create_barrier(
            Arc::clone(&fixture.pod_creates),
            "127.0.0.1",
            None,
            false,
            Some(pinned),
        ),
        fixture.worker.port(),
        None,
    );
    let mut state = fixture.state.clone();
    state.sandbox_controller = crate::controllers::sandbox::SandboxController::new(
        crate::controllers::substrate::Substrate::from_kubernetes(kubernetes),
    );
    let org = fetch_local_org(&fixture.pool).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);
    let response = app
        .oneshot(connect_route_request(
            sandbox_id,
            ConnectRouteRequestShape {
                label: "empty body",
                body: None,
            },
        )?)
        .await?;
    assert_connect_success_response(
        response,
        StatusCode::OK,
        sandbox_id,
        "empty body",
        "interrupted pinned restore with expired ownership",
    )
    .await?;
    let executions: Vec<(i64, bool, String)> = sqlx::query_as(sql!(
        "SELECT generation, active, state FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 ORDER BY generation"
    ))
    .bind(sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(executions, vec![(2, true, "running".to_string())]);
    let creates = fixture.pod_creates.lock().expect("capture mutex poisoned");
    assert_eq!(
        creates.len(),
        1,
        "recover the existing generation exactly once"
    );
    let successor = &creates[0];
    assert_eq!(
        successor.pointer("/metadata/annotations/space.pplx.net~1suspend-restore-storage-uri"),
        Some(&json!(storage_uri))
    );
    assert_eq!(
        successor.pointer("/metadata/annotations/space.pplx.net~1suspend-restore-source-node"),
        Some(&json!("unavailable-source"))
    );
    let successor: Pod = serde_json::from_value(successor.clone())?;
    let spec = successor.spec.expect("successor spec");
    assert!(spec.node_name.is_none());
    let terms = spec
        .affinity
        .and_then(|affinity| affinity.node_affinity)
        .and_then(|affinity| affinity.required_during_scheduling_ignored_during_execution)
        .map(|selector| selector.node_selector_terms)
        .unwrap_or_default();
    assert!(
        !terms
            .iter()
            .flat_map(|term| term.match_fields.iter().flatten())
            .any(|field| field.key == "metadata.name"),
        "fallback must escape the unavailable source; other placement boundaries remain"
    );
    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_rejects_missing_restore_context_and_preserves_recovery_state(
    #[future] missing_failed_recovery_fixture: TestResult<MissingFailedRecoveryFixture>,
) -> TestResult {
    // Arrange.
    let fixture = missing_failed_recovery_fixture.await?;
    let request_shape = ConnectRouteRequestShape {
        label: "empty body",
        body: None,
    };

    // Act.
    for _ in 0..2 {
        let response = fixture
            .app
            .clone()
            .oneshot(connect_route_request(fixture.sandbox_id, request_shape)?)
            .await?;
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert!(!response.headers().contains_key(header::RETRY_AFTER));
    }

    // Assert.
    let executions = sqlx::query_as::<
        _,
        (
            i64,
            bool,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(sql!(
        "SELECT generation, active, state, pod_uid, host(pod_ip), k8s_node_name
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation",
    ))
    .bind(fixture.sandbox_id)
    .fetch_all(&fixture.pool)
    .await?;
    assert_eq!(
        executions,
        vec![
            (1, false, "forgotten".to_string(), None, None, None),
            (2, true, "recovery_failed".to_string(), None, None, None,),
        ]
    );

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_recovers_missing_pod_after_current_suspend_snapshot(
    #[future] missing_pod_suspend_fixture: TestResult<MissingPodSuspendFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    // Arrange.
    let fixture = missing_pod_suspend_fixture.await?;

    // Act.
    let response = fixture
        .app
        .clone()
        .oneshot(connect_route_request(
            fixture.sandbox_id,
            ConnectRouteRequestShape {
                label: "empty body",
                body: None,
            },
        )?)
        .await?;

    // Assert.
    assert_connect_success_response(
        response,
        StatusCode::OK,
        fixture.sandbox_id,
        "empty body",
        "missing pod after a current durable suspend snapshot",
    )
    .await?;
    let executions: Vec<(i64, bool, String)> = sqlx::query_as(
        "SELECT generation, active, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation",
    )
    .bind(fixture.sandbox_id)
    .fetch_all(&fixture.app_state.pool)
    .await?;
    assert_eq!(
        executions,
        vec![
            (1, false, "suspended".to_string()),
            (2, true, "running".to_string()),
        ]
    );
    let pod_creates = fixture
        .app_state
        .pod_creates
        .lock()
        .expect("capture mutex poisoned");
    assert_eq!(pod_creates.len(), 1);
    assert_eq!(
        pod_creates[0]
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .and_then(|annotations| {
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str)
            }),
        Some(fixture.storage_uri)
    );

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_recovers_missing_pod_despite_newer_failed_snapshot_attempt(
    #[future] missing_pod_suspend_fixture: TestResult<MissingPodSuspendFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    // Arrange.
    let fixture = missing_pod_suspend_fixture.await?;
    sqlx::query(
        "INSERT INTO snapshots
             (id, sandbox_id, type, state, timestamp, org_id, source_node_id,
              local_artifact_id, artifact_kind, artifact_origin)
         VALUES
             ($1, $2, 'full', 'failed', NOW() + INTERVAL '1 second',
              '00000000-0000-0000-0000-000000000001', $3, $4, 'full', 'suspend')",
    )
    .bind(Uuid::now_v7())
    .bind(fixture.sandbox_id)
    .bind(fixture.app_state.worker_node_id)
    .bind(Uuid::now_v7())
    .execute(&fixture.app_state.pool)
    .await?;

    // Act.
    let response = fixture
        .app
        .clone()
        .oneshot(connect_route_request(
            fixture.sandbox_id,
            ConnectRouteRequestShape {
                label: "empty body",
                body: None,
            },
        )?)
        .await?;

    // Assert.
    assert_connect_success_response(
        response,
        StatusCode::OK,
        fixture.sandbox_id,
        "empty body",
        "missing pod after a newer failed snapshot attempt",
    )
    .await?;
    let pod_creates = fixture
        .app_state
        .pod_creates
        .lock()
        .expect("capture mutex poisoned");
    assert_eq!(pod_creates.len(), 1);
    assert_eq!(
        pod_creates[0]
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .and_then(|annotations| {
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str)
            }),
        Some(fixture.storage_uri)
    );

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[case::marker_before_lifecycle(MissingPodSuspendMarker::BeforeLifecycle)]
#[case::markerless_snapshot(MissingPodSuspendMarker::Markerless)]
#[case::marker_between_lifecycle_and_activity(MissingPodSuspendMarker::BetweenLifecycleAndActivity)]
#[tokio::test(flavor = "current_thread")]
async fn runtime_client_recovers_missing_suspending_pod(
    #[case] marker: MissingPodSuspendMarker,
    #[future] missing_pod_suspend_fixture: TestResult<MissingPodSuspendFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    use crate::controllers::substrate::{RecoveryCharge, SubstrateAdapter};

    // Arrange.
    let fixture = missing_pod_suspend_fixture.await?;
    match marker {
        MissingPodSuspendMarker::BeforeLifecycle => {}
        MissingPodSuspendMarker::Markerless => {
            sqlx::query(
                "UPDATE sandboxes
                    SET current_local_artifact_id = NULL,
                        current_local_artifact_kind = NULL,
                        current_local_artifact_origin = NULL
                  WHERE id = $1",
            )
            .bind(fixture.sandbox_id)
            .execute(&fixture.app_state.pool)
            .await?;
        }
        MissingPodSuspendMarker::BetweenLifecycleAndActivity => {
            sqlx::query(
                "UPDATE snapshots sn
                    SET timestamp = s.lifecycle_changed_at + INTERVAL '1 second'
                   FROM sandboxes s
                  WHERE sn.sandbox_id = s.id
                    AND s.id = $1
                    AND sn.local_artifact_id = s.current_local_artifact_id",
            )
            .bind(fixture.sandbox_id)
            .execute(&fixture.app_state.pool)
            .await?;
        }
    }

    // Act.
    fixture
        .app_state
        .substrate
        .runtime_client(fixture.sandbox_id, RecoveryCharge::Exempt)
        .await?;

    // Assert.
    let executions: Vec<(i64, bool, String)> = sqlx::query_as(
        "SELECT generation, active, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1
          ORDER BY generation",
    )
    .bind(fixture.sandbox_id)
    .fetch_all(&fixture.app_state.pool)
    .await?;
    assert_eq!(
        executions,
        vec![
            (1, false, "suspended".to_string()),
            (2, true, "running".to_string()),
        ]
    );
    let pod_creates = fixture
        .app_state
        .pod_creates
        .lock()
        .expect("capture mutex poisoned");
    assert_eq!(pod_creates.len(), 1);
    assert_eq!(
        pod_creates[0]
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .and_then(|annotations| {
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str)
            }),
        Some(fixture.storage_uri)
    );

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_preserves_missing_pod_while_suspend_snapshot_is_not_ready(
    #[future]
    #[with("creating")]
    missing_pod_suspend_fixture: TestResult<MissingPodSuspendFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    // Arrange.
    let fixture = missing_pod_suspend_fixture.await?;

    // Act.
    let response = fixture
        .app
        .clone()
        .oneshot(connect_route_request(
            fixture.sandbox_id,
            ConnectRouteRequestShape {
                label: "empty body",
                body: None,
            },
        )?)
        .await?;

    // Assert.
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let sandbox: (String, Option<Uuid>) = sqlx::query_as(
        "SELECT lifecycle, current_local_artifact_id
           FROM sandboxes
          WHERE id = $1",
    )
    .bind(fixture.sandbox_id)
    .fetch_one(&fixture.app_state.pool)
    .await?;
    assert_eq!(sandbox.0, "suspending");
    assert!(sandbox.1.is_some());
    let execution: (bool, String) = sqlx::query_as(
        "SELECT active, state
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND generation = 1",
    )
    .bind(fixture.sandbox_id)
    .fetch_one(&fixture.app_state.pool)
    .await?;
    assert_eq!(execution, (true, "running".to_string()));
    assert!(
        fixture
            .app_state
            .pod_creates
            .lock()
            .expect("capture mutex poisoned")
            .is_empty()
    );

    Ok(())
}

#[cfg(feature = "kubernetes")]
#[rstest]
#[case::single_connect(1)]
#[case::concurrent_connects(2)]
#[tokio::test(flavor = "current_thread")]
async fn connect_route_redrives_when_suspend_finalization_retires_started_execution(
    #[case] _connection_count: usize,
    #[future]
    #[with(_connection_count)]
    in_flight_connect_suspend_race_fixture: TestResult<InFlightConnectSuspendRaceFixture>,
) -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }

    // Arrange.
    let InFlightConnectSuspendRaceFixture { race, requests } =
        in_flight_connect_suspend_race_fixture.await?;

    // Act.
    let mut finalization = race.app_state.pool.begin().await?;
    sqlx::query("UPDATE snapshots SET state = 'ready' WHERE sandbox_id = $1")
        .bind(race.sandbox_id)
        .execute(&mut *finalization)
        .await?;
    sqlx::query("SELECT id FROM sandboxes WHERE id = $1 FOR UPDATE")
        .bind(race.sandbox_id)
        .fetch_one(&mut *finalization)
        .await?;
    sqlx::query(
        "UPDATE sandbox_k8s_executions
            SET state = 'suspended', active = FALSE, pod_ip = NULL, updated_at = NOW()
          WHERE sandbox_id = $1 AND generation = 1",
    )
    .bind(race.sandbox_id)
    .execute(&mut *finalization)
    .await?;
    sqlx::query(
        "UPDATE sandboxes
            SET lifecycle = 'suspended',
                placement = 'unplaced',
                target_node_id = COALESCE(target_node_id, active_node_id),
                active_node_id = NULL
          WHERE id = $1",
    )
    .bind(race.sandbox_id)
    .execute(&mut *finalization)
    .await?;
    finalization.commit().await?;

    // Assert.
    for request in requests {
        let response = tokio::time::timeout(Duration::from_secs(5), request).await??;
        assert_connect_success_response(
            response,
            StatusCode::OK,
            race.sandbox_id,
            "empty body",
            "observed paused while suspend finalization retires the started execution",
        )
        .await?;
    }
    let active_generation: i64 = sqlx::query_scalar(
        "SELECT generation
           FROM sandbox_k8s_executions
          WHERE sandbox_id = $1 AND active",
    )
    .bind(race.sandbox_id)
    .fetch_one(&race.app_state.pool)
    .await?;
    assert_eq!(active_generation, 2);
    let pod_creates = race
        .app_state
        .pod_creates
        .lock()
        .expect("capture mutex poisoned");
    assert_eq!(pod_creates.len(), 1);
    assert_eq!(
        pod_creates[0]
            .pointer("/metadata/annotations")
            .and_then(serde_json::Value::as_object)
            .and_then(|annotations| {
                annotations
                    .get(SANDBOX_SUSPEND_RESTORE_STORAGE_URI_ANNOTATION)
                    .and_then(serde_json::Value::as_str)
            }),
        Some(race.storage_uri)
    );

    Ok(())
}

async fn fetch_local_org(pool: &PgPool) -> TestResult<Organization> {
    Ok(sqlx::query_as::<_, Organization>(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations
         WHERE id = '00000000-0000-0000-0000-000000000001'",
    )
    .fetch_one(pool)
    .await?)
}

async fn insert_connect_route_sandbox(
    pool: &PgPool,
    sandbox_id: SandboxId,
    lifecycle: &str,
    placement: &str,
    active_node_id: Option<NodeId>,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, template_id, content_hash, network_policy_generation
         )
         VALUES (
             $1,
             '00000000-0000-0000-0000-000000000010',
             '00000000-0000-0000-0000-000000000001',
             'space',
             $2,
             $3,
             $4,
             'seed',
             'hash-seed',
             1
         )",
    )
    .bind(sandbox_id)
    .bind(lifecycle)
    .bind(placement)
    .bind(active_node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_connect_route_observed_state(
    pool: &PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
    observed_status: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandbox_observed_states (
             sandbox_id, node_id, status, runtime, runtime_version,
             guest_os, guest_arch, state_format_name, state_format_version,
             template, snapshots_enabled
         )
         VALUES (
             $1, $2, $3, 'fc', '1.15.1',
             'linux', 'aarch64', 'firecracker-snapshot', '9.0.0',
             'seed', TRUE
         )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .bind(observed_status)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_connect_route_ready_worker(
    pool: &PgPool,
    node_id: NodeId,
    endpoint: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_direct (node_id, node_api_endpoint)
         VALUES ($1, $2)",
    )
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_status_snapshots (
             node_id, observed_at, stale_after, reachable, role,
             scheduling_status, health_healthy, services, updated_at
         )
         VALUES (
             $1, NOW(), NOW() + INTERVAL '1 hour', TRUE, 'worker',
             'ready', TRUE, '[]'::JSONB, NOW()
         )",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_runtime_installations
             (node_id, runtime, runtime_version)
         VALUES ($1, 'fc', '1.15.1')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_runtime_guests
             (node_id, runtime, runtime_version, guest_os, guest_arch)
         VALUES ($1, 'fc', '1.15.1', 'linux', 'aarch64')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_runtime_state_formats (
             node_id, runtime, runtime_version, state_format_name,
             state_format_version, can_create, can_load
         )
         VALUES ($1, 'fc', '1.15.1', 'firecracker-snapshot', '9.0.0', TRUE, TRUE)",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn connect_route_request(
    sandbox_id: SandboxId,
    shape: ConnectRouteRequestShape,
) -> Result<Request<Body>, HttpError> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(format!("/sandboxes/{sandbox_id}/connect"))
        .header(header::HOST, TEST_ECW_API_HOST);
    if shape.body.is_some() {
        builder = builder.header(header::CONTENT_TYPE, "application/json");
    }
    builder.body(match shape.body {
        Some(body) => Body::from(body),
        None => Body::empty(),
    })
}

async fn assert_connect_success_response(
    response: Response,
    expected_status: StatusCode,
    sandbox_id: SandboxId,
    request_label: &str,
    db_shape_label: &str,
) -> TestResult {
    let response_status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX).await?;
    assert_eq!(
        response_status,
        expected_status,
        "{request_label} / {db_shape_label}: {}",
        String::from_utf8_lossy(&body)
    );
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        value.get("sandboxID"),
        Some(&json!(sandbox_id.to_string())),
        "{request_label} / {db_shape_label}"
    );
    assert_eq!(
        value.get("clientID"),
        Some(&json!(sandbox_id.to_string())),
        "{request_label} / {db_shape_label}"
    );
    assert_eq!(
        value.get("templateID"),
        Some(&json!("seed")),
        "{request_label} / {db_shape_label}"
    );
    assert_eq!(
        value.get("domain"),
        Some(&json!(TEST_ECW_HOST)),
        "{request_label} / {db_shape_label}"
    );
    assert!(value.get("envdVersion").is_some());
    assert!(value.get("envdAccessToken").is_some());
    assert!(value.get("trafficAccessToken").is_some());
    Ok(())
}

async fn start_connect_route_sim_worker(node_id: NodeId) -> TestResult<SimWorkerHarness> {
    Ok(SimWorkerHarness::builder(node_id.to_string())
        .with_internal_api()
        .start()
        .await?)
}

#[test]
fn metadata_uses_sandbox_metadata() {
    let mut sandbox = test_sandbox();
    sandbox.metadata = sqlx::types::Json(std::collections::BTreeMap::from([
        (
            "name".to_string(),
            SandboxMetadataValue::String("session-123".to_string()),
        ),
        ("priority".to_string(), SandboxMetadataValue::Number(3)),
        ("debug".to_string(), SandboxMetadataValue::Bool(true)),
    ]));
    let response = SandboxResponse {
        name: sandbox.name.clone(),
        user_id: Some(sandbox.user_id),
        org_id: Some(sandbox.org_id),
        lifecycle: Some(sandbox.lifecycle.clone()),
        placement: Some(sandbox.placement.clone()),
        active_node_id: sandbox.active_node_id,
        metadata: sandbox.metadata.0.clone(),
        status: Some("started".to_string()),
        ..native_sandbox_response(sandbox.id)
    };

    let metadata = sandbox_metadata(&sandbox, &response);
    assert_eq!(
        metadata.get("name").map(String::as_str),
        Some("session-123")
    );
    assert_eq!(metadata.get("priority").map(String::as_str), Some("3"));
    assert_eq!(metadata.get("debug").map(String::as_str), Some("true"));
}

#[tokio::test(flavor = "current_thread")]
async fn failed_terminal_state_control_plane_routes_return_failed_to_place() -> TestResult {
    let db = pg_test_db().await?;
    let get_sandbox_id = SandboxId::new();
    let connect_sandbox_id = SandboxId::new();
    let resume_sandbox_id = SandboxId::new();
    let timeout_sandbox_id = SandboxId::new();
    let refresh_sandbox_id = SandboxId::new();
    let org = insert_failed_terminal_sandbox(&db.pool, get_sandbox_id).await?;
    insert_failed_terminal_sandbox(&db.pool, connect_sandbox_id).await?;
    insert_failed_terminal_sandbox(&db.pool, resume_sandbox_id).await?;
    insert_failed_terminal_sandbox(&db.pool, timeout_sandbox_id).await?;
    insert_failed_terminal_sandbox(&db.pool, refresh_sandbox_id).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);

    let get_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/sandboxes/{get_sandbox_id}"))
                .header(header::HOST, TEST_ECW_API_HOST)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(get_response.status(), StatusCode::OK);
    let body = to_bytes(get_response.into_body(), usize::MAX).await?;
    let value: serde_json::Value = serde_json::from_slice(&body)?;
    assert_eq!(
        value.get("sandboxID"),
        Some(&json!(get_sandbox_id.to_string()))
    );
    assert_eq!(value.get("state"), Some(&json!("failed")));

    for (label, request) in [
        (
            "failed connect sandbox",
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{connect_sandbox_id}/connect"))
                .header(header::HOST, TEST_ECW_API_HOST)
                .body(Body::empty())?,
        ),
        (
            "failed resume sandbox",
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{resume_sandbox_id}/resume"))
                .header(header::HOST, TEST_ECW_API_HOST)
                .body(Body::empty())?,
        ),
        (
            "failed timeout sandbox",
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{timeout_sandbox_id}/timeout"))
                .header(header::HOST, TEST_ECW_API_HOST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"timeout":120}"#))?,
        ),
        (
            "failed refresh sandbox",
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{refresh_sandbox_id}/refreshes"))
                .header(header::HOST, TEST_ECW_API_HOST)
                .body(Body::empty())?,
        ),
    ] {
        let response = app.clone().oneshot(request).await?;
        assert_e2b_error_response(
            response,
            StatusCode::INTERNAL_SERVER_ERROR,
            500,
            "Failed to place sandbox",
            label,
        )
        .await;
    }
    Ok(())
}

#[test]
fn detail_sandbox_uses_cached_runtime_spec() {
    let mut sandbox = test_sandbox();
    sandbox.template_id = Some("cached-template".to_string());
    sandbox.vcpu = Some(4);
    sandbox.memory_mib = Some(8192);
    sandbox.disk_mib = Some(32768);
    let response = SandboxResponse {
        template: None,
        disk_usage_bytes: None,
        ..native_sandbox_response(sandbox.id)
    };

    let detail = build_detail_sandbox(
        &test_ecw_credential_keyring(),
        &sandbox,
        &response,
        Some(TEST_ECW_HOST.to_string()),
    )
    .unwrap();

    assert_eq!(detail.template_id, "cached-template");
    assert_eq!(detail.cpu_count, 4);
    assert_eq!(detail.memory_mb, 8192);
    assert_eq!(detail.disk_size_mb, 32768);
    assert_eq!(detail.domain.as_deref(), Some(TEST_ECW_HOST));
}

#[test]
fn sandbox_template_id_does_not_append_content_hash() {
    let mut sandbox = test_sandbox();
    sandbox.template_id = Some("app-runtime".to_string());
    sandbox.content_hash = Some("66c258cefa258e64704c18490fee0c7e2ef4da15".to_string());
    let response = SandboxResponse {
        template: Some("app-runtime".to_string()),
        ..native_sandbox_response(sandbox.id)
    };

    assert_eq!(
        sandbox_template_id(&sandbox, &response, &super::E2bRuntimeSpec::default()),
        "app-runtime"
    );
}

#[test]
fn sandbox_template_id_preserves_unpinned_template_id() {
    let mut sandbox = test_sandbox();
    sandbox.template_id = Some("app-runtime".to_string());
    let response = SandboxResponse {
        template: Some("app-runtime".to_string()),
        ..native_sandbox_response(sandbox.id)
    };

    assert_eq!(
        sandbox_template_id(&sandbox, &response, &super::E2bRuntimeSpec::default()),
        "app-runtime"
    );
}

async fn e2b_path_requires_api_key(path: &str, host: Option<&str>) {
    let response = e2b_path_response(path, host).await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        value,
        json!({
            "code": 401,
            "message": "Invalid or missing API key"
        })
    );
}

async fn e2b_path_response(path: &str, host: Option<&str>) -> axum::response::Response {
    let state = test_state().await;
    let app =
        path_root_router_with_ecw_hosts(state.clone(), test_ecw_host_config()).with_state(state);
    let mut request = Request::builder().uri(path);

    if let Some(host) = host {
        request = request.header(header::HOST, host);
    }

    app.oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn e2b_path_response_with_logs(
    path: &str,
    host: Option<&str>,
) -> (axum::response::Response, Vec<CapturedEvent>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(EventFieldCapture {
        events: Arc::clone(&events),
    });
    let guard = tracing::subscriber::set_default(subscriber);
    let response = e2b_path_response(path, host).await;
    drop(guard);

    let events = events.lock().unwrap().clone();
    (response, events)
}

fn only_ecw_request_log(events: &[CapturedEvent]) -> &[(String, String)] {
    let matching = ecw_request_logs(events);
    assert_eq!(matching.len(), 1, "events={events:?}");
    matching[0]
}

fn assert_no_ecw_request_log(events: &[CapturedEvent]) {
    let matching = ecw_request_logs(events);
    assert!(matching.is_empty(), "events={events:?}");
}

fn ecw_request_logs(events: &[CapturedEvent]) -> Vec<&[(String, String)]> {
    events
        .iter()
        .filter(|event| {
            event.iter().any(|(field_name, field_value)| {
                field_name == "event_type" && field_value == ECW_SANDBOX_API_LOG_EVENT
            })
        })
        .map(Vec::as_slice)
        .collect()
}

fn test_ecw_host_config() -> EcwHostConfig {
    EcwHostConfig::from_env_value(Some(
        r#"[
            {"apiHost":"api.e2b.example.test","host":"e2b.example.test"},
            {"apiHost":"api.e2b.staging.example.test","host":"e2b.staging.example.test"}
        ]"#,
    ))
}

/// The ECW half of the contention attribution, which the public-surface table
/// in `routes/public/tests.rs` does not reach.
///
/// Both entry points matter and neither was covered: `translate::controller_error`
/// is the path for every controller error on this surface, and
/// `fork::fork_store_error` is the fork-admission path, whose advisory-lock
/// race is the case the ECW comments single out. A refactor dropping either
/// early return would leave every ECW contention 503 re-labelled
/// `Cluster`/`BackendStatus` with the suite green.
#[tokio::test(flavor = "current_thread")]
async fn ecw_contention_keeps_its_own_attribution() -> pplx_space_test_env::TestResult {
    use pplx_space_metrics::response::{ResponseKind, ResponseOrigin};

    let Some(db) = pplx_space_db_test_fixtures::try_pg_test_db().await? else {
        return Ok(());
    };

    // Controller path.
    let mut contended = crate::controllers::ControllerError::service_unavailable("contended");
    contended.mark_lock_contention_for_test();
    let mapped = super::translate::controller_error(contended);
    assert_eq!(
        mapped.status_for_test(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        mapped.classification_for_test(),
        (ResponseOrigin::Api, ResponseKind::Internal),
        "a Postgres lock race is this service's, not the fleet's"
    );

    // A non-contention 503 keeps the fleet attribution, so the branch is doing
    // the discriminating rather than the status code.
    let fleet = crate::controllers::ControllerError::service_unavailable("node is gone");
    assert_eq!(
        super::translate::controller_error(fleet).classification_for_test(),
        (ResponseOrigin::Cluster, ResponseKind::BackendStatus)
    );

    // Fork-admission path.
    let contended = crate::public_operations::PublicOperationStoreError::Sql(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await,
    );
    let mapped = super::fork::fork_store_error_for_test("admit fork", contended);
    assert_eq!(
        mapped.status_for_test(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );

    // A real conflict on the same path stays a 500.
    let conflict = crate::public_operations::PublicOperationStoreError::Sql(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await,
    );
    assert_eq!(
        super::fork::fork_store_error_for_test("admit fork", conflict).status_for_test(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );

    // The replay probe, which runs *before* admission. A 500 here is worse than
    // a 500 after it: the callers who reach the probe are exactly the ones
    // holding an idempotency key, whose retry would find the committed record
    // and get the deduped answer instead of forking twice.
    let contended = crate::fork_operations::AdmitForkOperationError::Store(
        crate::public_operations::PublicOperationStoreError::Sql(
            pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await,
        ),
    );
    assert_eq!(
        super::fork::fork_admission_error_for_test("observe fork replay", contended)
            .status_for_test(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE,
        "a lock race on the replay probe must be retryable"
    );

    // A non-store admission failure on the same path is a real 500: a bad
    // fingerprint or an incomplete federated context does not get better on
    // retry.
    assert_eq!(
        super::fork::fork_admission_error_for_test(
            "observe fork replay",
            crate::fork_operations::AdmitForkOperationError::IncompleteFederatedContext,
        )
        .status_for_test(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );

    // The federated-owner materialization. Its contention case has to reach
    // the translator (so it lands `Api`/`Internal` rather than a fleet fault),
    // but *only* its contention case: the translator's fallthrough stamps
    // `Cluster`/`BackendStatus`, so routing every outcome through it would
    // blame the sandbox fleet for a decode failure in this service's own
    // upsert. Both directions asserted, because fixing one by inverting it is
    // exactly how this regressed once.
    let contended = crate::federated_identity::MaterializeOwnerError::Organization(
        pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await,
    );
    let mapped = super::fork::fork_owner_error_for_test(contended);
    assert_eq!(
        mapped.classification_for_test(),
        (ResponseOrigin::Api, ResponseKind::Internal),
        "a lock race on the owner upsert is this service's database"
    );
    assert_eq!(
        mapped.status_for_test(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );

    let internal =
        crate::federated_identity::MaterializeOwnerError::Organization(sqlx::Error::RowNotFound);
    let mapped = super::fork::fork_owner_error_for_test(internal);
    assert_eq!(
        mapped.status_for_test(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        mapped.classification_for_test(),
        (ResponseOrigin::Api, ResponseKind::Internal),
        "a non-contention failure in our own upsert is not the fleet's"
    );

    // The fork path's bare reads — the replay-owner `organizations` lookup and
    // the persisted deadline. They are ordinary reads with no locking clause,
    // which is exactly why they were left on the 500: nothing in the SQL says
    // they can block, and under the role bound they can.
    let contended = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "55P03").await;
    assert_eq!(
        super::fork::fork_sql_error_for_test("resolve fork replay owner", &contended)
            .status_for_test(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
    let real = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert_eq!(
        super::fork::fork_sql_error_for_test("resolve fork replay owner", &real).status_for_test(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    );
    Ok(())
}

/// Route-level regression for grant renewal on reconnect: drives the real
/// `/connect` handler and proves a carried grant advances the persisted
/// row's blob and expiry — deleting the handler's persistence block fails
/// this test. Renewal ordering (issued-at monotone, expiry fallback, stale
/// skips) is the store's contract, owned by the `schema_tests` suite.
#[tokio::test(flavor = "current_thread")]
async fn connect_renews_the_persisted_federated_grant() -> TestResult {
    use crate::federated_identity::{
        DEFAULT_FEDERATED_GRANT_AUDIENCE, FEDERATED_OWNER_GRANT_PURPOSE,
        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER, FEDERATED_SERVICE_GRANT_HEADER,
        FederatedServiceGrant, upsert_sandbox_federated_service_grant,
    };

    let db = pg_test_db().await?;
    let fixture = federated_connect_fixture(&db).await?;
    let now_unix = time::OffsetDateTime::now_utc().unix_timestamp();
    let initial_expiry = now_unix + 3 * 24 * 3600;
    let fresher_expiry = now_unix + 7 * 24 * 3600;
    upsert_sandbox_federated_service_grant(
        &db.pool,
        fixture.sandbox_id,
        &fixture.owner,
        &FederatedServiceGrant::from_unix_expiry(b"initial-grant".to_vec(), initial_expiry)?,
        DEFAULT_FEDERATED_GRANT_AUDIENCE,
        FEDERATED_OWNER_GRANT_PURPOSE,
    )
    .await?;

    let response = fixture
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(format!("/sandboxes/{}/connect", fixture.sandbox_id))
                .header(header::HOST, TEST_ECW_API_HOST)
                .header(FEDERATED_SERVICE_GRANT_HEADER, "renewed-grant")
                .header(
                    FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
                    fresher_expiry.to_string(),
                )
                .body(Body::empty())?,
        )
        .await?;
    assert_ne!(
        response.status(),
        StatusCode::UNAUTHORIZED,
        "a valid grant must never be rejected"
    );
    let (blob, expiry): (Vec<u8>, i64) = sqlx::query_as(sql!(
        "SELECT grant_blob, EXTRACT(EPOCH FROM grant_expires_at)::bigint
         FROM sandbox_federated_service_grants
         WHERE sandbox_id = $1"
    ))
    .bind(fixture.sandbox_id)
    .fetch_one(&db.pool)
    .await?;
    assert_eq!(blob, b"renewed-grant".to_vec());
    assert_eq!(expiry, fresher_expiry);

    Ok(())
}

struct FederatedConnectFixture {
    app: Router,
    sandbox_id: SandboxId,
    owner: crate::federated_identity::FederatedOwner,
}

/// A federated-owned sandbox behind the real e2b router, with the federated
/// owner injected as the auth extension the middleware would attach.
async fn federated_connect_fixture(db: &TestDb) -> TestResult<FederatedConnectFixture> {
    use pplx_space_common_core::types::{UpstreamIssuer, UpstreamOrgId, UpstreamSubjectId};

    let sandbox_id = SandboxId::new();
    let org_id = OrgId::new();
    let user_id = UserId::new();
    sqlx::query(sql!(
        "INSERT INTO organizations (
             id, name, slug, owner_source, upstream_issuer, upstream_org_id
         )
         VALUES ($1, 'Federated Connect Org', $2, 'federated_identity', 'example-issuer', 'example-org-1')"
    ))
    .bind(org_id)
    .bind(format!("federated-connect-{org_id}"))
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO users (
             id, email, password_hash, display_name, org_id, role,
             owner_source, upstream_issuer, upstream_subject_id
         )
         VALUES ($1, $2, 'federated-user-no-local-password', 'Federated Connect User',
                 $3, 'member', 'federated_identity', 'example-issuer', '123')"
    ))
    .bind(user_id)
    .bind(format!("federated-connect-{user_id}@space.invalid"))
    .bind(org_id)
    .execute(&db.pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
         VALUES ($1, $2, $3, 1)"
    ))
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(&db.pool)
    .await?;

    let owner = crate::federated_identity::FederatedOwner {
        issuer: UpstreamIssuer::parse("example-issuer")?,
        org_id: UpstreamOrgId::parse("example-org-1")?,
        subject_id: UpstreamSubjectId::parse("123")?,
    };
    let org = sqlx::query_as::<_, Organization>(sql!(
        "SELECT id, name, slug, parent_id, theme_primary, theme_secondary, logo_url,
                byok_enabled, snapshot_base_object_storage_uri, created_at, updated_at
         FROM organizations WHERE id = $1"
    ))
    .bind(org_id)
    .fetch_one(&db.pool)
    .await?;
    let state = AppState::from_pool(db.pool.clone()).await?;
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(crate::middleware::auth::AuthFederatedOwner(
            owner.clone(),
        )))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);
    Ok(FederatedConnectFixture {
        app,
        sandbox_id,
        owner,
    })
}

/// While the federated grant leg ramps, a bare connect (no grant pair) is not
/// rejected for the missing grant; it proceeds and fails later for the unknown
/// sandbox.
#[tokio::test]
async fn connect_without_a_grant_is_not_rejected_during_the_ramp() {
    let db = pg_test_db().await.expect("test db");
    let fixture = federated_connect_fixture(&db).await.expect("fixture");
    let bare_connect = Request::builder()
        .method("POST")
        .uri(format!("/sandboxes/{}/connect", Uuid::now_v7()))
        .header(header::HOST, TEST_ECW_API_HOST)
        .body(Body::empty())
        .expect("request");

    let response = fixture
        .app
        .clone()
        .oneshot(bare_connect)
        .await
        .expect("router");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[rstest]
#[case::status(Method::GET, "/templates/tenant-inputs/builds/foreign/status", "")]
#[case::start(Method::POST, "/v2/templates/tenant-inputs/builds/foreign", "{}")]
#[tokio::test]
async fn template_build_rejects_another_orgs_inputs(
    #[future] template_alias_fixture: TestResult<TemplateAliasFixture>,
    #[case] method: Method,
    #[case] path: &str,
    #[case] body: &str,
) -> TestResult {
    let fixture = template_alias_fixture.await?;
    sqlx::query(pplx_space_common_core::sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, daemon_job_id, org_id)
         VALUES ('tenant-inputs', 'foreign', $1, $2)"
    ))
    .bind(Uuid::now_v7().to_string())
    .bind(Uuid::from_u128(2))
    .execute(&fixture.pool)
    .await?;

    let response = fixture
        .client
        .app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("x-api-key", TEMPLATE_ALIAS_API_KEY)
                .header(header::HOST, TEST_ECW_API_HOST)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_owned()))?,
        )
        .await?;

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn unsupported_connect_policy_and_local_reset_remain_terminal_preconditions() {
    for message in [
        "connect requires a single AccessIdle stage",
        "local lifecycle policy overrides cluster timeout",
    ] {
        let response = super::controller_error(crate::controllers::ControllerError::from(
            tonic::Status::failed_precondition(message),
        ))
        .into_response();
        assert_eq!(response.status(), StatusCode::PRECONDITION_FAILED);
        assert!(
            response
                .headers()
                .get(axum::http::header::RETRY_AFTER)
                .is_none()
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn connect_applies_deadline_after_slow_activation() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let worker = start_connect_route_sim_worker(node).await?;
    insert_connect_route_ready_worker(&db.pool, node, worker.endpoint()).await?;
    let sandbox = SandboxId::new();
    insert_connect_route_sandbox(&db.pool, sandbox, "suspended", "on_node", Some(node)).await?;
    sqlx::query(sql!(
        "INSERT INTO sandbox_direct_route_provenance
        (sandbox_id, last_route_generation, last_execution_generation, updated_at)
        VALUES ($1, 1, 1, NOW())"
    ))
    .bind(sandbox)
    .execute(&db.pool)
    .await?;
    let org = fetch_local_org(&db.pool).await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    // Supply the original activity timestamp, as retained across a slow wake.
    // No wall-clock sleep: freshness must be independent of its age.
    let accepted = jiff::Timestamp::now().as_second() - 90;
    let response = super::lifecycle::connect_sandbox_inner(
        axum::extract::State(state),
        super::hosts::MatchedEcwHost(super::hosts::EcwHostPair {
            api_host: "api.e2b.example.test".into(),
            host: "e2b.example.test".into(),
        }),
        Extension(AuthOrg(org)),
        None,
        axum::extract::Path(sandbox.to_string()),
        HeaderMap::new(),
        Some(crate::e2b::E2bJson(super::ConnectSandboxRequestE2b {
            timeout: Some(900),
            memory: None,
        })),
        prost_types::Timestamp {
            seconds: accepted,
            nanos: 0,
        },
    )
    .await
    .map_err(|error| format!("connect failed: {:?}", error.into_response().status()))?;
    assert_eq!(response.0.sandbox_id, sandbox.to_string());
    let record = worker.state().get(&sandbox.to_string()).await?;
    assert_eq!(record.status, status::RUNNING);
    assert_eq!(record.access_extended_until, Some(accepted + 900));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn deadline_routes_resolve_unpublished_execution_identity() -> TestResult {
    use prost::Message;

    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let worker = start_connect_route_sim_worker(node).await?;
    insert_connect_route_ready_worker(&db.pool, node, worker.endpoint()).await?;
    let org = fetch_local_org(&db.pool).await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);

    for (label, route, requested, generation, published_generation, expected_status) in [
        (
            "changed reset",
            "timeout",
            120,
            Some(7),
            None,
            StatusCode::NO_CONTENT,
        ),
        (
            "same-value reset",
            "timeout",
            300,
            Some(7),
            None,
            StatusCode::NO_CONTENT,
        ),
        ("extension", "connect", 1200, Some(7), None, StatusCode::OK),
        (
            "unidentified reset",
            "timeout",
            300,
            None,
            None,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "unidentified extension",
            "connect",
            1200,
            None,
            None,
            StatusCode::SERVICE_UNAVAILABLE,
        ),
        (
            "published identity with omitted State generation",
            "timeout",
            300,
            None,
            Some(7_i64),
            StatusCode::NO_CONTENT,
        ),
        (
            "superseded execution",
            "timeout",
            300,
            Some(7),
            Some(6_i64),
            StatusCode::PRECONDITION_FAILED,
        ),
    ] {
        let sandbox = SandboxId::new();
        let key = sandbox.to_string();
        // The worker has a resident execution, but its receipt has not reached
        // the API. A missing receipt cannot turn a timeout reset into a no-op.
        insert_connect_route_sandbox(&db.pool, sandbox, "started", "on_node", Some(node)).await?;
        let policy = e2b_auto_lifecycle_policy(300, true, false);
        sqlx::query(sql!(
            "UPDATE sandboxes SET auto_lifecycle_json = $1 WHERE id = $2"
        ))
        .bind(sqlx::types::Json(&policy))
        .bind(sandbox)
        .execute(&db.pool)
        .await?;
        if let Some(published_generation) = published_generation {
            sqlx::query(sql!(
                "INSERT INTO sandbox_direct_route_identities
                (sandbox_id, route_generation, current_node_id, substrate_kind,
                 execution_generation, activation_attempt_id)
                VALUES ($1, 7, $2, 'space', $3, $4)"
            ))
            .bind(sandbox)
            .bind(node)
            .bind(published_generation)
            .bind(Uuid::now_v7())
            .execute(&db.pool)
            .await?;
        }
        worker.state().ensure_default_running(&key).await?;
        worker
            .state()
            .set_route_identity_provenance(&key, Some(7), generation, None)
            .await?;
        let floor = jiff::Timestamp::now().as_second() + 900;
        worker
            .state()
            .apply_access_deadline(
                &key,
                Some(floor),
                Some(crate::controllers::auto_lifecycle::policy_to_proto(&policy).encode_to_vec()),
            )
            .await?;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/sandboxes/{sandbox}/{route}"))
                    .header(header::HOST, TEST_ECW_API_HOST)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"timeout":{requested}}}"#)))?,
            )
            .await?;
        assert_eq!(response.status(), expected_status, "{label}");
        let runtime = worker.state().get(&key).await?;
        assert_eq!(
            runtime.status,
            status::RUNNING,
            "{label}: must not recreate execution"
        );
        assert_eq!(runtime.execution_generation, generation, "{label}");
        if !expected_status.is_success() {
            assert_eq!(runtime.access_extended_until, Some(floor), "{label}");
        } else if route == "timeout" {
            assert_eq!(
                runtime.access_extended_until, None,
                "{label}: reset must reach worker"
            );
            assert_eq!(
                runtime.access_idle_policy,
                crate::controllers::auto_lifecycle::policy_to_proto(&e2b_auto_lifecycle_policy(
                    requested, true, false
                ))
                .encode_to_vec(),
                "{label}"
            );
        } else {
            assert!(
                runtime
                    .access_extended_until
                    .is_some_and(|deadline| deadline >= floor + 300),
                "{label}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_route_distinguishes_dormant_intent_from_unreachable_execution() -> TestResult {
    let db = pg_test_db().await?;
    let org = fetch_local_org(&db.pool).await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    configure_test_ecw_credential_keyring(&mut state);
    let app = e2b_api_routes()
        .layer(Extension(AuthOrg(org)))
        .layer(Extension(test_ecw_host_config()))
        .with_state(state);

    for (label, lifecycle, managed_by, assigned, requested, expected_status, duplicate_wake) in [
        (
            "suspended changed",
            "suspended",
            "space",
            false,
            120_u32,
            StatusCode::NO_CONTENT,
            false,
        ),
        (
            "suspended unchanged",
            "suspended",
            "space",
            false,
            300,
            StatusCode::NO_CONTENT,
            false,
        ),
        (
            "unplaced started changed",
            "started",
            "k8s",
            false,
            120,
            StatusCode::NO_CONTENT,
            false,
        ),
        (
            "unplaced started unchanged",
            "started",
            "k8s",
            false,
            300,
            StatusCode::NO_CONTENT,
            false,
        ),
        (
            "assigned unreachable changed",
            "started",
            "space",
            true,
            120,
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            "assigned unreachable unchanged",
            "started",
            "space",
            true,
            300,
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            "direct unplaced remains uncertain",
            "started",
            "space",
            false,
            120,
            StatusCode::SERVICE_UNAVAILABLE,
            false,
        ),
        (
            "equivalent noncanonical policy unchanged",
            "started",
            "k8s",
            false,
            300,
            StatusCode::NO_CONTENT,
            true,
        ),
        (
            "noncanonical policy changed",
            "started",
            "k8s",
            false,
            120,
            StatusCode::NO_CONTENT,
            true,
        ),
    ] {
        let sandbox_id = SandboxId::new();
        // An assigned worker without a published endpoint is not evidence of
        // absence. Kubernetes has durable activation ownership; its unplaced
        // intent needs no RPC, while direct launch has no equivalent proof.
        let node_id = if assigned {
            let node = NodeId::new();
            sqlx::query(sql!(
                "INSERT INTO nodes (id, address, role, managed_by, state)
                 VALUES ($1, '127.0.0.1', 'worker', 'space', 'registered')"
            ))
            .bind(node)
            .execute(&db.pool)
            .await?;
            Some(node)
        } else {
            None
        };
        insert_connect_route_sandbox(
            &db.pool,
            sandbox_id,
            lifecycle,
            if assigned { "on_node" } else { "unplaced" },
            node_id,
        )
        .await?;
        let mut initial_policy = e2b_auto_lifecycle_policy(300, true, duplicate_wake);
        if duplicate_wake {
            initial_policy.wake.on.push(initial_policy.wake.on[0]);
        }
        sqlx::query(sql!(
            "UPDATE sandboxes SET auto_lifecycle_json = $1, managed_by = $3,
             next_auto_event_at = extract(epoch FROM now())::bigint + 300 WHERE id = $2"
        ))
        .bind(sqlx::types::Json(&initial_policy))
        .bind(sandbox_id)
        .bind(managed_by)
        .execute(&db.pool)
        .await?;

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/sandboxes/{sandbox_id}/timeout"))
                    .header(header::HOST, TEST_ECW_API_HOST)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(r#"{{"timeout":{requested}}}"#)))?,
            )
            .await?;
        assert_eq!(response.status(), expected_status, "{label}");

        let sqlx::types::Json(stored): sqlx::types::Json<AutoLifecyclePolicy> = sqlx::query_scalar(
            sql!("SELECT auto_lifecycle_json FROM sandboxes WHERE id = $1"),
        )
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        let config = e2b_lifecycle_config_from_policy(&stored).expect("e2b lifecycle");
        assert_eq!(config.timeout_secs, requested, "{label}");
        assert!(config.auto_pause, "{label}");
        assert_eq!(config.auto_resume, duplicate_wake, "{label}");
        let scheduling_preserved: bool = sqlx::query_scalar(sql!(
            "SELECT next_auto_event_at IS NOT NULL FROM sandboxes WHERE id = $1"
        ))
        .bind(sandbox_id)
        .fetch_one(&db.pool)
        .await?;
        assert_eq!(scheduling_preserved, requested == 300, "{label}");
        if requested == 300 {
            assert_eq!(
                stored, initial_policy,
                "equivalent policy must not be rewritten: {label}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn timeout_observes_existing_activation_without_retargeting_or_restarting() -> TestResult {
    if !crate::test_support::enter_sim_worker_process() {
        return Ok(());
    }
    let db = pg_test_db().await?;
    let node = NodeId::new();
    let worker = start_connect_route_sim_worker(node).await?;
    insert_connect_route_ready_worker(&db.pool, node, worker.endpoint()).await?;
    let state = AppState::from_pool(db.pool.clone()).await?;

    for outcome in ["ready", "superseded", "expired"] {
        let id = SandboxId::new();
        let key = id.to_string();
        insert_connect_route_sandbox(&db.pool, id, "started", "on_node", Some(node)).await?;
        sqlx::query(sql!(
            "INSERT INTO sandbox_direct_route_identities
             (sandbox_id, route_generation, current_node_id, substrate_kind,
              execution_generation, activation_attempt_id)
             VALUES ($1, 7, $2, 'space', 7, $3)"
        ))
        .bind(id)
        .bind(node)
        .bind(Uuid::now_v7())
        .execute(&db.pool)
        .await?;
        worker.state().ensure_default_running(&key).await?;
        worker
            .state()
            .set_route_identity_provenance(&key, Some(7), Some(7), None)
            .await?;
        worker.state().set_status(&key, status::CREATING).await?;
        let floor = jiff::Timestamp::now().as_second() + 900;
        worker
            .state()
            .apply_access_deadline(&key, Some(floor), None)
            .await?;
        let mut sandbox = test_sandbox();
        sandbox.id = id;
        sandbox.lifecycle = "started".into();
        sandbox.active_execution_generation = Some(7);
        let deadline = tokio::time::Instant::now()
            + if outcome == "expired" {
                Duration::from_millis(350)
            } else {
                Duration::from_secs(5)
            };
        let observed_at = prost_types::Timestamp {
            seconds: jiff::Timestamp::now().as_second(),
            nanos: 0,
        };
        let operation = state.sandbox_controller.set_access_idle_timeout(
            &db.pool,
            &sandbox,
            120,
            observed_at,
            deadline,
            None,
        );
        tokio::pin!(operation);
        // Observe the transaction's real commit before changing activation
        // evidence. The request must remain pending while the worker is creating.
        let committed = async {
            loop {
                let sqlx::types::Json(policy): sqlx::types::Json<AutoLifecyclePolicy> =
                    sqlx::query_scalar(sql!(
                        "SELECT auto_lifecycle_json FROM sandboxes WHERE id = $1"
                    ))
                    .bind(id)
                    .fetch_one(&db.pool)
                    .await?;
                if e2b_lifecycle_config_from_policy(&policy).is_some_and(|p| p.timeout_secs == 120)
                {
                    return Ok::<(), sqlx::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        tokio::select! {
            result = &mut operation => panic!("{outcome}: completed before activation: {result:?}"),
            result = committed => result?,
        }
        // This short real-time bound exercises the network observation loop;
        // simulated time cannot reliably drive the live HTTP/2 connection.
        assert!(
            tokio::time::timeout(Duration::from_millis(120), &mut operation)
                .await
                .is_err(),
            "{outcome}: timeout must not reset a creating execution"
        );
        assert_eq!(
            worker.state().get(&key).await?.access_extended_until,
            Some(floor)
        );
        match outcome {
            "ready" => {
                worker.state().set_status(&key, status::RUNNING).await?;
            }
            "superseded" => {
                sqlx::query(sql!("UPDATE sandbox_direct_route_identities SET execution_generation = 8 WHERE sandbox_id = $1"))
                    .bind(id).execute(&db.pool).await?;
            }
            "expired" => {}
            _ => unreachable!(),
        }
        let result = operation.await;
        if outcome == "ready" {
            assert!(result.is_ok(), "{result:?}");
        } else {
            assert_eq!(
                result.expect_err("activation cannot accept reset").status(),
                if outcome == "superseded" {
                    StatusCode::PRECONDITION_FAILED
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
        }
        let runtime = worker.state().get(&key).await?;
        assert_eq!(
            runtime.execution_generation,
            Some(7),
            "{outcome}: never replace execution"
        );
        assert_eq!(
            runtime.status,
            if outcome == "ready" {
                status::RUNNING
            } else {
                status::CREATING
            }
        );
        assert_eq!(
            runtime.access_extended_until,
            if outcome == "ready" {
                None
            } else {
                Some(floor)
            }
        );
    }
    Ok(())
}
