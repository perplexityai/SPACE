mod prefetch_catalog;
mod reconcile_claim_origins;
mod substrate_scope;
#[cfg(feature = "kubernetes")]
mod template_node_identity;
use pplx_space_common_core::types::{LocalArtifactId, NodeId, OrgId, SnapshotId, UserId};

use super::metadata::*;
use super::*;

use crate::proto::{
    DataPlaneReconnectReason, DataPlaneRevocationStatus, PublishedSandboxState,
    SandboxServiceRouteCredentialKind, SandboxServiceRouteCredentialProof, SandboxServiceRouteKind,
    SandboxServiceRouteTransport,
};

use pplx_space_common_core::{
    sql,
    types::{EcwCredentialEpoch, ExecutionGeneration, GuestPort, SandboxId},
};
use pplx_space_common_grpc::SandboxServiceRouteNotReady;
use pplx_space_common_svc::ecw_credential::{EcwCredentialKeyring, EcwCredentialKind};
use pplx_space_common_svc::space_api_route::SpaceApiRoutePurpose;
use pplx_space_common_svc::space_api_route_ed25519::{
    SPACE_API_ROUTE_ED25519_ISSUER_ENV, SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
    SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV, SpaceApiRouteEd25519Authority,
    SpaceApiRouteEd25519Claims, SpaceApiRouteEd25519Token,
    decode_unverified_space_api_route_ed25519_claims,
};
use pplx_space_db_test_fixtures::{
    SEED_ORG_ID as SEEDED_ORG_ID, SEED_USER_ID as SEEDED_USER_ID, TestDb, insert_ready_worker_node,
    try_pg_test_db_with_max_connections,
};
use pplx_space_node_api_sim::SimWorkerHarness;
use sha2::{Digest, Sha256};
use tonic::Code;
use uuid::Uuid;

use pplx_space_test_env::TestResult;

const TEST_ECW_CREDENTIAL_KEY: &[u8] = b"internal-api-ecw-credential-test-key";

fn test_ecw_credential_keyring() -> EcwCredentialKeyring {
    EcwCredentialKeyring::from_current_key_for_test(TEST_ECW_CREDENTIAL_KEY)
}

pub(crate) fn test_route_ed25519_authority() -> std::sync::Arc<SpaceApiRouteEd25519Authority> {
    {
        let mut env = pplx_space_test_env::TestEnv::default();
        env.set("SPACE_CLUSTER_ID", "testing");
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_PATH_ENV,
            pplx_space_common_svc::space_api_route_ed25519::test_signing_key_path()
                .to_str()
                .unwrap(),
        );
        env.set(
            SPACE_API_ROUTE_ED25519_SIGNING_KEY_ID_ENV,
            "internal-api-test-key",
        );
        env.set(SPACE_API_ROUTE_ED25519_ISSUER_ENV, "cluster-api");
        std::sync::Arc::new(
            SpaceApiRouteEd25519Authority::load_required_from_environment(&env).unwrap(),
        )
    }
}

fn verify_proto_route_token(
    token: &pplx_space_node_api_client::proto::SpaceApiRouteToken,
) -> TestResult<SpaceApiRouteEd25519Claims> {
    Ok(decode_unverified_space_api_route_ed25519_claims(
        &SpaceApiRouteEd25519Token::parse(token.token.clone())?,
    )?)
}

struct ClusterFixture {
    service: ClusterApiService,
    _db: TestDb,
}

#[tokio::test]
async fn transfer_registry_covers_kubernetes_sources() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    for (managed_by, role, address, expected_endpoint) in [
        (
            "space",
            ProtoNodeRole::Worker,
            Some("192.0.2.10"),
            "https://direct-worker:9802",
        ),
        (
            "space",
            ProtoNodeRole::Builder,
            Some("192.0.2.11"),
            "direct-builder:9801",
        ),
        (
            "k8s",
            ProtoNodeRole::Worker,
            Some("192.0.2.12"),
            "https://192.0.2.12:9800",
        ),
        (
            "k8s",
            ProtoNodeRole::Builder,
            Some("2001:db8::13"),
            "https://[2001:db8::13]:9800",
        ),
        ("k8s", ProtoNodeRole::Worker, None, ""),
    ] {
        let id = NodeId::new();
        let role_label = if role == ProtoNodeRole::Worker {
            "worker"
        } else {
            "builder"
        };
        sqlx::query(sql!("INSERT INTO nodes (id, address, role, managed_by, state) VALUES ($1, $2::INET, $3, $4, 'ready')"))
            .bind(id).bind(address).bind(role_label).bind(managed_by).execute(pool).await?;
        if managed_by == "space" {
            insert_data_plane_direct_node_endpoint(pool, id, expected_endpoint).await?;
        } else {
            sqlx::query(sql!(
                "INSERT INTO node_k8s (node_id, k8s_node_name) VALUES ($1, $2)"
            ))
            .bind(id)
            .bind(id.to_string())
            .execute(pool)
            .await?;
        }
        let list = |role, include_deleted| {
            fixture.service.list_nodes(Request::new(ListNodesRequest {
                role,
                include_deleted,
            }))
        };
        let response = list(None, false).await?.into_inner();
        let node = response
            .nodes
            .iter()
            .find(|node| node.id == id.to_string())
            .expect("registered source must be discoverable for transfer admission");
        assert_eq!(node.node_api_endpoint, expected_endpoint);
        assert_eq!(node.role, role as i32);
        assert_eq!(node.managed_by, managed_by);
        assert!(node.deleted_at.is_none());
        let opposite = if role == ProtoNodeRole::Worker {
            ProtoNodeRole::Builder
        } else {
            ProtoNodeRole::Worker
        };
        assert!(
            list(Some(opposite as i32), false)
                .await?
                .into_inner()
                .nodes
                .iter()
                .all(|node| node.id != id.to_string())
        );

        // The direct-node lookup stays direct-only; Kubernetes inventory is
        // merged in by ListNodes, not by this helper.
        let direct = fixture.service.fetch_nodes(None, false).await?;
        assert_eq!(
            direct.iter().any(|node| node.id == id.to_string()),
            managed_by == "space"
        );
        if managed_by == "space" {
            sqlx::query(sql!(
                "UPDATE node_direct SET deleted_at = NOW() WHERE node_id = $1"
            ))
            .bind(id)
            .execute(pool)
            .await?;
        } else {
            sqlx::query(sql!(
                "INSERT INTO node_k8s_tombstones (node_id, k8s_node_name, role) VALUES ($1, $2, $3)"
            ))
            .bind(id)
            .bind(id.to_string())
            .bind(role_label)
            .execute(pool)
            .await?;
        }
        assert!(
            list(None, false)
                .await?
                .into_inner()
                .nodes
                .iter()
                .all(|node| node.id != id.to_string())
        );
        let deleted = list(Some(role as i32), true)
            .await?
            .into_inner()
            .nodes
            .into_iter()
            .find(|node| node.id == id.to_string())
            .expect("include_deleted retains evidence");
        assert!(deleted.deleted_at.is_some());
        if managed_by == "k8s" {
            sqlx::query(sql!("DELETE FROM node_k8s_tombstones WHERE node_id = $1"))
                .bind(id)
                .execute(pool)
                .await?;
            sqlx::query(sql!("UPDATE nodes SET state = 'deleted' WHERE id = $1"))
                .bind(id)
                .execute(pool)
                .await?;
            assert!(
                list(None, false)
                    .await?
                    .into_inner()
                    .nodes
                    .iter()
                    .all(|node| node.id != id.to_string())
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn runtime_recipe_authenticates_empty_nodes_without_inventing_policy() -> TestResult {
    use pplx_space_node_api_client::proto::GetNodeRuntimeDesiredStateRequest;
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node = NodeId::new();
    insert_data_plane_worker_node(&fixture.service.pool, node, "192.0.2.20").await?;
    let message = GetNodeRuntimeDesiredStateRequest {
        node_id: node.to_string(),
        role: Some(ProtoNodeRole::Worker as i32),
    };
    let error = fixture
        .service
        .get_node_runtime_desired_state(Request::new(message.clone()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let error = fixture
        .service
        .get_node_runtime_desired_state(request_from_node(message.clone(), "192.0.2.21".parse()?))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
    let response = fixture
        .service
        .get_node_runtime_desired_state(request_from_node(message, "192.0.2.20".parse()?))
        .await?
        .into_inner();
    assert!(response.recipe.is_none());
    Ok(())
}

async fn cluster_fixture() -> TestResult<Option<ClusterFixture>> {
    cluster_fixture_with_max_connections(2).await
}

async fn cluster_fixture_with_max_connections(
    max_connections: u32,
) -> TestResult<Option<ClusterFixture>> {
    let Some(db) = try_pg_test_db_with_max_connections(max_connections).await? else {
        return Ok(None);
    };
    let pool = db.pool.clone();
    let service = ClusterApiService::with_security_state(
        pool,
        Some(test_route_ed25519_authority()),
        std::sync::Arc::new(test_ecw_credential_keyring()),
    )
    // Disabled by default, because `with_security_state` builds the cache from
    // `from_env()` and a test that inherits it inherits the developer's shell.
    // `SPACE_INTERNAL_API_VISIBILITY_CACHE_TTL_SECS` is a real variable now — it
    // is set in the `testing` and `experimental` overlays — so exporting it while
    // debugging one thing would silently change the behavior of every RPC test
    // that takes a successful visibility read before injecting a break. Tests
    // that want a live cache ask for one with `with_visibility_cache`.
    .with_template_visibility_cache(disabled_visibility_cache());

    Ok(Some(ClusterFixture { service, _db: db }))
}

/// Rebuild a fixture around a different visibility cache. Purely so its call
/// sites read as one line each.
///
/// `cluster_fixture` already installs a disabled cache, so passing
/// `disabled_visibility_cache()` here is emphasis at the call site, not a
/// requirement — worth knowing before concluding from one of those call sites
/// that the default is something else.
fn with_visibility_cache(
    fixture: ClusterFixture,
    cache: crate::template_visibility_cache::TemplateVisibilityCache,
) -> ClusterFixture {
    ClusterFixture {
        service: fixture.service.with_template_visibility_cache(cache),
        _db: fixture._db,
    }
}

/// A visibility cache with both windows shut, i.e. pre-cache behavior: every
/// lookup queries, and every failed query degrades immediately.
fn disabled_visibility_cache() -> crate::template_visibility_cache::TemplateVisibilityCache {
    visibility_cache(0, 0)
}

/// A visibility cache with the two windows set independently, in seconds. Each
/// test opens exactly one of them, which is what lets an answer be attributed to
/// a specific path from outside the module: with `ttl = 0` a cached answer can
/// only have come from the stale fallback, and with `stale_ceiling = 0` it can
/// only have come from the fresh window.
fn visibility_cache(
    ttl_secs: u64,
    stale_ceiling_secs: u64,
) -> crate::template_visibility_cache::TemplateVisibilityCache {
    crate::template_visibility_cache::TemplateVisibilityCache::new(
        crate::template_visibility_cache::CacheConfig {
            ttl: std::time::Duration::from_secs(ttl_secs),
            stale_ceiling: std::time::Duration::from_secs(stale_ceiling_secs),
            max_entries: std::num::NonZeroUsize::new(1024).expect("1024 is non-zero"),
        },
    )
}

async fn sim_worker_node_api(node_id: NodeId) -> TestResult<SimWorkerHarness> {
    Ok(SimWorkerHarness::builder(node_id.to_string())
        .start()
        .await?)
}

pub(super) fn publisher_request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::build::BuildAuthenticatedCaller::trusted_by_configuration(),
    );
    request
}

/// A request as the data-plane edge sends it: the auth layer has attached the
/// guest-bridge caller's authorities.
fn data_plane_request<T>(body: T) -> Request<T> {
    let mut request = Request::new(body);
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::data_plane_edge_for_test(),
    );
    request
}

async fn register_template_with_org(
    service: &ClusterApiService,
    name: &str,
    org_id: Option<OrgId>,
) -> Result<(), tonic::Status> {
    service
        .register_template(publisher_request(RegisterTemplateRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            name: name.to_string(),
            description: None,
            org_id: org_id.map(|org_id| org_id.to_string()),
        }))
        .await?;
    Ok(())
}

async fn register_template(service: &ClusterApiService, name: &str) -> Result<(), tonic::Status> {
    register_template_with_org(service, name, None).await
}

async fn template_org_id(
    service: &ClusterApiService,
    name: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar::<_, Option<Uuid>>("SELECT org_id FROM templates WHERE name = $1")
        .bind(name)
        .fetch_one(&service.pool)
        .await
}

async fn register_release(
    service: &ClusterApiService,
    hash: &str,
    tags: &[&str],
    storage_connection_strings: &[&str],
) -> Result<RegisterTemplateReleaseResponse, tonic::Status> {
    Ok(service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "fc".to_string(),
            hash: hash.to_string(),
            tags: tags.iter().map(|tag| (*tag).to_string()).collect(),
            storage_connection_strings: storage_connection_strings
                .iter()
                .map(|uri| (*uri).to_string())
                .collect(),
            org_id: None,
            execution: None,
        }))
        .await?
        .into_inner())
}

async fn register_release_with_execution(
    service: &ClusterApiService,
    hash: &str,
    execution: &pplx_space_common_core::types::ExecutionMetadata,
) -> Result<RegisterTemplateReleaseResponse, tonic::Status> {
    Ok(service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "fc".to_string(),
            hash: hash.to_string(),
            tags: vec![hash.to_string()],
            storage_connection_strings: Vec::new(),
            org_id: None,
            execution: Some(execution.into()),
        }))
        .await?
        .into_inner())
}

struct PrivateBaseFixture {
    cluster: ClusterFixture,
    owner: Uuid,
    hash: String,
}

#[rstest::fixture]
async fn private_base_fixture() -> TestResult<PrivateBaseFixture> {
    let fixture = with_visibility_cache(
        cluster_fixture().await?.expect("Postgres required"),
        visibility_cache(60, 60),
    );
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let hash = format!("sha256:{}", "a".repeat(64));
    register_template_with_org(
        &fixture.service,
        "private-base",
        Some(OrgId::from_uuid(owner)),
    )
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!("INSERT INTO template_releases (id, template_name, type, hash, org_id) VALUES ($1, 'private-base', 'fc', $2, $3)"))
        .bind(release_id).bind(&hash).bind(owner).execute(&fixture.service.pool).await?;
    sqlx::query(sql!("INSERT INTO template_release_tags (template_name, template_release_id, tag) VALUES ('private-base', $1, 'stable')"))
        .bind(release_id).execute(&fixture.service.pool).await?;
    sqlx::query(sql!("INSERT INTO template_release_storage_locations (template_release_id, storage_connection_string, org_id) VALUES ($1, 's3://private/rootfs?region=us-east-2', $2)"))
        .bind(release_id).bind(owner).execute(&fixture.service.pool).await?;

    Ok(PrivateBaseFixture {
        cluster: fixture,
        owner,
        hash,
    })
}

#[rstest::rstest]
#[tokio::test(flavor = "current_thread")]
async fn scoped_release_lookup_hides_private_bases(
    #[future] private_base_fixture: TestResult<PrivateBaseFixture>,
) -> TestResult {
    let PrivateBaseFixture {
        cluster: fixture,
        owner,
        hash,
    } = private_base_fixture.await?;
    let stranger = SEEDED_ORG_ID.to_string();

    for by_hash in [false, true] {
        let lookup = |name: &str, org: Option<String>| {
            let service = fixture.service.clone();
            let name = name.to_string();
            let hash = hash.clone();
            async move {
                if by_hash {
                    service
                        .find_template_release_by_hash(Request::new(
                            FindTemplateReleaseByHashRequest {
                                generic_only: false,
                                template_name: name,
                                hash,
                                caller_org_id: org,
                            },
                        ))
                        .await
                } else {
                    service
                        .find_template_release(Request::new(FindTemplateReleaseRequest {
                            generic_only: false,
                            template_name: name,
                            runtime: "firecracker".into(),
                            tag: "stable".into(),
                            caller_org_id: org,
                        }))
                        .await
                }
            }
        };
        sqlx::query(sql!(
            "UPDATE templates SET visibility = 'generic' WHERE name = 'private-base'"
        ))
        .execute(&fixture.service.pool)
        .await?;
        let shared = lookup("private-base", Some(stranger.clone()))
            .await?
            .into_inner();
        assert_eq!(shared.authorized_org_id, Some(stranger.clone()));
        assert_eq!(shared.template_visibility(), TemplateVisibility::Generic);
        sqlx::query(sql!(
            "UPDATE templates SET visibility = 'org_specific' WHERE name = 'private-base'"
        ))
        .execute(&fixture.service.pool)
        .await?;
        let denied = lookup("private-base", Some(stranger.clone()))
            .await
            .expect_err("private base must be hidden");
        let absent = lookup("nonexistent", Some(stranger.clone()))
            .await
            .expect_err("missing base");
        assert_eq!(denied.code(), Code::NotFound);
        assert_eq!(
            (denied.code(), denied.message()),
            (absent.code(), absent.message())
        );
        let allowed = lookup("private-base", Some(owner.to_string()))
            .await?
            .into_inner();
        assert_eq!(allowed.authorized_org_id, Some(owner.to_string()));
        assert_eq!(allowed.release.expect("authorized release").hash, hash);

        let legacy = lookup("private-base", None).await?.into_inner();
        assert_eq!(legacy.authorized_org_id, None);
        assert_eq!(legacy.release.expect("legacy release").hash, hash);
    }
    Ok(())
}

fn port_state(
    sandbox_id: &str,
    protocol: &str,
    guest_port: u32,
    apply_state: &str,
    error: Option<&str>,
) -> crate::proto::SandboxPortExposureApplyState {
    crate::proto::SandboxPortExposureApplyState {
        sandbox_id: sandbox_id.to_string(),
        protocol: port_protocol_to_proto(protocol.trim()) as i32,
        guest_port,
        apply_state: port_apply_state_to_proto(apply_state.trim()) as i32,
        error: error.map(str::to_string),
        bridge_status: None,
        service_generation: None,
    }
}

#[test]
fn parse_port_exposure_apply_states_accepts_valid_batch() {
    let sandbox_id =
        SandboxId::from_uuid(Uuid::parse_str("019d84bd-280d-7331-99c6-b63377f3a3a2").unwrap());

    let parsed = parse_port_exposure_apply_states(vec![
        port_state(&sandbox_id.to_string(), "tcp", 8080, "applied", None),
        port_state(
            "019d84bd-280d-7331-99c6-b63377f3a3a3",
            " tcp ",
            8081,
            " failed ",
            Some(" apply failed "),
        ),
        port_state(
            "019d84bd-280d-7331-99c6-b63377f3a3a4",
            "tcp",
            8082,
            "failed",
            Some("  "),
        ),
    ])
    .unwrap();

    assert_eq!(parsed.len(), 3);
    assert_eq!(parsed[0].sandbox_id, sandbox_id);
    assert_eq!(parsed[0].protocol, "tcp");
    assert_eq!(parsed[0].guest_port, 8080);
    assert_eq!(parsed[0].apply_state, "applied");
    assert_eq!(parsed[0].error, None);
    assert_eq!(parsed[1].protocol, "tcp");
    assert_eq!(parsed[1].apply_state, "failed");
    assert_eq!(parsed[1].error.as_deref(), Some("apply failed"));
    assert_eq!(parsed[2].error, None);
}

#[test]
fn parse_port_exposure_apply_states_rejects_duplicates() {
    let sandbox_id = "019d84bd-280d-7331-99c6-b63377f3a3a2";
    let err = parse_port_exposure_apply_states(vec![
        port_state(sandbox_id, "tcp", 8080, "applied", None),
        port_state(sandbox_id, "tcp", 8080, "failed", Some("later")),
    ])
    .unwrap_err();

    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("duplicate sandbox port exposure"));
}

#[test]
fn parse_port_exposure_apply_states_rejects_invalid_values() {
    for state in [
        port_state("not-a-sandbox-id", "tcp", 8080, "applied", None),
        port_state(
            "019d84bd-280d-7331-99c6-b63377f3a3a2",
            "udp",
            8080,
            "applied",
            None,
        ),
        port_state(
            "019d84bd-280d-7331-99c6-b63377f3a3a2",
            "tcp",
            0,
            "applied",
            None,
        ),
        port_state(
            "019d84bd-280d-7331-99c6-b63377f3a3a2",
            "tcp",
            8080,
            "open",
            None,
        ),
    ] {
        let err = parse_port_exposure_apply_states(vec![state]).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_without_org_keeps_system_template_unowned() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let template_name = format!("system-template-{}", Uuid::now_v7());

    register_template(&fixture.service, &template_name).await?;
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        None
    );

    register_template(&fixture.service, &template_name).await?;
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        None
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_register_template_is_idempotent() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(8).await? else {
        return Ok(());
    };
    let template_name = format!("concurrent-template-{}", Uuid::now_v7());
    let org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let service = fixture.service.clone();
        let template_name = template_name.clone();
        handles.push(tokio::spawn(async move {
            register_template_with_org(&service, &template_name, Some(OrgId::from_uuid(org_id)))
                .await
        }));
    }

    for handle in handles {
        handle.await??;
    }
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        Some(org_id)
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_with_org_cannot_adopt_unowned_template() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let template_name = format!("adopt-template-{}", Uuid::now_v7());
    let tenant_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;

    register_template(&fixture.service, &template_name).await?;
    let error = register_template_with_org(
        &fixture.service,
        &template_name,
        Some(OrgId::from_uuid(tenant_org_id)),
    )
    .await
    .expect_err("tenant cannot adopt an unowned template");
    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        None
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_without_org_preserves_existing_owner() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let template_name = format!("owned-template-{}", Uuid::now_v7());
    let tenant_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;

    register_template_with_org(
        &fixture.service,
        &template_name,
        Some(OrgId::from_uuid(tenant_org_id)),
    )
    .await?;
    let error = register_template(&fixture.service, &template_name)
        .await
        .expect_err("omitting the org cannot bypass existing ownership");
    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        Some(tenant_org_id)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_does_not_reactivate_deleting_or_deleted_template() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;

    for desired_state in ["deleting", "deleted"] {
        let template_name = format!("{desired_state}-template-{}", Uuid::now_v7());
        register_template(&fixture.service, &template_name).await?;
        sqlx::query(
            "UPDATE templates
             SET desired_state = $2,
                 delete_requested_at = NOW()
             WHERE name = $1",
        )
        .bind(&template_name)
        .bind(desired_state)
        .execute(&fixture.service.pool)
        .await?;

        let error = register_template_with_org(
            &fixture.service,
            &template_name,
            Some(OrgId::from_uuid(org_id)),
        )
        .await
        .expect_err("registration must not reactivate a template being deleted");
        assert_eq!(error.code(), Code::PermissionDenied);

        let (actual_state, delete_requested_at_preserved): (String, bool) = sqlx::query_as(
            "SELECT desired_state,
                    delete_requested_at IS NOT NULL
             FROM templates
             WHERE name = $1",
        )
        .bind(&template_name)
        .fetch_one(&fixture.service.pool)
        .await?;
        assert_eq!(actual_state, desired_state);
        assert!(delete_requested_at_preserved);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_with_different_org_rejects_existing_owner() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let template_name = format!("foreign-template-{}", Uuid::now_v7());
    let original_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002")?;
    let tenant_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;

    register_template_with_org(
        &fixture.service,
        &template_name,
        Some(OrgId::from_uuid(original_org_id)),
    )
    .await?;
    let error = register_template_with_org(
        &fixture.service,
        &template_name,
        Some(OrgId::from_uuid(tenant_org_id)),
    )
    .await
    .expect_err("different explicit org must be rejected");

    assert_eq!(error.code(), Code::PermissionDenied);
    assert_eq!(
        template_org_id(&fixture.service, &template_name).await?,
        Some(original_org_id)
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn build_release_keeps_the_submitting_org_and_rejects_another_owner() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000002")?;
    let other = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let name = format!("owned-build-{}", Uuid::now_v7());
    register_template_with_org(&fixture.service, &name, Some(OrgId::from_uuid(owner))).await?;
    let request = RegisterTemplateReleaseRequest {
        system_publication_grant_id: None,
        tenant_publication_grant_id: None,
        template_name: name.clone(),
        runtime: "fc".into(),
        hash: format!("sha256:{}", "a".repeat(64)),
        tags: vec!["latest".into()],
        storage_connection_strings: vec![],
        org_id: Some(owner.to_string()),
        execution: None,
    };

    fixture
        .service
        .register_template_release(publisher_request(request.clone()))
        .await?;
    let rejected = fixture
        .service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            org_id: Some(other.to_string()),
            ..request
        }))
        .await;

    assert_eq!(rejected.unwrap_err().code(), Code::PermissionDenied);
    let (template_org, release_org): (Option<Uuid>, Option<Uuid>) = sqlx::query_as(sql!(
        "SELECT t.org_id, r.org_id FROM templates t JOIN template_releases r ON r.template_name = t.name WHERE t.name = $1"
    )).bind(&name).fetch_one(&fixture.service.pool).await?;
    assert_eq!((template_org, release_org), (Some(owner), Some(owner)));
    Ok(())
}

/// A release registered before the `sha256:` prefix was required is stored
/// bare. Registering the same digest in canonical form, as build-manager does
/// when it republishes tags for an already published release, must attach the
/// tags to that row rather than create a second, artifact-less release beside
/// it and move the tags there.
#[tokio::test(flavor = "current_thread")]
async fn register_template_release_reuses_a_bare_hash_release() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    register_template(&fixture.service, "code-runtime").await?;
    let bare = "c".repeat(64);
    let canonical = format!("sha256:{bare}");
    let storage = format!("s3://bucket/templates/code-runtime/{canonical}?region=us-east-1");
    let legacy_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash)
         VALUES ($1, 'code-runtime', 'fc', $2)"
    ))
    .bind(legacy_id)
    .bind(&bare)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_name, template_release_id, tag)
         VALUES ('code-runtime', $1, 'stable')"
    ))
    .bind(legacy_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string)
         VALUES ($1, $2)"
    ))
    .bind(legacy_id)
    .bind(&storage)
    .execute(pool)
    .await?;

    // A tags-only republication of the canonical spelling.
    let republished = register_release(&fixture.service, &canonical, &["latest"], &[]).await?;
    assert_eq!(
        republished.release.expect("release").id,
        legacy_id.to_string()
    );
    let releases: i64 = sqlx::query_scalar(sql!(
        "SELECT COUNT(*) FROM template_releases WHERE template_name = 'code-runtime'"
    ))
    .fetch_one(pool)
    .await?;
    assert_eq!(
        releases, 1,
        "no second release may appear beside the bare row"
    );

    let latest = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(latest.release.expect("latest").id, legacy_id.to_string());
    assert_eq!(latest.storage_locations.len(), 1);
    assert_eq!(latest.storage_locations[0].connection_string, storage);
    Ok(())
}

/// When a digest already exists under both spellings, registration must land
/// on the row that owns the recorded artifacts, whichever spelling and
/// whichever age that is. Tags attached to the artifact-less duplicate would
/// resolve to a release nothing can pull.
#[tokio::test(flavor = "current_thread")]
async fn register_template_release_prefers_the_duplicate_that_owns_a_location() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    register_template(&fixture.service, "code-runtime").await?;

    // The older row is always the bare one; the location sits on the older
    // row in one arrangement and on the newer canonical row in the other.
    for (digit, location_on_older) in [('d', false), ('e', true)] {
        let bare = digit.to_string().repeat(64);
        let canonical = format!("sha256:{bare}");
        let storage = format!("s3://bucket/templates/code-runtime/{canonical}?region=us-east-1");
        let older = Uuid::now_v7();
        let newer = Uuid::now_v7();
        sqlx::query(sql!(
            "INSERT INTO template_releases (id, template_name, type, hash, created_at)
             VALUES ($1, 'code-runtime', 'fc', $2, NOW() - INTERVAL '1 day'),
                    ($3, 'code-runtime', 'fc', $4, NOW())"
        ))
        .bind(older)
        .bind(&bare)
        .bind(newer)
        .bind(&canonical)
        .execute(pool)
        .await?;
        let owner = if location_on_older { older } else { newer };
        sqlx::query(sql!(
            "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string)
             VALUES ($1, $2)"
        ))
        .bind(owner)
        .bind(&storage)
        .execute(pool)
        .await?;

        let tag = format!("release-{digit}");
        let republished =
            register_release(&fixture.service, &canonical, &[tag.as_str()], &[]).await?;
        assert_eq!(
            republished.release.expect("release").id,
            owner.to_string(),
            "registration must pick the duplicate that owns the artifacts"
        );
        let resolved = fixture
            .service
            .find_template_release(Request::new(FindTemplateReleaseRequest {
                generic_only: false,
                caller_org_id: None,
                template_name: "code-runtime".to_string(),
                runtime: "firecracker".to_string(),
                tag,
            }))
            .await?
            .into_inner();
        assert_eq!(resolved.release.expect("tagged").id, owner.to_string());
        assert_eq!(resolved.storage_locations.len(), 1);
        assert_eq!(resolved.storage_locations[0].connection_string, storage);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_release_is_idempotent_by_hash() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let digest_a = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let digest_b = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let storage_a = "s3://bucket/templates/code-runtime/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?region=us-east-1";
    let storage_a_replica = "s3://replica-bucket/templates/code-runtime/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?region=us-west-2";
    let storage_b = "s3://bucket/templates/code-runtime/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb?region=us-east-1";

    let first = register_release(&fixture.service, digest_a, &[digest_a], &[storage_a]).await?;
    let second = register_release(
        &fixture.service,
        digest_a,
        &[digest_a, digest_a],
        &[storage_a, storage_a_replica, storage_a_replica],
    )
    .await?;

    let first_release = first.release.expect("first release");
    let second_release = second.release.expect("second release");
    assert_eq!(first_release.id, second_release.id);
    assert_eq!(second.tags.len(), 1);
    assert_eq!(second.tags[0].tag, digest_a);
    let second_storage_locations = second
        .storage_locations
        .iter()
        .map(|location| location.connection_string.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(second_storage_locations.len(), 2);
    assert!(second_storage_locations.contains(storage_a));
    assert!(second_storage_locations.contains(storage_a_replica));

    let by_hash = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: digest_a.to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        by_hash.release.expect("release by hash").id,
        first_release.id
    );
    let by_hash_storage_locations = by_hash
        .storage_locations
        .iter()
        .map(|location| location.connection_string.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(by_hash_storage_locations.len(), 2);
    assert!(by_hash_storage_locations.contains(storage_a));
    assert!(by_hash_storage_locations.contains(storage_a_replica));

    let by_tag = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: digest_a.to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(by_tag.release.expect("release by tag").id, first_release.id);

    let release_b = register_release(
        &fixture.service,
        digest_b,
        &[digest_b, "latest"],
        &[storage_b],
    )
    .await?;
    assert_eq!(release_b.tags.len(), 2);

    let retagged = fixture
        .service
        .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
            system_publication_grant_id: None,
            tenant_publication_grant_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "fc".to_string(),
            hash: digest_a.to_string(),
            tags: vec!["latest".to_string()],
            storage_connection_strings: vec![storage_a.to_string()],
            org_id: None,
            execution: None,
        }))
        .await
        .expect("retag should succeed")
        .into_inner();
    assert_eq!(
        retagged.release.expect("retagged release").id,
        first_release.id
    );
    assert_eq!(retagged.tags.len(), 1);
    assert_eq!(retagged.tags[0].tag, "latest");

    let latest = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        latest.release.expect("release by latest").id,
        first_release.id
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn storage_policy_rejects_registration_without_changing_release_or_tags() -> TestResult {
    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    fixture.service.trusted_template_storage =
        trusted_storage::TrustedTemplateStorageBuckets::from_buckets(["trusted-templates"]);
    register_template(&fixture.service, "code-runtime").await?;
    let original_hash = format!("sha256:{}", "a".repeat(64));
    let rejected_hash = format!("sha256:{}", "b".repeat(64));
    let trusted = "s3://trusted-templates/templates/original/?region=us-east-1&path_style=false";
    let original =
        register_release(&fixture.service, &original_hash, &["latest"], &[trusted]).await?;
    let original_id = original.release.unwrap().id;

    for storage in [
        "s3://attacker-bucket/templates/x?region=us-east-1",
        "s3://trusted-templates-evil/templates/x?region=us-east-1",
        "s3://trusted-templates/templates/x?region=us-east-1&endpoint=https://user-secret:password-secret@attacker.invalid/path?token=token-secret",
        "s3://trusted-templates/templates/x?region=us-east-1&endpoint=",
        "s3://trusted-templates/templates/x?region=us-east-1&invalid=token-secret",
    ] {
        let rejected = register_release(
            &fixture.service,
            &rejected_hash,
            &["latest"],
            &[trusted, storage],
        )
        .await
        .expect_err("untrusted storage must reject the entire registration");

        assert_eq!(rejected.code(), Code::InvalidArgument);
        for private_value in [
            storage,
            "attacker-bucket",
            "user-secret",
            "password-secret",
            "token-secret",
        ] {
            assert!(!rejected.message().contains(private_value));
        }
        let hashes: Vec<String> = sqlx::query_scalar(sql!(
            "SELECT hash FROM template_releases WHERE template_name = 'code-runtime'"
        ))
        .fetch_all(&fixture.service.pool)
        .await?;
        assert_eq!(hashes, std::slice::from_ref(&original_hash));
        let latest = fixture
            .service
            .find_template_release(Request::new(FindTemplateReleaseRequest {
                generic_only: false,
                template_name: "code-runtime".into(),
                runtime: "fc".into(),
                tag: "latest".into(),
                caller_org_id: None,
            }))
            .await?
            .into_inner();
        assert_eq!(latest.release.unwrap().id, original_id);
        assert_eq!(latest.storage_locations.len(), 1);
        assert_eq!(latest.storage_locations[0].connection_string, trusted);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn register_template_release_allows_no_storage_locations() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let release = register_release(&fixture.service, digest, &[digest, "latest"], &[]).await?;

    assert!(release.release.is_some());
    assert_eq!(
        release
            .tags
            .iter()
            .map(|tag| tag.tag.as_str())
            .collect::<Vec<_>>(),
        vec![digest, "latest"]
    );
    assert!(release.storage_locations.is_empty());

    let by_hash = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: digest.to_string(),
        }))
        .await?
        .into_inner();
    assert!(by_hash.release.is_some());
    assert!(by_hash.storage_locations.is_empty());

    Ok(())
}

/// Registration is the canonical writer of `template_releases.execution_metadata`,
/// with write-once semantics: the first registration that carries execution
/// metadata wins, later registrations never overwrite it (releases are
/// immutable per `(template_name, hash)`), and a release registered without
/// metadata is backfilled by a later registration that has it.
#[tokio::test(flavor = "current_thread")]
async fn register_template_release_persists_execution_metadata_write_once() -> TestResult {
    use pplx_space_common_core::types::{Arch, Digest, EnvironmentReference, ExecutionMetadata};

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let digest_a = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let digest_b = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let x86 = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![])
        .with_environment(EnvironmentReference::new(Digest::sha256([1; 32])));
    let arm = ExecutionMetadata::initial_firecracker(Arch::Aarch64, vec![]);

    let stored_execution = |hash: &str| {
        let pool = fixture.service.pool.clone();
        let hash = hash.to_string();
        async move {
            sqlx::query_scalar::<_, Option<serde_json::Value>>(sql!(
                "SELECT execution_metadata FROM template_releases
                 WHERE template_name = 'code-runtime' AND hash = $1"
            ))
            .bind(hash)
            .fetch_one(&pool)
            .await
        }
    };

    // First registration with metadata populates the row.
    register_release_with_execution(&fixture.service, digest_a, &x86).await?;
    assert_eq!(
        stored_execution(digest_a).await?,
        Some(serde_json::to_value(&x86)?)
    );

    // Identical retries succeed; conflicting evidence must not be acknowledged.
    register_release_with_execution(&fixture.service, digest_a, &x86).await?;
    let error = register_release_with_execution(&fixture.service, digest_a, &arm)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);
    assert_eq!(
        stored_execution(digest_a).await?,
        Some(serde_json::to_value(&x86)?),
        "release execution metadata is write-once"
    );

    // Removing or replacing a binding cannot publish a new tag as a side effect.
    for changed in [
        ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]),
        x86.clone()
            .with_environment(EnvironmentReference::new(Digest::sha256([2; 32]))),
    ] {
        let error = fixture
            .service
            .register_template_release(publisher_request(RegisterTemplateReleaseRequest {
                system_publication_grant_id: None,
                tenant_publication_grant_id: None,
                template_name: "code-runtime".into(),
                runtime: "fc".into(),
                hash: digest_a.into(),
                tags: vec!["conflicting".into()],
                storage_connection_strings: vec![],
                org_id: None,
                execution: Some((&changed).into()),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::AlreadyExists);
        let count: i64 = sqlx::query_scalar(sql!(
            "SELECT count(*) FROM template_release_tags WHERE template_name = 'code-runtime' AND tag = 'conflicting'"
        )).fetch_one(&fixture.service.pool).await?;
        assert_eq!(count, 0);
    }

    // A release registered without metadata stays NULL...
    register_release(&fixture.service, digest_b, &[digest_b], &[]).await?;
    assert_eq!(stored_execution(digest_b).await?, None);

    // ...and a later registration that carries metadata backfills it.
    register_release_with_execution(&fixture.service, digest_b, &arm).await?;
    assert_eq!(
        stored_execution(digest_b).await?,
        Some(serde_json::to_value(&arm)?)
    );

    let error = register_release_with_execution(&fixture.service, digest_b, &x86)
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::AlreadyExists);

    Ok(())
}

/// A tags-only republication, which build-manager issues when the release is
/// already published or when Credential Manager refused the write for that
/// reason, carries no storage location and no execution metadata, so it adds
/// its tag and leaves the published release's locations and metadata exactly
/// as they were, including a `NULL` metadata column.
#[tokio::test(flavor = "current_thread")]
async fn tags_only_republication_leaves_locations_and_metadata_unchanged() -> TestResult {
    use pplx_space_common_core::types::{Arch, ExecutionMetadata};

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;
    let digest_a = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let digest_b = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let storage_a = "s3://bucket/templates/code-runtime/sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa?region=us-east-1";
    let storage_b = "s3://bucket/templates/code-runtime/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb?region=us-east-1";
    let stored = |hash: &str| {
        let pool = fixture.service.pool.clone();
        let hash = hash.to_string();
        async move {
            sqlx::query_as::<_, (Option<String>, i64)>(sql!(
                "SELECT r.execution_metadata->'guest'->>'architecture',
                        (SELECT COUNT(*) FROM template_release_storage_locations l
                          WHERE l.template_release_id = r.id)
                 FROM template_releases r
                 WHERE r.template_name = 'code-runtime' AND r.hash = $1"
            ))
            .bind(hash)
            .fetch_one(&pool)
            .await
        }
    };

    // A published release with metadata and one location...
    let x86 = ExecutionMetadata::initial_firecracker(Arch::X86_64, vec![]);
    register_release_with_execution(&fixture.service, digest_a, &x86).await?;
    register_release(&fixture.service, digest_a, &["latest"], &[storage_a]).await?;
    // ...and one published before metadata was recorded.
    register_release(&fixture.service, digest_b, &["latest"], &[storage_b]).await?;
    assert_eq!(stored(digest_a).await?, (Some("x86_64".to_string()), 1));
    assert_eq!(stored(digest_b).await?, (None, 1));

    for digest in [digest_a, digest_b] {
        let republished = register_release(&fixture.service, digest, &["stable"], &[]).await?;
        // The response lists what this call registered: the tag, no location.
        assert!(republished.storage_locations.is_empty());
        assert_eq!(
            republished
                .tags
                .iter()
                .map(|tag| tag.tag.as_str())
                .collect::<Vec<_>>(),
            vec!["stable"]
        );
    }
    assert_eq!(stored(digest_a).await?, (Some("x86_64".to_string()), 1));
    assert_eq!(stored(digest_b).await?, (None, 1));
    Ok(())
}

/// The hash validator canonicalizes both a bare 64-hex digest and
/// a `sha256:`-prefixed digest to the same prefixed form, and still
/// rejects non-digest input. No DB required.
#[test]
fn validated_release_hash_tolerates_bare_and_prefixed() {
    let bare = "47c10b6843ac47c10b6843ac47c10b6843ac47c10b6843ac47c10b6843ac47c1";
    let prefixed = format!("sha256:{bare}");

    assert_eq!(validated_release_hash(bare).unwrap(), prefixed);
    assert_eq!(validated_release_hash(&prefixed).unwrap(), prefixed);

    assert_eq!(
        validated_release_hash("not-a-hash").unwrap_err().code(),
        Code::InvalidArgument
    );
    // Wrong length is rejected, not blindly prefixed.
    assert_eq!(
        validated_release_hash("abc123").unwrap_err().code(),
        Code::InvalidArgument
    );
}

/// A digest stored **bare** (no `sha256:` prefix) in
/// `template_releases.hash` must still resolve through
/// `FindTemplateReleaseByHash`. Before tolerant matching, the
/// hash validator rejected a bare digest with `InvalidArgument`
/// before the query ran, so prefetch — which dispatches a pull by
/// the resolved digest — could never warm a bare-hashed release.
#[tokio::test(flavor = "current_thread")]
async fn find_template_release_by_hash_resolves_bare_hash() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let bare = "47c10b6843ac47c10b6843ac47c10b6843ac47c10b6843ac47c10b6843ac47c1";
    let prefixed = format!("sha256:{bare}");
    let storage = format!("s3://bucket/templates/code-runtime/{prefixed}?region=us-east-1");

    // Bypass the register path's canonicalizing validator to exercise legacy
    // release rows containing a bare hash.
    let release_id: Uuid = sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash)
             SELECT 'code-runtime', 'fc', $1
             FROM templates WHERE name = 'code-runtime'
             RETURNING id",
    )
    .bind(bare)
    .fetch_one(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_release_storage_locations
                 (template_release_id, storage_connection_string)
             VALUES ($1, $2)",
    )
    .bind(release_id)
    .bind(&storage)
    .execute(&fixture.service.pool)
    .await?;

    // Querying by the bare digest resolves the row (today this
    // returned InvalidArgument before reaching SQL).
    let by_bare = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: bare.to_string(),
        }))
        .await?
        .into_inner();
    let by_bare_release = by_bare.release.expect("release by bare hash");
    assert_eq!(by_bare_release.id, release_id.to_string());
    assert_eq!(by_bare.storage_locations.len(), 1);
    assert_eq!(by_bare.storage_locations[0].connection_string, storage);

    // The canonical `sha256:`-prefixed form resolves the same
    // bare-stored row (tolerant matching in both directions).
    let by_prefixed = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: prefixed.clone(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        by_prefixed.release.expect("release by prefixed hash").id,
        release_id.to_string()
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_latest_template_releases_orders_and_limits() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    // Register four releases in known insertion order; rely on
    // template_releases.created_at DESC to order them.
    let digests = [
        "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        "sha256:2222222222222222222222222222222222222222222222222222222222222222",
        "sha256:3333333333333333333333333333333333333333333333333333333333333333",
        "sha256:4444444444444444444444444444444444444444444444444444444444444444",
    ];
    let storage_uri = "s3://bucket/templates/code-runtime/?region=us-east-1".to_string();
    for digest in digests {
        register_release(
            &fixture.service,
            digest,
            &[digest],
            std::slice::from_ref(&storage_uri.as_str()),
        )
        .await?;
    }

    // limit=3 → 3 most-recent (digests[3], digests[2], digests[1]).
    let latest = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 3,
        }))
        .await?
        .into_inner();
    assert_eq!(latest.entries.len(), 3);
    assert_eq!(
        latest.entries[0]
            .release
            .as_ref()
            .expect("entry 0 release")
            .hash,
        digests[3]
    );
    assert_eq!(
        latest.entries[1]
            .release
            .as_ref()
            .expect("entry 1 release")
            .hash,
        digests[2]
    );
    assert_eq!(
        latest.entries[2]
            .release
            .as_ref()
            .expect("entry 2 release")
            .hash,
        digests[1]
    );
    for entry in &latest.entries {
        assert_eq!(entry.storage_locations.len(), 1);
        assert_eq!(entry.storage_locations[0].connection_string, storage_uri);
    }

    // Unknown template → empty entries (NOT NotFound). Treating
    // this as a normal resting state keeps the prefetcher quiet
    // for templates that exist cluster-side but have no releases.
    register_template(&fixture.service, "no-releases").await?;
    let empty = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "no-releases".to_string(),
            runtime: "firecracker".to_string(),
            limit: 5,
        }))
        .await?
        .into_inner();
    assert!(empty.entries.is_empty());

    // limit=0 → InvalidArgument.
    let zero = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 0,
        }))
        .await
        .expect_err("limit=0 must be rejected");
    assert_eq!(zero.code(), Code::InvalidArgument);

    // limit > 50 → InvalidArgument.
    let too_big = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 51,
        }))
        .await
        .expect_err("limit=51 must be rejected");
    assert_eq!(too_big.code(), Code::InvalidArgument);

    Ok(())
}

/// `list_warm_pool_template_releases` protects the on-node template GC
/// from evicting builds a warm pool still targets. A pool config whose
/// `enabled = FALSE` (a drained pool for a superseded release) targets
/// nothing, so its release must NOT appear in the protected set —
/// otherwise every drained release pins its build forever and the GC
/// can never reclaim disk from disabled, zero-slot pool configs.
#[tokio::test(flavor = "current_thread")]
async fn list_warm_pool_template_releases_excludes_disabled_pool_configs() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let enabled_hash = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let disabled_hash = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    let storage_uri = "s3://bucket/templates/code-runtime/?region=us-east-1".to_string();

    let enabled_id: Uuid = register_release(
        &fixture.service,
        enabled_hash,
        &["enabled-release"],
        std::slice::from_ref(&storage_uri.as_str()),
    )
    .await?
    .release
    .expect("enabled release")
    .id
    .parse()?;
    let disabled_id: Uuid = register_release(
        &fixture.service,
        disabled_hash,
        &["disabled-release"],
        std::slice::from_ref(&storage_uri.as_str()),
    )
    .await?
    .release
    .expect("disabled release")
    .id
    .parse()?;

    // One enabled pool wants slots; one disabled pool has zero target slots
    // but retains its configuration row.
    for (release_id, enabled, target_ready_slots) in
        [(enabled_id, true, 5), (disabled_id, false, 0)]
    {
        sqlx::query(
            "INSERT INTO kubernetes_pool_configs (
                 template_release_id, vcpu, memory_mib, target_ready_slots,
                 refill_per_tick, max_warming_slots, namespace, enabled, managed_by
             )
             VALUES ($1, 2, 4096, $2, 1, 1, 'space', $3, 'autoscaler')",
        )
        .bind(release_id)
        .bind(target_ready_slots)
        .bind(enabled)
        .execute(&fixture.service.pool)
        .await?;
    }

    let releases = fixture
        .service
        .list_warm_pool_template_releases(Request::new(ListWarmPoolTemplateReleasesRequest {}))
        .await?
        .into_inner()
        .releases;
    let hashes: std::collections::HashSet<String> = releases.into_iter().map(|r| r.hash).collect();

    assert!(
        hashes.contains(enabled_hash),
        "enabled pool config's release must stay protected: {hashes:?}",
    );
    assert!(
        !hashes.contains(disabled_hash),
        "disabled pool config's release must NOT be protected (else its build is never GC'd): {hashes:?}",
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn list_warm_pool_template_releases_protects_republished_retained_release() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;

    let republished_hash =
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
    let current_hash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let storage_uri = "s3://bucket/templates/code-runtime/?region=us-east-1";
    let republished_id: Uuid = register_release(
        &fixture.service,
        republished_hash,
        &[republished_hash],
        &[storage_uri],
    )
    .await?
    .release
    .expect("republished release")
    .id
    .parse()?;
    let current_id: Uuid = register_release(
        &fixture.service,
        current_hash,
        &[current_hash],
        &[storage_uri],
    )
    .await?
    .release
    .expect("current release")
    .id
    .parse()?;
    sqlx::query(
        "UPDATE template_releases
         SET created_at = CASE id
             WHEN $1 THEN NOW() - INTERVAL '2 days'
             ELSE NOW() - INTERVAL '1 day'
         END
         WHERE id IN ($1, $2)",
    )
    .bind(republished_id)
    .bind(current_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "UPDATE template_release_tags
         SET modified_at = NOW() - INTERVAL '1 day'
         WHERE template_release_id = $1",
    )
    .bind(current_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "UPDATE template_release_tags
         SET modified_at = NOW()
         WHERE template_release_id = $1
           AND tag = $2",
    )
    .bind(republished_id)
    .bind(republished_hash)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO warm_pool_policies (
             template_name, runtime, vcpu, memory_mib, min_ready_slots,
             max_ready_slots, retain_releases
         ) VALUES ('code-runtime', 'fc', 2, 4096, 1, 10, 1)",
    )
    .execute(&fixture.service.pool)
    .await?;

    let releases = fixture
        .service
        .list_warm_pool_template_releases(Request::new(ListWarmPoolTemplateReleasesRequest {}))
        .await?
        .into_inner()
        .releases;
    let hashes = releases
        .into_iter()
        .map(|release| release.hash)
        .collect::<std::collections::HashSet<_>>();

    assert_eq!(
        hashes,
        std::collections::HashSet::from([republished_hash.to_string()])
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn base_snapshot_policy_state_prefers_requesting_node_inventory() -> TestResult {
    use std::net::{Ipv4Addr, SocketAddr};
    use tonic::transport::server::TcpConnectInfo;

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "app-runtime").await?;
    let worker_id = Uuid::now_v7();
    let other_worker_id = Uuid::now_v7();
    let worker_addr = Ipv4Addr::new(10, 0, 0, 20);

    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES
                 ($1, $2::INET, 'worker', 'space', 'ready'),
                 ($3, '10.0.0.21'::INET, 'worker', 'space', 'ready')",
    )
    .bind(worker_id)
    .bind(worker_addr.to_string())
    .bind(other_worker_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_base_snapshot_policies (
                 template, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib
             )
             VALUES ('app-runtime', TRUE, 2, 1024)",
    )
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_template_inventory (
                 node_id, template_name, content_hash, runtime, vcpu, memory_mib,
                 observed_at, built_at
             )
             VALUES
                 ($1, 'app-runtime', 'local-old', 'fc', 4, 2048,
                  NOW() - INTERVAL '1 hour', '2026-05-01T00:00:00Z'),
                 ($1, 'app-runtime', 'local-new', 'fc', 8, 4096,
                  NOW(), '2026-05-02T00:00:00Z'),
                 ($2, 'app-runtime', 'other-newer', 'fc', 16, 8192,
                  NOW() + INTERVAL '1 hour', '2026-05-03T00:00:00Z')",
    )
    .bind(worker_id)
    .bind(other_worker_id)
    .execute(&fixture.service.pool)
    .await?;

    let mut request = Request::new(GetNodeTemplateBaseSnapshotPolicyStateRequest {
        node_id: worker_id.to_string(),
        templates: vec!["app-runtime".to_string()],
        role: Some(ProtoNodeRole::Worker as i32),
    });
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(SocketAddr::from((worker_addr, 49000))),
    });

    let response = fixture
        .service
        .get_node_template_base_snapshot_policy_state(request)
        .await?
        .into_inner();

    assert_eq!(response.templates.len(), 1);
    let state = &response.templates[0];
    assert_eq!(state.template, "app-runtime");
    assert!(state.ensure_base_snapshot);
    assert_eq!(state.snapshot_vcpu, 8);
    assert_eq!(state.snapshot_memory_mib, 4096);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn template_desired_state_projects_snapshot_pool_shape() -> TestResult {
    use std::net::{Ipv4Addr, SocketAddr};
    use tonic::transport::server::TcpConnectInfo;

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "seed").await?;
    let worker_id = Uuid::now_v7();
    let worker_addr = Ipv4Addr::new(10, 0, 0, 22);

    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'worker', 'space', 'ready')",
    )
    .bind(worker_id)
    .bind(worker_addr.to_string())
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_base_snapshot_policies (
                 template, ensure_base_snapshot, snapshot_vcpu, snapshot_memory_mib
             )
             VALUES ('seed', TRUE, 1, 512)",
    )
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO template_pool_policies (template, target_count, boot_mode)
             VALUES ('seed', 3, 'snapshot')",
    )
    .execute(&fixture.service.pool)
    .await?;

    let mut request = Request::new(GetNodeTemplateDesiredStateRequest {
        node_id: worker_id.to_string(),
        templates: vec!["seed".to_string()],
        role: Some(ProtoNodeRole::Worker as i32),
    });
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(SocketAddr::from((worker_addr, 49000))),
    });

    let response = fixture
        .service
        .get_node_template_desired_state(request)
        .await?
        .into_inner();

    assert_eq!(response.templates.len(), 1);
    let state = &response.templates[0];
    assert_eq!(state.template, "seed");
    assert_eq!(state.present, Some(true));
    let pool = state.pool.as_ref().expect("snapshot pool policy");
    assert_eq!(pool.target_count, 3);
    assert_eq!(pool.boot_mode, "snapshot");
    assert_eq!(pool.vcpu, Some(1));
    assert_eq!(pool.memory_mib, Some(512));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sandbox_desired_state_uses_active_node_ownership_for_port_exposures() -> TestResult {
    use std::net::{Ipv4Addr, SocketAddr};
    use tonic::transport::server::TcpConnectInfo;

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let node_addr = Ipv4Addr::new(10, 0, 0, 9);
    let other_node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let missing_sandbox_id = SandboxId::new();
    let other_node_sandbox_id = SandboxId::new();
    let pool_reserved_sandbox_id = SandboxId::new();

    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'worker', 'k8s', 'registered')",
    )
    .bind(node_id)
    .bind(node_addr.to_string())
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.10'::INET, 'worker', 'k8s', 'registered')",
    )
    .bind(other_node_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, network_policy_generation
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 'started',
                 'on_node',
                 $2,
                 1
             )",
    )
    .bind(other_node_sandbox_id)
    .bind(other_node_id)
    .execute(&fixture.service.pool)
    .await?;

    // An unclaimed warm-pool slot reserves its sandbox id before any
    // sandboxes row exists; it must never be reported deleted.
    let template_name = format!("pool-tmpl-{}", Uuid::now_v7());
    sqlx::query("INSERT INTO templates (id, name) VALUES ($1, $2)")
        .bind(Uuid::now_v7())
        .bind(&template_name)
        .execute(&fixture.service.pool)
        .await?;
    let template_release_id: Uuid = sqlx::query_scalar(
        "INSERT INTO template_releases (template_name, type, hash)
             VALUES ($1, 'fc', 'testhash') RETURNING id",
    )
    .bind(&template_name)
    .fetch_one(&fixture.service.pool)
    .await?;
    let pool_config_id: Uuid = sqlx::query_scalar(
        "INSERT INTO kubernetes_pool_configs
             (template_release_id, vcpu, memory_mib, target_ready_slots, refill_per_tick,
              max_warming_slots, namespace)
             VALUES ($1, 1, 512, 1, 1, 1, 'sandboxes') RETURNING id",
    )
    .bind(template_release_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO kubernetes_pool_slots
             (config_id, template_release_id, reserved_sandbox_id, template_name, content_hash,
              vcpu, memory_mib, pod_name, pod_namespace, state)
             VALUES ($1, $2, $3, $4, 'testhash', 1, 512, $5, 'sandboxes', 'ready')",
    )
    .bind(pool_config_id)
    .bind(template_release_id)
    .bind(pool_reserved_sandbox_id)
    .bind(&template_name)
    .bind(format!("pool-pod-{pool_reserved_sandbox_id}"))
    .execute(&fixture.service.pool)
    .await?;

    sqlx::query(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_override_json,
                 network_policy_generation
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 'suspended',
                 'unplaced',
                 NULL,
                 $2,
                 $3,
                 1
             )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&fixture.service.pool)
    .await?;

    sqlx::query(
        "INSERT INTO sandbox_port_exposures (
                 sandbox_id, protocol, guest_port, visibility, source,
                 desired_state, apply_state
             )
             VALUES
                 ($1, 'tcp', 9012, 'private', 'api', 'open', 'pending'),
                 ($1, 'tcp', 9013, 'private', 'ecw', 'open', 'pending')",
    )
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;

    let mut request = Request::new(GetNodeSandboxDesiredStateRequest {
        node_id: node_id.to_string(),
        sandbox_ids: vec![
            sandbox_id.to_string(),
            missing_sandbox_id.to_string(),
            other_node_sandbox_id.to_string(),
            pool_reserved_sandbox_id.to_string(),
        ],
        role: Some(ProtoNodeRole::Worker as i32),
    });
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(SocketAddr::from((node_addr, 49000))),
    });

    let response = fixture
        .service
        .get_node_sandbox_desired_state(request)
        .await?
        .into_inner();
    assert!(
        response.sandboxes.is_empty(),
        "sandbox without active_node_id must not produce node-local desired state"
    );
    assert_eq!(
        response.deleted_sandbox_ids,
        vec![missing_sandbox_id.to_string()],
        "only ids with no sandboxes row at all are deleted; unplaced rows, \
         other-node rows, and unclaimed pool-slot reservations must never \
         be reported deleted"
    );

    sqlx::query(
        "UPDATE sandboxes
             SET lifecycle = 'started',
                 placement = 'on_node',
                 active_node_id = $2,
                 snapshots_enabled_desired = TRUE
             WHERE id = $1",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(&fixture.service.pool)
    .await?;

    let mut request = Request::new(GetNodeSandboxDesiredStateRequest {
        node_id: node_id.to_string(),
        sandbox_ids: vec![
            sandbox_id.to_string(),
            missing_sandbox_id.to_string(),
            other_node_sandbox_id.to_string(),
            pool_reserved_sandbox_id.to_string(),
        ],
        role: Some(ProtoNodeRole::Worker as i32),
    });
    request.extensions_mut().insert(TcpConnectInfo {
        local_addr: None,
        remote_addr: Some(SocketAddr::from((node_addr, 49000))),
    });

    let response = fixture
        .service
        .get_node_sandbox_desired_state(request)
        .await?
        .into_inner();
    assert_eq!(response.sandboxes.len(), 1);
    assert_eq!(response.sandboxes[0].port_exposures.len(), 1);
    assert_eq!(response.sandboxes[0].port_exposures[0].guest_port, 9012);
    // The payload columns are conditionally projected; assigned-here rows
    // must still carry their real values, not the NULL arm of the CASE.
    assert!(response.sandboxes[0].snapshots_enabled_desired);
    assert!(response.sandboxes[0].auto_lifecycle.is_some());
    assert_eq!(
        response.deleted_sandbox_ids,
        vec![missing_sandbox_id.to_string()],
    );
    Ok(())
}

/// A tokenless `ResolveDataPlaneSessionRoute` is the data-plane edge's route
/// refresh for a session it already attached. Every other admitted workload
/// must present the session's attach token; otherwise a live session ID alone
/// would mint a route token for another tenant's session.
#[tokio::test(flavor = "current_thread")]
async fn data_plane_route_resolve_without_attach_token_requires_the_edge_identity() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());
    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.11").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.80.0.42", 42).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;
    let tokenless = || ResolveDataPlaneSessionRouteRequest {
        session_id: session_id.clone(),
        protocol_session_id: None,
        token_sha256: None,
        required_scope: None,
        requested_transport: None,
    };

    for (label, caller) in [
        ("no authenticated caller", None),
        (
            "admitted workload without the edge authority",
            Some(
                pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test(),
            ),
        ),
    ] {
        let mut request = Request::new(tokenless());
        if let Some(caller) = caller {
            request.extensions_mut().insert(caller);
        }
        let err = fixture
            .service
            .resolve_data_plane_session_route(request)
            .await
            .expect_err(label);
        assert_eq!(err.code(), Code::Unauthenticated, "{label}");
    }

    let response = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(tokenless()))
        .await?
        .into_inner();
    assert_eq!(response.session_id, session_id);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_route_resolve_hydrates_backend_and_bumps_generation() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let first_node = NodeId::new();
    let second_node = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, first_node, "10.0.0.11").await?;
    insert_data_plane_worker_node(&fixture.service.pool, second_node, "10.0.0.12").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, first_node).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.80.0.42", 42).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;

    let response = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: None,
        }))
        .await?
        .into_inner();

    assert_eq!(response.session_id, session_id);
    assert_eq!(response.current_node_id, first_node.to_string());
    assert_eq!(response.route_generation, 2);
    let backend = response.backend.expect("backend route");
    assert_eq!(backend.node_id, first_node.to_string());
    assert_eq!(backend.endpoint, "https://10.0.0.11:9800");
    assert_eq!(backend.transport.as_deref(), Some("grpc"));
    let revocation = response.revocation.expect("revocation state");
    assert_eq!(revocation.status, DataPlaneRevocationStatus::Active as i32);
    let claims = verify_proto_route_token(
        response
            .space_api_route_token
            .as_ref()
            .expect("route token minted"),
    )?;
    assert_eq!(claims.route_identity.route_generation().get(), 42);
    assert_eq!(
        claims.route_identity.execution_generation(),
        ExecutionGeneration::new(42).unwrap()
    );

    sqlx::query("UPDATE sandboxes SET active_node_id = $2 WHERE id = $1")
        .bind(sandbox_id)
        .bind(second_node)
        .execute(&fixture.service.pool)
        .await?;

    let migrated = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id,
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: None,
        }))
        .await?
        .into_inner();

    assert_eq!(migrated.current_node_id, second_node.to_string());
    assert_eq!(migrated.route_generation, 3);
    assert_eq!(
        migrated.backend.expect("migrated backend").endpoint,
        "https://10.0.0.12:9800",
    );
    Ok(())
}

struct UnavailableIngressActivation(std::sync::atomic::AtomicUsize);
#[tonic::async_trait]
impl crate::ingress_activation::IngressActivation for UnavailableIngressActivation {
    async fn prepare(&self, _: SandboxId, _: std::time::Duration) -> Result<(), Status> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(Status::unavailable(
            "synthetic activation owner unavailable",
        ))
    }
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_route_resolve_mints_service_port_route_token_for_port_session() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("data-plane-port-internal-token")),
        ),
    ]) {
        return Ok(());
    }

    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let activation = std::sync::Arc::new(UnavailableIngressActivation(
        std::sync::atomic::AtomicUsize::new(0),
    ));
    fixture.service.ingress_activation = Some(activation.clone());
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("port_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.13").await?;
    let node_api = sim_worker_node_api(node_id).await?;
    insert_data_plane_direct_node_endpoint(&fixture.service.pool, node_id, node_api.endpoint())
        .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.80.0.13", 13).await?;
    insert_data_plane_session_with_protocol(
        &fixture.service.pool,
        &session_id,
        None,
        sandbox_id,
        "port",
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
         SET target_port = 8080, target_protocol = 'tcp',
             token_expires_at = NOW() + INTERVAL '1 hour'
         WHERE session_id = $1"
    ))
    .bind(&session_id)
    .execute(&fixture.service.pool)
    .await?;

    let response = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: Some(SandboxServiceRouteTransport::ServicePortIngress as i32),
        }))
        .await?
        .into_inner();

    assert_eq!(response.kind, "port");
    assert_eq!(response.target_port, Some(8080));
    let claims = verify_proto_route_token(
        response
            .space_api_route_token
            .as_ref()
            .expect("ServicePort ingress token minted"),
    )?;
    assert_eq!(claims.purpose, SpaceApiRoutePurpose::ServicePortIngress);
    assert_eq!(claims.service_port, GuestPort::try_from_u16(8080).ok());
    assert_eq!(
        claims.route_identity.execution_generation(),
        ExecutionGeneration::new(13).unwrap()
    );

    assert_eq!(
        activation.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "healthy ingress must serve while the activation owner is unavailable"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended', active_node_id = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;
    let error = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id,
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: Some(SandboxServiceRouteTransport::ServicePortIngress as i32),
        }))
        .await
        .unwrap_err();
    assert_eq!(error.message(), "synthetic activation owner unavailable");
    assert_eq!(activation.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_route_resolve_reports_missing_k8s_backend_endpoint() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());

    insert_data_plane_k8s_worker_node_without_address(
        &fixture.service.pool,
        node_id,
        "ip-192-0-2-178.ec2.internal",
    )
    .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;

    let err = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id,
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: None,
        }))
        .await
        .expect_err("missing K8s node address should fail route resolution");

    assert_eq!(err.code(), Code::FailedPrecondition);
    let message = err.message();
    assert!(message.contains("data-plane session has no backend endpoint"));
    assert!(message.contains("ip-192-0-2-178.ec2.internal"));
    assert!(message.contains("nodes.address"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_revocation_state_reports_persisted_epoch_and_reason() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("proto_{}", Uuid::now_v7());
    let protocol_session_id = format!("proto_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.21").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_data_plane_session_with_protocol(
        &fixture.service.pool,
        &session_id,
        Some(&protocol_session_id),
        sandbox_id,
        "protocol",
    )
    .await?;

    // space-api owns session revocation and writes it straight to Postgres;
    // the cluster API only serves the read side.
    sqlx::query(sql!(
        "UPDATE data_plane_sessions
            SET revocation_epoch = 42,
                revocation_status = 'revoked',
                revocation_reason = 'user_requested',
                revocation_updated_at = NOW(),
                state = 'revoked'
          WHERE session_id = $1"
    ))
    .bind(&session_id)
    .execute(&fixture.service.pool)
    .await?;

    let state = fixture
        .service
        .get_data_plane_session_revocation_state(Request::new(
            GetDataPlaneSessionRevocationStateRequest {
                session_id,
                protocol_session_id: Some(protocol_session_id),
            },
        ))
        .await?
        .into_inner()
        .revocation
        .expect("revocation state");

    assert_eq!(state.epoch, 42);
    assert_eq!(state.status, DataPlaneRevocationStatus::Revoked as i32);
    assert_eq!(state.reason.as_deref(), Some("user_requested"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_process_binding_is_idempotent_and_conflict_checked() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.41").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.80.0.41", 41).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;

    let first = fixture
        .service
        .bind_data_plane_session_process(Request::new(BindDataPlaneSessionProcessRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            process_id: "proc_1".to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(first.process_id, "proc_1");
    assert!(first.newly_bound);

    let replay = fixture
        .service
        .bind_data_plane_session_process(Request::new(BindDataPlaneSessionProcessRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            process_id: "proc_1".to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(replay.process_id, "proc_1");
    assert!(!replay.newly_bound);

    let route = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            token_sha256: None,
            required_scope: None,
            requested_transport: None,
        }))
        .await?
        .into_inner();
    assert_eq!(route.backend_process_id.as_deref(), Some("proc_1"));

    let conflict = fixture
        .service
        .bind_data_plane_session_process(Request::new(BindDataPlaneSessionProcessRequest {
            session_id,
            protocol_session_id: None,
            process_id: "proc_2".to_string(),
        }))
        .await
        .expect_err("second process id should be rejected");
    assert_eq!(conflict.code(), Code::AlreadyExists);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_reconnect_record_updates_public_diagnostics() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.42").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;

    let first = fixture
        .service
        .record_data_plane_session_reconnect(Request::new(RecordDataPlaneSessionReconnectRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            reason: DataPlaneReconnectReason::WorkerUnavailable as i32,
            observed_route_generation: 1,
        }))
        .await?
        .into_inner();
    assert_eq!(first.reconnect_count, 1);
    assert_eq!(
        first.last_reconnect_reason,
        DataPlaneReconnectReason::WorkerUnavailable as i32
    );

    let second = fixture
        .service
        .record_data_plane_session_reconnect(Request::new(RecordDataPlaneSessionReconnectRequest {
            session_id,
            protocol_session_id: None,
            reason: DataPlaneReconnectReason::RouteGenerationChanged as i32,
            observed_route_generation: 2,
        }))
        .await?
        .into_inner();
    assert_eq!(second.reconnect_count, 2);
    assert_eq!(
        second.last_reconnect_reason,
        DataPlaneReconnectReason::RouteGenerationChanged as i32
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn data_plane_route_resolve_validates_token_and_scope() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let session_id = format!("term_{}", Uuid::now_v7());

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.31").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.80.0.31", 31).await?;
    insert_data_plane_session(&fixture.service.pool, &session_id, sandbox_id).await?;
    sqlx::query(
        "UPDATE data_plane_sessions
             SET token_hash = 'tokenhash',
                 token_expires_at = NOW() + ('5 minutes')::INTERVAL,
                 scopes = ARRAY['terminal:connect']",
    )
    .execute(&fixture.service.pool)
    .await?;

    // Valid attach credentials work without tokenless refresh authority.
    let mut request = Request::new(ResolveDataPlaneSessionRouteRequest {
        session_id: session_id.clone(),
        protocol_session_id: None,
        token_sha256: Some("tokenhash".to_string()),
        required_scope: Some("terminal:connect".to_string()),
        requested_transport: None,
    });
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test(),
    );
    let route = fixture
        .service
        .resolve_data_plane_session_route(request)
        .await?
        .into_inner();

    assert_eq!(route.sandbox_id, sandbox_id.to_string());
    assert_eq!(route.kind, "terminal");
    assert_eq!(route.adapter_state_json, "{}");

    let invalid_token = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id: session_id.clone(),
            protocol_session_id: None,
            token_sha256: Some("wrong".to_string()),
            required_scope: Some("terminal:connect".to_string()),
            requested_transport: None,
        }))
        .await
        .expect_err("wrong token should fail");
    assert_eq!(invalid_token.code(), Code::Unauthenticated);

    let invalid_scope = fixture
        .service
        .resolve_data_plane_session_route(data_plane_request(ResolveDataPlaneSessionRouteRequest {
            session_id,
            protocol_session_id: None,
            token_sha256: Some("tokenhash".to_string()),
            required_scope: Some("port:connect".to_string()),
            requested_transport: None,
        }))
        .await
        .expect_err("wrong scope should fail");
    assert_eq!(invalid_scope.code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_allows_envd_with_api_key() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("ecw-route-test-secret")),
        ),
    ]) {
        return Ok(());
    }

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let api_key = "ecw-api-key";
    let api_key_hash = sha256_hex(api_key);

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.41").await?;
    let node_api = sim_worker_node_api(node_id).await?;
    insert_data_plane_direct_node_endpoint(&fixture.service.pool, node_id, node_api.endpoint())
        .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.90.0.41", 1).await?;
    mark_sandbox_command_ready_pending(&fixture.service.pool, sandbox_id).await?;
    insert_e2b_api_key(&fixture.service.pool, api_key_hash.as_str()).await?;

    let route = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 49_983,
                route_kind: SandboxServiceRouteKind::Agent as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::OrgApiKey,
                    api_key_hash,
                )],
            },
        ))
        .await?
        .into_inner();

    assert_eq!(route.sandbox_id, sandbox_id.to_string());
    assert_eq!(route.port, 49_983);
    assert_eq!(route.current_node_id, node_id.to_string());
    assert_eq!(route.lifecycle, "started");
    assert_eq!(route.route_generation, 1);
    assert_eq!(
        route.backend.expect("backend").endpoint,
        node_api.endpoint()
    );
    assert!(route.space_api_route_token.is_some());
    assert!(
        node_api
            .state()
            .last_wait_ready_route_token()
            .await
            .is_some(),
        "command-ready probe must forward the SPACE API route token"
    );
    let command_ready_pending: bool =
        sqlx::query_scalar("SELECT command_ready_pending FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert!(!command_ready_pending);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_reports_missing_k8s_backend_endpoint() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let api_key = "ecw-missing-backend-key";
    let api_key_hash = sha256_hex(api_key);

    insert_data_plane_k8s_worker_node_without_address(
        &fixture.service.pool,
        node_id,
        "ip-192-0-2-178.ec2.internal",
    )
    .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_e2b_api_key(&fixture.service.pool, api_key_hash.as_str()).await?;

    let err = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 49_983,
                route_kind: SandboxServiceRouteKind::Agent as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::OrgApiKey,
                    api_key_hash,
                )],
            },
        ))
        .await
        .expect_err("missing K8s node address should fail ECW route resolution");

    assert_eq!(err.code(), Code::FailedPrecondition);
    assert_eq!(
        SandboxServiceRouteNotReady::from_metadata(err.metadata()),
        Some(SandboxServiceRouteNotReady)
    );
    assert_eq!(
        SandboxServiceRouteNotReady::from_status(&err),
        Some(SandboxServiceRouteNotReady)
    );
    let message = err.message();
    assert!(message.contains("sandbox service route has no backend endpoint"));
    assert!(message.contains("ip-192-0-2-178.ec2.internal"));
    assert!(message.contains("nodes.address"));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_allows_envd_with_envd_token() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_TOKEN",
        Some(std::ffi::OsStr::new("ecw-route-test-secret")),
    )]) {
        return Ok(());
    }

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.43").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.90.0.43", 43).await?;
    let token = ecw_test_token(sandbox_id, EcwTestTokenKind::Envd);

    let route = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 49_983,
                route_kind: SandboxServiceRouteKind::Agent as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxAgentToken,
                    sha256_hex(&token),
                )],
            },
        ))
        .await?
        .into_inner();

    assert_eq!(route.sandbox_id, sandbox_id.to_string());
    assert_eq!(route.port, 49_983);
    assert_eq!(route.current_node_id, node_id.to_string());
    let invalid = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 49_983,
                route_kind: SandboxServiceRouteKind::Agent as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxAgentToken,
                    sha256_hex("wrong"),
                )],
            },
        ))
        .await
        .expect_err("wrong envd token should fail");
    assert_eq!(invalid.code(), Code::Unauthenticated);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_rejects_traffic_token_for_envd() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[(
        "SPACE_INTERNAL_TOKEN",
        Some(std::ffi::OsStr::new("ecw-route-test-secret")),
    )]) {
        return Ok(());
    }

    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.44").await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.90.0.44", 44).await?;
    let traffic_token = ecw_test_token(sandbox_id, EcwTestTokenKind::Traffic);

    // The traffic token is handed to consumers of exposed service ports. It
    // must not authorize the envd route, which grants guest file and process
    // control; only the envd token or the owning org's API key may.
    let rejected = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 49_983,
                route_kind: SandboxServiceRouteKind::Agent as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                    sha256_hex(&traffic_token),
                )],
            },
        ))
        .await
        .expect_err("a valid traffic token must not authorize the envd route");
    assert_eq!(rejected.code(), Code::Unauthenticated);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_allows_service_with_traffic_token() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("ecw-route-test-secret")),
        ),
    ]) {
        return Ok(());
    }

    let Some(mut fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let activation = std::sync::Arc::new(UnavailableIngressActivation(
        std::sync::atomic::AtomicUsize::new(0),
    ));
    fixture.service.ingress_activation = Some(activation.clone());
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();

    insert_data_plane_worker_node(&fixture.service.pool, node_id, "10.0.0.42").await?;
    let node_api = sim_worker_node_api(node_id).await?;
    insert_data_plane_direct_node_endpoint(&fixture.service.pool, node_id, node_api.endpoint())
        .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    mark_sandbox_command_ready_pending(&fixture.service.pool, sandbox_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "10.90.0.42", 17).await?;
    let token = ecw_test_token(sandbox_id, EcwTestTokenKind::Traffic);

    let ingress_route = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 8080,
                route_kind: SandboxServiceRouteKind::ServicePort as i32,
                transport: SandboxServiceRouteTransport::ServicePortIngress as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                    sha256_hex(&token),
                )],
            },
        ))
        .await?
        .into_inner();

    assert_eq!(ingress_route.sandbox_id, sandbox_id.to_string());
    assert_eq!(ingress_route.port, 8080);
    assert_eq!(ingress_route.route_generation, 17);
    assert!(ingress_route.space_api_route_token.is_none());
    let ingress_claims = verify_proto_route_token(
        ingress_route
            .service_port_ingress_token
            .as_ref()
            .expect("service port ingress route token minted"),
    )?;
    assert_eq!(
        ingress_claims.purpose,
        SpaceApiRoutePurpose::ServicePortIngress
    );
    assert_eq!(ingress_claims.route_identity.route_generation().get(), 17);
    assert_eq!(
        ingress_claims.route_identity.execution_generation(),
        ExecutionGeneration::new(17).unwrap()
    );
    assert_eq!(
        ingress_claims.service_port,
        Some(GuestPort::try_from_u16(8080)?)
    );
    let ingress_wait_ready_claims =
        decode_unverified_space_api_route_ed25519_claims(&SpaceApiRouteEd25519Token::parse(
            node_api
                .state()
                .last_wait_ready_route_token()
                .await
                .expect("ingress route must preserve the command-ready probe"),
        )?)?;
    assert_eq!(
        ingress_wait_ready_claims.purpose,
        SpaceApiRoutePurpose::SpaceApiBridge
    );
    assert_eq!(ingress_wait_ready_claims.service_port, None);
    let command_ready_pending: bool =
        sqlx::query_scalar("SELECT command_ready_pending FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert!(!command_ready_pending);
    let exposure_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*)
         FROM sandbox_port_exposures
         WHERE sandbox_id = $1
           AND protocol = 'tcp'
           AND guest_port = 8080",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(exposure_count, 0);

    let direct = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 8080,
                route_kind: SandboxServiceRouteKind::ServicePort as i32,
                transport: SandboxServiceRouteTransport::Direct as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                    sha256_hex(&token),
                )],
            },
        ))
        .await
        .expect_err("direct public service routing is retired");
    assert_eq!(direct.code(), Code::InvalidArgument);

    let invalid = fixture
        .service
        .unstable_resolve_sandbox_service_route(Request::new(
            UnstableResolveSandboxServiceRouteRequest {
                sandbox_id: sandbox_id.to_string(),
                port: 8080,
                route_kind: SandboxServiceRouteKind::ServicePort as i32,
                transport: SandboxServiceRouteTransport::ServicePortIngress as i32,
                credential_proofs: vec![route_credential_proof(
                    SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                    sha256_hex("wrong"),
                )],
            },
        ))
        .await
        .expect_err("wrong traffic token should fail");
    assert_eq!(invalid.code(), Code::PermissionDenied);

    assert_eq!(
        activation.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "healthy ingress must serve while the activation owner is unavailable"
    );
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspended', active_node_id = NULL WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;
    for (proof, expected, calls) in [
        (
            sha256_hex("invalid-traffic-token"),
            Code::PermissionDenied,
            0,
        ),
        (sha256_hex(&token), Code::Unavailable, 1),
    ] {
        let error = fixture
            .service
            .unstable_resolve_sandbox_service_route(Request::new(
                UnstableResolveSandboxServiceRouteRequest {
                    sandbox_id: sandbox_id.to_string(),
                    port: 8080,
                    route_kind: SandboxServiceRouteKind::ServicePort as i32,
                    transport: SandboxServiceRouteTransport::ServicePortIngress as i32,
                    credential_proofs: vec![route_credential_proof(
                        SandboxServiceRouteCredentialKind::SandboxTrafficToken,
                        proof,
                    )],
                },
            ))
            .await
            .unwrap_err();
        assert_eq!(error.code(), expected);
        assert_eq!(
            activation.0.load(std::sync::atomic::Ordering::SeqCst),
            calls
        );
    }
    Ok(())
}

fn route_credential_proof(
    kind: SandboxServiceRouteCredentialKind,
    sha256: String,
) -> SandboxServiceRouteCredentialProof {
    SandboxServiceRouteCredentialProof {
        kind: kind as i32,
        sha256,
    }
}

pub(crate) async fn insert_data_plane_worker_node(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    address: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'worker', 'k8s', 'ready')",
    )
    .bind(node_id)
    .bind(address)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_data_plane_direct_node_endpoint(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    endpoint: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO node_direct (node_id, node_api_endpoint, deleted_at)
             VALUES ($1, $2, NULL)
             ON CONFLICT (node_id) DO UPDATE
             SET node_api_endpoint = EXCLUDED.node_api_endpoint,
                 deleted_at = NULL,
                 updated_at = NOW()",
    )
    .bind(node_id)
    .bind(endpoint)
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_data_plane_k8s_worker_node_without_address(
    pool: &sqlx::PgPool,
    node_id: NodeId,
    k8s_node_name: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO nodes (id, role, managed_by, state)
             VALUES ($1, 'worker', 'k8s', 'ready')",
    )
    .bind(node_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid)
             VALUES ($1, $2, 'missing-backend-test-uid')",
    )
    .bind(node_id)
    .bind(k8s_node_name)
    .execute(pool)
    .await?;
    Ok(())
}

pub(crate) async fn insert_data_plane_active_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, network_policy_generation
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000010',
                 '00000000-0000-0000-0000-000000000001',
                 'k8s',
                 'started',
                 'placed',
                 $2,
                 $2,
                 1
             )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn mark_sandbox_command_ready_pending(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE sandboxes SET command_ready_pending = TRUE WHERE id = $1")
        .bind(sandbox_id)
        .execute(pool)
        .await?;
    Ok(())
}

pub(crate) async fn insert_active_execution(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    pod_ip: &str,
    generation: i64,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions (
                 id, sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                 pod_ip, route_activation_id, state, active
             )
             VALUES (
                 $1, $2, $3, $4, 'space', $5, $6::INET, $7, 'running', true
             )",
    )
    .bind(Uuid::now_v7())
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "sandbox-pod-{}-{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(format!("pod-uid-{generation}"))
    .bind(pod_ip)
    .bind(Uuid::now_v7())
    .execute(pool)
    .await?;
    Ok(())
}

async fn insert_e2b_api_key(pool: &sqlx::PgPool, key_hash: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO api_keys (
                 id, org_id, name, key_hash, created_by
             )
             VALUES (
                 $1,
                 '00000000-0000-0000-0000-000000000001',
                 'ecw test key',
                 $2,
                 '00000000-0000-0000-0000-000000000010'
             )",
    )
    .bind(Uuid::now_v7())
    .bind(key_hash)
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum EcwTestTokenKind {
    Envd,
    Traffic,
}

impl EcwTestTokenKind {
    const fn credential_kind(self) -> EcwCredentialKind {
        match self {
            Self::Envd => EcwCredentialKind::Envd,
            Self::Traffic => EcwCredentialKind::Traffic,
        }
    }
}

fn ecw_test_token(sandbox_id: SandboxId, kind: EcwTestTokenKind) -> String {
    test_ecw_credential_keyring()
        .mint(
            sandbox_id,
            EcwCredentialEpoch::new(1).expect("test credential epoch is valid"),
            kind.credential_kind(),
        )
        .expect("test ECW credential mints")
        .expect("test keyring has a current key")
        .secret()
        .to_string()
}

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) async fn insert_data_plane_session(
    pool: &sqlx::PgPool,
    session_id: &str,
    sandbox_id: SandboxId,
) -> Result<(), sqlx::Error> {
    insert_data_plane_session_with_protocol(pool, session_id, None, sandbox_id, "terminal").await
}

async fn insert_data_plane_session_with_protocol(
    pool: &sqlx::PgPool,
    session_id: &str,
    protocol_session_id: Option<&str>,
    sandbox_id: SandboxId,
    kind: &str,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO data_plane_sessions (
                 session_id, protocol_session_id, org_id, sandbox_id, kind
             )
             VALUES (
                 $1, $2,
                 '00000000-0000-0000-0000-000000000001',
                 $3, $4
             )",
    )
    .bind(session_id)
    .bind(protocol_session_id)
    .bind(sandbox_id)
    .bind(kind)
    .execute(pool)
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn direct_origin_node_id_does_not_reactivate_soft_deleted_node() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, '10.0.0.12'::INET, 'worker', 'space', 'deleted')",
    )
    .bind(node_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(
        "INSERT INTO node_direct (node_id, name, node_api_endpoint, deleted_at)
             VALUES ($1, 'worker-1', '10.0.0.12:9802', NOW())",
    )
    .bind(node_id)
    .execute(&fixture.service.pool)
    .await?;

    let err = fixture
        .service
        .resolve_origin_node_id(&node_id.to_string(), Some(SpaceNodeRole::Worker))
        .await
        .expect_err("deleted direct nodes must require explicit admin registration");
    assert_eq!(err.code(), Code::NotFound);

    let (state, deleted_at_present, address): (String, bool, Option<String>) = sqlx::query_as(
        "SELECT n.state, nd.deleted_at IS NOT NULL, host(n.address)
                   FROM nodes n
                   JOIN node_direct nd ON nd.node_id = n.id
                  WHERE n.id = $1",
    )
    .bind(node_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(state, "deleted");
    assert!(deleted_at_present);
    assert_eq!(address.as_deref(), Some("10.0.0.12"));
    Ok(())
}

// ── Immutable suspend-snapshot invariants ──────────────────────────────

/// Insert a minimal `sandboxes` row so the suspend-snapshot RPCs can
/// satisfy their FK lookups. Returns the sandbox UUID for reuse.
/// A sandbox placed on a worker node registered at [`TEST_WORKER_IP`].
async fn seed_sandbox(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    let node_id = NodeId::new();
    insert_ready_worker_node(pool, node_id).await?;
    sqlx::query(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, active_node_id, target_node_id,
                 network_policy_generation
             )
             VALUES ($1, $2::uuid, $3::uuid, $4, $4, 1)",
    )
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// A sandbox with no node placement.
async fn seed_unplaced_sandbox(pool: &sqlx::PgPool) -> TestResult<SandboxId> {
    let sandbox_id = SandboxId::new();
    sqlx::query(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
             VALUES ($1, $2::uuid, $3::uuid, 1)",
    )
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .execute(pool)
    .await?;
    Ok(sandbox_id)
}

/// The address `insert_ready_worker_node` registers nodes at.
const TEST_WORKER_IP: std::net::IpAddr = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

/// A request that arrived over TCP from `node_ip`.
fn request_from_node<T>(message: T, node_ip: std::net::IpAddr) -> Request<T> {
    let mut request = Request::new(message);
    request
        .extensions_mut()
        .insert(tonic::transport::server::TcpConnectInfo {
            local_addr: None,
            remote_addr: Some(std::net::SocketAddr::from((node_ip, 49000))),
        });
    request
}

/// A node-scoped request from `node_ip`: the TCP origin plus the narrow
/// authority every TokenReview identity carries.
fn node_scoped_request<T>(message: T, node_ip: std::net::Ipv4Addr) -> Request<T> {
    let mut request = request_from_node(message, std::net::IpAddr::V4(node_ip));
    request.extensions_mut().insert(
        pplx_space_common_svc::auth::NodeApiAuthenticatedCaller::unprivileged_workload_for_test(),
    );
    request
}

/// Registers an org-specific template owned by `owner` with one release that
/// is tagged `latest` and has a storage location. Returns the release id and
/// its hash.
async fn seed_private_release(
    fixture: &ClusterFixture,
    template: &str,
    owner: Uuid,
) -> TestResult<(Uuid, String)> {
    let pool = &fixture.service.pool;
    let hash = format!("sha256:{}", "b".repeat(64));
    register_template_with_org(&fixture.service, template, Some(OrgId::from_uuid(owner))).await?;
    sqlx::query(sql!(
        "UPDATE templates SET visibility = 'org_specific' WHERE name = $1"
    ))
    .bind(template)
    .execute(pool)
    .await?;
    let release_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO template_releases (id, template_name, type, hash, org_id)
         VALUES ($1, $2, 'fc', $3, $4)"
    ))
    .bind(release_id)
    .bind(template)
    .bind(&hash)
    .bind(owner)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_tags (template_name, template_release_id, tag)
         VALUES ($1, $2, 'latest')"
    ))
    .bind(template)
    .bind(release_id)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "INSERT INTO template_release_storage_locations
             (template_release_id, storage_connection_string, org_id)
         VALUES ($1, 's3://private/rootfs?region=us-east-2', $2)"
    ))
    .bind(release_id)
    .bind(owner)
    .execute(pool)
    .await?;
    Ok((release_id, hash))
}

/// Registers a ready Kubernetes-managed node of `role` at `address`.
async fn insert_k8s_node(
    pool: &sqlx::PgPool,
    address: std::net::Ipv4Addr,
    role: &str,
) -> TestResult<Uuid> {
    let id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO nodes (id, address, role, managed_by, state)
         VALUES ($1, $2::INET, $3, 'k8s', 'ready')"
    ))
    .bind(id)
    .bind(address.to_string())
    .bind(role)
    .execute(pool)
    .await?;
    Ok(id)
}

/// Whether a node at `node_ip` can resolve `template`'s `latest` release. All
/// three node-facing reads are issued and must agree; `NotFound` and an empty
/// listing read as not visible, any other error fails the test.
async fn release_visible_from(
    fixture: &ClusterFixture,
    template: &str,
    hash: &str,
    node_ip: std::net::Ipv4Addr,
    caller_org_id: Option<Uuid>,
) -> TestResult<bool> {
    fn visible(
        result: Result<tonic::Response<FindTemplateReleaseResponse>, tonic::Status>,
    ) -> TestResult<bool> {
        match result {
            Ok(_) => Ok(true),
            Err(status) if status.code() == Code::NotFound => Ok(false),
            Err(status) => Err(status.into()),
        }
    }
    let caller_org_id = caller_org_id.map(|org| org.to_string());
    let by_tag = visible(
        fixture
            .service
            .find_template_release(node_scoped_request(
                FindTemplateReleaseRequest {
                    generic_only: false,
                    caller_org_id: caller_org_id.clone(),
                    template_name: template.to_string(),
                    runtime: "firecracker".to_string(),
                    tag: "latest".to_string(),
                },
                node_ip,
            ))
            .await,
    )?;
    let by_hash = visible(
        fixture
            .service
            .find_template_release_by_hash(node_scoped_request(
                FindTemplateReleaseByHashRequest {
                    generic_only: false,
                    caller_org_id,
                    template_name: template.to_string(),
                    hash: hash.to_string(),
                },
                node_ip,
            ))
            .await,
    )?;
    let listed = !fixture
        .service
        .list_latest_template_releases(node_scoped_request(
            ListLatestTemplateReleasesRequest {
                template_name: template.to_string(),
                runtime: "firecracker".to_string(),
                limit: 5,
            },
            node_ip,
        ))
        .await?
        .into_inner()
        .entries
        .is_empty();
    assert_eq!(
        by_tag, by_hash,
        "tag and hash lookups must agree for {template}"
    );
    assert_eq!(
        by_tag, listed,
        "ListLatest must agree with the lookups for {template}"
    );
    Ok(by_tag)
}

/// Node-scoped callers (every TokenReview identity, and the static token once
/// TokenReview is configured) do not see an org-specific release the control
/// plane has not tied to their node, whether they are a registered worker or
/// an address that is not a node at all, and asserting the owner's
/// `caller_org_id` grants nothing. Generic templates stay resolvable, and a
/// caller without the auth extension keeps the control-plane view.
#[tokio::test(flavor = "current_thread")]
async fn node_scoped_lookups_hide_unplaced_private_releases() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let (_, hash) = seed_private_release(&fixture, "private-base", owner).await?;
    register_template(&fixture.service, "code-runtime").await?;
    let generic_hash = format!("sha256:{}", "c".repeat(64));
    register_release(
        &fixture.service,
        &generic_hash,
        &[&generic_hash, "latest"],
        &["s3://bucket/templates/code-runtime/generic?region=us-east-1"],
    )
    .await?;
    let worker_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    insert_k8s_node(&fixture.service.pool, worker_ip, "worker").await?;
    let unregistered_ip = std::net::Ipv4Addr::new(10, 0, 0, 99);

    for node_ip in [worker_ip, unregistered_ip] {
        for caller_org_id in [None, Some(owner)] {
            assert!(
                !release_visible_from(&fixture, "private-base", &hash, node_ip, caller_org_id)
                    .await?,
                "{node_ip} must not see an unplaced private release (asserted org {caller_org_id:?})"
            );
        }
        assert!(
            release_visible_from(&fixture, "code-runtime", &generic_hash, node_ip, None).await?,
            "{node_ip} must still resolve generic templates"
        );
    }
    fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "private-base".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?;
    Ok(())
}

/// A sandbox of the template placed on a node, by active or target
/// placement, lets that node and only that node resolve the release.
#[tokio::test(flavor = "current_thread")]
async fn placed_sandbox_unlocks_private_release_for_its_node_only() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let (release_id, hash) = seed_private_release(&fixture, "private-base", owner).await?;
    let worker_a_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    let worker_b_ip = std::net::Ipv4Addr::new(10, 0, 0, 31);
    let worker_a = insert_k8s_node(pool, worker_a_ip, "worker").await?;
    let worker_b = insert_k8s_node(pool, worker_b_ip, "worker").await?;

    let sandbox_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, command_ready_pending,
             network_policy_generation, template_id
         )
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node', $4, $4, TRUE, 1, 'private-base')"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(owner)
    .bind(worker_a)
    .execute(pool)
    .await?;
    assert!(release_visible_from(&fixture, "private-base", &hash, worker_a_ip, None).await?);
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_b_ip, None).await?);
    let found = fixture
        .service
        .find_template_release(node_scoped_request(
            FindTemplateReleaseRequest {
                generic_only: false,
                caller_org_id: None,
                template_name: "private-base".to_string(),
                runtime: "firecracker".to_string(),
                tag: "latest".to_string(),
            },
            worker_a_ip,
        ))
        .await?
        .into_inner();
    assert_eq!(
        found.release.expect("placed release").id,
        release_id.to_string()
    );
    assert_eq!(found.authorized_org_id, None);

    // A pending migration is a target placement; the destination may pull
    // before it becomes active, and the source keeps access while it still
    // hosts the sandbox.
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(worker_b)
    .execute(pool)
    .await?;
    assert!(release_visible_from(&fixture, "private-base", &hash, worker_a_ip, None).await?);
    assert!(release_visible_from(&fixture, "private-base", &hash, worker_b_ip, None).await?);
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(worker_b)
    .execute(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_a_ip, None).await?);
    Ok(())
}

/// A node reads a sandbox's suspend snapshot location, exact artifact backup
/// location and organization snapshot storage only while the sandbox is
/// placed on it: active there, or as the target of a pending migration or
/// restore. Another node reads `NotFound`, and a request with no remote
/// address is refused. A former host can still ask whether an identity it
/// holds is backed up, and learns only that.
#[tokio::test(flavor = "current_thread")]
async fn placed_sandbox_locations_resolve_for_its_node_only() -> TestResult {
    use crate::proto::{
        BackupReadinessQuery, LocalArtifactBackupIdentity, backup_readiness_query::Artifact,
    };
    use prost::Message as _;
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let worker_a_ip = std::net::Ipv4Addr::new(10, 0, 0, 40);
    let worker_b_ip = std::net::Ipv4Addr::new(10, 0, 0, 41);
    let worker_a = insert_k8s_node(pool, worker_a_ip, "worker").await?;
    let worker_b = insert_k8s_node(pool, worker_b_ip, "worker").await?;

    let sandbox_id = SandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, command_ready_pending,
             network_policy_generation
         )
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node', $4, $4, TRUE, 1)"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .bind(worker_a)
    .execute(pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE organizations
         SET snapshot_base_object_storage_uri = 's3://org-snapshots/base'
         WHERE id = $1::uuid"
    ))
    .bind(SEEDED_ORG_ID)
    .execute(pool)
    .await?;

    // The hosting node registers a ready suspend snapshot and a ready backup
    // of a stopped rootfs, the two artifacts a restore looks up.
    let suspend_attempt = SnapshotId::new();
    let suspend_artifact = LocalArtifactId::new();
    begin_artifact_backup_from_node(
        &fixture.service,
        std::net::IpAddr::V4(worker_a_ip),
        sandbox_id,
        suspend_artifact,
        suspend_attempt,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    finalize_ready_from_node(&fixture.service, worker_a_ip, suspend_attempt).await?;
    let disk_artifact = LocalArtifactId::new();
    let disk_attempt = SnapshotId::new();
    begin_artifact_backup_from_node(
        &fixture.service,
        std::net::IpAddr::V4(worker_a_ip),
        sandbox_id,
        disk_artifact,
        disk_attempt,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;
    finalize_ready_from_node(&fixture.service, worker_a_ip, disk_attempt).await?;
    // Finalizing the suspend snapshot projects the sandbox as suspended and
    // releases its active placement. Pin it back on the hosting node so the
    // placement under test is the one written below, not that projection.
    sqlx::query(sql!(
        "UPDATE sandboxes
         SET lifecycle = 'started', placement = 'on_node',
             active_node_id = $2, target_node_id = $2
         WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(worker_a)
    .execute(pool)
    .await?;

    let reads = |node_ip: Option<std::net::Ipv4Addr>| {
        let service = &fixture.service;
        async move {
            let suspend = service
                .get_suspend_snapshot_location(maybe_from_node(
                    GetSuspendSnapshotLocationRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    node_ip,
                ))
                .await
                .map(|response| response.into_inner().snapshot_id);
            let backup = service
                .get_artifact_backup_location(maybe_from_node(
                    GetArtifactBackupLocationRequest {
                        sandbox_id: sandbox_id.to_string(),
                        local_artifact_id: disk_artifact.to_string(),
                        artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32,
                        readiness: None,
                    },
                    node_ip,
                ))
                .await
                .map(|response| {
                    let response = response.into_inner();
                    assert_eq!(
                        response.ready, None,
                        "a location reply is not a readiness reply"
                    );
                    response.backup_attempt_id
                });
            let storage = service
                .get_sandbox_snapshot_storage_uri(maybe_from_node(
                    GetSandboxSnapshotStorageUriRequest {
                        sandbox_id: sandbox_id.to_string(),
                    },
                    node_ip,
                ))
                .await
                .map(|response| response.into_inner().storage_uri);
            (suspend, backup, storage)
        }
    };
    /// The suspend snapshot id, the backup attempt id and the organization
    /// storage URI, as one node read them.
    type LocationReads = (
        Result<String, tonic::Status>,
        Result<String, tonic::Status>,
        Result<Option<String>, tonic::Status>,
    );
    let codes = |results: &LocationReads| {
        [
            results.0.as_ref().err().map(tonic::Status::code),
            results.1.as_ref().err().map(tonic::Status::code),
            results.2.as_ref().err().map(tonic::Status::code),
        ]
    };

    let (suspend, backup, storage) = reads(Some(worker_a_ip)).await;
    assert_eq!(suspend?, suspend_attempt.to_string());
    assert_eq!(backup?, disk_attempt.to_string());
    assert_eq!(storage?.as_deref(), Some("s3://org-snapshots/base"));
    assert_eq!(
        codes(&reads(Some(worker_b_ip)).await),
        [Some(tonic::Code::NotFound); 3]
    );
    assert_eq!(
        codes(&reads(None).await),
        [Some(tonic::Code::PermissionDenied); 3]
    );

    // A pending migration or restore is a target placement: the destination
    // reads before it becomes active, and the source keeps access while it
    // still hosts the sandbox.
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(worker_b)
    .execute(pool)
    .await?;
    assert_eq!(codes(&reads(Some(worker_a_ip)).await), [None; 3]);
    assert_eq!(codes(&reads(Some(worker_b_ip)).await), [None; 3]);
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $2 WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(worker_b)
    .execute(pool)
    .await?;
    assert_eq!(
        codes(&reads(Some(worker_a_ip)).await),
        [Some(tonic::Code::NotFound); 3]
    );

    // The former host still needs to evict its retained cache. Exact backup
    // proofs remain available after relocation without reopening URI reads.
    let exact = |id: LocalArtifactId, kind| {
        Artifact::LocalArtifact(LocalArtifactBackupIdentity {
            local_artifact_id: id.to_string(),
            artifact_kind: kind,
        })
    };
    let full = pplx_space_sbx_mgr_client::proto::ArtifactKind::Full as i32;
    let disk = pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32;
    let readiness =
        |sandbox: SandboxId, artifact: Option<Artifact>| GetArtifactBackupLocationRequest {
            sandbox_id: sandbox.to_string(),
            readiness: Some(BackupReadinessQuery { artifact }),
            ..Default::default()
        };
    let check = |sandbox: SandboxId, artifact: Artifact, node_ip| {
        let service = &fixture.service;
        let request = readiness(sandbox, Some(artifact));
        async move {
            let response = service
                .get_artifact_backup_location(maybe_from_node(request, node_ip))
                .await?
                .into_inner();
            // A readiness reply carries no identity, location or digests,
            // whatever its answer.
            assert_eq!(
                response,
                GetArtifactBackupLocationResponse {
                    ready: response.ready,
                    ..Default::default()
                }
            );
            Ok::<_, tonic::Status>(response.ready.expect("a readiness reply sets ready"))
        }
    };
    for artifact in [
        exact(suspend_artifact, full),
        exact(disk_artifact, disk),
        Artifact::LatestReadySuspendSnapshotId(suspend_attempt.to_string()),
        Artifact::DurablyRestorable(true),
    ] {
        assert!(check(sandbox_id, artifact.clone(), Some(worker_a_ip)).await?);
        assert!(!check(SandboxId::new(), artifact.clone(), Some(worker_a_ip)).await?);
        for remote in [None, Some(std::net::Ipv4Addr::new(10, 0, 0, 42))] {
            assert_eq!(
                check(sandbox_id, artifact.clone(), remote)
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::PermissionDenied,
            );
        }
    }
    for artifact in [
        exact(LocalArtifactId::new(), full),
        exact(suspend_artifact, disk),
        exact(disk_artifact, full),
        Artifact::LatestReadySuspendSnapshotId(disk_attempt.to_string()),
        Artifact::LatestReadySuspendSnapshotId(SnapshotId::new().to_string()),
    ] {
        assert!(!check(sandbox_id, artifact, Some(worker_a_ip)).await?);
    }
    // A readiness query names its identity only inside `readiness`; mixing it
    // with the location fields is refused, as are empty or invalid identities.
    let mixed_id = GetArtifactBackupLocationRequest {
        local_artifact_id: disk_artifact.to_string(),
        ..readiness(sandbox_id, Some(exact(disk_artifact, disk)))
    };
    let mixed_kind = GetArtifactBackupLocationRequest {
        artifact_kind: disk,
        ..readiness(sandbox_id, Some(exact(disk_artifact, disk)))
    };
    for request in [
        mixed_id,
        mixed_kind,
        readiness(sandbox_id, None),
        readiness(sandbox_id, Some(exact(suspend_artifact, 0))),
        readiness(
            sandbox_id,
            Some(Artifact::LatestReadySuspendSnapshotId(
                "invalid".to_string(),
            )),
        ),
        readiness(sandbox_id, Some(Artifact::DurablyRestorable(false))),
        readiness(
            sandbox_id,
            Some(Artifact::LocalArtifact(LocalArtifactBackupIdentity {
                local_artifact_id: "invalid".to_string(),
                artifact_kind: full,
            })),
        ),
    ] {
        let status = fixture
            .service
            .get_artifact_backup_location(maybe_from_node(request, Some(worker_a_ip)))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // Rolling upgrade: a server that predates `readiness` decodes only
    // fields 1-3. A location request keeps identical bytes, and a readiness
    // query reaches it with an empty artifact id, which its location path
    // (the validation kept here) rejects before any lookup.
    let location = GetArtifactBackupLocationRequest {
        sandbox_id: sandbox_id.to_string(),
        local_artifact_id: disk_artifact.to_string(),
        artifact_kind: disk,
        readiness: None,
    };
    assert_eq!(
        location.encode_to_vec(),
        LocationOnlyRequest {
            sandbox_id: location.sandbox_id.clone(),
            local_artifact_id: location.local_artifact_id.clone(),
            artifact_kind: disk,
        }
        .encode_to_vec()
    );
    for artifact in [
        exact(disk_artifact, disk),
        Artifact::LatestReadySuspendSnapshotId(suspend_attempt.to_string()),
    ] {
        let wire = readiness(sandbox_id, Some(artifact)).encode_to_vec();
        let seen_by_old_server = LocationOnlyRequest::decode(wire.as_slice())?;
        assert_eq!(
            seen_by_old_server,
            LocationOnlyRequest {
                sandbox_id: sandbox_id.to_string(),
                ..Default::default()
            }
        );
        let request = GetArtifactBackupLocationRequest::decode(
            seen_by_old_server.encode_to_vec().as_slice(),
        )?;
        let status = fixture
            .service
            .get_artifact_backup_location(maybe_from_node(request, Some(worker_a_ip)))
            .await
            .unwrap_err();
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    // An unpinned cache's proof must reject an older ready attempt, while an
    // immutable pinned artifact can still use that same durable backup.
    sqlx::query(sql!(
        "UPDATE snapshots SET timestamp = NOW() - INTERVAL '1 hour' WHERE id = $1"
    ))
    .bind(suspend_attempt)
    .execute(pool)
    .await?;
    let newer_attempt = SnapshotId::new();
    let newer_artifact = LocalArtifactId::new();
    begin_artifact_backup_from_node(
        &fixture.service,
        std::net::IpAddr::V4(worker_b_ip),
        sandbox_id,
        newer_artifact,
        newer_attempt,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    finalize_ready_from_node(&fixture.service, worker_b_ip, newer_attempt).await?;
    assert!(
        !check(
            sandbox_id,
            Artifact::LatestReadySuspendSnapshotId(suspend_attempt.to_string()),
            Some(worker_a_ip)
        )
        .await?
    );
    assert!(check(sandbox_id, exact(suspend_artifact, full), Some(worker_a_ip)).await?);
    assert!(
        check(
            sandbox_id,
            Artifact::LatestReadySuspendSnapshotId(newer_attempt.to_string()),
            Some(worker_a_ip)
        )
        .await?
    );
    for state in ["creating", "failed", "deleting"] {
        sqlx::query(sql!("UPDATE snapshots SET state = $2 WHERE id = $1"))
            .bind(newer_attempt)
            .bind(state)
            .execute(pool)
            .await?;
        assert!(!check(sandbox_id, exact(newer_artifact, full), Some(worker_a_ip)).await?);
        assert!(
            !check(
                sandbox_id,
                Artifact::LatestReadySuspendSnapshotId(newer_attempt.to_string()),
                Some(worker_a_ip)
            )
            .await?
        );
    }
    sqlx::query(sql!("UPDATE snapshots SET state = 'ready' WHERE id = $1"))
        .bind(newer_attempt)
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "UPDATE snapshot_storage_locations SET orphaned_at = NOW() WHERE snapshot_id = $1"
    ))
    .bind(newer_attempt)
    .execute(pool)
    .await?;
    assert!(!check(sandbox_id, exact(newer_artifact, full), Some(worker_a_ip)).await?);
    assert!(
        !check(
            sandbox_id,
            Artifact::LatestReadySuspendSnapshotId(suspend_attempt.to_string()),
            Some(worker_a_ip)
        )
        .await?
    );
    assert!(
        !check(
            sandbox_id,
            Artifact::LatestReadySuspendSnapshotId(newer_attempt.to_string()),
            Some(worker_a_ip)
        )
        .await?
    );
    Ok(())
}

/// `GetArtifactBackupLocationRequest` as decoded by a server that predates
/// its readiness query.
#[derive(Clone, PartialEq, prost::Message)]
struct LocationOnlyRequest {
    #[prost(string, tag = "1")]
    sandbox_id: String,
    #[prost(string, tag = "2")]
    local_artifact_id: String,
    #[prost(int32, tag = "3")]
    artifact_kind: i32,
}

/// A request from `node_ip` when given, otherwise one with no remote address.
fn maybe_from_node<T>(message: T, node_ip: Option<std::net::Ipv4Addr>) -> Request<T> {
    match node_ip {
        Some(node_ip) => request_from_node(message, std::net::IpAddr::V4(node_ip)),
        None => Request::new(message),
    }
}

/// Finalize `snapshot_id` as `ready` from the node at `node_ip`.
async fn finalize_ready_from_node(
    service: &ClusterApiService,
    node_ip: std::net::Ipv4Addr,
    snapshot_id: SnapshotId,
) -> Result<(), tonic::Status> {
    service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: snapshot_id.to_string(),
                target_state: SuspendSnapshotTerminalState::Ready as i32,
                bytes_uploaded: 4096,
                failure_reason: None,
                artifact_digests: Vec::new(),
            },
            std::net::IpAddr::V4(node_ip),
        ))
        .await
        .map(|_| ())
}

/// A pre-dispatch build grant lets registered builder nodes resolve the base
/// it names while it is fresh, with no `template_build_jobs` row at all: the
/// API writes the grant before `StartBuild`, and the builder that takes the
/// job may pull the base before the API learns which builder that was. Once
/// pinned to the accepting builder, the grant keeps covering that builder
/// alone well past the unpinned window, so a job queued behind long builds
/// still resolves its base when it runs, and a second build of the same
/// target accepted on another builder has a grant of its own, so both keep
/// access. Workers gain nothing from it, build history alone grants nothing,
/// stale grants of either phase have expired, and another organization's
/// grant does not cover this base.
#[tokio::test(flavor = "current_thread")]
async fn fresh_build_grant_unlocks_private_base_for_builders_only() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let stranger = Uuid::parse_str("00000000-0000-0000-0000-000000000002")?;
    let (_, hash) = seed_private_release(&fixture, "private-base", owner).await?;
    let builder_ip = std::net::Ipv4Addr::new(10, 0, 0, 40);
    let worker_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    let builder = insert_k8s_node(pool, builder_ip, "builder").await?;
    insert_k8s_node(pool, worker_ip, "worker").await?;

    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
         VALUES ($1, 'private-child', 'private-base', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .execute(pool)
    .await?;
    assert!(
        release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?,
        "a fresh grant must cover the builder before any job row exists"
    );
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_ip, None).await?);

    // Build history is not authorization: a recorded job for the owning
    // organization on this builder grants nothing once the grant is gone.
    sqlx::query(sql!("DELETE FROM template_build_base_grants"))
        .execute(pool)
        .await?;
    sqlx::query(sql!(
        "INSERT INTO template_build_jobs (template_id, build_id, org_id, builder_node_id)
         VALUES ('private-child', 'build-1', $1, $2)"
    ))
    .bind(owner)
    .bind(builder)
    .execute(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, builder_ip, Some(owner)).await?);

    // An unpinned grant ages out after the window the pin should have closed.
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants
             (id, template_id, base_template_name, org_id, granted_at)
         VALUES ($1, 'private-child', 'private-base', $2, NOW() - INTERVAL '2 hours')"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .execute(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?);

    // Pinned to the builder that accepted the job, the same aged grant keeps
    // covering that builder and only that builder: a build that waited in its
    // queue behind long jobs still resolves its base when it finally runs.
    let other_builder_ip = std::net::Ipv4Addr::new(10, 0, 0, 41);
    let other_builder = insert_k8s_node(pool, other_builder_ip, "builder").await?;
    sqlx::query(sql!(
        "UPDATE template_build_base_grants SET builder_node_id = $1
         WHERE template_id = 'private-child'"
    ))
    .bind(builder)
    .execute(pool)
    .await?;
    assert!(
        release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?,
        "a pinned grant must outlive the unpinned window for its builder"
    );
    assert!(!release_visible_from(&fixture, "private-base", &hash, other_builder_ip, None).await?);
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_ip, None).await?);

    // A second build of the same target accepted on the other builder has a
    // grant of its own; accepting it takes nothing from the first builder.
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants
             (id, template_id, base_template_name, org_id, builder_node_id)
         VALUES ($1, 'private-child', 'private-base', $2, $3)"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .bind(other_builder)
    .execute(pool)
    .await?;
    assert!(release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?);
    assert!(release_visible_from(&fixture, "private-base", &hash, other_builder_ip, None).await?);
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_ip, None).await?);

    // A pinned grant does not last forever either.
    sqlx::query(sql!(
        "UPDATE template_build_base_grants SET granted_at = NOW() - INTERVAL '8 days'
         WHERE template_id = 'private-child'"
    ))
    .execute(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?);

    // Another organization's grant for the same base name does not apply.
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
         VALUES ($1, 'stranger-child', 'private-base', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(stranger)
    .execute(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?);
    Ok(())
}

/// An enabled warm pool lets registered workers resolve the pooled template,
/// because warm slots are scheduled onto any worker before the control plane
/// records where they landed. Builders and addresses that are not registered
/// nodes gain nothing, and a disabled pool grants nothing.
#[tokio::test(flavor = "current_thread")]
async fn enabled_warm_pool_unlocks_private_release_for_registered_workers_only() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let (release_id, hash) = seed_private_release(&fixture, "private-base", owner).await?;
    let worker_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    let builder_ip = std::net::Ipv4Addr::new(10, 0, 0, 40);
    let unregistered_ip = std::net::Ipv4Addr::new(10, 0, 0, 99);
    insert_k8s_node(pool, worker_ip, "worker").await?;
    insert_k8s_node(pool, builder_ip, "builder").await?;

    let config_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         )
         VALUES ($1, 2, 4096, 1, 1, 1, 'space', FALSE, 'autoscaler')
         RETURNING id"
    ))
    .bind(release_id)
    .fetch_one(pool)
    .await?;
    assert!(!release_visible_from(&fixture, "private-base", &hash, worker_ip, None).await?);

    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET enabled = TRUE WHERE id = $1"
    ))
    .bind(config_id)
    .execute(pool)
    .await?;
    assert!(release_visible_from(&fixture, "private-base", &hash, worker_ip, None).await?);
    assert!(!release_visible_from(&fixture, "private-base", &hash, builder_ip, None).await?);
    assert!(!release_visible_from(&fixture, "private-base", &hash, unregistered_ip, None).await?);
    Ok(())
}

async fn seed_active_k8s_sandbox(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandboxes (
                 id, user_id, org_id, managed_by, lifecycle, placement,
                 active_node_id, target_node_id, command_ready_pending,
                 network_policy_generation
             )
             VALUES (
                 $1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node',
                 $4, $4, TRUE, 1
             )",
    )
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(SEEDED_ORG_ID)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_active_k8s_execution(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    generation: i64,
    pod_ip: &str,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandbox_k8s_executions (
                 sandbox_id, generation, pod_name, pod_namespace, pod_uid,
                 pod_ip, state, active
             )
             VALUES ($1, $2, $3, 'space', $4, $5::INET, 'running', TRUE)",
    )
    .bind(sandbox_id)
    .bind(generation)
    .bind(format!(
        "space-sbx-{}-g{generation}",
        sandbox_id.as_uuid().simple()
    ))
    .bind(format!("pod-uid-{generation}"))
    .bind(pod_ip)
    .execute(pool)
    .await?;
    Ok(())
}

async fn seed_applied_port_exposure(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_id: NodeId,
) -> TestResult {
    sqlx::query(
        "INSERT INTO sandbox_port_exposures (
                 sandbox_id, protocol, guest_port, visibility, source,
                 desired_state, apply_state, last_applied_node_id, last_error,
                 last_bridge_status, last_applied_bridge_generation, last_observed_at
             )
             VALUES (
                 $1, 'tcp', 9013, 'private', 'api',
                 'open', 'applied', $2, 'stale error',
                 'loopback_forwarded', 'bridge-generation-1', NOW()
             )",
    )
    .bind(sandbox_id)
    .bind(node_id)
    .execute(pool)
    .await?;
    Ok(())
}

fn published_sandbox_state(sandbox_id: SandboxId, status: &str) -> PublishedSandboxState {
    PublishedSandboxState {
        sandbox_id: sandbox_id.to_string(),
        present: true,
        status: Some(status.to_string()),
        template: Some("app-runtime".to_string()),
        content_hash: Some("sha256:test".to_string()),
        runtime: Some("fc".to_string()),
        snapshots_enabled: true,
        ..PublishedSandboxState::default()
    }
}

async fn seed_federated_sandbox(pool: &sqlx::PgPool) -> TestResult<(SandboxId, OrgId)> {
    let org_id = OrgId::new();
    let user_id = UserId::new();
    let sandbox_id = SandboxId::new();
    sqlx::query(
            "INSERT INTO organizations (
                 id, name, slug, owner_source, upstream_issuer, upstream_org_id
             )
             VALUES ($1, 'Federated Example Org', $2, 'federated_identity', 'example-issuer', 'example-upstream-org')",
        )
        .bind(org_id)
        .bind(format!("federated-{org_id}"))
        .execute(pool)
        .await?;
    sqlx::query(
        "INSERT INTO users (
                 id, email, password_hash, display_name, org_id, role,
                 owner_source, upstream_issuer, upstream_subject_id
             )
             VALUES (
                 $1, $2, 'federated-user-no-local-password', 'Federated Example User', $3, 'member',
                 'federated_identity', 'example-issuer', '123'
             )",
    )
    .bind(user_id)
    .bind(format!("federated-{user_id}@space.invalid"))
    .bind(org_id)
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO sandboxes (id, user_id, org_id, network_policy_generation)
             VALUES ($1, $2, $3, 1)",
    )
    .bind(sandbox_id)
    .bind(user_id)
    .bind(org_id)
    .execute(pool)
    .await?;
    Ok((sandbox_id, org_id))
}

/// Build a per-attempt storage URI for the test bucket. The exact shape
/// is opaque to the cluster API (it's stored verbatim and round-tripped
/// to the restore client) so the test just needs distinct strings per
/// snapshot_id to mirror real upload behavior.
fn test_storage_uri(sandbox_id: SandboxId, snapshot_id: SnapshotId) -> String {
    format!("s3://test-bucket/suspend_snapshots/{sandbox_id}/{snapshot_id}?region=us-east-1")
}

fn test_snapshot_execution_for_arch(
    arch: pplx_space_common_core::types::Arch,
) -> pplx_space_execution_common::proto::ExecutionMetadata {
    let execution = pplx_space_common_core::types::ExecutionMetadata::initial_firecracker(
        arch,
        vec![pplx_space_common_core::types::ArtifactComponentMetadata {
            kind: pplx_space_common_core::types::ArtifactComponentKind::Kernel,
            role: Some(
                pplx_space_common_core::types::ArtifactComponentRole::parse("boot").unwrap(),
            ),
            version: Some(
                pplx_space_common_core::types::ArtifactVersion::parse("6.10.14").unwrap(),
            ),
            digest: Some(
                format!("sha256:{}", "c".repeat(64))
                    .parse::<pplx_space_common_core::types::Digest>()
                    .unwrap(),
            ),
            format: Some(pplx_space_common_core::types::ArtifactFormat::parse("vmlinux").unwrap()),
        }],
    );
    (&execution).into()
}

fn test_snapshot_execution() -> pplx_space_execution_common::proto::ExecutionMetadata {
    test_snapshot_execution_for_arch(pplx_space_common_core::types::Arch::Aarch64)
}

/// Seed a `creating` suspend snapshot without artifact identity: the row
/// shape the retired single-snapshot upload flow wrote, which
/// `GetSuspendSnapshotLocation` still serves.
async fn seed_suspend_snapshot(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
) -> Result<(), tonic::Status> {
    seed_suspend_snapshot_with_storage_uri(
        service,
        sandbox_id,
        snapshot_id,
        test_storage_uri(sandbox_id, snapshot_id),
    )
    .await
}

async fn seed_suspend_snapshot_with_storage_uri(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
    storage_uri: String,
) -> Result<(), tonic::Status> {
    seed_suspend_snapshot_with_identity(
        service,
        sandbox_id,
        snapshot_id,
        storage_uri,
        test_snapshot_execution(),
    )
    .await
}

async fn seed_suspend_snapshot_with_identity(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
    snapshot_id: SnapshotId,
    storage_uri: String,
    execution: pplx_space_execution_common::proto::ExecutionMetadata,
) -> Result<(), tonic::Status> {
    let storage_uri = ExactStorageUri::parse(&storage_uri)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid storage_uri: {e}")))?;
    let execution_metadata = snapshot_execution_metadata(Some(&execution))?;
    let snapshot = service
        .lookup_snapshot_sandbox(sandbox_id)
        .await?
        .new_snapshot(
            snapshot_id,
            sandbox_id,
            SnapshotKind::Full,
            SnapshotState::Creating,
            storage_uri,
        )?
        .with_execution_metadata(execution_metadata);
    let mut tx = service
        .pool
        .begin()
        .await
        .map_err(|e| tonic::Status::internal(format!("begin tx: {e}")))?;
    snapshot_store::insert(&mut tx, &snapshot)
        .await
        .map_err(snapshot_insert_status)?;
    tx.commit()
        .await
        .map_err(|e| tonic::Status::internal(format!("commit tx: {e}")))
}

#[tokio::test]
async fn suspend_snapshot_preserves_federated_sandbox_space_org() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let (sandbox_id, org_id) = seed_federated_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    let (snapshot_org_id, storage_org_id): (OrgId, OrgId) = sqlx::query_as(
        "SELECT s.org_id, ssl.org_id
             FROM snapshots s
             JOIN snapshot_storage_locations ssl ON ssl.snapshot_id = s.id
             WHERE s.id = $1",
    )
    .bind(snapshot_id)
    .fetch_one(&fixture.service.pool)
    .await?;

    assert_eq!(snapshot_org_id, org_id);
    assert_eq!(storage_org_id, org_id);
    Ok(())
}

#[tokio::test]
async fn suspend_snapshot_rejects_reused_id_with_different_storage_uri() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();
    let original_storage_uri = test_storage_uri(sandbox_id, snapshot_id);

    seed_suspend_snapshot_with_storage_uri(
        &fixture.service,
        sandbox_id,
        snapshot_id,
        original_storage_uri.clone(),
    )
    .await?;

    let error = seed_suspend_snapshot_with_storage_uri(
        &fixture.service,
        sandbox_id,
        snapshot_id,
        format!("s3://test-bucket/alternate/{snapshot_id}?region=us-east-1"),
    )
    .await
    .expect_err("a snapshot ID must identify exactly one storage URI");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let storage_uris: Vec<String> = sqlx::query_scalar(
        "SELECT storage_connection_string
           FROM snapshot_storage_locations
          WHERE snapshot_id = $1",
    )
    .bind(snapshot_id)
    .fetch_all(&fixture.service.pool)
    .await?;
    assert_eq!(storage_uris, vec![original_storage_uri]);
    Ok(())
}

#[tokio::test]
async fn suspend_snapshot_rejects_reused_id_with_different_identity() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let other_sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();
    let storage_uri = test_storage_uri(sandbox_id, snapshot_id);

    seed_suspend_snapshot_with_identity(
        &fixture.service,
        sandbox_id,
        snapshot_id,
        storage_uri.clone(),
        test_snapshot_execution(),
    )
    .await?;

    for (case, retry_sandbox_id, retry_execution) in [
        (
            "different sandbox",
            other_sandbox_id,
            test_snapshot_execution(),
        ),
        (
            "different execution",
            sandbox_id,
            test_snapshot_execution_for_arch(pplx_space_common_core::types::Arch::X86_64),
        ),
    ] {
        let error = seed_suspend_snapshot_with_identity(
            &fixture.service,
            retry_sandbox_id,
            snapshot_id,
            storage_uri.clone(),
            retry_execution,
        )
        .await
        .expect_err(case);
        assert_eq!(error.code(), tonic::Code::FailedPrecondition, "{case}");
    }

    let artifact_attempt_id = SnapshotId::new();
    let artifact_storage_uri = test_storage_uri(sandbox_id, artifact_attempt_id);
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        LocalArtifactId::new(),
        artifact_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;
    let error = seed_suspend_snapshot_with_identity(
        &fixture.service,
        sandbox_id,
        artifact_attempt_id,
        artifact_storage_uri,
        test_snapshot_execution(),
    )
    .await
    .expect_err("an artifact-backup ID must not be reused for a suspend snapshot");
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    Ok(())
}

/// Helper to finalize a backup attempt through `FinalizeArtifactBackup`.
async fn finalize(
    service: &ClusterApiService,
    snapshot_id: SnapshotId,
    target: SuspendSnapshotTerminalState,
    bytes: i64,
) -> Result<(), tonic::Status> {
    service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: snapshot_id.to_string(),
                target_state: target as i32,
                bytes_uploaded: bytes,
                failure_reason: None,
                artifact_digests: Vec::new(),
            },
            TEST_WORKER_IP,
        ))
        .await
        .map(|_| ())
}

/// Helper to call `GetSuspendSnapshotLocation` for assertions, from the node
/// the seeded sandboxes are placed on.
async fn get_location(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
) -> Result<GetSuspendSnapshotLocationResponse, tonic::Status> {
    service
        .get_suspend_snapshot_location(request_from_node(
            GetSuspendSnapshotLocationRequest {
                sandbox_id: sandbox_id.to_string(),
            },
            TEST_WORKER_IP,
        ))
        .await
        .map(|r| r.into_inner())
}

async fn begin_artifact_backup(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    backup_attempt_id: SnapshotId,
    artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind,
    artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin,
) -> Result<(), tonic::Status> {
    begin_artifact_backup_from_node(
        service,
        TEST_WORKER_IP,
        sandbox_id,
        local_artifact_id,
        backup_attempt_id,
        artifact_kind,
        artifact_origin,
    )
    .await
}

async fn begin_artifact_backup_from_node(
    service: &ClusterApiService,
    node_ip: std::net::IpAddr,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    backup_attempt_id: SnapshotId,
    artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind,
    artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin,
) -> Result<(), tonic::Status> {
    service
        .begin_artifact_backup(request_from_node(
            BeginArtifactBackupRequest {
                sandbox_id: sandbox_id.to_string(),
                local_artifact_id: local_artifact_id.to_string(),
                artifact_kind: artifact_kind as i32,
                backup_attempt_id: backup_attempt_id.to_string(),
                storage_uri: test_storage_uri(sandbox_id, backup_attempt_id),
                artifact_origin: artifact_origin as i32,
                snapshot_name: None,
                execution: Some(test_snapshot_execution()),
                capture_host_cpu: None,
                execution_generation: None,
                fence_verdict_only: false,
            },
            node_ip,
        ))
        .await
        .map(|_| ())
}

struct BeginArtifactBackupWithStorageUriInput {
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    backup_attempt_id: SnapshotId,
    storage_uri: String,
    artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind,
    artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin,
    snapshot_name: Option<String>,
}

async fn begin_artifact_backup_with_storage_uri(
    service: &ClusterApiService,
    input: BeginArtifactBackupWithStorageUriInput,
) -> Result<(), tonic::Status> {
    service
        .begin_artifact_backup(request_from_node(
            BeginArtifactBackupRequest {
                sandbox_id: input.sandbox_id.to_string(),
                local_artifact_id: input.local_artifact_id.to_string(),
                artifact_kind: input.artifact_kind as i32,
                backup_attempt_id: input.backup_attempt_id.to_string(),
                storage_uri: input.storage_uri,
                artifact_origin: input.artifact_origin as i32,
                snapshot_name: input.snapshot_name,
                execution: Some(test_snapshot_execution()),
                capture_host_cpu: None,
                execution_generation: None,
                fence_verdict_only: false,
            },
            TEST_WORKER_IP,
        ))
        .await
        .map(|_| ())
}

async fn finalize_artifact_backup(
    service: &ClusterApiService,
    backup_attempt_id: SnapshotId,
) -> Result<(), tonic::Status> {
    finalize_artifact_backup_from_node(service, TEST_WORKER_IP, backup_attempt_id).await
}

async fn finalize_artifact_backup_from_node(
    service: &ClusterApiService,
    node_ip: std::net::IpAddr,
    backup_attempt_id: SnapshotId,
) -> Result<(), tonic::Status> {
    service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: backup_attempt_id.to_string(),
                target_state: SuspendSnapshotTerminalState::Ready as i32,
                bytes_uploaded: 42,
                failure_reason: None,
                artifact_digests: Vec::new(),
            },
            node_ip,
        ))
        .await
        .map(|_| ())
}

/// Two worker nodes at distinct addresses; the sandbox is placed on the first.
async fn seed_sandbox_on_source_node(
    pool: &sqlx::PgPool,
) -> TestResult<(SandboxId, std::net::IpAddr, std::net::IpAddr)> {
    let source_node = NodeId::new();
    let other_node = NodeId::new();
    let source_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 40));
    let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 41));
    sqlx::query(
        "INSERT INTO nodes (id, address, role, managed_by, state)
             VALUES ($1, $2::INET, 'worker', 'k8s', 'ready'),
                    ($3, $4::INET, 'worker', 'k8s', 'ready')",
    )
    .bind(source_node)
    .bind(source_ip.to_string())
    .bind(other_node)
    .bind(other_ip.to_string())
    .execute(pool)
    .await?;
    let sandbox_id = SandboxId::new();
    seed_active_k8s_sandbox(pool, sandbox_id, source_node).await?;
    Ok((sandbox_id, source_ip, other_ip))
}

/// Place the sandbox on the registered node at `node_ip`.
async fn place_sandbox_on_node_at(
    pool: &sqlx::PgPool,
    sandbox_id: SandboxId,
    node_ip: std::net::IpAddr,
) -> TestResult {
    sqlx::query(
        "UPDATE sandboxes s
            SET placement = 'on_node', lifecycle = 'started',
                active_node_id = n.id, target_node_id = n.id
           FROM nodes n
          WHERE s.id = $1 AND host(n.address) = $2",
    )
    .bind(sandbox_id)
    .bind(node_ip.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// `retention` holds an uncommitted delete of the attempt row, which makes the
/// replay's insert wait; commit once it is waiting.
async fn commit_retention_once_replay_blocks(
    pool: &sqlx::PgPool,
    retention: sqlx::Transaction<'static, sqlx::Postgres>,
) -> TestResult {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM pg_stat_activity
              WHERE wait_event_type = 'Lock'
                AND query LIKE '%INSERT INTO snapshots%'",
        )
        .fetch_one(pool)
        .await?;
        if waiting > 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            return Err("the replay never blocked on the held attempt row".into());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    retention.commit().await?;
    Ok(())
}

async fn snapshot_row_count(pool: &sqlx::PgPool, snapshot_id: SnapshotId) -> TestResult<i64> {
    Ok(
        sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(pool)
            .await?,
    )
}

async fn snapshot_state(pool: &sqlx::PgPool, snapshot_id: SnapshotId) -> TestResult<String> {
    Ok(
        sqlx::query_scalar("SELECT state FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(pool)
            .await?,
    )
}

/// Only the sandbox's source node may open a backup; a refusal inserts nothing.
#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_admits_only_the_sandboxes_source_node() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, other_ip) = seed_sandbox_on_source_node(pool).await?;
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = SnapshotId::new();
    let unknown_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 42));

    for caller in [other_ip, unknown_ip] {
        let err = begin_artifact_backup_from_node(
            &fixture.service,
            caller,
            sandbox_id,
            local_artifact_id,
            backup_attempt_id,
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
        )
        .await
        .expect_err("a node that does not hold the sandbox must not open its backup");
        assert_eq!(err.code(), Code::PermissionDenied, "{err}");
        assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    }

    begin_artifact_backup_from_node(
        &fixture.service,
        source_ip,
        sandbox_id,
        local_artifact_id,
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");

    let unplaced = seed_unplaced_sandbox(pool).await?;
    let err = begin_artifact_backup_from_node(
        &fixture.service,
        source_ip,
        unplaced,
        LocalArtifactId::new(),
        SnapshotId::new(),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await
    .expect_err("a sandbox with no node has no node that may publish for it");
    assert_eq!(err.code(), Code::FailedPrecondition, "{err}");
    Ok(())
}

/// A registered attempt stays bound to its source node across retirement and
/// relocation.
#[tokio::test(flavor = "current_thread")]
async fn begin_replay_for_a_registered_attempt_stays_bound_to_its_source_node() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, other_ip) = seed_sandbox_on_source_node(pool).await?;
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = SnapshotId::new();
    let begin_from = |node_ip| {
        begin_artifact_backup_from_node(
            &fixture.service,
            node_ip,
            sandbox_id,
            local_artifact_id,
            backup_attempt_id,
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
        )
    };
    begin_from(source_ip).await?;

    // Retirement unplaces the sandbox.
    sqlx::query(
        "UPDATE sandboxes
            SET placement = 'unplaced', lifecycle = 'stopped',
                active_node_id = NULL, target_node_id = NULL
          WHERE id = $1",
    )
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    begin_from(source_ip).await?;
    let err = begin_from(other_ip)
        .await
        .expect_err("an unplaced sandbox's attempt still belongs to the node that opened it");
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    // Relocation to the other node.
    place_sandbox_on_node_at(pool, sandbox_id, other_ip).await?;
    begin_from(source_ip).await?;
    let err = begin_from(other_ip)
        .await
        .expect_err("the replacement node did not open this attempt");
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");

    finalize_artifact_backup_from_node(&fixture.service, source_ip, backup_attempt_id).await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "ready");
    Ok(())
}

/// Retention can delete the attempt between a pre-transaction read and the
/// insert; the stale replay must still be refused with nothing committed.
#[tokio::test(flavor = "current_thread")]
async fn begin_replay_racing_retention_cannot_register_a_row_for_another_node() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(5).await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, other_ip) = seed_sandbox_on_source_node(pool).await?;
    let backup_attempt_id = SnapshotId::new();
    begin_artifact_backup_from_node(
        &fixture.service,
        source_ip,
        sandbox_id,
        LocalArtifactId::new(),
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;
    place_sandbox_on_node_at(pool, sandbox_id, other_ip).await?;

    // Retention's delete is in flight.
    let mut retention = pool.begin().await?;
    assert!(snapshot_store::delete_snapshot_catalog(&mut retention, backup_attempt_id).await?);

    let replay = begin_artifact_backup_from_node(
        &fixture.service,
        source_ip,
        sandbox_id,
        LocalArtifactId::new(),
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    );
    let (replay, retention) =
        tokio::join!(replay, commit_retention_once_replay_blocks(pool, retention));
    retention?;

    let err = replay.expect_err("a replay whose attempt row vanished must not register a new one");
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    Ok(())
}

/// Only the node that opened the attempt may finalize it; the row stays
/// `creating` otherwise.
#[tokio::test(flavor = "current_thread")]
async fn finalize_artifact_backup_admits_only_the_attempts_source_node() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let (sandbox_id, source_ip, other_ip) = seed_sandbox_on_source_node(pool).await?;
    let backup_attempt_id = SnapshotId::new();
    begin_artifact_backup_from_node(
        &fixture.service,
        source_ip,
        sandbox_id,
        LocalArtifactId::new(),
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;

    let err = finalize_artifact_backup_from_node(&fixture.service, other_ip, backup_attempt_id)
        .await
        .expect_err("a node that did not open the attempt must not finalize it");
    assert_eq!(err.code(), Code::PermissionDenied, "{err}");
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");

    finalize_artifact_backup_from_node(&fixture.service, source_ip, backup_attempt_id).await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "ready");
    Ok(())
}

/// The URI must end with `<sandbox_id>/<backup_attempt_id>`; anything above
/// that suffix is free.
#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_rejects_a_storage_uri_outside_the_attempts_prefix() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let sandbox_id = seed_sandbox(pool).await?;
    let victim_sandbox_id = SandboxId::new();
    let backup_attempt_id = SnapshotId::new();
    let other_attempt_id = SnapshotId::new();

    for foreign_uri in [
        format!(
            "s3://test-bucket/suspend_snapshots/{victim_sandbox_id}/{backup_attempt_id}?region=us-east-1"
        ),
        format!(
            "s3://test-bucket/suspend_snapshots/{sandbox_id}/{other_attempt_id}?region=us-east-1"
        ),
        format!("s3://test-bucket/suspend_snapshots/{sandbox_id}?region=us-east-1"),
    ] {
        let err = begin_artifact_backup_with_storage_uri(
            &fixture.service,
            BeginArtifactBackupWithStorageUriInput {
                sandbox_id,
                local_artifact_id: LocalArtifactId::new(),
                backup_attempt_id,
                storage_uri: foreign_uri.clone(),
                artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
                artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
                snapshot_name: None,
            },
        )
        .await
        .expect_err("a URI outside this attempt's prefix must be refused");
        assert_eq!(err.code(), Code::InvalidArgument, "{foreign_uri}: {err}");
        assert_eq!(snapshot_row_count(pool, backup_attempt_id).await?, 0);
    }

    begin_artifact_backup_with_storage_uri(
        &fixture.service,
        BeginArtifactBackupWithStorageUriInput {
            sandbox_id,
            local_artifact_id: LocalArtifactId::new(),
            backup_attempt_id,
            storage_uri: format!(
                "s3://other-bucket/clusters/abc/suspend_snapshots/7f/{sandbox_id}/{backup_attempt_id}/?region=us-east-1"
            ),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
            snapshot_name: None,
        },
    )
    .await?;
    assert_eq!(snapshot_state(pool, backup_attempt_id).await?, "creating");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_suspend_artifact_projects_stale_started_after_early_node_publish() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&fixture.service.pool, node_id).await?;
    seed_active_k8s_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    seed_active_k8s_execution(&fixture.service.pool, sandbox_id, 2, "100.64.0.2").await?;
    seed_applied_port_exposure(&fixture.service.pool, sandbox_id, node_id).await?;

    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;

    let mut conn = fixture.service.pool.acquire().await?;
    publish_sandbox_states(
        &mut conn,
        node_id,
        &[published_sandbox_state(sandbox_id, "suspended")],
    )
    .await?;
    drop(conn);

    let before_finalize: String =
        sqlx::query_scalar("SELECT lifecycle FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert_eq!(before_finalize, "started");

    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(backup_attempt_id)).await?;

    let (lifecycle, placement, active_node_id, target_node_id, command_ready_pending): (
        String,
        String,
        Option<NodeId>,
        Option<NodeId>,
        bool,
    ) = sqlx::query_as(
        "SELECT lifecycle, placement, active_node_id, target_node_id, command_ready_pending
             FROM sandboxes
             WHERE id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert_eq!(target_node_id, Some(node_id));
    assert!(!command_ready_pending);

    let (execution_active, execution_state, execution_pod_ip) =
        sqlx::query_as::<_, (bool, String, Option<String>)>(
            "SELECT active, state, pod_ip::TEXT
                 FROM sandbox_k8s_executions
                 WHERE sandbox_id = $1
                 ORDER BY generation DESC
                 LIMIT 1",
        )
        .bind(sandbox_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert!(!execution_active);
    assert_eq!(execution_state, "suspended");
    assert_eq!(execution_pod_ip, None);

    let (
        apply_state,
        last_applied_node_id,
        last_error,
        last_bridge_status,
        last_applied_bridge_generation,
        has_last_observed_at,
    ): (
        String,
        Option<NodeId>,
        Option<String>,
        Option<String>,
        Option<String>,
        bool,
    ) = sqlx::query_as(
        "SELECT apply_state,
                    last_applied_node_id,
                    last_error,
                    last_bridge_status,
                    last_applied_bridge_generation,
                    last_observed_at IS NOT NULL
             FROM sandbox_port_exposures
             WHERE sandbox_id = $1",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(apply_state, "pending");
    assert_eq!(last_applied_node_id, None);
    assert_eq!(last_error, None);
    assert_eq!(last_bridge_status, None);
    assert_eq!(last_applied_bridge_generation, None);
    assert!(!has_last_observed_at);

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_suspend_artifact_does_not_retire_api_owned_suspend() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&fixture.service.pool, node_id).await?;
    seed_active_k8s_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    seed_active_k8s_execution(&fixture.service.pool, sandbox_id, 3, "100.64.0.3").await?;

    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;

    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(backup_attempt_id)).await?;

    let (lifecycle, active, execution_state): (String, bool, String) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.active, e.state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(lifecycle, "suspending");
    assert!(active);
    assert_eq!(execution_state, "running");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_suspend_artifact_projects_committed_suspend() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&fixture.service.pool, node_id).await?;
    seed_active_k8s_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    seed_active_k8s_execution(&fixture.service.pool, sandbox_id, 4, "100.64.0.4").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;

    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'suspended'
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;
    sqlx::query(sql!(
        "UPDATE sandboxes
            SET current_local_artifact_id = $2,
                current_local_artifact_kind = 'full',
                current_local_artifact_origin = 'suspend'
          WHERE id = $1"
    ))
    .bind(sandbox_id)
    .bind(local_artifact_id)
    .execute(&fixture.service.pool)
    .await?;

    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(backup_attempt_id)).await?;

    let (lifecycle, placement, active_node_id, execution_active, execution_state): (
        String,
        String,
        Option<NodeId>,
        bool,
        String,
    ) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, s.placement, s.active_node_id, e.active, e.state
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(lifecycle, "suspended");
    assert_eq!(placement, "unplaced");
    assert_eq!(active_node_id, None);
    assert!(!execution_active);
    assert_eq!(execution_state, "suspended");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_suspend_artifact_without_committed_marker_preserves_suspend() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&fixture.service.pool, node_id).await?;
    seed_active_k8s_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    seed_active_k8s_execution(&fixture.service.pool, sandbox_id, 5, "100.64.0.5").await?;
    sqlx::query(sql!(
        "UPDATE sandboxes SET lifecycle = 'suspending' WHERE id = $1"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;

    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    sqlx::query(sql!(
        "UPDATE sandbox_k8s_executions
            SET state = 'suspended'
          WHERE sandbox_id = $1
            AND active"
    ))
    .bind(sandbox_id)
    .execute(&fixture.service.pool)
    .await?;

    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(backup_attempt_id)).await?;

    let (lifecycle, execution_active): (String, bool) = sqlx::query_as(sql!(
        "SELECT s.lifecycle, e.active
           FROM sandboxes s
           JOIN sandbox_k8s_executions e ON e.sandbox_id = s.id
          WHERE s.id = $1"
    ))
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(lifecycle, "suspending");
    assert!(execution_active);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_stop_artifact_does_not_project_started_sandbox_suspended() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    insert_ready_worker_node(&fixture.service.pool, node_id).await?;
    seed_active_k8s_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    seed_active_k8s_execution(&fixture.service.pool, sandbox_id, 1, "100.64.0.3").await?;

    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;

    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(backup_attempt_id)).await?;

    let (lifecycle, placement, active_node_id): (String, String, Option<NodeId>) =
        sqlx::query_as("SELECT lifecycle, placement, active_node_id FROM sandboxes WHERE id = $1")
            .bind(sandbox_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert_eq!(lifecycle, "started");
    assert_eq!(placement, "on_node");
    assert_eq!(active_node_id, Some(node_id));

    let active_execution_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(
                 SELECT 1
                 FROM sandbox_k8s_executions
                 WHERE sandbox_id = $1
                   AND active
                   AND state = 'running'
                   AND pod_ip IS NOT NULL
             )",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert!(active_execution_exists);
    Ok(())
}

async fn get_artifact_location(
    service: &ClusterApiService,
    sandbox_id: SandboxId,
    local_artifact_id: LocalArtifactId,
    artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind,
) -> Result<GetArtifactBackupLocationResponse, tonic::Status> {
    service
        .get_artifact_backup_location(request_from_node(
            GetArtifactBackupLocationRequest {
                sandbox_id: sandbox_id.to_string(),
                local_artifact_id: local_artifact_id.to_string(),
                artifact_kind: artifact_kind as i32,
                readiness: None,
            },
            TEST_WORKER_IP,
        ))
        .await
        .map(|response| response.into_inner())
}

#[tokio::test(flavor = "current_thread")]
async fn suspend_snapshot_projects_execution_metadata() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    let projected: (String, String, String, String, serde_json::Value) = sqlx::query_as(
        "SELECT runtime_version, guest_os, state_format_name,
                        state_format_version, execution_metadata
                   FROM snapshots
                  WHERE id = $1",
    )
    .bind(snapshot_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(projected.0, "1.15.1");
    assert_eq!(projected.1, "linux");
    assert_eq!(projected.2, "firecracker-snapshot");
    assert_eq!(projected.3, "9.0.0");
    assert_eq!(projected.4["runtime"]["kind"], "firecracker");
    // The direct-substrate restore-candidate query reads component
    // compatibility from this array, so its shape (a `role` key that is
    // omitted when absent, scalar strings for version/format) is load-bearing.
    let components = projected.4["components"]
        .as_array()
        .expect("execution_metadata carries the components array");
    let kernel = components
        .iter()
        .find(|component| component["kind"] == "kernel")
        .expect("kernel component projected into execution_metadata");
    assert_eq!(kernel["role"], "boot");
    assert_eq!(kernel["version"], "6.10.14");
    assert_eq!(kernel["format"], "vmlinux");
    Ok(())
}

/// The mixed-version backfill is gone: a backup begin without execution
/// metadata must be rejected loudly (INVALID_ARGUMENT) and must not insert
/// a `snapshots` row a finalize could later flip to `ready`.
#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_rejects_missing_execution_metadata() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = SnapshotId::new();

    let err = fixture
        .service
        .begin_artifact_backup(Request::new(BeginArtifactBackupRequest {
            sandbox_id: sandbox_id.to_string(),
            local_artifact_id: local_artifact_id.to_string(),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32,
            backup_attempt_id: backup_attempt_id.to_string(),
            storage_uri: test_storage_uri(sandbox_id, backup_attempt_id),
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop as i32,
            snapshot_name: None,
            execution: None,
            capture_host_cpu: None,
            execution_generation: None,
            fence_verdict_only: false,
        }))
        .await
        .expect_err("begin without execution metadata must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);

    let inserted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE id = $1")
        .bind(backup_attempt_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(inserted, 0, "rejected begin must not insert a snapshot row");
    Ok(())
}

/// The begin handler carries the request's capture host CPU into the
/// persisted `execution_metadata`, where restore placement is meant to read
/// it. A value that couldn't be a node label is recorded as unknown, and the
/// backup itself still succeeds.
#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_persists_capture_host_cpu() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let begin_kind = |backup_attempt_id: SnapshotId,
                      artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind,
                      artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin,
                      capture_host_cpu: Option<&str>| {
        fixture.service.begin_artifact_backup(request_from_node(
            BeginArtifactBackupRequest {
                sandbox_id: sandbox_id.to_string(),
                local_artifact_id: LocalArtifactId::new().to_string(),
                artifact_kind: artifact_kind as i32,
                backup_attempt_id: backup_attempt_id.to_string(),
                storage_uri: test_storage_uri(sandbox_id, backup_attempt_id),
                artifact_origin: artifact_origin as i32,
                snapshot_name: None,
                execution: Some(test_snapshot_execution()),
                capture_host_cpu: capture_host_cpu.map(str::to_owned),
                execution_generation: None,
                fence_verdict_only: false,
            },
            TEST_WORKER_IP,
        ))
    };
    let begin = |backup_attempt_id: SnapshotId, capture_host_cpu: Option<&str>| {
        begin_kind(
            backup_attempt_id,
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
            capture_host_cpu,
        )
    };
    let recorded_host_cpu = |backup_attempt_id: SnapshotId| {
        sqlx::query_scalar::<_, Option<String>>(sql!(
            "SELECT execution_metadata->>'host_cpu' FROM snapshots WHERE id = $1"
        ))
        .bind(backup_attempt_id)
        .fetch_optional(&fixture.service.pool)
    };

    let recorded = SnapshotId::new();
    begin(recorded, Some("genuineintel-6-85-7")).await?;
    assert_eq!(
        recorded_host_cpu(recorded).await?,
        Some(Some("genuineintel-6-85-7".to_string()))
    );

    let padded = SnapshotId::new();
    begin(padded, Some(" GenuineIntel-6-85-7\n")).await?;
    assert_eq!(
        recorded_host_cpu(padded).await?,
        Some(Some("genuineintel-6-85-7".to_string()))
    );

    let unknown = SnapshotId::new();
    begin(unknown, None).await?;
    assert_eq!(recorded_host_cpu(unknown).await?, Some(None));

    let empty = SnapshotId::new();
    begin(empty, Some("  ")).await?;
    assert_eq!(recorded_host_cpu(empty).await?, Some(None));

    let invalid = SnapshotId::new();
    begin(invalid, Some("Intel(R) Xeon(R) Platinum 8259CL")).await?;
    assert_eq!(recorded_host_cpu(invalid).await?, Some(None));

    // A disk artifact holds no vCPU state, so a sender's CPU isn't recorded.
    let disk = SnapshotId::new();
    begin_kind(
        disk,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
        Some("genuineintel-6-85-7"),
    )
    .await?;
    assert_eq!(recorded_host_cpu(disk).await?, Some(None));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn generic_artifact_backup_lookup_requires_exact_identity_and_supports_disk() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let older_full_artifact = LocalArtifactId::new();
    let older_full_attempt = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        older_full_artifact,
        SnapshotId::from_uuid(older_full_attempt),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(older_full_attempt)).await?;

    let later_full_artifact = LocalArtifactId::new();
    let error = get_artifact_location(
        &fixture.service,
        sandbox_id,
        later_full_artifact,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
    )
    .await
    .expect_err("a prior full backup must not satisfy a later artifact identity");
    assert_eq!(error.code(), Code::NotFound);

    let disk_artifact = LocalArtifactId::new();
    let disk_attempt = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        disk_artifact,
        SnapshotId::from_uuid(disk_attempt),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;
    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(disk_attempt)).await?;
    let disk_location = get_artifact_location(
        &fixture.service,
        sandbox_id,
        disk_artifact,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
    )
    .await?;
    assert_eq!(disk_location.local_artifact_id, disk_artifact.to_string());
    assert_eq!(disk_location.backup_attempt_id, disk_attempt.to_string());
    assert_eq!(
        disk_location.artifact_kind,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk as i32
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_rejects_reused_attempt_with_different_identity_or_uri() -> TestResult
{
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let backup_attempt_id = Uuid::now_v7();
    let original_artifact = LocalArtifactId::new();
    let conflicting_artifact = LocalArtifactId::new();

    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        original_artifact,
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;

    let err = begin_artifact_backup_with_storage_uri(
        &fixture.service,
        BeginArtifactBackupWithStorageUriInput {
            sandbox_id,
            local_artifact_id: original_artifact,
            backup_attempt_id: SnapshotId::from_uuid(backup_attempt_id),
            storage_uri: format!(
                "s3://test-bucket/alternate/{sandbox_id}/{backup_attempt_id}?region=us-east-1"
            ),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
            snapshot_name: None,
        },
    )
    .await
    .expect_err("reusing a backup_attempt_id for a different URI must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);

    let err = begin_artifact_backup_with_storage_uri(
        &fixture.service,
        BeginArtifactBackupWithStorageUriInput {
            sandbox_id,
            local_artifact_id: conflicting_artifact,
            backup_attempt_id: SnapshotId::from_uuid(backup_attempt_id),
            storage_uri: format!(
                "s3://test-bucket/conflicting/{sandbox_id}/{backup_attempt_id}?region=us-east-1"
            ),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
            snapshot_name: None,
        },
    )
    .await
    .expect_err("reusing a backup_attempt_id for different bytes must fail");
    assert_eq!(err.code(), Code::FailedPrecondition);

    let storage_locations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM snapshot_storage_locations WHERE snapshot_id = $1",
    )
    .bind(backup_attempt_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(
        storage_locations, 1,
        "conflicting retry must not attach a second storage URI"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_begin_artifact_backup_is_idempotent() -> TestResult {
    const CONCURRENT_CALLS: usize = 32;

    let Some(fixture) = cluster_fixture_with_max_connections(CONCURRENT_CALLS as u32).await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = Uuid::now_v7();
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(CONCURRENT_CALLS));

    let mut handles = Vec::new();
    for _ in 0..CONCURRENT_CALLS {
        let service = fixture.service.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            begin_artifact_backup(
                &service,
                sandbox_id,
                local_artifact_id,
                SnapshotId::from_uuid(backup_attempt_id),
                pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
                pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
            )
            .await
        }));
    }

    for handle in handles {
        handle.await??;
    }
    let snapshot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE id = $1")
        .bind(backup_attempt_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(snapshot_count, 1);
    let storage_location_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM snapshot_storage_locations WHERE snapshot_id = $1",
    )
    .bind(backup_attempt_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(storage_location_count, 1);
    Ok(())
}

// The insert holds its transaction open while authorizing the source node.
// Concurrent callers each hold a pooled connection, so the authorization
// lookup must not need a second one; one connection makes that deterministic.
#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_completes_on_a_single_pooled_connection() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(1).await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        LocalArtifactId::new(),
        SnapshotId::from_uuid(Uuid::now_v7()),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_rejects_missing_or_invalid_snapshot_name() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let backup_attempt_id = SnapshotId::new();
    let local_artifact_id = LocalArtifactId::new();

    let err = begin_artifact_backup_with_storage_uri(
        &fixture.service,
        BeginArtifactBackupWithStorageUriInput {
            sandbox_id,
            local_artifact_id,
            backup_attempt_id,
            storage_uri: test_storage_uri(sandbox_id, backup_attempt_id),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot,
            snapshot_name: Some(" \t ".to_string()),
        },
    )
    .await
    .expect_err("blank snapshot_name must not satisfy SNAPSHOT origin");
    assert_eq!(err.code(), Code::InvalidArgument);

    let invalid_backup_attempt_id = SnapshotId::new();
    let err = begin_artifact_backup_with_storage_uri(
        &fixture.service,
        BeginArtifactBackupWithStorageUriInput {
            sandbox_id,
            local_artifact_id,
            backup_attempt_id: invalid_backup_attempt_id,
            storage_uri: test_storage_uri(sandbox_id, invalid_backup_attempt_id),
            artifact_kind: pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            artifact_origin: pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Snapshot,
            snapshot_name: Some("../checkpoint".to_string()),
        },
    )
    .await
    .expect_err("path-unsafe snapshot_name must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn begin_artifact_backup_accepts_unnamed_fork_disk_artifacts() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let backup_attempt_id = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        LocalArtifactId::new(),
        SnapshotId::from_uuid(backup_attempt_id),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Disk,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Fork,
    )
    .await?;

    let (origin, snapshot_name): (String, Option<String>) = sqlx::query_as(
        "SELECT artifact_origin, artifact_snapshot_name FROM snapshots WHERE id = $1",
    )
    .bind(backup_attempt_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(origin, "fork");
    assert_eq!(snapshot_name, None);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_suspend_lookup_ignores_newer_non_suspend_full_artifacts() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let suspend_snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, suspend_snapshot_id).await?;
    finalize(
        &fixture.service,
        suspend_snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        123,
    )
    .await?;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let stop_full_artifact = LocalArtifactId::new();
    let stop_full_attempt = Uuid::now_v7();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        stop_full_artifact,
        SnapshotId::from_uuid(stop_full_attempt),
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Stop,
    )
    .await?;
    finalize_artifact_backup(&fixture.service, SnapshotId::from_uuid(stop_full_attempt)).await?;

    let location = get_location(&fixture.service, sandbox_id).await?;
    assert_eq!(
        location.snapshot_id,
        suspend_snapshot_id.to_string(),
        "legacy suspend lookup must not return stop/snapshot-origin full artifacts"
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn begin_inserts_creating_row_invisible_to_get_location() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    // Row exists in DB ...
    let state: String = sqlx::query_scalar("SELECT state FROM snapshots WHERE id = $1")
        .bind(snapshot_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(state, "creating");

    // ... but is invisible to restore-side readers until finalize.
    let err = get_location(&fixture.service, sandbox_id)
        .await
        .expect_err("creating row must not be visible to GetSuspendSnapshotLocation");
    assert_eq!(err.code(), Code::NotFound);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn begin_is_idempotent_on_snapshot_id() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    // Second begin with the same snapshot_id is a no-op (sbx-mgr retry).
    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE sandbox_id = $1")
        .bind(sandbox_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(count, 1, "Begin retry must not duplicate the row");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_suspend_snapshot_inserts_preserve_one_storage_uri() -> TestResult {
    const CONCURRENT_CALLS: usize = 32;

    let Some(fixture) = cluster_fixture_with_max_connections(CONCURRENT_CALLS as u32).await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();
    let storage_uris = [
        test_storage_uri(sandbox_id, snapshot_id),
        format!("s3://test-bucket/alternate/{snapshot_id}?region=us-east-1"),
    ];
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(CONCURRENT_CALLS));

    let mut handles = Vec::new();
    for index in 0..CONCURRENT_CALLS {
        let service = fixture.service.clone();
        let barrier = barrier.clone();
        let storage_uri = storage_uris[index % storage_uris.len()].clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            seed_suspend_snapshot_with_storage_uri(&service, sandbox_id, snapshot_id, storage_uri)
                .await
        }));
    }

    let mut successes = 0;
    let mut mismatches = 0;
    for handle in handles {
        match handle.await? {
            Ok(()) => successes += 1,
            Err(error) => {
                assert_eq!(error.code(), Code::FailedPrecondition);
                mismatches += 1;
            }
        }
    }
    assert_eq!(successes, CONCURRENT_CALLS / storage_uris.len());
    assert_eq!(mismatches, CONCURRENT_CALLS / storage_uris.len());

    let snapshot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM snapshots WHERE id = $1")
        .bind(snapshot_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(snapshot_count, 1);

    let persisted_storage_uris: Vec<String> = sqlx::query_scalar(
        "SELECT storage_connection_string
         FROM snapshot_storage_locations
         WHERE snapshot_id = $1",
    )
    .bind(snapshot_id)
    .fetch_all(&fixture.service.pool)
    .await?;
    assert_eq!(persisted_storage_uris.len(), 1);
    assert!(storage_uris.contains(&persisted_storage_uris[0]));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_to_ready_makes_row_visible_to_get_location() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        12345,
    )
    .await?;

    let loc = get_location(&fixture.service, sandbox_id).await?;
    assert_eq!(loc.snapshot_id, snapshot_id.to_string());
    assert_eq!(loc.bytes_uploaded, 12345);
    assert!(loc.storage_uri.contains(&snapshot_id.to_string()));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_to_failed_keeps_row_invisible_to_get_location() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Failed,
        0,
    )
    .await?;

    let err = get_location(&fixture.service, sandbox_id)
        .await
        .expect_err("failed row must not be visible to GetSuspendSnapshotLocation");
    assert_eq!(err.code(), Code::NotFound);
    Ok(())
}

/// `FinalizeArtifactBackup` with `target_state=FAILED` persists the
/// caller-supplied `failure_reason` to the row so operators can read
/// the cause off a failed attempt later without correlating logs.
#[tokio::test(flavor = "current_thread")]
async fn finalize_failed_persists_failure_reason() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    let reason = "vol-mgr submit_push_file failed for 2 of 3 artifacts";
    fixture
        .service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: snapshot_id.to_string(),
                target_state: SuspendSnapshotTerminalState::Failed as i32,
                bytes_uploaded: 0,
                failure_reason: Some(reason.to_string()),
                artifact_digests: Vec::new(),
            },
            TEST_WORKER_IP,
        ))
        .await?;

    let persisted: Option<String> =
        sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert_eq!(persisted.as_deref(), Some(reason));
    Ok(())
}

/// When the caller omits `failure_reason`, the column stays NULL —
/// no spurious empty-string or "unknown" placeholder. Operators
/// reading the column can distinguish "no reason was reported" from
/// "the reason was empty".
#[tokio::test(flavor = "current_thread")]
async fn finalize_failed_without_reason_leaves_column_null() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Failed,
        0,
    )
    .await?;

    let persisted: Option<String> =
        sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert!(
        persisted.is_none(),
        "expected NULL failure_reason when caller omits it; got {persisted:?}"
    );
    Ok(())
}

/// A whitespace-only `failure_reason` is treated as absent (NULL),
/// not stored verbatim — matches the typical pattern at SPACE's
/// proto boundaries where empty / whitespace strings are
/// indistinguishable from missing.
#[tokio::test(flavor = "current_thread")]
async fn finalize_failed_blank_reason_is_treated_as_absent() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    fixture
        .service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: snapshot_id.to_string(),
                target_state: SuspendSnapshotTerminalState::Failed as i32,
                bytes_uploaded: 0,
                failure_reason: Some("   \t  ".to_string()),
                artifact_digests: Vec::new(),
            },
            TEST_WORKER_IP,
        ))
        .await?;

    let persisted: Option<String> =
        sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert!(persisted.is_none(), "blank reason must persist as NULL");
    Ok(())
}

/// `target_state=READY` always clears any prior-attempt
/// `failure_reason`. The column reflects only the row's *terminal*
/// state, not the history of finalize attempts.
#[tokio::test(flavor = "current_thread")]
async fn finalize_ready_clears_failure_reason() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;

    // Pre-set a failure_reason out-of-band so we can verify the
    // ready-path clears it. (Going from failed → ready via the RPC
    // is rejected as cross-terminal — covered by another test —
    // so we plant the value via SQL.)
    sqlx::query("UPDATE snapshots SET failure_reason = $2 WHERE id = $1")
        .bind(snapshot_id)
        .bind("simulated prior-attempt reason")
        .execute(&fixture.service.pool)
        .await?;

    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        1024,
    )
    .await?;

    let persisted: Option<String> =
        sqlx::query_scalar("SELECT failure_reason FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;
    assert!(
        persisted.is_none(),
        "ready path must clear failure_reason; got {persisted:?}"
    );
    Ok(())
}

/// Regression: finalizing must leave `timestamp` alone. The portability
/// gate's `timestamp >= placement_changed_at` predicate breaks if a state
/// transition rewrites it.
#[tokio::test(flavor = "current_thread")]
async fn finalize_does_not_update_timestamp() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    let original_timestamp: OffsetDateTime =
        sqlx::query_scalar("SELECT timestamp FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;

    // Force a meaningful clock delta between begin and finalize.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        42,
    )
    .await?;

    let finalized_timestamp: OffsetDateTime =
        sqlx::query_scalar("SELECT timestamp FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;

    assert_eq!(
        finalized_timestamp, original_timestamp,
        "Finalize must not overwrite the immutable creation timestamp"
    );
    Ok(())
}

/// Two completed suspends on the same sandbox produce two distinct rows
/// (not an overwrite) and the latest-by-creation-time wins for restore.
#[tokio::test(flavor = "current_thread")]
async fn two_suspends_produce_distinct_rows_latest_wins() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_a = SnapshotId::new();
    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_a).await?;
    finalize(
        &fixture.service,
        snapshot_a,
        SuspendSnapshotTerminalState::Ready,
        100,
    )
    .await?;

    // Sleep so the second UUIDv7 (and `timestamp DEFAULT NOW()`) is
    // strictly after the first.
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    let snapshot_b = SnapshotId::new();
    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_b).await?;
    finalize(
        &fixture.service,
        snapshot_b,
        SuspendSnapshotTerminalState::Ready,
        200,
    )
    .await?;

    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM snapshots WHERE sandbox_id = $1 AND state = 'ready'",
    )
    .bind(sandbox_id)
    .fetch_one(&fixture.service.pool)
    .await?;
    assert_eq!(count, 2, "both attempts must coexist as distinct rows");

    let loc = get_location(&fixture.service, sandbox_id).await?;
    assert_eq!(loc.snapshot_id, snapshot_b.to_string());
    assert_eq!(loc.bytes_uploaded, 200);
    Ok(())
}

/// Core regression for the read-during-upload race: a second suspend
/// that starts but is still uploading must never displace an already-
/// ready snapshot. Under the old per-sandbox-prefix overwrite model the
/// second upload's bytes could be observed mid-flight by readers that
/// pulled from S3 using the registered URI; under immutability the
/// `creating` row is invisible until finalize.
#[tokio::test(flavor = "current_thread")]
async fn mid_flight_attempt_does_not_displace_existing_ready() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;

    // Older attempt finalized successfully.
    let ready_id = SnapshotId::new();
    seed_suspend_snapshot(&fixture.service, sandbox_id, ready_id).await?;
    finalize(
        &fixture.service,
        ready_id,
        SuspendSnapshotTerminalState::Ready,
        100,
    )
    .await?;

    tokio::time::sleep(std::time::Duration::from_millis(10)).await;

    // Newer attempt begins (still uploading; never finalized).
    let in_flight_id = SnapshotId::new();
    seed_suspend_snapshot(&fixture.service, sandbox_id, in_flight_id).await?;

    // Restore-side read must return the ready row, not the in-flight
    // creating row, even though the creating row is newer.
    let loc = get_location(&fixture.service, sandbox_id).await?;
    assert_eq!(
        loc.snapshot_id,
        ready_id.to_string(),
        "in-flight (creating) row must be invisible to restore",
    );
    assert_eq!(loc.bytes_uploaded, 100);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn crossing_terminal_states_rejected_as_failed_precondition() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        10,
    )
    .await?;

    let err = finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Failed,
        0,
    )
    .await
    .expect_err("ready → failed must be refused");
    assert_eq!(err.code(), Code::FailedPrecondition);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_to_same_terminal_state_is_idempotent() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        50,
    )
    .await?;
    finalize(
        &fixture.service,
        snapshot_id,
        SuspendSnapshotTerminalState::Ready,
        50,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn finalize_unknown_snapshot_returns_not_found() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let _ = seed_sandbox(&fixture.service.pool).await?;
    let unknown_id = SnapshotId::new();
    let err = finalize(
        &fixture.service,
        unknown_id,
        SuspendSnapshotTerminalState::Ready,
        1,
    )
    .await
    .expect_err("finalize of unknown snapshot_id must be NotFound");
    assert_eq!(err.code(), Code::NotFound);
    Ok(())
}

/// Helper to heartbeat a backup attempt through `TouchArtifactBackup`.
async fn touch(service: &ClusterApiService, snapshot_id: SnapshotId) -> Result<(), tonic::Status> {
    service
        .touch_artifact_backup(Request::new(TouchArtifactBackupRequest {
            backup_attempt_id: snapshot_id.to_string(),
        }))
        .await
        .map(|_| ())
}

/// Read the (nullable) `last_attempt_at` heartbeat column.
async fn last_attempt_at(
    pool: &sqlx::PgPool,
    snapshot_id: SnapshotId,
) -> TestResult<Option<OffsetDateTime>> {
    Ok(
        sqlx::query_scalar("SELECT last_attempt_at FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(pool)
            .await?,
    )
}

/// `TouchArtifactBackup` bumps `last_attempt_at` on a `creating` row.
/// A new row leaves it NULL (the reaper falls back to
/// `timestamp`), and the heartbeat is the first thing to populate it.
#[tokio::test(flavor = "current_thread")]
async fn touch_bumps_last_attempt_at_for_creating_row() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let snapshot_id = SnapshotId::new();

    seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
    assert!(
        last_attempt_at(&fixture.service.pool, snapshot_id)
            .await?
            .is_none(),
        "begin must leave last_attempt_at NULL"
    );

    touch(&fixture.service, snapshot_id).await?;
    let after = last_attempt_at(&fixture.service.pool, snapshot_id).await?;
    assert!(
        after.is_some(),
        "touch must populate last_attempt_at for a creating row"
    );

    // A second heartbeat advances it (monotonic non-decreasing).
    let first = after.unwrap();
    touch(&fixture.service, snapshot_id).await?;
    let second = last_attempt_at(&fixture.service.pool, snapshot_id)
        .await?
        .expect("last_attempt_at must remain set after a second touch");
    assert!(
        second >= first,
        "repeated touch must not move last_attempt_at backwards"
    );
    Ok(())
}

/// A heartbeat against a terminal (`ready`/`failed`) row is a silent
/// no-op: no error, no state change, and `last_attempt_at` is not
/// resurrected. The reaper only ages `creating` rows, so a beat that
/// races a finalize must not rewrite the finalized row.
#[tokio::test(flavor = "current_thread")]
async fn touch_is_noop_on_terminal_rows() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };

    for (target, expected_state) in [
        (SuspendSnapshotTerminalState::Ready, "ready"),
        (SuspendSnapshotTerminalState::Failed, "failed"),
    ] {
        let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
        let snapshot_id = SnapshotId::new();

        seed_suspend_snapshot(&fixture.service, sandbox_id, snapshot_id).await?;
        finalize(&fixture.service, snapshot_id, target, 0).await?;

        // Terminalizing does not touch the heartbeat column, so it is
        // still NULL going into the touch.
        assert!(
            last_attempt_at(&fixture.service.pool, snapshot_id)
                .await?
                .is_none(),
            "{target:?} row should have NULL last_attempt_at before touch"
        );

        // Touch succeeds (no error) but must not write the column.
        touch(&fixture.service, snapshot_id).await?;
        assert!(
            last_attempt_at(&fixture.service.pool, snapshot_id)
                .await?
                .is_none(),
            "touch must not bump last_attempt_at on a {target:?} row"
        );

        let state: String = sqlx::query_scalar("SELECT state FROM snapshots WHERE id = $1")
            .bind(snapshot_id)
            .fetch_one(&fixture.service.pool)
            .await?;
        assert_eq!(
            state, expected_state,
            "touch must not change the terminal state"
        );
    }
    Ok(())
}

/// A heartbeat for a snapshot_id that doesn't exist is a silent no-op,
/// not a `NOT_FOUND`. The heartbeat is advisory; the worker fires it
/// best-effort and swallows the result, so there's nothing to gain from
/// surfacing an error it would only discard.
#[tokio::test(flavor = "current_thread")]
async fn touch_unknown_snapshot_is_noop() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let unknown_id = SnapshotId::new();
    touch(&fixture.service, unknown_id)
        .await
        .expect("touch of unknown snapshot_id must be a no-op, not an error");
    Ok(())
}

/// A malformed `snapshot_id` is still rejected at the wire boundary —
/// the no-op-on-missing leniency is for well-formed-but-absent ids, not
/// garbage input.
#[tokio::test(flavor = "current_thread")]
async fn touch_invalid_snapshot_id_is_invalid_argument() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let err = fixture
        .service
        .touch_artifact_backup(Request::new(TouchArtifactBackupRequest {
            backup_attempt_id: "not-a-uuid".to_string(),
        }))
        .await
        .expect_err("malformed snapshot_id must be rejected");
    assert_eq!(err.code(), Code::InvalidArgument);
    Ok(())
}

const VISIBILITY_TEST_DIGEST: &str =
    "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
const VISIBILITY_TEST_STORAGE: &str =
    "s3://bucket/templates/code-runtime/sha256:cccc?region=us-east-1";

/// If the control-plane schema lacks the `templates.visibility` column
/// — this binary, which queries the column, running against a database
/// that predates the `086_templates_visibility` migration (new server,
/// old schema; the reverse pairing can't arise, since a server old
/// enough to predate the migration never queries the column) — the
/// lookup must degrade to UNSPECIFIED and, critically, must not take
/// down release resolution itself, because the same RPCs serve
/// on-demand sandbox-create pulls. The migration now always adds the
/// column in the test chain, so we recreate the old-schema shape by
/// dropping it (CASCADE also removes the `org_specific_requires_owner`
/// CHECK that references it).
///
/// Covers all three call sites, including `find_template_release_by_hash`:
/// each is a separate `template_visibility` call and a separate response
/// field, so a miss on any one of them is a distinct bug.
#[tokio::test(flavor = "current_thread")]
async fn template_visibility_degrades_to_unspecified_without_column() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    // Registration records the template's launch scope, so it needs the
    // column; the degradation under test is release resolution reading a
    // schema that lost it afterwards.
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;
    sqlx::query("ALTER TABLE templates DROP COLUMN visibility CASCADE")
        .execute(&fixture.service.pool)
        .await?;

    let by_tag = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner();
    assert!(by_tag.release.is_some(), "resolution must still succeed");
    assert_eq!(
        by_tag.template_visibility(),
        TemplateVisibility::Unspecified
    );

    let by_hash = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: VISIBILITY_TEST_DIGEST.to_string(),
        }))
        .await?
        .into_inner();
    assert!(by_hash.release.is_some(), "resolution must still succeed");
    assert_eq!(
        by_hash.template_visibility(),
        TemplateVisibility::Unspecified
    );

    let latest = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 5,
        }))
        .await?
        .into_inner();
    assert_eq!(latest.entries.len(), 1);
    assert_eq!(
        latest.entries[0].template_visibility(),
        TemplateVisibility::Unspecified
    );
    assert_eq!(
        latest.template_visibility(),
        TemplateVisibility::Unspecified,
        "response-level field degrades too, not just the per-entry one"
    );
    Ok(())
}

/// The visibility lookup must read from `visibility_pool`, not `pool`.
///
/// This is the whole point of the separate pool: the release queries never
/// touch `templates`, so DDL on that table can only reach release resolution
/// by way of the visibility lookup draining the shared pool. Wiring the lookup
/// back to `self.pool` would restore that path silently — every other test
/// here passes either way, because with one pool the two are the same object.
///
/// Pinned behaviourally rather than by inspecting a field: the injected pool
/// points at a database where `templates.visibility` still exists while the
/// service's main pool points at one where it has been dropped. Only a lookup
/// that really uses the injected pool can report GENERIC here.
#[tokio::test(flavor = "current_thread")]
async fn visibility_lookup_reads_from_the_visibility_pool() -> TestResult {
    let Some(main) = cluster_fixture().await? else {
        return Ok(());
    };
    // The visibility cache has to be off here. This test makes three successive
    // calls for one template to pin that each of the three call sites reads the
    // sideline pool; with the cache live, calls two and three answer from the
    // freshness window without issuing a statement, so their assertions stop
    // observing which pool the call site uses and the test passes with two of the
    // three regressions present.
    let main = with_visibility_cache(main, disabled_visibility_cache());
    let Some(sideline) = cluster_fixture().await? else {
        return Ok(());
    };

    for service in [&main.service, &sideline.service] {
        register_template(service, "code-runtime").await?;
        register_release(
            service,
            VISIBILITY_TEST_DIGEST,
            &["latest"],
            &[VISIBILITY_TEST_STORAGE],
        )
        .await?;
    }
    // Main pool's database loses the column; the sideline's keeps it.
    // Registration records launch scope and so ran first, above.
    sqlx::query("ALTER TABLE templates DROP COLUMN visibility CASCADE")
        .execute(&main.service.pool)
        .await?;

    let service = main
        .service
        .clone()
        .with_visibility_pool(sideline.service.pool.clone());
    let response = service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner();

    assert!(
        response.release.is_some(),
        "the release must still resolve from the main pool"
    );
    assert_eq!(
        response.template_visibility(),
        TemplateVisibility::Generic,
        "visibility must come from the injected pool, whose schema still has \
         the column; reading it from the main pool would report UNSPECIFIED"
    );

    // All three call sites, for the same reason the column-missing test covers
    // all three: each is a separate `template_visibility` call, so wiring any
    // one of them back to the shared pool is its own distinct bug.
    let by_hash = service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: VISIBILITY_TEST_DIGEST.to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        by_hash.template_visibility(),
        TemplateVisibility::Generic,
        "find_template_release_by_hash must also read the injected pool"
    );

    let latest = service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 5,
        }))
        .await?
        .into_inner();
    assert_eq!(
        latest.template_visibility(),
        TemplateVisibility::Generic,
        "list_latest_template_releases must also read the injected pool"
    );
    Ok(())
}

/// The unrecognized-value arm of the mapping. Unreachable through the
/// database as shipped — `086_templates_visibility` constrains the column
/// to `('generic','org_specific')` — so the test drops that CHECK to reach
/// it. Worth covering even so: it is a distinct branch with its own metric
/// and log, and without a test it would survive being deleted outright.
#[tokio::test(flavor = "current_thread")]
async fn template_visibility_unrecognized_value_is_unspecified() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;
    sqlx::query("ALTER TABLE templates DROP CONSTRAINT templates_visibility_check")
        .execute(&fixture.service.pool)
        .await?;
    sqlx::query("UPDATE templates SET visibility = 'public' WHERE name = $1")
        .bind("code-runtime")
        .execute(&fixture.service.pool)
        .await?;

    let by_tag = fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner();
    assert!(
        by_tag.release.is_some(),
        "an unparseable visibility must not fail release resolution"
    );
    assert_eq!(
        by_tag.template_visibility(),
        TemplateVisibility::Unspecified
    );
    Ok(())
}

/// With the `templates.visibility` column present (the
/// `086_templates_visibility` migration is in the chain), the release
/// responses carry the mapped enum: `generic` → GENERIC and
/// `org_specific` → ORG_SPECIFIC. The template is registered with an
/// owner so the migration's `org_specific_requires_owner` CHECK is
/// satisfied when we flip it. The unrecognized-value → UNSPECIFIED
/// branch of the mapping is now unreachable through the database (the
/// migration's `visibility IN ('generic','org_specific')` CHECK
/// forbids any other value); its sibling UNSPECIFIED path — column
/// absent — is exercised by
/// `template_visibility_degrades_to_unspecified_without_column`.
#[tokio::test(flavor = "current_thread")]
async fn template_visibility_maps_column_values() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, disabled_visibility_cache());
    let tenant_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;

    let find_by_tag = |tag: String| {
        let service = fixture.service.clone();
        async move {
            service
                .find_template_release(Request::new(FindTemplateReleaseRequest {
                    generic_only: false,
                    caller_org_id: None,
                    template_name: "code-runtime".to_string(),
                    runtime: "firecracker".to_string(),
                    tag,
                }))
                .await
                .map(|resp| resp.into_inner())
        }
    };

    assert_eq!(
        find_by_tag("latest".to_string())
            .await?
            .template_visibility(),
        TemplateVisibility::Generic
    );

    // The flip below has to be visible to the very next call, so this test runs
    // with the visibility cache disabled. It verifies the column-value mapping,
    // not the cache; that a live flip is *not* immediately visible under the
    // production configuration is the cache's documented staleness window, and
    // it has its own tests below.
    sqlx::query("UPDATE templates SET visibility = 'org_specific', org_id = $2 WHERE name = $1")
        .bind("code-runtime")
        .bind(tenant_org_id)
        .execute(&fixture.service.pool)
        .await?;
    assert_eq!(
        find_by_tag("latest".to_string())
            .await?
            .template_visibility(),
        TemplateVisibility::OrgSpecific
    );

    let by_hash = fixture
        .service
        .find_template_release_by_hash(Request::new(FindTemplateReleaseByHashRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            hash: VISIBILITY_TEST_DIGEST.to_string(),
        }))
        .await?
        .into_inner();
    assert_eq!(
        by_hash.template_visibility(),
        TemplateVisibility::OrgSpecific
    );

    let latest = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 5,
        }))
        .await?
        .into_inner();
    assert_eq!(latest.entries.len(), 1);
    assert_eq!(
        latest.entries[0].template_visibility(),
        TemplateVisibility::OrgSpecific
    );
    Ok(())
}

/// The response-level visibility on ListLatestTemplateReleases must
/// be set even when the template has zero releases — that's what
/// lets the prefetch loop drop stale pins for an org-specific
/// template whose releases are all gone (there are no entries left
/// to carry the per-entry field).
#[tokio::test(flavor = "current_thread")]
async fn list_latest_carries_visibility_with_zero_releases() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let tenant_org_id = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    register_template(&fixture.service, "code-runtime").await?;
    sqlx::query("UPDATE templates SET visibility = 'org_specific', org_id = $2 WHERE name = $1")
        .bind("code-runtime")
        .bind(tenant_org_id)
        .execute(&fixture.service.pool)
        .await?;

    let latest = fixture
        .service
        .list_latest_template_releases(Request::new(ListLatestTemplateReleasesRequest {
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            limit: 5,
        }))
        .await?
        .into_inner();
    assert!(latest.entries.is_empty());
    assert_eq!(
        latest.template_visibility(),
        TemplateVisibility::OrgSpecific
    );
    Ok(())
}

/// Seed one successful visibility read for `code-runtime`, then make the column
/// unreachable, so a later lookup can only answer from the cache.
///
/// Dropping the column is the sharpest available failure injection: it is a
/// genuine `Err` from the same statement, which is the arm the fallback hangs
/// off, and unlike a killed pool it cannot be confused with an acquire timeout.
async fn seed_then_break_visibility(
    fixture: &ClusterFixture,
    breaking_sql: &'static str,
) -> TestResult {
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;

    assert_eq!(
        visibility_of_find(fixture).await?,
        TemplateVisibility::Generic,
        "the seeding read must have succeeded, or the fallback has nothing to serve"
    );

    sqlx::query(breaking_sql)
        .execute(&fixture.service.pool)
        .await?;
    Ok(())
}

/// Breaks it with `42703 undefined_column` — a *durable* failure, so it must
/// invalidate and degrade rather than fall back.
const BREAK_VISIBILITY_COLUMN_MISSING: &str =
    "ALTER TABLE templates DROP COLUMN visibility CASCADE";

/// Breaks it with `42P01 undefined_table`, also durable. Kept distinct from the
/// dropped column so the durable set is exercised at more than one SQLSTATE.
const BREAK_VISIBILITY_TABLE_MISSING: &str = "ALTER TABLE templates RENAME TO templates_hidden";

async fn visibility_of_find(fixture: &ClusterFixture) -> TestResult<TemplateVisibility> {
    Ok(fixture
        .service
        .find_template_release(Request::new(FindTemplateReleaseRequest {
            generic_only: false,
            caller_org_id: None,
            template_name: "code-runtime".to_string(),
            runtime: "firecracker".to_string(),
            tag: "latest".to_string(),
        }))
        .await?
        .into_inner()
        .template_visibility())
}

/// The bound on the above: with no usable cached read the lookup degrades
/// exactly as it did before the cache — what makes this a bounded absorption of
/// a blip rather than a permanent carve-out.
///
/// A zero ceiling is how that state is reached here, which is the kill switch's
/// configuration rather than the aging path: `get_within` short-circuits on a
/// zero window before comparing ages. The aging comparison itself is covered by
/// the unit tests, which can move the clock; an RPC-level test would have to
/// sleep out a real ceiling to reach it. Named for what it checks rather than
/// for the aging path it does not take.
///
/// Note it is over-determined, so treat it as a wiring check rather than a guard
/// on either mechanism: with both windows at zero, `store` never writes *and*
/// `get_within` never reads, so `UNSPECIFIED` is guaranteed twice and deleting
/// either short-circuit leaves this green. The per-guard behavior is pinned in
/// the cache's own unit tests, which can configure the two windows apart.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_visibility_lookup_degrades_when_the_cache_is_off() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, disabled_visibility_cache());
    seed_then_break_visibility(&fixture, BREAK_VISIBILITY_TABLE_MISSING).await?;

    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Unspecified,
        "with no usable cached read the lookup must degrade as it did before the cache"
    );
    Ok(())
}

/// The load half: inside the freshness window the lookup does not query at all.
///
/// Attributed without reading a counter, by making the query fatal if it ran:
/// the `templates` table is renamed out from under the lookup and the staleness
/// ceiling is zero, so any answer other than UNSPECIFIED proves no statement was
/// issued.
#[tokio::test(flavor = "current_thread")]
async fn a_fresh_visibility_read_is_answered_without_querying() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, visibility_cache(600, 0));
    seed_then_break_visibility(&fixture, BREAK_VISIBILITY_TABLE_MISSING).await?;

    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Generic,
        "a fresh entry must be served without a query; a query here could only degrade"
    );
    Ok(())
}

/// Seed one successful read, then make the visibility lookup fail *transiently*
/// by closing the pool it runs on.
///
/// This is the only injection here that is genuinely transient. Every schema-level
/// break (dropped column, renamed table) is durable, and durable failures
/// deliberately invalidate rather than fall back — so a schema break cannot be
/// used to exercise the fallback at all. Closing the sideline pool produces a
/// `PoolClosed`, which carries no `DatabaseError` and is not a decode error, so
/// it lands in the transient class. The release query is unaffected because it
/// runs on the service's general pool.
async fn seed_then_break_visibility_transiently(
    fixture: ClusterFixture,
    sideline: &ClusterFixture,
) -> TestResult<ClusterFixture> {
    let fixture = ClusterFixture {
        service: fixture
            .service
            .with_visibility_pool(sideline.service.pool.clone()),
        _db: fixture._db,
    };
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;
    // Seed the sideline database with the same template so the seeding read
    // succeeds there too, then close the pool underneath it.
    register_template(&sideline.service, "code-runtime").await?;
    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Generic,
        "the seeding read must have succeeded, or the fallback has nothing to serve"
    );
    sideline.service.pool.close().await;
    Ok(fixture)
}

/// A durable failure must invalidate, so a *later transient* failure cannot fall
/// back to the value either.
///
/// This is the ordering that matters and the one a per-invocation gate missed:
/// suppressing the fallback only for the durable attempt leaves the cached value
/// resident, and the next pool timeout serves it — after the database has already
/// established that the column is not readable.
///
/// The second lookup **has to be transient**, and an earlier version of this test
/// got that wrong: it made two durable lookups, and since a durable failure never
/// reaches `get_stale` at all, deleting the `invalidate` call left both
/// assertions passing. Here the sideline pool is closed between the two calls, so
/// lookup two is a `PoolClosed` — the one shape that *would* serve a surviving
/// entry. `ttl 0` keeps the freshness window from answering either call.
#[tokio::test(flavor = "current_thread")]
async fn a_durable_failure_invalidates_so_a_later_transient_one_cannot_serve_stale() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let Some(sideline) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, visibility_cache(0, 600));
    let fixture = ClusterFixture {
        service: fixture
            .service
            .with_visibility_pool(sideline.service.pool.clone()),
        _db: fixture._db,
    };
    register_template(&fixture.service, "code-runtime").await?;
    register_release(
        &fixture.service,
        VISIBILITY_TEST_DIGEST,
        &["latest"],
        &[VISIBILITY_TEST_STORAGE],
    )
    .await?;
    register_template(&sideline.service, "code-runtime").await?;

    // Warm the cache through the sideline pool.
    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Generic,
        "the seeding read must succeed, or there is nothing for the fallback to serve"
    );

    // Durable failure: the column is gone from the database the lookup reads.
    sqlx::query(BREAK_VISIBILITY_COLUMN_MISSING)
        .execute(&sideline.service.pool)
        .await?;
    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Unspecified,
        "a durable failure must degrade"
    );

    // Now a *transient* failure. If the durable arm had left the entry resident,
    // this is the call that would serve it.
    sideline.service.pool.close().await;
    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Unspecified,
        "the durable failure must have invalidated, leaving the transient one nothing to serve"
    );
    Ok(())
}

/// The complement, so the test above cannot be satisfied by a fallback that never
/// works: the same transient failure *does* serve a cached read when no durable
/// failure preceded it.
#[tokio::test(flavor = "current_thread")]
async fn a_transient_failure_alone_serves_the_cached_read() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let Some(sideline) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, visibility_cache(0, 600));
    let fixture = seed_then_break_visibility_transiently(fixture, &sideline).await?;

    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Generic,
        "a transient failure with no durable failure before it must serve the cached read"
    );
    Ok(())
}

/// A renamed table (`42P01`) degrades rather than serving a remembered value —
/// end to end, through the real RPC and a real Postgres.
///
/// What this cannot show is *why*. `42P01` is on the durable list, so the failure
/// both refuses the fallback and erases the entry; but an unclassified code also
/// refuses the fallback, so removing `42P01` from that list would leave this test
/// green. The durable-specific half — the erasure — is pinned by
/// `visibility_degradation_tests::a_durable_failure_erases_the_cached_read`,
/// which can ask the predicates directly. This test's job is that the wiring
/// carries a real table-level failure all the way to `UNSPECIFIED` without a
/// cached read papering over it.
#[tokio::test(flavor = "current_thread")]
async fn a_missing_templates_table_degrades_end_to_end() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let fixture = with_visibility_cache(fixture, visibility_cache(0, 600));
    seed_then_break_visibility(&fixture, BREAK_VISIBILITY_TABLE_MISSING).await?;

    assert_eq!(
        visibility_of_find(&fixture).await?,
        TemplateVisibility::Unspecified,
        "a durable schema failure must not be covered by the cache"
    );
    Ok(())
}

mod backup_generation_fence;
mod publication;
mod restoring_routes;

#[tokio::test(flavor = "current_thread")]
async fn ecw_route_resolve_enforces_key_surface_and_organization() -> TestResult {
    if !pplx_space_test_env::enter_test_process(&[
        (
            "SPACE_INTERNAL_RPC_INSECURE_DEV",
            Some(std::ffi::OsStr::new("true")),
        ),
        (
            "SPACE_INTERNAL_TOKEN",
            Some(std::ffi::OsStr::new("route-scope-test-token")),
        ),
    ]) {
        return Ok(());
    }
    let fixture = cluster_fixture()
        .await?
        .expect("Postgres is required for this reproduction");
    let node_id = NodeId::new();
    let sandbox_id = SandboxId::new();
    let key_hash = sha256_hex("route-scope-test-native-only-key");
    insert_data_plane_worker_node(&fixture.service.pool, node_id, "192.0.2.41").await?;
    let node_api = sim_worker_node_api(node_id).await?;
    insert_data_plane_direct_node_endpoint(&fixture.service.pool, node_id, node_api.endpoint())
        .await?;
    insert_data_plane_active_sandbox(&fixture.service.pool, sandbox_id, node_id).await?;
    insert_active_execution(&fixture.service.pool, sandbox_id, "192.0.2.42", 1).await?;
    insert_e2b_api_key(&fixture.service.pool, &key_hash).await?;

    let request = |hash: String| {
        Request::new(UnstableResolveSandboxServiceRouteRequest {
            sandbox_id: sandbox_id.to_string(),
            port: 49_983,
            route_kind: SandboxServiceRouteKind::Agent as i32,
            transport: SandboxServiceRouteTransport::Direct as i32,
            credential_proofs: vec![route_credential_proof(
                SandboxServiceRouteCredentialKind::OrgApiKey,
                hash,
            )],
        })
    };

    let invalid = fixture
        .service
        .unstable_resolve_sandbox_service_route(request(sha256_hex("wrong-key")))
        .await
        .expect_err("an invalid key must be rejected");
    assert_eq!(invalid.code(), Code::Unauthenticated);

    for surfaces in [vec!["ecw"], vec!["native"], vec!["ecw", "native"]] {
        sqlx::query(pplx_space_common_core::sql!(
            "UPDATE api_keys SET allowed_surfaces = $1 WHERE key_hash = $2"
        ))
        .bind(&surfaces)
        .bind(&key_hash)
        .execute(&fixture.service.pool)
        .await?;
        let result = fixture
            .service
            .unstable_resolve_sandbox_service_route(request(key_hash.clone()))
            .await;
        if surfaces.contains(&"ecw") {
            let route = result?.into_inner();
            assert_eq!(route.sandbox_id, sandbox_id.to_string());
            assert!(route.space_api_route_token.is_some());
        } else {
            assert!(
                matches!(result, Err(status) if status.code() == Code::Unauthenticated),
                "native-only keys cannot control the ECW agent"
            );
        }
    }

    sqlx::query(pplx_space_common_core::sql!(
        "UPDATE api_keys SET org_id = '00000000-0000-0000-0000-000000000002' WHERE key_hash = $1"
    ))
    .bind(&key_hash)
    .execute(&fixture.service.pool)
    .await?;
    let other_org = fixture
        .service
        .unstable_resolve_sandbox_service_route(request(key_hash))
        .await
        .expect_err("a key from another organization must be rejected");
    assert_eq!(other_org.code(), Code::Unauthenticated);
    Ok(())
}

fn test_artifact_digests() -> Vec<crate::proto::SnapshotArtifactDigest> {
    vec![
        crate::proto::SnapshotArtifactDigest {
            artifact: "memfile".to_string(),
            digest: format!("sha256:{}", "22".repeat(32)),
            size_bytes: 1 << 30,
        },
        crate::proto::SnapshotArtifactDigest {
            artifact: "binfile".to_string(),
            digest: format!("sha256:{}", "11".repeat(32)),
            size_bytes: 4096,
        },
    ]
}

async fn finalize_ready_with_digests(
    service: &ClusterApiService,
    backup_attempt_id: SnapshotId,
    artifact_digests: Vec<crate::proto::SnapshotArtifactDigest>,
) -> Result<(), tonic::Status> {
    service
        .finalize_artifact_backup(request_from_node(
            FinalizeArtifactBackupRequest {
                backup_attempt_id: backup_attempt_id.to_string(),
                target_state: SuspendSnapshotTerminalState::Ready as i32,
                bytes_uploaded: 4096 + (1 << 30),
                failure_reason: None,
                artifact_digests,
            },
            TEST_WORKER_IP,
        ))
        .await
        .map(|_| ())
}

/// Digests the source node reports at finalize travel with the backup to
/// both restore lookups, ordered by artifact file name.
#[tokio::test(flavor = "current_thread")]
async fn finalize_to_ready_publishes_artifact_digests_to_restore_lookups() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let local_artifact_id = LocalArtifactId::new();
    let backup_attempt_id = SnapshotId::new();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;
    finalize_ready_with_digests(&fixture.service, backup_attempt_id, test_artifact_digests())
        .await?;

    let mut expected = test_artifact_digests();
    expected.sort_by(|a, b| a.artifact.cmp(&b.artifact));
    let artifact = get_artifact_location(
        &fixture.service,
        sandbox_id,
        local_artifact_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
    )
    .await?;
    assert_eq!(artifact.artifact_digests, expected);
    let suspend = get_location(&fixture.service, sandbox_id).await?;
    assert_eq!(suspend.snapshot_id, backup_attempt_id.to_string());
    assert_eq!(suspend.artifact_digests, expected);
    Ok(())
}

/// Malformed digests are refused at the wire boundary and leave the attempt
/// open for a well-formed retry.
#[tokio::test(flavor = "current_thread")]
async fn finalize_rejects_malformed_artifact_digests() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
    let backup_attempt_id = SnapshotId::new();
    begin_artifact_backup(
        &fixture.service,
        sandbox_id,
        LocalArtifactId::new(),
        backup_attempt_id,
        pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
        pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
    )
    .await?;

    let good = test_artifact_digests();
    let mut unknown_artifact = good.clone();
    unknown_artifact[0].artifact = "../memfile".to_string();
    let mut malformed_digest = good.clone();
    malformed_digest[0].digest = "sha256:not-hex".to_string();
    let mut duplicate_artifact = good.clone();
    duplicate_artifact[1].artifact = "memfile".to_string();
    for artifact_digests in [unknown_artifact, malformed_digest, duplicate_artifact] {
        let err =
            finalize_ready_with_digests(&fixture.service, backup_attempt_id, artifact_digests)
                .await
                .expect_err("malformed digests must be refused");
        assert_eq!(err.code(), Code::InvalidArgument);
    }
    let state: String = sqlx::query_scalar("SELECT state FROM snapshots WHERE id = $1")
        .bind(backup_attempt_id)
        .fetch_one(&fixture.service.pool)
        .await?;
    assert_eq!(state, "creating");
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn restore_locations_keep_digests_during_catalog_deletion() -> TestResult {
    let Some(fixture) = cluster_fixture_with_max_connections(4).await? else {
        return Ok(());
    };
    let mut results = Vec::new();
    for use_artifact_lookup in [false, true] {
        let sandbox_id = seed_sandbox(&fixture.service.pool).await?;
        let local_artifact_id = LocalArtifactId::new();
        let attempt = SnapshotId::new();
        begin_artifact_backup(
            &fixture.service,
            sandbox_id,
            local_artifact_id,
            attempt,
            pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
            pplx_space_sbx_mgr_client::proto::ArtifactOrigin::Suspend,
        )
        .await?;
        finalize_ready_with_digests(&fixture.service, attempt, test_artifact_digests()).await?;

        // Stop the digest read after the location read, then perform real
        // teardown. Object bytes outlive the catalog until the orphan sweep.
        let mut deletion = fixture.service.pool.begin().await?;
        sqlx::query(sql!(
            "LOCK TABLE snapshot_artifact_digests IN ACCESS EXCLUSIVE MODE"
        ))
        .execute(&mut *deletion)
        .await?;
        let blocker: i32 = sqlx::query_scalar(sql!("SELECT pg_backend_pid()"))
            .fetch_one(&mut *deletion)
            .await?;
        let lookup = async {
            if use_artifact_lookup {
                get_artifact_location(
                    &fixture.service,
                    sandbox_id,
                    local_artifact_id,
                    pplx_space_sbx_mgr_client::proto::ArtifactKind::Full,
                )
                .await
                .map(|location| location.artifact_digests)
            } else {
                get_location(&fixture.service, sandbox_id)
                    .await
                    .map(|location| location.artifact_digests)
            }
        };
        let remove = async {
            loop {
                let blocked: bool = sqlx::query_scalar(sql!(
                    "SELECT EXISTS (
                         SELECT 1 FROM pg_stat_activity
                         WHERE datname = current_database()
                           AND $1 = ANY(pg_blocking_pids(pid)))"
                ))
                .bind(blocker)
                .fetch_one(&fixture.service.pool)
                .await?;
                if blocked {
                    break;
                }
                tokio::task::yield_now().await;
            }
            pplx_space_sandbox_teardown::finalize_sandbox_db_delete_tx(&mut deletion, sandbox_id)
                .await?;
            deletion.commit().await
        };
        let (result, removed) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            tokio::join!(lookup, remove)
        })
        .await?;
        removed?;
        results.push((use_artifact_lookup, result));
    }

    let mut expected = test_artifact_digests();
    expected.sort_by(|a, b| a.artifact.cmp(&b.artifact));
    for (use_artifact_lookup, result) in results {
        match result {
            Ok(digests) => assert_eq!(
                digests, expected,
                "a selected location must retain its digests (artifact lookup: {use_artifact_lookup})"
            ),
            Err(status) => assert_eq!(status.code(), Code::NotFound),
        }
    }
    Ok(())
}

/// The node-bound template and policy reads answer for the templates the
/// node has a sandbox placed on, warms, or builds from, plus generic ones,
/// and for the sandboxes placed on it: the same placement the release lookups
/// use. Inventory is reported by the node itself, so holding a template only
/// lets the node hear that the template is being retired.
#[tokio::test(flavor = "current_thread")]
async fn node_template_and_policy_reads_follow_placement() -> TestResult {
    let Some(fixture) = cluster_fixture().await? else {
        return Ok(());
    };
    let pool = &fixture.service.pool;
    let owner = Uuid::parse_str("00000000-0000-0000-0000-000000000003")?;
    let (release_id, _) = seed_private_release(&fixture, "private-base", owner).await?;
    register_template(&fixture.service, "code-runtime").await?;
    let host_ip = std::net::Ipv4Addr::new(10, 0, 0, 30);
    let other_ip = std::net::Ipv4Addr::new(10, 0, 0, 31);
    let builder_ip = std::net::Ipv4Addr::new(10, 0, 0, 40);
    let host = insert_k8s_node(pool, host_ip, "worker").await?;
    let other = insert_k8s_node(pool, other_ip, "worker").await?;
    let builder = insert_k8s_node(pool, builder_ip, "builder").await?;
    let sandbox_id = Uuid::now_v7();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (
             id, user_id, org_id, managed_by, lifecycle, placement,
             active_node_id, target_node_id, command_ready_pending,
             network_policy_generation, template_id
         )
         VALUES ($1, $2::uuid, $3::uuid, 'k8s', 'started', 'on_node', $4, $4, TRUE, 1, 'private-base')"
    ))
    .bind(sandbox_id)
    .bind(SEEDED_USER_ID)
    .bind(owner)
    .bind(host)
    .execute(pool)
    .await?;

    struct Seen {
        policy: Vec<String>,
        sandboxes: Vec<String>,
        desired: Vec<(String, Option<bool>)>,
        base: Vec<String>,
    }
    async fn seen_by(
        fixture: &ClusterFixture,
        node: Uuid,
        node_ip: std::net::Ipv4Addr,
        role: ProtoNodeRole,
        sandbox_id: Uuid,
    ) -> TestResult<Seen> {
        let node_ip = std::net::IpAddr::V4(node_ip);
        let templates = vec!["private-base".to_string(), "code-runtime".to_string()];
        let role = Some(role as i32);
        let policy = fixture
            .service
            .get_node_network_policy_state(request_from_node(
                GetNodeNetworkPolicyStateRequest {
                    node_id: node.to_string(),
                    templates: templates.clone(),
                    sandbox_ids: vec![sandbox_id.to_string()],
                    role,
                },
                node_ip,
            ))
            .await?
            .into_inner();
        let desired = fixture
            .service
            .get_node_template_desired_state(request_from_node(
                GetNodeTemplateDesiredStateRequest {
                    node_id: node.to_string(),
                    templates: templates.clone(),
                    role,
                },
                node_ip,
            ))
            .await?
            .into_inner();
        let base = fixture
            .service
            .get_node_template_base_snapshot_policy_state(request_from_node(
                GetNodeTemplateBaseSnapshotPolicyStateRequest {
                    node_id: node.to_string(),
                    templates,
                    role,
                },
                node_ip,
            ))
            .await?
            .into_inner();
        let mut seen = Seen {
            policy: policy.templates.into_iter().map(|t| t.template).collect(),
            sandboxes: policy.sandboxes.into_iter().map(|s| s.sandbox_id).collect(),
            desired: desired
                .templates
                .into_iter()
                .map(|t| (t.template, t.present))
                .collect(),
            base: base.templates.into_iter().map(|t| t.template).collect(),
        };
        seen.policy.sort();
        seen.desired.sort();
        seen.base.sort();
        Ok(seen)
    }
    fn assert_seen(seen: &Seen, templates: &[&str], sandbox: Option<Uuid>, label: &str) {
        let templates: Vec<String> = templates.iter().map(|t| t.to_string()).collect();
        let present: Vec<(String, Option<bool>)> =
            templates.iter().map(|t| (t.clone(), Some(true))).collect();
        assert_eq!(seen.policy, templates, "{label}: policy templates");
        assert_eq!(seen.base, templates, "{label}: base-snapshot templates");
        assert_eq!(seen.desired, present, "{label}: desired templates");
        let sandbox: Vec<String> = sandbox.into_iter().map(|id| id.to_string()).collect();
        assert_eq!(seen.sandboxes, sandbox, "{label}: sandboxes");
    }
    const BOTH: &[&str] = &["code-runtime", "private-base"];
    const GENERIC: &[&str] = &["code-runtime"];
    let worker = ProtoNodeRole::Worker;

    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, BOTH, Some(sandbox_id), "active host");
    let seen = seen_by(&fixture, other, other_ip, worker, sandbox_id).await?;
    assert_seen(&seen, GENERIC, None, "unplaced worker");
    let seen = seen_by(
        &fixture,
        builder,
        builder_ip,
        ProtoNodeRole::Builder,
        sandbox_id,
    )
    .await?;
    assert_seen(&seen, GENERIC, None, "unplaced builder");

    // A pending migration is a target placement: the destination may read
    // before the sandbox is active there, and the source keeps reading while
    // it still hosts the sandbox.
    sqlx::query(sql!(
        "UPDATE sandboxes SET target_node_id = $1 WHERE id = $2"
    ))
    .bind(other)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, BOTH, Some(sandbox_id), "migration source");
    let seen = seen_by(&fixture, other, other_ip, worker, sandbox_id).await?;
    assert_seen(&seen, BOTH, Some(sandbox_id), "migration target");

    // Once the sandbox is active only on the destination, the source loses it.
    sqlx::query(sql!(
        "UPDATE sandboxes SET active_node_id = $1, target_node_id = NULL WHERE id = $2"
    ))
    .bind(other)
    .bind(sandbox_id)
    .execute(pool)
    .await?;
    let seen = seen_by(&fixture, other, other_ip, worker, sandbox_id).await?;
    assert_seen(&seen, BOTH, Some(sandbox_id), "active destination");
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, GENERIC, None, "former host");

    // Inventory is what the node reports about itself, so holding a template
    // is not a placement.
    sqlx::query(sql!(
        "INSERT INTO node_template_inventory (node_id, template_name)
         VALUES ($1, 'private-base')"
    ))
    .bind(host)
    .execute(pool)
    .await?;
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, GENERIC, None, "self-reported inventory");

    // An enabled warm pool covers registered workers, not builders.
    let config_id: Uuid = sqlx::query_scalar(sql!(
        "INSERT INTO kubernetes_pool_configs (
             template_release_id, vcpu, memory_mib, target_ready_slots,
             refill_per_tick, max_warming_slots, namespace, enabled, managed_by
         )
         VALUES ($1, 2, 4096, 1, 1, 1, 'space', FALSE, 'autoscaler')
         RETURNING id"
    ))
    .bind(release_id)
    .fetch_one(pool)
    .await?;
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, GENERIC, None, "disabled pool");
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET enabled = TRUE WHERE id = $1"
    ))
    .bind(config_id)
    .execute(pool)
    .await?;
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, BOTH, None, "enabled pool, worker");
    let seen = seen_by(
        &fixture,
        builder,
        builder_ip,
        ProtoNodeRole::Builder,
        sandbox_id,
    )
    .await?;
    assert_seen(&seen, GENERIC, None, "enabled pool, builder");
    sqlx::query(sql!(
        "UPDATE kubernetes_pool_configs SET enabled = FALSE WHERE id = $1"
    ))
    .bind(config_id)
    .execute(pool)
    .await?;

    // A fresh build grant covers registered builders, not workers.
    sqlx::query(sql!(
        "INSERT INTO template_build_base_grants (id, template_id, base_template_name, org_id)
         VALUES ($1, 'private-child', 'private-base', $2)"
    ))
    .bind(Uuid::now_v7())
    .bind(owner)
    .execute(pool)
    .await?;
    let seen = seen_by(
        &fixture,
        builder,
        builder_ip,
        ProtoNodeRole::Builder,
        sandbox_id,
    )
    .await?;
    assert_seen(&seen, BOTH, None, "fresh grant, builder");
    let seen = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_seen(&seen, GENERIC, None, "fresh grant, worker");
    sqlx::query(sql!("DELETE FROM template_build_base_grants"))
        .execute(pool)
        .await?;

    // Retiring a template: a node that still holds it hears `present = false`
    // and nothing else, while placement keeps serving the sandbox's host.
    sqlx::query(sql!(
        "UPDATE templates SET desired_state = 'deleting' WHERE name = 'private-base'"
    ))
    .execute(pool)
    .await?;
    let retiring = vec![
        ("code-runtime".to_string(), Some(true)),
        ("private-base".to_string(), Some(false)),
    ];
    let holder = seen_by(&fixture, host, host_ip, worker, sandbox_id).await?;
    assert_eq!(holder.policy, GENERIC, "retiring holder: policy templates");
    assert_eq!(
        holder.base, GENERIC,
        "retiring holder: base-snapshot templates"
    );
    assert_eq!(
        holder.desired, retiring,
        "retiring holder: desired templates"
    );
    assert!(holder.sandboxes.is_empty(), "retiring holder: sandboxes");
    let active = seen_by(&fixture, other, other_ip, worker, sandbox_id).await?;
    assert_eq!(
        active.policy, BOTH,
        "retiring sandbox host: policy templates"
    );
    assert_eq!(
        active.base, BOTH,
        "retiring sandbox host: base-snapshot templates"
    );
    assert_eq!(
        active.desired, retiring,
        "retiring sandbox host: desired templates"
    );
    assert_eq!(active.sandboxes, vec![sandbox_id.to_string()]);
    Ok(())
}
