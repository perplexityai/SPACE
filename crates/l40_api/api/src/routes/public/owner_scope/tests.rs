use super::*;
use axum::{
    Router,
    body::Body,
    http::{Method, StatusCode},
};
use pplx_space_common_core::sql;
use pplx_space_common_core::types::OrgId;
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_public_api::OperationId;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use tower::ServiceExt;
use uuid::Uuid;

const KEY: &str = "native-federated-test-key";
const ORG: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01";
const OTHER_ORG: &str = "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a02";
const GRANT: &str = "opaque-native-create-grant";

struct Fixture {
    db: TestDb,
    app: Router,
}

async fn fixture() -> TestDbResult<Fixture> {
    let db = pg_test_db().await?;
    sqlx::query(sql!(
        "INSERT INTO api_keys (
             id, org_id, name, key_hash, allowed_surfaces, federated_owner_assertions_enabled
         ) VALUES ($1, $2, 'native-owner-test', $3, ARRAY['native'], TRUE)"
    ))
    .bind(Uuid::now_v7())
    .bind(SEED_ORG_ID)
    .bind(crate::auth::hash_token(KEY))
    .execute(&db.pool)
    .await?;
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    // The sweeps below cover every mounted operation, hidden ones included.
    state.native_hidden_operations =
        crate::routes::public::hidden_operations::HiddenOperationsGate::all_orgs();
    state.federated_identity = crate::federated_identity::FederatedIdentityConfig {
        enabled: true,
        policy: crate::federated_identity::FederatedIdentityPolicy::parse(
            Some("test-issuer"),
            None,
            Some(r#"["0"]"#),
        )?,
    };
    let app = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    Ok(Fixture { db, app })
}

fn owner_headers(org: &str, subject: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        crate::federated_identity::FEDERATED_ORG_ID_HEADER,
        org.parse().unwrap(),
    );
    headers.insert(
        crate::federated_identity::FEDERATED_USER_ID_HEADER,
        subject.parse().unwrap(),
    );
    headers
}

fn grant_headers(mut headers: HeaderMap) -> HeaderMap {
    headers.insert(FEDERATED_SERVICE_GRANT_HEADER, GRANT.parse().unwrap());
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    headers.insert(
        FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
        (now + 3600).to_string().parse().unwrap(),
    );
    headers.insert(
        FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
        now.to_string().parse().unwrap(),
    );
    headers
}

async fn send(
    app: &Router,
    method: Method,
    path: &str,
    headers: HeaderMap,
    body: Value,
) -> TestDbResult<(StatusCode, Value)> {
    let mut request = Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
        .header("content-type", "application/json")
        .header("idempotency-key", "same-key");
    request.headers_mut().unwrap().extend(headers);
    let response = app
        .clone()
        .oneshot(request.body(Body::from(serde_json::to_vec(&body)?))?)
        .await?;
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 1024 * 1024).await?;
    Ok((status, serde_json::from_slice(&body)?))
}

async fn create(fixture: &Fixture, org: &str, subject: &str) -> TestDbResult<Value> {
    let (status, response) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        grant_headers(owner_headers(org, subject)),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{response}");
    Ok(response)
}

async fn seed_target(fixture: &Fixture, response: &Value) -> TestDbResult<PublicSandboxId> {
    let sandbox_id: PublicSandboxId = response["sandbox_id"].as_str().unwrap().parse()?;
    let operation_id: OperationId = response["operation_id"].as_str().unwrap().parse()?;
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_override_json,
             network_policy_generation)
         SELECT $1, actor_user_id, org_id, 'started', $3, 1
         FROM public_operations WHERE operation_id = $2"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(operation_id.as_uuid())
    .bind(sqlx::types::Json(
        pplx_space_net_common::NetworkPolicyV1::default(),
    ))
    .execute(&fixture.db.pool)
    .await?;
    Ok(sandbox_id)
}

#[tokio::test]
async fn native_admission_records_effective_owner_and_scopes_replay_and_polling() -> TestDbResult<()>
{
    let fixture = fixture().await?;
    let first = create(&fixture, ORG, "101").await?;
    let retry = create(&fixture, ORG, "101").await?;
    let second = create(&fixture, ORG, "102").await?;
    let third = create(&fixture, OTHER_ORG, "101").await?;
    assert_eq!(first, retry);
    assert_ne!(first["operation_id"], second["operation_id"]);
    assert_ne!(first["operation_id"], third["operation_id"]);
    let operation_id: OperationId = first["operation_id"].as_str().unwrap().parse()?;
    let (org_id, actor_id, subject, payload): (OrgId, Uuid, String, sqlx::types::Json<Value>) =
        sqlx::query_as(sql!(
            "SELECT op.org_id, op.actor_user_id, u.upstream_subject_id, op.payload_json
             FROM public_operations op JOIN users u ON u.id = op.actor_user_id
             WHERE op.operation_id = $1"
        ))
        .bind(operation_id.as_uuid())
        .fetch_one(&fixture.db.pool)
        .await?;
    assert_ne!(org_id, OrgId::from_uuid(SEED_ORG_ID));
    assert_ne!(actor_id, SEED_USER_ID);
    assert_eq!(subject, "101");
    let stored: crate::public_sandbox_operations::CreateSandboxOperationPayload =
        serde_json::from_value(payload.0)?;
    let (owner, grant) = stored.federated_create.as_ref().unwrap().restore()?;
    assert_eq!(owner.org_id.as_str(), ORG);
    assert_eq!(owner.subject_id.as_str(), "101");
    assert_eq!(grant.as_bytes(), GRANT.as_bytes());
    assert!(!format!("{stored:?}").contains(GRANT));
    for suffix in ["", "/events"] {
        let path = format!("/v1/operations/{operation_id}{suffix}");
        let (status, _) = send(
            &fixture.app,
            Method::GET,
            &path,
            owner_headers(ORG, "101"),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::OK);
        for headers in [
            owner_headers(ORG, "102"),
            owner_headers(OTHER_ORG, "101"),
            HeaderMap::new(),
        ] {
            let (status, _) = send(&fixture.app, Method::GET, &path, headers, Value::Null).await?;
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }
    Ok(())
}

#[tokio::test]
async fn deleted_sandbox_replay_is_authorized_by_its_retained_operation() -> TestDbResult<()> {
    use crate::public_operations::{
        ClaimLimits, ClaimPublicOperations, CompleteClaimedPublicOperation,
    };
    use pplx_space_public_api::{
        OperationKind, PublicOperationResult, PublicSandboxOperationResult, SandboxLifecycleAction,
        SandboxLifecycleState,
    };
    use std::time::Duration;

    for owner in [HeaderMap::new(), owner_headers(ORG, "101")] {
        let fixture = fixture().await?;
        let (status, created) = send(
            &fixture.app,
            Method::POST,
            "/v1/sandboxes",
            grant_headers(owner.clone()),
            json!({"template":"seed:latest"}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{created}");
        let sandbox_id = seed_target(&fixture, &created).await?;
        let path = format!("/v1/sandboxes/{sandbox_id}");
        let (status, deleted) = send(
            &fixture.app,
            Method::DELETE,
            &path,
            owner.clone(),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{deleted}");
        let operation_id: OperationId = deleted["operation_id"].as_str().unwrap().parse()?;
        let store = PublicOperationStore::new(fixture.db.pool.clone());
        let claim = store
            .claim_operations(ClaimPublicOperations {
                limits: ClaimLimits::any(1),
                claim_stale_after: Duration::from_secs(60),
                claimable_kinds: &[OperationKind::DeleteSandbox],
            })
            .await?;
        assert_eq!(claim.operations[0].id, operation_id);
        sqlx::query(sql!("DELETE FROM sandboxes WHERE id = $1"))
            .bind(sandbox_id.as_uuid())
            .execute(&fixture.db.pool)
            .await?;
        assert!(
            store
                .complete_claimed_operation(CompleteClaimedPublicOperation {
                    claim_token: claim.claim_token,
                    operation_id,
                    progress_message: "deleted".into(),
                    result_json: serde_json::to_value(PublicOperationResult::Sandbox(
                        PublicSandboxOperationResult {
                            sandbox_id,
                            state: SandboxLifecycleState::Deleted,
                            action: Some(SandboxLifecycleAction::Delete),
                            controller_response: None
                        }
                    ))?,
                })
                .await?
        );

        let (status, replay) = send(
            &fixture.app,
            Method::DELETE,
            &path,
            owner.clone(),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
        assert_eq!(replay["operation_id"], deleted["operation_id"]);
        for headers in [owner.clone(), owner_headers(OTHER_ORG, "101")] {
            let mut request = Request::builder()
                .method(Method::DELETE)
                .uri(&path)
                .header("authorization", format!("Bearer {KEY}"));
            request.headers_mut().unwrap().extend(headers);
            let response = fixture
                .app
                .clone()
                .oneshot(request.body(Body::empty())?)
                .await?;
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "an absent key cannot authorize replay"
            );
        }
        let mut fresh = owner.clone();
        fresh.insert("idempotency-key", "new-delete".parse()?);
        for (method, target, headers) in [
            (Method::DELETE, path.clone(), fresh),
            (Method::DELETE, path.clone(), owner_headers(ORG, "102")),
            (
                Method::DELETE,
                path.clone(),
                owner_headers(OTHER_ORG, "101"),
            ),
            (Method::GET, path.clone(), owner.clone()),
            (Method::POST, format!("{path}:wake"), owner.clone()),
            (
                Method::DELETE,
                format!(
                    "/v1/sandboxes/{}",
                    PublicSandboxId::from(pplx_space_common_core::types::SandboxId::new())
                ),
                owner.clone(),
            ),
        ] {
            let (status, response) =
                send(&fixture.app, method, &target, headers, Value::Null).await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn native_create_requires_grant_before_enqueue() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        owner_headers(ORG, "101"),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let count: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM public_operations"))
        .fetch_one(&fixture.db.pool)
        .await?;
    assert_eq!(count, 0);
    Ok(())
}

#[tokio::test]
async fn native_resources_and_capability_minting_deny_other_subjects() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let first = create(&fixture, ORG, "101").await?;
    let second = create(&fixture, ORG, "102").await?;
    let sandbox = seed_target(&fixture, &first).await?;
    let other_sandbox = seed_target(&fixture, &second).await?;
    let path = format!("/v1/sandboxes/{sandbox}");
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        &path,
        owner_headers(ORG, "101"),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        "/v1/sandboxes",
        owner_headers(ORG, "101"),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    let listed = body["data"].as_array().unwrap();
    assert_eq!(listed.len(), 1);
    assert!(serde_json::to_string(listed)?.contains(&sandbox.to_string()));
    assert!(!serde_json::to_string(listed)?.contains(&other_sandbox.to_string()));
    for (method, suffix) in [
        (Method::GET, ""),
        (Method::DELETE, ""),
        (Method::POST, ":wake"),
        (Method::POST, "/files:read"),
        (Method::POST, "/files:write"),
        (Method::GET, "/processes"),
        (Method::POST, "/commands"),
        (Method::POST, "/terminal-sessions"),
        (Method::POST, "/port-sessions"),
        (Method::GET, "/sessions"),
        (Method::GET, "/snapshots"),
    ] {
        for headers in [
            owner_headers(ORG, "102"),
            owner_headers(OTHER_ORG, "101"),
            HeaderMap::new(),
        ] {
            let (status, body) = send(
                &fixture.app,
                method.clone(),
                &format!("{path}{suffix}"),
                headers,
                json!({}),
            )
            .await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{method} {suffix}: {body}");
        }
    }
    // Both lifecycle mutations reach operation admission for the correct subject.
    for (method, suffix) in [(Method::POST, ":wake"), (Method::DELETE, "")] {
        let (status, body) = send(
            &fixture.app,
            method,
            &format!("{path}{suffix}"),
            grant_headers(owner_headers(ORG, "101")),
            json!({}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    }
    let stored_grant: Vec<u8> = sqlx::query_scalar(sql!(
        "SELECT grant_blob FROM sandbox_federated_service_grants WHERE sandbox_id = $1"
    ))
    .bind(sandbox.as_uuid())
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(stored_grant, GRANT.as_bytes());
    // A wrong subject cannot overwrite/refresh the victim grant.
    let mut spoof = grant_headers(owner_headers(ORG, "102"));
    spoof.insert(FEDERATED_SERVICE_GRANT_HEADER, "attacker-grant".parse()?);
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        &format!("{path}:wake"),
        spoof,
        json!({}),
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let unchanged: Vec<u8> = sqlx::query_scalar(sql!(
        "SELECT grant_blob FROM sandbox_federated_service_grants WHERE sandbox_id = $1"
    ))
    .bind(sandbox.as_uuid())
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(unchanged, stored_grant);
    Ok(())
}

#[tokio::test]
async fn legacy_native_stays_local_and_has_no_asserted_owner_bridge() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let (status, local) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        HeaderMap::new(),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{local}");
    let sandbox = seed_target(&fixture, &local).await?;
    let path = format!("/v1/sandboxes/{sandbox}");
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &path,
        HeaderMap::new(),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &fixture.app,
        Method::GET,
        &path,
        owner_headers(ORG, "101"),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn native_command_capabilities_are_subject_scoped_through_get_and_revoke() -> TestDbResult<()>
{
    let fixture = fixture().await?;
    let first = create(&fixture, ORG, "101").await?;
    let second = create(&fixture, ORG, "102").await?;
    let sandbox = seed_target(&fixture, &first).await?;
    let other = seed_target(&fixture, &second).await?;
    let path = format!("/v1/sandboxes/{sandbox}/commands");
    let (status, command) = send(
        &fixture.app,
        Method::POST,
        &path,
        owner_headers(ORG, "101"),
        json!({"command":["echo", "hello"]}),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{command}");
    assert!(
        command["attach"]["token"]
            .as_str()
            .unwrap()
            .starts_with("dpt_")
    );
    let (status, other_command) = send(
        &fixture.app,
        Method::POST,
        &format!("/v1/sandboxes/{other}/commands"),
        owner_headers(ORG, "102"),
        json!({"command":["echo", "hello"]}),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{other_command}");
    assert_ne!(command["session_id"], other_command["session_id"]);
    assert_ne!(command["attach"]["token"], other_command["attach"]["token"]);
    let (status, replay) = send(
        &fixture.app,
        Method::POST,
        &path,
        owner_headers(ORG, "101"),
        json!({"command":["echo", "hello"]}),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED, "{replay}");
    assert_eq!(command["session_id"], replay["session_id"]);
    let session_id = command["session_id"].as_str().unwrap();
    let session_path = format!("/v1/sandboxes/{sandbox}/sessions/{session_id}");
    for method in [Method::GET, Method::DELETE] {
        let (status, _) = send(
            &fixture.app,
            method.clone(),
            &session_path,
            owner_headers(ORG, "102"),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, body) = send(
            &fixture.app,
            method,
            &session_path,
            owner_headers(ORG, "101"),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (actor, sandbox_owner, revoked): (Uuid, Uuid, bool) = sqlx::query_as(sql!(
        "SELECT ds.actor_user_id, s.user_id, ds.revocation_status = 'revoked'
         FROM data_plane_sessions ds JOIN sandboxes s ON s.id = ds.sandbox_id
         WHERE session_id = $1"
    ))
    .bind(session_id)
    .fetch_one(&fixture.db.pool)
    .await?;
    assert_eq!(actor, sandbox_owner);
    assert!(revoked);
    Ok(())
}

#[tokio::test]
async fn a_local_credential_in_a_materialized_org_cannot_access_federated_resources()
-> TestDbResult<()> {
    let fixture = fixture().await?;
    let created = create(&fixture, ORG, "101").await?;
    let sandbox = seed_target(&fixture, &created).await?;
    const LOCAL_KEY: &str = "local-key-in-federated-org";
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         SELECT $1, org_id, 'local-scope-regression', $2, ARRAY['native']
         FROM sandboxes WHERE id = $3"
    ))
    .bind(Uuid::now_v7())
    .bind(crate::auth::hash_token(LOCAL_KEY))
    .bind(sandbox.as_uuid())
    .execute(&fixture.db.pool)
    .await?;
    let mut local = HeaderMap::new();
    local.insert("authorization", format!("Bearer {LOCAL_KEY}").parse()?);
    for path in [
        format!("/v1/sandboxes/{sandbox}"),
        format!(
            "/v1/operations/{}",
            created["operation_id"].as_str().unwrap()
        ),
    ] {
        let (status, _) =
            send(&fixture.app, Method::GET, &path, local.clone(), Value::Null).await?;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
    let (status, body) = send(
        &fixture.app,
        Method::GET,
        "/v1/sandboxes",
        local.clone(),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["data"].as_array().unwrap().is_empty());
    let (status, _) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        local,
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn queued_native_create_grant_renewal_is_monotone_and_terminal_payload_is_scrubbed()
-> TestDbResult<()> {
    use crate::public_operations::{
        ClaimLimits, ClaimPublicOperations, CompleteClaimedPublicOperation,
    };
    use pplx_space_public_api::OperationKind;
    use std::time::Duration;
    let fixture = fixture().await?;
    let created = create(&fixture, ORG, "101").await?;
    let operation_id: OperationId = created["operation_id"].as_str().unwrap().parse()?;
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let mut renewal = grant_headers(owner_headers(ORG, "101"));
    renewal.insert(FEDERATED_SERVICE_GRANT_HEADER, "renewed-grant".parse()?);
    renewal.insert(
        FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
        (now + 1).to_string().parse()?,
    );
    let (status, replay) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        renewal,
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
    assert_eq!(created, replay);
    // A stale retry cannot put the old grant back.
    let mut stale = grant_headers(owner_headers(ORG, "101"));
    stale.insert(
        FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
        (now - 1).to_string().parse()?,
    );
    let (status, replay) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        stale,
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
    assert_eq!(replay, created);
    let store = PublicOperationStore::new(fixture.db.pool.clone());
    let claim = store
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(1),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::CreateSandbox],
        })
        .await?;
    assert_eq!(claim.operations.len(), 1);
    let claimed = &claim.operations[0];
    let payload: crate::public_sandbox_operations::CreateSandboxOperationPayload =
        serde_json::from_value(claimed.payload.clone())?;
    let (owner, grant) = payload.federated_create.unwrap().restore()?;
    assert_eq!(owner.subject_id.as_str(), "101");
    assert_eq!(grant.as_bytes(), b"renewed-grant");
    assert!(
        store
            .complete_claimed_operation(CompleteClaimedPublicOperation {
                claim_token: claim.claim_token,
                operation_id,
                progress_message: "completed".to_string(),
                result_json: Value::Null,
            })
            .await?
    );
    let stored: sqlx::types::Json<Value> = sqlx::query_scalar(sql!(
        "SELECT payload_json FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(&fixture.db.pool)
    .await?;
    assert!(
        stored
            .0
            .pointer("/federated_create/service_grant_base64")
            .is_none()
    );
    assert_eq!(
        stored.0.pointer("/federated_create/subject_id"),
        Some(&json!("101"))
    );
    Ok(())
}

/// Every statement the request sends to Postgres, by `sql!` locator (or the
/// leading SQL keyword for unlocated ones such as `BEGIN`), in order.
/// Keep `sqlx::query` events enabled for the whole test process.
///
/// Scoped subscribers on other test threads come and go concurrently, and a
/// callsite-interest rebuild racing ours can compute the global max level (and
/// cached interest) from a dispatcher set that no longer includes ours, which
/// silently drops every event we meant to count. One process-lifetime
/// dispatcher that accepts everything keeps each rebuild's answer "enabled";
/// events still reach only the subscriber in scope on this thread.
fn keep_query_events_enabled() {
    static ALWAYS_ENABLED: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
    ALWAYS_ENABLED.get_or_init(|| tracing::Dispatch::new(tracing_subscriber::registry()));
    tracing::callsite::rebuild_interest_cache();
}

async fn statements_during(
    request: impl std::future::Future<Output = TestDbResult<(StatusCode, Value)>>,
) -> TestDbResult<(StatusCode, Value, Vec<String>)> {
    use pplx_space_observability::test_capture::{CapturedEventsLayer, field_value};
    use tracing_subscriber::layer::SubscriberExt;

    let layer = CapturedEventsLayer::new().with_target("sqlx::query");
    let events = layer.events();
    let _guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
    keep_query_events_enabled();
    let (status, body) = request.await?;
    let statements = events
        .events()
        .iter()
        .map(|event| {
            let summary = field_value(event, "summary").unwrap_or_default();
            let statement = field_value(event, "db.statement").unwrap_or_default();
            let sql = if statement.is_empty() {
                summary
            } else {
                statement
            };
            match sql
                .trim_start()
                .strip_prefix("/*")
                .and_then(|rest| rest.split_once("*/"))
            {
                Some((locator, _)) => locator.to_string(),
                None => sql
                    .split_whitespace()
                    .next()
                    .unwrap_or_default()
                    .to_string(),
            }
        })
        .collect();
    Ok((status, body, statements))
}

fn sandbox_row_loads(statements: &[String]) -> usize {
    statements
        .iter()
        .filter(|locator| locator.starts_with("pplx_space_api::controllers::sandbox::responses:"))
        .count()
}

#[tokio::test]
async fn sandbox_scoped_requests_load_owner_and_row_once() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let first = create(&fixture, ORG, "101").await?;
    let second = create(&fixture, ORG, "102").await?;
    let sandbox = seed_target(&fixture, &first).await?;
    let other = seed_target(&fixture, &second).await?;

    let (status, body, statements) = statements_during(send(
        &fixture.app,
        Method::POST,
        &format!("/v1/sandboxes/{sandbox}/commands"),
        owner_headers(ORG, "101"),
        json!({"command": ["echo", "hello"]}),
    ))
    .await?;
    eprintln!(
        "POST /commands statements ({}): {statements:#?}",
        statements.len()
    );
    assert_eq!(status, StatusCode::CREATED, "{body}");
    // Auth (3), the sandbox row, then the session store's lock, lookup,
    // session, operation with its event, idempotency record, and commit.
    assert_eq!(statements.len(), 10, "{statements:#?}");
    assert_eq!(sandbox_row_loads(&statements), 1, "{statements:#?}");
    // An already-materialized owner resolves in one read.
    assert_eq!(
        statements
            .iter()
            .filter(|locator| locator.starts_with("pplx_space_api::federated_identity::store:"))
            .count(),
        1,
        "{statements:#?}"
    );

    // Data-plane handlers stop at the runtime here, but the row is still
    // loaded once, by the authorization layer.
    let (_, _, statements) = statements_during(send(
        &fixture.app,
        Method::POST,
        &format!("/v1/sandboxes/{sandbox}/files:stat"),
        owner_headers(ORG, "101"),
        json!({"path": "/tmp"}),
    ))
    .await?;
    eprintln!(
        "POST files:stat statements ({}): {statements:#?}",
        statements.len()
    );
    assert_eq!(sandbox_row_loads(&statements), 1, "{statements:#?}");

    // The shared row does not widen scope: another subject's sandbox is still
    // the authorization layer's 404, and nothing reaches the handler.
    let (status, _, statements) = statements_during(send(
        &fixture.app,
        Method::POST,
        &format!("/v1/sandboxes/{other}/commands"),
        owner_headers(ORG, "101"),
        json!({"command": ["echo", "hello"]}),
    ))
    .await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(sandbox_row_loads(&statements), 1, "{statements:#?}");
    Ok(())
}

/// A missing sandbox and someone else's sandbox answer every file route with
/// the same bare `not_found`: the path detail that tells a client "the sandbox
/// is yours, the path is not there" is reserved for after the owner check.
#[tokio::test]
async fn file_routes_answer_missing_and_unowned_sandboxes_without_a_path_detail() -> TestDbResult<()>
{
    let fixture = fixture().await?;
    let other_subject = create(&fixture, ORG, "102").await?;
    let other_subject = seed_target(&fixture, &other_subject).await?;
    let other_org = create(&fixture, OTHER_ORG, "101").await?;
    let other_org = seed_target(&fixture, &other_org).await?;
    let missing = PublicSandboxId::new();

    let mut answers = Vec::new();
    for sandbox in [missing, other_subject, other_org] {
        for (route, body) in [
            ("files:stat", json!({"path": "/tmp/missing"})),
            ("files:list", json!({"path": "/tmp/missing"})),
            ("files:read", json!({"path": "/tmp/missing", "length": 1})),
            ("files:remove", json!({"path": "/tmp/missing"})),
        ] {
            let (status, body) = send(
                &fixture.app,
                Method::POST,
                &format!("/v1/sandboxes/{sandbox}/{route}"),
                owner_headers(ORG, "101"),
                body,
            )
            .await?;
            assert_eq!(status, StatusCode::NOT_FOUND, "{route}: {body}");
            assert_eq!(body["code"], "not_found", "{route}: {body}");
            assert_eq!(body["details"], json!([]), "{route}: {body}");
            answers.push((body["code"].clone(), body["message"].clone()));
        }
    }
    // Indistinguishable, message included.
    answers.dedup();
    assert_eq!(answers.len(), 1, "{answers:#?}");
    Ok(())
}

const REQUEST_ID: &str = "req_owner-scope-test";
const UNKNOWN_SANDBOX: &str = "sbx_00000000-0000-7000-8000-000000000000";
const UNKNOWN_OPERATION: &str = "op_00000000-0000-7000-8000-000000000000";

fn request_id_headers(mut headers: HeaderMap) -> HeaderMap {
    headers.insert("x-request-id", REQUEST_ID.parse().unwrap());
    headers
}

fn error_body(code: &str, message: &str) -> Value {
    json!({
        "code": code,
        "message": message,
        "request_id": REQUEST_ID,
        "retryable": false,
        "details": [],
    })
}

/// `template` with `{id_param}` replaced by `id` and any other path parameter
/// by a placeholder: authorization answers before those are read.
fn concrete_path(template: &str, id_param: &str, id: &str) -> String {
    let mut path = String::new();
    let mut rest = template;
    while let Some((head, tail)) = rest.split_once('{') {
        let (name, tail) = tail.split_once('}').expect("path parameter should close");
        path.push_str(head);
        path.push_str(if name == id_param { id } else { "placeholder" });
        rest = tail;
    }
    path.push_str(rest);
    path
}

#[tokio::test]
async fn every_path_id_route_rejects_a_malformed_id_and_hides_an_unknown_one() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let spec = crate::routes::public_docs::full_openapi_json().await.0;
    let mut swept = BTreeSet::new();
    for (operation, _) in super::super::mounted_operations() {
        let (id_param, malformed, unknown, invalid, not_found) =
            if operation.path().contains("{sandbox_id}") {
                (
                    "sandbox_id",
                    "not-a-sandbox-id",
                    UNKNOWN_SANDBOX,
                    "invalid sandbox id",
                    "sandbox not found",
                )
            } else if operation.path().contains("{operation_id}") {
                (
                    "operation_id",
                    "not-an-operation-id",
                    UNKNOWN_OPERATION,
                    "invalid operation id",
                    "operation not found",
                )
            } else {
                continue;
            };
        let method = operation.method().openapi_name();
        let documented = &spec["paths"][operation.relative_path()][method]["responses"]["400"];
        assert_eq!(
            documented["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/PublicErrorEnvelope",
            "{method} {} does not document its malformed-id 400 envelope",
            operation.path()
        );
        for headers in [HeaderMap::new(), owner_headers(ORG, "101")] {
            for (id, expected_status, expected_body) in [
                // The same answer the handler gives an unparseable id.
                (
                    malformed,
                    StatusCode::BAD_REQUEST,
                    error_body("invalid_request", invalid),
                ),
                (
                    unknown,
                    StatusCode::NOT_FOUND,
                    error_body("not_found", not_found),
                ),
            ] {
                let path = concrete_path(operation.path(), id_param, id);
                let (status, body) = send(
                    &fixture.app,
                    Method::from_bytes(method.to_ascii_uppercase().as_bytes())?,
                    &path,
                    request_id_headers(headers.clone()),
                    json!({}),
                )
                .await?;
                assert_eq!(status, expected_status, "{method} {path}: {body}");
                assert_eq!(body, expected_body, "{method} {path}");
            }
        }
        swept.insert(operation.path());
    }
    // Guards the sweep itself: point reads, `:action` suffixes, sub-resources
    // and both operation routes.
    for path in [
        pplx_space_public_api::routes::GET_SANDBOX.path(),
        pplx_space_public_api::routes::SUSPEND_SANDBOX.path(),
        pplx_space_public_api::routes::LIST_PROCESSES.path(),
        pplx_space_public_api::routes::GET_OPERATION.path(),
        pplx_space_public_api::routes::LIST_OPERATION_EVENTS.path(),
    ] {
        assert!(swept.contains(path), "{path} was not swept");
    }
    Ok(())
}

/// The middleware parses only what precedes the first `:` of `{sandbox_id}`
/// (the `:action` suffix), so a suffixed id naming an owned sandbox passes it
/// and the handler's own parse answers. One route per handler module is
/// enough: the GETs need no body, and changes-enable stands in for `files`,
/// which has no GET route.
#[tokio::test]
async fn a_suffixed_id_of_an_owned_sandbox_gets_the_handlers_malformed_id_answer()
-> TestDbResult<()> {
    let fixture = fixture().await?;
    let (status, created) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        HeaderMap::new(),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{created}");
    let sandbox = seed_target(&fixture, &created).await?;
    let mut swept = BTreeSet::new();
    for (operation, _) in super::super::mounted_operations() {
        if !operation.path().contains("{sandbox_id}")
            || (operation.method() != pplx_space_public_api::routes::NativeHttpMethod::Get
                && operation != pplx_space_public_api::routes::ENABLE_FILE_CHANGES)
        {
            continue;
        }
        let method = operation.method().openapi_name();
        for suffix in [":x", "%3Ax"] {
            let path = concrete_path(
                operation.path(),
                "sandbox_id",
                &format!("{sandbox}{suffix}"),
            );
            let (status, body) = send(
                &fixture.app,
                Method::from_bytes(method.to_ascii_uppercase().as_bytes())?,
                &path,
                request_id_headers(HeaderMap::new()),
                json!({}),
            )
            .await?;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{method} {path}: {body}");
            assert_eq!(
                body,
                error_body("invalid_request", "invalid sandbox id"),
                "{method} {path}"
            );
        }
        swept.insert(operation.path());
    }
    // One route per handler module that parses `{sandbox_id}` itself.
    for path in [
        pplx_space_public_api::routes::GET_SANDBOX.path(),
        pplx_space_public_api::routes::GET_SANDBOX_NETWORK_POLICY.path(),
        pplx_space_public_api::routes::LIST_SESSIONS.path(),
        pplx_space_public_api::routes::LIST_PROCESSES.path(),
        pplx_space_public_api::routes::LIST_PORTS.path(),
        pplx_space_public_api::routes::LIST_SNAPSHOTS.path(),
        pplx_space_public_api::routes::ENABLE_FILE_CHANGES.path(),
        pplx_space_public_api::routes::STREAM_SANDBOX_LIFECYCLE_EVENTS.path(),
    ] {
        assert!(swept.contains(path), "{path} was not swept");
    }
    Ok(())
}

#[tokio::test]
async fn a_sandbox_outside_the_callers_scope_is_answered_as_an_unknown_one() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let (status, local) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        HeaderMap::new(),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{local}");
    let local_sandbox = seed_target(&fixture, &local).await?;
    let federated = create(&fixture, ORG, "101").await?;
    let federated_sandbox = seed_target(&fixture, &federated).await?;
    // A local credential in the federated sandbox's org, which is not the
    // local sandbox's org.
    const OTHER_ORG_KEY: &str = "local-key-in-another-org";
    sqlx::query(sql!(
        "INSERT INTO api_keys (id, org_id, name, key_hash, allowed_surfaces)
         SELECT $1, org_id, 'other-org-scope-test', $2, ARRAY['native']
         FROM sandboxes WHERE id = $3"
    ))
    .bind(Uuid::now_v7())
    .bind(crate::auth::hash_token(OTHER_ORG_KEY))
    .bind(federated_sandbox.as_uuid())
    .execute(&fixture.db.pool)
    .await?;
    let mut other_org = HeaderMap::new();
    other_org.insert("authorization", format!("Bearer {OTHER_ORG_KEY}").parse()?);
    // Each sandbox is visible to its own caller, so the 404s below come from
    // the scope check rather than from a row that was never seeded.
    for (sandbox, headers) in [
        (local_sandbox, HeaderMap::new()),
        (federated_sandbox, owner_headers(ORG, "101")),
    ] {
        let (status, body) = send(
            &fixture.app,
            Method::GET,
            &format!("/v1/sandboxes/{sandbox}"),
            headers,
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::OK, "{sandbox}: {body}");
    }
    // The other-org read is also refused by the handler's own org filter; only
    // the other-subject read depends on the middleware's scope check alone.
    for (sandbox, headers) in [
        (UNKNOWN_SANDBOX.to_owned(), HeaderMap::new()),
        (local_sandbox.to_string(), other_org),
        (federated_sandbox.to_string(), owner_headers(ORG, "102")),
    ] {
        let (status, body) = send(
            &fixture.app,
            Method::GET,
            &format!("/v1/sandboxes/{sandbox}"),
            request_id_headers(headers),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{sandbox}: {body}");
        assert_eq!(
            body,
            error_body("not_found", "sandbox not found"),
            "{sandbox}"
        );
    }
    Ok(())
}

#[test]
fn federated_idempotency_key_is_checked_before_it_is_hashed() {
    let subject_id = UserId::from_uuid(SEED_USER_ID);
    let with_key = |key: String| {
        let mut headers = HeaderMap::new();
        headers.insert("idempotency-key", HeaderValue::from_str(&key).unwrap());
        headers
    };
    let max = pplx_space_public_api::limits::MAX_IDEMPOTENCY_KEY_BYTES;

    let mut accepted = with_key("k".repeat(max));
    namespace_idempotency_key(&mut accepted, subject_id).unwrap();
    assert!(
        accepted["idempotency-key"]
            .to_str()
            .unwrap()
            .starts_with("native-owner-")
    );

    let mut rejected = with_key("k".repeat(max + 1));
    assert!(namespace_idempotency_key(&mut rejected, subject_id).is_err());
    assert_eq!(rejected["idempotency-key"].len(), max + 1, "left unhashed");
}

async fn operation_outcome(
    pool: &sqlx::PgPool,
    operation_id: &Value,
) -> TestDbResult<(String, Option<Value>)> {
    let operation_id: OperationId = operation_id.as_str().unwrap().parse()?;
    let (state, error): (String, Option<sqlx::types::Json<Value>>) = sqlx::query_as(sql!(
        "SELECT state, terminal_error_json FROM public_operations WHERE operation_id = $1"
    ))
    .bind(operation_id.as_uuid())
    .fetch_one(pool)
    .await?;
    Ok((state, error.map(|error| error.0)))
}

/// A delete that arrives before its sandbox's create has recorded the
/// sandbox row is admitted, not answered as an unknown sandbox. It cancels
/// the create, which never runs, and then succeeds with nothing to delete.
/// Its key replays it before and after it runs; once the create is gone, a
/// new delete is unknown again.
#[tokio::test]
async fn a_delete_before_its_sandboxs_row_exists_cancels_the_create() -> TestDbResult<()> {
    use crate::controllers::sandbox::SandboxController;
    use crate::public_operation_driver::{
        OperationExecutionConfig, OperationExecutor, drive_claimed_operation,
    };
    use crate::public_operations::{ClaimLimits, ClaimPublicOperations};
    use crate::public_sandbox_operations::SandboxLifecycleOperationExecutor;
    use pplx_space_public_api::OperationKind;
    use std::time::Duration;

    for owner in [HeaderMap::new(), owner_headers(ORG, "101")] {
        let fixture = fixture().await?;
        let pool = &fixture.db.pool;
        let (status, created) = send(
            &fixture.app,
            Method::POST,
            "/v1/sandboxes",
            grant_headers(owner.clone()),
            json!({"template":"seed:latest"}),
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{created}");
        let path = format!("/v1/sandboxes/{}", created["sandbox_id"].as_str().unwrap());
        let (status, deleted) = send(
            &fixture.app,
            Method::DELETE,
            &path,
            owner.clone(),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{deleted}");
        assert_eq!(deleted["sandbox_id"], created["sandbox_id"]);
        assert_ne!(deleted["operation_id"], created["operation_id"]);
        let (state, error) = operation_outcome(pool, &created["operation_id"]).await?;
        assert_eq!(state, "canceled");
        let error = error.expect("a canceled create carries its error");
        assert_eq!(error["reason"], "superseded_by_delete");
        assert_eq!(error["delete_operation_id"], deleted["operation_id"]);
        let (status, replay) = send(
            &fixture.app,
            Method::DELETE,
            &path,
            owner.clone(),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
        assert_eq!(replay["operation_id"], deleted["operation_id"]);

        let store = PublicOperationStore::new(pool.clone());
        let mut claim = store
            .claim_operations(ClaimPublicOperations {
                limits: ClaimLimits::any(8),
                claim_stale_after: Duration::from_secs(60),
                claimable_kinds: &[OperationKind::CreateSandbox, OperationKind::DeleteSandbox],
            })
            .await?;
        let operation = claim.operations.pop().expect("the delete is claimable");
        assert!(
            claim.operations.is_empty(),
            "the canceled create never runs"
        );
        assert_eq!(json!(operation.id), deleted["operation_id"]);
        let executor = SandboxLifecycleOperationExecutor::new(
            pool.clone(),
            SandboxController::new(crate::test_support::direct_substrate(pool)),
        );
        drive_claimed_operation(
            store,
            claim.claim_token,
            OperationExecutionConfig::default(),
            operation.clone(),
            OperationExecutor::Dispatcher,
            async move { executor.execute(&operation).await.into() },
        )
        .await?;
        let (state, _) = operation_outcome(pool, &deleted["operation_id"]).await?;
        assert_eq!(state, "succeeded");
        let rows: i64 = sqlx::query_scalar(sql!("SELECT COUNT(*) FROM sandboxes"))
            .fetch_one(pool)
            .await?;
        assert_eq!(rows, 0);

        let (status, replay) = send(
            &fixture.app,
            Method::DELETE,
            &path,
            owner.clone(),
            Value::Null,
        )
        .await?;
        assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
        assert_eq!(replay["operation_id"], deleted["operation_id"]);
        let mut fresh = owner.clone();
        fresh.insert("idempotency-key", "new-delete".parse()?);
        let (status, response) =
            send(&fixture.app, Method::DELETE, &path, fresh, Value::Null).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    }
    Ok(())
}

/// A create still in flight lets only its own owner scope delete its
/// sandbox: every other caller gets the unknown sandbox's 404, and the
/// create stays queued.
#[tokio::test]
async fn a_create_in_flight_is_deletable_only_in_its_owner_scope() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let created = create(&fixture, ORG, "101").await?;
    let path = format!("/v1/sandboxes/{}", created["sandbox_id"].as_str().unwrap());
    for headers in [
        owner_headers(ORG, "102"),
        owner_headers(OTHER_ORG, "101"),
        HeaderMap::new(),
    ] {
        let (status, response) =
            send(&fixture.app, Method::DELETE, &path, headers, Value::Null).await?;
        assert_eq!(status, StatusCode::NOT_FOUND, "{response}");
    }
    let (state, _) = operation_outcome(&fixture.db.pool, &created["operation_id"]).await?;
    assert_eq!(state, "queued");
    Ok(())
}

/// The create records its row and finishes after the delete's authorization
/// has missed the row but before it looks for the create, so it finds no
/// create in flight either. It reads the row again and admits the delete.
#[tokio::test]
async fn a_delete_racing_its_creates_row_insert_is_still_admitted() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let created = create(&fixture, ORG, "101").await?;
    let path = format!("/v1/sandboxes/{}", created["sandbox_id"].as_str().unwrap());
    let app = fixture.app.clone();
    let (status, deleted) = crate::test_support::finish_create_while_a_request_reads_operations(
        &fixture.db.pool,
        created["operation_id"].as_str().unwrap().parse()?,
        created["sandbox_id"].as_str().unwrap().parse()?,
        async move {
            send(
                &app,
                Method::DELETE,
                &path,
                owner_headers(ORG, "101"),
                Value::Null,
            )
            .await
        },
    )
    .await??;
    assert_eq!(status, StatusCode::ACCEPTED, "{deleted}");
    assert_eq!(deleted["sandbox_id"], created["sandbox_id"]);
    let (state, _) = operation_outcome(&fixture.db.pool, &deleted["operation_id"]).await?;
    assert_eq!(state, "queued");
    Ok(())
}

/// Create a sandbox, claim its create as the dispatcher would, and delete the
/// sandbox before the create has recorded its row. Answers the create's and
/// the delete's responses.
async fn delete_behind_a_running_create(fixture: &Fixture) -> TestDbResult<(Value, Value)> {
    use crate::public_operations::{ClaimLimits, ClaimPublicOperations};
    use pplx_space_public_api::OperationKind;
    use std::time::Duration;

    let created = create(fixture, ORG, "101").await?;
    let claim = PublicOperationStore::new(fixture.db.pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(8),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::CreateSandbox],
        })
        .await?;
    assert_eq!(claim.operations.len(), 1, "the create is claimed");
    let path = format!("/v1/sandboxes/{}", created["sandbox_id"].as_str().unwrap());
    let (status, deleted) = send(
        &fixture.app,
        Method::DELETE,
        &path,
        owner_headers(ORG, "101"),
        Value::Null,
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{deleted}");
    assert_eq!(deleted["sandbox_id"], created["sandbox_id"]);
    Ok((created, deleted))
}

/// A delete that arrives while its create runs, before the create has
/// recorded the sandbox row, is admitted. The running create is left to
/// finish, and the dispatcher holds the delete behind it.
#[tokio::test]
async fn a_delete_while_its_create_runs_before_the_row_exists_waits_for_it() -> TestDbResult<()> {
    use crate::public_operations::{ClaimLimits, ClaimPublicOperations};
    use pplx_space_public_api::OperationKind;
    use std::time::Duration;

    let fixture = fixture().await?;
    let pool = &fixture.db.pool;
    let (created, deleted) = delete_behind_a_running_create(&fixture).await?;
    assert_eq!(
        operation_outcome(pool, &created["operation_id"]).await?,
        ("running".to_owned(), None)
    );
    let (state, _) = operation_outcome(pool, &deleted["operation_id"]).await?;
    assert_eq!(state, "queued");
    let claim = PublicOperationStore::new(pool.clone())
        .claim_operations(ClaimPublicOperations {
            limits: ClaimLimits::any(8),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::CreateSandbox, OperationKind::DeleteSandbox],
        })
        .await?;
    assert!(
        claim.operations.is_empty(),
        "the delete waits for the running create"
    );
    Ok(())
}

/// Without the keyed claim nothing holds a delete behind its sandbox's
/// running create. A delete that runs before the create has recorded the
/// sandbox row must still not finish first, reporting a sandbox the create
/// then records: it waits, and once the create has recorded the sandbox, it
/// deletes it.
#[tokio::test]
async fn an_unkeyed_delete_waits_for_a_create_yet_to_record_its_row() -> TestDbResult<()> {
    use crate::controllers::sandbox::SandboxController;
    use crate::public_operations::{
        ClaimLimits, ClaimPublicOperations, PublicOperationRequeueReason,
    };
    use crate::public_sandbox_operations::SandboxLifecycleOperationExecutor;
    use pplx_space_public_api::{OperationKind, OperationState};
    use std::time::Duration;

    let fixture = fixture().await?;
    let pool = &fixture.db.pool;
    let (created, deleted) = delete_behind_a_running_create(&fixture).await?;
    let mut claim = PublicOperationStore::new(pool.clone())
        .claim_operations_unkeyed(ClaimPublicOperations {
            limits: ClaimLimits::any(8),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    let delete = claim
        .operations
        .pop()
        .expect("the unkeyed claim takes the delete");
    assert_eq!(json!(delete.id), deleted["operation_id"]);
    let executor = SandboxLifecycleOperationExecutor::new(
        pool.clone(),
        SandboxController::new(crate::test_support::direct_substrate(pool)),
    );

    let waiting = executor.execute(&delete).await;
    assert_eq!(
        waiting.state,
        OperationState::Queued,
        "{}",
        waiting.progress_message
    );
    assert_eq!(
        waiting.requeue_reason,
        PublicOperationRequeueReason::WorkflowProgress
    );

    crate::test_support::record_the_row_and_succeed(
        &mut *pool.acquire().await?,
        created["operation_id"].as_str().unwrap().parse()?,
        created["sandbox_id"].as_str().unwrap().parse()?,
    )
    .await?;
    let finished = executor.execute(&delete).await;
    assert_eq!(
        finished.state,
        OperationState::Succeeded,
        "{}",
        finished.progress_message
    );
    assert_eq!(finished.progress_message, "sandbox delete dispatched");
    Ok(())
}

/// As above, but the create records the sandbox and finishes after the
/// delete's execution has missed the row and before it looks for the create,
/// so it finds no create in flight either. It reads the row again and
/// deletes the sandbox, instead of reporting it already deleted.
#[tokio::test]
async fn an_unkeyed_delete_racing_its_creates_row_insert_deletes_the_sandbox() -> TestDbResult<()> {
    use crate::controllers::sandbox::SandboxController;
    use crate::public_operations::{ClaimLimits, ClaimPublicOperations};
    use crate::public_sandbox_operations::SandboxLifecycleOperationExecutor;
    use pplx_space_public_api::{OperationKind, OperationState};
    use std::time::Duration;

    let fixture = fixture().await?;
    let pool = &fixture.db.pool;
    let (created, _) = delete_behind_a_running_create(&fixture).await?;
    let mut claim = PublicOperationStore::new(pool.clone())
        .claim_operations_unkeyed(ClaimPublicOperations {
            limits: ClaimLimits::any(8),
            claim_stale_after: Duration::from_secs(60),
            claimable_kinds: &[OperationKind::DeleteSandbox],
        })
        .await?;
    let delete = claim
        .operations
        .pop()
        .expect("the unkeyed claim takes the delete");
    let executor = SandboxLifecycleOperationExecutor::new(
        pool.clone(),
        SandboxController::new(crate::test_support::direct_substrate(pool)),
    );
    let finished = crate::test_support::finish_create_while_a_request_reads_operations(
        pool,
        created["operation_id"].as_str().unwrap().parse()?,
        created["sandbox_id"].as_str().unwrap().parse()?,
        async move { executor.execute(&delete).await },
    )
    .await?;
    assert_eq!(
        finished.state,
        OperationState::Succeeded,
        "{}",
        finished.progress_message
    );
    assert_eq!(finished.progress_message, "sandbox delete dispatched");
    Ok(())
}

/// Two requests with one key delete a sandbox whose create has not recorded
/// its row. The second misses the first's key, then sees the create the
/// first canceled, and misses the row; it must replay the first, not answer
/// that the sandbox does not exist.
#[tokio::test]
async fn a_same_key_retry_racing_an_early_delete_replays_it() -> TestDbResult<()> {
    use crate::public_operations::{
        ApiProduct, CreatePublicOperation, CreatePublicOperationIdempotency,
        CreatePublicOperationResult,
    };
    use crate::public_sandbox_operations::SandboxLifecycleOperationPayload;
    use crate::routes::public::sandboxes::{
        lifecycle_method, lifecycle_path_template, lifecycle_request_fingerprint,
    };
    use pplx_space_common_core::types::IdempotencyKey;
    use pplx_space_public_api::{OperationKind, OperationTargetType, SandboxLifecycleAction};

    let fixture = fixture().await?;
    let pool = &fixture.db.pool;
    let (status, created) = send(
        &fixture.app,
        Method::POST,
        "/v1/sandboxes",
        grant_headers(HeaderMap::new()),
        json!({"template":"seed:latest"}),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED, "{created}");
    let sandbox_id: PublicSandboxId = created["sandbox_id"].as_str().unwrap().parse()?;
    let actor: Option<Uuid> = sqlx::query_scalar(sql!(
        "SELECT actor_user_id FROM public_operations WHERE operation_id = $1"
    ))
    .bind(
        created["operation_id"]
            .as_str()
            .unwrap()
            .parse::<OperationId>()?
            .as_uuid(),
    )
    .fetch_one(pool)
    .await?;

    // The first delete, admitted in a transaction that holds the operations
    // locked until the retry waits on them. Outside the pool, which the retry
    // and the wait draw on.
    let outside = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with((*pool.connect_options()).clone())
        .await?;
    let action = SandboxLifecycleAction::Delete;
    let key = IdempotencyKey::parse("same-key")?;
    let fingerprint = lifecycle_request_fingerprint(sandbox_id, action, None)
        .map_err(|_| "fingerprint the delete")?;
    let idempotency = CreatePublicOperationIdempotency {
        method: lifecycle_method(action),
        path_template: lifecycle_path_template(action),
        key: &key,
        request_fingerprint: &fingerprint,
    };
    let mut first = PublicOperationStore::new(outside.clone())
        .begin_operation_admission(
            OrgId::from_uuid(SEED_ORG_ID),
            ApiProduct::Native,
            &idempotency,
        )
        .await?;
    let app = fixture.app.clone();
    let path = format!("/v1/sandboxes/{sandbox_id}");
    let retry = crate::test_support::start_a_request_blocked_on_operations(
        pool,
        first.transaction(),
        async move { send(&app, Method::DELETE, &path, HeaderMap::new(), Value::Null).await },
    )
    .await?;
    let target_id = sandbox_id.to_string();
    let CreatePublicOperationResult::Created(first_id) = first
        .create(CreatePublicOperation {
            org_id: OrgId::from_uuid(SEED_ORG_ID),
            actor_user_id: actor.map(UserId::from_uuid),
            api_product: ApiProduct::Native,
            kind: OperationKind::DeleteSandbox,
            target_type: Some(OperationTargetType::Sandbox),
            target_id: Some(&target_id),
            payload_json: SandboxLifecycleOperationPayload {
                sandbox_id,
                action,
                rollback: None,
                wake_activity: None,
            }
            .into_json(),
            idempotency: Some(idempotency),
            traceparent: None,
        })
        .await?
    else {
        panic!("the first delete is created");
    };

    outside.close().await;
    let (status, replay) = retry.await??;
    assert_eq!(status, StatusCode::ACCEPTED, "{replay}");
    assert_eq!(replay["operation_id"], json!(first_id));
    let (state, _) = operation_outcome(pool, &created["operation_id"]).await?;
    assert_eq!(state, "canceled");
    Ok(())
}
