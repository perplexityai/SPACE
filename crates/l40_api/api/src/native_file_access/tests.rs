use axum::{Router, body::Body, http::StatusCode};
use pplx_space_common_core::{sql, types::UserId};
use pplx_space_db_test_fixtures::{SEED_ORG_ID, SEED_USER_ID, TestDb, TestDbResult, pg_test_db};
use pplx_space_native_files::RequestDigest;
use pplx_space_node_api_client::proto::{
    authorize_native_file_request_response::Outcome as Authorized,
    prepare_native_file_call_response::Outcome as Prepared,
};
use pplx_space_public_api::{PublicErrorCode, PublicErrorEnvelope, PublicSandboxId};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::*;
use crate::routes::AppState;

/// A key in the seeded org, which has users, so it acts for one.
const KEY: &str = "native-file-access-key";
/// A key in the seeded org that has no users: a service credential acting for
/// no owner.
const OWNERLESS_KEY: &str = "native-file-access-ownerless-key";
const OWNERLESS_ORG: Uuid = Uuid::from_u128(2);
/// A key that may assert a federated owner.
const FEDERATED_KEY: &str = "native-file-access-federated-key";
const REQUEST_ID: &str = "req-native-file-access";

struct Fixture {
    db: TestDb,
    service: NativeFileAccessService,
    /// The public router, for comparing answers and creating sandboxes.
    public: Router,
}

async fn fixture() -> TestDbResult<Fixture> {
    let db = pg_test_db().await?;
    for (key, org, federated) in [
        (KEY, SEED_ORG_ID, false),
        (OWNERLESS_KEY, OWNERLESS_ORG, false),
        (FEDERATED_KEY, SEED_ORG_ID, true),
    ] {
        sqlx::query(sql!(
            "INSERT INTO api_keys (
                 id, org_id, name, key_hash, allowed_surfaces, federated_owner_assertions_enabled
             ) VALUES ($1, $2, $3, $4, ARRAY['native'], $5)"
        ))
        .bind(Uuid::now_v7())
        .bind(org)
        .bind(key)
        .bind(crate::auth::hash_token(key))
        .bind(federated)
        .execute(&db.pool)
        .await?;
    }
    let mut state = AppState::from_pool(db.pool.clone()).await?;
    state.federated_identity = crate::federated_identity::FederatedIdentityConfig {
        enabled: true,
        policy: crate::federated_identity::FederatedIdentityPolicy::parse(
            Some("test-issuer"),
            None,
            Some(r#"["0"]"#),
        )?,
    };
    let service = NativeFileAccessService::new(
        crate::routes::public::native_file_access_router(state.clone()),
        CancellationToken::new(),
    );
    let public = Router::new()
        .nest("/v1", crate::routes::public::v1_router(state.clone()))
        .with_state(state);
    Ok(Fixture {
        db,
        service,
        public,
    })
}

/// A stopped sandbox in `org`, so a request reaches for its runtime and is
/// answered without one.
async fn seed_sandbox(fixture: &Fixture, org: Uuid) -> TestDbResult<PublicSandboxId> {
    let sandbox_id = PublicSandboxId::new();
    sqlx::query(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         VALUES ($1, $2, $3, 'stopped', 1)"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(SEED_USER_ID)
    .bind(org)
    .execute(&fixture.db.pool)
    .await?;
    Ok(sandbox_id)
}

fn request(
    operation: NativeFileOperation,
    sandbox_id: &PublicSandboxId,
    headers: &[(&str, &str)],
) -> NativeFileRequest {
    NativeFileRequest {
        operation: operation.into(),
        sandbox_id: sandbox_id.to_string(),
        headers: headers
            .iter()
            .map(|(name, value)| NativeHttpHeader {
                name: (*name).to_string(),
                value: value.as_bytes().to_vec(),
            })
            .collect(),
        request_id: REQUEST_ID.to_string(),
    }
}

async fn authorize(fixture: &Fixture, request: NativeFileRequest) -> Authorized {
    fixture
        .service
        .authorize_native_file_request(Request::new(AuthorizeNativeFileRequestRequest {
            request: Some(request),
        }))
        .await
        .expect("authorization answers")
        .into_inner()
        .outcome
        .expect("authorization decides")
}

async fn prepare(
    service: &NativeFileAccessService,
    request: NativeFileRequest,
    fingerprint: &RequestFingerprint,
) -> Prepared {
    service
        .prepare_native_file_call(Request::new(PrepareNativeFileCallRequest {
            request: Some(request),
            request_fingerprint: Some(fingerprint.as_str().to_string()),
        }))
        .await
        .expect("preparation answers")
        .into_inner()
        .outcome
        .expect("preparation decides")
}

fn envelope(refused: &NativeHttpResponse) -> PublicErrorEnvelope {
    serde_json::from_slice(&refused.body).expect("a refusal is a Native envelope")
}

fn header<'a>(refused: &'a NativeHttpResponse, name: &str) -> Option<&'a [u8]> {
    refused
        .headers
        .iter()
        .find(|header| header.name == name)
        .map(|header| header.value.as_slice())
}

fn fingerprint(data: &[u8]) -> RequestFingerprint {
    pplx_space_native_files::write_fingerprint("/workspace/a", false, false, None, &[data.to_vec()])
        .unwrap()
}

fn is_idempotency_conflict(outcome: &Prepared) -> bool {
    matches!(outcome, Prepared::Refused(refused)
        if refused.status == 409 && envelope(refused).code == PublicErrorCode::IdempotencyConflict)
}

/// A request without a valid credential gets exactly what its public route
/// answers: the 401 envelope, carrying the edge's request id.
#[tokio::test]
async fn a_request_without_valid_credentials_is_refused_as_its_route_refuses_it() -> TestDbResult<()>
{
    let fixture = fixture().await?;
    let sandbox_id = seed_sandbox(&fixture, SEED_ORG_ID).await?;
    for credential in [None, Some("Bearer not-a-native-key")] {
        let headers: Vec<(&str, &str)> = credential
            .map(|credential| ("authorization", credential))
            .into_iter()
            .collect();
        let Authorized::Refused(refused) = authorize(
            &fixture,
            request(NativeFileOperation::Stat, &sandbox_id, &headers),
        )
        .await
        else {
            panic!("{credential:?} was authorized");
        };
        assert_eq!(refused.status, 401, "{credential:?}");
        let refusal = envelope(&refused);
        assert_eq!(refusal.code, PublicErrorCode::Unauthorized);
        assert_eq!(refusal.request_id.as_deref(), Some(REQUEST_ID));
        assert_eq!(
            header(&refused, "x-request-id"),
            Some(REQUEST_ID.as_bytes())
        );

        let mut public = axum::http::Request::builder()
            .method("POST")
            .uri(format!("/v1/sandboxes/{sandbox_id}/files:stat"))
            .header("content-type", "application/json")
            .header("x-request-id", REQUEST_ID);
        if let Some(credential) = credential {
            public = public.header("authorization", credential);
        }
        let response = tower::ServiceExt::oneshot(
            fixture.public.clone(),
            public.body(Body::from(r#"{"path":"/workspace"}"#))?,
        )
        .await?;
        assert_eq!(u32::from(response.status().as_u16()), refused.status);
        let body = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
        assert_eq!(
            serde_json::from_slice::<Value>(&body)?,
            serde_json::from_slice::<Value>(&refused.body)?,
            "{credential:?}"
        );
    }
    Ok(())
}

/// A service credential acting for no owner may read files, as on the public
/// routes, and is refused every mutation with the routes' 403.
#[tokio::test]
async fn a_credential_without_an_owner_is_authorized_for_reads_only() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let sandbox_id = seed_sandbox(&fixture, OWNERLESS_ORG).await?;
    let credential = format!("Bearer {OWNERLESS_KEY}");
    let headers = [
        ("authorization", credential.as_str()),
        ("idempotency-key", "ownerless"),
    ];
    for operation in [
        NativeFileOperation::Stat,
        NativeFileOperation::List,
        NativeFileOperation::Read,
    ] {
        let outcome = authorize(&fixture, request(operation, &sandbox_id, &headers)).await;
        assert_eq!(
            outcome,
            Authorized::Authorized(NativeFileAuthorization {
                org_id: OWNERLESS_ORG.to_string(),
                owner_user_id: None,
            }),
            "{operation:?}"
        );
    }
    for operation in [
        NativeFileOperation::Write,
        NativeFileOperation::WriteBatch,
        NativeFileOperation::Mkdir,
        NativeFileOperation::Remove,
        NativeFileOperation::Rename,
        NativeFileOperation::ChangesEnable,
        NativeFileOperation::ChangesDisable,
        NativeFileOperation::Modified,
    ] {
        let Authorized::Refused(refused) =
            authorize(&fixture, request(operation, &sandbox_id, &headers)).await
        else {
            panic!("{operation:?} was authorized for a credential with no owner");
        };
        assert_eq!(refused.status, 403, "{operation:?}");
        assert_eq!(envelope(&refused).code, PublicErrorCode::Forbidden);
    }

    // The owner a credential acts for is the authorization's.
    let owned = seed_sandbox(&fixture, SEED_ORG_ID).await?;
    let credential = format!("Bearer {KEY}");
    let outcome = authorize(
        &fixture,
        request(
            NativeFileOperation::Write,
            &owned,
            &[("authorization", credential.as_str())],
        ),
    )
    .await;
    assert_eq!(
        outcome,
        Authorized::Authorized(NativeFileAuthorization {
            org_id: SEED_ORG_ID.to_string(),
            owner_user_id: Some(SEED_USER_ID.to_string()),
        })
    );
    Ok(())
}

/// A federated owner's `Idempotency-Key` is bound in that owner's namespace,
/// as the public route binds it, so a retry on either server meets the same
/// binding.
#[tokio::test]
async fn a_federated_owners_key_is_bound_in_its_own_namespace() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let owner = [
        (
            crate::federated_identity::FEDERATED_ORG_ID_HEADER,
            "8a3f64c2-1f2b-4c4e-9a90-1f0d4f6f5a01",
        ),
        (
            crate::federated_identity::FEDERATED_USER_ID_HEADER,
            "federated-file-subject",
        ),
    ];
    let now = time::OffsetDateTime::now_utc().unix_timestamp();
    let (expires_at, issued_at) = ((now + 3600).to_string(), now.to_string());
    let mut create = axum::http::Request::builder()
        .method("POST")
        .uri("/v1/sandboxes")
        .header("authorization", format!("Bearer {FEDERATED_KEY}"))
        .header("content-type", "application/json")
        .header("idempotency-key", "federated-file-create")
        .header(
            crate::federated_identity::FEDERATED_SERVICE_GRANT_HEADER,
            "opaque-native-create-grant",
        )
        .header(
            crate::federated_identity::FEDERATED_SERVICE_GRANT_EXPIRES_AT_HEADER,
            expires_at,
        )
        .header(
            crate::federated_identity::FEDERATED_SERVICE_GRANT_ISSUED_AT_HEADER,
            issued_at,
        );
    for (name, value) in owner {
        create = create.header(name, value);
    }
    let response = tower::ServiceExt::oneshot(
        fixture.public.clone(),
        create.body(Body::from(
            serde_json::to_vec(&json!({"template": "seed:latest"})).unwrap(),
        ))?,
    )
    .await?;
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let created: Value =
        serde_json::from_slice(&axum::body::to_bytes(response.into_body(), 64 * 1024).await?)?;
    let sandbox_id: PublicSandboxId = created["sandbox_id"].as_str().unwrap().parse()?;
    let subject: UserId = sqlx::query_scalar(sql!(
        "INSERT INTO sandboxes (id, user_id, org_id, lifecycle, network_policy_generation)
         SELECT $1, actor_user_id, org_id, 'stopped', 1
         FROM public_operations WHERE operation_id = $2
         RETURNING user_id"
    ))
    .bind(sandbox_id.as_uuid())
    .bind(
        created["operation_id"]
            .as_str()
            .unwrap()
            .parse::<pplx_space_public_api::OperationId>()?
            .as_uuid(),
    )
    .fetch_one(&fixture.db.pool)
    .await?;

    let credential = format!("Bearer {FEDERATED_KEY}");
    let mut headers = vec![
        ("authorization", credential.as_str()),
        ("idempotency-key", "federated-file-write"),
    ];
    headers.extend(owner);
    let outcome = prepare(
        &fixture.service,
        request(NativeFileOperation::Write, &sandbox_id, &headers),
        &fingerprint(b"first"),
    )
    .await;
    assert!(!is_idempotency_conflict(&outcome), "{outcome:?}");

    let scoped = |key: &str| {
        let mut digest = RequestDigest::default();
        digest
            .field(sandbox_id.as_uuid().to_string().as_bytes())
            .field(key.as_bytes());
        digest.finish().unwrap().as_str().to_string()
    };
    let mut namespaced = Sha256::new();
    namespaced.update(subject.as_uuid().as_bytes());
    namespaced.update(b"federated-file-write");
    let namespaced = format!("native-owner-{}", hex::encode(namespaced.finalize()));
    let bound: Vec<String> = sqlx::query_scalar(sql!(
        "SELECT idempotency_key FROM public_idempotency_records WHERE path_template = $1"
    ))
    .bind(pplx_space_public_api::routes::WRITE_FILE.path())
    .fetch_all(&fixture.db.pool)
    .await?;
    assert_eq!(bound, vec![scoped(&namespaced)]);
    Ok(())
}

/// A request that reuses a key with a different body is refused before
/// anything reaches for the sandbox's runtime, so it can neither charge an
/// activation nor wake the sandbox. Runtime acquisition starts at the drain
/// gate, whose snapshot check reads `snapshot_storage_locations`: with that
/// table locked, the changed request still gets its 409, while the bound one
/// waits on the lock.
#[tokio::test]
async fn a_changed_request_is_refused_before_the_runtime_is_reached() -> TestDbResult<()> {
    use sqlx::Connection as _;

    let fixture = fixture().await?;
    let sandbox_id = seed_sandbox(&fixture, SEED_ORG_ID).await?;
    let credential = format!("Bearer {KEY}");
    let write = || {
        request(
            NativeFileOperation::Write,
            &sandbox_id,
            &[
                ("authorization", credential.as_str()),
                ("idempotency-key", "file-access-before-runtime"),
            ],
        )
    };
    let outcome = prepare(&fixture.service, write(), &fingerprint(b"first")).await;
    assert!(!is_idempotency_conflict(&outcome), "{outcome:?}");

    let mut lock = sqlx::PgConnection::connect(&fixture.db.database_url).await?;
    let mut held = lock.begin().await?;
    sqlx::query(sql!(
        "LOCK TABLE snapshot_storage_locations IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *held)
    .await?;

    let outcome = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        prepare(&fixture.service, write(), &fingerprint(b"other")),
    )
    .await
    .map_err(|_| "the changed request waited on runtime acquisition")?;
    assert!(is_idempotency_conflict(&outcome), "{outcome:?}");

    let retry = {
        let service = fixture.service.clone();
        let request = write();
        tokio::spawn(async move { prepare(&service, request, &fingerprint(b"first")).await })
    };
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    assert!(
        !retry.is_finished(),
        "the bound request did not wait on runtime acquisition, so the lock proves nothing"
    );
    held.rollback().await?;
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(30), retry).await??;
    assert!(!is_idempotency_conflict(&outcome), "{outcome:?}");
    Ok(())
}

/// The key is bound before the runtime is reached, so a request whose runtime
/// could not be reached still holds its key: a changed request under it is
/// refused, and no grant was issued for either.
#[tokio::test]
async fn a_request_is_bound_before_any_grant() -> TestDbResult<()> {
    let fixture = fixture().await?;
    let sandbox_id = seed_sandbox(&fixture, SEED_ORG_ID).await?;
    let credential = format!("Bearer {KEY}");
    let mkdir = || {
        request(
            NativeFileOperation::Mkdir,
            &sandbox_id,
            &[
                ("authorization", credential.as_str()),
                ("idempotency-key", "file-access-bound-first"),
            ],
        )
    };
    let first = pplx_space_native_files::mkdir_fingerprint("/workspace/d", false, None)?;
    let outcome = prepare(&fixture.service, mkdir(), &first).await;
    let Prepared::Refused(refused) = &outcome else {
        panic!("a stopped sandbox's runtime was granted: {outcome:?}");
    };
    assert_ne!(envelope(refused).code, PublicErrorCode::IdempotencyConflict);

    let changed = pplx_space_native_files::mkdir_fingerprint("/workspace/e", false, None)?;
    let outcome = prepare(&fixture.service, mkdir(), &changed).await;
    assert!(is_idempotency_conflict(&outcome), "{outcome:?}");

    // A bound operation without its fingerprint is the caller's mistake.
    let error = fixture
        .service
        .prepare_native_file_call(Request::new(PrepareNativeFileCallRequest {
            request: Some(mkdir()),
            request_fingerprint: None,
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    Ok(())
}
