use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use k8s_openapi::api::authentication::v1::UserInfo;
use kube::client::{Body, ClientBuilder};
use pplx_space_common_core::types::NodeId;
use pplx_space_common_svc::sql;
use pplx_space_db_test_fixtures::{TestDb, TestDbResult, insert_ready_worker_node, pg_test_db};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
const TEST_POD_NAME: &str = "caller-pod";
const TEST_POD_UID: &str = "018f1f4f-7e70-7f10-bc3c-12b1605a18c5";
const TEST_NODE_NAME: &str = "kind-worker";
const TEST_NODE_UID: &str = "kind-worker-uid";

pub(crate) async fn registered_node_db() -> TestDbResult<(TestDb, NodeId)> {
    let db = pg_test_db().await?;
    let id = NodeId::new();
    insert_ready_worker_node(&db.pool, id).await?;
    sqlx::query(sql!(
        "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
    ))
    .bind(id)
    .bind(TEST_NODE_NAME)
    .bind(TEST_NODE_UID)
    .execute(&db.pool)
    .await?;
    Ok((db, id))
}

fn test_config() -> K8sAuthConfig {
    K8sAuthConfig {
        namespace: "space".to_owned(),
        audience: "space-credential-manager".to_owned(),
        allowed: build_allowed_map(
            "space",
            [
                "space-volume-manager".to_owned(),
                "space-api".to_owned(),
                "space-sandbox-manager".to_owned(),
            ],
        )
        .unwrap(),
        cache_ttl: Duration::from_secs(30),
        negative_cache_ttl: Duration::from_secs(2),
        cache_max_entries: 16,
        max_inflight_reviews: 8,
        review_rate_per_sec: 1000,
        review_timeout: Duration::from_secs(5),
    }
}

fn test_reviewer(config: K8sAuthConfig, client: kube::Client) -> CredmgrTokenReviewer {
    let review_limiter = Arc::new(tokio::sync::Semaphore::new(config.max_inflight_reviews));
    let rate_limiter = Arc::new(ReviewRateLimiter::new(config.review_rate_per_sec));
    let (pods, mut writer) = reflector::store();
    writer.apply_watcher_event(&watcher::Event::Init);
    writer.apply_watcher_event(&watcher::Event::InitApply(pod()));
    writer.apply_watcher_event(&watcher::Event::InitDone);
    CredmgrTokenReviewer {
        reviews: kube::Api::all(client),
        config: Arc::new(config),
        cache: Arc::new(Mutex::new(HashMap::new())),
        in_flight: Arc::new(Mutex::new(HashMap::new())),
        review_limiter,
        rate_limiter,
        pods,
        database_pool: sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap(),
    }
}

fn status(audience: &str, username: &str) -> TokenReviewStatus {
    TokenReviewStatus {
        audiences: Some(vec![audience.to_owned()]),
        authenticated: Some(true),
        user: Some(UserInfo {
            username: Some(username.to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn bound_status(
    audience: &str,
    username: &str,
    node_name: &str,
    node_uid: &str,
) -> TokenReviewStatus {
    let mut status = status(audience, username);
    status.user.as_mut().unwrap().extra = Some(std::collections::BTreeMap::from([
        (
            "authentication.kubernetes.io/pod-name".to_owned(),
            vec![TEST_POD_NAME.to_owned()],
        ),
        (
            "authentication.kubernetes.io/pod-uid".to_owned(),
            vec![TEST_POD_UID.to_owned()],
        ),
        (
            "authentication.kubernetes.io/node-name".to_owned(),
            vec![node_name.to_owned()],
        ),
        (
            "authentication.kubernetes.io/node-uid".to_owned(),
            vec![node_uid.to_owned()],
        ),
    ]));
    status
}

fn reviewer_returning_status(
    config: K8sAuthConfig,
    status: TokenReviewStatus,
) -> (CredmgrTokenReviewer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let body = Arc::new(
        serde_json::to_vec(&TokenReview {
            metadata: Default::default(),
            spec: TokenReviewSpec {
                audiences: None,
                token: None,
            },
            status: Some(status),
        })
        .unwrap(),
    );
    let service = tower::service_fn({
        let calls = Arc::clone(&calls);
        move |_req: http::Request<Body>| {
            let calls = Arc::clone(&calls);
            let body = Arc::clone(&body);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, BoxError>(
                    http::Response::builder()
                        .status(201)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from((*body).clone()))
                        .unwrap(),
                )
            }
        }
    });
    let client = ClientBuilder::new(service, "space").build();
    (test_reviewer(config, client), calls)
}

fn reviewer_returning_error(config: K8sAuthConfig) -> (CredmgrTokenReviewer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let service = tower::service_fn({
        let calls = Arc::clone(&calls);
        move |_req: http::Request<Body>| {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<http::Response<Body>, BoxError>(Box::new(std::io::Error::other(
                    "apiserver unavailable",
                )))
            }
        }
    });
    let client = ClientBuilder::new(service, "space").build();
    (test_reviewer(config, client), calls)
}

pub(crate) fn caller_reviewer(
    service: ClientService,
    database_pool: sqlx::PgPool,
) -> (CredmgrTokenReviewer, Arc<AtomicUsize>) {
    let mut config = test_config();
    config.allowed = build_allowed_map("space", [service.to_string()]).unwrap();
    let reviewed = bound_status(
        &config.audience,
        &format!("system:serviceaccount:space:{service}"),
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    let (mut reviewer, calls) = reviewer_returning_status(config, reviewed);
    reviewer.database_pool = database_pool;
    (reviewer, calls)
}

async fn assert_status_rejected(status: TokenReviewStatus) {
    let (reviewer, _) = reviewer_returning_status(test_config(), status);
    let error = reviewer
        .resolve_token_identity(&projected_token(), Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
}

pub(crate) fn test_identity(service: ClientService) -> K8sIdentity {
    K8sIdentity::from_token_review(
        service,
        TEST_POD_NAME,
        TEST_POD_UID,
        TEST_NODE_NAME,
        TEST_NODE_UID,
    )
    .unwrap()
}

fn projected_token() -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    jwt_with_exp(exp)
}

#[tokio::test]
async fn ordinary_service_without_pod_or_node_binding_is_rejected() {
    let mut unbound = status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
    );
    unbound.user.as_mut().unwrap().extra = Some(std::collections::BTreeMap::from([(
        "authentication.kubernetes.io/node-name".to_owned(),
        vec![TEST_NODE_NAME.to_owned()],
    )]));
    let (reviewer, calls) = reviewer_returning_status(test_config(), unbound);
    let error = reviewer
        .resolve_token_identity(&projected_token(), Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn ordinary_service_missing_only_pod_name_is_rejected() {
    let mut reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewed
        .user
        .as_mut()
        .unwrap()
        .extra
        .as_mut()
        .unwrap()
        .remove("authentication.kubernetes.io/pod-name");
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn ordinary_service_missing_only_pod_uid_is_rejected() {
    let mut reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewed
        .user
        .as_mut()
        .unwrap()
        .extra
        .as_mut()
        .unwrap()
        .remove("authentication.kubernetes.io/pod-uid");
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn ordinary_service_with_malformed_pod_name_is_rejected() {
    let mut reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewed
        .user
        .as_mut()
        .unwrap()
        .extra
        .as_mut()
        .unwrap()
        .insert(
            "authentication.kubernetes.io/pod-name".to_owned(),
            vec!["INVALID POD".to_owned()],
        );
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn ordinary_service_with_malformed_node_name_is_rejected() {
    let reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        "INVALID NODE",
        TEST_NODE_UID,
    );
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn ordinary_service_missing_node_uid_is_rejected() {
    let mut reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewed
        .user
        .as_mut()
        .unwrap()
        .extra
        .as_mut()
        .unwrap()
        .remove("authentication.kubernetes.io/node-uid");
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn ordinary_service_with_multiple_node_uids_is_rejected() {
    let mut reviewed = bound_status(
        "space-credential-manager",
        "system:serviceaccount:space:space-volume-manager",
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewed
        .user
        .as_mut()
        .unwrap()
        .extra
        .as_mut()
        .unwrap()
        .insert(
            "authentication.kubernetes.io/node-uid".to_owned(),
            vec!["first-uid".to_owned(), "second-uid".to_owned()],
        );
    assert_status_rejected(reviewed).await;
}

#[tokio::test]
async fn unrecognized_service_account_is_permission_denied() {
    let (reviewer, _) = reviewer_returning_status(
        test_config(),
        status(
            "space-credential-manager",
            "system:serviceaccount:space:space-node-manager",
        ),
    );
    let err = reviewer
        .resolve_token_identity(&projected_token(), Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn audience_mismatch_is_unauthenticated() {
    let (reviewer, _) = reviewer_returning_status(
        test_config(),
        status(
            "some-other-audience",
            "system:serviceaccount:space:space-api",
        ),
    );
    let err = reviewer
        .resolve_token_identity(&projected_token(), Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn apiserver_outage_is_retryable_and_not_cached() {
    let (reviewer, calls) = reviewer_returning_error(test_config());
    let token = projected_token();

    let first = reviewer
        .resolve_token_identity(&token, Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    let second = reviewer
        .resolve_token_identity(&token, Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();

    // Transient infrastructure failure: retryable, and not negative-cached
    // (each attempt re-reviews so a recovered apiserver validates at once).
    assert_eq!(first.code(), tonic::Code::Unavailable);
    assert_eq!(second.code(), tonic::Code::Unavailable);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

fn jwt_with_exp(exp: u64) -> String {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload =
        general_purpose::URL_SAFE_NO_PAD.encode(serde_json::json!({ "exp": exp }).to_string());
    format!("{header}.{payload}.signature")
}

#[test]
fn service_account_name_maps_to_client_service() {
    let allowed = build_allowed_map(
        "space",
        ["space-volume-manager".to_owned(), "space-api".to_owned()],
    )
    .unwrap();
    assert_eq!(
        allowed.get("system:serviceaccount:space:space-volume-manager"),
        Some(&ClientService::VolumeManager)
    );
    assert_eq!(
        allowed.get("system:serviceaccount:space:space-api"),
        Some(&ClientService::ApiSvc)
    );
}

#[test]
fn node_agent_service_account_maps_to_its_client_service() {
    // The prerequisite for the agent presenting a projected token instead of
    // the shared static one: its ServiceAccount must resolve to an identity.
    let allowed = build_allowed_map("space", ["space-credential-manager".to_owned()]).unwrap();
    assert_eq!(
        allowed.get("system:serviceaccount:space:space-credential-manager"),
        Some(&ClientService::NodeCredentialAgent)
    );
}

#[test]
fn shared_service_account_alias_maps_to_node_agent() {
    let allowed = build_allowed_map(
        "space",
        ["space-node-stack=space-credential-manager".to_owned()],
    )
    .unwrap();
    assert_eq!(
        allowed.get("system:serviceaccount:space:space-node-stack"),
        Some(&ClientService::NodeCredentialAgent)
    );
}

#[test]
fn malformed_or_unrecognized_allow_entry_is_rejected() {
    for entry in [
        "space-not-a-client-service",
        "",
        "=space-api",
        "space-node-stack=",
        "space-node-stack=space-api=space-volume-manager",
    ] {
        assert!(
            matches!(
                build_allowed_map("space", [entry.to_owned()]),
                Err(K8sAuthConfigError::InvalidAllowedServiceAccountEntry { .. })
            ),
            "entry {entry:?} must fail startup parsing"
        );
    }
}

#[test]
fn conflicting_aliases_are_rejected_in_both_orders() {
    for entries in [
        [
            "space-node-stack=space-credential-manager".to_owned(),
            "space-node-stack=space-api".to_owned(),
        ],
        [
            "space-node-stack=space-api".to_owned(),
            "space-node-stack=space-credential-manager".to_owned(),
        ],
    ] {
        assert!(matches!(
            build_allowed_map("space", entries),
            Err(K8sAuthConfigError::ConflictingAllowedServiceAccountMapping { .. })
        ));
    }
}

#[test]
fn exact_duplicate_aliases_are_deduplicated() {
    let allowed = build_allowed_map(
        "space",
        [
            "space-node-stack=space-credential-manager".to_owned(),
            "space-node-stack=space-credential-manager".to_owned(),
        ],
    )
    .unwrap();
    assert_eq!(allowed.len(), 1);
    assert_eq!(
        allowed.get("system:serviceaccount:space:space-node-stack"),
        Some(&ClientService::NodeCredentialAgent)
    );
}

#[test]
fn default_allow_set_covers_every_client_service() {
    let names = allowed_service_names();
    let allowed = build_allowed_map("space", names).unwrap();
    assert_eq!(allowed.len(), ClientService::ALL.len());
}

#[test]
fn already_qualified_username_is_left_intact() {
    assert_eq!(
        service_account_username("space", "system:serviceaccount:other:space-api"),
        "system:serviceaccount:other:space-api"
    );
    assert_eq!(
        service_account_username("space", "space-api"),
        "system:serviceaccount:space:space-api"
    );
}

#[test]
fn namespace_prefers_env_over_file() {
    assert_eq!(
        auth_namespace_from(Some("  custom-ns ".to_owned()), Path::new("/nonexistent")),
        Some("custom-ns".to_owned())
    );
    assert_eq!(
        auth_namespace_from(Some("   ".to_owned()), Path::new("/nonexistent")),
        None
    );
}

#[test]
fn accepted_cache_expiry_is_capped_by_jwt_exp() {
    let now = Instant::now();
    let now_system = UNIX_EPOCH + Duration::from_secs(1_000);
    let token = jwt_with_exp(1_010);
    let expires_at = accepted_token_cache_expiry(&token, now, now_system, Duration::from_secs(30));
    // exp(1010) - skew(5) - now(1000) = 5s, which is < the 30s TTL.
    assert_eq!(expires_at, now + Duration::from_secs(5));
}

#[test]
fn accepted_cache_expiry_uses_ttl_when_jwt_exp_is_later() {
    let now = Instant::now();
    let now_system = UNIX_EPOCH + Duration::from_secs(1_000);
    let token = jwt_with_exp(1_600);
    let expires_at = accepted_token_cache_expiry(&token, now, now_system, Duration::from_secs(30));
    assert_eq!(expires_at, now + Duration::from_secs(30));
}

#[test]
fn rate_limiter_caps_burst_then_refills() {
    let limiter = ReviewRateLimiter::new(10);
    let start = Instant::now();
    for _ in 0..10 {
        assert!(limiter.try_acquire_at(start));
    }
    assert!(!limiter.try_acquire_at(start));
    assert!(limiter.try_acquire_at(start + Duration::from_secs(1)));
}

#[test]
fn rate_limiter_ignores_out_of_order_timestamps() {
    let limiter = ReviewRateLimiter::new(5);
    let t_old = Instant::now();
    let t_new = t_old + Duration::from_secs(10);
    for _ in 0..5 {
        assert!(limiter.try_acquire_at(t_old));
    }
    let mut granted = 0;
    for _ in 0..100 {
        if limiter.try_acquire_at(t_old) {
            granted += 1;
        }
        if limiter.try_acquire_at(t_new) {
            granted += 1;
        }
    }
    assert!(
        granted <= 6,
        "out-of-order timestamps minted extra tokens: granted {granted} (burst 5)"
    );
}

#[test]
fn cache_review_result_evicts_earliest_expiring_at_capacity() {
    let now = Instant::now();
    let earliest = TokenCacheKey {
        token_digest: token_digest("first"),
        audience: "space-credential-manager".to_owned(),
    };
    let later = TokenCacheKey {
        token_digest: token_digest("second"),
        audience: "space-credential-manager".to_owned(),
    };
    let inserted = TokenCacheKey {
        token_digest: token_digest("third"),
        audience: "space-credential-manager".to_owned(),
    };
    let mut cache = HashMap::from([
        (
            earliest.clone(),
            CachedReview {
                result: CachedReviewResult::Accepted {
                    identity: test_identity(ClientService::ApiSvc),
                },
                expires_at: now + Duration::from_secs(5),
            },
        ),
        (
            later.clone(),
            CachedReview {
                result: CachedReviewResult::Accepted {
                    identity: test_identity(ClientService::ApiSvc),
                },
                expires_at: now + Duration::from_secs(30),
            },
        ),
    ]);

    let (tag, evicted_capacity) = cache_review_result(
        &mut cache,
        inserted.clone(),
        CachedReviewResult::Accepted {
            identity: test_identity(ClientService::VolumeManager),
        },
        now + Duration::from_secs(20),
        now,
        2,
    );

    assert_eq!(tag, "insert_authenticated");
    assert!(evicted_capacity);
    assert!(!cache.contains_key(&earliest));
    assert!(cache.contains_key(&later));
    assert!(cache.contains_key(&inserted));
}

#[test]
fn validate_status_requires_matching_audience() {
    let status = TokenReviewStatus {
        audiences: Some(vec!["some-other-audience".to_owned()]),
        authenticated: Some(true),
        user: Some(k8s_openapi::api::authentication::v1::UserInfo {
            username: Some("system:serviceaccount:space:space-api".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        validate_token_review_status(Some(status), "space-credential-manager"),
        Err(ReviewFailure::Rejected {
            reason: RejectReason::AudienceMismatch
        })
    );
}

#[test]
fn validate_status_returns_username_on_success() {
    let status = TokenReviewStatus {
        audiences: Some(vec!["space-credential-manager".to_owned()]),
        authenticated: Some(true),
        user: Some(k8s_openapi::api::authentication::v1::UserInfo {
            username: Some("system:serviceaccount:space:space-api".to_owned()),
            ..Default::default()
        }),
        ..Default::default()
    };
    assert_eq!(
        validate_token_review_status(Some(status), "space-credential-manager"),
        Ok(ReviewedToken {
            username: "system:serviceaccount:space:space-api".to_owned(),
            pod_name: None,
            pod_uid: None,
            node_name: None,
            node_uid: None,
        })
    );
}

fn pod() -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": TEST_POD_NAME, "namespace": "space", "uid": TEST_POD_UID},
        "status": {"phase": "Running", "podIPs": [{"ip": "10.0.0.2"}, {"ip": "2001:db8::2"}]}
    }))
    .unwrap()
}

#[tokio::test]
async fn cached_identity_tracks_pod_state() {
    use tonic::Code::{PermissionDenied, Unavailable};
    use watcher::Event::{Apply, Delete, Init, InitDone};
    let (db, _) = registered_node_db().await.unwrap();
    let (mut reviewer, reviews) = caller_reviewer(ClientService::VolumeManager, db.pool.clone());
    let (pods, mut writer) = reflector::store();
    reviewer.pods = pods;
    writer.apply_watcher_event(&Init);
    let mut replacement = pod();
    replacement.metadata.uid = Some("replacement-pod-uid".into());
    let mut pending = pod();
    pending.status.as_mut().unwrap().phase = Some("Pending".into());
    let token = projected_token();
    for (event, failure) in [
        (None, Some((Unavailable, "pod_cache_not_synced"))),
        (Some(InitDone), Some((PermissionDenied, "pod_not_found"))),
        (Some(Apply(pod())), None),
        (
            Some(Apply(replacement)),
            Some((PermissionDenied, "pod_uid_mismatch")),
        ),
        (
            Some(Apply(pending)),
            Some((PermissionDenied, "pod_not_running")),
        ),
        (
            Some(Delete(pod())),
            Some((PermissionDenied, "pod_not_found")),
        ),
        (Some(Apply(pod())), None),
    ] {
        if let Some(event) = event {
            writer.apply_watcher_event(&event);
        }
        let result = reviewer
            .resolve_token_identity(&token, Some("10.0.0.2".parse().unwrap()))
            .await;
        assert_eq!(
            result.as_ref().err().map(|e| (e.code(), e.message())),
            failure
        );
    }
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        1,
        "pod state is never part of the token cache"
    );
}

#[tokio::test]
async fn cached_identity_tracks_postgres_registration() {
    use tonic::Code::{PermissionDenied, Unavailable};
    let (db, node_id) = registered_node_db().await.unwrap();
    let closed_pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    closed_pool.close().await;
    let (mut reviewer, reviews) = caller_reviewer(ClientService::VolumeManager, db.pool.clone());
    let token = projected_token();
    // The same authenticated token must follow current name/UID registration,
    // including deletion and a row that has not acquired its UID yet.
    for (name, uid, registered) in [
        (Some(TEST_NODE_NAME), Some(TEST_NODE_UID), true),
        (Some("replacement-node"), Some(TEST_NODE_UID), false),
        (Some(TEST_NODE_NAME), Some("replacement-uid"), false),
        (Some(TEST_NODE_NAME), None, false),
        (None, None, false),
    ] {
        sqlx::query(sql!("DELETE FROM node_k8s WHERE node_id = $1"))
            .bind(node_id)
            .execute(&db.pool)
            .await
            .unwrap();
        if let Some(name) = name {
            sqlx::query(sql!(
                "INSERT INTO node_k8s (node_id, k8s_node_name, k8s_node_uid) VALUES ($1, $2, $3)"
            ))
            .bind(node_id)
            .bind(name)
            .bind(uid)
            .execute(&db.pool)
            .await
            .unwrap();
        }
        let result = reviewer
            .resolve_token_identity(&token, Some("10.0.0.2".parse().unwrap()))
            .await;
        let expected = (!registered).then_some((PermissionDenied, "node_identity_mismatch"));
        assert_eq!(
            result.as_ref().err().map(|e| (e.code(), e.message())),
            expected,
            "{name:?}/{uid:?}"
        );
    }
    reviewer.database_pool = closed_pool.clone();
    let result = reviewer
        .resolve_token_identity(&token, Some("10.0.0.2".parse().unwrap()))
        .await;
    let expected = Some((Unavailable, "node_store_unavailable"));
    assert_eq!(
        result.as_ref().err().map(|e| (e.code(), e.message())),
        expected
    );
    assert_eq!(
        reviews.load(Ordering::SeqCst),
        1,
        "node registration is never part of the token cache"
    );
}
