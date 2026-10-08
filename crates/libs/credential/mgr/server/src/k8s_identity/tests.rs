use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::credential_store::{CredentialStoreError, PgCredentialStore, SandboxOwner};
use async_trait::async_trait;
use k8s_openapi::api::authentication::v1::UserInfo;
use kube::client::{Body, ClientBuilder};
use pplx_space_common_core::types::{K8sNodeName, K8sNodeUid, NodeId, SandboxId};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;
pub(super) const TEST_POD_NAME: &str = "caller-pod";
pub(super) const TEST_POD_UID: &str = "018f1f4f-7e70-7f10-bc3c-12b1605a18c5";
pub(super) const TEST_NODE_NAME: &str = "kind-worker";
pub(super) const TEST_NODE_UID: &str = "kind-worker-uid";

pub(super) struct Nodes(pub(super) Option<NodeId>);
#[async_trait]
impl CredentialStore for Nodes {
    async fn registered_node(
        &self,
        _: &K8sNodeName,
        _: &K8sNodeUid,
    ) -> Result<Option<NodeId>, CredentialStoreError> {
        Ok(self.0)
    }
    async fn sandbox_owner(
        &self,
        _: SandboxId,
    ) -> Result<Option<SandboxOwner>, CredentialStoreError> {
        Ok(None)
    }
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
        pod_authorization_mode: PodAuthorizationMode::Enforce,
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
        credential_store: Some(Arc::new(Nodes(Some(NodeId::new())))),
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

pub(crate) fn caller_reviewer(service: ClientService) -> (CredmgrTokenReviewer, Arc<AtomicUsize>) {
    let mut config = test_config();
    config.allowed = build_allowed_map("space", [service.to_string()]).unwrap();
    let reviewed = bound_status(
        &config.audience,
        &format!("system:serviceaccount:space:{service}"),
        TEST_NODE_NAME,
        TEST_NODE_UID,
    );
    reviewer_returning_status(config, reviewed)
}

async fn assert_status_rejected(status: TokenReviewStatus) {
    let (mut reviewer, _) = reviewer_returning_status(test_config(), status);
    for mode in [PodAuthorizationMode::Observe, PodAuthorizationMode::Enforce] {
        Arc::make_mut(&mut reviewer.config).pod_authorization_mode = mode;
        let error = reviewer
            .authenticate(&projected_token(), Some("10.0.0.2".parse().unwrap()))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
    }
}

fn test_identity(service: ClientService) -> K8sIdentity {
    K8sIdentity::from_token_review(
        service,
        TEST_POD_NAME,
        TEST_POD_UID,
        TEST_NODE_NAME,
        TEST_NODE_UID,
    )
    .unwrap()
}

pub(super) fn projected_token() -> String {
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 600;
    jwt_with_exp(exp)
}

#[tokio::test]
async fn valid_token_resolves_to_full_kubernetes_identity() {
    let (reviewer, calls) = reviewer_returning_status(
        test_config(),
        bound_status(
            "space-credential-manager",
            "system:serviceaccount:space:space-volume-manager",
            TEST_NODE_NAME,
            TEST_NODE_UID,
        ),
    );
    let token = projected_token();

    let identity = reviewer
        .authenticate(&token, Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap();
    assert_eq!(identity.service(), ClientService::VolumeManager);
    assert_eq!(identity.pod().pod_name().as_str(), TEST_POD_NAME);
    assert_eq!(identity.pod().pod_uid().as_str(), TEST_POD_UID);
    assert_eq!(identity.node().node_name().as_str(), TEST_NODE_NAME);
    assert_eq!(identity.node().node_uid().as_str(), TEST_NODE_UID);
    // Second call is served from cache: no extra TokenReview.
    assert_eq!(
        reviewer
            .authenticate(&token, Some("10.0.0.2".parse().unwrap()))
            .await
            .unwrap(),
        identity
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
        .authenticate(&projected_token(), Some("10.0.0.2".parse().unwrap()))
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
async fn every_service_rechecks_pod_and_peer_on_cached_tokens() {
    for service in ClientService::ALL {
        let (mut reviewer, calls) = caller_reviewer(service);
        let token = projected_token();
        let peer = Some("10.0.0.2".parse().unwrap());
        let identity = reviewer.authenticate(&token, peer).await.unwrap();
        assert_eq!(identity, test_identity(service));
        assert_eq!(reviewer.authenticate(&token, peer).await.unwrap(), identity);

        for (peer, reason) in [
            (None, "peer_address_missing"),
            (Some("10.0.0.99".parse().unwrap()), "peer_ip_mismatch"),
        ] {
            let error = reviewer.authenticate(&token, peer).await.unwrap_err();
            assert_eq!(error.code(), tonic::Code::PermissionDenied, "{service}");
            assert_eq!(error.message(), reason, "{service}");
        }

        let (pods, mut writer) = reflector::store();
        let mut pod = pod();
        pod.metadata.uid = Some("replacement-pod-uid".into());
        writer.apply_watcher_event(&watcher::Event::Init);
        writer.apply_watcher_event(&watcher::Event::InitApply(pod));
        writer.apply_watcher_event(&watcher::Event::InitDone);
        reviewer.pods = pods;
        let error = reviewer.authenticate(&token, peer).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::PermissionDenied, "{service}");
        assert_eq!(error.message(), "pod_uid_mismatch", "{service}");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "{service}: one cached TokenReview"
        );
    }
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
        .authenticate(&projected_token(), Some("10.0.0.2".parse().unwrap()))
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
        .authenticate(&projected_token(), Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

#[tokio::test]
async fn apiserver_outage_is_retryable_and_not_cached() {
    let (reviewer, calls) = reviewer_returning_error(test_config());
    let token = projected_token();

    let first = reviewer
        .authenticate(&token, Some("10.0.0.2".parse().unwrap()))
        .await
        .unwrap_err();
    let second = reviewer
        .authenticate(&token, Some("10.0.0.2".parse().unwrap()))
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

fn auth(
    pod: Option<Pod>,
    credential_store: Option<Arc<dyn CredentialStore>>,
    mode: PodAuthorizationMode,
) -> CredmgrTokenReviewer {
    let (pods, mut writer) = reflector::store();
    writer.apply_watcher_event(&watcher::Event::Init);
    if let Some(pod) = pod {
        writer.apply_watcher_event(&watcher::Event::InitApply(pod));
    }
    writer.apply_watcher_event(&watcher::Event::InitDone);
    let (mut reviewer, _) = caller_reviewer(ClientService::NodeCredentialAgent);
    reviewer.pods = pods;
    reviewer.credential_store = credential_store;
    Arc::make_mut(&mut reviewer.config).pod_authorization_mode = mode;
    reviewer
}

fn pod() -> Pod {
    serde_json::from_value(serde_json::json!({
        "metadata": {"name": TEST_POD_NAME, "namespace": "space", "uid": TEST_POD_UID,
            "ownerReferences": [{"apiVersion": "apps/v1", "kind": "DaemonSet",
                "name": "worker-agents", "uid": "ds-uid", "controller": true}]},
        "spec": {"serviceAccountName": "space-credential-manager", "nodeName": TEST_NODE_NAME},
        "status": {"phase": "Running", "podIPs": [{"ip": "10.0.0.2"}, {"ip": "2001:db8::2"}]}
    }))
    .unwrap()
}

#[tokio::test]
async fn checks_current_pod_node_and_peer_in_both_modes() {
    // Each mutation represents an otherwise legitimate request. The oracle is
    // the four authorization conditions, not the check implementation.
    for case in [
        "allow",
        "uid",
        "phase",
        "owner",
        "no_owner",
        "node",
        "peer",
        "missing_peer",
        "missing_pod",
    ] {
        for mode in [PodAuthorizationMode::Observe, PodAuthorizationMode::Enforce] {
            let node_id = NodeId::new();
            let mut pod = pod();
            let mut peer = Some("10.0.0.2".parse().unwrap());
            let reason = match case {
                "allow" => None,
                "uid" => {
                    pod.metadata.uid = Some("replacement".into());
                    Some("pod_uid_mismatch")
                }
                "phase" => {
                    pod.status.as_mut().unwrap().phase = Some("Pending".into());
                    Some("pod_not_running")
                }
                "owner" => {
                    let owner = &mut pod.metadata.owner_references.as_mut().unwrap()[0];
                    owner.kind = "ReplicaSet".into();
                    owner.name = "workload".into();
                    None
                }
                "no_owner" => {
                    pod.metadata.owner_references = None;
                    None
                }
                "node" => Some("node_identity_mismatch"),
                "peer" => {
                    peer = Some("10.0.0.99".parse().unwrap());
                    Some("peer_ip_mismatch")
                }
                "missing_peer" => {
                    peer = None;
                    Some("peer_address_missing")
                }
                "missing_pod" => Some("pod_not_found"),
                _ => unreachable!(),
            };
            let auth = auth(
                (case != "missing_pod").then_some(pod),
                Some(Arc::new(Nodes((case != "node").then_some(node_id)))),
                mode,
            );
            let result = auth.authenticate(&projected_token(), peer).await;
            match (reason, mode) {
                (None, _) | (Some(_), PodAuthorizationMode::Observe) => {
                    result.unwrap();
                }
                (Some(reason), PodAuthorizationMode::Enforce) => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::PermissionDenied, "{case}");
                    assert_eq!(error.message(), reason, "{case}");
                }
            }
        }
    }
}

#[tokio::test]
async fn normalizes_mapped_ipv4_and_accepts_each_pod_ip() {
    let (auth, _) = caller_reviewer(ClientService::NodeCredentialAgent);
    for ip in ["10.0.0.2", "::ffff:10.0.0.2", "2001:db8::2"] {
        auth.authenticate(&projected_token(), Some(ip.parse().unwrap()))
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn dependency_failures_are_retryable_in_enforce_and_allowed_in_observe() {
    for mode in [PodAuthorizationMode::Observe, PodAuthorizationMode::Enforce] {
        let closed_pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .unwrap();
        closed_pool.close().await;
        for (store, reason) in [
            (
                Some(Arc::new(Nodes(Some(NodeId::new()))) as Arc<dyn CredentialStore>),
                "pod_cache_not_synced",
            ),
            (
                Some(Arc::new(PgCredentialStore::new(closed_pool.clone()))),
                "node_store_unavailable",
            ),
            (None, "node_store_unavailable"),
        ] {
            let mut auth = auth(Some(pod()), store, mode);
            let (cold_store, _writer) = reflector::store();
            if reason == "pod_cache_not_synced" {
                auth.pods = cold_store;
            }
            let result = auth
                .authenticate(&projected_token(), Some("10.0.0.2".parse().unwrap()))
                .await;
            match mode {
                PodAuthorizationMode::Observe => {
                    result.unwrap();
                }
                PodAuthorizationMode::Enforce => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::Unavailable);
                    assert_eq!(error.message(), reason);
                }
            }
        }
    }
}

#[test]
fn missing_or_unknown_mode_enforces() {
    for raw in [
        None,
        Some(""),
        Some("ENFORCE"),
        Some("observ"),
        Some("enforce"),
        Some("observe"),
    ] {
        let mode = PodAuthorizationMode::configured(raw);
        assert_eq!(
            mode,
            if raw == Some("observe") {
                PodAuthorizationMode::Observe
            } else {
                PodAuthorizationMode::Enforce
            }
        );
    }
}

#[tokio::test]
async fn authorization_tracks_reflector_readiness_creation_and_deletion() {
    let (mut reviewer, calls) = caller_reviewer(ClientService::NodeCredentialAgent);
    let (pods, mut writer) = reflector::store();
    reviewer.pods = pods;
    let token = projected_token();
    let peer = Some("10.0.0.2".parse().unwrap());
    let error = reviewer.authenticate(&token, peer).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "pod_cache_not_synced");

    writer.apply_watcher_event(&watcher::Event::Init);
    writer.apply_watcher_event(&watcher::Event::InitDone);
    let error = reviewer.authenticate(&token, peer).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(error.message(), "pod_not_found");

    writer.apply_watcher_event(&watcher::Event::Apply(pod()));
    reviewer.authenticate(&token, peer).await.unwrap();
    writer.apply_watcher_event(&watcher::Event::Delete(pod()));
    let error = reviewer.authenticate(&token, peer).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::PermissionDenied);
    assert_eq!(error.message(), "pod_not_found");
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the TokenReview is cached"
    );
}
