use super::*;
use crate::auth::NodeApiAuthenticator;
use std::sync::atomic::{AtomicUsize, Ordering};

use k8s_openapi::api::authentication::v1::UserInfo;
use kube::client::{Body, ClientBuilder};

type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn test_k8s_config() -> K8sTokenAuthConfig {
    K8sTokenAuthConfig {
        namespace: "space".to_string(),
        node_api_audience: "space-node-api".to_string(),
        node_api_internal_audience: "space-node-api-internal".to_string(),
        cluster_api_audience: "space-cluster-api".to_string(),
        node_api_allowed_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-api".to_string()
        ]),
        node_api_guest_bridge_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-data-plane".to_string(),
        ]),
        node_api_peer_node_usernames: BTreeSet::new(),
        node_api_trusted_wait_ready_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-api".to_string(),
        ]),
        node_api_trusted_policy_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-api".to_string(),
        ]),
        template_transfer_usernames: BTreeSet::new(),
        node_api_internal_allowed_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-sandbox-manager".to_string(),
        ]),
        cluster_api_allowed_usernames: BTreeSet::from([
            "system:serviceaccount:space:space-cluster-controller".to_string(),
        ]),
        cache_ttl: Duration::from_secs(30),
        negative_cache_ttl: Duration::from_secs(2),
        cache_max_entries: 2,
        max_inflight_reviews: 8,
        review_rate_per_sec: 1000,
        review_timeout: Duration::from_secs(5),
    }
}

#[tokio::test]
async fn data_plane_identity_cannot_reuse_guest_authority_for_control_or_transfer() {
    for username in [
        "system:serviceaccount:space:space-data-plane",
        "system:serviceaccount:edge:custom-data-plane",
    ] {
        let mut config = test_k8s_config();
        config
            .node_api_allowed_usernames
            .insert(username.to_owned());
        config
            .node_api_guest_bridge_usernames
            .insert(username.to_owned());
        config
            .node_api_trusted_wait_ready_usernames
            .insert(username.to_owned());
        config
            .cluster_api_allowed_usernames
            .insert(username.to_owned());
        // Even an accidental grant on the internal surface must not turn the
        // guest-facing service into a lifecycle controller.
        config
            .node_api_internal_allowed_usernames
            .insert(username.to_owned());
        let (reviewer, calls) =
            reviewer_returning_status(config, token_review_status("space-node-api", username));
        let token = projected_token();
        for (method, allowed) in [
            ("/space.node.api.v1.WorkerApi/DeleteSandbox", false),
            ("/space.node.api.v1.WorkerApi/SpacedStartProcess", true),
            ("/space.node.api.v1.WorkerApi/SpacedRead", true),
            ("/space.node.api.v1.WorkerApi/SpacedWaitReady", true),
            ("/space.node.api.v1.WorkerApi/ServicePortIngress", true),
            ("/space.node.api.v1.NodeInfo/GetNodeInfo", true),
            ("/space.node.api.v1.WorkerApi/CreateSandboxSnapshot", false),
            ("/space.node.api.v1.WorkerApi/KillSandbox", false),
            (
                "/space.node.api.v1.WorkerApi/SpacedFuturePrivilegedMethod",
                false,
            ),
        ] {
            let result = reviewer
                .authenticate(NodeRole::Worker, AuthSurface::Normal, method, &token)
                .await;
            if allowed {
                let caller = result.unwrap();
                assert_eq!(
                    caller.wait_ready_lifecycle_authority(),
                    WaitReadyLifecycleAuthority::Denied,
                    "{method}"
                );
                assert_eq!(
                    caller.data_plane_session_refresh_authority(),
                    DataPlaneSessionRefreshAuthority::Granted,
                    "{method}"
                );
            } else {
                assert_eq!(
                    result.unwrap_err().code(),
                    tonic::Code::PermissionDenied,
                    "{method}"
                );
            }
        }
        // The first denial and all later decisions share a cached authenticated
        // identity, not a cached method-authorization verdict.
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let policy = reviewer
            .config
            .policy(NodeRole::Worker, AuthSurface::WorkerInternal)
            .unwrap();
        assert_eq!(
            reviewer
                .authorize_username(
                    &policy,
                    AuthSurface::WorkerInternal,
                    "/space.node.api.v1.WorkerApiInternal/WakeSandbox",
                    username,
                    false
                )
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        let cluster_policy = reviewer
            .config
            .policy(NodeRole::Cluster, AuthSurface::Normal)
            .unwrap();
        for (method, allowed) in [
            ("RegisterTemplateRelease", false),
            ("ListNodes", false),
            ("FinalizeArtifactBackup", false),
            ("ResolveDataPlaneSessionRoute", true),
            ("BindDataPlaneSessionProcess", true),
            ("RecordDataPlaneSessionReconnect", true),
            ("GetDataPlaneSessionRevocationState", true),
            ("GetDataPlaneOrgRevocationEpoch", true),
            ("GetSandboxRuntimeAccess", true),
            ("UnstableResolveSandboxServiceRoute", true),
            ("UnstableResetSandboxServiceRoute", true),
            ("FutureSessionAdministration", false),
        ] {
            let result = reviewer.authorize_username(
                &cluster_policy,
                AuthSurface::Normal,
                &format!("/space.node.api.v1.ClusterApi/{method}"),
                username,
                false,
            );
            if allowed {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().code(),
                    tonic::Code::PermissionDenied,
                    "{method}"
                );
            }
        }
    }
}

fn cache_key(token: &str, audience: &str) -> TokenCacheKey {
    TokenCacheKey {
        token_digest: token_digest(token),
        audience: audience.to_string(),
    }
}

fn accepted_cache_entry(username: &str, expires_at: Instant) -> CachedTokenReview {
    CachedTokenReview {
        result: CachedTokenReviewResult::Accepted {
            username: username.to_string(),
            node_identity: AuthenticatedNodeIdentity::MissingKubernetesClaims,
        },
        expires_at,
    }
}

fn accepted_token_review_status(audience: &str) -> TokenReviewStatus {
    token_review_status(audience, "system:serviceaccount:space:space-api")
}

fn token_review_status(audience: &str, username: &str) -> TokenReviewStatus {
    TokenReviewStatus {
        audiences: Some(vec![audience.to_string()]),
        authenticated: Some(true),
        user: Some(UserInfo {
            username: Some(username.to_string()),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn token_review_body(status: TokenReviewStatus) -> Vec<u8> {
    serde_json::to_vec(&TokenReview {
        metadata: Default::default(),
        spec: TokenReviewSpec {
            audiences: None,
            token: None,
        },
        status: Some(status),
    })
    .unwrap()
}

fn reviewer_returning_status(
    config: K8sTokenAuthConfig,
    status: TokenReviewStatus,
) -> (K8sTokenReviewer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let body = Arc::new(token_review_body(status));
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
    (K8sTokenReviewer::with_client(config, client), calls)
}

fn reviewer_returning_error(config: K8sTokenAuthConfig) -> (K8sTokenReviewer, Arc<AtomicUsize>) {
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
    (K8sTokenReviewer::with_client(config, client), calls)
}

fn reviewer_that_hangs(config: K8sTokenAuthConfig) -> K8sTokenReviewer {
    let service = tower::service_fn(move |_req: http::Request<Body>| async move {
        // Never completes; the review timeout must fire.
        std::future::pending::<()>().await;
        Ok::<http::Response<Body>, BoxError>(http::Response::new(Body::from(Vec::new())))
    });
    let client = ClientBuilder::new(service, "space").build();
    K8sTokenReviewer::with_client(config, client)
}

fn jwt_with_exp(exp: u64) -> String {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload =
        general_purpose::URL_SAFE_NO_PAD.encode(serde_json::json!({ "exp": exp }).to_string());
    format!("{header}.{payload}.signature")
}

fn jwt_with_payload(payload: serde_json::Value) -> String {
    let header = general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"none","typ":"JWT"}"#);
    let payload = general_purpose::URL_SAFE_NO_PAD.encode(payload.to_string());
    format!("{header}.{payload}.signature")
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
async fn peer_node_identity_is_confined_to_template_export() {
    let username = "system:serviceaccount:space:space-node-api";
    let mut config = test_k8s_config();
    config
        .node_api_allowed_usernames
        .insert(username.to_owned());
    config
        .node_api_peer_node_usernames
        .insert(username.to_owned());
    config
        .template_transfer_usernames
        .insert(username.to_owned());
    // Listing the account for policy trust or on the internal surface must
    // not widen what a peer node may call.
    config
        .node_api_trusted_policy_usernames
        .insert(username.to_owned());
    config
        .node_api_internal_allowed_usernames
        .insert(username.to_owned());
    let (reviewer, calls) =
        reviewer_returning_status(config, token_review_status("space-node-api", username));
    let token = projected_token();
    for (method, allowed) in [
        ("/space.node.api.v1.WorkerApi/KillSandbox", false),
        ("/space.node.api.v1.WorkerApi/NodeExportTemplate", true),
        ("/space.node.api.v1.WorkerApi/SandboxState", false),
        ("/space.node.api.v1.WorkerApi/SandboxStats", false),
        ("/space.node.api.v1.WorkerApi/StopSandbox", false),
        ("/space.node.api.v1.WorkerApi/DeleteSandbox", false),
        ("/space.node.api.v1.WorkerApi/PullTemplate", false),
        ("/space.node.api.v1.WorkerApi/ListNodeTemplates", false),
        (
            "/space.node.api.v1.WorkerApi/SetSandboxNetworkPolicy",
            false,
        ),
        ("/space.node.api.v1.NodeInfo/GetNodeInfo", true),
    ] {
        let result = reviewer
            .authenticate(NodeRole::Worker, AuthSurface::Normal, method, &token)
            .await;
        if allowed {
            assert_eq!(
                result.unwrap().template_transfer_authority(),
                crate::auth::template_transfer::TemplateTransferAuthority::Granted,
                "{method}"
            );
        } else {
            assert_eq!(
                result.unwrap_err().code(),
                tonic::Code::PermissionDenied,
                "{method}"
            );
        }
    }
    // One cached identity; the method decision is made on every call.
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let builder_policy = reviewer
        .config
        .policy(NodeRole::Builder, AuthSurface::Normal)
        .unwrap();
    for (method, allowed) in [
        ("/space.node.api.v1.BuilderApi/NodeExportTemplate", true),
        ("/space.node.api.v1.BuilderApi/DeleteTemplate", false),
        ("/space.node.api.v1.BuilderApi/BuildTemplate", false),
        ("/space.node.api.v1.WorkerApi/NodeExportTemplate", false),
    ] {
        let result = reviewer.authorize_username(
            &builder_policy,
            AuthSurface::Normal,
            method,
            username,
            false,
        );
        assert_eq!(result.is_ok(), allowed, "{method}");
    }
    let internal_policy = reviewer
        .config
        .policy(NodeRole::Worker, AuthSurface::WorkerInternal)
        .unwrap();
    assert_eq!(
        reviewer
            .authorize_username(
                &internal_policy,
                AuthSurface::WorkerInternal,
                "/space.node.api.v1.WorkerApiInternal/WakeSandbox",
                username,
                false,
            )
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

#[test]
fn service_account_names_are_qualified() {
    assert_eq!(
        service_account_username("space", "workers"),
        "system:serviceaccount:space:workers"
    );
    assert_eq!(
        service_account_username("space", "system:serviceaccount:other:workers"),
        "system:serviceaccount:other:workers"
    );
}

#[test]
fn k8s_policy_selects_surface_audience_and_prequalified_service_accounts() {
    let config = test_k8s_config();

    let normal = config
        .policy(NodeRole::Worker, AuthSurface::Normal)
        .unwrap();
    assert_eq!(normal.audience, "space-node-api");
    assert_eq!(
        *normal.allowed_usernames,
        BTreeSet::from(["system:serviceaccount:space:space-api".to_string()])
    );
    assert_eq!(
        normal.trusted_wait_ready_usernames,
        Some(&BTreeSet::from([
            "system:serviceaccount:space:space-api".to_string()
        ]))
    );

    let internal = config
        .policy(NodeRole::Worker, AuthSurface::WorkerInternal)
        .unwrap();
    assert_eq!(internal.audience, "space-node-api-internal");
    assert_eq!(
        *internal.allowed_usernames,
        BTreeSet::from(["system:serviceaccount:space:space-sandbox-manager".to_string()])
    );
    assert!(internal.trusted_wait_ready_usernames.is_none());

    let cluster = config
        .policy(NodeRole::Cluster, AuthSurface::Normal)
        .unwrap();
    assert_eq!(cluster.audience, "space-cluster-api");
    assert_eq!(
        *cluster.allowed_usernames,
        BTreeSet::from(["system:serviceaccount:space:space-cluster-controller".to_string()])
    );
    assert!(cluster.trusted_wait_ready_usernames.is_none());
}

#[test]
fn wait_ready_lifecycle_authority_matches_the_complete_service_account_username() {
    let mut config = test_k8s_config();
    config.node_api_allowed_usernames = BTreeSet::from([
        "system:serviceaccount:space:space-api".to_string(),
        "system:serviceaccount:tenant-a:space-api".to_string(),
    ]);
    config.node_api_trusted_wait_ready_usernames =
        BTreeSet::from(["system:serviceaccount:space:space-api".to_string()]);
    let policy = config
        .policy(NodeRole::Worker, AuthSurface::Normal)
        .unwrap();

    assert_eq!(
        policy.wait_ready_lifecycle_authority("system:serviceaccount:space:space-api"),
        WaitReadyLifecycleAuthority::Granted
    );
    assert_eq!(
        policy.wait_ready_lifecycle_authority("system:serviceaccount:tenant-a:space-api"),
        WaitReadyLifecycleAuthority::Denied
    );
}

#[test]
fn data_plane_session_refresh_authority_is_granted_only_to_guest_bridge_accounts() {
    let mut config = test_k8s_config();
    config
        .cluster_api_allowed_usernames
        .insert("system:serviceaccount:space:space-data-plane".to_string());
    let policy = config
        .policy(NodeRole::Cluster, AuthSurface::Normal)
        .unwrap();

    assert_eq!(
        policy.data_plane_session_refresh_authority("system:serviceaccount:space:space-data-plane"),
        DataPlaneSessionRefreshAuthority::Granted
    );
    assert_eq!(
        policy.data_plane_session_refresh_authority(
            "system:serviceaccount:space:space-cluster-controller"
        ),
        DataPlaneSessionRefreshAuthority::Denied
    );
}

#[test]
fn network_policy_authority_gates_worker_and_builder_on_the_trusted_policy_set() {
    // The trusted policy set is explicit and independent of wait-ready. Both
    // worker and builder serve the policy RPCs, so both enforce it.
    let mut config = test_k8s_config();
    config.node_api_trusted_policy_usernames =
        BTreeSet::from(["system:serviceaccount:space:space-cluster-controller".to_string()]);
    for (role, surface) in [
        (NodeRole::Worker, AuthSurface::Normal),
        (NodeRole::Worker, AuthSurface::WorkerInternal),
        (NodeRole::Builder, AuthSurface::Normal),
    ] {
        let policy = config.policy(role, surface).unwrap();
        assert_eq!(
            policy.network_policy_mutation_authority(
                "system:serviceaccount:space:space-cluster-controller"
            ),
            NetworkPolicyMutationAuthority::Granted,
            "{role:?}"
        );
        // An admitted but untrusted data-plane account is refused — the vector.
        assert_eq!(
            policy.network_policy_mutation_authority("system:serviceaccount:space:workers"),
            NetworkPolicyMutationAuthority::Denied,
            "{role:?}"
        );
    }
}

#[test]
fn network_policy_authority_fails_closed_when_set_is_empty() {
    // An empty trusted policy set denies everyone on the policy-serving surfaces
    // (this is rejected at startup by validate_for_role, but the runtime check
    // must not fall open if it is ever reached).
    let mut config = test_k8s_config();
    config.node_api_trusted_policy_usernames.clear();
    for (role, surface) in [
        (NodeRole::Worker, AuthSurface::Normal),
        (NodeRole::Worker, AuthSurface::WorkerInternal),
        (NodeRole::Builder, AuthSurface::Normal),
    ] {
        let policy = config.policy(role, surface).unwrap();
        assert_eq!(
            policy.network_policy_mutation_authority("system:serviceaccount:space:space-api"),
            NetworkPolicyMutationAuthority::Denied,
            "{role:?}"
        );
    }
}

#[test]
fn network_policy_authority_is_denied_on_non_policy_surfaces() {
    // Cluster-API and the builder internal surface do not serve policy RPCs.
    let config = test_k8s_config();
    let cluster = config
        .policy(NodeRole::Cluster, AuthSurface::Normal)
        .unwrap();
    assert_eq!(
        cluster.network_policy_mutation_authority("system:serviceaccount:space:space-api"),
        NetworkPolicyMutationAuthority::Denied
    );
    let internal = config
        .policy(NodeRole::Builder, AuthSurface::WorkerInternal)
        .unwrap();
    assert_eq!(
        internal.network_policy_mutation_authority("system:serviceaccount:space:space-api"),
        NetworkPolicyMutationAuthority::Denied
    );
}

#[test]
fn auth_config_requires_trusted_sets_for_policy_serving_roles() {
    // Wait-ready is required for workers only; the trusted policy set is required
    // for both workers and builders (both serve the policy RPCs).
    let mut config = test_k8s_config();
    config.node_api_trusted_wait_ready_usernames.clear();
    assert!(matches!(
        config.validate_for_role(NodeRole::Worker),
        Err(AuthConfigError::TrustedWaitReadyServiceAccountsMissing)
    ));

    let mut config = test_k8s_config();
    config.node_api_trusted_policy_usernames.clear();
    assert!(matches!(
        config.validate_for_role(NodeRole::Worker),
        Err(AuthConfigError::TrustedPolicyServiceAccountsMissing)
    ));
    assert!(matches!(
        config.validate_for_role(NodeRole::Builder),
        Err(AuthConfigError::TrustedPolicyServiceAccountsMissing)
    ));
    // The cluster role does not serve the policy RPCs, so it is unaffected.
    config.validate_for_role(NodeRole::Cluster).unwrap();

    // A fully configured node validates for every role.
    test_k8s_config()
        .validate_for_role(NodeRole::Builder)
        .expect("configured builder validates");
}

#[test]
fn k8s_policy_fails_closed_for_empty_allow_list() {
    let mut config = test_k8s_config();
    config.node_api_allowed_usernames.clear();

    let error = config
        .policy(NodeRole::Worker, AuthSurface::Normal)
        .unwrap_err();
    assert_eq!(error, AuthPolicyRejection::NoAllowedServiceAccounts);
    assert_eq!(error.metric_reason(), "no_allowed_service_accounts");
}

#[test]
fn prune_token_review_cache_removes_expired_entries() {
    let now = Instant::now();
    let expired = cache_key("expired", "space-node-api");
    let active = cache_key("active", "space-node-api");
    let mut cache = HashMap::from([
        (
            expired.clone(),
            accepted_cache_entry("expired-sa", now - Duration::from_secs(1)),
        ),
        (
            active.clone(),
            accepted_cache_entry("active-sa", now + Duration::from_secs(30)),
        ),
    ]);

    prune_token_review_cache(&mut cache, now, 10);

    assert!(!cache.contains_key(&expired));
    assert!(cache.contains_key(&active));
}

#[test]
fn prune_token_review_cache_clears_when_capacity_is_zero() {
    let now = Instant::now();
    let mut cache = HashMap::from([(
        cache_key("active", "space-node-api"),
        accepted_cache_entry("active-sa", now + Duration::from_secs(30)),
    )]);

    prune_token_review_cache(&mut cache, now, 0);

    assert!(cache.is_empty());
}

#[test]
fn cache_token_review_result_caches_negative_result() {
    let now = Instant::now();
    let key = cache_key("bad", "space-node-api");
    let mut cache = HashMap::new();

    let (tag, evicted_capacity) = cache_token_review_result(
        &mut cache,
        key.clone(),
        CachedTokenReviewResult::Rejected {
            reason: TokenReviewRejectReason::Rejected,
        },
        now + Duration::from_secs(2),
        now,
        10,
    );

    assert_eq!(tag, "insert_deny");
    assert!(!evicted_capacity);
    assert_eq!(
        cache[&key].result,
        CachedTokenReviewResult::Rejected {
            reason: TokenReviewRejectReason::Rejected
        }
    );
}

#[test]
fn cache_token_review_result_evicts_earliest_expiring_entry_at_capacity() {
    let now = Instant::now();
    let earliest = cache_key("first", "space-node-api");
    let later = cache_key("second", "space-node-api");
    let inserted = cache_key("third", "space-node-api");
    let mut cache = HashMap::from([
        (
            earliest.clone(),
            accepted_cache_entry("first-sa", now + Duration::from_secs(5)),
        ),
        (
            later.clone(),
            accepted_cache_entry("second-sa", now + Duration::from_secs(30)),
        ),
    ]);

    let (tag, evicted_capacity) = cache_token_review_result(
        &mut cache,
        inserted.clone(),
        CachedTokenReviewResult::Accepted {
            username: "third-sa".to_string(),
            node_identity: AuthenticatedNodeIdentity::MissingKubernetesClaims,
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
fn accepted_token_cache_expiry_is_capped_by_jwt_exp() {
    let now = Instant::now();
    let now_system = UNIX_EPOCH + Duration::from_secs(1_000);
    let token = jwt_with_exp(1_010);

    let expires_at = accepted_token_cache_expiry(&token, now, now_system, Duration::from_secs(30));

    assert_eq!(expires_at, now + Duration::from_secs(5));
}

#[test]
fn accepted_token_cache_expiry_uses_ttl_when_jwt_exp_is_later() {
    let now = Instant::now();
    let now_system = UNIX_EPOCH + Duration::from_secs(1_000);
    let token = jwt_with_exp(1_600);

    let expires_at = accepted_token_cache_expiry(&token, now, now_system, Duration::from_secs(30));

    assert_eq!(expires_at, now + Duration::from_secs(30));
}

#[test]
fn jwt_exp_cache_deadline_ignores_malformed_jwts() {
    let now = Instant::now();
    let now_system = UNIX_EPOCH + Duration::from_secs(1_000);

    assert_eq!(
        jwt_exp_cache_deadline("header.payload", now, now_system),
        None
    );
    assert_eq!(
        jwt_exp_cache_deadline(&jwt_with_payload(serde_json::json!({})), now, now_system),
        None
    );
    assert_eq!(
        jwt_exp_cache_deadline(
            &jwt_with_payload(serde_json::json!({ "exp": "1600" })),
            now,
            now_system
        ),
        None
    );
    assert_eq!(
        jwt_exp_cache_deadline("header.not base64!.signature", now, now_system),
        None
    );
}

#[tokio::test]
async fn cached_result_evicts_expired_positive_entry() {
    let (reviewer, _) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let key = cache_key("expired", "space-node-api");
    lock_or_recover(&reviewer.cache).insert(
        key.clone(),
        accepted_cache_entry("system:serviceaccount:space:space-api", Instant::now()),
    );

    assert_eq!(reviewer.cached_result(&key), None);
    assert!(!lock_or_recover(&reviewer.cache).contains_key(&key));
}

#[tokio::test]
async fn in_flight_claim_drop_wakes_waiters() {
    let (reviewer, _) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let key = cache_key("token", "space-node-api");
    let claim = match reviewer.join_in_flight_or_claim(&key) {
        InFlightState::Claimed(claim) => claim,
        InFlightState::Waiting(_) => panic!("first caller should claim in-flight work"),
    };
    let waiter = match reviewer.join_in_flight_or_claim(&key) {
        InFlightState::Waiting(waiter) => waiter,
        InFlightState::Claimed(_) => panic!("second caller should wait"),
    };

    drop(claim);

    waiter.await.unwrap();
    assert!(matches!(
        reviewer.join_in_flight_or_claim(&key),
        InFlightState::Claimed(_)
    ));
}

#[tokio::test]
async fn in_flight_claim_drop_on_panic_allows_next_claim() {
    let (reviewer, _) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let key = cache_key("token", "space-node-api");
    let task = {
        let reviewer = reviewer.clone();
        let key = key.clone();
        tokio::spawn(async move {
            match reviewer.join_in_flight_or_claim(&key) {
                InFlightState::Claimed(_claim) => panic!("leader panicked"),
                InFlightState::Waiting(_) => panic!("first caller should claim in-flight work"),
            }
        })
    };

    assert!(task.await.unwrap_err().is_panic());
    assert!(matches!(
        reviewer.join_in_flight_or_claim(&key),
        InFlightState::Claimed(_)
    ));
}

#[tokio::test]
async fn token_review_unavailable_denies_retryably_without_caching() {
    let (reviewer, calls) = reviewer_returning_error(test_k8s_config());
    let token = projected_token();

    let first = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();
    let second = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // A TokenReview outage is transient infrastructure failure, not a
    // verdict on the token: deny with a retryable code (not UNAUTHENTICATED,
    // which callers treat as permanent), and do NOT negative-cache it —
    // each call re-attempts so a recovered apiserver validates immediately.
    assert_eq!(first.code(), tonic::Code::Unavailable);
    assert_eq!(second.code(), tonic::Code::Unavailable);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn review_rate_limiter_caps_burst_then_refills() {
    let limiter = ReviewRateLimiter::new(10); // 10/s, burst 10
    let start = Instant::now();
    // Burst of 10 succeeds, the 11th in the same instant is shed.
    for _ in 0..10 {
        assert!(limiter.try_acquire_at(start));
    }
    assert!(!limiter.try_acquire_at(start));
    // After ~1s, the bucket has refilled.
    assert!(limiter.try_acquire_at(start + Duration::from_secs(1)));
}

#[test]
fn review_rate_limiter_ignores_out_of_order_timestamps() {
    // Models concurrent callers whose pre-lock `Instant::now()` snapshots
    // are reordered. The bug: a stale snapshot regresses `last_refill`, so a
    // subsequent newer call recomputes elapsed from the stale base and mints
    // extra tokens. Alternating stale/newer calls must not exceed the burst.
    let limiter = ReviewRateLimiter::new(5); // 5/s, burst 5
    let t_old = Instant::now();
    let t_new = t_old + Duration::from_secs(10); // large gap → a buggy refill over-mints obviously
    // Drain the initial burst.
    for _ in 0..5 {
        assert!(limiter.try_acquire_at(t_old));
    }
    let mut granted = 0;
    for _ in 0..100 {
        if limiter.try_acquire_at(t_old) {
            granted += 1; // stale
        }
        if limiter.try_acquire_at(t_new) {
            granted += 1; // newer — only the first should refill
        }
    }
    // Fixed: only the first `t_new` call refills (to the burst of 5); stale
    // `t_old` calls add nothing and never regress `last_refill`, so total
    // grants ≈ the burst. Buggy: each `t_new` after a stale regression
    // re-mints ~5 tokens, so `granted` would be in the hundreds.
    assert!(
        granted <= 6,
        "out-of-order timestamps minted extra tokens: granted {granted} (burst 5)"
    );
}

#[tokio::test]
async fn token_review_timeout_denies_fast() {
    let mut config = test_k8s_config();
    config.review_timeout = Duration::from_millis(50);
    let reviewer = reviewer_that_hangs(config);
    let token = projected_token();

    let started = tokio::time::Instant::now();
    let err = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // A hung apiserver must fail closed promptly (not wait the kube
    // default) and with a retryable code, since it's a transient outage.
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "review should time out near the configured deadline"
    );
}

#[tokio::test]
async fn k8s_auth_rejects_unallowed_service_account() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        token_review_status("space-node-api", "system:serviceaccount:space:not-allowed"),
    );
    let token = projected_token();

    let err = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // Authenticated by the apiserver but not on the surface's allowlist:
    // permission denied (a verdict, distinct from an auth/transport error).
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn worker_api_audience_token_cannot_reach_worker_internal() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        token_review_status(
            "space-node-api",
            "system:serviceaccount:space:space-sandbox-manager",
        ),
    );
    let token = projected_token();

    let err = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::WorkerInternal,
            "/space.node.api.v1.WorkerApiInternal/WakeSandbox",
            &token,
        )
        .await
        .unwrap_err();

    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sandbox_manager_internal_audience_can_reach_worker_internal() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        token_review_status(
            "space-node-api-internal",
            "system:serviceaccount:space:space-sandbox-manager",
        ),
    );
    let token = projected_token();

    let caller = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::WorkerInternal,
            "/space.node.api.v1.WorkerApiInternal/WakeSandbox",
            &token,
        )
        .await
        .unwrap();

    assert_eq!(
        caller.wait_ready_lifecycle_authority(),
        WaitReadyLifecycleAuthority::Denied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn accepted_expired_token_review_is_not_cached() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let expired_token = jwt_with_exp(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 60,
    );

    reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &expired_token,
        )
        .await
        .unwrap();

    assert!(
        !lock_or_recover(&reviewer.cache)
            .contains_key(&cache_key(&expired_token, "space-node-api"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn static_fallback_disabled_allows_valid_k8s_token() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let authenticator = NodeApiAuthenticator {
        role: NodeRole::Worker,
        static_token: "static-token".to_string(),
        static_control_token: None,
        static_fallback_enabled: false,
        k8s: Some(reviewer),
    };
    let token = projected_token();

    authenticator
        .authenticate(
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            Some(&format!("Bearer {token}")),
        )
        .await
        .unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn static_fallback_with_k8s_configured_still_requires_wait_ready_route_token() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let authenticator = NodeApiAuthenticator {
        role: NodeRole::Worker,
        static_token: "static-token".to_string(),
        static_control_token: None,
        static_fallback_enabled: true,
        k8s: Some(reviewer),
    };

    let caller = authenticator
        .authenticate(
            "/space.node.api.v1.WorkerApi/SpacedWaitReady",
            Some("Bearer static-token"),
        )
        .await
        .unwrap()
        .expect("authenticated RPC has a caller policy");

    assert_eq!(
        caller.wait_ready_lifecycle_authority(),
        WaitReadyLifecycleAuthority::Denied
    );
    // Once service-account auth exists, the shared token no longer proves the
    // edge identity either, so it may not refresh session routes tokenless.
    assert_eq!(
        caller.data_plane_session_refresh_authority(),
        DataPlaneSessionRefreshAuthority::Denied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_cache_inserts_at_capacity_do_not_panic() {
    let (reviewer, _) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let mut tasks = Vec::new();

    for index in 0..8 {
        let reviewer = reviewer.clone();
        tasks.push(tokio::spawn(async move {
            reviewer.cache_result(
                cache_key(&format!("token-{index}"), "space-node-api"),
                CachedTokenReviewResult::Accepted {
                    username: format!("system:serviceaccount:space:space-api-{index}"),
                    node_identity: AuthenticatedNodeIdentity::MissingKubernetesClaims,
                },
                Instant::now() + Duration::from_secs(30),
            );
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert!(lock_or_recover(&reviewer.cache).len() <= reviewer.config.cache_max_entries);
}

#[test]
fn validate_token_review_status_accepts_confirmed_audience() {
    let identity = validate_token_review_status(
        Some(accepted_token_review_status("space-node-api")),
        "space-node-api",
    )
    .unwrap();

    assert_eq!(identity.username, "system:serviceaccount:space:space-api");
}

#[test]
fn validate_token_review_status_rejects_audience_mismatch() {
    let err = validate_token_review_status(
        Some(accepted_token_review_status("space-other-api")),
        "space-node-api",
    )
    .unwrap_err();

    assert_eq!(
        err,
        TokenReviewFailure::Rejected {
            reason: TokenReviewRejectReason::AudienceMismatch
        }
    );
}

#[test]
fn validate_token_review_status_rejects_unauthenticated_without_error() {
    let err = validate_token_review_status(
        Some(TokenReviewStatus {
            authenticated: Some(false),
            ..Default::default()
        }),
        "space-node-api",
    )
    .unwrap_err();

    assert_eq!(
        err,
        TokenReviewFailure::Rejected {
            reason: TokenReviewRejectReason::Rejected
        }
    );
}

#[test]
fn auth_namespace_reads_service_account_namespace_file() {
    let env = pplx_space_test_env::TestEnv::default();
    let path = std::env::temp_dir().join(format!(
        "space-auth-namespace-test-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(&path, "space-testing\n").unwrap();

    assert_eq!(
        auth_namespace_from_file(&env, &path).unwrap(),
        "space-testing"
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn valid_token_is_served_from_positive_cache() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let token = projected_token();

    for _ in 0..3 {
        reviewer
            .authenticate(
                NodeRole::Worker,
                AuthSurface::Normal,
                "/space.node.api.v1.WorkerApi/CreateSandbox",
                &token,
            )
            .await
            .unwrap();
    }

    // The first call validates against the apiserver; the rest are served
    // from the positive cache, so exactly one TokenReview is issued.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn rejected_token_is_negative_cached() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        TokenReviewStatus {
            authenticated: Some(false),
            ..Default::default()
        },
    );
    let token = projected_token();

    let first = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();
    let second = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // A genuine rejection is a verdict on the token, so it IS negative-cached
    // (short TTL): the second call is served from cache, with no second
    // apiserver round-trip.
    assert_eq!(first.code(), tonic::Code::Unauthenticated);
    assert_eq!(second.code(), tonic::Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn token_review_overloaded_sheds_with_unavailable() {
    let mut config = test_k8s_config();
    config.max_inflight_reviews = 1;
    config.review_timeout = Duration::from_millis(100);
    let (reviewer, calls) =
        reviewer_returning_status(config, accepted_token_review_status("space-node-api"));
    // Exhaust the single review permit so the next cache-miss review has to
    // wait on the concurrency limiter.
    let permit = Arc::clone(&reviewer.review_limiter)
        .acquire_owned()
        .await
        .unwrap();
    let token = projected_token();

    let started = tokio::time::Instant::now();
    let err = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // No permit frees within the review timeout: shed with a retryable code
    // (local backpressure, not a verdict) and never reach the apiserver.
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "saturated limiter should shed near the configured timeout"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    drop(permit);
}

#[tokio::test]
async fn token_review_rate_limited_sheds_with_unavailable() {
    let mut config = test_k8s_config();
    config.review_rate_per_sec = 1; // burst 1
    let (reviewer, calls) =
        reviewer_returning_status(config, accepted_token_review_status("space-node-api"));
    // Drain the single rate-limiter token (refill is 1/s, so a back-to-back
    // cache-miss review cannot replenish it) — the next review is shed.
    assert!(reviewer.rate_limiter.try_acquire());
    let token = projected_token();

    let err = reviewer
        .authenticate(
            NodeRole::Worker,
            AuthSurface::Normal,
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            &token,
        )
        .await
        .unwrap_err();

    // Rate-limiter shed is local backpressure: retryable UNAVAILABLE, and the
    // apiserver is never hit.
    assert_eq!(err.code(), tonic::Code::Unavailable);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_same_token_authenticate_issues_one_review() {
    // A mock apiserver that holds each review open briefly so concurrent
    // callers join the single-flight wait instead of each starting a review.
    let body = Arc::new(token_review_body(accepted_token_review_status(
        "space-node-api",
    )));
    let calls = Arc::new(AtomicUsize::new(0));
    let service = tower::service_fn({
        let calls = Arc::clone(&calls);
        let body = Arc::clone(&body);
        move |_req: http::Request<Body>| {
            let calls = Arc::clone(&calls);
            let body = Arc::clone(&body);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
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
    let reviewer = K8sTokenReviewer::with_client(test_k8s_config(), client);
    let token = projected_token();

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let reviewer = reviewer.clone();
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            reviewer
                .authenticate(
                    NodeRole::Worker,
                    AuthSurface::Normal,
                    "/space.node.api.v1.WorkerApi/CreateSandbox",
                    &token,
                )
                .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    // Single-flight collapses the 8 concurrent same-token calls into exactly
    // one apiserver TokenReview; the waiters read the freshly-cached verdict.
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn non_jwt_bearer_is_rejected_without_review() {
    let (reviewer, calls) = reviewer_returning_status(
        test_k8s_config(),
        accepted_token_review_status("space-node-api"),
    );
    let authenticator = NodeApiAuthenticator {
        role: NodeRole::Worker,
        static_token: "static-token".to_string(),
        static_control_token: None,
        static_fallback_enabled: true,
        k8s: Some(reviewer),
    };

    let err = authenticator
        .authenticate(
            "/space.node.api.v1.WorkerApi/CreateSandbox",
            Some("Bearer not-a-jwt"),
        )
        .await
        .unwrap_err();

    // A non-JWT bearer that isn't the static token is rejected at the shape
    // gate, before any TokenReview round-trip.
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
