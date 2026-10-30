use super::super::{
    BuildAuthenticator, authorize_template_build, system_publication_requires_grant,
    tenant_publication_requires_grant,
};
use super::*;
use crate::auth::test_support::{
    TokenReviewStatus, jwt_with_exp, pod_bound_service_account_status, service_account_status,
    token_review_service,
};
use kube::client::{Body, ClientBuilder};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::UNIX_EPOCH;
use tonic::Code;

const NODE_BUILD: &str = "/space.node.api.v1.BuilderApi/BuildTemplate";
const MANAGER_BUILD: &str = "/build.manager.v1.BuildManager/StartBuild";
const TOKEN: &str = "header.untrusted-claims.signature";
const BUILDER: &str = "system:serviceaccount:space:space-builder";

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn accounts(names: &[&str]) -> BTreeSet<String> {
    names
        .iter()
        .map(|name| format!("system:serviceaccount:space:{name}"))
        .collect()
}

fn policy(audience: &str) -> BuildPolicy {
    BuildPolicy::new(
        BuildSurface::Manager,
        audience.to_string(),
        accounts(&["space-api", "space-builder", "space-build-manager"]),
        accounts(&["space-builder", "space-build-manager"]),
        accounts(&["space-api", "space-builder", "space-build-manager"]),
    )
}

fn reviewer(
    status: TokenReviewStatus,
    expected_audience: &str,
) -> (BuildTokenReviewer, Arc<AtomicUsize>) {
    reviewer_for_token(status, expected_audience, TOKEN)
}

fn reviewer_for_token(
    status: TokenReviewStatus,
    expected_audience: &str,
    expected_token: &str,
) -> (BuildTokenReviewer, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let expected_policy = policy(expected_audience);
    let expected_audience = expected_audience.to_string();
    let expected_token = expected_token.to_string();
    let service = token_review_service({
        let calls = calls.clone();
        move |request| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.audiences, Some(vec![expected_audience.clone()]));
            assert_eq!(request.token.as_deref(), Some(expected_token.as_str()));
            status.clone()
        }
    });
    let reviewer = BuildTokenReviewer {
        reviews: kube::Api::all(ClientBuilder::new(service, "space").build()),
        policy: expected_policy,
        surface: BuildSurface::Manager,
        permits: tokio::sync::Semaphore::new(8),
        budget: Mutex::new(ReviewBudget {
            window: Instant::now(),
            remaining: 50,
            per_second: 50,
        }),
        timeout: Duration::from_secs(5),
        reviewed: TokenReviewCache::new(16),
        reviewed_ttl: Duration::from_secs(30),
    };
    (reviewer, calls)
}

/// A build-manager reviewer whose apiserver answers each review with `status`
/// after `delay`, or never when `delay` is `None`.
fn reviewer_answering_after(
    status: TokenReviewStatus,
    delay: Option<Duration>,
) -> (BuildTokenReviewer, Arc<AtomicUsize>) {
    let (mut reviewer, _) = reviewer(status.clone(), "space-build-manager");
    let calls = Arc::new(AtomicUsize::new(0));
    let body = serde_json::to_vec(&TokenReview {
        status: Some(status),
        ..Default::default()
    })
    .unwrap();
    let service = tower::service_fn({
        let calls = calls.clone();
        move |_request: http::Request<Body>| {
            let calls = calls.clone();
            let body = body.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                match delay {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => std::future::pending::<()>().await,
                }
                Ok::<_, BoxError>(
                    http::Response::builder()
                        .status(201)
                        .header(http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
            }
        }
    });
    reviewer.reviews = kube::Api::all(ClientBuilder::new(service, "space").build());
    (reviewer, calls)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn authenticator(reviewer: BuildTokenReviewer, fallback: bool) -> BuildAuthenticator {
    BuildAuthenticator {
        surface: if reviewer.policy.audience == "space-node-api" {
            BuildSurface::NodeApi
        } else {
            BuildSurface::Manager
        },
        static_token: "shared-token".to_string(),
        static_fallback_enabled: fallback,
        direct_publication_token: None,
        reviewer: Some(reviewer),
    }
}

#[tokio::test]
async fn build_manager_authenticates_only_reviewed_callers() {
    for (bearer, audience, account, fallback, expected) in [
        (
            "shared-token",
            "space-build-manager",
            "space-builder",
            true,
            Some(Code::Unauthenticated),
        ),
        (
            "shared-token",
            "space-build-manager",
            "space-builder",
            false,
            Some(Code::Unauthenticated),
        ),
        (TOKEN, "space-build-manager", "space-builder", true, None),
        (TOKEN, "space-build-manager", "space-builder", false, None),
        (
            TOKEN,
            "space-node-api",
            "space-builder",
            true,
            Some(Code::Unauthenticated),
        ),
        (
            TOKEN,
            "space-build-manager",
            "workers",
            true,
            Some(Code::PermissionDenied),
        ),
    ] {
        for method in ["StartBuild", "UploadBlob", "GetBuildStatus"] {
            let (reviewer, calls) = reviewer(
                service_account_status(audience, &format!("system:serviceaccount:space:{account}")),
                "space-build-manager",
            );
            let auth = authenticator(reviewer, fallback);

            let result = auth
                .authenticate(
                    &format!("/build.manager.v1.BuildManager/{method}"),
                    Some(&format!("Bearer {bearer}")),
                )
                .await;

            assert_eq!(result.err().map(|error| error.code()), expected);
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(bearer == TOKEN));
        }
    }
}

#[tokio::test]
async fn build_owner_authority_comes_from_reviewed_identity() {
    let tenant = Some("018f0000-0000-7000-8000-000000000002");
    for (username, owner, allowed) in [
        ("system:serviceaccount:space:space-api", tenant, true),
        ("system:serviceaccount:space:space-api", None, false),
        ("system:serviceaccount:space:space-builder", tenant, true),
        ("system:serviceaccount:space:space-builder", None, true),
        (
            "system:serviceaccount:space:space-build-manager",
            None,
            true,
        ),
        ("system:serviceaccount:space:workers", tenant, false),
        ("system:serviceaccount:other:space-api", tenant, false),
    ] {
        for (path, audience) in [
            (MANAGER_BUILD, "space-build-manager"),
            (NODE_BUILD, "space-node-api"),
        ] {
            let (reviewer, _) = reviewer(service_account_status(audience, username), audience);
            let auth = authenticator(reviewer, true);

            let result = auth
                .authenticate(path, Some(&format!("Bearer {TOKEN}")))
                .await
                .and_then(|caller| {
                    let mut request = tonic::Request::new(());
                    request.extensions_mut().insert(caller.unwrap());
                    authorize_template_build(&request, owner)?;
                    // Submission asserts the requester; only publication of
                    // the output is bound to a dispatched build.
                    assert!(!tenant_publication_requires_grant(&request));
                    Ok(())
                });

            assert_eq!(
                result.err().map(|error| error.code()),
                if allowed {
                    None
                } else {
                    Some(Code::PermissionDenied)
                },
                "{path}: {username}"
            );
        }
    }
}

#[tokio::test]
async fn legacy_node_caller_cannot_borrow_publisher_authority() {
    let (reviewer, calls) = reviewer(
        service_account_status("space-node-api", "system:serviceaccount:space:space-api"),
        "space-node-api",
    );
    let auth = authenticator(reviewer, true);
    let error = auth
        .authenticate(NODE_BUILD, Some("Bearer shared-token"))
        .await
        .unwrap_err();

    assert_eq!(error.code(), Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn invalid_review_and_exhausted_budget_fail_closed() {
    // (status, code as reviewed, code once the review also reports the token
    // unauthenticated)
    for (mut status, expected, expected_rejected) in [
        (
            TokenReviewStatus::default(),
            Code::Unauthenticated,
            Code::Unauthenticated,
        ),
        (
            service_account_status(
                "wrong-audience",
                "system:serviceaccount:space:space-builder",
            ),
            Code::Unauthenticated,
            Code::Unauthenticated,
        ),
        (
            TokenReviewStatus {
                error: Some("review backend unavailable".to_string()),
                ..service_account_status(
                    "space-build-manager",
                    "system:serviceaccount:space:space-builder",
                )
            },
            Code::Unavailable,
            Code::Unauthenticated,
        ),
    ] {
        for (authenticated, expected) in [
            (status.authenticated, expected),
            (Some(false), expected_rejected),
        ] {
            status.authenticated = authenticated;
            let (reviewer, calls) = reviewer(status.clone(), "space-build-manager");

            // A failed review is not cached: the second call reviews again.
            for _ in 0..2 {
                let result = reviewer.authenticate(TOKEN).await;

                assert_eq!(result.unwrap_err().code(), expected);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 2);
        }
    }
    let (reviewer, calls) = reviewer(
        service_account_status(
            "space-build-manager",
            "system:serviceaccount:space:space-builder",
        ),
        "space-build-manager",
    );
    reviewer.budget.lock().unwrap().remaining = 0;

    assert_eq!(
        reviewer.authenticate(TOKEN).await.unwrap_err().code(),
        Code::Unavailable
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

/// The apiserver's answer for a token with another audience: rejected, with
/// the reason in `error` and no audiences or user. A verdict, not an outage.
#[tokio::test]
async fn rejected_token_review_with_reason_is_unauthenticated() {
    let (reviewer, calls) = reviewer(
        TokenReviewStatus {
            authenticated: Some(false),
            error: Some(
                "[invalid bearer token, token audiences [\"space-node-api\"] is invalid for the target audiences [\"space-build-manager\"]]"
                    .to_string(),
            ),
            ..Default::default()
        },
        "space-build-manager",
    );

    let result = reviewer.authenticate(TOKEN).await;

    assert_eq!(result.unwrap_err().code(), Code::Unauthenticated);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn unavailable_token_review_never_grants_build_authority() {
    for stalled in [false, true] {
        let (mut reviewer, _) = reviewer(
            service_account_status(
                "space-build-manager",
                "system:serviceaccount:space:space-builder",
            ),
            "space-build-manager",
        );
        let service = tower::service_fn(move |_request: http::Request<Body>| async move {
            if stalled {
                std::future::pending::<()>().await;
            }
            Err::<http::Response<Body>, BoxError>(
                std::io::Error::other("review unavailable").into(),
            )
        });
        reviewer.reviews = kube::Api::all(ClientBuilder::new(service, "space").build());
        reviewer.timeout = Duration::from_millis(5);

        let result = reviewer.authenticate(TOKEN).await;

        assert_eq!(result.unwrap_err().code(), Code::Unavailable);
        assert_eq!(reviewer.permits.available_permits(), 8);
    }
}

/// Build-manager holds both tenant and system publication authority, so its
/// system publications need the API's system build grant; the deploy-time
/// publisher holds system authority alone and needs none.
#[tokio::test]
async fn publication_authority_is_method_scoped_and_comes_from_token_review() {
    // (system grant required, tenant grant required) for an allowed caller.
    for (account, owner, expected, requires_grant) in [
        (
            "workers",
            None,
            Some(Code::PermissionDenied),
            (false, false),
        ),
        (
            "workers",
            Some("tenant"),
            Some(Code::PermissionDenied),
            (false, false),
        ),
        (
            "space-api",
            Some("tenant"),
            Some(Code::PermissionDenied),
            (false, false),
        ),
        ("space-build-manager", None, None, (true, true)),
        ("space-build-manager", Some("tenant"), None, (true, true)),
        ("space-template-publish", None, None, (false, false)),
        (
            "space-template-publish",
            Some("tenant"),
            Some(Code::PermissionDenied),
            (false, false),
        ),
    ] {
        for method in ["RegisterTemplate", "RegisterTemplateRelease"] {
            let (mut reviewer, calls) = reviewer(
                bound_publisher_status(
                    "space-cluster-api",
                    &format!("system:serviceaccount:space:{account}"),
                ),
                "space-cluster-api",
            );
            let system = accounts(&["space-build-manager", "space-template-publish"]);
            let tenant = accounts(&["space-build-manager"]);
            reviewer.policy = BuildPolicy::new(
                BuildSurface::Publication,
                "space-cluster-api".to_string(),
                system.union(&tenant).cloned().collect(),
                system,
                tenant,
            );
            let mut auth = authenticator(reviewer, false);
            auth.surface = BuildSurface::Publication;
            let result = auth
                .authenticate(
                    &format!("/space.node.api.v1.ClusterApi/{method}"),
                    Some(&format!("Bearer {TOKEN}")),
                )
                .await
                .and_then(|caller| {
                    let mut request = tonic::Request::new(());
                    request.extensions_mut().insert(caller.unwrap());
                    authorize_template_build(&request, owner)?;
                    Ok((
                        system_publication_requires_grant(&request),
                        tenant_publication_requires_grant(&request),
                    ))
                });
            assert_eq!(
                result.as_ref().err().map(tonic::Status::code),
                expected,
                "{account}: {method}"
            );
            assert_eq!(
                result.unwrap_or((false, false)),
                requires_grant,
                "{account}: {method}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(
                auth.authenticate(
                    "/space.node.api.v1.ClusterApi/PublishNodeState",
                    Some("Bearer shared-token")
                )
                .await
                .unwrap()
                .is_none()
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}

fn bound_publisher_status(audience: &str, username: &str) -> TokenReviewStatus {
    pod_bound_service_account_status(audience, username, "builder-node", "builder-node-uid")
}

#[tokio::test]
async fn publication_requires_unambiguous_reviewed_pod_and_node() {
    for values in [
        None,
        Some(vec![]),
        Some(vec!["first".to_string(), "second".to_string()]),
    ] {
        for key in ["pod-name", "pod-uid", "node-name", "node-uid"] {
            let mut status = bound_publisher_status(
                "space-cluster-api",
                "system:serviceaccount:space:space-build-manager",
            );
            let extra = status.user.as_mut().unwrap().extra.as_mut().unwrap();
            let key = format!("authentication.kubernetes.io/{key}");
            match &values {
                Some(values) => {
                    extra.insert(key, values.clone());
                }
                None => {
                    extra.remove(&key);
                }
            }
            let (mut reviewer, calls) = reviewer(status, "space-cluster-api");
            reviewer.policy = BuildPolicy::new(
                BuildSurface::Publication,
                "space-cluster-api".into(),
                accounts(&["space-build-manager"]),
                BTreeSet::new(),
                accounts(&["space-build-manager"]),
            );

            // The second call is denied from the cached identity.
            for _ in 0..2 {
                let result = reviewer.authenticate(TOKEN).await;

                assert_eq!(result.unwrap_err().code(), Code::PermissionDenied);
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
}

/// Calls bearing an accepted token are answered from the cache until its
/// entry expires, without spending the per-second review budget.
#[tokio::test]
async fn a_reviewed_token_is_answered_from_the_cache_without_spending_budget() {
    let (reviewer, calls) = reviewer(
        service_account_status("space-build-manager", BUILDER),
        "space-build-manager",
    );
    reviewer.budget.lock().unwrap().remaining = 1;

    for _ in 0..10 {
        reviewer.authenticate(TOKEN).await.unwrap();
    }

    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// The cache holds who the caller is, not what it may do: the surface's build
/// policy is applied to the cached identity on every call.
#[tokio::test]
async fn a_cached_identity_is_authorized_on_every_call() {
    let (mut reviewer, calls) = reviewer(
        service_account_status("space-build-manager", BUILDER),
        "space-build-manager",
    );
    reviewer.authenticate(TOKEN).await.unwrap();

    reviewer.policy = BuildPolicy::new(
        BuildSurface::Manager,
        "space-build-manager".to_string(),
        accounts(&["space-api"]),
        BTreeSet::new(),
        accounts(&["space-api"]),
    );

    assert_eq!(
        reviewer.authenticate(TOKEN).await.unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// A token whose `exp` falls within the 5 s expiry skew is never cached and
/// is reviewed on every call; one that expires later is cached.
#[tokio::test]
async fn a_token_near_its_expiry_is_reviewed_on_every_call() {
    for (expires_in, expected_calls) in [(1, 2), (3_600, 1)] {
        let token = jwt_with_exp(unix_now() + expires_in);
        let (reviewer, calls) = reviewer_for_token(
            service_account_status("space-build-manager", BUILDER),
            "space-build-manager",
            &token,
        );

        for _ in 0..2 {
            reviewer.authenticate(&token).await.unwrap();
        }

        assert_eq!(calls.load(Ordering::SeqCst), expected_calls, "{expires_in}");
    }
}

/// Calls bearing one token while its review is in flight wait for that review
/// instead of each starting one.
#[tokio::test]
async fn concurrent_calls_with_one_token_share_one_review() {
    let (reviewer, calls) = reviewer_answering_after(
        service_account_status("space-build-manager", BUILDER),
        Some(Duration::from_millis(100)),
    );

    let (first, second, third, fourth) = tokio::join!(
        reviewer.authenticate(TOKEN),
        reviewer.authenticate(TOKEN),
        reviewer.authenticate(TOKEN),
        reviewer.authenticate(TOKEN),
    );

    for result in [first, second, third, fourth] {
        result.unwrap();
    }
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

/// One review timeout bounds each call, waits included: calls that share a
/// review the apiserver never answers all fail within it.
#[tokio::test(start_paused = true)]
async fn calls_waiting_on_a_hung_review_fail_within_one_timeout() {
    let (mut reviewer, calls) =
        reviewer_answering_after(service_account_status("space-build-manager", BUILDER), None);
    reviewer.timeout = Duration::from_millis(500);
    let started = tokio::time::Instant::now();

    let (first, second) = tokio::join!(reviewer.authenticate(TOKEN), reviewer.authenticate(TOKEN));

    assert_eq!(first.unwrap_err().code(), Code::Unavailable);
    assert_eq!(second.unwrap_err().code(), Code::Unavailable);
    assert!(
        started.elapsed() <= reviewer.timeout + Duration::from_millis(1),
        "both calls should fail within one review timeout, took {:?}",
        started.elapsed()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
