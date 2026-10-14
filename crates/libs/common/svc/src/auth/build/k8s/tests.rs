use super::super::{
    BuildAuthenticator, authorize_template_build, system_publication_requires_grant,
};
use super::*;
use crate::auth::test_support::{TokenReviewStatus, service_account_status, token_review_service};
use kube::client::{Body, ClientBuilder};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tonic::Code;

const NODE_BUILD: &str = "/space.node.api.v1.BuilderApi/BuildTemplate";
const MANAGER_BUILD: &str = "/build.manager.v1.BuildManager/StartBuild";
const TOKEN: &str = "header.untrusted-claims.signature";

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
    let calls = Arc::new(AtomicUsize::new(0));
    let expected_policy = policy(expected_audience);
    let expected_audience = expected_audience.to_string();
    let service = token_review_service({
        let calls = calls.clone();
        move |request| {
            calls.fetch_add(1, Ordering::SeqCst);
            assert_eq!(request.audiences, Some(vec![expected_audience.clone()]));
            assert_eq!(request.token.as_deref(), Some(TOKEN));
            status.clone()
        }
    });
    let reviewer = BuildTokenReviewer {
        reviews: kube::Api::all(ClientBuilder::new(service, "space").build()),
        policy: expected_policy,
        permits: tokio::sync::Semaphore::new(8),
        budget: Mutex::new(ReviewBudget {
            window: Instant::now(),
            remaining: 50,
            per_second: 50,
        }),
        timeout: Duration::from_secs(5),
    };
    (reviewer, calls)
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
                    authorize_template_build(&request, owner)
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
    for (mut status, expected) in [
        (TokenReviewStatus::default(), Code::Unauthenticated),
        (
            service_account_status(
                "wrong-audience",
                "system:serviceaccount:space:space-builder",
            ),
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
        ),
    ] {
        for authenticated in [status.authenticated, Some(false)] {
            status.authenticated = authenticated;
            let (reviewer, _) = reviewer(status.clone(), "space-build-manager");

            let result = reviewer.authenticate(TOKEN).await;

            assert_eq!(result.unwrap_err().code(), expected);
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
    for (account, owner, expected, requires_grant) in [
        ("workers", None, Some(Code::PermissionDenied), false),
        (
            "workers",
            Some("tenant"),
            Some(Code::PermissionDenied),
            false,
        ),
        (
            "space-api",
            Some("tenant"),
            Some(Code::PermissionDenied),
            false,
        ),
        ("space-build-manager", None, None, true),
        ("space-build-manager", Some("tenant"), None, true),
        ("space-template-publish", None, None, false),
        (
            "space-template-publish",
            Some("tenant"),
            Some(Code::PermissionDenied),
            false,
        ),
    ] {
        for method in ["RegisterTemplate", "RegisterTemplateRelease"] {
            let (mut reviewer, calls) = reviewer(
                service_account_status(
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
                    Ok(system_publication_requires_grant(&request))
                });
            assert_eq!(
                result.as_ref().err().map(tonic::Status::code),
                expected,
                "{account}: {method}"
            );
            assert_eq!(
                result.unwrap_or(false),
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
