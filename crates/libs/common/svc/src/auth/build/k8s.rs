//! Build submission policy, separate from the shared node and cluster verifier.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
use kube::api::PostParams;
use tonic::Status;

use super::{
    AuthenticatedBuildNode, BuildAuthError, BuildAuthenticatedCaller, BuildSurface,
    SystemTemplateAuthority, TenantTemplateAuthority,
};
use crate::auth::{is_jwt_shaped, k8s::K8sTokenAuthConfig};

pub(super) struct BuildTokenReviewer {
    reviews: kube::Api<TokenReview>,
    policy: BuildPolicy,
    permits: tokio::sync::Semaphore,
    budget: Mutex<ReviewBudget>,
    timeout: Duration,
}

pub(super) struct BuildPolicy {
    audience: String,
    allowed: BTreeSet<String>,
    system: BTreeSet<String>,
    /// System publishers whose system publications need an independently
    /// issued build grant.
    granted_system: BTreeSet<String>,
    tenant: BTreeSet<String>,
    /// Whether tenant authority on this surface is limited to dispatched
    /// builds' grants. Publication asserts ownership of an output; build
    /// submission asserts the requester the gateway resolved.
    tenant_requires_grant: bool,
}

impl BuildPolicy {
    fn new(
        surface: BuildSurface,
        audience: String,
        allowed: BTreeSet<String>,
        system: BTreeSet<String>,
        tenant: BTreeSet<String>,
    ) -> Self {
        // A publisher that also publishes tenant templates is the build path:
        // it carries the independently authorized job scope, but omitting the owner
        // must not let it classify a build as a system one on its own.
        let granted_system: BTreeSet<String> = match surface {
            BuildSurface::Publication => system.intersection(&tenant).cloned().collect(),
            BuildSurface::Manager | BuildSurface::NodeApi => BTreeSet::new(),
        };
        Self {
            audience,
            allowed,
            system: system.difference(&granted_system).cloned().collect(),
            granted_system,
            tenant,
            tenant_requires_grant: matches!(surface, BuildSurface::Publication),
        }
    }
}

struct ReviewBudget {
    window: Instant,
    remaining: u64,
    per_second: u64,
}

impl BuildTokenReviewer {
    pub(super) async fn from_env(
        config: &K8sTokenAuthConfig,
        surface: BuildSurface,
    ) -> Result<Self, BuildAuthError> {
        let accounts = |name| {
            std::env::var(name)
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(|name| format!("system:serviceaccount:{}:{name}", config.namespace))
                .collect::<BTreeSet<_>>()
        };
        let system = accounts(match surface {
            BuildSurface::Publication => "SPACE_TEMPLATE_PUBLISH_SYSTEM_SERVICE_ACCOUNTS",
            _ => "SPACE_BUILD_AUTH_SYSTEM_SERVICE_ACCOUNTS",
        });
        let tenant = accounts(match surface {
            BuildSurface::Publication => "SPACE_TEMPLATE_PUBLISH_TENANT_SERVICE_ACCOUNTS",
            _ => "SPACE_BUILD_AUTH_TENANT_SERVICE_ACCOUNTS",
        });
        let (audience, allowed) = match surface {
            BuildSurface::Publication => (
                config.cluster_api_audience.clone(),
                system.union(&tenant).cloned().collect(),
            ),
            BuildSurface::Manager => (
                "space-build-manager".to_string(),
                accounts("SPACE_BUILD_MANAGER_AUTH_ALLOWED_SERVICE_ACCOUNTS"),
            ),
            BuildSurface::NodeApi => (
                config.node_api_audience.clone(),
                accounts("SPACE_NODE_API_AUTH_TEMPLATE_BUILD_SERVICE_ACCOUNTS")
                    .intersection(&accounts("SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS"))
                    .cloned()
                    .collect(),
            ),
        };
        if allowed.is_empty() {
            return Err(BuildAuthError::AllowedServiceAccountsMissing);
        }
        let policy = BuildPolicy::new(surface, audience, allowed, system, tenant);
        if !policy.granted_system.is_empty() {
            tracing::info!(
                accounts = ?policy.granted_system,
                "system template publication by these accounts requires an independently issued build grant"
            );
        }
        let per_second = std::env::var("SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(50)
            .max(1);
        Ok(Self {
            reviews: kube::Api::all(crate::kube_client::traced_kube_client_from_env().await?),
            policy,
            permits: tokio::sync::Semaphore::new(config.max_inflight_reviews),
            budget: Mutex::new(ReviewBudget {
                window: Instant::now(),
                remaining: per_second,
                per_second,
            }),
            timeout: config.review_timeout,
        })
    }

    pub(super) async fn authenticate(
        &self,
        token: &str,
    ) -> Result<BuildAuthenticatedCaller, Status> {
        let policy = &self.policy;
        if !is_jwt_shaped(token) {
            return Err(Status::unauthenticated("invalid token"));
        }
        let _permit = self
            .permits
            .try_acquire()
            .map_err(|_| Status::unavailable("build token reviewer is busy"))?;
        {
            let mut budget = self
                .budget
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if budget.window.elapsed() >= Duration::from_secs(1) {
                budget.window = Instant::now();
                budget.remaining = budget.per_second;
            }
            if budget.remaining == 0 {
                return Err(Status::unavailable("build token review rate exceeded"));
            }
            budget.remaining -= 1;
        }
        // Review each build RPC instead of maintaining a second authentication
        // cache. Bound apiserver load and fail closed when review cannot complete.
        let review = TokenReview {
            spec: TokenReviewSpec {
                token: Some(token.to_string()),
                audiences: Some(vec![policy.audience.clone()]),
            },
            ..Default::default()
        };
        let response = tokio::time::timeout(
            self.timeout,
            self.reviews.create(&PostParams::default(), &review),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                audience = %policy.audience,
                timeout_secs = self.timeout.as_secs_f64(),
                "build TokenReview request timed out"
            );
            Status::unavailable("build token review timed out")
        })?
        .map_err(|error| {
            tracing::warn!(%error, audience = %policy.audience, "build TokenReview request failed");
            Status::unavailable("build token review failed")
        })?;
        let status = response
            .status
            .ok_or_else(|| Status::unauthenticated("invalid build token"))?;
        if let Some(error) = status.error.as_deref().filter(|error| !error.is_empty()) {
            tracing::warn!(error, audience = %policy.audience, "build TokenReview evaluation failed");
            return Err(Status::unavailable("build token review failed"));
        }
        if status.authenticated != Some(true)
            || !status
                .audiences
                .as_ref()
                .is_some_and(|audiences| audiences.contains(&policy.audience))
        {
            return Err(Status::unauthenticated("invalid build token"));
        }
        let user = status
            .user
            .ok_or_else(|| Status::unauthenticated("build token has no identity"))?;
        let username = user
            .username
            .as_deref()
            .ok_or_else(|| Status::unauthenticated("build token has no identity"))?;
        if !policy.allowed.contains(username) {
            return Err(Status::permission_denied("caller may not submit builds"));
        }
        let system = if policy.system.contains(username) {
            SystemTemplateAuthority::Unconditional
        } else if policy.granted_system.contains(username) {
            SystemTemplateAuthority::Granted
        } else {
            SystemTemplateAuthority::Denied
        };
        let tenant = if !policy.tenant.contains(username) {
            TenantTemplateAuthority::Denied
        } else if policy.tenant_requires_grant {
            TenantTemplateAuthority::Granted
        } else {
            TenantTemplateAuthority::Unconditional
        };
        let node = if tenant == TenantTemplateAuthority::Granted {
            Some(publication_node(&user)?)
        } else {
            None
        };
        Ok(BuildAuthenticatedCaller {
            tenant,
            system,
            node,
        })
    }
}

fn publication_node(
    user: &k8s_openapi::api::authentication::v1::UserInfo,
) -> Result<AuthenticatedBuildNode, Status> {
    use pplx_space_common_core::types::{PodName, PodUid};
    let denied = || Status::permission_denied("publication requires a pod-bound node identity");
    let extra = user.extra.as_ref().ok_or_else(denied)?;
    let claim = |key| match extra.get(key).map(Vec::as_slice) {
        Some([value]) if !value.is_empty() => Ok(value.as_str()),
        _ => Err(denied()),
    };
    PodName::parse(claim("authentication.kubernetes.io/pod-name")?).map_err(|_| denied())?;
    PodUid::parse(claim("authentication.kubernetes.io/pod-uid")?).map_err(|_| denied())?;
    Ok(AuthenticatedBuildNode {
        name: claim("authentication.kubernetes.io/node-name")?
            .parse()
            .map_err(|_| denied())?,
        uid: claim("authentication.kubernetes.io/node-uid")?
            .parse()
            .map_err(|_| denied())?,
    })
}

#[cfg(test)]
mod tests;
