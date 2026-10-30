//! Build submission policy, separate from the shared node and cluster verifier.

use std::collections::BTreeSet;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime};

use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec};
use kube::api::PostParams;
use tonic::Status;

use super::{
    AuthenticatedBuildNode, BuildAuthError, BuildAuthenticatedCaller, BuildSurface,
    SystemTemplateAuthority, TenantTemplateAuthority,
};
use crate::auth::is_jwt_shaped;
use crate::auth::k8s::{
    InFlightState, K8sTokenAuthConfig, Lookup, TokenCacheKey, TokenReviewCache,
    accepted_token_cache_expiry,
};

/// The least time left on a call's deadline for it to start a review.
const MIN_REVIEW_TIME: Duration = Duration::from_millis(100);

pub(super) struct BuildTokenReviewer {
    reviews: kube::Api<TokenReview>,
    policy: BuildPolicy,
    surface: BuildSurface,
    permits: tokio::sync::Semaphore,
    budget: Mutex<ReviewBudget>,
    timeout: Duration,
    /// Accepted reviews, each kept for `reviewed_ttl` after it returns or
    /// until 5 s before the token's `exp`, whichever is sooner. `policy` is
    /// applied to a cached identity on every call. A rejected or failed review
    /// is not cached.
    reviewed: TokenReviewCache<ReviewedBuildToken>,
    /// `K8sTokenAuthConfig::cache_ttl`, the TTL node-api's verifier uses.
    reviewed_ttl: Duration,
}

/// What a TokenReview established about a token, before any build policy.
#[derive(Clone, Debug)]
struct ReviewedBuildToken {
    username: String,
    node: Option<AuthenticatedBuildNode>,
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
            surface,
            permits: tokio::sync::Semaphore::new(config.max_inflight_reviews),
            budget: Mutex::new(ReviewBudget {
                window: Instant::now(),
                remaining: per_second,
                per_second,
            }),
            timeout: config.review_timeout,
            reviewed: TokenReviewCache::new(config.cache_max_entries),
            reviewed_ttl: config.cache_ttl,
        })
    }

    pub(super) async fn authenticate(
        &self,
        token: &str,
    ) -> Result<BuildAuthenticatedCaller, Status> {
        if !is_jwt_shaped(token) {
            return Err(Status::unauthenticated("invalid token"));
        }
        // One review timeout bounds the whole call, waits included. Without
        // it, callers bearing one token would review in turn behind a hung
        // apiserver, the k-th failing only after k timeouts.
        let deadline = tokio::time::Instant::now() + self.timeout;
        let key = TokenCacheKey::new(token, &self.policy.audience);
        let _claim = loop {
            if let Some(reviewed) = self.cached(&key) {
                return self.policy.authorize(&reviewed);
            }
            match self.reviewed.join_in_flight_or_claim(&key) {
                InFlightState::Claimed(claim) => {
                    if let Some(reviewed) = self.cached(&key) {
                        return self.policy.authorize(&reviewed);
                    }
                    break claim;
                }
                InFlightState::Waiting(waiter) => {
                    self.cache_metric("in_flight_wait");
                    if tokio::time::timeout_at(deadline, waiter).await.is_err() {
                        self.cache_metric("in_flight_timeout");
                        return Err(Status::unavailable("build token review timed out"));
                    }
                }
            }
        };
        self.cache_metric("miss");
        let reviewed = self.review(token, deadline).await?;
        let now = Instant::now();
        let expires_at =
            accepted_token_cache_expiry(token, now, SystemTime::now(), self.reviewed_ttl);
        if expires_at > now {
            if self.reviewed.insert(key, reviewed.clone(), expires_at, now) {
                self.cache_metric("evict_capacity");
            }
            self.cache_metric("insert_authenticated");
        }
        self.policy.authorize(&reviewed)
    }

    fn cached(&self, key: &TokenCacheKey) -> Option<ReviewedBuildToken> {
        match self.reviewed.lookup(key, Instant::now()) {
            Lookup::Live(reviewed) => {
                self.cache_metric("hit_authenticated");
                Some(reviewed)
            }
            Lookup::Expired => {
                self.cache_metric("evict_expired");
                None
            }
            Lookup::Absent => None,
        }
    }

    fn cache_metric(&self, result: &str) {
        pplx_space_metrics::auth::build_token_review_cache(self.surface.as_str(), result);
    }

    /// Reviews `token` with the apiserver by `deadline`, within the
    /// concurrency permits and the per-second budget. Bounds apiserver load
    /// and fails closed when the review cannot complete.
    async fn review(
        &self,
        token: &str,
        deadline: tokio::time::Instant,
    ) -> Result<ReviewedBuildToken, Status> {
        let policy = &self.policy;
        // A call that claims after another call's review failed has only what
        // is left of its deadline. Too little to finish a review is refused
        // before it spends a permit and a budget unit.
        let review_time = deadline.saturating_duration_since(tokio::time::Instant::now());
        if review_time < MIN_REVIEW_TIME {
            self.cache_metric("in_flight_timeout");
            return Err(Status::unavailable("build token review timed out"));
        }
        // A refusal here is counted, not logged: under load it repeats per
        // call, and the caller already sees the status.
        let Ok(_permit) = self.permits.try_acquire() else {
            self.cache_metric("busy");
            return Err(Status::unavailable("build token reviewer is busy"));
        };
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
                self.cache_metric("rate_limited");
                return Err(Status::unavailable("build token review rate exceeded"));
            }
            budget.remaining -= 1;
        }
        let review = TokenReview {
            spec: TokenReviewSpec {
                token: Some(token.to_string()),
                audiences: Some(vec![policy.audience.clone()]),
            },
            ..Default::default()
        };
        let response = tokio::time::timeout_at(
            deadline,
            self.reviews.create(&PostParams::default(), &review),
        )
        .await
        .map_err(|_| {
            tracing::warn!(
                audience = %policy.audience,
                timeout_secs = review_time.as_secs_f64(),
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
        let review_error = status.error.as_deref().filter(|error| !error.is_empty());
        // The apiserver answers an ordinary rejection (forged, expired or
        // wrong-audience token) with `authenticated: false` and the reason in
        // `error`. That is a verdict on the token, not a review outage, so it
        // is reported as UNAUTHENTICATED. The apiserver fills both fields from
        // one authenticator call, so an internal authenticator failure (such
        // as a failed bound-pod lookup) arrives in the same shape and is
        // reported the same way; node-api's reviewer makes the same choice.
        if status.authenticated != Some(true) {
            tracing::warn!(
                error = review_error.unwrap_or("TokenReview rejected token"),
                audience = %policy.audience,
                "build TokenReview rejected token"
            );
            return Err(Status::unauthenticated("invalid build token"));
        }
        // A conforming apiserver sets `error` only when authentication failed,
        // so this is effectively unreachable. Keep it so a contradictory
        // answer fails closed instead of granting build authority.
        if let Some(error) = review_error {
            tracing::warn!(error, audience = %policy.audience, "build TokenReview evaluation failed");
            return Err(Status::unavailable("build token review failed"));
        }
        if !status
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
        Ok(ReviewedBuildToken {
            username: username.to_owned(),
            node: AuthenticatedBuildNode::from_reviewed_user_info(&user),
        })
    }
}

impl BuildPolicy {
    /// Applies this surface's build policy to a reviewed identity. Runs on
    /// every call, cached or not.
    fn authorize(&self, reviewed: &ReviewedBuildToken) -> Result<BuildAuthenticatedCaller, Status> {
        let username = reviewed.username.as_str();
        if !self.allowed.contains(username) {
            return Err(Status::permission_denied("caller may not submit builds"));
        }
        let system = if self.system.contains(username) {
            SystemTemplateAuthority::Unconditional
        } else if self.granted_system.contains(username) {
            SystemTemplateAuthority::Granted
        } else {
            SystemTemplateAuthority::Denied
        };
        let tenant = if !self.tenant.contains(username) {
            TenantTemplateAuthority::Denied
        } else if self.tenant_requires_grant {
            TenantTemplateAuthority::Granted
        } else {
            TenantTemplateAuthority::Unconditional
        };
        let node = if tenant == TenantTemplateAuthority::Granted {
            Some(reviewed.node.clone().ok_or_else(|| {
                Status::permission_denied("publication requires a pod-bound node identity")
            })?)
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

#[cfg(test)]
mod tests;
