//! Kubernetes `TokenReview` authentication path for node-api.
//!
//! This whole module is gated behind the `k8s` cargo feature via `mod k8s;` in
//! the parent, so individual items here do not carry `#[cfg(feature = "k8s")]`.

use std::collections::{BTreeSet, HashMap, hash_map::Entry};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose};
use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec, TokenReviewStatus};
use kube::api::PostParams;
use sha2::{Digest, Sha256};
use tonic::Status;

use super::{
    AuthConfigError, AuthSurface, AuthenticatedNodeIdentity, DataPlaneSessionRefreshAuthority,
    NetworkPolicyMutationAuthority, NodeApiAuthenticatedCaller, NodeRole,
    SPACE_K8S_TOKEN_AUTH_ENABLED_ENV, WaitReadyLifecycleAuthority,
};

// The namespace is read from the pod's service account mount. The override is a
// test hook for in-process tests, which have no such mount.
const SPACE_K8S_TOKEN_AUTH_NAMESPACE_ENV: &str = "SPACE_K8S_TOKEN_AUTH_NAMESPACE";
const SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC_ENV: &str =
    "SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC";
const SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS";
const SPACE_NODE_API_AUTH_GUEST_BRIDGE_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_GUEST_BRIDGE_SERVICE_ACCOUNTS";
const SPACE_NODE_API_AUTH_PEER_NODE_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_PEER_NODE_SERVICE_ACCOUNTS";
const SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS";
const SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS";
const SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS";
const SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS";
const SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS";

const DEFAULT_NODE_API_AUDIENCE: &str = "space-node-api";
const DEFAULT_NODE_API_INTERNAL_AUDIENCE: &str = "space-node-api-internal";
const DEFAULT_CLUSTER_API_AUDIENCE: &str = "space-cluster-api";
const DEFAULT_TOKEN_REVIEW_CACHE_TTL_SECS: u64 = 30;
const DEFAULT_TOKEN_REVIEW_NEGATIVE_CACHE_TTL_SECS: u64 = 2;
const DEFAULT_TOKEN_REVIEW_CACHE_MAX_ENTRIES: usize = 4096;
// Single-flight already collapses concurrent requests bearing the *same* token
// into one TokenReview. This bounds concurrent reviews of *distinct* tokens so
// a spray of distinct JWT-shaped garbage (each a cache miss) cannot translate
// 1:1 into kube-apiserver TokenReview QPS; excess reviews queue on the permit.
const DEFAULT_TOKEN_REVIEW_MAX_INFLIGHT: usize = 8;
// Bound each TokenReview RPC so a hung/slow apiserver fails the request fast
// (closed) instead of pinning an in-flight permit for the kube client's
// multi-minute default read timeout — which would queue every cold-token auth
// behind it. Bounded review time also bounds the permit-acquire wait.
const DEFAULT_TOKEN_REVIEW_TIMEOUT_SECS: u64 = 5;
// Sustained cap on apiserver TokenReview calls per second per pod (with an
// equal burst). Legitimate steady-state is far below this — distinct tokens
// are validated once then cached ~30s — so this only bites a distinct-token
// spray. Generous enough that node restarts / token rotations don't shed.
const DEFAULT_TOKEN_REVIEW_RATE_PER_SEC: u64 = 50;
const TOKEN_EXPIRATION_SKEW: Duration = Duration::from_secs(5);

/// Token-bucket rate limiter for apiserver TokenReview calls. The concurrency
/// semaphore bounds how many reviews run at once but not the *rate*: fast
/// reviews free permits immediately, so a spray of distinct invalid tokens
/// could still drive sustained high QPS at the apiserver. This caps the
/// per-second rate (with a short burst) regardless of review latency.
struct ReviewRateLimiter {
    state: Mutex<RateLimiterState>,
    refill_per_sec: f64,
    burst: f64,
}

struct RateLimiterState {
    tokens: f64,
    last_refill: Instant,
}

impl ReviewRateLimiter {
    fn new(rate_per_sec: u64) -> Self {
        let burst = rate_per_sec.max(1) as f64;
        Self {
            state: Mutex::new(RateLimiterState {
                tokens: burst,
                last_refill: Instant::now(),
            }),
            refill_per_sec: burst,
            burst,
        }
    }

    fn try_acquire(&self) -> bool {
        self.try_acquire_at(Instant::now())
    }

    fn try_acquire_at(&self, now: Instant) -> bool {
        let mut state = lock_or_recover(&self.state);
        // `now` is captured before the lock, so a thread that wins the lock late
        // can carry an older timestamp than a thread that already refilled.
        // `saturating_duration_since` keeps a stale `now` from over-refilling
        // here, and clamping `last_refill` upward keeps it from regressing —
        // otherwise the next call would compute elapsed from a stale base and
        // mint extra tokens, weakening the rate limit.
        let elapsed = now
            .saturating_duration_since(state.last_refill)
            .as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.refill_per_sec).min(self.burst);
        state.last_refill = state.last_refill.max(now);
        if state.tokens >= 1.0 {
            state.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[derive(Clone)]
pub(super) struct K8sTokenReviewer {
    reviews: kube::Api<TokenReview>,
    config: Arc<K8sTokenAuthConfig>,
    cache: Arc<Mutex<HashMap<TokenCacheKey, CachedTokenReview>>>,
    in_flight: Arc<Mutex<HashMap<TokenCacheKey, Vec<tokio::sync::oneshot::Sender<()>>>>>,
    // Caps concurrent TokenReview calls to the apiserver across distinct tokens.
    review_limiter: Arc<tokio::sync::Semaphore>,
    // Caps the per-second rate of TokenReview calls across distinct tokens.
    rate_limiter: Arc<ReviewRateLimiter>,
}

impl K8sTokenReviewer {
    pub(super) async fn new(config: K8sTokenAuthConfig) -> Result<Self, kube::Error> {
        let client = crate::kube_client::traced_kube_client_from_env().await?;
        Ok(Self::with_client(config, client))
    }

    pub(super) fn with_client(config: K8sTokenAuthConfig, client: kube::Client) -> Self {
        let review_limiter = Arc::new(tokio::sync::Semaphore::new(config.max_inflight_reviews));
        let rate_limiter = Arc::new(ReviewRateLimiter::new(config.review_rate_per_sec));
        Self {
            reviews: kube::Api::all(client),
            config: Arc::new(config),
            cache: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            review_limiter,
            rate_limiter,
        }
    }

    pub(super) async fn authenticate(
        &self,
        role: NodeRole,
        surface: AuthSurface,
        method: &str,
        token: &str,
    ) -> Result<NodeApiAuthenticatedCaller, Status> {
        let policy = match self.config.policy(role, surface) {
            Ok(policy) => policy,
            Err(rejection) => {
                pplx_space_metrics::auth::node_api_decision(
                    "k8s",
                    surface.as_str(),
                    "deny",
                    rejection.metric_reason(),
                );
                return Err(rejection.into_status());
            }
        };
        let key = TokenCacheKey {
            token_digest: token_digest(token),
            audience: policy.audience.to_owned(),
        };

        let _in_flight_claim = loop {
            if let Some(cached) = self.cached_result(&key) {
                return self.authorize_cached_result(&policy, surface, method, cached);
            }
            match self.join_in_flight_or_claim(&key) {
                InFlightState::Claimed(claim) => {
                    if let Some(cached) = self.cached_result(&key) {
                        return self.authorize_cached_result(&policy, surface, method, cached);
                    }
                    break claim;
                }
                InFlightState::Waiting(waiter) => {
                    pplx_space_metrics::auth::token_review_cache("in_flight_wait");
                    let _ = waiter.await;
                    continue;
                }
            }
        };

        pplx_space_metrics::auth::token_review_cache("miss");
        // Rate-limit actual apiserver TokenReviews. Single-flight has already
        // collapsed same-token concurrency, so this gates *distinct*-token
        // reviews — the spray vector. The concurrency permit below bounds how
        // many run at once; the rate limiter bounds how many start per second
        // (which the permit alone does not, since fast reviews free permits
        // immediately). Shedding here is local backpressure: retryable, not a
        // verdict, so it is not cached.
        if !self.rate_limiter.try_acquire() {
            pplx_space_metrics::auth::token_review_cache("rate_limited");
            pplx_space_metrics::auth::node_api_decision(
                "k8s",
                surface.as_str(),
                "deny",
                "token_review_rate_limited",
            );
            tracing::warn!(
                surface = surface.as_str(),
                audience = policy.audience,
                "TokenReview rate limit exceeded; shedding auth request"
            );
            return Err(Status::unavailable("token validation is rate limited"));
        }
        // Bound concurrent apiserver TokenReviews; the permit is released when
        // `_review_permit` drops at the end of this scope.
        let _review_permit = match self.review_limiter.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                // Saturated. Wait only a bounded time for a permit so a spray
                // of distinct JWT-shaped tokens cannot grow the waiter set (and
                // its in-flight-map entries) without limit — shed with a
                // retryable status instead of queueing indefinitely.
                pplx_space_metrics::auth::token_review_cache("limiter_wait");
                match tokio::time::timeout(
                    self.config.review_timeout,
                    self.review_limiter.acquire(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    // The semaphore is never closed; a closed result is an
                    // invariant violation, not a fail-open condition.
                    Ok(Err(_)) => {
                        unreachable!("token review semaphore is never closed")
                    }
                    Err(_elapsed) => {
                        pplx_space_metrics::auth::token_review_cache("limiter_timeout");
                        pplx_space_metrics::auth::node_api_decision(
                            "k8s",
                            surface.as_str(),
                            "deny",
                            "token_review_overloaded",
                        );
                        tracing::warn!(
                            surface = surface.as_str(),
                            audience = policy.audience,
                            "TokenReview limiter saturated; shedding auth request"
                        );
                        // Do not negative-cache: this is local backpressure, not
                        // a verdict on the token. Retryable so a legitimate
                        // caller re-attempts once load subsides.
                        return Err(Status::unavailable("token validation is overloaded"));
                    }
                }
            }
        };
        let identity = match self.review_token(token, policy.audience).await {
            Ok(identity) => identity,
            Err(TokenReviewFailure::Rejected { reason }) => {
                self.cache_result(
                    key.clone(),
                    CachedTokenReviewResult::Rejected { reason },
                    Instant::now() + self.config.negative_cache_ttl,
                );
                pplx_space_metrics::auth::node_api_decision(
                    "k8s",
                    surface.as_str(),
                    "deny",
                    reason.as_str(),
                );
                return Err(reason.to_status());
            }
            Err(TokenReviewFailure::Unavailable) => {
                // Do not negative-cache an infrastructure failure: it is not a
                // verdict on the token, and caching it would keep denying a
                // valid token for the negative-TTL window after the apiserver
                // recovers. Single-flight still collapses the concurrent
                // same-token retries, and the rate limiter bounds the rest.
                pplx_space_metrics::auth::node_api_decision(
                    "k8s",
                    surface.as_str(),
                    "deny",
                    "token_review_unavailable",
                );
                return Err(TokenReviewRejectReason::Unavailable.to_status());
            }
        };

        let now = Instant::now();
        let expires_at =
            accepted_token_cache_expiry(token, now, SystemTime::now(), self.config.cache_ttl);
        if expires_at > now {
            self.cache_result(
                key.clone(),
                CachedTokenReviewResult::Accepted {
                    username: identity.username.clone(),
                    node_identity: identity.node_identity.clone(),
                },
                expires_at,
            );
        }
        self.authorize_username(&policy, surface, method, &identity.username, true)?;
        Ok(policy.authenticated_caller(&identity.username, identity.node_identity))
    }

    fn cached_result(&self, key: &TokenCacheKey) -> Option<CachedTokenReviewResult> {
        let now = Instant::now();
        let mut cache = lock_or_recover(&self.cache);
        match cache.get(key) {
            Some(cached) if cached.expires_at > now => Some(cached.result.clone()),
            Some(_) => {
                cache.remove(key);
                pplx_space_metrics::auth::token_review_cache("evict_expired");
                None
            }
            None => None,
        }
    }

    fn authorize_cached_result(
        &self,
        policy: &AuthPolicy<'_>,
        surface: AuthSurface,
        method: &str,
        cached: CachedTokenReviewResult,
    ) -> Result<NodeApiAuthenticatedCaller, Status> {
        match cached {
            CachedTokenReviewResult::Accepted {
                username,
                node_identity,
            } => {
                pplx_space_metrics::auth::token_review_cache("hit_authenticated");
                self.authorize_username(policy, surface, method, &username, false)?;
                Ok(policy.authenticated_caller(&username, node_identity))
            }
            CachedTokenReviewResult::Rejected { reason } => {
                pplx_space_metrics::auth::token_review_cache("hit_deny");
                pplx_space_metrics::auth::node_api_decision(
                    "k8s",
                    surface.as_str(),
                    "deny",
                    reason.as_str(),
                );
                Err(reason.to_status())
            }
        }
    }

    fn join_in_flight_or_claim(&self, key: &TokenCacheKey) -> InFlightState {
        let mut in_flight = lock_or_recover(&self.in_flight);
        match in_flight.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                entry.get_mut().push(tx);
                InFlightState::Waiting(rx)
            }
            Entry::Vacant(entry) => {
                entry.insert(Vec::new());
                InFlightState::Claimed(InFlightClaim {
                    key: key.clone(),
                    in_flight: Arc::clone(&self.in_flight),
                })
            }
        }
    }

    fn authorize_username(
        &self,
        policy: &AuthPolicy<'_>,
        surface: AuthSurface,
        method: &str,
        username: &str,
        log_accept: bool,
    ) -> Result<(), Status> {
        if !policy.allowed_usernames.contains(username) {
            tracing::warn!(
                service_account = username,
                method,
                surface = surface.as_str(),
                audience = policy.audience,
                "Kubernetes service account is not allowed for node-api surface"
            );
            pplx_space_metrics::auth::node_api_decision(
                "k8s",
                surface.as_str(),
                "deny",
                "service_account_not_allowed",
            );
            return Err(Status::permission_denied(format!(
                "service account {username} is not allowed for this node-api surface"
            )));
        }

        if policy.guest_bridge_usernames.contains(username)
            && !guest_bridge_method(policy.role, method)
        {
            return Err(Status::permission_denied(
                "guest-bridge service account is not allowed for this node-api method",
            ));
        }

        if policy.peer_node_usernames.contains(username) && !peer_node_method(policy.role, method) {
            return Err(Status::permission_denied(
                "peer node service account is not allowed for this node-api method",
            ));
        }

        if log_accept {
            tracing::info!(
                service_account = username,
                method,
                surface = surface.as_str(),
                audience = policy.audience,
                "accepted Kubernetes service account for node-api"
            );
        }
        pplx_space_metrics::auth::node_api_decision("k8s", surface.as_str(), "allow", "ok");
        Ok(())
    }

    fn cache_result(
        &self,
        key: TokenCacheKey,
        result: CachedTokenReviewResult,
        expires_at: Instant,
    ) {
        let mut cache = lock_or_recover(&self.cache);
        let (result_tag, evicted_capacity) = cache_token_review_result(
            &mut cache,
            key,
            result,
            expires_at,
            Instant::now(),
            self.config.cache_max_entries,
        );
        if evicted_capacity {
            pplx_space_metrics::auth::token_review_cache("evict_capacity");
        }
        pplx_space_metrics::auth::token_review_cache(result_tag);
    }

    async fn review_token(
        &self,
        token: &str,
        audience: &str,
    ) -> Result<ReviewedIdentity, TokenReviewFailure> {
        let review = TokenReview {
            metadata: Default::default(),
            spec: TokenReviewSpec {
                audiences: Some(vec![audience.to_owned()]),
                token: Some(token.to_owned()),
            },
            status: None,
        };

        let response = match tokio::time::timeout(
            self.config.review_timeout,
            self.reviews.create(&PostParams::default(), &review),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => {
                tracing::warn!(%error, audience, "TokenReview request failed");
                return Err(TokenReviewFailure::Unavailable);
            }
            Err(_elapsed) => {
                tracing::warn!(
                    audience,
                    timeout_secs = self.config.review_timeout.as_secs(),
                    "TokenReview request timed out"
                );
                return Err(TokenReviewFailure::Unavailable);
            }
        };

        validate_token_review_status(response.status, audience)
    }
}

enum InFlightState {
    Claimed(InFlightClaim),
    Waiting(tokio::sync::oneshot::Receiver<()>),
}

struct InFlightClaim {
    key: TokenCacheKey,
    in_flight: Arc<Mutex<HashMap<TokenCacheKey, Vec<tokio::sync::oneshot::Sender<()>>>>>,
}

impl Drop for InFlightClaim {
    fn drop(&mut self) {
        let waiters = lock_or_recover(&self.in_flight)
            .remove(&self.key)
            .unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(());
        }
    }
}

#[derive(Clone)]
pub(super) struct K8sTokenAuthConfig {
    pub(super) namespace: String,
    pub(super) node_api_audience: String,
    pub(super) node_api_internal_audience: String,
    pub(super) cluster_api_audience: String,
    node_api_allowed_usernames: BTreeSet<String>,
    node_api_guest_bridge_usernames: BTreeSet<String>,
    node_api_peer_node_usernames: BTreeSet<String>,
    node_api_trusted_wait_ready_usernames: BTreeSet<String>,
    node_api_trusted_policy_usernames: BTreeSet<String>,
    template_transfer_usernames: BTreeSet<String>,
    node_api_internal_allowed_usernames: BTreeSet<String>,
    cluster_api_allowed_usernames: BTreeSet<String>,
    cache_ttl: Duration,
    negative_cache_ttl: Duration,
    cache_max_entries: usize,
    pub(super) max_inflight_reviews: usize,
    review_rate_per_sec: u64,
    pub(super) review_timeout: Duration,
}

impl K8sTokenAuthConfig {
    /// Build the Kubernetes token-auth config from the environment.
    ///
    /// Returns `Ok(None)` when `SPACE_K8S_TOKEN_AUTH_ENABLED` is not set, so the
    /// caller falls back to the static-token-only path. When enabled, requires
    /// `KUBERNETES_SERVICE_HOST` to be present.
    pub(super) fn from_env() -> Result<Option<Self>, AuthConfigError> {
        Self::from_environment(&pplx_space_common_core::env::ProcessEnvironment)
    }

    pub(crate) fn from_environment(
        env: &dyn pplx_space_common_core::env::Environment,
    ) -> Result<Option<Self>, AuthConfigError> {
        if !super::env_bool_from_environment(env, SPACE_K8S_TOKEN_AUTH_ENABLED_ENV) {
            return Ok(None);
        }
        if env.var_os("KUBERNETES_SERVICE_HOST").is_none() {
            return Err(AuthConfigError::KubernetesServiceHostMissing);
        }

        let namespace = auth_namespace(env)?;
        let qualified_env_set = |name| {
            env_csv_set(env, name)
                .into_iter()
                .map(|service_account| service_account_username(&namespace, &service_account))
                .collect()
        };
        let mut guest_bridge_usernames: BTreeSet<String> =
            qualified_env_set(SPACE_NODE_API_AUTH_GUEST_BRIDGE_SERVICE_ACCOUNTS_ENV);
        // The built-in edge identity remains restricted even when deployments
        // add renamed or additional guest-facing service accounts.
        guest_bridge_usernames.insert(service_account_username(&namespace, "space-data-plane"));
        Ok(Some(Self {
            namespace: namespace.clone(),
            node_api_audience: DEFAULT_NODE_API_AUDIENCE.to_owned(),
            node_api_internal_audience: DEFAULT_NODE_API_INTERNAL_AUDIENCE.to_owned(),
            cluster_api_audience: DEFAULT_CLUSTER_API_AUDIENCE.to_owned(),
            node_api_allowed_usernames: qualified_env_set(
                SPACE_NODE_API_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV,
            ),
            node_api_guest_bridge_usernames: guest_bridge_usernames,
            node_api_peer_node_usernames: qualified_env_set(
                SPACE_NODE_API_AUTH_PEER_NODE_SERVICE_ACCOUNTS_ENV,
            ),
            node_api_trusted_wait_ready_usernames: qualified_env_set(
                SPACE_NODE_API_AUTH_TRUSTED_WAIT_READY_SERVICE_ACCOUNTS_ENV,
            ),
            node_api_trusted_policy_usernames: qualified_env_set(
                SPACE_NODE_API_AUTH_TRUSTED_POLICY_SERVICE_ACCOUNTS_ENV,
            ),
            template_transfer_usernames: qualified_env_set(
                SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS_ENV,
            ),
            node_api_internal_allowed_usernames: qualified_env_set(
                SPACE_NODE_API_INTERNAL_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV,
            ),
            cluster_api_allowed_usernames: qualified_env_set(
                SPACE_CLUSTER_API_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV,
            ),
            cache_ttl: Duration::from_secs(DEFAULT_TOKEN_REVIEW_CACHE_TTL_SECS),
            negative_cache_ttl: Duration::from_secs(DEFAULT_TOKEN_REVIEW_NEGATIVE_CACHE_TTL_SECS),
            cache_max_entries: DEFAULT_TOKEN_REVIEW_CACHE_MAX_ENTRIES,
            max_inflight_reviews: DEFAULT_TOKEN_REVIEW_MAX_INFLIGHT,
            review_rate_per_sec: env_u64(
                env,
                SPACE_K8S_TOKEN_AUTH_REVIEW_RATE_PER_SEC_ENV,
                DEFAULT_TOKEN_REVIEW_RATE_PER_SEC,
            ),
            review_timeout: Duration::from_secs(DEFAULT_TOKEN_REVIEW_TIMEOUT_SECS),
        }))
    }

    pub(super) fn validate_for_role(&self, role: NodeRole) -> Result<(), AuthConfigError> {
        if matches!(role, NodeRole::Worker | NodeRole::Builder)
            && self.template_transfer_usernames.is_empty()
        {
            tracing::warn!(
                setting = SPACE_NODE_API_AUTH_TEMPLATE_TRANSFER_SERVICE_ACCOUNTS_ENV,
                "template transfer allowlist is empty; peer template transfers are disabled"
            );
        }
        if role == NodeRole::Worker && self.node_api_trusted_wait_ready_usernames.is_empty() {
            return Err(AuthConfigError::TrustedWaitReadyServiceAccountsMissing);
        }
        // A worker serves the policy-mutation RPCs, and both roles gate template
        // deletion on this allowlist, so both must ship a non-empty one. Fail
        // closed at startup rather than silently admitting every allow-listed
        // caller.
        if matches!(role, NodeRole::Worker | NodeRole::Builder)
            && self.node_api_trusted_policy_usernames.is_empty()
        {
            return Err(AuthConfigError::TrustedPolicyServiceAccountsMissing);
        }
        Ok(())
    }

    fn policy(
        &self,
        role: NodeRole,
        surface: AuthSurface,
    ) -> Result<AuthPolicy<'_>, AuthPolicyRejection> {
        let (audience, allowed_usernames, trusted_wait_ready_usernames) = match (role, surface) {
            (NodeRole::Cluster, AuthSurface::Normal) => (
                self.cluster_api_audience.as_str(),
                &self.cluster_api_allowed_usernames,
                None,
            ),
            (NodeRole::Worker, AuthSurface::Normal) => (
                self.node_api_audience.as_str(),
                &self.node_api_allowed_usernames,
                Some(&self.node_api_trusted_wait_ready_usernames),
            ),
            (NodeRole::Builder, AuthSurface::Normal) => (
                self.node_api_audience.as_str(),
                &self.node_api_allowed_usernames,
                None,
            ),
            (_, AuthSurface::WorkerInternal) => (
                self.node_api_internal_audience.as_str(),
                &self.node_api_internal_allowed_usernames,
                None,
            ),
        };

        if allowed_usernames.is_empty() {
            return Err(AuthPolicyRejection::NoAllowedServiceAccounts);
        }

        // Policy-mutation trust is an explicit, independently-configured
        // allowlist (validated non-empty at startup for worker/builder). It is
        // `Some` only on the surfaces that serve policy or template-mutation
        // RPCs; every other surface reports `None`, which the authority check
        // treats as "deny".
        let trusted_policy_usernames = match (role, surface) {
            // Internal pool adoption also accepts caller-supplied policy. Its
            // caller must pass the internal allowlist as well as policy trust.
            (NodeRole::Worker | NodeRole::Builder, AuthSurface::Normal)
            | (NodeRole::Worker, AuthSurface::WorkerInternal) => {
                Some(&self.node_api_trusted_policy_usernames)
            }
            _ => None,
        };

        Ok(AuthPolicy {
            role,
            audience,
            allowed_usernames,
            trusted_wait_ready_usernames,
            guest_bridge_usernames: &self.node_api_guest_bridge_usernames,
            peer_node_usernames: &self.node_api_peer_node_usernames,
            trusted_policy_usernames,
            template_transfer_usernames: &self.template_transfer_usernames,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthPolicyRejection {
    NoAllowedServiceAccounts,
}

impl AuthPolicyRejection {
    const fn metric_reason(self) -> &'static str {
        match self {
            Self::NoAllowedServiceAccounts => "no_allowed_service_accounts",
        }
    }

    fn into_status(self) -> Status {
        match self {
            Self::NoAllowedServiceAccounts => Status::permission_denied(
                "Kubernetes token auth is enabled but no service accounts are allowed for this surface",
            ),
        }
    }
}

#[derive(Debug)]
struct AuthPolicy<'a> {
    role: NodeRole,
    audience: &'a str,
    allowed_usernames: &'a BTreeSet<String>,
    trusted_wait_ready_usernames: Option<&'a BTreeSet<String>>,
    guest_bridge_usernames: &'a BTreeSet<String>,
    peer_node_usernames: &'a BTreeSet<String>,
    trusted_policy_usernames: Option<&'a BTreeSet<String>>,
    template_transfer_usernames: &'a BTreeSet<String>,
}

/// Guest operations retain their sandbox-scoped route-token checks downstream.
/// Do not grant this identity every future `Spaced*` RPC implicitly: a new
/// method needs an explicit decision about the edge service's authority.
fn guest_bridge_method(role: NodeRole, path: &str) -> bool {
    if path == "/space.node.api.v1.NodeInfo/GetNodeInfo" {
        return true;
    }
    if role == NodeRole::Cluster {
        let Some(method) = path.strip_prefix("/space.node.api.v1.ClusterApi/") else {
            return false;
        };
        // These are the edge's existing session and runtime-routing calls,
        // not authority to mutate node inventory or artifact catalogs.
        return matches!(
            method,
            "ResolveDataPlaneSessionRoute"
                | "BindDataPlaneSessionProcess"
                | "RecordDataPlaneSessionReconnect"
                | "GetDataPlaneSessionRevocationState"
                | "GetDataPlaneOrgRevocationEpoch"
                | "GetSandboxRuntimeAccess"
                | "UnstableResolveSandboxServiceRoute"
                | "UnstableResetSandboxServiceRoute"
        );
    }
    if role != NodeRole::Worker {
        return false;
    }
    let Some(method) = path.strip_prefix("/space.node.api.v1.WorkerApi/") else {
        return false;
    };
    matches!(
        method,
        "SpacedGetStatus"
            | "SpacedWaitReady"
            | "SpacedStat"
            | "SpacedList"
            | "SpacedRead"
            | "SpacedWrite"
            | "SpacedWriteBatch"
            | "SpacedMkdir"
            | "SpacedRemove"
            | "SpacedRename"
            | "SpacedNormalizeWatchRoot"
            | "SpacedWatch"
            | "SpacedCreateWatcher"
            | "SpacedGetWatcherEvents"
            | "SpacedRemoveWatcher"
            | "SpacedEnableFileChanges"
            | "SpacedDisableFileChanges"
            | "SpacedGetModifiedFiles"
            | "SpacedStartProcess"
            | "SpacedConnectProcess"
            | "SpacedListProcesses"
            | "SpacedSendSignal"
            | "SpacedSendInput"
            | "SpacedCloseStdin"
            | "SpacedResizePty"
            | "SpacedKillProcess"
            | "SpacedOpenLoopbackBridge"
            | "SpacedCloseLoopbackBridge"
            | "ServicePortIngress"
    )
}

/// A peer node's node-api calls this node only to export a template it is
/// pulling (`connect_remote`, node-api `transfer.rs`). Every other RPC stays
/// refused for a peer-node account, whatever else lists it.
fn peer_node_method(role: NodeRole, path: &str) -> bool {
    if path == "/space.node.api.v1.NodeInfo/GetNodeInfo" {
        return true;
    }
    match role {
        NodeRole::Worker => path == "/space.node.api.v1.WorkerApi/NodeExportTemplate",
        NodeRole::Builder => path == "/space.node.api.v1.BuilderApi/NodeExportTemplate",
        NodeRole::Cluster => false,
    }
}

impl AuthPolicy<'_> {
    fn wait_ready_lifecycle_authority(&self, username: &str) -> WaitReadyLifecycleAuthority {
        if self
            .trusted_wait_ready_usernames
            .is_some_and(|usernames| usernames.contains(username))
            && !self.guest_bridge_usernames.contains(username)
        {
            WaitReadyLifecycleAuthority::Granted
        } else {
            WaitReadyLifecycleAuthority::Denied
        }
    }

    fn network_policy_mutation_authority(&self, username: &str) -> NetworkPolicyMutationAuthority {
        // Fail closed: only an explicitly trusted account on a policy-serving
        // surface is granted. `None` (a surface that does not serve the policy
        // RPCs) and any unlisted account are denied.
        match self.trusted_policy_usernames {
            Some(usernames) if usernames.contains(username) => {
                NetworkPolicyMutationAuthority::Granted
            }
            _ => NetworkPolicyMutationAuthority::Denied,
        }
    }

    /// Only the guest-bridge (data-plane edge) accounts refresh session
    /// routes without the attach token; every other admitted account must
    /// prove the attach it is resolving.
    fn data_plane_session_refresh_authority(
        &self,
        username: &str,
    ) -> DataPlaneSessionRefreshAuthority {
        if self.guest_bridge_usernames.contains(username) {
            DataPlaneSessionRefreshAuthority::Granted
        } else {
            DataPlaneSessionRefreshAuthority::Denied
        }
    }

    fn authenticated_caller(
        &self,
        username: &str,
        node_identity: AuthenticatedNodeIdentity,
    ) -> NodeApiAuthenticatedCaller {
        let mut caller = NodeApiAuthenticatedCaller::kubernetes_service_account(
            self.wait_ready_lifecycle_authority(username),
            self.network_policy_mutation_authority(username),
            self.data_plane_session_refresh_authority(username),
        );
        caller.node_identity = node_identity;
        if matches!(self.role, NodeRole::Worker | NodeRole::Builder)
            && self.template_transfer_usernames.contains(username)
            && !self.guest_bridge_usernames.contains(username)
        {
            caller.template_transfer_authority =
                super::template_transfer::TemplateTransferAuthority::Granted;
        }
        caller
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TokenCacheKey {
    token_digest: [u8; 32],
    audience: String,
}

#[derive(Clone, Debug)]
struct CachedTokenReview {
    result: CachedTokenReviewResult,
    expires_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CachedTokenReviewResult {
    Accepted {
        username: String,
        node_identity: AuthenticatedNodeIdentity,
    },
    Rejected {
        reason: TokenReviewRejectReason,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenReviewFailure {
    Rejected { reason: TokenReviewRejectReason },
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenReviewRejectReason {
    Rejected,
    MissingStatus,
    AudienceMismatch,
    MissingUser,
    Unavailable,
}

impl TokenReviewRejectReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "token_review_rejected",
            Self::MissingStatus => "token_review_missing_status",
            Self::AudienceMismatch => "token_review_audience_mismatch",
            Self::MissingUser => "token_review_missing_user",
            Self::Unavailable => "token_review_unavailable",
        }
    }

    /// gRPC status for a denied request. The request is denied either way;
    /// the *code* tells the caller whether to retry. A `TokenReview` outage
    /// (apiserver timeout, transient RBAC/network failure) is not a verdict on
    /// the token, so it maps to retryable `UNAVAILABLE` — otherwise callers
    /// that treat `UNAUTHENTICATED` as permanent (e.g. the snapshot-upload job
    /// queue) would fail permanently on a transient blip. Genuine rejections
    /// stay `UNAUTHENTICATED`.
    fn to_status(self) -> Status {
        match self {
            Self::Unavailable => Status::unavailable("token validation unavailable"),
            Self::Rejected | Self::MissingStatus | Self::AudienceMismatch | Self::MissingUser => {
                Status::unauthenticated("token validation failed")
            }
        }
    }
}

fn prune_token_review_cache(
    cache: &mut HashMap<TokenCacheKey, CachedTokenReview>,
    now: Instant,
    max_entries: usize,
) {
    if max_entries == 0 {
        cache.clear();
        return;
    }
    cache.retain(|_, cached| cached.expires_at > now);
}

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn cache_token_review_result(
    cache: &mut HashMap<TokenCacheKey, CachedTokenReview>,
    key: TokenCacheKey,
    result: CachedTokenReviewResult,
    expires_at: Instant,
    now: Instant,
    max_entries: usize,
) -> (&'static str, bool) {
    let result_tag = match &result {
        CachedTokenReviewResult::Accepted { .. } => "insert_authenticated",
        CachedTokenReviewResult::Rejected { .. } => "insert_deny",
    };
    if max_entries == 0 {
        cache.clear();
        return (result_tag, false);
    }

    prune_token_review_cache(cache, now, max_entries);
    // Capacity eviction is O(N), bounded by cache_max_entries. Keep the cap
    // modest; if this cache ever needs to grow materially, replace the HashMap
    // with a real LRU/LFU structure instead of stretching this scan.
    let evicted_capacity = if cache.len() >= max_entries {
        if let Some(evict_key) = cache
            .iter()
            .min_by_key(|(_, cached)| cached.expires_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&evict_key);
            true
        } else {
            false
        }
    } else {
        false
    };
    cache.insert(key, CachedTokenReview { result, expires_at });
    (result_tag, evicted_capacity)
}

fn accepted_token_cache_expiry(
    token: &str,
    now: Instant,
    now_system: SystemTime,
    cache_ttl: Duration,
) -> Instant {
    let ttl_deadline = now.checked_add(cache_ttl).unwrap_or(now);
    jwt_exp_cache_deadline(token, now, now_system)
        .map(|exp_deadline| std::cmp::min(ttl_deadline, exp_deadline))
        .unwrap_or(ttl_deadline)
}

fn jwt_exp_cache_deadline(token: &str, now: Instant, now_system: SystemTime) -> Option<Instant> {
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload = parts.next()?;
    let _signature = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    let payload = general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let payload: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let exp = payload.get("exp")?.as_u64()?;
    let exp = UNIX_EPOCH.checked_add(Duration::from_secs(exp))?;
    let effective_exp = exp.checked_sub(TOKEN_EXPIRATION_SKEW).unwrap_or(UNIX_EPOCH);
    let duration = effective_exp.duration_since(now_system).unwrap_or_default();
    now.checked_add(duration).or(Some(now))
}

#[derive(Debug)]
struct ReviewedIdentity {
    username: String,
    node_identity: AuthenticatedNodeIdentity,
}

fn validate_token_review_status(
    status: Option<TokenReviewStatus>,
    audience: &str,
) -> Result<ReviewedIdentity, TokenReviewFailure> {
    let status = status.ok_or(TokenReviewFailure::Rejected {
        reason: TokenReviewRejectReason::MissingStatus,
    })?;
    if status.authenticated != Some(true) {
        let error = status
            .error
            .as_deref()
            .unwrap_or("TokenReview rejected token");
        tracing::warn!(audience, error, "TokenReview rejected token");
        return Err(TokenReviewFailure::Rejected {
            reason: TokenReviewRejectReason::Rejected,
        });
    }
    if !status
        .audiences
        .as_ref()
        .is_some_and(|audiences| audiences.iter().any(|value| value == audience))
    {
        tracing::warn!(
            audience,
            "TokenReview response did not confirm requested audience"
        );
        return Err(TokenReviewFailure::Rejected {
            reason: TokenReviewRejectReason::AudienceMismatch,
        });
    }
    let user = status.user.ok_or(TokenReviewFailure::Rejected {
        reason: TokenReviewRejectReason::MissingUser,
    })?;
    let node_identity = AuthenticatedNodeIdentity::from_reviewed_user_info(&user);
    let username = user.username.ok_or(TokenReviewFailure::Rejected {
        reason: TokenReviewRejectReason::MissingUser,
    })?;
    Ok(ReviewedIdentity {
        username,
        node_identity,
    })
}

fn token_digest(token: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher.finalize().into()
}

fn service_account_username(namespace: &str, name: &str) -> String {
    if name.starts_with("system:serviceaccount:") {
        name.to_owned()
    } else {
        format!("system:serviceaccount:{namespace}:{name}")
    }
}

fn env_csv_set(env: &dyn pplx_space_common_core::env::Environment, name: &str) -> BTreeSet<String> {
    env.var(name)
        .ok()
        .into_iter()
        .flat_map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn env_u64(env: &dyn pplx_space_common_core::env::Environment, name: &str, default: u64) -> u64 {
    env.var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn auth_namespace(
    env: &dyn pplx_space_common_core::env::Environment,
) -> Result<String, AuthConfigError> {
    auth_namespace_from_file(
        env,
        Path::new("/var/run/secrets/kubernetes.io/serviceaccount/namespace"),
    )
}

fn auth_namespace_from_file(
    env: &dyn pplx_space_common_core::env::Environment,
    namespace_path: &Path,
) -> Result<String, AuthConfigError> {
    pplx_space_common_core::env::test_hook(env, SPACE_K8S_TOKEN_AUTH_NAMESPACE_ENV)
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::fs::read_to_string(namespace_path)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
        .ok_or(AuthConfigError::KubernetesNamespaceMissing)
}

#[cfg(test)]
mod tests;
