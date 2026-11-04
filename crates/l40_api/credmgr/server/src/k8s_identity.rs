//! Kubernetes `TokenReview` identity establishment for the credmgr TCP
//! listener.
//!
//! This replaces the mTLS client-certificate path as the way a TCP caller
//! proves *which service* it is. The caller presents a projected
//! ServiceAccount JWT in the `authorization: Bearer` header; this module
//! POSTs it to the kube-apiserver `TokenReview` API (scoped to the credmgr
//! audience), and maps the verified `system:serviceaccount:<ns>:<name>`
//! username to a [`K8sIdentity`]. Every accepted caller must carry exactly one
//! Pod name, Pod UID, node name, and node UID from its projected Pod token in
//! addition to its mapped [`ClientService`]. The apiserver does the
//! cryptographic half (signature, expiry, audience, live Pod binding); we do
//! the authorization half locally. The node metadata is issuance metadata, so
//! consumers must still fence both values against current authoritative state
//! before granting node-scoped access.
//!
//! Server-side TLS still terminates the connection — the client verifies the
//! credmgr server certificate and the channel is encrypted — but credmgr no
//! longer requires (or inspects) a *client* certificate. Identity moved from
//! the client cert SAN to the projected token.
//!
//! The machinery here (bounded positive/negative verdict cache, single-flight
//! collapse of concurrent same-token reviews, a token-bucket rate limit and a
//! concurrency cap on apiserver calls) mirrors the node-api TokenReview path
//! so a spray of distinct JWT-shaped garbage cannot translate 1:1 into
//! apiserver QPS.

use std::collections::{HashMap, hash_map::Entry};
use std::net::IpAddr;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::{Engine, engine::general_purpose};
use futures_util::FutureExt;
use k8s_openapi::api::authentication::v1::{TokenReview, TokenReviewSpec, TokenReviewStatus};
use k8s_openapi::api::core::v1::Pod;
use kube::api::PostParams;
use kube::runtime::{
    reflector::{self, ObjectRef, Store},
    watcher,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use pplx_space_common_core::types::{PodName, PodUid};
use pplx_space_common_svc::sql::node_registration::registered_node;

use crate::auth::{ClientService, K8sIdentity, K8sNodeIdentity};

const SPACE_CREDMGR_K8S_AUTH_ENABLED_ENV: &str = "SPACE_CREDMGR_K8S_AUTH_ENABLED";
const SPACE_CREDMGR_K8S_AUTH_NAMESPACE_ENV: &str = "SPACE_CREDMGR_K8S_AUTH_NAMESPACE";
const SPACE_CREDMGR_AUTH_AUDIENCE_ENV: &str = "SPACE_CREDMGR_AUTH_AUDIENCE";
const SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV: &str =
    "SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS";

const DEFAULT_CREDMGR_AUDIENCE: &str = "space-credential-manager";
const DEFAULT_CACHE_TTL_SECS: u64 = 30;
const DEFAULT_NEGATIVE_CACHE_TTL_SECS: u64 = 2;
const DEFAULT_CACHE_MAX_ENTRIES: usize = 4096;
const DEFAULT_MAX_INFLIGHT: usize = 8;
const DEFAULT_REVIEW_TIMEOUT_SECS: u64 = 5;
const DEFAULT_REVIEW_RATE_PER_SEC: u64 = 50;
const TOKEN_EXPIRATION_SKEW: Duration = Duration::from_secs(5);
const POD_NAME_EXTRA: &str = "authentication.kubernetes.io/pod-name";
const POD_UID_EXTRA: &str = "authentication.kubernetes.io/pod-uid";
const NODE_NAME_EXTRA: &str = "authentication.kubernetes.io/node-name";
const NODE_UID_EXTRA: &str = "authentication.kubernetes.io/node-uid";

const AUTH_SURFACE: &str = "credmgr-tcp";

/// Errors that prevent the TCP listener from coming up with JWT identity
/// enabled. Fail loud at boot rather than silently accept no callers.
#[derive(Debug, thiserror::Error)]
pub enum K8sAuthConfigError {
    #[error(
        "{SPACE_CREDMGR_K8S_AUTH_ENABLED_ENV} is set but KUBERNETES_SERVICE_HOST is not; \
         projected-token auth requires running inside Kubernetes"
    )]
    KubernetesServiceHostMissing,
    #[error(
        "{SPACE_CREDMGR_K8S_AUTH_ENABLED_ENV} is set but the pod namespace could not be \
         determined (set {SPACE_CREDMGR_K8S_AUTH_NAMESPACE_ENV} or mount the ServiceAccount token)"
    )]
    NamespaceMissing,
    #[error(
        "{SPACE_CREDMGR_K8S_AUTH_ENABLED_ENV} is set but no recognized credmgr client \
         ServiceAccounts are allowed"
    )]
    NoAllowedServiceAccounts,
    #[error(
        "{SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV} contains invalid entry {entry:?}: \
         expected a recognized service name or service-account=service"
    )]
    InvalidAllowedServiceAccountEntry { entry: String },
    #[error(
        "{SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV} maps ServiceAccount {service_account:?} \
         to both {existing} and {requested}"
    )]
    ConflictingAllowedServiceAccountMapping {
        service_account: String,
        existing: ClientService,
        requested: ClientService,
    },
    #[error("could not build a Kubernetes client for TokenReview: {0}")]
    KubeClient(#[from] kube::Error),
}

/// Whether the TCP listener is set up to validate projected ServiceAccount
/// tokens. Parsed once at startup from the environment.
#[derive(Clone)]
pub struct K8sAuthConfig {
    namespace: String,
    audience: String,
    /// `system:serviceaccount:<ns>:<name>` → the credmgr [`ClientService`] it
    /// maps to. Only ServiceAccounts in here can authenticate over TCP; the
    /// per-RPC allow-lists then decide which RPCs each service may call.
    allowed: HashMap<String, ClientService>,
    cache_ttl: Duration,
    negative_cache_ttl: Duration,
    cache_max_entries: usize,
    max_inflight_reviews: usize,
    review_rate_per_sec: u64,
    review_timeout: Duration,
}

impl K8sAuthConfig {
    /// Read config from the environment. Returns `Ok(None)` when JWT identity
    /// is disabled (`SPACE_CREDMGR_K8S_AUTH_ENABLED` unset/false), selecting
    /// the non-Kubernetes static-token-only listener mode.
    pub fn from_env() -> Result<Option<Self>, K8sAuthConfigError> {
        if !env_bool(SPACE_CREDMGR_K8S_AUTH_ENABLED_ENV) {
            return Ok(None);
        }
        if std::env::var_os("KUBERNETES_SERVICE_HOST").is_none() {
            return Err(K8sAuthConfigError::KubernetesServiceHostMissing);
        }
        let namespace = auth_namespace().ok_or(K8sAuthConfigError::NamespaceMissing)?;
        let audience = std::env::var(SPACE_CREDMGR_AUTH_AUDIENCE_ENV)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_CREDMGR_AUDIENCE.to_string());

        let allowed = build_allowed_map(&namespace, allowed_service_names())?;
        if allowed.is_empty() {
            return Err(K8sAuthConfigError::NoAllowedServiceAccounts);
        }

        Ok(Some(Self {
            namespace,
            audience,
            allowed,
            cache_ttl: Duration::from_secs(DEFAULT_CACHE_TTL_SECS),
            negative_cache_ttl: Duration::from_secs(DEFAULT_NEGATIVE_CACHE_TTL_SECS),
            cache_max_entries: DEFAULT_CACHE_MAX_ENTRIES,
            max_inflight_reviews: DEFAULT_MAX_INFLIGHT,
            review_rate_per_sec: DEFAULT_REVIEW_RATE_PER_SEC,
            review_timeout: Duration::from_secs(DEFAULT_REVIEW_TIMEOUT_SECS),
        }))
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn audience(&self) -> &str {
        &self.audience
    }
}

/// Build `system:serviceaccount:<ns>:<name>` → [`ClientService`] for every
/// requested ServiceAccount. The usual form is a service name such as
/// `space-volume-manager`. `account=service` maps a shared Pod ServiceAccount
/// to the credmgr client running inside that Pod; the Kind node stack uses this
/// form because its node services share one ServiceAccount.
fn build_allowed_map(
    namespace: &str,
    names: impl IntoIterator<Item = String>,
) -> Result<HashMap<String, ClientService>, K8sAuthConfigError> {
    let mut allowed = HashMap::new();
    for entry in names {
        let trimmed = entry.trim();
        let (service_account, service) = trimmed
            .split_once('=')
            .map_or((trimmed, trimmed), |(account, service)| {
                (account.trim(), service.trim())
            });
        let service = service.parse::<ClientService>().map_err(|_| {
            K8sAuthConfigError::InvalidAllowedServiceAccountEntry {
                entry: entry.clone(),
            }
        })?;
        if service_account.is_empty() || service_account.contains('=') {
            return Err(K8sAuthConfigError::InvalidAllowedServiceAccountEntry { entry });
        }

        let username = service_account_username(namespace, service_account);
        match allowed.entry(username) {
            Entry::Vacant(slot) => {
                slot.insert(service);
            }
            Entry::Occupied(slot) if *slot.get() == service => {}
            Entry::Occupied(slot) => {
                return Err(
                    K8sAuthConfigError::ConflictingAllowedServiceAccountMapping {
                        service_account: slot.key().clone(),
                        existing: *slot.get(),
                        requested: service,
                    },
                );
            }
        }
    }
    Ok(allowed)
}

/// The ServiceAccount names allowed: every credmgr client service.
/// The configured allowlist, or every credmgr client service when unset.
fn allowed_service_names() -> Vec<String> {
    let configured = std::env::var(SPACE_CREDMGR_AUTH_ALLOWED_SERVICE_ACCOUNTS_ENV)
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
        .collect::<Vec<_>>();
    if configured.is_empty() {
        ClientService::ALL
            .iter()
            .map(|service| service.as_str().to_owned())
            .collect()
    } else {
        configured
    }
}

/// Validates projected ServiceAccount tokens against the apiserver and maps
/// them to credmgr [`ClientService`] identities, with caching + backpressure.
/// Pod, node-registration, and transport-peer checks run on every request.
#[derive(Clone)]
pub struct CredmgrTokenReviewer {
    reviews: kube::Api<TokenReview>,
    config: Arc<K8sAuthConfig>,
    cache: Arc<Mutex<HashMap<TokenCacheKey, CachedReview>>>,
    in_flight: Arc<Mutex<HashMap<TokenCacheKey, Vec<tokio::sync::oneshot::Sender<()>>>>>,
    review_limiter: Arc<tokio::sync::Semaphore>,
    rate_limiter: Arc<ReviewRateLimiter>,
    pods: Store<Pod>,
    database_pool: sqlx::PgPool,
}

impl CredmgrTokenReviewer {
    pub(crate) fn new(
        config: K8sAuthConfig,
        client: kube::Client,
        database_pool: sqlx::PgPool,
        shutdown: CancellationToken,
    ) -> Self {
        let review_limiter = Arc::new(tokio::sync::Semaphore::new(config.max_inflight_reviews));
        let rate_limiter = Arc::new(ReviewRateLimiter::new(config.review_rate_per_sec));
        let pod_api = kube::Api::namespaced(client.clone(), config.namespace());
        let (pods, writer) = reflector::store();
        pplx_space_common_svc::kube_client::spawn_reflector(
            "credential-manager caller pods",
            writer,
            pod_api,
            watcher::Config::default(),
            shutdown,
        );
        Self {
            reviews: kube::Api::all(client),
            config: Arc::new(config),
            cache: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            review_limiter,
            rate_limiter,
            pods,
            database_pool,
        }
    }

    /// Authenticate a projected token and check its caller's pod, registered
    /// node, and connection IP, including on TokenReview cache hits. Every
    /// check must pass before the caller can reach a handler.
    pub async fn resolve_token_identity(
        &self,
        token: &str,
        peer: Option<IpAddr>,
    ) -> Result<K8sIdentity, Status> {
        let caller = self.review_token_identity(token).await?;
        let pod = self.pod(caller.pod().pod_name());
        let pod = pod.as_deref().map_err(Clone::clone);
        // Evaluate every check on TokenReview cache hits too, so diagnostics
        // include all failures and dependency errors remain retryable.
        let checks = [
            (
                "pod_uid",
                pod.clone()
                    .and_then(|pod| check_pod_uid(pod, caller.pod().pod_uid())),
            ),
            ("pod_running", pod.clone().and_then(check_pod_running)),
            (
                "node_identity",
                self.check_node_registration(caller.node()).await,
            ),
            ("peer_ip", pod.and_then(|pod| check_peer_ip(pod, peer))),
        ];
        let mut failure = None;
        for (check, result) in checks {
            self.report_check(check, &result, &caller, peer);
            if let Err(error) = result {
                // Dependency failures remain retryable even when another check denies.
                if failure.is_none() || error.code() == tonic::Code::Unavailable {
                    failure = Some(error);
                }
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(caller),
        }
    }

    fn pod(&self, name: &PodName) -> Result<Arc<Pod>, Status> {
        if !matches!(self.pods.wait_until_ready().now_or_never(), Some(Ok(()))) {
            return Err(Status::unavailable("pod_cache_not_synced"));
        }
        self.pods
            .get(&ObjectRef::new(name.as_str()).within(self.config.namespace()))
            .ok_or_else(|| Status::permission_denied("pod_not_found"))
    }

    async fn check_node_registration(&self, node: &K8sNodeIdentity) -> Result<(), Status> {
        let registered = registered_node(&self.database_pool, node.node_name(), node.node_uid())
            .await
            .map_err(|error| {
                tracing::warn!(%error, "node registration lookup failed");
                Status::unavailable("node_store_unavailable")
            })?;
        registered
            .map(|_| ())
            .ok_or_else(|| Status::permission_denied("node_identity_mismatch"))
    }

    fn report_check(
        &self,
        check: &'static str,
        result: &Result<(), Status>,
        caller: &K8sIdentity,
        peer: Option<IpAddr>,
    ) {
        let result_tag = if result.is_ok() { "allow" } else { "deny" };
        pplx_space_metrics::credmgr::pod_authorization(check, result_tag);
        if let Err(error) = result {
            tracing::warn!(check, result = result_tag,
                reason = error.message(), pod_uid = %caller.pod().pod_uid(),
                node_name = %caller.node().node_name(), peer_ip = ?peer,
                "credential pod authorization check failed");
        }
    }

    async fn review_token_identity(&self, token: &str) -> Result<K8sIdentity, Status> {
        let key = TokenCacheKey {
            token_digest: token_digest(token),
            audience: self.config.audience.clone(),
        };

        let _claim = loop {
            if let Some(cached) = self.cached_result(&key) {
                return self.resolve_cached(cached);
            }
            match self.join_in_flight_or_claim(&key) {
                InFlightState::Claimed(claim) => {
                    if let Some(cached) = self.cached_result(&key) {
                        return self.resolve_cached(cached);
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
        // Rate-limit + bound concurrency on actual apiserver reviews. Single-
        // flight already collapsed same-token concurrency, so this gates the
        // distinct-token spray vector. Shedding here is retryable backpressure,
        // not a verdict, so it is never cached.
        if !self.rate_limiter.try_acquire() {
            pplx_space_metrics::auth::token_review_cache("rate_limited");
            self.deny_metric("token_review_rate_limited");
            tracing::warn!(
                audience = self.config.audience,
                "credmgr TokenReview rate limit exceeded; shedding auth request"
            );
            return Err(Status::unavailable("token validation is rate limited"));
        }
        let _permit = match self.review_limiter.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                pplx_space_metrics::auth::token_review_cache("limiter_wait");
                match tokio::time::timeout(
                    self.config.review_timeout,
                    self.review_limiter.acquire(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => unreachable!("token review semaphore is never closed"),
                    Err(_elapsed) => {
                        pplx_space_metrics::auth::token_review_cache("limiter_timeout");
                        self.deny_metric("token_review_overloaded");
                        tracing::warn!(
                            audience = self.config.audience,
                            "credmgr TokenReview limiter saturated; shedding auth request"
                        );
                        return Err(Status::unavailable("token validation is overloaded"));
                    }
                }
            }
        };

        let reviewed = match self.review_token(token).await {
            Ok(reviewed) => reviewed,
            Err(ReviewFailure::Rejected { reason }) => {
                self.cache_result(
                    key.clone(),
                    CachedReviewResult::Rejected { reason },
                    Instant::now() + self.config.negative_cache_ttl,
                );
                self.deny_metric(reason.as_str());
                return Err(reason.to_status());
            }
            Err(ReviewFailure::Unavailable) => {
                // Infrastructure failure is not a verdict on the token: do not
                // negative-cache it, and deny retryably so a recovered
                // apiserver validates on the next attempt.
                self.deny_metric("token_review_unavailable");
                return Err(RejectReason::Unavailable.to_status());
            }
        };

        let service = match self.config.allowed.get(&reviewed.username).copied() {
            Some(service) => service,
            None => {
                self.cache_result(
                    key.clone(),
                    CachedReviewResult::Rejected {
                        reason: RejectReason::ServiceAccountNotAllowed,
                    },
                    Instant::now() + self.config.negative_cache_ttl,
                );
                tracing::warn!(
                    service_account = reviewed.username,
                    audience = self.config.audience,
                    "credmgr rejected a ServiceAccount that is not a recognized client"
                );
                self.deny_metric(RejectReason::ServiceAccountNotAllowed.as_str());
                return Err(RejectReason::ServiceAccountNotAllowed.to_status());
            }
        };

        let parsed_identity = match (
            reviewed.pod_name.as_deref(),
            reviewed.pod_uid.as_deref(),
            reviewed.node_name.as_deref(),
            reviewed.node_uid.as_deref(),
        ) {
            (Some(pod_name), Some(pod_uid), Some(node_name), Some(node_uid)) => {
                K8sIdentity::from_token_review(service, pod_name, pod_uid, node_name, node_uid)
            }
            _ => Err(crate::auth::InvalidK8sIdentity),
        };
        let identity = match parsed_identity {
            Ok(identity) => identity,
            Err(_) => {
                let reason = RejectReason::MissingNodeBinding;
                self.cache_result(
                    key.clone(),
                    CachedReviewResult::Rejected { reason },
                    Instant::now() + self.config.negative_cache_ttl,
                );
                self.deny_metric(reason.as_str());
                return Err(reason.to_status());
            }
        };

        let now = Instant::now();
        let expires_at =
            accepted_token_cache_expiry(token, now, SystemTime::now(), self.config.cache_ttl);
        if expires_at > now {
            self.cache_result(
                key.clone(),
                CachedReviewResult::Accepted {
                    identity: identity.clone(),
                },
                expires_at,
            );
        }
        tracing::info!(
            service_account = reviewed.username,
            service = service.as_str(),
            audience = self.config.audience,
            "credmgr accepted a projected ServiceAccount token"
        );
        pplx_space_metrics::auth::decision("k8s", AUTH_SURFACE, "allow", "ok");
        Ok(identity)
    }

    fn resolve_cached(&self, cached: CachedReviewResult) -> Result<K8sIdentity, Status> {
        match cached {
            CachedReviewResult::Accepted { identity } => {
                pplx_space_metrics::auth::token_review_cache("hit_authenticated");
                pplx_space_metrics::auth::decision("k8s", AUTH_SURFACE, "allow", "ok");
                Ok(identity)
            }
            CachedReviewResult::Rejected { reason } => {
                pplx_space_metrics::auth::token_review_cache("hit_deny");
                self.deny_metric(reason.as_str());
                Err(reason.to_status())
            }
        }
    }

    fn deny_metric(&self, reason: &str) {
        pplx_space_metrics::auth::decision("k8s", AUTH_SURFACE, "deny", reason);
    }

    fn cached_result(&self, key: &TokenCacheKey) -> Option<CachedReviewResult> {
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

    fn cache_result(&self, key: TokenCacheKey, result: CachedReviewResult, expires_at: Instant) {
        let mut cache = lock_or_recover(&self.cache);
        let (tag, evicted_capacity) = cache_review_result(
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
        pplx_space_metrics::auth::token_review_cache(tag);
    }

    async fn review_token(&self, token: &str) -> Result<ReviewedToken, ReviewFailure> {
        let review = TokenReview {
            metadata: Default::default(),
            spec: TokenReviewSpec {
                audiences: Some(vec![self.config.audience.clone()]),
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
                tracing::warn!(%error, audience = self.config.audience, "credmgr TokenReview request failed");
                return Err(ReviewFailure::Unavailable);
            }
            Err(_elapsed) => {
                tracing::warn!(
                    audience = self.config.audience,
                    timeout_secs = self.config.review_timeout.as_secs(),
                    "credmgr TokenReview request timed out"
                );
                return Err(ReviewFailure::Unavailable);
            }
        };
        validate_token_review_status(response.status, &self.config.audience)
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

/// Token-bucket rate limiter for apiserver TokenReview calls. The concurrency
/// semaphore bounds simultaneous reviews but not their rate (fast reviews free
/// permits immediately); this caps per-second rate with an equal burst.
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
        // `now` is captured before the lock; clamp both directions so a stale
        // snapshot from a late-winning thread neither over-refills here nor
        // regresses `last_refill` (which would over-mint on the next call).
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct TokenCacheKey {
    token_digest: [u8; 32],
    audience: String,
}

#[derive(Clone, Debug)]
struct CachedReview {
    result: CachedReviewResult,
    expires_at: Instant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum CachedReviewResult {
    Accepted { identity: K8sIdentity },
    Rejected { reason: RejectReason },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReviewFailure {
    Rejected { reason: RejectReason },
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RejectReason {
    Rejected,
    MissingStatus,
    AudienceMismatch,
    MissingUser,
    MissingNodeBinding,
    ServiceAccountNotAllowed,
    Unavailable,
}

impl RejectReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "token_review_rejected",
            Self::MissingStatus => "token_review_missing_status",
            Self::AudienceMismatch => "token_review_audience_mismatch",
            Self::MissingUser => "token_review_missing_user",
            Self::MissingNodeBinding => "token_review_missing_node_binding",
            Self::ServiceAccountNotAllowed => "service_account_not_allowed",
            Self::Unavailable => "token_review_unavailable",
        }
    }

    /// gRPC status for a denied request. The *code* tells the caller whether
    /// to retry: a TokenReview outage is transient infrastructure (retryable
    /// `UNAVAILABLE`), while a genuine rejection or a disallowed ServiceAccount
    /// is a permanent verdict.
    fn to_status(self) -> Status {
        match self {
            Self::Unavailable => Status::unavailable("token validation unavailable"),
            Self::ServiceAccountNotAllowed => Status::permission_denied(
                "ServiceAccount is not a recognized credential-manager client",
            ),
            Self::Rejected
            | Self::MissingStatus
            | Self::AudienceMismatch
            | Self::MissingUser
            | Self::MissingNodeBinding => Status::unauthenticated("token validation failed"),
        }
    }
}

fn prune_cache(cache: &mut HashMap<TokenCacheKey, CachedReview>, now: Instant, max_entries: usize) {
    if max_entries == 0 {
        cache.clear();
        return;
    }
    cache.retain(|_, cached| cached.expires_at > now);
}

fn cache_review_result(
    cache: &mut HashMap<TokenCacheKey, CachedReview>,
    key: TokenCacheKey,
    result: CachedReviewResult,
    expires_at: Instant,
    now: Instant,
    max_entries: usize,
) -> (&'static str, bool) {
    let tag = match &result {
        CachedReviewResult::Accepted { .. } => "insert_authenticated",
        CachedReviewResult::Rejected { .. } => "insert_deny",
    };
    if max_entries == 0 {
        cache.clear();
        return (tag, false);
    }
    prune_cache(cache, now, max_entries);
    // Capacity eviction is O(N), bounded by cache_max_entries. Keep the cap
    // modest; a materially larger cache should use a real LRU structure
    // instead of stretching this scan.
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
    cache.insert(key, CachedReview { result, expires_at });
    (tag, evicted_capacity)
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReviewedToken {
    username: String,
    pod_name: Option<String>,
    pod_uid: Option<String>,
    node_name: Option<String>,
    node_uid: Option<String>,
}

fn validate_token_review_status(
    status: Option<TokenReviewStatus>,
    audience: &str,
) -> Result<ReviewedToken, ReviewFailure> {
    let status = status.ok_or(ReviewFailure::Rejected {
        reason: RejectReason::MissingStatus,
    })?;
    if status.authenticated != Some(true) {
        let error = status
            .error
            .as_deref()
            .unwrap_or("TokenReview rejected token");
        tracing::warn!(audience, error, "credmgr TokenReview rejected token");
        return Err(ReviewFailure::Rejected {
            reason: RejectReason::Rejected,
        });
    }
    if !status
        .audiences
        .as_ref()
        .is_some_and(|audiences| audiences.iter().any(|value| value == audience))
    {
        tracing::warn!(
            audience,
            "credmgr TokenReview response did not confirm requested audience"
        );
        return Err(ReviewFailure::Rejected {
            reason: RejectReason::AudienceMismatch,
        });
    }
    let user = status.user.ok_or(ReviewFailure::Rejected {
        reason: RejectReason::MissingUser,
    })?;
    let username = user.username.ok_or(ReviewFailure::Rejected {
        reason: RejectReason::MissingUser,
    })?;
    let pod_name = single_extra_value(user.extra.as_ref(), POD_NAME_EXTRA).map(str::to_owned);
    let pod_uid = single_extra_value(user.extra.as_ref(), POD_UID_EXTRA).map(str::to_owned);
    let node_name = single_extra_value(user.extra.as_ref(), NODE_NAME_EXTRA).map(str::to_owned);
    let node_uid = single_extra_value(user.extra.as_ref(), NODE_UID_EXTRA).map(str::to_owned);
    Ok(ReviewedToken {
        username,
        pod_name,
        pod_uid,
        node_name,
        node_uid,
    })
}

fn single_extra_value<'a>(
    extra: Option<&'a std::collections::BTreeMap<String, Vec<String>>>,
    key: &str,
) -> Option<&'a str> {
    let values = extra?.get(key)?;
    match values.as_slice() {
        [value] if !value.is_empty() => Some(value),
        _ => None,
    }
}

fn check_pod_uid(pod: &Pod, expected: &PodUid) -> Result<(), Status> {
    if pod.metadata.uid.as_deref() != Some(expected.as_str()) {
        return Err(Status::permission_denied("pod_uid_mismatch"));
    }
    Ok(())
}

fn check_pod_running(pod: &Pod) -> Result<(), Status> {
    if pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        != Some("Running")
    {
        return Err(Status::permission_denied("pod_not_running"));
    }
    Ok(())
}

fn check_peer_ip(pod: &Pod, peer: Option<IpAddr>) -> Result<(), Status> {
    let peer = peer
        .map(normalize_ip)
        .ok_or_else(|| Status::permission_denied("peer_address_missing"))?;
    let matches = pod
        .status
        .as_ref()
        .and_then(|status| status.pod_ips.as_ref())
        .is_some_and(|ips| {
            ips.iter().any(|ip| {
                ip.ip
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| normalize_ip(ip) == peer)
            })
        });
    if !matches {
        return Err(Status::permission_denied("peer_ip_mismatch"));
    }
    Ok(())
}

fn normalize_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    }
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

fn lock_or_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            matches!(value.as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

fn auth_namespace() -> Option<String> {
    auth_namespace_from(
        std::env::var(SPACE_CREDMGR_K8S_AUTH_NAMESPACE_ENV).ok(),
        Path::new("/var/run/secrets/kubernetes.io/serviceaccount/namespace"),
    )
}

fn auth_namespace_from(env_value: Option<String>, namespace_path: &Path) -> Option<String> {
    env_value
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::fs::read_to_string(namespace_path)
                .ok()
                .map(|value| value.trim().to_owned())
                .filter(|value| !value.is_empty())
        })
}

#[cfg(test)]
pub(crate) mod tests;
