//! Intermediate-key broker backend for [`KeyWrapper`].
//!
//! For [`KekProvider::IntermediateKeyBroker`] orgs the KMS root lives in an
//! external service, the intermediate-key broker. It vends each org an
//! intermediate KEK — the *intermediate key* — that SPACE only ever persists in
//! broker-wrapped form. This backend asks the provider to unwrap the org's blob
//! ([`IntermediateKeyBrokerApi`]), holds the plaintext in memory for a bounded
//! TTL, and performs all per-DEK wrap/unwrap operations locally under the
//! cached key, so the provider sits on the once-per-TTL path rather than the
//! per-operation path.
//!
//! Fail-closed properties:
//! - the plaintext intermediate key never outlives the cache TTL without a
//!   fresh provider authorization;
//! - an explicit provider revocation (org suspended or offboarded) halts all
//!   wraps and unwraps for the org once the cached entry expires — the TTL
//!   is the revocation SLA — and surfaces as the typed
//!   `WrapError::AccessRevoked` / `UnwrapError::AccessRevoked` that upstream
//!   cleanup and sandbox-shutdown machinery keys on;
//! - no serve-stale: an expired entry plus an unreachable provider is an
//!   error, the same operational posture as a key-provider outage.
//!
//! This backend is installed only in the control-plane authority; node-local
//! deployments must not hold intermediate keys or dial the provider.

use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use dashmap::DashMap;
use pplx_space_common_svc::core::types::{KekProvider, MasterKeyIdentifier, OrgId, SandboxId};
use pplx_space_credential_provider_client::v2::issue_intermediate_key_response::Outcome as IssueOutcome;
use pplx_space_credential_provider_client::v2::unwrap_intermediate_key_response::Outcome as UnwrapOutcome;
use pplx_space_credential_provider_client::v2::{
    AuthorizationEvidence, IssueFailureReason, IssueIntermediateKeyRequest, TenantRef,
    UnwrapFailureReason, UnwrapIntermediateKeyRequest,
};
use pplx_space_credential_provider_client::{
    BearerTokenFileInterceptor, IntermediateKeyBrokerGrpcClient,
    traced_intermediate_key_broker_client,
};
use tokio::time::Instant;
use zeroize::{Zeroize, Zeroizing};

use crate::intermediate_key_identity::{IntermediateKeyIdentity, IntermediateKeyIdentityLookup};
use crate::key_wrapper::{DekKeySpec, GenerateDekOutput, KeyWrapper, UnwrapError, WrapError};

/// AES-GCM nonce length (96-bit, the only widely supported size).
const NONCE_LEN: usize = 12;

/// AES-GCM authentication tag length (128-bit).
const TAG_LEN: usize = 16;

/// Version byte prefixed to this backend's wrap bytes so the DEK wrap format
/// can evolve independently of the outer wrapped-DEK envelope version.
const INTERMEDIATE_KEY_WRAP_VERSION: u8 = 1;

const TENANT_NAMESPACE_ENV: &str = "SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_TENANT_NAMESPACE";
const GRANT_SCHEME_ENV: &str = "SPACE_CREDMGR_INTERMEDIATE_KEY_BROKER_GRANT_SCHEME";

/// Provider-owned wire identifiers. Existing deployments must preserve these
/// values: the namespace routes historical wrapped keys, and the scheme tells
/// the provider how to interpret the opaque grant.
#[derive(Clone, Debug)]
pub struct BrokerProtocolConfig {
    tenant_namespace: String,
    grant_scheme: String,
}

#[derive(Debug, thiserror::Error)]
#[error(
    "{setting} must be a non-empty identifier without whitespace when the intermediate-key broker is enabled"
)]
pub struct BrokerProtocolConfigError {
    setting: &'static str,
}

impl BrokerProtocolConfig {
    pub fn from_env() -> Result<Self, BrokerProtocolConfigError> {
        Self::parse(
            std::env::var(TENANT_NAMESPACE_ENV).ok().as_deref(),
            std::env::var(GRANT_SCHEME_ENV).ok().as_deref(),
        )
    }

    pub fn parse(
        tenant_namespace: Option<&str>,
        grant_scheme: Option<&str>,
    ) -> Result<Self, BrokerProtocolConfigError> {
        let required = |value: Option<&str>, setting| {
            value
                .filter(|value| !value.is_empty() && !value.chars().any(char::is_whitespace))
                .map(str::to_owned)
                .ok_or(BrokerProtocolConfigError { setting })
        };
        Ok(Self {
            tenant_namespace: required(tenant_namespace, TENANT_NAMESPACE_ENV)?,
            grant_scheme: required(grant_scheme, GRANT_SCHEME_ENV)?,
        })
    }
}

/// Plaintext intermediate key returned by the provider.
pub struct UnwrappedIntermediateKey {
    /// 32-byte key material. Zeroized when the cache entry is dropped.
    pub key: Zeroizing<[u8; 32]>,
    /// Provider-imposed upper bound on caching the plaintext. The effective
    /// cache TTL is `min(local config, this cap)`.
    pub max_cache_ttl: Duration,
}

/// Manual impl so key material can never reach logs through `{:?}`.
impl std::fmt::Debug for UnwrappedIntermediateKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnwrappedIntermediateKey")
            .field("key", &"<redacted>")
            .field("max_cache_ttl", &self.max_cache_ttl)
            .finish()
    }
}

/// Application-level unwrap outcomes, as classified by the provider in-band
/// (`UnwrapFailure` in the proto). The classes drive different SPACE
/// reactions, so they are never inferred from transport/gRPC status codes:
/// revocation triggers cleanup and sandbox shutdown, and a proxy or
/// credential misconfiguration between the services must not be able to
/// masquerade as it.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    /// The provider explicitly revoked access to this org's key (org
    /// suspended or offboarded). Fail closed; negative-cached; upstream
    /// this drives org cleanup and sandbox shutdown rather than retry.
    #[error("intermediate-key broker revoked access: {0}")]
    AccessRevoked(String),
    /// The tenant reference is unknown or invalid. Permanent for this input;
    /// unlike revocation it must not drive cleanup.
    #[error("intermediate-key broker rejected the tenant reference: {0}")]
    InvalidTenant(String),
    /// The key reference is invalid (unknown org, unknown key id, blob that
    /// fails broker validation or context binding). Permanent for these inputs.
    #[error("intermediate-key broker rejected the key reference: {0}")]
    InvalidKey(String),
    /// The broker rejected the opaque authorization evidence. This is
    /// permanent for the presented evidence, but is never revocation and must
    /// not trigger tenant cleanup: a renewed grant may immediately succeed.
    #[error("intermediate-key broker rejected authorization evidence: {0}")]
    AuthorizationRejected(String),
    /// The provider response violated the contract (missing outcome,
    /// unrecognized failure reason, key material that is not 32 bytes).
    #[error("intermediate-key broker returned a malformed response: {0}")]
    Malformed(String),
    /// Transient failure — an in-band UNAVAILABLE, any non-OK gRPC status
    /// (transport-level only by contract), or a deadline. Retriable with
    /// backoff; never cached.
    #[error("intermediate-key broker unavailable: {0}")]
    Unavailable(String),
}

/// A freshly issued intermediate key: the provider's stable per-version key id
/// and the broker-wrapped blob SPACE persists. Never carries plaintext.
#[derive(Debug)]
pub struct IssuedIntermediateKey {
    pub key_id: MasterKeyIdentifier,
    pub wrapped_key: Vec<u8>,
}

/// The provider operations this backend needs. Production uses
/// [`GrpcIntermediateKeyBroker`]; tests implement this directly.
#[async_trait]
pub trait IntermediateKeyBrokerApi: Send + Sync + 'static {
    /// Mint a fresh intermediate key for the org, returned wrapped-only.
    /// Called by provisioning ([`crate::kek_provisioning`]); issuing again
    /// for an enrolled org starts a rotation at the broker.
    async fn issue_intermediate_key(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<IssuedIntermediateKey, ProviderError>;

    async fn unwrap_intermediate_key(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        key_id: &MasterKeyIdentifier,
        wrapped_key: &[u8],
    ) -> Result<UnwrappedIntermediateKey, ProviderError>;
}

/// Error reading the persisted wrapped-intermediate-key blobs.
#[derive(Debug, thiserror::Error)]
pub enum WrappedKeyStoreError {
    #[error("wrapped intermediate-key lookup failed: {0}")]
    Lookup(#[source] sqlx::Error),
}

/// Source of the persisted broker-wrapped intermediate-key blobs, keyed by
/// `(org, key identifier)`. Production reads the `org_branching_keys`
/// sidecar table; tests use [`InMemoryWrappedKeyStore`].
#[async_trait]
pub trait WrappedIntermediateKeyStore: Send + Sync + 'static {
    async fn wrapped_key(
        &self,
        org: OrgId,
        key_id: &MasterKeyIdentifier,
    ) -> Result<Option<Vec<u8>>, WrappedKeyStoreError>;
}

/// In-process [`WrappedIntermediateKeyStore`] for tests and local development.
#[derive(Default)]
pub struct InMemoryWrappedKeyStore {
    blobs: DashMap<(OrgId, MasterKeyIdentifier), Vec<u8>>,
}

impl InMemoryWrappedKeyStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, org: OrgId, key_id: MasterKeyIdentifier, wrapped_key: Vec<u8>) {
        self.blobs.insert((org, key_id), wrapped_key);
    }
}

#[async_trait]
impl WrappedIntermediateKeyStore for InMemoryWrappedKeyStore {
    async fn wrapped_key(
        &self,
        org: OrgId,
        key_id: &MasterKeyIdentifier,
    ) -> Result<Option<Vec<u8>>, WrappedKeyStoreError> {
        Ok(self
            .blobs
            .get(&(org, key_id.clone()))
            .map(|entry| entry.value().clone()))
    }
}

/// Cache policy for unwrapped intermediate keys.
#[derive(Clone, Debug)]
pub struct IntermediateKeyCacheConfig {
    /// Local plaintext cache TTL ceiling; the product window is 10–15
    /// minutes. Effective TTL never exceeds this or the provider cap.
    pub ttl: Duration,
    /// Maximum fractional reduction from the effective TTL ceiling so
    /// replicas and orgs don't refresh in lockstep. `0.0` disables
    /// jitter; clamped to 50%.
    pub ttl_jitter: f64,
    /// How long a provider denial is remembered before re-asking.
    pub negative_ttl: Duration,
    /// Entry-count backstop. On overflow, expired entries are dropped, then
    /// the soonest-expiring live entries are evicted.
    pub max_entries: usize,
}

impl Default for IntermediateKeyCacheConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(900),
            ttl_jitter: 0.10,
            negative_ttl: Duration::from_secs(60),
            max_entries: 4096,
        }
    }
}

type CacheKey = (OrgId, MasterKeyIdentifier);
type IntermediateKeyCache = DashMap<CacheKey, Slot>;

/// RAII retirement of a single-flight gate. Runs on every drop — including
/// a refresh future canceled mid-flight — so gates for keys that never
/// cache (failed lookups, canceled requests) cannot accumulate.
struct GateGuard<'a> {
    map: &'a DashMap<CacheKey, Arc<tokio::sync::Mutex<()>>>,
    key: &'a CacheKey,
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl Drop for GateGuard<'_> {
    fn drop(&mut self) {
        // map(1) + ours(1) = 2 when no waiter holds a clone. ptr_eq keeps a
        // replacement gate (installed after an earlier removal) safe from
        // this retirement.
        self.map.remove_if(self.key, |_, gate| {
            Arc::ptr_eq(gate, &self.gate) && Arc::strong_count(gate) <= 2
        });
    }
}

enum Slot {
    Key {
        key: Arc<Zeroizing<[u8; 32]>>,
        expires_at: Instant,
    },
    Revoked {
        until: Instant,
    },
}

impl Slot {
    fn deadline(&self) -> Instant {
        match self {
            Slot::Key { expires_at, .. } => *expires_at,
            Slot::Revoked { until } => *until,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum ResolveError {
    #[error("intermediate-key broker revoked access to this org's key ({0}); failing closed")]
    Revoked(String),
    #[error("no wrapped intermediate key on file for this (org, key identifier)")]
    NoWrappedKey,
    #[error(transparent)]
    Store(#[from] WrappedKeyStoreError),
    #[error(transparent)]
    Provider(ProviderError),
}

/// [`KeyWrapper`] backend for [`KekProvider::IntermediateKeyBroker`]. See the
/// module docs for the trust and caching model.
pub struct IntermediateKeyWrapper {
    provider: Arc<dyn IntermediateKeyBrokerApi>,
    store: Arc<dyn WrappedIntermediateKeyStore>,
    config: IntermediateKeyCacheConfig,
    cache: Arc<IntermediateKeyCache>,
    expiry_changed: tokio::sync::watch::Sender<()>,
    expiry_task: OnceLock<tokio::task::JoinHandle<()>>,
    /// Per-key single-flight gates: concurrent misses coalesce into one
    /// provider round-trip.
    refresh: DashMap<CacheKey, Arc<tokio::sync::Mutex<()>>>,
}

impl IntermediateKeyWrapper {
    pub fn new(
        provider: Arc<dyn IntermediateKeyBrokerApi>,
        store: Arc<dyn WrappedIntermediateKeyStore>,
        config: IntermediateKeyCacheConfig,
    ) -> Self {
        let (expiry_changed, _) = tokio::sync::watch::channel(());
        Self {
            provider,
            store,
            config,
            cache: Arc::new(DashMap::new()),
            expiry_changed,
            expiry_task: OnceLock::new(),
            refresh: DashMap::new(),
        }
    }

    /// Start the resource-owned expiry task on first use, when construction
    /// is guaranteed to be inside a Tokio runtime. The task holds only a weak
    /// cache reference and stops when this wrapper is dropped.
    fn ensure_expiry_task(&self) {
        self.expiry_task.get_or_init(|| {
            tokio::spawn(
                ExpiryTask::new(Arc::downgrade(&self.cache), self.expiry_changed.subscribe()).run(),
            )
        });
    }

    /// Fast-path cache read. A fresh revocation fails closed immediately;
    /// an expired slot reads as a miss and is dropped eagerly — an expired
    /// `Slot::Key` still owns plaintext key material, which must not stay
    /// resident waiting for a later insert sweep.
    fn cached(&self, key: &CacheKey) -> Result<Option<Arc<Zeroizing<[u8; 32]>>>, ResolveError> {
        let now = Instant::now();
        {
            // Scoped so the shard read guard is released before remove_if
            // below needs the write lock.
            let Some(slot) = self.cache.get(key) else {
                return Ok(None);
            };
            match slot.value() {
                Slot::Key { key, expires_at } if *expires_at > now => {
                    return Ok(Some(key.clone()));
                }
                Slot::Revoked { until } if *until > now => {
                    return Err(ResolveError::Revoked("negative-cached".to_string()));
                }
                _ => {}
            }
        }
        // The deadline re-check keeps a concurrent refresh's fresh slot safe.
        self.cache.remove_if(key, |_, slot| slot.deadline() <= now);
        Ok(None)
    }

    fn effective_ttl(&self, provider_cap: Duration) -> Duration {
        self.effective_ttl_for_jitter_sample(provider_cap, rand::random::<f64>())
    }

    fn effective_ttl_for_jitter_sample(
        &self,
        provider_cap: Duration,
        jitter_sample: f64,
    ) -> Duration {
        debug_assert!((0.0..=1.0).contains(&jitter_sample));
        let ceiling = self.config.ttl.min(provider_cap);
        let jitter = if self.config.ttl_jitter > 0.0 {
            self.config.ttl_jitter.min(0.5)
        } else {
            0.0
        };
        ceiling.mul_f64(1.0 - jitter * jitter_sample)
    }

    /// Resolve the org's plaintext intermediate key: cache hit, or a
    /// single-flighted store read + provider unwrap.
    async fn intermediate_key(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        key_id: &MasterKeyIdentifier,
    ) -> Result<Arc<Zeroizing<[u8; 32]>>, ResolveError> {
        self.ensure_expiry_task();
        let cache_key = (org, key_id.clone());
        if let Some(key) = self.cached(&cache_key)? {
            return Ok(key);
        }

        let gate = GateGuard {
            gate: self
                .refresh
                .entry(cache_key.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone(),
            map: &self.refresh,
            key: &cache_key,
        };
        let _refreshing = gate.gate.lock().await;
        // Concurrent misses coalesce: the gate winner's request context is
        // presented to the broker and all waiters share the cached result.
        self.refresh_locked(&cache_key, org, requesting_sandbox, key_id)
            .await
        // On every exit — including cancellation at the lock, the store
        // read, or the provider RPC — the lock guard releases first, then
        // `gate` retires the map entry if no other task holds it.
    }

    /// The single-flighted refresh body; the caller holds this key's gate.
    async fn refresh_locked(
        &self,
        cache_key: &CacheKey,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        key_id: &MasterKeyIdentifier,
    ) -> Result<Arc<Zeroizing<[u8; 32]>>, ResolveError> {
        // Re-check under the gate: a concurrent holder may have refreshed.
        if let Some(key) = self.cached(cache_key)? {
            return Ok(key);
        }

        let wrapped = self
            .store
            .wrapped_key(org, key_id)
            .await?
            .ok_or(ResolveError::NoWrappedKey)?;
        match self
            .provider
            .unwrap_intermediate_key(org, requesting_sandbox, key_id, &wrapped)
            .await
        {
            Ok(unwrapped) => {
                let ttl = self.effective_ttl(unwrapped.max_cache_ttl);
                let key = Arc::new(unwrapped.key);
                tracing::debug!(org = %org.as_uuid(), key_id = %key_id, ttl_secs = ttl.as_secs(), "refreshed intermediate key");
                self.cache.insert(
                    cache_key.clone(),
                    Slot::Key {
                        key: key.clone(),
                        expires_at: Instant::now() + ttl,
                    },
                );
                self.sweep_cache();
                self.notify_expiry_change();
                Ok(key)
            }
            Err(ProviderError::AccessRevoked(detail)) => {
                tracing::warn!(
                    org = %org.as_uuid(),
                    key_id = %key_id,
                    detail = %detail,
                    "intermediate-key broker revoked access; failing closed"
                );
                self.cache.insert(
                    cache_key.clone(),
                    Slot::Revoked {
                        until: Instant::now() + self.config.negative_ttl,
                    },
                );
                self.sweep_cache();
                self.notify_expiry_change();
                Err(ResolveError::Revoked(detail))
            }
            Err(error) => Err(ResolveError::Provider(error)),
        }
    }

    /// Post-insert sweep. Expired slots are dropped on every insert — not
    /// only on overflow — because an expired `Slot::Key` still owns
    /// plaintext key material. The entry-count cap is a backstop:
    /// cardinality is orgs × live key versions, so overflow implies a bug
    /// or an abusive access pattern, and evicting a live entry only costs
    /// its holder an extra provider round-trip.
    fn sweep_cache(&self) {
        let now = Instant::now();
        self.cache.retain(|_, slot| slot.deadline() > now);
        while self.cache.len() > self.config.max_entries {
            let Some(victim) = self
                .cache
                .iter()
                .min_by_key(|entry| entry.value().deadline())
                .map(|entry| entry.key().clone())
            else {
                break;
            };
            self.cache.remove(&victim);
        }
    }

    fn notify_expiry_change(&self) {
        if self.expiry_changed.send(()).is_err() {
            // The reaper should only stop after this wrapper is dropped. If
            // it exits unexpectedly, fail secure instead of retaining
            // plaintext without an active expiry owner.
            tracing::error!("intermediate-key expiry task stopped unexpectedly; clearing cache");
            self.cache.clear();
        }
    }
}

impl Drop for IntermediateKeyWrapper {
    fn drop(&mut self) {
        if let Some(task) = self.expiry_task.get() {
            task.abort();
        }
    }
}

/// Own the expiry receiver and cache cleanup together. On cancellation or
/// panic, closing the receiver before clearing the cache ensures a concurrent
/// insertion either precedes the sweep or observes the closed channel and
/// clears itself.
struct ExpiryTask {
    cache: Weak<IntermediateKeyCache>,
    expiry_changed: Option<tokio::sync::watch::Receiver<()>>,
}

impl ExpiryTask {
    fn new(
        cache: Weak<IntermediateKeyCache>,
        expiry_changed: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        Self {
            cache,
            expiry_changed: Some(expiry_changed),
        }
    }

    fn changes(&mut self) -> &mut tokio::sync::watch::Receiver<()> {
        self.expiry_changed
            .as_mut()
            .expect("expiry receiver remains installed while the task runs")
    }

    /// Remove expired slots even when no later request observes them. Cache
    /// changes wake the task so a newly inserted earlier deadline immediately
    /// replaces its current timer.
    async fn run(mut self) {
        loop {
            let Some(cache) = self.cache.upgrade() else {
                return;
            };
            let now = Instant::now();
            let cached = cache.len();
            cache.retain(|_, slot| slot.deadline() > now);
            let expired = cached.saturating_sub(cache.len());
            // A span only for a pass that evicted something; the task wakes on
            // every cache change.
            if expired > 0 {
                let span = tracing::info_span!(
                    parent: None,
                    "credmgr.intermediate_key_cache.expire",
                    otel.kind = "internal",
                    "operation.name" = "credmgr.intermediate_key_cache.expire",
                    expired,
                    remaining = cache.len(),
                );
                drop(span);
            }
            let next_deadline = cache.iter().map(|entry| entry.value().deadline()).min();
            drop(cache);

            match next_deadline {
                Some(deadline) => {
                    tokio::select! {
                        biased;
                        changed = self.changes().changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        _ = tokio::time::sleep_until(deadline) => {}
                    }
                }
                None => {
                    if self.changes().changed().await.is_err() {
                        return;
                    }
                }
            }
        }
    }
}

impl Drop for ExpiryTask {
    fn drop(&mut self) {
        drop(self.expiry_changed.take());
        if let Some(cache) = self.cache.upgrade() {
            cache.clear();
        }
    }
}

fn wrap_resolve_error(error: ResolveError) -> WrapError {
    // A contended read of the wrapped-intermediate-key table is retryable, and
    // `Unavailable` is the code this enum already reserves for that. Without
    // this arm it falls to the `Backend` catch-all below, which the service
    // returns as gRPC `Internal` — permanent, on a statement whose retry
    // succeeds. `space_creds` carries a role-level `lock_timeout`, so a DDL
    // lock on that table would otherwise become a fast permanent-looking
    // credential failure instead of the slow success it used to be.
    if let ResolveError::Store(WrappedKeyStoreError::Lookup(sql)) = &error
        && pplx_space_common_svc::sql::contention::is_lock_contention(sql)
    {
        return WrapError::Unavailable(error.to_string());
    }
    match error {
        ResolveError::Revoked(_) => WrapError::AccessRevoked,
        ResolveError::Provider(error @ ProviderError::InvalidKey(_)) => {
            WrapError::InvalidKey(error.to_string())
        }
        ResolveError::Provider(ProviderError::AuthorizationRejected(msg)) => {
            WrapError::AuthorizationRejected(msg)
        }
        ResolveError::Provider(ProviderError::Unavailable(msg)) => WrapError::Unavailable(msg),
        other => WrapError::Backend(other.to_string()),
    }
}

fn unwrap_resolve_error(error: ResolveError) -> UnwrapError {
    // As in `wrap_resolve_error`: contention is retryable, not a backend fault.
    if let ResolveError::Store(WrappedKeyStoreError::Lookup(sql)) = &error
        && pplx_space_common_svc::sql::contention::is_lock_contention(sql)
    {
        return UnwrapError::Unavailable(error.to_string());
    }
    match error {
        ResolveError::Revoked(_) => UnwrapError::AccessRevoked,
        ResolveError::NoWrappedKey => UnwrapError::UnknownOrgKek,
        ResolveError::Provider(error @ ProviderError::InvalidKey(_)) => {
            UnwrapError::InvalidKey(error.to_string())
        }
        ResolveError::Provider(ProviderError::AuthorizationRejected(msg)) => {
            UnwrapError::AuthorizationRejected(msg)
        }
        ResolveError::Provider(ProviderError::Unavailable(msg)) => UnwrapError::Unavailable(msg),
        other => UnwrapError::Backend(other.to_string()),
    }
}

fn aes256(key: &Zeroizing<[u8; 32]>) -> Aes256Gcm {
    Aes256Gcm::new_from_slice(&key[..]).expect("AES-256-GCM accepts a 32-byte key")
}

#[async_trait]
impl KeyWrapper for IntermediateKeyWrapper {
    async fn generate_and_wrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        spec: DekKeySpec,
        aad: &[u8],
    ) -> Result<GenerateDekOutput, WrapError> {
        if provider != KekProvider::IntermediateKeyBroker {
            return Err(WrapError::UnsupportedProvider(provider));
        }
        let intermediate_key = self
            .intermediate_key(org, requesting_sandbox, master_key_identifier)
            .await
            .map_err(wrap_resolve_error)?;

        // CSRNG DEK. `rand::fill` is the workspace-standard CSPRNG draw.
        let mut plaintext_dek = vec![0u8; spec.byte_len()];
        rand::fill(&mut plaintext_dek[..]);

        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::fill(&mut nonce_bytes[..]);
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = aes256(&intermediate_key)
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext_dek,
                    aad,
                },
            )
            .map_err(|e| WrapError::Backend(format!("aes-gcm encrypt: {e}")))?;

        // Wire format: version(1) || nonce(12) || ciphertext_with_tag.
        let mut wrapped_dek = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        wrapped_dek.push(INTERMEDIATE_KEY_WRAP_VERSION);
        wrapped_dek.extend_from_slice(&nonce_bytes);
        wrapped_dek.extend_from_slice(&ciphertext);

        Ok(GenerateDekOutput {
            plaintext_dek,
            wrapped_dek,
        })
    }

    async fn unwrap_dek(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        provider: KekProvider,
        master_key_identifier: &MasterKeyIdentifier,
        wrapped_dek: &[u8],
        aad: &[u8],
    ) -> Result<Vec<u8>, UnwrapError> {
        if provider != KekProvider::IntermediateKeyBroker {
            return Err(UnwrapError::UnsupportedProvider(provider));
        }
        // Validate framing before spending a provider round-trip on garbage.
        let Some((&version, rest)) = wrapped_dek.split_first() else {
            return Err(UnwrapError::Malformed);
        };
        if version != INTERMEDIATE_KEY_WRAP_VERSION || rest.len() < NONCE_LEN + TAG_LEN {
            return Err(UnwrapError::Malformed);
        }

        let intermediate_key = self
            .intermediate_key(org, requesting_sandbox, master_key_identifier)
            .await
            .map_err(unwrap_resolve_error)?;

        let (nonce_bytes, ciphertext) = rest.split_at(NONCE_LEN);
        let nonce_bytes: [u8; NONCE_LEN] =
            nonce_bytes.try_into().map_err(|_| UnwrapError::Malformed)?;
        let nonce = Nonce::from(nonce_bytes);
        aes256(&intermediate_key)
            .decrypt(
                &nonce,
                Payload {
                    msg: ciphertext,
                    aad,
                },
            )
            .map_err(|_| UnwrapError::AuthFailed)
    }
}

/// End-to-end deadline for identity resolution plus one provider RPC. Unwrap
/// callers for the same org queue behind the single-flight gate, so a hung
/// database lookup or network call must not wedge that org's key resolution;
/// a timeout maps to
/// [`ProviderError::Unavailable`] and is retried on the next operation.
const PROVIDER_OPERATION_DEADLINE: Duration = Duration::from_secs(10);

/// [`IntermediateKeyBrokerApi`] over the provider-neutral v2 gRPC surface,
/// using the configured tenant namespace and authorization scheme.
pub struct GrpcIntermediateKeyBroker {
    protocol: BrokerProtocolConfig,
    client: IntermediateKeyBrokerGrpcClient,
    identity: Arc<dyn IntermediateKeyIdentityLookup>,
}

impl GrpcIntermediateKeyBroker {
    /// Build over an established channel. Endpoint and TLS wiring are the
    /// binary's concern; every request carries the bearer credential read
    /// from `token_file` (network reachability is not the authorization
    /// boundary), and the operation deadline ([`PROVIDER_OPERATION_DEADLINE`])
    /// is enforced here regardless of how the channel was configured.
    ///
    /// `identity` resolves what each request claims about its org. It lives
    /// here rather than in the callers because it is a property of this wire
    /// protocol — the broker's tenant reference and authorization evidence —
    /// not of the abstract port, and because the cache-refresh path has no
    /// request context to carry it. Identity lookup and the network request
    /// share the end-to-end [`PROVIDER_OPERATION_DEADLINE`].
    pub fn from_channel(
        channel: tonic::transport::Channel,
        protocol: BrokerProtocolConfig,
        token_file: impl Into<std::path::PathBuf>,
        identity: Arc<dyn IntermediateKeyIdentityLookup>,
    ) -> Self {
        Self {
            protocol,
            client: traced_intermediate_key_broker_client(
                channel,
                BearerTokenFileInterceptor::new(token_file),
            ),
            identity,
        }
    }

    /// Fail the RPC rather than fall back to the SPACE uuid: a federated org
    /// whose identity cannot be read would otherwise get a key under a scope
    /// its grant can never name, and that key would then be the one persisted.
    async fn wire_identity(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<IntermediateKeyIdentity, ProviderError> {
        match requesting_sandbox {
            Some(sandbox) => self.identity.identity_for_sandbox(org, sandbox).await,
            None => self.identity.identity(org).await,
        }
        .map_err(|error| match error {
            crate::intermediate_key_identity::IdentityLookupError::Unavailable { .. } => {
                ProviderError::Unavailable(error.to_string())
            }
            crate::intermediate_key_identity::IdentityLookupError::Invalid { .. } => {
                ProviderError::InvalidTenant(error.to_string())
            }
        })
    }
}

fn provider_deadline_exceeded(operation: &str) -> ProviderError {
    ProviderError::Unavailable(format!(
        "{operation} deadline ({}s) exceeded",
        PROVIDER_OPERATION_DEADLINE.as_secs()
    ))
}

#[derive(Clone, Copy)]
enum BrokerFailureClass {
    Invalid,
    AuthorizationRejected,
    AccessRevoked,
    Unavailable,
    Unrecognized(i32),
}

impl From<IssueFailureReason> for BrokerFailureClass {
    fn from(reason: IssueFailureReason) -> Self {
        match reason {
            IssueFailureReason::InvalidTenant => Self::Invalid,
            IssueFailureReason::AuthorizationRejected => Self::AuthorizationRejected,
            IssueFailureReason::AccessRevoked => Self::AccessRevoked,
            IssueFailureReason::Unavailable => Self::Unavailable,
            IssueFailureReason::Unspecified => Self::Unrecognized(reason as i32),
        }
    }
}

impl From<UnwrapFailureReason> for BrokerFailureClass {
    fn from(reason: UnwrapFailureReason) -> Self {
        match reason {
            UnwrapFailureReason::InvalidKey => Self::Invalid,
            UnwrapFailureReason::AuthorizationRejected => Self::AuthorizationRejected,
            UnwrapFailureReason::AccessRevoked => Self::AccessRevoked,
            UnwrapFailureReason::Unavailable => Self::Unavailable,
            UnwrapFailureReason::Unspecified => Self::Unrecognized(reason as i32),
        }
    }
}

fn issue_failure_class(reason: i32) -> BrokerFailureClass {
    IssueFailureReason::try_from(reason)
        .map(BrokerFailureClass::from)
        .unwrap_or(BrokerFailureClass::Unrecognized(reason))
}

fn unwrap_failure_class(reason: i32) -> BrokerFailureClass {
    UnwrapFailureReason::try_from(reason)
        .map(BrokerFailureClass::from)
        .unwrap_or(BrokerFailureClass::Unrecognized(reason))
}

/// Issue and unwrap share one provider-failure taxonomy after their generated
/// wire enums have been parsed and matched exhaustively. Only their typed
/// invalid cases differ (`INVALID_TENANT` versus `INVALID_KEY`).
fn map_broker_failure(
    reason: BrokerFailureClass,
    detail: String,
    invalid: fn(String) -> ProviderError,
) -> ProviderError {
    match reason {
        BrokerFailureClass::Invalid => invalid(detail),
        BrokerFailureClass::AuthorizationRejected => ProviderError::AuthorizationRejected(detail),
        BrokerFailureClass::AccessRevoked => ProviderError::AccessRevoked(detail),
        BrokerFailureClass::Unavailable => ProviderError::Unavailable(detail),
        BrokerFailureClass::Unrecognized(reason) => {
            ProviderError::Malformed(format!("unrecognized failure reason {reason}: {detail}"))
        }
    }
}

fn wire_tenant(protocol: &BrokerProtocolConfig, identity: &IntermediateKeyIdentity) -> TenantRef {
    TenantRef {
        namespace: protocol.tenant_namespace.clone(),
        id: identity.wire_org_id.clone(),
    }
}

fn wire_authorization(
    protocol: &BrokerProtocolConfig,
    identity: &IntermediateKeyIdentity,
) -> Option<AuthorizationEvidence> {
    identity
        .grant
        .as_ref()
        .map(|presented| AuthorizationEvidence {
            scheme: protocol.grant_scheme.clone(),
            subject: Some(presented.user_id.clone()),
            evidence: presented.grant.as_bytes().to_vec(),
        })
}

/// Apply SPACE's independently stored evidence expiry after the broker call.
/// The broker cap is authoritative even when SPACE has no expiry metadata;
/// when it does, consuming RPC/network time may only shorten plaintext
/// residency, never extend it.
fn cap_cache_ttl_by_evidence(
    broker_cap: Duration,
    evidence_expires_at: Option<jiff::Timestamp>,
    now: jiff::Timestamp,
) -> Result<Duration, ProviderError> {
    let Some(expires_at) = evidence_expires_at else {
        return Ok(broker_cap);
    };
    let remaining = Duration::try_from(expires_at.duration_since(now)).map_err(|_| {
        ProviderError::AuthorizationRejected(
            "authorization evidence expired during key release".to_string(),
        )
    })?;
    if remaining.is_zero() {
        return Err(ProviderError::AuthorizationRejected(
            "authorization evidence expired during key release".to_string(),
        ));
    }
    Ok(broker_cap.min(remaining))
}

#[async_trait]
impl IntermediateKeyBrokerApi for GrpcIntermediateKeyBroker {
    async fn issue_intermediate_key(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
    ) -> Result<IssuedIntermediateKey, ProviderError> {
        let deadline = Instant::now() + PROVIDER_OPERATION_DEADLINE;
        let identity =
            tokio::time::timeout_at(deadline, self.wire_identity(org, requesting_sandbox))
                .await
                .map_err(|_| provider_deadline_exceeded("issue"))??;
        let mut client = self.client.clone();
        let tenant = wire_tenant(&self.protocol, &identity);
        let authorization = wire_authorization(&self.protocol, &identity);
        let call = client.issue_intermediate_key(IssueIntermediateKeyRequest {
            tenant: Some(tenant),
            authorization,
        });
        let response = tokio::time::timeout_at(deadline, call)
            .await
            .map_err(|_| provider_deadline_exceeded("issue"))?
            // Statuses are transport-level only; see unwrap below.
            .map_err(|status| ProviderError::Unavailable(status.to_string()))?
            .into_inner();

        match response.outcome {
            Some(IssueOutcome::Key(issued)) => {
                let key_id = MasterKeyIdentifier::parse(issued.key_id).map_err(|e| {
                    ProviderError::Malformed(format!("issued key_id is invalid: {e}"))
                })?;
                if issued.wrapped_key.is_empty() {
                    return Err(ProviderError::Malformed(
                        "issued wrapped_key is empty".to_string(),
                    ));
                }
                Ok(IssuedIntermediateKey {
                    key_id,
                    wrapped_key: issued.wrapped_key,
                })
            }
            Some(IssueOutcome::Failure(failure)) => Err(map_broker_failure(
                issue_failure_class(failure.reason),
                failure.detail,
                ProviderError::InvalidTenant,
            )),
            None => Err(ProviderError::Malformed(
                "response carried no outcome".to_string(),
            )),
        }
    }

    async fn unwrap_intermediate_key(
        &self,
        org: OrgId,
        requesting_sandbox: Option<SandboxId>,
        key_id: &MasterKeyIdentifier,
        wrapped_key: &[u8],
    ) -> Result<UnwrappedIntermediateKey, ProviderError> {
        let deadline = Instant::now() + PROVIDER_OPERATION_DEADLINE;
        let identity =
            tokio::time::timeout_at(deadline, self.wire_identity(org, requesting_sandbox))
                .await
                .map_err(|_| provider_deadline_exceeded("unwrap"))??;
        let mut client = self.client.clone();
        let tenant = wire_tenant(&self.protocol, &identity);
        let authorization = wire_authorization(&self.protocol, &identity);
        let evidence_expires_at = identity.grant.as_ref().map(|grant| grant.expires_at);
        let call = client.unwrap_intermediate_key(UnwrapIntermediateKeyRequest {
            tenant: Some(tenant),
            key_id: key_id.as_str().to_owned(),
            wrapped_key: wrapped_key.to_vec(),
            authorization,
        });
        let response = tokio::time::timeout_at(deadline, call)
            .await
            .map_err(|_| provider_deadline_exceeded("unwrap"))?
            // By contract, gRPC statuses are transport-level only, so every
            // non-OK status is retriable unavailability. An auth-middleware
            // or proxy failure between the services must never read as an
            // application outcome — in particular never as a revocation,
            // which triggers cleanup and sandbox shutdown.
            .map_err(|status| ProviderError::Unavailable(status.to_string()))?
            .into_inner();

        let material = match response.outcome {
            Some(UnwrapOutcome::Key(material)) => material,
            Some(UnwrapOutcome::Failure(failure)) => {
                return Err(map_broker_failure(
                    unwrap_failure_class(failure.reason),
                    failure.detail,
                    ProviderError::InvalidKey,
                ));
            }
            None => {
                return Err(ProviderError::Malformed(
                    "response carried no outcome".to_string(),
                ));
            }
        };

        let mut plaintext = material.plaintext_key;
        let key: [u8; 32] = match plaintext.as_slice().try_into() {
            Ok(key) => key,
            Err(_) => {
                let len = plaintext.len();
                plaintext.zeroize();
                return Err(ProviderError::Malformed(format!(
                    "expected a 32-byte key, got {len} bytes"
                )));
            }
        };
        plaintext.zeroize();
        let key = Zeroizing::new(key);

        // Protobuf boundary: parse the cap through jiff per the repo time
        // convention, converting to std only for the tokio-clock cache math.
        // Message-field presence is explicit — a present zero cap means "do
        // not cache" and is preserved; negative or sign-inconsistent
        // durations (and nanos beyond the protobuf spec bound, which would
        // otherwise carry into seconds) are contract violations.
        let broker_cache_ttl = match material.max_cache_ttl {
            None => {
                return Err(ProviderError::Malformed(
                    "response omitted required max_cache_ttl".to_string(),
                ));
            }
            Some(d) if d.seconds < 0 || d.nanos < 0 || d.nanos > 999_999_999 => {
                return Err(ProviderError::Malformed(format!(
                    "invalid max_cache_ttl ({}s, {}ns)",
                    d.seconds, d.nanos
                )));
            }
            Some(d) => {
                let cap = jiff::SignedDuration::new(d.seconds, d.nanos);
                Duration::try_from(cap).map_err(|e| {
                    ProviderError::Malformed(format!("max_cache_ttl out of range: {e}"))
                })?
            }
        };
        let max_cache_ttl = cap_cache_ttl_by_evidence(
            broker_cache_ttl,
            evidence_expires_at,
            jiff::Timestamp::now(),
        )?;

        Ok(UnwrappedIntermediateKey { key, max_cache_ttl })
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod grpc_tests;
