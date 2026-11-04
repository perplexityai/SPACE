use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use super::timing::ADMISSION_RESERVE;
use jiff::Timestamp;
use lru::LruCache;
use pplx_space_net_common::DomainName;
use tokio::sync::{OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;
use tonic::Status;

use super::authority::{AuthorityLease, GatewayAuthority};
use super::material::{CertificateLease, Fence, VerifiedMaterial};
use super::{CERTIFICATE_REQUEST_TIMEOUT, CertificateRequester};
use crate::state::GatewayNetworkState;

const MAX_REQUESTS_PER_SECOND: usize = 256;
const MAX_CONCURRENT_REQUESTS: usize = 4;
const MAX_CACHED_CERTIFICATES: usize = 4096;

#[derive(Clone)]
pub struct CertificateContext {
    network: GatewayNetworkState,
    shutdown: CancellationToken,
}

impl std::fmt::Debug for CertificateContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateContext").finish_non_exhaustive()
    }
}

impl CertificateContext {
    pub fn new(network: GatewayNetworkState, shutdown: CancellationToken) -> Self {
        Self { network, shutdown }
    }
}

#[derive(Clone)]
pub struct CertificateProvider {
    inner: Arc<Provider>,
}

impl std::fmt::Debug for CertificateProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertificateProvider")
            .finish_non_exhaustive()
    }
}

struct Provider {
    authority: GatewayAuthority,
    capacity: Arc<Semaphore>,
    rate: Mutex<(tokio::time::Instant, usize)>,
    context: CertificateContext,
    cache: Mutex<Cache>,
}

struct Cache {
    ready: LruCache<(DomainName, u16), Arc<Entry>>,
    // Pending misses never displace usable leaves. Weak references allow the
    // last cancelled waiter to release its entry without a background task.
    pending: HashMap<(DomainName, u16), Weak<Entry>>,
}

struct Entry {
    fence: Fence,
    value: OnceCell<Arc<CertificateLease>>,
    authority: Arc<AuthorityLease>,
}

impl Entry {
    fn current(&self) -> bool {
        self.fence.is_current() && self.value.get().is_none_or(|v| admissible(v))
    }
}

impl CertificateProvider {
    pub fn new(authority: GatewayAuthority, context: CertificateContext) -> Self {
        Self {
            inner: Arc::new(Provider {
                authority,
                context,
                capacity: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
                rate: Mutex::new((tokio::time::Instant::now(), 0)),
                cache: Mutex::new(Cache {
                    ready: LruCache::new(
                        NonZeroUsize::new(MAX_CACHED_CERTIFICATES).expect("nonzero cache bound"),
                    ),
                    pending: HashMap::new(),
                }),
            }),
        }
    }

    pub fn requester(&self) -> CertificateRequester {
        let provider = self.clone();
        Arc::new(move |host, port| {
            let provider = provider.clone();
            Box::pin(async move { provider.get(host, port).await })
        })
    }

    /// Bounds the entire lookup, key generation and signing operation to two
    /// seconds. Concurrent same-target calls share a cancellation-safe OnceCell.
    pub async fn get(&self, host: DomainName, port: u16) -> Result<Arc<CertificateLease>, Status> {
        tokio::time::timeout(CERTIFICATE_REQUEST_TIMEOUT, self.get_inner(host, port))
            .await
            .map_err(|_| {
                lookup_failed(
                    "deadline_exceeded",
                    Status::deadline_exceeded("certificate acquisition deadline"),
                )
            })?
    }

    /// Removes stale material on each lookup; callers can also wire this into
    /// their periodic cache sweep to release keys while the gateway is idle.
    pub fn prune(&self) -> Result<(), Status> {
        let mut cache = self
            .inner
            .cache
            .lock()
            .map_err(|_| Status::internal("certificate cache poisoned"))?;
        let stale: Vec<_> = cache
            .ready
            .iter()
            .filter(|(_, e)| !e.current())
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            cache.ready.pop(&key);
            pplx_space_metrics::gateway::certificate_cache_event("pruned_ready");
        }
        let pending_before = cache.pending.len();
        cache
            .pending
            .retain(|_, entry| entry.upgrade().is_some_and(|entry| entry.current()));
        for _ in cache.pending.len()..pending_before {
            pplx_space_metrics::gateway::certificate_cache_event("pruned_pending");
        }
        pplx_space_metrics::gateway::certificate_cache_entries(
            cache.ready.len(),
            MAX_CACHED_CERTIFICATES,
        );
        Ok(())
    }

    async fn get_inner(
        &self,
        host: DomainName,
        port: u16,
    ) -> Result<Arc<CertificateLease>, Status> {
        self.prune()
            .map_err(|status| lookup_failed("cache_poisoned", status))?;
        if port == 0 {
            return Err(lookup_failed(
                "invalid_port",
                Status::invalid_argument("certificate port is zero"),
            ));
        }
        let context = &self.inner.context;
        let snapshot = context.network.snapshot().await;
        if snapshot.mode != crate::proto::NetworkState::Ready || context.shutdown.is_cancelled() {
            return Err(lookup_failed(
                "policy_not_ready",
                Status::failed_precondition("certificate request requires ready policy"),
            ));
        }
        let policy = snapshot.policy.ok_or_else(|| {
            lookup_failed(
                "policy_missing",
                Status::failed_precondition("missing policy"),
            )
        })?;
        let authority = self
            .inner
            .authority
            .current()
            .map_err(|status| lookup_failed("authority_unavailable", status))?;
        let key = (host.clone(), port);
        // `source` is the terminal `result` label for a lookup that succeeds.
        // Failures below record their own bounded cause instead, so every
        // lookup contributes exactly one sample.
        let (entry, source) = {
            let mut cache = self.inner.cache.lock().map_err(|_| {
                lookup_failed(
                    "cache_poisoned",
                    Status::internal("certificate cache poisoned"),
                )
            })?;
            if cache.ready.peek(&key).is_some_and(|e| !e.current()) {
                cache.ready.pop(&key);
                pplx_space_metrics::gateway::certificate_cache_event("stale_pop");
            }
            if let Some(entry) = cache.ready.get(&key) {
                (entry.clone(), "ready_hit")
            } else if let Some(entry) = cache
                .pending
                .get(&key)
                .and_then(Weak::upgrade)
                .filter(|entry| entry.current())
            {
                (entry, "pending_join")
            } else {
                cache
                    .pending
                    .retain(|_, entry| entry.upgrade().is_some_and(|entry| entry.current()));
                if cache.pending.len() >= MAX_CONCURRENT_REQUESTS {
                    return Err(lookup_failed(
                        "pending_full",
                        Status::resource_exhausted("certificate cache is busy"),
                    ));
                }
                let entry = Arc::new(Entry {
                    fence: Fence {
                        state: context.network.clone(),
                        policy,
                        identity: authority.execution_token(),
                        shutdown: context.shutdown.clone(),
                        authority: authority.retired(),
                    },
                    value: OnceCell::new(),
                    authority,
                });
                cache.pending.insert(key.clone(), Arc::downgrade(&entry));
                (entry, "signed")
            }
        };
        let certificate = tokio::select! {
            biased;
            () = entry.fence.identity.cancelled() => return Err(lookup_failed("identity_changed", Status::aborted("certificate execution changed"))),
            () = entry.fence.shutdown.cancelled() => return Err(lookup_failed("shutdown", Status::cancelled("gateway stopped"))),
            result = entry.value.get_or_try_init(|| self.acquire(host, port, &entry)) => result?.clone(),
        };
        if !admissible(&certificate) {
            return Err(lookup_failed(
                "not_admissible",
                Status::aborted("certificate became unusable or remaining lifetime is too short"),
            ));
        }
        let mut cache = self.inner.cache.lock().map_err(|_| {
            lookup_failed(
                "cache_poisoned",
                Status::internal("certificate cache poisoned"),
            )
        })?;
        if cache
            .pending
            .get(&key)
            .is_some_and(|pending| pending.ptr_eq(&Arc::downgrade(&entry)))
        {
            cache.pending.remove(&key);
        }
        // LRU eviction happens only after a successful, still-current signing
        // operation. Overload and failed initializations leave warm hits intact.
        if entry.current() {
            // LruCache::put reports a replaced value for the same key, never the
            // entry displaced by the capacity bound. Detect that displacement
            // before inserting so cache pressure stays distinguishable from churn.
            if cache.ready.len() == MAX_CACHED_CERTIFICATES && !cache.ready.contains(&key) {
                pplx_space_metrics::gateway::certificate_cache_event("evicted");
            }
            cache.ready.put(key, entry);
        }
        pplx_space_metrics::gateway::certificate_cache_entries(
            cache.ready.len(),
            MAX_CACHED_CERTIFICATES,
        );
        drop(cache);
        pplx_space_metrics::gateway::certificate_lookup(source);
        Ok(certificate)
    }

    async fn acquire(
        &self,
        host: DomainName,
        port: u16,
        entry: &Entry,
    ) -> Result<Arc<CertificateLease>, Status> {
        let provider = &self.inner;
        let permit = provider.capacity.clone().try_acquire_owned().map_err(|_| {
            lookup_failed(
                "capacity_exhausted",
                Status::resource_exhausted("certificate request capacity exceeded"),
            )
        })?;
        {
            let mut rate = provider.rate.lock().map_err(|_| {
                lookup_failed(
                    "rate_budget_poisoned",
                    Status::internal("certificate rate budget poisoned"),
                )
            })?;
            let now = tokio::time::Instant::now();
            if now.duration_since(rate.0) >= Duration::from_secs(1) {
                *rate = (now, 0);
            }
            if rate.1 >= MAX_REQUESTS_PER_SECOND {
                return Err(lookup_failed(
                    "rate_limited",
                    Status::resource_exhausted("certificate request rate exceeded"),
                ));
            }
            rate.1 += 1;
        }
        let authority = entry.authority.clone();
        let fence = entry.fence.clone();
        // The permit stays with blocking work if cancellation drops the waiter.
        let mut signing = SigningTimer::started();
        let signed = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
                .map_err(|_| Status::internal("leaf key generation failed"))?;
            let signed = authority.sign_leaf(&host, &key)?;
            CertificateLease::new(
                VerifiedMaterial {
                    host,
                    port,
                    key: key.into(),
                    leaf: signed.der,
                    issuer: authority.certificate.clone(),
                    node_issuer: authority.node_certificate.clone(),
                    not_before: signed.not_before,
                    not_after: signed.not_after,
                },
                fence,
            )
            .map(Arc::new)
            .map_err(|_| Status::aborted("local leaf is no longer usable"))
        })
        .await;
        match signed {
            Ok(Ok(lease)) => {
                signing.finish("success");
                Ok(lease)
            }
            Ok(Err(status)) => {
                signing.finish("error");
                Err(lookup_failed("signing_error", status))
            }
            Err(_) => {
                signing.finish("worker_failed");
                Err(lookup_failed(
                    "signing_worker_failed",
                    Status::internal("local leaf signing worker failed"),
                ))
            }
        }
    }
}

/// Emits a signing duration even when the waiter is dropped.
///
/// The blocking worker keeps running after cancellation, so a dropped waiter
/// would otherwise contribute no sample at all — losing exactly the slow tail
/// this measurement exists to expose. A `cancelled` sample records how long the
/// caller waited before giving up, not how long signing ultimately took.
struct SigningTimer {
    started_at: std::time::Instant,
    finished: bool,
}

impl SigningTimer {
    fn started() -> Self {
        Self {
            started_at: std::time::Instant::now(),
            finished: false,
        }
    }

    fn finish(&mut self, result: &str) {
        self.finished = true;
        self.record(result);
    }

    fn record(&self, result: &str) {
        pplx_space_metrics::gateway::certificate_signing_duration_ms(
            result,
            self.started_at.elapsed().as_secs_f64() * 1_000.0,
        );
    }
}

impl Drop for SigningTimer {
    fn drop(&mut self) {
        if !self.finished {
            self.record("cancelled");
        }
    }
}

/// Terminal accounting for one refused lookup. Each refusal names its own
/// bounded cause here because the returned gRPC code cannot separate a CA
/// outage from the local concurrency, rate and cache bounds that share
/// `RESOURCE_EXHAUSTED`.
fn lookup_failed(result: &'static str, status: Status) -> Status {
    pplx_space_metrics::gateway::certificate_lookup(result);
    status
}

fn admissible(certificate: &CertificateLease) -> bool {
    Timestamp::now()
        .checked_add(ADMISSION_RESERVE)
        .is_ok_and(|deadline| certificate.is_usable_until(deadline))
}

#[cfg(test)]
mod tests;
