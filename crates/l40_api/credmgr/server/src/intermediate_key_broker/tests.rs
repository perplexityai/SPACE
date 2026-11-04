use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::key_wrapper::CompositeKeyWrapper;

const EXT: KekProvider = KekProvider::IntermediateKeyBroker;

fn key_id() -> MasterKeyIdentifier {
    MasterKeyIdentifier::parse("provider/bk-v1").unwrap()
}

/// Deterministic per-org key so cross-org tests observe distinct key
/// material without the fake tracking issued blobs.
fn org_key(org: OrgId) -> [u8; 32] {
    let uuid = *org.as_uuid().as_bytes();
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(&uuid);
    key[16..].copy_from_slice(&uuid);
    key
}

enum Mode {
    Ok,
    Revoked,
    InvalidKey,
    AuthorizationRejected,
    Unavailable,
}

struct FakeProvider {
    calls: AtomicUsize,
    mode: Mutex<Mode>,
    max_cache_ttl: Duration,
    delay: Duration,
}

impl FakeProvider {
    fn new() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            mode: Mutex::new(Mode::Ok),
            max_cache_ttl: Duration::from_secs(3_600),
            delay: Duration::ZERO,
        }
    }

    fn set_mode(&self, mode: Mode) {
        *self.mode.lock().unwrap() = mode;
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl IntermediateKeyBrokerApi for FakeProvider {
    async fn issue_intermediate_key(
        &self,
        _org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
    ) -> Result<IssuedIntermediateKey, ProviderError> {
        Ok(IssuedIntermediateKey {
            key_id: key_id(),
            wrapped_key: b"kms-wrapped-blob".to_vec(),
        })
    }

    async fn unwrap_intermediate_key(
        &self,
        org: OrgId,
        _requesting_sandbox: Option<SandboxId>,
        _key_id: &MasterKeyIdentifier,
        _wrapped_key: &[u8],
    ) -> Result<UnwrappedIntermediateKey, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        match *self.mode.lock().unwrap() {
            Mode::Ok => Ok(UnwrappedIntermediateKey {
                key: Zeroizing::new(org_key(org)),
                max_cache_ttl: self.max_cache_ttl,
            }),
            Mode::Revoked => Err(ProviderError::AccessRevoked("injected revocation".into())),
            Mode::InvalidKey => Err(ProviderError::InvalidKey("injected invalid key".into())),
            Mode::AuthorizationRejected => Err(ProviderError::AuthorizationRejected(
                "injected expired grant".into(),
            )),
            Mode::Unavailable => Err(ProviderError::Unavailable("injected outage".into())),
        }
    }
}

struct Fixture {
    wrapper: Arc<IntermediateKeyWrapper>,
    provider: Arc<FakeProvider>,
    store: Arc<InMemoryWrappedKeyStore>,
}

fn fixture_with(provider: FakeProvider, config: IntermediateKeyCacheConfig) -> Fixture {
    let provider = Arc::new(provider);
    let store = Arc::new(InMemoryWrappedKeyStore::new());
    let wrapper = Arc::new(IntermediateKeyWrapper::new(
        provider.clone(),
        store.clone(),
        config,
    ));
    Fixture {
        wrapper,
        provider,
        store,
    }
}

/// Deterministic cache policy: no jitter, generous TTL.
fn test_config() -> IntermediateKeyCacheConfig {
    IntermediateKeyCacheConfig {
        ttl: Duration::from_secs(900),
        ttl_jitter: 0.0,
        negative_ttl: Duration::from_secs(60),
        max_entries: 4096,
    }
}

fn fixture() -> Fixture {
    fixture_with(FakeProvider::new(), test_config())
}

fn enroll(f: &Fixture, org: OrgId) {
    f.store.insert(org, key_id(), b"kms-wrapped-blob".to_vec());
}

#[tokio::test]
async fn wrap_then_unwrap_round_trips() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let aad: &[u8] = b"org/user/purpose";

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, aad)
        .await
        .unwrap();
    assert_eq!(out.plaintext_dek.len(), 32);
    let recovered = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, aad)
        .await
        .unwrap();
    assert_eq!(recovered, out.plaintext_dek);
}

#[tokio::test]
async fn unwrap_rejects_wrong_aad() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"original")
        .await
        .unwrap();
    let err = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"different")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::AuthFailed), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_cross_org_claim() {
    let f = fixture();
    let org_a = OrgId::new();
    let org_b = OrgId::new();
    enroll(&f, org_a);
    enroll(&f, org_b);
    let out = f
        .wrapper
        .generate_and_wrap_dek(org_a, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    // Org B is enrolled with its own intermediate key: the resolved key
    // material differs, so AEAD authentication fails.
    let err = f
        .wrapper
        .unwrap_dek(org_b, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::AuthFailed), "got {err:?}");
}

#[tokio::test]
async fn unwrap_rejects_unenrolled_org() {
    let f = fixture();
    let org_a = OrgId::new();
    let org_b = OrgId::new();
    enroll(&f, org_a);
    let out = f
        .wrapper
        .generate_and_wrap_dek(org_a, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    // Org B has no wrapped blob on file: fail before any provider call.
    let err = f
        .wrapper
        .unwrap_dek(org_b, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::UnknownOrgKek), "got {err:?}");
}

#[tokio::test]
async fn wrap_fails_for_unprovisioned_org() {
    let f = fixture();
    let err = f
        .wrapper
        .generate_and_wrap_dek(OrgId::new(), None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, WrapError::Backend(_)), "got {err:?}");
    assert_eq!(f.provider.calls(), 0);
}

#[tokio::test]
async fn rejects_unsupported_provider() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let err = f
        .wrapper
        .generate_and_wrap_dek(
            org,
            None,
            KekProvider::AwsKms,
            &key_id(),
            DekKeySpec::Aes256,
            b"",
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, WrapError::UnsupportedProvider(KekProvider::AwsKms)),
        "got {err:?}",
    );
    let err = f
        .wrapper
        .unwrap_dek(org, None, KekProvider::InMemory, &key_id(), &[0u8; 64], b"")
        .await
        .unwrap_err();
    assert!(
        matches!(err, UnwrapError::UnsupportedProvider(KekProvider::InMemory)),
        "got {err:?}",
    );
}

#[tokio::test]
async fn unwrap_rejects_malformed_framing() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    // Too short to carry version + nonce + tag.
    let err = f
        .wrapper
        .unwrap_dek(
            org,
            None,
            EXT,
            &key_id(),
            &[INTERMEDIATE_KEY_WRAP_VERSION; 5],
            b"",
        )
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::Malformed), "got {err:?}");
    // Unknown wrap version.
    let mut bad_version = vec![0xFFu8];
    bad_version.extend_from_slice(&[0u8; NONCE_LEN + TAG_LEN]);
    let err = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &bad_version, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::Malformed), "got {err:?}");
    // Framing is validated before the key is resolved.
    assert_eq!(f.provider.calls(), 0);
}

#[tokio::test(start_paused = true)]
async fn cache_serves_within_ttl_and_refreshes_after_expiry() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    assert_eq!(f.provider.calls(), 1);

    tokio::time::advance(Duration::from_secs(300)).await;
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
    assert_eq!(f.provider.calls(), 1, "within TTL must be a cache hit");

    tokio::time::advance(Duration::from_secs(601)).await;
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
    assert_eq!(f.provider.calls(), 2, "past TTL must re-unwrap");
}

#[tokio::test(start_paused = true)]
async fn provider_ttl_cap_tightens_config_ttl() {
    let mut provider = FakeProvider::new();
    provider.max_cache_ttl = Duration::from_secs(60);
    let f = fixture_with(provider, test_config());
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(61)).await;
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
    assert_eq!(
        f.provider.calls(),
        2,
        "provider cap must override the 900s config TTL"
    );
}

#[tokio::test(start_paused = true)]
async fn revocation_takes_effect_after_ttl_expiry() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    f.provider.set_mode(Mode::Revoked);

    // The cached key keeps serving until expiry: the TTL is the SLA.
    tokio::time::advance(Duration::from_secs(300)).await;
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
    assert_eq!(f.provider.calls(), 1);

    // Past expiry, the revocation surfaces as the typed variant the
    // cleanup/shutdown machinery keys on, and every operation fails
    // closed.
    tokio::time::advance(Duration::from_secs(601)).await;
    let err = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::AccessRevoked), "got {err:?}");
    let err = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, WrapError::AccessRevoked), "got {err:?}");
}

#[tokio::test]
async fn provider_invalid_key_is_a_permanent_wrap_failure() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    f.provider.set_mode(Mode::InvalidKey);

    let err = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();

    assert!(matches!(err, WrapError::InvalidKey(_)), "got {err:?}");
}

#[tokio::test]
async fn authorization_rejection_is_not_revocation_or_negative_cached() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    f.provider.set_mode(Mode::AuthorizationRejected);

    for expected_calls in 1..=2 {
        let err = f
            .wrapper
            .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
            .await
            .unwrap_err();
        assert!(
            matches!(err, WrapError::AuthorizationRejected(_)),
            "got {err:?}"
        );
        assert_eq!(
            f.provider.calls(),
            expected_calls,
            "a renewed grant may succeed immediately, so rejection cannot be cached"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn provider_invalid_key_is_a_permanent_unwrap_failure() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    f.provider.set_mode(Mode::InvalidKey);

    tokio::time::advance(Duration::from_secs(901)).await;
    let err = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();

    let UnwrapError::InvalidKey(detail) = err else {
        panic!("got {err:?}");
    };
    assert!(detail.contains("injected invalid key"));
}

#[tokio::test(start_paused = true)]
async fn revocations_are_negative_cached() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    f.provider.set_mode(Mode::Revoked);

    for _ in 0..3 {
        let err = f
            .wrapper
            .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
            .await
            .unwrap_err();
        assert!(matches!(err, WrapError::AccessRevoked), "got {err:?}");
    }
    assert_eq!(f.provider.calls(), 1, "repeat revocations must not re-dial");

    tokio::time::advance(Duration::from_secs(61)).await;
    let _ = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();
    assert_eq!(f.provider.calls(), 2, "negative cache must expire");
}

#[tokio::test(start_paused = true)]
async fn expired_cache_plus_outage_fails_closed() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    f.provider.set_mode(Mode::Unavailable);

    tokio::time::advance(Duration::from_secs(901)).await;
    // No serve-stale: the expired key must not be used, and the outage
    // surfaces as the typed retriable variant.
    let err = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();
    assert!(matches!(err, UnwrapError::Unavailable(_)), "got {err:?}");

    // Outages are not negative-cached: recovery is immediate.
    f.provider.set_mode(Mode::Ok);
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn concurrent_misses_single_flight() {
    let mut provider = FakeProvider::new();
    provider.delay = Duration::from_millis(50);
    let f = fixture_with(provider, test_config());
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(901)).await;

    let mut handles = Vec::new();
    for _ in 0..8 {
        let wrapper = f.wrapper.clone();
        let wrapped = out.wrapped_dek.clone();
        handles.push(tokio::spawn(async move {
            wrapper
                .unwrap_dek(org, None, EXT, &key_id(), &wrapped, b"")
                .await
        }));
    }
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
    assert_eq!(
        f.provider.calls(),
        2,
        "one initial fetch + one coalesced refresh"
    );
}

#[tokio::test(start_paused = true)]
async fn cap_evicts_soonest_expiring_entry() {
    let mut config = test_config();
    config.max_entries = 2;
    let f = fixture_with(FakeProvider::new(), config);
    let orgs = [OrgId::new(), OrgId::new(), OrgId::new()];
    for org in orgs {
        enroll(&f, org);
    }

    for org in orgs {
        f.wrapper
            .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
            .await
            .unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
    }
    assert_eq!(f.provider.calls(), 3);
    assert!(f.wrapper.cache.len() <= 2, "cap must hold");

    // The first org expired soonest and was evicted; using it again
    // costs a fresh provider round-trip.
    f.wrapper
        .generate_and_wrap_dek(orgs[0], None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    assert_eq!(f.provider.calls(), 4);
}

/// An expired `Slot::Key` still owns plaintext key material; it must be
/// removed when observed, not merely bypassed as a miss.
#[tokio::test(start_paused = true)]
async fn expired_entries_are_removed_when_observed() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    assert_eq!(f.wrapper.cache.len(), 1);

    tokio::time::advance(Duration::from_secs(901)).await;
    // A failed read of the expired key must still drop the entry.
    f.provider.set_mode(Mode::Unavailable);
    let _ = f
        .wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap_err();
    assert_eq!(
        f.wrapper.cache.len(),
        0,
        "an observed expired entry must be dropped, releasing its plaintext"
    );
}

/// Expiry itself must release the cache's zeroizing owner; no later read,
/// insert, or capacity sweep may be required to retire idle plaintext.
#[tokio::test(start_paused = true)]
async fn idle_entries_release_zeroizing_owner_at_expiry() {
    fn assert_zeroize_on_drop<T: zeroize::ZeroizeOnDrop>(_: &T) {}

    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    f.wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();

    let cached_owner = {
        let slot = f.wrapper.cache.get(&(org, key_id())).unwrap();
        let Slot::Key { key, .. } = slot.value() else {
            panic!("expected cached plaintext key")
        };
        assert_zeroize_on_drop(key.as_ref());
        Arc::downgrade(key)
    };

    tokio::time::advance(Duration::from_secs(900)).await;
    tokio::task::yield_now().await;

    assert!(
        f.wrapper.cache.is_empty(),
        "the expiry timer must remove idle plaintext"
    );
    assert!(
        cached_owner.upgrade().is_none(),
        "expiry must drop the cache's Zeroizing owner"
    );
}

#[tokio::test(start_paused = true)]
async fn earlier_deadline_reschedules_expiry_timer() {
    let f = fixture();
    let cached_org = OrgId::new();
    let revoked_org = OrgId::new();
    enroll(&f, cached_org);
    enroll(&f, revoked_org);

    f.wrapper
        .generate_and_wrap_dek(cached_org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    // Let the reaper begin sleeping on the plaintext key's 900s deadline.
    tokio::task::yield_now().await;

    f.provider.set_mode(Mode::Revoked);
    let error = f
        .wrapper
        .generate_and_wrap_dek(revoked_org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();
    assert!(matches!(error, WrapError::AccessRevoked));
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(60)).await;
    tokio::task::yield_now().await;

    assert!(
        f.wrapper.cache.contains_key(&(cached_org, key_id())),
        "the later plaintext deadline must remain scheduled"
    );
    assert!(
        !f.wrapper.cache.contains_key(&(revoked_org, key_id())),
        "the watch update must reschedule the timer to the earlier deadline"
    );
}

#[tokio::test(start_paused = true)]
async fn stopped_expiry_task_releases_zeroizing_owners() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    f.wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();

    let cached_owner = {
        let slot = f.wrapper.cache.get(&(org, key_id())).unwrap();
        let Slot::Key { key, .. } = slot.value() else {
            panic!("expected cached plaintext key")
        };
        Arc::downgrade(key)
    };

    f.wrapper.expiry_task.get().unwrap().abort();
    for _ in 0..10 {
        if f.wrapper.cache.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(
        f.wrapper.cache.is_empty(),
        "stopping the expiry task must clear cached plaintext"
    );
    assert!(
        cached_owner.upgrade().is_none(),
        "task cleanup must drop the cache's Zeroizing owner"
    );
}

/// Inserts sweep all expired entries, not only on cap overflow — an org
/// that is never requested again must not keep plaintext resident just
/// because the cache is under its cap.
#[tokio::test(start_paused = true)]
async fn inserts_sweep_expired_entries_below_the_cap() {
    let f = fixture();
    let orgs = [OrgId::new(), OrgId::new()];
    for org in orgs {
        enroll(&f, org);
    }
    f.wrapper
        .generate_and_wrap_dek(orgs[0], None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(901)).await;

    // orgs[0]'s entry is expired; inserting orgs[1] must sweep it.
    f.wrapper
        .generate_and_wrap_dek(orgs[1], None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    assert_eq!(
        f.wrapper.cache.len(),
        1,
        "the insert-path sweep must drop expired entries below the cap"
    );
}

/// A refresh future dropped mid-flight (client disconnect, deadline)
/// must still retire its single-flight gate.
#[tokio::test(start_paused = true)]
async fn canceled_refreshes_retire_their_gates() {
    let mut provider = FakeProvider::new();
    provider.delay = Duration::from_secs(3600);
    let f = fixture_with(provider, test_config());
    let org = OrgId::new();
    enroll(&f, org);

    let wrapper = f.wrapper.clone();
    let handle = tokio::spawn(async move {
        let _ = wrapper
            .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
            .await;
    });
    // Walk the spawned task into the provider call (current-thread
    // runtime: each yield lets it advance one await point).
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(f.provider.calls(), 1, "refresh must be in flight");
    handle.abort();
    let _ = handle.await;
    assert_eq!(
        f.wrapper.refresh.len(),
        0,
        "a canceled refresh must retire its gate"
    );
}

#[test]
fn jittered_ttl_respects_local_and_provider_ceilings_at_extrema() {
    let mut config = test_config();
    config.ttl_jitter = 0.1;
    let f = fixture_with(FakeProvider::new(), config);
    let cases = [
        (
            "no downward jitter with a looser provider cap",
            0.0,
            Duration::from_secs(1_200),
            900,
        ),
        (
            "midpoint jitter with a looser provider cap",
            0.5,
            Duration::from_secs(1_200),
            855,
        ),
        (
            "maximum downward jitter with a looser provider cap",
            1.0,
            Duration::from_secs(1_200),
            810,
        ),
        (
            "no downward jitter with a tighter provider cap",
            0.0,
            Duration::from_secs(600),
            600,
        ),
        (
            "midpoint jitter with a tighter provider cap",
            0.5,
            Duration::from_secs(600),
            570,
        ),
        (
            "maximum downward jitter with a tighter provider cap",
            1.0,
            Duration::from_secs(600),
            540,
        ),
        (
            "maximum downward jitter with a looser provider cap",
            1.0,
            Duration::from_secs(1_200),
            810,
        ),
        ("explicit zero provider cap", 0.5, Duration::ZERO, 0),
    ];

    for (case, sample, provider_cap, expected_secs) in cases {
        assert_eq!(
            f.wrapper
                .effective_ttl_for_jitter_sample(provider_cap, sample),
            Duration::from_secs(expected_secs),
            "{case}"
        );
    }
}

#[test]
fn evidence_expiry_further_bounds_the_broker_cache_cap() {
    let now = jiff::Timestamp::from_second(1_000).unwrap();
    let expires_at = jiff::Timestamp::from_second(1_180).unwrap();
    assert_eq!(
        cap_cache_ttl_by_evidence(Duration::from_secs(600), Some(expires_at), now).unwrap(),
        Duration::from_secs(180)
    );
    assert_eq!(
        cap_cache_ttl_by_evidence(Duration::from_secs(60), Some(expires_at), now).unwrap(),
        Duration::from_secs(60),
        "the broker may impose a tighter cap than the stored evidence expiry"
    );
}

#[test]
fn expired_evidence_cannot_release_a_cacheable_key() {
    let now = jiff::Timestamp::from_second(1_000).unwrap();
    for expires_at in [
        jiff::Timestamp::from_second(999).unwrap(),
        jiff::Timestamp::from_second(1_000).unwrap(),
    ] {
        assert!(matches!(
            cap_cache_ttl_by_evidence(Duration::from_secs(600), Some(expires_at), now),
            Err(ProviderError::AuthorizationRejected(_))
        ));
    }
}

/// Failed lookups (unenrolled org, store error, outage) never insert a
/// cache entry, so their single-flight gates must retire on exit rather
/// than accumulate — repeated unique failing keys must not grow memory.
#[tokio::test]
async fn failed_lookups_do_not_leak_single_flight_gates() {
    let f = fixture();
    for _ in 0..32 {
        // Unenrolled org: resolve fails with NoWrappedKey.
        let err = f
            .wrapper
            .generate_and_wrap_dek(OrgId::new(), None, EXT, &key_id(), DekKeySpec::Aes256, b"")
            .await
            .unwrap_err();
        assert!(matches!(err, WrapError::Backend(_)), "got {err:?}");
    }
    assert_eq!(
        f.wrapper.refresh.len(),
        0,
        "single-flight gates for failed lookups must retire"
    );
}

/// An explicit zero cap means "do not cache": every operation must
/// reauthorize through the provider.
#[tokio::test(start_paused = true)]
async fn zero_ttl_cap_forces_reauthorization_every_use() {
    let mut provider = FakeProvider::new();
    provider.max_cache_ttl = Duration::ZERO;
    let f = fixture_with(provider, test_config());
    let org = OrgId::new();
    enroll(&f, org);

    let out = f
        .wrapper
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    f.wrapper
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();
    assert_eq!(
        f.provider.calls(),
        2,
        "a zero cap disables caching: each operation reauthorizes"
    );
}

#[tokio::test]
async fn composite_routes_intermediate_key_broker() {
    let f = fixture();
    let org = OrgId::new();
    enroll(&f, org);
    let installed = CompositeKeyWrapper::new().with_intermediate_key_broker(
        IntermediateKeyWrapper::new(f.provider.clone(), f.store.clone(), test_config()),
    );
    let out = installed
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap();
    installed
        .unwrap_dek(org, None, EXT, &key_id(), &out.wrapped_dek, b"")
        .await
        .unwrap();

    // The node-local shape: backend not installed => fail closed.
    let not_installed = CompositeKeyWrapper::new();
    let err = not_installed
        .generate_and_wrap_dek(org, None, EXT, &key_id(), DekKeySpec::Aes256, b"")
        .await
        .unwrap_err();
    assert!(
        matches!(err, WrapError::UnsupportedProvider(EXT)),
        "got {err:?}"
    );
}

#[test]
fn broker_protocol_requires_both_explicit_identifiers() {
    for invalid in [None, Some(""), Some(" "), Some("urn:example:bad value")] {
        let missing_namespace = BrokerProtocolConfig::parse(invalid, Some("urn:example:grant:v1"));
        let missing_scheme = BrokerProtocolConfig::parse(Some("urn:example:tenant"), invalid);

        assert_eq!(missing_namespace.unwrap_err().setting, TENANT_NAMESPACE_ENV);
        assert_eq!(missing_scheme.unwrap_err().setting, GRANT_SCHEME_ENV);
    }
}

/// A contended read of `org_branching_keys` must reach the caller as a
/// retryable code, not as a backend fault.
///
/// This is the third condition the `space_creds` `lock_timeout` depends on,
/// and the one that is easiest to miss: the store used to stringify its
/// `sqlx::Error`, so by the time either mapper saw it the SQLSTATE was gone
/// and every failure became `Backend` -> gRPC `Internal`. Bounding the role
/// while that was true would have turned a slow success into a fast
/// permanent-looking credential failure — the exact trade the role was held
/// out of the bound to avoid.
#[tokio::test(flavor = "current_thread")]
async fn a_contended_branching_key_read_is_retryable() -> pplx_space_test_env::TestResult {
    let Some(db) = pplx_space_db_test_fixtures::try_raw_pg_test_db().await? else {
        return Ok(());
    };

    for sqlstate in ["55P03", "40001", "40P01"] {
        // One raise per mapper, since both take the error by value.
        let for_wrap = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        assert!(
            matches!(
                wrap_resolve_error(ResolveError::Store(WrappedKeyStoreError::Lookup(for_wrap))),
                WrapError::Unavailable(_)
            ),
            "{sqlstate} on wrap must be retryable"
        );
        let for_unwrap = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, sqlstate).await;
        assert!(
            matches!(
                unwrap_resolve_error(ResolveError::Store(WrappedKeyStoreError::Lookup(
                    for_unwrap
                ))),
                UnwrapError::Unavailable(_)
            ),
            "{sqlstate} on unwrap must be retryable"
        );
    }

    // A real query failure stays a backend fault, so the branch discriminates
    // on the SQLSTATE and not on the variant.
    let real = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert!(matches!(
        wrap_resolve_error(ResolveError::Store(WrappedKeyStoreError::Lookup(real))),
        WrapError::Backend(_)
    ));
    let real = pplx_space_db_test_fixtures::raise_sqlstate(&db.pool, "23505").await;
    assert!(matches!(
        unwrap_resolve_error(ResolveError::Store(WrappedKeyStoreError::Lookup(real))),
        UnwrapError::Backend(_)
    ));
    Ok(())
}
