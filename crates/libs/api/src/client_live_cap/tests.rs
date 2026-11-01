use std::collections::BTreeSet;
use std::num::NonZeroU64;
use std::time::Duration;

use pplx_space_common_core::types::{ClientId, SandboxId};
use pplx_space_feature_flags::FeatureFlagClient;
use pplx_space_test_env::TestResult;
use sqlx::postgres::PgPoolOptions;

use super::pending_set::PendingTiming;
use super::*;
use crate::client_concurrency::tests::try_redis_url;
use crate::controllers::substrate::{ActivationClientAttribution, Substrate};

fn id(value: &str) -> ClientId {
    ClientId::parse(value).expect("valid id")
}

fn cap(value: u64) -> Option<NonZeroU64> {
    NonZeroU64::new(value)
}

fn config(per_client: Option<NonZeroU64>, unknown: Option<NonZeroU64>) -> ClientLiveCapConfig {
    ClientLiveCapConfig {
        per_client,
        unknown,
        known_clients: BTreeSet::from(["alpha".to_string()]),
        concurrency_overrides_active: true,
        redis_url: None,
    }
}

fn lazy_pool() -> sqlx::PgPool {
    // Counting never touches Postgres; a lazy pool keeps these Docker-free
    // apart from Redis.
    PgPoolOptions::new()
        .connect_lazy("postgres://unused@localhost/unused")
        .expect("lazy pool builds without connecting")
}

const TIMING: PendingTiming = PendingTiming {
    ttl: Duration::from_secs(30),
    grace: Duration::from_secs(60),
};

/// A cap over `substrate` with its pending set under a prefix of its own.
fn live_cap(
    substrate: Substrate,
    url: &str,
    config: ClientLiveCapConfig,
    timing: PendingTiming,
) -> ClientLiveCap {
    ClientLiveCap::with_config(
        substrate,
        ClientLiveCapConfig {
            redis_url: Some(url.to_string()),
            ..config
        },
        FeatureFlagClient::disabled(),
    )
    .isolated(&uuid::Uuid::now_v7().simple().to_string(), timing)
}

#[cfg(feature = "kubernetes")]
mod kubernetes_substrate {
    use axum::http::{Request, Response};
    use k8s_openapi::api::core::v1::Pod;
    use kube::client::{Body, ClientBuilder};
    use pplx_space_common_core::kubernetes::{SANDBOX_CLIENT_ID_LABEL, SANDBOX_ID_ANNOTATION};

    use super::*;
    use crate::controllers::substrate::kubernetes::KubernetesSubstrate;

    type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

    /// A running sandbox pod for `sandbox`, labeled with `client`.
    pub(super) fn labeled_pod(sandbox: SandboxId, client: &str) -> Pod {
        serde_json::from_value(serde_json::json!({
            "metadata": {
                "name": format!("sandbox-{sandbox}"),
                "namespace": "space",
                "labels": { SANDBOX_CLIENT_ID_LABEL: client },
                "annotations": { SANDBOX_ID_ANNOTATION: sandbox.to_string() },
            },
            "spec": { "containers": [] },
            "status": { "phase": "Running" },
        }))
        .expect("valid pod")
    }

    /// A Kubernetes substrate whose warm reflector shows exactly `pods`.
    pub(super) fn seeded(pods: Vec<Pod>) -> KubernetesSubstrate {
        let service = tower::service_fn(|_req: Request<Body>| async {
            Ok::<_, BoxError>(Response::new(Body::from(Vec::new())))
        });
        let client = ClientBuilder::new(service, "space").build();
        KubernetesSubstrate::with_seeded_caches(lazy_pool(), client, 1, pods)
    }

    pub(super) fn substrate(pods: Vec<Pod>) -> Substrate {
        Substrate::from_kubernetes(seeded(pods))
    }
}

#[cfg(feature = "kubernetes")]
use kubernetes_substrate::{labeled_pod, substrate as kubernetes};

#[cfg(feature = "kubernetes")]
/// Wait for a dropped reservation's background release to land: `client`
/// activating `sandbox` passes the read-only pre-check once it has.
async fn until_under_cap(cap: &ClientLiveCap, client: &ClientId, sandbox: SandboxId) {
    for _ in 0..100 {
        if cap.precheck(client, sandbox).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{client} stayed at its cap");
}

/// The pending set covers admissions the reflector hasn't seen yet: with no
/// pod visible anywhere, a client still can't be admitted past its cap.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn the_pending_set_counts_admissions_the_reflector_has_not_seen() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(cap(2), None), TIMING);
    let alpha = id("alpha");
    let first = cap.reserve(&alpha, SandboxId::new()).await?;
    let second = cap.reserve(&alpha, SandboxId::new()).await?;
    assert!(first.is_some() && second.is_some(), "under the cap");
    let refusal = cap
        .reserve(&alpha, SandboxId::new())
        .await
        .expect_err("two pending admissions fill a cap of two");
    assert!(refusal.message.contains("\"alpha\""), "{}", refusal.message);
    assert!(refusal.retry_after >= Duration::from_secs(5));
    // Another client's cap is its own.
    assert!(
        cap.reserve(&id("unlisted"), SandboxId::new())
            .await?
            .is_none()
    );
    Ok(())
}

/// A sandbox both pending and visible to the reflector counts once.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_pending_sandbox_the_reflector_shows_counts_once() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let seen = SandboxId::new();
    let cap = live_cap(
        kubernetes(vec![labeled_pod(seen, "alpha")]),
        &url,
        config(cap(2), None),
        TIMING,
    );
    let alpha = id("alpha");
    let _seen = cap.reserve(&alpha, seen).await?.expect("its own sandbox");
    let _second = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("one sandbox held, counted once");
    cap.reserve(&alpha, SandboxId::new())
        .await
        .expect_err("now at the cap");
    Ok(())
}

/// A running sandbox is never refused (RFC Decision 6): a client at its cap
/// can still reach or replace a sandbox it holds, by reservation and by the
/// pre-check alike, while a new one is refused.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_running_sandbox_is_never_refused() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let (held, other) = (SandboxId::new(), SandboxId::new());
    let cap = live_cap(
        kubernetes(vec![
            labeled_pod(held, "alpha"),
            labeled_pod(other, "alpha"),
        ]),
        &url,
        config(cap(2), None),
        TIMING,
    );
    let alpha = id("alpha");
    cap.precheck(&alpha, held)
        .await
        .expect("a held sandbox passes");
    let reached = cap
        .reserve(&alpha, held)
        .await
        .expect("a held sandbox is admitted");
    assert!(reached.is_some());
    let fresh = SandboxId::new();
    cap.precheck(&alpha, fresh)
        .await
        .expect_err("a new one is at the cap");
    cap.reserve(&alpha, fresh)
        .await
        .expect_err("a new one is at the cap");
    Ok(())
}

/// With attribution off no pod carries a client label, so the cap admits
/// rather than counting only the last half minute's admissions.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn attribution_off_admits_without_counting() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let mut substrate = kubernetes_substrate::seeded(vec![labeled_pod(SandboxId::new(), "alpha")]);
    substrate.k8s.activation_client_attribution = ActivationClientAttribution::Disabled;
    let cap = live_cap(
        Substrate::from_kubernetes(substrate),
        &url,
        config(cap(1), None),
        TIMING,
    );
    let alpha = id("alpha");
    for _ in 0..3 {
        assert!(cap.reserve(&alpha, SandboxId::new()).await?.is_none());
    }
    cap.precheck(&alpha, SandboxId::new()).await?;
    Ok(())
}

/// A stale pod cache never refuses on its frozen index, which may still hold
/// sandboxes the client deleted; the pending set alone bounds the admissions.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_stale_pod_cache_caps_by_the_pending_set_alone() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let frozen = kubernetes_substrate::seeded(vec![
        labeled_pod(SandboxId::new(), "alpha"),
        labeled_pod(SandboxId::new(), "alpha"),
    ]);
    frozen.mark_pod_watch_error_for_test();
    let cap = live_cap(
        Substrate::from_kubernetes(frozen),
        &url,
        config(cap(2), None),
        TIMING,
    );
    let alpha = id("alpha");
    let _first = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("the frozen pods don't count");
    let _second = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("one pending, under the cap");
    cap.reserve(&alpha, SandboxId::new())
        .await
        .expect_err("two pending admissions fill a cap of two");
    Ok(())
}

/// Direct has no per-client inventory (it answers `Unsupported`), so admission
/// admits without reserving anything, however many it already admitted.
#[tokio::test]
async fn direct_admits_as_unknown() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let direct = Substrate::from_direct(
        crate::controllers::substrate::direct::DirectSubstrate::new(lazy_pool())
            .with_activation_client_attribution(ActivationClientAttribution::Enabled),
    );
    let cap = live_cap(direct, &url, config(cap(1), None), TIMING);
    let alpha = id("alpha");
    for _ in 0..3 {
        assert!(cap.reserve(&alpha, SandboxId::new()).await?.is_none());
    }
    cap.precheck(&alpha, SandboxId::new()).await?;
    Ok(())
}

/// A failed attempt uncounts only itself: with two attempts sharing one
/// sandbox's member, dropping one keeps the sandbox counted, and dropping
/// the last releases it.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_failed_attempt_never_uncounts_a_sibling() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(cap(1), None), TIMING);
    let alpha = id("alpha");
    let shared = SandboxId::new();
    let first = cap.reserve(&alpha, shared).await?.expect("admitted");
    let sibling = cap.reserve(&alpha, shared).await?.expect("its own sandbox");
    drop(first);
    let probe = SandboxId::new();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cap.precheck(&alpha, probe)
        .await
        .expect_err("the sibling still holds the member");
    drop(sibling);
    until_under_cap(&cap, &alpha, probe).await;
    Ok(())
}

/// A kept member covers the watch delay and then lapses on its own; a
/// teardown forgets it at once.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_kept_member_lapses_after_its_ttl_or_when_forgotten() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let short = PendingTiming {
        ttl: Duration::from_secs(1),
        grace: Duration::from_secs(1),
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(cap(1), None), short);
    let alpha = id("alpha");
    cap.reserve(&alpha, SandboxId::new())
        .await?
        .expect("admitted")
        .keep();
    let probe = SandboxId::new();
    cap.precheck(&alpha, probe)
        .await
        .expect_err("kept member counts");
    tokio::time::sleep(Duration::from_millis(1300)).await;
    cap.precheck(&alpha, probe)
        .await
        .expect("lapsed after its TTL");

    let torn_down = SandboxId::new();
    cap.reserve(&alpha, torn_down)
        .await?
        .expect("admitted")
        .keep();
    cap.precheck(&alpha, probe)
        .await
        .expect_err("kept member counts");
    cap.forget(torn_down).await;
    cap.precheck(&alpha, probe).await.expect("forgotten");
    Ok(())
}

/// A lapsed member's slot may go to a sibling, so nothing revives it without
/// a cap check: not its renewal, a warm-hit swap, or a reserve with a read
/// taken while it was live.
#[tokio::test]
async fn a_lapsed_member_is_never_revived_past_the_cap() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let set = PendingSet::from_url(&url)?.isolated(&uuid::Uuid::now_v7().simple().to_string());
    let short = PendingTiming {
        ttl: Duration::from_secs(2),
        grace: Duration::from_secs(60),
    };
    let alpha = id("alpha");
    let ids = CountedIds::Exactly(vec![alpha.clone()]);
    let member = |sandbox, owner| Member {
        client: &alpha,
        sandbox,
        owner,
    };
    let lapsing = SandboxId::new();
    let read = set.read(&ids).await?;
    let verdict = set
        .reserve(&ids, &read, &member(lapsing, "first"), 1, 0, short)
        .await?;
    assert_eq!(verdict, ReserveVerdict::Admitted);
    let before_lapse = set.read(&ids).await?;
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert!(
        !set.renew(&member(lapsing, "first"), short).await?,
        "not renewed"
    );
    assert!(
        !set.swap(&member(lapsing, "first"), SandboxId::new(), short)
            .await?,
        "not moved"
    );

    let read = set.read(&ids).await?;
    let verdict = set
        .reserve(
            &ids,
            &read,
            &member(SandboxId::new(), "sibling"),
            1,
            0,
            short,
        )
        .await?;
    assert_eq!(
        verdict,
        ReserveVerdict::Admitted,
        "the lapsed member freed the slot"
    );
    let verdict = set
        .reserve(&ids, &before_lapse, &member(lapsing, "retry"), 1, 0, short)
        .await?;
    assert_eq!(verdict, ReserveVerdict::Retry, "rechecked, not renewed");
    let read = set.read(&ids).await?;
    let verdict = set
        .reserve(&ids, &read, &member(lapsing, "retry"), 1, 0, short)
        .await?;
    assert_eq!(verdict, ReserveVerdict::Refused);
    Ok(())
}

/// Once a pod may exist, dropping the reservation (a failure after the
/// substrate call) leaves the member to lapse rather than uncounting a pod
/// the reflectors haven't shown yet.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_reservation_whose_pod_may_exist_lapses_when_dropped() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let short = PendingTiming {
        ttl: Duration::from_secs(1),
        grace: Duration::from_secs(1),
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(cap(1), None), short);
    let alpha = id("alpha");
    let probe = SandboxId::new();
    let mut reservation = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("admitted");
    reservation.pod_may_exist();
    drop(reservation);
    tokio::time::sleep(Duration::from_millis(200)).await;
    cap.precheck(&alpha, probe)
        .await
        .expect_err("the dropped member still counts");
    tokio::time::sleep(Duration::from_millis(1300)).await;
    cap.precheck(&alpha, probe)
        .await
        .expect("lapsed after its TTL");

    // Undone, the drop releases at once again.
    let mut reservation = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("admitted");
    reservation.pod_may_exist();
    reservation.no_pod_yet();
    drop(reservation);
    // Well inside the 1 s TTL, so only a release, not a lapse, frees it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    cap.precheck(&alpha, probe).await.expect("released at once");
    Ok(())
}

/// A warm hit moves the create's member to the adopted sandbox: the planned
/// id stops counting and the adopted one starts.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_swapped_member_counts_under_the_adopted_sandbox() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(cap(1), None), TIMING);
    let alpha = id("alpha");
    let (planned, adopted) = (SandboxId::new(), SandboxId::new());
    let mut create = cap.reserve(&alpha, planned).await?.expect("admitted");
    create.swap(adopted).await;
    assert_eq!(create.sandbox(), adopted);
    cap.precheck(&alpha, adopted)
        .await
        .expect("its own sandbox");
    cap.precheck(&alpha, planned)
        .await
        .expect_err("the planned id no longer counts as its own");
    create.keep();
    Ok(())
}

/// Unlisted ids share `unknown`'s cap through the pending set's registry, and
/// a listed id with no per-client default isn't capped at all.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn unlisted_ids_share_the_unknown_cap() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let cap = live_cap(kubernetes(Vec::new()), &url, config(None, cap(1)), TIMING);
    let _gamma = cap
        .reserve(&id("gamma"), SandboxId::new())
        .await?
        .expect("admitted");
    cap.reserve(&id("delta"), SandboxId::new())
        .await
        .expect_err("gamma's pending sandbox fills unknown's cap");
    cap.reserve(&ClientId::unknown(), SandboxId::new())
        .await
        .expect_err("so does a caller that sent no id");
    assert!(cap.reserve(&id("alpha"), SandboxId::new()).await?.is_none());
    assert!(
        cap.reserve(&ClientId::native(), SandboxId::new())
            .await?
            .is_none()
    );
    Ok(())
}

/// A Redis error admits: the cap is a guardrail, never a create-path SPOF.
/// The client is already at its cap of one, so only failing open admits.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn a_redis_error_admits() -> TestResult {
    let cap = live_cap(
        kubernetes(vec![labeled_pod(SandboxId::new(), "alpha")]),
        "redis://127.0.0.1:1",
        config(cap(1), cap(1)),
        TIMING,
    );
    assert!(cap.reserve(&id("alpha"), SandboxId::new()).await?.is_none());
    cap.precheck(&id("alpha"), SandboxId::new()).await?;
    Ok(())
}

fn flag(json: serde_json::Value) -> QuotasFlag {
    QuotasFlag::parse(&json)
}

/// A listed id is its own key; everything else but `native` resolves to
/// `unknown`, which counts every id outside the key set.
#[test]
fn ids_resolve_to_bounded_keys() {
    let config = config(cap(4), cap(2));
    let alpha = resolve(&config, None, &id("alpha")).expect("listed");
    assert_eq!(alpha.key, "alpha");
    assert_eq!(alpha.ids, CountedIds::Exactly(vec![id("alpha")]));
    assert_eq!(alpha.cap, NonZeroU64::new(4).unwrap());
    let gamma = resolve(&config, None, &id("gamma")).expect("unlisted");
    assert_eq!(gamma.key, "unknown");
    assert_eq!(gamma.ids, CountedIds::AllExcept(vec![id("alpha")]));
    assert_eq!(gamma.cap, NonZeroU64::new(2).unwrap());
    assert_eq!(resolve(&config, None, &ClientId::unknown()), Some(gamma));
    assert_eq!(resolve(&config, None, &ClientId::native()), None);
}

/// The quotas flag tunes a cap the environment turned on and lists ids, but
/// never turns a cap on (RFC Decision 2); reserved names can't be listed, and
/// a malformed field drops only itself.
#[test]
fn the_flag_tunes_caps_but_never_turns_one_on() {
    let tuned = flag(serde_json::json!({
        "live": "{\"beta\": 9, \"unknown\": 3, \"native\": 1, \"zeta\": 0}",
        "overrides": { "omega": 5 },
    }));
    let env_both = config(cap(4), cap(2));
    let beta = resolve(&env_both, Some(&tuned), &id("beta")).expect("listed by the flag");
    assert_eq!((beta.key.as_str(), beta.cap.get()), ("beta", 9));
    let omega = resolve(&env_both, Some(&tuned), &id("omega")).expect("listed by overrides");
    assert_eq!((omega.key.as_str(), omega.cap.get()), ("omega", 4));
    let unknown = resolve(&env_both, Some(&tuned), &id("gamma")).expect("unknown");
    assert_eq!(unknown.cap.get(), 3);
    assert_eq!(
        unknown.ids,
        CountedIds::AllExcept(vec![id("alpha"), id("beta"), id("omega")])
    );
    assert_eq!(resolve(&env_both, Some(&tuned), &ClientId::native()), None);
    // A zero entry is ignored: it neither lists its id nor lifts a cap.
    let zeta = resolve(&env_both, Some(&tuned), &id("zeta")).expect("still capped");
    assert_eq!((zeta.key.as_str(), zeta.cap.get()), ("unknown", 3));

    let env_off = config(None, None);
    assert_eq!(resolve(&env_off, Some(&tuned), &id("beta")), None);
    assert_eq!(resolve(&env_off, Some(&tuned), &id("gamma")), None);
    // With only `unknown` capped, a `live` entry doesn't list its id, so the
    // id stays under `unknown`'s cap rather than leaving it uncapped.
    let unknown_only = config(None, cap(2));
    let beta_unlisted = resolve(&unknown_only, Some(&tuned), &id("beta")).expect("unknown");
    assert_eq!(
        (beta_unlisted.key.as_str(), beta_unlisted.cap.get()),
        ("unknown", 3)
    );

    // Nor does an `overrides` entry.
    let omega_unlisted = resolve(&unknown_only, Some(&tuned), &id("omega")).expect("unknown");
    assert_eq!(omega_unlisted.key, "unknown");

    let concurrency_off = ClientLiveCapConfig {
        concurrency_overrides_active: false,
        ..config(cap(4), cap(2))
    };
    assert_eq!(
        resolve(&concurrency_off, Some(&tuned), &id("omega")).map(|check| check.key),
        Some("unknown".to_string())
    );

    let broken = flag(serde_json::json!({ "live": "not json", "overrides": { "omega": 5 } }));
    assert!(broken.live.is_empty());
    assert!(broken.override_ids.contains("omega"));

    // One malformed entry drops only itself, not the rest of the map.
    let one_bad = flag(serde_json::json!({ "live": { "beta": 9, "typo": "5", "neg": -1 } }));
    assert_eq!(one_bad.live, HashMap::from([("beta".to_string(), 9)]));
}

/// The cap reads the flag through the feature-flag client, gated on the env
/// defaults.
#[cfg(feature = "kubernetes")]
#[tokio::test]
async fn the_cap_reads_live_overrides_from_the_quotas_flag() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let cap = ClientLiveCap::with_config(
        kubernetes(Vec::new()),
        ClientLiveCapConfig {
            redis_url: Some(url.to_string()),
            ..config(cap(5), None)
        },
        FeatureFlagClient::fixed_json([(
            pplx_space_feature_flags::SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
            serde_json::json!({ "live": { "alpha": 1 } }),
        )]),
    )
    .isolated(&uuid::Uuid::now_v7().simple().to_string(), TIMING);
    let alpha = id("alpha");
    let _held = cap
        .reserve(&alpha, SandboxId::new())
        .await?
        .expect("admitted");
    cap.reserve(&alpha, SandboxId::new())
        .await
        .expect_err("the flag lowered alpha's cap to one");
    Ok(())
}

/// A flag that stops resolving keeps its last valid value for
/// `KEY_SET_STALENESS`, so a short provider outage moves no client between
/// keys; past that, the key set is the env allowlist alone.
#[test]
fn the_last_valid_flag_stands_through_a_short_outage() {
    let retention = FlagRetention::default();
    let start = std::time::Instant::now();
    let listed = flag(serde_json::json!({ "live": { "beta": 9 } }));
    assert_eq!(retention.get(None, start), None, "nothing resolved yet");
    assert_eq!(
        retention.get(Some(listed.clone()), start),
        Some(listed.clone())
    );
    assert_eq!(
        retention.get(None, start + KEY_SET_STALENESS),
        Some(listed),
        "an outage within the bound keeps the key set"
    );
    assert_eq!(
        retention.get(None, start + KEY_SET_STALENESS + Duration::from_secs(1)),
        None,
        "past the bound, the env allowlist alone"
    );
}
