use std::collections::BTreeSet;

use pplx_space_feature_flags::FeatureFlagClient;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::redis::Redis;
use uuid::Uuid;

use super::redis_limiter::RedisConcurrencyStore;
use super::*;

use pplx_space_test_env::TestResult;

// ---------------------------------------------------------------------------
// Pure config / classification / cap-resolution logic (no Redis).
// ---------------------------------------------------------------------------

fn known_set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|s| s.to_string()).collect()
}

/// [`ClientKey::classify`] against `config`'s static allowlist, as the
/// limiter built from `config` would resolve it.
fn classify(
    client_id: Option<&str>,
    config: &ClientConcurrencyConfig,
    overrides: Option<&QuotaOverrides>,
) -> ClientKey {
    ClientKey::classify(
        client_id,
        &KnownClients::new(&config.known_clients),
        overrides,
    )
}

fn env_caps() -> ClientConcurrencyConfig {
    ClientConcurrencyConfig {
        mode: Mode::Enforce,
        max_in_flight_per_client: 128,
        max_in_flight_unspecified: 16,
        known_clients: known_set(&["foreground_client", "scheduled-client", "background-client"]),
        max_hold: Duration::from_secs(120),
        redis_url: None,
    }
}

#[test]
fn config_defaults_are_conservative_observe_mode() {
    let config = ClientConcurrencyConfig::default();
    assert_eq!(config.mode, Mode::Observe);
    assert_eq!(config.max_in_flight_per_client, DEFAULT_MAX_PER_CLIENT);
    assert_eq!(config.max_in_flight_unspecified, DEFAULT_MAX_UNSPECIFIED);
    // The shared bucket is tighter than a per-client bucket.
    assert!(config.max_in_flight_unspecified < config.max_in_flight_per_client);
    assert!(config.known_clients.is_empty());
    assert_eq!(config.max_hold, DEFAULT_MAX_HOLD);
    assert!(config.max_hold > crate::controllers::substrate::NODE_RPC_TIMEOUT);
    assert!(config.redis_url.is_none());
}

#[test]
fn mode_parses_known_values_case_insensitively() {
    assert_eq!("observe".parse::<Mode>().ok(), Some(Mode::Observe));
    assert_eq!(" ENFORCE ".parse::<Mode>().ok(), Some(Mode::Enforce));
    "nonsense".parse::<Mode>().unwrap_err();
    assert_eq!(Mode::Enforce.as_str(), "enforce");
    assert_eq!(
        Mode::Observe.to_string().parse::<Mode>().ok(),
        Some(Mode::Observe)
    );
}

#[test]
fn classify_routes_only_allowlisted_ids_to_their_own_bucket() {
    let config = env_caps();

    // Known id → its own bucket.
    assert!(matches!(
        classify(Some("foreground_client"), &config, None),
        ClientKey::Client(_)
    ));
    // No id, blank id, and *unrecognized* id all collapse to the shared bucket —
    // this is the fix for the per-request-random-id bypass.
    for shared in [
        None,
        Some(""),
        Some("   "),
        Some("rand-abc-123"),
        Some("unspecified"),
    ] {
        assert_eq!(
            classify(shared, &config, None),
            ClientKey::Unspecified,
            "{shared:?} must share the unspecified bucket"
        );
    }
}

#[test]
fn eppo_override_key_allowlists_a_client_at_runtime() {
    // A config whose static allowlist does NOT include `newbot`.
    let config = ClientConcurrencyConfig {
        known_clients: known_set(&["foreground_client"]),
        ..env_caps()
    };
    let overrides: QuotaOverrides =
        serde_json::from_value(serde_json::json!({ "overrides": { "newbot": 5 } })).unwrap();

    // Without the flag `newbot` is unknown → shared bucket.
    assert_eq!(
        classify(Some("newbot"), &config, None),
        ClientKey::Unspecified
    );
    // With the override present, `newbot` is allowlisted → its own bucket.
    assert_eq!(
        classify(Some("newbot"), &config, Some(&overrides)),
        ClientKey::Client("newbot".into())
    );
}

#[test]
fn cap_for_falls_back_to_env_when_no_overrides() {
    let config = env_caps();
    let foreground_client = classify(Some("foreground_client"), &config, None);
    assert_eq!(config.cap_for(&foreground_client, None), 128);
    assert_eq!(config.cap_for(&ClientKey::Unspecified, None), 16);
}

#[test]
fn eppo_overrides_take_precedence_over_env() {
    let json = serde_json::json!({
        "unspecified": 4,
        "overrides": { "foreground_client": 256, "scheduled-client": 32 },
    });
    let overrides: QuotaOverrides = serde_json::from_value(json).unwrap();
    let config = env_caps();
    let cap = |id| config.cap_for(&classify(id, &config, Some(&overrides)), Some(&overrides));

    // Per-client override wins.
    assert_eq!(cap(Some("foreground_client")), 256);
    assert_eq!(cap(Some("scheduled-client")), 32);
    // Allowlisted, but no explicit override → env per-client cap (no `default`).
    assert_eq!(cap(Some("background-client")), 128);
    // Shared bucket → flag `unspecified`.
    assert_eq!(cap(None), 4);
    // Unrecognized id → shared bucket, same `unspecified` cap.
    assert_eq!(cap(Some("rand-xyz")), 4);
}

#[test]
fn partial_eppo_overrides_fall_back_to_env_per_field() {
    let json = serde_json::json!({ "overrides": { "scheduled-client": 8 } });
    let overrides: QuotaOverrides = serde_json::from_value(json).unwrap();
    let config = env_caps();
    let cap = |id| config.cap_for(&classify(id, &config, Some(&overrides)), Some(&overrides));

    assert_eq!(cap(Some("scheduled-client")), 8);
    // Allowlisted, no override → env per-client cap.
    assert_eq!(cap(Some("foreground_client")), 128);
    // No `unspecified` in the flag → env cap for the shared bucket.
    assert_eq!(cap(None), 16);
}

#[test]
fn quota_overrides_ignores_unknown_json_fields() {
    let json = serde_json::json!({ "unspecified": 10, "future_field": "ignored" });
    let overrides: QuotaOverrides = serde_json::from_value(json).unwrap();
    assert_eq!(overrides.unspecified, Some(10));
}

/// The override map reads the same whether it arrives as an object or as the
/// JSON text flagd hands back for a nested value.
///
/// flagd 0.2.2 stringifies any nested object in a flag variant, so without
/// this the whole assignment fails to deserialize and every cap silently
/// reverts to its env value — a flag that looks configured and does nothing.
/// Both encodings are compared against the same parsed result, because what
/// matters is that a deployment gets identical caps whichever backend serves
/// the flag.
#[test]
fn quota_overrides_read_the_same_when_flagd_stringifies_the_map() {
    let as_object = serde_json::json!({
        "unspecified": 10,
        "overrides": { "acme": 32, "scheduled-client": 4 },
    });
    let as_text = serde_json::json!({
        "unspecified": 10,
        "overrides": r#"{"acme":32,"scheduled-client":4}"#,
    });

    let from_object: QuotaOverrides = serde_json::from_value(as_object).unwrap();
    let from_text: QuotaOverrides = serde_json::from_value(as_text).unwrap();

    assert_eq!(from_text.unspecified, Some(10));
    assert_eq!(from_text.overrides.get("acme"), Some(&32));
    assert_eq!(from_text.overrides.get("scheduled-client"), Some(&4));
    assert_eq!(from_object.overrides, from_text.overrides);
}

/// A string under `overrides` that is not an encoded cap map is still an
/// error, so the flagd workaround does not become a hole that accepts any
/// malformed assignment.
#[test]
fn a_stringified_override_map_that_is_not_a_cap_map_is_rejected() {
    for encoded in [r#"{"acme":"lots"}"#, r#"["acme"]"#, "acme=32", "{"] {
        let json = serde_json::json!({ "overrides": encoded });
        assert!(
            serde_json::from_value::<QuotaOverrides>(json).is_err(),
            "{encoded:?} should not parse as an override map"
        );
    }
}

/// A rejected override says what is wrong with it.
///
/// Stated as the operator sees it: a bad cap reverts *every* client to its
/// env-configured value, and the logged parse error is the only thing that
/// says which entry to go fix. Dispatching the two accepted shapes through an
/// untagged enum satisfies the rejection test above while reporting nothing
/// but "data did not match any variant", which names neither the entry nor
/// the problem — so the message is asserted here rather than just the `Err`.
#[test]
fn a_rejected_override_map_reports_the_offending_value() {
    let as_object = serde_json::json!({ "overrides": { "acme": "lots" } });
    let error = serde_json::from_value::<QuotaOverrides>(as_object)
        .expect_err("a non-numeric cap should be rejected");
    let message = error.to_string();
    assert!(
        message.contains("lots") && message.contains("usize"),
        "the error should name the bad value and the expected type, got {message:?}"
    );

    let as_text = serde_json::json!({ "overrides": r#"{"acme":"lots"}"# });
    let error = serde_json::from_value::<QuotaOverrides>(as_text)
        .expect_err("a non-numeric cap should be rejected inside the encoded form too");
    let message = error.to_string();
    assert!(
        message.contains("lots") && message.contains("usize"),
        "the encoded form's error should name the bad value too, got {message:?}"
    );
}

#[test]
fn redis_key_is_namespaced_and_distinguishes_unspecified() {
    assert_eq!(
        ClientKey::Client("foreground_client".into()).redis_key(),
        "space:api:client_concurrency:v1:client:foreground_client"
    );
    assert_eq!(
        ClientKey::Unspecified.redis_key(),
        "space:api:client_concurrency:v1:unspecified"
    );
    // Even a client literally named like the shared sentinel keys under
    // `:client:`, so it can't collide with the shared bucket.
    for adversarial in ["unspecified", "__unspecified__"] {
        assert_ne!(
            ClientKey::Client(adversarial.into()).redis_key(),
            ClientKey::Unspecified.redis_key(),
            "client id {adversarial:?} must not share the shared-bucket key"
        );
    }
}

/// The env-master-first policy as this limiter observes it, asserted through
/// what the limiter *does* rather than what it reports.
///
/// The observable is rejection at the cap. With a real store and a cap of one,
/// an active limiter rejects a second concurrent acquire; an inactive one fails
/// open and admits it. That difference is the gate, and it needs Redis — with
/// no store even an active limiter admits (as `misconfigured`), which is why
/// the cheaper no-Redis version of this test had to read a decision tag to see
/// anything at all.
///
/// Asserted at the call site rather than on `limiter_active`, because a helper
/// that is right and a call site that ignores it is the failure this exists to
/// catch.
#[tokio::test]
async fn the_limiter_applies_the_env_master_first_policy() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };

    /// Whether the limiter shapes traffic: hold one slot at a cap of one, then
    /// see whether a second acquire is refused.
    ///
    /// A fresh client id per call, so a slot held by one row cannot outlive its
    /// guard into the next.
    async fn shapes_traffic(url: &str, enabled: bool, feature_flags: FeatureFlagClient) -> bool {
        let client_id = format!("foreground_client-{}", Uuid::now_v7());
        let limiter = ClientConcurrencyLimiter::with_config(
            ClientConcurrencyConfig {
                mode: if enabled { Mode::Enforce } else { Mode::Off },
                redis_url: Some(url.to_string()),
                ..tight_enforce(&client_id)
            },
            feature_flags,
        );

        let _held = admit(
            limiter
                .try_acquire(Operation::Create, Some(&client_id))
                .await,
        );
        matches!(
            limiter
                .try_acquire(Operation::Reconnect, Some(&client_id))
                .await,
            ClientConcurrencyDecision::Rejected { .. }
        )
    }

    let assignment = |value: bool| {
        FeatureFlagClient::fixed_bools([(SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG.key(), value)])
    };

    // Env off is the hard off: no assignment, not even a live `true`, brings
    // the limiter back.
    assert!(!shapes_traffic(&url, false, FeatureFlagClient::disabled()).await);
    assert!(!shapes_traffic(&url, false, assignment(true)).await);

    // Env on: the assignment decides, and the only direction it can move a
    // limiter the environment enabled is off.
    assert!(shapes_traffic(&url, true, assignment(true)).await);
    assert!(!shapes_traffic(&url, true, assignment(false)).await);

    // Env on with nothing resolved. No provider installed keeps what the
    // environment asked for; a provider with no assignment falls back to the
    // flag's declaration. Both are on for this guardrail.
    assert!(shapes_traffic(&url, true, FeatureFlagClient::disabled()).await);
    assert!(
        shapes_traffic(
            &url,
            true,
            FeatureFlagClient::fixed_bools([("other", true)])
        )
        .await
    );

    Ok(())
}

/// An override is configuration, not a gate, so the env master suppresses it
/// rather than letting it retune caps for a limiter the environment turned off.
#[test]
fn quota_overrides_are_suppressed_when_the_env_master_is_off() {
    let quotas = || {
        FeatureFlagClient::fixed_json([(
            SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
            serde_json::json!({"unspecified": 3}),
        )])
    };
    let limiter = |enabled: bool| {
        ClientConcurrencyLimiter::with_config(
            ClientConcurrencyConfig {
                mode: if enabled { Mode::Enforce } else { Mode::Off },
                redis_url: None,
                ..env_caps()
            },
            quotas(),
        )
    };

    assert_eq!(
        limiter(true).quota_overrides(),
        Some(QuotaOverrides {
            unspecified: Some(3),
            overrides: std::collections::HashMap::new(),
        }),
    );
    assert_eq!(limiter(false).quota_overrides(), None);
}

#[test]
fn known_clients_env_unset_is_empty() {
    let clients = parse_known_clients_env("SPACE_TEST_CLIENTS_DEFINITELY_UNSET");

    assert!(clients.is_empty());
}

#[test]
fn known_clients_parse_trims_deduplicates_and_drops_empty_tokens() {
    assert_eq!(
        parse_known_clients(
            "foreground_client, scheduled-client ,,foreground_client,background-client"
        ),
        known_set(&["foreground_client", "scheduled-client", "background-client"])
    );
    for raw in ["", "   ", " , , "] {
        assert!(parse_known_clients(raw).is_empty());
    }
}

// ---------------------------------------------------------------------------
// Fail-open policy (no Docker needed).
// ---------------------------------------------------------------------------

fn admit(decision: ClientConcurrencyDecision) -> ClientConcurrencyGuard {
    match decision {
        ClientConcurrencyDecision::Admitted(guard) => guard,
        ClientConcurrencyDecision::Rejected { .. } => panic!("expected admitted, got rejected"),
    }
}

fn assert_rejected(decision: ClientConcurrencyDecision) {
    match decision {
        ClientConcurrencyDecision::Rejected { retry_after, .. } => {
            assert_eq!(retry_after, RETRY_AFTER)
        }
        ClientConcurrencyDecision::Admitted(_) => panic!("expected rejected, got admitted"),
    }
}

fn limiter_with(config: ClientConcurrencyConfig) -> ClientConcurrencyLimiter {
    ClientConcurrencyLimiter::with_config(config, FeatureFlagClient::disabled())
}

#[tokio::test]
async fn missing_store_fails_open() {
    let limiter = limiter_with(ClientConcurrencyConfig {
        max_in_flight_per_client: 1,
        redis_url: None,
        ..env_caps()
    });
    let _g = admit(
        limiter
            .try_acquire(Operation::Create, Some("foreground_client"))
            .await,
    );
    let _h = admit(
        limiter
            .try_acquire(Operation::Create, Some("foreground_client"))
            .await,
    );
}

#[tokio::test]
async fn zero_cap_admits_untracked() {
    let limiter = limiter_with(ClientConcurrencyConfig {
        max_in_flight_per_client: 0,
        redis_url: Some("redis://127.0.0.1:1".to_string()),
        ..env_caps()
    });
    let _g = admit(
        limiter
            .try_acquire(Operation::Create, Some("foreground_client"))
            .await,
    );
}

#[tokio::test]
async fn unreachable_store_fails_open() {
    let limiter = limiter_with(ClientConcurrencyConfig {
        max_in_flight_per_client: 1,
        redis_url: Some("redis://127.0.0.1:1".to_string()),
        ..env_caps()
    });
    let _g = admit(
        limiter
            .try_acquire(Operation::Create, Some("foreground_client"))
            .await,
    );
    let _h = admit(
        limiter
            .try_acquire(Operation::Create, Some("foreground_client"))
            .await,
    );
}

#[test]
fn key_set_entries_that_cant_name_a_client_are_ignored() {
    // Reserved keys in any case, then entries that aren't Kubernetes label
    // values: a space, an edge that isn't alphanumeric, a DogStatsD separator,
    // and one character past the 63-character limit.
    let rejected = [
        "unknown",
        "Unknown",
        "overflow",
        "OVERFLOW",
        "native",
        "Native",
        "two words",
        "-leading-dash",
        "a|b",
        "a,b",
        &"x".repeat(64),
    ];
    let accepted = ["foreground_client", "batch.7", &"x".repeat(63)];
    let entries: Vec<&str> = rejected.iter().chain(&accepted).copied().collect();

    // Each entry once in the static allowlist, once only as an override key.
    let static_only = ClientConcurrencyConfig {
        known_clients: known_set(&entries),
        ..env_caps()
    };
    let override_only = ClientConcurrencyConfig {
        known_clients: BTreeSet::new(),
        ..env_caps()
    };
    let overrides = QuotaOverrides {
        overrides: entries.iter().map(|id| (id.to_string(), 5)).collect(),
        ..QuotaOverrides::default()
    };

    for (config, overrides) in [(&static_only, None), (&override_only, Some(&overrides))] {
        for id in rejected {
            assert_eq!(
                classify(Some(id), config, overrides),
                ClientKey::Unspecified,
                "{id:?} can't name a client, so it must share the unspecified bucket"
            );
        }
        for id in accepted {
            assert_eq!(
                classify(Some(id), config, overrides),
                ClientKey::Client(id.to_string().into()),
                "{id:?} is a valid key-set entry"
            );
        }
    }
}

#[test]
fn override_only_ids_keep_their_bucket_past_the_interning_cap() {
    let config = ClientConcurrencyConfig {
        known_clients: BTreeSet::new(),
        ..env_caps()
    };
    // Unique ids so no other test in this process has interned them, and more
    // of them than the cap can hold however many runtime ids are already
    // interned.
    let ids: Vec<String> = (0..=MAX_RUNTIME_CLIENT_IDS)
        .map(|n| format!("rotating-override-{}-{n}", Uuid::now_v7()))
        .collect();
    let overrides = QuotaOverrides {
        overrides: ids.iter().map(|id| (id.clone(), 5)).collect(),
        ..QuotaOverrides::default()
    };

    // Interning bounds memory, not buckets: every override-only id keeps its
    // own bucket and cap however full the interning cap is.
    let interned: Vec<Option<&'static str>> =
        ids.iter().map(|id| intern_override_client_id(id)).collect();
    for id in &ids {
        let key = classify(Some(id), &config, Some(&overrides));
        assert_eq!(key, ClientKey::Client(id.clone().into()));
        assert_eq!(config.cap_for(&key, Some(&overrides)), 5);
    }

    // The interned set stops growing at the cap, and an id interned before it
    // filled resolves to the same copy again rather than leaking another.
    assert_eq!(interned.last(), Some(&None));
    for (id, copy) in ids.iter().zip(&interned) {
        if let Some(copy) = copy {
            let again = intern_override_client_id(id).expect("interned ids stay interned");
            assert!(std::ptr::eq(*copy, again));
        }
    }
}

// ---------------------------------------------------------------------------
// Redis-backed behavior (testcontainers, or SPACE_TEST_REDIS_URL).
// ---------------------------------------------------------------------------

/// Prefers `SPACE_TEST_REDIS_URL` so the Lua can be exercised without Docker;
/// otherwise starts a throwaway container. `None` skips the test, which only a
/// run that opted out of required test dependencies allows.
async fn try_redis_url() -> TestResult<Option<(Option<ContainerAsync<Redis>>, String)>> {
    if let Ok(url) = std::env::var("SPACE_TEST_REDIS_URL")
        && !url.trim().is_empty()
    {
        return Ok(Some((None, url.trim().to_string())));
    }
    let (image_name, image_tag) = pplx_space_db_test_fixtures::redis_image_ref()?;
    let node = match Redis::default()
        .with_name(image_name)
        .with_tag(image_tag)
        .start()
        .await
    {
        Ok(node) => node,
        Err(error) => {
            pplx_space_db_test_fixtures::report_unavailable_dependency("redis", &error)?;
            return Ok(None);
        }
    };
    let port = node.get_host_port_ipv4(6379).await?;
    Ok(Some((Some(node), format!("redis://127.0.0.1:{port}"))))
}

async fn try_store() -> TestResult<Option<(Option<ContainerAsync<Redis>>, RedisConcurrencyStore)>> {
    match try_redis_url().await? {
        Some((node, url)) => Ok(Some((node, RedisConcurrencyStore::from_url(&url)?))),
        None => Ok(None),
    }
}

fn unique_key() -> String {
    format!("{KEY_PREFIX}:test:{}", Uuid::now_v7())
}

const HOLD: Duration = Duration::from_secs(60);
const KEY_TTL: Duration = Duration::from_secs(120);

#[tokio::test]
async fn store_admits_up_to_cap_then_reports_over_cap() -> TestResult {
    let Some((_node, store)) = try_store().await? else {
        return Ok(());
    };
    let key = unique_key();

    let a = store
        .acquire(&key, 2, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert!(a.added && !a.over_cap && a.count == 1);
    let b = store
        .acquire(&key, 2, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert!(b.added && !b.over_cap && b.count == 2);
    let c = store
        .acquire(&key, 2, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert!(!c.added && c.over_cap, "third acquire must be rejected");
    Ok(())
}

#[tokio::test]
async fn store_force_admits_over_cap_for_observe_mode() -> TestResult {
    let Some((_node, store)) = try_store().await? else {
        return Ok(());
    };
    let key = unique_key();

    store
        .acquire(&key, 1, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    let forced = store
        .acquire(&key, 1, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), true)
        .await?;
    assert!(forced.added && forced.over_cap && forced.count == 2);
    Ok(())
}

#[tokio::test]
async fn store_release_frees_a_slot() -> TestResult {
    let Some((_node, store)) = try_store().await? else {
        return Ok(());
    };
    let key = unique_key();
    let member = Uuid::now_v7().to_string();

    store
        .acquire(&key, 1, HOLD, KEY_TTL, &member, false)
        .await?;
    let blocked = store
        .acquire(&key, 1, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert!(!blocked.added);

    store.release(&key, &member).await?;
    let after = store
        .acquire(&key, 1, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert!(after.added && after.count == 1);
    Ok(())
}

#[tokio::test]
async fn store_reclaims_expired_slots() -> TestResult {
    let Some((_node, store)) = try_store().await? else {
        return Ok(());
    };
    let key = unique_key();

    store
        .acquire(
            &key,
            1,
            Duration::from_secs(1),
            KEY_TTL,
            &Uuid::now_v7().to_string(),
            false,
        )
        .await?;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let after = store
        .acquire(
            &key,
            1,
            Duration::from_secs(1),
            KEY_TTL,
            &Uuid::now_v7().to_string(),
            false,
        )
        .await?;
    assert!(
        after.added && after.count == 1,
        "expired slot must be reclaimed"
    );
    Ok(())
}

#[tokio::test]
async fn concurrent_acquires_cannot_overspend_the_cap() -> TestResult {
    let Some((_node, store)) = try_store().await? else {
        return Ok(());
    };
    let key = unique_key();
    let cap = 5;

    let mut handles = Vec::new();
    for _ in 0..50 {
        let store = store.clone();
        let key = key.clone();
        handles.push(tokio::spawn(async move {
            store
                .acquire(&key, cap, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
                .await
                .map(|o| o.added)
        }));
    }
    let mut admitted = 0;
    for handle in handles {
        if handle.await?? {
            admitted += 1;
        }
    }
    assert_eq!(
        admitted, cap,
        "exactly `cap` of the racing acquires may win"
    );
    Ok(())
}

fn redis_limiter(url: &str, config: ClientConcurrencyConfig) -> ClientConcurrencyLimiter {
    limiter_with(ClientConcurrencyConfig {
        redis_url: Some(url.to_string()),
        ..config
    })
}

/// An enforce-mode config keyed to a single allowlisted `client`, sized so one
/// slot fills the per-client and unspecified caps.
fn tight_enforce(client: &str) -> ClientConcurrencyConfig {
    ClientConcurrencyConfig {
        mode: Mode::Enforce,
        max_in_flight_per_client: 1,
        max_in_flight_unspecified: 1,
        known_clients: known_set(&[client]),
        max_hold: HOLD,
        redis_url: None,
    }
}

#[tokio::test]
async fn disabled_limiter_short_circuits_before_the_store() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let client = format!("foreground_client-{}", Uuid::now_v7());
    // Kill-switch OFF, but a reachable store + cap > 0: the disabled check must
    // short-circuit before Redis, so no slot is ever reserved. Distinguishes the
    // `Disabled` early-return from the fail-open store paths — if the kill-switch
    // check were removed, the call would reserve a slot and the probe below would
    // see count 2.
    let limiter = redis_limiter(
        &url,
        ClientConcurrencyConfig {
            mode: Mode::Off,
            ..tight_enforce(&client)
        },
    );
    let _g = admit(limiter.try_acquire(Operation::Create, Some(&client)).await);

    // Probe the shared store directly: the disabled call reserved nothing, so
    // this probe's own slot is the only member (count 1).
    let store = RedisConcurrencyStore::from_url(&url)?;
    let key = ClientKey::Client(client.clone().into()).redis_key();
    let probe = store
        .acquire(&key, 100, HOLD, KEY_TTL, &Uuid::now_v7().to_string(), false)
        .await?;
    assert_eq!(probe.count, 1, "disabled limiter must not reserve a slot");
    Ok(())
}

#[tokio::test]
async fn limiter_enforces_cluster_wide_per_client_cap() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let client = format!("foreground_client-{}", Uuid::now_v7());
    // Two independent limiter instances share the store → simulates two replicas.
    let replica_a = redis_limiter(&url, tight_enforce(&client));
    let replica_b = redis_limiter(&url, tight_enforce(&client));

    let _held = admit(
        replica_a
            .try_acquire(Operation::Create, Some(&client))
            .await,
    );
    // The *other* replica sees the shared count and rejects — the whole point.
    assert_rejected(
        replica_b
            .try_acquire(Operation::Reconnect, Some(&client))
            .await,
    );
    Ok(())
}

#[tokio::test]
async fn limiter_unknown_ids_share_one_throttled_bucket() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    // Unlike every other bucket in this suite, `ClientKey::Unspecified`'s Redis
    // key is fixed by production semantics (see `redis_key_is_namespaced...`),
    // not test-namespaced. On a shared/reused Redis (including a nextest retry
    // of this same test), a leftover member from a prior run would make the
    // bucket look pre-occupied and flake the very first acquire below. Reset it.
    RedisConcurrencyStore::from_url(&url)?
        .clear(&ClientKey::Unspecified.redis_key())
        .await?;

    // `known` is allowlisted; everything else shares the unspecified bucket (cap 1).
    let known = format!("foreground_client-{}", Uuid::now_v7());
    let limiter = redis_limiter(&url, tight_enforce(&known));
    let tag = Uuid::now_v7();

    // Two *different* unknown ids contend on the ONE shared bucket: the second
    // is rejected — a rotating/random id can't mint itself a fresh allowance.
    let _anon = admit(
        limiter
            .try_acquire(Operation::Create, Some(&format!("rand-a-{tag}")))
            .await,
    );
    assert_rejected(
        limiter
            .try_acquire(Operation::Create, Some(&format!("rand-b-{tag}")))
            .await,
    );
    // The allowlisted client has its own bucket, unaffected by the full shared one.
    let _known = admit(limiter.try_acquire(Operation::Create, Some(&known)).await);
    Ok(())
}

#[tokio::test]
async fn limiter_guard_drop_releases_the_shared_slot() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let client = format!("foreground_client-{}", Uuid::now_v7());
    let limiter = redis_limiter(&url, tight_enforce(&client));

    // Models composition with the cluster limiter: acquire, then a downstream
    // rejection drops the guard, which must free the shared slot.
    drop(admit(
        limiter.try_acquire(Operation::Create, Some(&client)).await,
    ));

    // Release is fire-and-forget on drop; poll until the slot frees.
    let mut freed = false;
    for _ in 0..40 {
        match limiter.try_acquire(Operation::Create, Some(&client)).await {
            ClientConcurrencyDecision::Admitted(_) => {
                freed = true;
                break;
            }
            ClientConcurrencyDecision::Rejected { .. } => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    assert!(freed, "guard drop must release the shared slot");
    Ok(())
}

#[tokio::test]
async fn limiter_observe_mode_never_rejects() -> TestResult {
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    let client = format!("foreground_client-{}", Uuid::now_v7());
    let limiter = redis_limiter(
        &url,
        ClientConcurrencyConfig {
            mode: Mode::Observe,
            ..tight_enforce(&client)
        },
    );

    let _a = admit(limiter.try_acquire(Operation::Create, Some(&client)).await);
    // Over cap, but observe admits anyway (would_reject metered).
    let _b = admit(limiter.try_acquire(Operation::Create, Some(&client)).await);
    Ok(())
}
