use pplx_space_common_core::types::ClientKey;

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use pplx_space_db_test_fixtures::try_pg_test_db;
use sqlx::PgPool;
use testcontainers::ContainerAsync;
use testcontainers_modules::redis::Redis;
use tracing::Subscriber;
use tracing_subscriber::{Layer, layer::Context, prelude::*, registry::LookupSpan};
use uuid::Uuid;

use super::redis_bucket::{BucketSpec, RedisTokenBucket};
use super::*;
use crate::controllers::substrate::Substrate;
// Direct is only a placeholder capacity source for most of this module's tests:
// they need zero or some eligible nodes. `capacity_probe_failure_fails_open`
// needs a count that fails, which only Direct's can.
use crate::test_support::{
    direct_substrate, enabled_config, seed_start_eligible_node, try_redis_admission, try_redis_url,
};

use pplx_space_test_env::TestResult;

fn admission(
    substrate: Substrate,
    config: ClusterSandboxAdmissionConfig,
    bucket: Option<RedisTokenBucket>,
) -> ClusterSandboxAdmission {
    // No provider installed, which under the env-master-first policy leaves the
    // environment authoritative — so admission behavior here stays driven by
    // config, as every test below but the policy one wants.
    admission_with_flags(substrate, config, bucket, FeatureFlagClient::disabled())
}

fn admission_with_flags(
    substrate: Substrate,
    config: ClusterSandboxAdmissionConfig,
    bucket: Option<RedisTokenBucket>,
    feature_flags: FeatureFlagClient,
) -> ClusterSandboxAdmission {
    ClusterSandboxAdmission {
        substrate,
        bucket,
        config: sanitize_config(config),
        feature_flags,
    }
}

async fn try_redis_bucket() -> TestResult<Option<(Option<ContainerAsync<Redis>>, RedisTokenBucket)>>
{
    match try_redis_url().await? {
        Some((node, url)) => Ok(Some((node, RedisTokenBucket::from_url(&url)?))),
        None => Ok(None),
    }
}

fn spec(key: &str, refill_rps: f64, burst: f64) -> BucketSpec<'_> {
    BucketSpec {
        key,
        refill_rps,
        burst,
    }
}

#[test]
fn sanitize_config_falls_back_to_absolute_cluster_defaults() {
    let config = sanitize_config(ClusterSandboxAdmissionConfig {
        enabled: false,
        refill_rps: f64::NAN,
        burst: 0.0,
        redis_url: None,
        client_rate: None,
    });

    assert!(!config.enabled);
    assert!((config.refill_rps - DEFAULT_REFILL_RPS).abs() < f64::EPSILON);
    assert!((config.burst - DEFAULT_BURST).abs() < f64::EPSILON);
}

#[test]
fn sanitize_config_clamps_sub_one_burst_up_to_one() {
    // A burst below one token can never satisfy the one-token consume, which
    // would permanently rate-limit the cluster; it must clamp up to 1.0.
    let config = sanitize_config(ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: 6.0,
        burst: 0.25,
        redis_url: None,
        client_rate: None,
    });
    assert!((config.burst - 1.0).abs() < f64::EPSILON);
}

#[test]
fn should_reject_empty_only_on_confident_zero() {
    // Confident zero nodes, or a non-positive refill rate, reject.
    assert!(should_reject_empty(Some(0), 6.0));
    assert!(should_reject_empty(Some(4), 0.0));
    // Unknown capacity (cold node cache) and any positive count fail open.
    assert!(!should_reject_empty(None, 6.0));
    assert!(!should_reject_empty(Some(1), 6.0));
}

/// The env-master-first policy as this limiter observes it, asserted on
/// `admission_enabled` rather than on the helper it calls: a helper that is
/// right and a call site that ignores it is the failure this is here to catch.
///
/// `limiter_active`, the hand-rolled `env && flag` this replaced, was unit
/// tested on its own and told you nothing about whether the limiter consulted
/// it.
#[tokio::test]
async fn admission_enabled_applies_the_env_master_first_policy() {
    // Lazy pool: admission_enabled() reads only config + the flag client, never
    // the substrate, so no live database is needed. `connect_lazy` still wants a
    // Tokio context for its internal tasks, hence the async test.
    let pool = PgPool::connect_lazy("postgres://unused").expect("lazy pool");
    let with_flags = |enabled: bool, flags: FeatureFlagClient| {
        admission_with_flags(
            direct_substrate(&pool),
            ClusterSandboxAdmissionConfig {
                enabled,
                ..enabled_config()
            },
            None,
            flags,
        )
    };
    let assignment = |value: bool| {
        FeatureFlagClient::fixed_bools([(SPACE_CLUSTER_RATE_LIMITER_FLAG.key(), value)])
    };
    let no_provider = FeatureFlagClient::disabled;

    // Env off is the hard off: no assignment, not even a live `true`, brings
    // the limiter back.
    assert!(!with_flags(false, no_provider()).admission_enabled());
    assert!(!with_flags(false, assignment(true)).admission_enabled());

    // Env on: the assignment decides, and the only direction it can move a
    // limiter the environment enabled is off.
    assert!(with_flags(true, assignment(true)).admission_enabled());
    assert!(!with_flags(true, assignment(false)).admission_enabled());

    // Env on with nothing resolved, by either route: no provider installed and
    // a provider with no assignment both take the flag's declaration, which is
    // on for this guardrail. That is the point — an unreachable Eppo can never
    // silently drop it.
    assert!(with_flags(true, no_provider()).admission_enabled());
    assert!(
        with_flags(true, FeatureFlagClient::fixed_bools([("other", true)])).admission_enabled()
    );
}

#[tokio::test]
async fn capacity_probe_failure_fails_open() {
    // A substrate whose capacity probe errors: lazy pool at a dead port with a
    // short acquire timeout so the inventory `COUNT` fails fast. The limiter must
    // never be a single point of failure on the create path, so a probe *error*
    // fails open (admits) just like a Redis error — it must not surface as a hard
    // error that rejects the activation.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(std::time::Duration::from_millis(200))
        .connect_lazy("postgres://127.0.0.1:1/unused")
        .expect("lazy pool");
    let admission = admission(direct_substrate(&pool), enabled_config(), None);

    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await
        .expect("capacity-probe failure must fail open (admit), not reject the create");
}

/// The window a hint sampled above `earliest` is allowed to land in.
///
/// Mirrors `retry_after_with_jitter`'s clamp so the helper is total over any
/// `earliest`, including the saturated one `earliest_refill` can return — a bare
/// `earliest + jitter` would overflow `Duration`'s `Add` and panic instead of
/// reporting. No caller passes a saturated base today; this keeps the helper
/// from being the thing that breaks when one does.
///
/// Because it recomputes that clamp, it **cannot** distinguish clamping the base
/// from clamping the sum: for a 10,000s base a flat 3600s sits inside
/// `[3600, 3604]`. That property is owned by
/// `an_unusable_refill_rate_is_capped_without_flattening_the_jitter`, which
/// compares against the exact reachable set instead.
#[track_caller]
fn assert_hint_in_window(hint: Duration, earliest: Duration) {
    let base = Duration::from_secs(
        earliest
            .as_secs()
            .clamp(RETRY_AFTER_FLOOR.as_secs(), RETRY_AFTER_CAP.as_secs()),
    );
    let widest = base + RETRY_AFTER_JITTER;
    assert!(
        (base..=widest).contains(&hint),
        "hint {hint:?} escaped [{base:?}, {widest:?}]"
    );
}

#[test]
fn retry_after_hint_spreads_over_every_whole_second_from_one_to_five() {
    // One assertion for the whole policy, against a literal oracle rather than
    // the constants the code reads: the reachable hint set is exactly
    // {1s..5s}. That pins the bounds, proves both ends and every second
    // between them are drawn, and fails on a constant — the defect this
    // replaces, where `(deficit / refill_rps).ceil().max(1.0)` over a `deficit`
    // the Lua bucket confines to `(0, 1]` handed every shed caller the same
    // second at 6/s and 60/s alike.
    //
    // 200 draws miss one of five values with probability at most 5 * (4/5)^200,
    // about 2e-19, so this is deterministic in practice.
    let sampled: BTreeSet<Duration> = (0..200)
        .map(|_| jittered_retry_after(RETRY_AFTER_FLOOR))
        .collect();
    let expected: BTreeSet<Duration> = (1..=5).map(Duration::from_secs).collect();
    assert_eq!(sampled, expected);
}

#[test]
fn widest_jitter_offset_lands_on_the_gateway_retry_after() {
    // Algebra, not a property: while the width is *derived* from
    // `GATEWAY_RETRY_AFTER` this reduces to `FLOOR + (GATEWAY - FLOOR)` and
    // cannot fail. Its value is one step later — once someone replaces the
    // derivation with a literal, this is what fails the next time
    // `GATEWAY_RETRY_AFTER` moves, which the set-equality test would not catch.
    //
    // From a one-second base, the widest offset lands exactly on
    // `GATEWAY_RETRY_AFTER`, so a shed activation advertises no longer a wait
    // than a converging sandbox.
    // Deliberately not the unqualified property: below one token per second the
    // base is refill-derived and the hint legitimately exceeds that bound (see
    // `earliest_refill_is_a_real_wait_below_one_token_per_second`). Asserted
    // rather than left in prose because deriving the width from
    // `GATEWAY_RETRY_AFTER` is what stops the two drifting.
    assert_eq!(
        retry_after_with_jitter(RETRY_AFTER_FLOOR, Duration::ZERO),
        RETRY_AFTER_FLOOR
    );
    assert_eq!(
        retry_after_with_jitter(RETRY_AFTER_FLOOR, RETRY_AFTER_JITTER),
        crate::controllers::substrate::GATEWAY_RETRY_AFTER
    );
}

#[test]
fn earliest_refill_collapses_to_the_header_floor_at_or_above_one_token_per_second() {
    // At these rates, a missing token is refilled within the first second,
    // which the header cannot express — so the floor is the whole of what a
    // caller sees, and the refill arithmetic contributes nothing.
    //
    // At one token per second a full deficit is due at *exactly* one second,
    // the boundary that still collapses onto the floor.
    for refill_rps in [60.0, 6.0, 1.0] {
        for deficit in [f64::MIN_POSITIVE, 0.5, 1.0] {
            assert_eq!(
                earliest_refill(deficit, refill_rps),
                RETRY_AFTER_FLOOR,
                "refill_rps={refill_rps} deficit={deficit}"
            );
        }
    }
}

#[test]
fn earliest_refill_is_a_real_wait_below_one_token_per_second() {
    // The regime where the arithmetic is NOT constant, and where dropping it
    // for a flat 1-5s window would advertise a retry before a token can exist.
    // `sanitize_config` accepts any positive rate and the rate-limiter smoke
    // runs at 0.1, so these are reachable configurations, not hypotheticals.
    assert_eq!(earliest_refill(1.0, 0.1), Duration::from_secs(10));
    assert_eq!(earliest_refill(0.5, 0.1), Duration::from_secs(5));
    assert_eq!(earliest_refill(1.0, 0.001), Duration::from_secs(1000));
    // A partial second rounds up, never down: 0.25 tokens at 0.1/s is 2.5s.
    assert_eq!(earliest_refill(0.25, 0.1), Duration::from_secs(3));
}

#[test]
fn the_cap_is_the_only_reason_a_caller_returns_before_its_token() {
    // The one mechanism that breaks the invariant below, asserted rather than
    // left outside its rate list. `RETRY_AFTER_CAP` bounds the base, so once the
    // honest wait exceeds an hour the hint is shorter than the token's arrival
    // and the caller does come back too early. This is an existence proof at one
    // rate, not a boundary: every configuration whose `deficit / refill_rps`
    // exceeds the cap does the same. One futile retry per hour against a limiter
    // admitting less than one activation per hour — accepted, and priced on
    // `RETRY_AFTER_CAP`. Pinned here so the next reader finds the exception with
    // the rule instead of discovering it in production.
    let refill_rps = 1e-4;
    let earliest = earliest_refill(1.0, refill_rps);
    assert_eq!(earliest, Duration::from_secs(10_000));
    let hint = rate_limited_retry_after(&[(0.0, refill_rps)]);
    assert!(
        hint < earliest,
        "the cap should still bind at {refill_rps}/s: hint {hint:?} vs token at {earliest:?}"
    );
    assert_hint_in_window(hint, RETRY_AFTER_CAP);
}

#[test]
fn a_subnormal_refill_rate_is_capped_rather_than_panicking() {
    // `1e-320` is subnormal but finite and positive, so `valid_positive_f64`
    // accepts it and it reaches this arithmetic unmodified — while `1.0 / 1e-320`
    // overflows to infinity. The hint this replaced went through
    // `Duration::from_secs_f64`, which *panics* on a non-finite value, on the
    // sandbox create path. `try_from_secs_f64` makes it an error the cap
    // absorbs. The node-side limiter carries the twin regression test,
    // `activation_admission_extreme_refill_rate_does_not_panic_retry_after`.
    assert_hint_in_window(rate_limited_retry_after(&[(0.0, 1e-320)]), RETRY_AFTER_CAP);
    // And the other end of the same guard: a deficit that is already satisfied
    // must floor rather than take the unrepresentable-ratio branch.
    assert_eq!(earliest_refill(0.0, 60.0), RETRY_AFTER_FLOOR);
    assert_eq!(earliest_refill(-1.0, 60.0), RETRY_AFTER_FLOOR);
}

#[test]
fn a_shed_caller_is_never_told_to_return_before_a_token_can_exist() {
    // The operator-visible property, stated without reference to how the hint
    // is computed: whatever a caller is told, waiting that long is enough for
    // the token it was refused to have accumulated. A flat window would break
    // it at every rate below one token per second. It holds for every rate at
    // which the honest wait fits under `RETRY_AFTER_CAP`; the test above pins
    // the one regime where the cap takes precedence.
    // The last two walk up to `RETRY_AFTER_CAP` without crossing it: 1/3000
    // gives a 3000s base, and 1/3600 gives exactly 3600s, where the clamp is
    // reached but does not bind. That is the neighbourhood a boundary error
    // would hide in. Note 1/3599 would *not* serve — `1.0/(1.0/3599.0)` is
    // 3599.0000000000005, whose remainder rounds up to a 3600s base, so it is
    // indistinguishable from 1/3600 here.
    for refill_rps in [60.0, 6.0, 1.0, 0.5, 0.1, 0.001, 1.0 / 3000.0, 1.0 / 3600.0] {
        for deficit in [f64::MIN_POSITIVE, 0.25, 0.5, 1.0] {
            let earliest = earliest_refill(deficit, refill_rps);
            for _ in 0..8 {
                let hint = jittered_retry_after(earliest);
                assert!(
                    hint >= earliest,
                    "refill_rps={refill_rps} deficit={deficit}: hint {hint:?} \
                     falls due before the token at {earliest:?}"
                );
                assert_hint_in_window(hint, earliest);
            }
        }
    }
}

#[test]
fn a_rate_limited_hint_is_paced_by_the_configured_refill_rate() {
    // Docker-free coverage of the *function* the rate-limit call site calls —
    // not of the call site itself, which only
    // `exhausted_bucket_rate_limits_activation` reaches and which
    // needs Postgres and Redis. So a refactor of `admit_activation` that
    // stopped passing `config.refill_rps` would still be caught only there,
    // and only by a run with those dependencies up. This
    // covers the arithmetic: a sub-one-token-per-second rate must be honoured
    // rather than collapsed onto the header floor.
    // Non-constancy lives here rather than in the Redis-backed test: this calls
    // the function the rate-limit call site calls, with no bucket and no clock,
    // so a flattened hint cannot be masked by refill drift.
    //
    // An empty bucket at 0.1 tokens/s puts the missing token 10s away, so the
    // reachable set is exactly {10s..14s} — paced by the configured rate, and
    // spread. 200 draws miss one of five values with probability at most
    // 5 * (4/5)^200, about 2e-19.
    let earliest = Duration::from_secs(10);
    let sampled: BTreeSet<Duration> = (0..200)
        .map(|_| rate_limited_retry_after(&[(0.0, 0.1)]))
        .collect();
    let expected: BTreeSet<Duration> = (0..=RETRY_AFTER_JITTER.as_secs())
        .map(|offset| earliest + Duration::from_secs(offset))
        .collect();
    // Set equality already implies every hint is at or above `earliest`, so
    // there is no separate floor assertion here — that property is stated where
    // it can fail, in `a_shed_caller_is_never_told_to_return_before_a_token_can_exist`.
    assert_eq!(sampled, expected);
    // A high refill rate collapses onto the floor.
    let fast_refill: BTreeSet<Duration> = (0..200)
        .map(|_| rate_limited_retry_after(&[(0.0, 60.0)]))
        .collect();
    let floor_expected: BTreeSet<Duration> = (0..=RETRY_AFTER_JITTER.as_secs())
        .map(|offset| RETRY_AFTER_FLOOR + Duration::from_secs(offset))
        .collect();
    assert_eq!(fast_refill, floor_expected);
}

#[test]
fn an_unusable_refill_rate_is_capped_without_flattening_the_jitter() {
    // A rate small enough that the honest wait has no useful expression must
    // still produce a storable hint. Note the cap errs *short* here, by a wide
    // margin — that trade is priced on `RETRY_AFTER_CAP`; the erring-long
    // direction belongs to the saturating step inside `earliest_refill`, not to
    // this clamp.
    //
    // What this test is for: the cap must bound the *base*, not the sum. `valid_positive_f64`
    // accepts any finite positive rate, so at or below one token per hour the
    // cap is reachable, and clamping the sum would hand every caller there the
    // identical hint — the synchronization this whole window exists to break,
    // reinstated in the one regime where the guard rail bites. NaN cannot reach
    // here (`sanitize_config` rejects it) but must take the same direction.
    for refill_rps in [f64::MIN_POSITIVE, f64::NAN, 1.0 / 7200.0] {
        let earliest = earliest_refill(1.0, refill_rps);
        let sampled: BTreeSet<Duration> =
            (0..200).map(|_| jittered_retry_after(earliest)).collect();
        let expected: BTreeSet<Duration> = (0..=RETRY_AFTER_JITTER.as_secs())
            .map(|offset| RETRY_AFTER_CAP + Duration::from_secs(offset))
            .collect();
        assert_eq!(sampled, expected, "refill_rps={refill_rps}");
    }
}

#[test]
fn every_hint_in_the_window_survives_header_rendering_unchanged() {
    // Each second the sampler can draw reaches the caller as that same second.
    // This is what the whole-second quantization buys, and it is a property of
    // this module's window rather than of the renderer — the renderer's own
    // rounding is pinned beside it in `controllers::tests`.
    for jitter_secs in 0..=RETRY_AFTER_JITTER.as_secs() {
        let hint = retry_after_with_jitter(RETRY_AFTER_FLOOR, Duration::from_secs(jitter_secs));
        let rendered = crate::controllers::retry_after_header_value(hint)
            .expect("a whole-second hint renders as a header value");
        assert_eq!(
            rendered.to_str().expect("header value is ASCII"),
            hint.as_secs().to_string(),
            "jitter offset {jitter_secs}s did not survive header rendering"
        );
    }
}

#[tokio::test]
async fn unreachable_redis_consume_errors() {
    // A bucket pointed at a dead endpoint surfaces an error from `consume`,
    // which the admission layer turns into a fail-open admit.
    let bucket = RedisTokenBucket::from_url("redis://127.0.0.1:1").expect("pool builds lazily");
    let result = bucket
        .consume(&[spec("space:test:unreachable", 6.0, 60.0)], 1.0)
        .await;
    assert!(
        result.is_err(),
        "consume against an unreachable Redis must error so the caller fails open"
    );
}

#[tokio::test]
async fn redis_consume_records_client_span_on_error() {
    let bucket = RedisTokenBucket::from_url("redis://127.0.0.1:1").expect("pool builds lazily");
    let capture = RedisSpanCapture::default();
    let subscriber = tracing_subscriber::registry().with(capture.clone());

    let guard = tracing::subscriber::set_default(subscriber);
    let result = bucket
        .consume(&[spec("space:test:trace", 6.0, 60.0)], 1.0)
        .await;
    drop(guard);

    assert!(result.is_err(), "dead Redis endpoint should fail consume");
    let spans = capture.spans.lock().unwrap().clone();
    let span = spans
        .iter()
        .find(|span| span.name == "redis.client")
        .expect("consume should emit a redis.client span");

    for (field, want) in [
        ("otel.kind", "client"),
        ("otel.name", "redis EVALSHA"),
        ("operation.name", "redis.client"),
        ("db.system", "redis"),
        ("db.operation", "EVALSHA"),
        ("db.operation.name", "EVALSHA"),
        ("server.address", "127.0.0.1"),
        ("server.port", "1"),
        ("network.transport", "tcp"),
        ("otel.status_code", "ERROR"),
    ] {
        assert_eq!(
            field_value(&span.fields, field).as_deref(),
            Some(want),
            "field {field}; fields={:?}",
            span.fields,
        );
    }
    assert!(
        field_value(&span.fields, "otel.status_description").is_some(),
        "error span should record a status description: {:?}",
        span.fields,
    );
}

#[tokio::test]
async fn shared_bucket_allows_burst_then_rejects() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let key = format!("space:test:{}", Uuid::now_v7());

    // Tiny refill + single-token burst: the first consume drains it, the
    // second is rejected because refill over the round-trip is negligible.
    let first = bucket.consume(&[spec(&key, 0.001, 1.0)], 1.0).await?;
    assert!(first.admitted, "{first:?}");

    let second = bucket.consume(&[spec(&key, 0.001, 1.0)], 1.0).await?;
    assert!(!second.admitted, "{second:?}");
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct RedisSpanCapture {
    spans: Arc<Mutex<Vec<CapturedSpan>>>,
}

#[derive(Clone, Debug)]
struct CapturedSpan {
    name: &'static str,
    fields: Vec<(String, String)>,
}

impl<S> Layer<S> for RedisSpanCapture
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: Context<'_, S>,
    ) {
        if attrs.metadata().name() != "redis.client" {
            return;
        }

        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        attrs.record(&mut visitor);
        self.spans.lock().unwrap().push(CapturedSpan {
            name: attrs.metadata().name(),
            fields: visitor.into_fields(),
        });
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: Context<'_, S>,
    ) {
        let Some(span) = ctx.span(id) else {
            return;
        };
        if span.metadata().name() != "redis.client" {
            return;
        }

        let mut visitor = pplx_space_observability::test_capture::RecordingVisitor::default();
        values.record(&mut visitor);
        if let Some(captured) = self.spans.lock().unwrap().last_mut() {
            captured.fields.extend(visitor.into_fields());
        }
    }
}

fn field_value(fields: &[(String, String)], name: &str) -> Option<String> {
    fields
        .iter()
        .rev()
        .find(|(field_name, _)| field_name == name)
        .map(|(_, value)| value.clone())
}

#[tokio::test]
async fn concurrent_consumers_cannot_overspend_shared_bucket() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let key = format!("space:test:{}", Uuid::now_v7());

    let buckets = [spec(&key, 0.001, 1.0)];
    let first = bucket.consume(&buckets, 1.0);
    let second = bucket.consume(&buckets, 1.0);
    let (first, second) = tokio::join!(first, second);

    let outcomes = [first?, second?];
    let admitted = outcomes.iter().filter(|outcome| outcome.admitted).count();
    let rejected = outcomes.iter().filter(|outcome| !outcome.admitted).count();
    assert_eq!(
        admitted, 1,
        "exactly one consumer spends the token: {outcomes:?}"
    );
    assert_eq!(
        rejected, 1,
        "the other consumer is rate limited: {outcomes:?}"
    );
    Ok(())
}

#[tokio::test]
async fn disabled_admission_admits_without_touching_store() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let config = ClusterSandboxAdmissionConfig {
        enabled: false,
        ..enabled_config()
    };
    // No bucket and no eligible node: a disabled limiter must still admit.
    let admission = admission(direct_substrate(&db.pool), config, None);
    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await?;
    Ok(())
}

#[tokio::test]
async fn empty_cluster_rejects_activation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    // Enabled, no start-eligible nodes seeded -> reject before touching Redis.
    let admission = admission(direct_substrate(&db.pool), enabled_config(), None);
    let error = admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await
        .expect_err("empty cluster must reject");
    assert!(
        matches!(error, ClusterAdmissionError::EmptyCluster { .. }),
        "{error:?}"
    );
    // No bucket was consulted, so the hint starts at the header floor — and
    // because nothing refills, this path has none of the wall-clock drift the
    // rate-limited one does, so its reachable set is exact.
    //
    // Asserted as a set rather than a range on purpose: a range containing the
    // floor is satisfied by the flat `Duration::from_secs(1)` this replaced, so
    // it would have passed against the unfixed code. This is the only coverage
    // the empty-cluster call site's jitter has.
    let mut hints = BTreeSet::new();
    hints.insert(error.retry_after().expect("empty cluster carries a hint"));
    for _ in 0..100 {
        let error = admission
            .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
            .await
            .expect_err("an empty cluster keeps rejecting");
        hints.insert(error.retry_after().expect("empty cluster carries a hint"));
    }
    let expected: BTreeSet<Duration> = (0..=RETRY_AFTER_JITTER.as_secs())
        .map(|offset| RETRY_AFTER_FLOOR + Duration::from_secs(offset))
        .collect();
    // 101 draws (one before the loop, 100 inside) miss one of five values with
    // probability at most 5 * (4/5)^101, under 1e-9.
    assert_eq!(hints, expected);
    Ok(())
}

#[tokio::test]
async fn fails_open_when_no_redis_configured() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    seed_start_eligible_node(&db.pool).await?;
    // Enabled with an eligible node but no Redis bucket: admit (fail open).
    let admission = admission(direct_substrate(&db.pool), enabled_config(), None);
    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await?;
    Ok(())
}

#[tokio::test]
async fn exhausted_bucket_rate_limits_activation() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    // The production bucket key is fixed; on a shared test Redis a retry of
    // this test would inherit the prior attempt's consumed token.
    bucket.clear(super::SANDBOX_ACTIVATION_BUCKET).await?;
    seed_start_eligible_node(&db.pool).await?;

    // Single-token bucket with negligible refill: first activation admits,
    // second is rate limited through the full admission path.
    let config_refill_rps = 0.001;
    let config = ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: config_refill_rps,
        burst: 1.0,
        redis_url: None,
        client_rate: None,
    };
    let admission = admission(direct_substrate(&db.pool), config, Some(bucket));

    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await?;
    let error = admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await
        .expect_err("second activation must be rate limited");
    assert!(
        matches!(error, ClusterAdmissionError::RateLimited { .. }),
        "{error:?}"
    );

    // What this test can and cannot show. The bucket is `refill_rps: 0.001`, so
    // Redis refills it in real time and the base drifts down by one second for
    // every second the loop takes. That rules out an exact window *and* a
    // non-constancy assertion: drift alone yields distinct hints, so
    // `hints.len() > 1` here would pass against a jitter-free implementation
    // that merely spent a second in the loop. Non-constancy at this call site
    // belongs to `a_rate_limited_hint_is_paced_by_the_configured_refill_rate`,
    // which calls the same function with no bucket and no clock.
    //
    // What remains is wall-clock-independent and still worth asserting here:
    // refill only ever *shrinks* the deficit, so no hint can exceed the initial
    // 1000s base plus the widest offset; and any hint above the whole
    // floor-derived window proves the configured rate reached the caller through
    // the real rejection path, which no pure test can show.
    let paced_above = RETRY_AFTER_FLOOR + RETRY_AFTER_JITTER;
    let widest = Duration::from_secs(1000) + RETRY_AFTER_JITTER;
    let mut hints = BTreeSet::new();
    hints.insert(error.retry_after().expect("rate limit carries a hint"));
    for _ in 0..11 {
        let error = admission
            .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
            .await
            .expect_err("the drained bucket keeps rate limiting");
        hints.insert(error.retry_after().expect("rate limit carries a hint"));
    }
    for hint in &hints {
        assert!(
            *hint > paced_above,
            "hint {hint:?} is inside the floor-derived window, so the configured \
             {:?}/s refill was not consulted",
            config_refill_rps
        );
        assert!(
            *hint <= widest,
            "hint {hint:?} exceeds the widest the initial deficit allows ({widest:?})"
        );
    }
    Ok(())
}

#[tokio::test]
async fn with_config_builds_bucket_from_config_redis_url() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let Some((_node, url)) = try_redis_url().await? else {
        return Ok(());
    };
    seed_start_eligible_node(&db.pool).await?;

    // Both this test and exhausted_bucket_rate_limits_activation drive the fixed
    // bucket key through admit_activation; on a shared SPACE_TEST_REDIS_URL
    // server they'd contend for the same single token. Select a separate logical
    // DB so this test's bucket is isolated (test redis URLs are path-less).
    let url = format!("{}/1", url.trim_end_matches('/'));
    // Same retry-contamination guard as exhausted_bucket_rate_limits_activation,
    // for this test's own logical DB.
    RedisTokenBucket::from_url(&url)?
        .clear(super::SANDBOX_ACTIVATION_BUCKET)
        .await?;

    // The full production constructor path: the Redis URL is carried through
    // config, not read from process env, so `with_config` is a complete
    // interface a caller can point at a test Redis. A single-token bucket
    // exhausts on the second activation.
    let config = ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: 0.001,
        burst: 1.0,
        redis_url: Some(url),
        client_rate: None,
    };
    let admission = ClusterSandboxAdmission::with_config(
        direct_substrate(&db.pool),
        config,
        FeatureFlagClient::disabled(),
    );

    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await?;
    let error = admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await
        .expect_err("second activation must be rate limited through with_config");
    assert!(
        matches!(error, ClusterAdmissionError::RateLimited { .. }),
        "{error:?}"
    );
    Ok(())
}

// --- Per-client activation rate -------------------------------------------

#[test]
fn a_client_rate_needs_a_positive_refill_and_at_least_one_token_of_burst() {
    for refill in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        assert_eq!(
            ClientActivationRate::new(refill, Some(5.0)),
            None,
            "{refill}"
        );
    }
    // No burst, or an unusable one, is one second of refill.
    let rate = |refill, burst| ClientActivationRate::new(refill, burst).expect("usable");
    assert_eq!(rate(4.0, None).burst, 4.0);
    assert_eq!(rate(4.0, Some(f64::NAN)).burst, 4.0);
    // Below one token either way rounds up, or the client could never activate.
    assert_eq!(rate(0.1, None).burst, 1.0);
    assert_eq!(rate(4.0, Some(0.5)).burst, 1.0);
    assert_eq!(rate(4.0, Some(20.0)).burst, 20.0);
}

#[test]
fn the_env_client_rate_is_off_when_its_refill_is_unusable() {
    let parse = ClientActivationRate::parse;
    assert_eq!(
        parse(" 2 ", Some("20")),
        ClientActivationRate::new(2.0, Some(20.0))
    );
    assert_eq!(parse("2", None), ClientActivationRate::new(2.0, None));
    // A bad burst keeps the rate, at one second of refill.
    assert_eq!(
        parse("2", Some("lots")),
        ClientActivationRate::new(2.0, None)
    );
    // A bad refill is no per-client rate, never a built-in one.
    assert_eq!(parse("fast", Some("20")), None);
    assert_eq!(parse("0", Some("20")), None);
}

#[test]
fn sanitize_config_drops_an_unusable_hand_built_client_rate() {
    let config = |client_rate| {
        sanitize_config(ClusterSandboxAdmissionConfig {
            client_rate,
            ..enabled_config()
        })
        .client_rate
    };
    assert_eq!(
        config(Some(ClientActivationRate {
            refill_rps: 0.0,
            burst: 5.0,
        })),
        None
    );
    assert_eq!(
        config(Some(ClientActivationRate {
            refill_rps: 2.0,
            burst: 0.0,
        })),
        ClientActivationRate::new(2.0, None)
    );
}

/// Redis Cluster's key-to-slot mapping, from the cluster specification: CRC16
/// (XMODEM) of the hash tag when the key has a non-empty `{...}`, of the whole
/// key otherwise, mod 16384. Written out here because the redis crate doesn't
/// export its copy.
fn cluster_slot(key: &str) -> u16 {
    let bytes = key.as_bytes();
    let hashed = bytes
        .iter()
        .position(|b| *b == b'{')
        .and_then(|open| {
            let close = bytes[open + 1..].iter().position(|b| *b == b'}')?;
            (close > 0).then(|| &bytes[open + 1..open + 1 + close])
        })
        .unwrap_or(bytes);
    let mut crc: u16 = 0;
    for byte in hashed {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc % 16384
}

#[test]
fn every_client_bucket_shares_the_cluster_buckets_slot() {
    // The spec's own check value, so the oracle above is the real function.
    assert_eq!(cluster_slot("123456789"), 0x31C3);
    let cluster = cluster_slot(SANDBOX_ACTIVATION_BUCKET);
    for client in ["acme", "globex", "unknown", "a.b-c_d"] {
        let key = client_bucket_key(client);
        assert_eq!(cluster_slot(&key), cluster, "{key}");
        assert_ne!(key, SANDBOX_ACTIVATION_BUCKET);
    }
}

fn rate_flags(value: serde_json::Value) -> FeatureFlagClient {
    FeatureFlagClient::fixed_json([(
        pplx_space_feature_flags::SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
        value,
    )])
}

#[test]
fn the_quotas_flag_retunes_or_exempts_single_clients() {
    let env_rate = ClientActivationRate::new(2.0, Some(20.0));
    let flags = rate_flags(serde_json::json!({
        // The concurrency limiter's fields share the flag and are ignored here.
        "unspecified": 4,
        "overrides": {"acme": 8},
        "rate": {
            "acme": {"refill_rps": 0.5, "burst": 5},
            "globex": null,
            "unknown": {"refill_rps": 1},
            "typo": {"refill_rps": 0},
        },
    }));
    let overrides = ClientRateOverrides::from_flags(true, &flags);
    let rate_for = |client| ClientRateOverrides::rate_for(overrides.as_ref(), client, env_rate);

    assert_eq!(rate_for("acme"), ClientActivationRate::new(0.5, Some(5.0)));
    assert_eq!(rate_for("globex"), None, "null exempts the client");
    assert_eq!(rate_for("unknown"), ClientActivationRate::new(1.0, None));
    assert_eq!(rate_for("typo"), env_rate, "a bad entry keeps the env rate");
    assert_eq!(
        rate_for("other"),
        env_rate,
        "an absent entry keeps the env rate"
    );
    // With no env rate, only the flag's entries rate anyone.
    assert_eq!(
        ClientRateOverrides::rate_for(overrides.as_ref(), "other", None),
        None
    );
}

#[test]
fn the_quotas_flag_rate_map_decodes_when_flagd_stringifies_it() {
    let flags = rate_flags(serde_json::json!({
        "rate": r#"{"acme": {"refill_rps": 0.5, "burst": 5}}"#,
    }));
    let overrides = ClientRateOverrides::from_flags(true, &flags);
    assert_eq!(
        ClientRateOverrides::rate_for(overrides.as_ref(), "acme", None),
        ClientActivationRate::new(0.5, Some(5.0))
    );
}

#[test]
fn the_quotas_flag_falls_back_to_the_env_rate_when_it_cannot_apply() {
    let rated = serde_json::json!({"rate": {"acme": {"refill_rps": 0.5}}});

    // The env master is the hard off for the override, as for the gate.
    let off = ClientRateOverrides::from_flags(false, &rate_flags(rated.clone()));
    assert_eq!(off, None);
    // No provider, and an assignment that doesn't parse.
    assert_eq!(
        ClientRateOverrides::from_flags(true, &FeatureFlagClient::disabled()),
        None
    );
    let malformed = rate_flags(serde_json::json!({"rate": "not a map"}));
    assert_eq!(ClientRateOverrides::from_flags(true, &malformed), None);
}

#[test]
fn a_malformed_rate_entry_costs_only_its_own_key() {
    let env_rate = ClientActivationRate::new(2.0, None);
    let flags = rate_flags(serde_json::json!({
        "rate": {
            "acme": "fast",
            "typo": {"refil_rps": 1},
            "globex": null,
            "initech": {"refill_rps": 0.5},
        },
    }));
    let overrides = ClientRateOverrides::from_flags(true, &flags);
    let rate_for = |client| ClientRateOverrides::rate_for(overrides.as_ref(), client, env_rate);

    assert_eq!(rate_for("acme"), env_rate);
    assert_eq!(rate_for("typo"), env_rate);
    // The other keys' exemptions and rates survive the bad entries.
    assert_eq!(rate_for("globex"), None);
    assert_eq!(rate_for("initech"), ClientActivationRate::new(0.5, None));
}

#[test]
fn a_two_bucket_rejection_waits_for_the_slower_bucket() {
    // Cluster at 60/s is at the floor; a client at 0.1/s needs 10s for a whole
    // token, so every hint sits in the window above 10s.
    for _ in 0..50 {
        assert_hint_in_window(
            rate_limited_retry_after(&[(0.0, 60.0), (0.0, 0.1)]),
            Duration::from_secs(10),
        );
    }
    // Order doesn't matter, and one short bucket is the single-bucket hint.
    for _ in 0..50 {
        assert_hint_in_window(
            rate_limited_retry_after(&[(0.0, 0.1), (0.0, 60.0)]),
            Duration::from_secs(10),
        );
        assert_hint_in_window(
            rate_limited_retry_after(&[(0.5, 0.1)]),
            Duration::from_secs(5),
        );
    }
}

#[test]
fn the_client_message_names_the_client_and_explains_unknown() {
    let named = client_rate_limited_message(&client_key("acme"));
    assert!(named.starts_with("Client rate limit:"), "{named}");
    assert!(named.contains("`acme`"), "{named}");
    assert!(!named.contains("X-Client-ID"), "{named}");
    let unknown = client_rate_limited_message(ClientKey::unknown_ref());
    assert!(unknown.contains("X-Client-ID"), "{unknown}");
    assert!(unknown.contains("implicit wakes"), "{unknown}");
}

fn client_key(id: &str) -> ClientKey {
    ClientKey::allowlisted(id).expect("a label-safe test client id")
}

/// Keys under a fresh namespace, so a shared test Redis and nextest retries
/// never see another run's tokens.
fn fresh_keys() -> (String, String) {
    let cluster = format!("space:test:{}", Uuid::now_v7());
    let client = format!("{{{cluster}}}:client:acme");
    (cluster, client)
}

fn assert_tokens(actual: f64, expected: f64, what: &str) {
    // The refill rates below are 0.001/s, so a slow test run moves a count by
    // well under this.
    assert!(
        (actual - expected).abs() < 0.05,
        "{what}: {actual} tokens, expected about {expected}"
    );
}

#[tokio::test]
async fn an_empty_client_bucket_takes_no_cluster_token() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let (cluster, client) = fresh_keys();
    let both = [spec(&cluster, 0.001, 5.0), spec(&client, 0.001, 1.0)];

    let first = bucket.consume(&both, 1.0).await?;
    assert!(first.admitted, "{first:?}");
    assert_tokens(first.remaining[0], 4.0, "cluster after the admit");
    assert_tokens(first.remaining[1], 0.0, "client after the admit");

    for _ in 0..3 {
        let rejected = bucket.consume(&both, 1.0).await?;
        assert!(!rejected.admitted, "{rejected:?}");
        assert_tokens(rejected.remaining[0], 4.0, "cluster after a rejection");
        assert_tokens(rejected.remaining[1], 0.0, "client after a rejection");
    }

    // Read the cluster bucket on its own: every rejection left it untouched.
    let cluster_only = bucket.consume(&[spec(&cluster, 0.001, 5.0)], 1.0).await?;
    assert!(cluster_only.admitted, "{cluster_only:?}");
    assert_tokens(
        cluster_only.remaining[0],
        3.0,
        "cluster after its own consume",
    );
    Ok(())
}

#[tokio::test]
async fn an_empty_cluster_bucket_takes_no_client_token() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let (cluster, client) = fresh_keys();
    let both = [spec(&cluster, 0.001, 1.0), spec(&client, 0.001, 5.0)];

    assert!(bucket.consume(&both, 1.0).await?.admitted);
    for _ in 0..3 {
        let rejected = bucket.consume(&both, 1.0).await?;
        assert!(!rejected.admitted, "{rejected:?}");
        assert_tokens(rejected.remaining[0], 0.0, "cluster after a rejection");
        assert_tokens(rejected.remaining[1], 4.0, "client after a rejection");
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_two_bucket_consumers_spend_both_buckets_together() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let (cluster, client) = fresh_keys();
    let both = [spec(&cluster, 0.001, 3.0), spec(&client, 0.001, 10.0)];

    let outcomes = futures_util::future::join_all((0..8).map(|_| bucket.consume(&both, 1.0))).await;
    let outcomes = outcomes.into_iter().collect::<Result<Vec<_>, _>>()?;
    let admitted = outcomes.iter().filter(|outcome| outcome.admitted).count();
    assert_eq!(
        admitted, 3,
        "the cluster bucket admits its burst: {outcomes:?}"
    );

    // The five rejections took nothing from the client bucket either.
    let after = bucket.consume(&[spec(&client, 0.001, 10.0)], 1.0).await?;
    assert_tokens(
        after.remaining[0],
        6.0,
        "client after 3 admits and its own consume",
    );
    Ok(())
}

/// An admission-path config: one token a second would refill during a test,
/// so both rates are 0.001/s, and every bucket starts full.
fn rated_config(
    cluster_burst: f64,
    client_rate: Option<ClientActivationRate>,
) -> ClusterSandboxAdmissionConfig {
    ClusterSandboxAdmissionConfig {
        enabled: true,
        refill_rps: 0.001,
        burst: cluster_burst,
        redis_url: None,
        client_rate,
    }
}

#[tokio::test]
async fn a_client_over_its_rate_is_rejected_while_others_are_admitted() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let config = rated_config(10.0, ClientActivationRate::new(0.001, Some(2.0)));
    let Some((_node, admission)) =
        try_redis_admission(&db.pool, 2, config, FeatureFlagClient::disabled()).await?
    else {
        return Ok(());
    };
    // Fresh client keys, so a nextest retry starts from full client buckets.
    let flooding = client_key(&format!("flood-{}", Uuid::now_v7()));
    let other = client_key(&format!("other-{}", Uuid::now_v7()));

    for class in [
        ClusterActivationClass::Cold,
        ClusterActivationClass::Restore,
    ] {
        admission.admit_activation(class, &flooding).await?;
    }
    for class in [
        ClusterActivationClass::Cold,
        ClusterActivationClass::Restore,
    ] {
        let error = admission
            .admit_activation(class, &flooding)
            .await
            .expect_err("cold and restore share the client's bucket");
        let message = error.to_string();
        assert!(message.starts_with("Client rate limit:"), "{message}");
        assert!(message.contains(flooding.as_str()), "{message}");
        // The client's 0.001/s refill paces the hint, not the header floor.
        assert!(
            error.retry_after().expect("rate limit carries a hint")
                > RETRY_AFTER_FLOOR + RETRY_AFTER_JITTER,
            "{error:?}"
        );
    }

    // Another client still gets in, and so does `unknown`, which has a bucket
    // of its own.
    admission
        .admit_activation(ClusterActivationClass::Cold, &other)
        .await?;
    admission
        .admit_activation(ClusterActivationClass::Cold, ClientKey::unknown_ref())
        .await?;

    // Four admits, and the two rejections took no token from either bucket.
    let (cluster, client) = admission.stored_tokens(&flooding).await?;
    assert_tokens(cluster.expect("charged"), 6.0, "cluster after four admits");
    assert_tokens(client.expect("charged"), 0.0, "client after its two admits");
    Ok(())
}

#[tokio::test]
async fn an_empty_cluster_bucket_is_reported_as_the_server_limit() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let config = rated_config(1.0, ClientActivationRate::new(0.001, Some(5.0)));
    let Some((_node, admission)) =
        try_redis_admission(&db.pool, 3, config, FeatureFlagClient::disabled()).await?
    else {
        return Ok(());
    };
    let client = client_key(&format!("client-{}", Uuid::now_v7()));

    admission
        .admit_activation(ClusterActivationClass::Cold, &client)
        .await?;
    let error = admission
        .admit_activation(ClusterActivationClass::Cold, &client)
        .await
        .expect_err("the cluster bucket is empty");
    let message = error.to_string();
    assert!(message.starts_with("Server rate limit:"), "{message}");

    // The cluster rejection took nothing from the client's bucket.
    let (_, client_tokens) = admission.stored_tokens(&client).await?;
    assert_tokens(
        client_tokens.expect("charged"),
        4.0,
        "client after one admit",
    );
    Ok(())
}

#[tokio::test]
async fn an_exempt_client_draws_only_from_the_cluster_bucket() -> TestResult {
    let Some(db) = try_pg_test_db().await? else {
        return Ok(());
    };
    let client = client_key(&format!("exempt-{}", Uuid::now_v7()));
    // An env rate of one token, which the flag lifts for this client.
    let config = rated_config(10.0, ClientActivationRate::new(0.001, Some(1.0)));
    let flags = rate_flags(serde_json::json!({"rate": {client.as_str(): null}}));
    let Some((_node, admission)) = try_redis_admission(&db.pool, 4, config, flags).await? else {
        return Ok(());
    };
    for _ in 0..3 {
        admission
            .admit_activation(ClusterActivationClass::Cold, &client)
            .await?;
    }
    let (cluster, client_tokens) = admission.stored_tokens(&client).await?;
    assert_tokens(cluster.expect("charged"), 7.0, "cluster after three admits");
    assert_eq!(client_tokens, None, "an exempt client has no bucket");
    Ok(())
}

#[tokio::test]
async fn a_slow_bucket_outlives_its_refill_and_an_unbounded_one_is_capped() -> TestResult {
    let Some((_node, bucket)) = try_redis_bucket().await? else {
        return Ok(());
    };
    let (slow, unbounded) = fresh_keys();
    // One token at 1e-8/s takes 1e8 seconds, about three years, to refill.
    // At 1e-320/s `burst / rate` is infinite, past anything EXPIRE accepts:
    // uncapped, the script fails after its writes and the caller admits open.
    let both = [spec(&slow, 1e-8, 1.0), spec(&unbounded, 1e-320, 1.0)];

    let first = bucket.consume(&both, 1.0).await?;
    assert!(first.admitted, "{first:?}");
    let second = bucket.consume(&both, 1.0).await?;
    assert!(!second.admitted, "both tokens are spent: {second:?}");

    // A key that expired early would come back full, handing out tokens the
    // rate never refilled, so the slow key lives its whole refill time.
    let slow_ttl = bucket.ttl(&slow).await?;
    assert!(
        (100_000_000..=100_000_010).contains(&slow_ttl),
        "slow bucket TTL {slow_ttl}s"
    );
    assert_eq!(bucket.ttl(&unbounded).await?, 1_000_000_000_000);
    Ok(())
}
