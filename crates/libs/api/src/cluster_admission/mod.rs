//! Cluster-wide sandbox activation admission.
//!
//! A shared token bucket gates sandbox *activations* — cold create/start and
//! restore work that spends a node slot (and optionally a Kubernetes pod) —
//! before API and cluster-controller execution paths fan out to substrate
//! placement. The bucket lives in Redis: a single atomic `EVALSHA` per
//! admission does refill + consume + TTL against one key, so every replica
//! shares one rate with no `SELECT … FOR UPDATE` contention and no OLTP
//! coupling.
//!
//! The limiter is a guardrail, not a correctness gate, so it **fails open**:
//! any Redis error (unreachable, timeout, malformed reply) admits the
//! activation and emits a `store_unavailable` decision metric. It must never
//! become a single point of failure on the create path.

use std::future::Future;
use std::time::Duration;

use pplx_space_common_core::env::{env_flag_var, parse_positive_f64_env, valid_positive_f64};
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_CLUSTER_RATE_LIMITER_FLAG};
use pplx_space_metrics::sandbox::admission::{
    AdmissionDecision, AdmissionOperationClass, AdmissionRejectReason,
};
use rand::RngExt as _;
use tokio_util::sync::CancellationToken;

use crate::controllers::substrate::{
    ActivationAdmission, ActivationAdmissionClass, ActivationAdmissionFuture,
    ActivationAdmissionRejection, GATEWAY_RETRY_AFTER, Substrate,
};

mod redis_bucket;
#[cfg(test)]
mod tests;

use redis_bucket::{ConsumeOutcome, RedisTokenBucket};

/// Redis key for the shared cluster activation bucket. Namespaced so it never
/// collides with other SPACE keys in a shared Redis.
const SANDBOX_ACTIVATION_BUCKET: &str = "space:cluster_admission:sandbox_activation";
const DEFAULT_REFILL_RPS: f64 = 60.0;
const DEFAULT_BURST: f64 = 60.0;
const DEFAULT_ENABLED: bool = true;
const ENABLED_ENV: &str = "SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED";
/// Test-only: smoke runs shrink the bucket so a short probe can drain it.
const BURST_ENV: &str = "SPACE_CLUSTER_ACTIVATION_ADMISSION_BURST";
const REFILL_RPS_ENV: &str = "SPACE_CLUSTER_ACTIVATION_ADMISSION_REFILL_RPS";
const REDIS_URL_ENV: &str = "SPACE_REDIS_URL";

/// The limiter is a single cluster-wide bucket, so every replica evaluates the
/// flag against one constant subject: the assignment is a pure cluster on/off,
/// not a per-sandbox or per-org percentage rollout.
const CLUSTER_ADMISSION_FLAG_SUBJECT: &str = "space-cluster";

/// Smallest `Retry-After` the header can carry, and so the floor of every hint
/// this module produces: delay-seconds form is an integer, and
/// `controllers::retry_after_header_value` rounds a fractional remainder up and
/// clamps to `1`. Deliberately the same floor, for the same reason, as
/// `controllers::substrate::LOCK_CONTENTION_RETRY_AFTER` — its doc carries the
/// argument in full.
const RETRY_AFTER_FLOOR: Duration = Duration::from_secs(1);

/// Uniform whole-second jitter added *above* the earliest second a token can
/// exist, so a shed population spreads over the whole seconds that width
/// admits — five of them at the current value — instead of re-phasing onto one.
///
/// Derived from [`GATEWAY_RETRY_AFTER`] rather than restated, so the jitter
/// *width* is tied to a bound the crate already reasons about instead of to a
/// second literal that can drift from it. Note this fixes the width, not the
/// widest hint: a refill-derived base can carry the total up to
/// `RETRY_AFTER_CAP + RETRY_AFTER_JITTER`. Deliberately *not*
/// stated here as a comparison against what a converging sandbox waits: that
/// bound is a clamp ceiling on some paths and a flat hint on others, so any
/// short version of the comparison is wrong about one of them. Read
/// `GATEWAY_RETRY_AFTER` and `RESTORE_RETRY_AFTER_FLOOR` in
/// `controllers::substrate` for what those paths actually advertise.
///
/// The cost of deriving it: a wait-budget constant now also sets a
/// load-shedding spread, since narrowing `GATEWAY_RETRY_AFTER` narrows the
/// number of cohorts a shed population splits into. One test makes that noisy
/// rather than silent —
/// `retry_after_hint_spreads_over_every_whole_second_from_one_to_five` oracles
/// against the literal `{1s..=5s}` as a *set*, so any move fails it. Note the
/// cluster-controller dispatcher's cross-crate `1s..=5s` bound does not: it
/// checks one draw against a range, so a narrower window still passes.
///
/// Both extremes are wrong, and the server cannot size the wait from what it
/// knows: on a rejection the bucket reports `remaining` in `[0, 1)` however much
/// load is queued behind it. So the width is policy, priced from both sides:
///
/// * **Too narrow** re-forms the herd. A flat hint makes SPACE the
///   synchronizer — it sheds thousands of callers and re-phases every one of
///   them onto the same second, on top of the retry amplification the callers
///   already bring.
/// * **Too wide** adds latency to restores that would have succeeded on the
///   next refill tick. Whenever a rejection happens the bucket is by definition
///   within one token of admitting — the Lua rejects only at `tokens < 1.0` —
///   so at a 60/s refill the token it was refused lands ~17ms later, at the
///   herd's peak as much as its tail. Every second of width past that is
///   latency charged to a caller who would have succeeded, and on the async
///   create path the same value paces the dispatcher's requeue.
///
/// What no width buys is making a herd fit: ~150/s against a 60/s bucket does
/// not fit at any hint. Spreading the shed population is only SPACE declining to
/// add its own synchronization on top of the caller's burst.
const RETRY_AFTER_JITTER: Duration = Duration::from_secs(
    GATEWAY_RETRY_AFTER
        .as_secs()
        .saturating_sub(RETRY_AFTER_FLOOR.as_secs()),
);
const _: () = assert!(
    RETRY_AFTER_JITTER.as_secs() > 0,
    "GATEWAY_RETRY_AFTER must exceed the header floor or every shed caller gets one hint"
);

/// Ceiling on the refill-derived *base* of a hint — the jitter offset is added
/// above it, so the widest hint this module can produce is `RETRY_AFTER_CAP +
/// RETRY_AFTER_JITTER`. Not a tuned policy: a guard rail for a configured
/// `refill_rps` small enough that no expressible hint is honest.
///
/// Two things a future editor must not infer, both of which cost a review round
/// to establish:
///
/// * **The bound is Postgres's `timestamp` range, not integer saturation.** The
///   async requeue writes this as `NOW() + make_interval(secs => …)` in
///   `public_operations`, and that statement fails — abandoning the requeue
///   rather than pacing it — from roughly 9.2e12 seconds upward, about a million
///   times lower than where `duration_secs_i64` saturates. So a `== u64::MAX`
///   check is *not* an equivalent guard.
/// * **This ceiling can outrank [`earliest_refill`]'s token-arrival guarantee,
///   and it wins.** Once
///   the honest wait exceeds the cap, the caller is told to return before its
///   token can exist. Accepted: a limiter admitting less than one activation an
///   hour is closed for practical purposes, and one futile retry per hour beats
///   the hot loop an un-paced hint would cause.
///
/// `the_cap_is_the_only_reason_a_caller_returns_before_its_token` and
/// `an_unusable_refill_rate_is_capped_without_flattening_the_jitter` pin the
/// second, with the worked numbers — read them there rather than deriving them
/// here. The first is reasoning, not a tested property: the requeue's
/// `make_interval` round-trip is covered for ordinary hints in
/// `public_operations`' own tests, but nothing exercises it anywhere near this
/// threshold. Treat it as the argument for why this constant is not
/// interchangeable with a saturation check, and verify it against Postgres
/// before relying on the number.
const RETRY_AFTER_CAP: Duration = Duration::from_secs(60 * 60);
// `retry_after_with_jitter` clamps into `[RETRY_AFTER_FLOOR, RETRY_AFTER_CAP]`,
// and `u64::clamp` panics when its bounds are inverted. Cheap to assert, and it
// keeps a create-path panic from being one edit away.
const _: () = assert!(
    RETRY_AFTER_FLOOR.as_secs() <= RETRY_AFTER_CAP.as_secs(),
    "the Retry-After floor must not exceed the cap; u64::clamp would panic"
);

#[derive(Clone)]
pub struct ClusterSandboxAdmission {
    substrate: Substrate,
    bucket: Option<RedisTokenBucket>,
    config: ClusterSandboxAdmissionConfig,
    feature_flags: FeatureFlagClient,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClusterSandboxAdmissionConfig {
    pub enabled: bool,
    pub refill_rps: f64,
    pub burst: f64,
    /// The shared token-bucket store URL (`redis://` / `rediss://`). `None`
    /// disables the bucket (fail-open `misconfigured`). Carried in config so
    /// `with_config` is a complete, testable interface; the process-env read
    /// lives only in [`Self::from_env`].
    pub redis_url: Option<String>,
}

impl Default for ClusterSandboxAdmissionConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_ENABLED,
            refill_rps: DEFAULT_REFILL_RPS,
            burst: DEFAULT_BURST,
            redis_url: None,
        }
    }
}

impl ClusterSandboxAdmissionConfig {
    pub fn from_env() -> Self {
        let defaults = Self::default();
        Self {
            enabled: env_flag_var(ENABLED_ENV, defaults.enabled),
            refill_rps: parse_positive_f64_env(REFILL_RPS_ENV, defaults.refill_rps),
            burst: pplx_space_common_core::env::test_hook_var(BURST_ENV)
                .and_then(|raw| raw.trim().parse::<f64>().ok())
                .filter(|burst| burst.is_finite() && *burst >= 1.0)
                .unwrap_or(defaults.burst),
            redis_url: std::env::var(REDIS_URL_ENV)
                .ok()
                .map(|url| url.trim().to_string())
                .filter(|url| !url.is_empty()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterActivationClass {
    Cold,
    Restore,
}

impl ClusterActivationClass {
    const fn metric_class(self) -> AdmissionOperationClass {
        match self {
            Self::Cold => AdmissionOperationClass::ColdActivation,
            Self::Restore => AdmissionOperationClass::RestoreActivation,
        }
    }
}

/// Admission rejects only when the cluster is out of activation capacity. Every
/// other failure mode (Redis error, capacity-probe error, missing store) fails
/// open inside [`ClusterSandboxAdmission::admit_activation`] and never reaches a
/// caller — the limiter is a guardrail, not a create-path SPOF.
///
/// The two rejection reasons are **kept distinct** because the async dispatcher
/// must treat them differently: [`Self::RateLimited`] (full token bucket) is
/// transient backpressure that should re-queue indefinitely behind the limiter,
/// but [`Self::EmptyCluster`] (a confident zero eligible nodes) is a durable
/// capacity outage that must re-queue against the bounded transient-error budget
/// so a full-cluster incident eventually terminates the create rather than
/// re-queuing forever.
#[derive(Debug, thiserror::Error)]
pub enum ClusterAdmissionError {
    /// The shared token bucket is empty — wait behind the limiter (backpressure).
    #[error("{message}")]
    RateLimited {
        message: String,
        retry_after: Duration,
    },
    /// No eligible start-capable nodes — bounded retry, not indefinite backpressure.
    #[error("{message}")]
    EmptyCluster {
        message: String,
        retry_after: Duration,
    },
}

impl ClusterAdmissionError {
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::RateLimited { retry_after, .. } | Self::EmptyCluster { retry_after, .. } => {
                Some(*retry_after)
            }
        }
    }
}

impl ClusterSandboxAdmission {
    pub fn new(substrate: Substrate, feature_flags: FeatureFlagClient) -> Self {
        Self::with_config(
            substrate,
            ClusterSandboxAdmissionConfig::from_env(),
            feature_flags,
        )
    }

    pub fn with_config(
        substrate: Substrate,
        config: ClusterSandboxAdmissionConfig,
        feature_flags: FeatureFlagClient,
    ) -> Self {
        // Borrow the URL to build the bucket before the config is moved into
        // sanitize_config; env parsing already happened in from_env.
        let bucket = build_bucket(config.redis_url.as_deref());
        Self {
            substrate,
            bucket,
            config: sanitize_config(config),
            feature_flags,
        }
    }

    /// Whether the limiter should shape this activation, under the
    /// env-master-first policy: `SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED` is
    /// the hard off, and the flag can only disable a limiter the env var
    /// enabled. A deployment with no flag provider installed keeps the limiter
    /// its environment asked for; one whose provider is installed and cannot
    /// answer falls back to the flag's declaration, which is on.
    fn admission_enabled(&self) -> bool {
        self.feature_flags.gate_enabled(
            self.config.enabled,
            SPACE_CLUSTER_RATE_LIMITER_FLAG,
            CLUSTER_ADMISSION_FLAG_SUBJECT,
        )
    }

    /// This limiter's liveness-heartbeat loop, for the caller to spawn.
    ///
    /// A method rather than a free function so that starting a heartbeat takes
    /// a limiter in hand: a tick from a process whose admission path does not
    /// exist would vouch for a decision counter that process never writes.
    /// That is a shape, not an enforced guarantee — this and the constructor
    /// are both `pub`. The returned loop itself captures nothing from `self`
    /// (only the token and a `fn()`), so the spawned task holds no limiter.
    ///
    /// A heartbeat visible in Datadog means this process is running and the
    /// whole chain from its `counter` call to ingestion works. It does **not**
    /// mean the admission decision counter's call sites are reachable, and it
    /// reports nothing about the limiter's configuration or its verdicts.
    pub fn liveness_heartbeat(
        &self,
        shutdown_token: CancellationToken,
    ) -> impl Future<Output = ()> + Send + 'static {
        tracing::info!(
            env_enabled = self.config.enabled,
            interval_secs = crate::limiter_liveness::HEARTBEAT_INTERVAL.as_secs(),
            "cluster activation limiter liveness heartbeat starting"
        );
        crate::limiter_liveness::run_heartbeat(
            crate::limiter_liveness::HEARTBEAT_INTERVAL,
            shutdown_token,
            pplx_space_metrics::sandbox::admission::cluster_heartbeat,
        )
    }

    /// Consume one activation token from the shared cluster bucket.
    ///
    /// Rejects with [`ClusterAdmissionError::EmptyCluster`] when the cluster has
    /// no start-capable nodes, or [`ClusterAdmissionError::RateLimited`] when the
    /// shared bucket is empty. On any Redis error the limiter fails open: the
    /// activation is admitted and a `store_unavailable` decision metric is
    /// emitted rather than returning an error.
    pub async fn admit_activation(
        &self,
        class: ClusterActivationClass,
    ) -> Result<(), ClusterAdmissionError> {
        // `false` means admission is disabled or a capacity probe failed —
        // already metered inside `prepare_activation_admission`, which admits
        // open without the bucket.
        if !prepare_activation_admission(
            &self.substrate,
            &self.config,
            self.admission_enabled(),
            class,
        )
        .await?
        {
            return Ok(());
        }

        let Some(bucket) = self.bucket.as_ref() else {
            // Enabled but no usable store (missing/invalid SPACE_REDIS_URL):
            // admit (never a startup crash / SPOF), but flag it as a distinct
            // steady-state `misconfigured` signal to page on — the guardrail is
            // silently absent, not merely riding out a transient Redis blip.
            emit_decision(
                class,
                AdmissionDecision::Accepted,
                AdmissionRejectReason::Misconfigured,
                None,
            );
            return Ok(());
        };

        match bucket
            .consume(
                SANDBOX_ACTIVATION_BUCKET,
                self.config.refill_rps,
                self.config.burst,
                1.0,
            )
            .await
        {
            Ok(ConsumeOutcome::Admitted { remaining }) => {
                emit_decision(
                    class,
                    AdmissionDecision::Accepted,
                    AdmissionRejectReason::None,
                    Some(remaining),
                );
                Ok(())
            }
            Ok(ConsumeOutcome::RateLimited { remaining }) => {
                emit_decision(
                    class,
                    AdmissionDecision::Rejected,
                    AdmissionRejectReason::RateLimited,
                    Some(remaining),
                );
                Err(ClusterAdmissionError::RateLimited {
                    // Server-wide throttle, worded so a client can tell it apart
                    // from the per-client concurrency limit: this one is not
                    // caused by (nor fixable by) the client's own request rate.
                    message: "Server rate limit: the sandbox platform is shedding \
                              create/restore load to protect shared cluster capacity. \
                              Not specific to your client — retry with backoff."
                        .to_string(),
                    retry_after: rate_limited_retry_after(remaining, self.config.refill_rps),
                })
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    operation_class = %class.metric_class(),
                    "cluster activation admission store unavailable; admitting (fail open)"
                );
                emit_decision(
                    class,
                    AdmissionDecision::Accepted,
                    AdmissionRejectReason::StoreUnavailable,
                    None,
                );
                Ok(())
            }
        }
    }
}

impl ActivationAdmission for ClusterSandboxAdmission {
    fn admit(&self, class: ActivationAdmissionClass) -> ActivationAdmissionFuture<'_> {
        let class = match class {
            ActivationAdmissionClass::Cold => ClusterActivationClass::Cold,
            ActivationAdmissionClass::Restore => ClusterActivationClass::Restore,
        };
        Box::pin(async move {
            self.admit_activation(class).await.map_err(|error| {
                let rate_limited = matches!(&error, ClusterAdmissionError::RateLimited { .. });
                ActivationAdmissionRejection::new(
                    error.to_string(),
                    error.retry_after(),
                    rate_limited,
                )
            })
        })
    }
}

fn build_bucket(url: Option<&str>) -> Option<RedisTokenBucket> {
    let Some(url) = url.map(str::trim).filter(|url| !url.is_empty()) else {
        tracing::warn!(
            var = REDIS_URL_ENV,
            "cluster activation admission has no Redis URL; failing open (all activations admitted)"
        );
        return None;
    };
    match RedisTokenBucket::from_url(url) {
        Ok(bucket) => Some(bucket),
        Err(error) => {
            tracing::error!(
                %error,
                "failed to build Redis pool for cluster activation admission; failing open"
            );
            None
        }
    }
}

/// Runs the pre-bucket admission gates for one activation and meters them.
///
/// Returns `Ok(true)` when the caller should consume a token from the shared
/// bucket, and `Ok(false)` when the activation is admitted open without the
/// bucket (disabled, or a fail-open capacity-probe error) — the fail-open
/// decision metric is already emitted here in that case. Returns
/// `Err(EmptyCluster)` only on a confident empty cluster.
async fn prepare_activation_admission(
    substrate: &Substrate,
    config: &ClusterSandboxAdmissionConfig,
    enabled: bool,
    class: ClusterActivationClass,
) -> Result<bool, ClusterAdmissionError> {
    if !enabled {
        emit_decision(
            class,
            AdmissionDecision::Accepted,
            AdmissionRejectReason::Disabled,
            None,
        );
        return Ok(false);
    }

    let eligible_nodes = match substrate.eligible_start_node_count().await {
        Ok(eligible_nodes) => eligible_nodes,
        Err(error) => {
            // Fail open on a capacity-probe failure: the limiter must never be a
            // single point of failure on the create path, so a transient
            // substrate/DB error admits the activation (bypassing the bucket)
            // rather than surfacing as a hard error that rejects the create.
            // Distinct fail-open reason from `store_unavailable` (Redis) so it
            // can be alerted on separately.
            tracing::warn!(
                %error,
                operation_class = %class.metric_class(),
                "cluster activation admission capacity probe failed; admitting (fail open)"
            );
            emit_decision(
                class,
                AdmissionDecision::Accepted,
                AdmissionRejectReason::CapacityUnavailable,
                None,
            );
            return Ok(false);
        }
    };
    // Emit the outcome-independent gauges exactly once, here, so the bucket-path
    // decision emitters don't double-count them.
    if let Some(count) = eligible_nodes {
        pplx_space_metrics::sandbox::admission::cluster_eligible_nodes(count);
    }
    pplx_space_metrics::sandbox::admission::cluster_refill_rps(config.refill_rps);

    if should_reject_empty(eligible_nodes, config.refill_rps) {
        emit_decision(
            class,
            AdmissionDecision::Rejected,
            AdmissionRejectReason::EmptyCluster,
            None,
        );
        return Err(ClusterAdmissionError::EmptyCluster {
            message: "Server capacity limit: no eligible sandbox activation nodes are \
                      available. Not specific to your client — retry with backoff."
                .to_string(),
            // This guard runs before the bucket, so there is no deficit to
            // derive an earliest second from: start at the header floor and
            // jitter above it, the same window a shed rate-limit draws.
            //
            // One window for both, and *not* because the two conditions last
            // the same time — it is because neither carries a duration signal.
            // `Some(0)` does not mean one thing: the two `Substrate` impls of
            // `eligible_start_node_count` filter on different properties, so
            // what has to change for the count to become non-zero — and how
            // long that takes — differs by substrate and is not summarizable
            // here. Read `direct.rs` and `kubernetes/mod.rs` for the actual
            // predicates. No hint can size a condition whose duration is that
            // undetermined, so de-synchronizing the callers is the whole of
            // what one can honestly buy, and that argument does not vary
            // between the two rejections.
            //
            // The cost this accepts: an empty-cluster requeue *counts* against
            // the dispatcher's attempt budget. A rate-limited one does not,
            // because `cluster_admission_controller_error` in
            // `controllers::sandbox::lifecycle` attaches
            // `ClusterActivationAdmission` to that variant and deliberately
            // omits it here — this path is a bounded retry, not indefinite
            // backpressure. The budget is a count — 16 attempts by
            // default — not a deadline, so pacing them 1–5s apart instead of 1s
            // flat multiplies the wall clock a queued create spends before
            // terminal failure by about 3x on average, 5x worst case, over the
            // 15 gaps a 16-attempt budget buys. Both figures shrink toward 1
            // once the dispatcher's tick is counted, since it is a constant
            // addend on either side. Erring long is the right direction for a
            // condition that may clear on its own, and it buys no execution
            // slot: releasing with a hint clears the claim, so the operation
            // waits unclaimed.
            retry_after: jittered_retry_after(RETRY_AFTER_FLOOR),
        });
    }

    Ok(true)
}

/// The `Retry-After` hint for a bucket rejection, paced by the configured refill
/// rate and then jittered above it.
///
/// One function rather than two composed at the call site, so the floor cannot
/// be dropped by changing an argument: `remaining` sizes only *how long until
/// the missing token exists*, and it cannot size the wait itself — on a
/// rejection it is always in `[0, 1)` however many callers are queued behind it.
/// Everything above the floor is therefore policy, and the policy is to spread
/// the shed population rather than pick one number for all of it.
fn rate_limited_retry_after(remaining: f64, refill_rps: f64) -> Duration {
    // Every activation requests exactly one token, so the deficit is whatever
    // fraction of that token the bucket is short.
    jittered_retry_after(earliest_refill(1.0 - remaining, refill_rps))
}

/// Sample the `Retry-After` hint for one shed activation: `earliest` — the first
/// second in which the caller could actually succeed — plus uniform whole-second
/// jitter.
///
/// Sampled independently per rejection rather than per caller, so a client that
/// retries and is shed again draws a fresh offset instead of staying in the
/// cohort it first landed in.
fn jittered_retry_after(earliest: Duration) -> Duration {
    let offset = rand::rng().random_range(0..=RETRY_AFTER_JITTER.as_secs());
    retry_after_with_jitter(earliest, Duration::from_secs(offset))
}

/// Place a hint `jitter` above `earliest`, having first clamped `earliest`
/// into `[RETRY_AFTER_FLOOR, RETRY_AFTER_CAP]`. Split from the sampler so the
/// window's ends are pinned by an exact test rather than a statistical one.
///
/// Takes whole seconds from `earliest` and discards any remainder, which is the
/// *opposite* direction from [`earliest_refill`]'s deliberate round-up — so a
/// fractional `earliest` would shorten the hint. Safe only because both callers
/// pass whole seconds; a third caller must round before calling, not after.
///
/// The clamp lands on the base and the offset is added *after*, so the widest
/// hint is `RETRY_AFTER_CAP + RETRY_AFTER_JITTER`. Clamping the sum instead
/// would read as tidier and would hand every caller at a sub-hourly refill rate
/// the identical hint — reinstating, in the one regime where the cap is
/// reachable, exactly the synchronization this window exists to break.
///
/// Whole seconds throughout, because both consumers of the hint are
/// integer-seconds and they truncate in *opposite* directions: the header rounds
/// a fractional remainder up and clamps to `1`
/// (`controllers::retry_after_header_value`), while the async create path
/// truncates down through `duration_secs_i64` in `public_operations`, which is
/// `as_secs()` — so a sub-second hint would set `next_attempt_at = NOW()` and
/// turn backpressure into an immediate re-claim.
fn retry_after_with_jitter(earliest: Duration, jitter: Duration) -> Duration {
    Duration::from_secs(
        earliest
            .as_secs()
            .clamp(RETRY_AFTER_FLOOR.as_secs(), RETRY_AFTER_CAP.as_secs())
            .saturating_add(jitter.as_secs()),
    )
}

/// The earliest whole second in which `deficit` tokens can have accumulated at
/// `refill_rps`, floored at [`RETRY_AFTER_FLOOR`].
///
/// This is the floor under any rate-limit hint the cap does not bind (see
/// [`RETRY_AFTER_CAP`], which outranks it above an hour): a hint falling due
/// before the token does guarantees a futile retry, the same defect as one that
/// is too short, just caused by arithmetic rather than by policy.
///
/// The reason it usually disappears: the Lua bucket rejects only when
/// `tokens < requested` and every activation requests `1.0`, so `deficit` is
/// always in `(0, 1]`. At any `refill_rps >= 1` that puts the answer at or under
/// one second, at or below what the header can carry, so the floor is the whole
/// of what the caller sees — at 6/s and at 60/s alike. The arithmetic only becomes
/// visible below one token per second, and there it is real: 0.1/s against an
/// empty bucket is a genuine 10s wait. [`sanitize_config`] accepts any positive
/// rate and `scripts/tests/smoke-kind-rate-limiters.sh` runs at 0.1, so that
/// regime is exercised rather than hypothetical.
fn earliest_refill(deficit: f64, refill_rps: f64) -> Duration {
    let ratio = deficit / refill_rps;
    // A non-positive ratio from a non-positive *deficit* means there is nothing
    // to wait for — the token is already there. Split out rather than folded
    // into the error arm below, because `try_from_secs_f64` rejects a negative
    // the same way it rejects an unrepresentable one, and the two want
    // *opposite* fallbacks. Not reachable today: `remaining` is confined to
    // `[0, 1)` so `deficit` is in `(0, 1]`, and a non-positive `refill_rps` —
    // which would make the ratio non-positive for the opposite reason, the
    // token never arriving — cannot get here either, because `sanitize_config`
    // replaces it and `should_reject_empty` rejects on it first. A second
    // caller or a per-class `requested` would reach the deficit case, and the
    // wrong branch would pace an already-satisfiable activation by an hour.
    //
    // NaN fails this comparison and falls through to the saturating arm, which
    // is intended. No claim is made here about how a NaN or an infinity could
    // arrive: the bucket's own `math.min(burst, …)` clamps some of them away
    // before they are ever returned, and which ones is Lua-semantics detail
    // that has been got wrong twice. The guard is unconditional instead, and
    // every non-finite input lands on the cap rather than the floor.
    if ratio <= 0.0 {
        return RETRY_AFTER_FLOOR;
    }
    let seconds = Duration::try_from_secs_f64(ratio)
        // Round a partial second up — the direction the header renderer rounds —
        // so the hint does not fall due before the token exists.
        .map(|refill| {
            refill
                .as_secs()
                .saturating_add(u64::from(refill.subsec_nanos() > 0))
        })
        // NaN, or a wait too large for a `Duration`. Both mean a configured rate
        // small enough that no expressible hint is honest, so saturate and let
        // `retry_after_with_jitter`'s clamp resolve it to `RETRY_AFTER_CAP`.
        // Erring long here merely delays a caller; erring short would be the
        // futile-retry loop this function exists to prevent. This is also what
        // keeps the create path panic-free: `refill_rps = 1e-320` passes
        // `valid_positive_f64`, and `1.0 / 1e-320` is infinity, which
        // `Duration::from_secs_f64` panics on.
        .unwrap_or(u64::MAX)
        .max(RETRY_AFTER_FLOOR.as_secs());
    Duration::from_secs(seconds)
}

/// Emit one admission decision. `tokens` is the bucket remainder when the
/// decision came from a bucket consume, `None` on the fail-open paths where the
/// count is unknown. The refill-rate and eligible-node gauges are emitted once
/// upstream in [`prepare_activation_admission`], not here.
fn emit_decision(
    class: ClusterActivationClass,
    decision: AdmissionDecision,
    reason: AdmissionRejectReason,
    tokens: Option<f64>,
) {
    pplx_space_metrics::sandbox::admission::cluster_decision(
        class.metric_class(),
        decision,
        reason,
    );
    if let Some(tokens) = tokens {
        pplx_space_metrics::sandbox::admission::cluster_tokens(tokens);
    }
}

/// Whether admission should reject with `empty_cluster` before touching the
/// bucket. Only a *confident* zero rejects: `None` (capacity unknown, e.g. a
/// cold Kubernetes node cache) fails open. A non-positive refill rate can never
/// accumulate a token, so it also rejects rather than wedging silently.
fn should_reject_empty(eligible_nodes: Option<u64>, refill_rps: f64) -> bool {
    eligible_nodes == Some(0) || refill_rps <= 0.0
}

fn sanitize_config(config: ClusterSandboxAdmissionConfig) -> ClusterSandboxAdmissionConfig {
    let defaults = ClusterSandboxAdmissionConfig::default();
    ClusterSandboxAdmissionConfig {
        enabled: config.enabled,
        refill_rps: valid_positive_f64(config.refill_rps).unwrap_or(defaults.refill_rps),
        // A burst below one token can never satisfy the one-token consume every
        // activation requests, permanently rate-limiting the cluster; clamp up.
        burst: valid_positive_f64(config.burst)
            .map(|burst| burst.max(1.0))
            .unwrap_or(defaults.burst),
        redis_url: config.redis_url,
    }
}
