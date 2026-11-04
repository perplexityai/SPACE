//! Eppo provider, compiled when the default `eppo` Cargo feature is enabled.
//! Every Eppo SDK type in this crate is confined to this module: the rest of the
//! crate speaks only [`FeatureFlagProvider`]'s vocabulary.

use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use pplx_space_metrics::feature_flags::{self as feature_flag_metrics, DiagnosticCode};

use self::env::{EPPO_BASE_URL_ENV, EPPO_SDK_KEY_ENV, eppo_sdk_key_from_env, non_empty_env};
use crate::{AttributeValue, Attributes, FeatureFlagProvider, ProviderState, Unresolved};

mod env;

/// Cap on the set of distinct flags tracked for the diagnostic below. The set is
/// what makes that diagnostic fire once per flag rather than once per
/// evaluation; this cap is what keeps the set — and the `flag_key` cardinality
/// of the diagnostic counter — bounded on a deployment that reads more unset
/// flags than this.
const MAX_DIAGNOSED_UNASSIGNED_FLAGS: usize = 64;

/// While no configuration has been seen, this bounds how often an unassigned
/// read fetches evaluation details *to check whether one arrived since* — at
/// most once per interval.
///
/// It is not a bound on details calls as a whole: the per-flag diagnostic
/// fetches them too, so up to [`MAX_DIAGNOSED_UNASSIGNED_FLAGS`] first-reads
/// plus one probe can each fetch inside a single interval. That burst is
/// bounded for the process's life; this interval bounds the part that is not.
///
/// It bounds probe *frequency*, not detection *latency*. A configuration that
/// lands is noticed no later than the first unassigned read occurring an
/// interval after the last probe — which on a reader slower than the interval
/// is its next read, whenever that comes. Sooner is common: a resolved
/// assignment, an evaluation error, or an unassigned read that claims a fresh
/// diagnosis slot all observe readiness without waiting for the interval.
///
/// The probe cannot run on every unassigned read. A provider that never becomes
/// ready is the *normal* shape of an Eppo misconfiguration — a wrong key or an
/// unreachable base URL surfaces inside the poller loop, not at construction
/// ([`crate::UnavailableProvider`] says so) — so "while unready" is not a window
/// that closes. Probing per read would double this crate's SDK calls for the
/// life of such a process, and space-api reads two flags on every create and
/// reconnect.
///
/// **Time, not a read count.** Sampling one read in N bounds the cost but leaves
/// the *classification* lagging by N reads of unknown spacing: a reconciler that
/// reads one flag per tick would report `ProviderUnready` for N ticks after its
/// configuration arrived — hours, on a slow loop — and a monitor on sustained
/// `outcome:provider_unready` would fire on a healthy provider. An interval
/// bounds that lag in wall clock instead, which is the unit the operator reading
/// the metric actually has. It also self-tunes: a hot reader pays one extra call
/// per interval, and a reader slower than the interval pays on every read, which
/// for a slow reader is nothing.
///
/// **What it still gives up.** Without any sampling, a read with no
/// configuration observed always fetched details, so `config_fetched_at`
/// classified that read from current evidence. A non-probing read answers from
/// the latch instead, so for up to one interval after a configuration arrives
/// such reads report `ProviderUnready` where `NoAssignment` is the truth, and
/// `state()` answers `Unready` on a provider that is in fact ready. That is a
/// diagnosis inaccuracy, not a behavior change — both reasons take the flag's
/// declared fallback through [`crate::policy`], and `state()` has no caller
/// outside this crate — and it is reachable only where *every* flag is
/// unassigned, since any resolved assignment or evaluation error latches
/// readiness for free.
const READINESS_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// `last_readiness_probe_millis` when no probe has run yet.
const NO_PROBE_YET: u64 = u64::MAX;

struct EppoClientConfig {
    sdk_key: String,
    base_url: Option<String>,
}

impl EppoClientConfig {
    fn from_env(sdk_key: String) -> Self {
        Self {
            sdk_key,
            base_url: non_empty_env(EPPO_BASE_URL_ENV),
        }
    }
}

/// Eppo adapter: owns the SDK client, its poller thread, targeting conversion,
/// and the Eppo-specific evaluation diagnostics. Install it with
/// [`crate::FeatureFlagClient::from_provider`], or let
/// [`crate::FeatureFlagClient::from_env`] do it.
pub struct EppoProvider {
    client: eppo::Client<'static>,
    poller_thread: Option<eppo::PollerThread>,
    /// Set the first time this provider sees evidence that the poller has
    /// delivered a configuration, and never cleared.
    ///
    /// The SDK offers no non-blocking readiness query. `PollerThread`'s two —
    /// `wait_for_configuration` and `wait_for_configuration_timeout` — both
    /// `block_on` the poller's own runtime, which panics when called from
    /// inside Tokio, and that is where every SPACE service constructs its
    /// client (pinned by
    /// `eppo_client_initializes_inside_tokio_runtime_without_panicking`).
    /// `Client`'s `configuration_store` is private with no accessor. So
    /// readiness here is *observed* from evaluations rather than asked for.
    /// Three observations set it. Any resolved assignment, which the SDK
    /// documents cannot happen before the first fetch ("The Client returns
    /// `None` for assignments before the first configuration is fetched",
    /// `eppo-5.2.1/src/poller.rs:23-25`). Any evaluation *error*, reachable
    /// only after the SDK evaluated against a loaded configuration, because a
    /// missing one yields `Ok(None)` instead
    /// (`eppo_core-10.0.0/src/eval/eval_assignment.rs:114,131`). And an
    /// unassigned read whose details carry a `config_fetched_at`.
    ///
    /// Latching rather than tracking the live state is deliberate, and the SDK
    /// agrees: `ConfigurationStore` exposes `set_configuration` and no way to
    /// clear it (`eppo_core-10.0.0/src/configuration_store.rs:36-44`), so a
    /// later fetch failure leaves the last configuration serving.
    /// `Unresolved::EvaluationFailed` already covers a read that breaks.
    configuration_seen: AtomicBool,
    /// Baseline for [`Self::last_readiness_probe_millis`], so the probe clock
    /// needs no wall time and no allocation.
    started_at: Instant,
    /// Milliseconds since [`Self::started_at`] at the last readiness probe, or
    /// [`NO_PROBE_YET`]. Millis-since-a-baseline rather than a
    /// `Mutex<Instant>` so the stamp fits an atomic: the alternative is a
    /// second lock acquisition on a path that already takes the diagnosis-set
    /// mutex unconditionally, for a value that is read and replaced and never
    /// read together with anything else. It is reached only on an unassigned
    /// read, and only while no configuration has been seen — on a latched
    /// provider it never runs at all.
    last_readiness_probe_millis: AtomicU64,
    diagnosed_unassigned_flags: std::sync::Mutex<std::collections::HashSet<String>>,
    /// Count of `get_assignment_details` calls, so a test can assert that a
    /// read answered from state rather than by asking the SDK again.
    ///
    /// Behind `cfg(test)` rather than unconditional, which is the narrower of
    /// two defensible trades. Unconditional would cost eight bytes per process
    /// and one relaxed increment on a path that already takes a mutex, and
    /// would leave one struct shape instead of two; `cfg(test)` keeps the
    /// production type free of state nothing in production reads, at the price
    /// of that second shape. Neither protects against a refactor moving the
    /// call and leaving the increment behind — only a test asserting the count
    /// is non-zero does, which is why one does.
    ///
    /// The two
    /// things bounding these calls bound different quantities:
    /// [`MAX_DIAGNOSED_UNASSIGNED_FLAGS`] caps the diagnostic's share for the
    /// process's whole life, while [`READINESS_PROBE_INTERVAL`] caps only the
    /// probe's *rate* — so a process that never reaches `Ready` keeps spending
    /// roughly one call per interval indefinitely, which is the cost that
    /// interval exists to make small rather than finite.
    ///
    /// It exists because "answered without a further SDK call" has no other
    /// observable: the details path returns the same `Unresolved` the latch
    /// does, so an assertion on the return value cannot tell them apart.
    #[cfg(test)]
    details_calls: AtomicUsize,
}

impl std::fmt::Debug for EppoProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EppoProvider")
            .finish_non_exhaustive()
    }
}

/// What reading `EPPO_SDK_KEY` and `EPPO_BASE_URL` produced.
///
/// Three outcomes rather than an `Option`, because the two failures differ:
/// nobody asked for a provider, versus somebody did and did not get one. The
/// client turns that into [`crate::ProviderState::Absent`] or
/// [`crate::ProviderState::Unready`], which is the distinction this whole
/// module's state model exists to preserve.
#[derive(Debug)]
pub enum EppoStartup {
    /// No `EPPO_SDK_KEY`, so no provider was asked for.
    NotConfigured,
    /// A key was set and the poller started. Says nothing about whether a
    /// configuration has arrived; see [`EppoProvider::state`].
    ///
    /// Boxed because `EppoProvider` dwarfs the other two variants and
    /// `clippy::large_enum_variant` is denied. The allocation is free in
    /// practice: this value is built once per process and handed straight to
    /// [`crate::FeatureFlagClient::from_provider`], which re-homes it in an
    /// `Arc` anyway.
    Started(Box<EppoProvider>),
    /// A key was set and startup failed. The error is already logged.
    Failed,
}

impl EppoProvider {
    /// Start polling using `EPPO_SDK_KEY` and optional `EPPO_BASE_URL`,
    /// reporting which of the three startup outcomes happened. Logs either
    /// failure. Does not block on the initial configuration fetch.
    #[must_use]
    pub fn from_env() -> EppoStartup {
        let Some(sdk_key) = eppo_sdk_key_from_env() else {
            tracing::info!("{EPPO_SDK_KEY_ENV} is unset; feature flags are disabled");
            return EppoStartup::NotConfigured;
        };

        match Self::from_sdk_key(EppoClientConfig::from_env(sdk_key)) {
            Ok(provider) => EppoStartup::Started(Box::new(provider)),
            Err(err) => {
                // Not "feature flags are disabled": the deployment asked for a
                // provider, so its flags take their declared failure behavior
                // rather than falling through to local configuration.
                tracing::warn!(%err, "failed to start the Eppo poller; every flag will resolve to its declared fallback");
                EppoStartup::Failed
            }
        }
    }

    fn from_sdk_key(config: EppoClientConfig) -> Result<Self, eppo::Error> {
        let mut client_config = eppo::ClientConfig::from_api_key(config.sdk_key);
        if let Some(base_url) = config.base_url {
            client_config = client_config.base_url(base_url);
        }

        let mut client = client_config.to_client();
        // Start the poller thread but do NOT block on the initial configuration
        // fetch: `wait_for_configuration_timeout` calls `block_on` internally,
        // which panics ("Cannot start a runtime from within a runtime") because
        // client construction happens inside the Tokio runtime at API startup.
        // Evaluations resolve nothing until the poller delivers config.
        let poller_thread = client.start_poller_thread()?;

        Ok(Self {
            client,
            poller_thread: Some(poller_thread),
            configuration_seen: AtomicBool::new(false),
            started_at: Instant::now(),
            last_readiness_probe_millis: AtomicU64::new(NO_PROBE_YET),
            diagnosed_unassigned_flags: std::sync::Mutex::new(std::collections::HashSet::new()),
            #[cfg(test)]
            details_calls: AtomicUsize::new(0),
        })
    }

    /// Whether this flag has not been diagnosed yet and is allowed to be,
    /// recording it as diagnosed if so.
    ///
    /// Separate from the diagnosis itself because the details call has two
    /// callers with different cadences: this one, once per flag, and the
    /// readiness probe, at most once per [`READINESS_PROBE_INTERVAL`]. The
    /// warn and its counter fire only for this one.
    fn claim_diagnosis_slot(&self, flag_key: &str) -> bool {
        let mut diagnosed_flags = self
            .diagnosed_unassigned_flags
            .lock()
            .expect("diagnosed unassigned feature flags mutex poisoned");
        if diagnosed_flags.contains(flag_key)
            || diagnosed_flags.len() >= MAX_DIAGNOSED_UNASSIGNED_FLAGS
        {
            return false;
        }
        diagnosed_flags.insert(flag_key.to_string());
        true
    }

    /// Whether this read should fetch evaluation details to look for a
    /// configuration, claiming the slot if so.
    ///
    /// True on the first call — a provider that is ready from the start must be
    /// classified correctly at once — and then at most once per
    /// [`READINESS_PROBE_INTERVAL`] of *stamped* readings. The
    /// compare-exchange is what makes that at-most-once across concurrent
    /// readers rather than once per thread.
    ///
    /// "Of stamped readings" is the precise claim: a reader descheduled for
    /// longer than the interval between taking its clock reading and winning
    /// the exchange stamps a stale one, which lets the next call claim
    /// immediately. That costs one extra details call and cannot compound,
    /// because a winning exchange only ever advances the stamp — reading past
    /// the first, which necessarily lowers it from the [`NO_PROBE_YET`]
    /// sentinel.
    ///
    /// Mutual exclusion across concurrent readers is established by reading
    /// this function, not by a test. A winning exchange can only store a
    /// reading at least one interval past the one it replaced, so each claim
    /// advances the stamp and a loser re-reads and declines. A threaded test
    /// cannot usefully pin it: the shipped code and a naive `load`/`store`
    /// diverge only while the stamp is still [`NO_PROBE_YET`] — afterwards both
    /// take the early return and are identical — so the divergence window is
    /// one load/store pair per provider, and a 16-thread test hits it a
    /// fraction of one percent of the time no matter how many iterations it
    /// runs. Deterministic coverage needs `loom`, or a barrier inside this
    /// function.
    fn claim_readiness_probe(&self) -> bool {
        let interval = u64::try_from(READINESS_PROBE_INTERVAL.as_millis()).unwrap_or(u64::MAX);
        // Clamped below the sentinel: were `now` ever to saturate to
        // `NO_PROBE_YET`, storing it would make every later call take the
        // never-probed branch and probe on every read — the exact cost this
        // interval exists to bound. Only reachable after ~584 million years of
        // uptime, and cheaper to exclude than to reason about.
        let now = u64::try_from(self.started_at.elapsed().as_millis())
            .unwrap_or(u64::MAX)
            .min(NO_PROBE_YET - 1);
        let mut last = self.last_readiness_probe_millis.load(Ordering::Relaxed);
        loop {
            // `saturating_sub`, because `now < last` happens under ordinary
            // contention and not just on a misbehaving clock: this thread can
            // read `now`, lose the exchange to a thread that read a later
            // `now`, and then re-evaluate its own stale reading against the
            // winner's. Saturating to zero answers "no probe", which is right;
            // plain subtraction would panic in debug.
            if last != NO_PROBE_YET && now.saturating_sub(last) < interval {
                return false;
            }
            match self.last_readiness_probe_millis.compare_exchange_weak(
                last,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                // Another reader claimed it; re-read and re-decide rather than
                // probing too.
                Err(observed) => last = observed,
            }
        }
    }

    /// How many times this provider has asked the SDK for evaluation details.
    #[cfg(test)]
    fn details_calls(&self) -> usize {
        self.details_calls.load(Ordering::Relaxed)
    }

    /// Make the readiness probe unclaimable, for a test whose assertion would
    /// otherwise depend on how much wall clock passed between two statements.
    ///
    /// Permanent by the clamp rather than by the clock: `now` is capped at
    /// `NO_PROBE_YET - 1`, which is the value stored here, so `now - last`
    /// saturates to zero on every later call and the interval never elapses.
    #[cfg(test)]
    fn suppress_readiness_probe(&self) {
        self.last_readiness_probe_millis
            .store(NO_PROBE_YET - 1, Ordering::Relaxed);
    }

    /// Classify an unassigned read, and diagnose it the first time this flag
    /// produces one.
    ///
    /// The SDK returns the same "no value" for a flag with no allocation for
    /// this subject and for a client that has no configuration at all, so the
    /// details call is the only thing that separates them. It runs when the
    /// diagnostic below needs it — at most once per flag, at most
    /// [`MAX_DIAGNOSED_UNASSIGNED_FLAGS`] times in all — and, while no
    /// configuration has been seen, at most once per
    /// [`READINESS_PROBE_INTERVAL`], so a configuration arriving later is
    /// noticed no later than the first unassigned read an interval after the
    /// last probe — and sooner whenever a read resolves, errors, or claims a
    /// fresh diagnosis slot.
    ///
    /// Tying it to the diagnostic alone would be the obvious shape — the
    /// diagnostic needs the details anyway — and is wrong: a process whose
    /// flags were all first read before its configuration arrived would have
    /// spent every slot while unready and then never look again, reporting
    /// `Unready` and `ProviderUnready` for the rest of its life. Running it on
    /// every unready read is wrong in the other direction; see
    /// [`READINESS_PROBE_INTERVAL`].
    fn classify_unassigned(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Arc<eppo::Attributes>,
    ) -> Unresolved {
        let diagnose = self.claim_diagnosis_slot(flag_key);
        let configuration_seen = self.configuration_seen.load(Ordering::Relaxed);
        // `!configuration_seen` first: once latched there is nothing left to
        // probe for. Deliberately untested — removing it costs one extra SDK
        // call per interval on a ready provider and changes no classification,
        // so there is no observable for a test to assert on.
        let probe_readiness = !configuration_seen && self.claim_readiness_probe();
        if !diagnose && !probe_readiness {
            // Nothing to ask the SDK for. Without a configuration observed,
            // the honest answer is that the provider cannot answer yet.
            return if configuration_seen {
                Unresolved::NoAssignment
            } else {
                Unresolved::ProviderUnready
            };
        }

        let subject_key_owned = subject_key.into();
        #[cfg(test)]
        self.details_calls.fetch_add(1, Ordering::Relaxed);
        let details = self
            .client
            .get_assignment_details(flag_key, &subject_key_owned, attributes)
            .evaluation_details;
        if details.config_fetched_at.is_some() {
            self.configuration_seen.store(true, Ordering::Relaxed);
        }

        if diagnose {
            let diagnostic_code = match details.flag_evaluation_code {
                Some(eppo::FlagEvaluationCode::Match) => DiagnosticCode::Match,
                Some(eppo::FlagEvaluationCode::ConfigurationMissing) => {
                    DiagnosticCode::ConfigurationMissing
                }
                Some(eppo::FlagEvaluationCode::FlagUnrecognizedOrDisabled) => {
                    DiagnosticCode::FlagUnrecognizedOrDisabled
                }
                Some(eppo::FlagEvaluationCode::DefaultAllocationNull) => {
                    DiagnosticCode::DefaultAllocationNull
                }
                Some(eppo::FlagEvaluationCode::TypeMismatch) => DiagnosticCode::TypeMismatch,
                Some(eppo::FlagEvaluationCode::UnexpectedConfigurationError) => {
                    DiagnosticCode::UnexpectedConfigurationError
                }
                None => DiagnosticCode::NotEvaluated,
            };
            feature_flag_metrics::unassigned_diagnostic(flag_key, diagnostic_code);
            tracing::warn!(
                flag_key,
                reason = diagnostic_code.as_str(),
                environment = details.environment_name.as_deref().unwrap_or("unknown"),
                config_fetched_at = ?details.config_fetched_at,
                config_published_at = ?details.config_published_at,
                "feature flag returned no assignment; resolving nothing"
            );
        }

        if details.config_fetched_at.is_some() {
            Unresolved::NoAssignment
        } else {
            Unresolved::ProviderUnready
        }
    }

    /// The shared tail of all five typed reads: hand back the assignment, or
    /// the reason there wasn't one.
    ///
    /// The `feature_flags.evaluation` tally is not emitted here — the client
    /// owns it, so a deployment with no provider at all is counted on the same
    /// metric. What stays here is the part only a backend can produce: the
    /// SDK's own evaluation code for an unassigned read.
    fn resolve<T>(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Arc<eppo::Attributes>,
        result: Result<Option<T>, eppo::EvaluationError>,
    ) -> Result<T, Unresolved> {
        match result {
            Ok(Some(value)) => {
                // An assignment cannot come from an absent configuration, so
                // this is the cheap readiness observation: no details call.
                self.configuration_seen.store(true, Ordering::Relaxed);
                Ok(value)
            }
            Ok(None) => Err(self.classify_unassigned(flag_key, subject_key, attributes)),
            Err(err) => {
                // An evaluation error proves a configuration exists: the SDK
                // maps a missing one to `Ok(None)`
                // (`eppo_core-10.0.0/src/eval/eval_assignment.rs:114,131`), so
                // `Err` is reachable only after `eval_flag` ran against a
                // loaded configuration. Without this store, a flag defined
                // with the wrong variation type — every read erroring — leaves
                // `state()` reporting `Unready` for the process's life while
                // the counter says `outcome:error`, the exact disagreement the
                // trait docs ask providers to avoid.
                self.configuration_seen.store(true, Ordering::Relaxed);
                // "resolving nothing" rather than "using default": four of the
                // five reads let the flag's declaration supply a fallback, but
                // `json_value` has none — its caller owns the fallback. This
                // one message covers all five, so it states what `resolve`
                // returns.
                tracing::warn!(
                    flag_key,
                    %err,
                    "feature flag evaluation failed; resolving nothing"
                );
                Err(Unresolved::EvaluationFailed)
            }
        }
    }

    /// Convert provider-neutral targeting inputs into the SDK's attribute map.
    fn sdk_attributes(attributes: &Attributes) -> Arc<eppo::Attributes> {
        Arc::new(
            attributes
                .iter()
                .map(|(key, value)| {
                    let value = match value {
                        AttributeValue::Bool(value) => eppo::AttributeValue::from(*value),
                        AttributeValue::Number(value) => eppo::AttributeValue::from(*value),
                        AttributeValue::String(value) => eppo::AttributeValue::from(value.clone()),
                        AttributeValue::Null => eppo::AttributeValue::null(),
                    };
                    (key.as_str().into(), value)
                })
                .collect(),
        )
    }
}

impl FeatureFlagProvider for EppoProvider {
    fn state(&self) -> ProviderState {
        if self.configuration_seen.load(Ordering::Relaxed) {
            ProviderState::Ready
        } else {
            ProviderState::Unready
        }
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        let subject = subject_key.into();
        let attributes = Self::sdk_attributes(attributes);
        let result = self
            .client
            .get_boolean_assignment(flag_key, &subject, &attributes);
        self.resolve(flag_key, subject_key, &attributes, result)
    }

    fn integer_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<i64, Unresolved> {
        let subject = subject_key.into();
        let attributes = Self::sdk_attributes(attributes);
        let result = self
            .client
            .get_integer_assignment(flag_key, &subject, &attributes);
        self.resolve(flag_key, subject_key, &attributes, result)
    }

    fn float_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<f64, Unresolved> {
        let subject = subject_key.into();
        let attributes = Self::sdk_attributes(attributes);
        let result = self
            .client
            .get_numeric_assignment(flag_key, &subject, &attributes);
        self.resolve(flag_key, subject_key, &attributes, result)
    }

    fn string_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<String, Unresolved> {
        let subject = subject_key.into();
        let attributes = Self::sdk_attributes(attributes);
        let result = self
            .client
            .get_string_assignment(flag_key, &subject, &attributes);
        self.resolve(flag_key, subject_key, &attributes, result)
            .map(|value| value.to_string())
    }

    fn json_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        let subject = subject_key.into();
        let attributes = Self::sdk_attributes(attributes);
        let result = self
            .client
            .get_json_assignment(flag_key, &subject, &attributes);
        self.resolve(flag_key, subject_key, &attributes, result)
            .map(|value| (*value).clone())
    }
}

impl Drop for EppoProvider {
    fn drop(&mut self) {
        if let Some(poller_thread) = &self.poller_thread {
            poller_thread.stop();
        }
    }
}

#[cfg(test)]
mod tests;
