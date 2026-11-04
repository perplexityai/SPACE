use std::sync::Arc;

use pplx_space_metrics::feature_flags as feature_flag_metrics;

use crate::{
    Attributes, BoolFlag, FeatureFlagProvider, FloatFlag, IntegerFlag, JsonFlag, ProviderState,
    StringFlag, Unresolved, policy,
};

/// Typed flag reads over whichever [`FeatureFlagProvider`] was installed,
/// falling back to each flag's own declaration when nothing resolves.
/// Cloning shares one provider.
#[derive(Clone)]
pub struct FeatureFlagClient {
    provider: Option<Arc<dyn FeatureFlagProvider>>,
}

/// Fixed boolean assignments for tests. Only `bool_value` resolves anything;
/// the other four report no assignment, so every non-boolean read falls
/// through to its flag's declared fallback — the same contract a backend with
/// those flags unset has.
struct FixedBools(std::collections::HashMap<String, bool>);

/// Fixed string assignments for tests. Only a string read resolves anything;
/// every other typed read falls through to its flag's declared fallback.
struct FixedStrings(std::collections::HashMap<String, String>);

/// Fixed JSON assignments for tests. Only a JSON read resolves anything; every
/// other typed read falls through to its flag's declared fallback.
struct FixedJson(std::collections::HashMap<String, serde_json::Value>);

impl FeatureFlagProvider for FixedJson {
    /// A fixed map has no configuration to wait for.
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn json_value(
        &self,
        flag_key: &str,
        _subject_key: &str,
        _attributes: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        self.0
            .get(flag_key)
            .cloned()
            .ok_or(Unresolved::NoAssignment)
    }
}

impl FeatureFlagProvider for FixedStrings {
    /// A fixed map has no configuration to wait for.
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(
        &self,
        flag_key: &str,
        _subject_key: &str,
        _attributes: &Attributes,
    ) -> Result<String, Unresolved> {
        self.0
            .get(flag_key)
            .cloned()
            .ok_or(Unresolved::NoAssignment)
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        Err(Unresolved::NoAssignment)
    }
}

impl FeatureFlagProvider for FixedBools {
    /// A fixed map has no configuration to wait for.
    fn state(&self) -> ProviderState {
        ProviderState::Ready
    }

    fn bool_value(
        &self,
        flag_key: &str,
        _subject_key: &str,
        _attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        self.0
            .get(flag_key)
            .copied()
            .ok_or(Unresolved::NoAssignment)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::NoAssignment)
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        Err(Unresolved::NoAssignment)
    }
}

/// Which provider backends this binary was built with, for the log line that
/// fires when none of them ends up installed. A reader needs to tell "rebuild
/// with a provider" apart from "configure the provider you have".
const COMPILED_PROVIDERS: &str = {
    #[cfg(all(feature = "eppo", feature = "flagd"))]
    {
        "eppo, flagd"
    }
    #[cfg(all(feature = "eppo", not(feature = "flagd")))]
    {
        "eppo"
    }
    #[cfg(all(feature = "flagd", not(feature = "eppo")))]
    {
        "flagd"
    }
    #[cfg(not(any(feature = "eppo", feature = "flagd")))]
    {
        "none"
    }
};

impl FeatureFlagClient {
    #[must_use]
    pub fn disabled() -> Self {
        Self { provider: None }
    }

    /// Test double resolving the given boolean flags to fixed values.
    /// Flags absent from the map — and every non-boolean typed read — resolve
    /// to the flag's declared fallback, exactly like a backend with those flags
    /// unset.
    #[must_use]
    pub fn fixed_bools<K, I>(flags: I) -> Self
    where
        K: Into<String>,
        I: IntoIterator<Item = (K, bool)>,
    {
        Self::from_provider(FixedBools(
            flags
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        ))
    }

    /// Test double resolving the given string flags to fixed values.
    /// Flags absent from the map — and every non-string typed read — resolve to
    /// the flag's declared fallback, exactly like a backend with those flags
    /// unset.
    #[must_use]
    pub fn fixed_strings<K, V, I>(flags: I) -> Self
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        Self::from_provider(FixedStrings(
            flags
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
        ))
    }

    /// Test double resolving the given JSON flags to fixed documents.
    /// Flags absent from the map — and every non-JSON typed read — resolve to
    /// the flag's declared fallback, exactly like a backend with those flags
    /// unset.
    #[must_use]
    pub fn fixed_json<K, I>(flags: I) -> Self
    where
        K: Into<String>,
        I: IntoIterator<Item = (K, serde_json::Value)>,
    {
        Self::from_provider(FixedJson(
            flags
                .into_iter()
                .map(|(key, value)| (key.into(), value))
                .collect(),
        ))
    }

    /// Install a provider. Clones of this client share the provider and keep it
    /// alive; the provider is dropped with the last clone. This neither waits
    /// for the provider to become ready nor fetches any configuration, so reads
    /// before the provider has assignments resolve
    /// [`Unresolved::ProviderUnready`].
    #[must_use]
    pub fn from_provider(provider: impl FeatureFlagProvider + 'static) -> Self {
        Self {
            provider: Some(Arc::new(provider)),
        }
    }

    /// The client a service gets from its environment: the provider compiled
    /// in and configured by environment variables, or a disabled client when
    /// none is configured.
    ///
    /// Eppo never blocks on a configuration fetch — evaluations resolve
    /// nothing until assignments arrive. flagd does block, once: its provider
    /// connects to its flag source and waits for an initial flag set before
    /// this returns, so a deployment that asked for flagd and cannot reach it
    /// finds out at startup instead of serving declared fallbacks while
    /// looking healthy.
    ///
    /// Providers are tried in order: flagd when `SPACE_FLAGD_ENABLED` is
    /// truthy, then Eppo. Each is skipped when its feature is not compiled in,
    /// and installing none of them is a supported configuration rather than an
    /// error.
    ///
    /// A flagd that was asked for and failed to build falls through to Eppo,
    /// but does not fall through to nothing: with no other provider
    /// configured it installs [`crate::UnavailableProvider`], so it reports
    /// [`ProviderState::Unready`] rather than the [`ProviderState::Absent`]
    /// of a deployment that named no backend at all.
    ///
    /// With the default `eppo` feature, `EPPO_SDK_KEY` decides between all
    /// three [`ProviderState`]s (see [`crate::eppo::EppoProvider::from_env`]):
    /// unset is [`ProviderState::Absent`], set and started is
    /// [`ProviderState::Unready`] until the first configuration lands, and set
    /// but failed to start is [`ProviderState::Unready`] permanently, via
    /// [`crate::UnavailableProvider`]. Built without the feature there is no
    /// provider to install and this is always `Absent`.
    #[must_use]
    pub fn from_env() -> Self {
        let client = Self::install_from_env();
        match client.provider_state() {
            // Warn, not info: a deployment in this state cannot have any flag
            // changed without a redeploy, and a service whose flags all sit at
            // their declared fallbacks should learn why from its logs rather
            // than from its Cargo features.
            //
            // Reached at *runtime*, not gated on a `cfg`, because "no
            // provider was installed" and "no provider was compiled in" are
            // different states and only the first is what went wrong here. A
            // `cfg` can only see the second, so it misses every deployment
            // that has a provider compiled in and installs none: `eppo`
            // compiled with no `EPPO_SDK_KEY`, and `flagd` compiled and
            // started without `SPACE_FLAGD_ENABLED` — the one backend that
            // needs an opt-in beyond its own configuration, and so the only
            // one that can be present, healthy, and never asked for.
            //
            // Naming what *is* compiled is what makes the line actionable: it
            // separates "rebuild with a provider feature" from "set the
            // variable the provider you already have is waiting for".
            ProviderState::Absent => tracing::warn!(
                compiled_providers = COMPILED_PROVIDERS,
                "no feature-flag provider was installed; every flag resolves to its \
                 declared fallback and cannot be changed without a redeploy"
            ),
            // `unready` at startup is the expected state, not a fault: the
            // provider's first configuration has not landed yet. Logged anyway
            // so a process that never leaves it has a first line to compare
            // against.
            state => tracing::info!(
                provider_state = %state,
                "resolved the feature flag provider from the environment"
            ),
        }
        client
    }

    /// Install whichever provider the environment configures, or none.
    ///
    /// Separate from [`Self::from_env`] so the startup line above is written
    /// once and covers every build: the arms below are `cfg`-gated and their
    /// shape differs per feature combination, while "what did we end up with"
    /// is the same question in all of them.
    fn install_from_env() -> Self {
        // flagd is checked first because it is the only provider that has to be
        // asked for: `SPACE_FLAGD_ENABLED` is an explicit opt-in, so a
        // deployment that sets it has said which backend it wants. Eppo's
        // signal is its SDK key, which a SPACE cluster running flagd would not
        // have set anyway.
        #[cfg(feature = "flagd")]
        if crate::openfeature::flagd::enabled_in_env() {
            match crate::openfeature::flagd::in_process_from_env() {
                Ok(provider) => return Self::from_provider(provider),
                // Deliberately not fatal, and deliberately louder than Eppo's
                // equivalent: flagd was explicitly asked for and did not come
                // up, so this is a misconfiguration rather than an absent
                // optional backend.
                //
                // It says "falling through", not "flags are disabled", because
                // it does not know yet: a build with both providers compiled
                // and `EPPO_SDK_KEY` set installs Eppo below and serves flags
                // normally. Claiming disabled here would send whoever reads
                // this line looking for an outage that isn't happening — and
                // the provider-less case has its own warning in `from_env`.
                Err(err) => {
                    tracing::error!(
                        %err,
                        "{} is set but the flagd provider failed to start; falling through to the remaining providers",
                        crate::openfeature::flagd::SPACE_FLAGD_ENABLED_ENV
                    );
                    // If nothing else installs, this deployment is
                    // configured-and-broken, which is not the empty slot.
                    // [`ProviderState::Absent`] is the *deliberate* state — a
                    // deployment that named no backend — and an operator who
                    // set `SPACE_FLAGD_ENABLED` and got nothing is in the
                    // other situation. `UnavailableProvider` is what keeps
                    // them apart when the failure happens before there is
                    // anything to ask; it changes no gate's answer, only what
                    // `provider_state` and the `outcome` tag report.
                    return Self::eppo_from_env()
                        .unwrap_or_else(|| Self::from_provider(crate::UnavailableProvider));
                }
            }
        }

        Self::eppo_from_env().unwrap_or_else(Self::disabled)
    }

    /// The Eppo client this environment configures, or `None` when it
    /// configures none — including because the SDK is not compiled in.
    ///
    /// `None` rather than a disabled client so a caller can put something else
    /// in the slot: the flagd arm above needs "Eppo took it" and "the slot is
    /// still free" to be different answers.
    fn eppo_from_env() -> Option<Self> {
        #[cfg(feature = "eppo")]
        {
            let client = Self::for_eppo_startup(crate::eppo::EppoProvider::from_env());
            client.is_configured().then_some(client)
        }
        #[cfg(not(feature = "eppo"))]
        {
            None
        }
    }

    /// Turn an Eppo startup outcome into a client.
    ///
    /// Separate from [`Self::from_env`] because the arm that matters is the one
    /// hardest to provoke: `eppo::Client::start_poller_thread` fails only when
    /// a background thread cannot be spawned, so there is no environment a
    /// test can set to reach [`crate::eppo::EppoStartup::Failed`] through
    /// `from_env`. Constructing the outcome directly is what makes the
    /// configured-but-broken deployment testable at all.
    #[cfg(feature = "eppo")]
    pub(crate) fn for_eppo_startup(startup: crate::eppo::EppoStartup) -> Self {
        match startup {
            crate::eppo::EppoStartup::Started(provider) => Self::from_provider(*provider),
            // Configured and broken is *not* the empty slot. An operator who
            // set `EPPO_SDK_KEY` and got nothing wants each flag's declared
            // failure behavior, where a deployment that set nothing wants its
            // own environment to stand — and `crate::policy` can only tell
            // those apart if this one reports a provider.
            crate::eppo::EppoStartup::Failed => Self::from_provider(crate::UnavailableProvider),
            crate::eppo::EppoStartup::NotConfigured => Self::disabled(),
        }
    }

    /// Whether a provider exists and can serve assignments right now.
    ///
    /// This is the only statement of what an installed provider does and does
    /// not promise, so the trait and module docs link here rather than
    /// restating it. [`ProviderState::Ready`] means the provider has
    /// configuration to evaluate against — not that any particular flag has an
    /// assignment, and not that a read of a given type will resolve.
    ///
    /// Prefer [`Self::gate_enabled`] over branching on this directly: a caller
    /// that wants "fall through to local configuration when no provider is
    /// installed" is describing the env-master-first policy, and
    /// [`crate::policy`] already implements it.
    #[must_use]
    pub fn provider_state(&self) -> ProviderState {
        self.provider
            .as_ref()
            .map_or(ProviderState::Absent, |provider| provider.state())
    }

    /// Whether a provider is installed at all, ready or not.
    ///
    /// Coarser than [`Self::provider_state`], and says nothing about whether
    /// an unready provider will ever become ready: one pointed at an
    /// unreachable backend stays installed and stays unready. Kept because
    /// "was a backend configured at all" is a question about deployment shape
    /// that a caller may want separately from whether it is working.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.provider.is_some()
    }

    /// Apply the env-master-first policy to a boolean gate: read `flag`, then
    /// layer it under `env_enabled`. See [`crate::policy`] for the policy and
    /// for why every reason a read resolves nothing takes the same branch.
    ///
    /// Taking the env master as an argument is the point of this method. The
    /// alternative — a bare read at the call site, ANDed with an env var read
    /// somewhere nearby — is what let the precedence drift.
    #[must_use]
    pub fn gate_enabled(&self, env_enabled: bool, flag: BoolFlag, subject_key: &str) -> bool {
        self.gate_enabled_with_attributes(env_enabled, flag, subject_key, Attributes::default())
    }

    /// [`Self::gate_enabled`] with targeting attributes.
    ///
    /// The read happens even when `env_enabled` is false, and its outcome is
    /// still counted. Short-circuiting would save an in-memory evaluation per
    /// request and cost the `feature_flags.evaluation` series for that flag
    /// entirely while the env master is off — at which point zero series means
    /// either "the master is off" or "this code path is never reached", which
    /// is the kind of ambiguity this whole change exists to remove.
    ///
    /// For two of the three call sites it also matches what they did before:
    /// both limiters evaluated `env && flag` with both sides already resolved.
    /// The third,
    /// `ClusterApiClients::client_for_subject` in
    /// `pplx-space-data-plane-server`, keeps its short-circuit and reads the
    /// flag once at construction instead. Its master is off on every
    /// deployment and it sits on a request path, where a per-read emission is
    /// not a cheap increment: `DatadogSink::emit_dogstatsd` does no
    /// aggregation, so each call is two `format!`s, a clone of the constant
    /// tags, a `join`, and a synchronous `UdpSocket::send`. The one read keeps
    /// the series; only a rate that restated the request rate is gone.
    ///
    /// `Attributes` is taken by value throughout this API and never consumed,
    /// so a caller holding a long-lived one pays a map clone it cannot avoid.
    /// That one is worth fixing, not from here.
    #[must_use]
    pub fn gate_enabled_with_attributes(
        &self,
        env_enabled: bool,
        flag: BoolFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> bool {
        policy::gate_enabled(
            env_enabled,
            flag,
            self.resolve_bool(flag, subject_key, attributes),
        )
    }

    /// Apply the env-master-first policy to a JSON override: read `flag`, then
    /// layer it under `env_enabled`. See [`crate::policy::Override`] for why
    /// the result carries three states.
    ///
    /// The override twin of [`Self::gate_enabled`], and it exists for the same
    /// reason: when composing `policy::override_allowed(env,
    /// client.resolve_json(..))` is the only spelling, the master is something
    /// a new call site has to remember rather than something it is handed. No
    /// existing caller had dropped one — the path this closes is a read built
    /// without a master, which is what the known-egress list was.
    #[must_use]
    pub fn json_override(
        &self,
        env_enabled: bool,
        flag: JsonFlag,
        subject_key: &str,
    ) -> policy::Override<serde_json::Value> {
        self.json_override_with_attributes(env_enabled, flag, subject_key, Attributes::default())
    }

    /// [`Self::json_override`] with targeting attributes.
    ///
    /// The read happens even when `env_enabled` is false, and its outcome is
    /// still counted — same trade as [`Self::gate_enabled_with_attributes`],
    /// for the same reason.
    #[must_use]
    pub fn json_override_with_attributes(
        &self,
        env_enabled: bool,
        flag: JsonFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> policy::Override<serde_json::Value> {
        policy::override_allowed(
            env_enabled,
            self.resolve_json(flag, subject_key, attributes),
        )
    }

    /// A boolean flag's value, or its declared fallback when nothing resolves.
    #[must_use]
    pub fn bool_value(&self, flag: BoolFlag, subject_key: &str) -> bool {
        self.bool_value_with_attributes(flag, subject_key, Attributes::default())
    }

    /// [`Self::bool_value`] with targeting attributes.
    #[must_use]
    pub fn bool_value_with_attributes(
        &self,
        flag: BoolFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> bool {
        self.resolve_bool(flag, subject_key, attributes)
            .unwrap_or_else(|_| flag.when_unresolved())
    }

    /// A boolean flag's value, or why it resolved nothing.
    ///
    /// The reason-carrying form. Use it when the *reason* changes the answer —
    /// which for an env-mastered gate it does, so prefer [`Self::gate_enabled`]
    /// there rather than matching on this by hand.
    pub fn resolve_bool(
        &self,
        flag: BoolFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Result<bool, Unresolved> {
        let resolved = self.resolve(flag.key(), |provider| {
            provider.bool_value(flag.key(), subject_key, &attributes)
        });
        if let Ok(value) = resolved {
            feature_flag_metrics::boolean_assignment(flag.key(), value);
        }
        resolved
    }

    /// An integer flag's value, or its declared fallback when nothing resolves.
    #[must_use]
    pub fn integer_value(&self, flag: IntegerFlag, subject_key: &str) -> i64 {
        self.resolve_integer(flag, subject_key, Attributes::default())
            .unwrap_or_else(|_| flag.when_unresolved())
    }

    /// An integer flag's value, or why it resolved nothing.
    pub fn resolve_integer(
        &self,
        flag: IntegerFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Result<i64, Unresolved> {
        self.resolve(flag.key(), |provider| {
            provider.integer_value(flag.key(), subject_key, &attributes)
        })
    }

    /// A float flag's value, or its declared fallback when nothing resolves.
    #[must_use]
    pub fn float_value(&self, flag: FloatFlag, subject_key: &str) -> f64 {
        self.resolve_float(flag, subject_key, Attributes::default())
            .unwrap_or_else(|_| flag.when_unresolved())
    }

    /// A float flag's value, or why it resolved nothing.
    pub fn resolve_float(
        &self,
        flag: FloatFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Result<f64, Unresolved> {
        self.resolve(flag.key(), |provider| {
            provider.float_value(flag.key(), subject_key, &attributes)
        })
    }

    /// A string flag's value, or its declared fallback when nothing resolves.
    #[must_use]
    pub fn string_value(
        &self,
        flag: StringFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> String {
        self.resolve_string(flag, subject_key, attributes)
            .unwrap_or_else(|_| flag.when_unresolved().to_owned())
    }

    /// A string flag's value, or why it resolved nothing.
    pub fn resolve_string(
        &self,
        flag: StringFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Result<String, Unresolved> {
        self.resolve(flag.key(), |provider| {
            provider.string_value(flag.key(), subject_key, &attributes)
        })
    }

    /// A JSON flag's value, or `None` when nothing resolved.
    ///
    /// Master-free: it applies no policy, so a JSON flag layered under an env
    /// var wants [`Self::json_override`] instead. Every env-mastered JSON read
    /// in this workspace goes through that one; this form survives for a flag
    /// with no master and for test fakes. The difference is not visible in a
    /// diff —
    /// both return "no document" for an unresolved read, and only the override
    /// form also returns it for a master an operator turned off.
    ///
    /// A JSON flag declares no fallback ([`JsonFlag`] says why), so the caller
    /// deserializes, validates, and supplies its own local configuration on
    /// `None`. Use [`Self::resolve_json`] when the reason matters.
    #[must_use]
    pub fn json_value(
        &self,
        flag: JsonFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Option<serde_json::Value> {
        self.resolve_json(flag, subject_key, attributes).ok()
    }

    /// A JSON flag's value, or why it resolved nothing.
    pub fn resolve_json(
        &self,
        flag: JsonFlag,
        subject_key: &str,
        attributes: Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        self.resolve(flag.key(), |provider| {
            provider.json_value(flag.key(), subject_key, &attributes)
        })
    }

    /// The shared tail of every typed read: ask the installed provider, or
    /// report that there is none, and count the outcome either way.
    ///
    /// Counting here rather than in each provider is what makes "no provider
    /// installed" and "provider installed and broken" separable in Datadog. A
    /// provider-free deployment emits no provider telemetry by definition, so a
    /// tally owned by the providers could never have distinguished the two —
    /// it would have shown the absence of a series for both.
    fn resolve<T>(
        &self,
        flag_key: &str,
        read: impl FnOnce(&Arc<dyn FeatureFlagProvider>) -> Result<T, Unresolved>,
    ) -> Result<T, Unresolved> {
        let resolved = match self.provider.as_ref() {
            // A provider is asked only if it claims to have a backend. One
            // reporting `Absent` says it has none, and the trait promises the
            // client then treats it exactly like an empty slot — so it must not
            // be asked, and its answer must not reach the caller as an
            // assignment. No shipped provider reports `Absent`, so this costs
            // one atomic load per read and changes nothing today; it is here so
            // the state and the reads cannot disagree.
            Some(provider) if provider.state() != ProviderState::Absent => read(provider),
            _ => Err(Unresolved::NoProvider),
        };
        feature_flag_metrics::evaluation(
            flag_key,
            resolved.as_ref().map_or_else(
                |unresolved| evaluation_outcome(*unresolved),
                |_| feature_flag_metrics::EvaluationOutcome::Assigned,
            ),
        );
        resolved
    }
}

/// The reason a read resolved nothing, as the metrics crate's tag vocabulary.
///
/// Two enums rather than one because the metrics crate cannot depend on this
/// one — it is this crate's dependency, not the other way round. A new
/// [`Unresolved`] variant is a compile error here, and
/// `every_reason_maps_to_a_distinct_evaluation_outcome` keeps the mapping
/// injective so two reasons cannot collapse into one indistinguishable tag.
const fn evaluation_outcome(unresolved: Unresolved) -> feature_flag_metrics::EvaluationOutcome {
    match unresolved {
        Unresolved::NoProvider => feature_flag_metrics::EvaluationOutcome::NoProvider,
        Unresolved::ProviderUnready => feature_flag_metrics::EvaluationOutcome::ProviderUnready,
        Unresolved::NoAssignment => feature_flag_metrics::EvaluationOutcome::Unassigned,
        Unresolved::EvaluationFailed => feature_flag_metrics::EvaluationOutcome::Error,
    }
}

#[cfg(test)]
mod tests;
