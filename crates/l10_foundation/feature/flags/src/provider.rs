//! The vendor-neutral evaluation contract. Nothing here names a flag vendor or
//! depends on a vendor SDK; [`crate::eppo`] is one implementation of it.

use std::collections::HashMap;

/// Targeting inputs independent of a provider's SDK. Providers convert these
/// into whatever their backend takes.
pub type Attributes = HashMap<String, AttributeValue>;

/// One targeting attribute: the scalar shapes every flag backend agrees on.
///
/// `Null` is for an attribute the caller knows has no value, so a caller with an
/// optional attribute can pass it unconditionally instead of branching on
/// whether to insert the key at all.
///
/// It does **not** buy a distinction from omitting the key. Eppo's `IS_NULL`
/// computes "null" as *absent or present-and-null* (`eppo_core`
/// `eval/eval_rules.rs:71-75`), so a rule cannot tell the two apart; and a
/// backend with no null representation may only be able to drop the key. Treat
/// `Null` as a convenience for expressing absence, not as a third state.
#[derive(Clone, Debug, PartialEq)]
pub enum AttributeValue {
    Bool(bool),
    Number(f64),
    String(String),
    Null,
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        Self::Number(value)
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

/// Whether a flag provider exists and can serve assignments right now.
///
/// The distinction [`Self::Absent`] draws is the one a single per-read fallback
/// cannot: a deployment that configures no provider on purpose — OSS, Kind,
/// local dev — and a deployment whose provider broke both stop resolving flags,
/// and they want opposite treatment. The first wants its local configuration to
/// stand; the second wants the flag's declared failure behavior. `Absent` is
/// what lets [`crate::policy`] give them different answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProviderState {
    /// No provider is installed. Every flag resolves to its declaration's
    /// fallback and cannot change without a redeploy — the steady state of a
    /// deployment that configures no backend, not a fault.
    Absent,
    /// A provider is installed but has nothing to evaluate against yet: its
    /// first configuration has not arrived, or the backend it fetches from is
    /// unreachable. Normal for the first seconds of a process; a fault if it
    /// persists.
    Unready,
    /// A provider is installed and has configuration to evaluate against. Says
    /// nothing about whether any particular flag has an assignment.
    Ready,
}

impl ProviderState {
    /// The metric tag value and log field for this state.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Unready => "unready",
            Self::Ready => "ready",
        }
    }
}

impl std::fmt::Display for ProviderState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why a typed read produced no value.
///
/// Four situations used to arrive at a call site as one `None`, and at Datadog
/// as the absence of a series, so a deployment that installs no flag backend on
/// purpose could not be told from one whose backend had broken. They are
/// separate variants so a metric, a log line, and a caller that asks can tell
/// them apart. They deliberately do *not* make a gate answer differently — see
/// [`crate::policy`] for why the reason stops at the telemetry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unresolved {
    /// No provider was installed, so nothing was asked. Produced by
    /// [`crate::FeatureFlagClient`] itself; a provider may also return it to
    /// report that it has no backend of its own.
    NoProvider,
    /// The provider was asked and has no configuration to answer from yet.
    ProviderUnready,
    /// The provider answered from configuration it has: this flag has no
    /// assignment for this subject, or none of the requested type.
    NoAssignment,
    /// The provider tried to evaluate and failed.
    EvaluationFailed,
}

impl Unresolved {
    /// The metric tag value and log field for this outcome.
    ///
    /// [`Self::NoAssignment`] and [`Self::EvaluationFailed`] render as
    /// `unassigned` and `error` rather than as their variant names: those are
    /// the tag values the Eppo provider already emitted, so no existing tag
    /// value disappears. The two new reasons get new values.
    ///
    /// The tag values carry over; one population does not. Before this split,
    /// *every* unassigned read counted `unassigned`, including the reads a
    /// process takes before its first configuration is **observed**. Those now
    /// count `provider_unready` instead, so `outcome:unassigned` loses the
    /// post-restart burst it used to include — visible at every deploy to
    /// anything computing a ratio or an alert threshold from that series alone.
    /// Nothing in this repository queries it.
    ///
    /// Note "observed", not "arrives": observation can lag arrival by up to a
    /// readiness-probe interval, so a genuinely unassigned read can carry
    /// `provider_unready` briefly after a configuration has landed.
    /// `provider_unready` is therefore not a startup-only population, and a
    /// monitor that pages on any steady-state occurrence of it will fire on a
    /// healthy process. See `crate::eppo`'s probe interval.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NoProvider => "no_provider",
            Self::ProviderUnready => "provider_unready",
            Self::NoAssignment => "unassigned",
            Self::EvaluationFailed => "error",
        }
    }
}

impl std::fmt::Display for Unresolved {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The provider to install for a backend that was configured but could not be
/// constructed.
///
/// Leaving the slot empty instead would report [`ProviderState::Absent`], which
/// is the *deliberate* state — a deployment that configured no backend. An
/// operator who set the credentials and got nothing is in a different
/// situation and wants the flag's declared failure behavior, not their
/// environment's answer. This type is what keeps those apart when the failure
/// happens before there is anything to ask.
///
/// It reports [`ProviderState::Unready`] rather than a state of its own: from
/// outside, "configured, cannot answer yet" is the same situation whether the
/// cause is a construction that failed or a first fetch that has not landed,
/// and the three-state model is deliberately that coarse.
///
/// Motivated by the backends rather than by Eppo, where this is rare:
/// `eppo::Client::start_poller_thread` fails only if the background thread
/// cannot be spawned, and a bad key or base URL instead surfaces inside the
/// poller loop (`eppo_core-10.0.0/src/configuration_poller.rs:156`) as a
/// provider that simply never becomes ready. OpenFeature's flagd is the other
/// shape: `InProcessResolver::new` returns a `Result` and an unreachable
/// daemon fails right there.
#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableProvider;

impl FeatureFlagProvider for UnavailableProvider {
    fn state(&self) -> ProviderState {
        ProviderState::Unready
    }

    fn bool_value(&self, _: &str, _: &str, _: &Attributes) -> Result<bool, Unresolved> {
        Err(Unresolved::ProviderUnready)
    }

    fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
        Err(Unresolved::ProviderUnready)
    }

    fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
        Err(Unresolved::ProviderUnready)
    }

    fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
        Err(Unresolved::ProviderUnready)
    }

    fn json_value(
        &self,
        _: &str,
        _: &str,
        _: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        Err(Unresolved::ProviderUnready)
    }
}

/// One evaluation operation per flag type, plus the provider's own readiness.
///
/// Each read method is typed, so a flag's type is settled by which method the
/// caller reached for and a provider cannot answer with some other shape. A
/// flag read as a type its backend disagrees with resolves nothing rather than
/// a coerced value: a JSON assignment that happens to be a JSON boolean is not
/// a boolean flag. Which [`Unresolved`] a mismatch is belongs to the backend,
/// because only it knows whether its store treats one as an absence or as an
/// error — Eppo answers [`Unresolved::EvaluationFailed`], since its SDK raises;
/// a backend holding one map per type answers
/// [`Unresolved::NoAssignment`].
///
/// Evaluate from local state without blocking on network I/O. Return the
/// [`Unresolved`] variant that says *why* nothing resolved;
/// [`crate::FeatureFlagClient`] hands that reason to its caller and counts it
/// as the `outcome` tag. Preserve the subject key and the
/// attributes when forwarding to a targeting SDK. Providers own their
/// background work and stop it when dropped; client clones share one provider.
///
/// A provider still owns its *backend-specific* diagnostics — the Eppo provider
/// warns once per flag with the SDK's own evaluation code. What it no longer
/// owns is the shared tally: the client counts every read, so a provider-free
/// deployment is counted too.
///
/// **Every method is required.** A backend that cannot serve a flag type writes
/// `Err(Unresolved::NoAssignment)` for it explicitly, which costs one line and
/// makes the gap greppable. Defaulted bodies were the alternative and are
/// deliberately not used: a supported type whose method was simply forgotten
/// would then compile and leave every read of it at its declared fallback
/// forever — a kill-switch that never engages, with nothing to notice it.
/// Requiring the methods also means adding a sixth flag type is a compile error
/// at every provider rather than a silent fallback. See
/// [`crate::FeatureFlagClient::provider_state`] for what "a provider is
/// installed" does and does not promise.
pub trait FeatureFlagProvider: Send + Sync {
    /// Whether this provider can serve assignments right now, answered without
    /// blocking and without a flag key in hand.
    ///
    /// A backend with no readiness concept — one that evaluates a local rule
    /// set, or a fixed test double — returns [`ProviderState::Ready`]. Return
    /// [`ProviderState::Absent`] only to report that this provider has no
    /// backend at all, which the client treats exactly as an empty provider
    /// slot.
    ///
    /// Keep this consistent with the reads: a provider that answers
    /// [`Unresolved::ProviderUnready`] should report [`ProviderState::Unready`]
    /// here, so an operator reading the state and an operator reading the
    /// evaluation counter reach the same conclusion.
    fn state(&self) -> ProviderState;

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved>;

    fn integer_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<i64, Unresolved>;

    /// Kept distinct from [`Self::integer_value`] rather than folded into one
    /// number type: OpenFeature's provider SPI resolves int and float
    /// separately, and a backend that converts a double through an integer read
    /// loses it — flagd's `resolve_int_value` goes through `as_i64()`, so a
    /// `0.25` variant would resolve to nothing with no way for a flag author to
    /// see why.
    fn float_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<f64, Unresolved>;

    fn string_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<String, Unresolved>;

    fn json_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<serde_json::Value, Unresolved>;
}
