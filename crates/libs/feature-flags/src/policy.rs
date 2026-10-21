//! The env-master-first evaluation policy, in one place.
//!
//! Several SPACE features are gated twice: by a static environment variable set
//! at deploy time (the **env master**) and by a dynamic flag that an operator
//! can flip without a redeploy. The intended relationship was written per flag
//! in a doc comment and re-implemented at each call site — as two identical
//! `const fn limiter_active(env, flag) -> env && flag` copies in
//! `crates/libs/api`, and elsewhere as nothing at all but the order in which
//! two checks happen to run. Nothing enforced any of it, so a call site could
//! drop the env master, get the precedence backwards, or pass a fallback that
//! disagrees with the one its sibling passes for the same flag, and no test or
//! lint would notice.
//!
//! # The policy
//!
//! * **Env off → the feature is off.** No provider evaluation can turn it on.
//!   This is what makes the env var a genuine kill switch: an operator who
//!   redeploys with it off does not have to also reason about the flag backend.
//! * **Env on, an assignment resolved → the assignment, which can only
//!   restrict.** It turns the feature off, or supplies typed overrides.
//!   Whatever it says, it cannot widen enablement past what env allowed,
//!   because env-off already returned.
//! * **Env on, nothing resolved → the flag's declared fallback.** Every reason
//!   lands here: no provider installed, a provider with no configuration yet,
//!   no assignment for the subject, and a failed evaluation.
//!
//! # Why the reason does not change the answer
//!
//! [`Unresolved`] distinguishes four reasons and this module deliberately
//! treats them alike. The tempting exception is to give "no provider installed"
//! its own answer — `true`, on the argument that a deployment configuring no
//! backend on purpose should get what its environment asked for. Don't. For
//! every flag routed through here today, all of which declare `true`, it is
//! indistinguishable from the declared fallback; the one case where it *does*
//! differ is a rollout gate declaring `false`, and there it is wrong. No
//! provider means no assignment, so it means no rollout.
//!
//! A fallback chosen for "the backend cannot answer" is the same answer a
//! caller wants for "there is no backend", and overriding it would make a
//! `false`-declaring flag unsafe to layer under an env master at all.
//!
//! The reason stays first-class where it has a consumer. The client counts it
//! as the `outcome` tag on `feature_flags.evaluation`, so a deployment with no
//! backend is still distinguishable in Datadog from one whose backend broke,
//! and [`crate::FeatureFlagClient::provider_state`] answers that question
//! directly for a caller that needs it. What the reason does not do is silently
//! change a gate's answer.
//!
//! # Applying it
//!
//! [`FeatureFlagClient::gate_enabled`](crate::FeatureFlagClient::gate_enabled)
//! and [`FeatureFlagClient::json_override`](crate::FeatureFlagClient::json_override)
//! are the call-site shapes: each reads and applies in one call, so the env
//! master is an argument a caller cannot forget to pass. The free functions here
//! are the same policy without a client, for a caller that already holds a read
//! and for tests that want the policy without a backend.
//!
//! A gate that a deployment can also open without a provider, as
//! `space-native-hidden-operations` is opened by an env list of orgs, still
//! passes its master here and ORs the list outside the gate. The master then
//! closes everything the flag opened and nothing the list did, so the list is
//! not an exception to "env off is a kill switch": it is a second, static
//! answer the master was never asked about.
//!
//! A gate answers `bool`, so its two "nothing resolved" reasons collapse into
//! the flag's declared fallback. An override has no declaration to fall back to
//! and its value *may itself be* the feature, so [`Override`] keeps "the master
//! is off" and "the master is on and nothing resolved" apart and lets the caller
//! decide whether they differ.

use crate::{BoolFlag, Unresolved};

/// Apply the env-master-first policy to a boolean gate.
///
/// See the [module docs](self) for the policy this implements, including why
/// every [`Unresolved`] reason takes the same branch. `read` is the provider's
/// answer for `flag`, as [`crate::FeatureFlagClient::resolve_bool`] returns it.
#[must_use]
pub fn gate_enabled(env_enabled: bool, flag: BoolFlag, read: Result<bool, Unresolved>) -> bool {
    if !env_enabled {
        // The hard off. Deliberately before the read is consulted: a provider
        // that resolved `true` must not be able to reach the line below.
        return false;
    }
    // Nothing resolved, for whatever reason: the flag's own declaration says
    // what that means for this feature, including "there was no provider".
    read.unwrap_or_else(|_| flag.when_unresolved())
}

/// What an env-mastered *override* resolved to — a typed configuration
/// document rather than an on/off gate.
///
/// Three states, because the two that mean "the provider supplied nothing" are
/// not the same answer. An earlier revision returned `Option<T>` and collapsed
/// them, on the reasoning that with nothing resolved whatever local
/// configuration the caller already has *is* the answer. That holds only for a
/// caller whose own gate already returned on a disabled master, which the first
/// two callers both had. It is false for a caller whose override *is* the
/// feature — there, a disabled master means the feature does not run, and local
/// configuration is exactly what must not be used.
///
/// The collapse is still available, as [`Self::resolved`], for the callers it
/// is correct for. What is no longer available is reaching it by accident:
/// `override_allowed(env, read).unwrap_or(local)` used to compile, pass clippy,
/// and keep a feature running on local configuration after an operator flipped
/// the master off. Dropping the env master silently is the failure this module
/// exists to prevent, so it should not be the shortest thing to write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Override<T> {
    /// The env master is off. The feature does not run; local configuration is
    /// not a fallback, because there is nothing to fall back *to*.
    Disabled,
    /// The master is on and the provider answered. Validation is still the
    /// caller's — the policy does not inspect the document.
    Resolved(T),
    /// The master is on and nothing resolved, for any reason: no provider
    /// installed, a provider with no configuration yet, no assignment for the
    /// subject, or a failed evaluation. Whatever the caller configured locally
    /// stands.
    Local,
}

impl<T> Override<T> {
    /// Collapse [`Self::Disabled`] and [`Self::Local`] into `None`.
    ///
    /// Correct only where the caller's own gate has already returned on a
    /// disabled master, so the two cannot be distinguished downstream anyway.
    /// Callers should say which gate that is at the call site — if none exists,
    /// match the three variants instead.
    #[must_use]
    pub fn resolved(self) -> Option<T> {
        match self {
            Self::Resolved(value) => Some(value),
            Self::Disabled | Self::Local => None,
        }
    }
}

/// Apply the env-master-first policy to a provider-supplied override.
///
/// See [`Override`] for why the result carries three states rather than two.
/// The caller keeps ownership of validation and of its own per-field fallback,
/// so an override never collapses to a boolean to fit a gate's shape.
#[must_use]
pub fn override_allowed<T>(env_enabled: bool, read: Result<T, Unresolved>) -> Override<T> {
    if !env_enabled {
        // The hard off, before the read is consulted — same ordering as
        // `gate_enabled`, and for the same reason.
        return Override::Disabled;
    }
    match read {
        Ok(value) => Override::Resolved(value),
        Err(_) => Override::Local,
    }
}

#[cfg(test)]
mod tests;
