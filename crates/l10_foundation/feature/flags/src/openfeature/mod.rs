//! OpenFeature adapter: resolve SPACE's typed flag reads through any
//! [OpenFeature](https://openfeature.dev/) provider.
//!
//! This module is vendor-neutral twice over. SPACE's own evaluation surface
//! names no vendor, and OpenFeature's provider SPI names no backend, so
//! installing this adapter buys every backend with an OpenFeature provider
//! rather than one. [`flagd`] is the reference backend and the one an OSS
//! deployment is expected to run; nothing here depends on it.
//!
//! ```no_run
//! # #[cfg(feature = "flagd")] {
//! use pplx_space_feature_flags::{FeatureFlagClient, openfeature};
//!
//! let provider = openfeature::flagd::in_process_from_env()?;
//! let flags = FeatureFlagClient::from_provider(provider);
//! # }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Reads are synchronous and the provider is not
//!
//! [`crate::FeatureFlagProvider`] is a synchronous trait, because its callers
//! are synchronous functions; OpenFeature's SPI is `async` throughout. [`worker`]
//! carries the whole weight of that mismatch and documents why it is shaped the
//! way it is. What matters from outside: a read is a handoff to one dedicated
//! thread and a blocking wait for the reply, bounded by a timeout, and it is
//! safe from a sync function, a current-thread runtime, or a multi-threaded one.
//!
//! Configure the backend for **local evaluation** — flagd's in-process mode,
//! not its RPC mode. The trait asks providers to resolve from local state
//! without blocking on network I/O, and the adapter can only be as local as the
//! provider underneath it.
//!
//! # What this adapter can and cannot carry
//!
//! Four things do not survive the crossing intact, each worth knowing before
//! writing flag definitions. Where each one comes from — and what, if
//! anything, it emits — differs, so each bullet says so rather than being
//! summarised here; a sweeping claim over the four has been wrong every way
//! it has been phrased:
//!
//! * [`crate::AttributeValue::Null`] is dropped, silently. OpenFeature's
//!   context has no null, so a targeting rule sees the attribute as absent
//!   instead. A property of OpenFeature, so it holds for every backend.
//! * A JSON flag must be a JSON **object**, a property of OpenFeature: it
//!   resolves JSON as a structure type, so a flag whose value is a bare
//!   number, a string, or a top-level array cannot be resolved at all. How
//!   visible that is depends on the backend, because only the backend
//!   chooses the error code. flagd reports a type mismatch, which lands in
//!   the **error** rate and emits a warning naming the code; a provider that
//!   reported the same flag as not-found would make it silent instead, since
//!   not-found is the one code this adapter reads as "no assignment".
//! * A number arrives as a float, silently. This one is SPACE's own doing,
//!   not OpenFeature's: [`crate::AttributeValue`] carries a single `f64`,
//!   while OpenFeature's context has a distinct `Int(i64)`. So an integer
//!   past 2^53 loses precision that a backend — flagd's in-process engine
//!   among them — would otherwise have kept, and widening the neutral
//!   vocabulary would fix it.
//! * An attribute named `targetingKey` or `$flagd` is dropped, with a
//!   warning, because a backend would let it overwrite the subject or its own
//!   metadata. `targetingKey` is OpenFeature's; `$flagd` belongs to the
//!   evaluation engine behind flagd's in-process resolvers, so another
//!   backend would reserve different names.
//!
//! [`convert`] states each mechanism.
//!
//! One part of OpenFeature's lifecycle is also absent: **provider hooks are
//! not run.** Reads call the provider's resolvers directly rather than going
//! through the SDK client, so a provider whose `before` hook rewrites the
//! context — or whose `after`/`error`/`finally` hooks do required work —
//! behaves differently here than it would through the SDK. flagd supplies no
//! hooks, so the reference backend is unaffected; a provider that does supply
//! them logs an error at startup rather than resolving quietly wrong.
//!
//! # A read blocks the thread that makes it
//!
//! The handoff is a blocking wait, bounded by a deadline, on whatever thread
//! called it — and SPACE's flag reads happen inside `async fn`s, so in a
//! server that thread is a runtime worker. A healthy local-evaluation backend
//! answers in microseconds and the cost is invisible. A wedged one does not:
//! every concurrent read parks a worker until its deadline, and enough of
//! them park the whole runtime rather than just the flag reads.
//!
//! The deadline bounds how long any one read waits; nothing here bounds that
//! starvation, because nothing here can — the trait is synchronous and its
//! callers are synchronous functions. It is the standing cost of running an
//! asynchronous SDK under a synchronous contract, and the reason to configure
//! the backend for local evaluation rather than for a network round trip.

mod convert;
mod worker;

#[cfg(feature = "flagd")]
pub mod flagd;

use std::future::Future;

use open_feature::EvaluationErrorCode;
use open_feature::provider::{FeatureProvider, ProviderStatus};

use self::worker::{FlagValue, Outcome, Worker};
use crate::{Attributes, FeatureFlagProvider, ProviderState, Unresolved};

pub use self::worker::OpenFeatureProviderError;

/// OpenFeature adapter: owns the worker thread that drives the OpenFeature
/// provider and the targeting conversion between SPACE's vocabulary and
/// OpenFeature's. Install it with
/// [`crate::FeatureFlagClient::from_provider`].
pub struct OpenFeatureProvider {
    worker: Worker,
}

impl std::fmt::Debug for OpenFeatureProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OpenFeatureProvider")
            .finish_non_exhaustive()
    }
}

impl OpenFeatureProvider {
    /// Install an already-constructed OpenFeature provider.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker thread or its runtime cannot be
    /// started. The provider itself is already built, so
    /// [`OpenFeatureProviderError::BuildProvider`] cannot occur here.
    pub fn new<P: FeatureProvider>(provider: P) -> Result<Self, OpenFeatureProviderError> {
        Self::from_builder(move || std::future::ready(Ok(provider)))
    }

    /// Construct the provider on the adapter's own runtime.
    ///
    /// OpenFeature SDK constructors are async — flagd's connects to its sync
    /// source and waits for an initial flag set — and a caller reaching for a
    /// flag client has no runtime to build one on. The builder runs on the
    /// worker thread, and this call blocks until it succeeds or fails.
    ///
    /// # Errors
    ///
    /// Returns an error when the worker cannot be started or the builder fails.
    /// A backend that is unreachable at startup fails here rather than
    /// resolving nothing on every later read, so a caller can tell the two
    /// apart.
    ///
    /// The wait covers the builder **and** the provider's `initialize`, and is
    /// bounded by neither: a provider that hangs in either one hangs startup.
    /// That is deliberate — the alternative is a deadline racing the
    /// provider's own, killing a backend that was merely slow.
    ///
    /// flagd bounds itself, but generously: **about 40 seconds** against a
    /// flag source that is not answering. Its in-process gRPC connector uses a
    /// hard-coded 5s connect timeout — `FLAGD_DEADLINE_MS` does not bound this
    /// path — and retries `FLAGD_RETRY_GRACE_PERIOD` (default 5) times with a
    /// doubling backoff from `FLAGD_RETRY_BACKOFF_MS` (default 1s), so five
    /// attempts plus 1+2+4+8s of waiting. Only after connecting does the 5s
    /// deadline on the initial flag set apply. Its `initialize` is the SDK's
    /// no-op default.
    ///
    /// Size a startup probe against the 40s, not against the sync deadline: a
    /// probe that fires earlier kills the pod mid-retry and turns a slow flag
    /// source into a crash loop. Lower `FLAGD_RETRY_GRACE_PERIOD` to shorten
    /// it deliberately.
    pub fn from_builder<F, Fut, P>(build: F) -> Result<Self, OpenFeatureProviderError>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<P, Box<dyn std::error::Error + Send + Sync>>>,
        P: FeatureProvider,
    {
        Ok(Self {
            worker: Worker::start(build)?,
        })
    }

    /// The shared tail of all five typed reads: say what went wrong when
    /// something did, and hand back the assignment or the reason there wasn't
    /// one.
    ///
    /// The `feature_flags.evaluation` tally is not emitted here — the client
    /// owns it, so a deployment with no provider at all is counted on the same
    /// metric. What stays here is the part only this backend can produce: the
    /// OpenFeature error code behind a failure.
    ///
    /// The read's type picks the kind and the narrowing together through
    /// [`FlagValue`], so the two cannot disagree. The wrong-type arm below is
    /// therefore unreachable by construction; it still reports
    /// [`Unresolved::EvaluationFailed`] rather than
    /// [`Unresolved::NoAssignment`], because a silent wrong answer is the one
    /// outcome that must never look like an absent flag.
    fn resolve<T: FlagValue>(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<T, Unresolved> {
        match self
            .worker
            .resolve(T::KIND, flag_key, subject_key, attributes)
        {
            Ok(Outcome::Assigned(resolved)) => T::from_resolved(resolved).ok_or_else(|| {
                tracing::error!(
                    flag_key,
                    kind = ?T::KIND,
                    "OpenFeature adapter resolved a value of the wrong type; resolving nothing"
                );
                Unresolved::EvaluationFailed
            }),
            Ok(Outcome::Unassigned) => Err(Unresolved::NoAssignment),
            Ok(Outcome::Failed(error)) => {
                tracing::warn!(
                    flag_key,
                    code = %error.code,
                    message = error.message.as_deref().unwrap_or_default(),
                    // "resolving nothing" rather than "using default": four of
                    // the five reads let the flag's declaration supply a
                    // fallback, but `json_value` has none — its caller owns the
                    // fallback. One message covers all five, so it states what
                    // this function returns rather than what any one caller
                    // does with it.
                    "feature flag evaluation failed; resolving nothing"
                );
                // The one error code that is about the backend rather than
                // about this flag: OpenFeature defines `ProviderNotReady` as
                // a value resolved before the provider was initialized.
                // Keeping it out of `EvaluationFailed` is what stops the
                // first seconds of a process from landing in the error rate,
                // where an operator is watching for a backend that broke.
                Err(if error.code == EvaluationErrorCode::ProviderNotReady {
                    Unresolved::ProviderUnready
                } else {
                    Unresolved::EvaluationFailed
                })
            }
            Err(unavailable) => {
                tracing::warn!(
                    flag_key,
                    reason = unavailable.as_str(),
                    "the OpenFeature worker did not answer; resolving nothing"
                );
                Err(Unresolved::EvaluationFailed)
            }
        }
    }
}

impl FeatureFlagProvider for OpenFeatureProvider {
    /// The underlying provider's own `status`, narrowed to the three states
    /// SPACE's contract has.
    ///
    /// Never [`ProviderState::Absent`]: this adapter cannot exist without a
    /// provider to wrap, so "no backend at all" is not a state it can be in.
    ///
    /// OpenFeature makes `status` optional and defaults it to `Ready`, which
    /// is what flagd leaves it at — an unreachable sync source fails
    /// construction there instead, so the reference backend only ever reports
    /// `Ready` here. The narrowing below is for providers that do implement
    /// the lifecycle.
    fn state(&self) -> ProviderState {
        match self.worker.status() {
            // OpenFeature defines `STALE` as a cached state that may no
            // longer match the source of truth. There is still a
            // configuration to evaluate against, which is all `Ready`
            // claims; its assignments may just be behind.
            ProviderStatus::Ready | ProviderStatus::STALE => ProviderState::Ready,
            // `Error` is "initialized and cannot reliably resolve", which from
            // outside is the same situation as "not initialized yet": a
            // provider is installed and cannot answer. The three-state model
            // is deliberately that coarse.
            ProviderStatus::NotReady | ProviderStatus::Error => ProviderState::Unready,
        }
    }

    fn bool_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<bool, Unresolved> {
        self.resolve(flag_key, subject_key, attributes)
    }

    fn integer_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<i64, Unresolved> {
        self.resolve(flag_key, subject_key, attributes)
    }

    fn float_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<f64, Unresolved> {
        self.resolve(flag_key, subject_key, attributes)
    }

    fn string_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<String, Unresolved> {
        self.resolve(flag_key, subject_key, attributes)
    }

    fn json_value(
        &self,
        flag_key: &str,
        subject_key: &str,
        attributes: &Attributes,
    ) -> Result<serde_json::Value, Unresolved> {
        self.resolve(flag_key, subject_key, attributes)
    }
}

#[cfg(test)]
mod tests;
