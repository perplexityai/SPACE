//! Runtime feature-flag evaluation with per-flag declared fallbacks, over a
//! vendor-neutral provider interface.
//!
//! [`FeatureFlagClient`] holds at most one [`FeatureFlagProvider`] and falls
//! back to each flag's own declaration whenever the provider resolves nothing.
//! No type in the evaluation path names a flag vendor: [`Attributes`] carries
//! plain scalars, each flag type has its own typed read, and each backend
//! converts to and from its own SDK inside its own module.
//!
//! Two backends ship with the crate, each behind its own Cargo feature, and
//! neither is required:
//!
//! * `eppo`, behind the default `eppo` feature, uses the Eppo SDK.
//! * `openfeature`, behind `openfeature`, resolves through any
//!   [OpenFeature](https://openfeature.dev/) provider; its `flagd` submodule,
//!   behind `flagd`, adds flagd as the reference backend for an OSS
//!   deployment.
//!
//! They are named without doc links on purpose: each module only exists when
//! its feature is on, and a link would dangle in the builds where it is not.
//!
//! `README.md` covers choosing between them, deploying flagd, and the flag
//! definitions SPACE reads.
//!
//! To use some other backend, implement [`FeatureFlagProvider`] and install it
//! with [`FeatureFlagClient::from_provider`]. Typed reads do not change:
//!
//! ```
//! use pplx_space_feature_flags::{
//!     Attributes, BoolFlag, FeatureFlagClient, FeatureFlagProvider, IntegerFlag, ProviderState,
//!     Unresolved,
//! };
//!
//! const EXAMPLE: BoolFlag = BoolFlag::new("example-feature", false);
//! const MISSING: BoolFlag = BoolFlag::new("missing-feature", true);
//! const BUDGET: IntegerFlag = IntegerFlag::new("example-feature", 7);
//!
//! struct LocalFlags;
//! impl FeatureFlagProvider for LocalFlags {
//!     // A local rule set has no configuration to wait for.
//!     fn state(&self) -> ProviderState { ProviderState::Ready }
//!
//!     fn bool_value(&self, key: &str, _s: &str, _a: &Attributes) -> Result<bool, Unresolved> {
//!         if key == "example-feature" { Ok(true) } else { Err(Unresolved::NoAssignment) }
//!     }
//!
//!     // This backend serves only booleans, and says so for the rest. Every
//!     // method is required, so a type it *meant* to serve can't go missing.
//!     fn integer_value(&self, _: &str, _: &str, _: &Attributes) -> Result<i64, Unresolved> {
//!         Err(Unresolved::NoAssignment)
//!     }
//!     fn float_value(&self, _: &str, _: &str, _: &Attributes) -> Result<f64, Unresolved> {
//!         Err(Unresolved::NoAssignment)
//!     }
//!     fn string_value(&self, _: &str, _: &str, _: &Attributes) -> Result<String, Unresolved> {
//!         Err(Unresolved::NoAssignment)
//!     }
//!     fn json_value(
//!         &self, _: &str, _: &str, _: &Attributes,
//!     ) -> Result<serde_json::Value, Unresolved> {
//!         Err(Unresolved::NoAssignment)
//!     }
//! }
//!
//! let flags = FeatureFlagClient::from_provider(LocalFlags);
//! assert_eq!(flags.provider_state(), ProviderState::Ready);
//! assert!(flags.bool_value(EXAMPLE, "sandbox-1"));
//! // Nothing resolved, so the flag's own declaration stands — not an argument
//! // this call site chose.
//! assert!(flags.bool_value(MISSING, "sandbox-1"));
//! assert_eq!(flags.integer_value(BUDGET, "sandbox-1"), 7);
//! // And the reason is available when it matters.
//! assert_eq!(
//!     flags.resolve_bool(MISSING, "sandbox-1", Attributes::default()),
//!     Err(Unresolved::NoAssignment),
//! );
//! assert_eq!(
//!     FeatureFlagClient::disabled().resolve_bool(EXAMPLE, "sandbox-1", Attributes::default()),
//!     Err(Unresolved::NoProvider),
//! );
//! ```
//!
//! # Two things this crate owns, that used to live at every call site
//!
//! **A flag's fallback is declared beside its key.** [`BoolFlag`] and friends
//! carry the value a read falls back to, so a call site names a declaration
//! instead of choosing a fallback, and the flag list itself says which flag
//! fails on and which fails off. Two call sites reading the same constant
//! therefore agree — [`Flag::new`] is public, so that covers callers who go
//! through the shared constants rather than the key itself. See [`flag`].
//!
//! **A flag layered under a deploy-time environment variable goes through one
//! policy.** Env off is a hard off; env on lets a resolved assignment restrict
//! but never widen; env on with nothing resolved takes the flag's declared
//! fallback, for every reason including no provider at all. See [`policy`] and
//! [`FeatureFlagClient::gate_enabled`]. Per-flag documentation below points at
//! that policy rather than restating it.
//!
//! [`FeatureFlagClient::disabled`] and [`FeatureFlagClient::fixed_bools`] need
//! no credentials and no provider crate. A caller that must distinguish "no
//! provider installed" from "installed but resolving nothing" reads
//! [`FeatureFlagClient::provider_state`] or the [`Unresolved`] reason from a
//! `resolve_*` call.
//!
//! A backend whose *construction* fails installs [`UnavailableProvider`]
//! rather than leaving the slot empty, so "nobody configured a backend" and
//! "somebody configured one and it did not come up" stay apart all the way to
//! the policy.
//!
//! # Building without the Eppo SDK
//!
//! Depend on this crate with `default-features = false` and the client, the
//! provider trait, attributes, the flag declarations, the policy, and the
//! fixed-assignment test doubles all still compile — the `eppo` crate is then
//! absent from everything your build links. CI asserts that on the normal
//! dependency graph, which is every path that reaches a binary; a build- or
//! dev-dependency reaching the SDK is neither guarded nor linked into a
//! consumer's binary. [`FeatureFlagClient::from_env`] keeps its signature and
//! returns a disabled client in that build, because no provider is compiled in
//! for an environment variable to configure.
//!
//! Metrics are *not* behind the feature. An SDK-free deployment is the one with
//! no vendor console to look at, so it is the one that most needs
//! `feature_flags.evaluation{outcome:no_provider}` to show that its flags are
//! inert on purpose rather than broken. That is paid for: `pplx-space-metrics`
//! brings `pplx-space-common-core` and `pplx-space-observability` into the
//! SDK-free graph, so it is no longer `serde_json` plus `tracing` and nothing
//! else. Every one of those is a SPACE crate that any binary linking this one
//! already has, which is why the trade went this way — the SDK is a vendor
//! dependency an operator may not want, and SPACE's own metrics plumbing is
//! not.
//!
//! **Inside the SPACE workspace the SDK-free build takes an extra step,
//! deliberately.** The root `workspace.dependencies` entry leaves `eppo`
//! enabled, and Cargo does not let a member override an inherited dependency's
//! defaults — so a member cannot opt out on its own edge. One that genuinely
//! needs the SDK-free build has to set `default-features = false` on the root
//! entry *and* add an explicit `features = ["eppo"]` to every other member, in
//! the same change. That is more friction than a per-member opt-out, and it is
//! the point: it keeps the safe default, so a new consumer written as
//! `{ workspace = true }` gets a working flag client instead of one that
//! silently resolves every flag to its declared fallback.
//!
//! That mistake would also be hard to see, which is the other half of the
//! argument. This workspace is a virtual manifest with no `default-members`, so
//! any invocation without `-p`/`--workspace` selects *every* member and
//! resolver-2 unifies `eppo` back in from whichever sibling asks for it —
//! including every release build, which selects targets with `--bin`
//! (`docker/Dockerfile.rust-build-common`). Only a `-p`-scoped build of such a
//! consumer would actually come up without a provider, so the failure would
//! surface on a devbox or a one-off `cargo run -p` long before any image.
//!
//! CI compiles, lints, and runs this crate's tests in both configurations; to
//! reproduce locally, `cargo test -p pplx-space-feature-flags` and
//! `cargo test -p pplx-space-feature-flags --no-default-features`.

mod client;
pub mod flag;
pub mod policy;
mod provider;

#[cfg(feature = "eppo")]
pub mod eppo;

#[cfg(feature = "openfeature")]
pub mod openfeature;

pub use client::FeatureFlagClient;
pub use flag::{BoolFlag, Flag, FloatFlag, IntegerFlag, JsonFlag, StringFlag};
pub use provider::{
    AttributeValue, Attributes, FeatureFlagProvider, ProviderState, UnavailableProvider, Unresolved,
};

/// Boolean flag acting as a dynamic cluster-wide kill-switch for the
/// Redis-backed sandbox activation rate limiter. Layered under the static
/// `SPACE_CLUSTER_ACTIVATION_ADMISSION_ENABLED` env master by [`policy`].
/// Evaluated by space-api and space-cluster-controller on a single static
/// cluster subject (cluster-wide on/off, not a per-sandbox percentage rollout);
/// falls back on so an unreachable provider preserves the on-by-default
/// guardrail.
pub const SPACE_CLUSTER_RATE_LIMITER_FLAG: BoolFlag =
    BoolFlag::new("space-cluster-rate-limiter", true);

/// Boolean flag acting as a dynamic runtime kill-switch for the space-api
/// per-client concurrency limiter. Layered under the static
/// `SPACE_API_CLIENT_CONCURRENCY_MODE` (`off` is the hard off) by [`policy`]. Evaluated
/// on a single static cluster subject (cluster-wide on/off, not a per-sandbox
/// percentage rollout); falls back on so an unreachable provider preserves the
/// on-by-default guardrail.
pub const SPACE_API_CLIENT_CONCURRENCY_LIMITER_FLAG: BoolFlag =
    BoolFlag::new("space-api-client-concurrency-limiter", true);

/// JSON-typed flag carrying runtime overrides for the space-api per-client
/// concurrency limiter (caps on concurrent create/reconnect operations per
/// `X-Client-ID`). Shape:
/// `{"unspecified": <usize>, "overrides": {"<client-id>": <usize>}}`
/// — all fields optional; an absent field falls back to the env/Helm-configured
/// cap, and an `overrides` key also allowlists that client id. Evaluated on the
/// constant cluster subject (the quota map is cluster-wide, not a per-subject
/// rollout). Layered under `SPACE_API_CLIENT_CONCURRENCY_MODE` by
/// [`policy::override_allowed`]: the flag can only retune caps, never enable a
/// limiter the env var turned off.
pub const SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG: JsonFlag =
    JsonFlag::new("space-api-client-concurrency-quotas");

/// Exact domain names whose failed DNS or TCP connections should emit an error
/// from net-gateway. Evaluated by sandbox-manager with the sandbox ID as the
/// stable subject and passed when the gateway process starts. Existing gateways
/// pick up changes when cycled. An assignment that does not validate leaves
/// the deployment's local list standing rather than clearing it, which is the
/// fallback this flag's type declares.
///
/// This selects which failures are *logged*. It neither permits nor denies
/// egress, so an operator reading it as a network control is reading it wrong.
///
/// `SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS` is the local list a deployment
/// with no provider uses.
///
/// Two shapes are accepted: the bare array `["egress-proxy.example.test"]`
/// and the object `{"domains": ["egress-proxy.example.test"]}`. The object
/// exists because no OpenFeature backend can express the bare array — it resolves a
/// JSON flag as a structure, and a structure cannot be a top-level array.
///
/// A third encoding exists because flagd 0.2.2's in-process resolvers
/// stringify any nested array: served through flagd, the list arrives as the
/// JSON *text* `["…"]` under `domains`. The reader decodes that, so all three
/// forms deliver the same domains. Pinned by `tests/flagd_end_to_end.rs` here
/// and by the reader's own tests in `pplx-space-sbx-mgr`.
pub const SPACE_NET_GATEWAY_KNOWN_EGRESS_DOMAINS_FLAG: JsonFlag =
    JsonFlag::new("space-net-gateway-known-egress-domains");

/// Selects EndpointSlice-balanced cluster-api clients in data-plane. The
/// deploy-time endpoint-discovery setting is the hard availability gate; this
/// flag falls back off and is evaluated per sandbox, session, or organization,
/// with `cluster_id` available for cluster-specific targeting. This lets
/// operators ramp traffic gradually and return it to the legacy Service client
/// without restarting pods.
///
/// Routed through [`policy`], with the EndpointSlice client's presence as the
/// env master. Its deploy-time gate is an `Option<Client>` rather than a
/// boolean env var — data-plane installs a real provider only when endpoint
/// discovery is on *and* that client connects — so with no EndpointSlice
/// client the call site returns the Service client before reading the flag,
/// and with one it hands the helper a master that is on.
///
/// That answers identically to the `is_some() && bool_value(..)` it replaced,
/// on every deployment: the policy takes this flag's declared `false` for
/// every reason a read resolves nothing. The precedence here was never at
/// risk, and routing it through the helper does not fix a bug. What it buys
/// is uniformity with the other two gates, plus one read at construction
/// when there is no EndpointSlice client, so `feature_flags.evaluation` shows
/// the flag even where it cannot apply — which for this flag is not a
/// hypothetical difference. That read is once per process rather than per
/// routing decision, because this is a request path and the answer cannot
/// change while the process runs.
///
/// The counter does not say *why* nothing resolved, and at least four
/// deployments land on `outcome:no_provider` here: endpoint discovery off,
/// which is the default; discovery on but the EndpointSlice client failed to
/// connect; discovery on with that client up but no SDK key; and any build
/// with no backend feature compiled in, where there is nothing for
/// [`FeatureFlagClient::from_env`] to install. The first two leave the master
/// false and the third leaves it true, so the tag does not even separate the
/// master's states; the fourth is orthogonal and can accompany either. Use the
/// startup log or [`FeatureFlagClient::provider_state`] for that.
pub const SPACE_CLUSTER_API_CLIENT_SIDE_BALANCING_FLAG: BoolFlag =
    BoolFlag::new("space-cluster-api-client-side-balancing", false);

/// Boolean rollout gate for claiming a warm-pool slot when space-api admits a
/// native create, so the create's public ID is the slot's reserved ID.
/// Layered under the static `SPACE_NATIVE_CREATE_POOL_CLAIM_ENABLED` env master
/// (off by default) by [`policy`]. Evaluated on the constant cluster subject
/// (cluster-wide on/off). Falls back off: a rollout gate, so a provider that
/// cannot answer means no rollout. Turning it off stops new admission claims
/// only; creates already queued with a claim still run through the executor's
/// admitted-claim path.
pub const SPACE_NATIVE_CREATE_POOL_CLAIM_FLAG: BoolFlag =
    BoolFlag::new("space-native-create-pool-claim", false);
