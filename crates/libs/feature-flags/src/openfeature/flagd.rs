//! flagd: the reference OpenFeature backend for an OSS SPACE deployment.
//!
//! [flagd](https://flagd.dev/) is configuration-file driven and has no console.
//! Flag definitions are JSON, held in a file or served by a flagd daemon, and
//! an operator changes a flag by changing that configuration.
//!
//! # Evaluation is always local
//!
//! flagd's Rust provider offers four resolvers; this crate compiles only the
//! two that evaluate **in process**, against a flag set held locally and kept
//! current in the background:
//!
//! | Resolver | `FLAGD_RESOLVER` | Flag definitions come from |
//! | -- | -- | -- |
//! | In-process | `in-process` (the default) | a flagd daemon's sync stream, over gRPC |
//! | File | `file` or `offline` | a JSON file on disk, re-read when it changes |
//!
//! Setting `FLAGD_OFFLINE_FLAG_SOURCE_PATH` without `FLAGD_RESOLVER` selects
//! File on its own, so a deployment that points at a file gets the file
//! resolver whether or not it also names one.
//!
//! The remote-evaluation resolvers (`rpc`, `rest`) are left out at the Cargo
//! level rather than rejected at runtime, so a deployment cannot configure its
//! way into a flag read that makes a network round trip. `FLAGD_RESOLVER=rpc`
//! on a SPACE build falls back to in-process rather than taking effect.
//!
//! That matters because [`crate::FeatureFlagProvider`] promises reads that
//! resolve from local state without blocking on network I/O, and SPACE reads
//! flags on paths like sandbox creation where a round trip would be felt.
//!
//! # Configuration
//!
//! [`SPACE_FLAGD_ENABLED_ENV`] is what makes
//! [`crate::FeatureFlagClient::from_env`] reach for flagd at all, because
//! flagd's own defaults point at `localhost` and would otherwise make any
//! build with this feature look configured. The gate lives there, not in
//! [`in_process_from_env`] — calling that directly builds a provider
//! regardless, which is what a caller installing it by hand through
//! [`crate::FeatureFlagClient::from_provider`] wants. Everything else
//! is the standard `FLAGD_*` environment surface the flagd SDK reads for
//! itself — `FLAGD_HOST`, `FLAGD_PORT`, `FLAGD_RESOLVER`,
//! `FLAGD_OFFLINE_FLAG_SOURCE_PATH`, `FLAGD_SOURCE_SELECTOR`, and the TLS and
//! retry settings — so flagd's own documentation stays accurate for SPACE.

use open_feature_flagd::{FlagdOptions, FlagdProvider};

use super::{OpenFeatureProvider, OpenFeatureProviderError};

/// Opt in to flagd. Off unless set to a truthy value.
pub const SPACE_FLAGD_ENABLED_ENV: &str = "SPACE_FLAGD_ENABLED";

/// Whether the deployment asked for flagd.
///
/// Truthiness comes from the workspace's shared vocabulary rather than a local
/// comparison, so `1`, `yes`, `on`, and `enabled` work here exactly as they do
/// on every other `SPACE_*` knob. A local `== "true"` would leave flagd off for
/// an operator who wrote the form that works everywhere else, with no log line
/// saying why.
#[must_use]
pub fn enabled_in_env() -> bool {
    enabled_from_environment(&pplx_space_env_vars::ProcessEnvironment)
}

pub(super) fn enabled_from_environment(env: &dyn pplx_space_env_vars::Environment) -> bool {
    pplx_space_env_vars::env_flag(env.var(SPACE_FLAGD_ENABLED_ENV).ok().as_deref(), false)
}

/// Build a flagd-backed provider from the `FLAGD_*` environment.
///
/// # Errors
///
/// Returns an error when the worker cannot start or flagd's own construction
/// fails. Construction is where an unreachable sync source surfaces: the
/// in-process store waits for an initial flag set and gives up if it does not
/// arrive, which is deliberate — a deployment learns at startup that its flag
/// source is wrong, rather than serving defaults indefinitely and looking
/// healthy. Once started, a later disconnect does **not** fail reads: the
/// last-synced flag set stays in memory and the sync stream reconnects.
pub fn in_process_from_env() -> Result<OpenFeatureProvider, OpenFeatureProviderError> {
    in_process(FlagdOptions::default())
}

/// Build a flagd provider with explicitly supplied SDK options.
pub fn in_process(options: FlagdOptions) -> Result<OpenFeatureProvider, OpenFeatureProviderError> {
    OpenFeatureProvider::from_builder(move || async move {
        FlagdProvider::new(options)
            .await
            .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
    })
}
