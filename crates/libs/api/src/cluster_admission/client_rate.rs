//! Per-client activation rates: what each client's bucket refills at, and the
//! Redis key it lives under.
//!
//! The cluster bucket is one budget for every client, so a client that floods
//! it gets everyone else's creates and restores rejected too. A per-client
//! bucket caps what one client can spend. It's consumed in the same script as
//! the cluster bucket (see `token_bucket.lua`), so an activation takes a token
//! from both or from neither: a token isn't refundable, and taking one from
//! the client bucket for an activation the cluster bucket then refuses would
//! charge the client for work that never ran.
//!
//! Off unless configured. The env pair sets one rate for every client key,
//! `unknown` included, and the `rate` field of
//! [`SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG`] retunes or exempts single
//! keys. Cold creates and restores draw from the same bucket; the class is
//! only a metric tag.

use std::collections::HashMap;

use pplx_space_common_core::env::valid_positive_f64;
use pplx_space_feature_flags::{FeatureFlagClient, SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG};

use super::SANDBOX_ACTIVATION_BUCKET;

pub(super) const CLIENT_REFILL_RPS_ENV: &str =
    "SPACE_CLUSTER_ACTIVATION_ADMISSION_CLIENT_REFILL_RPS";
pub(super) const CLIENT_BURST_ENV: &str = "SPACE_CLUSTER_ACTIVATION_ADMISSION_CLIENT_BURST";

/// One client bucket's refill rate and capacity.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClientActivationRate {
    pub refill_rps: f64,
    pub burst: f64,
}

impl ClientActivationRate {
    /// A usable rate, or `None` when `refill_rps` isn't a positive finite
    /// number. A missing or unusable `burst` is one second of refill, and any
    /// burst is at least one token: a bucket smaller than the one token every
    /// activation requests would reject that client forever.
    pub fn new(refill_rps: f64, burst: Option<f64>) -> Option<Self> {
        let refill_rps = valid_positive_f64(refill_rps)?;
        let burst = burst
            .and_then(valid_positive_f64)
            .unwrap_or(refill_rps)
            .max(1.0);
        Some(Self { refill_rps, burst })
    }

    /// The env-configured rate for every client key, or `None` (no per-client
    /// rate) when the refill variable is unset. A set but unusable value also
    /// means no per-client rate, with a warning: falling back to some built-in
    /// rate would throttle clients nobody configured.
    pub(super) fn from_env() -> Option<Self> {
        let refill = std::env::var(CLIENT_REFILL_RPS_ENV).ok()?;
        let burst = std::env::var(CLIENT_BURST_ENV).ok();
        Self::parse(&refill, burst.as_deref())
    }

    /// [`Self::from_env`] over explicit values, so it's testable without
    /// process env.
    pub(super) fn parse(refill: &str, burst: Option<&str>) -> Option<Self> {
        let burst = burst.and_then(|raw| {
            raw.trim().parse::<f64>().ok().or_else(|| {
                tracing::warn!(
                    var = CLIENT_BURST_ENV,
                    value = raw,
                    "per-client activation burst isn't a number; using one second of refill"
                );
                None
            })
        });
        let rate = refill
            .trim()
            .parse::<f64>()
            .ok()
            .and_then(|refill_rps| Self::new(refill_rps, burst));
        if rate.is_none() {
            tracing::warn!(
                var = CLIENT_REFILL_RPS_ENV,
                value = refill,
                "per-client activation refill rate isn't a positive number; no per-client rate"
            );
        }
        rate
    }
}

/// The Redis key of `client`'s bucket.
///
/// The braces make the whole cluster key this key's hash tag. A key without
/// braces hashes in full, so the cluster key and every client key map to the
/// same slot of a clustered Redis, which is what lets one script touch both.
/// The cluster key keeps its name, so a rolling deploy never splits the
/// cluster budget across an old and a new key.
pub(super) fn client_bucket_key(client: &str) -> String {
    format!("{{{SANDBOX_ACTIVATION_BUCKET}}}:client:{client}")
}

/// The per-client fields of the quotas flag. Every other field belongs to the
/// concurrency limiter and is ignored here.
#[derive(Debug, Default, PartialEq, serde::Deserialize)]
#[serde(default)]
pub(super) struct ClientRateOverrides {
    /// Per-client-key rates. An entry replaces the env rate for that key, and
    /// a `null` entry exempts the key from any per-client rate. Entries stay
    /// undecoded here and are decoded one key at a time in
    /// [`Self::rate_for`], so a malformed entry costs only its own key, never
    /// the other keys' rates and exemptions.
    #[serde(deserialize_with = "crate::client_concurrency::deserialize_flagd_map")]
    rate: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Deserialize)]
struct RateEntry {
    refill_rps: f64,
    #[serde(default)]
    burst: Option<f64>,
}

impl ClientRateOverrides {
    /// The overrides from the quotas flag, or `None` when the env master is
    /// off, nothing resolved, or the assignment doesn't parse. All three fall
    /// back to the env rate.
    pub(super) fn from_flags(env_enabled: bool, flags: &FeatureFlagClient) -> Option<Self> {
        crate::client_concurrency::read_quotas_flag(
            env_enabled,
            flags,
            "per-client activation rate",
        )
    }

    /// The rate for `client`: its flag entry if it has one, otherwise
    /// `env_rate`. An entry that doesn't decode, or whose `refill_rps` isn't a
    /// positive number, is skipped with a warning, so a typo in one client's
    /// entry leaves that client on the env rate instead of exempting it.
    pub(super) fn rate_for(
        overrides: Option<&Self>,
        client: &str,
        env_rate: Option<ClientActivationRate>,
    ) -> Option<ClientActivationRate> {
        let Some(entry) = overrides.and_then(|overrides| overrides.rate.get(client)) else {
            return env_rate;
        };
        if entry.is_null() {
            return None;
        }
        let rate = serde_json::from_value::<RateEntry>(entry.clone())
            .ok()
            .and_then(|entry| ClientActivationRate::new(entry.refill_rps, entry.burst));
        if rate.is_none() {
            tracing::warn!(
                flag = SPACE_API_CLIENT_CONCURRENCY_QUOTAS_FLAG.key(),
                client_key = client,
                %entry,
                "per-client activation rate entry isn't a positive refill rate; using the env rate"
            );
        }
        rate.or(env_rate)
    }
}
