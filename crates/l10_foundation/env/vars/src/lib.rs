//! The [`Environment`] source and shared parsing for flag, destructive-gate
//! and positive-`f64` environment variables.
//!
//! SPACE services read on/off knobs from env vars in many places, and each
//! call site historically re-invented the truthy/falsy vocabulary with a
//! local `matches!`. This crate is the single vocabulary:
//!
//! - truthy: `1`, `true`, `yes`, `on`, `enabled`
//! - falsy: `0`, `false`, `no`, `off`, `disabled`
//!
//! Matching is case-insensitive and whitespace-trimmed. Unset, empty, or
//! unrecognized values fall back to the caller's default, so a typo in a
//! deployment env degrades to the shipped behavior instead of silently
//! flipping a feature. Callers pick the default per knob: opt-in features
//! pass `false`, kill switches that must fail toward the fixed behavior
//! pass `true`.
//!
//! It is its own crate so that crates that know nothing about SPACE can parse
//! their settings without depending on `pplx-space-common-core`.

/// Interpret an env var's raw value as an on/off flag, falling back to
/// `default` when the value is absent or not in the shared vocabulary.
pub fn env_flag(raw: Option<&str>, default: bool) -> bool {
    raw.and_then(parse_env_flag).unwrap_or(default)
}

/// The shared vocabulary itself: `None` when `raw` is not in it, for a caller
/// whose fallback for an unrecognized value differs from its unset default.
pub fn parse_env_flag(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" | "enabled" => Some(true),
        "0" | "false" | "no" | "off" | "disabled" => Some(false),
        _ => None,
    }
}

/// [`env_flag`] over `std::env::var(name)`.
pub fn env_flag_var(name: &str, default: bool) -> bool {
    env_flag(std::env::var(name).ok().as_deref(), default)
}

/// Gate for DESTRUCTIVE paths: enabled only when unset or explicitly
/// truthy. Unlike [`env_flag`], falsy and unrecognized values BOTH read
/// as disabled — an emergency kill-switch flip with a typo must stop the
/// destructive path, never silently leave it running. Unset stays
/// enabled so a rollout that has not rendered the knob keeps the shipped
/// default.
pub fn env_destructive_gate(raw: Option<&str>) -> bool {
    match raw {
        None => true,
        Some(raw) => matches!(
            raw.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on" | "enabled"
        ),
    }
}

/// [`env_destructive_gate`] over `std::env::var(name)`.
pub fn env_destructive_gate_var(name: &str) -> bool {
    env_destructive_gate(std::env::var(name).ok().as_deref())
}

/// `Some(value)` when `value` is finite and strictly positive, else `None`.
/// The shared sanity check for rate/burst-style knobs, where a zero, negative,
/// NaN, or infinite value is never a legal configuration.
#[must_use]
pub fn valid_positive_f64(value: f64) -> Option<f64> {
    value.is_finite().then_some(value).filter(|v| *v > 0.0)
}

/// Parse env var `name` as a strictly-positive `f64`, falling back to `default`
/// when it is unset, unparseable, or not [`valid_positive_f64`]. Kept
/// dependency-free (no logging) so this base crate stays minimal; callers that
/// want a misconfiguration warning can compare against `default` at their edge.
#[must_use]
pub fn parse_positive_f64_env(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.trim().parse::<f64>().ok())
        .and_then(valid_positive_f64)
        .unwrap_or(default)
}

#[cfg(test)]
mod tests;

/// Read-only configuration source. Production reads the process environment;
/// callers constructing isolated configurations can supply an owned map.
pub trait Environment {
    fn var_os(&self, name: &str) -> Option<std::ffi::OsString>;

    fn var(&self, name: &str) -> Result<String, std::env::VarError> {
        self.var_os(name)
            .ok_or(std::env::VarError::NotPresent)?
            .into_string()
            .map_err(std::env::VarError::NotUnicode)
    }
}

/// The live process environment; reading never mutates it.
pub struct ProcessEnvironment;

impl Environment for ProcessEnvironment {
    fn var_os(&self, name: &str) -> Option<std::ffi::OsString> {
        std::env::var_os(name)
    }
}
