use std::time::Duration;

use pplx_space_common_core::types::{PositiveDuration, snapshot_sweeper_grace_seconds};

/// A borrowed configuration source shared by the reconciler parsers.
pub(crate) struct EnvConfig<'a>(pub(crate) &'a dyn pplx_space_common_core::env::Environment);

impl EnvConfig<'_> {
    pub(crate) fn test_raw(&self, var: &str) -> Option<String> {
        pplx_space_common_core::env::test_hook(self.0, var)
    }

    pub(crate) fn test_duration_secs(&self, var: &str, default: Duration) -> Duration {
        match self.test_raw(var).and_then(|raw| raw.parse::<u64>().ok()) {
            Some(secs) if secs > 0 => Duration::from_secs(secs),
            _ => default,
        }
    }

    pub(crate) fn test_bounded_u32(&self, var: &str, default: u32, ceiling: u32) -> u32 {
        match self
            .test_raw(var)
            .and_then(|raw| parse_clamped(&raw, ceiling))
        {
            Some(value) if value > 0 => value,
            _ => default,
        }
    }

    pub(crate) fn truthy_default(&self, var: &str, default: bool) -> bool {
        match self.0.var(var).as_deref() {
            Ok("1" | "true" | "True" | "TRUE") => true,
            Ok("0" | "false" | "False" | "FALSE") => false,
            _ => default,
        }
    }

    /// A switch whose `true` is the non-destructive mode, such as a dry run.
    /// Unset keeps `default`; a value in the shared `env_flag` vocabulary
    /// wins; any other present value, empty included, turns it on with a
    /// warning. A misspelled dry-run switch must keep the dry run, never start
    /// deleting.
    pub(crate) fn safe_mode_flag(&self, var: &str, default: bool) -> bool {
        let Some(raw) = self.0.var_os(var) else {
            return default;
        };
        let raw = raw.to_string_lossy();
        pplx_space_common_core::env::parse_env_flag(&raw).unwrap_or_else(|| {
            tracing::warn!(
                variable = var,
                value = %raw,
                "unrecognized value for a safe-mode switch; keeping it on"
            );
            true
        })
    }

    /// Kill switch for a reconciler that writes or deletes and ships enabled:
    /// unset keeps it on, an explicit truthy value keeps it on, and any other
    /// present value (including `off`, whitespace, or an empty string)
    /// disables it. A misspelled switch must stop the writes, never leave them
    /// running. Same semantics as the shared `env_destructive_gate`.
    pub(crate) fn destructive_gate(&self, var: &str) -> bool {
        pplx_space_common_core::env::env_destructive_gate(self.0.var(var).ok().as_deref())
    }

    /// An optional positive duration from seconds: unset or unparseable keeps
    /// `default`, and `0` turns it off.
    pub(crate) fn optional_positive_duration_secs(
        &self,
        var: &str,
        default: Option<PositiveDuration>,
    ) -> Option<PositiveDuration> {
        match self.0.var(var).ok().and_then(|raw| raw.parse::<u64>().ok()) {
            Some(0) => None,
            Some(value) => PositiveDuration::new(Duration::from_secs(value)),
            None => default,
        }
    }

    pub(crate) fn duration_secs(&self, var: &str, default: Duration) -> Duration {
        match self.0.var(var).ok().and_then(|raw| raw.parse::<u64>().ok()) {
            Some(secs) if secs > 0 => Duration::from_secs(secs),
            _ => default,
        }
    }

    /// Like [`Self::duration_secs`], clamped to `ceiling` the way
    /// [`Self::bounded_u32`] clamps a count.
    pub(crate) fn bounded_duration_secs(
        &self,
        var: &str,
        default: Duration,
        ceiling: Duration,
    ) -> Duration {
        match self
            .0
            .var(var)
            .ok()
            .and_then(|raw| parse_clamped(&raw, ceiling.as_secs()))
        {
            Some(secs) if secs > 0 => Duration::from_secs(secs),
            _ => default,
        }
    }

    /// Like [`Self::bounded_duration_secs`], typed so the result can't be zero.
    pub(crate) fn bounded_positive_duration_secs(
        &self,
        var: &str,
        default: PositiveDuration,
        ceiling: Duration,
    ) -> PositiveDuration {
        PositiveDuration::new(self.bounded_duration_secs(var, default.get(), ceiling))
            .unwrap_or(default)
    }

    /// Like [`Self::bounded_u32`], for a `usize` setting.
    pub(crate) fn bounded_usize(&self, var: &str, default: usize, ceiling: usize) -> usize {
        match self
            .0
            .var(var)
            .ok()
            .and_then(|raw| parse_clamped(&raw, ceiling))
        {
            Some(n) if n > 0 => n,
            _ => default,
        }
    }

    pub(crate) fn positive_u32(&self, var: &str, default: u32) -> u32 {
        match self.0.var(var).ok().and_then(|raw| raw.parse::<u32>().ok()) {
            Some(n) if n > 0 => n,
            _ => default,
        }
    }

    /// Like [`Self::bounded_u32`] but `0` is a valid value.
    ///
    /// Use this for safety ceilings where zero is an operational kill switch.
    pub(crate) fn bounded_u32_allow_zero(&self, var: &str, default: u32, ceiling: u32) -> u32 {
        self.0
            .var(var)
            .ok()
            .and_then(|raw| parse_clamped(&raw, ceiling))
            .unwrap_or(default)
    }

    /// A positive `u32` clamped to `ceiling`. Unset, `0` or a non-number keeps
    /// `default`; any value above `ceiling` clamps to it (see
    /// [`parse_clamped`]).
    pub(crate) fn bounded_u32(&self, var: &str, default: u32, ceiling: u32) -> u32 {
        match self
            .0
            .var(var)
            .ok()
            .and_then(|raw| parse_clamped(&raw, ceiling))
        {
            Some(n) if n > 0 => n,
            _ => default,
        }
    }

    /// Test-hook form of an optional plain `Duration` from seconds. `0` maps
    /// to `None`.
    pub(crate) fn test_optional_duration_secs(
        &self,
        var: &str,
        default: Option<Duration>,
    ) -> Option<Duration> {
        snapshot_sweeper_grace_seconds(self.test_raw(var).as_deref(), default)
    }
}

/// Parse `raw` as an integer clamped to `ceiling`, or `None` if it isn't one.
///
/// A value too large for `T` clamps too. An operator typoing extra digits
/// meant a big number, and clamping is the ceiling's contract: an over-ceiling
/// value that fits already clamps, so one that overflows must clamp
/// identically rather than fail to parse and silently fall back to a default.
/// Matching the overflow error does that for any length, which parsing into a
/// wider type can't.
///
/// The parser reports overflow as soon as a numeric prefix overflows, before
/// it reaches a bad character, so only an all-digit value counts as one. A
/// value like `5000000000 ` stays garbage: several of these readers are safety
/// caps, and clamping garbage to a `u32::MAX` ceiling would lift the cap.
fn parse_clamped<T>(raw: &str, ceiling: T) -> Option<T>
where
    T: std::str::FromStr<Err = std::num::ParseIntError> + Ord,
{
    match raw.parse::<T>() {
        Ok(n) => Some(n.min(ceiling)),
        Err(err)
            if *err.kind() == std::num::IntErrorKind::PosOverflow
                && raw
                    .strip_prefix('+')
                    .unwrap_or(raw)
                    .bytes()
                    .all(|b| b.is_ascii_digit()) =>
        {
            Some(ceiling)
        }
        Err(_) => None,
    }
}

pub(crate) fn truthy_default(var: &str, default: bool) -> bool {
    EnvConfig(&pplx_space_common_core::env::ProcessEnvironment).truthy_default(var, default)
}

pub(crate) fn destructive_gate(var: &str) -> bool {
    EnvConfig(&pplx_space_common_core::env::ProcessEnvironment).destructive_gate(var)
}

pub(crate) fn duration_secs(var: &str, default: Duration) -> Duration {
    EnvConfig(&pplx_space_common_core::env::ProcessEnvironment).duration_secs(var, default)
}

pub(crate) fn positive_u32(var: &str, default: u32) -> u32 {
    EnvConfig(&pplx_space_common_core::env::ProcessEnvironment).positive_u32(var, default)
}

#[cfg(test)]
mod tests;
