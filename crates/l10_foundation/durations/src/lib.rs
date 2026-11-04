//! Generic duration value types and parsers.
//!
//! [`PositiveDuration`] is a `Duration` known to be greater than zero, and
//! [`SimpleDuration`] is a positive duration read from and written as the
//! compact `s/m/h/d` syntax (`30s`, `1.5m`, `0.5d`). The crate also holds
//! that syntax's parse and format functions and the parser for gRPC's
//! `grpc-timeout` metadata value.
//!
//! None of it depends on the SPACE domain model, so it is its own crate rather
//! than part of `pplx-space-common-core`: a crate that needs only these values
//! can depend on it alone. common-core re-exports every item at its original
//! paths.

use std::{fmt, str::FromStr, time::Duration};

/// Error returned when a duration role wrapper is built from zero.
///
/// # Examples
///
/// ```rust
/// # use std::time::Duration;
/// # use pplx_space_durations::*;
/// assert!(SimpleDuration::new(Duration::ZERO).is_err());
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("duration must be greater than zero")]
pub struct DurationParseError;

/// Error returned when parsing a compact human duration like `1.5m`.
///
/// Supported units are `s`, `m`, `h`, and `d`.
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum SimpleDurationParseError {
    #[error("duration cannot be empty")]
    Empty,
    #[error("invalid duration '{0}'")]
    Invalid(String),
    #[error("unsupported duration unit in '{0}', expected one of s/m/h/d")]
    UnsupportedUnit(String),
    #[error("duration must be greater than zero")]
    Zero,
    #[error("duration '{0}' is too precise; max precision is 1ns")]
    TooPrecise(String),
    #[error("duration '{0}' is too large")]
    TooLarge(String),
    #[error("duration '{0}' must resolve to a whole number of seconds")]
    WholeSeconds(String),
}

/// Formats a duration using SPACE's compact `s/m/h/d` syntax.
///
/// Whole-day, whole-hour, and whole-minute durations prefer their larger unit.
/// Everything else is rendered as seconds, with up to 9 fractional digits.
pub fn format_simple_duration(value: Duration) -> String {
    let seconds = value.as_secs();
    if value.subsec_nanos() == 0 && seconds > 0 && seconds.is_multiple_of(86_400) {
        format!("{}d", seconds / 86_400)
    } else if value.subsec_nanos() == 0 && seconds > 0 && seconds.is_multiple_of(3_600) {
        format!("{}h", seconds / 3_600)
    } else if value.subsec_nanos() == 0 && seconds > 0 && seconds.is_multiple_of(60) {
        format!("{}m", seconds / 60)
    } else if value.subsec_nanos() == 0 {
        format!("{seconds}s")
    } else {
        let mut fractional = format!("{:09}", value.subsec_nanos());
        while fractional.ends_with('0') {
            fractional.pop();
        }
        format!("{seconds}.{fractional}s")
    }
}

/// Parses a compact human duration like `30s`, `1.5m`, or `0.5d`.
///
/// The accepted shape is one decimal number followed by exactly one unit:
/// `s`, `m`, `h`, or `d`.
///
/// # Examples
///
/// ```rust
/// # use std::time::Duration;
/// # use pplx_space_durations::parse_simple_duration;
/// assert_eq!(parse_simple_duration("1.5m")?, Duration::from_secs(90));
/// assert_eq!(parse_simple_duration("2h")?, Duration::from_secs(7_200));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn parse_simple_duration(value: &str) -> Result<Duration, SimpleDurationParseError> {
    const NANOS_PER_SEC: u128 = 1_000_000_000;

    let value = value.trim();
    if value.is_empty() {
        return Err(SimpleDurationParseError::Empty);
    }

    let unit = value
        .chars()
        .last()
        .ok_or(SimpleDurationParseError::Empty)?;
    let unit_nanos = match unit {
        's' => NANOS_PER_SEC,
        'm' => 60 * NANOS_PER_SEC,
        'h' => 60 * 60 * NANOS_PER_SEC,
        'd' => 24 * 60 * 60 * NANOS_PER_SEC,
        _ => return Err(SimpleDurationParseError::UnsupportedUnit(value.to_string())),
    };

    let number = value
        .strip_suffix(unit)
        .ok_or_else(|| SimpleDurationParseError::Invalid(value.to_string()))?
        .trim();
    if number.is_empty() {
        return Err(SimpleDurationParseError::Invalid(value.to_string()));
    }

    let (whole, fractional) = match number.split_once('.') {
        Some((whole, fractional)) if !fractional.contains('.') => (whole, Some(fractional)),
        Some(_) => return Err(SimpleDurationParseError::Invalid(value.to_string())),
        None => (number, None),
    };
    if whole.is_empty()
        || !whole.chars().all(|ch| ch.is_ascii_digit())
        || fractional.is_some_and(|fractional| {
            fractional.is_empty() || !fractional.chars().all(|ch| ch.is_ascii_digit())
        })
    {
        return Err(SimpleDurationParseError::Invalid(value.to_string()));
    }

    let whole: u128 = whole
        .parse()
        .map_err(|_| SimpleDurationParseError::Invalid(value.to_string()))?;
    let mut total_nanos = whole
        .checked_mul(unit_nanos)
        .ok_or_else(|| SimpleDurationParseError::TooLarge(value.to_string()))?;

    if let Some(fractional) = fractional {
        let fractional_value: u128 = fractional
            .parse()
            .map_err(|_| SimpleDurationParseError::Invalid(value.to_string()))?;
        let scale = 10u128
            .checked_pow(fractional.len() as u32)
            .ok_or_else(|| SimpleDurationParseError::TooLarge(value.to_string()))?;
        let fractional_nanos = fractional_value
            .checked_mul(unit_nanos)
            .ok_or_else(|| SimpleDurationParseError::TooLarge(value.to_string()))?;
        if fractional_nanos % scale != 0 {
            return Err(SimpleDurationParseError::TooPrecise(value.to_string()));
        }
        total_nanos = total_nanos
            .checked_add(fractional_nanos / scale)
            .ok_or_else(|| SimpleDurationParseError::TooLarge(value.to_string()))?;
    }

    if total_nanos == 0 {
        return Err(SimpleDurationParseError::Zero);
    }

    let seconds = total_nanos / NANOS_PER_SEC;
    if seconds > u64::MAX as u128 {
        return Err(SimpleDurationParseError::TooLarge(value.to_string()));
    }

    Ok(Duration::new(
        seconds as u64,
        (total_nanos % NANOS_PER_SEC) as u32,
    ))
}

/// Parses a compact human duration that must resolve to a whole number of seconds.
///
/// # Examples
///
/// ```rust
/// # use pplx_space_durations::parse_simple_duration_secs;
/// assert_eq!(parse_simple_duration_secs("1.5m")?, 90);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn parse_simple_duration_secs(value: &str) -> Result<u64, SimpleDurationParseError> {
    let duration = parse_simple_duration(value)?;
    if duration.subsec_nanos() != 0 {
        return Err(SimpleDurationParseError::WholeSeconds(
            value.trim().to_string(),
        ));
    }
    Ok(duration.as_secs())
}

/// Parses a gRPC `grpc-timeout` metadata value.
///
/// The gRPC wire format is an integer plus one unit suffix: `H`, `M`, `S`,
/// `m`, `u`, or `n`, representing hours, minutes, seconds, milliseconds,
/// microseconds, and nanoseconds.
pub fn parse_grpc_timeout(value: &str) -> Option<Duration> {
    let unit = value.chars().last()?;
    let amount = value.get(..value.len().checked_sub(unit.len_utf8())?)?;
    if amount.is_empty() || !amount.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let amount = amount.parse::<u64>().ok()?;
    match unit {
        'H' => Some(Duration::from_secs(amount.saturating_mul(60 * 60))),
        'M' => Some(Duration::from_secs(amount.saturating_mul(60))),
        'S' => Some(Duration::from_secs(amount)),
        'm' => Some(Duration::from_millis(amount)),
        'u' => Some(Duration::from_micros(amount)),
        'n' => Some(Duration::from_nanos(amount)),
        _ => None,
    }
}

/// Positive duration parsed from SPACE's compact `s/m/h/d` syntax.
///
/// This is the strong CLI and API boundary type for human-entered durations.
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SimpleDuration(PositiveDuration);

impl SimpleDuration {
    pub fn new(value: Duration) -> Result<Self, DurationParseError> {
        PositiveDuration::new(value)
            .map(Self)
            .ok_or(DurationParseError)
    }

    pub fn as_duration(self) -> Duration {
        self.0.get()
    }
}

impl fmt::Display for SimpleDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_simple_duration(self.as_duration()))
    }
}

impl FromStr for SimpleDuration {
    type Err = SimpleDurationParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let duration = parse_simple_duration(value)?;
        Self::new(duration).map_err(|_| SimpleDurationParseError::Zero)
    }
}

impl TryFrom<String> for SimpleDuration {
    type Error = SimpleDurationParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<SimpleDuration> for String {
    fn from(value: SimpleDuration) -> Self {
        value.to_string()
    }
}

impl TryFrom<Duration> for SimpleDuration {
    type Error = DurationParseError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SimpleDuration> for Duration {
    fn from(value: SimpleDuration) -> Self {
        value.as_duration()
    }
}

/// A `Duration` that is statically known to be greater than zero.
///
/// Shape mirrors `commonware_utils::NonZeroDuration` — same struct,
/// same `Option<Self>` smart constructor, same `new_panic` /
/// `get` / `From<Self> for Duration` shape — but kept in-tree to
/// avoid pulling the whole `commonware-utils` dep-tree in for a
/// 25-line wrapper.
///
/// Use this when a knob "doesn't allow zero" — most commonly wrapped
/// as `Option<PositiveDuration>`, where `None` carries the disabled
/// state and `Some(pd)` carries a real positive duration. The
/// type-level non-zero invariant lets downstream code drop redundant
/// `if d.is_zero()` guards: by the time you hold a `PositiveDuration`,
/// you cannot have one whose underlying `Duration` is `ZERO`.
///
/// The smart constructor is [`PositiveDuration::new`]; the inverse is
/// [`PositiveDuration::get`] or the `From<PositiveDuration> for
/// Duration` impl. Arithmetic on `PositiveDuration` is intentionally
/// not provided — perform it on the underlying `Duration` and
/// reconstruct.
///
/// # Examples
///
/// ```rust
/// # use std::time::Duration;
/// # use pplx_space_durations::PositiveDuration;
/// assert!(PositiveDuration::new(Duration::ZERO).is_none());
/// let p = PositiveDuration::new(Duration::from_secs(30)).unwrap();
/// assert_eq!(p.get(), Duration::from_secs(30));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct PositiveDuration(Duration);

impl PositiveDuration {
    /// Constructs a `PositiveDuration` from a `Duration`, returning
    /// `None` for `Duration::ZERO`.
    pub fn new(value: Duration) -> Option<Self> {
        if value.is_zero() {
            None
        } else {
            Some(Self(value))
        }
    }

    /// Constructs a `PositiveDuration`, panicking on `Duration::ZERO`.
    /// Use only for constants known to be positive at the call site —
    /// e.g. `PositiveDuration::new_panic(Duration::from_secs(600))`.
    pub fn new_panic(value: Duration) -> Self {
        Self::new(value).expect("duration must be non-zero")
    }

    /// Returns the wrapped `Duration`.
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl From<PositiveDuration> for Duration {
    fn from(value: PositiveDuration) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests;
