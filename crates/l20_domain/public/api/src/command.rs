use pplx_space_common_core::types::SimpleDuration;

use super::PublicApiParseError;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, de};

pub(crate) const MAX_COMMAND_TIMEOUT_SOURCE_BYTES: usize = 24;
#[cfg(feature = "serde")]
const MAX_COMMAND_TIMEOUT_WHOLE_DIGITS: usize = 9;

#[cfg(feature = "serde")]
fn parse_timeout(value: &str) -> Result<SimpleDuration, PublicApiParseError> {
    if value.len() > MAX_COMMAND_TIMEOUT_SOURCE_BYTES || value.trim() != value {
        return Err(PublicApiParseError("invalid command timeout"));
    }

    let unit = value
        .chars()
        .last()
        .ok_or(PublicApiParseError("invalid command timeout"))?;
    let number = value
        .strip_suffix(unit)
        .ok_or(PublicApiParseError("invalid command timeout"))?;
    let (whole, fractional) = number
        .split_once('.')
        .map_or((number, None), |(whole, fractional)| {
            (whole, Some(fractional))
        });
    if whole.trim_start_matches('0').len() > MAX_COMMAND_TIMEOUT_WHOLE_DIGITS {
        return Err(PublicApiParseError("invalid command timeout"));
    }

    let max_fractional_digits = match unit {
        's' => 9,
        'm' => 10,
        'h' | 'd' => 11,
        _ => return Err(PublicApiParseError("invalid command timeout")),
    };
    if fractional
        .is_some_and(|fractional| fractional.trim_end_matches('0').len() > max_fractional_digits)
    {
        return Err(PublicApiParseError("invalid command timeout"));
    }

    value
        .parse()
        .map_err(|_| PublicApiParseError("invalid command timeout"))
}

#[cfg(feature = "serde")]
pub(crate) fn deserialize_timeout<'de, D>(
    deserializer: D,
) -> Result<Option<SimpleDuration>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)?
        .map(|value| parse_timeout(&value).map_err(de::Error::custom))
        .transpose()
}
