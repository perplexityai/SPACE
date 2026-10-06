use pplx_space_common_core::types::SimpleDuration;

use crate::PublicApiParseError;

#[cfg(feature = "serde")]
pub(crate) fn deserialize_timeout<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<SimpleDuration, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    deserialize_idle_timeout(deserializer)?
        .ok_or_else(|| serde::de::Error::custom("timeout must be a duration string"))
}

pub(crate) fn idle_timeout_secs(timeout: SimpleDuration) -> Result<u64, PublicApiParseError> {
    let duration = timeout.as_duration();
    if duration.subsec_nanos() != 0 || duration.as_secs() > i64::MAX as u64 {
        return Err(PublicApiParseError(
            "idle_timeout must resolve to whole seconds within the supported deadline range",
        ));
    }
    Ok(duration.as_secs())
}

#[cfg(feature = "serde")]
pub(crate) fn deserialize_idle_timeout<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<Option<SimpleDuration>, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            let seconds = pplx_space_common_core::types::parse_simple_duration_secs(&value)
                .map_err(serde::de::Error::custom)?;
            let timeout = SimpleDuration::new(std::time::Duration::from_secs(seconds))
                .map_err(serde::de::Error::custom)?;
            idle_timeout_secs(timeout).map_err(serde::de::Error::custom)?;
            Ok(timeout)
        })
        .transpose()
}

#[cfg(feature = "serde")]
pub(crate) fn deserialize_idle_action<'de, Deserializer>(
    deserializer: Deserializer,
) -> Result<Option<crate::SandboxIdleAction>, Deserializer::Error>
where
    Deserializer: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    Option::<String>::deserialize(deserializer)?
        .map(|value| {
            crate::SandboxIdleAction::parse_known(&value).map_err(serde::de::Error::custom)
        })
        .transpose()
}
