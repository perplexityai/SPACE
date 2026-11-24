//! Column conversions shared by the store's row decoders and binds.

use rusqlite::{Result, types::Type};

use pplx_space_net_common::NetworkPolicyV1;
use pplx_space_policy_authority::PolicyAuthority;
use pplx_space_template_model::TemplateCachePartition;

pub(crate) fn nonnegative_i64_to_u64(value: i64, column: usize) -> Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Integer, Box::new(error))
    })
}

pub(crate) fn saturating_u64_to_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

pub(crate) fn network_policy_json(policy: &NetworkPolicyV1) -> Result<String> {
    serde_json::to_string(policy)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

pub(crate) fn default_network_policy_json() -> Result<String> {
    network_policy_json(&NetworkPolicyV1::deny_all())
}

pub(crate) fn parse_network_policy_json(
    value: Option<String>,
    column: usize,
) -> Result<NetworkPolicyV1> {
    match value {
        Some(json) => match serde_json::from_str(&json) {
            Ok(policy) => Ok(policy),
            Err(error) => Err(rusqlite::Error::FromSqlConversionFailure(
                column,
                Type::Text,
                Box::new(error),
            )),
        },
        None => Ok(NetworkPolicyV1::deny_all()),
    }
}

pub(crate) fn parse_policy_authority(value: &str, column: usize) -> Result<PolicyAuthority> {
    value.parse().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}

pub(crate) fn parse_cache_partition(value: &str, column: usize) -> Result<TemplateCachePartition> {
    TemplateCachePartition::from_key(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(column, Type::Text, Box::new(error))
    })
}
