use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

const POOL_CONFIG_OWNER_MANUAL: &str = "manual";
const POOL_CONFIG_OWNER_AUTOSCALER: &str = "autoscaler";

/// Who manages a `kubernetes_pool_configs` row: `manual` rows are operator
/// upserts that the autoscaler must never overwrite; `autoscaler` rows are
/// owned by the policy-driven reconcile in [`crate::WarmPoolStore::autoscale_configs`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolConfigOwner {
    Manual,
    Autoscaler,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported pool config owner '{0}'")]
pub struct PoolConfigOwnerParseError(String);

impl PoolConfigOwner {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Manual => POOL_CONFIG_OWNER_MANUAL,
            Self::Autoscaler => POOL_CONFIG_OWNER_AUTOSCALER,
        }
    }
}

impl fmt::Display for PoolConfigOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PoolConfigOwner {
    type Err = PoolConfigOwnerParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            POOL_CONFIG_OWNER_MANUAL => Ok(Self::Manual),
            POOL_CONFIG_OWNER_AUTOSCALER => Ok(Self::Autoscaler),
            other => Err(PoolConfigOwnerParseError(other.to_string())),
        }
    }
}

impl sqlx::Type<sqlx::Postgres> for PoolConfigOwner {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for PoolConfigOwner {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        raw.parse::<Self>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
