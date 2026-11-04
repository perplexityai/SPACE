use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};

const API_SURFACE_ECW: &str = "ecw";
const API_SURFACE_NATIVE: &str = "native";

/// An API surface a service credential may authenticate against.
///
/// A key is scoped to the surfaces it is issued for. The two surfaces have
/// different request semantics and different callers, so a credential minted
/// for one must not silently work on the other.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiSurface {
    /// The E2B-compatible translation boundary.
    Ecw,
    /// The SPACE API `/v1` surface, serialized as `native` for the internal
    /// credential-scope contract.
    Native,
}

#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
#[error("unsupported api surface '{0}'")]
pub struct ApiSurfaceParseError(String);

impl ApiSurface {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ecw => API_SURFACE_ECW,
            Self::Native => API_SURFACE_NATIVE,
        }
    }
}

impl fmt::Display for ApiSurface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ApiSurface {
    type Err = ApiSurfaceParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            API_SURFACE_ECW => Ok(Self::Ecw),
            API_SURFACE_NATIVE => Ok(Self::Native),
            other => Err(ApiSurfaceParseError(other.to_string())),
        }
    }
}

impl sqlx::Type<sqlx::Postgres> for ApiSurface {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

impl sqlx::postgres::PgHasArrayType for ApiSurface {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <&str as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for ApiSurface {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(self.as_str(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for ApiSurface {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        raw.parse::<Self>().map_err(Into::into)
    }
}
