//! `SemVer`, a validated semantic version, and the error returned when a
//! string does not parse as one.
//!
//! It wraps the `semver` crate's `Version`; its serde form and its Postgres
//! (sqlx) value are the version's text. Nothing in it is specific to SPACE's
//! domain model, so it lives apart from `pplx-space-common-core`, which
//! re-exports it as `types::version` and `types::{SemVer, SemVerParseError}`.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct SemVer(semver::Version);

#[derive(Debug, thiserror::Error)]
#[error("invalid semantic version '{value}': {message}")]
pub struct SemVerParseError {
    value: String,
    message: String,
}

impl SemVer {
    pub fn parse(value: impl AsRef<str>) -> Result<Self, SemVerParseError> {
        let value = value.as_ref();
        semver::Version::parse(value)
            .map(Self)
            .map_err(|error| SemVerParseError {
                value: value.to_string(),
                message: error.to_string(),
            })
    }

    pub fn as_version(&self) -> &semver::Version {
        &self.0
    }
}

impl fmt::Display for SemVer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for SemVer {
    type Err = SemVerParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<String> for SemVer {
    type Error = SemVerParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<SemVer> for String {
    fn from(value: SemVer) -> Self {
        value.to_string()
    }
}

impl From<semver::Version> for SemVer {
    fn from(value: semver::Version) -> Self {
        Self(value)
    }
}

#[cfg(feature = "serde")]
impl Serialize for SemVer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for SemVer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(de::Error::custom)
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for SemVer {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <str as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <str as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for SemVer {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let value = self.to_string();
        <&str as sqlx::Encode<'q, sqlx::Postgres>>::encode(value.as_str(), buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for SemVer {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <&str as sqlx::Decode<'r, sqlx::Postgres>>::decode(value)?;
        Self::parse(raw).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests;
