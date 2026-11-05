//! Identity of a persisted background job, and the error parsing one.
//!
//! It is its own crate so that the job queue (`pplx-space-common-jobs`) can
//! name a job without depending on `pplx-space-accounts`, which re-exports
//! both items alongside its account identifiers, and `pplx-space-common-core`
//! from `types`.

use std::fmt;
use std::str::FromStr;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Error returned when an auth/account UUID cannot be parsed.
///
/// [`JobId`] and the account identifiers `pplx-space-accounts` defines share
/// it.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// # use pplx_space_job_id::{AuthIdParseError, JobId};
/// assert!(matches!(
///     JobId::from_str("not-a-uuid"),
///     Err(AuthIdParseError::InvalidUuid(_))
/// ));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AuthIdParseError {
    #[error("invalid auth UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// Identity of a persisted background job, retained across retries and recovery.
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
#[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct JobId(Uuid);

impl JobId {
    /// Creates a fresh `JobId` using a UUIDv7 value.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_job_id::JobId;
    /// let id = JobId::new();
    /// assert!(!id.to_string().is_empty());
    /// ```
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Wraps an existing UUID as a `JobId`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_job_id::JobId;
    /// let uuid = uuid::Uuid::nil();
    /// let id = JobId::from_uuid(uuid);
    /// assert_eq!(id.as_uuid(), uuid);
    /// ```
    pub const fn from_uuid(value: Uuid) -> Self {
        Self(value)
    }

    /// Returns the underlying UUID.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use pplx_space_job_id::JobId;
    /// let uuid = uuid::Uuid::nil();
    /// let id = JobId::from_uuid(uuid);
    /// assert_eq!(id.as_uuid(), uuid);
    /// ```
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

impl fmt::Debug for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("JobId").field(&self.to_string()).finish()
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl Default for JobId {
    fn default() -> Self {
        Self::new()
    }
}

impl FromStr for JobId {
    type Err = AuthIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self).map_err(Into::into)
    }
}

impl TryFrom<String> for JobId {
    type Error = AuthIdParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::from_str(&value)
    }
}

impl From<JobId> for String {
    fn from(value: JobId) -> Self {
        value.to_string()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::postgres::PgHasArrayType for JobId {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::postgres::PgHasArrayType>::array_type_info()
    }
}

#[cfg(feature = "sqlx")]
impl sqlx::Type<sqlx::Postgres> for JobId {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        <Uuid as sqlx::Type<sqlx::Postgres>>::type_info()
    }

    fn compatible(ty: &sqlx::postgres::PgTypeInfo) -> bool {
        <Uuid as sqlx::Type<sqlx::Postgres>>::compatible(ty)
    }
}

#[cfg(feature = "sqlx")]
impl<'q> sqlx::Encode<'q, sqlx::Postgres> for JobId {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <Uuid as sqlx::Encode<'q, sqlx::Postgres>>::encode_by_ref(&self.0, buf)
    }
}

#[cfg(feature = "sqlx")]
impl<'r> sqlx::Decode<'r, sqlx::Postgres> for JobId {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<Uuid as sqlx::Decode<'r, sqlx::Postgres>>::decode(
            value,
        )?))
    }
}

// SQLite stores the UUID as text, as for the other identifiers in
// node-local schemas. Unit tests compile it without the feature, through the
// rusqlite dev-dependency, so the round trip always runs.
#[cfg(any(feature = "rusqlite", test))]
impl rusqlite::types::ToSql for JobId {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        Ok(self.to_string().into())
    }
}

#[cfg(any(feature = "rusqlite", test))]
impl rusqlite::types::FromSql for JobId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        value
            .as_str()?
            .parse()
            .map_err(|error| rusqlite::types::FromSqlError::Other(Box::new(error)))
    }
}

#[cfg(test)]
mod tests;
