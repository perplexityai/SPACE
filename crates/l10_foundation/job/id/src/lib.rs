//! Identity of a persisted background job, the UUID identifier macros it is
//! built with, and the error parsing one.
//!
//! It is its own crate so that the job queue (`pplx-space-common-jobs`) can
//! name a job without depending on `pplx-space-accounts` or
//! `pplx-space-common-core`. The account identifiers in
//! `pplx-space-accounts` are made with the same [`uuid_id!`] and
//! [`sqlite_uuid!`] macros and share [`AuthIdParseError`].

/// Error returned when an auth/account UUID cannot be parsed.
///
/// Every identifier [`uuid_id!`] defines, [`JobId`] and the account
/// identifiers `pplx-space-accounts` defines among them, shares it.
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

/// What [`uuid_id!`] and [`sqlite_uuid!`] expand to, so a caller needs no
/// `uuid` dependency of its own to use them.
#[doc(hidden)]
pub mod __private {
    pub use uuid::Uuid;
}

/// Defines a `Copy` identifier wrapping a UUID: `new` (UUIDv7), `from_uuid`
/// and `as_uuid`, `Display` and `FromStr` as the hyphenated UUID, with
/// [`AuthIdParseError`] as the parse error, and a `Debug` that names the type.
///
/// The `serde` and `sqlx` impls are compiled when the calling crate's own
/// `serde` and `sqlx` features are on, and use its `serde` and `sqlx`
/// dependencies: serde as the UUID's string form, sqlx as a Postgres `UUID`.
/// A caller declares both features, even if it enables neither. Attributes
/// before the name, doc comments included, are put on the struct.
///
/// ```rust
/// pplx_space_job_id::uuid_id!(
///     /// Example identifier.
///     WidgetId
/// );
///
/// let id: WidgetId = "00000000-0000-0000-0000-000000000000".parse().unwrap();
/// assert_eq!(id.as_uuid(), uuid::Uuid::nil());
/// ```
#[macro_export]
macro_rules! uuid_id {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
        #[cfg_attr(feature = "serde", serde(try_from = "String", into = "String"))]
        #[derive(Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name($crate::__private::Uuid);

        impl $name {
            #[doc = concat!(
                "Creates a fresh `",
                stringify!($name),
                "` using a UUIDv7 value.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use ",
                module_path!(),
                "::*;\n",
                "let id = ",
                stringify!($name),
                "::new();\n",
                "assert!(!id.to_string().is_empty());\n",
                "```"
            )]
            pub fn new() -> Self {
                Self($crate::__private::Uuid::now_v7())
            }

            #[doc = concat!(
                "Wraps an existing UUID as a `",
                stringify!($name),
                "`.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use ",
                module_path!(),
                "::*;\n",
                "let uuid = uuid::Uuid::nil();\n",
                "let id = ",
                stringify!($name),
                "::from_uuid(uuid);\n",
                "assert_eq!(id.as_uuid(), uuid);\n",
                "```"
            )]
            pub const fn from_uuid(value: $crate::__private::Uuid) -> Self {
                Self(value)
            }

            #[doc = concat!(
                "Returns the underlying UUID.\n\n",
                "# Examples\n\n",
                "```rust\n",
                "# use ",
                module_path!(),
                "::*;\n",
                "let uuid = uuid::Uuid::nil();\n",
                "let id = ",
                stringify!($name),
                "::from_uuid(uuid);\n",
                "assert_eq!(id.as_uuid(), uuid);\n",
                "```"
            )]
            pub const fn as_uuid(self) -> $crate::__private::Uuid {
                self.0
            }
        }

        impl ::core::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($name))
                    .field(&self.to_string())
                    .finish()
            }
        }

        impl ::core::fmt::Display for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                ::core::fmt::Display::fmt(&self.0, f)
            }
        }

        impl ::core::default::Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl ::core::str::FromStr for $name {
            type Err = $crate::AuthIdParseError;

            fn from_str(value: &str) -> ::core::result::Result<Self, Self::Err> {
                $crate::__private::Uuid::parse_str(value)
                    .map(Self)
                    .map_err(::core::convert::Into::into)
            }
        }

        impl ::core::convert::TryFrom<String> for $name {
            type Error = $crate::AuthIdParseError;

            fn try_from(value: String) -> ::core::result::Result<Self, Self::Error> {
                <Self as ::core::str::FromStr>::from_str(&value)
            }
        }

        impl ::core::convert::From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        #[cfg(feature = "sqlx")]
        impl ::sqlx::postgres::PgHasArrayType for $name {
            fn array_type_info() -> ::sqlx::postgres::PgTypeInfo {
                <$crate::__private::Uuid as ::sqlx::postgres::PgHasArrayType>::array_type_info()
            }
        }

        #[cfg(feature = "sqlx")]
        impl ::sqlx::Type<::sqlx::Postgres> for $name {
            fn type_info() -> ::sqlx::postgres::PgTypeInfo {
                <$crate::__private::Uuid as ::sqlx::Type<::sqlx::Postgres>>::type_info()
            }

            fn compatible(ty: &::sqlx::postgres::PgTypeInfo) -> bool {
                <$crate::__private::Uuid as ::sqlx::Type<::sqlx::Postgres>>::compatible(ty)
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'q> ::sqlx::Encode<'q, ::sqlx::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut ::sqlx::postgres::PgArgumentBuffer,
            ) -> ::core::result::Result<::sqlx::encode::IsNull, ::sqlx::error::BoxDynError> {
                <$crate::__private::Uuid as ::sqlx::Encode<'q, ::sqlx::Postgres>>::encode_by_ref(
                    &self.0, buf,
                )
            }
        }

        #[cfg(feature = "sqlx")]
        impl<'r> ::sqlx::Decode<'r, ::sqlx::Postgres> for $name {
            fn decode(
                value: ::sqlx::postgres::PgValueRef<'r>,
            ) -> ::core::result::Result<Self, ::sqlx::error::BoxDynError> {
                Ok(Self(
                    <$crate::__private::Uuid as ::sqlx::Decode<'r, ::sqlx::Postgres>>::decode(value)?,
                ))
            }
        }
    };
}

/// Stores each listed type in SQLite as its `Display` text and reads it back
/// with `FromStr`, as node-local schemas store UUID identifiers. The impls use
/// the calling crate's `rusqlite` dependency; a caller gates the invocation on
/// its own feature.
///
/// ```rust
/// pplx_space_job_id::uuid_id!(WidgetId);
/// pplx_space_job_id::sqlite_uuid!(WidgetId);
///
/// let conn = rusqlite::Connection::open_in_memory().unwrap();
/// let id = WidgetId::new();
/// let stored: String = conn.query_row("SELECT ?1", [&id], |row| row.get(0)).unwrap();
/// assert_eq!(stored, id.to_string());
/// ```
#[macro_export]
macro_rules! sqlite_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl ::rusqlite::types::ToSql for $name {
            fn to_sql(&self) -> ::rusqlite::Result<::rusqlite::types::ToSqlOutput<'_>> {
                Ok(self.to_string().into())
            }
        }

        impl ::rusqlite::types::FromSql for $name {
            fn column_result(value: ::rusqlite::types::ValueRef<'_>) -> ::rusqlite::types::FromSqlResult<Self> {
                value.as_str()?.parse().map_err(|error| ::rusqlite::types::FromSqlError::Other(Box::new(error)))
            }
        }
    )+};
}

uuid_id!(
    /// Identity of a persisted background job, retained across retries and recovery.
    JobId
);

// SQLite stores the UUID as text, as for the other identifiers in
// node-local schemas. Unit tests compile it without the feature, through the
// rusqlite dev-dependency, so the round trip always runs.
#[cfg(any(feature = "rusqlite", test))]
sqlite_uuid!(JobId);

#[cfg(test)]
mod tests;
