//! Identifiers that wrap a UUID, and their codecs: [`uuid_id!`] defines one,
//! [`postgres_uuid!`] stores one in Postgres as a `UUID` and
//! [`sqlite_uuid!`] in SQLite as text, and [`AuthIdParseError`] is the error
//! parsing one that [`uuid_id!`] defines.
//!
//! `pplx-space-job-id`'s `JobId` and the account identifiers in
//! `pplx-space-accounts` are made with [`uuid_id!`]. Model crates invoke
//! [`postgres_uuid!`] for identifiers they define themselves. The codec macros
//! can't live in the codec facade, `pplx-space-sql-codec`, which archcheck
//! holds to re-exports.

/// Error returned when a UUID identifier cannot be parsed.
///
/// Every identifier [`uuid_id!`] defines shares it, `pplx-space-job-id`'s
/// `JobId` and the account identifiers in `pplx-space-accounts` among them.
///
/// # Examples
///
/// ```rust
/// # use std::str::FromStr;
/// use pplx_space_uuid_id::AuthIdParseError;
///
/// pplx_space_uuid_id::uuid_id!(WidgetId);
///
/// assert!(matches!(
///     WidgetId::from_str("not-a-uuid"),
///     Err(AuthIdParseError::InvalidUuid(_))
/// ));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, thiserror::Error)]
pub enum AuthIdParseError {
    #[error("invalid auth UUID: {0}")]
    InvalidUuid(#[from] uuid::Error),
}

/// What [`uuid_id!`] and [`postgres_uuid!`] expand to, so a caller needs no
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
/// `serde` and `sqlx` features are on, and use its `serde` and
/// `pplx-space-sql-codec` dependencies: serde as the UUID's string form,
/// sqlx as a Postgres `UUID`, through [`postgres_uuid!`]. A caller declares
/// both features, even if it enables neither, and its `serde` dependency
/// needs serde's `derive` feature and its `sqlx` feature
/// `pplx-space-sql-codec/postgres`.
/// Attributes before the name, doc comments included, are put on the
/// struct.
///
/// The generated methods' doc examples import the invoking module through
/// `module_path!()`, so invoke it in a module reachable from outside the
/// crate, or those doctests fail to compile. They also call `uuid`, so a
/// caller needs it at least as a dev-dependency.
///
/// ```rust
/// pplx_space_uuid_id::uuid_id!(
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
        #[cfg_attr(feature = "serde", serde(try_from = "::std::string::String", into = "::std::string::String"))]
        #[derive(
            ::core::clone::Clone,
            ::core::marker::Copy,
            ::core::cmp::Eq,
            ::core::cmp::PartialEq,
            ::core::cmp::Ord,
            ::core::cmp::PartialOrd,
            ::core::hash::Hash,
        )]
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
                "let id = ",
                stringify!($name),
                "::new();\n",
                "assert_eq!(",
                stringify!($name),
                "::from_uuid(id.as_uuid()), id);\n",
                "assert_eq!(",
                stringify!($name),
                "::from_uuid(uuid::Uuid::nil()).as_uuid(), uuid::Uuid::nil());\n",
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
                "let id = ",
                stringify!($name),
                "::new();\n",
                "assert_eq!(",
                stringify!($name),
                "::from_uuid(id.as_uuid()), id);\n",
                "assert_eq!(",
                stringify!($name),
                "::from_uuid(uuid::Uuid::nil()).as_uuid(), uuid::Uuid::nil());\n",
                "```"
            )]
            pub const fn as_uuid(self) -> $crate::__private::Uuid {
                self.0
            }
        }

        impl ::core::fmt::Debug for $name {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.debug_tuple(stringify!($name))
                    .field(&::std::string::ToString::to_string(self))
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

        impl ::core::convert::TryFrom<::std::string::String> for $name {
            type Error = $crate::AuthIdParseError;

            fn try_from(value: ::std::string::String) -> ::core::result::Result<Self, Self::Error> {
                <Self as ::core::str::FromStr>::from_str(&value)
            }
        }

        impl ::core::convert::From<$name> for ::std::string::String {
            fn from(value: $name) -> Self {
                ::std::string::ToString::to_string(&value)
            }
        }

        #[cfg(feature = "sqlx")]
        $crate::postgres_uuid!($name);
    };
}

/// Stores each listed type in Postgres as a `UUID`, and a list of them as a
/// `UUID[]`: the `Type`, `Encode`, `Decode` and `PgHasArrayType` impls
/// delegate to `uuid::Uuid`'s, through the type's `as_uuid(self)` and
/// `from_uuid`, so the type must be `Copy`. The impls use the calling crate's
/// `pplx-space-sql-codec` dependency, with its `postgres` feature; a caller
/// gates the invocation on its own feature. [`uuid_id!`] invokes it for the
/// identifiers it defines.
///
/// ```rust
/// use pplx_space_sql_codec::postgres::PgHasArrayType;
/// use pplx_space_sql_codec::{Postgres, Type};
/// use uuid::Uuid;
///
/// #[derive(Clone, Copy)]
/// struct WidgetId(Uuid);
///
/// impl WidgetId {
///     const fn from_uuid(value: Uuid) -> Self {
///         Self(value)
///     }
///
///     const fn as_uuid(self) -> Uuid {
///         self.0
///     }
/// }
///
/// pplx_space_uuid_id::postgres_uuid!(WidgetId);
///
/// assert_eq!(
///     <WidgetId as Type<Postgres>>::type_info(),
///     <Uuid as Type<Postgres>>::type_info(),
/// );
/// assert_eq!(
///     <WidgetId as PgHasArrayType>::array_type_info(),
///     <Uuid as PgHasArrayType>::array_type_info(),
/// );
/// ```
#[macro_export]
macro_rules! postgres_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl ::pplx_space_sql_codec::Type<::pplx_space_sql_codec::Postgres> for $name {
            fn type_info() -> ::pplx_space_sql_codec::postgres::PgTypeInfo {
                <$crate::__private::Uuid as ::pplx_space_sql_codec::Type<::pplx_space_sql_codec::Postgres>>::type_info()
            }

            fn compatible(ty: &::pplx_space_sql_codec::postgres::PgTypeInfo) -> bool {
                <$crate::__private::Uuid as ::pplx_space_sql_codec::Type<::pplx_space_sql_codec::Postgres>>::compatible(ty)
            }
        }

        impl ::pplx_space_sql_codec::postgres::PgHasArrayType for $name {
            fn array_type_info() -> ::pplx_space_sql_codec::postgres::PgTypeInfo {
                <$crate::__private::Uuid as ::pplx_space_sql_codec::postgres::PgHasArrayType>::array_type_info()
            }
        }

        impl<'q> ::pplx_space_sql_codec::Encode<'q, ::pplx_space_sql_codec::Postgres> for $name {
            fn encode_by_ref(
                &self,
                buf: &mut ::pplx_space_sql_codec::postgres::PgArgumentBuffer,
            ) -> ::core::result::Result<::pplx_space_sql_codec::encode::IsNull, ::pplx_space_sql_codec::error::BoxDynError> {
                <$crate::__private::Uuid as ::pplx_space_sql_codec::Encode<'q, ::pplx_space_sql_codec::Postgres>>::encode_by_ref(
                    &self.as_uuid(), buf,
                )
            }
        }

        impl<'r> ::pplx_space_sql_codec::Decode<'r, ::pplx_space_sql_codec::Postgres> for $name {
            fn decode(
                value: ::pplx_space_sql_codec::postgres::PgValueRef<'r>,
            ) -> ::core::result::Result<Self, ::pplx_space_sql_codec::error::BoxDynError> {
                ::core::result::Result::Ok(Self::from_uuid(
                    <$crate::__private::Uuid as ::pplx_space_sql_codec::Decode<'r, ::pplx_space_sql_codec::Postgres>>::decode(value)?,
                ))
            }
        }
    )+};
}

/// Stores each listed type in SQLite as its `Display` text and reads it back
/// with `FromStr`, as node-local schemas store UUID identifiers. The impls use
/// the calling crate's `pplx-space-sql-codec` dependency, with its `sqlite`
/// feature; a caller gates the invocation on its own feature.
///
/// ```rust
/// pplx_space_uuid_id::uuid_id!(WidgetId);
/// pplx_space_uuid_id::sqlite_uuid!(WidgetId);
///
/// let conn = rusqlite::Connection::open_in_memory().unwrap();
/// let id = WidgetId::new();
/// let stored: String = conn.query_row("SELECT ?1", [&id], |row| row.get(0)).unwrap();
/// assert_eq!(stored, id.to_string());
/// ```
#[macro_export]
macro_rules! sqlite_uuid {
    ($($name:ty),+ $(,)?) => {$(
        impl ::pplx_space_sql_codec::sqlite::types::ToSql for $name {
            fn to_sql(&self) -> ::pplx_space_sql_codec::sqlite::Result<::pplx_space_sql_codec::sqlite::types::ToSqlOutput<'_>> {
                ::core::result::Result::Ok(::core::convert::Into::into(
                    ::std::string::ToString::to_string(self),
                ))
            }
        }

        impl ::pplx_space_sql_codec::sqlite::types::FromSql for $name {
            fn column_result(value: ::pplx_space_sql_codec::sqlite::types::ValueRef<'_>) -> ::pplx_space_sql_codec::sqlite::types::FromSqlResult<Self> {
                value
                    .as_str()?
                    .parse()
                    .map_err(|error| ::pplx_space_sql_codec::sqlite::types::FromSqlError::Other(::std::boxed::Box::new(error)))
            }
        }
    )+};
}
