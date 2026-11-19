//! The items a codec needs to encode a type into a SQL driver's values and
//! decode it back, and nothing else: no query, pool, connection, executor,
//! transaction or row.
//!
//! An `l10_foundation` or `l20_domain` crate implements its types' codecs
//! against these re-exports, behind an optional dependency on this crate
//! and a feature of its own, instead of depending on the driver: storage
//! helpers aside, archcheck lets those layers reach a driver no other way,
//! and lets only stores and storage helpers turn the driver features on,
//! directly or through such a codec feature, with today's exceptions
//! baselined. So these re-exports are what a codec can use. This
//! crate's entry in `[rules.layers.storage.codec_facades]` in
//! `ARCHITECTURE.toml` lists each of them, and archcheck reads this source
//! with syn, a Rust parser, and strictly: `pub mod` blocks and `pub use`
//! items of the listed exports, with `cfg` and `doc` attributes and
//! comments, and nothing else. Adding an export means adding it there, a
//! policy change reviewed as one: anything a codec does not strictly need
//! belongs in a store.
//!
//! The re-exports keep the drivers' paths, below the crate root for sqlx
//! and below `sqlite` for rusqlite, and are the drivers' own items, so an
//! impl of `Type` is an impl of `sqlx::Type` for the stores that bind the
//! type.

#[cfg(feature = "postgres")]
pub use sqlx::{Decode, Encode, Postgres, Type};

/// sqlx's `encode` items.
#[cfg(feature = "postgres")]
pub mod encode {
    pub use sqlx::encode::IsNull;
}

/// sqlx's `error` items.
#[cfg(feature = "postgres")]
pub mod error {
    pub use sqlx::error::BoxDynError;
}

/// sqlx's Postgres items.
#[cfg(feature = "postgres")]
pub mod postgres {
    pub use sqlx::postgres::{PgArgumentBuffer, PgHasArrayType, PgTypeInfo, PgValueRef};
}

/// rusqlite's items.
#[cfg(feature = "sqlite")]
pub mod sqlite {
    pub use rusqlite::Result;

    /// rusqlite's `types` items.
    pub mod types {
        pub use rusqlite::types::{
            FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef,
        };
    }
}
