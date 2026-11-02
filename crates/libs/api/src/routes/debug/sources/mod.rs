use pplx_space_common_core::sql;
use sqlx::{PgPool, Postgres, Transaction};

pub(super) mod lifecycle;
pub(super) mod observed;
pub(super) mod pods;
pub(super) mod ports;
pub(super) mod quarantine;
pub(super) mod snapshots;
pub(super) mod timeline;

pub(in crate::routes::debug) async fn begin_read_only(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!("SET TRANSACTION READ ONLY"))
        .execute(&mut *tx)
        .await?;
    Ok(tx)
}

pub(in crate::routes::debug) fn source_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
