use pplx_space_common_core::sql;
use sqlx::{PgPool, Postgres, Transaction};

pub(super) mod lifecycle;
pub(super) mod observed;
pub(super) mod pods;
pub(super) mod ports;
pub(super) mod quarantine;
pub(super) mod snapshots;
pub(super) mod timeline;

/// A read-only transaction for one debug source. `REPEATABLE READ` so the
/// several statements a source runs over the same rows see one snapshot:
/// under `READ COMMITTED` a snapshot could read `creating` beside a storage
/// placement taken after it finished, or a quarantine episode beside history
/// that already records its release.
pub(in crate::routes::debug) async fn begin_read_only(
    pool: &PgPool,
) -> Result<Transaction<'_, Postgres>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(sql!(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    ))
    .execute(&mut *tx)
    .await?;
    Ok(tx)
}

pub(in crate::routes::debug) fn source_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
