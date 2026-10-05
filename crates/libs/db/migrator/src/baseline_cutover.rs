use pplx_space_common_svc::sql;
use pplx_space_common_svc::sql::baseline_cutover::{CutoverError, CutoverManifest, HistoryRow};
use sqlx::{PgPool, migrate::Migrator};

const POSTGRES_BASELINE_ADVISORY_LOCK: i64 = 0x5350_4143_455f_424c;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Admission(#[from] CutoverError),
    #[error("PostgreSQL baseline cutover failed: {0}")]
    Database(#[from] sqlx::Error),
    #[error("PostgreSQL cutover archive already exists; recovery history will not be overwritten")]
    ExistingArchive,
}

/// Convert only the reviewed historical catalog. Ordinary SQLx handles fresh and public histories.
pub async fn prepare(pool: &PgPool, migrator: &Migrator) -> Result<(), Error> {
    let Some(baseline) = migrator
        .iter()
        .filter(|m| m.migration_type.is_up_migration())
        .min_by_key(|m| m.version)
    else {
        return Ok(());
    };
    let mut tx = pool.begin().await?;
    sqlx::raw_sql(sql!(
        "SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '30s'"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!("SELECT pg_advisory_xact_lock($1)"))
        .bind(POSTGRES_BASELINE_ADVISORY_LOCK)
        .execute(&mut *tx)
        .await?;
    let exists: bool = sqlx::query_scalar(sql!(
        "SELECT to_regclass('public._sqlx_migrations') IS NOT NULL"
    ))
    .fetch_one(&mut *tx)
    .await?;
    if !exists {
        return Ok(());
    }
    sqlx::query(sql!(
        "LOCK TABLE public._sqlx_migrations IN ACCESS EXCLUSIVE MODE"
    ))
    .execute(&mut *tx)
    .await?;
    let rows: Vec<HistoryRow> = sqlx::query_as(sql!(
        "SELECT version, success, checksum FROM public._sqlx_migrations ORDER BY version"
    ))
    .fetch_all(&mut *tx)
    .await?;
    if !rows.iter().any(|r| r.0 < baseline.version) {
        return Ok(());
    }
    let manifest = CutoverManifest::embedded()?;
    manifest.validate_target("postgres", baseline)?;
    manifest.validate_legacy("postgres", &rows)?;
    let archived: bool = sqlx::query_scalar(sql!(
        "SELECT to_regclass('space_migration_archive.postgres_history') IS NOT NULL"
    ))
    .fetch_one(&mut *tx)
    .await?;
    if archived {
        return Err(Error::ExistingArchive);
    }
    sqlx::raw_sql(sql!(
        "CREATE TABLE space_migration_archive.postgres_history AS TABLE public._sqlx_migrations;
        REVOKE ALL ON space_migration_archive.postgres_history FROM PUBLIC;
        DELETE FROM public._sqlx_migrations;"
    ))
    .execute(&mut *tx)
    .await?;
    sqlx::query(sql!("INSERT INTO public._sqlx_migrations (version, description, success, checksum, execution_time)
        VALUES ($1, $2, TRUE, $3, 0)"))
        .bind(baseline.version).bind(baseline.description.as_ref()).bind(baseline.checksum.as_ref())
        .execute(&mut *tx).await?;
    tx.commit().await?;
    tracing::info!(
        version = baseline.version,
        "completed validated PostgreSQL baseline history cutover"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
