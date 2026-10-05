use super::*;
use crate::sql::baseline_cutover::{CutoverManifest, HistoryRow};
use rusqlite::{Transaction, TransactionBehavior};

const SERVICE_HISTORY: &str = "_sqlx_migrations";
const COMMON_HISTORY: &str = "_space_common_schema_migrations";

pub(super) async fn prepare(
    store: &str,
    path: &Path,
    service: &Migrator,
    common: Option<&Migrator>,
) -> SqliteResult<()> {
    if !super::super::baselines::is_public(service) {
        return Ok(());
    }
    let store = store.to_owned();
    let path = path.to_owned();
    let service = Migrator::with_migrations(service.iter().cloned().collect());
    let common = common.map(|m| Migrator::with_migrations(m.iter().cloned().collect()));
    tokio::task::spawn_blocking(move || {
        if !path.exists() {
            return Ok(());
        }
        let mut conn = Connection::open(&path).context("failed to open database for cutover")?;
        conn.busy_timeout(Duration::from_secs(5))
            .context("failed to set cutover lock timeout")?;
        convert(&store, &mut conn, &service, common.as_ref())
    })
    .await
    .context("SQLite baseline cutover task failed")?
}

fn history(conn: &Connection, table: &str) -> SqliteResult<Vec<HistoryRow>> {
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name = ?1)",
            [table],
            |r| r.get(0),
        )
        .context("failed to inspect cutover history")?;
    if !exists {
        return Ok(Vec::new());
    }
    conn.prepare(&format!(
        "SELECT version, success, checksum FROM {table} ORDER BY version"
    ))
    .context("failed to prepare cutover history query")?
    .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
    .context("failed to query cutover history")?
    .collect::<Result<_, _>>()
    .context("failed to read cutover history")
}

fn first(migrator: &Migrator) -> SqliteResult<&Migration> {
    migrator
        .iter()
        .filter(|m| m.migration_type.is_up_migration())
        .min_by_key(|m| m.version)
        .context("cutover requires a nonempty catalog")
}

fn needs_cutover(rows: &[HistoryRow], migrator: &Migrator) -> SqliteResult<bool> {
    let baseline = first(migrator)?.version;
    Ok(super::super::baselines::is_public(migrator) && rows.iter().any(|r| r.0 < baseline))
}

fn validate(
    manifest: &CutoverManifest,
    store: &str,
    rows: &[HistoryRow],
    migrator: &Migrator,
) -> SqliteResult<()> {
    if rows.is_empty() {
        return Err(SqliteError::message(
            "cutover requires complete service and shared histories",
        ));
    }
    if rows
        .first()
        .is_some_and(|r| r.0 < super::super::baselines::PUBLIC_SQLITE_BASELINE)
    {
        manifest
            .validate_legacy(store, rows)
            .context("cutover history rejected")?;
    } else {
        let checksums = AcceptedChecksums::embedded().context("invalid checksum manifest")?;
        for (version, success, checksum) in rows {
            if !success
                || !migrator.iter().any(|m| {
                    m.migration_type.is_up_migration()
                        && m.version == *version
                        && checksums.accepts(store, *version, &m.checksum, checksum)
                })
            {
                return Err(SqliteError::message(
                    "cutover found incompatible public history",
                ));
            }
        }
        let max_applied = rows.last().context("cutover history is empty")?.0;
        if migrator.iter().any(|m| {
            m.migration_type.is_up_migration()
                && m.version <= max_applied
                && !rows.iter().any(|r| r.0 == m.version)
        }) {
            return Err(SqliteError::message(
                "cutover found incomplete public history",
            ));
        }
    }
    if needs_cutover(rows, migrator)? {
        manifest
            .validate_target(store, first(migrator)?)
            .context("cutover target rejected")?;
    }
    Ok(())
}

fn convert(
    store: &str,
    conn: &mut Connection,
    service: &Migrator,
    common: Option<&Migrator>,
) -> SqliteResult<()> {
    let manifest = CutoverManifest::embedded().context("invalid cutover manifest")?;
    if !manifest.stores.contains_key(store) {
        return Ok(());
    }
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Exclusive)
        .context("failed to lock SQLite baseline cutover")?;
    let service_rows = history(&tx, SERVICE_HISTORY)?;
    let common_rows = if common.is_some() {
        history(&tx, COMMON_HISTORY)?
    } else {
        Vec::new()
    };
    let convert_service = needs_cutover(&service_rows, service)?;
    let convert_common = common
        .map(|m| needs_cutover(&common_rows, m))
        .transpose()?
        .unwrap_or(false);
    if !convert_service && !convert_common {
        return Ok(());
    }
    validate(&manifest, store, &service_rows, service)?;
    if let Some(m) = common {
        validate(&manifest, "common", &common_rows, m)?;
    }
    let integrity: String = tx
        .query_row("PRAGMA integrity_check", [], |r| r.get(0))
        .context("cutover integrity check failed")?;
    if integrity != "ok" {
        return Err(SqliteError::message(format!(
            "SQLite integrity violations prevent cutover: {integrity}",
        )));
    }
    if store == "sandbox-manager"
        && tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM jobs WHERE kind = 'snapshot_upload'
         AND status IN ('pending', 'running') AND CASE WHEN json_valid(payload)
         THEN json_type(payload, '$.includes_metadata_job') IS NOT 'true' ELSE 1 END)",
                [],
                |r| r.get::<_, bool>(0),
            )
            .context("failed to check snapshot-upload admission")?
    {
        return Err(SqliteError::message(
            "active snapshot uploads lack staged metadata; finish them before cutover",
        ));
    }
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS _space_baseline_archive (
        store TEXT NOT NULL, version BIGINT NOT NULL, description TEXT NOT NULL,
        installed_on TIMESTAMP NOT NULL, success BOOLEAN NOT NULL,
        checksum BLOB NOT NULL, execution_time BIGINT NOT NULL,
        PRIMARY KEY (store, version));",
    )
    .context("failed to create cutover history archive")?;
    if convert_service {
        replace(&tx, store, SERVICE_HISTORY, first(service)?)?;
    }
    if convert_common {
        replace(
            &tx,
            "common",
            COMMON_HISTORY,
            first(common.context("missing common catalog")?)?,
        )?;
    }
    tx.commit().context("failed to commit baseline cutover")?;
    tracing::info!(
        store,
        convert_service,
        convert_common,
        "completed validated baseline history cutover"
    );
    Ok(())
}

fn replace(
    tx: &Transaction<'_>,
    store: &str,
    table: &str,
    baseline: &Migration,
) -> SqliteResult<()> {
    if tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM _space_baseline_archive WHERE store = ?1)",
            [store],
            |r| r.get::<_, bool>(0),
        )
        .context("failed to inspect cutover archive")?
    {
        return Err(SqliteError::message(
            "cutover archive already exists; refusing to overwrite recovery history",
        ));
    }
    tx.execute(
        &format!(
            "INSERT INTO _space_baseline_archive SELECT ?1, version, description,
        installed_on, success, checksum, execution_time FROM {table}"
        ),
        [store],
    )
    .context("failed to archive migration history")?;
    tx.execute(&format!("DELETE FROM {table}"), [])
        .context("failed to replace migration history")?;
    tx.execute(
        &format!(
            "INSERT INTO {table} (version, description, success, checksum, execution_time)
        VALUES (?1, ?2, TRUE, ?3, 0)"
        ),
        params![
            baseline.version,
            baseline.description.as_ref(),
            baseline.checksum.as_ref()
        ],
    )
    .context("failed to record public baseline")?;
    Ok(())
}

#[cfg(test)]
mod tests;
