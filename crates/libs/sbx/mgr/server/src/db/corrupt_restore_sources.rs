use pplx_space_common_core::types::ExactStorageUri;
use rusqlite::{Connection, Result, params};

pub(crate) fn record_corrupt_restore_source(
    conn: &Connection,
    sandbox_id: &str,
    source: &ExactStorageUri,
) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO corrupt_restore_sources (sandbox_id, storage_uri) VALUES (?1, ?2)",
        params![sandbox_id, source.parsed().to_string()],
    )?;
    Ok(())
}

pub(crate) fn restore_source_is_corrupt(
    conn: &Connection,
    sandbox_id: &str,
    source: &ExactStorageUri,
) -> Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM corrupt_restore_sources WHERE sandbox_id = ?1 AND storage_uri = ?2)",
        params![sandbox_id, source.parsed().to_string()],
        |row| row.get(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corruption_survives_reopen_is_source_scoped_and_is_removed_with_sandbox() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sandbox.db");
        crate::db::create_test_schema_on_disk(&path);
        let source: ExactStorageUri = "s3://test-bucket/snapshot-one?region=us-east-1"
            .parse()
            .unwrap();
        let other: ExactStorageUri = "s3://test-bucket/snapshot-two?region=us-east-1"
            .parse()
            .unwrap();
        let sandbox_id = pplx_space_common_core::types::SandboxId::new().to_string();
        let conn = Connection::open(&path).unwrap();
        assert!(!restore_source_is_corrupt(&conn, &sandbox_id, &source).unwrap());
        record_corrupt_restore_source(&conn, &sandbox_id, &source).unwrap();
        record_corrupt_restore_source(&conn, &sandbox_id, &source).unwrap();
        drop(conn);
        let conn = Connection::open(&path).unwrap();
        assert!(restore_source_is_corrupt(&conn, &sandbox_id, &source).unwrap());
        assert!(!restore_source_is_corrupt(&conn, &sandbox_id, &other).unwrap());
        assert!(!restore_source_is_corrupt(&conn, "another-sandbox", &source).unwrap());
        crate::db::delete_sandbox(&conn, &sandbox_id).unwrap();
        assert!(!restore_source_is_corrupt(&conn, &sandbox_id, &source).unwrap());
    }
}
