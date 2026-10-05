use super::*;

#[test]
fn upload_format_admission_preserves_terminal_evidence_without_writes() {
    for payload in [
        "malformed",
        "{}",
        r#"{"includes_metadata_job":false}"#,
        r#"{"includes_metadata_job":1}"#,
        r#"{"includes_metadata_job":true}"#,
    ] {
        for status in ["pending", "running", "completed", "failed"] {
            let conn = Connection::open_in_memory().unwrap();
            for migration in COMMON_SCHEMA_MIGRATOR
                .iter()
                .filter(|migration| migration.migration_type.is_up_migration())
            {
                conn.execute_batch(migration.sql.as_ref()).unwrap();
            }
            conn.execute("INSERT INTO jobs (job_id, kind, status, payload) VALUES ('cutover', 'snapshot_upload', ?1, ?2)", [status, payload]).unwrap();
            let before = conn.total_changes();
            let expected = !matches!(status, "pending" | "running")
                || payload == r#"{"includes_metadata_job":true}"#;
            let result = prepare_migrations_inner(&conn);
            assert_eq!(result.is_ok(), expected, "{status}: {payload}");
            if !expected {
                assert!(matches!(
                    result,
                    Err(PrepareMigrationsError::LegacyUpload(_))
                ));
                assert_eq!(conn.total_changes(), before);
            }
            assert_eq!(
                conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                    .unwrap(),
                0
            );
            assert_eq!(
                conn.query_row(
                    "SELECT payload FROM jobs WHERE job_id = 'cutover'",
                    [],
                    |row| row.get::<_, String>(0)
                )
                .unwrap(),
                payload
            );
        }
    }
}

#[test]
fn schema_floor_admission_rejects_forward_only_stores_without_writes() {
    for (required, preparation_accepts, previous_cutover_accepts, current_accepts) in [
        (0, true, true, true),
        (1, true, true, true),
        (2, false, true, true),
        (3, false, false, true),
        (4, false, false, false),
        (-1, false, false, false),
    ] {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "user_version", required).unwrap();
        assert_eq!(check_schema_floor(&conn, 1).is_ok(), preparation_accepts);
        assert_eq!(
            check_schema_floor(&conn, 2).is_ok(),
            previous_cutover_accepts
        );
        assert_eq!(check_schema_floor(&conn, 3).is_ok(), current_accepts);
        if check_schema_floor(&conn, SUPPORTED_SCHEMA_FLOOR).is_err() {
            let changes = conn.total_changes();
            let error = prepare_migrations_inner(&conn).unwrap_err();
            assert!(matches!(
                error,
                PrepareMigrationsError::UnsupportedSchemaFloor { .. }
            ));
            assert_eq!(conn.total_changes(), changes);
            assert_eq!(
                conn.query_row("SELECT COUNT(*) FROM sqlite_schema", [], |row| row
                    .get::<_, i64>(0))
                    .unwrap(),
                0
            );
        }
        assert_eq!(
            conn.pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            required
        );
    }
}
